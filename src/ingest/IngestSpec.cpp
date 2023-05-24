/*
 * Copyright 2017-present varchar.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "IngestSpec.h"

#include <gflags/gflags.h>
#include <gperftools/heap-profiler.h>
#include <rapidjson/document.h>

#include "MacroRow.h"
#include "common/Evidence.h"
#include "execution/BlockManager.h"
#include "execution/meta/TableService.h"
#include "meta/Macro.h"
#include "meta/TestTable.h"
#include "storage/CsvReader.h"
#include "storage/JsonReader.h"
#include "storage/NFS.h"
#include "storage/ParquetReader.h"
#include "storage/VectorReader.h"
#include "storage/http/Http.h"
#include "storage/kafka/KafkaReader.h"
#include "type/Serde.h"

// TODO(cao) - system wide enviroment configs should be moved to cluster config to provide
// table-wise customization
DEFINE_string(NTEST_LOADER, "NebulaTest", "define the loader name for loading nebula test data");
DEFINE_uint64(NBLOCK_MAX_ROWS, 1000000, "max rows per block");

/**
 * We will sync etcd configs for cluster info into this memory object
 * To understand cluster status - total nodes.
 */
namespace nebula {
namespace ingest {

using dsu = nebula::meta::DataSourceUtils;
using nebula::common::Evidence;
using nebula::common::MapKV;
using nebula::common::unordered_map;
using nebula::execution::BlockManager;
using nebula::execution::io::BatchBlock;
using nebula::execution::io::BlockList;
using nebula::execution::io::BlockLoader;
using nebula::execution::meta::TableService;
using nebula::memory::Batch;
using nebula::meta::BessType;
using nebula::meta::BlockSignature;
using nebula::meta::DataFormat;
using nebula::meta::DataSource;
using nebula::meta::Macro;
using nebula::meta::SpecSplitPtr;
using nebula::meta::Table;
using nebula::meta::TablePtr;
using nebula::meta::TableSpecPtr;
using nebula::meta::TestTable;
using nebula::meta::TimeSpec;
using nebula::meta::TimeType;
using nebula::storage::CsvReader;
using nebula::storage::JsonVectorReader;
using nebula::storage::makeJsonReader;
using nebula::storage::ParquetReader;
using nebula::storage::http::HttpService;
using nebula::storage::kafka::KafkaReader;
using nebula::storage::kafka::KafkaSegment;
using nebula::surface::RowCursor;
using nebula::surface::RowData;
using nebula::type::LongType;
using nebula::type::TypeSerializer;

static constexpr auto LOADER_SWAP = "Swap";
static constexpr auto LOADER_ROLL = "Roll";
static constexpr auto LOADER_API = "Api";

// a settings to overwrite batch size of a table
static constexpr auto BATCH_SIZE = "batch";

// load some nebula test data into current process
void loadNebulaTestData(const TableSpecPtr& table, const std::string& spec) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  // (NOTE) table->max_seconds is not serialized, it will be 0
  int64_t start = table->timeSpec.unixTimeValue;
  int64_t end = start + table->max_seconds;

  // let's plan these many data std::thread::hardware_concurrency()
  TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    int64_t begin = start + i * window;
    bm->add(nebula::meta::BlockSignature{
      table->name, "v1", i++, begin, begin + window, spec });
  }
}

size_t IngestSpec::work() noexcept {
  // register the table in the working node
  // in case it is the first time
  auto registry = TableService::singleton()->get(table_);

  // TODO(cao) - refator this to have better hirachy for different ingest types.
  const auto& loader = table_->loader;
  if (loader == FLAGS_NTEST_LOADER) {
    loadNebulaTestData(table_, id_);
    return 1;
  }

  // either swap, they are reading files
  if (loader == LOADER_SWAP) {
    return this->loadSwap();
  }

  // if roll, they are reading files
  if (loader == LOADER_ROLL) {
    return this->loadRoll();
  }

  if (loader == LOADER_API) {
    return this->loadApi();
  }

  if (DataSource::KAFKA == table_->source) {
    return this->loadKafka(split());
  }

  if (DataSource::ROCKSET == table_->source) {
    return this->loadRockset(split());
  }

  // can not hanlde other loader type yet
  return 0;
}

bool IngestSpec::load(BlockList& blocks) noexcept {
  // if domain is present - assume it's S3 file
  auto fs = nebula::storage::makeFS(dsu::getProtocol(table_->source), domain_, table_->settings);
  auto localFs = nebula::storage::makeFS("local");

  // attach a local file to each split
  for (auto& split : splits_) {
    // attach a local file for this split
    split->local = localFs->temp();

    // there might be S3 error which returns tmpFile as empty
    if (!fs->copy(split->path, split->local)) {
      LOG(WARNING) << "Failed to copy file to local: " << split->path;
      continue;
    }
  }

  // check if data blocks with the same ingest ID exists
  // since it is a swap loader, we will remove those blocks
  bool result = this->ingest(blocks);

  // NOTE: assuming tmp file is created by mkstemp API
  // we unlink it for os to recycle it (linux), ignoring the result
  // https://stackoverflow.com/questions/32445579/when-a-file-created-with-mkstemp-is-deleted
  for (auto& split : splits_) {
    unlink(split->local.c_str());
    split->local.resize(0);
  }

  // swap each of the blocks into block manager
  // as long as they share the same table / spec
  return result;
}

size_t IngestSpec::loadSwap() noexcept {
  // LOCAL data source only supported in swap for local test scenarios on single node
  // As it doesn't make sense for distributed system to share a local data source
  if (dsu::isFileSystem(table_->source)) {
    // TODO(cao) - make a better size estimation to understand total blocks to have
    BlockList blocks;

    // load current
    size_t numBlocks = 0;
    auto result = this->load(blocks);
    if (result) {
      // unique specs by table and spec
      // to avoid calling removeBySpec repeatedly on the same value
      std::unordered_multimap<std::string, std::string> uniqueSpecs;
      for (const auto& b : blocks) {
        uniqueSpecs.emplace(b->table(), b->spec());
      }

      // remove blocks that shares the same spec / table
      auto bm = BlockManager::init();
      for (auto& ts : uniqueSpecs) {
        bm->removeBySpec(ts.first, ts.second);
      }

      // move all new blocks in
      numBlocks = bm->add(blocks);
    }

    return numBlocks;
  }

  return 0;
}

size_t IngestSpec::loadRoll() noexcept {
  if (dsu::isFileSystem(table_->source)) {
    BlockList blocks;

    // load current
    size_t numBlocks = 0;
    auto result = this->load(blocks);
    if (result) {
      auto bm = BlockManager::init();
      // move all new blocks in
      numBlocks = bm->add(blocks);
    }

    return numBlocks;
  }

  return 0;
}

size_t IngestSpec::loadApi() noexcept {
  BlockList blocks;
  auto result = false;
  size_t numBlocks = 0;

  // normal file system access: S3, GS, Local
  if (dsu::isFileSystem(table_->source)) {
    // load is a wrapper of ingest which download files to local
    result = this->load(blocks);
  }

  if (table_->source == DataSource::GSHEET) {
    result = this->ingest(blocks);
  }

  if (table_->source == DataSource::HTTP) {
    result = this->loadHttp(blocks, split());
  }

  if (result) {
    auto bm = BlockManager::init();
    // move all new blocks in
    numBlocks = bm->add(blocks);
  }

  return numBlocks;
}

// TODO(cao): refactor it to reuse similar flow as `load`
// the only difference is one is downlaod data from HTTP
// while the other is from cloud storage.
bool IngestSpec::loadHttp(BlockList& blocks,
                          SpecSplitPtr split,
                          std::vector<std::string> headers,
                          std::string_view data) noexcept {
  // id is the file path, copy it from s3 to a local folder
  auto local = nebula::storage::makeFS("local");
  split->local = local->temp();

  // download the HTTP file to local as temp file
  HttpService http;
  const auto& url = split->path;

  // set access token if present
  auto& token = this->table_->settings["token"];
  if (!token.empty()) {
    headers.push_back(fmt::format("Authorization: Bearer {0}", token));
  }

  // if there are some preset headers, pass them into the same object
  for (auto& header : this->table_->headers) {
    headers.push_back(header);
  }

  // the sheet content in this json objects
  if (!http.download(url, headers, data, split->local)) {
    LOG(WARNING) << "Failed to download to local: " << url;
    return false;
  }

  // check if data blocks with the same ingest ID exists
  // since it is a swap loader, we will remove those blocks
  bool result = this->ingest(blocks);

  // NOTE: assuming tmp file is created by mkstemp API
  // we unlink it for os to recycle it (linux), ignoring the result
  // https://stackoverflow.com/questions/32445579/when-a-file-created-with-mkstemp-is-deleted
  {
    unlink(split->local.c_str());
    split->local.resize(0);
  }

  // swap each of the blocks into block manager
  // as long as they share the same table / spec
  return result;
}

std::unique_ptr<RowCursor> IngestSpec::readGSheet() noexcept {
  HttpService http;
  // read access token from table settings
  std::vector<std::string> headers{
    "Accept: application/json"
  };

  // TODO(cao): do we allow empty token - if it's public?
  auto& token = this->table_->settings["token"];
  if (!token.empty()) {
    headers.push_back(fmt::format("Authorization: Bearer {0}", token));
  }

  // the sheet content in this json objects
  auto split = this->split();
  auto json = http.readJson(split->path, headers);

  auto doc = std::make_unique<rapidjson::Document>();
  if (doc->Parse(json.c_str()).HasParseError()) {
    LOG(WARNING) << "Error parsing google sheet reply: " << json;
    return nullptr;
  }

  if (!doc->IsObject()) {
    LOG(WARNING) << "Expect an object for google sheet reply.";
    return nullptr;
  }

  auto root = doc->GetObject();

  // ensure this is a columns data
  auto md = root.FindMember("majorDimension");
  if (md == root.MemberEnd()) {
    LOG(WARNING) << "majorDimension not found in google sheet reply.";
    return nullptr;
  }

  // read column vectors from its values property, it's type of [[]...]
  auto columns = root.FindMember("values");
  if (columns == root.MemberEnd()) {
    LOG(WARNING) << "values not found in google sheet reply.";
    return nullptr;
  }

  // wrap this into a column vector readers
  auto values = columns->value.GetArray();
  const auto schema = TypeSerializer::from(table_->schema);

  // size() will have total number of rows for current spec
  return std::make_unique<JsonVectorReader>(schema, std::move(doc), values, this->size());
}

size_t IngestSpec::loadRockset(SpecSplitPtr split) noexcept {
  // get a table definition
  auto table = table_->to();

  // load all blocks
  BlockList blocks;
  const auto& serde = table_->rocksetSerde;
  std::vector<std::string> headers{
    "Content-Type: application/json",
    fmt::format("Authorization: ApiKey {0}", serde.apiKey)
  };
  auto dataFormat =
    "{{\"parameters\": ["
    "{{\"type\": \"string\", \"value\": \"{0}\", \"name\": \"start_time\"}},"
    "{{\"type\": \"string\", \"value\": \"{1}\", \"name\": \"end_time\"}}"
    "]}}";
  auto data = fmt::format(dataFormat,
                          Evidence::fmt_iso8601(split->watermark),
                          Evidence::fmt_iso8601(split->watermark + serde.interval));

  size_t numBlocks = 0;
  if (this->loadHttp(blocks, split, std::move(headers), data)) {
    auto bm = BlockManager::init();
    // move all new blocks in
    numBlocks = bm->add(blocks);
  }

  // rockset use HTTP rest api to load data but, we need to send
  return numBlocks;
}

// current is a kafka spec
size_t IngestSpec::loadKafka(SpecSplitPtr split) noexcept {
#ifdef PPROF
  HeapProfilerStart("/tmp/heap_ingest_kafka.out");
#endif
  // build up the segment to consume
  // note that: Kafka path is composed by this pattern: "{partition}_{offset}_{size}"
  auto segment = KafkaSegment::from(split->path);
  KafkaReader reader(table_, std::move(segment));

  // time function
  MacroRow macroRow(table_->timeSpec, split->watermark, split->macros);

  // get a table definition
  auto table = table_->to();

  // build a batch
  auto batch = std::make_shared<Batch>(*table, reader.size());

  auto lowTime = std::numeric_limits<int64_t>::max();
  auto highTime = std::numeric_limits<int64_t>::min();

  while (reader.hasNext()) {
    auto& r = reader.next();
    const auto& row = macroRow.set(&r);

    // TODO(cao) - Kafka may produce NULL row due to corruption or exception
    // ideally we can handle nulls in our system, however, let's skip null row for now.
    int64_t time = row.readLong(Table::TIME_COLUMN);
    if (time == 0) {
      continue;
    }

    // update time range before adding the row to the batch
    if (time < lowTime) {
      lowTime = time;
    }

    if (time > highTime) {
      highTime = time;
    }

    // add a new entry
    batch->add(row);
  }

  // build a block and add it to block manager
  batch->seal();
  BlockManager::init()->add(
    BlockLoader::from(
      BlockSignature{ table->name(), "v1", 0, lowTime, highTime, id_ }, batch));

#ifdef PPROF
  HeapProfilerStop();
#endif

  // iterate this read until it reaches end of the segment - blocking queue?
  return 1;
}

bool IngestSpec::ingest(BlockList& blocks) noexcept {
  auto table = table_->to();
  auto version = version_;

  // load the data into batch based on block.id * 50000 as offset so that we can keep every 50K rows per block
  const auto& specId = id();
  LOG(INFO) << "Ingesting current spec: " << specId;

  // get table schema and create a table
  const auto schema = TypeSerializer::from(table_->schema);

  // list all columns that we could read from given data file
  std::vector<std::string> columns;
  columns.reserve(schema->size());
  for (size_t i = 0, size = schema->size(); i < size; ++i) {
    auto type = schema->childType(i);
    auto name = type->name();
    auto macro = table->fromMacro(name);
    if (macro.size() == 0) {
      columns.push_back(name);
    }
  }

  // limit at 1b on single host but respect table settings
  size_t bRows = FLAGS_NBLOCK_MAX_ROWS;
  auto itr = table_->settings.find(BATCH_SIZE);
  if (itr != table_->settings.end()) {
    bRows = folly::to<size_t>(itr->second);
  }

  // a lambda to build batch block
  std::pair<int64_t, int64_t> range{ std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::min() };
  auto makeBlock = [&table, &version, &range, &specId](size_t bid, std::shared_ptr<Batch> b) {
    // seal the block
    b->seal();
    LOG(INFO) << "Push a block: " << b->state();

    return BlockLoader::from(
      // build up a block signature with table name, sequence and spec
      BlockSignature{
        table->name(),
        version,
        bid,
        range.first,
        range.second,
        specId },
      b);
  };

  // This may result in many blocks since it's partitioned in each ingestion spec.
  std::unordered_map<size_t, std::shared_ptr<Batch>> batches;
  auto pod = table->pod();
  size_t blockId = 0;

  // TODO: introduce a flag to fail whole spec when bad file hit
  // ISSUE: https://github.com/varchar-io/nebula/issues/175
  for (const auto& split : splits_) {
    std::unique_ptr<RowCursor> source = nullptr;
    try {
      if (table_->source == DataSource::GSHEET) {
        source = this->readGSheet();
      } else if (table_->format == DataFormat::CSV) {
        source = std::make_unique<CsvReader>(split->local, table_->csv, columns);
      } else if (table_->format == DataFormat::JSON) {
        source = makeJsonReader(split->local, table_->json, schema, columns);
      } else if (table_->format == DataFormat::PARQUET) {
        // schema is modified with time column, we need original schema here
        source = std::make_unique<ParquetReader>(split->local, schema, columns);
      } else {
        LOG(ERROR) << "Supported data formats: csv, json, parquet.";
        continue;
      }

      // no valid reader is created
      if (source == nullptr) {
        continue;
      }

      // build time row to handle time column and macro columns reading
      MacroRow macroRow(table_->timeSpec, split->watermark, split->macros);

      // ingest current reader into the blocks
      while (source->hasNext()) {
        auto& r = source->next();
        const auto& row = macroRow.set(&r);

        // for non-partitioned, all batch's pid will be 0
        size_t pid = 0;
        BessType bess = -1;
        if (pod) {
          pid = pod->pod(row, bess);
        }

        // get the batch
        auto batch = batches[pid];
        if (batch == nullptr) {
          batch = std::make_shared<Batch>(*table, bRows, pid);
          batches[pid] = batch;
        }

        // if this is already full
        if (batch->getRows() >= bRows) {
          // move it to the manager and erase it from the map
          blocks.push_front(makeBlock(blockId++, batch));

          // make a new batch
          batch = std::make_shared<Batch>(*table, bRows, pid);
          batches[pid] = batch;
          range = { std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::min() };
        }

        // update time range before adding the row to the batch
        // get time column value
        int64_t time = row.readLong(Table::TIME_COLUMN);
        if (time < range.first) {
          range.first = time;
        }

        if (time > range.second) {
          range.second = time;
        }

        // add a new entry
        batch->add(row, bess);
      }
    } catch (const std::exception& exp) {
      LOG(ERROR) << "Exception in creating reader for table " << table_->toString() << ", file: " << split->path << ", exception: " << exp.what();
      continue;
    }
  }

  // build all data blocks
  // move all blocks in map into block manager
  for (auto& itr : batches) {
    // TODO(cao) - the block maybe too small
    // to waste lots of memory especially in case of sparse storage
    // we need to try to compress them if useful to save memory
    blocks.push_front(makeBlock(blockId++, itr.second));
  }

  // return all blocks built up so far
  LOG(INFO) << "Memory Pool Report: " << nebula::common::Pool::getDefault().report();
  return true;
}

} // namespace ingest
} // namespace nebula
