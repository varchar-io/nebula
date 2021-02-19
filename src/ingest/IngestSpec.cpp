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

#include "TimeRow.h"
#include "common/Evidence.h"
#include "execution/BlockManager.h"
#include "execution/meta/TableService.h"
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
using nebula::common::unordered_map;
using nebula::execution::BlockManager;
using nebula::execution::io::BatchBlock;
using nebula::execution::io::BlockList;
using nebula::execution::io::BlockLoader;
using nebula::execution::meta::TableService;
using nebula::memory::Batch;
using nebula::meta::BessType;
using nebula::meta::BlockSignature;
using nebula::meta::DataSource;
using nebula::meta::Table;
using nebula::meta::TablePtr;
using nebula::meta::TableSpecPtr;
using nebula::meta::TestTable;
using nebula::meta::TimeSpec;
using nebula::meta::TimeType;
using nebula::storage::CsvReader;
using nebula::storage::JsonReader;
using nebula::storage::JsonVectorReader;
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

// reading settings if it has special delimeter, by default tab "\t"
static constexpr auto CSV_DELIMITER_KEY = "csv.delimiter";
// a setting indicates if the csv file has header, by default true
static constexpr auto CSV_HEADER_KEY = "csv.header";
// a settings to overwrite batch size of a table
static constexpr auto BATCH_SIZE = "batch";

// build blocks from a row cursor source
bool build(TablePtr, RowCursor&, BlockList&,
           size_t, const std::string&, TimeRow&) noexcept;

// load some nebula test data into current process
void loadNebulaTestData(const TableSpecPtr& table, const std::string& spec) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  // (NOTE) table->max_seconds is not serialized, it will be 0
  auto start = table->timeSpec.unixTimeValue;
  auto end = start + table->max_seconds;

  // let's plan these many data std::thread::hardware_concurrency()
  TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add(nebula::meta::BlockSignature{
      table->name, i++, begin, begin + window, spec });
  }
}

bool IngestSpec::work() noexcept {
  // TODO(cao) - refator this to have better hirachy for different ingest types.
  const auto& loader = table_->loader;
  if (loader == FLAGS_NTEST_LOADER) {
    loadNebulaTestData(table_, id_);
    return true;
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
    return this->loadKafka();
  }

  // can not hanlde other loader type yet
  return false;
}

bool IngestSpec::load(BlockList& blocks) noexcept {
  // if domain is present - assume it's S3 file
  auto fs = nebula::storage::makeFS(dsu::getProtocol(table_->source), domain_);

  // id is the file path, copy it from s3 to a local folder
  auto local = nebula::storage::makeFS("local");
  auto tmpFile = local->temp();

  // there might be S3 error which returns tmpFile as empty
  if (!fs->copy(path_, tmpFile)) {
    LOG(WARNING) << "Failed to copy file to local: " << path_;
    return false;
  }

  // check if data blocks with the same ingest ID exists
  // since it is a swap loader, we will remove those blocks
  bool result = this->ingest(tmpFile, blocks);

  // NOTE: assuming tmp file is created by mkstemp API
  // we unlink it for os to recycle it (linux), ignoring the result
  // https://stackoverflow.com/questions/32445579/when-a-file-created-with-mkstemp-is-deleted
  unlink(tmpFile.c_str());

  // swap each of the blocks into block manager
  // as long as they share the same table / spec
  return result;
}

bool IngestSpec::loadSwap() noexcept {
  // LOCAL data source only supported in swap for local test scenarios on single node
  // As it doesn't make sense for distributed system to share a local data source
  if (dsu::isFileSystem(table_->source)) {
    // TODO(cao) - make a better size estimation to understand total blocks to have
    BlockList blocks;

    // load current
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
      bm->add(blocks);
    }

    return result;
  }

  return false;
}

bool IngestSpec::loadRoll() noexcept {
  if (dsu::isFileSystem(table_->source)) {
    BlockList blocks;

    // load current
    auto result = this->load(blocks);
    if (result) {
      auto bm = BlockManager::init();
      // move all new blocks in
      bm->add(blocks);
    }

    return result;
  }

  return false;
}

bool IngestSpec::loadApi() noexcept {
  BlockList blocks;
  auto result = false;
  if (dsu::isFileSystem(table_->source)) {
    // load current
    result = this->load(blocks);
  }

  if (table_->source == DataSource::GSHEET) {
    result = this->loadGSheet(blocks);
  }

  if (result) {
    auto bm = BlockManager::init();
    // move all new blocks in
    bm->add(blocks);
  }

  return result;
}

bool IngestSpec::loadGSheet(BlockList& blocks) noexcept {
  HttpService http;
  const auto& url = this->path();

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
  auto json = http.readJson(url, headers);
  rapidjson::Document doc;
  if (doc.Parse(json.c_str()).HasParseError()) {
    LOG(WARNING) << "Error parsing google sheet reply: " << json;
    return false;
  }

  if (!doc.IsObject()) {
    LOG(WARNING) << "Expect an object for google sheet reply.";
    return false;
  }

  auto root = doc.GetObject();

  // ensure this is a columns data
  auto md = root.FindMember("majorDimension");
  if (md == root.MemberEnd()) {
    LOG(WARNING) << "majorDimension not found in google sheet reply.";
    return false;
  }

  // read column vectors from its values property, it's type of [[]...]
  auto columns = root.FindMember("values");
  if (columns == root.MemberEnd()) {
    LOG(WARNING) << "values not found in google sheet reply.";
    return false;
  }

  // wrap this into a column vector readers
  auto values = columns->value.GetArray();
  const auto schema = TypeSerializer::from(table_->schema);

  // size() will have total number of rows for current spec
  JsonVectorReader reader(schema, values, this->size());
  auto tb = table_->to();
  TableService::singleton()->enroll(tb);

  TimeRow timeRow(table_->timeSpec, watermark_);
  return build(tb, reader, blocks, FLAGS_NBLOCK_MAX_ROWS, id_, timeRow);
}

// current is a kafka spec
bool IngestSpec::loadKafka() noexcept {
#ifdef PPROF
  HeapProfilerStart("/tmp/heap_ingest_kafka.out");
#endif
  // build up the segment to consume
  // note that: Kafka path is composed by this pattern: "{partition}_{offset}_{size}"
  auto segment = KafkaSegment::from(path_);
  KafkaReader reader(table_, std::move(segment));

  // time function
  TimeRow timeRow(table_->timeSpec, watermark_);

  // get a table definition
  auto table = table_->to();

  // enroll the table in case it is the first time
  TableService::singleton()->enroll(table);

  // build a batch
  auto batch = std::make_shared<Batch>(*table, reader.size());

  auto lowTime = std::numeric_limits<size_t>::max();
  auto highTime = std::numeric_limits<size_t>::min();

  while (reader.hasNext()) {
    auto& r = reader.next();
    const auto& row = timeRow.set(&r);

    // TODO(cao) - Kafka may produce NULL row due to corruption or exception
    // ideally we can handle nulls in our system, however, let's skip null row for now.
    size_t time = row.readLong(Table::TIME_COLUMN);
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
      BlockSignature{ table->name(), 0, lowTime, highTime, id_ }, batch));

#ifdef PPROF
  HeapProfilerStop();
#endif

  // iterate this read until it reaches end of the segment - blocking queue?
  return true;
}

#define OVERWRITE_IF_EXISTS(VAR, KEY, FUNC) \
  {                                         \
    auto itr = table_->settings.find(KEY);  \
    if (itr != table_->settings.end()) {    \
      VAR = FUNC(itr->second);              \
    }                                       \
  }

bool IngestSpec::ingest(const std::string& file, BlockList& blocks) noexcept {
  // TODO(cao) - support column selection in ingestion and expand time column
  // to other columns for simple transformation
  // but right now, we're expecting the same schema of data
  // based on time spec, we need to replace or append time column
  TimeRow timeRow(table_->timeSpec, watermark_);
  auto table = table_->to();

  // enroll the table in case it is the first time
  TableService::singleton()->enroll(table);

  // load the data into batch based on block.id * 50000 as offset so that we can keep every 50K rows per block
  LOG(INFO) << "Ingesting from " << file;

  // get table schema and create a table
  const auto schema = TypeSerializer::from(table_->schema);

  // list all columns describing the current file
  std::vector<std::string> columns;
  columns.reserve(schema->size());
  for (size_t i = 0, size = schema->size(); i < size; ++i) {
    auto type = schema->childType(i);
    columns.push_back(type->name());
  }

  // depends on the type
  std::unique_ptr<RowCursor> source = nullptr;
  if (table_->format == "csv") {
    auto delimiter = '\t';
    auto withHeader = true;
    OVERWRITE_IF_EXISTS(delimiter, CSV_DELIMITER_KEY, [](auto& s) { return s.at(0); })
    OVERWRITE_IF_EXISTS(withHeader, CSV_HEADER_KEY, [](auto& s) { return folly::to<bool>(s); })

    source = std::make_unique<CsvReader>(file, delimiter, withHeader, columns);
  } else if (table_->format == "json") {
    source = std::make_unique<JsonReader>(file, schema);
  } else if (table_->format == "parquet") {
    // schema is modified with time column, we need original schema here
    source = std::make_unique<ParquetReader>(file, schema);
  } else {
    LOG(ERROR) << "Unsupported file format: " << table_->format;
    return false;
  }

  // limit at 1b on single host
  size_t bRows = FLAGS_NBLOCK_MAX_ROWS;
  OVERWRITE_IF_EXISTS(bRows, BATCH_SIZE, [](auto& s) { return folly::to<size_t>(s); })

  return build(table, *source, blocks, bRows, id_, timeRow);
}

#undef OVERWRITE_IF_EXISTS

// ingest a row cursor into block list
bool build(
  TablePtr table,
  RowCursor& cursor,
  BlockList& blocks,
  size_t bRows,
  const std::string& spec,
  TimeRow& timeRow) noexcept {
#ifdef PPROF
  HeapProfilerStart("/tmp/heap_ingest.out");
#endif

  std::pair<size_t, size_t> range{ std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::min() };

  // a lambda to build batch block
  auto makeBlock = [&table, &range, &spec](size_t bid, std::shared_ptr<Batch> b) {
    // seal the block
    b->seal();
    LOG(INFO) << "Push a block: " << b->state();

    return BlockLoader::from(
      // build up a block signature with table name, sequence and spec
      BlockSignature{
        table->name(),
        bid,
        range.first,
        range.second,
        spec },
      b);
  };

  // TODO(cao): we haven't done global lookup for the same partition
  // This may result in many blocks since it's partitioned in each ingestion spec.
  unordered_map<size_t, std::shared_ptr<Batch>> batches;
  // auto batch = std::make_shared<Batch>(*table, bRows);
  auto pod = table->pod();

  size_t blockId = 0;
  while (cursor.hasNext()) {
    auto& r = cursor.next();
    const auto& row = timeRow.set(&r);

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
      range = { std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::min() };
    }

    // update time range before adding the row to the batch
    // get time column value
    size_t time = row.readLong(Table::TIME_COLUMN);
    if (time < range.first) {
      range.first = time;
    }

    if (time > range.second) {
      range.second = time;
    }

    // add a new entry
    batch->add(row, bess);
  }

  // move all blocks in map into block manager
  for (auto& itr : batches) {
    // TODO(cao) - the block maybe too small
    // to waste lots of memory especially in case of sparse storage
    // we need to try to compress them if useful to save memory
    blocks.push_front(makeBlock(blockId++, itr.second));
  }

#ifdef PPROF
  HeapProfilerStop();
#endif
  // return all blocks built up so far
  LOG(INFO) << "Memory Pool Report: " << nebula::common::Pool::getDefault().report();
  return true;
}

} // namespace ingest
} // namespace nebula