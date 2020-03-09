/*
 * Copyright 2017-present Shawn Cao
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

#include "common/Evidence.h"
#include "execution/BlockManager.h"
#include "execution/meta/TableService.h"
#include "meta/TestTable.h"
#include "storage/CsvReader.h"
#include "storage/NFS.h"
#include "storage/ParquetReader.h"
#include "storage/kafka/KafkaReader.h"
#include "type/Serde.h"

// TODO(cao) - system wide enviroment configs should be moved to cluster config to provide
// table-wise customization
DEFINE_string(NTEST_LOADER, "NebulaTest", "define the loader name for loading nebula test data");
DEFINE_uint64(NBLOCK_MAX_ROWS, 50000, "max rows per block");

/**
 * We will sync etcd configs for cluster info into this memory object
 * To understand cluster status - total nodes.
 */
namespace nebula {
namespace ingest {

using nebula::common::Evidence;
using nebula::execution::BlockManager;
using nebula::execution::io::BatchBlock;
using nebula::execution::io::BlockLoader;
using nebula::execution::meta::TableService;
using nebula::memory::Batch;
using nebula::meta::BessType;
using nebula::meta::BlockSignature;
using nebula::meta::DataSource;
using nebula::meta::Table;
using nebula::meta::TableSpecPtr;
using nebula::meta::TestTable;
using nebula::meta::TimeSpec;
using nebula::meta::TimeType;
using nebula::storage::CsvReader;
using nebula::storage::ParquetReader;
using nebula::storage::kafka::KafkaReader;
using nebula::storage::kafka::KafkaSegment;
using nebula::surface::RowCursor;
using nebula::surface::RowData;
using nebula::type::LongType;
using nebula::type::TypeSerializer;

static constexpr auto LOADER_SWAP = "Swap";
static constexpr auto LOADER_ROLL = "Roll";

// load some nebula test data into current process
void loadNebulaTestData(const TableSpecPtr& table, const std::string& spec) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  // (NOTE) table->max_hr is not serialized, it will be 0
  auto start = table->timeSpec.unixTimeValue;
  auto end = start + Evidence::HOUR_SECONDS * table->max_hr;

  // let's plan these many data std::thread::hardware_concurrency()
  TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add(nebula::meta::BlockSignature{
      testTable.name(), i++, begin, begin + window, spec });
  }
}

bool IngestSpec::work() noexcept {
  // TODO(cao) - refator this to have better hirachy for different ingest types.
  const auto& loader = table_->loader;
  if (loader == FLAGS_NTEST_LOADER) {
    loadNebulaTestData(table_, signature_);
    return true;
  }

  const auto data = table_->source;
  if (data == DataSource::S3) {
    // either swap, they are reading files
    if (loader == LOADER_SWAP) {
      return this->loadSwap();
    }

    // if roll, they are reading files
    if (loader == LOADER_ROLL) {
      return this->loadRoll();
    }
  }

  if (data == DataSource::KAFKA) {
    return this->loadKafka();
  }

  // can not hanlde other loader type yet
  return false;
}

bool IngestSpec::load(BlockList& blocks) noexcept {
  // TODO(cao) - columar format reader (parquet) should be able to
  // access cloud storage directly to save networkbandwidth, but right now
  // we only can download it to local temp file and read it
  auto fs = nebula::storage::makeFS("s3", domain_);

  // id is the file path, copy it from s3 to a local folder
  std::string tmpFile = fs->copy(id_);

  // there might be S3 error which returns tmpFile as empty
  if (tmpFile.empty()) {
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
  if (table_->source == DataSource::S3) {
    // TODO(cao) - make a better size estimation to understand total blocks to have
    std::vector<BatchBlock> blocks;
    blocks.reserve(32);

    // load current
    auto result = this->load(blocks);
    if (result) {
      auto bm = BlockManager::init();
      for (BatchBlock& b : blocks) {
        // remove blocks that shares the same spec / table
        bm->removeSameSpec(b.signature());
      }

      // move all new blocks in
      bm->add(std::move(blocks));
    }

    return result;
  }

  return false;
}

bool IngestSpec::loadRoll() noexcept {
  if (table_->source == DataSource::S3) {
    // TODO(cao) - make a better size estimation to understand total blocks to have
    std::vector<BatchBlock> blocks;
    blocks.reserve(32);

    // load current
    auto result = this->load(blocks);
    if (result) {
      auto bm = BlockManager::init();
      // move all new blocks in
      bm->add(std::move(blocks));
    }

    return result;
  }

  return false;
}

// current is a kafka spec
bool IngestSpec::loadKafka() noexcept {
  // build up the segment to consume
  auto segment = KafkaSegment::from(id_);
  KafkaReader reader(table_, std::move(segment));

  // get a table definition
  auto table = table_->to();

  // enroll the table in case it is the first time
  TableService::singleton()->enroll(table);

  // build a batch
  auto batch = std::make_shared<Batch>(*table, reader.size());

  auto lowTime = std::numeric_limits<size_t>::max();
  auto highTime = std::numeric_limits<size_t>::min();
  while (reader.hasNext()) {
    auto& row = reader.next();

    // update time range before adding the row to the batch
    // get time column value
    size_t time = row.readLong(Table::TIME_COLUMN);

    // TODO(cao) - Kafka may produce NULL row due to corruption or exception
    // ideally we can handle nulls in our system, however, let's skip null row for now.
    if (time == 0) {
      continue;
    }

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
  BlockManager::init()->add(
    BlockLoader::from(
      BlockSignature{ table->name(), 0, lowTime, highTime, signature_ }, batch));

  // iterate this read until it reaches end of the segment - blocking queue?
  return true;
}

// row wrapper to translate "date" string into reserved "_time_" column
class RowWrapperWithTime : public nebula::surface::RowData {
public:
  RowWrapperWithTime(std::function<int64_t(const RowData*)> timeFunc)
    : timeFunc_{ std::move(timeFunc) } {}
  ~RowWrapperWithTime() = default;
  bool set(const RowData* row) {
    row_ = row;
    return true;
  }

// raw date to _time_ columm in ingestion time
#define TRANSFER(TYPE, FUNC)                           \
  TYPE FUNC(const std::string& field) const override { \
    return row_->FUNC(field);                          \
  }

  TRANSFER(bool, readBool)
  TRANSFER(int8_t, readByte)
  TRANSFER(int16_t, readShort)
  TRANSFER(int32_t, readInt)
  TRANSFER(std::string_view, readString)
  TRANSFER(float, readFloat)
  TRANSFER(double, readDouble)
  TRANSFER(int128_t, readInt128)
  TRANSFER(std::unique_ptr<nebula::surface::ListData>, readList)
  TRANSFER(std::unique_ptr<nebula::surface::MapData>, readMap)

  bool isNull(const std::string& field) const override {
    if (UNLIKELY(field == Table::TIME_COLUMN)) {
      // timestamp in string 2016-07-15 14:38:03
      return false;
    }

    return row_->isNull(field);
  }

  // _time_ is in long type and it's coming from date string
  int64_t readLong(const std::string& field) const override {
    if (UNLIKELY(field == Table::TIME_COLUMN)) {
      // timestamp in string 2016-07-15 14:38:03
      return timeFunc_(row_);
    }

    return row_->readLong(field);
  }

private:
  std::function<int64_t(const RowData*)> timeFunc_;
  const RowData* row_;
};

bool IngestSpec::ingest(const std::string& file, BlockList& blocks) noexcept {
  // TODO(cao) - support column selection in ingestion and expand time column
  // to other columns for simple transformation
  // but right now, we're expecting the same schema of data

  // based on time spec, we need to replace or append time column
  auto timeType = table_->timeSpec.type;
  std::function<int64_t(const RowData*)> timeFunc;

  // static time spec
  switch (timeType) {
  case TimeType::STATIC: {
    timeFunc = [value = table_->timeSpec.unixTimeValue](const RowData*) { return value; };
    break;
  }
  case TimeType::CURRENT: {
    timeFunc = [](const RowData*) { return Evidence::unix_timestamp(); };
    break;
  }
  case TimeType::COLUMN: {
    const auto& ts = table_->timeSpec;
    // TODO(cao) - currently only support string column with time pattern
    // and unix time stamp value as bigint if pattern is absent.
    // ts.pattern is required: time string pattern or special value such as UNIXTIME

    // Note: time column can not be NULL
    // unfortunately if the data has it as null, we return 1 as indicator
    // we can not use 0, because Nebula doesn't allow query time range fall into 0 start/end.
    static constexpr size_t NULL_TIME = 1;
    constexpr auto UNIX_TS = "UNIXTIME";
    if (ts.pattern == UNIX_TS) {
      timeFunc = [&ts](const RowData* r) {
        if (UNLIKELY(r->isNull(ts.colName))) {
          return NULL_TIME;
        }

        return (size_t)r->readLong(ts.colName);
      };
    } else {
      timeFunc = [&ts](const RowData* r) {
        if (UNLIKELY(r->isNull(ts.colName))) {
          return NULL_TIME;
        }

        return Evidence::time(r->readString(ts.colName), ts.pattern);
      };
    }
    break;
  }
  case TimeType::MACRO: {
    const auto& ts = table_->timeSpec;
    // TODO(cao) - support only one macro for now, need to generalize it
    if (ts.pattern == "date") {
      timeFunc = [d = mdate_](const RowData*) { return d; };
    } else {
      timeFunc = [](const RowData*) { return 0; };
    }
    break;
  }
  default: {
    LOG(ERROR) << "Unsupported time type: " << (int)timeType;
    return {};
  }
  }

  auto table = table_->to();

  // enroll the table in case it is the first time
  TableService::singleton()->enroll(table);

  size_t blockId = 0;

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
    source = std::make_unique<CsvReader>(file, '\t', columns);
  } else if (table_->format == "parquet") {
    // schema is modified with time column, we need original schema here
    source = std::make_unique<ParquetReader>(file, schema);
  } else {
    LOG(ERROR) << "Unsupported file format: " << table_->format;
    return false;
  }

  // limit at 1b on single host
  const size_t bRows = FLAGS_NBLOCK_MAX_ROWS;
  RowWrapperWithTime rw{ std::move(timeFunc) };
  std::pair<size_t, size_t> range{ std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::min() };

  // a lambda to build batch block
  auto makeBlock = [&table, &range, spec = signature_](size_t bid, std::shared_ptr<Batch> b) {
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
  std::unordered_map<size_t, std::shared_ptr<Batch>> batches;
  // auto batch = std::make_shared<Batch>(*table, bRows);
  auto pod = table->pod();

  while (source->hasNext()) {
    auto& row = source->next();
    rw.set(&row);

    // for non-partitioned, all batch's pid will be 0
    size_t pid = 0;
    BessType bess = -1;
    if (pod) {
      pid = pod->pod(rw, bess);
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
      blocks.push_back(makeBlock(blockId++, batch));

      // make a new batch
      batch = std::make_shared<Batch>(*table, bRows, pid);
      batches[pid] = batch;
      range = { std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::min() };
    }

    // update time range before adding the row to the batch
    // get time column value
    size_t time = rw.readLong(Table::TIME_COLUMN);
    if (time < range.first) {
      range.first = time;
    }

    if (time > range.second) {
      range.second = time;
    }

    // add a new entry
    batch->add(rw, bess);
  }

  // move all blocks in map into block manager
  for (auto& itr : batches) {
    blocks.push_back(makeBlock(blockId++, itr.second));
  }

  // return all blocks built up so far
  return true;
}

} // namespace ingest
} // namespace nebula