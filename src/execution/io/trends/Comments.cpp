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

#include "Comments.h"
#include <iostream>
#include <rapidjson/document.h>
#include <string>
#include <unordered_map>
#include "common/Evidence.h"
#include "execution/BlockManager.h"
#include "memory/Batch.h"
#include "meta/NBlock.h"
#include "storage/CsvReader.h"
#include "storage/local/File.h"

DEFINE_uint64(COMMENTS_MAX, std::numeric_limits<uint64_t>::max(), "0 means no maximum");

/**
 * This module is to build special case for trends which has some hard coded data.
 * Will be deleted after it's done with pilot run.
 */
namespace nebula {
namespace execution {
namespace io {
namespace trends {

using nebula::common::Evidence;
using nebula::execution::BlockManager;
using nebula::execution::io::BatchBlock;
using nebula::memory::Batch;
using nebula::meta::Table;
using nebula::storage::CsvReader;
using nebula::storage::local::File;

// row wrapper to translate "date" string into reserved "_time_" column
class CommentsRawRow : public nebula::surface::RowData {
public:
  bool set(const RowData* row) {
    time_ = Evidence::time(row->readString("created_at"), "%Y-%m-%d %H:%M:%S");
    user_ = row->readLong("user_id");
    comments_ = row->readString("comments");
    sign_ = row->readString("pin_signature");
    return true;
  }

// raw date to _time_ columm in ingestion time
#define TRANSFER(TYPE, FUNC)                             \
  TYPE FUNC(const std::string& field) const override {   \
    throw NException(fmt::format("{0} not hit", field)); \
  }

  TRANSFER(bool, readBool)
  TRANSFER(int8_t, readByte)
  TRANSFER(int16_t, readShort)
  TRANSFER(int32_t, readInt)
  TRANSFER(float, readFloat)
  TRANSFER(double, readDouble)
  TRANSFER(std::unique_ptr<nebula::surface::ListData>, readList)
  TRANSFER(std::unique_ptr<nebula::surface::MapData>, readMap)

  bool isNull(const std::string&) const override {
    return false;
  }

  // _time_ is in long type and it's coming from date string
  int64_t readLong(const std::string& field) const override {
    if (field == Table::TIME_COLUMN) {
      // timestamp in string 2016-07-15 14:38:03
      return time_;
    }

    if (field == "user_id") {
      return user_;
    }

    if (field == "pin_id") {
      return pin_;
    }

    throw NException("not hit");
  }

  std::string_view readString(const std::string& field) const override {
    if (field == "comments") {
      return comments_;
    }

    if (field == "pin_signature") {
      return sign_;
    }

    throw NException("not hit");
  }

#undef TRANSFER

private:
  time_t time_;
  int64_t user_;
  int64_t pin_;
  std::string comments_;
  std::string sign_;
};

void CommentsTable::load(const std::string& file) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // PURE TEST CODE: load data from a file path
  const std::vector<std::string> columns = { "pin_signature", "user_id", "comments", "created_at" };

  std::unordered_map<size_t, std::shared_ptr<Batch>> blocksByDate;
  std::unordered_map<size_t, std::pair<size_t, size_t>> timeRangeByDate;
  size_t blockId = 0;

  // load the data into batch based on block.id * 50000 as offset so that we can keep every 50K rows per block
  LOG(INFO) << "Reading comments from " << file;
  CsvReader reader(file, '\t', columns);

  // limit at 1b on single host
  const size_t bRows = 100000;
  CommentsRawRow cRow;
  size_t count = 0;
  size_t errors = 0;
  while (reader.hasNext()) {
    if (count++ % 1000000 == 0) {
      LOG(INFO) << fmt::format("Loaded {0} comments.", count);
      if (count >= FLAGS_COMMENTS_MAX) {
        break;
      }
    }

    auto& row = reader.next();
    if (!cRow.set(&row)) {
      errors++;
      continue;
    }

    // get time column value
    size_t time = cRow.readLong(Table::TIME_COLUMN);
    size_t date = Evidence::date(time);
    const auto itr = blocksByDate.find(date);
    auto empty = true;
    if (itr != blocksByDate.end()) {
      empty = false;
      // if this is already full
      if (itr->second->getRows() >= bRows) {
        auto& range = timeRangeByDate.at(itr->first);
        // move it to the manager and erase it from the map
        bm->add(BlockLoader::from({ name_, blockId++, range.first, range.second }, itr->second));
        empty = true;
      }
    }

    // add a new entry
    if (empty) {
      // emplace basically means
      blocksByDate[date] = std::make_shared<Batch>(*this, bRows);
      timeRangeByDate[date] = { std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::min() };
    }

    // for sure, we have it now
    auto& block = blocksByDate.at(date);
    auto& range = timeRangeByDate.at(date);
    N_ENSURE(block != nullptr, "block has to be valid.");
    if (time < range.first) {
      range.first = time;
    }

    if (time > range.second) {
      range.second = time;
    }

    block->add(cRow);
  }

  // move all blocks in map into block manager
  for (auto itr = blocksByDate.begin(); itr != blocksByDate.end(); ++itr) {
    auto& range = timeRangeByDate.at(itr->first);
    bm->add(BlockLoader::from({ name_, blockId++, range.first, range.second }, itr->second));
  }

  auto metrics = bm->getTableMetrics(NAME);
  LOG(INFO) << fmt::format("blocks: {0}, skipped: {1}, total: {2}", std::get<0>(metrics), errors, count);
}

///////////////////////////////////////////////////////////////////////////////////////////////////
class SignautresRawRow : public nebula::surface::RowData {
public:
  bool set(const RowData* row) {
    time_ = Evidence::unix_timestamp();
    sign_ = row->readString("pin_signature");
    pin_ = row->readLong("pin_id");
    return true;
  }

// raw date to _time_ columm in ingestion time
#define TRANSFER(TYPE, FUNC)                             \
  TYPE FUNC(const std::string& field) const override {   \
    throw NException(fmt::format("{0} not hit", field)); \
  }

  TRANSFER(bool, readBool)
  TRANSFER(int8_t, readByte)
  TRANSFER(int16_t, readShort)
  TRANSFER(int32_t, readInt)
  TRANSFER(float, readFloat)
  TRANSFER(double, readDouble)
  TRANSFER(std::unique_ptr<nebula::surface::ListData>, readList)
  TRANSFER(std::unique_ptr<nebula::surface::MapData>, readMap)

  bool isNull(const std::string&) const override {
    return false;
  }

  // _time_ is in long type and it's coming from date string
  int64_t readLong(const std::string& field) const override {
    if (field == Table::TIME_COLUMN) {
      // timestamp in string 2016-07-15 14:38:03
      return time_;
    }

    if (field == "pin_id") {
      return pin_;
    }

    throw NException("not hit");
  }

  std::string_view readString(const std::string& field) const override {
    if (field == "pin_signature") {
      return sign_;
    }

    throw NException("not hit");
  }

#undef TRANSFER

private:
  time_t time_;
  int64_t pin_;
  std::string sign_;
};

void SignaturesTable::load(const std::string& file) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // PURE TEST CODE: load data from a file path
  const std::vector<std::string> columns = { "pin_signature", "pin_id" };

  size_t blockId = 0;

  // load the data into batch based on block.id * 50000 as offset so that we can keep every 50K rows per block
  LOG(INFO) << "Reading signatures from " << file;
  CsvReader reader(file, '\t', columns);

  // limit at 1b on single host
  const size_t bRows = 100000;
  auto batch = std::make_shared<Batch>(*this, bRows);
  SignautresRawRow sRow;
  size_t count = 0;
  while (reader.hasNext()) {
    if (count++ % 1000000 == 0) {
      LOG(INFO) << fmt::format("Loaded {0} signatures.", count);
      if (count >= FLAGS_COMMENTS_MAX) {
        break;
      }
    }

    auto& row = reader.next();
    sRow.set(&row);

    // get time column value
    size_t time = common::Evidence::unix_timestamp();
    // if this is already full
    if (batch->getRows() >= bRows) {
      // move it to the manager and erase it from the map
      bm->add(BlockLoader::from({ name_, blockId++, time, time }, batch));
      batch = std::make_shared<Batch>(*this, bRows);
    }

    // add a new entry
    batch->add(sRow);
  }

  // move all blocks in map into block manager
  if (batch->getRows() > 0) {
    size_t time = common::Evidence::unix_timestamp();
    bm->add(BlockLoader::from({ name_, blockId++, time, time }, batch));
  }

  auto metrics = bm->getTableMetrics(NAME);
  LOG(INFO) << fmt::format("blocks: {0}, total: {1}", std::get<0>(metrics), count);
}

} // namespace trends
} // namespace io
} // namespace execution
} // namespace nebula