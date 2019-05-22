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

#include "Pins.h"
#include <iostream>
#include <string>
#include <unordered_map>
#include "common/Evidence.h"
#include "execution/BlockManager.h"
#include "memory/Batch.h"
#include "meta/NBlock.h"
#include "storage/CsvReader.h"
#include "storage/local/File.h"

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
using nebula::memory::Batch;
using nebula::meta::NBlock;
using nebula::meta::Table;
using nebula::storage::CsvReader;
using nebula::storage::local::File;

// row wrapper to translate "date" string into reserved "_time_" column
class PinsRawRow : public nebula::surface::RowData {
public:
  void set(const RowData* row) {
    row_ = row;
  }

// raw date to _time_ columm in ingestion time
#define TRANSFER(TYPE, FUNC)                           \
  TYPE FUNC(const std::string& field) const override { \
    return row_->FUNC(field);                          \
  }

  TRANSFER(bool, isNull)
  TRANSFER(bool, readBool)
  TRANSFER(int8_t, readByte)
  TRANSFER(int16_t, readShort)
  TRANSFER(int32_t, readInt)
  TRANSFER(float, readFloat)
  TRANSFER(double, readDouble)
  TRANSFER(std::string_view, readString)
  TRANSFER(std::unique_ptr<nebula::surface::ListData>, readList)
  TRANSFER(std::unique_ptr<nebula::surface::MapData>, readMap)

  // _time_ is in long type and it's coming from date string
  int64_t readLong(const std::string& field) const override {
    if (field == Table::TIME_COLUMN) {
      // timestamp in string 2016-07-15 14:38:03
      return Evidence::time(row_->readString("created_at"), "%Y-%m-%d %H:%M:%S");
    }

    // others just fall through
    return row_->readLong(field);
  }

#undef TRANSFER

private:
  const RowData* row_;
};

std::shared_ptr<nebula::meta::MetaService> PinsTable::getMs() const {
  return std::make_shared<PinsMetaService>();
}

void PinsTable::load(size_t max) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // PURE TEST CODE: load data from a file path
  auto dir = "/tmp/pinss";
  const std::vector<std::string> columns = { "id", "board_id", "user_id", "created_at", "title", "details", "signature", "repins" };

  std::unordered_map<size_t, std::unique_ptr<Batch>> blocksByDate;
  std::unordered_map<size_t, std::pair<size_t, size_t>> timeRangeByDate;
  size_t blockId = 0;
  auto files = File::list(dir);
  auto filesLimit = max == 0 ? files.size() : std::min(max, files.size());
  for (size_t i = 0; i < filesLimit; ++i) {
    // do not load more than max blocks max==0: load all
    auto& file = files.at(i);

    // load the data into batch based on block.id * 50000 as offset so that we can keep every 50K rows per block
    CsvReader reader(fmt::format("{0}/{1}", dir, file), '\t', columns);
    const size_t bRows = 50000;
    PinsRawRow pinsRow;
    while (reader.hasNext()) {
      auto& row = reader.next();
      pinsRow.set(&row);

      // get time column value
      size_t time = pinsRow.readLong(Table::TIME_COLUMN);
      size_t date = Evidence::date(time);
      const auto itr = blocksByDate.find(date);
      // LOG(INFO) << "time: " << time << ", date:" << date;
      auto empty = true;
      if (itr != blocksByDate.end()) {
        empty = false;
        // if this is already full
        if (itr->second->getRows() >= bRows) {
          auto& range = timeRangeByDate.at(itr->first);
          // move it to the manager and erase it from the map
          bm->add(NBlock(name_, blockId++, range.first, range.second), std::move(itr->second));
          empty = true;
        }
      }

      // add a new entry
      if (empty) {
        // emplace basically means
        blocksByDate[date] = std::make_unique<Batch>(this->getSchema());
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

      block->add(pinsRow);
    }
  }

  // move all blocks in map into block manager
  for (auto itr = blocksByDate.begin(); itr != blocksByDate.end(); ++itr) {
    auto& range = timeRangeByDate.at(itr->first);
    bm->add(NBlock(name_, blockId++, range.first, range.second), std::move(itr->second));
  }

  auto metrics = bm->getTableMetrics(NAME);
  LOG(INFO) << "blocks: " << std::get<0>(metrics);
}

} // namespace trends
} // namespace io
} // namespace execution
} // namespace nebula