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

#include "Trends.h"
#include <unordered_map>
#include "common/Evidence.h"
#include "execution/BlockManager.h"
#include "memory/Batch.h"
#include "meta/NBlock.h"
#include "storage/CsvReader.h"

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
using nebula::storage::CsvReader;

// row wrapper to translate "date" string into reserved "_time_" column
class TrendsRawRow : public nebula::surface::RowData {
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
    if (field == nebula::meta::Table::TIME_COLUMN) {
      return Evidence::time(row_->readString("date"), "%Y-%m-%d");
    }

    // others just fall through
    return row_->readLong(field);
  }

#undef TRANSFER

private:
  const RowData* row_;
};

std::shared_ptr<nebula::meta::MetaService> TrendsTable::getMs() const {
  return std::shared_ptr<nebula::meta::MetaService>(new TrendsMetaService());
}

void TrendsTable::load(size_t max) {
  const auto shouldStop = [max](size_t blockId) -> bool {
    return max > 0 && blockId >= max;
  };

  // load test data to run this query
  auto bm = BlockManager::init();

  // PURE TEST CODE: load data from a file path
  auto file = "/tmp/pin.trends.csv";

  // load the data into batch based on block.id * 50000 as offset so that we can keep every 50K rows per block
  CsvReader reader(file);
  // every X rows, we split it into a block
  const auto bRows = 50000;
  std::unordered_map<size_t, std::unique_ptr<Batch>> blocksByTime;
  size_t blockId = 0;
  TrendsRawRow trendsRow;
  while (reader.hasNext()) {
    auto& row = reader.next();
    trendsRow.set(&row);

    // get time column value
    auto time = trendsRow.readLong(Table::TIME_COLUMN);
    const auto itr = blocksByTime.find(time);
    auto empty = true;
    if (itr != blocksByTime.end()) {
      empty = false;
      // if this is already full
      if (itr->second->getRows() >= bRows) {
        // move it to the manager and erase it from the map
        bm->add(NBlock(name_, blockId++, time, time), std::move(itr->second));
        empty = true;
      }
    }

    // add a new entry
    if (empty) {
      // emplace basically means
      blocksByTime[time] = std::make_unique<Batch>(*this, bRows);
    }

    // for sure, we have it now
    auto& block = blocksByTime.at(time);
    N_ENSURE(block != nullptr, "block has to be valid.");
    block->add(trendsRow);

    // do not load more than max blocks max==0: load all
    if (shouldStop(blockId)) {
      return;
    }
  }

  // move all blocks in map into block manager
  for (auto itr = blocksByTime.begin(); itr != blocksByTime.end() && !shouldStop(blockId); ++itr) {
    bm->add(NBlock(name_, blockId++, itr->first, itr->first), std::move(itr->second));
  }
}

} // namespace trends
} // namespace io
} // namespace execution
} // namespace nebula