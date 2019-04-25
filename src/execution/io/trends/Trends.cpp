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
  TRANSFER(std::string, readString)
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

void TrendsTable::loadTrends(size_t max) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // PURE TEST CODE: load data from a file path
  auto file = "/tmp/pin.trends.csv";

  // load the data into batch based on block.id * 50000 as offset so that we can keep every 50K rows per block
  CsvReader reader(file);
  // every 100K rows, we split it into a block
  const auto bRows = 100000;
  std::unique_ptr<Batch> block = nullptr;
  size_t cursor = 0;
  size_t blockId = 0;
  TrendsRawRow trendsRow;
  while (reader.hasNext()) {
    // create a new block
    if (cursor % bRows == 0) {
      // add current block if it's the first time init
      if (cursor > 0) {
        N_ENSURE(block != nullptr, "block should be valid");
        NBlock b(*this, blockId);
        LOG(INFO) << " adding one block: " << (block != nullptr) << ", rows=" << cursor;
        bm->add(b, std::move(block));
      }

      // create new block data
      blockId += 1;
      block = std::make_unique<Batch>(this->getSchema());

      // do not load more than max blocks max==0: load all
      if (max > 0 && blockId > max) {
        break;
      }
    }

    cursor++;
    auto& row = reader.next();
    // add all entries belonging to test_data_limit_100000 into the block
    trendsRow.set(&row);
    block->add(trendsRow);
  }

  // add last block if it has rows
  if (block->getRows() > 0) {
    NBlock b(*this, blockId);
    LOG(INFO) << " adding last block: " << (block != nullptr) << ", rows=" << cursor;
    bm->add(b, std::move(block));
  }
}

} // namespace trends
} // namespace io
} // namespace execution
} // namespace nebula