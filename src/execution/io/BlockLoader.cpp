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

#include "BlockLoader.h"
#include "storage/CsvReader.h"
#include "type/Serde.h"

/**
 * Exchange block units between memory and storage.s
 */
namespace nebula {
namespace execution {
namespace io {

using nebula::memory::Batch;
using nebula::meta::TestTable;
using nebula::storage::CsvReader;
using nebula::type::TypeSerializer;

std::unique_ptr<nebula::memory::Batch> BlockLoader::load(const nebula::meta::NBlock& block) {
  if (block.getTable().name() == TestTable::name()) {
    return loadTestBlock();
  }

  if (block.getTable().name() == "trends.draft") {
    return loadTrendsBlock(block);
  }

  throw NException("Not implemented yet");
}

std::unique_ptr<nebula::memory::Batch> BlockLoader::loadTestBlock() {
  auto schema = TypeSerializer::from(TestTable::schema());
  auto block = std::make_unique<Batch>(schema);

  // use 1024 rows for testing
  auto rows = 10000;

  // use the specified seed so taht the data can repeat
  auto seed = nebula::common::Evidence::unix_timestamp();

  nebula::surface::MockRowData row(seed);
  for (auto i = 0; i < rows; ++i) {
    block->add(row);
  }
  // print out the block state
  LOG(INFO) << "Loaded test block: " << block->state();

  return block;
}

std::unique_ptr<nebula::memory::Batch> BlockLoader::loadTrendsBlock(const nebula::meta::NBlock& nb) {
  auto schema = TypeSerializer::from(TestTable::trendsTableSchema());
  auto block = std::make_unique<Batch>(schema);

  // PURE TEST CODE: load data from a file path
  auto file = "/Users/shawncao/trends.draft.csv";

  // load the data into batch based on block.id * 50000 as offset so that we can keep every 50K rows per block
  CsvReader reader(file);
  const auto bRows = 50000;
  size_t i = 0;
  auto start = nb.getId() * bRows;
  while (reader.hasNext()) {
    auto& row = reader.next();
    // add all entries belonging to test_data_limit_100000 into the block
    if (row.readString("methodology") == "test_data_limit_100000") {
      if (i++ >= start) {
        block->add(row);
        if (block->getRows() >= bRows) {
          break;
        }
      }
    }
  }

  LOG(INFO) << "Loaded from " << file << ": " << block->state();

  return block;
}

} // namespace io
} // namespace execution
} // namespace nebula