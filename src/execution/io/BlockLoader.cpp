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
#include "common/Evidence.h"
#include "meta/TestTable.h"
#include "surface/MockSurface.h"

/**
 * Exchange block units between memory and storage.s
 */
namespace nebula {
namespace execution {
namespace io {

using nebula::common::Evidence;
using nebula::memory::Batch;
using nebula::meta::NBlock;
using nebula::meta::TestTable;
using nebula::surface::MockRowData;

std::unique_ptr<Batch> BlockLoader::load(const NBlock& block) {
  TestTable test;
  if (block.getTable() == test.name()) {
    return loadTestBlock(block);
  }

  throw NException("Not implemented yet");
}

class TimeProvidedRow : public MockRowData {
public:
  TimeProvidedRow(size_t seed, size_t start, size_t end)
    : MockRowData(seed), rRand_{ Evidence::rand(start, end, seed) } {}

  virtual int64_t readLong(const std::string& field) const override {
    if (field == nebula::meta::Table::TIME_COLUMN) {
      return rRand_();
    }

    return MockRowData::readLong(field);
  }

private:
  // range [start_, end_]
  std::function<int64_t()> rRand_;
};

std::unique_ptr<Batch> BlockLoader::loadTestBlock(const NBlock& nb) {
  TestTable test;
  auto block = std::make_unique<Batch>(test.schema());

  // use 1024 rows for testing
  auto rows = 10000;

  // use the specified seed so taht the data can repeat
  auto seed = Evidence::unix_timestamp();

  // a seed that triggers a bug
  // seed = 1556824936;

  TimeProvidedRow row(seed, nb.start(), nb.end());
  for (auto i = 0; i < rows; ++i) {
    block->add(row);
  }

  // print out the block state
  LOG(INFO) << "Loaded test block: seed=" << seed << ", state=" << block->state();

  return block;
}

} // namespace io
} // namespace execution
} // namespace nebula