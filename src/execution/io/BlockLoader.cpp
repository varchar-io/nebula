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
#include "surface/MockSurface.h"

/**
 * Exchange block units between memory and storage.s
 */
namespace nebula {
namespace execution {
namespace io {

using nebula::common::Evidence;
using nebula::execution::io::BatchBlock;
using nebula::memory::Batch;
using nebula::meta::BlockSignature;
using nebula::meta::BlockState;
using nebula::meta::NBlock;
using nebula::meta::TestTable;
using nebula::surface::MockRowData;

BatchBlock BlockLoader::from(const BlockSignature& sign, std::shared_ptr<nebula::memory::Batch> b) {
  N_ENSURE_NOT_NULL(b, "requires a solid batch");
  BlockState state{ b->getRows(), b->getRawSize() };
  return BatchBlock(sign, b, state);
}

BatchBlock BlockLoader::load(const BlockSignature& block) {
  if (block.table == test_.name()) {
    return loadTestBlock(block);
  }

  throw NException("Not implemented yet");
}

class TimeProvidedRow : public MockRowData {
  static int32_t id() {
    // TODO(cao) - just an interesting thing, why number 8888 is not out of this static function?
    static int32_t i = 0;
    return i++;
  }

public:
  TimeProvidedRow(size_t seed, size_t start, size_t end)
    : MockRowData(seed), rRand_{ Evidence::rand(start, end, seed) } {}

  virtual int64_t readLong(const std::string& field) const override {
    if (field == nebula::meta::Table::TIME_COLUMN) {
      return rRand_();
    }

    return MockRowData::readLong(field);
  }

  virtual int32_t readInt(const std::string& field) const override {
    // make all id are sequentially sorted
    if (field == "id") {
      return id();
    }

    return MockRowData::readInt(field);
  }

private:
  // range [start_, end_]
  std::function<int64_t()> rRand_;
};

BatchBlock BlockLoader::loadTestBlock(const BlockSignature& b) {
  // use 1024 rows for testing
  auto rows = 10000;
  auto block = std::make_shared<Batch>(test_, rows);

  // use the specified seed so taht the data can repeat
  auto seed = Evidence::unix_timestamp();

  // a seed that triggers a bug
  seed = 1583309624;

  TimeProvidedRow row(seed, b.start, b.end);
  for (auto i = 0; i < rows; ++i) {
    block->add(row);
  }

  // print out the block state
  LOG(INFO) << "Loaded test block: seed=" << seed << ", state=" << block->state();

  return BatchBlock(b, block, { block->getRows(), block->getRawSize() });
}

} // namespace io
} // namespace execution
} // namespace nebula