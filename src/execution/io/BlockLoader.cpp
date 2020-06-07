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
using nebula::execution::io::BlockList;
using nebula::memory::Batch;
using nebula::meta::BessType;
using nebula::meta::BlockSignature;
using nebula::meta::BlockState;
using nebula::meta::NBlock;
using nebula::meta::TestTable;
using nebula::surface::MockRowData;

std::shared_ptr<BatchBlock> BlockLoader::from(const BlockSignature& sign, std::shared_ptr<nebula::memory::Batch> b) {
  N_ENSURE_NOT_NULL(b, "requires a solid batch");
  return std::make_shared<BatchBlock>(sign, b, BlockState{ b->getRows(), b->getMemory() });
}

BlockList BlockLoader::load(const BlockSignature& block) {
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

  virtual std::string_view readString(const std::string& field) const override {
    static std::array<std::string, 4> VALUES{ "a", "b", "c", "d" };
    static std::array<std::string, 10> STACKS{
      "a\nb\nc",
      "x\ny\nz",
      "a\nb\nd",
      "a\nx",
      "a\nx\ny",
      "x\nk\nz",
      "x\ny\nm",
      "a\nb\nc\nd",
      "a\nb\nc\nd\ne",
      "a\nb\nx"
    };

    // make all id are sequentially sorted
    if (field == "tag") {
      auto index = std::abs(rRand_()) % 4;
      return VALUES[index];
    }

    if (field == "stack") {
      return STACKS[std::abs(rRand_()) % STACKS.size()];
    }

    return MockRowData::readString(field);
  }

private:
  // range [start_, end_]
  std::function<int64_t()> rRand_;
};

BlockList BlockLoader::loadTestBlock(const BlockSignature& b) {
  // use 1024 rows for testing
  auto rows = 10000;
  // auto block = std::make_shared<Batch>(test_, rows);

  // use the specified seed so taht the data can repeat
  auto seed = Evidence::unix_timestamp();

  std::unordered_map<size_t, std::shared_ptr<Batch>> batches;
  // auto batch = std::make_shared<Batch>(*table, bRows);
  auto pod = test_.pod();

  // a seed that triggers a bug
  // seed = 1583309624;

  TimeProvidedRow row(seed, b.start, b.end);
  for (auto i = 0; i < rows; ++i) {
    // for non-partitioned, all batch's pid will be 0
    size_t pid = 0;
    BessType bess = -1;
    if (pod) {
      pid = pod->pod(row, bess);
    }

    // get the batch
    auto batch = batches[pid];
    if (batch == nullptr) {
      batch = std::make_shared<Batch>(test_, rows, pid);
      batches[pid] = batch;
    }

    batch->add(row, bess);
  }

  // print out the block state
  LOG(INFO) << "Loaded test blocks: " << batches.size() << " using seed=" << seed;
  BlockList blocks;
  for (auto& itr : batches) {
    auto block = itr.second;
    blocks.push_front(std::make_shared<BatchBlock>(
      BlockSignature{
        b.table,
        b.id * 10 + itr.first,
        b.start,
        b.end,
        b.spec },
      block,
      BlockState{ block->getRows(), block->getRawSize() }));
  }

  return blocks;
}

} // namespace io
} // namespace execution
} // namespace nebula