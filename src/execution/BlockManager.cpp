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

#include "BlockManager.h"
#include <folly/Conv.h>
#include <regex>
#include "type/Tree.h"

/**
 * Nebula execution in block managment.
 */
namespace nebula {
namespace execution {

using nebula::memory::Batch;
using nebula::meta::NBlock;
using nebula::meta::Table;
using nebula::type::Kind;
using nebula::type::Schema;
using nebula::type::TypeNode;

// static members definition
std::mutex BlockManager::smux;
std::shared_ptr<BlockManager> BlockManager::inst = nullptr;

std::shared_ptr<BlockManager> BlockManager::init() {
  std::lock_guard<std::mutex> lock(BlockManager::smux);
  if (inst == nullptr) {
    inst = std::shared_ptr<BlockManager>(new BlockManager());
  }

  return inst;
}

/// HACK! - Replace it!
std::pair<std::string, std::string> hackColEqValue(std::string_view input) {
  std::regex col_regex("&&\\(F:(\\w+)==C:(\\w+)\\)\\)");
  std::smatch matches;
  std::string str(input.data(), input.size());
  if (std::regex_search(str, matches, col_regex) && matches.size() == 3) {
    return { matches[1].str(),
             matches[2].str() };
  }

  return { "", "" };
}

const std::vector<Batch*> BlockManager::query(const Table& table, const ExecutionPlan& plan) {
  // 1. a table and a predicate should determined by meta service how many blocks we should query
  // 2. determine how many blocks are not in memory yet, if they are not, load them in
  // 3. fan out the query plan to execute on each block in parallel (not this function but the caller)
  std::vector<Batch*> tableBlocks;
  auto total = 0;
  const auto& window = plan.getWindow();

  // check if there are some predicates we can evaluate here
  const auto& compute = plan.fetch<PhaseType::COMPUTE>();

  // TODO(cao) - This is a big hack which is only working for one case
  // We should have tree walking on the evaluation tree to decide if we can do so
  // 1. a column in or equal predication
  // 2. this predication can impact the whole tree
  // LOG(INFO) << "filter signature: " << filter.signature();
  auto pair = hackColEqValue(compute.filter().signature());
  std::function<bool(Batch*)> pass = [](Batch*) { return true; };

  if (pair.first.size() > 0) {
    const auto& colName = pair.first;
    Kind kind = Kind::INVALID;

    // TODO(cao) - we should use table.schema() but here we use compute input schema instead
    // before we have official meta service, all table has its own definition, so this table may be generic
    compute.inputSchema()->onChild(colName, [&kind](const TypeNode& found) {
      kind = found->k();
    });

    N_ENSURE(kind != Kind::INVALID, "column not found");
#define KIND_CHECK(K, T)                                                                                            \
  case Kind::K: {                                                                                                   \
    pass = [&colName, &pair](Batch* batch) -> bool { return batch->probably(colName, folly::to<T>(pair.second)); }; \
    break;                                                                                                          \
  }

    switch (kind) {
      KIND_CHECK(BOOLEAN, bool)
      KIND_CHECK(TINYINT, int8_t)
      KIND_CHECK(SMALLINT, int16_t)
      KIND_CHECK(INTEGER, int32_t)
      KIND_CHECK(BIGINT, int64_t)
      KIND_CHECK(REAL, float)
      KIND_CHECK(DOUBLE, double)
    default:
      break;
    }

#undef KIND_CHECK
  }

  for (auto& b : blocks_) {
    const auto& block = b.first;
    if (block.getTable() == table.name()) {
      ++total;

      Batch* ptr = b.second.get();
      if (block.overlap(window) && pass(ptr)) {
        tableBlocks.push_back(ptr);
      }
    }
  }

  LOG(INFO) << fmt::format("Fetch blcoks {0} / {1} for table {2} in window [{3}, {4}]. ",
                           tableBlocks.size(), total, table.name(), window.first, window.second);
  return tableBlocks;
}

bool BlockManager::add(const NBlock& block) {
  // ensure the block is not in memory
  nebula::execution::io::BlockLoader loader;
  auto batch = loader.load(block);

  return this->add(block, std::move(batch));
}

bool BlockManager::add(const nebula::meta::NBlock& block, std::unique_ptr<nebula::memory::Batch> data) {
  // batch is in memory now
  N_ENSURE_NOT_NULL(data, "block data can't be null");

  // collect this block metrics
  collectBlockMetrics(block, *data);

  // add it to the manage list
  blocks_[block] = std::move(data);

  // TODO(cao) - current is simple solution
  // we need comprehensive fault torellance and hanlding
  return true;
}

bool BlockManager::remove(const nebula::meta::NBlock&) {
  throw NException("Not implemeneted yet");
}

} // namespace execution
} // namespace nebula