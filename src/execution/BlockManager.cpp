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
#include <folly/String.h>

/**
 * Nebula execution in block managment.
 */
namespace nebula {
namespace execution {

using nebula::memory::Batch;
using nebula::meta::NBlock;
using nebula::meta::Table;

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

const std::vector<Batch*> BlockManager::query(const Table& table) {
  // 1. a table and a predicate should determined by meta service how many blocks we should query
  // 2. determine how many blocks are not in memory yet, if they are not, load them in
  // 3. fan out the query plan to execute on each block in parallel (not this function but the caller)
  LOG(INFO) << "Fetch all blcoks for table: " << table.name();
  std::vector<Batch*> tableBlocks;
  for (auto& b : blocks_) {
    if (b.first.getTable() == table) {
      tableBlocks.push_back(b.second.get());
    }
  }

  return tableBlocks;
}

bool BlockManager::add(const NBlock& block) {
  // ensure the block is not in memory
  nebula::execution::io::BlockLoader loader;
  auto batch = loader.load(block);

  // batch is in memory now
  N_ENSURE_NOT_NULL(batch);

  // add it to the manage list
  blocks_[block] = std::move(batch);

  // TODO(cao) - current is simple solution
  // we need comprehensive fault torellance and hanlding
  return true;
}

bool BlockManager::remove(const nebula::meta::NBlock&) {
  throw NException("Not implemeneted yet");
}

} // namespace execution
} // namespace nebula