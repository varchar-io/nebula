/*
 * Copyright 2017-present varchar.io
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

#include "common/Folly.h"
#include "type/Tree.h"

/**
 * Nebula execution in block managment.
 */
namespace nebula {
namespace execution {

using nebula::execution::io::BatchBlock;
using nebula::execution::io::BlockList;
using nebula::memory::BatchPtr;
using nebula::meta::NBlock;
using nebula::meta::NNode;
using nebula::meta::Table;
using nebula::surface::eval::BlockEval;
using nebula::surface::eval::Histogram;
using nebula::surface::eval::ValueEval;
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

// query all nodes that hold data for given table
const std::vector<NNode> BlockManager::query(const std::string& table) {
  std::lock_guard<std::mutex> lock(dmux_);
  std::vector<NNode> nodes;

  // go through all nodes's block set
  for (auto n = data_.begin(); n != data_.end(); ++n) {
    const auto& states = n->second;
    if (states.find(table) != states.end()) {
      nodes.push_back(n->first);
    }
  }

  return nodes;
}

static constexpr auto BATCH_SIZE = 100;
folly::Future<FilteredBlocks> batch(folly::ThreadPoolExecutor& pool,
                                    const ValueEval& filter,
                                    std::array<BatchPtr, BATCH_SIZE> input,
                                    size_t size) {
  auto p = std::make_shared<folly::Promise<FilteredBlocks>>();
  pool.addWithPriority(
    [&filter, input, size, p]() {
      FilteredBlocks blocks;
      blocks.reserve(BATCH_SIZE);
      for (size_t i = 0; i < size; ++i) {
        auto ptr = input[i];

        auto eval = filter.eval(*ptr);
        if (eval != BlockEval::NONE) {
          blocks.emplace_back(ptr, eval);
        }
      }

      // compute phase on block and return the result
      p->setValue(blocks);
    },
    folly::Executor::HI_PRI);
  return p->getFuture();
}

const FilteredBlocks BlockManager::query(const Table& table, const PlanPtr plan, folly::ThreadPoolExecutor& pool) {
  // 1. a table and a predicate should determined by meta service how many blocks we should query
  // 2. determine how many blocks are not in memory yet, if they are not, load them in
  // 3. fan out the query plan to execute on each block in parallel (not this function but the caller)
  auto total = 0;
  const auto& window = plan->getWindow();
  const auto& version = plan->tableVersion();

  // check if there are some predicates we can evaluate here
  const auto& filter = plan->fetch<PhaseType::COMPUTE>().filter();

  std::array<BatchPtr, BATCH_SIZE> list;
  std::vector<folly::Future<FilteredBlocks>> futures;
  futures.reserve(1024);

  const auto& self = local();
  auto ts = self.find(table.name());
  if (ts == self.end()) {
    return {};
  }

  auto index = 0;
  for (auto& b : ts->second->query(window, version)) {
    ++total;
    list[index++] = b;

    if (index == BATCH_SIZE) {
      futures.push_back(batch(pool, filter, list, index));
      index = 0;
    }
  }

  if (index > 0) {
    futures.push_back(batch(pool, filter, list, index));
  }

  // collect futures
  FilteredBlocks tableBlocks;
  tableBlocks.reserve(total);
  auto x = folly::collectAll(futures).get();
  for (auto it = x.begin(); it < x.end(); ++it) {
    // if the result is empty
    if (!it->hasValue()) {
      continue;
    }

    for (auto& item : it->value()) {
      tableBlocks.push_back(item);
    }
  }

  LOG(INFO) << fmt::format("Fetch blcoks {0} / {1} for table {2} in window [{3}, {4}]. ",
                           tableBlocks.size(), total, table.name(), window.first, window.second);
  return tableBlocks;
}

// add block into the target table states
// look for the correct table state object and add this block into it
bool BlockManager::addBlock(TableStates& target, std::shared_ptr<io::BatchBlock> block) {
  const auto& table = block->table();
  auto ts = target.find(table);
  std::shared_ptr<TableState> ptr = nullptr;
  if (ts == target.end()) {
    ptr = std::make_shared<TableState>(table);
    target.emplace(table, ptr);
  } else {
    ptr = ts->second;
  }

  // add this block into the table state object
  return ptr->add(block);
}

bool BlockManager::add(std::shared_ptr<io::BatchBlock> block) {
  std::lock_guard<std::mutex> lock(dmux_);
  const auto& node = block->residence();

  // remote blocks will not have data pointer
  if (!node.isInProc()) {
    N_ENSURE(block->data() == nullptr, "remote block won't have data pointer.");
  }

  // counter - note that this may be used for appromimate only, not for accurate internal state
  ++blocks_;

  auto itr = data_.find(node);
  if (itr != data_.end()) {
    return addBlock(itr->second, block);
  }

  data_.emplace(node, TableStates{});
  return addBlock(data_.at(node), block);
}

size_t BlockManager::add(BlockList& range) {
  size_t numAdded = 0;
  for (auto itr = range.begin(); itr != range.end(); ++itr) {
    auto& b = *itr;
    if (!add(b)) {
      LOG(WARNING) << "Failed to add a data block of spec: " << b->spec();
    } else {
      ++numAdded;
    }
  }

  return numAdded;
}

// remove all blocks that share the given spec
// NOTE: thread-unsafe~!
size_t BlockManager::removeBySpec(const std::string& table, const std::string& spec) {
  size_t count = 0;
  auto& self = local();
  auto state = self.find(table);
  if (state != self.end()) {
    count += state->second->remove(spec);
  }

  // decrement blocks counter
  blocks_ -= count;

  return count;
}

std::shared_ptr<Histogram> BlockManager::hist(const std::string& table, size_t col) const {
  std::lock_guard<std::mutex> lock(dmux_);

  // go through all nodes's block set
  // TODO: wrong - first block histogram will be polllueted
  std::shared_ptr<Histogram> hist = nullptr;
  for (auto n = data_.begin(); n != data_.end(); ++n) {
    const auto& states = n->second;
    auto state = states.find(table);
    if (state != states.end()) {
      auto inst = state->second->hists().at(col);
      if (hist == nullptr) {
        // make a copy
        hist = nebula::surface::eval::from(inst->toString());
      } else {
        hist->merge(*inst);
      }
    }
  }

  return hist;
}

} // namespace execution
} // namespace nebula