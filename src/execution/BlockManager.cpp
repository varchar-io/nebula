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

#include "common/Folly.h"
#include "type/Tree.h"

/**
 * Nebula execution in block managment.
 */
namespace nebula {
namespace execution {

using nebula::execution::io::BatchBlock;
using nebula::execution::io::BlockList;
using nebula::memory::Batch;
using nebula::meta::BlockSignature;
using nebula::meta::BlockState;
using nebula::meta::NBlock;
using nebula::meta::NNode;
using nebula::meta::Table;
using nebula::surface::eval::BlockEval;
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

void BlockManager::collectBlockMetrics(const io::BatchBlock& meta, TableStates& states) {
  const auto& table = meta.getTable();
  if (states.find(table) == states.end()) {
    states[table] = { 0, 0, 0, std::numeric_limits<size_t>::max(), 0 };
  }

  auto& tuple = states.at(table);
  const auto& state = meta.state();
  std::get<0>(tuple) += 1;
  std::get<1>(tuple) += state.numRows;
  std::get<2>(tuple) += state.rawSize;
  std::get<3>(tuple) = std::min(std::get<3>(tuple), meta.start());
  std::get<4>(tuple) = std::max(std::get<4>(tuple), meta.end());
}

bool BlockManager::tableInBlockSet(const std::string& table, const BlockSet& bs) {
  for (auto& b : bs) {
    if (table == b.getTable()) {
      return true;
    }
  }

  return false;
}

// query all nodes that hold data for given table
const std::vector<NNode> BlockManager::query(const std::string& table) {
  std::vector<NNode> nodes;

  // all blocks in proc
  if (tableInBlockSet(table, blocks_)) {
    nodes.push_back(NNode::inproc());
  }

  // go through all nodes's block set
  for (auto n = remotes_.begin(); n != remotes_.end(); ++n) {
    if (tableInBlockSet(table, n->second)) {
      nodes.push_back(n->first);
    }
  }

  return nodes;
}

static constexpr auto BATCH_SIZE = 100;
folly::Future<FilteredBlocks> batch(folly::ThreadPoolExecutor& pool,
                                    const nebula::surface::eval::ValueEval& filter,
                                    std::array<Batch*, BATCH_SIZE> input,
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

const FilteredBlocks BlockManager::query(const Table& table, const ExecutionPlan& plan, folly::ThreadPoolExecutor& pool) {
  // 1. a table and a predicate should determined by meta service how many blocks we should query
  // 2. determine how many blocks are not in memory yet, if they are not, load them in
  // 3. fan out the query plan to execute on each block in parallel (not this function but the caller)
  auto total = 0;
  const auto& window = plan.getWindow();

  // check if there are some predicates we can evaluate here
  const auto& filter = plan.fetch<PhaseType::COMPUTE>().filter();

  std::array<Batch*, BATCH_SIZE> list;
  std::vector<folly::Future<FilteredBlocks>> futures;
  futures.reserve(1024);

  auto index = 0;
  for (auto& b : blocks_) {
    if (b.getTable() == table.name()) {
      ++total;
      if (b.overlap(window)) {
        list[index++] = b.data().get();

        if (index == BATCH_SIZE) {
          futures.push_back(batch(pool, filter, list, index));
          index = 0;
        }
      }
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

bool BlockManager::add(const BlockSignature& sign) {
  // load data should happening somehwere elese
  nebula::execution::io::BlockLoader loader;
  auto block = loader.load(sign);
  return this->add(block);
}

bool BlockManager::add(const BatchBlock& block) {
  // collect metrics anyways.
  collectBlockMetrics(block, tableStates_);

  const auto& node = block.residence();
  // ensure the block is not in memory
  if (node.isInProc()) {
    blocks_.insert(block);
  } else {

    // remote blocks
    N_ENSURE(block.data() == nullptr, "remote block won't have data pointer.");

    auto itr = remotes_.find(node);
    if (itr != remotes_.end()) {
      itr->second.insert(block);
    } else {
      remotes_[node] = {};
      remotes_.at(node).insert(block);
    }
  }

  return true;
}

bool BlockManager::add(BlockList range) {
  std::move(range.begin(), range.end(), std::inserter(blocks_, blocks_.begin()));
  return true;
}

bool BlockManager::remove(const BatchBlock&) {
  throw NException("Not implemeneted yet");
}

// remove block that share the given ID
// NOTE: thread-unsafe~!
size_t BlockManager::removeById(const std::string& id) {
  //TODO(cao) - perf issue: we should not iterate all
  // instead, leverage the hash set nature by converting id into a BlockSignature
  auto itr = blocks_.begin();
  while (itr != blocks_.end()) {
    if (itr->signature().toString() == id) {
      itr = blocks_.erase(itr);
      return 1;
    }

    ++itr;
  }

  return 0;
}

// swap a new block set for given node
void BlockManager::set(const NNode& node, BlockSet set) {
  // just overwrite the existing key
  remotes_[node] = std::move(set);
}

// remove all blocks that share the given spec
// NOTE: thread-unsafe~!
size_t BlockManager::removeSameSpec(const nebula::meta::BlockSignature& bs) {
  size_t count = 0;
  auto itr = blocks_.begin();
  while (itr != blocks_.end()) {
    if (bs.sameSpec(itr->signature())) {
      itr = blocks_.erase(itr);
      count++;
      continue;
    }

    ++itr;
  }

  return count;
}

void BlockManager::updateTableMetrics() {
  // remove existing states
  TableStates states;
  NodeSpecs specs;

  // go through all blocks and do the aggregation again
  for (auto i = blocks_.begin(); i != blocks_.end(); ++i) {
    collectBlockMetrics(*i, states);
  }

  // go through all nodes's block set
  for (auto n = remotes_.begin(); n != remotes_.end(); ++n) {
    std::unordered_set<std::string> specSet;
    const auto& bs = n->second;
    for (auto i = bs.begin(); i != bs.end(); ++i) {
      collectBlockMetrics(*i, states);
      specSet.emplace(i->spec());
    }

    // set the spec set to the current node
    specs.emplace(n->first, specSet);
  }

  // TODO(cao) - update table states_ for those entry changes, otherwise, keep it no change.
  // do atomic swap?
  std::swap(states, tableStates_);
  std::swap(specs, specs_);
}

} // namespace execution
} // namespace nebula