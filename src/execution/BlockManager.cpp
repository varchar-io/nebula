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
#include <regex>
#include "common/Folly.h"
#include "type/Tree.h"

/**
 * Nebula execution in block managment.
 */
namespace nebula {
namespace execution {

using nebula::execution::io::BatchBlock;
using nebula::memory::Batch;
using nebula::meta::BlockSignature;
using nebula::meta::BlockState;
using nebula::meta::NBlock;
using nebula::meta::NNode;
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
    if (b.getTable() == table.name()) {
      ++total;

      Batch* ptr = b.data().get();
      if (b.overlap(window) && pass(ptr)) {
        tableBlocks.push_back(ptr);
      }
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

bool BlockManager::add(std::vector<io::BatchBlock> range) {
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