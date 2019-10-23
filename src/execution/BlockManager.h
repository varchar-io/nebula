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

#pragma once

#include <mutex>
#include <unordered_map>
#include "ExecutionPlan.h"
#include "io/BlockLoader.h"
#include "meta/NBlock.h"

/**
 * Define nebula execution runtime.
 * This object should be singleton per node. It manages all data segments that loaded in memory.
 * And their attributes, such as their time range, partition keys, and of course table name.
 * 
 */
namespace nebula {
namespace execution {

struct Hash {
  inline size_t operator()(const io::BatchBlock& b) const noexcept {
    return b.hash();
  }
};

struct Equal {
  inline bool operator()(const io::BatchBlock& b1, const io::BatchBlock& b2) const noexcept {
    return b1 == b2;
  }
};

using BlockSet = std::unordered_set<io::BatchBlock, Hash, Equal>;

class BlockManager {
  using TableStates = std::unordered_map<std::string, std::tuple<size_t, size_t, size_t, size_t, size_t>>;
  using NodeSpecs = std::unordered_map<nebula::meta::NNode, std::unordered_set<std::string>, nebula::meta::NodeHash, nebula::meta::NodeEqual>;

public:
  BlockManager(BlockManager&) = delete;
  BlockManager(BlockManager&&) = delete;
  virtual ~BlockManager() = default;

  static std::shared_ptr<BlockManager> init();

public:
  // TODO(cao) - this interface needs predicate push down to filter out blocks
  const std::vector<nebula::memory::Batch*> query(const nebula::meta::Table&, const ExecutionPlan&);

  // query all nodes that hold data for given table
  const std::vector<nebula::meta::NNode> query(const std::string&);

  // add a block into the system - the data may be loaded internal
  bool add(const nebula::meta::BlockSignature&);

  bool add(std::vector<io::BatchBlock>);

  // add a block already loaded
  bool add(const io::BatchBlock&);

  // remove a block from managmenet pool
  bool remove(const io::BatchBlock&);

  // remove a block from management pool by given ID
  size_t removeById(const std::string&);

  // swap an external block set for given node
  void set(const nebula::meta::NNode&, BlockSet);

  std::tuple<size_t, size_t, size_t, size_t, size_t> getTableMetrics(const std::string& table) const {
    if (tableStates_.find(table) == tableStates_.end()) {
      return { 0, 0, 0, 0, 0 };
    }

    return tableStates_.at(table);
  }

  const BlockSet& all(const nebula::meta::NNode& node = nebula::meta::NNode::inproc()) {
    if (node.isInProc()) {
      return blocks_;
    }

    // it may reutrn empty result if the node is not in
    return remotes_[node];
  }

  std::vector<std::string> getTables(const size_t limit) const noexcept {
    std::vector<std::string> tables;
    tables.reserve(limit);
    for (auto& item : tableStates_) {
      tables.push_back(item.first);
      if (tables.size() >= limit) {
        break;
      }
    }

    return tables;
  }

  // update table metrics
  void updateTableMetrics();

  // remove blocks that share the spec of given block signature
  size_t removeSameSpec(const nebula::meta::BlockSignature&);

  // has spec in node
  bool hasSpec(const nebula::meta::NNode& node, const std::string& spec) {
    auto entry = specs_.find(node);
    if (entry != specs_.end()) {
      auto& set = entry->second;
      auto item = set.find(spec);
      return item != set.end();
    }

    return false;
  }

private:
  // table: <block count, row count, raw size, min time, max time>
  TableStates tableStates_;

  // in-proc blocks
  BlockSet blocks_;

  // meta data for remote blocks
  std::unordered_map<nebula::meta::NNode, BlockSet, nebula::meta::NodeHash, nebula::meta::NodeEqual> remotes_;

  // node to spec set (by spec signature) mapping, updated by udpate table metrics
  NodeSpecs specs_;

private:
  static std::mutex smux;
  static std::shared_ptr<BlockManager> inst;
  BlockManager() {}

  static void collectBlockMetrics(const io::BatchBlock&, TableStates&);
  static bool tableInBlockSet(const std::string&, const BlockSet&);
};

} // namespace execution
} // namespace nebula
