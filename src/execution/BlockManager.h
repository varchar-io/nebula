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

#pragma once

#include <forward_list>
#include <mutex>

#include "ExecutionPlan.h"
#include "TableState.h"
#include "common/Folly.h"
#include "common/Hash.h"

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

// table name to table state object mapping
using TableStates = nebula::common::unordered_map<std::string, std::shared_ptr<TableState>>;
using FilteredBlocks = std::vector<nebula::memory::EvaledBlock>;

class BlockManager {
public:
  BlockManager(BlockManager&) = delete;
  BlockManager(BlockManager&&) = delete;
  virtual ~BlockManager() = default;

  static std::shared_ptr<BlockManager> init();

public:
  const FilteredBlocks query(const nebula::meta::Table&, const PlanPtr, folly::ThreadPoolExecutor&);

  // query all nodes that hold data for given table
  const std::vector<nebula::meta::NNode> query(const std::string&);

  // add given block into the target table states repo
  static bool addBlock(TableStates&, std::shared_ptr<io::BatchBlock>);

  // add a block list, will change the list
  bool add(io::BlockList&);

  // add a block already loaded
  bool add(std::shared_ptr<io::BatchBlock>);

  // add a block into the system - the data may be loaded internal
  inline bool add(const nebula::meta::BlockSignature& sign) {
    // block loader
    nebula::execution::io::BlockLoader loader;
    auto list = loader.load(sign);
    return add(list);
  }

  // remove blocks by table name and spec signature
  // return number of blocks removed
  size_t removeBySpec(const std::string&, const std::string&);

  // get table state for given table name
  const TableStateBase& state(const std::string& table) const {
    const auto& self = local();
    if (self.find(table) == self.end()) {
      return TableStateBase::empty();
    }

    return *self.at(table);
  }

  // get all table states for given node
  inline const TableStates& states(const nebula::meta::NNode& node = nebula::meta::NNode::inproc()) {
    // it may reutrn empty result if the node is not in
    return data_[node];
  }

  // swap table states for given node
  inline void swap(const nebula::meta::NNode& node, TableStates states) {
    std::lock_guard<std::mutex> lock(dmux_);
    data_[node] = states;
  }

  inline size_t numBlocks() const {
    return blocks_;
  }

  // get table list of current node
  nebula::common::unordered_set<std::string> tables(const size_t limit) const noexcept {
    std::lock_guard<std::mutex> lock(dmux_);
    nebula::common::unordered_set<std::string> tables;
    for (const auto& node : data_) {
      for (const auto& ts : node.second) {
        tables.emplace(ts.first);
      }

      // need not more than limit
      if (tables.size() >= limit) {
        break;
      }
    }

    return tables;
  }

  // has spec in node
  bool hasSpec(const nebula::meta::NNode& node, const std::string& table, const std::string& spec) {
    std::lock_guard<std::mutex> lock(dmux_);
    auto entry = data_.find(node);
    if (entry != data_.end()) {
      const auto& states = entry->second;
      auto ts = states.find(table);
      if (ts != states.end()) {
        return ts->second->hasSpec(spec);
      }
    }

    return false;
  }

  TableStateBase metrics(const std::string& table) const {
    std::lock_guard<std::mutex> lock(dmux_);
    TableStateBase metricsOnly{ table };
    // aggregate all nodes for given table
    for (auto& ts : data_) {
      const auto& states = ts.second;
      auto found = states.find(table);
      if (found != states.end()) {
        metricsOnly.merge(*found->second);
      }
    }

    return metricsOnly;
  }

  // get historgram of given table/column
  std::shared_ptr<nebula::surface::eval::Histogram> hist(const std::string&, size_t) const;

private:
  BlockManager() : blocks_{ 0 } {
    data_.emplace(nebula::meta::NNode::inproc(), TableStates{});
  }

  inline TableStates& local() {
    return data_.at(nebula::meta::NNode::inproc());
  }

  inline const TableStates& local() const {
    return data_.at(nebula::meta::NNode::inproc());
  }

private:
  // counter for in/out of blocks
  size_t blocks_;

  // meta data for remote blocks
  nebula::common::unordered_map<
    nebula::meta::NNode,
    TableStates,
    nebula::meta::NodeHash,
    nebula::meta::NodeEqual>
    data_;
  mutable std::mutex dmux_;

private:
  static std::mutex smux;
  static std::shared_ptr<BlockManager> inst;
};

} // namespace execution
} // namespace nebula
