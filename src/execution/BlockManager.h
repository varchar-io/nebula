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
  inline size_t operator()(const nebula::meta::NBlock& b) const noexcept {
    return b.hash();
  }
};

struct Equal {
  inline bool operator()(const nebula::meta::NBlock& b1, const nebula::meta::NBlock& b2) const noexcept {
    return b1 == b2;
  }
};

class BlockManager {
public:
  BlockManager(BlockManager&) = delete;
  BlockManager(BlockManager&&) = delete;
  virtual ~BlockManager() = default;

  static std::shared_ptr<BlockManager> init();

public:
  // TODO(cao) - this interface needs predicate push down to filter out blocks
  const std::vector<nebula::memory::Batch*> query(const nebula::meta::Table&, const std::pair<size_t, size_t>&);
  bool add(const nebula::meta::NBlock&);

  // TODO(cao) - for short-term hack, will be removed
  bool add(const nebula::meta::NBlock&, std::unique_ptr<nebula::memory::Batch>);
  bool remove(const nebula::meta::NBlock&);

  std::tuple<size_t, size_t, size_t, size_t, size_t> getTableMetrics(const std::string table) const {
    if (tableStates_.find(table) == tableStates_.end()) {
      return { 0, 0, 0, 0, 0 };
    }

    return tableStates_.at(table);
  }

private:
  void collectBlockMetrics(const nebula::meta::NBlock& meta, const nebula::memory::Batch& block) {
    const auto& table = meta.getTable();
    if (tableStates_.find(table) == tableStates_.end()) {
      tableStates_[table] = { 0, 0, 0, 0, 0 };
    }

    auto& tuple = tableStates_.at(table);
    std::get<0>(tuple) += 1;
    std::get<1>(tuple) += block.getRows();
    std::get<2>(tuple) += block.getRawSize();
    std::get<3>(tuple) = std::min(std::get<3>(tuple), meta.start());
    std::get<4>(tuple) = std::max(std::get<4>(tuple), meta.end());
  }

  // table: <block count, row count, raw size, min time, max time>
  std::unordered_map<std::string, std::tuple<size_t, size_t, size_t, size_t, size_t>> tableStates_;
  std::unordered_map<nebula::meta::NBlock, std::unique_ptr<nebula::memory::Batch>, Hash, Equal> blocks_;

private:
  static std::mutex smux;
  static std::shared_ptr<BlockManager> inst;
  BlockManager() {}
};

} // namespace execution
} // namespace nebula
