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

// static const std::function<size_t(const nebula::meta::NBlock&)> hash_nblock = [](const nebula::meta::NBlock& v) -> size_t {
//   return 0;
// };

struct Hash {
  inline size_t operator()(const nebula::meta::NBlock& b) const _NOEXCEPT {
    return b.hash();
  }
};

struct Equal {
  inline bool operator()(const nebula::meta::NBlock& b1, const nebula::meta::NBlock& b2) const _NOEXCEPT {
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
  const std::vector<nebula::memory::Batch*> query(const nebula::meta::Table&);
  bool add(const nebula::meta::NBlock&);
  bool remove(const nebula::meta::NBlock&);

private:
  std::unordered_map<nebula::meta::NBlock, std::unique_ptr<nebula::memory::Batch>, Hash, Equal> blocks_;

private:
  static std::mutex smux;
  static std::shared_ptr<BlockManager> inst;
  BlockManager() {}
};

} // namespace execution
} // namespace nebula
