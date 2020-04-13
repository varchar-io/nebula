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

#include <fmt/format.h>
#include <list>

#include "common/Task.h"
#include "execution/BlockManager.h"

/**
 * Define node server that does the work as nebula server asks.
 */
namespace nebula {
namespace ingest {

// a task to expire list of blocks
class BlockExpire : public nebula::common::Signable {
public:
  BlockExpire(std::list<std::string> blocks) : blocks_{ std::move(blocks) } {
    sign_ = fmt::format("{0}", blocks_.size());
  }
  virtual ~BlockExpire() = default;

  virtual const std::string& signature() const override {
    return sign_;
  }

  inline const std::list<std::string>& blocks() const {
    return blocks_;
  }

  bool work() const {
    auto bm = nebula::execution::BlockManager::init();

    // process the block expire list
    auto removed = 0;
    for (auto& bid : blocks_) {
      // if system has this block, remove it, otherwise skip it
      removed += bm->removeById(bid);
    }

    // TODO(cao) - we need to update task states so data can be ingested again.
    LOG(INFO) << "Removed expired blocks: " << removed;
    return true;
  }

private:
  std::list<std::string> blocks_;
  std::string sign_;
};

} // namespace ingest
} // namespace nebula