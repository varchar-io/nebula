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
#include <unordered_set>

#include "common/Hash.h"
#include "common/Task.h"
#include "execution/BlockManager.h"

/**
 * Define node server that does the work as nebula server asks.
 */
namespace nebula {
namespace ingest {

// a task to expire list of specs
// why we expire spec instead of individual blocks?
// it may look we can do better granularity at block level,
// but it actually increases the management cost with little foreable benefits
class BlockExpire : public nebula::common::Identifiable {
public:
  BlockExpire(std::unordered_set<std::pair<std::string, std::string>> specs) : specs_{ std::move(specs) } {
    size_t mix = std::accumulate(specs_.begin(), specs_.end(), 0, [](size_t v, const auto& str) {
      return v ^ nebula::common::Hasher::hashString(str.second);
    });

    // TODO(cao) - signature equals mix hash and size - it is still not guranteed to be unique
    // which implies if collision happens, some expire task may not be executed.
    sign_ = fmt::format("{0}_{1}", mix, specs_.size());
  }
  virtual ~BlockExpire() = default;

  virtual const std::string& id() const override {
    return sign_;
  }

  inline const std::unordered_set<std::pair<std::string, std::string>>& specs() const {
    return specs_;
  }

  bool work() const {
    auto bm = nebula::execution::BlockManager::init();

    // process the block expire list
    auto removed = 0;
    for (auto& spec : specs_) {
      // if system has this block, remove it, otherwise skip it
      removed += bm->removeBySpec(spec.first, spec.second);
    }

    LOG(INFO) << "Removed expired specs: " << removed;
    return removed > 0;
  }

private:
  // table-spec pairs
  std::unordered_set<std::pair<std::string, std::string>> specs_;
  std::string sign_;
};

} // namespace ingest
} // namespace nebula