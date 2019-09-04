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

#include "common/Folly.h"
#include "ingest/SpecRepo.h"

/**
 * TODO(cao) - major node states will be sync through cluster management system 
 * such as etcd, shard manager, kubenetes or zookeeper
 * 
 * At this momment, we're sync through rpc, and it's possible we'll continue maintain this.
 */
namespace nebula {
namespace service {
namespace server {

class NodeSync {
public:
  static std::shared_ptr<folly::FunctionScheduler> async(
    folly::ThreadPoolExecutor&, const nebula::ingest::SpecRepo&, size_t) noexcept;
};

} // namespace server
} // namespace service
} // namespace nebula