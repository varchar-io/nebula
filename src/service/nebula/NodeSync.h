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

#include <folly/experimental/FunctionScheduler.h>
#include <glog/logging.h>
#include "execution/BlockManager.h"
#include "meta/NNode.h"
#include "service/node/NodeClient.h"

/**
 * TODO(cao) - major node states will be sync through cluster management system 
 * such as etcd, shard manager, kubenetes or zookeeper
 * 
 * At this momment, we're sync through rpc, and it's possible we'll continue maintain this.
 */
namespace nebula {
namespace service {
class NodeSync {
public:
  static std::shared_ptr<folly::FunctionScheduler> async(folly::ThreadPoolExecutor& pool) noexcept {
    static const auto MS = std::chrono::milliseconds(1000);

    // schedule the function every 5 seconds
    auto fs = std::make_shared<folly::FunctionScheduler>();
    fs->addFunction(
      [&pool]() {
        LOG(INFO) << "Node Sync scheduled.";
        // fetch all nodes in the cluster
        nebula::meta::NNode local{
          nebula::meta::NRole::NODE,
          "10.1.51.48",
          9199
        };

        // TODO(cao) - expensive to recaculate metrics over and over again.
        // For every update - consider update with delta only
        NodeClient client(local, pool);
        client.state();

        // after state update
        nebula::execution::BlockManager::init()->updateTableMetrics();
      },
      5 * MS,
      "Node-Sync");

    // start the schedule
    fs->start();

    // return the scheduler so that handler holder can stop it
    return fs;
  }
};
} // namespace service
} // namespace nebula