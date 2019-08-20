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

#include <glog/logging.h>

#include "execution/BlockManager.h"
#include "ingest/SpecRepo.h"
#include "meta/ClusterInfo.h"
#include "meta/NNode.h"
#include "service/node/NodeClient.h"
#include "service/node/RemoteNodeConnector.h"

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
  static std::shared_ptr<folly::FunctionScheduler> async(
    folly::ThreadPoolExecutor& pool,
    const nebula::ingest::SpecRepo& specRepo,
    size_t intervalMs) noexcept {

    // schedule the function every 5 seconds
    auto fs = std::make_shared<folly::FunctionScheduler>();

    // having a none query node connector
    RemoteNodeConnector connector(nullptr);
    fs->addFunction(
      [&pool, &specRepo, &connector]() {
        // TODO(cao) - here, we may have incurred too many communications between server and nodes.
        // we should batch all requests for each node and communicate once.
        // but assuming changes delta won't be large in small cluster, should be fast enough for now

        const auto& ci = nebula::meta::ClusterInfo::singleton();
        auto nodesTalked = 0;
        for (const auto& node : ci.nodes()) {
          // fetch node state in server
          auto client = connector.makeClient(node, pool);
          client->state();
          nodesTalked++;
        }

        // after state update
        nebula::execution::BlockManager::init()->updateTableMetrics();

        // iterate over all specs, if it needs to be process, process it
        auto taskNotified = 0;
        for (const auto& spec : specRepo.specs()) {
          const auto& sp = spec.second;
          if (sp->assigned() && sp->needSync()) {
            taskNotified++;
          }
        }

        LOG(INFO) << fmt::format("Communicated tasks {0} to nodes {1}.", taskNotified, nodesTalked);
      },
      std::chrono::milliseconds(intervalMs),
      "Node-Sync");

    // start the schedule
    fs->start();

    // return the scheduler so that handler holder can stop it
    return fs;
  }
};
} // namespace service
} // namespace nebula