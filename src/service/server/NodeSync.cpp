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

#include "NodeSync.h"

#include <glog/logging.h>

#include "common/Evidence.h"
#include "ingest/SpecRepo.h"
#include "meta/NNode.h"
#include "service/node/NodeClient.h"
#include "service/node/RemoteNodeConnector.h"

/**
 * Node Sync from "nodes" to "server"
 */
namespace nebula {
namespace service {
namespace server {

using nebula::common::Evidence;
using nebula::execution::core::NodeClient;
using nebula::ingest::SpecRepo;
using nebula::meta::NNode;

void NodeSync::sync(folly::ThreadPoolExecutor& pool) noexcept {
  const Evidence::Duration duration;
  auto connector = std::make_shared<node::RemoteNodeConnector>(nullptr);
  auto clientMaker = [&pool, &connector](const NNode& node) -> std::unique_ptr<NodeClient> {
    return connector->makeClient(node, pool);
  };

  // refresh specs snapshot for every registered table
  auto& specRepo = SpecRepo::singleton();
  const auto numSpecs = specRepo.refresh();
  if (numSpecs == 0) {
    LOG(WARNING) << "No-Sync: no specs generated in current cycle.";
    return;
  }

  // expire those blocks that pass retention
  auto numExpired = specRepo.expire(clientMaker);

  // assign all specs to available nodes, and reutrn number of tasks communicated
  auto tasksNodes = specRepo.assign(clientMaker);
  if (tasksNodes.first > 0) {
    LOG(INFO) << "Communicated tasks=" << tasksNodes.first
              << " expired=" << numExpired
              << " to nodes=" << tasksNodes.second
              << " using ms=" << duration.elapsedMs();
  }
}

std::shared_ptr<folly::FunctionScheduler> NodeSync::async(
  folly::ThreadPoolExecutor& pool, size_t intervalMs) noexcept {

  // schedule the function every 5 seconds
  auto fs = std::make_shared<folly::FunctionScheduler>();

  // having a none query node connector
  fs->addFunction(
    [&pool]() {
      sync(pool);
    },
    std::chrono::milliseconds(intervalMs),
    "Node-Sync");

  // start the schedule
  fs->start();

  // return the scheduler so that handler holder can stop it
  return fs;
}
} // namespace server
} // namespace service
} // namespace nebula