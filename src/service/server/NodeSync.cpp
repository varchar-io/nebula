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

#include "NodeSync.h"

#include <glog/logging.h>

#include "execution/BlockManager.h"
#include "meta/ClusterInfo.h"
#include "meta/NNode.h"
#include "service/base/NebulaService.h"
#include "service/node/NodeClient.h"
#include "service/node/RemoteNodeConnector.h"

/**
 * Node Sync from "nodes" to "server"
 */
namespace nebula {
namespace service {
namespace server {

using nebula::common::Signable;
using nebula::common::Task;
using nebula::common::TaskState;
using nebula::common::TaskType;
using nebula::execution::BlockManager;
using nebula::ingest::SpecRepo;
using nebula::ingest::SpecState;
using nebula::meta::ClusterInfo;

std::shared_ptr<folly::FunctionScheduler> NodeSync::async(
  folly::ThreadPoolExecutor& pool,
  const SpecRepo& specRepo,
  size_t intervalMs) noexcept {

  // schedule the function every 5 seconds
  auto fs = std::make_shared<folly::FunctionScheduler>();

  // having a none query node connector
  auto connector = std::make_shared<node::RemoteNodeConnector>(nullptr);
  fs->addFunction(
    [&pool, &specRepo, connector]() {
      // TODO(cao) - here, we may have incurred too many communications between server and nodes.
      // we should batch all requests for each node and communicate once.
      // but assuming changes delta won't be large in small cluster, should be fast enough for now

      const auto& ci = ClusterInfo::singleton();
      auto nodesTalked = 0;
      for (const auto& node : ci.nodes()) {
        // fetch node state in server
        auto client = connector->makeClient(node, pool);
        client->state();
        nodesTalked++;
      }

      // after state update
      BlockManager::init()->updateTableMetrics();

      // iterate over all specs, if it needs to be process, process it
      auto taskNotified = 0;
      for (auto& spec : specRepo.specs()) {
        auto& sp = spec.second;
        if (sp->assigned() && sp->needSync()) {
          taskNotified++;

          // connect the node to sync the task over
          auto client = connector->makeClient(sp->affinity(), pool);

          // build a task out of this spec
          Task t(TaskType::INGESTION, std::static_pointer_cast<Signable>(sp));
          TaskState state = client->task(t);

          // udpate spec state so that it won't be resent
          if (state == TaskState::SUCCEEDED) {
            sp->setState(SpecState::READY);
          }

          // TODO(cao) - what about if this task is failed?
          // we can remove its assigned node and wait it to be reassin to different node for retry
          // but what if it keeps failing? we need counter for it
          if (state == TaskState::FAILED) {
            LOG(INFO) << "Task failed: " << t.signature();
          }
        }
      }

      if (taskNotified > 0) {
        LOG(INFO) << fmt::format("Communicated tasks {0} to nodes {1}.", taskNotified, nodesTalked);
      }
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