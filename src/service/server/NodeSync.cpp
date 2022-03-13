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
#include "common/Identifiable.h"
#include "execution/BlockManager.h"
#include "execution/meta/TableService.h"
#include "ingest/BlockExpire.h"
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

using nebula::common::Evidence;
using nebula::common::Identifiable;
using nebula::common::Task;
using nebula::common::TaskState;
using nebula::common::TaskType;
using nebula::execution::BlockManager;
using nebula::execution::core::NodeClient;
using nebula::execution::meta::TableService;
using nebula::ingest::BlockExpire;
using nebula::ingest::SpecRepo;
using nebula::meta::ClusterInfo;
using nebula::meta::NNode;
using nebula::meta::SpecState;

void NodeSync::sync(folly::ThreadPoolExecutor& pool, SpecRepo& specRepo) noexcept {
  const Evidence::Duration duration;
  auto connector = std::make_shared<node::RemoteNodeConnector>(nullptr);
  auto clientMaker = [&pool, &connector](const NNode& node) -> std::unique_ptr<NodeClient> {
    return connector->makeClient(node, pool);
  };

  // refresh specs snapshot for every registered table
  const auto numSpecs = specRepo.refresh();

  // expire those blocks that pass retention
  auto nodes = specRepo.expire(clientMaker);

  // assign unassigned specs
  // assign each spec to a node if it needs to be processed
  // TODO(cao) - build resource constaints here to reach a balance
  // for now, we just spin new specs into nodes with lower memory size
  std::sort(nodes.begin(), nodes.end(), [](auto& n1, auto& n2) {
    return n1.size < n2.size;
  });

  // do status check before spec assignment
  const auto numActiveNodes = nodes.size();
  if (numSpecs == 0 || numActiveNodes == 0) {
    LOG(WARNING) << "No-Sync: num-specs=" << numSpecs << ", num-active-nodes=" << numActiveNodes;
    // what if we don't have active nodes?
    if (numActiveNodes == 0) {
      for (const auto& node : ClusterInfo::singleton().nodes()) {
        LOG(INFO) << "Node State Check: node=" << node.toString() << ", state=" << (int)node.state;
      }
    }
  }

  // assign all specs to available nodes
  specRepo.assign(nodes);

  // iterate over all specs, if it needs to be process, process it
  // auto taskNotified = 0;
  // for (auto& spec : specRepo.specs()) {
  //   auto& sp = spec.second;
  //   if (sp->assigned()) {
  //     // TODO(cao): handle node reset event. SpecRepo needs to reset spec state if a node reset
  //     // if assigned to a node, but the node doesn't have the spec, we reset the spec state to
  //     // if (!bm->hasSpec(sp->affinity(), sp->table()->name, sp->id())) {
  //     //   sp->state(SpecState::RENEW);
  //     // }

  //     if (sp->needSync()) {
  //       taskNotified++;

  //       // connect the node to sync the task over
  //       auto client = connector->makeClient(sp->affinity(), pool);

  //       // build a task out of this spec
  //       Task t(TaskType::INGESTION, std::static_pointer_cast<Identifiable>(sp));
  //       TaskState state = client->task(t);

  //       // udpate spec state so that it won't be resent
  //       if (state == TaskState::SUCCEEDED) {
  //         sp->state(SpecState::READY);
  //       }
  //       // we can remove its assigned node and wait it to be reassin to different node for retry
  //       // but what if it keeps failing? we need counter for it
  //       else if (state == TaskState::FAILED || state == TaskState::QUEUE) {
  //         // TODO(cao) - post process for case if this task failed?
  //         LOG(WARNING) << "Task state: " << (char)state
  //                      << " at node: " << sp->affinity().toString()
  //                      << " | " << t.signature();
  //       }
  //     }
  //   }
  // }

  // if (taskNotified > 0) {
  //   LOG(INFO) << "Communicated tasks=" << taskNotified
  //             << " to nodes=" << nodesTalked
  //             << " using ms=" << duration.elapsedMs();
  // }
}

std::shared_ptr<folly::FunctionScheduler> NodeSync::async(
  folly::ThreadPoolExecutor& pool, SpecRepo& specRepo, size_t intervalMs) noexcept {

  // schedule the function every 5 seconds
  auto fs = std::make_shared<folly::FunctionScheduler>();

  // having a none query node connector
  fs->addFunction(
    [&pool, &specRepo]() {
      sync(pool, specRepo);
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