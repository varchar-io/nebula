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

#include <fmt/format.h>
#include <folly/Conv.h>

#include "SpecRepo.h"

#include "common/Evidence.h"
#include "execution/BlockManager.h"
#include "execution/meta/SpecProvider.h"
#include "execution/meta/TableService.h"
#include "ingest/BlockExpire.h"
#include "storage/NFS.h"
#include "storage/kafka/KafkaTopic.h"

/**
 * We will sync etcd configs for cluster info into this memory object
 * To understand cluster status - total nodes.
 */
namespace nebula {
namespace ingest {

using dsu = nebula::meta::DataSourceUtils;
using nebula::common::Evidence;
using nebula::common::Identifiable;
using nebula::common::MapKV;
using nebula::common::Task;
using nebula::common::TaskState;
using nebula::common::TaskType;
using nebula::common::unordered_map;
using nebula::execution::BlockManager;
using nebula::execution::TableSpecSet;
using nebula::execution::core::NodeClient;
using nebula::execution::core::NodeConnector;
using nebula::execution::meta::SpecProvider;
using nebula::execution::meta::TableService;
using nebula::meta::ClusterInfo;
using nebula::meta::DataSource;
using nebula::meta::DataSourceUtils;
using nebula::meta::Macro;
using nebula::meta::NNode;
using nebula::meta::NNodeSet;
using nebula::meta::PatternMacro;
using nebula::meta::SpecPtr;
using nebula::meta::SpecSplit;
using nebula::meta::SpecSplitPtr;
using nebula::meta::SpecState;
using nebula::meta::TableSpecPtr;
using nebula::meta::TimeSpec;
using nebula::meta::TimeType;
using nebula::storage::FileInfo;
using nebula::storage::kafka::KafkaSegment;
using nebula::storage::kafka::KafkaTopic;

// generate a list of ingestion spec based on cluster info
size_t SpecRepo::refresh() noexcept {
  // cluster info and table service
  const auto& ci = ClusterInfo::singleton();
  const auto& ts = TableService::singleton();

  // we only support adding new spec to the repo
  // if a spec is already in repo, we skip it
  // for some use case such as data refresh, it will have the same signature
  // if data is newer (e.g file size + timestamp), we should mark it as replacement.
  const auto& tableSpecs = ci.tables();

  size_t numSpecs = 0;
  // generate a version all spec to be made during this batch: {config version}_{current unix timestamp}
  SpecProvider provider;
  const auto version = fmt::format("{0}.{1}", ci.version(), Evidence::unix_timestamp());
  for (auto itr = tableSpecs.cbegin(); itr != tableSpecs.cend(); ++itr) {
    const auto& table = *itr;
    auto registry = ts->get(table);
    std::vector<SpecPtr> snapshot = provider.generate(version, table);
    numSpecs += snapshot.size();

    registry->update(snapshot);
  }

  return numSpecs;
}

// remove (or take it offline) all expired blocks from active nodes
std::vector<NNode> SpecRepo::expire(const ClientMaker& clientMaker) noexcept {
  // cluster manager and a local block manager
  const auto& ci = ClusterInfo::singleton();
  const auto& bm = BlockManager::init();
  const auto& ts = TableService::singleton();

  const auto& clusterNodes = ci.nodes();
  std::vector<NNode> nodes;
  nodes.reserve(clusterNodes.size());
  for (const auto& node : clusterNodes) {
    if (node.isActive()) {
      // fetch node state in server
      auto client = clientMaker(node);
      client->update();

      // extracting all expired spec from existing blocks on this node
      // make a copy since it's possible to be removed.
      const auto& states = bm->states(node);

      // recording expired block ID for given node
      TableSpecSet expired;
      size_t memorySize = 0;
      for (auto itr = states.begin(); itr != states.end(); ++itr) {
        const auto& state = itr->second;
        auto pairs = state->expired([&ts](const std::string& table, const std::string& spec) -> bool {
          // find the spec from table registery who tracks all online specs
          const auto& registry = ts->query(table);

          // check if the table registry has this spec online
          if (!registry.empty() && registry.online(spec)) {
            return false;
          }

          // remove it otherise
          return true;
        });

        if (!pairs.empty()) {
          // should be the same as std::unordered_set.merge
          expired.insert(pairs.begin(), pairs.end());
        }

        // TODO(cao): use memory size rather than data raw size
        // accumulate memory usage for this node
        memorySize += state->rawBytes();
      }

      // sync expire task to node
      const auto expireSize = expired.size();
      if (expireSize > 0) {
        Task t(TaskType::EXPIRATION, std::shared_ptr<Identifiable>(new BlockExpire(std::move(expired))));
        TaskState state = client->task(t);
        LOG(INFO) << fmt::format("Expire {0} specs in node {1}: {2}", expireSize, node.server, (char)state);
      }

      // push a node with a size
      NNode n{ node };
      n.size = memorySize;
      nodes.push_back(std::move(n));
    }
  }

  // return all active nodes that we have communicated
  return nodes;
}

size_t SpecRepo::assign(const std::vector<NNode>& nodes, const ClientMaker& clientMaker) noexcept {
  const auto size = nodes.size();
  if (size == 0) {
    LOG(WARNING) << "No nodes to assign nebula specs.";
    return 0;
  }

  const auto& ts = TableService::singleton();
  size_t idx = 0;

  // for each spec
  // TODO(cao): should we do hash-based shuffling here to ensure a stable assignment?
  // Round-robin is easy to break the position affinity whenever new spec is coming
  // Or we can keep order of the specs so that any old spec is associated.
  auto numTasks = 0;
  auto tables = ts->all();
  for (auto& registry : tables) {
    auto specs = registry->all();
    for (auto& spec : specs) {
      // if the spec is not assigned to a node yet
      if (!spec->assigned()) {
        auto startId = idx;
        while (true) {
          auto& n = nodes.at(idx);
          if (n.isActive()) {
            spec->affinity(n);
            idx = (idx + 1) % size;
            break;
          }

          idx = (idx + 1) % size;
          if (idx == startId) {
            LOG(ERROR) << "No active node found to assign a spec.";
            return numTasks;
          }
        }
      }

      // check if the spec needs to be communicated to the node
      if (spec->needSync()) {
        ++numTasks;

        // get the client
        auto client = clientMaker(spec->affinity());
        Task t(TaskType::INGESTION, std::static_pointer_cast<Identifiable>(spec));
        TaskState state = client->task(t);

        // udpate spec state so that it won't be resent
        if (state == TaskState::SUCCEEDED) {
          spec->state(SpecState::READY);
        } else if (state == TaskState::FAILED || state == TaskState::QUEUE) {
          // TODO(cao) - post process for case if this task failed?
          LOG(WARNING) << "Task state: " << (char)state
                       << " at node: " << spec->affinity().toString()
                       << " | " << t.signature();
        }
      }
    }
  }

  // number of tasks communicated
  return numTasks;
}

} // namespace ingest
} // namespace nebula