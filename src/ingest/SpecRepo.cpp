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
#include "execution/meta/SpecProvider.h"
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
using nebula::common::unordered_map;
using nebula::execution::meta::SpecProvider;
using nebula::meta::ClusterInfo;
using nebula::meta::DataSource;
using nebula::meta::DataSourceUtils;
using nebula::meta::Macro;
using nebula::meta::MapKV;
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
void SpecRepo::refresh(const ClusterInfo& ci) noexcept {
  // we only support adding new spec to the repo
  // if a spec is already in repo, we skip it
  // for some use case such as data refresh, it will have the same signature
  // if data is newer (e.g file size + timestamp), we should mark it as replacement.
  std::vector<SpecPtr> specs;
  const auto& tableSpecs = ci.tables();

  // generate a version all spec to be made during this batch: {config version}_{current unix timestamp}
  SpecProvider provider;
  const auto version = fmt::format("{0}.{1}", ci.version(), Evidence::unix_timestamp());
  for (auto itr = tableSpecs.cbegin(); itr != tableSpecs.cend(); ++itr) {
    std::vector<SpecPtr> snapshot = provider.generate(version, *itr);
    specs.insert(specs.end(), std::begin(snapshot), std::end(snapshot));
  }

  // process all specs to mark their status
  update(specs);
}

void SpecRepo::update(const std::vector<SpecPtr>& specs) noexcept {
  // next version of all specs
  unordered_map<std::string, SpecPtr> next;
  next.reserve(specs.size());

  // go through the new spec list and update the existing ones
  // need lock here?
  auto brandnew = 0;
  auto renew = 0;
  auto removed = specs_.size() - specs.size();
  for (auto itr = specs.cbegin(), end = specs.cend(); itr != end; ++itr) {
    // check if we have this spec already?
    auto specPtr = (*itr);
    const auto& sign = specPtr->id();
    auto found = specs_.find(sign);
    if (found == specs_.end()) {
      ++brandnew;
    } else {
      auto prev = found->second;

      // by default, we carry over existing spec's properties
      const auto& node = prev->affinity();
      specPtr->affinity(node);
      specPtr->state(prev->state());

      // TODO(cao) - use only size for the checker for now, may extend to other properties
      // this is an update case, otherwise, spec doesn't change, ignore it.
      if (specPtr->size() != prev->size()) {
        specPtr->state(SpecState::RENEW);
        ++renew;
      }

      // if the node is not active, we may remove the affinity to allow new assignment
      if (!node.isActive()) {
        specPtr->affinity(NNode::invalid());
      }
    }

    // move to the next version
    next.emplace(sign, specPtr);
  }

  // print out update stats
  if (brandnew > 0 || renew > 0 || removed > 0) {
    LOG(INFO) << "Updating " << specs.size()
              << " specs: brandnew=" << brandnew
              << ", renew=" << renew
              << ", removed=" << removed
              << ", count=" << next.size();

    // let's swap with existing one
    if (specs.size() != next.size()) {
      LOG(WARNING) << "No duplicate specs allowed.";
    }

    std::swap(specs_, next);
  }
}

bool SpecRepo::confirm(const std::string& spec, const nebula::meta::NNode& node) noexcept {
  auto f = specs_.find(spec);
  // not found
  if (f == specs_.end()) {
    return false;
  }

  // reuse the same node for the same spec
  auto& sp = f->second;
  if (!sp->assigned()) {
    sp->affinity(node);
    return true;
  }

  // not in the same node
  auto& assignment = sp->affinity();
  if (!assignment.equals(node)) {
    LOG(INFO) << "Spec [" << spec << "] moves from " << node.server << " to " << assignment.server;
    return false;
  }

  return true;
}

void SpecRepo::assign(const std::vector<NNode>& nodes) noexcept {
  // we're looking for a stable assignmet, given the same set of nodes
  // this order is most likely having stable order
  // std::sort(nodes.begin(), nodes.end(), [](auto& n1, auto& n2) {
  //   return n1.server.compare(n2.server);
  // });
  const auto size = nodes.size();

  if (size == 0) {
    LOG(WARNING) << "No nodes to assign nebula specs.";
    return;
  }

  size_t idx = 0;

  // for each spec
  // TODO(cao): should we do hash-based shuffling here to ensure a stable assignment?
  // Round-robin is easy to break the position affinity whenever new spec is coming
  // Or we can keep order of the specs so that any old spec is associated.
  for (auto& spec : specs_) {
    // not assigned yet
    auto sp = spec.second;
    if (!sp->assigned()) {
      auto startId = idx;
      while (true) {
        auto& n = nodes.at(idx);
        if (n.isActive()) {
          sp->affinity(n);
          idx = (idx + 1) % size;
          break;
        }

        idx = (idx + 1) % size;
        if (idx == startId) {
          LOG(ERROR) << "No active node found to assign spec.";
          return;
        }
      }
    }
  }
}

} // namespace ingest
} // namespace nebula