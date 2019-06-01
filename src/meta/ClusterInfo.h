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

#include <mutex>
#include <unordered_set>
#include "NNode.h"

/**
 * We will sync etcd configs for cluster info into this memory object
 * To understand cluster status - total nodes.
 */
namespace nebula {
namespace meta {

struct NodeHash {
public:
  size_t operator()(const NNode& node) const {
    return (std::hash<size_t>()(static_cast<size_t>(node.role)))
           ^ (std::hash<size_t>()(node.port) * 111)
           ^ (std::hash<std::string>()(node.server) * 11111);
  }
};

struct NodeEqual {
public:
  bool operator()(const NNode& node1, const NNode& node2) const {
    // TODO(cao) - verify if there is duplicate nodes
    // while one is using IP and the other using DNS
    return node1.role == node2.role
           && node1.server == node2.server
           && node1.port == node2.port;
  }
};

using NNodeSet = std::unordered_set<NNode, NodeHash, NodeEqual>;

class ClusterInfo {
private:
  ClusterInfo() = default;
  ClusterInfo(ClusterInfo&) = delete;
  ClusterInfo(ClusterInfo&&) = delete;

public:
  virtual ~ClusterInfo() = default;

public:
  static ClusterInfo& singleton() {
    static ClusterInfo cluster;
    return cluster;
  }

public:
  void update(const NNode& node) {
    if (nodes_.find(node) != nodes_.end()) {
      return;
    }

    std::lock_guard<std::mutex> guard(set_);
    nodes_.insert(node);
  }

  std::vector<NNode> copy() {
    // get a copy of all nodes
    std::lock_guard<std::mutex> guard(set_);
    std::vector<NNode> nodes;
    nodes.reserve(nodes_.size());
    for (auto itr = nodes_.cbegin(); itr != nodes_.cend(); ++itr) {
      nodes.push_back(*itr);
    }

    return nodes;
  }

  const NNodeSet& nodes() const {
    return nodes_;
  }

private:
  std::mutex set_;
  NNodeSet nodes_;
};
} // namespace meta
} // namespace nebula