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

#pragma once

#include <glog/logging.h>

#include "common/Evidence.h"
#include "meta/NNode.h"

namespace nebula {
namespace meta {
// Node Manager provides management functionality to discover working nodes in the cluster.
// Initially it provides service discovery only such as address, port, host name
// In the long run, it may be evoloved to tracking node working load statistics as well
// These statistics can be used in the run time to do a good judgement on resource allocation.
// such as
// - Used memory, free memory
// - CPU utilization in last X window
// - Number of data sources distributed, etc.
struct NodeStats {
  // last ping timestamp in unix time milliseconds
  size_t lastPing;
};

using NNodeSet = nebula::common::unordered_set<NNode, NodeHash, NodeEqual>;
using NNodeMap = nebula::common::unordered_map<NNode, NodeStats, NodeHash, NodeEqual>;

class NodeManager {
protected:
  NodeManager() = default;

public:
  virtual ~NodeManager() = default;

  // return all available nodes
  virtual std::vector<NNode> nodes() const = 0;

  // mark a node as a bad node
  virtual void mark(const std::string&, NState = NState::BAD) = 0;

  // update node
  virtual void update(const NNode&) = 0;

public:
  static std::unique_ptr<NodeManager> create(NNodeSet);
  static std::unique_ptr<NodeManager> create();
};

class StaticNodeManager : public NodeManager {
public:
  StaticNodeManager(NNodeSet set) : nodes_{ std::move(set) } {}

  virtual std::vector<NNode> nodes() const override {
    // make a copy and return the copy - should be inexpensive
    return std::vector<NNode>(nodes_.begin(), nodes_.end());
  }

  virtual void mark(const std::string& node, NState state = NState::BAD) override {
    for (auto itr = nodes_.begin(); itr != nodes_.end(); ++itr) {
      if (node == itr->toString()) {
        itr->state = state;
        if (state == NState::BAD) {
          nodes_.erase(itr);
          LOG(WARNING) << "Removing a node as it's marked as bad.";
        }
        break;
      }
    }
  }

  virtual void update(const NNode&) override {
    // Do nothing as config mode doesn't accept node registration
    LOG(WARNING) << "Config mode does not update node list.";
  }
  
private:
  NNodeSet nodes_;
};

class DynamicNodeManager : public NodeManager {
  // a healthy node should report its status in last 3 seconds
  static constexpr auto HEALTHY_TIME = 3;

public:
  DynamicNodeManager() = default;
  ~DynamicNodeManager() = default;

public:
  virtual std::vector<NNode> nodes() const override {
    std::vector<NNode> nodes;
    nodes.reserve(nodes_.size());
    const auto now = nebula::common::Evidence::unix_timestamp();
    for (auto itr = nodes_.begin(); itr != nodes_.end(); ++itr) {
      auto ping = itr->second.lastPing;
      if (now - ping <= HEALTHY_TIME) {
        nodes.push_back(itr->first);
      } else {
        LOG(WARNING) << "Seeing an unhealthy node " << itr->first.toString() << " with last ping at " << ping;
      }
    }

    return nodes;
  }

  virtual void mark(const std::string& node, NState state = NState::BAD) override {
    for (auto itr = nodes_.begin(); itr != nodes_.end(); ++itr) {
      if (node == itr->first.toString()) {
        // only care about bad node and clean it up
        if (state == NState::BAD) {
          nodes_.erase(itr);
          LOG(WARNING) << "Removing a node as it's marked as bad.";
        }
        break;
      }
    }
  }

  virtual void update(const NNode& node) override {
    nodes_[node] = { nebula::common::Evidence::unix_timestamp() };
  }

private:
  NNodeMap nodes_;
};

} // namespace meta
} // namespace nebula