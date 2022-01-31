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

#include <mutex>
#include <yaml-cpp/yaml.h>

#include "MetaDb.h"
#include "NodeManager.h"
#include "TableSpec.h"
#include "common/Hash.h"

/**
 * We will sync etcd configs for cluster info into this memory object
 * To understand cluster status - total nodes.
 */
namespace nebula {
namespace meta {

// define metadb where metadata is stored and synced.
enum class DBType {
  NONE,
  NATIVE
};

struct MetaConf {
  DBType type;
  std::string store;
};

using CreateMetaDB = std::function<std::unique_ptr<MetaDb>(const MetaConf&)>;

enum class Discovery {
  // read nodes from config file
  CONFIG,
  // wait nodes to register themselves to service endpoint
  SERVICE
  // external service for service discovery.
};

// server options mapping in cluster.yml for server
struct ServerOptions {
  bool anode;
  bool authRequired;
  MetaConf metaConf;
  Discovery discovery;
};

class ClusterInfo {
private:
  // by default - use service discovery node manager
  // if config mode is specified, it will be replaced.
  ClusterInfo() : nodeManager_{ NodeManager::create() } {};
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
  // internal state changes that may require a reload
  inline bool shouldReload() const noexcept {
    return stateChanged_;
  }

  // load cluster info from config file
  void load(const std::string&, CreateMetaDB);

  // table name and its yaml definition content
  // should move to metadata server in future
  std::string addTable(const std::string&, const std::string&);

  // remove the table entry
  inline size_t removeTable(const std::string& table) noexcept {
    stateChanged_ = true;
    return runtimeTables_.erase(table);
  }

  inline const std::vector<NNode> nodes() const {
    return nodeManager_->nodes();
  }

  inline const TableSpecSet& tables() const {
    return tables_;
  }

  inline const std::string& version() const {
    return version_;
  }

  inline const ServerOptions& server() const {
    return server_;
  }

  inline MetaDb& db() const {
    return *db_;
  }

  inline void mark(const std::string& node, NState state = NState::BAD) {
    nodeManager_->mark(node, state);
  }

  inline NodeManager& nodeManager() const {
    return *nodeManager_;
  }

  void exit() noexcept {
    // do one more time backup before exiting
    // abnormal exiting handling (e.g. SIGINT)
    // close will be responsible for backup if needed
    if (db_) {
      db_->close();
    }
  }

private:
  std::unique_ptr<NodeManager> nodeManager_;
  TableSpecSet tables_;
  std::string version_;
  ServerOptions server_;
  std::unique_ptr<MetaDb> db_;
  nebula::common::unordered_map<std::string, YAML::Node> runtimeTables_;
  bool stateChanged_;
};
} // namespace meta
} // namespace nebula
