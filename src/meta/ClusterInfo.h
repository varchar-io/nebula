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

#include "MetaDb.h"
#include "NNode.h"
#include "TableSpec.h"
#include "common/Hash.h"

/**
 * We will sync etcd configs for cluster info into this memory object
 * To understand cluster status - total nodes.
 */
namespace nebula {
namespace meta {

using NNodeSet = nebula::common::unordered_set<NNode, NodeHash, NodeEqual>;

// define metadb where metadata is stored and synced.
enum class DBType {
  NATIVE
};

struct MetaConf {
  DBType type;
  std::string store;
};

using CreateMetaDB = std::function<std::unique_ptr<MetaDb>(const MetaConf&)>;

// server options mapping in cluster.yml for server
struct ServerOptions {
  bool anode;
  bool authRequired;
  MetaConf metaConf;
};

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
  void load(const std::string&, CreateMetaDB);

  std::vector<NNode> copy();

  inline const NNodeSet& nodes() const {
    return nodes_;
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
    for (auto itr = nodes_.begin(); itr != nodes_.end(); ++itr) {
      if (node == itr->toString()) {
        // same as std::unordered_set.extract
        itr->state = state;
        nodes_.erase(itr);
        break;
      }
    }
  }

  void exit() noexcept {
    // do one more time backup before exiting
    // abnormal exiting handling (e.g. SIGINT)
    if (db_) {
      db_->close();
      db_->backup();
    }
  }

private:
  void update(const NNode& node);

private:
  std::mutex set_;
  NNodeSet nodes_;
  TableSpecSet tables_;
  std::string version_;
  ServerOptions server_;
  std::unique_ptr<MetaDb> db_;
};
} // namespace meta
} // namespace nebula
