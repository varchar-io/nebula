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

#include "ClusterInfo.h"
#include "DataSpec.h"
#include "NNode.h"
#include "Table.h"
#include "TableSpec.h"
#include "common/Chars.h"
#include "common/Evidence.h"

/**
 * Define nebula table and system metadata
 * which manages what data segments are loaded in memory for each table
 * This meta data can persist and sync with external DB system such as MYSQL or RocksDB
 * (A KV store is necessary for Nebula to manage all metadata)
 *
 * (Also - Is this responsibility of zookeeper?)
 */
namespace nebula {
namespace meta {

using TablePtr = std::shared_ptr<nebula::meta::Table>;

class TableRegistry {
public:
  explicit TableRegistry(const TablePtr table, const std::string& version = "v1", size_t stl = 0)
    : table_{ table }, version_{ version }, ttl_{ stl } {}
  virtual ~TableRegistry() = default;

public:
  inline bool empty() const {
    return table_ == nullptr;
  }

  inline TablePtr table() const {
    return table_;
  }

  // check if current registry is expired
  inline bool expired() const {
    return ttl_.expired();
  }

  inline void activate() {
    ttl_.reset();
  }

  inline void deactivate() {
    ttl_.reset(1);
  }

  // check if a spec should be online or not
  // a spec is online if it's included in current snapshot
  // and it should be assigned to a node already
  inline bool online(const std::string& specId) const noexcept {
    auto spec = onlineSpecs_.find(specId);
    if (spec == onlineSpecs_.end()) {
      return false;
    }

    // should we require it to be in ready state?
    return spec->second->assigned();
  }

  inline size_t numSpecs(bool online = true) const noexcept {
    if (online) {
      return onlineSpecs_.size();
    }

    // update the logic for offline case
    return 0;
  }

  inline std::vector<SpecPtr> all(bool online = true) const {
    if (online) {
      std::vector<SpecPtr> specs;
      for (auto itr = onlineSpecs_.begin(); itr != onlineSpecs_.end(); ++itr) {
        // push the table ref
        specs.push_back(itr->second);
      }

      return specs;
    }

    // update here for offline case
    return {};
  }

  inline const std::string& version() const noexcept {
    return version_;
  }

  // update specs
  void update(const std::string& version, const std::vector<SpecPtr>&) noexcept;

private:
  TablePtr table_;
  // internal table instance version
  std::string version_;
  // seconds to live: if active + stl exceeds current time
  // the table registry is expired and will be removed
  TTL ttl_;

  // maintain online specs (in memory)
  nebula::common::unordered_map<std::string, SpecPtr> onlineSpecs_;

  // TODO: maintain offline specs as well, support transition between online/offline
public:
  static const TableRegistry& null() {
    static const TableRegistry EMPTY{ nullptr };
    return EMPTY;
  }
};

using TableRegistryPtr = std::shared_ptr<TableRegistry>;

class MetaService {
  // DB-key: total query served
  static constexpr auto QUERY_COUNT = "#queries";

protected:
  MetaService() : queryCount_{ 0 } {}

public:
  virtual ~MetaService() = default;

  virtual const TableRegistry& query(const std::string&) {
    return TableRegistry::null();
  }

  virtual std::vector<NNode> queryNodes(const std::shared_ptr<Table>, std::function<bool(const NNode&)>) {
    return {};
  }

public:
  size_t incrementQueryServed() noexcept;

  // short a URL into a 6-letter code
  std::string shortenUrl(const std::string& url) noexcept;

  // fetch a URL by a 6-letter code
  // empty result if not found
  inline std::string getUrl(const std::string& code) noexcept {
    std::string url;
    metadb().read(code, url);
    return url;
  }

protected:
  inline MetaDb& metadb() const {
    // proxy call to access cluster level meta DB
    return ClusterInfo::singleton().db();
  }

private:
  // counter for number of queries served
  size_t queryCount_;
};

} // namespace meta
} // namespace nebula