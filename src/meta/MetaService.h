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
#include "NNode.h"
#include "Table.h"
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
  explicit TableRegistry(const TablePtr table, size_t stl = 0)
    : table_{ table }, stl_{ stl } {
    activate();
  }

public:
  // check if current registry is expired
  inline bool expired() const {
    return stl_ > 0 && active_ + stl_ < nebula::common::Evidence::unix_timestamp();
  }

  inline void activate() {
    active_ = nebula::common::Evidence::unix_timestamp();
  }

  inline TablePtr table() const {
    return table_;
  }

private:
  TablePtr table_;
  // last active time stamp
  size_t active_;
  // seconds to live: if active + stl exceeds current time
  // the table registry is expired and will be removed
  size_t stl_;
};

class MetaService {
  // DB-key: total query served
  static constexpr auto QUERY_COUNT = "#queries";

protected:
  MetaService() : queryCount_{ 0 } {}

public:
  virtual ~MetaService() = default;

  virtual TableRegistry& query(const std::string& name) {
    static auto EMPTY = TableRegistry{ std::make_shared<Table>(name) };
    return EMPTY;
  }

  virtual std::vector<NNode> queryNodes(const std::shared_ptr<Table>, std::function<bool(const NNode&)>) {
    return {};
  }

public:
  size_t incrementQueryServed() noexcept {
    auto& db = metadb();

    std::string value;
    if (queryCount_ == 0 && db.read(QUERY_COUNT, value)) {
      queryCount_ = folly::to<size_t>(value);
    }
    value = std::to_string(++queryCount_);
    db.write(QUERY_COUNT, value);

    return queryCount_;
  }

  // short a URL into a 6-letter code
  std::string shortenUrl(const std::string& url) noexcept {
    static constexpr auto MAX_ATTEMPT = 5;
    auto& db = metadb();
    // handle collision
    auto str = url.data();
    auto size = url.size();
    auto i = 0;
    while (i++ < MAX_ATTEMPT) {
      // get digest
      auto digest = nebula::common::Chars::digest(str, size);
      // if the digest does not exist, we save it and return
      std::string value;
      if (db.read(digest, value)) {
        // same URL already existing
        if (value == url) {
          return digest;
        }

        // not the same, we have collision, change input and try it again
        size -= 6;
        continue;
      }

      // key is not existing, write it out
      db.write(digest, url);
      return digest;
    }

    // nothing is working
    return {};
  }

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
  size_t queryCount_;
};
} // namespace meta
} // namespace nebula