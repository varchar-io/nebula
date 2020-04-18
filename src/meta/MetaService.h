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

#include <glog/logging.h>

#include "NNode.h"
#include "Table.h"
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
protected:
  MetaService() = default;

public:
  virtual ~MetaService() = default;

  virtual TableRegistry& query(const std::string& name) {
    static auto EMPTY = TableRegistry{ std::make_shared<Table>(name) };
    return EMPTY;
  }

  virtual std::vector<NNode> queryNodes(const std::shared_ptr<Table>, std::function<bool(const NNode&)>) {
    return {};
  }
};
} // namespace meta
} // namespace nebula