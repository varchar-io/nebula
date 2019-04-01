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

#include "common/Errors.h"
#include "glog/logging.h"
#include "type/Type.h"

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

using nebula::type::Schema;

class Table {
public:
  Table(const std::string& name) : name_{ name }, schema_{ nullptr } {
    // TODO(cao) - load table properties from meta data service
    loadTable();
  }
  virtual ~Table() = default;

public:
  virtual Schema getSchema() const {
    N_ENSURE_NOT_NULL(schema_, fmt::format("invalid table not found = {0}", name_));
    return schema_;
  }

  inline std::string name() const {
    return name_;
  }

protected:
  // table name is global unique, but it can be organized by some namespace style naming convention
  // such as "nebula.test"
  std::string name_;
  Schema schema_;

private:
  void loadTable();
};
} // namespace meta
} // namespace nebula