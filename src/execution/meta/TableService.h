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

#include "execution/BlockManager.h"
#include "meta/ClusterInfo.h"
#include "meta/MetaService.h"
#include "meta/NNode.h"
#include "meta/Table.h"
#include "meta/TestTable.h"

/**
 * 
 * Define table meta data service provider for nebula execution runtime.
 * 
 */
namespace nebula {
namespace execution {
namespace meta {

using TablePtr = std::shared_ptr<nebula::meta::Table>;
class TableService : public nebula::meta::MetaService {

private:
  TableService() {
    // always enroll test table
    enroll(std::make_shared<nebula::meta::TestTable>());
  }
  TableService(TableService&) = delete;
  TableService(TableService&&) = delete;

public:
  virtual ~TableService() = default;

public:
  static std::shared_ptr<TableService>& singleton() {
    static auto ts = std::shared_ptr<TableService>(new TableService());
    return ts;
  }

public:
  virtual std::shared_ptr<nebula::meta::Table> query(const std::string& name) override {
    auto it = tables_.find(name);
    if (it != tables_.end()) {
      return it->second;
    }

    return std::make_shared<nebula::meta::Table>(name);
  }

  virtual std::vector<nebula::meta::NNode> queryNodes(
    const std::shared_ptr<nebula::meta::Table>,
    std::function<bool(const nebula::meta::NNode&)>) override;

  // TODO(cao) - currently the data source of table definition is from system configs
  // this system wide truth is not good for schema evolution.
  // e.g. what about if different data blocks loaded in different time has different schema?
  void enroll(const TablePtr& tp) {
    // no need a lock
    const auto& tn = tp->name();
    if (tables_.find(tn) == tables_.end()) {
      tables_[tn] = tp;
    }
  }

  // enroll/refresh tables from the whole cluster info
  void enroll(const nebula::meta::ClusterInfo&);

private:
  // preset is a list of preload table before meta service functions
  std::unordered_map<std::string, TablePtr> tables_;
};
} // namespace meta
} // namespace execution
} // namespace nebula