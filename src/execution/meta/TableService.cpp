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

#include "TableService.h"
#include "type/Serde.h"

/**
 * 
 * Define table meta data service provider for nebula execution runtime.
 * 
 */
namespace nebula {
namespace execution {
namespace meta {

using nebula::execution::BlockManager;
using nebula::meta::ClusterInfo;
using nebula::meta::NNode;
using nebula::meta::Table;
using nebula::type::LongType;
using nebula::type::TypeSerializer;

// query all nodes which contains data block for given table
// predicate will run client logic to check if a node should be return
std::vector<NNode> TableService::queryNodes(
  const TablePtr table, std::function<bool(const NNode&)> predicate) {
  auto bm = BlockManager::init();
  auto nodes = bm->query(table->name());

  // apply predicate
  std::remove_if(nodes.begin(), nodes.end(), [&predicate](const NNode& node) {
    return predicate(node);
  });

  return nodes;
}

void TableService::enroll(const ClusterInfo& ci) {
  for (const auto& tp : ci.tables()) {
    // TODO(cao) - need some contract definition whether user can define _time_ column or not
    // TIME_COLUMN is reserved column, can't be configured by user
    auto schema = TypeSerializer::from(tp->schema);
    schema->addChild(LongType::createTree(Table::TIME_COLUMN));

    enroll(std::make_shared<Table>(tp->name, schema, tp->columnProps));
  }
}

} // namespace meta
} // namespace execution
} // namespace nebula