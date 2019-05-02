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

#include "TestTable.h"
#include "type/Serde.h"

/**
 * Nebula test table used for integration test.
 * Used to turn on/off test hooks.
 * This will be removed after we implemented the meta service
 */
namespace nebula {
namespace meta {

using nebula::type::TypeSerializer;

const std::string& TestTable::name() {
  static const std::string NAME = "nebula.test";
  return NAME;
}

const std::string& TestTable::schema() {
  static const std::string SCHEMA = "ROW<id:int, event:string, items:list<string>, flag:bool, value:tinyint>";
  return SCHEMA;
}

MockTable::MockTable(const std::string& name) : Table(name) {
  if (name == nebula::meta::TestTable::name()) {
    schema_ = TypeSerializer::from(nebula::meta::TestTable::schema());
  }
}

std::shared_ptr<nebula::meta::Table> MockMs::query(const std::string& name) {
  return std::make_shared<MockTable>(name);
}

std::vector<nebula::meta::NNode> MockMs::queryNodes(
  const std::shared_ptr<nebula::meta::Table>,
  std::function<bool(const nebula::meta::NNode&)>) {
  return { nebula::meta::NNode::local() };
}

} // namespace meta
} // namespace nebula