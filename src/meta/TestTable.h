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

#include <string>
#include "MetaService.h"
#include "Table.h"
#include "type/Serde.h"

/**
 * Nebula test table used for integration test.
 * Used to turn on/off test hooks.
 */
namespace nebula {
namespace meta {

class MockMs : public MetaService {
public:
  virtual std::shared_ptr<nebula::meta::Table> query(const std::string& name) override;

  virtual std::vector<nebula::meta::NNode> queryNodes(
    const std::shared_ptr<nebula::meta::Table>,
    std::function<bool(const nebula::meta::NNode&)>) override;
};

class TestTable : public Table {
  static constexpr auto NAME = "nebula.test";

public:
  TestTable() : Table(NAME) {
    // TODO(cao) - let's make date as a number
    schema_ = nebula::type::TypeSerializer::from(
      "ROW<_time_: bigint, id:int, event:string, items:list<string>, flag:bool, value:tinyint>");
  }

  virtual std::shared_ptr<nebula::meta::MetaService> getMs() const override {
    return std::make_shared<MockMs>();
  }
};

} // namespace meta
} // namespace nebula