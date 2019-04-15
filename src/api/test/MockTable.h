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

#include "gtest/gtest.h"
#include <glog/logging.h>
#include "common/Errors.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "fmt/format.h"
#include "gmock/gmock.h"
#include "meta/MetaService.h"
#include "meta/NNode.h"
#include "meta/Table.h"
#include "meta/TestTable.h"
#include "type/Serde.h"

namespace nebula {
namespace api {
namespace test {

using nebula::type::TypeSerializer;

class MockTable : public nebula::meta::Table {
public:
  MockTable(const std::string& name) : Table(name) {
    schema_ = TypeSerializer::from(nebula::meta::TestTable::schema());
  }
  virtual ~MockTable() = default;
};

class MockMs : public nebula::meta::MetaService {
public:
  virtual std::shared_ptr<nebula::meta::Table> query(const std::string& name) override {
    return std::make_shared<MockTable>(name);
  }

  virtual std::vector<nebula::meta::NNode> queryNodes(
    const std::shared_ptr<nebula::meta::Table>,
    std::function<bool(const nebula::meta::NNode&)>) override {
    return { nebula::meta::NNode::local() };
  }
};
} // namespace test
} // namespace api
} // namespace nebula