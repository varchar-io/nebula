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
#include "common/Errors.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "meta/MetaService.h"
#include "meta/Table.h"
#include "type/Serde.h"

namespace nebula {
namespace api {
namespace test {

using nebula::type::TypeSerializer;

class MockTable : public nebula::meta::Table {
public:
  MockTable(const std::string& name) : Table(name) {
    schema_ = TypeSerializer::from("ROW<id:int, items:list<string>, flag:bool>");
  }
  virtual ~MockTable() = default;
};

class MockMs : public nebula::meta::MetaService {
public:
  virtual std::shared_ptr<nebula::meta::Table> query(const std::string& name) override {
    return std::make_shared<MockTable>(name);
  }
};
} // namespace test
} // namespace api
} // namespace nebula