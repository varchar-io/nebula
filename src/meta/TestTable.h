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

#include <string>
#include "Table.h"
#include "surface/eval/ValueEval.h"
#include "type/Serde.h"

/**
 * Nebula test table used for integration test.
 * Used to turn on/off test hooks.
 */
namespace nebula {
namespace meta {

class TestTable : public Table {
  static constexpr auto NAME = "nebula.test";
  static auto CP() {
    static const Column COL_ID{ true, false, false, "", {}, {} };
    // place an access rule on event column requiring user to be in nebula-users to read
    static const Column COL_EVENT{
      false,
      true,
      false,
      "",
      { AccessRule{ AccessType::READ, { "nebula-users" }, ActionType::MASK } },
      {}
    };
    static const Column COL_TAG{
      false,
      false,
      true,
      "",
      {},
      { { "a", "b", "c", "d" }, 1 }
    };
    static const Column COL_VALUE{ false, false, false, "23", {} };
    static const Column COL_STAMP{ false, false, true, "128", {} };

    return ColumnProps{ { "id", COL_ID },
                        { "event", COL_EVENT },
                        { "tag", COL_TAG },
                        { "value", COL_VALUE },
                        { "stamp", COL_STAMP } };
  }

public:
  TestTable() : Table(NAME,
                      nebula::type::TypeSerializer::from(
                        "ROW<_time_: bigint, id:int, event:string, tag:string, items:list<string>, flag:bool, value:tinyint, weight:double, stamp:int128, stack:string>"),
                      CP(),
                      {}) {
    // make a test value eval list for testing
    fields_.reserve(10);
    fields_.emplace_back(nebula::surface::eval::constant(1));
    fields_.emplace_back(nebula::surface::eval::constant(2));
    fields_.emplace_back(nebula::surface::eval::constant("3"));
    fields_.emplace_back(nebula::surface::eval::constant("4"));
    // TODO(cao) - we don't have a good value eval set up for LIST type
    // use a fake one to replace - this may cause problem!!
    fields_.emplace_back(nebula::surface::eval::constant(1));
    fields_.emplace_back(nebula::surface::eval::constant(true));
    fields_.emplace_back(nebula::surface::eval::constant((int8_t)0));
    fields_.emplace_back(nebula::surface::eval::constant(0.1));
    fields_.emplace_back(nebula::surface::eval::constant((int128_t)1));
    fields_.emplace_back(nebula::surface::eval::constant("a\nb"));
  }

  const nebula::surface::eval::Fields& testFields() const {
    return fields_;
  }

private:
  nebula::surface::eval::Fields fields_;
};

// define a partitioned table
class TestPartitionedTable : public Table {
  static constexpr auto NAME = "nebula.test.partition";
  static auto CP() {
    static const Column D1{ false, false, false, "a", {}, { { "a", "b", "c", "e", "f", "g" }, 3 } };
    static const Column D2{ false, false, false, "1", {}, { { "1", "2", "3", "4" }, 2 } };
    static const Column D3{ false, false, false, "11", {}, { { "11", "12", "13" }, 1 } };

    return ColumnProps{ { "d1", D1 }, { "d2", D2 }, { "d3", D3 } };
  }

public:
  TestPartitionedTable() : Table(
    NAME,
    nebula::type::TypeSerializer::from(
      "ROW<_time_: bigint, d1:string, d2:tinyint, d3:int, value:tinyint, weight:double>"),
    CP(),
    {}) {
    // make a test value eval list for testing
    fields_.reserve(6);
    fields_.emplace_back(nebula::surface::eval::constant(1));
    fields_.emplace_back(nebula::surface::eval::constant("2"));
    fields_.emplace_back(nebula::surface::eval::constant((int8_t)3));
    fields_.emplace_back(nebula::surface::eval::constant(4));
    fields_.emplace_back(nebula::surface::eval::constant((int8_t)5));
    fields_.emplace_back(nebula::surface::eval::constant(6.0));
  }

  const nebula::surface::eval::Fields& testFields() const {
    return fields_;
  }

private:
  nebula::surface::eval::Fields fields_;
};

} // namespace meta
} // namespace nebula