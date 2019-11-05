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
#include "Table.h"
#include "type/Serde.h"

/**
 * Nebula test table used for integration test.
 * Used to turn on/off test hooks.
 */
namespace nebula {
namespace meta {

class TestTable : public Table {
  static constexpr auto NAME = "nebula.test";

public:
  TestTable() : Table(NAME) {
    // TODO(cao) - let's make date as a number
    schema_ = nebula::type::TypeSerializer::from(
      "ROW<_time_: bigint, id:int, event:string, items:list<string>, flag:bool, value:tinyint, weight:double, stamp:int128>");
  }

  virtual Column column(const std::string& col) const noexcept override {
    if (col == "id") {
      // enable bloom filter on id column
      return Column{ true, false, "" };
    }

    if (col == "event") {
      // enable bloom filter on id column
      return Column{ false, true, "" };
    }

    if (col == "value") {
      return Column{ false, false, "23" };
    }

    if (col == "stamp") {
      return Column{ false, false, "128" };
    }

    return Table::column(col);
  }
};

} // namespace meta
} // namespace nebula