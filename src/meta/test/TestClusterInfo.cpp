/*
 * Copyright 2020-present
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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/Evidence.h"
#include "meta/ClusterInfo.h"
#include "meta/TestUtils.h"

namespace nebula {
namespace meta {
namespace test {

using nebula::common::Evidence;

constexpr auto TB_DEF =
  "retention:\n"
  "  max-mb: 10000\n"
  "  max-hr: 240\n"
  "schema: \"ROW<id:int, event:string, items:list<string>, flag:bool, value:tinyint>\"\n"
  "data: local\n"
  "loader: NebulaTest\n"
  "source: \"\"\n"
  "backup: s3://nebula/n100/\n"
  "format: none\n"
  "time:\n"
  "  type: static\n"
  "  value: 1565994194\n";

// test table management in cluster info
TEST(ClusterInfoTest, TestTableSpecManagement) {
  auto createDB = [](const nebula::meta::MetaConf&) {
    return std::make_unique<nebula::meta::VoidDb>();
  };

  auto& ci = ClusterInfo::singleton();
  {
    // NON-ephemeral table, will be wiped out
    auto tableSpec = nebula::meta::genTableSpec();
    tableSpec->name = "test1";
    ci.addTable(tableSpec);
    EXPECT_EQ(ci.tables().size(), 1);
  }

  // load the test config which includes 3 table definitions
  // previous added table is wiped since it's not in the settings
  ci.load("configs/test.yml", createDB);
  EXPECT_EQ(ci.tables().size(), 3);

  // add a runtime table definition
  ci.addTable("runtime_table", TB_DEF);

  // runtime table will be included: 3+1
  ci.load("configs/test.yml", createDB);
  EXPECT_EQ(ci.tables().size(), 4);

  // add a new ephemeral table which should not wiped by normal cluster load
  constexpr auto TTL = 3;
  {
    // add an ephemeral table, will not be wiped out
    auto tableSpec = nebula::meta::genTableSpec();
    tableSpec->name = "test2";
    tableSpec->ttl.stl = TTL;
    ci.addTable(tableSpec);
    EXPECT_EQ(ci.tables().size(), 5);
  }

  // load the cluster table again
  ci.load("configs/test.yml", createDB);
  EXPECT_EQ(ci.tables().size(), 5);

  // at last, let's print out all table names
  LOG(INFO) << "Active tables: ";
  for (auto& t : ci.tables()) {
    LOG(INFO) << t->name;
  }

  // sleep for the time so that
  Evidence::sleep((TTL + 1) * 1000);
  ci.load("configs/test.yml", createDB);
  EXPECT_EQ(ci.tables().size(), 4);
}

} // namespace test
} // namespace meta
} // namespace nebula
