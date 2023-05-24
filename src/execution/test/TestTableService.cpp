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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/Evidence.h"
#include "execution/meta/TableService.h"
#include "meta/TestUtils.h"

namespace nebula {
namespace execution {
namespace test {

using nebula::common::Evidence;
using nebula::execution::meta::TableService;
using nebula::meta::DataSpec;
using nebula::meta::NNode;
using nebula::meta::NRole;
using nebula::meta::SpecPtr;
using nebula::meta::SpecSplit;
using nebula::meta::SpecSplitPtr;
using nebula::meta::SpecState;
using nebula::meta::TTL;

TEST(TableServiceTest, TestTableService) {
  auto& ts = TableService::singleton();

  // non-existing table registry
  auto& tb = ts->query("non-existing");
  EXPECT_TRUE(tb.empty());

  // auto-register when using get method
  auto tableSpec = nebula::meta::genTableSpec();
  auto tr1 = ts->get(tableSpec);
  EXPECT_FALSE(tr1->empty());
  EXPECT_FALSE(tr1->expired());
  EXPECT_EQ(tr1->table()->name(), "test");

  // enroll a ephemeral table
  auto tableSpec2 = nebula::meta::genTableSpec("test2");
  tableSpec2->ttl = TTL{ 1 };
  EXPECT_EQ(tableSpec2->ttl.stl, 1);
  auto tr2 = ts->get(tableSpec2);
  EXPECT_FALSE(tr2->empty());
  EXPECT_FALSE(tr2->expired());

  // till now, we have two tables and one empty
  auto list = ts->all();
  EXPECT_EQ(list.size(), 3);

  // put some specs into each table
  const NNode node{ NRole::NODE, "localhost", 9190 };
  std::vector<SpecSplitPtr> splits{ std::make_shared<SpecSplit>("s3://test/a.csv", 10, 1) };
  std::vector<SpecPtr> spec1 = { std::make_shared<DataSpec>(tableSpec, "1.0", "s3://test", splits, SpecState::NEW) };
  std::vector<SpecPtr> spec2 = { std::make_shared<DataSpec>(tableSpec2, "1.0", "s3://test", splits, SpecState::NEW) };

  // numSpecs will return all specs in track regardless they are online or not
  {
    tr1->update("v1", spec1);
    tr2->update("v1", spec2);
    EXPECT_EQ(tr1->numSpecs(), 1);
    EXPECT_EQ(tr2->numSpecs(), 1);
  }

  // only online spec with affinity is declared as online
  {
    spec2.at(0)->affinity(node);
    auto specId1 = spec1.at(0)->id();
    auto specId2 = spec2.at(0)->id();
    EXPECT_FALSE(tr1->online(specId1));
    EXPECT_TRUE(tr2->online(specId2));
  }

  // the ephemeral table expried - any update will remove all its spec
  {
    Evidence::sleep(2000);
    tr1->update("v1", spec1);
    tr2->update("v1", spec2);
    EXPECT_EQ(tr1->numSpecs(), 1);
    EXPECT_EQ(tr2->numSpecs(), 0);
  }
}

} // namespace test
} // namespace execution
} // namespace nebula