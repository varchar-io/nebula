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

#include "gtest/gtest.h"
#include <glog/logging.h>
#include <sys/mman.h>
#include "MockTable.h"
#include "api/dsl/Dsl.h"
#include "api/dsl/Expressions.h"
#include "common/Cursor.h"
#include "common/Errors.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "execution/BlockManager.h"
#include "execution/ExecutionPlan.h"
#include "execution/core/ServerExecutor.h"
#include "fmt/format.h"
#include "gmock/gmock.h"
#include "meta/NBlock.h"
#include "meta/Table.h"
#include "surface/DataSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace api {
namespace test {

using namespace nebula::api::dsl;
using nebula::common::Cursor;
using nebula::execution::BlockManager;
using nebula::execution::core::ServerExecutor;
using nebula::meta::NBlock;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeSerializer;

TEST(ApiTest, TestDataFromCsv) {
  auto tbl = "trends.draft";
  auto ms = std::make_shared<MockMs>();

  // query this table
  const auto query = table(tbl, ms)
                       .where(col("query") == "yoga")
                       .select(
                         col("dt"),
                         sum(col("count")).as("total"))
                       .groupby({ 1 });

  // compile the query into an execution plan
  auto plan = query.compile();

  // print out the plan through logging
  plan->display();

  // load test data to run this query
  auto bm = BlockManager::init();
  auto ptable = ms->query(tbl);

  // ensure block 0 of the test table (load from storage if not in memory)
  NBlock block(*ptable, 0);
  bm->add(block);

  auto tick = nebula::common::Evidence::ticks();
  // pass the query plan to a server to execute - usually it is itself
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Query: select dt, sum(count) as total from trends.draft where query='yoga' group by 1;";
  auto duration = (nebula::common::Evidence::ticks() - tick) / 1000;
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << duration << " ms";
  LOG(INFO) << fmt::format("col: {0:12} | {1:12}", "Date", "Total");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:50} | {1:12}",
                             row.readString("dt"),
                             row.readInt("total"));
  }
}

TEST(ApiTest, TestMatchedKeys) {
  auto tbl = "trends.draft";
  auto ms = std::make_shared<MockMs>();

  // query this table
  const auto query = table(tbl, ms)
                       .where(like(col("query"), "leg work%"))
                       .select(
                         col("query"),
                         col("dt"),
                         sum(col("count")).as("total"))
                       .groupby({ 1, 2 });

  // compile the query into an execution plan
  auto plan = query.compile();

  // print out the plan through logging
  plan->display();

  // load test data to run this query
  auto bm = BlockManager::init();
  auto ptable = ms->query(tbl);

  // ensure block 0 of the test table (load from storage if not in memory)
  NBlock block(*ptable, 0);
  bm->add(block);

  auto tick = nebula::common::Evidence::ticks();
  // pass the query plan to a server to execute - usually it is itself
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Query: select query, count(1) as total from trends.draft where query like '%work%' group by 1;";
  auto duration = (nebula::common::Evidence::ticks() - tick) / 1000;
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << duration << " ms";
  LOG(INFO) << fmt::format("col: {0:20} | {1:12} | {2:12}", "Query", "Date", "Total");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:20} | {1:12} | {2:12}",
                             row.readString("query"),
                             row.readString("dt"),
                             row.readInt("total"));
  }
}

TEST(ApiTest, TestMultipleBlocks) {
  auto tbl = "trends.draft";
  auto ms = std::make_shared<MockMs>();

  // query this table
  const auto query = table(tbl, ms)
                       .where(like(col("query"), "leg work%"))
                       .select(
                         col("query"),
                         sum(col("count")).as("total"))
                       .groupby({ 1 });

  // compile the query into an execution plan
  auto plan = query.compile();

  // print out the plan through logging
  plan->display();

  // load test data to run this query
  auto bm = BlockManager::init();
  auto ptable = ms->query(tbl);

  // add 5 blocks numbered from 0 to 4 inclusively
  for (auto i = 0; i < 5; ++i) {
    NBlock block(*ptable, i);
    bm->add(block);
  }

  auto tick = nebula::common::Evidence::ticks();
  // pass the query plan to a server to execute - usually it is itself
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Query: select query, count(1) as total from trends.draft where query like '%leg work%' group by 1;";
  auto duration = (nebula::common::Evidence::ticks() - tick) / 1000;
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << duration << " ms";
  LOG(INFO) << fmt::format("col: {0:20} | {1:12}", "Query", "Total");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:20} | {1:12}",
                             row.readString("query"),
                             row.readInt("total"));
  }
}

} // namespace test
} // namespace api
} // namespace nebula