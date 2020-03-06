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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "Test.hpp"

#include "api/dsl/Dsl.h"
#include "api/dsl/Expressions.h"
#include "common/Cursor.h"
#include "common/Errors.h"
#include "common/Evidence.h"
#include "common/Folly.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "execution/ExecutionPlan.h"
#include "execution/core/ServerExecutor.h"
#include "execution/meta/TableService.h"
#include "meta/NBlock.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace api {
namespace test {

using namespace nebula::api::dsl;
using nebula::common::Cursor;
using nebula::common::Evidence;
using nebula::execution::core::ServerExecutor;
using nebula::execution::meta::TableService;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeSerializer;

TEST(ApiTest, TestMultipleBlocks) {
  auto data = genData();

  // query this table
  auto ms = TableService::singleton();
  auto tableName = std::get<0>(data);
  auto start = std::get<1>(data);
  auto end = std::get<2>(data);
  auto query = table(tableName, ms)
                 .where(col("_time_") > start && col("_time_") < end && like(col("event"), "NN%"))
                 .select(
                   col("event"),
                   count(col("value")).as("total"))
                 .groupby({ 1 })
                 .sortby({ 2 }, SortType::DESC)
                 .limit(10);

  // compile the query into an execution plan
  QueryContext ctx{ "nebula", { "nebula-users" } };
  auto plan = query.compile(ctx);
  plan->setWindow({ start, end });

  // print out the plan through logging
  plan->display();

  nebula::common::Evidence::Duration tick;
  folly::CPUThreadPoolExecutor pool{ 8 };
  // pass the query plan to a server to execute - usually it is itself
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(pool, *plan);

  // query should have results
  EXPECT_GT(result->size(), 0);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << fmt::format("col: {0:20} | {1:12}", "event", "Total");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:20} | {1:12}",
                             row.readString("event"),
                             row.readInt("total"));
  }
}

TEST(ApiTest, TestStringEqEmpty) {
  auto data = genData();

  // query this table
  auto ms = TableService::singleton();
  auto tableName = std::get<0>(data);
  auto start = std::get<1>(data);
  auto end = std::get<2>(data);
  auto query = table(tableName, ms)
                 .where(col("_time_") > start && col("_time_") < end && col("event") == "")
                 .select(
                   col("event"),
                   count(col("value")).as("total"))
                 .groupby({ 1 })
                 .sortby({ 2 }, SortType::DESC)
                 .limit(10);

  // compile the query into an execution plan
  QueryContext ctx{ "nebula", { "nebula-users" } };
  auto plan = query.compile(ctx);
  plan->setWindow({ start, end });

  // print out the plan through logging
  plan->display();

  nebula::common::Evidence::Duration tick;
  // pass the query plan to a server to execute - usually it is itself
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(pool, *plan);

  // query should have results
  EXPECT_EQ(result->size(), 1);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << fmt::format("col: {0:20} | {1:12}", "event", "Total");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:20} | {1:12}",
                             row.readString("event"),
                             row.readInt("total"));
  }
}

TEST(ApiTest, TestBlockSkipByBloomfilter) {
  auto data = genData();

  // we know we don't have an id larger than this number, so the query should skip all blocks
  auto ms = TableService::singleton();
  auto tableName = std::get<0>(data);
  auto start = std::get<1>(data);
  auto end = std::get<2>(data);
  auto query = table(tableName, ms)
                 .where(col("_time_") > start && col("_time_") < end && col("id") == 8989)
                 .select(
                   col("event"),
                   count(col("value")).as("total"))
                 .groupby({ 1 })
                 .sortby({ 2 }, SortType::DESC)
                 .limit(10);

  // compile the query into an execution plan
  QueryContext ctx{ "nebula", { "nebula-users" } };
  auto plan = query.compile(ctx);
  plan->setWindow({ start, end });

  // print out the plan through logging
  plan->display();

  nebula::common::Evidence::Duration tick;
  // pass the query plan to a server to execute - usually it is itself
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(pool, *plan);

  // query should have results
  EXPECT_EQ(result->size(), 1);
}

TEST(ApiTest, TestAvgAggregation) {
  auto data = genData();

  // we know we don't have an id larger than this number, so the query should skip all blocks
  auto ms = TableService::singleton();
  auto tableName = std::get<0>(data);
  auto start = std::get<1>(data);
  auto end = std::get<2>(data);
  auto query = table(tableName, ms)
                 .where(col("_time_") > start && col("_time_") < end)
                 .select(
                   col("event"),
                   avg(col("value")).as("avg"),
                   sum(col("value")).as("sum"),
                   sum(col("stamp")).as("sint128"),
                   count(1).as("count"),
                   avg(col("weight")).as("davg"))
                 .groupby({ 1 })
                 .sortby({ 2 }, SortType::DESC)
                 .limit(10);

  // compile the query into an execution plan
  QueryContext ctx{ "nebula", { "nebula-users" } };
  auto plan = query.compile(ctx);
  plan->setWindow({ start, end });

  // print out the plan through logging
  plan->display();

  nebula::common::Evidence::Duration tick;
  // pass the query plan to a server to execute - usually it is itself
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(pool, *plan);

  // query should have results
  EXPECT_EQ(result->size(), 10);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << fmt::format("col: {0:20} | {1:12} | {2:20} | {3:12} | {4:20} | {5:12}", "event", "v.avg", "w.avg", "v.sum", "s.sum", "v.count");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:20} | {1:12} | {2:20} | {3:12} | {4:20} | {5:12}",
                             row.readString("event"),
                             row.readByte("avg"),
                             row.readDouble("davg"),
                             row.readLong("sum"),
                             row.readLong("sint128"),
                             row.readLong("count"));
  }
}

TEST(ApiTest, TestPercentile) {
  auto data = genData();

  // query this table
  auto ms = TableService::singleton();
  auto tableName = std::get<0>(data);
  auto start = std::get<1>(data);
  auto end = std::get<2>(data);
  auto query = table(tableName, ms)
                 .where(col("_time_") > start && col("_time_") < end)
                 .select(
                   col("event"),
                   pct(col("value"), 50).as("p50"),
                   pct(col("value"), 90).as("p90"),
                   pct(col("value"), 99).as("p99"))
                 .groupby({ 1 })
                 .sortby({ 2 }, SortType::DESC)
                 .limit(10);

  // compile the query into an execution plan
  QueryContext ctx{ "nebula", { "nebula-users" } };
  auto plan = query.compile(ctx);
  plan->setWindow({ start, end });

  // print out the plan through logging
  plan->display();

  nebula::common::Evidence::Duration tick;
  // pass the query plan to a server to execute - usually it is itself
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(pool, *plan);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << fmt::format("col: {0:20} | {1:12} | {2:12} | {3:12}", "event", "v.p50", "v.p90", "v.p99");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:20} | {1:12} | {2:12} | {3:12}",
                             row.readString("event"),
                             row.readByte("p50"),
                             row.readByte("p90"),
                             row.readByte("p99"));
  }
}

TEST(ApiTest, TestAccessControl) {
  auto data = genData();

  // we know we don't have an id larger than this number, so the query should skip all blocks
  auto ms = TableService::singleton();
  auto tableName = std::get<0>(data);
  auto start = std::get<1>(data);
  auto end = std::get<2>(data);
  auto query = table(tableName, ms)
                 .where(col("_time_") > start && col("_time_") < end)
                 .select(
                   col("event"),
                   col("value"),
                   col("weight"))
                 .limit(10);

  // compile the query into an execution plan
  // test table require nebula-users to read event column, refer TestTable.h
  QueryContext ctx{ "nebula", { "nebula-guest" } };
  auto plan = query.compile(ctx);
  plan->setWindow({ start, end });

  // print out the plan through logging
  plan->display();

  nebula::common::Evidence::Duration tick;
  // pass the query plan to a server to execute - usually it is itself
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(pool, *plan);

  // query should have results
  EXPECT_EQ(result->size(), 10);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << fmt::format("col: {0:20} | {1:12} | {2:12}", "event", "value", "weight");
  while (result->hasNext()) {
    const auto& row = result->next();

    // expect event to be masked
    auto event = row.readString("event");
    EXPECT_EQ(event, "***");
    LOG(INFO) << fmt::format("row: {0:20} | {1:12} | {2:12}",
                             event,
                             row.readByte("value"),
                             row.readDouble("weight"));
  }
}

} // namespace test
} // namespace api
} // namespace nebula
