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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include "api/dsl/Dsl.h"
#include "api/dsl/Expressions.h"
#include "common/Cursor.h"
#include "common/Errors.h"
#include "common/Evidence.h"
#include "common/Folly.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "execution/BlockManager.h"
#include "execution/ExecutionPlan.h"
#include "execution/core/ServerExecutor.h"
#include "execution/io/trends/Trends.h"
#include "execution/meta/TableService.h"
#include "fmt/format.h"
#include "gmock/gmock.h"
#include "meta/NBlock.h"
#include "meta/TestTable.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace api {
namespace test {

using namespace nebula::api::dsl;
using nebula::common::Cursor;
using nebula::common::Evidence;
using nebula::execution::BlockManager;
using nebula::execution::core::ServerExecutor;
using nebula::execution::io::trends::TrendsTable;
using nebula::execution::meta::TableService;
using nebula::meta::TestTable;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeSerializer;

TEST(ApiTest, TestReadSamples) {
  TrendsTable trends;
  auto ms = std::make_shared<TableService>();

  // query this table
  const auto upbound = std::numeric_limits<int64_t>::max();
  const auto query = table(trends.name(), ms)
                       .where(col("_time_") > 0 && col("_time_") < upbound && starts(col("query"), "winter"))
                       .select(
                         col("*"))
                       .limit(10);

  // compile the query into an execution plan
  auto plan = query.compile();

  // print out the plan through logging
  plan->setWindow({ 0, upbound });
  plan->display();

  // load test data to run this query
  trends.load(256);

  nebula::common::Evidence::Duration tick;
  // pass the query plan to a server to execute - usually it is itself
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << fmt::format("col: {0:20} | {1:12} | {2:12}", "Query", "Count", "Time");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:20} | {1:12} | {2:12}",
                             row.readString("query"),
                             row.readLong("count"),
                             row.readLong(nebula::meta::Table::TIME_COLUMN));
  }
}

TEST(ApiTest, TestDataFromCsv) {
  TrendsTable trends;
  auto ms = std::make_shared<TableService>();

  // query this table
  const auto upbound = std::numeric_limits<int64_t>::max();
  const auto query = table(trends.name(), ms)
                       .where(col("_time_") > 0 && col("_time_") < upbound && starts(col("query"), "winter"))
                       .select(
                         col("query"),
                         sum(col("count")).as("total"))
                       .groupby({ 1 });

  // compile the query into an execution plan
  auto plan = query.compile();

  // print out the plan through logging
  plan->setWindow({ 0, upbound });
  plan->display();

  // load test data to run this query
  trends.load(256);

  nebula::common::Evidence::Duration tick;
  // pass the query plan to a server to execute - usually it is itself
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << fmt::format("col: {0:20} | {1:12}", "Query", "Total");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:20} | {1:12}",
                             row.readString("query"),
                             row.readLong("total"));
  }
}

TEST(ApiTest, TestMatchedKeys) {
  TrendsTable trends;
  auto ms = std::make_shared<TableService>();

  // query this table
  const auto query = table(trends.name(), ms)
                       .where(like(col("query"), "leg work%"))
                       .select(
                         col("query"),
                         col("_time_"),
                         sum(col("count")).as("total"))
                       .groupby({ 1, 2 });

  // compile the query into an execution plan
  auto plan = query.compile();

  // print out the plan through logging
  plan->display();

  // load test data to run this query
  trends.load(1);

  nebula::common::Evidence::Duration tick;
  // pass the query plan to a server to execute - usually it is itself
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Query: select query, count(1) as total from trends.draft where query like '%work%' group by 1;";
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << fmt::format("col: {0:20} | {1:12} | {2:12}", "Query", "Date", "Total");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:20} | {1:12} | {2:12}",
                             row.readString("query"),
                             row.readLong("_time_"),
                             row.readInt("total"));
  }
}

TEST(ApiTest, TestMultipleBlocks) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  int64_t start = nebula::common::Evidence::time("2019-01-01", "%Y-%m-%d");
  int64_t end = Evidence::time("2019-05-01", "%Y-%m-%d");

  // let's plan these many data std::thread::hardware_concurrency()
  auto ms = std::make_shared<TableService>();
  nebula::meta::TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add({ testTable.name(), i++, begin, begin + window });
  }

  // query this table
  const auto query = table(testTable.name(), ms)
                       .where(col("_time_") > start && col("_time_") < end && like(col("event"), "NN%"))
                       .select(
                         col("event"),
                         count(col("value")).as("total"))
                       .groupby({ 1 })
                       .sortby({ 2 }, SortType::DESC)
                       .limit(10);

  // compile the query into an execution plan
  auto plan = query.compile();
  plan->setWindow({ start, end });

  // print out the plan through logging
  plan->display();

  nebula::common::Evidence::Duration tick;
  // pass the query plan to a server to execute - usually it is itself
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan);

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
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  int64_t start = nebula::common::Evidence::time("2019-01-01", "%Y-%m-%d");
  int64_t end = Evidence::time("2019-05-01", "%Y-%m-%d");

  // let's plan these many data std::thread::hardware_concurrency()
  auto ms = std::make_shared<TableService>();
  nebula::meta::TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add({ testTable.name(), i++, begin, begin + window });
  }

  // query this table
  const auto query = table(testTable.name(), ms)
                       .where(col("_time_") > start && col("_time_") < end && col("event") == "")
                       .select(
                         col("event"),
                         count(col("value")).as("total"))
                       .groupby({ 1 })
                       .sortby({ 2 }, SortType::DESC)
                       .limit(10);

  // compile the query into an execution plan
  auto plan = query.compile();
  plan->setWindow({ start, end });

  // print out the plan through logging
  plan->display();

  nebula::common::Evidence::Duration tick;
  // pass the query plan to a server to execute - usually it is itself
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan);

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
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  int64_t start = nebula::common::Evidence::time("2019-01-01", "%Y-%m-%d");
  int64_t end = Evidence::time("2019-05-01", "%Y-%m-%d");

  // let's plan these many data std::thread::hardware_concurrency()
  auto ms = std::make_shared<TableService>();
  nebula::meta::TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add({ testTable.name(), i++, begin, begin + window });
  }

  // we know we don't have an id larger than this number, so the query should skip all blocks
  const auto query = table(testTable.name(), ms)
                       .where(col("_time_") > start && col("_time_") < end && col("id") == 8989)
                       .select(
                         col("event"),
                         count(col("value")).as("total"))
                       .groupby({ 1 })
                       .sortby({ 2 }, SortType::DESC)
                       .limit(10);

  // compile the query into an execution plan
  auto plan = query.compile();
  plan->setWindow({ start, end });

  // print out the plan through logging
  plan->display();

  // no block will be picked for this query
  auto blocks = bm->query(testTable, *plan);
  EXPECT_EQ(blocks.size(), 1);

  nebula::common::Evidence::Duration tick;
  // pass the query plan to a server to execute - usually it is itself
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan);

  // query should have results
  EXPECT_EQ(result->size(), 1);
}

} // namespace test
} // namespace api
} // namespace nebula
