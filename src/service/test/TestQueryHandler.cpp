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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/Folly.h"
#include "execution/core/NodeConnector.h"
#include "execution/core/ServerExecutor.h"
#include "execution/io/trends/Trends.h"
#include "execution/meta/TableService.h"
#include "fmt/format.h"
#include "meta/NBlock.h"
#include "meta/TestTable.h"
#include "service/nebula/NebulaService.h"
#include "service/nebula/QueryHandler.h"
#include "service/node/RemoteNodeConnector.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace service {
namespace test {

using namespace nebula::api::dsl;
using nebula::common::Cursor;
using nebula::common::Evidence;
using nebula::execution::BlockManager;
using nebula::execution::core::NodeConnector;
using nebula::execution::core::ServerExecutor;
using nebula::execution::io::trends::TrendsTable;
using nebula::execution::meta::TableService;
using nebula::meta::NBlock;
using nebula::meta::TestTable;
using nebula::service::ErrorCode;
using nebula::surface::RowCursorPtr;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeSerializer;

TEST(ServiceTest, TestQueryHandler) {
  TrendsTable trends;

  // load test data to run this query
  trends.load(10);

  nebula::common::Evidence::Duration tick;
  QueryHandler handler;
  QueryRequest request;
  request.set_table(trends.name());
  request.set_start(Evidence::time("2019-02-01", "%Y-%m-%d"));
  request.set_end(Evidence::time("2019-05-01", "%Y-%m-%d"));
  auto pa = request.mutable_filtera();
  {
    auto expr = pa->add_expression();
    expr->set_column("query");
    expr->set_op(Operation::LIKE);
    expr->add_value("lego%");
  }

  request.add_dimension("query");
  request.add_dimension("_time_");
  auto metric = request.add_metric();
  metric->set_column("count");
  metric->set_method(Rollup::SUM);

  ErrorCode err = ErrorCode::NONE;

  // No error in compiling the query
  auto query = handler.build(trends, request, err);
  auto plan = handler.compile(query, { request.start(), request.end() }, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  // No error in exeucting the query
  auto connector = std::make_shared<NodeConnector>();
  auto result = handler.query(*plan, connector, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Query: select query, count(1) as total from trends.draft where query like '%work%' group by 1;";
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << fmt::format("col: {0:40} | {1:12} | {2:12}", "Query", "Date", "Total");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:40} | {1:12} | {2:12}",
                             row.readString("query"),
                             row.readLong("_time_"),
                             row.readInt("count.sum"));
  }
}

TEST(ServiceTest, TestJsonifyResults) {
  TrendsTable trends;

  // load test data to run this query
  trends.load(4);

  nebula::common::Evidence::Duration tick;
  QueryHandler handler;
  QueryRequest request;
  request.set_table(trends.name());
  request.set_start(Evidence::time("2017-02-01", "%Y-%m-%d"));
  request.set_end(Evidence::time("2019-05-01", "%Y-%m-%d"));
  auto pa = request.mutable_filtera();
  // COUNT needs to be more than 2
  {
    auto expr = pa->add_expression();
    expr->set_column("count");
    expr->set_op(Operation::MORE);
    expr->add_value("2");
  }
  // AND query like "lego %"
  {
    auto expr = pa->add_expression();
    expr->set_column("query");
    expr->set_op(Operation::LIKE);
    expr->add_value("apple%");
  }

  request.add_dimension("query");
  request.add_dimension("_time_");
  auto metric = request.add_metric();
  metric->set_column("count");
  metric->set_method(Rollup::SUM);

  ErrorCode err = ErrorCode::NONE;

  // No error in compiling the query
  auto query = handler.build(trends, request, err);
  auto plan = handler.compile(query, { request.start(), request.end() }, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  // No error in exeucting the query
  auto connector = std::make_shared<NodeConnector>();
  auto result = handler.query(*plan, connector, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  LOG(INFO) << "Execute the query and jsonify results: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << ServiceProperties::jsonify(result, plan->getOutputSchema());
}

TEST(ServiceTest, TestQueryTimeline) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  auto start = Evidence::time("2019-01-01", "%Y-%m-%d");
  auto end = Evidence::time("2019-05-01", "%Y-%m-%d");

  // let's plan these many data std::thread::hardware_concurrency()
  nebula::meta::TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add({ testTable.name(), i++, begin, begin + window });
  }

  // load test data to run this query
  nebula::common::Evidence::Duration tick;
  QueryHandler handler;
  QueryRequest request;
  request.set_table(testTable.name());
  request.set_start(Evidence::time("2019-02-01", "%Y-%m-%d"));
  request.set_end(Evidence::time("2019-05-01", "%Y-%m-%d"));
  auto pa = request.mutable_filtera();
  // COUNT needs to be more than 2
  {
    auto expr = pa->add_expression();
    expr->set_column("event");
    expr->set_op(Operation::LIKE);
    expr->add_value("NN%");
  }

  // set the query purpose as timeline
  request.set_display(DisplayType::TIMELINE);
  request.set_window(7 * 24 * 3600);
  auto metric = request.add_metric();
  metric->set_column("value");
  metric->set_method(Rollup::COUNT);

  ErrorCode err = ErrorCode::NONE;

  // No error in compiling the query
  auto query = handler.build(testTable, request, err);
  auto plan = handler.compile(query, { request.start(), request.end() }, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  // No error in exeucting the query
  auto connector = std::make_shared<NodeConnector>();
  auto result = handler.query(*plan, connector, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  LOG(INFO) << "Execute the query and jsonify results: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << ServiceProperties::jsonify(result, plan->getOutputSchema());
}

TEST(ServiceTest, TestStringFilters) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  auto start = Evidence::time("2019-01-01", "%Y-%m-%d");
  auto end = Evidence::time("2019-05-01", "%Y-%m-%d");

  // let's plan these many data std::thread::hardware_concurrency()
  nebula::meta::TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add({ testTable.name(), i++, begin, begin + window });
  }

  // load test data to run this query
  nebula::common::Evidence::Duration tick;
  QueryHandler handler;
  QueryRequest request;
  request.set_table(testTable.name());
  request.set_start(Evidence::time("2019-02-01", "%Y-%m-%d"));
  request.set_end(Evidence::time("2019-05-01", "%Y-%m-%d"));
  auto pa = request.mutable_filtera();
  // COUNT needs to be more than 2
  {
    auto expr = pa->add_expression();
    expr->set_column("event");
    expr->set_op(Operation::EQ);
    expr->add_value("");
  }

  // set the query purpose as timeline
  request.set_display(DisplayType::TABLE);
  auto metric = request.add_metric();
  metric->set_column("value");
  metric->set_method(Rollup::COUNT);

  ErrorCode err = ErrorCode::NONE;

  // No error in compiling the query
  auto query = handler.build(testTable, request, err);
  auto plan = handler.compile(query, { request.start(), request.end() }, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  // No error in exeucting the query
  auto connector = std::make_shared<NodeConnector>();
  auto result = handler.query(*plan, connector, err);

  // No error in exeucting the query
  EXPECT_EQ(err, ErrorCode::NONE);

  LOG(INFO) << "Execute the query and jsonify results: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << ServiceProperties::jsonify(result, plan->getOutputSchema());
}

TEST(ServiceTest, TestQuerySamples) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  auto start = Evidence::time("2019-01-01", "%Y-%m-%d");
  auto end = Evidence::time("2019-05-01", "%Y-%m-%d");

  // let's plan these many data std::thread::hardware_concurrency()
  nebula::meta::TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add({ testTable.name(), i++, begin, begin + window });
  }

  // load test data to run this query
  nebula::common::Evidence::Duration tick;
  QueryHandler handler;
  QueryRequest request;
  request.set_table(testTable.name());
  request.set_start(Evidence::time("2019-02-01", "%Y-%m-%d"));
  request.set_end(Evidence::time("2019-05-01", "%Y-%m-%d"));
  auto pa = request.mutable_filtera();
  // COUNT needs to be more than 2
  {
    auto expr = pa->add_expression();
    expr->set_column("event");
    expr->set_op(Operation::EQ);
    expr->add_value("NN");
    expr->add_value("NNN");
  }

  // set the query purpose as timeline
  request.set_display(DisplayType::TABLE);
  request.add_dimension("id");
  request.add_dimension("event");
  request.add_dimension("flag");
  request.add_dimension("value");

  ErrorCode err = ErrorCode::NONE;

  // No error in compiling the query
  auto query = handler.build(testTable, request, err);
  auto plan = handler.compile(query, { request.start(), request.end() }, err);
  plan->display();
  EXPECT_EQ(err, ErrorCode::NONE);

  // No error in exeucting the query
  auto connector = std::make_shared<NodeConnector>();
  auto result = handler.query(*plan, connector, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  LOG(INFO) << "Execute the query and jsonify results: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << ServiceProperties::jsonify(result, plan->getOutputSchema());
}

class MockRow : public nebula::surface::MockRowData {
public:
  MockRow() = default;
  MOCK_CONST_METHOD1(readLong, int64_t(const std::string&));
  MOCK_CONST_METHOD1(readInt, int32_t(const std::string&));
  MOCK_CONST_METHOD1(isNull, bool(const std::string&));
};

class MockCursor : public Cursor<RowData> {
public:
  MockCursor(const MockRow& row) : Cursor<RowData>(1), row_{ row } {}
  virtual const RowData& next() override {
    index_++;
    return row_;
  }

  virtual std::unique_ptr<RowData> item(size_t) const override {
    return {};
  }

private:
  const MockRow& row_;
};

TEST(ServiceTest, TestJsonLib) {
  // set up table for testing
  MockRow rowData;

  EXPECT_CALL(rowData, readLong("id")).WillRepeatedly(testing::Return(678776975068960826));
  EXPECT_CALL(rowData, readInt("value")).WillRepeatedly(testing::Return(320));

  auto cursor = std::make_shared<MockCursor>(rowData);
  auto json = ServiceProperties::jsonify(std::static_pointer_cast<Cursor<RowData>>(cursor), TypeSerializer::from("ROW<id:bigint, value:int>"));

  // encoding long value in string
  EXPECT_EQ(json, "[{\"id\":\"678776975068960826\",\"value\":320}]");
}

TEST(ServiceTest, TestQuerySerde) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  auto start = Evidence::time("2019-01-01", "%Y-%m-%d");
  auto end = Evidence::time("2019-05-01", "%Y-%m-%d");

  // let's plan these many data std::thread::hardware_concurrency()
  auto ms = std::make_shared<TableService>();
  nebula::meta::TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add({ testTable.name(), i++, begin, begin + window });
  }

  const auto query = table(testTable.name(), ms)
                       .where(like(col("event"), "NN%"))
                       .select(
                         col("event"),
                         col("flag"),
                         max(col("id") * 2).as("max_id"),
                         min(col("id") + 1).as("min_id"),
                         count(1).as("count"))
                       .groupby({ 1, 2 })
                       .sortby({ 5 }, SortType::DESC)
                       .limit(10);
  auto plan1 = query.compile();
  plan1->setWindow({ start, end });

  // pass the query plan to a server to execute - usually it is itself
  auto result1 = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan1);
  auto str1 = ServiceProperties::jsonify(result1, plan1->getOutputSchema());

  // serialize this query
  nebula::common::Evidence::Duration tick;
  auto ser = QuerySerde::serialize(query, plan1->id(), { start, end });
  auto plan2 = QuerySerde::from(ms, &ser);

  LOG(INFO) << "Query serde time (ms): " << tick.elapsedMs();

  // pass the query plan to a server to execute - usually it is itself
  auto result2 = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan2);
  auto str2 = ServiceProperties::jsonify(result2, plan2->getOutputSchema());

  // check these two plans are the same
  EXPECT_EQ(nebula::type::TypeSerializer::to(plan1->getOutputSchema()),
            nebula::type::TypeSerializer::to(plan2->getOutputSchema()));

  // JSON results of two queries are the same
  EXPECT_EQ(str1, str2);
  LOG(INFO) << "result is " << str1;
}

TEST(ServiceTest, TestDataSerde) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  auto start = Evidence::time("2019-01-01", "%Y-%m-%d");
  auto end = Evidence::time("2019-05-01", "%Y-%m-%d");

  // let's plan these many data std::thread::hardware_concurrency()
  auto ms = std::make_shared<TableService>();
  nebula::meta::TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add({ testTable.name(), i++, begin, begin + window });
  }

  const auto query = table(testTable.name(), ms)
                       .where(like(col("event"), "NN%"))
                       .select(
                         col("event"),
                         col("flag"),
                         max(col("id") * 2).as("max_id"),
                         min(col("id") + 1).as("min_id"),
                         count(1).as("count"))
                       .groupby({ 1, 2 })
                       .sortby({ 5 }, SortType::DESC)
                       .limit(10);
  auto plan1 = query.compile();
  plan1->setWindow({ start, end });

  // pass the query plan to a server to execute - usually it is itself

  // get result with default in proc connector
  nebula::common::Evidence::Duration tick;
  auto result1 = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan1);
  LOG(INFO) << "Query time with in-proc connector (ms): " << tick.elapsedMs();
  auto str1 = ServiceProperties::jsonify(result1, plan1->getOutputSchema());

  // get result with cross-boundary connector
  // auto connector = std::make_shared<RemoteNodeConnector>();
  // tick.reset();
  // auto result2 = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan1, connector);
  // LOG(INFO) << "Query time with remote connector (ms): " << tick.elapsedMs();
  // auto str2 = ServiceProperties::jsonify(result2, plan1->getOutputSchema());

  // // JSON results of two queries are the same
  // EXPECT_EQ(str1, str2);
  LOG(INFO) << "result is " << str1;
}

} // namespace test
} // namespace service
} // namespace nebula

/*
 * This is the recommended main function for all tests.
 * The Makefile links it into all of the test programs so that tests do not need
 * to - and indeed should typically not - define their own main() functions
 */
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv);
  FLAGS_logtostderr = 1;

  return RUN_ALL_TESTS();
}
