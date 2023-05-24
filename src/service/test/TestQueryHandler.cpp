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

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "api/test/Test.hpp"
#include "common/Folly.h"
#include "execution/core/NodeConnector.h"
#include "execution/core/ServerExecutor.h"
#include "execution/meta/TableService.h"
#include "fmt/format.h"
#include "meta/NBlock.h"
#include "meta/TestTable.h"
#include "service/base/NebulaService.h"
#include "service/node/RemoteNodeConnector.h"
#include "service/server/QueryHandler.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace service {
namespace test {

using namespace nebula::api::dsl;
using nebula::common::Cursor;
using nebula::common::Evidence;
using nebula::execution::QueryContext;
using nebula::execution::core::NodeConnector;
using nebula::execution::core::ServerExecutor;
using nebula::execution::meta::TableService;
using nebula::meta::NBlock;
using nebula::meta::TestTable;
using nebula::service::base::ErrorCode;
using nebula::service::base::QuerySerde;
using nebula::service::base::ServiceProperties;
using nebula::service::server::QueryHandler;
using nebula::surface::RowCursorPtr;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeSerializer;

TEST(ServiceTest, TestQueryTimeline) {
  auto data = nebula::api::test::genData();

  // query this table
  auto ms = TableService::singleton();
  auto tableName = std::get<0>(data);
  auto start = std::get<1>(data);
  auto end = std::get<2>(data);

  // load test data to run this query
  nebula::common::Evidence::Duration tick;
  QueryHandler handler;
  QueryRequest request;
  TestTable testTable;
  request.set_table(tableName);
  request.set_start(start);
  request.set_end(end);
  request.set_time_unit(1);
  auto pa = request.mutable_filtera();
  // COUNT needs to be more than 2
  {
    auto expr = pa->add_expression();
    expr->set_column("event");
    expr->set_op(Operation::LIKE);
    expr->add_value("NN%");
  }

  // set the query purpose as timeline
  request.set_timeline(true);
  request.set_window(7 * 24 * 3600);
  auto metric = request.add_metric();
  metric->set_column("value");
  metric->set_method(Rollup::COUNT);

  ErrorCode err = ErrorCode::NONE;

  // No error in compiling the query
  auto query = handler.build(testTable, request, err);
  auto plan = handler.compile(query,
                              "v1",
                              { request.start(), request.end() },
                              QueryContext::def(),
                              err);
  EXPECT_EQ(err, ErrorCode::NONE);

  // No error in exeucting the query
  auto connector = std::make_shared<NodeConnector>();
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result = handler.query(pool, plan, connector, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  LOG(INFO) << "Execute the query and jsonify results: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << ServiceProperties::jsonify(result, plan->getOutputSchema());
}

TEST(ServiceTest, TestStringFilters) {
  auto data = nebula::api::test::genData();

  // query this table
  auto ms = TableService::singleton();
  auto tableName = std::get<0>(data);
  auto start = std::get<1>(data);
  auto end = std::get<2>(data);

  // let's plan these many data std::thread::hardware_concurrency()
  nebula::meta::TestTable testTable;
  // load test data to run this query
  nebula::common::Evidence::Duration tick;
  QueryHandler handler;
  QueryRequest request;
  request.set_table(testTable.name());
  request.set_start(start);
  request.set_end(end);
  auto pa = request.mutable_filtera();
  // COUNT needs to be more than 2
  {
    auto expr = pa->add_expression();
    expr->set_column("event");
    expr->set_op(Operation::EQ);
    expr->add_value("");
  }

  // set the query purpose as timeline
  auto metric = request.add_metric();
  metric->set_column("value");
  metric->set_method(Rollup::COUNT);

  ErrorCode err = ErrorCode::NONE;

  // No error in compiling the query
  auto query = handler.build(testTable, request, err);
  auto plan = handler.compile(query,
                              "v1",
                              { request.start(), request.end() },
                              QueryContext::def(),
                              err);
  EXPECT_EQ(err, ErrorCode::NONE);

  // No error in exeucting the query
  auto connector = std::make_shared<NodeConnector>();
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result = handler.query(pool, plan, connector, err);

  // No error in exeucting the query
  EXPECT_EQ(err, ErrorCode::NONE);

  LOG(INFO) << "Execute the query and jsonify results: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << ServiceProperties::jsonify(result, plan->getOutputSchema());
}

TEST(ServiceTest, TestQuerySamples) {
  auto data = nebula::api::test::genData();

  // query this table
  auto ms = TableService::singleton();
  auto tableName = std::get<0>(data);
  auto start = std::get<1>(data);
  auto end = std::get<2>(data);

  // let's plan these many data std::thread::hardware_concurrency()
  nebula::meta::TestTable testTable;
  // load test data to run this query
  nebula::common::Evidence::Duration tick;
  QueryHandler handler;
  QueryRequest request;
  request.set_table(testTable.name());
  request.set_start(start);
  request.set_end(end);
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
  request.add_dimension("id");
  request.add_dimension("event");
  request.add_dimension("flag");
  request.add_dimension("value");

  ErrorCode err = ErrorCode::NONE;

  // No error in compiling the query
  auto query = handler.build(testTable, request, err);
  auto plan = handler.compile(query,
                              "v1",
                              { request.start(), request.end() },
                              QueryContext::def(),
                              err);
  plan->display();
  EXPECT_EQ(err, ErrorCode::NONE);

  // No error in exeucting the query
  auto connector = std::make_shared<NodeConnector>();
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result = handler.query(pool, plan, connector, err);
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
  auto data = nebula::api::test::genData();

  // query this table
  auto ms = TableService::singleton();
  auto tableName = std::get<0>(data);
  auto start = std::get<1>(data);
  auto end = std::get<2>(data);

  // let's plan these many data std::thread::hardware_concurrency()
  nebula::meta::TestTable testTable;

  auto query = table(testTable.name(), ms)
                 .where(like(col("event"), "NN%"))
                 .select(
                   col("event"),
                   col("flag"),
                   max(col("id") * 2).as("max_id"),
                   min(col("id") + 1).as("min_id"),
                   count(1).as("count"),
                   avg(col("weight")).as("avg_weight"))
                 .groupby({ 1, 2 })
                 .sortby({ 5 }, SortType::DESC)
                 .limit(10);
  auto plan1 = query.compile(QueryContext::def());
  plan1->setWindow({ start, end });
  plan1->setTableVersion("v1");

  // pass the query plan to a server to execute - usually it is itself
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result1 = ServerExecutor(nebula::meta::NNode::local().toString()).execute(pool, plan1);
  auto str1 = ServiceProperties::jsonify(result1, plan1->getOutputSchema());

  // serialize this query
  nebula::common::Evidence::Duration tick;
  auto ser = QuerySerde::serialize(query, plan1->id(), { start, end }, "v1");
  auto q = QuerySerde::deserialize(ms, &ser);
  auto plan2 = QuerySerde::from(q, start, end, "v1");

  LOG(INFO) << "Query serde time (ms): " << tick.elapsedMs();

  // pass the query plan to a server to execute - usually it is itself
  auto result2 = ServerExecutor(nebula::meta::NNode::local().toString()).execute(pool, plan2);
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
  auto data = nebula::api::test::genData();

  // query this table
  auto ms = TableService::singleton();
  auto tableName = std::get<0>(data);
  auto start = std::get<1>(data);
  auto end = std::get<2>(data);

  // let's plan these many data std::thread::hardware_concurrency()
  nebula::meta::TestTable testTable;
  auto query = table(testTable.name(), ms)
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
  auto plan1 = query.compile(QueryContext::def());
  plan1->setWindow({ start, end });
  plan1->setTableVersion("v1");

  // pass the query plan to a server to execute - usually it is itself

  // get result with default in proc connector
  nebula::common::Evidence::Duration tick;
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result1 = ServerExecutor(nebula::meta::NNode::local().toString()).execute(pool, plan1);
  LOG(INFO) << "Query time with in-proc connector (ms): " << tick.elapsedMs();
  auto str1 = ServiceProperties::jsonify(result1, plan1->getOutputSchema());

  // get result with cross-boundary connector
  // auto connector = std::make_shared<RemoteNodeConnector>();
  // tick.reset();
  // auto result2 = ServerExecutor(nebula::meta::NNode::local().toString()).execute(plan1, connector);
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
