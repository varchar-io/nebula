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
#include <folly/init/Init.h>
#include <glog/logging.h>
#include "execution/core/ServerExecutor.h"
#include "execution/io/trends/Trends.h"
#include "fmt/format.h"
#include "meta/TestTable.h"
#include "service/nebula/NebulaService.h"
#include "service/nebula/QueryHandler.h"
#include "surface/DataSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace service {
namespace test {

using namespace nebula::api::dsl;
using nebula::common::Cursor;
using nebula::common::Evidence;
using nebula::execution::BlockManager;
using nebula::execution::core::ServerExecutor;
using nebula::execution::io::trends::TrendsTable;
using nebula::service::ErrorCode;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeSerializer;

TEST(ServiceTest, TestQueryHandler) {
  TrendsTable trends;

  // load test data to run this query
  trends.loadTrends(10);

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
  auto plan = handler.compile(request, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  // No error in exeucting the query
  auto result = handler.query(*plan, err);
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
  trends.loadTrends(4);

  nebula::common::Evidence::Duration tick;
  QueryHandler handler;
  QueryRequest request;
  request.set_table(trends.name());
  request.set_start(Evidence::time("2019-02-01", "%Y-%m-%d"));
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
    expr->add_value("lego %");
  }

  request.add_dimension("query");
  request.add_dimension("_time_");
  auto metric = request.add_metric();
  metric->set_column("count");
  metric->set_method(Rollup::SUM);

  ErrorCode err = ErrorCode::NONE;

  // No error in compiling the query
  auto plan = handler.compile(request, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  // No error in exeucting the query
  auto result = handler.query(*plan, err);
  EXPECT_EQ(err, ErrorCode::NONE);

  LOG(INFO) << "Execute the query and jsonify results: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << ServiceProperties::jsonify(result, plan->getOutputSchema());
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
