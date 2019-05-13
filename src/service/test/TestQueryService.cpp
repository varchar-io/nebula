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

TEST(ServiceTest, TestServiceEndpoint) {
  TrendsTable trends;
  // load test data to run this query
  trends.loadTrends(10);

  //    .where(col("_time_") > 1554076800 && col("_time_") < 1556582400 && like(col("query"), "apple%"))
  //    .select(
  //      col("query"),
  //      count(col("count")).as("total"))
  //    .groupby({ 1 })
  //    .sortby({ 2 }, SortType::DESC)
  //    .limit(10);
  nebula::common::Evidence::Duration tick;
  QueryRequest request;
  request.set_table(trends.name());
  request.set_start(1554076800);
  request.set_end(1556582400);
  auto pa = request.mutable_filtera();
  {
    auto expr = pa->add_expression();
    expr->set_column("query");
    expr->set_op(Operation::LIKE);
    expr->add_value("apple%");
  }

  // set dimensions and metrics
  request.add_dimension("query");
  auto metric = request.add_metric();
  metric->set_column("count");
  metric->set_method(Rollup::COUNT);

  // set sorting and limit schema
  auto order = request.mutable_order();
  order->set_column("count");
  order->set_type(OrderType::DESC);
  request.set_top(10);

  // execute the service code
  QueryHandler handler;
  ErrorCode error = ErrorCode::NONE;
  // compile the query into a plan
  auto plan = handler.compile(trends, request, error);

  // set time range constraints in execution plan directly since it should always present
  plan->display();
  EXPECT_EQ(error, ErrorCode::NONE);

  nebula::surface::RowCursor result = handler.query(*plan, error);
  EXPECT_EQ(error, ErrorCode::NONE);

  LOG(INFO) << "JSON BLOB:";
  LOG(INFO) << ServiceProperties::jsonify(result, plan->getOutputSchema());
}

} // namespace test
} // namespace service
} // namespace nebula
