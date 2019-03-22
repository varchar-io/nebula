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
#include "MockTable.h"
#include "api/dsl/Dsl.h"
#include "api/dsl/Expressions.h"
#include "common/Cursor.h"
#include "common/Errors.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "execution/ExecutionPlan.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "meta/Table.h"
#include "surface/DataSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace api {
namespace test {

using namespace nebula::api::dsl;
using nebula::common::Cursor;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeSerializer;

TEST(ApiTest, TestQueryStructure) {
  // set up table for testing
  auto ms = std::make_shared<MockMs>();
  const auto query = table("nebula.test", ms)
                       .where(col("event") == "NN")
                       .select(col("flag"), max(col("id") * 2).as("max_id"))
                       .groupby({ 1 })
                       .sortby({ 2 })
                       .limit(100);

  // compile the query into an execution plan
  auto plan = query.compile();

  // print out the plan through logging
  LOG(INFO) << "plan is empty: " << (plan == nullptr);
  // plan->display();

  // execute a plan on a server: for demo, we run the server on localhost:9190
  // auto result = plan->execute("localhost:9190");

  // print out result;
}
} // namespace test
} // namespace api
} // namespace nebula