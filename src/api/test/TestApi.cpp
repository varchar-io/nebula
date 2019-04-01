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

TEST(ApiTest, TestExprValueEval) {
  const auto seed = common::Evidence::ticks();
  // set up table for testing
  nebula::surface::MockRowData rowData(seed);
  nebula::surface::MockRowData mirror(seed);
  auto expr = col("flag") == true;
  bool expected1 = mirror.readBool("flag") == true;
  auto expr2 = col("id") * 2 + 10 - (col("id") / 4);
  auto id1 = mirror.readInt("id");
  auto id2 = mirror.readInt("id");
  auto expected2 = id1 * 2 + 10 - (id2 / 4);

  auto ms = std::make_shared<MockMs>();
  auto tbl = ms->query("nebula.test");
  LOG(INFO) << "call types";
  expr.type(*tbl);
  expr2.type(*tbl);
  LOG(INFO) << "convert to value evals";
  auto v1 = expr.asEval();
  auto v2 = expr2.asEval();

  LOG(INFO) << "evalulate v1 expression with mock row";
  auto x = v1->eval<bool>(rowData);
  LOG(INFO) << "evalulate v2 expression with mock row";
  auto y = v2->eval<int>(rowData);
  LOG(INFO) << "x=" << x << ", y=" << y;

  EXPECT_EQ(x, expected1);
  EXPECT_EQ(y, expected2);

  // test UDF expression evaluation
  {
    auto udf = reverse(expr);
    udf.type(*tbl);
    EXPECT_EQ(udf.kind(), nebula::type::Kind::BOOLEAN);
    auto v3 = udf.asEval();
    auto colrefs = udf.columnRefs();
    EXPECT_EQ(colrefs.size(), 1);
    EXPECT_EQ(colrefs[0], "flag");

    auto r = v3->eval<bool>(rowData);
    LOG(INFO) << " UDF eval result: " << r;
    bool exp3 = !(mirror.readBool("flag") == true);
    EXPECT_EQ(r, exp3);
  }

  // test UDAF expression
  {
    auto modexp = col("id") % 100;
    auto udaf = max(modexp);
    udaf.type(*tbl);
    EXPECT_EQ(udaf.kind(), nebula::type::Kind::INTEGER);
    auto v4 = udaf.asEval();
    auto udaf_colrefs = udaf.columnRefs();
    EXPECT_EQ(udaf_colrefs.size(), 1);
    EXPECT_EQ(udaf_colrefs[0], "id");

    // call evaluate multiple times and see max value out of
    auto r1 = v4->eval<int>(rowData);
    auto r2 = v4->eval<int>(rowData);
    EXPECT_GE(r2, r1);
    auto r3 = v4->eval<int>(rowData);
    EXPECT_GE(r3, r2);
    auto r4 = v4->eval<int>(rowData);
    EXPECT_GE(r4, r3);
    LOG(INFO) << " 4 values: " << r1 << ", " << r2 << ", " << r3 << ", " << r4;
  }
}

} // namespace test
} // namespace api
} // namespace nebula