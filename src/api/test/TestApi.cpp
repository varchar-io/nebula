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
#include <sys/mman.h>
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
#include "meta/TestTable.h"
#include "surface/DataSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace api {
namespace test {

using namespace nebula::api::dsl;
using nebula::common::Cursor;
using nebula::execution::BlockManager;
using nebula::execution::core::ServerExecutor;
using nebula::meta::MockMs;
using nebula::meta::NBlock;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeSerializer;

TEST(ApiTest, TestQueryStructure) {
  auto tbl = "nebula.test";
  // set up table for testing
  auto ms = std::make_shared<MockMs>();
  const auto query = table(tbl, ms)
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

  LOG(INFO) << "Compiling query: ";
  LOG(INFO) << "                  select event, flag, max(id*2) as max_id, min(id+1) as min_id, count(1) as count";
  LOG(INFO) << "                  from nebula.test";
  LOG(INFO) << "                  group by 1, 2 ";
  LOG(INFO) << "                  order by 5";
  LOG(INFO) << "                  limit 100";
  // compile the query into an execution plan
  auto plan = query.compile();

  // print out the plan through logging
  plan->display();

  nebula::common::Evidence::Duration tick;
  // load test data to run this query
  auto bm = BlockManager::init();
  auto ptable = ms->query(tbl);

  // ensure block 0 of the test table (load from storage if not in memory)
  NBlock block(ptable->name(), 0, 0, 0);
  bm->add(block);

  // execute a plan on a server: for demo, we run the server on localhost:9190
  LOG(INFO) << "Loaded 100K rows data using " << tick.elapsedMs() << " ms";
  tick.reset();

  // pass the query plan to a server to execute - usually it is itself
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(*plan);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << fmt::format("col: {0:12} | {1:12} | {2:12} | {3:12} | {4:12}", "EVENT", "FLAG", "MAX_ID", "MIN_ID", "COUNT");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:12} | {1:12} | {2:12} | {3:12} | {4:12}",
                             row.isNull("event") ? "<NULL>" : row.readString("event"),
                             row.readBool("flag"),
                             row.readInt("max_id"),
                             row.readInt("min_id"),
                             row.readInt("count"));
  }
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
    LOG(INFO) << "test max UDAF";
    auto modexp = col("id") % 100;
    auto udaf = max(modexp);
    udaf.type(*tbl);
    EXPECT_EQ(udaf.kind(), nebula::type::Kind::INTEGER);
    auto v4up = udaf.asEval();
    auto v4 = static_cast<nebula::execution::eval::UDAF<nebula::type::Kind::INTEGER>*>(v4up.get());
    auto udaf_colrefs = udaf.columnRefs();
    EXPECT_EQ(udaf_colrefs.size(), 1);
    EXPECT_EQ(udaf_colrefs[0], "id");

    // call evaluate multiple times and see max value out of
    auto r1 = v4->eval(rowData);
    auto r2 = v4->eval(rowData);
    r2 = v4->agg(r1, r2);
    EXPECT_GE(r2, r1);
    auto r3 = v4->eval(rowData);
    r3 = v4->agg(r2, r3);
    EXPECT_GE(r3, r2);
    auto r4 = v4->eval(rowData);
    r4 = v4->agg(r3, r4);
    EXPECT_GE(r4, r3);
    LOG(INFO) << " 4 values: " << r1 << ", " << r2 << ", " << r3 << ", " << r4;
  }
}

} // namespace test
} // namespace api
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