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
#include "execution/eval/ValueEval.h"
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
using nebula::execution::BlockManager;
using nebula::execution::core::ServerExecutor;
using nebula::execution::eval::EvalContext;
using nebula::execution::meta::TableService;
using nebula::meta::BlockSignature;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeSerializer;

TEST(ApiTest, TestQueryStructure) {
  auto tbl = "nebula.test";
  // set up table for testing
  auto ms = TableService::singleton();
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
  BlockSignature block{ ptable->name(), 0, 0, 0 };
  bm->add(block);

  // execute a plan on a server: for demo, we run the server on localhost:9190
  LOG(INFO) << "Loaded 100K rows data using " << tick.elapsedMs() << " ms";
  tick.reset();

  // pass the query plan to a server to execute - usually it is itself
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(pool, *plan);

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

TEST(ApiTest, TestSortingAndTop) {
  auto tbl = "nebula.test";
  // set up table for testing
  auto ms = TableService::singleton();
  const auto query = table(tbl, ms)
                       .where(like(col("event"), "N%"))
                       .select(
                         col("event"),
                         col("flag"),
                         max(col("id") * 2).as("max_id"),
                         min(col("id") + 1).as("min_id"),
                         count(col("id")).as("count"),
                         sum(col("value")).as("sum"))
                       .groupby({ 1, 2 })
                       .sortby({ 5 }, SortType::DESC)
                       .limit(5);
  // compile the query into an execution plan
  auto plan = query.compile();
  // load test data to run this query
  auto bm = BlockManager::init();
  auto ptable = ms->query(tbl);
  BlockSignature block{ ptable->name(), 0, 0, 0 };
  bm->add(block);
  nebula::common::Evidence::Duration tick;

  // pass the query plan to a server to execute - usually it is itself
  folly::CPUThreadPoolExecutor pool{ 8 };
  auto result = ServerExecutor(nebula::meta::NNode::local().toString()).execute(pool, *plan);

  // print out result;
  LOG(INFO) << "----------------------------------------------------------------";
  LOG(INFO) << "Get Results With Rows: " << result->size() << " using " << tick.elapsedMs() << " ms";
  LOG(INFO) << fmt::format("col: {0:12} | {1:12} | {2:12} | {3:12} | {4:12} | {5:12}", "EVENT", "FLAG", "MAX_ID", "MIN_ID", "COUNT", "SUM");
  while (result->hasNext()) {
    const auto& row = result->next();
    LOG(INFO) << fmt::format("row: {0:12} | {1:12} | {2:12} | {3:12} | {4:12} | {5:12}",
                             row.isNull("event") ? "<NULL>" : row.readString("event"),
                             row.readBool("flag"),
                             row.readInt("max_id"),
                             row.readInt("min_id"),
                             row.readInt("count"),
                             row.readInt("sum"));
  }
}

class MockRow : public nebula::surface::MockRowData {
public:
  MockRow() = default;
  MOCK_CONST_METHOD1(readBool, bool(const std::string&));
  MOCK_CONST_METHOD1(readInt, int32_t(const std::string&));
  MOCK_CONST_METHOD1(isNull, bool(const std::string&));
};

TEST(ApiTest, TestExprValueEval) {
  // set up table for testing
  MockRow rowData;
  MockRow mirror;

  EXPECT_CALL(rowData, readBool("flag")).WillRepeatedly(testing::Return(true));
  EXPECT_CALL(rowData, readInt("id")).WillRepeatedly(testing::Return(320));
  EXPECT_CALL(rowData, isNull(testing::_)).WillRepeatedly(testing::Return(false));
  EXPECT_CALL(mirror, readBool("flag")).WillRepeatedly(testing::Return(true));
  EXPECT_CALL(mirror, readInt("id")).WillRepeatedly(testing::Return(320));

  auto expr = col("flag") == true;
  bool expected1 = mirror.readBool("flag") == true;
  auto expr2 = col("id") * 2 + 10 - (col("id") / 4);
  auto id1 = mirror.readInt("id");
  auto id2 = mirror.readInt("id");
  auto expected2 = id1 * 2 + 10 - (id2 / 4);

  auto ms = TableService::singleton();
  auto tbl = ms->query("nebula.test");
  expr.type(*tbl);
  expr2.type(*tbl);
  auto v1 = expr.asEval();
  auto v2 = expr2.asEval();

  EvalContext ctx;
  ctx.reset(rowData);
  bool valid = true;
  auto x = v1->eval<bool>(ctx, valid);
  valid = true;
  auto y = v2->eval<int>(ctx, valid);
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

    bool valid = true;
    auto r = v3->eval<bool>(ctx, valid);
    LOG(INFO) << " UDF eval result: " << r << " valid=" << valid;
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
    bool valid = true;
    auto r1 = v4->eval(ctx, valid);
    auto r2 = v4->eval(ctx, valid);
    r2 = v4->compute(r1, r2);
    EXPECT_GE(r2, r1);
    auto r3 = v4->eval(ctx, valid);
    r3 = v4->compute(r2, r3);
    EXPECT_GE(r3, r2);
    auto r4 = v4->eval(ctx, valid);
    r4 = v4->compute(r3, r4);
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