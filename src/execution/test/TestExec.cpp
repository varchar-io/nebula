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
#include <gtest/gtest.h>
#include "execution/ExecutionPlan.h"
#include "execution/core/BlockExecutor.h"
#include "execution/eval/UDF.h"
#include "execution/eval/ValueEval.h"
#include "execution/io/trends/Pins.h"
#include "execution/serde/RowCursorSerde.h"
#include "memory/Batch.h"
#include "meta/TestTable.h"
#include "surface/MockSurface.h"

namespace nebula {
namespace execution {
namespace test {

using nebula::execution::core::BlockExecutor;
using nebula::execution::eval::column;
using nebula::execution::eval::constant;
using nebula::execution::eval::EvalContext;
using nebula::execution::eval::UDAF;
using nebula::memory::Batch;
using nebula::surface::MockRowData;
using nebula::surface::RowData;
using nebula::type::TypeSerializer;

TEST(ExecutionTest, TestOperator) {
  LOG(INFO) << "Execution provides physical executuin units at any stage";
}

TEST(ExecutionTest, TestLoadPins) {
  nebula::execution::io::trends::PinsTable pins;
  pins.load(1);
}

static constexpr auto line = [](const RowData& r) {
  return fmt::format("({0}, {1}, {2})",
                     r.isNull("id") ? 0 : r.readInt("id"),
                     r.isNull("event") ? "NULL" : r.readString("event"),
                     r.isNull("flag") ? true : r.readBool("flag"));
};

class TestUdaf : public UDAF<nebula::type::Kind::INTEGER> {
public:
  using AggFunction = std::function<int32_t(int32_t, int32_t)>;
  TestUdaf()
    : UDAF<nebula::type::Kind::INTEGER>("TestExec",
                                        [](int32_t a, int32_t b) { return a + b; },
                                        [](int32_t a, int32_t b) { return a + b; }) {}
  virtual ~TestUdaf() = default;
  virtual int32_t run(EvalContext&, bool&) const override {
    return 1;
  }

  virtual void global(const nebula::surface::RowData&) override {
  }
};

TEST(ExecutionTest, TestRowCursorSerde) {
  yorel::yomm2::update_methods();

  // normal row cursor
  {
    nebula::surface::MockRowCursor rowCursor;
    auto schema = TypeSerializer::from(
      "ROW<id:int, event:string, flag:bool>");

    auto fb = nebula::execution::serde::asBuffer(rowCursor, schema);
    EXPECT_EQ(fb->getRows(), 8);
  }

  // construct a SamplesExecutor to run a samples query
  {
    nebula::meta::TestTable test;
    auto size = 10;
    Batch batch(test, size);
    MockRowData row;
    for (auto i = 0; i < size; ++i) {
      batch.add(row);
    }

    LOG(INFO) << "build up a block compute result";
    auto outputSchema = TypeSerializer::from("ROW<id:int, event:string, flag:bool>");
    nebula::execution::BlockPhase plan(test.schema(), outputSchema);

    std::vector<std::unique_ptr<eval::ValueEval>> selects;
    selects.reserve(3);
    selects.push_back(column<int32_t>("id"));
    selects.push_back(column<std::string_view>("event"));
    selects.push_back(column<bool>("flag"));
    plan.scan(test.name())
      .compute(std::move(selects))
      .filter(constant<bool>(true))
      .aggregate(false)
      .limit(size);

    auto cursor = nebula::execution::core::compute(batch, plan);
    auto fb = nebula::execution::serde::asBuffer(*cursor, outputSchema);

    LOG(INFO) << "verify buffer size casted from cursor: " << typeid(cursor).name();
    EXPECT_EQ(fb->getRows(), size);

    // verify every row is the same
    LOG(INFO) << "verify every row is the same as batch";
    auto accessor = batch.makeAccessor();
    for (auto i = 0; i < size; ++i) {
      const auto& rb = accessor->seek(i);
      const auto& rf = fb->row(i);
      EXPECT_EQ(line(rb), line(rf));
      LOG(INFO) << "verify row: " << i << ", b=" << line(rb) << ",f=" << line(rf);
    }
  }

  // construct a BlockExecutor to run an aggregated query
  {
    nebula::meta::TestTable test;
    auto size = 10;
    Batch batch(test, size);
    MockRowData row;
    for (auto i = 0; i < size; ++i) {
      batch.add(row);
    }

    LOG(INFO) << "build up a block compute result";
    auto outputSchema = TypeSerializer::from("ROW<key:int, agg:int>");
    nebula::execution::BlockPhase plan(test.schema(), outputSchema);

    std::vector<std::unique_ptr<eval::ValueEval>> selects;
    selects.reserve(2);
    selects.push_back(constant<int32_t>(20));
    selects.push_back(std::make_unique<TestUdaf>());
    plan.scan(test.name())
      .compute(std::move(selects))
      .filter(constant<bool>(true))
      .keys({ 0 })
      .aggregate(true);

    auto cursor = nebula::execution::core::compute(batch, plan);
    auto fb = nebula::execution::serde::asBuffer(*cursor, outputSchema);

    LOG(INFO) << "verify buffer size casted from cursor";
    EXPECT_EQ(fb->getRows(), 1);
    const auto& rf = fb->row(0);
    EXPECT_EQ(rf.readInt("key"), 20);
  }
}

} // namespace test
} // namespace execution
} // namespace nebula