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
#include <yorel/yomm2/cute.hpp>

#include "execution/ExecutionPlan.h"
#include "execution/core/BlockExecutor.h"
#include "execution/serde/RowCursorSerde.h"
#include "memory/Batch.h"
#include "meta/TestTable.h"
#include "surface/MockSurface.h"
#include "surface/eval/UDF.h"
#include "surface/eval/ValueEval.h"

namespace nebula {
namespace execution {
namespace test {

using nebula::execution::core::BlockExecutor;
using nebula::memory::Batch;
using nebula::memory::EvaledBlock;
using nebula::surface::Accessor;
using nebula::surface::MockRowData;
using nebula::surface::RowData;
using nebula::surface::eval::BlockEval;
using nebula::surface::eval::column;
using nebula::surface::eval::constant;
using nebula::surface::eval::custom;
using nebula::surface::eval::UDAF;
using nebula::type::Kind;
using nebula::type::TypeSerializer;

static constexpr auto line = [](const RowData& r) {
  return fmt::format("({0}, {1}, {2})",
                     r.isNull("id") ? 0 : r.readInt("id"),
                     r.isNull("event") ? "NULL" : r.readString("event"),
                     r.isNull("flag") ? true : r.readBool("flag"));
};
static constexpr auto line2 = [](const Accessor& r) {
  return fmt::format("({0}, {1}, {2})",
                     r.readInt("id").value_or(0),
                     r.readString("event").value_or("NULL"),
                     r.readBool("flag").value_or(true));
};

class TestUdaf : public UDAF<nebula::type::Kind::INTEGER> {
public:
  using BaseAggregator = typename UDAF<nebula::type::Kind::INTEGER>::BaseAggregator;

  class Aggregator : public BaseAggregator {
  public:
    virtual ~Aggregator() = default;
    // aggregate an value in
    inline virtual void merge(int32_t v) override {
      value += v;
    }

    // aggregate another aggregator
    inline virtual void mix(const nebula::surface::eval::Sketch& another) override {
      auto v2 = static_cast<const Aggregator&>(another).value;
      value += v2;
    }

    inline virtual bool fit(size_t space) override {
      // only 4 bytes to store
      return space >= 4;
    }

    inline virtual NativeType finalize() override {
      return value;
    }

    // serialize into a buffer
    inline virtual size_t serialize(nebula::common::ExtendableSlice&, size_t) override {
      // return slice.write(offset, value);
      return 4;
    }

    // deserialize from a given buffer, and bin size
    inline virtual size_t load(nebula::common::ExtendableSlice&, size_t) override {
      return 4;
    }

  private:
    int32_t value;
  };

public:
  TestUdaf()
    : UDAF<nebula::type::Kind::INTEGER>("TestExec",
                                        nebula::surface::eval::constant(1),
                                        []() -> std::shared_ptr<Aggregator> {
                                          return std::make_shared<Aggregator>();
                                        }) {}
  virtual ~TestUdaf() = default;
};

TEST(ExecutionTest, TestRowCursorSerde) {
  yorel::yomm2::update_methods();

  // normal row cursor
  {
    nebula::surface::MockRowCursor rowCursor;
    auto schema = TypeSerializer::from(
      "ROW<id:int, event:string, flag:bool>");

    nebula::surface::eval::Fields f;
    f.reserve(8);
    f.emplace_back(nebula::surface::eval::constant(1));
    f.emplace_back(nebula::surface::eval::constant("2"));
    f.emplace_back(nebula::surface::eval::constant(true));

    auto fb = nebula::execution::serde::asBuffer(rowCursor, schema, f);
    EXPECT_EQ(fb->getRows(), 8L);
  }

  // construct a SamplesExecutor to run a samples query
  {
    nebula::meta::TestTable test;
    auto size = 10;
    auto batch = std::make_shared<Batch>(test, size);
    MockRowData row;
    for (auto i = 0; i < size; ++i) {
      batch->add(row);
    }

    LOG(INFO) << "build up a block compute result";
    auto outputSchema = TypeSerializer::from("ROW<id:int, event:string, flag:bool>");
    nebula::execution::BlockPhase plan(test.schema(), outputSchema);

    nebula::surface::eval::Fields selects;
    selects.reserve(3);
    selects.push_back(column<int32_t>("id"));
    selects.push_back(column<std::string_view>("event"));
    selects.push_back(column<bool>("flag"));
    plan.scan(test.name())
      .compute(std::move(selects))
      .filter(constant<bool>(true))
      .aggregate(0, { false, false, false })
      .limit(size);

    EvaledBlock eb{ batch, BlockEval::PARTIAL };
    auto cursor = nebula::execution::core::compute(eb, plan);
    auto fb = nebula::execution::serde::asBuffer(*cursor, outputSchema, plan.fields());

    LOG(INFO) << "verify buffer size casted from cursor: " << typeid(cursor).name();
    EXPECT_EQ(fb->getRows(), size);

    // verify every row is the same
    LOG(INFO) << "verify every row is the same as batch";
    auto accessor = batch->makeAccessor();
    for (auto i = 0; i < size; ++i) {
      const auto& rb = accessor->seek(i);
      const auto& rf = fb->row(i);
      EXPECT_EQ(line2(rb), line(rf));
      LOG(INFO) << "verify row: " << i << ", b=" << line2(rb) << ",f=" << line(rf);
    }
  }

  // construct a BlockExecutor to run an aggregated query
  {
    nebula::meta::TestTable test;
    auto size = 10;
    auto batch = std::make_shared<Batch>(test, size);
    MockRowData row;
    MockRowData sameRow;
    int idSum = 0;
    for (auto i = 0; i < size; ++i) {
      batch->add(row);
      idSum += sameRow.readInt("id");
    }

    LOG(INFO) << "build up a block compute result";
    auto outputSchema = TypeSerializer::from("ROW<key:int, agg:int>");
    nebula::execution::BlockPhase plan(test.schema(), outputSchema);

    nebula::surface::eval::Fields selects;
    selects.reserve(2);
    selects.push_back(constant<int32_t>(20));
    selects.push_back(std::make_unique<TestUdaf>());
    plan.scan(test.name())
      .compute(std::move(selects))
      .filter(constant<bool>(true))
      .keys({ 0 })
      .aggregate(1, { false, true });

    EvaledBlock eb{ batch, BlockEval::PARTIAL };
    auto cursor = nebula::execution::core::compute(eb, plan);
    auto fb = nebula::execution::serde::asBuffer(*cursor, outputSchema, plan.fields());

    LOG(INFO) << "verify buffer size casted from cursor";
    EXPECT_EQ(fb->getRows(), 1);
    const auto& rf = fb->row(0);
    EXPECT_EQ(rf.readInt("key"), 20);
  }
}

TEST(ExecutionTest, TestCustomColumn) {
  nebula::meta::TestTable test;
  auto size = 1;
  auto batch = std::make_shared<Batch>(test, size);
  MockRowData row;
  MockRowData sameRow;
  for (auto i = 0; i < size; ++i) {
    batch->add(row);
  }

  LOG(INFO) << "build up a block compute result with custom column";

  // 2x of id value for id2 -
  // cautious about the int32 overflow value difference between JS and CPP
  // using safer arthmetic operations like "-2000"
  auto expr = "const id2 = () => nebula.column(\"id\") - 2000;";
  auto outputSchema = TypeSerializer::from("ROW<id:int, id2:int>");
  nebula::execution::BlockPhase plan(test.schema(), outputSchema);

  nebula::surface::eval::Fields selects;
  selects.reserve(2);
  selects.push_back(column<int32_t>("id"));
  selects.push_back(custom<int32_t>("id2", expr));
  plan.scan(test.name())
    .compute(std::move(selects))
    .filter(constant<bool>(true));

  EvaledBlock eb{ batch, BlockEval::PARTIAL };
  auto cursor = nebula::execution::core::compute(eb, plan);
  while (cursor->hasNext()) {
    const auto& row = cursor->next();
    auto id = row.readInt("id");
    auto id2 = row.readInt("id2");
    EXPECT_TRUE(id - 2000 == id2);
  }
}

} // namespace test
} // namespace execution
} // namespace nebula