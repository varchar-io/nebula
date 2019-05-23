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
#include <glog/logging.h>
#include "common/Evidence.h"
#include "execution/eval/ValueEval.h"
#include "fmt/format.h"
#include "gmock/gmock.h"
#include "surface/DataSurface.h"

namespace nebula {
namespace execution {
namespace test {

using nebula::common::Evidence;
using nebula::execution::eval::column;
using nebula::execution::eval::constant;
using nebula::execution::eval::EvalContext;
using nebula::execution::eval::TypeValueEval;
using nebula::execution::eval::ValueEval;
using nebula::surface::MockRowData;
using nebula::surface::RowData;

class MockRow : public nebula::surface::MockRowData {
public:
  MockRow() = default;
  MOCK_CONST_METHOD1(readBool, bool(const std::string&));
  MOCK_CONST_METHOD1(readInt, int32_t(const std::string&));
  MOCK_CONST_METHOD1(readFloat, float(const std::string&));
  MOCK_CONST_METHOD1(isNull, bool(const std::string&));
};

TEST(ValueEvalTest, TestValueEval) {
  auto ivalue = 10;
  MockRow row;
  EXPECT_CALL(row, readBool("flag")).WillRepeatedly(testing::Return(true));
  EXPECT_CALL(row, readInt(testing::_)).WillRepeatedly(testing::Return(ivalue));
  EXPECT_CALL(row, isNull(testing::_)).WillRepeatedly(testing::Return(false));

  // int = 3 + col('a')
  EvalContext ctx;
  ctx.reset(row);
  auto b1 = nebula::execution::eval::constant(ivalue);
  bool valid = true;
  auto v1 = b1->eval<int>(ctx, valid);
  LOG(INFO) << "b1 eval: " << v1 << ", valid:" << valid;
  EXPECT_EQ(v1, ivalue);

  auto b2 = nebula::execution::eval::column<int>("a");

  valid = true;
  auto v2 = b2->eval<int>(ctx, valid);
  LOG(INFO) << "b2 eval: " << v2;
  EXPECT_EQ(v1, v2);

  auto b3 = nebula::execution::eval::add<int, int, int>(std::move(b1), std::move(b2));
  valid = true;
  auto sum = b3->eval<int>(ctx, valid);
  auto expected_sum = v1 + v2;
  LOG(INFO) << "b3 eval: " << sum;
  EXPECT_EQ(sum, expected_sum);

  auto b4 = nebula::execution::eval::constant(ivalue);
  auto b5 = nebula::execution::eval::lt<int, int>(std::move(b3), std::move(b4));
  valid = true;
  EXPECT_FALSE(b5->eval<bool>(ctx, valid));
}

TEST(ValueEvalTest, TestValueEvalArthmetic) {
  // add two values
  {
    const int cvalue = 128;
    MockRow row;
    EXPECT_CALL(row, readBool("flag")).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(row, readInt(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, isNull(testing::_)).WillRepeatedly(testing::Return(false));

    EvalContext ctx;
    ctx.reset(row);

    auto b1 = nebula::execution::eval::constant(cvalue);
    auto b2 = nebula::execution::eval::column<float>("x");
    auto b3 = nebula::execution::eval::add<float, int, float>(std::move(b1), std::move(b2));
    bool valid = true;
    float add = b3->eval<float>(ctx, valid);
    EXPECT_EQ(add, row.readFloat("x") + cvalue);
  }

  {
    const int cvalue = 10;
    MockRow row;
    EXPECT_CALL(row, readBool("flag")).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(row, readInt(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, isNull(testing::_)).WillRepeatedly(testing::Return(false));

    EvalContext ctx;
    ctx.reset(row);
    auto b1 = nebula::execution::eval::constant(cvalue);
    auto b2 = nebula::execution::eval::column<float>("y");
    auto b3 = nebula::execution::eval::mul<float, int, float>(std::move(b1), std::move(b2));
    bool valid = true;
    float mul = b3->eval<float>(ctx, valid);
    EXPECT_EQ(mul, row.readFloat("y") * cvalue);
  }

  {
    const int cvalue = 2;
    MockRow row;
    EXPECT_CALL(row, readBool("flag")).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(row, readInt(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, isNull(testing::_)).WillRepeatedly(testing::Return(false));

    EvalContext ctx;
    ctx.reset(row);
    auto b1 = nebula::execution::eval::constant(cvalue);
    auto b2 = nebula::execution::eval::column<float>("z");
    auto b3 = nebula::execution::eval::sub<float, int, float>(std::move(b1), std::move(b2));
    bool valid = true;
    float sub = b3->eval<float>(ctx, valid);
    EXPECT_EQ(sub, cvalue - row.readFloat("z"));
  }

  {
    const int cvalue = 2;
    MockRow row;
    EXPECT_CALL(row, readBool("flag")).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(row, readInt(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, isNull(testing::_)).WillRepeatedly(testing::Return(false));

    EvalContext ctx;
    ctx.reset(row);
    auto b1 = nebula::execution::eval::constant(cvalue);
    auto b2 = nebula::execution::eval::column<float>("d");
    auto b3 = nebula::execution::eval::div<float, float, int>(std::move(b2), std::move(b1));
    bool valid = true;
    float div = b3->eval<float>(ctx, valid);
    EXPECT_EQ(div, row.readFloat("d") / cvalue);
  }
}

TEST(ValueEvalTest, TestValueEvalLogical) {
  // add two values
  {
    const int cvalue = 32;
    MockRow row;
    EXPECT_CALL(row, readBool("flag")).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(row, readInt(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, isNull(testing::_)).WillRepeatedly(testing::Return(false));

    EvalContext ctx;
    ctx.reset(row);
    auto b1 = nebula::execution::eval::constant(cvalue);
    auto b2 = nebula::execution::eval::column<float>("x");
    auto b3 = nebula::execution::eval::mul<float, int, float>(std::move(b1), std::move(b2));
    auto b4 = nebula::execution::eval::constant(32 * 33);
    auto b5 = nebula::execution::eval::gt<float, int>(std::move(b3), std::move(b4));
    bool valid = true;
    EXPECT_EQ(b5->eval<bool>(ctx, valid), false);

    auto b6 = nebula::execution::eval::constant(true);
    auto b7 = nebula::execution::eval::bor<bool, bool>(std::move(b5), std::move(b6));
    auto b8 = nebula::execution::eval::constant(true);
    auto b9 = nebula::execution::eval::eq<bool, bool>(std::move(b7), std::move(b8));
    valid = true;
    EXPECT_EQ(b9->eval<bool>(ctx, valid), true);

    auto b10 = nebula::execution::eval::column<std::string_view>("s");
    valid = true;
    LOG(INFO) << "b10=" << b10->eval<std::string_view>(ctx, valid);
  }
}

TEST(ValueEvalTest, TestEvaluationContext) {
  EvalContext context;
  auto b1 = nebula::execution::eval::constant(2);
  auto b2 = nebula::execution::eval::column<float>("x");
  auto b3 = nebula::execution::eval::mul<float, int, float>(std::move(b1), std::move(b2));
  auto b4 = nebula::execution::eval::constant(3);
  auto b5 = nebula::execution::eval::gt<float, int>(std::move(b3), std::move(b4));

  {
    MockRow row;
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(1));
    EXPECT_CALL(row, isNull(testing::_)).WillRepeatedly(testing::Return(false));

    // 1*2 < 3
    context.reset(row);
    bool valid = true;
    // value cached by the same row, so no matter how many calls.
    // evaluate result should be the same
    LOG(INFO) << "signature of b5=" << b5->signature();
    for (auto i = 0; i < 1000; ++i) {
      EXPECT_EQ(valid, true);
      EXPECT_EQ(context.eval<bool>(*b5, valid), false);
    }
  }

  // reset the row to context, it will get larger data since incrementation
  {
    MockRow row;
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(2));
    EXPECT_CALL(row, isNull(testing::_)).WillRepeatedly(testing::Return(false));

    // 2*2 > 3
    bool valid = true;
    context.reset(row);
    for (auto i = 0; i < 1000; ++i) {
      auto result = context.eval<bool>(*b5, valid);
      EXPECT_EQ(valid, true);
      EXPECT_EQ(result, true);
    }
  }

  // reset the row to context, it will get larger data since incrementation
  {
    MockRow row;
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(2));
    EXPECT_CALL(row, isNull(testing::_)).WillRepeatedly(testing::Return(true));

    // 2*2 > 3
    context.reset(row);
    for (auto i = 0; i < 1000; ++i) {
      bool valid = true;
      auto result = context.eval<bool>(*b5, valid);
      EXPECT_EQ(valid, false);
      if (valid) {
        EXPECT_EQ(result, true);
      }
    }
  }
}

} // namespace test
} // namespace execution
} // namespace nebula