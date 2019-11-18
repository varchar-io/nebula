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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/Evidence.h"
#include "surface/eval/ValueEval.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"

namespace nebula {
namespace execution {
namespace test {

using nebula::common::Evidence;
using nebula::surface::eval::column;
using nebula::surface::eval::constant;
using nebula::surface::eval::eq;
using nebula::surface::eval::EvalContext;
using nebula::surface::eval::gt;
using nebula::surface::eval::TypeValueEval;
using nebula::surface::MockRowData;
using nebula::surface::RowData;
using nebula::surface::eval::ValueEval;

class MockRow : public nebula::surface::MockRowData {
public:
  MockRow() = default;
  MOCK_CONST_METHOD1(readBool, bool(const std::string&));
  MOCK_CONST_METHOD1(readInt, int32_t(const std::string&));
  MOCK_CONST_METHOD1(readFloat, float(const std::string&));
  MOCK_CONST_METHOD1(readString, std::string_view(const std::string&));
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
  auto b1 = nebula::surface::eval::constant(ivalue);
  bool valid = true;
  auto v1 = b1->eval<int>(ctx, valid);
  LOG(INFO) << "b1 eval: " << v1 << ", valid:" << valid;
  EXPECT_EQ(v1, ivalue);

  auto b2 = nebula::surface::eval::column<int>("a");

  valid = true;
  auto v2 = b2->eval<int>(ctx, valid);
  LOG(INFO) << "b2 eval: " << v2;
  EXPECT_EQ(v1, v2);

  auto b3 = nebula::surface::eval::add<int, int, int>(std::move(b1), std::move(b2));
  valid = true;
  auto sum = b3->eval<int>(ctx, valid);
  auto expected_sum = v1 + v2;
  LOG(INFO) << "b3 eval: " << sum;
  EXPECT_EQ(sum, expected_sum);

  auto b4 = nebula::surface::eval::constant(ivalue);
  auto b5 = nebula::surface::eval::lt<int, int>(std::move(b3), std::move(b4));
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

    auto b1 = nebula::surface::eval::constant(cvalue);
    auto b2 = nebula::surface::eval::column<float>("x");
    auto b3 = nebula::surface::eval::add<float, int, float>(std::move(b1), std::move(b2));
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
    auto b1 = nebula::surface::eval::constant(cvalue);
    auto b2 = nebula::surface::eval::column<float>("y");
    auto b3 = nebula::surface::eval::mul<float, int, float>(std::move(b1), std::move(b2));
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
    auto b1 = nebula::surface::eval::constant(cvalue);
    auto b2 = nebula::surface::eval::column<float>("z");
    auto b3 = nebula::surface::eval::sub<float, int, float>(std::move(b1), std::move(b2));
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
    auto b1 = nebula::surface::eval::constant(cvalue);
    auto b2 = nebula::surface::eval::column<float>("d");
    auto b3 = nebula::surface::eval::div<float, float, int>(std::move(b2), std::move(b1));
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
    auto b1 = nebula::surface::eval::constant(cvalue);
    auto b2 = nebula::surface::eval::column<float>("x");
    auto b3 = nebula::surface::eval::mul<float, int, float>(std::move(b1), std::move(b2));
    auto b4 = nebula::surface::eval::constant(32 * 33);
    auto b5 = nebula::surface::eval::gt<float, int>(std::move(b3), std::move(b4));
    bool valid = true;
    EXPECT_EQ(b5->eval<bool>(ctx, valid), false);

    auto b6 = nebula::surface::eval::constant(true);
    auto b7 = nebula::surface::eval::bor<bool, bool>(std::move(b5), std::move(b6));
    auto b8 = nebula::surface::eval::constant(true);
    auto b9 = nebula::surface::eval::eq<bool, bool>(std::move(b7), std::move(b8));
    valid = true;
    EXPECT_EQ(b9->eval<bool>(ctx, valid), true);

    auto b10 = nebula::surface::eval::column<std::string_view>("s");
    valid = true;
    LOG(INFO) << "b10=" << b10->eval<std::string_view>(ctx, valid);
  }
}

TEST(ValueEvalTest, TestStringValues) {
  MockRow row;
  const auto c = "abcdefg";
  EXPECT_CALL(row, readString(testing::_)).WillRepeatedly(testing::Return(c));
  EXPECT_CALL(row, isNull(testing::_)).WillRepeatedly(testing::Return(false));

  EvalContext ctx;
  ctx.reset(row);
  bool valid = true;

  {
    auto b1 = nebula::surface::eval::constant("abcdef");
    auto v1 = b1->eval<std::string_view>(ctx, valid);
    EXPECT_EQ(v1, "abcdef");

    valid = true;
    auto b2 = nebula::surface::eval::column<std::string_view>("x");
    auto v2 = b2->eval<std::string_view>(ctx, valid);
    EXPECT_EQ(v2, c);

    valid = true;
    auto b3 = eq<std::string_view, std::string_view>(std::move(b1), std::move(b2));
    EXPECT_EQ(b3->eval<bool>(ctx, valid), false);
  }

  {
    valid = true;
    auto b1 = nebula::surface::eval::constant(c);
    EXPECT_EQ(b1->eval<std::string_view>(ctx, valid), c);

    valid = true;
    auto b2 = nebula::surface::eval::column<std::string_view>("x");
    EXPECT_EQ(b2->eval<std::string_view>(ctx, valid), c);

    valid = true;
    auto b3 = eq<std::string_view, std::string_view>(std::move(b1), std::move(b2));
    EXPECT_EQ(b3->eval<bool>(ctx, valid), true);
  }

  {
    valid = true;
    auto b1 = nebula::surface::eval::constant("abc");
    EXPECT_EQ(b1->eval<std::string_view>(ctx, valid), "abc");

    valid = true;
    auto b2 = nebula::surface::eval::column<std::string_view>("x");
    EXPECT_EQ(b2->eval<std::string_view>(ctx, valid), c);

    valid = true;
    auto b3 = gt<std::string_view, std::string_view>(std::move(b2), std::move(b1));
    EXPECT_EQ(b3->eval<bool>(ctx, valid), true);
  }

  {
    auto b1 = nebula::surface::eval::column<std::string_view>("x");
    auto b2 = nebula::surface::eval::column<std::string_view>("x");

    valid = true;
    auto b3 = gt<std::string_view, std::string_view>(std::move(b1), std::move(b2));
    EXPECT_EQ(b3->eval<bool>(ctx, valid), false);
  }
}

TEST(ValueEvalTest, TestEvaluationContext) {
  EvalContext context;
  auto b1 = nebula::surface::eval::constant(2);
  auto b2 = nebula::surface::eval::column<float>("x");
  auto b3 = nebula::surface::eval::mul<float, int, float>(std::move(b1), std::move(b2));
  auto b4 = nebula::surface::eval::constant(3);
  auto b5 = nebula::surface::eval::gt<float, int>(std::move(b3), std::move(b4));

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