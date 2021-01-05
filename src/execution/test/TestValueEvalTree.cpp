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
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "surface/eval/ValueEval.h"

namespace nebula {
namespace execution {
namespace test {

using nebula::common::Evidence;
using nebula::surface::MockRowData;
using nebula::surface::RowData;
using nebula::surface::eval::column;
using nebula::surface::eval::constant;
using nebula::surface::eval::eq;
using nebula::surface::eval::EvalContext;
using nebula::surface::eval::gt;
using nebula::surface::eval::TypeValueEval;
using nebula::surface::eval::ValueEval;

class MockRow : public nebula::surface::MockAccessor {
public:
  MockRow() = default;
  MOCK_CONST_METHOD1(readBool, std::optional<bool>(const std::string&));
  MOCK_CONST_METHOD1(readInt, std::optional<int32_t>(const std::string&));
  MOCK_CONST_METHOD1(readFloat, std::optional<float>(const std::string&));
  MOCK_CONST_METHOD1(readString, std::optional<std::string_view>(const std::string&));
};

TEST(ValueEvalTest, TestValueEval) {
  auto ivalue = 10;
  MockRow row;
  EXPECT_CALL(row, readBool("flag")).WillRepeatedly(testing::Return(true));
  EXPECT_CALL(row, readInt(testing::_)).WillRepeatedly(testing::Return(ivalue));

  // int = 3 + col('a')
  EvalContext ctx{ false };
  ctx.reset(row);
  auto b1 = nebula::surface::eval::constant(ivalue);
  auto v1 = b1->eval<int>(ctx);
  LOG(INFO) << "b1 eval: " << v1.value();
  EXPECT_EQ(v1, ivalue);

  auto b2 = nebula::surface::eval::column<int>("a");

  auto v2 = b2->eval<int>(ctx);
  LOG(INFO) << "b2 eval: " << v2.value();
  EXPECT_EQ(v1, v2);

  auto b3 = nebula::surface::eval::add<int, int, int>(std::move(b1), std::move(b2));
  auto sum = b3->eval<int>(ctx);
  auto expected_sum = v1.value() + v2.value();
  LOG(INFO) << "b3 eval: " << sum.value();
  EXPECT_EQ(sum, expected_sum);

  auto b4 = nebula::surface::eval::constant(ivalue);
  auto b5 = nebula::surface::eval::lt<int, int>(std::move(b3), std::move(b4));
  EXPECT_EQ(b5->eval<bool>(ctx), false);
}

TEST(ValueEvalTest, TestCompatibleCast) {
  EvalContext ctx{ false };

#define EVAL_TEST(T1, T2)                                       \
  {                                                             \
    T1 value = 90;                                              \
    auto c = nebula::surface::eval::constant(value);            \
    auto ev = c->eval<T2>(ctx);                                 \
    LOG(INFO) << "Origin=" << value << ", Eval=" << ev.value(); \
  }

  EVAL_TEST(int8_t, int8_t)
  EVAL_TEST(int16_t, int16_t)
  EVAL_TEST(int32_t, int32_t)
  EVAL_TEST(int64_t, int64_t)

  // TODO: support compatible casting in type eval
  // EVAL_TEST(int8_t, int32_t)

#undef EVAL_TEST
}

TEST(ValueEvalTest, TestValueEvalArthmetic) {
  // add two values
  {
    const int cvalue = 128;
    MockRow row;
    EXPECT_CALL(row, readBool("flag")).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(row, readInt(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(cvalue));

    EvalContext ctx{ false };
    ctx.reset(row);

    auto b1 = nebula::surface::eval::constant(cvalue);
    auto b2 = nebula::surface::eval::column<float>("x");
    auto b3 = nebula::surface::eval::add<float, int, float>(std::move(b1), std::move(b2));
    auto add = b3->eval<float>(ctx);
    EXPECT_EQ(add, row.readFloat("x").value() + cvalue);
  }

  {
    const int cvalue = 10;
    MockRow row;
    EXPECT_CALL(row, readBool("flag")).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(row, readInt(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(cvalue));

    EvalContext ctx{ false };
    ctx.reset(row);
    auto b1 = nebula::surface::eval::constant(cvalue);
    auto b2 = nebula::surface::eval::column<float>("y");
    auto b3 = nebula::surface::eval::mul<float, int, float>(std::move(b1), std::move(b2));
    auto mul = b3->eval<float>(ctx);
    EXPECT_EQ(mul, row.readFloat("y").value() * cvalue);
  }

  {
    const int cvalue = 2;
    MockRow row;
    EXPECT_CALL(row, readBool("flag")).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(row, readInt(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(cvalue));

    EvalContext ctx{ false };
    ctx.reset(row);
    auto b1 = nebula::surface::eval::constant(cvalue);
    auto b2 = nebula::surface::eval::column<float>("z");
    auto b3 = nebula::surface::eval::sub<float, int, float>(std::move(b1), std::move(b2));
    auto sub = b3->eval<float>(ctx);
    EXPECT_EQ(sub, cvalue - row.readFloat("z").value());
  }

  {
    const int cvalue = 2;
    MockRow row;
    EXPECT_CALL(row, readBool("flag")).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(row, readInt(testing::_)).WillRepeatedly(testing::Return(cvalue));
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(cvalue));

    EvalContext ctx{ false };
    ctx.reset(row);
    auto b1 = nebula::surface::eval::constant(cvalue);
    auto b2 = nebula::surface::eval::column<float>("d");
    auto b3 = nebula::surface::eval::div<float, float, int>(std::move(b2), std::move(b1));
    auto div = b3->eval<float>(ctx);
    EXPECT_EQ(div, row.readFloat("d").value() / cvalue);
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
    EXPECT_CALL(row, readString(testing::_)).WillRepeatedly(testing::Return("abc"));

    EvalContext ctx{ false };
    ctx.reset(row);
    auto b1 = nebula::surface::eval::constant(cvalue);
    auto b2 = nebula::surface::eval::column<float>("x");
    auto b3 = nebula::surface::eval::mul<float, int, float>(std::move(b1), std::move(b2));
    auto b4 = nebula::surface::eval::constant(32 * 33);
    auto b5 = nebula::surface::eval::gt<float, int>(std::move(b3), std::move(b4));
    EXPECT_EQ(b5->eval<bool>(ctx).value(), false);

    auto b6 = nebula::surface::eval::constant(true);
    auto b7 = nebula::surface::eval::bor<bool, bool>(std::move(b5), std::move(b6));
    auto b8 = nebula::surface::eval::constant(true);
    auto b9 = nebula::surface::eval::eq<bool, bool>(std::move(b7), std::move(b8));
    EXPECT_EQ(b9->eval<bool>(ctx).value(), true);

    auto b10 = nebula::surface::eval::column<std::string_view>("s");
    LOG(INFO) << "b10=" << b10->eval<std::string_view>(ctx).value();
  }
}

TEST(ValueEvalTest, TestStringValues) {
  MockRow row;
  const auto c = "abcdefg";
  EXPECT_CALL(row, readString(testing::_)).WillRepeatedly(testing::Return(c));

  EvalContext ctx{ false };
  ctx.reset(row);

  {
    auto b1 = nebula::surface::eval::constant("abcdef");
    auto v1 = b1->eval<std::string_view>(ctx);
    EXPECT_EQ(v1, "abcdef");

    auto b2 = nebula::surface::eval::column<std::string_view>("x");
    auto v2 = b2->eval<std::string_view>(ctx);
    EXPECT_EQ(v2, c);

    auto b3 = eq<std::string_view, std::string_view>(std::move(b1), std::move(b2));
    EXPECT_EQ(b3->eval<bool>(ctx), false);
  }

  {
    auto b1 = nebula::surface::eval::constant(c);
    EXPECT_EQ(b1->eval<std::string_view>(ctx), c);

    auto b2 = nebula::surface::eval::column<std::string_view>("x");
    EXPECT_EQ(b2->eval<std::string_view>(ctx), c);

    auto b3 = eq<std::string_view, std::string_view>(std::move(b1), std::move(b2));
    EXPECT_EQ(b3->eval<bool>(ctx), true);
  }

  {
    auto b1 = nebula::surface::eval::constant("abc");
    EXPECT_EQ(b1->eval<std::string_view>(ctx), "abc");

    auto b2 = nebula::surface::eval::column<std::string_view>("x");
    EXPECT_EQ(b2->eval<std::string_view>(ctx), c);

    auto b3 = gt<std::string_view, std::string_view>(std::move(b2), std::move(b1));
    EXPECT_EQ(b3->eval<bool>(ctx).value(), true);
  }

  {
    auto b1 = nebula::surface::eval::column<std::string_view>("x");
    auto b2 = nebula::surface::eval::column<std::string_view>("x");

    auto b3 = gt<std::string_view, std::string_view>(std::move(b1), std::move(b2));
    EXPECT_EQ(b3->eval<bool>(ctx).value(), false);
  }
}

TEST(ValueEvalTest, TestEvaluationContext) {
  EvalContext ctx{ false };
  auto b1 = nebula::surface::eval::constant(2);
  auto b2 = nebula::surface::eval::column<float>("x");
  auto b3 = nebula::surface::eval::mul<float, int, float>(std::move(b1), std::move(b2));
  auto b4 = nebula::surface::eval::constant(3);
  auto b5 = nebula::surface::eval::gt<float, int>(std::move(b3), std::move(b4));

  {
    MockRow row;
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(1));

    // 1*2 < 3
    ctx.reset(row);
    // value cached by the same row, so no matter how many calls.
    // evaluate result should be the same
    LOG(INFO) << "signature of b5=" << b5->signature();
    for (auto i = 0; i < 1000; ++i) {
      EXPECT_EQ(b5->eval<bool>(ctx), false);
    }
  }

  // reset the row to context, it will get larger data since incrementation
  {
    MockRow row;
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(2));

    // 2*2 > 3
    ctx.reset(row);
    for (auto i = 0; i < 1000; ++i) {
      auto result = b5->eval<bool>(ctx);
      EXPECT_EQ(result, true);
    }
  }

  // reset the row to context, it will get larger data since incrementation
  {
    MockRow row;
    EXPECT_CALL(row, readFloat(testing::_)).WillRepeatedly(testing::Return(std::nullopt));

    // 2*2 > 3
    ctx.reset(row);
    for (auto i = 0; i < 1000; ++i) {
      auto result = b5->eval<bool>(ctx);
      EXPECT_EQ(result, std::nullopt);
    }
  }
}

} // namespace test
} // namespace execution
} // namespace nebula