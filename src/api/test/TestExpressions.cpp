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

#include "api/dsl/Dsl.h"
#include "api/dsl/Expressions.h"
#include "api/dsl/Serde.h"
#include "common/Cursor.h"
#include "common/Errors.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "execution/ExecutionPlan.h"
#include "execution/meta/TableService.h"
#include "meta/TestTable.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "surface/eval/ValueEval.h"
#include "type/Serde.h"

namespace nebula {
namespace api {
namespace test {

using nebula::api::dsl::between;
using nebula::api::dsl::col;
using nebula::api::dsl::Serde;
using nebula::execution::meta::TableService;
using nebula::surface::MockAccessor;
using nebula::surface::eval::EvalContext;

struct X {
  X(int id) : id_{ id } {}
  template <typename M, bool OK = std::is_same<char*, std::decay_t<M>>::value>
  X& print(const M& m) {
    LOG(INFO) << "Is string=" << OK << ", value = " << m << ", type=" << typeid(std::decay_t<M>).name();
    return *this;
  }

  virtual ~X() {
    LOG(INFO) << "x instance destroyed";
  }

  int id_;

  template <typename T>
  bool operator==(T) const {
    LOG(INFO) << "compared with == to an int";
    return true;
  }
  static X make() {
    return X(0);
  }
};

struct Y : public X {
  Y(int i) : X(i) {}
  // virtual ~Y() {
  //  LOG(INFO) << "Y instance destroyed";
  // }

  // I don't know why this is working well - if not sure, it might be dangrous
  // as the shared pointer may be deleted any time while reference is still using?
  static Y& make() {
    return *std::make_shared<Y>(1);
  }
};

TEST(ExpressionsTest, TypeDetection) {
  {
    X::make().print("abc").print(16);
  }

#define SEXPR(T, ...) (std::make_shared<T>(__VA_ARGS__))

  auto s = SEXPR(nebula::api::dsl::ConstExpression<int>, 1);
#undef SEXPR

  // create shared pointer but use
  const auto& y = Y::make();
  if (y == 3) {
    LOG(INFO) << "passed operator equal via reference type";
  }
}

TEST(ExpressionsTest, TestExpressionType) {
  // all the expressions are working as non-const and non-reference
  // to favor the expresssion chain
  auto ms = TableService::singleton();
  auto tbl = ms->query("nebula.test").table();

  // constant type
  nebula::api::dsl::ConstExpression<int> a(1);
  auto K = [](const nebula::api::dsl::TypeInfo& node) { return node.native; };

  EXPECT_EQ(K(a.type(tbl->lookup())), nebula::type::INTEGER);
  LOG(INFO) << "PASS: constant expr";

  // column expression
  auto cid = col("id");
  auto cflag = col("flag");
  EXPECT_EQ(K(cid.type(tbl->lookup())), nebula::type::INTEGER);
  EXPECT_EQ(K(cflag.type(tbl->lookup())), nebula::type::BOOLEAN);
  LOG(INFO) << "PASS: column expr";

  // logical expression
  // && (cid == 3);
  auto c1 = cflag == true;
  EXPECT_EQ(K(c1.type(tbl->lookup())), nebula::type::BOOLEAN);
  auto c2 = cid == 3;
  EXPECT_EQ(K(c2.type(tbl->lookup())), nebula::type::BOOLEAN);
  auto c3 = c1 && c2;
  EXPECT_EQ(K(c3.type(tbl->lookup())), nebula::type::BOOLEAN);
  // auto c4 = cid == 50;
  // EXPECT_EQ(K(c4.type(tbl->lookup())), nebula::type::BOOLEAN);
  LOG(INFO) << "PASS: logical expr";

  // arthmetic expression
  auto trans = cid * 10;
  EXPECT_EQ(K(trans.type(tbl->lookup())), nebula::type::INTEGER);
  LOG(INFO) << "PASS: arthmetic expr 1";

  auto transF = trans * 3.5f;
  EXPECT_EQ(K(transF.type(tbl->lookup())), nebula::type::REAL);
  LOG(INFO) << "PASS: arthmetic expr 2";

  // mix up arthemtic and logical
  auto mix = ((cid * 5 / 7 - 10) > 100) && cflag;
  EXPECT_EQ(K(mix.type(tbl->lookup())), nebula::type::BOOLEAN);
  LOG(INFO) << "PASS: mix arthemtic and logical";

  // test alias
  auto c = nebula::api::dsl::v("xyz").as("fake");
  EXPECT_EQ(K(c.type(tbl->lookup())), nebula::type::VARCHAR);
  LOG(INFO) << "PASS: constant string with alias";
}

TEST(ExpressionsTest, TestExpressionEval) {
  auto ms = TableService::singleton();
  auto tbl = ms->query("nebula.test").table();
  {
    auto idvalue = (col("id") * 0) != 0;
    idvalue.type(tbl->lookup());

    auto eval = idvalue.asEval();
    MockAccessor mr;
    EvalContext ctx{ false };
    ctx.reset(mr);
    auto res = eval->eval<bool>(ctx);
    EXPECT_EQ(res, false);
  }

  {
    auto eventValue = col("event") == "abc";
    eventValue.type(tbl->lookup());

    auto eval = eventValue.asEval();
    MockAccessor mr;
    EvalContext ctx{ false };
    ctx.reset(mr);
    auto res = eval->eval<bool>(ctx);
    EXPECT_EQ(res, false);
  }
}

#define VERIFY_ITEM_I(I)                                                                               \
  {                                                                                                    \
    constexpr auto item = std::get<I>(expectations);                                                   \
    constexpr nebula::type::Kind R = nebula::api::dsl::ArthmeticCombination::result(item[0], item[1]); \
    EXPECT_EQ(R, item[2]);                                                                             \
  }

TEST(ExpressionsTest, TestArthmeticTypeCombination) {
  // NOT SURE why - but we need double bracets to put the initializer list
  constexpr std::array<std::array<nebula::type::Kind, 3>, 5> expectations
    = { { { { nebula::type::Kind::TINYINT, nebula::type::Kind::INTEGER, nebula::type::Kind::INTEGER } },
          { { nebula::type::Kind::SMALLINT, nebula::type::Kind::BIGINT, nebula::type::Kind::BIGINT } },
          { { nebula::type::Kind::DOUBLE, nebula::type::Kind::BIGINT, nebula::type::Kind::DOUBLE } },
          { { nebula::type::Kind::DOUBLE, nebula::type::Kind::REAL, nebula::type::Kind::DOUBLE } },
          { { nebula::type::Kind::INTEGER, nebula::type::Kind::REAL, nebula::type::Kind::REAL } } } };

  VERIFY_ITEM_I(0)
  VERIFY_ITEM_I(1)
  VERIFY_ITEM_I(2)
  VERIFY_ITEM_I(3)
  VERIFY_ITEM_I(4)

  // check if non compile time works
  {
    auto k1 = nebula::type::Kind::SMALLINT;
    auto k2 = nebula::type::Kind::REAL;
    nebula::type::Kind R = nebula::api::dsl::ArthmeticCombination::result(k1, k2);
    LOG(INFO) << "result type: " << static_cast<size_t>(R);
  }
}

// NOTE: in the test app, the MockRow may be linked to another declaration
// in TestApi.cpp or TestValueEvalTree.cpp, better to put this in different namespace
// or abstract it in some common place for reuse.
class MockRow2 : public MockAccessor {
public:
  MockRow2() = default;

  virtual std::optional<std::string_view> readString(const std::string&) const override {
    return "abcdefg";
  }
};

TEST(ExpressionsTest, TestLikeAndPrefix) {
  // set up table for testing
  MockRow2 rowData;

  auto ms = TableService::singleton();
  auto tbl = ms->query("nebula.test").table();

  // verify the mocked data has expected result
  EXPECT_EQ(rowData.readString("event"), "abcdefg");

  EvalContext ctx{ false };
  ctx.reset(rowData);
  // evaluate like
  {
    auto eventValue = nebula::api::dsl::like(col("event"), "abc%");
    eventValue.type(tbl->lookup());

    auto eval = eventValue.asEval();
    auto res = eval->eval<bool>(ctx);
    EXPECT_EQ(res, true);
  }

  {
    auto eventValue = nebula::api::dsl::like(col("event"), "bc%");
    eventValue.type(tbl->lookup());

    auto eval = eventValue.asEval();
    auto res = eval->eval<bool>(ctx);
    EXPECT_EQ(res, false);
  }

  // evaluate prefix
  {
    auto eventValue = nebula::api::dsl::starts(col("event"), "abc");
    eventValue.type(tbl->lookup());

    auto eval = eventValue.asEval();
    auto res = eval->eval<bool>(ctx);
    EXPECT_EQ(res, true);
  }

  {
    auto eventValue = nebula::api::dsl::starts(col("event"), "bc");
    eventValue.type(tbl->lookup());

    auto eval = eventValue.asEval();
    auto res = eval->eval<bool>(ctx);
    EXPECT_EQ(res, false);
  }
}

#undef VERIFY_ITEM_I

class MockRow3 : public MockAccessor {
public:
  MockRow3() = default;

  MOCK_CONST_METHOD1(readByte, std::optional<int8_t>(const std::string&));
  MOCK_CONST_METHOD1(readInt, std::optional<int32_t>(const std::string&));
  MOCK_CONST_METHOD1(readString, std::optional<std::string_view>(const std::string&));
};

// Test serde of expressions
TEST(ExpressionsTest, TestSerde) {
  auto ms = TableService::singleton();
  auto tbl = ms->query("nebula.test").table();
  EvalContext ctx{ false };
  MockRow3 rowData;
  EXPECT_CALL(rowData, readByte(testing::_)).WillRepeatedly(testing::Return(20));
  EXPECT_CALL(rowData, readInt(testing::_)).WillRepeatedly(testing::Return(32));
  EXPECT_CALL(rowData, readString(testing::_)).WillRepeatedly(testing::Return("string"));
  ctx.reset(rowData);

  // constant expression serde - int
  {
    nebula::api::dsl::ConstExpression<int> i1(1);
    auto i1ser = Serde::serialize(i1);
    auto exp = Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(exp->alias(), i1.alias());

    // verify kind
    i1.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), i1.typeInfo());

    // verify value evaluation
    auto v1 = i1.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<int>(ctx), 1);
    EXPECT_EQ(v2->eval<int>(ctx), 1);
  }

  // constant expression serde - string
  {
    nebula::api::dsl::ConstExpression<std::string_view> s1("abc");
    auto s1ser = Serde::serialize(s1);
    auto exp = Serde::deserialize(s1ser);

    // vreify alias
    EXPECT_EQ(exp->alias(), s1.alias());

    // verify kind
    s1.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), s1.typeInfo());

    // verify value evaluation
    auto v1 = s1.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<std::string_view>(ctx), "abc");
    EXPECT_EQ(v2->eval<std::string_view>(ctx), "abc");
  }

  // column expression serde - int col
  {
    auto cid = col("id").as("cid");
    auto i1ser = Serde::serialize(cid);
    auto exp = Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cid.alias(), "cid");
    EXPECT_EQ(exp->alias(), "cid");

    // verify kind
    cid.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), cid.typeInfo());

    // verify value evaluation
    auto v1 = cid.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<int>(ctx), 32);
    EXPECT_EQ(v2->eval<int>(ctx), 32);
  }
  {
    auto cid = col("event").as("eid");
    auto i1ser = Serde::serialize(cid);
    auto exp = Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cid.alias(), "eid");
    EXPECT_EQ(exp->alias(), "eid");

    // verify kind
    cid.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), cid.typeInfo());

    // verify value evaluation
    auto v1 = cid.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<std::string_view>(ctx), "string");
    EXPECT_EQ(v2->eval<std::string_view>(ctx), "string");
  }

  // logical expression serde - int not equal
  {
    auto cid = (col("id") * 0) != 1;
    auto i1ser = Serde::serialize(cid);
    auto exp = Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cid.alias(), "id");
    EXPECT_EQ(exp->alias(), "id");

    // verify kind
    cid.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), cid.typeInfo());

    // verify value evaluation
    auto v1 = cid.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<bool>(ctx), true);
    EXPECT_EQ(v2->eval<bool>(ctx), true);
  }

  // logical expression serde - strings equal
  {
    auto cevent = col("event") == "abc";
    auto i1ser = Serde::serialize(cevent);
    auto exp = Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cevent.alias(), "event");
    EXPECT_EQ(exp->alias(), "event");

    // verify kind
    cevent.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), cevent.typeInfo());

    // verify value evaluation
    auto v1 = cevent.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<bool>(ctx), false);
    EXPECT_EQ(v2->eval<bool>(ctx), false);
  }

  // logical expression serde - double compare
  {
    auto cw = col("weight") > 1.5;
    auto i1ser = Serde::serialize(cw);
    auto exp = Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cw.alias(), "weight");
    EXPECT_EQ(exp->alias(), "weight");

    // verify kind
    cw.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), cw.typeInfo());

    // verify value evaluation
    auto v1 = cw.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<bool>(ctx), false);
    EXPECT_EQ(v2->eval<bool>(ctx), false);
  }

  // arthmetic expresssions
  {
    auto cv = col("value") * 10 + 100;
    auto i1ser = Serde::serialize(cv);
    auto exp = Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cv.alias(), "value");
    EXPECT_EQ(exp->alias(), "value");

    // verify kind
    cv.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), cv.typeInfo());

    // verify value evaluation
    auto v1 = cv.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<int>(ctx), 300);
    EXPECT_EQ(v2->eval<int>(ctx), 300);
  }

  // expression with UDF like
  {
    auto cl = nebula::api::dsl::like(col("event"), "str%").as("el");
    auto i1ser = Serde::serialize(cl);
    auto exp = Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cl.alias(), "el");
    EXPECT_EQ(exp->alias(), "el");

    // verify kind
    cl.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), cl.typeInfo());

    // verify value evaluation
    auto v1 = cl.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<bool>(ctx), true);
    EXPECT_EQ(v2->eval<bool>(ctx), true);
  }

  // expression with UDF like
  {
    std::vector<std::string> values{ "str%", "string" };
    auto ci = nebula::api::dsl::in(col("event"), std::move(values)).as("ei");
    auto i1ser = Serde::serialize(ci);
    auto exp = Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(ci.alias(), "ei");
    EXPECT_EQ(exp->alias(), "ei");

    // verify kind
    ci.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), ci.typeInfo());

    // verify value evaluation
    auto v1 = ci.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<bool>(ctx), true);
    EXPECT_EQ(v2->eval<bool>(ctx), true);
  }

  // expression with UDAF pct
  {
    LOG(INFO) << "Starting to test UDAF PCT";
    auto ci = nebula::api::dsl::pct(col("value"), 99).as("p99");
    auto i1ser = Serde::serialize(ci);
    auto exp = Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(ci.alias(), "p99");
    EXPECT_EQ(exp->alias(), "p99");

    // verify kind
    ci.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), ci.typeInfo());

    // verify value evaluation
    auto v1 = ci.asEval();

    using AggType = nebula::surface::eval::Aggregator<nebula::type::Kind::INTEGER, nebula::type::Kind::INTEGER>;
    auto s1 = std::static_pointer_cast<AggType>(
      v1->sketch<nebula::type::Kind::INTEGER, nebula::type::Kind::INTEGER>());
    for (int i = 0; i <= 100; ++i) {
      s1->merge(i);
    }

    auto pv = s1->finalize();
    EXPECT_NEAR(pv, 99, 1);
  }

  // expression with UDAF hist
  {
    LOG(INFO) << "Starting to test UDAF HIST";
    auto ci = nebula::api::dsl::hist(nebula::api::dsl::col("weight"), 30.0, 80.0).as("hist");
    auto i1ser = nebula::api::dsl::Serde::serialize(ci);
    auto exp = nebula::api::dsl::Serde::deserialize(i1ser);
    LOG(INFO) << "exp deserialized: " << exp;

    // vreify alias
    EXPECT_EQ(ci.alias(), "hist");
    EXPECT_EQ(exp->alias(), "hist");

    // verify kind
    ci.type(tbl->lookup());
    exp->type(tbl->lookup());
    EXPECT_EQ(exp->typeInfo(), ci.typeInfo());

    // verify value evaluation
    auto v1 = ci.asEval();

    using AggType = nebula::surface::eval::Aggregator<nebula::type::Kind::VARCHAR, nebula::type::Kind::DOUBLE>;
    auto s1 = std::static_pointer_cast<AggType>(
      v1->sketch<nebula::type::Kind::VARCHAR, nebula::type::Kind::DOUBLE>());
    for (int i = 0; i <= 100; i += 20) {
      s1->merge(i);
    }
    auto expectedJson = "{\"b\":[[2.2250738585072014e-308,30.0,2,20.0],[30.0,32.5,0,0.0],[32.5,35.0,0,0.0],[35.0,37.5,0,0.0],[37.5,40.0,0,0.0],[40.0,42.5,1,40.0],[42.5,45.0,0,0.0],[45.0,47.5,0,0.0],[47.5,50.0,0,0.0],[50.0,52.5,0,0.0],[52.5,55.0,0,0.0],[55.0,57.5,0,0.0],[57.5,60.0,0,0.0],[60.0,62.5,1,60.0],[62.5,65.0,0,0.0],[65.0,67.5,0,0.0],[67.5,70.0,0,0.0],[70.0,72.5,0,0.0],[72.5,75.0,0,0.0],[75.0,77.5,0,0.0],[77.5,80.0,0,0.0],[80.0,1.7976931348623157e308,2,180.0]]}";
    std::ostringstream finalizedStr;
    LOG(INFO) << "final string in test: " << s1->finalize();
    finalizedStr << s1->finalize();
    EXPECT_EQ(finalizedStr.str(), expectedJson);
  }
}

TEST(ExpressionsTest, TestBetweenExpression) {
  auto ms = TableService::singleton();
  auto tbl = ms->query("nebula.test").table();

  // set up table for testing
  MockRow3 rowData;
  EvalContext ctx{ false };
  ctx.reset(rowData);

  // define expression
  auto expr = between(col("id"), 3, 10);
  expr.type(tbl->lookup());

  // id will be valued as 5
  {
    EXPECT_CALL(rowData, readInt("id")).WillRepeatedly(testing::Return(5));
    auto result = expr.asEval()->eval<bool>(ctx);
    EXPECT_TRUE(result != std::nullopt);
    EXPECT_EQ(result.value(), true);
  }

  // id will be valued as 2 out of range
  {
    EXPECT_CALL(rowData, readInt("id")).WillRepeatedly(testing::Return(2));
    auto result = expr.asEval()->eval<bool>(ctx);
    EXPECT_TRUE(result != std::nullopt);
    EXPECT_EQ(result.value(), false);
  }

  // id will be valued as 11 out of range
  {
    EXPECT_CALL(rowData, readInt("id")).WillRepeatedly(testing::Return(11));
    auto result = expr.asEval()->eval<bool>(ctx);
    EXPECT_TRUE(result != std::nullopt);
    EXPECT_EQ(result.value(), false);
  }

  // id will be valued as null
  {
    EXPECT_CALL(rowData, readInt("id")).WillRepeatedly(testing::Return(std::nullopt));
    auto result = expr.asEval()->eval<bool>(ctx);
    EXPECT_TRUE(result == std::nullopt);
  }

  // serialize this expression
  {
    EXPECT_CALL(rowData, readInt("id")).WillRepeatedly(testing::Return(3));
    auto s = Serde::serialize(expr);
    auto x = Serde::deserialize(s);
    x->type(tbl->lookup());
    auto result = x->asEval()->eval<bool>(ctx);
    EXPECT_TRUE(result != std::nullopt);
    EXPECT_TRUE(result.value());
  }
}

} // namespace test
} // namespace api
} // namespace nebula