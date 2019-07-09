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
#include "api/dsl/Dsl.h"
#include "api/dsl/Expressions.h"
#include "api/dsl/Serde.h"
#include "common/Cursor.h"
#include "common/Errors.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "execution/ExecutionPlan.h"
#include "execution/eval/ValueEval.h"
#include "execution/meta/TableService.h"
#include "fmt/format.h"
#include "gmock/gmock.h"
#include "meta/TestTable.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace api {
namespace test {

using nebula::execution::meta::TableService;

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
  TableService ms;
  auto tbl = ms.query("nebula.test");

  // constant type
  nebula::api::dsl::ConstExpression<int> a(1);
  auto K = [](const nebula::type::TreeNode& node) { return nebula::type::TypeBase::k(node); };

  EXPECT_EQ(K(a.type(*tbl)), nebula::type::INTEGER);
  LOG(INFO) << "PASS: constant expr";

  // column expression
  auto cid = nebula::api::dsl::col("id");
  auto cflag = nebula::api::dsl::col("flag");
  EXPECT_EQ(K(cid.type(*tbl)), nebula::type::INTEGER);
  EXPECT_EQ(K(cflag.type(*tbl)), nebula::type::BOOLEAN);
  LOG(INFO) << "PASS: column expr";

  // logical expression
  // && (cid == 3);
  auto c1 = cflag == true;
  EXPECT_EQ(K(c1.type(*tbl)), nebula::type::BOOLEAN);
  auto c2 = cid == 3;
  EXPECT_EQ(K(c2.type(*tbl)), nebula::type::BOOLEAN);
  auto c3 = c1 && c2;
  EXPECT_EQ(K(c3.type(*tbl)), nebula::type::BOOLEAN);
  // auto c4 = cid == 50;
  // EXPECT_EQ(K(c4.type(*tbl)), nebula::type::BOOLEAN);
  LOG(INFO) << "PASS: logical expr";

  // arthmetic expression
  auto trans = cid * 10;
  EXPECT_EQ(K(trans.type(*tbl)), nebula::type::INTEGER);
  LOG(INFO) << "PASS: arthmetic expr 1";

  auto transF = trans * 3.5f;
  EXPECT_EQ(K(transF.type(*tbl)), nebula::type::REAL);
  LOG(INFO) << "PASS: arthmetic expr 2";

  // mix up arthemtic and logical
  auto mix = ((cid * 5 / 7 - 10) > 100) && cflag;
  EXPECT_EQ(K(mix.type(*tbl)), nebula::type::BOOLEAN);
  LOG(INFO) << "PASS: mix arthemtic and logical";

  // test alias
  auto c = nebula::api::dsl::v("xyz").as("fake");
  EXPECT_EQ(K(c.type(*tbl)), nebula::type::VARCHAR);
  LOG(INFO) << "PASS: constant string with alias";
}

TEST(ExpressionsTest, TestExpressionEval) {
  TableService ms;
  auto tbl = ms.query("nebula.test");
  {
    auto idvalue = (nebula::api::dsl::col("id") * 0) != 0;
    idvalue.type(*tbl);

    auto eval = idvalue.asEval();
    nebula::surface::MockRowData mr;
    nebula::execution::eval::EvalContext ctx;
    ctx.reset(mr);
    bool valid = true;
    bool res = eval->eval<bool>(ctx, valid);
    EXPECT_EQ(res, false);
  }

  {
    auto eventValue = nebula::api::dsl::col("event") == "abc";
    eventValue.type(*tbl);

    auto eval = eventValue.asEval();
    nebula::surface::MockRowData mr;
    nebula::execution::eval::EvalContext ctx;
    ctx.reset(mr);
    bool valid = true;
    bool res = eval->eval<bool>(ctx, valid);
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
class MockRow2 : public nebula::surface::MockRowData {
public:
  MockRow2() = default;

  virtual std::string_view readString(const std::string&) const override {
    return "abcdefg";
  }
  virtual std::string_view readString(size_t) const override {
    return "abcdefg";
  }
  MOCK_CONST_METHOD1(isNull, bool(const std::string&));
};

TEST(ExpressionsTest, TestLikeAndPrefix) {
  // set up table for testing
  MockRow2 rowData;

  EXPECT_CALL(rowData, isNull(testing::_)).WillRepeatedly(testing::Return(false));

  TableService ms;
  auto tbl = ms.query("nebula.test");

  // verify the mocked data has expected result
  EXPECT_EQ(rowData.readString("event"), "abcdefg");

  nebula::execution::eval::EvalContext ctx;
  ctx.reset(rowData);
  // evaluate like
  {
    auto eventValue = nebula::api::dsl::like(nebula::api::dsl::col("event"), "abc%");
    eventValue.type(*tbl);

    auto eval = eventValue.asEval();
    bool valid = true;
    bool res = eval->eval<bool>(ctx, valid);
    EXPECT_EQ(res, true);
  }

  {
    auto eventValue = nebula::api::dsl::like(nebula::api::dsl::col("event"), "bc%");
    eventValue.type(*tbl);

    auto eval = eventValue.asEval();
    bool valid = true;
    bool res = eval->eval<bool>(ctx, valid);
    EXPECT_EQ(res, false);
  }

  // evaluate prefix
  {
    auto eventValue = nebula::api::dsl::starts(nebula::api::dsl::col("event"), "abc");
    eventValue.type(*tbl);

    auto eval = eventValue.asEval();
    bool valid = true;
    bool res = eval->eval<bool>(ctx, valid);
    EXPECT_EQ(res, true);
  }

  {
    auto eventValue = nebula::api::dsl::starts(nebula::api::dsl::col("event"), "bc");
    eventValue.type(*tbl);

    auto eval = eventValue.asEval();
    bool valid = true;
    bool res = eval->eval<bool>(ctx, valid);
    EXPECT_EQ(res, false);
  }
}

#undef VERIFY_ITEM_I

class MockRow3 : public nebula::surface::MockRowData {
public:
  MockRow3() = default;

  MOCK_CONST_METHOD1(readByte, int8_t(const std::string&));
  MOCK_CONST_METHOD1(readInt, int32_t(const std::string&));
  MOCK_CONST_METHOD1(readString, std::string_view(const std::string&));
  MOCK_CONST_METHOD1(isNull, bool(const std::string&));
};

// Test serde of expressions
TEST(ExpressionsTest, TestSerde) {
  TableService ms;
  auto tbl = ms.query("nebula.test");
  nebula::execution::eval::EvalContext ctx;
  MockRow3 rowData;
  EXPECT_CALL(rowData, isNull(testing::_)).WillRepeatedly(testing::Return(false));
  EXPECT_CALL(rowData, readByte(testing::_)).WillRepeatedly(testing::Return(20));
  EXPECT_CALL(rowData, readInt(testing::_)).WillRepeatedly(testing::Return(32));
  EXPECT_CALL(rowData, readString(testing::_)).WillRepeatedly(testing::Return("string"));
  ctx.reset(rowData);

  // constant expression serde - int
  {
    nebula::api::dsl::ConstExpression<int> i1(1);
    auto i1ser = nebula::api::dsl::Serde::serialize(i1);
    auto exp = nebula::api::dsl::Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(exp->alias(), i1.alias());

    // verify kind
    i1.type(*tbl);
    exp->type(*tbl);
    EXPECT_EQ(exp->kind(), i1.kind());

    // verify value evaluation
    bool valid = true;
    auto v1 = i1.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<int>(ctx, valid), 1);
    EXPECT_EQ(valid, true);
    EXPECT_EQ(v2->eval<int>(ctx, valid), 1);
    EXPECT_EQ(valid, true);
  }

  // constant expression serde - string
  {
    nebula::api::dsl::ConstExpression<std::string_view> s1("abc");
    auto s1ser = nebula::api::dsl::Serde::serialize(s1);
    auto exp = nebula::api::dsl::Serde::deserialize(s1ser);

    // vreify alias
    EXPECT_EQ(exp->alias(), s1.alias());

    // verify kind
    s1.type(*tbl);
    exp->type(*tbl);
    EXPECT_EQ(exp->kind(), s1.kind());

    // verify value evaluation
    bool valid = true;
    auto v1 = s1.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<std::string_view>(ctx, valid), "abc");
    EXPECT_EQ(valid, true);
    EXPECT_EQ(v2->eval<std::string_view>(ctx, valid), "abc");
    EXPECT_EQ(valid, true);
  }

  // column expression serde - int col
  {
    auto cid = nebula::api::dsl::col("id").as("cid");
    auto i1ser = nebula::api::dsl::Serde::serialize(cid);
    auto exp = nebula::api::dsl::Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cid.alias(), "cid");
    EXPECT_EQ(exp->alias(), "cid");

    // verify kind
    cid.type(*tbl);
    exp->type(*tbl);
    EXPECT_EQ(exp->kind(), cid.kind());

    // verify value evaluation
    bool valid = true;
    auto v1 = cid.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<int>(ctx, valid), 32);
    EXPECT_EQ(valid, true);
    EXPECT_EQ(v2->eval<int>(ctx, valid), 32);
    EXPECT_EQ(valid, true);
  }
  {
    auto cid = nebula::api::dsl::col("event").as("eid");
    auto i1ser = nebula::api::dsl::Serde::serialize(cid);
    auto exp = nebula::api::dsl::Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cid.alias(), "eid");
    EXPECT_EQ(exp->alias(), "eid");

    // verify kind
    cid.type(*tbl);
    exp->type(*tbl);
    EXPECT_EQ(exp->kind(), cid.kind());

    // verify value evaluation
    bool valid = true;
    auto v1 = cid.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<std::string_view>(ctx, valid), "string");
    EXPECT_EQ(valid, true);
    EXPECT_EQ(v2->eval<std::string_view>(ctx, valid), "string");
    EXPECT_EQ(valid, true);
  }

  // logical expression serde - int not equal
  {
    auto cid = (nebula::api::dsl::col("id") * 0) != 1;
    auto i1ser = nebula::api::dsl::Serde::serialize(cid);
    auto exp = nebula::api::dsl::Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cid.alias(), "id");
    EXPECT_EQ(exp->alias(), "id");

    // verify kind
    cid.type(*tbl);
    exp->type(*tbl);
    EXPECT_EQ(exp->kind(), cid.kind());

    // verify value evaluation
    bool valid = true;
    auto v1 = cid.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<bool>(ctx, valid), true);
    EXPECT_EQ(valid, true);
    EXPECT_EQ(v2->eval<bool>(ctx, valid), true);
    EXPECT_EQ(valid, true);
  }

  // logical expression serde - strings equal
  {
    auto cevent = nebula::api::dsl::col("event") == "abc";
    auto i1ser = nebula::api::dsl::Serde::serialize(cevent);
    auto exp = nebula::api::dsl::Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cevent.alias(), "event");
    EXPECT_EQ(exp->alias(), "event");

    // verify kind
    cevent.type(*tbl);
    exp->type(*tbl);
    EXPECT_EQ(exp->kind(), cevent.kind());

    // verify value evaluation
    bool valid = true;
    auto v1 = cevent.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<bool>(ctx, valid), false);
    EXPECT_EQ(valid, true);
    EXPECT_EQ(v2->eval<bool>(ctx, valid), false);
    EXPECT_EQ(valid, true);
  }

  // arthmetic expresssions
  {
    auto cv = nebula::api::dsl::col("value") * 10 + 100;
    auto i1ser = nebula::api::dsl::Serde::serialize(cv);
    auto exp = nebula::api::dsl::Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cv.alias(), "value");
    EXPECT_EQ(exp->alias(), "value");

    // verify kind
    cv.type(*tbl);
    exp->type(*tbl);
    EXPECT_EQ(exp->kind(), cv.kind());

    // verify value evaluation
    bool valid = true;
    auto v1 = cv.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<int>(ctx, valid), 300);
    EXPECT_EQ(valid, true);
    EXPECT_EQ(v2->eval<int>(ctx, valid), 300);
    EXPECT_EQ(valid, true);
  }

  // expression with UDF like
  {
    auto cl = nebula::api::dsl::like(nebula::api::dsl::col("event"), "str%").as("el");
    auto i1ser = nebula::api::dsl::Serde::serialize(cl);
    auto exp = nebula::api::dsl::Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(cl.alias(), "el");
    EXPECT_EQ(exp->alias(), "el");

    // verify kind
    cl.type(*tbl);
    exp->type(*tbl);
    EXPECT_EQ(exp->kind(), cl.kind());

    // verify value evaluation
    bool valid = true;
    auto v1 = cl.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<bool>(ctx, valid), true);
    EXPECT_EQ(valid, true);
    EXPECT_EQ(v2->eval<bool>(ctx, valid), true);
    EXPECT_EQ(valid, true);
  }

  // expression with UDF like
  {
    std::vector<std::string> values{ "str%", "string" };
    auto ci = nebula::api::dsl::in(nebula::api::dsl::col("event"), values).as("ei");
    auto i1ser = nebula::api::dsl::Serde::serialize(ci);
    auto exp = nebula::api::dsl::Serde::deserialize(i1ser);

    // vreify alias
    EXPECT_EQ(ci.alias(), "ei");
    EXPECT_EQ(exp->alias(), "ei");

    // verify kind
    ci.type(*tbl);
    exp->type(*tbl);
    EXPECT_EQ(exp->kind(), ci.kind());

    // verify value evaluation
    bool valid = true;
    auto v1 = ci.asEval();
    auto v2 = exp->asEval();

    EXPECT_EQ(v1->eval<bool>(ctx, valid), true);
    EXPECT_EQ(valid, true);
    EXPECT_EQ(v2->eval<bool>(ctx, valid), true);
    EXPECT_EQ(valid, true);
  }
}

} // namespace test
} // namespace api
} // namespace nebula