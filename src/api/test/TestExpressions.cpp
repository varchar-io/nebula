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
  bool operator==(T v) const {
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
  MockMs ms;
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

#undef VERIFY_ITEM_I

} // namespace test
} // namespace api
} // namespace nebula