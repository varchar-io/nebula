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

#include "api/dsl/Expressions.h"
#include "api/udf/Avg.h"
#include "api/udf/Count.h"
#include "api/udf/In.h"
#include "api/udf/Like.h"
#include "api/udf/Not.h"
#include "api/udf/Prefix.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "surface/eval/ValueEval.h"

namespace nebula {
namespace api {
namespace test {

TEST(UDFTest, TestNot) {
  nebula::surface::MockRowData row;
  nebula::surface::eval::EvalContext ctx;
  ctx.reset(row);

  auto f = std::make_shared<nebula::api::dsl::ConstExpression<bool>>(false);
  nebula::api::udf::Not n("n", f->asEval());
  bool valid = true;
  EXPECT_EQ(n.eval(ctx, valid), true);

  auto t = std::make_shared<nebula::api::dsl::ConstExpression<bool>>(true);
  nebula::api::udf::Not y("n", t->asEval());
  valid = true;
  EXPECT_EQ(y.eval(ctx, valid), false);
}

TEST(UDFTest, TestLike) {
  std::vector<std::tuple<std::string, std::string, bool>> data{
    { "a", "a%", true },
    { "a", "%a%", true },
    { "a", "%a", true },
    { "abc", "abc%", true },
    { "abc", "%abc%", true },
    { "abc", "%abc", true },
    { "abcdefg", "abc%", true },
    { "Shawn says hi", "%says%", true },
    { "long time no see", "%see", true },
    { "nebula is cool", "%is", false },
    { "nebula is awesome", "nebula%", true },
    { "hi there ", "%i th%", true },
    { "hi there ", "i th%", false },
    { "hi there", "%there", true },
    { "easy dessert pizza recipe", "%recipe%", true }
  };
  nebula::surface::MockRowData row;
  nebula::surface::eval::EvalContext ctx;
  ctx.reset(row);

  for (const auto& item : data) {
    const auto& s = std::get<0>(item);
    const auto& p = std::get<1>(item);
    auto r = std::get<2>(item);
    LOG(INFO) << "Match " << s << " with " << p << " is " << r;
    auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(s);
    nebula::api::udf::Like l("l", c->asEval(), p);
    bool valid = true;
    EXPECT_EQ(l.eval(ctx, valid), r);
  }
}

TEST(UDFTest, TestILike) {
  std::vector<std::tuple<std::string, std::string, bool>> data{
    { "a", "A%", true },
    { "a", "%A%", true },
    { "a", "%A", true },
    { "abc", "aBc%", true },
    { "abc", "%abC%", true },
    { "abc", "%abc", true },
    { "abcdefg", "AbC%", true },
    { "Shawn says hi", "%saYs%", true },
    { "long time no see", "%sEe", true },
    { "nebula is cool", "%iS", false },
    { "nebula is awesome", "nEbUla%", true },
    { "hi there ", "%i Th%", true },
    { "hi there ", "i tH%", false },
    { "hi there", "%thEre", true },
    { "easy dessert pizza recipe", "%rEcIpe%", true }
  };
  nebula::surface::MockRowData row;
  nebula::surface::eval::EvalContext ctx;
  ctx.reset(row);

  for (const auto& item : data) {
    const auto& s = std::get<0>(item);
    const auto& p = std::get<1>(item);
    auto r = std::get<2>(item);
    LOG(INFO) << "Match " << s << " with " << p << " is " << r;
    auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(s);
    nebula::api::udf::Like l("l", c->asEval(), p, false);
    bool valid = true;
    EXPECT_EQ(l.eval(ctx, valid), r);
  }
}

TEST(UDFTest, TestPrefix) {
  std::vector<std::tuple<std::string, std::string, bool>> data{
    { "abcdefg", "abc", true },
    { "Shawn says hi", "says", false },
    { "long time no see", "long time", true },
    { "nebula is cool", "is", false },
    { "nebula is awesome", "nebula", true },
    { "hi there ", "%i th", false },
    { "hi there ", "i th", false },
    { "hi there", "hi there", true }
  };
  nebula::surface::MockRowData row;
  nebula::surface::eval::EvalContext ctx;
  ctx.reset(row);

  for (const auto& item : data) {
    const auto& s = std::get<0>(item);
    const auto& p = std::get<1>(item);
    auto r = std::get<2>(item);
    LOG(INFO) << "Match " << s << " with " << p << " is " << r;
    auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(s);
    nebula::api::udf::Prefix prefix("p", c->asEval(), p);
    bool valid = true;
    EXPECT_EQ(prefix.eval(ctx, valid), r);
  }
}

TEST(UDFTest, TestIPrefix) {
  std::vector<std::tuple<std::string, std::string, bool>> data{
    { "Abcdefg", "aBc", true },
    { "Shawn says hi", "Says", false },
    { "loNg tiMe no see", "lonG tIme", true },
    { "NebUla is awesome", "neBulA", true },
    { "Hi there", "hI tHere", true }
  };
  nebula::surface::MockRowData row;
  nebula::surface::eval::EvalContext ctx;
  ctx.reset(row);

  for (const auto& item : data) {
    const auto& s = std::get<0>(item);
    const auto& p = std::get<1>(item);
    auto r = std::get<2>(item);
    LOG(INFO) << "Match " << s << " with " << p << " is " << r;
    auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(s);
    nebula::api::udf::Prefix prefix("p", c->asEval(), p, false);
    bool valid = true;
    EXPECT_EQ(prefix.eval(ctx, valid), r);
  }
}

TEST(UDFTest, TestPrefixContext) {
  std::vector<std::tuple<std::string, std::string, bool>> data{
    { "abcdefg", "abc", true },
  };

  nebula::surface::MockRowData row;
  nebula::surface::eval::EvalContext context;
  context.reset(row);

  auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>("abcdfeg");
  nebula::api::udf::Prefix prefix("p", c->asEval(), "abc");
  for (auto i = 0; i < 1000; ++i) {
    bool valid = true;
    // auto result = prefix.eval(row, valid);
    auto result = context.eval<bool>(prefix, valid);
    EXPECT_EQ(valid, true);
    EXPECT_EQ(result, true);
  }
}

TEST(UDFTest, TestIn) {
  {
    // set, target, in or not in, result
    using S = std::vector<std::string>;
    std::vector<std::tuple<S, std::string, bool, bool>> data{
      { { "abcdefg", "abc" }, "abc", true, true },
      { { "x", "y", "z" }, "x", true, true },
      { { "x", "y", "z" }, "a", true, false },
      { { "x", "y", "z" }, "x", false, false },
      { { "x", "y", "z" }, "a", false, true },
      { { "x", "y", "z" }, "z", false, false },
    };

    nebula::surface::MockRowData row;
    nebula::surface::eval::EvalContext ctx;
    ctx.reset(row);

    for (const auto& item : data) {
      const auto& s = std::get<0>(item);
      const auto& t = std::get<1>(item);
      const auto& f = std::get<2>(item);
      const auto& r = std::get<3>(item);

      auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(t);
      if (f) {
        nebula::api::udf::In<nebula::type::Kind::VARCHAR> in("i", c->asEval(), s);
        bool valid = true;
        EXPECT_EQ(in.eval(ctx, valid), r);
      } else {
        nebula::api::udf::In<nebula::type::Kind::VARCHAR> in("i", c->asEval(), s, f);
        bool valid = true;
        EXPECT_EQ(in.eval(ctx, valid), r);
      }
    }
  }

  {
    // set, target, in or not in, result
    using S = std::vector<int32_t>;
    std::vector<std::tuple<S, int32_t, bool, bool>> data{
      { { 0, 1, 2 }, 1, true, true },
      { { 66, 73, 54 }, 54, true, true },
      { { 23, 45, 67, 89 }, 11, true, false },
      { { 11, 22, 33 }, 22, false, false },
      { { 11, 22, 33 }, 44, false, true },
      { { 23, 34, 45, 56 }, 45, false, false },
    };

    nebula::surface::MockRowData row;
    nebula::surface::eval::EvalContext ctx;
    ctx.reset(row);

    for (const auto& item : data) {
      const auto& s = std::get<0>(item);
      const auto& t = std::get<1>(item);
      const auto& f = std::get<2>(item);
      const auto& r = std::get<3>(item);

      auto c = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(t);
      if (f) {
        nebula::api::udf::In<nebula::type::Kind::INTEGER> in("i", c->asEval(), s);
        bool valid = true;
        EXPECT_EQ(in.eval(ctx, valid), r);
      } else {
        nebula::api::udf::In<nebula::type::Kind::INTEGER> in("i", c->asEval(), s, f);
        bool valid = true;
        EXPECT_EQ(in.eval(ctx, valid), r);
      }
    }
  }
}

TEST(UDFTest, TestCount) {

  using CType = nebula::api::udf::Count<nebula::type::Kind::INTEGER>;

  // simulate the run times 11 for c1 and 22 for c2
  CType::StoreType count1 = 0;
  nebula::surface::eval::EvalContext ctx;
  bool invalid;
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(0);
  CType cf("count", v9->asEval());

  for (auto i = 0; i < 11; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ci("count1", vi->asEval());
    count1 = cf.merge(count1, ci.eval(ctx, invalid));
  }

  CType::StoreType count2 = 0;
  for (auto i = 0; i < 22; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ci("count2", vi->asEval());
    count2 = cf.merge(count2, ci.eval(ctx, invalid));
  }

  // partial merge
  CType c3("count3", v9->asEval());
  CType::StoreType count3 = c3.merge(count1, count2);

  // we will ask itself for finalize
  CType::NativeType count4 = c3.finalize(count3);
  EXPECT_EQ(count4, 33);
}

TEST(UDFTest, TestSum) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(0);
  using CType = nebula::api::udf::Sum<nebula::type::Kind::INTEGER>;
  nebula::surface::eval::EvalContext ctx;
  bool invalid;
  CType sf("sum", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  CType::NativeType expected = 0;
  CType::StoreType sum1 = 0;
  for (auto i = 0; i < 11; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType si("sum1", vi->asEval());

    sum1 = sf.merge(sum1, si.eval(ctx, invalid));
    expected += i;
  }

  CType::StoreType sum2 = 0;
  for (auto i = 0; i < 22; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType si("sum2", vi->asEval());

    sum2 = sf.merge(sum2, si.eval(ctx, invalid));
    expected += i;
  }

  // partial merge
  CType s3("sum3", v9->asEval());
  CType::StoreType sum3 = s3.merge(sum1, sum2);

  // we will ask itself for finalize
  CType::NativeType sum4 = s3.finalize(sum3);
  EXPECT_EQ(sum4, expected);
}

TEST(UDFTest, TestMin) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(0);
  using CType = nebula::api::udf::Min<nebula::type::Kind::INTEGER>;
  nebula::surface::eval::EvalContext ctx;
  bool invalid;
  CType mf("min", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  CType::NativeType expected = 0;
  CType::StoreType min1 = 0;
  for (auto i = 0; i < 11; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType mi("min1", vi->asEval());

    min1 = mf.merge(min1, mi.eval(ctx, invalid));
    expected = std::min(i, expected);
  }

  CType::StoreType min2 = 0;
  for (auto i = 0; i < 22; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType mi("min2", vi->asEval());

    min1 = mf.merge(min1, mi.eval(ctx, invalid));
    expected = std::min(i, expected);
  }

  // partial merge
  CType m3("min3", v9->asEval());
  CType::StoreType min3 = m3.merge(min1, min2);

  // we will ask itself for finalize
  CType::NativeType min4 = m3.finalize(min3);
  EXPECT_EQ(min4, expected);
}

TEST(UDFTest, TestMax) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(0);
  using CType = nebula::api::udf::Max<nebula::type::Kind::INTEGER>;
  nebula::surface::eval::EvalContext ctx;
  bool invalid;
  CType mf("max", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  CType::NativeType expected = 0;
  CType::StoreType max1 = 0;
  for (auto i = 0; i < 11; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType mi("max1", vi->asEval());
    max1 = mf.merge(max1, mi.eval(ctx, invalid));
    expected = std::max(i, expected);
  }

  CType::StoreType max2 = 0;
  for (auto i = 0; i < 22; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType mi("max1", vi->asEval());
    max2 = mf.merge(max2, mi.eval(ctx, invalid));
    expected = std::max(i, expected);
  }

  // partial merge
  CType m3("max3", v9->asEval());
  CType::StoreType max3 = m3.merge(max1, max2);

  // we will ask itself for finalize
  CType::NativeType max4 = m3.finalize(max3);
  EXPECT_EQ(max4, expected);
}

TEST(UDFTest, TestAvg) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(0);
  using CType = nebula::api::udf::Avg<nebula::type::Kind::INTEGER>;
  nebula::surface::eval::EvalContext ctx;
  bool invalid;
  CType af("avg", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  CType::NativeType sum = 0;
  CType::NativeType count = 0;

  CType::StoreType avg1 = 0;
  for (auto i = 0; i < 11; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ai("avg1", vi->asEval());
    avg1 = af.merge(avg1, ai.eval(ctx, invalid));

    sum += i;
    count += 1;
  }

  CType::StoreType avg2 = 0;
  for (auto i = 0; i < 22; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ai("avg2", vi->asEval());
    avg2 = af.merge(avg2, ai.eval(ctx, invalid));

    sum += i;
    count += 1;
  }

  // partial merge
  CType a3("avg3", v9->asEval());
  CType::StoreType avg3 = a3.merge(avg1, avg2);

  // we will ask itself for finalize
  CType::NativeType avg4 = a3.finalize(avg3);
  EXPECT_EQ(avg4, sum / count);
}

TEST(UDFTest, TestAvgByte) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int8_t>>(0);
  using CType = nebula::api::udf::Avg<nebula::type::Kind::TINYINT>;
  nebula::surface::eval::EvalContext ctx;
  bool invalid;
  CType af("avg", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  int64_t sum = 0;
  int64_t count = 0;

  CType::StoreType avg1 = 0;
  for (auto i = 0; i < 127; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ai("avg2", vi->asEval());
    avg1 = af.merge(avg1, ai.eval(ctx, invalid));

    sum += i;
    count += 1;
  }

  CType::StoreType avg2 = 0;
  for (auto i = 0; i < 127; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ai("avg2", vi->asEval());
    avg2 = af.merge(avg2, ai.eval(ctx, invalid));

    sum += i;
    count += 1;
  }

  // partial merge
  CType a3("avg3", v9->asEval());
  CType::StoreType avg3 = a3.merge(avg1, avg2);

  // we will ask itself for finalize
  CType::NativeType avg4 = a3.finalize(avg3);
  EXPECT_EQ(avg4, sum / count);
}

// TEST(UDFTest, TestTDigest) {
//   using CType = nebula::api::udf::TDigest<nebula::type::Kind::VARCHAR, nebula::type::Kind::INTEGER>;

//   // simulate the run times 11 for c1 and 22 for c2
//   nebula::surface::eval::EvalContext ctx;
//   bool invalid;
//   auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(0);
//   CType td1("td1", v9->asEval());

//   for (auto i = 0; i < 11; ++i) {
//     auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
//     CType ci("count1", vi->asEval());
//     td1 = td1.merge(td1, ci.eval(ctx, invalid));
//   }

//   CType::StoreType count2 = 0;
//   for (auto i = 0; i < 22; ++i) {
//     auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
//     CType ci("count2", vi->asEval());
//     count2 = cf.merge(count2, ci.eval(ctx, invalid));
//   }

//   // partial merge
//   CType c3("count3", v9->asEval());
//   CType::StoreType count3 = c3.merge(count1, count2);

//   // we will ask itself for finalize
//   CType::NativeType count4 = c3.finalize(count3);
//   EXPECT_EQ(count4, 33);
// }

} // namespace test
} // namespace api
} // namespace nebula