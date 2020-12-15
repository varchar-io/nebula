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
#include <rapidjson/document.h>

#include "api/dsl/Expressions.h"
#include "api/udf/Avg.h"
#include "api/udf/Between.h"
#include "api/udf/Cardinality.h"
#include "api/udf/Count.h"
#include "api/udf/In.h"
#include "api/udf/Like.h"
#include "api/udf/Not.h"
#include "api/udf/Pct.h"
#include "api/udf/Prefix.h"
#include "api/udf/Tpm.h"
#include "api/udf/Histogram.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "surface/eval/ValueEval.h"

namespace nebula {
namespace api {
namespace test {

using nebula::common::unordered_set;

TEST(UDFTest, TestNot) {
  nebula::surface::MockAccessor row;
  nebula::surface::eval::EvalContext ctx{ false };
  ctx.reset(row);

  auto f = std::make_shared<nebula::api::dsl::ConstExpression<bool>>(false);
  nebula::api::udf::Not n("n", f->asEval());
  EXPECT_EQ(n.eval(ctx), true);

  auto t = std::make_shared<nebula::api::dsl::ConstExpression<bool>>(true);
  nebula::api::udf::Not y("n", t->asEval());
  EXPECT_EQ(y.eval(ctx), false);
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
  nebula::surface::MockAccessor row;
  nebula::surface::eval::EvalContext ctx{ false };
  ctx.reset(row);

  for (const auto& item : data) {
    const auto& s = std::get<0>(item);
    const auto& p = std::get<1>(item);
    auto r = std::get<2>(item);
    LOG(INFO) << "Match " << s << " with " << p << " is " << r;
    auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(s);
    nebula::api::udf::Like l("l", c->asEval(), p);
    EXPECT_EQ(l.eval(ctx), r);
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
  nebula::surface::MockAccessor row;
  nebula::surface::eval::EvalContext ctx{ false };
  ctx.reset(row);

  for (const auto& item : data) {
    const auto& s = std::get<0>(item);
    const auto& p = std::get<1>(item);
    auto r = std::get<2>(item);
    LOG(INFO) << "Match " << s << " with " << p << " is " << r;
    auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(s);
    nebula::api::udf::Like l("l", c->asEval(), p, false);
    EXPECT_EQ(l.eval(ctx), r);
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
  nebula::surface::MockAccessor row;
  nebula::surface::eval::EvalContext ctx{ false };
  ctx.reset(row);

  for (const auto& item : data) {
    const auto& s = std::get<0>(item);
    const auto& p = std::get<1>(item);
    auto r = std::get<2>(item);
    LOG(INFO) << "Match " << s << " with " << p << " is " << r;
    auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(s);
    nebula::api::udf::Prefix prefix("p", c->asEval(), p);
    EXPECT_EQ(prefix.eval(ctx), r);
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
  nebula::surface::MockAccessor row;
  nebula::surface::eval::EvalContext ctx{ false };
  ctx.reset(row);

  for (const auto& item : data) {
    const auto& s = std::get<0>(item);
    const auto& p = std::get<1>(item);
    auto r = std::get<2>(item);
    LOG(INFO) << "Match " << s << " with " << p << " is " << r;
    auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(s);
    nebula::api::udf::Prefix prefix("p", c->asEval(), p, false);
    EXPECT_EQ(prefix.eval(ctx), r);
  }
}

TEST(UDFTest, TestPrefixContext) {
  std::vector<std::tuple<std::string, std::string, bool>> data{
    { "abcdefg", "abc", true },
  };

  nebula::surface::MockAccessor row;
  nebula::surface::eval::EvalContext ctx{ false };
  ctx.reset(row);

  auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>("abcdfeg");
  nebula::api::udf::Prefix prefix("p", c->asEval(), "abc");
  for (auto i = 0; i < 1000; ++i) {
    // auto result = prefix.eval(row, valid);
    auto result = prefix.eval(ctx);
    EXPECT_EQ(result, true);
  }
}

TEST(UDFTest, TestBetween) {
  nebula::surface::MockAccessor row;
  nebula::surface::eval::EvalContext ctx{ false };
  ctx.reset(row);

  auto count = 0;
  for (auto i = 0; i < 20; ++i) {
    auto c = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    nebula::api::udf::Between<nebula::type::Kind::INTEGER> between("b", c, 3, 8);
    auto x = between.eval(ctx);
    EXPECT_TRUE(x != std::nullopt);
    if (x.value()) {
      count++;
    }
  }

  // 6 items in range [3, 8]
  EXPECT_EQ(count, 6);
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

    nebula::surface::MockAccessor row;
    nebula::surface::eval::EvalContext ctx{ false };
    ctx.reset(row);

    for (const auto& item : data) {
      const auto& sv = std::get<0>(item);
      auto s = std::make_shared<unordered_set<std::string_view>>(sv.begin(), sv.end());
      const auto& t = std::get<1>(item);
      const auto& f = std::get<2>(item);
      const auto& r = std::get<3>(item);

      auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(t);
      if (f) {
        nebula::api::udf::In<nebula::type::Kind::VARCHAR> in("i", c, s);
        EXPECT_EQ(in.eval(ctx), r);
      } else {
        nebula::api::udf::In<nebula::type::Kind::VARCHAR> in("i", c, s, f);
        EXPECT_EQ(in.eval(ctx), r);
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

    nebula::surface::MockAccessor row;
    nebula::surface::eval::EvalContext ctx{ false };
    ctx.reset(row);

    for (const auto& item : data) {
      const auto& sv = std::get<0>(item);
      auto s = std::make_shared<unordered_set<int32_t>>(sv.begin(), sv.end());

      const auto& t = std::get<1>(item);
      const auto& f = std::get<2>(item);
      const auto& r = std::get<3>(item);

      auto c = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(t);
      if (f) {
        nebula::api::udf::In<nebula::type::Kind::INTEGER> in("i", c, s);
        EXPECT_EQ(in.eval(ctx), r);
      } else {
        nebula::api::udf::In<nebula::type::Kind::INTEGER> in("i", c, s, f);
        EXPECT_EQ(in.eval(ctx), r);
      }
    }
  }
}

TEST(UDFTest, TestCount) {

  using CType = nebula::api::udf::Count<nebula::type::Kind::INTEGER>;

  // simulate the run times 11 for c1 and 22 for c2
  nebula::surface::eval::EvalContext ctx{ false };
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(0);
  CType cf("count", v9->asEval());
  auto count1 = cf.sketch();

  for (auto i = 0; i < 11; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ci("count1", vi->asEval());
    auto v = ci.eval(ctx);
    EXPECT_EQ(v, 1);
    count1->merge(v.value());
  }

  auto count2 = cf.sketch();
  for (auto i = 0; i < 22; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ci("count2", vi->asEval());
    count2->merge(ci.eval(ctx).value());
  }

  // partial merge
  count1->mix(*count2);

  // we will ask itself for finalize
  CType::NativeType count4 = count1->finalize();
  EXPECT_EQ(count4, 33);
}

TEST(UDFTest, TestSum) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int8_t>>(0);
  using CType = nebula::api::udf::Sum<nebula::type::Kind::TINYINT>;
  nebula::surface::eval::EvalContext ctx{ false };
  CType sf("sum", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  CType::NativeType expected = 0;
  auto sum1 = sf.sketch();
  for (auto i = 0; i < 11; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int8_t>>(i);
    CType si("sum1", vi->asEval());

    sum1->merge(si.eval(ctx).value());
    expected += i;
  }

  auto sum2 = sf.sketch();
  for (auto i = 0; i < 22; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int8_t>>(i);
    CType si("sum2", vi->asEval());

    sum2->merge(si.eval(ctx).value());
    expected += i;
  }

  // partial merge
  CType s3("sum3", v9->asEval());
  sum1->mix(*sum2);

  // we will ask itself for finalize
  auto sum4 = sum1->finalize();
  EXPECT_EQ(sum4, expected);

  // serialize
  nebula::common::ExtendableSlice slice(1024);
  EXPECT_EQ(sum1->serialize(slice, 12), 8);
  auto sum5 = sf.sketch();
  EXPECT_EQ(sum5->load(slice, 12), 8);
  auto sv = sum1->finalize();
  EXPECT_EQ(sv, expected);
}

TEST(UDFTest, TestSum128) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int128_t>>(12);
  using CType = nebula::api::udf::Sum<nebula::type::Kind::INT128>;
  nebula::surface::eval::EvalContext ctx{ false };
  CType sf("sum", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  CType::NativeType expected = 0;
  auto sum1 = sf.sketch();
  for (auto i = 0; i < 5; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int128_t>>(i);
    CType si("sum1", vi->asEval());

    sum1->merge(si.eval(ctx).value());
    expected += i;
  }

  auto sum2 = sf.sketch();
  for (auto i = 0; i < 22; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int128_t>>(i);
    CType si("sum2", vi->asEval());

    sum2->merge(si.eval(ctx).value());
    expected += i;
  }

  // partial merge
  CType s3("sum3", v9->asEval());
  sum1->mix(*sum2);

  // we will ask itself for finalize
  auto sum4 = sum1->finalize();
  EXPECT_TRUE(sum4 == expected);
}

TEST(UDFTest, TestMin) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(0);
  using CType = nebula::api::udf::Min<nebula::type::Kind::INTEGER>;
  nebula::surface::eval::EvalContext ctx{ false };
  CType mf("min", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  CType::NativeType expected = std::numeric_limits<int32_t>::max();
  auto min1 = mf.sketch();
  for (auto i = 5; i < 11; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType mi("min1", vi->asEval());

    min1->merge(mi.eval(ctx).value());
    expected = std::min(i, expected);
  }

  auto min2 = mf.sketch();
  for (auto i = 6; i < 22; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType mi("min2", vi->asEval());

    min2->merge(mi.eval(ctx).value());
    expected = std::min(i, expected);
  }

  // partial merge
  min1->mix(*min2);

  // we will ask itself for finalize
  auto min4 = min1->finalize();
  EXPECT_EQ(min4, expected);
  LOG(INFO) << "min=" << min4 << ", expected=" << expected;
}

TEST(UDFTest, TestMax) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(0);
  using CType = nebula::api::udf::Max<nebula::type::Kind::INTEGER>;
  nebula::surface::eval::EvalContext ctx{ false };
  CType mf("max", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  CType::NativeType expected = std::numeric_limits<int32_t>::min();
  auto max1 = mf.sketch();
  for (auto i = 1; i < 11; ++i) {
    int32_t v = i * -1;
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(v);
    CType mi("max1", vi->asEval());
    max1->merge(mi.eval(ctx).value());
    expected = std::max(v, expected);
  }

  auto max2 = mf.sketch();
  for (auto i = 1; i < 22; ++i) {
    int32_t v = i * -1;
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(v);
    CType mi("max1", vi->asEval());
    max2->merge(mi.eval(ctx).value());
    expected = std::max(v, expected);
  }

  // partial merge
  CType m3("max3", v9->asEval());
  max1->mix(*max2);

  // we will ask itself for finalize
  CType::NativeType max4 = max1->finalize();
  EXPECT_EQ(max4, expected);
  LOG(INFO) << "max=" << max4 << ", expected=" << expected;
}

TEST(UDFTest, TestAvg) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(0);
  using CType = nebula::api::udf::Avg<nebula::type::Kind::INTEGER>;
  nebula::surface::eval::EvalContext ctx{ false };
  CType af("avg", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  CType::NativeType sum = 0;
  CType::NativeType count = 0;

  auto avg1 = af.sketch();
  for (auto i = 0; i < 11; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ai("avg1", vi->asEval());
    avg1->merge(ai.eval(ctx).value());

    sum += i;
    count += 1;
  }

  auto avg2 = af.sketch();
  for (auto i = 0; i < 22; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ai("avg2", vi->asEval());
    avg2->merge(ai.eval(ctx).value());

    sum += i;
    count += 1;
  }

  // partial merge
  CType a3("avg3", v9->asEval());
  avg1->mix(*avg2);

  // we will ask itself for finalize
  CType::NativeType avg4 = avg1->finalize();
  EXPECT_EQ(avg4, sum / count);
}

TEST(UDFTest, TestAvgByte) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int8_t>>(0);
  using CType = nebula::api::udf::Avg<nebula::type::Kind::TINYINT>;
  nebula::surface::eval::EvalContext ctx{ false };
  CType af("avg", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  int64_t sum = 0;
  int64_t count = 0;

  auto avg1 = af.sketch();
  for (auto i = 0; i < 127; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int8_t>>(i);
    CType ai("avg2", vi->asEval());
    avg1->merge(ai.eval(ctx).value());

    sum += i;
    count += 1;
  }

  auto avg2 = af.sketch();
  for (auto i = 0; i < 127; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int8_t>>(i);
    CType ai("avg2", vi->asEval());
    avg2->merge(ai.eval(ctx).value());

    sum += i;
    count += 1;
  }

  // partial merge
  avg1->mix(*avg2);

  // we will ask itself for finalize
  CType::NativeType avg4 = avg1->finalize();
  EXPECT_EQ(avg4, sum / count);
}

TEST(UDFTest, TestAvgInt128) {
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int128_t>>(0);
  using CType = nebula::api::udf::Avg<nebula::type::Kind::INT128>;
  nebula::surface::eval::EvalContext ctx{ false };
  CType af("avg", v9->asEval());

  // simulate the run times 11 for c1 and 22 for c2
  int128_t sum = 0;
  int64_t count = 0;

  auto avg1 = af.sketch();
  for (auto i = 0; i < 127; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int128_t>>(i);
    CType ai("avg2", vi->asEval());
    avg1->merge(ai.eval(ctx).value());

    sum += i;
    count += 1;
  }

  auto avg2 = af.sketch();
  for (auto i = 0; i < 127; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int128_t>>(i);
    CType ai("avg2", vi->asEval());
    avg2->merge(ai.eval(ctx).value());

    sum += i;
    count += 1;
  }

  // partial merge
  avg1->mix(*avg2);

  // we will ask itself for finalize
  CType::NativeType avg4 = avg1->finalize();
  EXPECT_TRUE(avg4 == sum / count);
}

TEST(UDFTest, TestPct) {
  using CType = nebula::api::udf::Pct<nebula::type::Kind::INTEGER>;

  // simulate the run times 11 for c1 and 22 for c2
  nebula::surface::eval::EvalContext ctx{ false };
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(0);
  double percentile = 99.0;
  CType tf("td1", v9->asEval(), percentile);

  auto td1 = tf.sketch();
  for (auto i = 0; i < 110; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ci("count1", vi->asEval(), percentile);
    td1->merge(ci.eval(ctx).value());
  }

  auto td2 = tf.sketch();
  for (auto i = 0; i < 220; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<int32_t>>(i);
    CType ci("count2", vi->asEval(), percentile);
    td2->merge(ci.eval(ctx).value());
  }

  // partial merge
  CType c3("count3", v9->asEval(), percentile);
  td1->mix(*td2);

  // we will ask itself for finalize
  CType::NativeType td4 = td1->finalize();
  EXPECT_NEAR(td4, 217, 1);
  auto json = static_cast<CType::Aggregator*>(td1.get())->jsonfy();
  LOG(INFO) << "sketch in json: " << json;
}

TEST(UDFTest, TestHist) {
  using CType = nebula::api::udf::Hist<nebula::type::Kind::DOUBLE>;

  // simulate the run times 11 for c1 and 22 for c2
  nebula::surface::eval::EvalContext ctx{ false };
  auto v9 = std::make_shared<nebula::api::dsl::ConstExpression<double>>(0);
  double max = 40.0;
  double min = 30.0;
  CType th("th1", v9->asEval(), min, max);

  auto th1 = th.sketch();
  for (auto i = 0; i < 110; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<double>>(i);
    CType hist("hist1", vi->asEval(), min, max);
    th1->merge(hist.eval(ctx).value());
  }

  auto th2 = th.sketch();
  for (auto i = 110; i < 220; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<double>>(i);
    CType hist("hist2", vi->asEval(), min, max);
    th2->merge(hist.eval(ctx).value());
  }

  // partial merge
  th1->mix(*th2);

  LOG(INFO) << "run tests here";
  // we will ask itself for finalize
  auto jsonStr = (std::string)(th1->finalize());
  nebula::common::ExtendableSlice slice(1024);
  EXPECT_EQ(th1->serialize(slice, 12), 467);
  EXPECT_EQ(th1->load(slice, 12), 467);
  CType::NativeType load = th1->finalize();
  EXPECT_EQ(load, jsonStr);
}

TEST(UDFTest, TestTpm) {
  using CType = nebula::api::udf::Tpm<nebula::type::Kind::VARCHAR>;
  // paths to merge
  std::string_view stacks[] = {
    "A\nB\nC",
    "A\nB\nD",
    "A\nX",
    "A\nX\nY",
  };

  // simulate the run times 11 for c1 and 22 for c2
  nebula::surface::eval::EvalContext ctx{ false };
  auto v1 = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>("");
  CType tpm("tmp1", v1->asEval());

  auto sketch = tpm.sketch();
  for (size_t i = 0, size = std::size(stacks); i < size; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(stacks[i]);
    CType ci("tmp", vi->asEval());
    sketch->merge(ci.eval(ctx).value());
  }

  auto sketch2 = tpm.sketch();
  for (size_t i = 0, size = std::size(stacks); i < size; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(stacks[i]);
    CType ci("tmp2", vi->asEval());
    sketch2->merge(ci.eval(ctx).value());
  }

  // partial merge
  sketch->mix(*sketch2);

  // we will ask itself for finalize
  // "ROOT" -> A  -> B -> C
  //                   -> D
  //                 X -> Y
  // const std::string_view expected =
  //   "{\"name\":\"\",\"value\":8,\"children\":"
  //   "[{\"name\":\"A\",\"value\":8,\"children\":"
  //   "[{\"name\":\"X\",\"value\":4,\"children\":"
  //   "[{\"name\":\"Y\",\"value\":2}]},{\"name\":\"B\",\"value\":4,\"children\":"
  //   "[{\"name\":\"C\",\"value\":2},{\"name\":\"D\",\"value\":2}]}]}]}";
  CType::NativeType json = sketch->finalize();

#define VERIFY_C_D                                        \
  EXPECT_EQ(c["name"], "C");                              \
  EXPECT_EQ(c["value"], 2);                               \
  EXPECT_TRUE(c.FindMember("children") == c.MemberEnd()); \
  EXPECT_EQ(d["name"], "D");                              \
  EXPECT_EQ(d["value"], 2);                               \
  EXPECT_TRUE(d.FindMember("children") == d.MemberEnd());

#define VERIFY_B                       \
  EXPECT_EQ(b["name"], "B");           \
  EXPECT_EQ(b["value"], 4);            \
  auto l3b = b["children"].GetArray(); \
  EXPECT_EQ(l3b.Size(), 2);            \
  auto temp2 = l3b[0].GetObject();     \
  if (temp2["name"] == "C") {          \
    auto c = temp2;                    \
    auto d = l3b[1].GetObject();       \
    VERIFY_C_D                         \
  } else {                             \
    auto d = temp2;                    \
    auto c = l3b[1].GetObject();       \
    VERIFY_C_D                         \
  }

#define VERIFY_Y               \
  auto y = l3x[0].GetObject(); \
  EXPECT_EQ(y["name"], "Y");   \
  EXPECT_EQ(y["value"], 2);    \
  EXPECT_TRUE(y.FindMember("children") == y.MemberEnd());

#define VERIFY_X                       \
  EXPECT_EQ(x["name"], "X");           \
  EXPECT_EQ(x["value"], 4);            \
  auto l3x = x["children"].GetArray(); \
  EXPECT_EQ(l3x.Size(), 1);            \
  VERIFY_Y

  rapidjson::Document doc;
  doc.Parse(json.data(), json.length());

  // verify root
  auto root = doc.GetObject();
  EXPECT_EQ(root["name"], "");
  EXPECT_EQ(root["value"], 8);
  auto l1 = root["children"].GetArray();
  EXPECT_EQ(l1.Size(), 1);

  // verify a
  auto a = l1[0].GetObject();
  EXPECT_EQ(a["name"], "A");
  EXPECT_EQ(a["value"], 8);
  auto l2 = a["children"].GetArray();
  EXPECT_EQ(l2.Size(), 2);
  auto temp = l2[0].GetObject();

  if (temp["name"] == "B") {
    auto b = temp;
    auto x = l2[1].GetObject();
    VERIFY_B
    VERIFY_X
  } else {
    auto x = temp;
    auto b = l2[1].GetObject();
    VERIFY_B
    VERIFY_X
  }

#undef VERIFY_X
#undef VERIFY_Y
#undef VERIFY_B
#undef VERIFY_C_D
}

TEST(UDFTest, TestCardinality) {
  using CType = nebula::api::udf::Cardinality<nebula::type::Kind::VARCHAR>;
  // paths to merge
  std::string_view stacks[] = {
    "A\nB\nC",
    "A\nB\nD",
    "A\nX",
    "A\nX\nY",
  };

  // simulate the run times 11 for c1 and 22 for c2
  nebula::surface::eval::EvalContext ctx{ false };
  auto v1 = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>("");
  CType card("tmp1", v1->asEval());

  auto sketch = card.sketch();
  for (size_t i = 0, size = std::size(stacks); i < size; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(stacks[i]);
    CType ci("tmp", vi->asEval());
    sketch->merge(ci.eval(ctx).value());
  }

  auto sketch2 = card.sketch();
  for (size_t i = 0, size = std::size(stacks); i < size; ++i) {
    auto vi = std::make_shared<nebula::api::dsl::ConstExpression<std::string_view>>(stacks[i]);
    CType ci("tmp2", vi->asEval());
    sketch2->merge(ci.eval(ctx).value());
  }

  // partial merge
  EXPECT_EQ(sketch->finalize(), 4);
  EXPECT_EQ(sketch2->finalize(), 4);
  sketch->mix(*sketch2);

  // we will ask itself for finalize
  CType::NativeType cardinality_est = sketch->finalize();
  EXPECT_TRUE(cardinality_est >= 3 && cardinality_est <= 5);
  LOG(INFO) << "cardinality: " << cardinality_est;
}

} // namespace test
} // namespace api
} // namespace nebula