/*
 * Copyright 2017-present varchar.io
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

#include "surface/eval/Histogram.h"

namespace nebula {
namespace surface {
namespace test {

using nebula::surface::eval::BoolHistogram;
using nebula::surface::eval::Histogram;
using nebula::surface::eval::IntHistogram;
using nebula::surface::eval::RealHistogram;

TEST(HistogramTest, TestSerde) {
  {
    Histogram orig{ "col", 2 };
    auto hist = nebula::surface::eval::from(orig.toString());
    EXPECT_EQ(hist->count, 2);
  }

  {
    BoolHistogram orig{ "col", 2, 4 };
    auto histBase = nebula::surface::eval::from(orig.toString());
    auto hist = static_cast<BoolHistogram*>(histBase.get());
    EXPECT_EQ(hist->count, 2);
    EXPECT_EQ(hist->trueValues, 4);
  }

  {
    RealHistogram orig{ "col", 2, 1.0, 3.6, 4.6 };
    auto histBase = nebula::surface::eval::from(orig.toString());
    auto hist = static_cast<RealHistogram*>(histBase.get());
    EXPECT_EQ(hist->count, 2);
    EXPECT_NEAR(hist->v_min, 1.0, 0.0000000001);
    EXPECT_NEAR(hist->v_max, 3.6, 0.0000000001);
    EXPECT_NEAR(hist->v_sum, 4.6, 0.0000000001);
  }

  {
    IntHistogram orig{ "col", 2, 1, 3, 4 };
    auto histBase = nebula::surface::eval::from(orig.toString());
    auto hist = static_cast<IntHistogram*>(histBase.get());
    EXPECT_EQ(hist->count, 2);
    EXPECT_EQ(hist->v_min, 1);
    EXPECT_EQ(hist->v_max, 3);
    EXPECT_EQ(hist->v_sum, 4);
  }
}

TEST(HistogramTest, TestMerge) {
  {
    Histogram h{ "col", 2 };
    Histogram h2{ "col", 3 };
    h.merge(h2);
    EXPECT_EQ(h.count, 5);
  }

  {
    BoolHistogram h{ "col", 4, 4 };
    BoolHistogram h2{ "col", 3, 1 };
    h.merge(h2);
    EXPECT_EQ(h.count, 7);
    EXPECT_EQ(h.trueValues, 5);
  }

  {
    RealHistogram h{ "col", 2, 1.0, 3.6, 4.6 };
    RealHistogram h2{ "col", 3, 1.2, 4.6, 10.2 };
    h.merge(h2);
    EXPECT_EQ(h.count, 5);
    EXPECT_NEAR(h.v_min, 1.0, 0.0000000001);
    EXPECT_NEAR(h.v_max, 4.6, 0.0000000001);
    EXPECT_NEAR(h.v_sum, 14.8, 0.0000000001);
  }

  {
    IntHistogram h{ "col", 2, 1, 3, 4 };
    IntHistogram h2{ "col", 1, 1, 1, 1 };
    h.merge(h2);
    EXPECT_EQ(h.count, 3);
    EXPECT_EQ(h.v_min, 1);
    EXPECT_EQ(h.v_max, 3);
    EXPECT_EQ(h.v_sum, 5);
  }
}

TEST(HistogramTest, TestFailOver) {
  // test realhistogram handling infinite value
  {
    const auto inf = std::numeric_limits<float>::infinity();
    const auto expected = "{\"type\":\"REAL\",\"name\":\"col\",\"count\":2,\"min\":1.0,\"max\":3.6,\"sum\":null}";
    RealHistogram orig{ "col", 2, 1.0, 3.6, inf };
    EXPECT_EQ(orig.toString(), expected);
    auto histBase = nebula::surface::eval::from(expected);
    auto hist = static_cast<RealHistogram*>(histBase.get());
    EXPECT_EQ(hist->count, 2);
    EXPECT_NEAR(hist->v_min, 1.0, 0.0000000001);
    EXPECT_NEAR(hist->v_max, 3.6, 0.0000000001);

    // null is evaluated as 0
    EXPECT_EQ(hist->v_sum, 0);
  }
  // test inthistogram handling infinite value
  {
    // integer's infinite value is 0
    const auto inf = std::numeric_limits<int64_t>::infinity();
    EXPECT_EQ(inf, 0);
    const auto expected = "{\"type\":\"INT\",\"name\":\"col\",\"count\":2,\"min\":1,\"max\":3,\"sum\":0}";
    IntHistogram orig{ "col", 2, 1, 3, inf };
    EXPECT_EQ(orig.toString(), expected);
    auto histBase = nebula::surface::eval::from(expected);
    auto hist = static_cast<IntHistogram*>(histBase.get());
    EXPECT_EQ(hist->count, 2);
    EXPECT_EQ(hist->v_min, 1);
    EXPECT_EQ(hist->v_max, 3);
    EXPECT_TRUE(std::isfinite(hist->v_sum));
  }
}

} // namespace test
} // namespace surface
} // namespace nebula