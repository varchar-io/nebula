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

#include <chrono>
#include <fmt/format.h>
#include <folly/stats/Histogram.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sstream>

#include "common/Evidence.h"

/**
 * Test all stats algorithms provided by folly/stats package
 */
namespace nebula {
namespace common {
namespace test {
TEST(StatsTest, TestHistogram) {
  // basic histogram functions
  {
    // construct histogram object with (bucketSize, minValue, maxValue)
    // bucketSize = value range internval for each bucket
    folly::Histogram<int64_t> hist(1, 0, 100);
    for (int64_t i = 0; i < 100; ++i) {
      hist.addValue(i);
    }

    // 2 extra buckets, one is below min, the other is above max
    EXPECT_EQ(hist.getNumBuckets(), 102);

    // check value are falling into the expected bucket
    double epsilon = 1e-6;
    for (unsigned int n = 0; n <= 100; ++n) {
      double pct = n / 100.0;

      // Floating point arithmetic isn't 100% accurate, and if we just divide
      // (n / 100) the value should be exactly on a bucket boundary.  Add espilon
      // to ensure we fall in the upper bucket.
      if (n < 100) {
        double lowPct = -1.0;
        double highPct = -1.0;
        unsigned int bucketIdx = hist.getPercentileBucketIdx(pct + epsilon, &lowPct, &highPct);
        EXPECT_EQ(n + 1, bucketIdx);
        EXPECT_FLOAT_EQ(n / 100.0, lowPct);
        EXPECT_FLOAT_EQ((n + 1) / 100.0, highPct);
      }

      // value falling into lower bucket
      if (n > 0) {
        double lowPct = -1.0;
        double highPct = -1.0;
        unsigned int bucketIdx = hist.getPercentileBucketIdx(pct - epsilon, &lowPct, &highPct);
        EXPECT_EQ(n, bucketIdx);
        EXPECT_FLOAT_EQ((n - 1) / 100.0, lowPct);
        EXPECT_FLOAT_EQ(n / 100.0, highPct);
      }

      // test percentile estimate
      auto pctEst = hist.getPercentileEstimate(pct);
      EXPECT_EQ(n, pctEst);
    }
  }

  {
    folly::Histogram<double> h(100.0, 0.0, 7.0);
    EXPECT_EQ(3, h.getNumBuckets());

    for (double n = 0; n < 7; n += 1) {
      h.addValue(n);
    }
    EXPECT_EQ(0, h.getBucketByIndex(0).count);
    EXPECT_EQ(7, h.getBucketByIndex(1).count);
    EXPECT_EQ(0, h.getBucketByIndex(2).count);
    EXPECT_EQ(3.0, h.getPercentileEstimate(0.5));

    h.addValue(-1.0);
    EXPECT_EQ(1, h.getBucketByIndex(0).count);
    h.addValue(7.5);
    EXPECT_EQ(1, h.getBucketByIndex(2).count);
    EXPECT_NEAR(3.0, h.getPercentileEstimate(0.5), 1e-14);
  }

  // test histogram serde
  {
    using nebula::common::Evidence;
    folly::Histogram<double> hist(5.0, 1.0, 100.0);
    auto rand = Evidence::rand<int64_t>(1, 100, std::numeric_limits<int64_t>::max());
    for (auto i = 0; i < 1000; ++i) {
      hist.addValue(rand() * 1.0);
    }

    // print out the histogram
    LOG(INFO) << "Total Count:" << hist.computeTotalCount()
              << ", number of buckets: " << hist.getNumBuckets()
              << ", P95: " << hist.getPercentileEstimate(0.95)
              << ", P25: " << hist.getPercentileEstimate(0.25)
              << ", P99: " << hist.getPercentileEstimate(0.99);

    // print out as TSV
    std::ostringstream value;
    hist.toTSV(value);
    LOG(INFO) << value.str();
  }
}

} // namespace test
} // namespace common
} // namespace nebula