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
#include <folly/stats/TDigest.h>
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

// more interesting tests see:
// https://github.com/facebook/folly/blob/master/folly/stats/test/TDigestTest.cpp
TEST(StatsTest, TestTDigest) {
  // build percentiles
  folly::TDigest digest(100);

  EXPECT_TRUE(digest.empty());

  // a vector has value ranging from 1 to 1000
  constexpr auto numberOfValues = 1000;
  std::vector<double> values;
  for (int i = 1; i <= numberOfValues; ++i) {
    values.push_back(i);
  }

  digest = digest.merge(values);

  EXPECT_FALSE(digest.empty());
  EXPECT_EQ(digest.count(), numberOfValues);
  EXPECT_NEAR(digest.max(), numberOfValues, 1e-14);
  EXPECT_NEAR(digest.min(), 1, 1e-14);
  EXPECT_NEAR(digest.mean(), numberOfValues / 2, 1);
  EXPECT_NEAR(digest.sum(), (1 + numberOfValues) * numberOfValues / 2, 1e-14);
  EXPECT_NEAR(digest.estimateQuantile(0.1), 100, 1);

  // can we serde this data structure
  {
    auto centroids = digest.getCentroids();
    std::vector<std::pair<double, double>> serializedCentroids;
    serializedCentroids.reserve(centroids.size());
    std::transform(centroids.begin(), centroids.end(),
                   std::back_inserter(serializedCentroids),
                   [](auto& c) { return std::make_pair(c.mean(), c.weight()); });

    auto sum = digest.sum();
    auto count = digest.count();
    auto max = digest.max();
    auto min = digest.min();
    auto maxSize = digest.maxSize();

    // build a new TDigest
    std::vector<folly::TDigest::Centroid> centroids2;
    centroids2.reserve(serializedCentroids.size());
    std::transform(serializedCentroids.begin(), serializedCentroids.end(),
                   std::back_inserter(centroids2), [](auto& p) {
                     return folly::TDigest::Centroid(std::get<0>(p), std::get<1>(p));
                   });

    folly::TDigest digest2(std::move(centroids2), sum, count, max, min, maxSize);

    // verify digest and digest2 have the same properties
    EXPECT_EQ(digest2.count(), numberOfValues);
    EXPECT_NEAR(digest2.max(), numberOfValues, 1e-14);
    EXPECT_NEAR(digest2.min(), 1, 1e-14);
    EXPECT_NEAR(digest2.mean(), numberOfValues / 2, 1);
    EXPECT_NEAR(digest2.sum(), (1 + numberOfValues) * numberOfValues / 2, 1e-14);

    // verify digest 2 has all similar percentile estimates as original digest
    for (auto i = 1; i < 10; ++i) {
      auto percentile = i * 10.0 / 100;
      LOG(INFO) << "verify percentile before / after serde: " << percentile;
      EXPECT_NEAR(digest.estimateQuantile(percentile), digest2.estimateQuantile(percentile), 1e-14);
    }
  }
}

} // namespace test
} // namespace common
} // namespace nebula