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
#include <fstream>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sstream>

#include "common/CountMin.h"
#include "common/Evidence.h"
#include "common/Hash.h"
#include "common/HyperLogLog.h"
#include "common/Memory.h"

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

template <typename CM>
void queryFrequency(const CM& sketch, bool top20Only = true) {
  LOG(INFO) << "Query top 20 items: ";
  // we know the top list in final result, and let's compare them
  std::array<size_t, 20> top20 = { 922203297757ul,
                                   905860166503ul,
                                   902065567321ul,
                                   899049071324ul,
                                   961238559656ul,
                                   898037969690ul,
                                   918644201389ul,
                                   935249274030ul,
                                   940816893800ul,
                                   901468450490ul,
                                   918105274631ul,
                                   930494031304ul,
                                   948192800438ul,
                                   907002707958ul,
                                   952015364871ul,
                                   905661505034ul,
                                   908182459161ul,
                                   938071044570ul,
                                   906224180441ul,
                                   903212673711ul };
  // top 20 items real frequency
  std::array<uint32_t, 20> f20 = {
    12034u,
    9367u,
    7623u,
    6597u,
    6400u,
    5900u,
    5736u,
    5332u,
    4657u,
    4046u,
    3969u,
    3849u,
    3814u,
    3689u,
    3630u,
    3393u,
    3255u,
    3068u,
    3051u,
    3012u
  };

  // let's query their frequency
  LOG(INFO) << "Top 20 items: value | frequency | fact | diff";
  for (size_t i = 0, size = std::size(top20); i < size; ++i) {
    auto v = top20[i];
    auto f = sketch.query(v);
    auto e = f20[i];
    LOG(INFO) << v << '\t' << f << '\t' << e << '\t' << std::fixed << (f - e) * 100.0f / f << "%";
  }

  if (!top20Only) {
    // above value are exactly the same with real value!!
    // let us check another range 96~100
    LOG(INFO) << "Query range 96-100 items: ";
    auto r96100 = {
      907597569023ul,
      936735925371ul,
      913679799510ul,
      918093243960ul,
      912627836830ul
    };

    // let's query their frequency
    for (auto v : r96100) {
      LOG(INFO) << v << '\t' << sketch.query(v);
    }

    LOG(INFO) << "Query range 406-410 items: ";
    auto r40610 = {
      953149521481ul,
      919419135649ul,
      910354020924ul,
      922393050504ul,
      894556912866ul
    };
    for (auto v : r40610) {
      LOG(INFO) << v << '\t' << sketch.query(v);
    }
  }
}

TEST(FrequencyTest, TestCountMin) {
  nebula::common::CountMin sketch;
  sketch.add(944760692332ul, 5);
  sketch.add(899429799057ul, 500);
  sketch.add(954939770857ul, 135);
  sketch.add(920236059316ul, 443);
  sketch.add(954939770857ul, 23);
  sketch.add(944760692332ul, 14);

  LOG(INFO) << "Total count: " << sketch.count();
  LOG(INFO) << "944760692332ul: 19 = " << sketch.query(944760692332ul);
  LOG(INFO) << "899429799057ul: 500 = " << sketch.query(899429799057ul);
  LOG(INFO) << "954939770857ul: 158 = " << sketch.query(954939770857ul);
  LOG(INFO) << "920236059316ul: 443 = " << sketch.query(920236059316ul);
}

TEST(FrequencyTest, TestCountMinOnFile) {
  // this file is just a list of long integers
  // the algo is trying to compute frequency of each item (more specifically top items)
  std::ifstream fstream("/Users/shawncao/ig1.csv");
  std::string line;
  nebula::common::CountMin sketch;
  while (std::getline(fstream, line)) {
    sketch.add(folly::to<size_t>(line));
  }

  queryFrequency(sketch);
}

TEST(FrequencyTest, TestCountMinM63OnFile) {
  // this file is just a list of long integers
  // the algo is trying to compute frequency of each item (more specifically top items)
  std::ifstream fstream("test/data/ig1.csv");
  std::string line;

  // 2.5K sketch
  nebula::common::CountMin<5, 63> sketch;
  while (std::getline(fstream, line)) {
    sketch.add(folly::to<size_t>(line));
  }

  queryFrequency(sketch);
}

TEST(FrequencyTest, TestCountMinMergeOnFile) {
  // this file is just a list of long integers
  // the algo is trying to compute frequency of each item (more specifically top items)
  std::ifstream fstream("test/data/ig1.csv");
  std::string line;

  // space got to be the same to be mergeable
  using SketchType = nebula::common::CountMin<5, 256, uint32_t>;
  SketchType sketch;
  auto items = std::make_unique<SketchType>();

  // measure compressed storage size
  auto blocks = 0;
  auto sizeSum = 0;
  while (std::getline(fstream, line)) {
    items->add(folly::to<size_t>(line));

    // every 1000 items, we merge it into summary sketch and start a new one
    if (items->count() == 1000) {
      auto slice = items->compress();
      // LOG(INFO) << "items size: " << items->size() << ", slice size: " << slice->size();
      ++blocks;
      sizeSum += slice->size();

      // merge this into main sketch
      sketch.merge(*items);
      items = std::make_unique<SketchType>();
    }
  }

  // calculate storage cost
  LOG(INFO) << SketchType::name()
            << " = total blocks: " << blocks
            << ", sum_bytes: " << sizeSum
            << ", avg bytes per block:" << sizeSum / blocks;

  // merge last time
  sketch.merge(*items);

  // test out top 20 queries
  queryFrequency(sketch);
}

TEST(HashTest, TestXxhVector) {
  std::array<size_t, 2> v = { 944760692332ul, 899429799057ul };

  // simulate 20x5 matrix
  constexpr auto MOD = 20;
  constexpr auto SIZE = 5;

  for (size_t k = 0; k < v.size(); ++k) {
    auto vk = v.at(k);
    auto hashes = nebula::common::Hasher::hash64<SIZE>(&vk, sizeof(vk));
    EXPECT_EQ(SIZE, hashes.size());

    LOG(INFO) << "Hashes for v" << k;
    for (auto i = 0; i < SIZE; ++i) {
      auto h = hashes.at(i);
      LOG(INFO) << "h=" << h << ", mod=" << h % MOD;
    }
  }
}

TEST(HyperLogLogTest, TestLeadingZeroCount) {
  EXPECT_EQ(HyperLogLog::leadingZeros(0), 33);
  uint32_t v = 1;
  for (auto i = 0; i < 32; ++i) {
    EXPECT_EQ(HyperLogLog::leadingZeros(v), 32 - i);
    v <<= 1;
  }
}

TEST(HyperLogLogTest, TestTypedInput) {
  HyperLogLog hll(8);
  // int
  hll.add(0);
  // long type
  hll.add(1L);
  // float type
  hll.add(2.0f);
  // double type
  hll.add(3.0);
  // bool type
  hll.add(true);
  // string type
  hll.add("abc");
  std::string_view v = "xyz";
  hll.add(v);
  LOG(INFO) << "Estimate: " << hll.estimate();
  EXPECT_EQ((int)hll.estimate(), 7);
}

TEST(HyperLogLogTest, TestHLLEstimate) {
  constexpr size_t dataNum = (size_t(1) << 20) + size_t(1);
  // starting from 10, error rate should be less than 5%
  // and decreasing as bit width increases (size doubles for every step).
  auto threshold = 0.05;
  for (size_t n = 10; n <= 20; ++n) {
    HyperLogLog hll(n);
    for (size_t i = 0; i < dataNum; ++i) {
      auto str = std::to_string(i);
      hll.add(str.data(), str.size());
    }

    auto cardinality = hll.estimate();
    auto error = std::abs(cardinality - dataNum) / dataNum;
    LOG(INFO) << "K=" << n << " | size=" << hll.size() << " | err=" << error;
    EXPECT_TRUE(error < threshold);
  }
}

TEST(HyperLogLogTest, TestHLLMerge) {
  // Merge increases error rate way faster than single one
  // I think it's because of hash space is not enough for merge
  // so we should choose a relatively larger bitwidth to merge high cardinallity cases.
  // for example, if dataNumm is 1<<N, we should use hll(N-1) as well.
  constexpr size_t dataNum = (size_t(1) << 21) + size_t(1);
  constexpr size_t half = dataNum / 2;
  constexpr double threshold = 0.01;
  HyperLogLog hll1(20), hll2(20);
  HyperLogLog* hll = &hll1;

  for (size_t i = 0; i < dataNum; ++i) {
    auto str = std::to_string(i);
    hll->add(str.data(), str.size());
    if (i == half) {
      hll = &hll2;
    }
  }

  auto cardinality1 = hll1.estimate();
  auto cardinality2 = hll2.estimate();
  auto delta = std::abs(cardinality1 - cardinality2) / cardinality1;
  LOG(INFO) << "CARD1=" << cardinality1 << " | CARD2=" << cardinality2;
  EXPECT_TRUE(delta < threshold);

  // merge them
  hll1.merge(hll2);
  auto cardinality = hll1.estimate();
  auto error = std::abs(cardinality - dataNum) / dataNum;
  LOG(INFO) << "size=" << hll1.size() << " | err=" << error << ", CARD=" << (size_t)cardinality;
  EXPECT_TRUE(error < threshold);
}

TEST(HyperLogLogTest, TestHLLSerde) {
  constexpr size_t dataNum = (size_t(1) << 21) + size_t(1);
  HyperLogLog hll(20);
  for (size_t i = 0; i < dataNum; ++i) {
    auto str = std::to_string(i);
    hll.add(str.data(), str.size());
  }

  auto card1 = (size_t)hll.estimate();
  LOG(INFO) << "Cardinality=" << card1;
  std::stringstream str;
  hll.serialize(str);
  str.seekg(0, std::ios::end);
  LOG(INFO) << "Stream Size=" << str.tellg();

  // restore it as another HLL
  HyperLogLog hll2(20);
  str.seekg(0, std::ios::beg);
  hll2.deserialize(str);
  auto card2 = (size_t)hll2.estimate();
  LOG(INFO) << "Cardinality=" << card2;
  EXPECT_EQ(card1, card2);
}

} // namespace test
} // namespace common
} // namespace nebula