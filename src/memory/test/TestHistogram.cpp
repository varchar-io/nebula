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

#include "memory/Batch.h"
#include "memory/serde/TypeDataFactory.h"
#include "surface/eval/Histogram.h"

#include "meta/Table.h"
#include "meta/TestTable.h"
#include "surface/MockSurface.h"

/**
 * Flat Buffer is used to store / compute run time data. 
 * Test its interfaces and functions here.
 */
namespace nebula {
namespace memory {
namespace test {

using nebula::memory::serde::TypeDataFactory;
using nebula::surface::eval::BoolHistogram;
using nebula::surface::eval::Histogram;
using nebula::surface::eval::IntHistogram;
using nebula::surface::eval::RealHistogram;

TEST(HistogramTest, TestHistogramTypes) {
  std::unique_ptr<Histogram> histo = std::make_unique<Histogram>();
  histo->count = 2;

  EXPECT_EQ(histo->count, 2);

  // test bool histogram
  {
    nebula::meta::Column column;
    auto m = TypeDataFactory::createMeta(nebula::type::Kind::BOOLEAN, column);
    m->histogram(true);
    m->histogram(false);
    m->histogram(true);
    const auto& bh = m->histogram<BoolHistogram>();
    EXPECT_EQ(bh.count, 3);
    EXPECT_EQ(bh.trueValues, 2);
  }

  // test integer histogram
#define INT_TYPE_TEST(K)                                                                             \
  {                                                                                                  \
    using CT = nebula::type::TypeTraits<nebula::type::Kind::K>::CppType;                             \
    auto max = static_cast<CT>(std::numeric_limits<CT>::max() * nebula::common::Evidence::rand()()); \
    uint64_t count = 100;                                                                            \
    CT min = max - count + 1;                                                                        \
    nebula::meta::Column column;                                                                     \
    auto m = TypeDataFactory::createMeta(nebula::type::Kind::K, column);                             \
    int64_t sum = 0;                                                                                 \
    for (auto i = min; i <= max; ++i) {                                                              \
      sum += i;                                                                                      \
      EXPECT_EQ(m->histogram(i), i - min + 1);                                                       \
    }                                                                                                \
    const auto& ih = m->histogram<IntHistogram>();                                                   \
    EXPECT_EQ(ih.count, count);                                                                      \
    EXPECT_EQ(ih.min(), min);                                                                        \
    EXPECT_EQ(ih.max(), max);                                                                        \
    EXPECT_EQ(ih.sum(), sum);                                                                        \
    EXPECT_EQ(ih.avg(), sum / count);                                                                \
  }

  INT_TYPE_TEST(TINYINT)
  INT_TYPE_TEST(SMALLINT)
  INT_TYPE_TEST(INTEGER)
  INT_TYPE_TEST(BIGINT)

#undef INT_TYPE_TEST

// test floating values histogram
#define REAL_TYPE_TEST(K)                                                \
  {                                                                      \
    nebula::meta::Column column;                                         \
    auto m = TypeDataFactory::createMeta(nebula::type::Kind::K, column); \
    EXPECT_EQ(m->histogram(1.02), 1);                                    \
    EXPECT_EQ(m->histogram(2.0), 2);                                     \
    EXPECT_EQ(m->histogram(1.38), 3);                                    \
    EXPECT_EQ(m->histogram(0.38), 4);                                    \
    const auto& rh = m->histogram<RealHistogram>();                      \
    EXPECT_EQ(rh.count, 4);                                              \
    EXPECT_NEAR(rh.min(), 0.38, 1e-14);                                  \
    EXPECT_NEAR(rh.max(), 2.0, 1e-14);                                   \
    EXPECT_NEAR(rh.sum(), 4.78, 1e-14);                                  \
    EXPECT_NEAR(rh.avg(), 1.195, 1e-14);                                 \
  }

  REAL_TYPE_TEST(REAL)
  REAL_TYPE_TEST(DOUBLE)

#undef REAL_TYPE_TEST

  // other types are all using basic histogram with only count
  {
    nebula::meta::Column column;
    auto m = TypeDataFactory::createMeta(nebula::type::Kind::VARCHAR, column);
    m->histogram("true");
    m->histogram("abc");
    m->histogram("xyz");
    const auto& h = m->histogram();
    EXPECT_EQ(h->count, 3);
  }
}

TEST(HistogramTest, TestBlockHistogram) {
  nebula::meta::TestTable test;
  // write some data into a batch and verify its histogram
  {
    // add 10 rows
    auto rows = 10;
    Batch batch1(test, rows);
    // use the specified seed so taht the data can repeat
    auto seed = nebula::common::Evidence::unix_timestamp();

    nebula::surface::MockRowData row(seed);
    for (auto i = 0; i < rows; ++i) {
      batch1.add(row);
    }

    // verify batch data
    EXPECT_EQ(batch1.getRows(), rows);

    // print out the batch state
    LOG(INFO) << "Batch: " << batch1.state();

    // verify the batch histogram of all columns -
    // ROW<_time_: bigint, id:int, event:string, items:list<string>, flag:bool, value:tinyint, weight:double, stamp:int128>"
    LOG(INFO) << "====== Histogram Of Columns ======";
    LOG(INFO) << "Time: " << batch1.histogram<IntHistogram>("_time_").toString();
    LOG(INFO) << "ID: " << batch1.histogram<IntHistogram>("id").toString();
    LOG(INFO) << "Event: " << batch1.histogram("event")->toString();
    LOG(INFO) << "Items: " << batch1.histogram("items")->toString();
    LOG(INFO) << "Flag: " << batch1.histogram<BoolHistogram>("flag").toString();
    LOG(INFO) << "Value: " << batch1.histogram<IntHistogram>("value").toString();
    LOG(INFO) << "Weight: " << batch1.histogram<RealHistogram>("weight").toString();
    LOG(INFO) << "Stamp: " << batch1.histogram("stamp")->toString();
  }
}

} // namespace test
} // namespace memory
} // namespace nebula