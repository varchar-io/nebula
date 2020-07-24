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

#include <folly/container/F14Set.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unordered_set>
#include "common/Evidence.h"
#include "common/robin_hood.h"

/**
 * Test some open hash set immplementations
 */
namespace nebula {
namespace common {
namespace test {

/**
 * Robinhood set stands out in 3 choices of comparison.
 * On My Macbook (2.6 GHz 6-Core Intel Core i7):
 * I20200724 13:38:31.830534 16981440 TestHash.cpp:53] Insert 1M random ulong values into std::set, taken: 938
 * I20200724 13:38:32.129688 16981440 TestHash.cpp:53] Found 0 in 1M random values from std::set, taken:298
 * I20200724 13:38:32.491488 16981440 TestHash.cpp:56] Insert 1M random ulong values into robin_hood::set, taken: 361
 * I20200724 13:38:32.543473 16981440 TestHash.cpp:56] Found 0 in 1M random values from robin_hood::set, taken:51
 * I20200724 13:38:33.302404 16981440 TestHash.cpp:59] Insert 1M random ulong values into f14::set, taken: 758
 * I20200724 13:38:33.467963 16981440 TestHash.cpp:59] Found 0 in 1M random values from f14::set, taken:165
 * 
 * On My Linux Dev Machine:
 * I0724 20:40:06.198599   787 TestHash.cpp:53] Insert 1M random ulong values into std::set, taken: 310
 * I0724 20:40:06.360635   787 TestHash.cpp:53] Found 0 in 1M random values from std::set, taken:161
 * I0724 20:40:06.464469   787 TestHash.cpp:56] Insert 1M random ulong values into robin_hood::set, taken: 103
 * I0724 20:40:06.481870   787 TestHash.cpp:56] Found 0 in 1M random values from robin_hood::set, taken:17
 * I0724 20:40:06.605304   787 TestHash.cpp:59] Insert 1M random ulong values into f14::set, taken: 123
 * I0724 20:40:06.635087   787 TestHash.cpp:59] Found 0 in 1M random values from f14::set, taken:29
 */
TEST(CommonTest, TestHashSet) {
  constexpr size_t size = 1000000;
#define run_test(S, N)                                                                                               \
  {                                                                                                                  \
    auto rand1 = Evidence::rand<size_t>(0, std::numeric_limits<size_t>::max());                                      \
    Evidence::Duration duration;                                                                                     \
    for (size_t i = 0; i < size; ++i) {                                                                              \
      S.emplace(rand1());                                                                                            \
    }                                                                                                                \
    LOG(INFO) << "Insert 1M random ulong values into " << #N << "::set, taken: " << duration.elapsedMs();            \
    duration.reset();                                                                                                \
    const auto end = S.end();                                                                                        \
    auto found = 0;                                                                                                  \
    for (size_t i = 0; i < size; ++i) {                                                                              \
      if (S.find(rand()) != end) {                                                                                   \
        ++found;                                                                                                     \
      }                                                                                                              \
    }                                                                                                                \
    LOG(INFO) << "Found " << found << " in 1M random values from " << #N << "::set, taken:" << duration.elapsedMs(); \
  }

  std::unordered_set<size_t> std_set;
  run_test(std_set, std);

  robin_hood::unordered_set<size_t> rh_set;
  run_test(rh_set, robin_hood);

  folly::F14FastSet<size_t> f14_set;
  run_test(f14_set, f14);
}
} // namespace test
} // namespace common
} // namespace nebula