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
#include "common/Errors.h"
#include "fmt/format.h"
#include <glog/logging.h>
#include "roaring.hh"

/**
 * Test namespace for testing external dependency APIs
 */
namespace nebula {
namespace common {
namespace test {
// copied from the example of croaring page in github
TEST(RoraringTest, TestRoaringBitmap) {
  Roaring r1;
  for (uint32_t i = 100; i < 1000; i++) {
    r1.add(i);
  }

  // check whether a value is contained
  EXPECT_TRUE(r1.contains(500));
  LOG(INFO) << "r1  cardinality=" << r1.cardinality();

  // if your bitmaps have long runs, you can compress them by calling
  // run_optimize
  uint32_t size = r1.getSizeInBytes();
  r1.runOptimize();

  uint32_t compact_size = r1.getSizeInBytes();
  LOG(INFO) << fmt::format("size optimization: before={0}, after={1}.", size, compact_size);

  // create a new bitmap with varargs
  Roaring r2 = Roaring::bitmapOf(5, 1, 2, 3, 5, 6);
  LOG(INFO) << "r2: " << r2.toString();

  // we can also create a bitmap from a pointer to 32-bit integers
  const uint32_t values[] = { 2, 3, 4 };
  Roaring r3(3, values);

  // we can also go in reverse and go from arrays to bitmaps
  uint64_t card1 = r1.cardinality();
  uint32_t* arr1 = new uint32_t[card1];
  r1.toUint32Array(arr1);
  Roaring r1f(card1, arr1);
  delete[] arr1;

  // bitmaps shall be equal
  EXPECT_TRUE(r1 == r1f);

  // we can copy and compare bitmaps
  Roaring z(r3);
  EXPECT_TRUE(r3 == z);

  // we can compute union two-by-two
  Roaring r1_2_3 = r1 | r2;
  r1_2_3 |= r3;

  // we can compute a big union
  const Roaring* allmybitmaps[] = { &r1, &r2, &r3 };
  Roaring bigunion = Roaring::fastunion(3, allmybitmaps);
  EXPECT_TRUE(r1_2_3 == bigunion);

  // we can compute intersection two-by-two
  Roaring i1_2 = r1 & r2;

  // we can write a bitmap to a pointer and recover it later
  uint32_t expectedsize = r1.getSizeInBytes();
  char* serializedbytes = new char[expectedsize];
  r1.write(serializedbytes);
  Roaring t = Roaring::read(serializedbytes);
  EXPECT_TRUE(r1 == t);
  delete[] serializedbytes;

  // we can iterate over all values using custom functions
  uint32_t counter = 0;
  r1.iterate(
    [](uint32_t value, void* param) {
      *(uint32_t*)param += value;
      return true;
    },
    &counter);

  // we can move iterators to skip values
  const uint32_t manyvalues[] = { 2, 3, 4, 7, 8 };
  Roaring rogue(5, manyvalues);
  Roaring::const_iterator j = rogue.begin();
  j.equalorlarger(4);
  EXPECT_EQ(*j, 4);
}
} // namespace test
} // namespace common
} // namespace nebula