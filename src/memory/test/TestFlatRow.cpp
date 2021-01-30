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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "memory/FlatRow.h"

/**
 * Flat Buffer is used to store / compute run time data. 
 * Test its interfaces and functions here.
 */
namespace nebula {
namespace memory {
namespace test {

using nebula::memory::FlatRow;

TEST(FlatRowTest, TestFlatRow) {
  // initialize a flat row with given schema
  FlatRow row(1024);

  // write a few key-values repeatly 1K times
  for (auto i = 0; i < 1000; ++i) {
    row.reset();

    std::vector<std::string> strList = { "reading", "coding", "movies", "hiking" };
    std::vector<int64_t> longList = { 3l, 4l, 5l };
    row.write("id", 2);
    row.write("name", "nebula");
    row.write("weight", 23.5f);
    row.write("hobbies", strList);
    row.write("scores", longList);
    row.write("memo", "memo status");
    row.write("flag", true);
    row.write("i128", int128_t(128));

    // row is ready to read now
    EXPECT_EQ(row.readBool("flag"), true);
    EXPECT_EQ(row.readInt("id"), 2);
    EXPECT_EQ(row.readString("name"), "nebula");
    EXPECT_EQ(row.readFloat("weight"), 23.5);
    EXPECT_TRUE(row.readInt128("i128") == 128);

    {
      auto list1 = row.readList("hobbies");
      auto size = list1->getItems();

      EXPECT_EQ(size, 4);
      for (auto i = 0; i < size; ++i) {
        auto str = list1->readString(i);
        EXPECT_EQ(str, strList[i]);
      }
    }

    {
      auto list1 = row.readList("scores");
      auto size = list1->getItems();

      EXPECT_EQ(size, 3);
      for (auto i = 0; i < size; ++i) {
        EXPECT_EQ(list1->readLong(i), longList[i]);
      }
    }
  }
}

TEST(FlatRowTest, TestFlatRowTreatMissingAsNulls) {
  // initialize a flat row with given schema
  FlatRow row(1024);
  EXPECT_THROW(row.isNull("abc"), std::out_of_range);

  FlatRow rowTreatsMissingAsNull(1024, true);
  EXPECT_TRUE(rowTreatsMissingAsNull.isNull("abc"));
}

} // namespace test
} // namespace memory
} // namespace nebula