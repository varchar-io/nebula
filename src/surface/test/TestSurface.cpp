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
#include <valarray>
#include "common/Evidence.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "surface/DataSurface.h"

namespace nebula {
namespace memory {
namespace test {

using namespace nebula::common;

TEST(SurfaceTest, TestDataSurface) {
  // these are all dumb right, they are just show case the interfaces usage
  // bool, int, string, list<float>
  nebula::surface::MockRowData mockRow;

  N_ENSURE(!(mockRow.isNull("x")), "no nulls in mock data");

  auto list = mockRow.readList("list");
  N_ENSURE_EQ(list->getItems(), 4, "mock 4 items");
  LOG(INFO) << "String column: " << mockRow.readString("str_col");
  for (int i = 0; i < 4; ++i) {
    LOG(INFO) << "List item: " << list->readFloat(i);
  }

  // pretend we have a map<int, string>
  auto map = mockRow.readMap("map");
  auto keys = map->readKeys();
  auto values = map->readValues();
  N_ENSURE_EQ(map->getItems(), keys->getItems(), "map items equals key items");
  N_ENSURE_EQ(map->getItems(), values->getItems(), "map items equals value items");

  for (auto i = 0, size = map->getItems(); i < size; ++i) {
    LOG(INFO) << "map: key=" << keys->readInt(i) << ",value=" << values->readString(i);
  }
}

TEST(SurfaceTest, TestSameMockDataWithSameSeed) {
  const auto seed = Evidence::unix_timestamp();
  nebula::surface::MockRowData mock1(seed);
  nebula::surface::MockRowData mock2(seed);

  for (auto i = 0; i < 1024; ++i) {
    EXPECT_EQ(mock1.readBool("b"), mock2.readBool("b"));
    EXPECT_EQ(mock1.readByte("b"), mock2.readByte("b"));
    EXPECT_EQ(mock1.readInt("b"), mock2.readInt("b"));
    EXPECT_EQ(mock1.readFloat("b"), mock2.readFloat("b"));
    EXPECT_EQ(mock1.readString("b"), mock2.readString("b"));
  }
}

} // namespace test
} // namespace memory
} // namespace nebula