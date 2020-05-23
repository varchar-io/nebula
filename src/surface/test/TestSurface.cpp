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
#include <glog/logging.h>
#include <valarray>
#include "common/Evidence.h"
#include "fmt/format.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "surface/eval/Script.h"

namespace nebula {
namespace memory {
namespace test {

using namespace nebula::common;

TEST(SurfaceTest, TestDataSurface) {
  // these are all dumb right, they are just show case the interfaces usage
  // bool, int, string, list<float>
  nebula::surface::MockRowData mockRow;

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

TEST(SurfaceTest, TestInt128Surface) {
  const auto seed = Evidence::unix_timestamp();
  nebula::surface::MockRowData mock1(seed);
  nebula::surface::MockRowData mock2(seed);

  for (auto i = 0; i < 1024; ++i) {
    EXPECT_EQ(mock1.readInt128("b"), mock2.readInt128("b"));
  }
}

TEST(SurfaceTest, TestScriptContext) {
  const auto seed = Evidence::unix_timestamp();
  nebula::surface::MockRowData mock1(seed);
  nebula::surface::MockRowData mock2(seed);
  nebula::surface::eval::ScriptContext script(
    [&mock1]() -> const nebula::surface::RowData& {
      return mock1;
    },
    [](const std::string& col) -> auto {
      if (col == "c") {
        return nebula::type::Kind::INTEGER;
      }
      if (col == "str") {
        return nebula::type::Kind::VARCHAR;
      }

      return nebula::type::Kind::INVALID;
    });

  // define the function in current eval context
  // int 32 value overflow:
  // C++: (1896463358 * 2 - 2147483648-2147483648)
  // JS: ??
  bool valid;
  script.eval<int32_t>("var x = () => nebula.column('c') - 100;", valid);
  script.eval<bool>("var y = () => nebula.column('str').length;", valid);
  for (auto i = 0; i < 16; ++i) {
    auto x = script.eval<int32_t>("x();", valid);
    auto col_c = mock2.readInt("c");
    EXPECT_TRUE(valid);
    EXPECT_EQ(x, col_c - 100);

    auto y = script.eval<int32_t>("y();", valid);
    auto col_str = mock2.readString("str");
    EXPECT_TRUE(valid);
    EXPECT_EQ(y, col_str.size());
  }
}

} // namespace test
} // namespace memory
} // namespace nebula