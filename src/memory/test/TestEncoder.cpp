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
#include "common/Memory.h"
#include "fmt/format.h"
#include "memory/Batch.h"
#include "memory/DataNode.h"
#include "memory/FlatRow.h"
#include "memory/encode/RleDecoder.h"
#include "memory/encode/RleEncoder.h"
#include "memory/encode/Utils.h"
#include "meta/Table.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace memory {
namespace test {
TEST(TypeDataTest, TestAddTypedData) {
  nebula::meta::Column column;

  auto b = nebula::memory::serde::TypeDataFactory::createData(nebula::type::Kind::BOOLEAN, column, 16);
  b->add(0, true);
  b->add(1, false);
  b->add(2, false);
  b->add(3, true);
  EXPECT_EQ(b->read<bool>(0), true);
  EXPECT_EQ(b->read<bool>(1), false);
  EXPECT_EQ(b->read<bool>(2), false);
  EXPECT_EQ(b->read<bool>(3), true);
}

TEST(TypeDataTest, TestStringReadWrite) {
  nebula::meta::Column column;
  auto s = nebula::memory::serde::TypeDataFactory::createData(nebula::type::Kind::VARCHAR, column, 16);
  std::string_view s1 = "Nebula";
  std::string_view s2 = "Is";
  std::string_view s3 = "So";
  std::string_view s4 = "Awesome";
  std::string_view s5 = "!";
  s->add(0, s1);
  s->add(1, s2);
  s->add(2, s3);
  s->add(3, s4);
  s->add(4, s5);

  EXPECT_EQ(s->read(0, 6), s1);
  EXPECT_EQ(s->read(6, 2), s2);
  EXPECT_EQ(s->read(8, 2), s3);
  EXPECT_EQ(s->read(10, 7), s4);
  EXPECT_EQ(s->read(17, 1), s5);
}

void testRle(int64_t* values, int size) {
  nebula::common::PagedSlice slice(4096);
  nebula::memory::encode::RleEncoder encoder(true, slice);

  for (int i = 0; i < size; ++i) {
    encoder.write(*(values + i));
  }

  encoder.flush();
  LOG(INFO) << "slice capacity: " << slice.capacity();

  // decode it
  nebula::memory::encode::RleDecoder decoder(true, slice);
  int64_t decoded[size];
  decoder.next(decoded, size);
  for (int i = 0; i < size; ++i) {
    ASSERT_EQ(decoded[i], values[i]);
  }
}

#define SIZE(data) (sizeof(data) / sizeof(int64_t))

TEST(RleTest, TestShortRun) {
  int64_t data[] = { 139, 222, 222, 222, 222, 34543245, 23232, 232334, 4545, 4545, 4545, 4545, 4545, 99232329, 9933434 };
  testRle(data, SIZE(data));
}

TEST(RleTest, TestDirect) {
  auto r = nebula::common::Evidence::rand(-1000, 1000);
  constexpr auto items = 4096;
  // delta encoding should work
  int64_t data[items];
  for (auto i = 0; i < items; ++i) {
    data[i] = r();
  }

  testRle(data, items);
}

TEST(RleTest, TestRlePatchedBase) {
  int64_t data[] = { 2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090,
                     2100, 2110, 2120, 2130, 2140, 2150, 2160, 2170, 2180, 2190 };
  testRle(data, SIZE(data));
}

TEST(RleTest, TestDelta) {
  constexpr auto items = 10240;
  // delta encoding should work
  int64_t data[items];
  auto delta = 1;
  for (auto i = 0; i < items; ++i) {
    // step up
    if (i % 100 == 0) {
      delta += 1;
    }

    data[i] = i * delta;
  }

  testRle(data, items);
}

TEST(RleTest, TestDelta2) {
  int64_t data[] = { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29 };
  testRle(data, SIZE(data));
}

TEST(RleTest, TestZigzag) {
  int64_t data[] = { 139, 222, 34543245, 23232, 232334 };
  constexpr auto size = SIZE(data);
  int64_t encoded[size];
  int64_t decoded[size];

  // encode
  for (size_t i = 0; i < size; ++i) {
    encoded[i] = nebula::memory::encode::zigZag(data[i]);
  }

  // decode
  for (size_t i = 0; i < size; ++i) {
    decoded[i] = nebula::memory::encode::unZigZag(encoded[i]);
  }

  // verify
  for (size_t i = 0; i < size; ++i) {
    EXPECT_EQ(decoded[i], data[i]);
  }
}

#undef SIZE

} // namespace test
} // namespace memory
} // namespace nebula