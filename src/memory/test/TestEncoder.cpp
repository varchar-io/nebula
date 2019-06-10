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
} // namespace test
} // namespace memory
} // namespace nebula