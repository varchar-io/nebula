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
#include "fmt/format.h"
#include "meta/NBlock.h"
#include "meta/Table.h"
#include "meta/TestTable.h"

namespace nebula {
namespace meta {
namespace test {

TEST(MetaTest, TestTestTable) {
  LOG(INFO) << "Table provides table logic data and physical data";
  LOG(INFO) << "Test table name: " << TestTable::name();
  LOG(INFO) << "Test table schema: " << TestTable::schema();
}

TEST(MetaTest, TestNBlock) {
  MockTable table("mock");
  NBlock b1(table, 0, 5, 10);

  // check in range
  ASSERT_TRUE(b1.inRange(5));
  ASSERT_TRUE(b1.inRange(10));
  ASSERT_TRUE(b1.inRange(7));
  ASSERT_FALSE(b1.inRange(11));
  ASSERT_FALSE(b1.inRange(1));

  // check overlap
  ASSERT_TRUE(b1.overlap({ 10, 11 }));
  ASSERT_TRUE(b1.overlap({ 5, 11 }));
  ASSERT_TRUE(b1.overlap({ 2, 11 }));
  ASSERT_TRUE(b1.overlap({ 2, 5 }));
  ASSERT_FALSE(b1.overlap({ 2, 4 }));
  ASSERT_FALSE(b1.overlap({ 12, 18 }));
}

} // namespace test
} // namespace meta
} // namespace nebula