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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "meta/NBlock.h"
#include "meta/Table.h"
#include "meta/TestTable.h"

namespace nebula {
namespace meta {
namespace test {

TEST(MetaTest, TestTestTable) {
  nebula::meta::TestTable test;
  LOG(INFO) << "Table provides table logic data and physical data";
  LOG(INFO) << "Test table name: " << test.name();
  LOG(INFO) << "Test table schema: " << test.schema();
}

TEST(MetaTest, TestNBlock) {
  BlockState state;
  NBlock<int> b1({ "mock", 0, 5, 10 }, NNode::inproc(), state);

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

TEST(MetaTest, TestNNode) {
  NNode n1{ NRole::NODE, "1.0.0.1", 90 };
  NNode n2{ n1 };
  ASSERT_TRUE(n1.equals(n2));
  LOG(INFO) << "N2=" << n2.toString();
}

} // namespace test
} // namespace meta
} // namespace nebula