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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/Memory.h"

namespace nebula {
namespace common {
namespace test {

TEST(SliceTest, TestWriteEmptySlice) {
#define WRITE_TO_EMTPY_SLICE(V, S)            \
  {                                           \
    nebula::common::ExtendableSlice slice(0); \
    EXPECT_EQ(slice.size(), 0);               \
    auto size = slice.write(0, V);            \
    EXPECT_TRUE(slice.size() > 0);            \
    EXPECT_EQ(size, S);                       \
  }

  WRITE_TO_EMTPY_SLICE(0, 4)
  WRITE_TO_EMTPY_SLICE(false, 1)
  WRITE_TO_EMTPY_SLICE(1L, 8)
#undef WRITE_TO_EMTPY_SLICE
}

} // namespace test
} // namespace common
} // namespace nebula
