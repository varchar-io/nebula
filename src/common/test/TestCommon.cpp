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
#include "Errors.h"
#include "fmt/format.h"
#include "glog/logging.h"

namespace nebula {
namespace common {
namespace test {

TEST(ErrorTests, Ensures) {
  // test generic ensure
  N_ENSURE(3 > 2, "true");
  try {
    N_ENSURE(2 > 3, "false");
  } catch (const NebulaException& ex) {
    LOG(INFO) << ex.toString();
  }

  // test ensure equals
  N_ENSURE_EQ((3 + 3), 6, "3+3 should be 6");
  try {
    N_ENSURE_EQ((3 + 3), 7, "3+3 should not be 7");
  } catch (const NebulaException& ex) {
    LOG(INFO) << ex.toString();
  }

  // test ensure greater or equals
  N_ENSURE_GE((3 + 3), 6, "3+3 should be 6");
  N_ENSURE_GT((3 + 3), 5, "3+3 should greater than 5");
  try {
    N_ENSURE_GE((3 + 3), 7, "3+3 should not be 7");
  } catch (const NebulaException& ex) {
    LOG(INFO) << ex.toString();
  }

  // test ensure less than
  N_ENSURE_LE((3 + 3), 6, "3+3 should be 6");
  N_ENSURE_LT((3 + 3), 7, "3+3 should less than 7");
  try {
    N_ENSURE_LT((3 + 3), 6, "3+3 should not be less than 6");
  } catch (const NebulaException& ex) {
    LOG(INFO) << ex.toString();
  }

  // test
  N_ENSURE_NE(3 + 3, 5, "3+3 not equal 5");
  try {
    N_ENSURE_NE((3 + 3), 6, "3+3 should equal 6");
  } catch (const NebulaException& ex) {
    LOG(INFO) << ex.toString();
  }
}

} // namespace test
} // namespace common
} // namespace nebula