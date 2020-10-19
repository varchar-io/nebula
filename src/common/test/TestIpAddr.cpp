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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/Ip.h"

/**
 * Test some open hash set immplementations
 */
namespace nebula {
namespace common {
namespace test {
TEST(CommonTest, TestIpAddr) {
  auto h1 = nebula::common::Ip::hostInfo();
  LOG(INFO) << "localhost<< " << h1.host << ", " << h1.ipv4;

#ifndef __APPLE__
  auto h2 = nebula::common::Ip::hostInfo(false);
  LOG(INFO) << "dns<< " << h2.host << ", " << h2.ipv4;
#endif
}
} // namespace test
} // namespace common
} // namespace nebula