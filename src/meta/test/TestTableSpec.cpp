/*
 * Copyright 2020-present
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

#include "meta/ClusterInfo.h"

namespace nebula {
namespace meta {
namespace test {

/**
 * Test extract pattern marco from table spec configuration
 */
TEST(MetaTest, TestExtractPatternMacro) {
  EXPECT_EQ(nebula::meta::extractPatternMacro("daily"), nebula::meta::PatternMacro::DAILY);

  EXPECT_EQ(nebula::meta::extractPatternMacro("HOURLY"), nebula::meta::PatternMacro::HOURLY);

  EXPECT_EQ(nebula::meta::extractPatternMacro("Hourly"), nebula::meta::PatternMacro::HOURLY);

  EXPECT_EQ(nebula::meta::extractPatternMacro("MINUTELY"), nebula::meta::PatternMacro::MINUTELY);

  EXPECT_EQ(nebula::meta::extractPatternMacro("SECONDLY"), nebula::meta::PatternMacro::SECONDLY);

  EXPECT_EQ(nebula::meta::extractPatternMacro("timestamp"), nebula::meta::PatternMacro::TIMESTAMP);

  EXPECT_EQ(nebula::meta::extractPatternMacro("DATE"), nebula::meta::PatternMacro::INVALID);
  // has to be date/hour/minute/second
  EXPECT_EQ(nebula::meta::extractPatternMacro("second"), nebula::meta::PatternMacro::INVALID);
  EXPECT_EQ(nebula::meta::extractPatternMacro("hour"), nebula::meta::PatternMacro::INVALID);
  EXPECT_EQ(nebula::meta::extractPatternMacro("minute"), nebula::meta::PatternMacro::INVALID);

  LOG(INFO) << "ExtractPatternMacro extract macro from pattern string";
}
} // namespace test
} // namespace meta
} // namespace nebula