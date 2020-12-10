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
  EXPECT_EQ(nebula::meta::extractPatternMacro("DATE"), nebula::meta::PatternMacro::DATE);

  EXPECT_EQ(nebula::meta::extractPatternMacro("DATE HOUR"), nebula::meta::PatternMacro::HOUR);

  EXPECT_EQ(nebula::meta::extractPatternMacro("HOUR DATE"), nebula::meta::PatternMacro::HOUR);

  EXPECT_EQ(nebula::meta::extractPatternMacro("DATE HOURMINUTE"), nebula::meta::PatternMacro::MINUTE);

  EXPECT_EQ(nebula::meta::extractPatternMacro("DATE HOUR #MINUTE-SECOND"), nebula::meta::PatternMacro::SECOND);

  EXPECT_EQ(nebula::meta::extractPatternMacro("TIMESTAMP"), nebula::meta::PatternMacro::TIMESTAMP);
  // has to be ts along
  EXPECT_EQ(nebula::meta::extractPatternMacro("TIMESTAMP SECOND"), nebula::meta::PatternMacro::INVALID);

  EXPECT_EQ(nebula::meta::extractPatternMacro("dATE"), nebula::meta::PatternMacro::INVALID);
  // has to be date/hour/minute/second
  EXPECT_EQ(nebula::meta::extractPatternMacro("SECOND"), nebula::meta::PatternMacro::INVALID);
  EXPECT_EQ(nebula::meta::extractPatternMacro("HOUR"), nebula::meta::PatternMacro::INVALID);
  EXPECT_EQ(nebula::meta::extractPatternMacro("MINUTE"), nebula::meta::PatternMacro::INVALID);

  LOG(INFO) << "ExtractPatternMacro extract macro from pattern string";
}
} // namespace test
} // namespace meta
} // namespace nebula