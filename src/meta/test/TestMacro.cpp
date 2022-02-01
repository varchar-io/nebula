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

#include "rapidjson/document.h"

#include "common/Errors.h"
#include "meta/Macro.h"
#include "common/Params.h"

namespace nebula {
namespace meta {
namespace test {

using nebula::meta::Macro;
using nebula::meta::PatternMacro;

/**
 * Test extract pattern marco from table spec configuration
 */
TEST(MacroTest, TestExtract) {
  EXPECT_EQ(Macro::extract("s3://nebula/{date}"), PatternMacro::DAILY);

  EXPECT_EQ(Macro::extract("s3://nebula/{date}/{HOUR}"), PatternMacro::HOURLY);

  // hour requires date
  EXPECT_EQ(Macro::extract("gs://nebula/{HOUR}"), PatternMacro::INVALID);

  EXPECT_EQ(Macro::extract("/var/nebula/{date}/{HOUR}/{minute}"), PatternMacro::MINUTELY);
  EXPECT_EQ(Macro::extract("/var/nebula/{date}/{minute}"), PatternMacro::INVALID);
  EXPECT_EQ(Macro::extract("/var/nebula/{hour}/{minute}"), PatternMacro::INVALID);

  EXPECT_EQ(Macro::extract("/var/nebula/{date}{hour}{minute}{second}"), PatternMacro::SECONDLY);
  EXPECT_EQ(Macro::extract("/var/nebula/{second}"), PatternMacro::INVALID);
  EXPECT_EQ(Macro::extract("/var/nebula/{minute}/{second}"), PatternMacro::INVALID);
  EXPECT_EQ(Macro::extract("/var/nebula/{hour}/{second}"), PatternMacro::INVALID);
  EXPECT_EQ(Macro::extract("/var/nebula/{date}/{hour}/minute/{second}"), PatternMacro::INVALID);

  EXPECT_EQ(Macro::extract("/var/nebula/{timestamp}"), PatternMacro::TIMESTAMP);
  EXPECT_EQ(Macro::extract("/var/nebula/{date}/{timestamp}"), PatternMacro::INVALID);
}

TEST(MacroTest, TestMaterialize) {
  std::time_t time(1611708922);

  // invalid
  EXPECT_EQ(Macro::materialize(PatternMacro::INVALID,
                               "s3://nebula/{timestamp}/x.csv",
                               time),
            "s3://nebula/{timestamp}/x.csv");

  // timestamp
  EXPECT_EQ(Macro::materialize(PatternMacro::TIMESTAMP,
                               "s3://nebula/{Timestamp}/x.csv",
                               time),
            "s3://nebula/1611708922/x.csv");

  EXPECT_EQ(Macro::materialize(PatternMacro::TIMESTAMP,
                               "s3://nebula/{time}/x.csv",
                               time),
            "s3://nebula/{time}/x.csv");

  // daily
  EXPECT_EQ(Macro::materialize(PatternMacro::DAILY,
                               "s3://nebula/{date}/x.csv",
                               time),
            "s3://nebula/2021-01-27/x.csv");
  EXPECT_EQ(Macro::materialize(PatternMacro::DAILY,
                               "s3://nebula/{date}/{hour}/x.csv",
                               time),
            "s3://nebula/2021-01-27/{hour}/x.csv");

  // hourly
  EXPECT_EQ(Macro::materialize(PatternMacro::HOURLY,
                               "s3://nebula/{date}/x.csv",
                               time),
            "s3://nebula/2021-01-27/x.csv");
  EXPECT_EQ(Macro::materialize(PatternMacro::HOURLY,
                               "s3://nebula/{date}/{hour}/x.csv",
                               time),
            "s3://nebula/2021-01-27/00/x.csv");

  // minutely
  EXPECT_EQ(Macro::materialize(PatternMacro::MINUTELY,
                               "s3://nebula/{date}/{hour}/{minute}/x.csv",
                               time),
            "s3://nebula/2021-01-27/00/55/x.csv");
  EXPECT_EQ(Macro::materialize(PatternMacro::MINUTELY,
                               "s3://nebula/{minute}/x.csv",
                               time),
            "s3://nebula/55/x.csv");

  // secondly
  EXPECT_EQ(Macro::materialize(PatternMacro::SECONDLY,
                               "s3://nebula/{minute}{second}/x.csv",
                               time),
            "s3://nebula/5522/x.csv");
  EXPECT_EQ(Macro::materialize(PatternMacro::SECONDLY,
                               "s3://nebula/{date}/{hour}/{minute}{no}{second}/x.csv",
                               time),
            "s3://nebula/2021-01-27/00/55{no}22/x.csv");
}

TEST(MacroTest, TestWatermark) {
  const auto json = "{\"date\":[\"2020-01-01\"], \"hour\" : [\"01\", \"02\"]}";
  rapidjson::Document cd;
  if (cd.Parse(json).HasParseError()) {
    throw NException(fmt::format("Error parsing params-json: {0}", json));
  }

  common::ParamList list(cd);
  auto p = list.next();
  std::vector<size_t> wms;
  while (p.size() > 0) {
    auto watermark = Macro::watermark(p);
    wms.push_back(watermark);
    p = list.next();
  }
  EXPECT_EQ(wms.size(), 2);
  // 2020-01-01 01:00:00
  EXPECT_EQ(wms.at(0), 1577840400);
  // 2020-01-01 02:00:00
  EXPECT_EQ(wms.at(1), 1577844000);
}

TEST(MacroTest, TestCustomMacros1) {
  const auto path = "{a}/{b}/{c}";
  const std::map<std::string, std::vector<std::string>> macroValues = {
    {"a", {"1", "2"}},
    {"b", {"3", "4"}},
    {"c", {"5", "6"}},
  };
  const auto paths = Macro::enumeratePathsWithCustomMacros(path, macroValues);
  const std::vector<std::string> expectedPaths = {
    "1/3/5",
    "1/3/6",
    "1/4/5",
    "1/4/6",
    "2/3/5",
    "2/3/6",
    "2/4/5",
    "2/4/6"
  };

  EXPECT_EQ(paths.size(), 8);
  for (const auto& expectedPath : expectedPaths) {
   EXPECT_NE(std::find(paths.begin(), paths.end(), expectedPath), paths.end()); 
  }
}

TEST(MacroTest, TestCustomMacros2) {
  const auto path = "{a}/{b}";
  const std::map<std::string, std::vector<std::string>> macroValues = {
    {"a", {"1"}},
    {"b", {"2", "3"}},
  };
  const auto paths = Macro::enumeratePathsWithCustomMacros(path, macroValues);
  const std::vector<std::string> expectedPaths = {"1/2", "1/3"};

  EXPECT_EQ(paths.size(), 2);
  for (const auto& expectedPath : expectedPaths) {
   EXPECT_NE(std::find(paths.begin(), paths.end(), expectedPath), paths.end()); 
  }
}

TEST(MacroTest, TestCustomMacros3) {
  const auto path = "a/b";
  const std::map<std::string, std::vector<std::string>> macroValues = {
    {"c", {"1", "2", "3"}}
  };
  const auto paths = Macro::enumeratePathsWithCustomMacros(path, macroValues);

  EXPECT_EQ(paths.size(), 1);
  EXPECT_EQ(paths[0], "a/b");
}

} // namespace test
} // namespace meta
} // namespace nebula