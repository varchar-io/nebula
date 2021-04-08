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
#include <sstream>

#include "storage/CsvReader.h"

namespace nebula {
namespace storage {
namespace test {

TEST(CsvTest, TestNormal) {
  std::stringstream line("a,b,c");
  nebula::storage::CsvRow row(',');
  row.readNext(line);
  const auto& d = row.rawData();
  EXPECT_EQ(d.size(), 3);
  EXPECT_EQ(d.at(0), "a");
  EXPECT_EQ(d.at(1), "b");
  EXPECT_EQ(d.at(2), "c");
}

TEST(CsvTest, TestLineParse) {
  std::stringstream line("a, b ,c");
  nebula::storage::CsvRow row(',');
  row.readNext(line);
  const auto& d = row.rawData();
  EXPECT_EQ(d.size(), 3);
  EXPECT_EQ(d.at(0), "a");
  EXPECT_EQ(d.at(1), " b ");
  EXPECT_EQ(d.at(2), "c");
}

TEST(CsvTest, TestMultiLineParse) {
  std::stringstream line("a,b,c\n\n\nx,y,z\r\n");
  nebula::storage::CsvRow row(',');
  {
    row.readNext(line);
    const auto& d = row.rawData();
    EXPECT_EQ(d.size(), 3);
    EXPECT_EQ(d.at(0), "a");
    EXPECT_EQ(d.at(1), "b");
    EXPECT_EQ(d.at(2), "c");
  }
  {
    row.readNext(line);
    const auto& d = row.rawData();
    EXPECT_EQ(d.size(), 3);
    EXPECT_EQ(d.at(0), "x");
    EXPECT_EQ(d.at(1), "y");
    EXPECT_EQ(d.at(2), "z");
    EXPECT_FALSE(!line);
  }
  {
    row.readNext(line);
    const auto& d = row.rawData();
    EXPECT_EQ(d.size(), 0);
    EXPECT_TRUE(!line);
  }
}

TEST(CsvTest, TestEscapedFields) {
  // test cases and expected results
  std::vector<std::tuple<std::string, std::vector<std::string>>> cases = {
    { "a ,\"b\",c", { "a ", "b", "c" } },
    { "a,\"b,\r\nx\"\"y\"\"\", c", { "a", "b,\r\nx\"y\"", " c" } },
    { "a,, c\r\n", { "a", "", " c" } },
  };

  for (auto& tp : cases) {
    std::stringstream line(std::get<0>(tp));
    auto& expected = std::get<1>(tp);
    nebula::storage::CsvRow row(',');
    row.readNext(line);
    const auto& d = row.rawData();
    EXPECT_EQ(d.size(), expected.size());
    EXPECT_EQ(d, expected);
  }
}

TEST(CsvTest, TestEmptyField) {
  // test cases and expected results
  std::vector<std::tuple<std::string, std::vector<std::string>>> cases = {
    { "a,,c", { "a", "", "c" } },
    { "a,,", { "a", "", "" } },
    { ",,", { "", "", "" } },
  };

  for (auto& tp : cases) {
    std::stringstream line(std::get<0>(tp));
    auto& expected = std::get<1>(tp);
    nebula::storage::CsvRow row(',');
    row.readNext(line);
    const auto& d = row.rawData();
    EXPECT_EQ(d.size(), expected.size());
    EXPECT_EQ(d, expected);
  }
}

} // namespace test
} // namespace storage
} // namespace nebula