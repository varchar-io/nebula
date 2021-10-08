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
  std::stringstream line("abc,\"{\"\"a\"\": \"\"123\"\", \"\"b\"\": \"\"456\"\"}\",99");
  nebula::storage::CsvRow row(',');
  row.readNext(line);
  const auto& d = row.rawData();
  EXPECT_EQ(d.size(), 3);
  EXPECT_EQ(d.at(0), "abc");
  EXPECT_EQ(d.at(1), "{\"a\": \"123\", \"b\": \"456\"}");
  EXPECT_EQ(d.at(2), "99");
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

TEST(CsvTest, TestMultiLineEndWithEmpty) {
  std::stringstream line("a,b,c,\n\n\nx,y,z,\r\n");
  nebula::storage::CsvRow row(',');
  {
    row.readNext(line);
    const auto& d = row.rawData();
    EXPECT_EQ(d.size(), 4);
    EXPECT_EQ(d.at(0), "a");
    EXPECT_EQ(d.at(1), "b");
    EXPECT_EQ(d.at(2), "c");
    EXPECT_EQ(d.at(3), "");
  }
  {
    row.readNext(line);
    const auto& d = row.rawData();
    EXPECT_EQ(d.size(), 4);
    EXPECT_EQ(d.at(0), "x");
    EXPECT_EQ(d.at(1), "y");
    EXPECT_EQ(d.at(2), "z");
    EXPECT_EQ(d.at(3), "");
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

TEST(CsvTest, TestException) {
  // illegal test cases
  std::vector<std::string> cases = {
    "a,\" \"b,c"
  };

  for (auto& text : cases) {
    std::stringstream line(text);
    nebula::storage::CsvRow row(',');
    EXPECT_THROW(row.readNext(line), nebula::common::NebulaException);
  }
}

TEST(CsvTest, TestComplexRealCsv) {
  nebula::meta::CsvProps csv{ true, "," };
  nebula::storage::CsvReader reader("test/data/test.csv", csv, {});
  auto lines = 0;
  while (reader.hasNext()) {
    auto& r = reader.next();
    auto dt = r.readString("dt");
    EXPECT_EQ(dt, "2021-03-28");
    ++lines;
  }

  EXPECT_EQ(lines, 2);
}

TEST(CsvTest, TestCsvExportedFromGSheet) {
  nebula::meta::CsvProps csv{ true, "," };
  nebula::storage::CsvReader reader("test/data/birthrate.csv", csv, {});
  auto lines = 0;
  while (reader.hasNext()) {
    auto& r = reader.next();
    LOG(INFO) << "Row: " << ++lines << ", first column: " << r.readString("Country Name");
  }

  EXPECT_EQ(lines, 267);
}

TEST(CsvTest, TestParseFormattedNumber) {
  std::stringstream line("abc,\"123,456\",99");
  nebula::storage::CsvRow row(',');
  row.setSchema([](const std::string& name) {
    if (name == "name") { return 0; }
    if (name == "value") { return 1; }
    if (name == "rank") { return 2; }
    return -1;
  });

  row.readNext(line);
  const auto& d = row.rawData();
  EXPECT_EQ(d.size(), 3);
  EXPECT_EQ(d.at(0), "abc");
  EXPECT_EQ(d.at(1), "123,456");
  EXPECT_EQ(d.at(2), "99");

  // csv data relies on folly::to if it can't recognize the format?
  // EXPECT_EQ(folly::to<int64_t>("123,456"), 123456);

  // schema of the data is <string, bigint, int>
  EXPECT_EQ(row.readString("name"), "abc");
  EXPECT_EQ(row.readLong("value"), 123456);
  EXPECT_EQ(row.readInt("rank"), 99);
}

} // namespace test
} // namespace storage
} // namespace nebula