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

#include "meta/Partition.h"

/**
 * Test namespace for testing external dependency APIs
 */
namespace nebula {
namespace meta {
namespace test {

TEST(PartitionTest, TestPartitionKey) {
  {
    nebula::meta::PartitionKey<std::string> country{
      "country",
      { "US", "CA", "FR", "BR", "GB" },
      2
    };

    EXPECT_EQ(std::ceil(std::log2(3)), 2);

    // 2, 2, 1 - disjoint set
    EXPECT_EQ(country.spaces(), 3);

    // bits needed to encode each space
    EXPECT_EQ(country.width(), 1);

    // get dimension value for given space and its bits value
    EXPECT_EQ(country.mask(), 1);

    // value 5 (bit value 1) in space 0 => CA
    EXPECT_EQ(country.value(5, 0), "CA");
    EXPECT_EQ(country.value(16, 0), "US");
    EXPECT_EQ(country.value(188, 1), "FR");
    EXPECT_EQ(country.value(189, 1), "BR");
    EXPECT_EQ(country.value(98, 2), "GB");
    EXPECT_THROW(country.value(99, 2), std::out_of_range);

    // check space where a value is at
    Position p1{ 0, 0 };
    EXPECT_EQ(country.space("US"), p1);
    Position p2{ 1, 1 };
    EXPECT_EQ(country.space("BR"), p2);
    Position p3{ 2, 0 };
    EXPECT_EQ(country.space("GB"), p3);
    EXPECT_THROW(country.space("SP"), nebula::common::NebulaException);

    // check space range for given values
    Span s1{ 0, 0 };
    EXPECT_EQ(country.span({ "US", "CA" }), s1);
    Span s2{ 0, 1 };
    EXPECT_EQ(country.span({ "BR", "CA" }), s2);
    Span s3{ 0, 2 };
    EXPECT_EQ(country.span({ "GB", "CA" }), s3);
  }
  {
    nebula::meta::PartitionKey<int32_t> age{
      "age",
      { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 },
      3
    };

    // 2, 2, 1 - disjoint set
    EXPECT_EQ(age.spaces(), 5);

    // bits needed to encode each space
    EXPECT_EQ(age.width(), 2);

    // get dimension value for given space and its bits value
    EXPECT_EQ(age.mask(), 3);

    // value 5 (bit value 1) in space 0 => CA
    EXPECT_EQ(age.value(5, 0), 2);
    EXPECT_EQ(age.value(2, 0), 3);
    EXPECT_EQ(age.value(0, 2), 7);
    EXPECT_EQ(age.value(6, 2), 9);
    EXPECT_EQ(age.value(2, 4), 15);

    // check space where a value is at
    Position p1{ 1, 1 };
    EXPECT_EQ(age.space(5), p1);
    Position p2{ 2, 1 };
    EXPECT_EQ(age.space(8), p2);
    Position p3{ 3, 0 };
    EXPECT_EQ(age.space(10), p3);
    EXPECT_THROW(age.space(0), nebula::common::NebulaException);

    // check slot method is working
    EXPECT_EQ(age.slot("5"), p1);
    EXPECT_EQ(age.slot("8"), p2);
    EXPECT_EQ(age.slot("10"), p3);

    // check space range for given values
    Span s1{ 0, 2 };
    EXPECT_EQ(age.span({ 1, 5, 8 }), s1);
    Span s2{ 1, 2 };
    EXPECT_EQ(age.span({ 4, 7 }), s2);
    Span s3{ 0, 4 };
    EXPECT_EQ(age.span({ 13, 8, 3 }), s3);
  }
}

TEST(PartitionTest, TestPartitions) {
  nebula::meta::PartitionKey<std::string> country{
    "country",
    { "US", "CA", "FR", "BR", "GB" },
    2
  };

  nebula::meta::PartitionKey<std::string> gender{
    "gender",
    { "Male", "Female", "Unknown" }
  };

  nebula::meta::PartitionKey<int8_t> age{
    "age",
    { 1, 2, 3, 4, 5, 6, 7, 8, 9 },
    2
  };

  // test values API to get value list for given space
  {
    std::vector<std::string> expected0{ "US", "CA" };
    EXPECT_EQ(country.values(0), expected0);
    std::vector<std::string> expected2{ "GB" };
    EXPECT_EQ(country.values(2), expected2);
  }
  {
    std::vector<std::string> expected0{ "Male" };
    EXPECT_EQ(gender.values(0), expected0);
    std::vector<std::string> expected1{ "Female" };
    EXPECT_EQ(gender.values(1), expected1);
    std::vector<std::string> expected2{ "Unknown" };
    EXPECT_EQ(gender.values(2), expected2);
  }
  {
    std::vector<int8_t> expected2{ 5, 6 };
    EXPECT_EQ(age.values(2), expected2);
    std::vector<int8_t> expected4{ 9 };
    EXPECT_EQ(age.values(4), expected4);
  }

  nebula::meta::Cube cube{ std::move(country), std::move(gender), std::move(age) };
  auto capacity = cube.capacity();
  constexpr auto keys = cube.numKeys();
  LOG(INFO) << "the cube with " << keys << " dimensions has max blocks: " << capacity;

  // list offset for each key: 1, 3, 9
  for (size_t i = 0; i < keys; ++i) {
    LOG(INFO) << "key: " << i << ", offset:" << cube.offset(i);
  }

  // for any combination, we can locate which pod this value belongs to
  // "US"->0, "Unknown"->2, 7->3, so pid = 0*1 + 2*3 + 3 * 9 = 31
  EXPECT_EQ(cube.pod(std::make_tuple("US", "Unknown", 7)), 33);
  // "CA"->0, "Male"->0, 4->1, so pid = 0*1 + 0*3 + 1 * 9 = 9
  EXPECT_EQ(cube.pod(std::make_tuple("CA", "Male", 4)), 9);
  // "GB"->2, "Female"->1, 9->4, so pid = 2*1 + 1*3 + 4 * 9 = 9
  EXPECT_EQ(cube.pod(std::make_tuple("GB", "Female", 9)), 41);

  // test pod ID range for given predicates
  {
    // [0, 0], [1, 1], [1, 3] =>
    // low = 0*1 + 1*3 + 1*9 = 12
    // high = 0*1 + 1*3 + 3*9 = 30
    Span expected{ 12, 30 };
    Predicate<std::string> c{ "country", { "US" } };
    Predicate<std::string> g{ "gender", { "Female" } };
    Predicate<int8_t> a{ "age", { 3, 8 } };
    auto range = cube.span(std::make_tuple(c.values(), g.values(), a.values()));
    EXPECT_EQ(range, expected);
  }
  {
    // [0, 2], [2, 2], [2, 2] =>
    // low = 0*1 + 2*3 + 2*9 = 24
    // high = 2*1 + 2*3 + 2*9 = 26
    Span expected{ 24, 26 };
    Predicate<std::string> c{ "country", {} };
    Predicate<std::string> g{ "gender", { "Unknown" } };
    Predicate<int8_t> a{ "age", { 5 } };
    auto range = cube.span(std::make_tuple(c.values(), g.values(), a.values()));
    EXPECT_EQ(range, expected);
  }

  {
    // TODO(cao): current mapping will fast reduce pods range if high cardinality spaces in low dimension
    // So we should sort all dimensions by number of spaces in decending order
    // [0, 2], [0, 2], [2, 4] =>
    // low = 0*1 + 0*3 + 2*9 = 18
    // high = 2*1 + 2*3 + 4*9 = 44
    Span expected{ 18, 44 };
    Predicate<std::string> c{ "country", {} };
    Predicate<std::string> g{ "gender", {} };
    Predicate<int8_t> a{ "age", { 5, 9 } };
    auto range = cube.span(std::make_tuple(c.values(), g.values(), a.values()));
    EXPECT_EQ(range, expected);
  }

  // test reverse pid to dimensions
  {
    std::array<size_t, 3> expected{ 2, 1, 4 };
    EXPECT_EQ(cube.locate(41), expected);
  }
  {
    std::array<size_t, 3> expected{ 1, 0, 4 };
    EXPECT_EQ(cube.locate(37), expected);
  }
  {
    std::array<size_t, 3> expected{ 2, 1, 3 };
    EXPECT_EQ(cube.locate(32), expected);
  }
}

} // namespace test
} // namespace meta
} // namespace nebula