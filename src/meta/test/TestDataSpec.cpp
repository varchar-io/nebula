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

#include "meta/DataSpec.h"
#include "meta/TestUtils.h"

namespace nebula {
namespace meta {
namespace test {

using nebula::common::MapKV;
using nebula::meta::DataFormat;
using nebula::meta::DataSpec;
using nebula::meta::SpecSplit;
using nebula::meta::SpecSplitPtr;
using nebula::meta::SpecState;

// build a list of spec splits
std::vector<SpecSplitPtr> genSplits() {
  std::vector<SpecSplitPtr> splits;
  splits.reserve(3);
  splits.push_back(std::make_shared<SpecSplit>("s3://test/a.csv", 10, 1));
  splits.push_back(std::make_shared<SpecSplit>("s3://test/b.csv", 12, 3, MapKV{ { "x", "y" } }));
  splits.push_back(std::make_shared<SpecSplit>("s3://test/c.csv", 14, 2));
  return splits;
}

TEST(SpecTest, TestDataSpec) {
  auto tableSpec = nebula::meta::genTableSpec();
  auto splits = genSplits();
  DataSpec spec{ tableSpec, "1.0", "s3://test", splits, SpecState::NEW };
  constexpr auto EXPECTED_ID = "test.1.0@[s3://test/a.csv#0#10#1,s3://test/b.csv#0#12#3,s3://test/c.csv#0#14#2,]";
  EXPECT_EQ(spec.id(), EXPECTED_ID);
  EXPECT_EQ(spec.version(), "1.0");
  EXPECT_EQ(spec.size(), 36);
  EXPECT_EQ(spec.domain(), "s3://test");

  // test serde using msgpack
  auto str = DataSpec::serialize(spec);

  // validate values after deserialized
  {
    auto ptr = DataSpec::deserialize(str);
    // test table is serialized
    EXPECT_EQ(ptr->table()->name, "test");
    EXPECT_EQ(ptr->table()->format, DataFormat::CSV);

    // test basic info
    EXPECT_EQ(ptr->version(), "1.0");
    EXPECT_EQ(ptr->size(), 36);
    EXPECT_EQ(ptr->domain(), "s3://test");
    EXPECT_EQ(ptr->id(), EXPECTED_ID);

    // test splits
    EXPECT_EQ(ptr->splits().size(), 3);
    const auto secondSplit = ptr->splits().at(1);
    EXPECT_EQ(secondSplit->path, "s3://test/b.csv");
    EXPECT_EQ(secondSplit->offset, 0);
    EXPECT_EQ(secondSplit->size, 12);
    EXPECT_EQ(secondSplit->watermark, 3);
    const auto EXPECTED_MACROS = MapKV{ { "x", "y" } };
    EXPECT_EQ(secondSplit->macros, EXPECTED_MACROS);
  }
}

} // namespace test
} // namespace meta
} // namespace nebula