/*
 * Copyright 2017-present
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

#include <fstream>
#include <gtest/gtest.h>
#include <istream>
#include <rapidjson/document.h>

#include "service/base/LoadSpec.h"

namespace nebula {
namespace service {
namespace test {

TEST(LoadSpecTest, TestLoadSpecDeserialization) {
  auto json = "test/data/load_spec.json";
  std::ifstream file(json);
  std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  rapidjson::Document doc;
  auto& r = doc.Parse(content.data(), content.length());

  EXPECT_FALSE(r.HasParseError());
  nebula::service::base::LoadSpec spec(r);
  EXPECT_EQ(spec.format, "JSON");
  EXPECT_EQ(spec.json.rowsField, "results");
  auto& cm = spec.json.columnsMap;
  EXPECT_EQ(cm.size(), 8);
  for (auto& itr : cm) {
    EXPECT_EQ(itr.first, itr.second);
  }

  // verify header is loaded
  EXPECT_EQ(spec.headers.size(), 1);
  EXPECT_EQ(spec.headers.at(0), "header: abc");
}

} // namespace test
} // namespace service
} // namespace nebula
