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
#include <istream>

#include "memory/FlatRow.h"
#include "storage/JsonReader.h"
#include "type/Serde.h"

namespace nebula {
namespace storage {
namespace test {

using nebula::memory::FlatRow;
using nebula::storage::JsonReader;
using nebula::storage::JsonRow;
using nebula::type::TypeSerializer;

TEST(JsonTest, DISABLED_TestJsonReader) {
  auto file = "/home/shawncao/pme_sample.txt";
  auto schema = TypeSerializer::from(
    "ROW<__time:long,app:tinyint,country:string,publish_type:int,gender:tinyint,downstream:string,contenttype:string,ct:int>");
  JsonReader r(file, schema);
  size_t count = 0;
  while (r.hasNext()) {
    const auto& row = r.next();
    count++;
    LOG(INFO) << "time: " << row.readLong("__time")
              << ", app: " << (int)row.readByte("app")
              << ", country: " << (row.isNull("country") ? "null" : row.readString("country"))
              << ", ct: " << row.readInt("ct");
  }
  LOG(INFO) << "count=" << count;
}

TEST(JsonTest, TestLocate) {
  auto json = "{\"a\": {\"b\": {\"c\": 1, \"d\": 2}}}";
  rapidjson::Document doc;
  auto& parsed = doc.Parse(json, strlen(json));
  EXPECT_FALSE(parsed.HasParseError());
  EXPECT_TRUE(doc.IsObject());

  // locate and check path a/b/c
  {
    auto node = nebula::storage::locate(doc, "/a/b/c");
    EXPECT_NE(node, nullptr);
    EXPECT_FALSE(node->IsNull());
    EXPECT_EQ(node->GetInt(), 1);
  }

  // locate check path a/b/d
  {
    auto node = nebula::storage::locate(doc, "/a/b/d");
    EXPECT_NE(node, nullptr);
    EXPECT_FALSE(node->IsNull());
    EXPECT_EQ(node->GetInt(), 2);
  }

  // locate and check path a/c/b
  {
    auto node = nebula::storage::locate(doc, "/a/c/b");
    EXPECT_EQ(node, nullptr);
  }
}

TEST(JsonTest, TestJsonToFlatRow) {
  auto json = "test/data/test_flat.json";
  auto schema = TypeSerializer::from(
    "ROW<timestamp:bigint, name:string, "
    "metrics.method:string, metrics.request_id:bigint,"
    "metadata.build_type:string, metadata.platform:string>");

  std::ifstream file(json);
  std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

  LOG(INFO) << "content=" << content;

  FlatRow flat{ 1024 };
  JsonRow row{ schema };
  auto flag = row.parse(content.data(), content.size(), flat);

  EXPECT_TRUE(flag);
  // read flat row
  LOG(INFO) << "time: " << flat.readLong("timestamp")
            << ", name: " << flat.readString("name")
            << ", metrics.method: " << flat.readString("metrics.method")
            << ", metrics.request_id: " << flat.readLong("metrics.request_id")
            << ", metadata.build_type: " << flat.readString("metadata.build_type")
            << ", metadata.platform: " << flat.readString("metadata.platform");
}

} // namespace test
} // namespace storage
} // namespace nebula