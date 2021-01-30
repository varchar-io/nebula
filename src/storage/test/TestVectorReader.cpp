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
#include <rapidjson/document.h>

#include "storage/VectorReader.h"
#include "type/Serde.h"

namespace nebula {
namespace storage {
namespace test {

using nebula::storage::JsonVectorReader;
using nebula::type::TypeSerializer;

TEST(StorageTest, TestVectorReaderBasic) {
  constexpr auto json =
    "{ \
        \"values\": [ \
            [\"id\", 1, 2, 3], \
            [\"name\", \"shawn\", \"john\", \"james\"], \
            [\"score\", 1.5, 2.6, 3.2], \
            [\"age\", 30, 10, 2] \
        ] \
    }";

  rapidjson::Document doc;
  if (doc.Parse(json).HasParseError()) {
    throw NException("invalid json content");
  }

  auto root = doc.GetObject();
  auto values = root["values"].GetArray();
  auto schema = TypeSerializer::from("ROW<id:bigint, name:varchar, score:float, age:int>");

  JsonVectorReader vectorReader(schema, values, 4);
  EXPECT_EQ(vectorReader.size(), 4);

  // go through every single row
  // ROW-1
  {
    EXPECT_TRUE(vectorReader.hasNext());
    auto& row = vectorReader.next();
    EXPECT_EQ(row.readLong("id"), 1);
    EXPECT_EQ(row.readString("name"), "shawn");
    EXPECT_NEAR(row.readFloat("score"), 1.5, 0.00001);
    EXPECT_EQ(row.readInt("age"), 30);
  }

  // ROW-2
  {
    EXPECT_TRUE(vectorReader.hasNext());
    auto& row = vectorReader.next();
    EXPECT_EQ(row.readLong("id"), 2);
    EXPECT_EQ(row.readString("name"), "john");
    EXPECT_NEAR(row.readFloat("score"), 2.6, 0.00001);
    EXPECT_EQ(row.readInt("age"), 10);
  }

  // ROW-3
  {
    EXPECT_TRUE(vectorReader.hasNext());
    auto& row = vectorReader.next();
    EXPECT_EQ(row.readLong("id"), 3);
    EXPECT_EQ(row.readString("name"), "james");
    EXPECT_NEAR(row.readFloat("score"), 3.2, 0.00001);
    EXPECT_EQ(row.readInt("age"), 2);
  }

  // no more rows
  EXPECT_FALSE(vectorReader.hasNext());
}

TEST(StorageTest, TestVectorReaderHeadless) {
  constexpr auto json =
    "{ \
        \"values\": [ \
            [1], \
            [\"shawn\"], \
            [1.5, 2.6, 3.2], \
            [30, 10, 2] \
        ] \
    }";

  rapidjson::Document doc;
  if (doc.Parse(json).HasParseError()) {
    throw NException("invalid json content");
  }

  auto root = doc.GetObject();
  auto values = root["values"].GetArray();
  auto schema = TypeSerializer::from("ROW<id:bigint, name:varchar, score:float, age:int>");

  JsonVectorReader vectorReader(schema, values, 1, true);
  EXPECT_EQ(vectorReader.size(), 1);

  // go through every single row
  // ROW-1
  {
    EXPECT_TRUE(vectorReader.hasNext());
    auto& row = vectorReader.next();
    EXPECT_EQ(row.readLong("id"), 1);
    EXPECT_EQ(row.readString("name"), "shawn");
    EXPECT_NEAR(row.readFloat("score"), 1.5, 0.00001);
    EXPECT_EQ(row.readInt("age"), 30);
  }

  // no more rows
  EXPECT_FALSE(vectorReader.hasNext());
}

TEST(StorageTest, TestVectorReaderTypeConversion) {
  constexpr auto json =
    "{ \
        \"values\": [ \
            [\"id\", 1, 2, \"3\", 4], \
            [\"name\", \"shawn\", 101, \"james\", 202], \
            [\"score\", 1.5, \"2.6\", 3.2], \
            [\"age\", \"30\", 10, 2, \"xyz\"] \
        ] \
    }";

  rapidjson::Document doc;
  if (doc.Parse(json).HasParseError()) {
    throw NException("invalid json content");
  }

  auto root = doc.GetObject();
  auto values = root["values"].GetArray();
  auto schema = TypeSerializer::from("ROW<id:bigint, name:varchar, score:float, age:int>");

  JsonVectorReader vectorReader(schema, values, 5);
  EXPECT_EQ(vectorReader.size(), 5);

  // go through every single row
  // ROW-1
  {
    EXPECT_TRUE(vectorReader.hasNext());
    auto& row = vectorReader.next();
    EXPECT_EQ(row.readLong("id"), 1);
    EXPECT_EQ(row.readString("name"), "shawn");
    EXPECT_NEAR(row.readFloat("score"), 1.5, 0.00001);
    EXPECT_EQ(row.readInt("age"), 30);
  }

  // ROW-2
  {
    EXPECT_TRUE(vectorReader.hasNext());
    auto& row = vectorReader.next();
    EXPECT_EQ(row.readLong("id"), 2);
    EXPECT_EQ(row.readString("name"), "101");
    EXPECT_NEAR(row.readFloat("score"), 2.6, 0.00001);
    EXPECT_EQ(row.readInt("age"), 10);
  }

  // ROW-3
  {
    EXPECT_TRUE(vectorReader.hasNext());
    auto& row = vectorReader.next();
    EXPECT_EQ(row.readLong("id"), 3);
    EXPECT_EQ(row.readString("name"), "james");
    EXPECT_NEAR(row.readFloat("score"), 3.2, 0.00001);
    EXPECT_EQ(row.readInt("age"), 2);
  }

  // ROW-4
  {
    EXPECT_TRUE(vectorReader.hasNext());
    auto& row = vectorReader.next();
    EXPECT_EQ(row.readLong("id"), 4);
    EXPECT_EQ(row.readString("name"), "202");
    EXPECT_NEAR(row.readFloat("score"), 0, 0.00001);
    EXPECT_EQ(row.readInt("age"), 0);
  }

  // no more rows
  EXPECT_FALSE(vectorReader.hasNext());
}

} // namespace test
} // namespace storage
} // namespace nebula