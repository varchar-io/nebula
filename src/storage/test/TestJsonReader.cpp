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

#include "storage/JsonReader.h"
#include "type/Serde.h"

namespace nebula {
namespace storage {
namespace test {

using nebula::storage::JsonReader;
TEST(JsonTest, DISABLED_TestJsonReader) {
  auto file = "/home/shawncao/pme_sample.txt";
  auto schema = nebula::type::TypeSerializer::from(
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

} // namespace test
} // namespace storage
} // namespace nebula