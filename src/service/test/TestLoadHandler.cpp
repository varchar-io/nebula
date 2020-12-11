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

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "service/server/LoadHandler.h"

namespace nebula {
namespace service {
namespace test {

using nebula::service::server::LoadHandler;

TEST(ServiceTest, loadConfigured) {
  const auto json = "{\"date\":[\"2020-01-01\"], \"hour\" : [\"01\", \"02\"]}";
  rapidjson::Document cd;
  if (cd.Parse(json).HasParseError()) {
    throw NException(fmt::format("Error parsing params-json: {0}", json));
  }
  LoadHandler ptr;

  common::ParamList list(cd);
  auto p = list.next();
  std::vector<size_t> wms;
  while (p.size() > 0) {
    auto watermark = ptr.extractWatermark(p);
    wms.push_back(watermark);
    p = list.next();
  }
  EXPECT_EQ(wms.size(), 2);
  // 2020-01-01 01:00:00
  EXPECT_EQ(wms.at(0), 1577840400);
  // 2020-01-01 02:00:00
  EXPECT_EQ(wms.at(1), 1577844000);
}

} // namespace test
} // namespace service
} // namespace nebula
