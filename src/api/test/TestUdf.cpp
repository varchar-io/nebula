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

#include "gtest/gtest.h"
#include <glog/logging.h>
#include "api/dsl/Expressions.h"
#include "api/udf/Like.h"
#include "api/udf/MyUdf.h"
#include "api/udf/Prefix.h"
#include "surface/DataSurface.h"

namespace nebula {
namespace api {
namespace test {

TEST(UDFTest, TestNot) {
  nebula::surface::MockRowData row;
  auto f = std::make_shared<nebula::api::dsl::ConstExpression<bool>>(false);
  nebula::api::udf::Not n(f);
  bool valid = true;
  EXPECT_EQ(n.eval(row, valid), true);

  auto t = std::make_shared<nebula::api::dsl::ConstExpression<bool>>(true);
  nebula::api::udf::Not y(t);
  valid = true;
  EXPECT_EQ(y.eval(row, valid), false);
}

TEST(UDFTest, TestLike) {
  std::vector<std::tuple<std::string, std::string, bool>> data{
    { "abcdefg", "abc%", true },
    { "Shawn says hi", "%says%", true },
    { "long time no see", "%see", true },
    { "nebula is cool", "%is", false },
    { "nebula is awesome", "nebula%", true },
    { "hi there ", "%i th%", true },
    { "hi there ", "i th%", false },
    { "hi there", "%there", true }
  };
  nebula::surface::MockRowData row;

  for (const auto& item : data) {
    const auto& s = std::get<0>(item);
    const auto& p = std::get<1>(item);
    auto r = std::get<2>(item);
    LOG(INFO) << "Match " << s << " with " << p << " is " << r;
    auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string>>(s);
    nebula::api::udf::Like l(c, p);
    bool valid = true;
    EXPECT_EQ(l.eval(row, valid), r);
  }
}

TEST(UDFTest, TestPrefix) {
  std::vector<std::tuple<std::string, std::string, bool>> data{
    { "abcdefg", "abc", true },
    { "Shawn says hi", "says", false },
    { "long time no see", "long time", true },
    { "nebula is cool", "is", false },
    { "nebula is awesome", "nebula", true },
    { "hi there ", "%i th", false },
    { "hi there ", "i th", false },
    { "hi there", "hi there", true }
  };
  nebula::surface::MockRowData row;

  for (const auto& item : data) {
    const auto& s = std::get<0>(item);
    const auto& p = std::get<1>(item);
    auto r = std::get<2>(item);
    LOG(INFO) << "Match " << s << " with " << p << " is " << r;
    auto c = std::make_shared<nebula::api::dsl::ConstExpression<std::string>>(s);
    nebula::api::udf::Prefix prefix(c, p);
    bool valid = true;
    EXPECT_EQ(prefix.eval(row, valid), r);
  }
}

} // namespace test
} // namespace api
} // namespace nebula