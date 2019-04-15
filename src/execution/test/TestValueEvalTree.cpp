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
#include "common/Evidence.h"
#include "execution/eval/ValueEval.h"
#include "fmt/format.h"
#include <glog/logging.h>
#include "surface/DataSurface.h"

namespace nebula {
namespace execution {
namespace test {

using nebula::common::Evidence;
using nebula::execution::eval::column;
using nebula::execution::eval::constant;
using nebula::execution::eval::TypeValueEval;
using nebula::execution::eval::ValueEval;
using nebula::surface::MockRowData;
using nebula::surface::RowData;

TEST(ValueEvalTest, TestValueEval) {
  MockRowData row(1);

  // int = 3 + col('a')
  auto b1 = nebula::execution::eval::constant(3);
  auto v1 = b1->eval<int>(row);
  LOG(INFO) << "b1 eval: " << v1;
  EXPECT_EQ(v1, 3);

  auto b2 = nebula::execution::eval::column<int>("a");

  MockRowData mirror(1);
  auto v2 = b2->eval<int>(row);
  LOG(INFO) << "b2 eval: " << v2;
  EXPECT_EQ(v2, mirror.readInt("a"));

  auto b3 = nebula::execution::eval::add<int, int, int>(std::move(b1), std::move(b2));
  auto sum = b3->eval<int>(row);
  auto expected_sum = v1 + mirror.readInt("a");
  LOG(INFO) << "b3 eval: " << sum;
  EXPECT_EQ(sum, expected_sum);

  auto b4 = nebula::execution::eval::constant(10);
  auto b5 = nebula::execution::eval::lt<int, int>(std::move(b3), std::move(b4));
  EXPECT_FALSE(b5->eval<bool>(row));
}

TEST(ValueEvalTest, TestValueEvalArthmetic) {
  auto seed = Evidence::ticks();
  // add two values
  MockRowData row(seed);
  MockRowData mirror(seed);

  // add two values
  {
    const int cvalue = 128;
    auto b1 = nebula::execution::eval::constant(cvalue);
    auto b2 = nebula::execution::eval::column<float>("x");
    auto b3 = nebula::execution::eval::add<float, int, float>(std::move(b1), std::move(b2));
    float add = b3->eval<float>(row);
    EXPECT_EQ(add, mirror.readFloat("x") + cvalue);
  }

  {
    const int cvalue = 10;
    auto b1 = nebula::execution::eval::constant(cvalue);
    auto b2 = nebula::execution::eval::column<float>("y");
    auto b3 = nebula::execution::eval::mul<float, int, float>(std::move(b1), std::move(b2));
    float mul = b3->eval<float>(row);
    EXPECT_EQ(mul, mirror.readFloat("y") * cvalue);
  }

  {
    const int cvalue = 2;
    auto b1 = nebula::execution::eval::constant(cvalue);
    auto b2 = nebula::execution::eval::column<float>("z");
    auto b3 = nebula::execution::eval::sub<float, int, float>(std::move(b1), std::move(b2));
    float sub = b3->eval<float>(row);
    EXPECT_EQ(sub, cvalue - mirror.readFloat("z"));
  }

  {
    const int cvalue = 2;
    auto b1 = nebula::execution::eval::constant(cvalue);
    auto b2 = nebula::execution::eval::column<float>("d");
    auto b3 = nebula::execution::eval::div<float, float, int>(std::move(b2), std::move(b1));
    float div = b3->eval<float>(row);
    EXPECT_EQ(div, mirror.readFloat("d") / cvalue);
  }
}

TEST(ValueEvalTest, TestValueEvalLogical) {
  auto seed = Evidence::ticks();
  // add two values
  MockRowData row(seed);
  MockRowData mirror(seed);

  // add two values
  {
    const int cvalue = 32;
    auto b1 = nebula::execution::eval::constant(cvalue);
    auto b2 = nebula::execution::eval::column<float>("x");
    auto b3 = nebula::execution::eval::mul<float, int, float>(std::move(b1), std::move(b2));
    auto b4 = nebula::execution::eval::constant(64);
    auto b5 = nebula::execution::eval::gt<float, int>(std::move(b3), std::move(b4));
    EXPECT_EQ(b5->eval<bool>(row), false);

    auto b6 = nebula::execution::eval::constant(true);
    auto b7 = nebula::execution::eval::bor<bool, bool>(std::move(b5), std::move(b6));
    auto b8 = nebula::execution::eval::constant(true);
    auto b9 = nebula::execution::eval::eq<bool, bool>(std::move(b7), std::move(b8));
  }
}

} // namespace test
} // namespace execution
} // namespace nebula