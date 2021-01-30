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
#include <yorel/yomm2/cute.hpp>

#include "execution/ExecutionPlan.h"
#include "execution/core/BlockExecutor.h"
#include "execution/serde/RowCursorSerde.h"
#include "memory/Batch.h"
#include "meta/TestTable.h"
#include "surface/MockSurface.h"
#include "surface/eval/UDF.h"
#include "surface/eval/ValueEval.h"

/// this test focus on optimized execution by data metadata, mostly histogram
namespace nebula {
namespace execution {
namespace test {

using nebula::execution::core::BlockExecutor;
using nebula::memory::Batch;
using nebula::surface::MockRowData;
using nebula::surface::RowData;
using nebula::surface::eval::column;
using nebula::surface::eval::constant;
using nebula::surface::eval::UDAF;
using nebula::type::TypeSerializer;

TEST(OptimizedQuery, TestPredicatePushdown) {
  nebula::meta::TestTable test;
  auto size = 10;
  nebula::memory::Batch batch(test, size);
  nebula::surface::MockRowData row;
  for (auto i = 0; i < size; ++i) {
    batch.add(row);
  }

  LOG(INFO) << "build up a block compute result";
  auto outputSchema = TypeSerializer::from("ROW<id:int, event:string, flag:bool>");
  nebula::execution::BlockPhase plan(test.schema(), outputSchema);

  nebula::surface::eval::Fields selects;
  selects.reserve(3);
  selects.push_back(column<int32_t>("id"));
  selects.push_back(column<std::string_view>("event"));
  selects.push_back(column<bool>("flag"));
  plan.scan(test.name())
    .compute(std::move(selects))
    .filter(constant<bool>(true))
    .aggregate(0, { false, false, false })
    .limit(size);

  auto cursor = nebula::execution::core::compute(batch, plan);
}
} // namespace test
} // namespace execution
} // namespace nebula