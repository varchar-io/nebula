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

#include "ExecutionPlan.h"
#include "fmt/format.h"

/**
 * Nebula execution.
 */
namespace nebula {
namespace execution {

using nebula::common::Cursor;
using nebula::meta::NNode;
using nebula::surface::RowData;

ExecutionPlan::ExecutionPlan(
  std::unique_ptr<Phase> plan,
  std::vector<NNode> nodes)
  : plan_{ std::move(plan) },
    nodes_{ std::move(nodes) } {
}

void ExecutionPlan::display() const {
  //TODO(cao) - display executuin plan details in different phases
  LOG(INFO) << "Display current executin plan";
}

Cursor<RowData&> ExecutionPlan::execute(const std::string& server) {
  throw NException("not impl");
}

} // namespace execution
} // namespace nebula