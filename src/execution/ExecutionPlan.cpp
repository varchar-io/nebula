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
#include <fmt/format.h>
#include <glog/logging.h>
#include "common/Likely.h"
#include "type/Serde.h"

/**
 * Nebula execution.
 */
namespace nebula {
namespace execution {

using nebula::common::Cursor;
using nebula::meta::NNode;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeSerializer;

ExecutionPlan::ExecutionPlan(
  std::unique_ptr<ExecutionPhase> plan,
  std::vector<NNode> nodes,
  Schema output)
  : uuid_{ "<uuid>" },
    plan_{ std::move(plan) },
    nodes_{ std::move(nodes) },
    output_{ output } {}

void ExecutionPlan::display() const {
  LOG(INFO) << "Query will be executed in nodes: " << nodes_.size();

  // display plan phases
  plan_->display();
}

const ExecutionPhase& ExecutionPlan::fetch(PhaseType type) const {
  // start from plan_ and along up stream to match phase type
  const auto& l1 = *plan_;
  if (l1.type() == type) {
    return l1;
  }

  const auto& l2 = l1.upstream();
  if (l2.type() == type) {
    return l2;
  }

  const auto& l3 = l2.upstream();
  if (l3.type() == type) {
    return l3;
  }

  throw NException("Phase not found in plan.");
}

template <>
const BlockPhase& ExecutionPlan::fetch<PhaseType::COMPUTE>() const {
  return dynamic_cast<const BlockPhase&>(fetch(PhaseType::COMPUTE));
}

template <>
const NodePhase& ExecutionPlan::fetch<PhaseType::PARTIAL>() const {
  return dynamic_cast<const NodePhase&>(fetch(PhaseType::PARTIAL));
}

template <>
const FinalPhase& ExecutionPlan::fetch<PhaseType::GLOBAL>() const {
  return dynamic_cast<const FinalPhase&>(fetch(PhaseType::GLOBAL));
}

void Phase<PhaseType::COMPUTE>::display() const {
  // display current phase type
  LOG(INFO) << "PHASE: " << PhaseTraits<PhaseType::COMPUTE>::name;
  LOG(INFO) << indent4 << "INPUT: " << TypeSerializer::to(input_);
  LOG(INFO) << indent4 << "OUTPUT: " << TypeSerializer::to(outputSchema());
  LOG(INFO) << indent4 << "SCAN: " << table_;
  LOG(INFO) << indent4 << "FILTER: " << bliteral(filter_ != nullptr);
  const auto hasAgg = (keys_.size() < fields_.size());
  LOG(INFO) << indent4 << "GROUP: " << bliteral(hasAgg);
  if (LIKELY(hasAgg)) {
    LOG(INFO) << indent4 << indent4 << "KEYS: " << join(keys_);
  }
}

void Phase<PhaseType::PARTIAL>::display() const {
  upstream_->display();

  LOG(INFO) << "PHASE: " << PhaseTraits<PhaseType::PARTIAL>::name;
  LOG(INFO) << indent4 << "INPUT: " << TypeSerializer::to(input_);
  LOG(INFO) << indent4 << "OUTPUT: " << TypeSerializer::to(outputSchema());
  LOG(INFO) << indent4 << "AGG: partial";
}

void Phase<PhaseType::GLOBAL>::display() const {
  upstream_->display();

  LOG(INFO) << "PHASE: " << PhaseTraits<PhaseType::GLOBAL>::name;
  LOG(INFO) << indent4 << "INPUT: " << TypeSerializer::to(input_);
  LOG(INFO) << indent4 << "OUTPUT: " << TypeSerializer::to(outputSchema());
  LOG(INFO) << indent4 << "AGG: global";
  LOG(INFO) << indent4 << "Schema Convert: " << diffInputOutput();
  const auto hasSort = sorts_.size() > 0;
  LOG(INFO) << indent4 << "SORT : " << bliteral(hasSort);
  if (LIKELY(hasSort)) {
    LOG(INFO) << indent4 << indent4 << "KEYS: " << join(sorts_);
  }
}

} // namespace execution
} // namespace nebula