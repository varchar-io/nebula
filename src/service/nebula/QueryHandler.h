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
#pragma once

#include "api/dsl/Dsl.h"
#include "api/dsl/Expressions.h"
#include "execution/ExecutionPlan.h"
#include "nebula.grpc.pb.h"
#include "service/nebula/NebulaService.h"
#include "surface/DataSurface.h"

/**
 * Define some basic sharable proerpties for nebula service
 */
namespace nebula {
namespace service {

class QueryHandler final {
public:
  std::unique_ptr<nebula::execution::ExecutionPlan> compile(const QueryRequest&, ErrorCode&) const noexcept;
  nebula::surface::RowCursor query(const nebula::execution::ExecutionPlan&, ErrorCode&) const noexcept;

private:
  // build the query object to execute
  nebula::api::dsl::Query build(const QueryRequest&) const;
  // build predicate into the query
  std::shared_ptr<nebula::api::dsl::Expression> buildPredicate(
    const Predicate&,
    const std::shared_ptr<nebula::meta::Table>,
    const std::shared_ptr<nebula::api::dsl::Expression> = nullptr,
    const nebula::api::dsl::LogicalOp = nebula::api::dsl::LogicalOp::AND) const;

  // build metric into the query
  std::shared_ptr<nebula::api::dsl::Expression> buildMetric(const Metric&) const;

  // validate the query request
  ErrorCode validate(const QueryRequest&) const noexcept;
};

} // namespace service
} // namespace nebula