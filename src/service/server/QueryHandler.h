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

#pragma once

#include "api/dsl/Dsl.h"
#include "api/dsl/Expressions.h"
#include "execution/ExecutionPlan.h"
#include "execution/core/NodeConnector.h"
#include "execution/meta/TableService.h"
#include "meta/Table.h"
#include "nebula.grpc.pb.h"
#include "service/base/NebulaService.h"
#include "surface/DataSurface.h"

/**
 * Define some basic sharable proerpties for nebula service
 */
namespace nebula {
namespace service {
namespace server {

class QueryHandler final {
public:
  QueryHandler() : ms_{ nebula::execution::meta::TableService::singleton() } {}
  // build the query object to execute
  std::shared_ptr<nebula::api::dsl::Query> build(
    const nebula::meta::Table&,
    const QueryRequest&,
    nebula::service::base::ErrorCode& err) const noexcept;

  nebula::execution::PlanPtr compile(
    const std::shared_ptr<nebula::api::dsl::Query>,
    const nebula::execution::QueryWindow&,
    std::unique_ptr<nebula::execution::QueryContext>,
    nebula::service::base::ErrorCode&) const noexcept;

  nebula::surface::RowCursorPtr query(
    folly::ThreadPoolExecutor&,
    const nebula::execution::PlanPtr,
    const std::shared_ptr<nebula::execution::core::NodeConnector> connector,
    nebula::service::base::ErrorCode&) const noexcept;

  inline std::shared_ptr<nebula::meta::MetaService> meta() const noexcept {
    return ms_;
  }

private:
  //  build query internally which can throw
  std::shared_ptr<nebula::api::dsl::Query> buildQuery(
    const nebula::meta::Table& tb,
    const QueryRequest& req) const;

  // build predicate into the query
  std::shared_ptr<nebula::api::dsl::Expression> buildPredicate(
    const Predicate&,
    const nebula::meta::Table&,
    const std::shared_ptr<nebula::api::dsl::Expression> = nullptr,
    const nebula::surface::eval::LogicalOp = nebula::surface::eval::LogicalOp::AND) const;

  // build metric into the query
  std::shared_ptr<nebula::api::dsl::Expression> buildMetric(const Metric&, const nebula::meta::Table&) const;

  // validate the query request
  nebula::service::base::ErrorCode validate(const QueryRequest&) const noexcept;

private:
  std::shared_ptr<nebula::meta::MetaService> ms_;
};

} // namespace server
} // namespace service
} // namespace nebula