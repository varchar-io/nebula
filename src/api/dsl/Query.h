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

#include <glog/logging.h>

#include "Context.h"
#include "Expressions.h"
#include "api/udf/Not.h"
#include "common/Cursor.h"
#include "execution/ExecutionPlan.h"
#include "meta/MetaService.h"
#include "meta/Table.h"
#include "surface/DataSurface.h"

/**
 * Define query strucuture.
 */
namespace nebula {
namespace api {
namespace dsl {

enum class SortType {
  ASC,
  DESC
};

/**
 * Define a logic query tree to be built by client.
 */
class Query {
public:
  Query(const std::string& table, const std::shared_ptr<nebula::meta::MetaService> ms)
    : ms_{ ms }, table_{ ms_->query(table) }, filter_{ nullptr }, limit_{ 0 } {}

  // The copy constructor is actually a move constructor
  // We do this is to favor DSL chain method
  Query(Query& q) : ms_{ q.ms_ },
                    table_{ q.table_ },
                    filter_{ std::move(q.filter_) },
                    selects_{ std::move(q.selects_) },
                    groups_{ std::move(q.groups_) },
                    sorts_{ std::move(q.sorts_) },
                    sortType_{ q.sortType_ },
                    limit_{ q.limit_ } {}

  Query(const Query&) = delete;
  virtual ~Query() = default;

public:
  // a filter accepts a bool expression as its parameter to evaluate.
  template <typename T>
  Query& where(const T& filter) {
    // apply filter and return myself
    filter_ = std::shared_ptr<Expression>(new T(filter));
    return *this;
  }

  // select any number of expressions
  template <typename... E>
  Query& select(const E&... select) {
    // make a copy in a shared pointer and save it.
    selects_ = preprocess(table_->schema(), { std::shared_ptr<Expression>(new E(select))... });
    return *this;
  }

  Query& select(const std::vector<std::shared_ptr<Expression>>& selects) {
    // make a copy in a shared pointer and save it.
    selects_ = preprocess(table_->schema(), selects);
    return *this;
  }

  // group by a list of columns
  Query& groupby(const std::vector<size_t>& groups) {
    groups_ = groups;
    return *this;
  }

  Query& sortby(std::vector<size_t> sorts, SortType type = SortType::ASC) {
    sorts_ = sorts;
    sortType_ = type;
    return *this;
  }

  Query& limit(size_t l) {
    limit_ = l;
    return *this;
  }

public:
  // compile the query into an execution plan
  std::unique_ptr<nebula::execution::ExecutionPlan> compile(const QueryContext&) const;

public:
  // table identifier
  std::shared_ptr<nebula::meta::MetaService> ms_;
  std::shared_ptr<nebula::meta::Table> table_;

  // expressions of each category
  // let's use sahred_ptr to tracking all expressions in the query
  std::shared_ptr<Expression> filter_;
  std::vector<std::shared_ptr<Expression>> selects_;

  // select index in select list
  std::vector<size_t> groups_;

  // sorting information
  std::vector<size_t> sorts_;
  SortType sortType_;

  // limit the results to return
  size_t limit_;

private:
  static std::vector<std::shared_ptr<Expression>> preprocess(
    const nebula::type::Schema&, const std::vector<std::shared_ptr<Expression>>&);
};

} // namespace dsl
} // namespace api
} // namespace nebula