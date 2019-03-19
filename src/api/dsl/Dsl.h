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

#include "Expressions.h"
#include "common/Cursor.h"
#include "execution/ExecutionPlan.h"
#include "glog/logging.h"
#include "surface/DataSurface.h"

/**
 * Define DSL methods.
 */
namespace nebula {
namespace api {
namespace dsl {

enum class SortType {
  ASC,
  DESC
};

/**
 * Define a logic query tree to be built by client
 */
class Query {
public:
  Query(const std::string& table)
    : table_{ table }, filter_{ nullptr }, limit_{ 0 } {}
  virtual ~Query() = default;

public:
  // a filter accepts a bool expression as its parameter to evaluate.
  Query& where(const Expression<bool>&);

  // select any number of expressions
  // use template parameter packs or use std::initializer_list?
  // template parameter pack example:
  // template <size_t... S>
  // decltype(auto) func(S... s) {
  //  return std::vector<size_t>{(s+1)...};
  // }
  template <typename... E>
  Query& select(const E&... select) {
    selects_ = { select... };
    return *this;
  }

  // group by a list of columns
  Query& groupby(std::vector<size_t> groups) {
    groupbys_ = groups;
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

  // execute current query to get result list
  // nebula::api::Cursor<nebula::surface::RowData&> execute();
  std::unique_ptr<nebula::execution::ExecutionPlan> compile() const;

private:
  // table identifier
  std::string table_;

  // expressions of each category
  std::unique_ptr<Expression<bool>> filter_;
  std::vector<BaseExpression> selects_;

  // select index in select list
  std::vector<size_t> groupbys_;

  // sorting information
  std::vector<size_t> sorts_;
  SortType sortType_;

  // limit the results to return
  size_t limit_;
};

// table to fetch a table - a unique identifier of a data set/category in nebula system
// the largest data unit to be ingested and computed.
// Nebula will enforce every single table to have time-stamp column, explicitly (user-defined) or implicitly (nebula-defined)
static Query table(const std::string& name) {
  return Query(name);
}

// build an column expression to represnt a column
static ColumnExpression col(const std::string& column) {
  return ColumnExpression(column);
}

// aggregation function - this should be extendable
static UDAFExpression max(const BaseExpression& expr) {
  // TODO(cao) - model UDAF/UDF with existing expression
  return UDAFExpression(expr, UDAF(UDAFRegistry::MAX));
}

} // namespace dsl
} // namespace api
} // namespace nebula