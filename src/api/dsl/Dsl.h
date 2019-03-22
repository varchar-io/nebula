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
#include "Utils.h"
#include "common/Cursor.h"
#include "execution/ExecutionPlan.h"
#include "glog/logging.h"
#include "meta/MetaService.h"
#include "meta/Table.h"
#include "surface/DataSurface.h"

/**
 * Define DSL methods.
 */
namespace nebula {
namespace api {
namespace dsl {

// A local macro to create shared pointer = nebula shared expression
#define NSE(T, ...) (std::make_shared<T>(__VA_ARGS__))

enum class SortType {
  ASC,
  DESC
};

/**
 * Define a logic query tree to be built by client
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
                    groupbys_{ std::move(q.groupbys_) },
                    sorts_{ std::move(q.sorts_) },
                    sortType_{ q.sortType_ },
                    limit_{ q.limit_ } {
    LOG(INFO) << "Query copied - original is destroyed.";
  }

  Query(const Query&) = delete;
  virtual ~Query() {
    LOG(INFO) << "Query instance destroyed";
  }

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
    selects_ = { std::shared_ptr<Expression>(new E(select))... };
    return *this;
  }

  // group by a list of columns
  Query& groupby(const std::vector<size_t>& groups) {
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

  // compile the query into an execution plan
  std::unique_ptr<nebula::execution::ExecutionPlan> compile() const;

private:
  // table identifier
  std::shared_ptr<nebula::meta::MetaService> ms_;
  std::shared_ptr<nebula::meta::Table> table_;

  // expressions of each category
  // let's use sahred_ptr to tracking all expressions in the query
  std::shared_ptr<Expression> filter_;
  std::vector<std::shared_ptr<Expression>> selects_;

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
static Query table(const std::string& name, const std::shared_ptr<nebula::meta::MetaService> metaservice = nullptr) {
  auto ms = metaservice;
  if (ms == nullptr) {
    // default one
    ms = std::make_shared<nebula::meta::MetaService>();
  }

  // before we use copy-elision by calling constructor directly
  // however it will end up destroyed soon after the chain methods done.
  // so we use a shared ptr to ensure the object lives long enough
  return Query(name, ms);
}

// build an column expression to represnt a column
static ColumnExpression col(const std::string& column) {
  return ColumnExpression(column);
}

// aggregation function - this should be extendable
template <typename T>
static UDAFExpression<T> max(const T& expr) {
  // TODO(cao) - model UDAF/UDF with existing expression
  return UDAFExpression(expr, UDAF(UDAFRegistry::MAX));
}

template <typename T, typename std::enable_if_t<!IS_T_LITERAL(T), bool> = true>
static ConstExpression<T> v(const T& value) {
  return ConstExpression<T>(value);
}

static ConstExpression<std::string> v(const std::string& value) {
  return ConstExpression<std::string>(value);
}

#undef NSE
} // namespace dsl
} // namespace api
} // namespace nebula