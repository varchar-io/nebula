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

#include <glog/logging.h>
#include <msgpack.hpp>

#include "Expressions.h"
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

struct CustomColumn {
  CustomColumn() = default;
  CustomColumn(const std::string& n, nebula::type::Kind k, const std::string& s)
    : name{ n }, kind{ k }, script{ s } {}
  std::string name;
  nebula::type::Kind kind;
  std::string script;

  MSGPACK_DEFINE(name, kind, script);

  static std::string serialize(const CustomColumn& cc) {
    std::stringstream buffer;
    msgpack::pack(buffer, cc);
    buffer.seekg(0);
    return buffer.str();
  }

  static CustomColumn deserialize(const char* data, size_t size) {
    msgpack::object_handle oh = msgpack::unpack(data, size);
    return oh.get().as<CustomColumn>();
  }
};

/**
 * Define a logic query tree to be built by client.
 */
class Query {
public:
  Query(const std::string& table, const std::shared_ptr<nebula::meta::MetaService> ms)
    : ms_{ ms }, table_{ ms_->query(table).table() }, filter_{ nullptr }, limit_{ 0 } {}

  // The copy constructor is actually a move constructor
  // We do this is to favor DSL chain method
  Query(Query& q) : ms_{ q.ms_ },
                    table_{ q.table_ },
                    customs_{ std::move(q.customs_) },
                    filter_{ std::move(q.filter_) },
                    selects_{ std::move(q.selects_) },
                    groups_{ std::move(q.groups_) },
                    sorts_{ std::move(q.sorts_) },
                    sortType_{ q.sortType_ },
                    limit_{ q.limit_ } {}
  Query(Query&&) = default;
  Query(const Query&) = delete;
  virtual ~Query() = default;

public:
  // apply for a new column with given  type and computational script in JS.
  // one option is to extend the new column into table or schema in table.
  // but we want to keep table object sharalbe and lightweigth in serde.
  // we will let new columns go with query / execution plan indicating it is query level feature.
  Query& apply(const std::string& name, nebula::type::Kind kind, const std::string& expr) {
    customs_.emplace_back(name, kind, expr);
    return *this;
  }

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
  nebula::execution::PlanPtr compile(std::unique_ptr<nebula::execution::QueryContext>);

public:
  // table identifier
  std::shared_ptr<nebula::meta::MetaService> ms_;
  std::shared_ptr<nebula::meta::Table> table_;

  // expressions of each category
  std::vector<CustomColumn> customs_;

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