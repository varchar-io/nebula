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
#include "Expressions.h"
#include "Query.h"
#include "api/udf/MyUdf.h"
#include "common/Cursor.h"
#include "execution/ExecutionPlan.h"
#include "execution/meta/TableService.h"
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

// table to fetch a table - a unique identifier of a data set/category in nebula system
// the largest data unit to be ingested and computed.
// Nebula will enforce every single table to have time-stamp column, explicitly (user-defined) or implicitly (nebula-defined)
__attribute__((unused)) static Query table(const std::string& name, const std::shared_ptr<nebula::meta::MetaService> metaservice = nullptr) {

  auto ms = metaservice;
  if (ms == nullptr) {
    // default one
    ms = nebula::execution::meta::TableService::singleton();
  }

  // before we use copy-elision by calling constructor directly
  // however it will end up destroyed soon after the chain methods done.
  // so we use a shared ptr to ensure the object lives long enough
  return Query(name, ms);
}

// build an column expression to represnt a column
__attribute__((unused)) static ColumnExpression col(const std::string& column) {
  return ColumnExpression(column);
}

// TODO(cao) - we probably want to make this DSL api type agnostic
// a UDF/UDAF return type can be runtime determined
// by default, max works for int type
template <typename T>
static UDFExpression<nebula::execution::eval::UDFType::MAX> max(const T& expr) {
  // TODO(cao) - model UDAF/UDF with existing expression
  return UDFExpression<nebula::execution::eval::UDFType::MAX>(std::shared_ptr<Expression>(new T(expr)));
}

template <typename T>
static UDFExpression<nebula::execution::eval::UDFType::MIN> min(const T& expr) {
  // TODO(cao) - model UDAF/UDF with existing expression
  return UDFExpression<nebula::execution::eval::UDFType::MIN>(std::shared_ptr<Expression>(new T(expr)));
}

template <typename T>
static UDFExpression<nebula::execution::eval::UDFType::COUNT> count(const T& expr) {
  // TODO(cao) - we may support column expression as well for count
  return UDFExpression<nebula::execution::eval::UDFType::COUNT>(std::shared_ptr<Expression>(new T(expr)));
}

template <>
__attribute__((unused)) UDFExpression<nebula::execution::eval::UDFType::COUNT> count<int>(const int& expr) {
  // TODO(cao) - we may support column expression as well for count
  auto c = std::make_shared<ConstExpression<int>>(expr);
  c->as("count");
  return UDFExpression<nebula::execution::eval::UDFType::COUNT>(c);
}

template <typename T>
static UDFExpression<nebula::execution::eval::UDFType::SUM> sum(const T& expr) {
  // TODO(cao) - model UDAF/UDF with existing expression
  return UDFExpression<nebula::execution::eval::UDFType::SUM>(std::shared_ptr<Expression>(new T(expr)));
}

template <typename T>
static LikeExpression like(const T& expr, const std::string& pattern) {
  // TODO(cao) - model UDAF/UDF with existing expression
  return LikeExpression(std::shared_ptr<Expression>(new T(expr)), pattern);
}

template <typename T>
static PrefixExpression starts(const T& expr, const std::string& prefix) {
  // TODO(cao) - model UDAF/UDF with existing expression
  return PrefixExpression(std::shared_ptr<Expression>(new T(expr)), prefix);
}

template <typename T, typename U>
static InExpression<U> in(const T& expr, const std::vector<U>& values) {
  return InExpression(std::shared_ptr<Expression>(new T(expr)), values);
}

template <typename T, typename U>
static InExpression<U> nin(const T& expr, const std::vector<U>& values) {
  return InExpression(std::shared_ptr<Expression>(new T(expr)), values, false);
}

// TODO(cao) - we should move UDF creation out of DSL as it's logical concept
// follow example of UDAF to be consistent
template <typename T>
static UDFExpression<nebula::execution::eval::UDFType::NOT> reverse(const T& expr) {
  return UDFExpression<nebula::execution::eval::UDFType::NOT>(std::shared_ptr<Expression>(new T(expr)));
}

template <typename T, typename std::enable_if_t<!IS_T_LITERAL(T), bool> = true>
static ConstExpression<T> v(const T& value) {
  return ConstExpression<T>(value);
}

__attribute__((unused)) static ConstExpression<std::string_view> v(const std::string& value) {
  return ConstExpression<std::string_view>(value);
}

#undef NSE
} // namespace dsl
} // namespace api
} // namespace nebula