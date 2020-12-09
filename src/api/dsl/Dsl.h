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
#include "common/Cursor.h"
#include "common/Zip.h"
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
__attribute__((unused)) static Query table(
  const std::string& name, const std::shared_ptr<nebula::meta::MetaService> metaservice = nullptr) {

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
static UDFExpression<nebula::surface::eval::UDFType::MAX> max(const T& expr) {
  return UDFExpression<nebula::surface::eval::UDFType::MAX>(std::shared_ptr<Expression>(new T(expr)));
}

template <typename T>
static UDFExpression<nebula::surface::eval::UDFType::MIN> min(const T& expr) {
  return UDFExpression<nebula::surface::eval::UDFType::MIN>(std::shared_ptr<Expression>(new T(expr)));
}

template <typename T>
static UDFExpression<nebula::surface::eval::UDFType::COUNT> count(const T& expr) {
  return UDFExpression<nebula::surface::eval::UDFType::COUNT>(std::shared_ptr<Expression>(new T(expr)));
}

template <>
__attribute__((unused)) UDFExpression<nebula::surface::eval::UDFType::COUNT> count<int>(const int& expr) {
  auto c = std::make_shared<ConstExpression<int>>(expr);
  c->as("count");
  return UDFExpression<nebula::surface::eval::UDFType::COUNT>(c);
}

template <typename T>
static UDFExpression<nebula::surface::eval::UDFType::SUM> sum(const T& expr) {
  return UDFExpression<nebula::surface::eval::UDFType::SUM>(std::shared_ptr<Expression>(new T(expr)));
}

template <typename T>
static UDFExpression<nebula::surface::eval::UDFType::AVG> avg(const T& expr) {
  return UDFExpression<nebula::surface::eval::UDFType::AVG>(std::shared_ptr<Expression>(new T(expr)));
}

template <typename T>
static UDFExpression<nebula::surface::eval::UDFType::PCT, double> pct(const T& expr, double&& percentile) {
  return UDFExpression<nebula::surface::eval::UDFType::PCT, double>(std::shared_ptr<Expression>(new T(expr)), std::move(percentile));
}

template <typename T>
static UDFExpression<nebula::surface::eval::UDFType::TPM, size_t> tpm(const T& expr, size_t threshold = 1) {
  return UDFExpression<nebula::surface::eval::UDFType::TPM, size_t>(std::shared_ptr<Expression>(new T(expr)), threshold);
}

template <typename T>
static UDFExpression<nebula::surface::eval::UDFType::CARD, bool> card(const T& expr, bool est = true) {
  return UDFExpression<nebula::surface::eval::UDFType::CARD, bool>(std::shared_ptr<Expression>(new T(expr)), est);
}

template <typename T>
static UDFExpression<nebula::surface::eval::UDFType::NOT> reverse(const T& expr) {
  return UDFExpression<nebula::surface::eval::UDFType::NOT>(std::shared_ptr<Expression>(new T(expr)));
}

// script expression is expressed by a script snippet
template <typename T>
static ScriptExpression<T> script(const std::string& name, const std::string& script) {
  return ScriptExpression<T>(name, script);
}

template <typename T>
static LikeExpression like(const T& expr, const std::string& pattern, bool caseSensitive = true) {
  return LikeExpression(std::shared_ptr<Expression>(new T(expr)), pattern, caseSensitive);
}

template <typename T>
static PrefixExpression starts(const T& expr, const std::string& prefix, bool caseSensitive = true) {
  return PrefixExpression(std::shared_ptr<Expression>(new T(expr)), prefix, caseSensitive);
}

template <typename T, typename U>
static InExpression<U> in(const T& expr, std::vector<U>&& values) {
  return InExpression(std::shared_ptr<Expression>(new T(expr)), std::move(values));
}

template <typename T, typename U>
static InExpression<U> nin(const T& expr, std::vector<U>&& values) {
  return InExpression(std::shared_ptr<Expression>(new T(expr)), std::move(values), false);
}

template <typename T, typename U>
static InExpression<U> in(const T& expr, nebula::common::Zip&& values) {
  return InExpression<U>(std::shared_ptr<Expression>(new T(expr)), std::move(values));
}

template <typename T, typename U>
static InExpression<U> nin(const T& expr, nebula::common::Zip&& values) {
  return InExpression<U>(std::shared_ptr<Expression>(new T(expr)), std::move(values), false);
}

template <typename T, typename U>
static BetweenExpression<U> between(const T& expr, const U&& min, const U&& max) {
  return BetweenExpression<U>(std::shared_ptr<Expression>(new T(expr)), std::move(min), std::move(max));
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