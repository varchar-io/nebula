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

#include "api/UDAF.h"
#include "glog/logging.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace dsl {

// a constant expression has its result type defined as T
template <typename T>
class ConstExpression;

// an expression has its result type defined as T
template <typename T>
class Expression;

// base expression
class BaseExpression {
public:
  Expression<bool> eq(const BaseExpression& expr);

  template <typename T>
  Expression<bool> eq(const Expression<T>& expr);

  Expression<bool> eq(const std::string& str);

public: // arthmetic operations
  // std::enable_if M is scalar data and T is also scalar
  template <typename M>
  auto operator*(const M& m) -> BaseExpression {
    return BaseExpression();
  }

  template <typename M>
  auto operator%(const M& m) -> BaseExpression {
    return BaseExpression();
  }

public:
  // rename current expression with an alias
  // TODO(cao) - need to auto generate column identifier for any new expression
  inline BaseExpression& as(const std::string& alias) {
    alias_ = alias;
    return *this;
  }

protected:
  // only one alias can be updated if client calls "as" multiple times
  std::string alias_;
};

// type-known expressions
template <typename T>
class Expression : public BaseExpression {
public:
  Expression() {}
  virtual ~Expression() = default;

public:
  T operator()() {
    // expression evaluate to some type of value
    return T();
  }
};

// represent a column - type is runtime resolved
class ColumnExpression : public BaseExpression {
public:
  ColumnExpression(const std::string& column) : column_{ column } {}

private:
  std::string column_;
};

// represent a constant expression, such as string literals or other values
template <typename T>
class ConstExpression : public Expression<T> {
public:
  ConstExpression(const T& value) : value_{ value } {}
  virtual ~ConstExpression() = default;

private:
  T value_;
};

// An UDAF expression
// TODO(cao) - need rework this since we need to come up with a framework
// to allow customized UDAFs to be plugged in
class UDAFExpression : public BaseExpression {
public:
  UDAFExpression(const BaseExpression& inner, nebula::api::UDAF udaf)
    : inner_{ inner }, udaf_{ udaf } {}
  virtual ~UDAFExpression() = default;

private:
  BaseExpression inner_;
  nebula::api::UDAF udaf_;
};
} // namespace dsl
} // namespace api
} // namespace nebula