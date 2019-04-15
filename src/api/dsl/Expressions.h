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

#include "Base.h"
#include "common/Errors.h"
#include "common/Likely.h"
#include "execution/eval/UDF.h"
#include "execution/eval/ValueEval.h"
#include "glog/logging.h"
#include "meta/Table.h"
#include "type/Tree.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace dsl {

// Note: (problems in designing the interfaces here)
// 1. we're using operator override to enable expressions with supported operations
// such as: (col("a") == 3), but operator override won't work on pointer types, so we can't use
// shared pointer to pass around these expressions
// 2. we're using dynamic virtual functions to determine runtime types of constructed expressions
// this is anti requirements of #1 since passing type around won't resolve virtual functions
// So we probably have a few options
// a. create shared_pointer but always use referecne type in interfaces.
// b. we can't use base expression everywhere, instead, we have to be generic in every single place
// where it embeds the expressions inside.

// to embrace dynamic and virtual function dispatch
// all API and data exchange part should use shared pointer
// we're not using unique ptr because it's dealing with schema
class Expression;

// A macro to limit the type to be base expression
#define IS_EXPRESSION(T) typename std::enable_if_t<std::is_base_of_v<Expression, T>, bool> = true
// used in definition signature to match the declaration without default value
#define IS_EXPRESSION_D(T) typename std::enable_if_t<std::is_base_of_v<Expression, T>, bool>

// a constant expression has its result type defined as T
template <typename T>
class ConstExpression;

// arthmetic expression
template <ArthmeticOp op, typename T1, typename T2>
class ArthmeticExpression;

// logical expression
template <LogicalOp op, typename T1, typename T2>
class LogicalExpression;

// base expression
#define ARTHMETIC_OP_CONST(OP, TYPE)                                                                                          \
  template <typename M, bool OK = std::is_arithmetic<M>::value>                                                               \
  auto operator OP(const M& m)->std::enable_if_t<OK, ArthmeticExpression<ArthmeticOp::TYPE, THIS_TYPE, ConstExpression<M>>> { \
    return ArthmeticExpression<ArthmeticOp::TYPE, THIS_TYPE, ConstExpression<M>>(*this, ConstExpression<M>(m));               \
  }

#define ARTHMETIC_OP_GENERIC(OP, TYPE)                                                     \
  template <typename R, IS_EXPRESSION(R)>                                                  \
  auto operator OP(const R& right)->ArthmeticExpression<ArthmeticOp::TYPE, THIS_TYPE, R> { \
    return ArthmeticExpression<ArthmeticOp::TYPE, THIS_TYPE, R>(*this, right);             \
  }

#define LOGICAL_OP_CONST(OP, TYPE)                                                                                        \
  template <typename M, bool OK = std::is_arithmetic<M>::value && !(std::is_same<char*, std::decay_t<M>>::value)>         \
  auto operator OP(const M& m)->std::enable_if_t<OK, LogicalExpression<LogicalOp::TYPE, THIS_TYPE, ConstExpression<M>>> { \
    return LogicalExpression<LogicalOp::TYPE, THIS_TYPE, ConstExpression<M>>(*this, ConstExpression<M>(m));               \
  }

#define LOGICAL_OP_GENERIC(OP, TYPE)                                                   \
  template <typename R, IS_EXPRESSION(R)>                                              \
  auto operator OP(const R& right)->LogicalExpression<LogicalOp::TYPE, THIS_TYPE, R> { \
    return LogicalExpression<LogicalOp::TYPE, THIS_TYPE, R>(*this, right);             \
  }

#define LOGICAL_OP_STRING(OP, TYPE) \
  auto operator OP(const std::string&)->LogicalExpression<LogicalOp::TYPE, THIS_TYPE, ConstExpression<std::string>>;

#define ALL_ARTHMETIC_OPS()    \
  ARTHMETIC_OP_CONST(+, ADD)   \
  ARTHMETIC_OP_GENERIC(+, ADD) \
  ARTHMETIC_OP_CONST(-, SUB)   \
  ARTHMETIC_OP_GENERIC(-, SUB) \
  ARTHMETIC_OP_CONST(*, MUL)   \
  ARTHMETIC_OP_GENERIC(*, MUL) \
  ARTHMETIC_OP_CONST(/, DIV)   \
  ARTHMETIC_OP_GENERIC(/, DIV) \
  ARTHMETIC_OP_CONST(%, MOD)   \
  ARTHMETIC_OP_GENERIC(%, MOD)

#define ALL_LOGICAL_OPS()     \
  LOGICAL_OP_CONST(==, EQ)    \
  LOGICAL_OP_STRING(==, EQ)   \
  LOGICAL_OP_GENERIC(==, EQ)  \
  LOGICAL_OP_CONST(>, GT)     \
  LOGICAL_OP_STRING(>, GT)    \
  LOGICAL_OP_GENERIC(>, GT)   \
  LOGICAL_OP_CONST(>=, GE)    \
  LOGICAL_OP_STRING(>=, GE)   \
  LOGICAL_OP_GENERIC(>=, GE)  \
  LOGICAL_OP_CONST(<, LT)     \
  LOGICAL_OP_STRING(<, LT)    \
  LOGICAL_OP_GENERIC(<, LT)   \
  LOGICAL_OP_CONST(<=, LE)    \
  LOGICAL_OP_STRING(<=, LE)   \
  LOGICAL_OP_GENERIC(<=, LE)  \
                              \
  LOGICAL_OP_GENERIC(&&, AND) \
  LOGICAL_OP_GENERIC(||, OR)

#define ALL_ARTHMETIC_LOGICAL_OPS() \
  ALL_ARTHMETIC_OPS()               \
  ALL_LOGICAL_OPS()

// every expression can be renamed as alias
#define ALIAS()                                 \
  decltype(auto) as(const std::string& alias) { \
    alias_ = alias;                             \
    return *this;                               \
  }

#define IS_AGG(T_F)                     \
  virtual bool isAgg() const override { \
    return T_F;                         \
  }

// NOTE:
// TODO(cao) - we put all these operator definitions in each concrete types
// IS only because the THIS_TYPE resolusion
// Right now, std::remove_reference<decltype(*this)>::type doesn't resolve runtime type but the current type
// where the method is called - if somehow we can figure out THIS_TYPE for runtime, we can save lots of duplicate code
// meaning we only need these operator defined in base expression.
class Expression {
public:
  Expression() : alias_{}, kind_{ nebula::type::Kind::INVALID } {}
  virtual ~Expression() = default;

public:
  // TODO(cao): need to rework to introduce context and visitor pattern
  // to deduce the type of this expression
  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) = 0;
  virtual bool isAgg() const = 0;
  virtual std::unique_ptr<nebula::execution::eval::ValueEval> asEval() const = 0;
  virtual std::vector<std::string> columnRefs() const {
    return {};
  };

public:
  std::string alias() const {
    return alias_;
  }

  nebula::type::Kind kind() const {
    return kind_;
  }

protected:
  static nebula::type::TreeNode typeCreate(nebula::type::Kind kind, std::string& alias) {
#define TYPE_CREATE_NODE(KIND, TYPE)              \
  case nebula::type::KIND: {                      \
    return nebula::type::TYPE::createTree(alias); \
  }

    // no result should have the final type KIND
    switch (kind) {
      TYPE_CREATE_NODE(TINYINT, ByteType)
      TYPE_CREATE_NODE(SMALLINT, ShortType)
      TYPE_CREATE_NODE(INTEGER, IntType)
      TYPE_CREATE_NODE(BIGINT, LongType)
      TYPE_CREATE_NODE(REAL, FloatType)
      TYPE_CREATE_NODE(DOUBLE, DoubleType)
    default:
      throw NException(fmt::format("not supported type {0} in arthmetic operations",
                                   nebula::type::TypeBase::kname(kind)));
    }
#undef TYPE_CREATE_NODE
  }

  // only one alias can be updated if client calls "as" multiple times
  std::string alias_;

  // set by type() method
  nebula::type::Kind kind_;
};

// arthmetic op traits will provide result KIND given left and right kind on given OP
template <ArthmeticOp OP, nebula::type::Kind LEFT, nebula::type::Kind RIGHT>
struct ArthmeticOpTraits {};

template <ArthmeticOp op, typename T1, typename T2>
class ArthmeticExpression : public Expression {
public:
  ArthmeticExpression(const T1& op1, const T2& op2) : op1_{ op1 }, op2_{ op2 } {
    // failed type check
    if constexpr (!std::is_base_of_v<Expression, T1> || !std::is_base_of_v<Expression, T2>) {
      throw NException("All arthmetic oprands need to be expression");
    }

    // get alias from the first valid op
    alias_ = op1_.alias().size() > 0 ? op1_.alias() : op2_.alias();
  }
  virtual ~ArthmeticExpression() = default;

public: // all operations
  ALL_ARTHMETIC_LOGICAL_OPS()
  // ALL_ARTHMETIC_OPS()

  ALIAS()

  IS_AGG(false)

  virtual std::unique_ptr<nebula::execution::eval::ValueEval> asEval() const override {
    auto v1 = op1_.asEval();
    auto v2 = op2_.asEval();
    N_ENSURE(v1 != nullptr, "op1 value eval is null");
    N_ENSURE(v2 != nullptr, "op2 value eval is null");

    // forward to the correct version of value eval creation
    return arthmetic_forward()(op, op1_.kind(), op2_.kind(), std::move(v1), std::move(v2));
  }

  virtual std::vector<std::string> columnRefs() const override {
    auto v1 = op1_.columnRefs();
    auto v2 = op2_.columnRefs();
    std::vector<std::string> merge;
    merge.reserve(v1.size() + v2.size());

    if (v1.size() > 0) {
      merge.insert(merge.end(), v1.begin(), v1.end());
    }

    if (v2.size() > 0) {
      merge.insert(merge.end(), v2.begin(), v2.end());
    }

    return merge;
  }

  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) override {
    op1_.type(table);
    op2_.type(table);

    kind_ = nebula::api::dsl::ArthmeticCombination::result(op1_.kind(), op2_.kind());
    return typeCreate(kind_, alias_);
  }

private:
  T1 op1_;
  T2 op2_;
};

// logical expression definition
template <LogicalOp op, typename T1, typename T2>
class LogicalExpression : public Expression {
public:
  LogicalExpression(const T1& op1, const T2& op2) : op1_{ op1 }, op2_{ op2 } {
    // failed type check
    if constexpr (!std::is_base_of_v<Expression, T1> || !std::is_base_of_v<Expression, T2>) {
      throw NException("All logical oprands need to be expression");
    }

    // get alias from the first valid op
    alias_ = op1_.alias().size() > 0 ? op1_.alias() : op2_.alias();
  }
  virtual ~LogicalExpression() = default;

public: // all logical operations
  ALL_LOGICAL_OPS()

  ALIAS()

  IS_AGG(false)

  // convert to value eval
  virtual std::unique_ptr<nebula::execution::eval::ValueEval> asEval() const override {
    auto v1 = op1_.asEval();
    auto v2 = op2_.asEval();
    N_ENSURE(v1 != nullptr, "op1 value eval is null");
    N_ENSURE(v2 != nullptr, "op2 value eval is null");

    // forward to the correct version of value eval creation
    return logical_forward()(op, op1_.kind(), op2_.kind(), std::move(v1), std::move(v2));
  }

  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) override {
    op1_.type(table);
    op2_.type(table);

    // validate non-comparison case.
    // unlike programming language, we don't allow implicity type conversion
    // so for AND and OR operations, we have to validate left and right are both bool type
    validate(table);

    // logical expression will always in bool type
    kind_ = nebula::type::Kind::BOOLEAN;
    return nebula::type::BoolType::createTree(alias_);
  }

#define BOTH_OPRANDS_BOOL()                                                    \
  N_ENSURE_EQ(op1_.kind(), nebula::type::Kind::BOOLEAN,                        \
              "AND/OR operations requires bool typed left and right oprands"); \
  N_ENSURE_EQ(op2_.kind(), nebula::type::Kind::BOOLEAN,                        \
              "AND/OR operations requires bool typed left and right oprands");

  void validate(const nebula::meta::Table& table) {
    if constexpr (op == LogicalOp::AND || op == LogicalOp::OR) {
      BOTH_OPRANDS_BOOL()
    }
  }

#undef BOTH_OPRANDS_BOOL

  virtual std::vector<std::string> columnRefs() const override {
    auto v1 = op1_.columnRefs();
    auto v2 = op2_.columnRefs();
    std::vector<std::string> merge;
    merge.reserve(v1.size() + v2.size());

    if (v1.size() > 0) {
      merge.insert(merge.end(), v1.begin(), v1.end());
    }

    if (v2.size() > 0) {
      merge.insert(merge.end(), v2.begin(), v2.end());
    }

    return merge;
  }

private:
  T1 op1_;
  T2 op2_;
};

// represent a column - type is runtime resolved
class ColumnExpression : public Expression {
public:
  ColumnExpression(const std::string& column)
    : column_{ column } {
    alias_ = column_;
  }
  virtual ~ColumnExpression() = default;

public: // all logical operations
  ALL_ARTHMETIC_LOGICAL_OPS()

  ALIAS()

  IS_AGG(false)

  virtual std::unique_ptr<nebula::execution::eval::ValueEval> asEval() const override;
  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) override;
  inline virtual std::vector<std::string> columnRefs() const override {
    return std::vector<std::string>{ column_ };
  }

private:
  std::string column_;
};

// represent a constant expression, such as string literals or other values
template <typename T>
class ConstExpression : public Expression {
public:
  ConstExpression(const T& value) : value_{ value } {}
  virtual ~ConstExpression() = default;

public:
  ALIAS()

  IS_AGG(false)

  // convert to value eval
  virtual std::unique_ptr<nebula::execution::eval::ValueEval> asEval() const override {
    return nebula::execution::eval::constant<T>(value_);
  }

  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) override {
    // duduce from T to nebula::type::Type
    auto node = nebula::type::TypeDetect<T>::type(alias_);
    kind_ = nebula::type::TypeBase::k(node);
    return node;
  }

private:
  T value_;
};

template <nebula::type::Kind KIND>
class UDFExpression : public Expression {
public:
  UDFExpression(std::shared_ptr<nebula::execution::eval::UDF<KIND>> udf) : udf_{ udf } {}
  virtual ~UDFExpression() = default;

public:
  ALIAS()

  IS_AGG(false)

  // convert to value eval
  virtual std::unique_ptr<nebula::execution::eval::ValueEval> asEval() const override {
    // TODO(cao): NEED to consolidate with UDAF
    return nullptr;
  }

  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) override {
    kind_ = KIND;
    return nebula::type::Type<KIND>::createTree(alias_);
  }

  virtual std::vector<std::string> columnRefs() const override {
    return udf_->columns();
  }

protected:
  std::shared_ptr<nebula::execution::eval::UDF<KIND>> udf_;
};

// An UDAF expression - some UDAF knows its type regardless inner type
// such as percentile (P50, P90), the majority UDAF infers type from its inner expression.
// So here - we will implement this first. When necessary, we may want to introduce different expresssion type.
// TODO(cao) - need rework this since we need to come up with a framework
// to allow customized UDAFs to be plugged in
class UDAFExpression : public Expression {
public:
  UDAFExpression(nebula::execution::eval::UDAF_REG udaf, std::shared_ptr<Expression> inner)
    : udaf_{ udaf }, inner_{ inner } {
  }
  virtual ~UDAFExpression() = default;

public:
  ALIAS()

  IS_AGG(true)

  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) override {
    auto innerType = inner_->type(table);

    // inner type is
    kind_ = inner_->kind();
    return typeCreate(kind_, alias_);
  }

  inline virtual std::vector<std::string> columnRefs() const override {
    return inner_->columnRefs();
  }

  // convert to value eval
  virtual std::unique_ptr<nebula::execution::eval::ValueEval> asEval() const override;

private:
  nebula::execution::eval::UDAF_REG udaf_;
  std::shared_ptr<Expression> inner_;
};

#undef ARTHMETIC_OP_CONST
#undef ARTHMETIC_OP_GENERIC
#undef LOGICAL_OP_CONST
#undef LOGICAL_OP_STRING
#undef LOGICAL_OP_GENERIC
#undef ALL_ARTHMETIC_LOGICAL_OPS
#undef IS_EXPRESSION
#undef IS_EXPRESSION_D
#undef ALIAS
#undef IS_AGG

} // namespace dsl
} // namespace api
} // namespace nebula