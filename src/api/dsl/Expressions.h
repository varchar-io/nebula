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

#include "Utils.h"
#include "api/UDAF.h"
#include "common/Errors.h"
#include "glog/logging.h"
#include "meta/Table.h"
#include "type/Tree.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace dsl {

// supported arthmetic operations
enum class ArthmeticOp {
  ADD,
  SUB,
  MUL,
  DIV,
  MOD,
  NEG
};

// supported logical operations
enum class LogicalOp {
  EQ,
  GT,
  GE,
  LT,
  LE,
  AND,
  OR
};

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
  Expression() = default;
  virtual ~Expression() = default;

public:
  // TODO(cao): need to rework to introduce context and visitor pattern
  // to deduce the type of this expression
  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) const = 0;
  virtual bool isAgg() const = 0;

public:
  std::string alias() const {
    return alias_;
  }

protected:
  // only one alias can be updated if client calls "as" multiple times
  std::string alias_;
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
    LOG(INFO) << "op1 alias: " << op1_.alias();
    alias_ = op1_.alias().size() > 0 ? op1_.alias() : op2_.alias();
  }
  virtual ~ArthmeticExpression() = default;

public: // all operations
  ALL_ARTHMETIC_LOGICAL_OPS()
  // ALL_ARTHMETIC_OPS()

  ALIAS()

  IS_AGG(false)

public:
  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) const override {
    const auto& type1 = op1_.type(table);
    const auto& type2 = op2_.type(table);

    auto kind1 = nebula::type::TypeBase::k(type1);
    auto kind2 = nebula::type::TypeBase::k(type2);

#define TYPE_OPRAND_OP(KIND)                                \
  case nebula::type::Kind::KIND: {                          \
    return validateAndGen<nebula::type::Kind::KIND>(kind2); \
  }

    switch (kind1) {
      TYPE_OPRAND_OP(TINYINT)
      TYPE_OPRAND_OP(SMALLINT)
      TYPE_OPRAND_OP(INTEGER)
      TYPE_OPRAND_OP(BIGINT)
      TYPE_OPRAND_OP(REAL)
      TYPE_OPRAND_OP(DOUBLE)
    default:
      throw NException("not supported type in arthmetic operations");
    }
  }
#undef TYPE_OPRAND_OP

protected:
#define RESULT_TYPE_ON_OPERANDS(RIGHT)                                     \
  case nebula::type::RIGHT: {                                              \
    result = ArthmeticOpTraits<op, kind, nebula::type::RIGHT>::ResultKind; \
    break;                                                                 \
  }

  template <nebula::type::Kind kind>
  nebula::type::TreeNode validateAndGen(nebula::type::Kind kind2) const {
    auto result = nebula::type::INVALID;
    switch (kind2) {
      RESULT_TYPE_ON_OPERANDS(TINYINT)
      RESULT_TYPE_ON_OPERANDS(SMALLINT)
      RESULT_TYPE_ON_OPERANDS(INTEGER)
      RESULT_TYPE_ON_OPERANDS(BIGINT)
      RESULT_TYPE_ON_OPERANDS(REAL)
      RESULT_TYPE_ON_OPERANDS(DOUBLE)
    default:
      throw NException("not supported type in arthmetic operations");
    }
#undef RESULT_TYPE_ON_OPERANDS

#define TYPE_CREATE_NODE(KIND, TYPE)               \
  case nebula::type::KIND: {                       \
    return nebula::type::TYPE::createTree(alias_); \
  }

    // no result should have the final type KIND
    switch (result) {
      TYPE_CREATE_NODE(TINYINT, ByteType)
      TYPE_CREATE_NODE(SMALLINT, ShortType)
      TYPE_CREATE_NODE(INTEGER, IntType)
      TYPE_CREATE_NODE(BIGINT, LongType)
      TYPE_CREATE_NODE(REAL, FloatType)
      TYPE_CREATE_NODE(DOUBLE, DoubleType)
    default:
      throw NException("not supported type in arthmetic operations");
    }
#undef TYPE_CREATE_NODE
  }

private:
  T1 op1_;
  T2 op2_;
}; // namespace dsl

#define ARTHMETIC_TYPE_COMBINATION_RESULT(LEFT, RIGHT, RESULT)                        \
  template <ArthmeticOp OP>                                                           \
  struct ArthmeticOpTraits<OP, nebula::type::Kind::LEFT, nebula::type::Kind::RIGHT> { \
    static constexpr nebula::type::Kind ResultKind = nebula::type::Kind::RESULT;      \
  };

// byte+byte=byte for any arthmetic type
ARTHMETIC_TYPE_COMBINATION_RESULT(TINYINT, TINYINT, TINYINT)
ARTHMETIC_TYPE_COMBINATION_RESULT(TINYINT, SMALLINT, SMALLINT)
ARTHMETIC_TYPE_COMBINATION_RESULT(TINYINT, INTEGER, INTEGER)
ARTHMETIC_TYPE_COMBINATION_RESULT(TINYINT, BIGINT, BIGINT)
ARTHMETIC_TYPE_COMBINATION_RESULT(TINYINT, REAL, REAL)
ARTHMETIC_TYPE_COMBINATION_RESULT(TINYINT, DOUBLE, DOUBLE)

ARTHMETIC_TYPE_COMBINATION_RESULT(SMALLINT, TINYINT, SMALLINT)
ARTHMETIC_TYPE_COMBINATION_RESULT(SMALLINT, SMALLINT, SMALLINT)
ARTHMETIC_TYPE_COMBINATION_RESULT(SMALLINT, INTEGER, INTEGER)
ARTHMETIC_TYPE_COMBINATION_RESULT(SMALLINT, BIGINT, BIGINT)
ARTHMETIC_TYPE_COMBINATION_RESULT(SMALLINT, REAL, REAL)
ARTHMETIC_TYPE_COMBINATION_RESULT(SMALLINT, DOUBLE, DOUBLE)

ARTHMETIC_TYPE_COMBINATION_RESULT(INTEGER, TINYINT, INTEGER)
ARTHMETIC_TYPE_COMBINATION_RESULT(INTEGER, SMALLINT, INTEGER)
ARTHMETIC_TYPE_COMBINATION_RESULT(INTEGER, INTEGER, INTEGER)
ARTHMETIC_TYPE_COMBINATION_RESULT(INTEGER, BIGINT, BIGINT)
ARTHMETIC_TYPE_COMBINATION_RESULT(INTEGER, REAL, REAL)
ARTHMETIC_TYPE_COMBINATION_RESULT(INTEGER, DOUBLE, DOUBLE)

ARTHMETIC_TYPE_COMBINATION_RESULT(BIGINT, TINYINT, BIGINT)
ARTHMETIC_TYPE_COMBINATION_RESULT(BIGINT, SMALLINT, BIGINT)
ARTHMETIC_TYPE_COMBINATION_RESULT(BIGINT, INTEGER, BIGINT)
ARTHMETIC_TYPE_COMBINATION_RESULT(BIGINT, BIGINT, BIGINT)
ARTHMETIC_TYPE_COMBINATION_RESULT(BIGINT, REAL, REAL)
ARTHMETIC_TYPE_COMBINATION_RESULT(BIGINT, DOUBLE, DOUBLE)

ARTHMETIC_TYPE_COMBINATION_RESULT(REAL, TINYINT, REAL)
ARTHMETIC_TYPE_COMBINATION_RESULT(REAL, SMALLINT, REAL)
ARTHMETIC_TYPE_COMBINATION_RESULT(REAL, INTEGER, REAL)
ARTHMETIC_TYPE_COMBINATION_RESULT(REAL, BIGINT, REAL)
ARTHMETIC_TYPE_COMBINATION_RESULT(REAL, REAL, REAL)
ARTHMETIC_TYPE_COMBINATION_RESULT(REAL, DOUBLE, DOUBLE)

ARTHMETIC_TYPE_COMBINATION_RESULT(DOUBLE, TINYINT, DOUBLE)
ARTHMETIC_TYPE_COMBINATION_RESULT(DOUBLE, SMALLINT, DOUBLE)
ARTHMETIC_TYPE_COMBINATION_RESULT(DOUBLE, INTEGER, DOUBLE)
ARTHMETIC_TYPE_COMBINATION_RESULT(DOUBLE, BIGINT, DOUBLE)
ARTHMETIC_TYPE_COMBINATION_RESULT(DOUBLE, REAL, DOUBLE)
ARTHMETIC_TYPE_COMBINATION_RESULT(DOUBLE, DOUBLE, DOUBLE)

#undef ARTHMETIC_TYPE_COMBINATION_RESULT

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

public:
  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) const override {
    // validate non-comparison case.
    // unlike programming language, we don't allow implicity type conversion
    // so for AND and OR operations, we have to validate left and right are both bool type
    validate(table);

    // logical expression will always in bool type
    return nebula::type::BoolType::createTree(alias_);
  }

protected:
#define BOTH_OPRANDS_BOOL()                                                             \
  N_ENSURE_EQ(nebula::type::TypeBase::k(op1_.type(table)), nebula::type::Kind::BOOLEAN, \
              "AND/OR operations requires bool typed left and right oprands");          \
  N_ENSURE_EQ(nebula::type::TypeBase::k(op2_.type(table)), nebula::type::Kind::BOOLEAN, \
              "AND/OR operations requires bool typed left and right oprands");

  void validate(const nebula::meta::Table& table) const {
    if constexpr (op == LogicalOp::AND || op == LogicalOp::OR) {
      BOTH_OPRANDS_BOOL()
    }
  }

#undef BOTH_OPRANDS_BOOL

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

public:
  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) const override {
    // TODO(cao) - respect passed in alias to rename current column expression
    // for example: select col1 as COOL from table

    // look up the table schema to deduce the table
    const auto& schema = table.getSchema();
    nebula::type::TreeNode nodeType;
    schema->onChild(column_, [&nodeType](const nebula::type::TypeNode& found) {
      nodeType = std::dynamic_pointer_cast<nebula::type::TreeBase>(found);
    });

    N_ENSURE_NOT_NULL(nodeType, fmt::format("column not found: {0}", column_));
    return nodeType;
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
  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) const override {
    // duduce from T to nebula::type::Type
    return nebula::type::TypeDetect<T>::type(alias_);
  }

  ALIAS()

  IS_AGG(false)

private:
  T value_;
};

// An UDAF expression
// TODO(cao) - need rework this since we need to come up with a framework
// to allow customized UDAFs to be plugged in
template <typename T, IS_EXPRESSION(T)>
class UDAFExpression : public Expression {
public:
  UDAFExpression(const T& inner, nebula::api::UDAF udaf)
    : inner_{ inner }, udaf_{ udaf } {
    alias_ = inner_.alias();
  }
  virtual ~UDAFExpression() = default;

public:
  virtual nebula::type::TreeNode type(const nebula::meta::Table& table) const override {
    // count will not rely on inner type
    if (udaf_.registry == UDAFRegistry::COUNT) {
      return nebula::type::LongType::createTree(alias_);
    }

    LOG(INFO) << " UDAF alias: " << alias_;
    return inner_.type(table);
  }

  ALIAS()

  IS_AGG(true)

private:
  T inner_;
  nebula::api::UDAF udaf_;
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