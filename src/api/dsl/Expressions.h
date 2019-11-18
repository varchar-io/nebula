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

#include <folly/Conv.h>
#include <glog/logging.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "Base.h"
#include "api/udf/In.h"
#include "api/udf/UDFFactory.h"
#include "common/Errors.h"
#include "common/Likely.h"
#include "meta/Table.h"
#include "surface/eval/UDF.h"
#include "surface/eval/ValueEval.h"
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

// A macro to limit the type to be base expression
#define IS_EXPRESSION(T) typename std::enable_if_t<std::is_base_of_v<Expression, T>, bool> = true
// used in definition signature to match the declaration without default value
#define IS_EXPRESSION_D(T) typename std::enable_if_t<std::is_base_of_v<Expression, T>, bool>

// TODO(cao) - we rely on a copy of current (*this) expression using implicit copy constructor
// We should consider https://en.cppreference.com/w/cpp/memory/enable_shared_from_this
#define ARTHMETIC_OP_CONST(OP, TYPE)                                                                                          \
  template <typename M, bool OK = std::is_arithmetic<M>::value>                                                               \
  auto operator OP(const M& m)->std::enable_if_t<OK, ArthmeticExpression<ArthmeticOp::TYPE, THIS_TYPE, ConstExpression<M>>> { \
    return ArthmeticExpression<ArthmeticOp::TYPE, THIS_TYPE, ConstExpression<M>>(                                             \
      std::make_shared<THIS_TYPE>(*this), std::make_shared<ConstExpression<M>>(m));                                           \
  }

#define ARTHMETIC_OP_GENERIC(OP, TYPE)                                                     \
  template <typename R, IS_EXPRESSION(R)>                                                  \
  auto operator OP(const R& right)->ArthmeticExpression<ArthmeticOp::TYPE, THIS_TYPE, R> { \
    return ArthmeticExpression<ArthmeticOp::TYPE, THIS_TYPE, R>(                           \
      std::make_shared<THIS_TYPE>(*this), std::make_shared<R>(right));                     \
  }

#define LOGICAL_OP_CONST(OP, TYPE)                                                                                        \
  template <typename M, bool OK = std::is_arithmetic<M>::value && !(std::is_same<char*, std::decay_t<M>>::value)>         \
  auto operator OP(const M& m)->std::enable_if_t<OK, LogicalExpression<LogicalOp::TYPE, THIS_TYPE, ConstExpression<M>>> { \
    return LogicalExpression<LogicalOp::TYPE, THIS_TYPE, ConstExpression<M>>(                                             \
      std::make_shared<THIS_TYPE>(*this), std::make_shared<ConstExpression<M>>(m));                                       \
  }

#define LOGICAL_OP_GENERIC(OP, TYPE)                                                   \
  template <typename R, IS_EXPRESSION(R)>                                              \
  auto operator OP(const R& right)->LogicalExpression<LogicalOp::TYPE, THIS_TYPE, R> { \
    return LogicalExpression<LogicalOp::TYPE, THIS_TYPE, R>(                           \
      std::make_shared<THIS_TYPE>(*this), std::make_shared<R>(right));                 \
  }

#define LOGICAL_OP_EXPR(OP, TYPE)                                                                                        \
  auto operator OP(const std::shared_ptr<Expression> right)->LogicalExpression<LogicalOp::TYPE, THIS_TYPE, Expression> { \
    return LogicalExpression<LogicalOp::TYPE, THIS_TYPE, Expression>(                                                    \
      std::make_shared<THIS_TYPE>(*this), right);                                                                        \
  }

#define LOGICAL_OP_STRING(OP, TYPE) \
  auto operator OP(std::string_view)->LogicalExpression<LogicalOp::TYPE, THIS_TYPE, ConstExpression<std::string_view>>;

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
  LOGICAL_OP_EXPR(==, EQ)     \
  LOGICAL_OP_CONST(!=, NEQ)   \
  LOGICAL_OP_STRING(!=, NEQ)  \
  LOGICAL_OP_GENERIC(!=, NEQ) \
  LOGICAL_OP_EXPR(!=, NEQ)    \
  LOGICAL_OP_CONST(>, GT)     \
  LOGICAL_OP_STRING(>, GT)    \
  LOGICAL_OP_GENERIC(>, GT)   \
  LOGICAL_OP_EXPR(>, GT)      \
  LOGICAL_OP_CONST(>=, GE)    \
  LOGICAL_OP_STRING(>=, GE)   \
  LOGICAL_OP_GENERIC(>=, GE)  \
  LOGICAL_OP_EXPR(>=, GE)     \
  LOGICAL_OP_CONST(<, LT)     \
  LOGICAL_OP_STRING(<, LT)    \
  LOGICAL_OP_GENERIC(<, LT)   \
  LOGICAL_OP_EXPR(<, LT)      \
  LOGICAL_OP_CONST(<=, LE)    \
  LOGICAL_OP_STRING(<=, LE)   \
  LOGICAL_OP_GENERIC(<=, LE)  \
  LOGICAL_OP_EXPR(<=, LE)     \
                              \
  LOGICAL_OP_GENERIC(&&, AND) \
  LOGICAL_OP_EXPR(&&, AND)    \
  LOGICAL_OP_GENERIC(||, OR)  \
  LOGICAL_OP_EXPR(||, OR)

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

// arthmetic op traits will provide result KIND given left and right kind on given OP
template <ArthmeticOp OP, nebula::type::Kind LEFT, nebula::type::Kind RIGHT>
struct ArthmeticOpTraits {};

template <ArthmeticOp op, typename T1, typename T2>
class ArthmeticExpression : public Expression {
public:
  ArthmeticExpression(const std::shared_ptr<T1> op1, const std::shared_ptr<T2>& op2)
    : op1_{ op1 }, op2_{ op2 } {
    // failed type check
    if constexpr (!std::is_base_of_v<Expression, T1> || !std::is_base_of_v<Expression, T2>) {
      throw NException("All arthmetic oprands need to be expression");
    }

    // get alias from the first valid op
    extractAlias(op1_->alias(), op2_->alias());
  }

  ArthmeticExpression(const ArthmeticExpression& other)
    : Expression(other), op1_{ other.op1_ }, op2_{ other.op2_ } {
  }

  ArthmeticExpression& operator=(const ArthmeticExpression& other) {
    op1_ = other.op1_;
    op2_ = other.op2_;
    return *this;
  }

  virtual ~ArthmeticExpression() = default;

public: // all operations
  ALL_ARTHMETIC_LOGICAL_OPS()
  // ALL_ARTHMETIC_OPS()

  ALIAS()

  IS_AGG(false)

  virtual std::unique_ptr<nebula::surface::eval::ValueEval> asEval() const override {
    auto v1 = op1_->asEval();
    auto v2 = op2_->asEval();
    N_ENSURE(v1 != nullptr, "op1 value eval is null");
    N_ENSURE(v2 != nullptr, "op2 value eval is null");

    // forward to the correct version of value eval creation
    return arthmetic_forward()(op, op1_->typeInfo().native, op2_->typeInfo().native, std::move(v1), std::move(v2));
  }

  virtual std::vector<std::string> columnRefs() const override {
    auto v1 = op1_->columnRefs();
    auto v2 = op2_->columnRefs();
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

  virtual TypeInfo type(const nebula::meta::Table& table) override {
    auto opt1 = op1_->type(table);
    auto opt2 = op2_->type(table);

    // arthmetic operation only cares native type
    type_ = TypeInfo{
      nebula::api::dsl::ArthmeticCombination::result(opt1.native, opt2.native)
    };
    return type_;
  }

  virtual std::unique_ptr<ExpressionData> serialize() const noexcept override {
    auto data = Expression::serialize();
    data->type = ExpressionType::ARTHMETIC;
    data->b_aop = op;
    data->b_left = std::move(op1_->serialize());
    data->b_right = std::move(op2_->serialize());
    return data;
  }

private:
  std::shared_ptr<T1> op1_;
  std::shared_ptr<T2> op2_;
};

// logical expression definition
template <LogicalOp op, typename T1, typename T2>
class LogicalExpression : public Expression {
public:
  LogicalExpression(const std::shared_ptr<T1> op1, const std::shared_ptr<T2> op2)
    : op1_{ op1 }, op2_{ op2 } {
    // failed type check
    if constexpr (!std::is_base_of_v<Expression, T1> || !std::is_base_of_v<Expression, T2>) {
      throw NException("All logical oprands need to be expression");
    }

    // get alias from the first valid op
    extractAlias(op1_->alias(), op2_->alias());
  }
  LogicalExpression(const LogicalExpression& other)
    : Expression(other), op1_{ other.op1_ }, op2_{ other.op2_ } {
  }

  LogicalExpression& operator=(const LogicalExpression& other) {
    op1_ = other.op1_;
    op2_ = other.op2_;
    return *this;
  }
  virtual ~LogicalExpression() = default;

public: // all logical operations
  ALL_LOGICAL_OPS()

  ALIAS()

  IS_AGG(false)

  // convert to value eval
  virtual std::unique_ptr<nebula::surface::eval::ValueEval> asEval() const override {
    auto v1 = op1_->asEval();
    auto v2 = op2_->asEval();
    N_ENSURE(v1 != nullptr, "op1 value eval is null");
    N_ENSURE(v2 != nullptr, "op2 value eval is null");

    // forward to the correct version of value eval creation
    return logical_forward()(op, op1_->typeInfo().native, op2_->typeInfo().native, std::move(v1), std::move(v2));
  }

  virtual TypeInfo type(const nebula::meta::Table& table) override {
    op1_->type(table);
    op2_->type(table);

    // validate non-comparison case.
    // unlike programming language, we don't allow implicity type conversion
    // so for AND and OR operations, we have to validate left and right are both bool type
    validate(table);

    // logical expression will always in bool type
    type_ = TypeInfo{ nebula::type::Kind::BOOLEAN };
    return type_;
  }

  virtual std::unique_ptr<ExpressionData> serialize() const noexcept override {
    auto data = Expression::serialize();
    data->type = ExpressionType::LOGICAL;
    data->b_lop = op;
    data->b_left = std::move(op1_->serialize());
    data->b_right = std::move(op2_->serialize());
    return data;
  }

#define BOTH_OPRANDS_BOOL()                                                    \
  N_ENSURE_EQ(op1_->typeInfo().native, nebula::type::Kind::BOOLEAN,            \
              "AND/OR operations requires bool typed left and right oprands"); \
  N_ENSURE_EQ(op2_->typeInfo().native, nebula::type::Kind::BOOLEAN,            \
              "AND/OR operations requires bool typed left and right oprands");

  void validate(const nebula::meta::Table&) {
    if constexpr (op == LogicalOp::AND || op == LogicalOp::OR) {
      BOTH_OPRANDS_BOOL()
    }
  }

#undef BOTH_OPRANDS_BOOL

  virtual std::vector<std::string> columnRefs() const override {
    auto v1 = op1_->columnRefs();
    auto v2 = op2_->columnRefs();
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
  std::shared_ptr<T1> op1_;
  std::shared_ptr<T2> op2_;
};

// represent a column - type is runtime resolved
class ColumnExpression : public Expression {
public:
  ColumnExpression(const std::string& column)
    : column_{ column } {
    alias_ = column_;
  }

  ColumnExpression(const ColumnExpression& other) = default;
  ColumnExpression& operator=(const ColumnExpression& other) = default;
  virtual ~ColumnExpression() = default;

public: // all logical operations
  ALL_ARTHMETIC_LOGICAL_OPS()

  ALIAS()

  IS_AGG(false)

  virtual std::unique_ptr<nebula::surface::eval::ValueEval> asEval() const override;
  virtual TypeInfo type(const nebula::meta::Table& table) override;
  virtual std::unique_ptr<ExpressionData> serialize() const noexcept override;
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
  ConstExpression(const ConstExpression&) = default;
  ConstExpression& operator=(const ConstExpression&) = default;
  virtual ~ConstExpression() = default;

public:
  ALIAS()

  IS_AGG(false)

  // convert to value eval
  virtual std::unique_ptr<nebula::surface::eval::ValueEval> asEval() const override {
    return nebula::surface::eval::constant(value_);
  }

  virtual TypeInfo type(const nebula::meta::Table&) override {
    // duduce from T to nebula::type::Type
    type_ = TypeInfo{ nebula::type::TypeDetect<T>::kind };
    return type_;
  }

  virtual std::unique_ptr<ExpressionData> serialize() const noexcept override {
    auto data = Expression::serialize();
    data->type = ExpressionType::CONSTANT;
    data->c_type = nebula::type::TypeDetect<T>::tid();
    data->c_value = folly::to<std::string>(value_);
    return data;
  }

private:
  T value_;
};

// An UDAF expression - some UDAF knows its type regardless inner type
// such as percentile (P50, P90), the majority UDAF infers type from its inner expression.
// So here - we will implement this first. When necessary, we may want to introduce different expresssion type.
// TODO(cao) - need rework this since we need to come up with a framework
// to allow customized UDAFs to be plugged in
template <nebula::surface::eval::UDFType UT>
class UDFExpression : public Expression {
public:
  UDFExpression(std::shared_ptr<Expression> inner)
    : inner_{ inner } {}
  UDFExpression(const UDFExpression&) = default;
  UDFExpression& operator=(const UDFExpression&) = default;
  virtual ~UDFExpression() = default;

public:
  ALIAS()

  IS_AGG(nebula::surface::eval::UdfTraits<UT>::UDAF)

  virtual TypeInfo type(const nebula::meta::Table& table) override {
    // always call inner_ type since it's going to change its state
    auto innerType = inner_->type(table);

    // if this UDF has pre-defined type, we don't need to get it from inner expression then
    type_ = TypeInfo{
      nebula::surface::eval::udfKind<UT, false>(innerType.native),
      nebula::surface::eval::udfKind<UT, true>(innerType.native),
    };

    return type_;
  }

  inline virtual std::vector<std::string> columnRefs() const override {
    return inner_->columnRefs();
  }

#define CASE_KIND_UDF(KIND)                                                               \
  case nebula::type::Kind::KIND: {                                                        \
    return nebula::api::udf::UDFFactory::createUDF<UT, nebula::type::Kind::KIND>(inner_); \
  }

  virtual std::unique_ptr<nebula::surface::eval::ValueEval> asEval() const override {
    // based on type, create different UDAF object and pass it to UDF function wrapper
    switch (type_.native) {
      CASE_KIND_UDF(BOOLEAN)
      CASE_KIND_UDF(TINYINT)
      CASE_KIND_UDF(SMALLINT)
      CASE_KIND_UDF(INTEGER)
      CASE_KIND_UDF(BIGINT)
      CASE_KIND_UDF(REAL)
      CASE_KIND_UDF(DOUBLE)
      CASE_KIND_UDF(VARCHAR)
    default:
      throw NException("kind not found.");
    }
  }

  virtual std::unique_ptr<ExpressionData> serialize() const noexcept override {
    auto data = Expression::serialize();
    data->type = ExpressionType::UDF;
    data->u_type = UT;
    data->inner = std::move(inner_->serialize());
    return data;
  }

#undef CASE_KIND_UDF

protected:
  std::shared_ptr<Expression> inner_;
};

template <nebula::surface::eval::UDFType UDF>
class BoolUDF : public Expression {
public:
  BoolUDF(std::shared_ptr<Expression> expr)
    : expr_{ expr } {}

public:
  IS_AGG(nebula::surface::eval::UdfTraits<UDF>::UDAF)

  virtual TypeInfo type(const nebula::meta::Table& table) override {
    expr_->type(table);

    // inner type is
    type_ = TypeInfo{ nebula::type::Kind::BOOLEAN };
    return type_;
  }

  virtual std::unique_ptr<ExpressionData> serialize() const noexcept override {
    auto data = Expression::serialize();
    data->type = ExpressionType::UDF;
    data->u_type = UDF;
    data->inner = std::move(expr_->serialize());
    return data;
  }

protected:
  std::shared_ptr<Expression> expr_;
};

using LikeBase = BoolUDF<nebula::surface::eval::UDFType::LIKE>;
class LikeExpression : public LikeBase {
public:
  LikeExpression(std::shared_ptr<Expression> left, const std::string& pattern, bool caseSensitive = true)
    : LikeBase(left), pattern_{ pattern }, caseSensitive_{ caseSensitive } {}

public:
  ALL_LOGICAL_OPS()
  ALIAS()

  virtual std::unique_ptr<nebula::surface::eval::ValueEval> asEval() const override {
    return nebula::api::udf::UDFFactory::createUDF<
      nebula::surface::eval::UDFType::LIKE,
      nebula::type::Kind::VARCHAR>(expr_, pattern_, caseSensitive_);
  }

  virtual std::unique_ptr<ExpressionData> serialize() const noexcept override {
    auto data = LikeBase::serialize();
    data->custom = pattern_;
    data->flag = caseSensitive_;
    return data;
  }

private:
  std::string pattern_;
  bool caseSensitive_;
};

using PrefixBase = BoolUDF<nebula::surface::eval::UDFType::PREFIX>;
class PrefixExpression : public PrefixBase {
public:
  PrefixExpression(std::shared_ptr<Expression> left, const std::string& prefix, bool caseSensitive = true)
    : PrefixBase(left), prefix_{ prefix }, caseSensitive_{ caseSensitive } {}

public:
  ALL_LOGICAL_OPS()
  ALIAS()

  virtual std::unique_ptr<nebula::surface::eval::ValueEval> asEval() const override {
    return nebula::api::udf::UDFFactory::createUDF<
      nebula::surface::eval::UDFType::PREFIX,
      nebula::type::Kind::VARCHAR>(expr_, prefix_, caseSensitive_);
  }

  virtual std::unique_ptr<ExpressionData> serialize() const noexcept override {
    auto data = PrefixBase::serialize();
    data->custom = prefix_;
    data->flag = caseSensitive_;
    return data;
  }

private:
  std::string prefix_;
  bool caseSensitive_;
};

using InBase = BoolUDF<nebula::surface::eval::UDFType::IN>;
template <typename T>
class InExpression : public InBase {
public:
  InExpression(std::shared_ptr<Expression> left, const std::vector<T>& values, bool in = true)
    : InBase(left), values_{ values }, in_{ in } {}

public:
  ALL_LOGICAL_OPS()
  ALIAS()

  virtual std::unique_ptr<nebula::surface::eval::ValueEval> asEval() const override {
    return in_ ?
             nebula::api::udf::UDFFactory::createUDF<
               nebula::surface::eval::UDFType::IN, nebula::type::TypeDetect<T>::kind>(expr_, values_) :
             nebula::api::udf::UDFFactory::createUDF<
               nebula::surface::eval::UDFType::IN, nebula::type::TypeDetect<T>::kind>(expr_, values_, false);
  }

#define TYPE_BRANCH(CPP, FUNC)            \
  if constexpr (std::is_same_v<T, CPP>) { \
    for (auto v : values_) {              \
      json.FUNC(v);                       \
    }                                     \
  }

  virtual std::unique_ptr<ExpressionData> serialize() const noexcept override {
    auto data = InBase::serialize();
    // itself is a simple JSON - better to use JSON library to serialize it
    // fmt provides join function as
    //   fmt::format("{0:.{1}f}", fmt::join(std::begin(test), std::end(test), ", ");
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> json(buffer);
    json.StartObject();
    json.Key("in");
    json.Bool(in_);
    auto dtype = nebula::type::TypeDetect<T>::tid();
    json.Key("dtype");
    json.String(dtype.data(), dtype.size());
    json.Key("values");
    json.StartArray();
    TYPE_BRANCH(bool, Bool)
    TYPE_BRANCH(int8_t, Int)
    TYPE_BRANCH(int16_t, Int)
    TYPE_BRANCH(int32_t, Int)
    TYPE_BRANCH(float, Double)
    TYPE_BRANCH(double, Double)

    // JSON can't represent big int unfortunately
    if constexpr (std::is_same_v<T, int64_t>) {
      for (auto v : values_) {
        auto str = std::to_string(v);
        json.String(str.data(), str.size());
      }
    }

    // string is different
    if constexpr (std::is_same_v<T, std::string>) {
      for (auto& v : values_) {
        json.String(v.data(), v.size());
      }
    }

    json.EndArray();
    json.EndObject();
    data->custom = buffer.GetString();
    return data;
  }

#undef TYPE_BRANCH

private:
  std::vector<T> values_;
  bool in_;
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