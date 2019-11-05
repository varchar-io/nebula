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

#include <algorithm>
#include <array>
#include <glog/logging.h>
#include <unordered_map>
#include "common/Errors.h"
#include "execution/eval/UDF.h"
#include "meta/Table.h"
#include "type/Tree.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace dsl {

// define all supported expression types
enum ExpressionType {
  UNKNOWN = 0,
  CONSTANT,
  COLUMN,
  ARTHMETIC,
  LOGICAL,
  UDF
};

// supported arthmetic operations
enum class ArthmeticOp {
  ADD,
  SUB,
  MUL,
  DIV,
  MOD
};

// supported logical operations
enum class LogicalOp {
  EQ,
  NEQ,
  GT,
  GE,
  LT,
  LE,
  AND,
  OR
};

#ifndef THIS_TYPE
#define THIS_TYPE typename std::remove_reference<decltype(*this)>::type
#endif

#ifndef IS_T_LITERAL
#define IS_T_LITERAL(T) std::is_same<char*, std::decay_t<T>>::value
#endif

// a structure to define an expression that can be serialized
// this is just a property bag - can be replaced by a hash map
struct ExpressionData {
  // static constexpr auto ALIAS = "alais";
  // std::unordered_map<std::string, std::string> properties;
  ExpressionType type = ExpressionType::UNKNOWN;
  std::string alias;
  // saved from typeid(T).name() for each template type
  // reference type/type.h for our supported data types:
  std::string c_type;
  std::string c_value;

  // column name
  std::string c_name;

  // logical op
  LogicalOp b_lop;

  // arthmetic op
  ArthmeticOp b_aop;

  // binary left and right
  std::unique_ptr<ExpressionData> b_left;
  std::unique_ptr<ExpressionData> b_right;

  // UDF type
  nebula::execution::eval::UDFType u_type;
  std::unique_ptr<ExpressionData> inner;

  // extra customized info
  std::string custom;

  // extra flag
  bool flag;
};

// NOTE:
// TODO(cao) - we put all these operator definitions in each concrete types
// IS only because the THIS_TYPE resolusion
// Right now, std::remove_reference<decltype(*this)>::type doesn't resolve runtime type but the current type
// where the method is called - if somehow we can figure out THIS_TYPE for runtime, we can save lots of duplicate code
// meaning we only need these operator defined in base expression.
class Expression {
public:
  Expression() : alias_{}, kind_{ nebula::type::Kind::INVALID } {}
  Expression(const Expression&) = default;
  Expression& operator=(const Expression&) = default;
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

  virtual std::unique_ptr<ExpressionData> serialize() const noexcept {
    auto data = std::make_unique<ExpressionData>();
    data->alias = alias_;
    return data;
  }

public:
  std::string alias() const {
    return alias_;
  }

  nebula::type::Kind kind() const {
    return kind_;
  }

protected:
  inline void extractAlias(const std::string& a1, const std::string& a2) {
    alias_ = a1.size() > 0 ? a1 : a2;
  }

  static nebula::type::TreeNode typeCreate(nebula::type::Kind kind, std::string&);

  // only one alias can be updated if client calls "as" multiple times
  std::string alias_;

  // set by type() method
  nebula::type::Kind kind_;
};

// a constant expression has its result type defined as T
template <typename T>
class ConstExpression;

// arthmetic expression
template <ArthmeticOp op, typename T1, typename T2>
class ArthmeticExpression;

// logical expression
template <LogicalOp op, typename T1, typename T2>
class LogicalExpression;

class ArthmeticCombination {
private:
  static constexpr std::array<nebula::type::Kind, 6> numbers{
    nebula::type::Kind::TINYINT,
    nebula::type::Kind::SMALLINT,
    nebula::type::Kind::INTEGER,
    nebula::type::Kind::BIGINT,
    nebula::type::Kind::REAL,
    nebula::type::Kind::DOUBLE
  };

  static constexpr size_t index(nebula::type::Kind k1, nebula::type::Kind k2) noexcept {
    constexpr size_t size = numbers.size();
    return (k1 - nebula::type::Kind::TINYINT) * size + (k2 - nebula::type::Kind::TINYINT);
  }

  // closure to initialize the map
  static constexpr std::array<nebula::type::Kind, 36> map = []() {
    constexpr size_t size = numbers.size();
    std::array<nebula::type::Kind, size * size> m{};
    for (size_t i = 0; i < size; ++i) {
      for (size_t j = 0; j < size; ++j) {
        m[i * size + j] = std::max(numbers[i], numbers[j]);
      }
    }

    return m;
  }();

public:
  static constexpr nebula::type::Kind result(nebula::type::Kind k1, nebula::type::Kind k2) noexcept {
    return map[index(k1, k2)];
  }
};

////////////////////////////////////////// FORWARD ARTH  //////////////////////////////////////////
struct arthmetic_forward {
  using Key = std::tuple<ArthmeticOp, nebula::type::Kind, nebula::type::Kind>;
  using Value = std::function<
    std::unique_ptr<nebula::execution::eval::ValueEval>(
      std::unique_ptr<nebula::execution::eval::ValueEval>,
      std::unique_ptr<nebula::execution::eval::ValueEval>)>;
  struct Hash {
    size_t operator()(const Key& k) const {
      return ((size_t)std::get<0>(k) << 32) | (std::get<1>(k) << 16) | (std::get<2>(k));
    }
  };
  using Map = std::unordered_map<Key, Value, Hash>;

  static const Map& map();

  std::unique_ptr<nebula::execution::eval::ValueEval> operator()(
    ArthmeticOp op,
    nebula::type::Kind k1,
    nebula::type::Kind k2,
    std::unique_ptr<nebula::execution::eval::ValueEval> v1,
    std::unique_ptr<nebula::execution::eval::ValueEval> v2);
};

////////////////////////////////////////// FORWARD LOGI  //////////////////////////////////////////
struct logical_forward {
  using Key = std::tuple<LogicalOp, nebula::type::Kind, nebula::type::Kind>;
  using Value = std::function<
    std::unique_ptr<nebula::execution::eval::ValueEval>(
      std::unique_ptr<nebula::execution::eval::ValueEval>,
      std::unique_ptr<nebula::execution::eval::ValueEval>)>;
  struct Hash {
    size_t operator()(const Key& k) const {
      return ((size_t)std::get<0>(k) << 32) | (std::get<1>(k) << 16) | (std::get<2>(k));
    }
  };
  using Map = std::unordered_map<Key, Value, Hash>;

  static const Map& map();

  std::unique_ptr<nebula::execution::eval::ValueEval> operator()(
    LogicalOp op,
    nebula::type::Kind k1,
    nebula::type::Kind k2,
    std::unique_ptr<nebula::execution::eval::ValueEval> v1,
    std::unique_ptr<nebula::execution::eval::ValueEval> v2);
};

} // namespace dsl
} // namespace api
} // namespace nebula