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

#include <fmt/format.h>
#include <glog/logging.h>
#include "common/Cursor.h"
#include "common/Likely.h"
#include "meta/NNode.h"
#include "surface/DataSurface.h"

/**
 * Execution on each expression to get final value.
 */
namespace nebula {
namespace execution {
namespace eval {

template <typename T>
class TypeValueEval;

// this is a tree, with each node to be either macro/value or operator
// this is translated from expression.
class ValueEval {
public:
  ValueEval(const std::string& sign) : sign_{ sign } {}
  virtual ~ValueEval() = default;

  // TODO(cao) - we definitely need to revisit and reevaluate if we should use std::optional<T> here
  // which shall simplify the interface a bit.
  // Current decision is to put a bool reference to indicate if the eval should be "NULL" value
  // passed value has to be true (default) - only set to false internally if the value is evaluated as NULL/empty
  // other alternative approaches:
  // 1. std::unique_ptr<T> to return nullptr or value pointer.
  // 2. std::optinal<T> to return optional value.
  // 3. a flag to indicate the return value should not be used
  template <typename T>
  T eval(const nebula::surface::RowData& row, bool& valid) const {
    // TODO(cao) - there is a problem wasted me a WHOLE day to figure out the root cause.
    // So docuemnt here for further robust engineering work, the case is like this:
    // When it create ValueEval, it uses template to generate TypeValueEval<std::string> for VARCHAR type
    // However, there is some mismatch to cause this function call to be eval<char*>(row)
    // obviously, below static_cast will give us a corrupted object since the concrete types mismatch.
    // This is a hard problem if we don't pay attention and waste lots of time to debug it, so how can we prevent similar problem
    // 1. we should do stronger type check to ensure the type used is consistent everywhere.
    // 2. we can enforce std::enable_if more on the template type to ensure the function is called in the expected "type"
    return static_cast<const TypeValueEval<T>*>(this)->eval(row, valid);
  }

  // identify a unique value evaluation object in given query context
  // TODO(cao) - consider using number instead for fast hashing
  const std::string& signature() const {
    return sign_;
  }

protected:
  std::string sign_;
};

// two utilities
#define OPT std::function<T(const nebula::surface::RowData&, const std::vector<std::unique_ptr<ValueEval>>&, bool&)>
#define OPT_LAMBDA(X)                                                                                              \
  ([](const nebula::surface::RowData& row, const std::vector<std::unique_ptr<ValueEval>>& children, bool& valid) { \
    X                                                                                                              \
  })

template <typename T>
class TypeValueEval : public ValueEval {
public:
  TypeValueEval(
    const std::string& sign,
    const OPT& op,
    std::vector<std::unique_ptr<ValueEval>> children)
    : ValueEval(sign), op_{ op }, children_{ std::move(children) } {}

  virtual ~TypeValueEval() = default;

  inline T eval(const nebula::surface::RowData& row, bool& valid) const {
    return op_(row, this->children_, valid);
  }

private:
  OPT op_;
  std::vector<std::unique_ptr<ValueEval>> children_;
};

///////////////////////////////////////////////////////////////////////////////////////////////////
// NOTE - in GCC 7, even it is template, we can't define a static method in header
// otherwise the GCC linker will treat it as deleted function during link time
// And we will see lots of issues like "xxx defined in discarded section"
template <typename T>
std::unique_ptr<ValueEval> constant(T v) {
  return std::unique_ptr<ValueEval>(
    new TypeValueEval<T>(
      fmt::format("C:{0}", v),
      [v](const nebula::surface::RowData&, const std::vector<std::unique_ptr<ValueEval>>&, bool&) -> T {
        return v;
      },
      {}));
}

#define NULL_CHECK(R)               \
  if (UNLIKELY(row.isNull(name))) { \
    valid = false;                  \
    return R;                       \
  }

template <typename T>
std::unique_ptr<ValueEval> column(const std::string& name) {
  return std::unique_ptr<ValueEval>(
    new TypeValueEval<T>(
      fmt::format("F:{0}", name),
      [name](const nebula::surface::RowData& row, const std::vector<std::unique_ptr<ValueEval>>&, bool& valid)
        -> T {
        // compile time branching based on template type T
        // I think it's better than using template specialization for this case
        if constexpr (std::is_same<T, bool>::value) {
          NULL_CHECK(false)
          return row.readBool(name);
        }

        if constexpr (std::is_same<T, int8_t>::value) {
          NULL_CHECK(0)
          return row.readByte(name);
        }

        if constexpr (std::is_same<T, int16_t>::value) {
          NULL_CHECK(0)
          return row.readShort(name);
        }

        if constexpr (std::is_same<T, int32_t>::value) {
          NULL_CHECK(0)
          return row.readInt(name);
        }

        if constexpr (std::is_same<T, int64_t>::value) {
          NULL_CHECK(0)
          return row.readLong(name);
        }

        if constexpr (std::is_same<T, float>::value) {
          NULL_CHECK(0)
          return row.readFloat(name);
        }

        if constexpr (std::is_same<T, double>::value) {
          NULL_CHECK(0)
          return row.readDouble(name);
        }

        if constexpr (std::is_same<T, std::string>::value) {
          NULL_CHECK("")
          return row.readString(name);
        }

        // TODO(cao): other types supported in DSL? for example: UDF on list or map
        throw NException("not supported template type");
      },
      {}));
}

#undef NULL_CHECK

// TODO(cao): optimization - fold constant nodes, we don't need keep a constant node
// WHEN arthmetic operation meets NULL (valid==false), return 0 and indicate valid as false
#define ARTHMETIC_VE(NAME, SIGN)                                                                  \
  template <typename T, typename T1, typename T2>                                                 \
  std::unique_ptr<ValueEval> NAME(std::unique_ptr<ValueEval> v1, std::unique_ptr<ValueEval> v2) { \
    static constexpr T INVALID = 0;                                                               \
    const auto s1 = v1->signature();                                                              \
    const auto s2 = v2->signature();                                                              \
    std::vector<std::unique_ptr<ValueEval>> branch;                                               \
    branch.reserve(2);                                                                            \
    branch.push_back(std::move(v1));                                                              \
    branch.push_back(std::move(v2));                                                              \
                                                                                                  \
    return std::unique_ptr<ValueEval>(                                                            \
      new TypeValueEval<T>(                                                                       \
        fmt::format("{0}:{1}:{2}", s1, #SIGN, s2),                                                \
        OPT_LAMBDA({                                                                              \
          auto v1 = children[0]->eval<T1>(row, valid);                                            \
          if (UNLIKELY(!valid)) {                                                                 \
            return INVALID;                                                                       \
          }                                                                                       \
          auto v2 = children[1]->eval<T2>(row, valid);                                            \
          if (UNLIKELY(!valid)) {                                                                 \
            return INVALID;                                                                       \
          }                                                                                       \
          return T(v1 SIGN v2);                                                                   \
        }),                                                                                       \
        std::move(branch)));                                                                      \
  }

ARTHMETIC_VE(add, +)
ARTHMETIC_VE(sub, -)
ARTHMETIC_VE(mul, *)
ARTHMETIC_VE(div, /)
ARTHMETIC_VE(mod, %)

#undef ARTHMETIC_VE

// TODO(cao) - merge with ARTHMETIC_VE since they are pretty much the same
// WHEN logical operation meets NULL (valid==false), return false and indicate valid as false
#define COMPARE_VE(NAME, SIGN)                                                                    \
  template <typename T1, typename T2>                                                             \
  std::unique_ptr<ValueEval> NAME(std::unique_ptr<ValueEval> v1, std::unique_ptr<ValueEval> v2) { \
    const auto s1 = v1->signature();                                                              \
    const auto s2 = v2->signature();                                                              \
    std::vector<std::unique_ptr<ValueEval>> branch;                                               \
    branch.reserve(2);                                                                            \
    branch.push_back(std::move(v1));                                                              \
    branch.push_back(std::move(v2));                                                              \
    return std::unique_ptr<ValueEval>(                                                            \
      new TypeValueEval<bool>(                                                                    \
        fmt::format("{0}:{1}:{2}", s1, #SIGN, s2),                                                \
        OPT_LAMBDA({                                                                              \
          auto v1 = children[0]->eval<T1>(row, valid);                                            \
          if (UNLIKELY(!valid)) {                                                                 \
            return false;                                                                         \
          }                                                                                       \
          auto v2 = children[1]->eval<T2>(row, valid);                                            \
          if (UNLIKELY(!valid)) {                                                                 \
            return false;                                                                         \
          }                                                                                       \
          return v1 SIGN v2;                                                                      \
        }),                                                                                       \
        std::move(branch)));                                                                      \
  }

COMPARE_VE(gt, >)
COMPARE_VE(ge, >=)
COMPARE_VE(eq, ==)
COMPARE_VE(neq, !=)
COMPARE_VE(lt, <)
COMPARE_VE(le, <=)

// unncessary compare but same signatures
COMPARE_VE(band, &&)
COMPARE_VE(bor, ||)

#undef COMPARE_VE

} // namespace eval
} // namespace execution
} // namespace nebula