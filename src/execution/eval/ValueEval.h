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
#include "common/Cursor.h"
#include "meta/NNode.h"
#include "surface/DataSurface.h"

/**
 * Execution on each expression to get final value.
 */
namespace nebula {
namespace execution {
namespace eval {

template <typename T>
struct TypeValueEval;

// this is a tree, with each node to be either macro/value or operator
// this is translated from expression.
struct ValueEval {
  ValueEval() = default;
  virtual ~ValueEval() = default;

  template <typename T>
  T eval(const nebula::surface::RowData& row) const {
    // TODO(cao) - there is a problem wasted me a WHOLE day to figure out the root cause.
    // So docuemnt here for further robust engineering work, the case is like this:
    // When it create ValueEval, it uses template to generate TypeValueEval<std::string> for VARCHAR type
    // However, there is some mismatch to cause this function call to be eval<char*>(row)
    // obviously, below static_cast will give us a corrupted object since the concrete types mismatch.
    // This is a hard problem if we don't pay attention and waste lots of time to debug it, so how can we prevent similar problem
    // 1. we should do stronger type check to ensure the type used is consistent everywhere.
    // 2. we can enforce std::enable_if more on the template type to ensure the function is called in the expected "type"
    return static_cast<const TypeValueEval<T>*>(this)->eval(row);
  }
};

// two utilities
#define OptType = T(*f)(const nebula::surface::RowData&, const std::vector<std::unique_ptr<ValueEval>>&);
#define OPT std::function<T(const nebula::surface::RowData&, const std::vector<std::unique_ptr<ValueEval>>&)>
#define OPT_LAMBDA(X)                                                                                 \
  ([](const nebula::surface::RowData& row, const std::vector<std::unique_ptr<ValueEval>>& children) { \
    X                                                                                                 \
  })

template <typename T>
struct TypeValueEval : public ValueEval {
  TypeValueEval(
    const OPT& op,
    std::vector<std::unique_ptr<ValueEval>> children)
    : op_{ op }, children_{ std::move(children) } {}

  virtual ~TypeValueEval() = default;

  inline T eval(const nebula::surface::RowData& row) const {
    return op_(row, this->children_);
  }

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
      [v](const nebula::surface::RowData&, const std::vector<std::unique_ptr<ValueEval>>&) -> T {
        return v;
      },
      {}));
}

template <typename T>
std::unique_ptr<ValueEval> column(const std::string& name) {
  return std::unique_ptr<ValueEval>(
    new TypeValueEval<T>(
      [name](const nebula::surface::RowData& row, const std::vector<std::unique_ptr<ValueEval>>&) -> T {
        // compile time branching based on template type T
        // I think it's better than using template specialization for this case
        if constexpr (std::is_same<T, bool>::value) {
          return row.readBool(name);
        }

        if constexpr (std::is_same<T, int8_t>::value) {
          return row.readByte(name);
        }

        if constexpr (std::is_same<T, int16_t>::value) {
          return row.readShort(name);
        }

        if constexpr (std::is_same<T, int32_t>::value) {
          return row.readInt(name);
        }

        if constexpr (std::is_same<T, int64_t>::value) {
          return row.readLong(name);
        }

        if constexpr (std::is_same<T, float>::value) {
          return row.readFloat(name);
        }

        if constexpr (std::is_same<T, double>::value) {
          return row.readDouble(name);
        }

        if constexpr (std::is_same<T, std::string>::value) {
          return row.readString(name);
        }

        // TODO(cao): other types supported in DSL? for example: UDF on list or map
        throw NException("not supported template type");
      },
      {}));
}

// TODO(cao): optimization - fold constant nodes, we don't need keep a constant node
#define ARTHMETIC_VE(NAME, SIGN)                                                                         \
  template <typename T, typename T1, typename T2>                                                        \
  std::unique_ptr<ValueEval> NAME(std::unique_ptr<ValueEval> v1, std::unique_ptr<ValueEval> v2) { \
    std::vector<std::unique_ptr<ValueEval>> branch;                                                      \
    branch.reserve(2);                                                                                   \
    branch.push_back(std::move(v1));                                                                     \
    branch.push_back(std::move(v2));                                                                     \
                                                                                                         \
    return std::unique_ptr<ValueEval>(                                                                   \
      new TypeValueEval<T>(OPT_LAMBDA({                                                                  \
                             auto v1 = children[0]->eval<T1>(row);                                       \
                             auto v2 = children[1]->eval<T2>(row);                                       \
                             auto v3 = v1 SIGN v2;                                                       \
                             return v3;                                                                  \
                           }),                                                                           \
                           std::move(branch)));                                                          \
  }

ARTHMETIC_VE(add, +)
ARTHMETIC_VE(sub, -)
ARTHMETIC_VE(mul, *)
ARTHMETIC_VE(div, /)
ARTHMETIC_VE(mod, %)

#undef ARTHMETIC_VE

// TODO(cao) - merge with ARTHMETIC_VE since they are pretty much the same
#define COMPARE_VE(NAME, SIGN)                                                                           \
  template <typename T1, typename T2>                                                                    \
  std::unique_ptr<ValueEval> NAME(std::unique_ptr<ValueEval> v1, std::unique_ptr<ValueEval> v2) { \
    std::vector<std::unique_ptr<ValueEval>> branch;                                                      \
    branch.reserve(2);                                                                                   \
    branch.push_back(std::move(v1));                                                                     \
    branch.push_back(std::move(v2));                                                                     \
    return std::unique_ptr<ValueEval>(                                                                   \
      new TypeValueEval<bool>(OPT_LAMBDA({                                                               \
                                return children[0]->eval<T1>(row) SIGN children[1]->eval<T2>(row);       \
                              }),                                                                        \
                              std::move(branch)));                                                       \
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