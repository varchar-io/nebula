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

#include "common/Cursor.h"
#include "glog/logging.h"
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

// a pure interface to evalue value based on type kind
template <nebula::type::Kind KIND>
class KindEval {
public:
  using NativeType = typename nebula::type::TypeTraits<KIND>::CppType;
  virtual ~KindEval() = default;
  virtual NativeType eval(const nebula::surface::RowData&) = 0;
};

// this is a tree, with each node to be either macro/value or operator
// this is translated from expression.
struct ValueEval {
  ValueEval(std::vector<std::unique_ptr<ValueEval>> children) : children_{ std::move(children) } {}
  virtual ~ValueEval() = default;

  template <typename T>
  T eval(const nebula::surface::RowData& row) {
    return static_cast<TypeValueEval<T>*>(this)->eval(row, children_);
  }

  std::vector<std::unique_ptr<ValueEval>> children_;
};

// two utilities
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
    : op_{ op }, ValueEval(std::move(children)) {}

  virtual ~TypeValueEval() = default;

  inline T eval(const nebula::surface::RowData& row, const std::vector<std::unique_ptr<ValueEval>>& children) {
    return op_(row, children);
  }

  OPT op_;
};

///////////////////////////////////////////////////////////////////////////////////////////////////

template <typename T>
static std::unique_ptr<ValueEval> constant(T v) {
  return std::unique_ptr<ValueEval>(
    new TypeValueEval<T>(
      [v](const nebula::surface::RowData& row, const std::vector<std::unique_ptr<ValueEval>>& children) -> T {
        return v;
      },
      {}));
}

template <typename T>
static std::unique_ptr<ValueEval> column(const std::string& name) {
  return std::unique_ptr<ValueEval>(
    new TypeValueEval<T>(
      [name](const nebula::surface::RowData& row, const std::vector<std::unique_ptr<ValueEval>>& children) -> T {
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
  static std::unique_ptr<ValueEval> NAME(std::unique_ptr<ValueEval> v1, std::unique_ptr<ValueEval> v2) { \
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
                             LOG(INFO) << "v1=" << v1 << ", v2=" << v2 << ", v3=" << v3;                 \
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
  static std::unique_ptr<ValueEval> NAME(std::unique_ptr<ValueEval> v1, std::unique_ptr<ValueEval> v2) { \
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
COMPARE_VE(lt, <)
COMPARE_VE(le, <=)

// unncessary compare but same signatures
COMPARE_VE(band, &&)
COMPARE_VE(bor, ||)

#undef COMPARE_VE

template <nebula::type::Kind KIND>
static std::unique_ptr<ValueEval> udf(std::shared_ptr<KindEval<KIND>> udf) {
  return std::unique_ptr<ValueEval>(
    new TypeValueEval<typename KindEval<KIND>::NativeType>(
      [udf](const nebula::surface::RowData& row, const std::vector<std::unique_ptr<ValueEval>>& children) -> decltype(auto) {
        // call the UDF to evalue the result
        return udf->eval(row);
      },
      {}));
}

} // namespace eval
} // namespace execution
} // namespace nebula