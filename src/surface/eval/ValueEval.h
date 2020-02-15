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
#include <unordered_map>

#include "common/Cursor.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "meta/NNode.h"
#include "surface/DataSurface.h"

/**
 * Execution on each expression to get final value.
 */
namespace nebula {
namespace surface {
namespace eval {
class EvalContext;

template <typename T, typename I = T>
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
  inline T eval(EvalContext& ctx, bool& valid) const {
    // TODO(cao) - there is a problem wasted me a WHOLE day to figure out the root cause.
    // So document here for further robust engineering work, the case is like this:
    // When it create ValueEval, it uses template to generate TypeValueEval<std::string> for VARCHAR type
    // However, there is some mismatch to cause this function call to be eval<char*>(row)
    // obviously, below static_cast will give us a corrupted object since the concrete types mismatch.
    // This is a hard problem if we don't pay attention and waste lots of time to debug it, so how can we prevent similar problem
    // 1. we should do stronger type check to ensure the type used is consistent everywhere.
    // 2. we can enforce std::enable_if more on the template type to ensure the function is called in the expected "type"
    // N_ENSURE_NOT_NULL(p, "type should match");
    auto p = static_cast<const TypeValueEval<T>*>(this);
    // N_ENSURE_NOT_NULL(p, "Type should match in value eval!");
    return p->eval(ctx, valid);
  }

  // merge t2 into t1
  template <typename T>
  inline T merge(T t1, T t2) const {
    auto p = static_cast<const TypeValueEval<T>*>(this);
    return p->merge(t1, t2);
  }

  // stack value into object
  template <typename T, typename I>
  inline T stack(T o, I v) const {
    auto p = static_cast<const TypeValueEval<T, I>*>(this);
    return p->stack(o, v);
  }

  // identify a unique value evaluation object in given query context
  // TODO(cao) - consider using number instead for fast hashing
  inline const std::string_view signature() const {
    return sign_;
  }

protected:
  std::string sign_;
};

// define a global type to represent runtime fields in schema
using Fields = std::vector<std::unique_ptr<ValueEval>>;

class EvalContext {
public:
  EvalContext(bool cache = false) : cache_{ cache }, slice_{ 1024 } {
    cursor_ = 1;
  }
  virtual ~EvalContext() = default;

  // change reference to row data, clear all cache.
  // and start build cache based on evaluation signautre.
  void reset(const nebula::surface::RowData&);

  // evaluate a value eval object in current context and return value reference.
  template <typename T>
  T eval(const ValueEval& ve, bool& valid) {
    if (LIKELY(!cache_)) {
      return ve.eval<T>(*this, valid);
    }

    const auto& sign = ve.signature();

    // if in evaluated list
    auto itr = map_.find(sign);
    if (itr != map_.end()) {
      auto offset = itr->second.first;
      valid = offset > 0;
      if (!valid) {
        return nebula::type::TypeDetect<T>::value;
      }

      return slice_.read<T>(offset);
    }

    N_ENSURE_NOT_NULL(row_, "reference a row object before evaluation.");
    const auto value = ve.eval<T>(*this, valid);
    if (!valid) {
      map_[sign] = { 0, 0 };
      return nebula::type::TypeDetect<T>::value;
    }
    const auto offset = cursor_;
    map_[sign] = { offset, 0 };
    cursor_ += slice_.write<T>(cursor_, value);

    return slice_.read<T>(offset);
  }

  inline const nebula::surface::RowData& row() const {
    return *row_;
  }

private:
  const bool cache_;
  const nebula::surface::RowData* row_;
  // a signature keyed tuples indicating if this expr evaluated (having entry) or not.
  std::unordered_map<std::string_view, std::pair<size_t, size_t>> map_;
  // layout all cached data, when reset, just move the cursor to 1
  // 0 is reserved, any evaluated data points to 0 if it is NULL
  size_t cursor_;
  nebula::common::PagedSlice slice_;
};

template <>
std::string_view EvalContext::eval(const ValueEval& ve, bool& valid);

// two utilities
#define StackFunction std::function<T(T, I)>
#define MergeFunction std::function<T(T, T)>
#define OPT std::function<T(EvalContext&, const std::vector<std::unique_ptr<ValueEval>>&, bool&)>
#define OPT_LAMBDA(X)                                                                           \
  ([](EvalContext& ctx, const std::vector<std::unique_ptr<ValueEval>>& children, bool& valid) { \
    X                                                                                           \
  })

template <typename T, typename I>
class TypeValueEval : public ValueEval {
public:
  TypeValueEval(const std::string& sign, const OPT&& op)
    : TypeValueEval(sign, std::move(op), {}, {}, {}) {}

  TypeValueEval(
    const std::string& sign,
    const OPT&& op,
    const StackFunction&& st,
    const MergeFunction&& mt,
    std::vector<std::unique_ptr<ValueEval>> children)
    : ValueEval(sign),
      op_{ std::move(op) },
      st_{ std::move(st) },
      mt_{ std::move(mt) },
      children_{ std::move(children) } {}

  virtual ~TypeValueEval() = default;

  inline T eval(EvalContext& ctx, bool& valid) const {
    return op_(ctx, this->children_, valid);
  }

  inline T merge(T t1, T t2) const {
    return mt_(t1, t2);
  }

  inline T stack(T o, I v) const {
    return st_(o, v);
  }

private:
  OPT op_;
  StackFunction st_;
  MergeFunction mt_;
  std::vector<std::unique_ptr<ValueEval>> children_;
};

///////////////////////////////////////////////////////////////////////////////////////////////////
// NOTE - in GCC 7, even it is template, we can't define a static method in header
// otherwise the GCC linker will treat it as deleted function during link time
// And we will see lots of issues like "xxx defined in discarded section"
template <typename T>
std::unique_ptr<ValueEval> constant(T v) {
  // get standard type for this constant after removing reference and const decors
  using ST = typename nebula::type::TypeDetect<std::remove_reference_t<std::remove_cv_t<T>>>::StandardType;
  return std::unique_ptr<ValueEval>(
    new TypeValueEval<ST>(
      fmt::format("C:{0}", v),
      [v](EvalContext&, const std::vector<std::unique_ptr<ValueEval>>&, bool&) -> ST {
        return v;
      }));
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
      [name](EvalContext& ctx, const std::vector<std::unique_ptr<ValueEval>>&, bool& valid)
        -> T {
        // This is the only place we need row object
        const auto& row = ctx.row();

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

        if constexpr (std::is_same<T, int128_t>::value) {
          NULL_CHECK(0)
          return row.readInt128(name);
        }

        if constexpr (std::is_same<T, std::string_view>::value) {
          NULL_CHECK("")
          return row.readString(name);
        }

        // TODO(cao): other types supported in DSL? for example: UDF on list or map
        throw NException("not supported template type");
      }));
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
        fmt::format("({0}{1}{2})", s1, #SIGN, s2),                                                \
        OPT_LAMBDA({                                                                              \
          auto v1 = ctx.eval<T1>(*children[0], valid);                                            \
          if (UNLIKELY(!valid)) {                                                                 \
            return INVALID;                                                                       \
          }                                                                                       \
          auto v2 = ctx.eval<T2>(*children[1], valid);                                            \
          if (UNLIKELY(!valid)) {                                                                 \
            return INVALID;                                                                       \
          }                                                                                       \
          return T(v1 SIGN v2);                                                                   \
        }),                                                                                       \
        {},                                                                                       \
        {},                                                                                       \
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
        fmt::format("({0}{1}{2})", s1, #SIGN, s2),                                                \
        OPT_LAMBDA({                                                                              \
          auto v1 = ctx.eval<T1>(*children.at(0), valid);                                         \
          if (UNLIKELY(!valid)) {                                                                 \
            return false;                                                                         \
          }                                                                                       \
          auto v2 = ctx.eval<T2>(*children.at(1), valid);                                         \
          if (UNLIKELY(!valid)) {                                                                 \
            return false;                                                                         \
          }                                                                                       \
          return v1 SIGN v2;                                                                      \
        }),                                                                                       \
        {},                                                                                       \
        {},                                                                                       \
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

#undef OPT_LAMBDA
#undef OPT
#undef MergeFunction
#undef StackFunction

} // namespace eval
} // namespace surface
} // namespace nebula