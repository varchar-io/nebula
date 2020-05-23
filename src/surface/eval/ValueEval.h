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

#include "Aggregator.h"
#include "EvalContext.h"
#include "Script.h"

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

// define a global type to represent runtime fields in schema
using Fields = std::vector<std::unique_ptr<ValueEval>>;

///////////////////////////////////////////////////////////////////////////////////////////////////
// NOTE - in GCC 7, even it is template, we can't define a static method in header
// otherwise the GCC linker will treat it as deleted function during link time
// And we will see lots of issues like "xxx defined in discarded section"
template <typename T>
std::unique_ptr<ValueEval> constant(T v) {
  // get standard type for this constant after removing reference and const decors
  using ST = typename nebula::type::TypeDetect<std::remove_reference_t<std::remove_cv_t<T>>>::StandardType;

  std::string sign;
  if constexpr (std::is_same_v<ST, int128_t>) {
    sign = fmt::format("C:{0}", nebula::common::Int128_U::to_string(v));
  } else {
    sign = fmt::format("C:{0}", v);
  }

  return std::unique_ptr<ValueEval>(
    new TypeValueEval<ST>(
      sign,
      ExpressionType::CONSTANT,
      [v](EvalContext&, const std::vector<std::unique_ptr<ValueEval>>&, bool&) -> ST { return v; },
      uncertain));
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
      ExpressionType::COLUMN,
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
      },
      uncertain));
}

#undef NULL_CHECK

// run script to compute custom value
template <typename T>
std::unique_ptr<ValueEval> custom(const std::string& name, const std::string& expr) {
  return std::unique_ptr<ValueEval>(
    new TypeValueEval<T>(
      fmt::format("S:{0}", name),
      ExpressionType::SCRIPT,
      [name, expr](EvalContext& ctx, const std::vector<std::unique_ptr<ValueEval>>&, bool& valid)
        -> T {
        // the first step is to evaluate the expression to ensure it works.
        // an example is like "var {name} = () => nebula.column('x') + 2;"
        // we don't need to evaluate this again and again - it should be cached
        ctx.script().eval<bool>(expr, valid, true);

        // only continue if current expression evaluated correctly
        // next let's invote it ot get the value we want
        if (valid) {
          return ctx.script().eval<T>(fmt::format("{0}();", name), valid);
        }

        return T(0);
      },
      uncertain));
}

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
        ExpressionType::ARTHMETIC,                                                                \
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
        uncertain,                                                                                \
        {},                                                                                       \
        std::move(branch)));                                                                      \
  }

ARTHMETIC_VE(add, +)
ARTHMETIC_VE(sub, -)
ARTHMETIC_VE(mul, *)
ARTHMETIC_VE(div, /)
ARTHMETIC_VE(mod, %)

#undef ARTHMETIC_VE

// build block evaluation function based on left and right expression connected with logical op
template <LogicalOp op>
EvalBlock buildEvalBlock(const std::unique_ptr<ValueEval>&, const std::unique_ptr<ValueEval>&) {
  return uncertain;
}

// build eval block function for each logical operation
#define BEB_LOGICAL(LOP) \
  template <>            \
  EvalBlock buildEvalBlock<LogicalOp::LOP>(const std::unique_ptr<ValueEval>&, const std::unique_ptr<ValueEval>&);

BEB_LOGICAL(GT)
BEB_LOGICAL(GE)
BEB_LOGICAL(EQ)
BEB_LOGICAL(NEQ)
BEB_LOGICAL(LT)
BEB_LOGICAL(LE)
BEB_LOGICAL(AND)
BEB_LOGICAL(OR)

#undef BEB_LOGICAL

// TODO(cao) - merge with ARTHMETIC_VE since they are pretty much the same
// WHEN logical operation meets NULL (valid==false), return false and indicate valid as false
#define COMPARE_VE(NAME, SIGN, LOP)                                                               \
  template <typename T1, typename T2>                                                             \
  std::unique_ptr<ValueEval> NAME(std::unique_ptr<ValueEval> v1, std::unique_ptr<ValueEval> v2) { \
    const auto s1 = v1->signature();                                                              \
    const auto s2 = v2->signature();                                                              \
    auto eb = buildEvalBlock<LOP>(v1, v2);                                                        \
    std::vector<std::unique_ptr<ValueEval>> branch;                                               \
    branch.reserve(2);                                                                            \
    branch.push_back(std::move(v1));                                                              \
    branch.push_back(std::move(v2));                                                              \
    return std::unique_ptr<ValueEval>(                                                            \
      new TypeValueEval<bool>(                                                                    \
        fmt::format("({0}{1}{2})", s1, #SIGN, s2),                                                \
        ExpressionType::LOGICAL,                                                                  \
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
        std::move(eb),                                                                            \
        {},                                                                                       \
        std::move(branch)));                                                                      \
  }

COMPARE_VE(gt, >, LogicalOp::GT)
COMPARE_VE(ge, >=, LogicalOp::GE)
COMPARE_VE(eq, ==, LogicalOp::EQ)
COMPARE_VE(neq, !=, LogicalOp::NEQ)
COMPARE_VE(lt, <, LogicalOp::LT)
COMPARE_VE(le, <=, LogicalOp::LE)

// unncessary compare but same signatures
COMPARE_VE(band, &&, LogicalOp::AND)
COMPARE_VE(bor, ||, LogicalOp::OR)

#undef COMPARE_VE

#undef OPT_LAMBDA
#undef OPT
#undef SketchMaker
#undef EvalBlock

} // namespace eval
} // namespace surface
} // namespace nebula