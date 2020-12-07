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
      [v](EvalContext&, const std::vector<std::unique_ptr<ValueEval>>&) -> std::optional<ST> { return v; },
      uncertain));
}

template <typename T>
std::unique_ptr<ValueEval> column(const std::string& name) {
  // this column name could be from custom result
  return std::unique_ptr<ValueEval>(
    new TypeValueEval<T>(
      fmt::format("F:{0}", name),
      ExpressionType::COLUMN,
      [name](EvalContext& ctx, const std::vector<std::unique_ptr<ValueEval>>&)
        -> std::optional<T> {
        // dedicate the read function to context as it knows how to handle special cases
        return ctx.read<T>(name);
      },
      uncertain));
}

// run script to compute custom value
// For now - script signature is its column name
template <typename T>
std::unique_ptr<ValueEval> custom(const std::string& name, const std::string& expr) {
  return std::unique_ptr<ValueEval>(
    new TypeValueEval<T>(
      name,
      ExpressionType::SCRIPT,
      [name, expr](EvalContext& ctx, const std::vector<std::unique_ptr<ValueEval>>&)
        -> std::optional<T> {
        // the first step is to evaluate the expression to ensure it works.
        // an example is like "var {name} = () => nebula.column('x') + 2;"
        // we don't need to evaluate this again and again - it should be cached
        auto decl = ctx.script().eval<bool>(expr, true);

        // only continue if current expression evaluated correctly
        // next let's invote it ot get the value we want
        if (decl) {
          return ctx.script().eval<T>(fmt::format("{0}();", name));
        }

        // return the default value of this type
        return std::nullopt;
      },
      uncertain));
}

// TODO(cao): optimization - fold constant nodes, we don't need keep a constant nodes
#define ARTHMETIC_VE(NAME, SIGN)                                                                  \
  template <typename T, typename T1, typename T2>                                                 \
  std::unique_ptr<ValueEval> NAME(std::unique_ptr<ValueEval> v1, std::unique_ptr<ValueEval> v2) { \
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
        OPT_LAMBDA(T, {                                                                           \
          auto v1 = children.at(0)->eval<T1>(ctx);                                                \
          if (UNLIKELY(v1 == std::nullopt)) {                                                     \
            return std::nullopt;                                                                  \
          }                                                                                       \
          auto v2 = children.at(1)->eval<T2>(ctx);                                                \
          if (UNLIKELY(v2 == std::nullopt)) {                                                     \
            return std::nullopt;                                                                  \
          }                                                                                       \
          return T(v1.value() SIGN v2.value());                                                   \
        }),                                                                                       \
        uncertain, {}, std::move(branch)));                                                       \
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
        OPT_LAMBDA(bool, {                                                                        \
          auto v1 = children.at(0)->eval<T1>(ctx);                                                \
          if (UNLIKELY(v1 == std::nullopt)) {                                                     \
            return std::nullopt;                                                                  \
          }                                                                                       \
          auto v2 = children.at(1)->eval<T2>(ctx);                                                \
          if (UNLIKELY(v2 == std::nullopt)) {                                                     \
            return std::nullopt;                                                                  \
          }                                                                                       \
          return v1.value() SIGN v2.value();                                                      \
        }),                                                                                       \
        std::move(eb), {}, std::move(branch)));                                                   \
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