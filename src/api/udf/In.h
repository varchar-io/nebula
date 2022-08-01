/*
 * Copyright 2017-present varchar.io
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

#include "surface/eval/UDF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {
/**
 * This UDF provides logic operations to determine if a value is in given set.
 */
template <nebula::type::Kind IK>
class In : public nebula::surface::eval::UDF<nebula::type::Kind::BOOLEAN, IK> {
  using UdfInBase = nebula::surface::eval::UDF<nebula::type::Kind::BOOLEAN, IK>;
  using InputType = typename nebula::type::TypeTraits<IK>::CppType;
  using EvalBlock = typename UdfInBase::EvalBlock;
  using SetType = typename std::shared_ptr<nebula::common::unordered_set<InputType>>;
  using ValueType = typename std::conditional<
    IK == nebula::type::Kind::VARCHAR,
    std::string,
    typename nebula::type::TypeTraits<IK>::CppType>::type;

public:
  In(const std::string& name,
     std::shared_ptr<nebula::api::dsl::Expression> expr,
     SetType values)
    : UdfInBase(
      name,
      expr->asEval(),
      // logic for "in []"
      [values](const std::optional<InputType>& source) -> std::optional<bool> {
        if (N_UNLIKELY(source == std::nullopt)) {
          return std::nullopt;
        }
        return values->find(source.value()) != values->end();
      },
      buildEvalBlock(expr, values, true)) {}

  In(const std::string& name,
     std::shared_ptr<nebula::api::dsl::Expression> expr,
     SetType values,
     bool in)
    : UdfInBase(
      name,
      expr->asEval(),
      // logic for "not in []"
      [values](const std::optional<InputType>& source) -> std::optional<bool> {
        if (N_UNLIKELY(source == std::nullopt)) {
          return std::nullopt;
        }
        return values->find(source.value()) == values->end();
      },
      buildEvalBlock(expr, values, false)) {
    N_ENSURE(!in, "this constructor is designed for NOT IN clauase");
  }

  virtual ~In() = default;

private:
  static EvalBlock buildEvalBlock(std::shared_ptr<nebula::api::dsl::Expression> expr,
                                  SetType values,
                                  bool in) {
    // only handle case "column in []"
    auto ve = expr->asEval();
    if (ve->expressionType() == nebula::surface::eval::ExpressionType::COLUMN) {
      // column expr signature is composed by "F:{col}"
      // const expr signature is compsoed by "C:{col}"
      std::string colName(ve->signature().substr(2));
      return [name = std::move(colName), values, in](const nebula::surface::eval::Block& b)
               -> nebula::surface::eval::BlockEval {
        auto A = in ? nebula::surface::eval::BlockEval::ALL : nebula::surface::eval::BlockEval::NONE;
        auto N = in ? nebula::surface::eval::BlockEval::NONE : nebula::surface::eval::BlockEval::ALL;

        // check partition values
        auto pv = b.partitionValues(name);
        // if no overlap, then the block can be skipped
        // if covered all values in partition values, then scan whole block
        if (pv.size() > 0) {
          size_t covered = 0;
          for (auto v : pv) {
            auto valueType = std::any_cast<ValueType>(v);
            InputType ev(valueType);
            if (values->find(ev) != values->end()) {
              covered++;
            }
          }

          // no overlap - skip the
          if (covered == 0) {
            return N;
          }

          // covered all values
          if (covered == pv.size()) {
            return A;
          }
        }

        // check bloom filter
        // if none of the values has possibility
        bool possible = false;
        for (const auto& v : *values) {
          possible = possible || b.probably(name, v);
          if (possible) {
            break;
          }
        }

        // if all false - no possiblity
        if (!possible) {
          return N;
        }

// check histogram  if the value range has no overlap with histogram [min, max]
// we can skip the block
#define DISPATCH_CASE(HT)                                                      \
  const auto [min, max] = std::minmax_element(values->begin(), values->end()); \
  auto histo = std::dynamic_pointer_cast<HT>(b.histogram(name));               \
  if (histo->min() > *max || *min > histo->max()) {                            \
    return N;                                                                  \
  }

        // only enable for scalar types
        if constexpr (nebula::type::TypeBase::isScalar(IK)) {
          switch (IK) {
          case nebula::type::Kind::TINYINT:
          case nebula::type::Kind::SMALLINT:
          case nebula::type::Kind::INTEGER:
          case nebula::type::Kind::BIGINT: {
            DISPATCH_CASE(nebula::surface::eval::IntHistogram)
            break;
          }
          case nebula::type::Kind::REAL:
          case nebula::type::Kind::DOUBLE: {
            DISPATCH_CASE(nebula::surface::eval::RealHistogram)
            break;
          }
          default: break;
          }
        }

#undef DISPATCH_CASE

        // by default - let's do row scan
        return nebula::surface::eval::BlockEval::PARTIAL;
      };
    }

    return nebula::surface::eval::uncertain;
  }
};

} // namespace udf
} // namespace api
} // namespace nebula