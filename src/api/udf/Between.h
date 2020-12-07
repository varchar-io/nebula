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

#include "surface/eval/UDF.h"

namespace nebula {
namespace api {
namespace udf {
/**
 * This UDF defines if value to check is in given range inclusive. [low, high]
 * This UDF only works for number types.
 */
template <nebula::type::Kind IK>
class Between : public nebula::surface::eval::UDF<nebula::type::Kind::BOOLEAN, IK> {
  using UdfInBase = nebula::surface::eval::UDF<nebula::type::Kind::BOOLEAN, IK>;
  using InputType = typename nebula::type::TypeTraits<IK>::CppType;
  using EvalBlock = typename UdfInBase::EvalBlock;

public:
  Between(const std::string& name,
          std::shared_ptr<nebula::api::dsl::Expression> expr,
          InputType min,
          InputType max)
    : UdfInBase(
      name,
      expr->asEval(),
      // logic for "in []"
      [min, max](const std::optional<InputType>& source) -> std::optional<bool> {
        if (UNLIKELY(source == std::nullopt)) {
          return std::nullopt;
        }

        auto v = source.value();
        return v >= min && v <= max;
      },
      buildEvalBlock(expr, min, max)) {}

  virtual ~Between() = default;

private:
  static EvalBlock buildEvalBlock(std::shared_ptr<nebula::api::dsl::Expression> expr,
                                  InputType min,
                                  InputType max) {
    // only handle case "column in []"
    auto ve = expr->asEval();
    if (ve->expressionType() == nebula::surface::eval::ExpressionType::COLUMN) {
      // column expr signature is composed by "F:{col}"
      // const expr signature is compsoed by "C:{col}"
      std::string colName(ve->signature().substr(2));
      return [name = std::move(colName), min, max](const nebula::surface::eval::Block& b)
               -> nebula::surface::eval::BlockEval {
        auto A = nebula::surface::eval::BlockEval::ALL;
        auto N = nebula::surface::eval::BlockEval::NONE;

        // check partition values
        auto pv = b.partitionValues(name);
        // if no overlap, then the block can be skipped
        // if covered all values in partition values, then scan whole block
        if (pv.size() > 0) {
          size_t covered = 0;
          for (auto v : pv) {
            auto value = std::any_cast<InputType>(v);
            if (value >= min && value <= max) {
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

// check histogram  if the value range has no overlap with histogram [min, max]
// we can skip the block
#define DISPATCH_CASE(HT)                                 \
  auto histo = static_cast<const HT&>(b.histogram(name)); \
  if (histo.min() > max || min > histo.max()) {           \
    return N;                                             \
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