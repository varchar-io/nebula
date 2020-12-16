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

#include "Avg.h"
#include "Between.h"
#include "Cardinality.h"
#include "Count.h"
#include "In.h"
#include "Like.h"
#include "Max.h"
#include "Min.h"
#include "Not.h"
#include "Pct.h"
#include "Prefix.h"
#include "Sum.h"
#include "Tpm.h"
#include "Histogram.h"
#include "api/dsl/Base.h"
#include "surface/eval/UDF.h"
#include "type/Type.h"

/**
 * Create UDF/UDAF object based on parameters
 */
namespace nebula {
namespace api {
namespace udf {

using UDFKind = nebula::surface::eval::UDFType;

class UDFFactory {
public:
  template <UDFKind UKIND, nebula::type::Kind IK, typename... Args>
  static std::unique_ptr<nebula::surface::eval::ValueEval>
    createUDF(std::shared_ptr<nebula::api::dsl::Expression> expr, Args&&... args) {

    constexpr auto name = nebula::surface::eval::UdfTraits<UKIND, IK>::Name;

    if constexpr (UKIND == UDFKind::NOT) {
      return std::make_unique<Not>(name, expr->asEval());
    }

    if constexpr (UKIND == UDFKind::MAX) {
      return std::make_unique<Max<IK>>(name, expr->asEval());
    }

    if constexpr (UKIND == UDFKind::MIN) {
      return std::make_unique<Min<IK>>(name, expr->asEval());
    }

    if constexpr (UKIND == UDFKind::COUNT) {
      return std::make_unique<Count<IK>>(name, expr->asEval());
    }

    if constexpr (UKIND == UDFKind::SUM) {
      return std::make_unique<Sum<IK>>(name, expr->asEval());
    }

    if constexpr (UKIND == UDFKind::AVG) {
      return std::make_unique<Avg<IK>>(name, expr->asEval());
    }

    if constexpr (UKIND == UDFKind::PCT) {
      return std::make_unique<Pct<IK>>(name, expr->asEval(), std::forward<Args>(args)...);
    }

    if constexpr (UKIND == UDFKind::TPM) {
      return std::make_unique<Tpm<IK>>(name, expr->asEval(), std::forward<Args>(args)...);
    }

    if constexpr (UKIND == UDFKind::CARD) {
      return std::make_unique<Cardinality<IK>>(name, expr->asEval(), std::forward<Args>(args)...);
    }

    if constexpr (UKIND == UDFKind::LIKE) {
      return std::make_unique<Like>(name, expr->asEval(), std::forward<Args>(args)...);
    }

    if constexpr (UKIND == UDFKind::PREFIX) {
      return std::make_unique<Prefix>(name, expr->asEval(), std::forward<Args>(args)...);
    }

    if constexpr (UKIND == UDFKind::IN) {
      return std::make_unique<In<IK>>(name, expr, std::forward<Args>(args)...);
    }

    if constexpr (UKIND == UDFKind::BETWEEN) {
      return std::make_unique<Between<IK>>(name, expr, std::forward<Args>(args)...);
    }

    if constexpr (UKIND == UDFKind::HIST) {
      return std::make_unique<Hist<IK>>(name, expr->asEval(), std::forward<Args>(args)...);
    }

    throw NException(fmt::format("Unimplemented UDF {0}", name));
  }
};

} // namespace udf
} // namespace api
} // namespace nebula