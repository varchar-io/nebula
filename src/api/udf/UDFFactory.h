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

#include "Count.h"
#include "Like.h"
#include "Max.h"
#include "Min.h"
#include "MyUdf.h"
#include "Prefix.h"
#include "Sum.h"
#include "api/dsl/Base.h"
#include "execution/eval/UDF.h"
#include "type/Type.h"

/**
 * Create UDF/UDAF object based on parameters
 */
namespace nebula {
namespace api {
namespace udf {

using UDFKind = nebula::execution::eval::UDFType;
using TypeKind = nebula::type::Kind;

class UDFFactory {
public:
  template <UDFKind UKIND, TypeKind KIND, typename... Args>
  static typename std::unique_ptr<nebula::execution::eval::UDF<KIND>>
    createUDF(std::shared_ptr<nebula::api::dsl::Expression> expr, Args&&... args) {
    constexpr auto name = nebula::execution::eval::UdfTraits<UKIND>::Name;

    if constexpr (UKIND == UDFKind::MAX) {
      return std::make_unique<Max<KIND>>(name, expr->asEval());
    }

    if constexpr (UKIND == UDFKind::MIN) {
      return std::make_unique<Min<KIND>>(name, expr->asEval());
    }

    if constexpr (UKIND == UDFKind::COUNT) {
      return std::make_unique<Count<KIND>>(name, expr->asEval());
    }

    if constexpr (UKIND == UDFKind::SUM) {
      return std::make_unique<Sum<KIND>>(name, expr->asEval());
    }

    if constexpr (UKIND == UDFKind::LIKE) {
      return std::make_unique<Like>(name, expr->asEval(), std::forward<Args>(args)...);
    }

    if constexpr (UKIND == UDFKind::PREFIX) {
      return std::make_unique<Prefix>(name, expr->asEval(), std::forward<Args>(args)...);
    }

    throw NException("Unimplemented UDF");
  }
};

template <>
typename std::unique_ptr<nebula::execution::eval::UDF<TypeKind::BOOLEAN>>
  UDFFactory::createUDF<UDFKind::NOT, TypeKind::BOOLEAN>(std::shared_ptr<nebula::api::dsl::Expression>);

} // namespace udf
} // namespace api
} // namespace nebula