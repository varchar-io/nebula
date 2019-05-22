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

#include "UDFFactory.h"

/**
 * Create UDF/UDAF object based on parameters
 */
namespace nebula {
namespace api {
namespace udf {

// FULL specialization is not template any more, the definition should come to cpp file
template <>
typename std::unique_ptr<nebula::execution::eval::UDF<TypeKind::BOOLEAN>>
  UDFFactory::createUDF<UDFKind::NOT, TypeKind::BOOLEAN>(std::shared_ptr<nebula::api::dsl::Expression> expr) {
  return std::make_unique<Not>(nebula::execution::eval::UdfTraits<UDFKind::NOT>::Name, expr->asEval());
}

} // namespace udf
} // namespace api
} // namespace nebula