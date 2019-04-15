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

#include "Max.h"
#include "Min.h"
#include "api/dsl/Expressions.h"
#include "execution/eval/UDF.h"
#include "execution/eval/ValueEval.h"
#include "type/Type.h"

/**
 * Create UDF/UDAF object based on parameters
 */
namespace nebula {
namespace api {
namespace udf {

class UDFFactory {
public:
  template <nebula::type::Kind KIND>
  static typename std::unique_ptr<nebula::execution::eval::UDF<KIND>>
    createUDAF(nebula::execution::eval::UDAF_REG reg, std::shared_ptr<nebula::api::dsl::Expression> expr) {
    switch (reg) {
    case nebula::execution::eval::UDAF_REG::MAX: {
      return std::unique_ptr<nebula::execution::eval::UDF<KIND>>(new Max<KIND>(expr));
    }
    case nebula::execution::eval::UDAF_REG::MIN: {
      return std::unique_ptr<nebula::execution::eval::UDF<KIND>>(new Min<KIND>(expr));
    }
    default:
      throw NException("UDAF is not registered");
    }
  }
};

} // namespace udf
} // namespace api
} // namespace nebula