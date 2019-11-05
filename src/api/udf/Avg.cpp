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

#include "Avg.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {
using nebula::type::Kind;

#define DO_NOT_SUPPORT(K)                                                                              \
  template <>                                                                                          \
  Avg<Kind::K>::Avg(const std::string& name, std::unique_ptr<nebula::execution::eval::ValueEval> expr) \
    : Avg<Kind::K>::BaseType(                                                                          \
        name,                                                                                          \
        std::move(expr),                                                                               \
        [](NativeType) -> StoreType { throw NException("not supported"); },                            \
        {},                                                                                            \
        {}) {}

DO_NOT_SUPPORT(INVALID)
DO_NOT_SUPPORT(BOOLEAN)
DO_NOT_SUPPORT(VARCHAR)
DO_NOT_SUPPORT(INT128)

#undef DO_NOT_SUPPORT

} // namespace udf
} // namespace api
} // namespace nebula