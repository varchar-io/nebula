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

#include <glog/logging.h>
#include "CommonUDF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {

// This UDF is doing the pattern match
// Not sure if this is standard SQL like spec
// It only accepts % as pattern matcher
// when pattern see %, treat it as macro, no escape support here.
bool match(const char* sp, const size_t ss, size_t si,
           const char* pp, const size_t ps, size_t pi);

using UdfLikeBase = CommonUDF<nebula::type::Kind::BOOLEAN, nebula::type::Kind::VARCHAR>;
class Like : public UdfLikeBase {
public:
  Like(std::shared_ptr<nebula::api::dsl::Expression> expr, const std::string& pattern)
    : UdfLikeBase(expr, [pattern](const ExprType& source) -> ReturnType {
        return match(source.data(), source.size(), 0, pattern.data(), pattern.size(), 0);
      }) {}
  virtual ~Like() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula