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

#include <fmt/format.h>

#include "surface/eval/UDF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {

// char equals
static inline bool eq(char a, char b) {
  return a == b;
}

// char equlas ignoring case
static inline bool ieq(char a, char b) {
  return a == b || std::tolower(a) == std::tolower(b);
}

// This UDF is doing the pattern match
// Not sure if this is standard SQL like spec
// It only accepts % as pattern matcher
// when pattern see %, treat it as macro, no escape support here.
bool match(const char* sp, const size_t ss, size_t si,
           const char* pp, const size_t ps, size_t pi,
           bool caseSensitive = true);

using UdfLikeBase = nebula::surface::eval::UDF<nebula::type::Kind::BOOLEAN, nebula::type::Kind::VARCHAR>;
class Like : public UdfLikeBase {
public:
  Like(const std::string& name,
       std::unique_ptr<nebula::surface::eval::ValueEval> expr,
       const std::string& pattern,
       bool caseSensitive = true,
       bool unlike = false)
    : UdfLikeBase(
      name,
      std::move(expr),
      [pattern, caseSensitive, unlike](const std::optional<InputType>& source)
        -> std::optional<NativeType> {
        if (N_UNLIKELY(source == std::nullopt)) {
          return std::nullopt;
        }

        auto v = source.value();
        auto m = match(v.data(), v.size(), 0,
                       pattern.data(), pattern.size(), 0,
                       caseSensitive);
        return unlike ? !m : m;
      }) {}
  virtual ~Like() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula