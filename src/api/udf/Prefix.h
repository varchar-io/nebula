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

#include "surface/eval/UDF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {

// This UDF similar to LIKE which does simple regular expression match
// Prefix is specifically for prefix match, special LIKE expression can be converted to prefix match
// such as "<anything>%"
// when pattern see %, treat it as macro, no escape support here.
using UdfPrefixBase = nebula::surface::eval::UDF<nebula::type::Kind::BOOLEAN, nebula::type::Kind::VARCHAR>;
class Prefix : public UdfPrefixBase {
public:
  Prefix(
    const std::string& name,
    std::unique_ptr<nebula::surface::eval::ValueEval> expr,
    const std::string& prefix,
    bool caseSensitive = true)
    : UdfPrefixBase(
        name,
        std::move(expr),
        [prefix, caseSensitive](const InputType& source, bool& valid) -> NativeType {
          if (valid) {
            const auto prefixSize = prefix.size();
            if (source.size() < prefixSize) {
              return false;
            }

            bool (*eqf)(char a, char b) = caseSensitive ? &eq : &ieq;
            for (size_t i = 0; i < prefixSize; ++i) {
              if (!(*eqf)(source.at(i), prefix.at(i))) {
                return false;
              }
            }

            return true;
          }

          return false;
        }) {}
  virtual ~Prefix() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula