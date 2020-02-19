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

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {

// UDAF - count, the input is the constant 1 which is an integer value always
template <nebula::type::Kind NK,
          nebula::type::Kind IK = nebula::type::Kind::INTEGER,
          typename BaseType = nebula::surface::eval::UDAF<NK, nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::COUNT, NK>::Store, IK>>
class Count : public BaseType {
public:
  using StoreType = typename BaseType::StoreType;
  // for count, we don't need evaluate inner expr
  // unless it's distinct a column, so we can safely replace it with a const expression with value 0
  Count(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval>)
    : BaseType(name,
               nebula::surface::eval::constant(1), // partial aggregate - sum each count results
               [](StoreType ov, StoreType nv) {
                 return ov + nv;
               }) {}
  virtual ~Count() = default;
};

template <>
Count<nebula::type::Kind::VARCHAR, nebula::type::Kind::INTEGER>::Count(
  const std::string&, std::unique_ptr<nebula::surface::eval::ValueEval>);

} // namespace udf
} // namespace api
} // namespace nebula