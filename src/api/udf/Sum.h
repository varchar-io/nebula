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

#include "CommonUDAF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {

// UDAF - sum
template <nebula::type::Kind KIND>
class Sum : public CommonUDAF<KIND> {
public:
  using NativeType = typename CommonUDAF<KIND>::NativeType;
  Sum(const std::string& name, std::unique_ptr<nebula::execution::eval::ValueEval> expr)
    : CommonUDAF<KIND>(name,
                       std::move(expr),
                       [](NativeType ov, NativeType nv) {
                         return ov + nv;
                       }) {}
  virtual ~Sum() = default;
};

template <>
Sum<nebula::type::Kind::INVALID>::Sum(const std::string& name, std::unique_ptr<nebula::execution::eval::ValueEval> expr);

template <>
Sum<nebula::type::Kind::BOOLEAN>::Sum(const std::string& name, std::unique_ptr<nebula::execution::eval::ValueEval> expr);

template <>
Sum<nebula::type::Kind::VARCHAR>::Sum(const std::string& name, std::unique_ptr<nebula::execution::eval::ValueEval> expr);

template <>
Sum<nebula::type::Kind::INT128>::Sum(const std::string& name, std::unique_ptr<nebula::execution::eval::ValueEval> expr);

} // namespace udf
} // namespace api
} // namespace nebula