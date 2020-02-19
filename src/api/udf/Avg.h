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

#include <atomic>

#include "surface/eval/UDF.h"

/**
 * Implement UDAF AVG, we will need both count and sum data maintained internally.
 * There are two ways to implement it 
 * 1) function level: maintain states inside the function itself. 
 * 2) execution level: modify schema by expanding avg(c) => {sum(c), count(c)} and do the division in global aggregation. 
 * 
 * The first approach is cleaner, but may need special handling to complicate UDF family.
 * The second approach requires execution engine to do special case handling.
 * 
 */
namespace nebula {
namespace api {
namespace udf {

template <typename T>
inline auto extract(const int128_t& v) ->
  typename std::enable_if_t<std::is_integral_v<T>, int64_t> {
  return nebula::common::high64<int64_t>(v);
}

template <typename T>
inline auto extract(const int128_t& v) ->
  typename std::enable_if_t<std::is_floating_point_v<T>, double> {
  return nebula::common::high64<double>(v);
}

// UDAF - avg
template <nebula::type::Kind NK,
          nebula::type::Kind IK = NK,
          typename BaseType = nebula::surface::eval::UDAF<NK, nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::AVG, NK>::Store, IK>>
class Avg : public BaseType {
  static constexpr int64_t INT64_ONE = 1;

public:
  using InputType = typename BaseType::InputType;
  using StoreType = typename BaseType::StoreType;
  using NativeType = typename BaseType::NativeType;

  Avg(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr)
    : BaseType(name,
               std::move(expr),
               // compute method
               [](InputType nv) -> StoreType {
                 StoreType ov = 0;
                 nebula::common::high64_add(ov, nv);
                 nebula::common::low64_add(ov, INT64_ONE);
                 return ov;
               },

               // merge method
               [](StoreType ov, StoreType nv) {
                 auto sum = extract<NativeType>(nv);
                 auto count = nebula::common::low64<int64_t>(nv);
                 // merge sum
                 nebula::common::high64_add(ov, sum);

                 // merge count
                 nebula::common::low64_add(ov, count);
                 return ov;
               },

               // finalize method
               [](const StoreType& v) -> NativeType {
                 auto count = nebula::common::low64<int64_t>(v);
                 if (count > 0) {
                   return extract<NativeType>(v) / count;
                 }
                 return 0;
               }) {}

  virtual ~Avg() = default;
};

template <>
Avg<nebula::type::Kind::INVALID>::Avg(
  const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr);

template <>
Avg<nebula::type::Kind::BOOLEAN>::Avg(
  const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr);

template <>
Avg<nebula::type::Kind::VARCHAR>::Avg(
  const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr);

template <>
Avg<nebula::type::Kind::INT128>::Avg(
  const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr);

} // namespace udf
} // namespace api
} // namespace nebula