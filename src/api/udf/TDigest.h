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
#include <folly/stats/TDigest.h>

#include "surface/eval/UDF.h"

/**
 * Implement UDAF TDigest to get quantiles of target values
 * Internally this UDAF maintains a sketch binary stream with all numbers in double type.
 */
namespace nebula {
namespace api {
namespace udf {

// UDAF - TDigest
template <nebula::type::Kind IK,
          typename Traits = nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::TDIGEST, IK>,
          typename BaseType = nebula::surface::eval::UDAF<Traits::Type, Traits::Store, IK>>
class TDigest : public BaseType {
  // most commonly for percentiles
  static constexpr size_t DIGEST_SIZE = 100;

public:
  using InputType = typename BaseType::InputType;
  using NativeType = typename BaseType::NativeType;
  using StoreType = typename BaseType::StoreType;

  TDigest(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr)
    : BaseType(name,
               std::move(expr),
               // compute method
               [](InputType nv) -> StoreType {
                 // we can not pay serde cost for all values to be integrated
                 // we need to get object out of a memory chunk
                 auto obj = new folly::TDigest(DIGEST_SIZE);
                 obj->merge({ nv });
                 return obj;
               },

               // stack method
               {},

               // merge method
               [](StoreType ov, StoreType nv) {
                 auto another = static_cast<const folly::TDigest*>(nv);
                 static_cast<folly::TDigest*>(ov)->merge({ another });

                 // delete another object
                 delete another;
                 return ov;
               },

               // finalize method
               [](const StoreType& v) -> NativeType {
                 // convert the v;
                 auto x = static_cast<folly::TDigest*>(v);
                 auto result = fmt::format("sum: {0}", x->sum());

                 // TODO(cao): if finalize method is not called, the object is leaked
                 // somehow we need some smart pointer rather than (void*)
                 delete x;
                 return result;
               }) {}

  virtual ~TDigest() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula