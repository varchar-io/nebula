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
#include "ValueEval.h"
#include "surface/DataSurface.h"
#include "type/Type.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace execution {
namespace eval {

#define TYPE_VALUE_EVAL_KIND nebula::execution::eval::TypeValueEval<typename nebula::type::TypeTraits<KIND>::CppType>

// define an UDF interface
template <nebula::type::Kind KIND>
class UDF : public TYPE_VALUE_EVAL_KIND {
public:
  UDF() : TYPE_VALUE_EVAL_KIND(
            [this](const nebula::surface::RowData& row, const std::vector<std::unique_ptr<ValueEval>>&) -> decltype(auto) {
              // call the UDF to evalue the result
              return this->run(row);
            },
            {}) {}
  virtual ~UDF() = default;

  // columns referenced
  virtual std::vector<std::string> columns() const = 0;

  virtual typename nebula::type::TypeTraits<KIND>::CppType run(const nebula::surface::RowData&) const = 0;
};

// UDAF is a state ful object
template <nebula::type::Kind KIND>
class UDAF : public UDF<KIND> {
public:
  using NativeType = typename nebula::type::TypeTraits<KIND>::CppType;
  using AggFunction = std::function<NativeType(NativeType, NativeType)>;
  UDAF(AggFunction&& agg) : agg_{ std::move(agg) } {}
  virtual ~UDAF() = default;

  // partial aggregate
  virtual void partial(const nebula::surface::RowData&) = 0;

  // global aggregate
  virtual void global(const nebula::surface::RowData&) = 0;

  inline NativeType agg(NativeType ov, NativeType nv) {
    return agg_(ov, nv);
  }

private:
  AggFunction agg_;
};

#undef TYPE_VALUE_EVAL_KIND

enum class UDFType {
  NOT,
  MAX,
  MIN,
  AVG,
  COUNT,
  SUM
};

template <UDFType UDFKind>
struct UdfTraits {};

#define DEFINE_UDF_TRAITS(NAME, IsUdaf)  \
  template <>                            \
  struct UdfTraits<UDFType::NAME> {      \
    static constexpr bool UDAF = IsUdaf; \
  };

// define each UDAF type taits
DEFINE_UDF_TRAITS(NOT, false)
DEFINE_UDF_TRAITS(MAX, true)
DEFINE_UDF_TRAITS(MIN, true)
DEFINE_UDF_TRAITS(AVG, true)
DEFINE_UDF_TRAITS(COUNT, true)
DEFINE_UDF_TRAITS(SUM, true)

#undef DEFINE_UDF_TRAITS

} // namespace eval
} // namespace execution
} // namespace nebula