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
            "U",
            [this](const nebula::surface::RowData& row, const std::vector<std::unique_ptr<ValueEval>>&, bool& valid) -> decltype(auto) {
              // call the UDF to evalue the result
              return this->run(row, valid);
            },
            {}) {}
  virtual ~UDF() = default;

  // columns referenced
  virtual std::vector<std::string> columns() const = 0;

  virtual typename nebula::type::TypeTraits<KIND>::CppType run(const nebula::surface::RowData&, bool&) const = 0;
};

// UDAF is a state ful object
template <nebula::type::Kind KIND>
class UDAF : public UDF<KIND> {
public:
  using NativeType = typename nebula::type::TypeTraits<KIND>::CppType;
  using AggFunction = std::function<NativeType(NativeType, NativeType)>;
  UDAF(AggFunction&& agg, AggFunction&& partial)
    : agg_{ std::move(agg) },
      partial_{ std::move(partial) } {}
  virtual ~UDAF() = default;

  // partial aggregate
  inline NativeType partial(NativeType ov, NativeType nv) {
    return partial_(ov, nv);
  }

  // global aggregate
  virtual void global(const nebula::surface::RowData&) = 0;

  inline NativeType agg(NativeType ov, NativeType nv) {
    return agg_(ov, nv);
  }

private:
  AggFunction agg_;
  AggFunction partial_;
};

#undef TYPE_VALUE_EVAL_KIND

enum class UDFType {
  NOT,
  LIKE,
  PREFIX,
  MAX,
  MIN,
  AVG,
  COUNT,
  SUM
};

template <UDFType UDFKind>
struct UdfTraits {};

#define DEFINE_UDF_TRAITS(NAME, ISUDAF, KIND)        \
  template <>                                        \
  struct UdfTraits<UDFType::NAME> {                  \
    static constexpr bool UDAF = ISUDAF;             \
    static constexpr nebula::type::Kind Type = KIND; \
  };

// define each UDAF type taits:
// ISUDAF: whether it is UDAF or normal UDF
// KIND: does this UDF has fixied result type, such as count -> bigint
// invalid means unknown determined at runt time
DEFINE_UDF_TRAITS(NOT, false, nebula::type::Kind::BOOLEAN)
DEFINE_UDF_TRAITS(LIKE, false, nebula::type::Kind::BOOLEAN)
DEFINE_UDF_TRAITS(PREFIX, false, nebula::type::Kind::BOOLEAN)
DEFINE_UDF_TRAITS(MAX, true, nebula::type::Kind::INVALID)
DEFINE_UDF_TRAITS(MIN, true, nebula::type::Kind::INVALID)
DEFINE_UDF_TRAITS(AVG, true, nebula::type::Kind::INVALID)
DEFINE_UDF_TRAITS(COUNT, true, nebula::type::Kind::BIGINT)
DEFINE_UDF_TRAITS(SUM, true, nebula::type::Kind::INVALID)

#undef DEFINE_UDF_TRAITS

} // namespace eval
} // namespace execution
} // namespace nebula