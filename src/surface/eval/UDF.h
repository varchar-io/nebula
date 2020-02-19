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
 * Define UDF properties used in the nebula DSL.
 * 
 * We define 2 types associated with UDF and 3 types for UDAF.
 * They are:
 *  Input Type -> the expression accepted by the UDF, 
 *                yes, this needs to be variadic to support multi-params in the future.
 *  Native Type -> type of the UDF, what type to use in its output schema
 *  Store Type (UDAF only) -> what is the intermidiate store type for this UDAF.
 * 
 *  As clean-up, let's use above names consistently everywhere, 
 *  and change the VAR name to NK (Native Kind), IK (Input Kind), SK (Store Kind)
 */
namespace nebula {
namespace surface {
namespace eval {

#define TYPE_VALUE_EVAL_KIND(K) TypeValueEval<typename nebula::type::TypeTraits<K>::CppType>

// define an UDF interface
template <nebula::type::Kind NK, nebula::type::Kind IK>
class UDF : public TYPE_VALUE_EVAL_KIND(NK) {
public:
  using NativeType = typename nebula::type::TypeTraits<NK>::CppType;
  using InputType = typename nebula::type::TypeTraits<IK>::CppType;
  using Logic = std::function<NativeType(const InputType&, bool& valid)>;

  UDF(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr, Logic&& logic)
    : TYPE_VALUE_EVAL_KIND(NK)(
        fmt::format("{0}({1})", name, expr->signature()),
        [this](EvalContext& ctx, const std::vector<std::unique_ptr<ValueEval>>&, bool& valid) -> decltype(auto) {
          // call the UDF to evalue the result
          return logic_(expr_->eval<InputType>(ctx, valid), valid);
        },
        {},
        {}),
      expr_{ std::move(expr) },
      logic_{ std::move(logic) } {}
  virtual ~UDF() = default;

private:
  std::unique_ptr<nebula::surface::eval::ValueEval> expr_;
  Logic logic_;
};

// UDAF is a state ful object, its eval signature is based on store type
template <nebula::type::Kind NK, nebula::type::Kind SK = NK, nebula::type::Kind IK = NK>
class UDAF : public TYPE_VALUE_EVAL_KIND(SK) {
public:
  // A UDAF genericlly goes through 3 different life phase, each phase they may deal with different data types.
  // NativeType - this defines the UDF kind which relates to its inner expression data type.
  // StoreType - this defines the storage type how states are stored for this UDAF
  // The 3 phases are
  // Compute Phase: it collects native type of data from expressions, apply compute and store states.
  // Merge Phase: it merges multiple storage types from different sources.
  // Final Phase: this is also called global merge phase, it eventually convert storage type into native type.
  // Hence, every phase has different input/output schema due to this difference.
  // For most of the cases, StoreType == NativeType, so there is almost no changes.
  using InputType = typename nebula::type::TypeTraits<IK>::CppType;
  using NativeType = typename nebula::type::TypeTraits<NK>::CppType;
  using StoreType = typename nebula::type::TypeTraits<SK>::CppType;

  // store(input -> store), merge(store -> store), finalize(store -> native)
  using StoreFunction = std::function<StoreType(InputType)>;
  using MergeFunction = std::function<StoreType(StoreType, StoreType)>;
  using FinalizeFunction = std::function<NativeType(StoreType)>;

  UDAF(const std::string& name,
       std::unique_ptr<ValueEval> expr,
       MergeFunction&& merge) : UDAF(name, std::move(expr), {}, std::move(merge), {}) {
  }

  UDAF(const std::string& name,
       std::unique_ptr<ValueEval> expr,
       StoreFunction&& store,
       MergeFunction&& merge,
       FinalizeFunction&& finalize)
    : TYPE_VALUE_EVAL_KIND(SK)(
        fmt::format("{0}({1})", name, expr->signature()),
        [this](EvalContext& ctx, const std::vector<std::unique_ptr<ValueEval>>&, bool& valid) -> decltype(auto) {
          // call the UDF to evalue the result
          auto exprValue = expr_->eval<InputType>(ctx, valid);
          if constexpr (SK == IK) {
            return exprValue;
          } else {
            // if input type is not the same as store type
            // hmm, store function should be called, otherwise we only resort to type cast
            if (store_) {
              return store_(exprValue);
            }

            return static_cast<StoreType>(exprValue);
          }
        },
        std::move(merge),
        {}),
      expr_{ std::move(expr) },
      store_{ std::move(store) },
      finalize_{ std::move(finalize) } {}
  virtual ~UDAF() = default;

  inline NativeType finalize(StoreType v) {
    if constexpr (SK == NK) {
      return v;
    } else {
      if (finalize_) {
        return finalize_(v);
      }
      return static_cast<NativeType>(v);
    }
  }

private:
  std::unique_ptr<ValueEval> expr_;
  StoreFunction store_;
  FinalizeFunction finalize_;
};

#undef TYPE_VALUE_EVAL_KIND

enum class UDFType {
  // UDF
  NOT,
  LIKE,
  PREFIX,
  IN,
  // UDAF
  MAX,
  MIN,
  AVG,
  COUNT,
  SUM
};

// UDF traits tells us:
// 1. What deduced type of the UDF according to its expression TYPEs (UDF may or may not accept multiple inputs)
// 2. (about cast: if a cast is applied, we treat it as separate expression evaluation so not handled by UDF itself).
// 3. All UDF's store type is empty since they are one-shot compute without state.
// 4. In contrast, all UDAF's store type will be defined event though they are the same as deduced type most times.
// Note that, deduced type can be called evaluated type as well.
// UDF Traits take UDF TYpe as as must-have parameter and variadic arguement of inner expression kinds.
// UK=UDF Kind, IK=Input Kind
template <UDFType UK, nebula::type::Kind... IK>
struct UdfTraits {};

// shortcut: repeat the same macro for all types
#define REPEAT_ALL_TYPES(MACRO, ARGS...)      \
  MACRO(nebula::type::Kind::BOOLEAN, ##ARGS)  \
  MACRO(nebula::type::Kind::TINYINT, ##ARGS)  \
  MACRO(nebula::type::Kind::SMALLINT, ##ARGS) \
  MACRO(nebula::type::Kind::INTEGER, ##ARGS)  \
  MACRO(nebula::type::Kind::BIGINT, ##ARGS)   \
  MACRO(nebula::type::Kind::REAL, ##ARGS)     \
  MACRO(nebula::type::Kind::DOUBLE, ##ARGS)   \
  MACRO(nebula::type::Kind::INT128, ##ARGS)   \
  MACRO(nebula::type::Kind::VARCHAR, ##ARGS)

#define STATIC_TRAITS(NAME, ISUDAF)            \
  template <>                                  \
  struct UdfTraits<UDFType::NAME> {            \
    static constexpr char const* Name = #NAME; \
    static constexpr bool UDAF = ISUDAF;       \
  };

#define FUNCTION_TRAITS(NAME, DEDUCED, STORE, INPUT...)                        \
  template <>                                                                  \
  struct UdfTraits<UDFType::NAME, ##INPUT> : public UdfTraits<UDFType::NAME> { \
    static constexpr nebula::type::Kind Type = DEDUCED;                        \
    static constexpr nebula::type::Kind Store = STORE;                         \
  };

// Shortcut: Normal Non-UDAF do not have STORE
#define UDF_TRAITS(NAME, DEDUCED, INPUT...) FUNCTION_TRAITS(NAME, DEDUCED, DEDUCED, ##INPUT)
#define UDF_TRAITS_INPUT1(INPUT, NAME, DEDUCED) UDF_TRAITS(NAME, DEDUCED, INPUT)

// shortcut: UDAF traits
#define UDAF_TRAITS(NAME, DEDUCED, STORE, INPUT...) FUNCTION_TRAITS(NAME, DEDUCED, STORE, ##INPUT)
#define UDAF_TRAITS_INPUT1(INPUT, NAME, DEDUCED, STORE) UDAF_TRAITS(NAME, DEDUCED, STORE, INPUT)
#define UDAF_NOT_SUPPORT(NAME, INPUT) UDAF_TRAITS(NAME, INPUT, INPUT, INPUT)

// shortcut: for UDAF that has same type for (deduced, store, input).
#define UDAF_SAME_AS_INPUT(K, NAME) UDAF_TRAITS(NAME, K, K, K)

// shortcut: for UDAF to define all supported types with same types
#define UDAF_SAME_AS_INPUT_ALL(NAME) REPEAT_ALL_TYPES(UDAF_SAME_AS_INPUT, NAME)

// define traits for UDF: NOT
// regardless what input expression is, it will always work as C++ syntax, such as
// (NOT STRING) !"abc" => true(if string is not null or empty) otherwise false
// (NOT NUMBER) !8 => false
//              !0 => true
// (NOT OBJECT) !obj => true(if obj is null) otherwise false
STATIC_TRAITS(NOT, false)
REPEAT_ALL_TYPES(UDF_TRAITS_INPUT1, NOT, nebula::type::Kind::BOOLEAN)

// define traits for UDF: LIKE
// like function is only applied to string type only.
STATIC_TRAITS(LIKE, false)
UDF_TRAITS(LIKE, nebula::type::Kind::BOOLEAN, nebula::type::Kind::VARCHAR)

// define traits for UDF: PREFIX
// PREFIX function applies to string type only.
STATIC_TRAITS(PREFIX, false)
UDF_TRAITS(PREFIX, nebula::type::Kind::BOOLEAN, nebula::type::Kind::VARCHAR)

// define traits for UDF: IN
// IN function looks for expected value in given list of values
STATIC_TRAITS(IN, false)
REPEAT_ALL_TYPES(UDF_TRAITS_INPUT1, IN, nebula::type::Kind::BOOLEAN)

// define each UDAF type taits
// define traits for UDAF: MAX
STATIC_TRAITS(MAX, true)
UDAF_SAME_AS_INPUT_ALL(MAX)

// define traits for UDAF: MIN
STATIC_TRAITS(MIN, true)
UDAF_SAME_AS_INPUT_ALL(MIN)

// define traits for UDAF: COUNT
// no matter what input type is, it always uses BIGINT as result
STATIC_TRAITS(COUNT, true)
REPEAT_ALL_TYPES(UDAF_TRAITS_INPUT1, COUNT, nebula::type::Kind::BIGINT, nebula::type::Kind::BIGINT)

// define traits for UDAF: SUM
// We use next level of types to store SUM, note that
// do not support SUM on INT128 type
// largest type in integral types are BIGINT means we don't even use INT128 to store it
// with saying that, SUM(BIGINT) could be overflow in the final result, this will also apply to AVG function.
// this is due to consideration of consistency with AVG function since BIGINT is the largest holder to sum up values.
STATIC_TRAITS(SUM, true)
UDAF_TRAITS(SUM, nebula::type::Kind::BIGINT, nebula::type::Kind::BIGINT, nebula::type::Kind::TINYINT)
UDAF_TRAITS(SUM, nebula::type::Kind::BIGINT, nebula::type::Kind::BIGINT, nebula::type::Kind::SMALLINT)
UDAF_TRAITS(SUM, nebula::type::Kind::BIGINT, nebula::type::Kind::BIGINT, nebula::type::Kind::INTEGER)
UDAF_TRAITS(SUM, nebula::type::Kind::BIGINT, nebula::type::Kind::BIGINT, nebula::type::Kind::BIGINT)
UDAF_TRAITS(SUM, nebula::type::Kind::DOUBLE, nebula::type::Kind::DOUBLE, nebula::type::Kind::REAL)
UDAF_TRAITS(SUM, nebula::type::Kind::DOUBLE, nebula::type::Kind::DOUBLE, nebula::type::Kind::DOUBLE)

// we do not support sum bool, string or int128
UDAF_NOT_SUPPORT(SUM, nebula::type::Kind::INVALID)
UDAF_NOT_SUPPORT(SUM, nebula::type::Kind::BOOLEAN)
UDAF_NOT_SUPPORT(SUM, nebula::type::Kind::VARCHAR)
UDAF_NOT_SUPPORT(SUM, nebula::type::Kind::INT128)

// define traits for UDAF: AVG
// We're always using 128bits as store type:
// lower 8 bytes is always store counts
// while high 8 bytes can be eitehr interpreted as double for floating numbers or bigint for integral numbers
// in finalize stage, AVG will get the division and convert to the return type
STATIC_TRAITS(AVG, true)
#define AVG_TYPE(K) UDAF_TRAITS(AVG, nebula::type::Kind::K, nebula::type::Kind::INT128, nebula::type::Kind::K)
AVG_TYPE(TINYINT)
AVG_TYPE(SMALLINT)
AVG_TYPE(INTEGER)
AVG_TYPE(BIGINT)
AVG_TYPE(REAL)
AVG_TYPE(DOUBLE)
#undef AVG_TYPE

// we do not support sum bool, string or int128
UDAF_NOT_SUPPORT(AVG, nebula::type::Kind::INVALID)
UDAF_NOT_SUPPORT(AVG, nebula::type::Kind::BOOLEAN)
UDAF_NOT_SUPPORT(AVG, nebula::type::Kind::VARCHAR)
UDAF_NOT_SUPPORT(AVG, nebula::type::Kind::INT128)

#undef UDAF_SAME_AS_INPUT_ALL
#undef UDAF_SAME_AS_INPUT
#undef UDAF_NOT_SUPPORT
#undef UDAF_TRAITS_INPUT1
#undef UDAF_TRAITS
#undef UDF_TRAITS
#undef FUNCTION_TRAITS
#undef STATIC_TRAITS
#undef REPEAT_ALL_TYPES

// Utility to fetch runtime UDF type by its inner expressions types.
#define CASE_TYPE_KIND1(K, F)                       \
  case nebula::type::Kind::K: {                     \
    return UdfTraits<UK, nebula::type::Kind::K>::F; \
  }

template <UDFType UK, bool storeKind = false>
nebula::type::Kind udfKind(nebula::type::Kind k) noexcept {
  // fetch store type
  if constexpr (storeKind) {
    switch (k) {
      CASE_TYPE_KIND1(BOOLEAN, Store)
      CASE_TYPE_KIND1(TINYINT, Store)
      CASE_TYPE_KIND1(SMALLINT, Store)
      CASE_TYPE_KIND1(INTEGER, Store)
      CASE_TYPE_KIND1(BIGINT, Store)
      CASE_TYPE_KIND1(REAL, Store)
      CASE_TYPE_KIND1(DOUBLE, Store)
      CASE_TYPE_KIND1(INT128, Store)
      CASE_TYPE_KIND1(VARCHAR, Store)
    default: return nebula::type::Kind::INVALID;
    }
  }

  // fetch UDF type itself
  if constexpr (!storeKind) {
    switch (k) {
      CASE_TYPE_KIND1(BOOLEAN, Type)
      CASE_TYPE_KIND1(TINYINT, Type)
      CASE_TYPE_KIND1(SMALLINT, Type)
      CASE_TYPE_KIND1(INTEGER, Type)
      CASE_TYPE_KIND1(BIGINT, Type)
      CASE_TYPE_KIND1(REAL, Type)
      CASE_TYPE_KIND1(DOUBLE, Type)
      CASE_TYPE_KIND1(INT128, Type)
      CASE_TYPE_KIND1(VARCHAR, Type)
    default: return nebula::type::Kind::INVALID;
    }
  }
}

#undef CASE_TYPE_KIND1

} // namespace eval
} // namespace surface
} // namespace nebula