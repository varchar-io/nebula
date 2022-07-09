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

#include <glog/logging.h>

#include "Aggregator.h"
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
 *  and change the VAR name to NK (Native Kind), IK (Input Kind)
 */
namespace nebula {
namespace surface {
namespace eval {

// TypeValueEval accepts two parameters: store/native type and input type
#define TYPE_VALUE_EVAL_KIND(S, I, T) \
  TypeValueEval<typename nebula::type::TypeTraits<S>::CppType, typename nebula::type::TypeTraits<I>::CppType, T>

// define an UDF interface
template <nebula::type::Kind NK,
          nebula::type::Kind IK,
          typename BaseType = TYPE_VALUE_EVAL_KIND(NK, IK, false)>
class UDF : public BaseType {
public:
  using NativeType = typename nebula::type::TypeTraits<NK>::CppType;
  using InputType = typename nebula::type::TypeTraits<IK>::CppType;
  using Logic = std::function<std::optional<NativeType>(const std::optional<InputType>&)>;
  using EvalBlock = std::function<BlockEval(const Block&)>;

  UDF(const std::string& name,
      std::unique_ptr<nebula::surface::eval::ValueEval> expr,
      Logic&& logic,
      EvalBlock&& eb = uncertain)
    : BaseType(
      fmt::format("{0}({1})", name, expr->signature()),
      ExpressionType::FUNCTION,
      [this](EvalContext& ctx, const std::vector<std::unique_ptr<ValueEval>>&) -> decltype(auto) {
        // call the UDF to evalue the result
        return logic_(expr_->eval<InputType>(ctx));
      },
      std::move(eb)),
      expr_{ std::move(expr) },
      logic_{ std::move(logic) } {}
  virtual ~UDF() = default;

private:
  std::unique_ptr<nebula::surface::eval::ValueEval> expr_;
  Logic logic_;
};

// UDAF is a state ful object, its eval signature is based on store type
// its eval method will return inner expression only, hence it has eval type as Input (IK)
template <nebula::type::Kind NK,
          nebula::type::Kind IK = NK,
          typename BaseType = TYPE_VALUE_EVAL_KIND(NK, IK, true)>
class UDAF : public BaseType {
public:
  // A UDAF genericlly goes through 3 different life phase, each phase they may deal with different data types.
  // NativeType - this defines the UDF kind which relates to its inner expression data type.
  // The 3 phases are
  // Compute Phase: it collects native type of data from expressions, apply compute and store states.
  // Merge Phase: it merges multiple storage types from different sources.
  // Final Phase: this is also called global merge phase, it eventually convert storage type into native type.
  // Hence, every phase has different input/output schema due to this difference.
  using InputType = typename nebula::type::TypeTraits<IK>::CppType;
  using NativeType = typename nebula::type::TypeTraits<NK>::CppType;
  using BaseAggregator = typename nebula::surface::eval::Aggregator<NK, IK>;
  using SketchMaker = typename std::function<std::shared_ptr<BaseAggregator>()>;

  UDAF(const std::string& name,
       std::unique_ptr<ValueEval> expr,
       SketchMaker&& maker)
    : BaseType(
      fmt::format("{0}({1})", name, expr->signature()),
      ExpressionType::FUNCTION,
      [this](EvalContext& ctx, const std::vector<std::unique_ptr<ValueEval>>&) -> decltype(auto) {
        // call the UDF to evalue the result
        return expr_->eval<InputType>(ctx);
      },
      uncertain,
      std::move(maker),
      {}),
      expr_{ std::move(expr) } {
  }
  virtual ~UDAF() = default;

private:
  std::unique_ptr<ValueEval> expr_;
};

#undef TYPE_VALUE_EVAL_KIND

enum class UDFType {
  // UDF
  NOT,
  LIKE,
  PREFIX,
  IN,
  BETWEEN,
  ROUNDTIMETOUNIT,
  // UDAF
  MAX,
  MIN,
  AVG,
  COUNT,
  SUM,
  PCT,
  TPM,
  CARD,
  HIST
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

#define FUNCTION_TRAITS(NAME, VALID, DEDUCED, INPUT...)                        \
  template <>                                                                  \
  struct UdfTraits<UDFType::NAME, ##INPUT> : public UdfTraits<UDFType::NAME> { \
    static constexpr bool Valid = VALID;                                       \
    static constexpr nebula::type::Kind Type = DEDUCED;                        \
  };

// Shortcut: Normal Non-UDAF do not have STORE
#define UDF_TRAITS(NAME, DEDUCED, INPUT...) FUNCTION_TRAITS(NAME, true, DEDUCED, ##INPUT)
#define UDF_TRAITS_INPUT1(INPUT, NAME, DEDUCED) UDF_TRAITS(NAME, DEDUCED, INPUT)

// shortcut: for UDAF that has same type for (deduced, store, input).
#define UDF_SAME_AS_INPUT(K, NAME) UDF_TRAITS(NAME, K, K)
#define UDF_NOT_SUPPORT(NAME, INPUT) FUNCTION_TRAITS(NAME, false, INPUT, INPUT)

// shortcut: for UDAF to define all supported types with same types
#define UDF_SAME_AS_INPUT_ALL(NAME) REPEAT_ALL_TYPES(UDF_SAME_AS_INPUT, NAME)

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

// define traits for UDF: ROUNDTIMETOUNIT
// ROUNDTIMETOUNIT function applies to integer type only.
STATIC_TRAITS(ROUNDTIMETOUNIT, false)
UDF_TRAITS(ROUNDTIMETOUNIT, nebula::type::Kind::BIGINT, nebula::type::Kind::BIGINT)

// define traits for UDF: IN
// IN function looks for expected value in given list of values
STATIC_TRAITS(IN, false)
REPEAT_ALL_TYPES(UDF_TRAITS_INPUT1, IN, nebula::type::Kind::BOOLEAN)

// define traits for UDF: between
// BETWEEN function looks for if evaluated value is in a given range
STATIC_TRAITS(BETWEEN, false)
UDF_TRAITS(BETWEEN, nebula::type::Kind::BOOLEAN, nebula::type::Kind::TINYINT)
UDF_TRAITS(BETWEEN, nebula::type::Kind::BOOLEAN, nebula::type::Kind::SMALLINT)
UDF_TRAITS(BETWEEN, nebula::type::Kind::BOOLEAN, nebula::type::Kind::INTEGER)
UDF_TRAITS(BETWEEN, nebula::type::Kind::BOOLEAN, nebula::type::Kind::BIGINT)
UDF_TRAITS(BETWEEN, nebula::type::Kind::BOOLEAN, nebula::type::Kind::REAL)
UDF_TRAITS(BETWEEN, nebula::type::Kind::BOOLEAN, nebula::type::Kind::DOUBLE)
UDF_NOT_SUPPORT(BETWEEN, nebula::type::Kind::INVALID)
UDF_NOT_SUPPORT(BETWEEN, nebula::type::Kind::BOOLEAN)
UDF_NOT_SUPPORT(BETWEEN, nebula::type::Kind::VARCHAR)
UDF_NOT_SUPPORT(BETWEEN, nebula::type::Kind::INT128)

// define each UDAF type taits
// define traits for UDAF: MAX
STATIC_TRAITS(MAX, true)
UDF_SAME_AS_INPUT_ALL(MAX)

// define traits for UDAF: MIN
STATIC_TRAITS(MIN, true)
UDF_SAME_AS_INPUT_ALL(MIN)

// define traits for UDAF: COUNT
// no matter what input type is, it always uses BIGINT as result
STATIC_TRAITS(COUNT, true)
REPEAT_ALL_TYPES(UDF_TRAITS_INPUT1, COUNT, nebula::type::Kind::BIGINT)

// define traits for UDAF: SUM
// We use next level of types to store SUM, note that
// do not support SUM on INT128 type
// largest type in integral types are BIGINT means we don't even use INT128 to store it
// with saying that, SUM(BIGINT) could be overflow in the final result, this will also apply to AVG function.
// this is due to consideration of consistency with AVG function since BIGINT is the largest holder to sum up values.
STATIC_TRAITS(SUM, true)
UDF_TRAITS(SUM, nebula::type::Kind::BIGINT, nebula::type::Kind::TINYINT)
UDF_TRAITS(SUM, nebula::type::Kind::BIGINT, nebula::type::Kind::SMALLINT)
UDF_TRAITS(SUM, nebula::type::Kind::BIGINT, nebula::type::Kind::INTEGER)
UDF_TRAITS(SUM, nebula::type::Kind::BIGINT, nebula::type::Kind::BIGINT)
UDF_TRAITS(SUM, nebula::type::Kind::DOUBLE, nebula::type::Kind::REAL)
UDF_TRAITS(SUM, nebula::type::Kind::DOUBLE, nebula::type::Kind::DOUBLE)
UDF_TRAITS(SUM, nebula::type::Kind::INT128, nebula::type::Kind::INT128)

// we do not support sum bool, string or int128
UDF_NOT_SUPPORT(SUM, nebula::type::Kind::INVALID)
UDF_NOT_SUPPORT(SUM, nebula::type::Kind::BOOLEAN)
UDF_NOT_SUPPORT(SUM, nebula::type::Kind::VARCHAR)

// define traits for UDAF: AVG
// We're always using 128bits as store type:
// lower 8 bytes is always store counts
// while high 8 bytes can be eitehr interpreted as double for floating numbers or bigint for integral numbers
// in finalize stage, AVG will get the division and convert to the return type
STATIC_TRAITS(AVG, true)
UDF_SAME_AS_INPUT(nebula::type::Kind::TINYINT, AVG)
UDF_SAME_AS_INPUT(nebula::type::Kind::SMALLINT, AVG)
UDF_SAME_AS_INPUT(nebula::type::Kind::INTEGER, AVG)
UDF_SAME_AS_INPUT(nebula::type::Kind::BIGINT, AVG)
UDF_SAME_AS_INPUT(nebula::type::Kind::REAL, AVG)
UDF_SAME_AS_INPUT(nebula::type::Kind::DOUBLE, AVG)
UDF_SAME_AS_INPUT(nebula::type::Kind::INT128, AVG)

// we do not support sum bool, string or int128
UDF_NOT_SUPPORT(AVG, nebula::type::Kind::INVALID)
UDF_NOT_SUPPORT(AVG, nebula::type::Kind::BOOLEAN)
UDF_NOT_SUPPORT(AVG, nebula::type::Kind::VARCHAR)

// add tdigest UDAF traits definition
// in schema, a digest is serialized as string type
// while it's store type is a customized serializable struct / object => void*
STATIC_TRAITS(PCT, true)
UDF_SAME_AS_INPUT(nebula::type::Kind::TINYINT, PCT)
UDF_SAME_AS_INPUT(nebula::type::Kind::SMALLINT, PCT)
UDF_SAME_AS_INPUT(nebula::type::Kind::INTEGER, PCT)
UDF_SAME_AS_INPUT(nebula::type::Kind::BIGINT, PCT)
UDF_SAME_AS_INPUT(nebula::type::Kind::REAL, PCT)
UDF_SAME_AS_INPUT(nebula::type::Kind::DOUBLE, PCT)

// do not support digest on these types
UDF_NOT_SUPPORT(PCT, nebula::type::Kind::INVALID)
UDF_NOT_SUPPORT(PCT, nebula::type::Kind::BOOLEAN)
UDF_NOT_SUPPORT(PCT, nebula::type::Kind::VARCHAR)
UDF_NOT_SUPPORT(PCT, nebula::type::Kind::INT128)

// TPM (Tree Path Merge) only supports merge string of stack and output to a binary/varchar
STATIC_TRAITS(TPM, true)
UDF_TRAITS(TPM, nebula::type::Kind::VARCHAR, nebula::type::Kind::VARCHAR)

// we do not support sum bool, string or int128
UDF_NOT_SUPPORT(TPM, nebula::type::Kind::INVALID)
UDF_NOT_SUPPORT(TPM, nebula::type::Kind::BOOLEAN)
UDF_NOT_SUPPORT(TPM, nebula::type::Kind::TINYINT)
UDF_NOT_SUPPORT(TPM, nebula::type::Kind::SMALLINT)
UDF_NOT_SUPPORT(TPM, nebula::type::Kind::INTEGER)
UDF_NOT_SUPPORT(TPM, nebula::type::Kind::BIGINT)
UDF_NOT_SUPPORT(TPM, nebula::type::Kind::REAL)
UDF_NOT_SUPPORT(TPM, nebula::type::Kind::DOUBLE)
UDF_NOT_SUPPORT(TPM, nebula::type::Kind::INT128)

// Cardinality UDF can be applied to any type - it uses their binary value
// define traits for UDAF: CARD
// no matter what input type is, it always uses BIGINT as result indicating cardinality
STATIC_TRAITS(CARD, true)
REPEAT_ALL_TYPES(UDF_TRAITS_INPUT1, CARD, nebula::type::Kind::BIGINT)

STATIC_TRAITS(HIST, true)
UDF_TRAITS(HIST, nebula::type::Kind::VARCHAR, nebula::type::Kind::TINYINT)
UDF_TRAITS(HIST, nebula::type::Kind::VARCHAR, nebula::type::Kind::SMALLINT)
UDF_TRAITS(HIST, nebula::type::Kind::VARCHAR, nebula::type::Kind::INTEGER)
UDF_TRAITS(HIST, nebula::type::Kind::VARCHAR, nebula::type::Kind::BIGINT)
UDF_TRAITS(HIST, nebula::type::Kind::VARCHAR, nebula::type::Kind::REAL)
UDF_TRAITS(HIST, nebula::type::Kind::VARCHAR, nebula::type::Kind::DOUBLE)

// do not support digest on these types (folly histogram doesn't support those types)
UDF_NOT_SUPPORT(HIST, nebula::type::Kind::INVALID)
UDF_NOT_SUPPORT(HIST, nebula::type::Kind::BOOLEAN)
UDF_NOT_SUPPORT(HIST, nebula::type::Kind::VARCHAR)
UDF_NOT_SUPPORT(HIST, nebula::type::Kind::INT128)

#undef UDF_SAME_AS_INPUT_ALL
#undef UDF_SAME_AS_INPUT
#undef UDF_NOT_SUPPORT
#undef UDF_TRAITS_INPUT1
#undef UDF_TRAITS
#undef FUNCTION_TRAITS
#undef STATIC_TRAITS
#undef REPEAT_ALL_TYPES

// Utility to fetch runtime UDF type by its inner expressions types.
#define CASE_TYPE_KIND1(K, F)                       \
  case nebula::type::Kind::K: {                     \
    return UdfTraits<UK, nebula::type::Kind::K>::F; \
  }

// get UDF result type kind based on its input type kind
template <UDFType UK>
nebula::type::Kind udfNativeKind(nebula::type::Kind k) noexcept {
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

// get UDF store type kind based on its input type kind
template <UDFType UK>
inline nebula::type::Kind udfStoreKind(nebula::type::Kind k) noexcept {
  // count is special - no matter what input is,
  // always using integer 1 as input
  if constexpr (UK == UDFType::COUNT) {
    return nebula::type::Kind::INTEGER;
  } else {
    return k;
  }
}

#undef CASE_TYPE_KIND1

} // namespace eval
} // namespace surface
} // namespace nebula