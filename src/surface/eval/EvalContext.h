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

#include <quickjs.h>
extern "C" {
#include <quickjs-libc.h>
}

#include "Block.h"
#include "Operation.h"
#include "Script.h"

#include "common/Hash.h"
#include "surface/DataSurface.h"

/**
 * Define a evaluation context - it carries runtime parser, cache mechanism for same expression evaluation.
 * Basically the context object will be used by every single value evalution compute.
 */
namespace nebula {
namespace surface {
namespace eval {

class EvalContext;

// for a given type value eval object, we know its output kind, input kind
// and a flag to indicate if evaluate input or not
template <typename T, typename I = T, bool EvalInput = false>
class TypeValueEval;

// define block evaluation result:
// ALL:     all rows will match the predicate, so further process no need to apply the filter again
// PARTIAL: maybe partial rows match, we don't know, further processs requires filter row by row
// NONE:    none rows will match, we can completely skip this block for further process
enum class BlockEval {
  ALL,
  PARTIAL,
  NONE
};

// by default, return BlockEval::PARTIAL indicating scan the whole block RBR (row-by-row)
// uncertain means we can not determine if we should cover whole block or skip whole block
inline BlockEval uncertain(const Block&) {
  return BlockEval::PARTIAL;
}

// this is a tree, with each node to be either macro/value or operator
// this is translated from expression.
class ValueEval {
public:
  ValueEval(const std::string& sign,
            ExpressionType et,
            nebula::type::Kind input,
            nebula::type::Kind output,
            bool aggregate)
    : sign_{ sign },
      et_{ et },
      input_{ input },
      output_{ output },
      aggregate_{ aggregate } {}
  virtual ~ValueEval() = default;

  // TODO(cao) - we definitely need to revisit and reevaluate if we should use std::optional<T> here
  // which shall simplify the interface a bit.
  // Current decision is to put a bool reference to indicate if the eval should be "NULL" value
  // passed value has to be true (default) - only set to false internally if the value is evaluated as NULL/empty
  // other alternative approaches:
  // 1. std::unique_ptr<T> to return nullptr or value pointer.
  // 2. std::optinal<T> to return optional value.
  // 3. a flag to indicate the return value should not be used
  template <typename T>
  inline std::optional<T> eval(EvalContext& ctx) const {
    // TODO(cao) - there is a problem wasted me a WHOLE day to figure out the root cause.
    // So document here for further robust engineering work, the case is like this:
    // When it create ValueEval, it uses template to generate TypeValueEval<std::string> for VARCHAR type
    // However, there is some mismatch to cause this function call to be eval<char*>(row)
    // obviously, below static_cast will give us a corrupted object since the concrete types mismatch.
    // This is a hard problem if we don't pay attention and waste lots of time to debug it, so how can we prevent similar problem
    // 1. we should do stronger type check to ensure the type used is consistent everywhere.
    // 2. we can enforce std::enable_if more on the template type to ensure the function is called in the expected "type"
    // N_ENSURE_NOT_NULL(p, "type should match");
    auto p = static_cast<const TypeValueEval<T>*>(this);
    // N_ENSURE_NOT_NULL(p, "Type should match in value eval!");
    return p->eval(ctx);
  }

  // stack value into object
  template <nebula::type::Kind OK, nebula::type::Kind IK>
  inline std::shared_ptr<Sketch> sketch() const {
    using TypeVE = TypeValueEval<typename nebula::type::TypeTraits<OK>::CppType, typename nebula::type::TypeTraits<IK>::CppType>;
    N_ENSURE(aggregate_, "only aggregated value can produce sketch");
    auto p = static_cast<const TypeVE*>(this);
    return p->sketch();
  }

  // evaluate the whole block of data and determine if process the block or not
  virtual BlockEval eval(const Block&) const = 0;

public:
  // identify a unique value evaluation object in given query context
  // TODO(cao) - consider using number instead for fast hashing
  inline const std::string_view signature() const {
    return sign_;
  }

  inline nebula::type::Kind inputType() const {
    return input_;
  }

  inline nebula::type::Kind outputType() const {
    return output_;
  }

  inline ExpressionType expressionType() const {
    return et_;
  }

  inline bool isAggregate() const {
    return aggregate_;
  }

protected:
  std::string sign_;
  ExpressionType et_;
  nebula::type::Kind input_;
  nebula::type::Kind output_;
  bool aggregate_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

// customized functions defined by each individual typed value eval object
#define EvalBlock std::function<BlockEval(const Block&)>
#define SketchMaker std::function<std::shared_ptr<Aggregator<OutputTD::kind, InputTD::kind>>()>
#define OPT std::function<std::optional<EvalType>(EvalContext&, const std::vector<std::unique_ptr<ValueEval>>&)>
#define OPT_LAMBDA(T, X)                                                                               \
  ([](EvalContext& ctx, const std::vector<std::unique_ptr<ValueEval>>& children) -> std::optional<T> { \
    X                                                                                                  \
  })

// TODO(cao): evaluate if we can template TypeValueEval with nebula::type::Kind
// rather than normal type so that we can keep consistent view across all foundation types
// Also easy to store and extract - otherwise we have to use TypeDetect to convert back and forth
template <typename T, typename I, bool EvalInput>
class TypeValueEval : public ValueEval {
  using OutputTD = typename nebula::type::TypeDetect<std::remove_reference_t<std::remove_cv_t<T>>>;
  using InputTD = typename nebula::type::TypeDetect<std::remove_reference_t<std::remove_cv_t<I>>>;
  using EvalType = typename std::conditional<EvalInput, I, T>::type;

public:
  TypeValueEval(const std::string& sign, const ExpressionType et, const OPT&& op, const EvalBlock&& eb)
    : TypeValueEval(sign, et, std::move(op), std::move(eb), {}, {}) {}

  TypeValueEval(
    const std::string& sign,
    const ExpressionType et,
    const OPT&& op,
    const EvalBlock&& eb,
    const SketchMaker&& st,
    std::vector<std::unique_ptr<ValueEval>> children)
    : ValueEval(sign, et, InputTD::kind, OutputTD::kind, st != nullptr),
      op_{ std::move(op) },
      eb_{ std::move(eb) },
      st_{ std::move(st) },
      children_{ std::move(children) } {}

  virtual ~TypeValueEval() = default;

  inline std::optional<EvalType> eval(EvalContext& ctx) const {
    return op_(ctx, this->children_);
  }

  inline std::shared_ptr<Aggregator<OutputTD::kind, InputTD::kind>> sketch() const {
    return st_();
  }

  virtual inline BlockEval eval(const Block& b) const override {
    return eb_(b);
  }

private:
  OPT op_;
  EvalBlock eb_;
  SketchMaker st_;
  std::vector<std::unique_ptr<ValueEval>> children_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

// cache evaluation result for reuse purpose in evaluaation context
struct EvalCache {
  explicit EvalCache(size_t size) : slice{ size } {
    reset();
  }

  void reset() {
    cursor = 1;
    map.clear();
  }

  // a signature keyed tuples indicating if this expr evaluated (having entry) or not.
  nebula::common::unordered_map<std::string_view, std::pair<size_t, size_t>> map;
  // layout all cached data, when reset, just move the cursor to 1
  // 0 is reserved, any evaluated data points to 0 if it is NULL
  size_t cursor;
  nebula::common::ExtendableSlice slice;
};

// define a global type to represent runtime fields in schema
using Fields = std::vector<std::unique_ptr<ValueEval>>;

// cache sharable script context data
struct ScriptData {
  explicit ScriptData(nebula::type::Schema schema, const Fields& c)
    : name2type{ nullptr }, customs{ c } {
    if (schema) {
      const size_t size = schema->size();
      auto map = std::make_unique<nebula::common::unordered_map<std::string, nebula::type::Kind>>(size);
      for (size_t i = 0; i < size; ++i) {
        auto f = schema->childType(i);
        map->emplace(f->name(), f->k());
      }

      std::swap(map, name2type);
    }
  }

  bool valid() const noexcept {
    return name2type != nullptr;
  }

  ValueEval* column(const std::string& col) const noexcept {
    for (auto& c : customs) {
      if (c->signature() == col) {
        return c.get();
      }
    }

    return nullptr;
  }

  // column name to type mapping - set by external
  std::unique_ptr<nebula::common::unordered_map<std::string, nebula::type::Kind>> name2type;

  // reference to custom fields
  const Fields& customs;
};

class EvalContext {
public:
  // standard shared context across rows through reset row object interface
  EvalContext(bool cache, std::shared_ptr<ScriptData> script = nullptr)
    : EvalContext(cache, script, nullptr) {}

  // case to wrap a single row without cache, cheap to create an instance of eval context
  EvalContext(std::unique_ptr<nebula::surface::Accessor> data, std::shared_ptr<ScriptData> script = nullptr)
    : EvalContext(false, script, std::move(data)) {}

  virtual ~EvalContext() = default;

  // change reference to row data, clear all cache.
  // and start build cache based on evaluation signautre.
  void reset(const nebula::surface::Accessor&);

  template <typename T>
  inline std::optional<T> read(const std::string& col) {
    // TODO(cao) - not ideal branching. we should be able to compile this before execution
    // fast lookup - best to turn all column's value materialization into a function pointer - type agnostic?
    // we exactly know what method to call for every single column

    // perf: we pay this check for every read
    if (UNLIKELY(scriptData_ != nullptr)) {
      ValueEval* ve = scriptData_->column(col);
      if (ve) {
        return ve->eval<T>(*this);
      }
    }

    // return *row_;
    // compile time branching based on template type T
    // I think it's better than using template specialization for this case
    if constexpr (std::is_same<T, bool>::value) {
      return row_->readBool(col);
    }

    if constexpr (std::is_same<T, int8_t>::value) {
      return row_->readByte(col);
    }

    if constexpr (std::is_same<T, int16_t>::value) {
      return row_->readShort(col);
    }

    if constexpr (std::is_same<T, int32_t>::value) {
      return row_->readInt(col);
    }

    if constexpr (std::is_same<T, int64_t>::value) {
      return row_->readLong(col);
    }

    if constexpr (std::is_same<T, float>::value) {
      return row_->readFloat(col);
    }

    if constexpr (std::is_same<T, double>::value) {
      return row_->readDouble(col);
    }

    if constexpr (std::is_same<T, int128_t>::value) {
      return row_->readInt128(col);
    }

    if constexpr (std::is_same<T, std::string_view>::value) {
      return row_->readString(col);
    }

    // TODO(cao): other types supported in DSL? for example: UDF on list or map
    throw NException("not supported template type");
  }

#undef NULL_CHECK

  inline ScriptContext& script() const {
    return *script_;
  }

private:
  EvalContext(bool cache,
              std::shared_ptr<ScriptData> scriptData,
              std::unique_ptr<nebula::surface::Accessor> data)
    : cache_{ !cache ? nullptr : std::make_unique<EvalCache>(1024) },
      scriptData_{ scriptData },
      script_{ scriptData == nullptr ? nullptr :
                                       std::make_unique<ScriptContext>(
                                         [this]() -> const nebula::surface::Accessor& { return *row_; },
                                         [this](const std::string& col) -> auto {
                                           return scriptData_->name2type->at(col);
                                         }) },
      data_{ std::move(data) },
      row_{ data_ ? data_.get() : nullptr } {}

private:
  std::unique_ptr<EvalCache> cache_;

  // script cache
  std::shared_ptr<ScriptData> scriptData_;

  // script context to evaluate script for value residing in current context
  std::unique_ptr<ScriptContext> script_;

  // the context may own a row object directly
  std::unique_ptr<nebula::surface::Accessor> data_;

  // row object pointer
  const nebula::surface::Accessor* row_;
};

} // namespace eval
} // namespace surface
} // namespace nebula