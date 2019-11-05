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

#include <numeric>
#include <unordered_set>
#include "common/Cursor.h"
#include "eval/ValueEval.h"
#include "meta/NNode.h"
#include "surface/DataSurface.h"
#include "type/Type.h"

/**
 * Define nebula execution runtime.
 * Every single node will receive the plan and know what execution unit it needs to run on top of its data range.
 * 
 * an execution plan includes 3 phases and a roundtrip execution loop:
 * Role: Controller -> Node         -> Block
 * Func: Global Agg <- Partial Agg  <- Scan
 * 
 * 
 * When we execute a plan:
 * 1. It starts from the controller which starts the query fan-out to all nodes 
 * and execute phase 3 when data comes back.
 * 2. Each node will receive the execution plan and start with phase 2, the node will figure out total block list
 * in current node that satisfied the query and start threads to execute phase 1.
 * 3. A low-level exeuction unit is the block data scan defined in the phase1.
 * 
 * TODO(cao) - An execution plan is serializable - consider using protobuf / thrift to define it.
 */
namespace nebula {
namespace execution {

enum class PhaseType {
  COMPUTE,
  PARTIAL,
  GLOBAL
};

template <PhaseType PHASE>
struct PhaseTraits {};

class ExecutionPhase;

// phase plan
template <PhaseType PT>
class Phase {};

// define query window type
using QueryWindow = std::pair<size_t, size_t>;
using BlockPhase = Phase<PhaseType::COMPUTE>;
using NodePhase = Phase<PhaseType::PARTIAL>;
using FinalPhase = Phase<PhaseType::GLOBAL>;

// An execution plan that can be serialized and passed around
// protobuf?
class ExecutionPlan {
public:
  ExecutionPlan(std::unique_ptr<ExecutionPhase> plan,
                std::vector<nebula::meta::NNode> nodes,
                nebula::type::Schema output);
  virtual ~ExecutionPlan() = default;

public:
  void display() const;

  template <PhaseType PT>
  const Phase<PT>& fetch() const;

  const std::vector<nebula::meta::NNode>& getNodes() const {
    return nodes_;
  }

  const std::string& id() const {
    return uuid_;
  }

  nebula::type::Schema getOutputSchema() const {
    return output_;
  }

  // set time range as [start, end] alias window
  inline void setWindow(const QueryWindow& window) noexcept {
    window_ = window;
  }

  inline const QueryWindow& getWindow() const noexcept {
    return window_;
  }

private:
  const ExecutionPhase& fetch(PhaseType type) const;

private:
  const std::string uuid_;
  std::unique_ptr<ExecutionPhase> plan_;
  std::vector<nebula::meta::NNode> nodes_;
  nebula::type::Schema output_;
  QueryWindow window_;
};

// base execution phase definition - templated lambda - looking for C++ 20?
static constexpr auto indent4 = "    ";
static constexpr auto bliteral = [](bool lv) { return lv ? "YES" : "NO"; };
static constexpr auto join = [](const std::vector<size_t>& vector) {
  return std::accumulate(vector.begin(), vector.end(), std::string(""), [](const std::string& s, size_t x) {
    if (s.empty()) {
      return std::to_string(x);
    }

    return fmt::format("{0}, {1}", s, x);
  });
};

class ExecutionPhase {
public:
  ExecutionPhase(nebula::type::Schema input) : input_{ input }, upstream_{ nullptr }, limit_{ 0 } {}
  ExecutionPhase(std::unique_ptr<ExecutionPhase> upstream)
    : upstream_{ std::move(upstream) } {
    input_ = upstream_->outputSchema();
  }
  virtual ~ExecutionPhase() = default;

  virtual nebula::type::Schema inputSchema() const {
    return input_;
  }

  virtual nebula::type::Schema outputSchema() const {
    return input_;
  }

  inline const ExecutionPhase& upstream() const {
    return *upstream_;
  }

  inline size_t top() const {
    return limit_;
  }

  virtual void display() const = 0;

  virtual PhaseType type() const = 0;

protected:
  nebula::type::Schema input_;
  std::unique_ptr<ExecutionPhase> upstream_;
  size_t limit_;
};

template <>
class Phase<PhaseType::COMPUTE> : public ExecutionPhase {
public:
  Phase(const nebula::type::Schema input, const nebula::type::Schema output)
    : ExecutionPhase(input), input_{ input }, output_{ output } {}
  virtual ~Phase() = default;

public:
  Phase& scan(const std::string& tbl) {
    table_ = tbl;
    return *this;
  }

  Phase& filter(std::unique_ptr<eval::ValueEval> f) {
    filter_ = std::move(f);
    return *this;
  }

  Phase& compute(std::vector<std::unique_ptr<eval::ValueEval>> s) {
    fields_ = std::move(s);
    return *this;
  }

  Phase& keys(const std::vector<size_t>& keys) {
    keys_ = keys;
    return *this;
  }

  Phase& limit(size_t limit) {
    limit_ = limit;
    return *this;
  }

  Phase& aggregate(bool hasAgg) {
    hasAgg_ = hasAgg;
    return *this;
  }

  virtual nebula::type::Schema outputSchema() const override {
    return output_;
  }

  virtual void display() const override;

  inline virtual PhaseType type() const override {
    return PhaseType::COMPUTE;
  }

public:
  inline const std::string& table() const {
    return table_;
  }

  inline const std::vector<size_t>& keys() const {
    return keys_;
  }

  inline const std::vector<std::unique_ptr<eval::ValueEval>>& fields() const {
    return fields_;
  }

  inline const eval::ValueEval& filter() const {
    return *filter_;
  }

  // decide if we want to cache expression evaluations
  // TODO(cao) - cache evaluation is interesting, some work need to be done to have fair evaluation
  // 1. collect both leaf and composition of evaluation expressions. asEval can open to receive and set
  // 2. we need to push EvalContext to every level evaluation, rather than just BlockExecutor which only test top level
  // 3. we need to keep constant evaluation from erasing, we can use signature to decide if its a const value or variables.
  inline bool cacheEval() const {
    return false;
  }

  inline bool hasAggregation() const {
    return hasAgg_;
  }

private:
  std::string table_;
  std::vector<std::unique_ptr<eval::ValueEval>> fields_;
  std::unique_ptr<eval::ValueEval> filter_;
  std::vector<size_t> keys_;
  bool hasAgg_;
  nebula::type::Schema input_;
  nebula::type::Schema output_;
};

template <>
class Phase<PhaseType::PARTIAL> : public ExecutionPhase {
public:
  Phase(std::unique_ptr<ExecutionPhase> upstream)
    : ExecutionPhase(std::move(upstream)), numAgg_{ 0 } {}

  virtual ~Phase() = default;

public:
  Phase& agg(size_t numAggColumns, const std::vector<bool>& aggColumns) {
    // processing the partial plan based on block plan
    const auto& bp = static_cast<const BlockPhase&>(*upstream_);
    numAgg_ = numAggColumns;
    aggCols_ = aggColumns;
    keys_ = bp.keys();
    // cross check if the keys is expected based on aggCols - changed plan?
    return *this;
  }

  virtual void display() const override;
  inline virtual PhaseType type() const override {
    return PhaseType::PARTIAL;
  }

  inline bool hasAgg() const {
    return numAgg_ > 0;
  }

  inline bool hasKeys() const {
    // select count(1) from t will not have key
    return numAgg_ < aggCols_.size();
  }

  inline bool isAggCol(size_t index) const {
    return aggCols_.at(index);
  }

  inline const std::vector<size_t>& keys() const {
    return keys_;
  }

  inline const std::vector<std::unique_ptr<eval::ValueEval>>& fields() const {
    return static_cast<const BlockPhase&>(*upstream_).fields();
  }

private:
  size_t numAgg_;
  std::vector<bool> aggCols_;
  std::vector<size_t> keys_;
};

template <>
class Phase<PhaseType::GLOBAL> : public ExecutionPhase {
public:
  Phase(std::unique_ptr<ExecutionPhase> upstream, nebula::type::Schema output = nullptr)
    : ExecutionPhase(std::move(upstream)),
      output_{ output } {}
  virtual ~Phase() = default;

public:
  Phase& agg(size_t numAggColumns, const std::vector<bool>& aggColumns) {
    // processing the partial plan based on block plan
    const auto& bp = static_cast<const NodePhase&>(*upstream_);
    numAgg_ = numAggColumns;
    aggCols_ = aggColumns;
    keys_ = bp.keys();
    // cross check if the keys is expected based on aggCols - changed plan?
    return *this;
  }

  Phase& sort(const std::vector<size_t>& sorts, bool desc) {
    sorts_ = sorts;
    desc_ = desc;
    return *this;
  }

  Phase& limit(size_t limit) {
    limit_ = limit;
    return *this;
  }

  virtual void display() const override;

  inline virtual nebula::type::Schema outputSchema() const override {
    // same output or input
    return output_ == nullptr ? input_ : output_;
  }

  inline virtual PhaseType type() const override {
    return PhaseType::GLOBAL;
  }

  inline const std::vector<size_t>& sorts() const {
    return sorts_;
  }

  inline bool isDesc() const {
    return desc_;
  }

  inline bool hasAgg() const {
    return numAgg_ > 0;
  }

  inline const std::vector<size_t>& keys() const {
    return keys_;
  }

  inline const std::vector<std::unique_ptr<eval::ValueEval>>& fields() const {
    return static_cast<const NodePhase&>(*upstream_).fields();
  }

  // indicate if the phase has different input/output (data type)
  // we will do a conversion using each field's finalize method
  inline bool diffInputOutput() const {
    return output_ != nullptr;
  }

private:
  std::vector<size_t> sorts_;
  // TODO(cao) - every sort column may have different order, single direction now
  bool desc_;

  size_t numAgg_;
  std::vector<bool> aggCols_;
  std::vector<size_t> keys_;
  nebula::type::Schema output_;
};

template <>
struct PhaseTraits<PhaseType::GLOBAL> {
  static constexpr auto name = "GLOBAL";
};

template <>
struct PhaseTraits<PhaseType::PARTIAL> {
  static constexpr auto name = "PARTIAL";
};

template <>
struct PhaseTraits<PhaseType::COMPUTE> {
  static constexpr auto name = "COMPUTE";
};

} // namespace execution
} // namespace nebula