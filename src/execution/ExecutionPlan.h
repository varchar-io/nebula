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

#include <folly/FBString.h>
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

// An execution plan that can be serialized and passed around
// protobuf?
class ExecutionPlan {
public:
  ExecutionPlan(std::unique_ptr<ExecutionPhase> plan, std::vector<nebula::meta::NNode> nodes);
  virtual ~ExecutionPlan() = default;

public:
  void display() const;

  template <PhaseType PT>
  const Phase<PT>& fetch() const;

  const std::vector<nebula::meta::NNode>& getNodes() const {
    return nodes_;
  }

  const folly::fbstring& id() const {
    return uuid_;
  }

private:
  const ExecutionPhase& fetch(PhaseType type) const;

private:
  const folly::fbstring uuid_;
  std::unique_ptr<ExecutionPhase> plan_;
  std::vector<nebula::meta::NNode> nodes_;
};

// base execution phase definition - templated lambda - looking for C++ 20?
static constexpr auto indent4 = "    ";
static constexpr auto bliteral = [](bool lv) { return lv ? "YES" : "NO"; };
static constexpr auto join = [](const std::vector<size_t>& vector) {
  return std::accumulate(vector.begin(), vector.end(), std::string(""), [](const std::string& s, size_t x) {
    return fmt::format("{0}, {1}", s, x);
  });
};

class ExecutionPhase {
public:
  ExecutionPhase(nebula::type::Schema input, std::unique_ptr<ExecutionPhase> upstream)
    : input_{ input }, upstream_{ std::move(upstream) } {
    N_ENSURE_NOT_NULL(input_, "input schema can't be null");
  }
  virtual ~ExecutionPhase() = default;

  virtual nebula::type::Schema outputSchema() const {
    return input_;
  }

  inline const ExecutionPhase& upstream() const {
    return *upstream_;
  }

  virtual void display() const = 0;

  virtual PhaseType type() const = 0;

protected:
  nebula::type::Schema input_;
  std::unique_ptr<ExecutionPhase> upstream_;
};

template <>
class Phase<PhaseType::COMPUTE> : public ExecutionPhase {
public:
  Phase(const nebula::type::Schema input, const nebula::type::Schema output)
    : ExecutionPhase(input, nullptr), output_{ output } {}
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

  Phase& keys(std::vector<size_t> keys) {
    keys_ = keys;
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

private:
  std::string table_;
  std::vector<std::unique_ptr<eval::ValueEval>> fields_;
  std::unique_ptr<eval::ValueEval> filter_;
  std::vector<size_t> keys_;
  nebula::type::Schema output_;
};

template <>
class Phase<PhaseType::PARTIAL> : public ExecutionPhase {
public:
  Phase(std::unique_ptr<ExecutionPhase> upstream)
    : ExecutionPhase(upstream->outputSchema(), std::move(upstream)) {}

  virtual ~Phase() = default;

public:
  Phase& agg() {
    return *this;
  }

  virtual void display() const override;
  inline virtual PhaseType type() const override {
    return PhaseType::PARTIAL;
  }
};

template <>
class Phase<PhaseType::GLOBAL> : public ExecutionPhase {
public:
  Phase(std::unique_ptr<ExecutionPhase> upstream)
    : ExecutionPhase(upstream->outputSchema(), std::move(upstream)) {}

  virtual ~Phase() = default;

public:
  Phase& agg() {
    return *this;
  }

  Phase& sort(std::vector<size_t> sorts) {
    sorts_ = sorts;
    return *this;
  }

  Phase& limit(size_t limit) {
    limit_ = limit;
    return *this;
  }

  virtual void display() const override;

  inline virtual PhaseType type() const override {
    return PhaseType::GLOBAL;
  }

private:
  std::vector<size_t> sorts_;
  size_t limit_;
};

using BlockPhase = Phase<PhaseType::COMPUTE>;
using NodePhase = Phase<PhaseType::PARTIAL>;
using FinalPhase = Phase<PhaseType::GLOBAL>;

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