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

#include "common/Cursor.h"
#include "eval/ValueEval.h"
#include "glog/logging.h"
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

class Phase;

// An execution plan that can be serialized and passed around
// protobuf?
class ExecutionPlan {
public:
  ExecutionPlan(std::unique_ptr<Phase> plan, std::vector<nebula::meta::NNode> nodes);
  virtual ~ExecutionPlan() = default;

public:
  void display() const;
  nebula::common::Cursor<nebula::surface::RowData&> execute(const std::string& server);

private:
  std::unique_ptr<Phase> plan_;
  std::vector<nebula::meta::NNode> nodes_;
};

// phase plan
class Phase {
public:
  Phase(PhaseType type, const nebula::type::Schema input) : type_{ type }, schema_{ input } {
  }

  Phase(PhaseType type, std::unique_ptr<Phase> upstream)
    : type_{ type }, upstream_{ std::move(upstream) }, schema_{ upstream_->outputSchema() } {
    // input schema is upstream's output schema
  }
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

public:
  nebula::type::Schema outputSchema() const {
    return nullptr;
  }

private:
  PhaseType type_;
  std::unique_ptr<Phase> upstream_;
  std::vector<std::unique_ptr<eval::ValueEval>> fields_;
  std::unique_ptr<eval::ValueEval> filter_;
  std::vector<size_t> keys_;
  std::vector<size_t> sorts_;
  std::string table_;
  size_t limit_;
  nebula::type::Schema schema_;
};

} // namespace execution
} // namespace nebula