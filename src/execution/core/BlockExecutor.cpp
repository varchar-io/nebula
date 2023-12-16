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

#include "BlockExecutor.h"

#include <gflags/gflags.h>
#include <gperftools/profiler.h>

#include "AggregationMerge.h"
#include "common/Likely.h"
#include "memory/keyed/HashFlat.h"
#include "surface/eval/UDF.h"

DEFINE_bool(CPU_PROF, false, "Enable cpu profile");
DEFINE_string(PROF_FILE, "/tmp/cpu_prof.out", "CPU profile output file");

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::memory::EvaledBlock;
using nebula::memory::keyed::HashFlat;
using nebula::surface::RowCursorPtr;
using nebula::surface::SchemaRow;
using nebula::surface::eval::BlockEval;
using nebula::surface::eval::EvalContext;
using nebula::surface::eval::ScriptData;
using nebula::type::Kind;
using nebula::type::Schema;

RowCursorPtr compute(
  const std::string& planId,
  const EvaledBlock& data,
  const nebula::execution::BlockPhase& phase) {
  // TODO(cao) - SamplesExecutor seems having trouble evaluating scripts
  // see TestQuery: ApiTest.TestScriptSamples for repro
  if (phase.hasAggregation() || phase.hasScript()) {
    return std::make_shared<BlockExecutor>(planId, data, phase);
  }

  return std::make_shared<SamplesExecutor>(planId, data, phase);
}

void BlockExecutor::compute() {
  // instrumentation for profiling on compute branch
  if (N_UNLIKELY(FLAGS_CPU_PROF)) {
    ProfilerStart(FLAGS_PROF_FILE.c_str());
  }

  // process every single row and put result in HashFlat
  auto accessor = data_.first->makeAccessor();
  const auto& fields = plan_.fields();
  const auto& filter = plan_.filter();

  // build context and computed row associated with this context
  auto ctx = std::make_shared<EvalContext>(plan_.cacheEval(), makeScriptData(plan_));

  // predicate pushdown evaluation on block metadata
  auto result = data_.second;

  // if all rows needed, we don't need to evaluate row by row
  bool scanAll = result == BlockEval::ALL;

  auto fieldMap = SchemaRow::name2index(plan_.outputSchema());
  ComputedRow cr(fieldMap, plan_.fields(), ctx);
  result_ = std::make_unique<HashFlat>(plan_.outputSchema(), fields);

  // we want to evaluate here for the whole block before we go to iterations of computing
  // by leveraging its metadata including histogram, bloom filter, dictionary etc.
  // the result we would like to see is:
  // ALL ROWS - all rows needed
  // NONE ROWS - no rows needed
  // PARTIAL ROWS - not sure, need to scan.
  // what the interface look like? filter.eval(data_.), we need an interface to wrap batch object
  // and provide methods like "probably? range()? in()?"
  // and these methods will be used in each individual ValueEval and give result like above.
  // So we need an special operator to be implemented to have this function
  const auto blockRows = data_.first->getRows();
  for (size_t i = 0; i < blockRows; ++i) {
    ctx->reset(accessor->seek(i));

    // if not fullfil the condition
    // ignore valid here - if system can't determine how to act on NULL value
    // we don't know how to make decision here too
    if (N_LIKELY(!scanAll)) {
      if (!filter.eval<bool>(*ctx).value_or(false)) {
        continue;
      }
    }

    // flat compute every new value of each field and set to corresponding column in flat
    result_->update(cr);
  }

  // after the compute flat should contain all the data we need.
  index_ = 0;
  size_ = result_->getRows();
  LOG(INFO) << "block executor: plan=" << planId_
            << ", output rows=" << size_
            << ", block eval=" << (int)result
            << ", block rows=" << blockRows;

  if (N_UNLIKELY(FLAGS_CPU_PROF)) {
    ProfilerStop();
  }
}

void SamplesExecutor::compute() {
  // build context and computed row associated with this context
  const auto blockRows = data_.first->getRows();
  samples_ = std::make_unique<ReferenceRows>(plan_, *data_.first);

  // after the compute flat should contain all the data we need.
  index_ = 0;
  size_ = samples_->size();

  LOG(INFO) << "samples executor: plan=" << planId_
            << ", output rows=" << size_
            << ", block rows=" << blockRows;
}

} // namespace core
} // namespace execution
} // namespace nebula