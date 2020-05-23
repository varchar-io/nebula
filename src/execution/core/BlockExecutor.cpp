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

#include "BlockExecutor.h"

#include <unordered_set>

#include "AggregationMerge.h"
#include "memory/keyed/HashFlat.h"
#include "surface/eval/UDF.h"

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::memory::EvaledBlock;
using nebula::memory::keyed::HashFlat;
using nebula::surface::RowCursorPtr;
using nebula::surface::eval::BlockEval;
using nebula::surface::eval::EvalContext;
using nebula::type::Kind;
using nebula::type::Schema;

RowCursorPtr compute(const EvaledBlock& data, const nebula::execution::BlockPhase& plan) {
  if (plan.hasAggregation()) {
    return std::make_shared<BlockExecutor>(data, plan);
  }

  return std::make_shared<SamplesExecutor>(data, plan);
}

void BlockExecutor::compute() {
  // process every single row and put result in HashFlat
  auto accessor = data_.first->makeAccessor();
  const auto& fields = plan_.fields();
  const auto& filter = plan_.filter();

  // build context and computed row associated with this context
  auto hasCustom = plan_.hasCustom();
  auto ctx = std::make_shared<EvalContext>(plan_.cacheEval(), hasCustom);

  // custom evaluation requires input schema
  if (hasCustom) {
    ctx->setSchema(plan_.inputSchema());
  }

  // predicate pushdown evaluation on block metadata
  auto result = data_.second;

  // if all rows needed, we don't need to evaluate row by row
  bool scanAll = result == BlockEval::ALL;

  ComputedRow cr(plan_, ctx);
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

  for (size_t i = 0, size = data_.first->getRows(); i < size; ++i) {
    ctx->reset(accessor->seek(i));

    // if not fullfil the condition
    // ignore valid here - if system can't determine how to act on NULL value
    // we don't know how to make decision here too
    bool valid = true;
    if (!scanAll && !ctx->eval<bool>(filter, valid)) {
      continue;
    }

    // flat compute every new value of each field and set to corresponding column in flat
    result_->update(cr);
  }

  // after the compute flat should contain all the data we need.
  index_ = 0;
  size_ = result_->getRows();
}

void SamplesExecutor::compute() {
  // build context and computed row associated with this context
  samples_ = std::make_unique<ReferenceRows>(plan_, *data_.first);

  // after the compute flat should contain all the data we need.
  index_ = 0;
  size_ = samples_->size();
}

} // namespace core
} // namespace execution
} // namespace nebula