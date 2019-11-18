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

using nebula::memory::keyed::HashFlat;
using nebula::surface::RowCursorPtr;
using nebula::surface::eval::EvalContext;
using nebula::type::Kind;

RowCursorPtr compute(const nebula::memory::Batch& data, const nebula::execution::BlockPhase& plan) {
  if (plan.hasAggregation()) {
    return std::make_shared<BlockExecutor>(data, plan);
  }

  return std::make_shared<SamplesExecutor>(data, plan);
}

void BlockExecutor::compute() {
  // process every single row and put result in HashFlat
  auto accessor = data_.makeAccessor();
  const auto& fields = plan_.fields();
  const auto& filter = plan_.filter();

  // build context and computed row associated with this context
  EvalContext ctx(plan_.cacheEval());
  ComputedRow cr(plan_.outputSchema(), ctx, fields);
  result_ = std::make_unique<HashFlat>(plan_.outputSchema(), plan_.keys(), fields);

  for (size_t i = 0, size = data_.getRows(); i < size; ++i) {
    ctx.reset(accessor->seek(i));

    // if not fullfil the condition
    // ignore valid here - if system can't determine how to act on NULL value
    // we don't know how to make decision here too
    bool valid = true;
    if (!ctx.eval<bool>(filter, valid)) {
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
  samples_ = std::make_unique<ReferenceRows>(plan_, data_);

  for (size_t i = 0, size = data_.getRows(); i < size; ++i) {
    // if we have enough samples, just return
    if (samples_->check(i) >= plan_.top()) {
      break;
    }
  }

  // after the compute flat should contain all the data we need.
  index_ = 0;
  size_ = samples_->size();
}

} // namespace core
} // namespace execution
} // namespace nebula