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

#include "ComputedRow.h"
#include "common/Cursor.h"
#include "execution/ExecutionPlan.h"
#include "memory/Batch.h"
#include "surface/DataSurface.h"
#include "surface/eval/ValueEval.h"

namespace nebula {
namespace execution {
namespace core {

static std::shared_ptr<nebula::surface::eval::ScriptData> makeScriptData(const BlockPhase& plan) noexcept {
  if (plan.hasScript()) {
    return std::make_shared<nebula::surface::eval::ScriptData>(plan.inputSchema(), plan.customs());
  }

  return {};
}

class ReferenceRows : public nebula::surface::RowCursor {
public:
  explicit ReferenceRows(const BlockPhase& plan, const nebula::memory::Batch& data)
    : nebula::surface::RowCursor(0),
      plan_{ plan },
      fieldMap_{ nebula::surface::SchemaRow::name2index(plan_.outputSchema()) },
      scriptData_{ makeScriptData(plan) },
      data_{ data },
      accessor_{ data.makeAccessor() },
      ctx_{ std::make_shared<nebula::surface::eval::EvalContext>(plan.cacheEval(), scriptData_) },
      filter_{ plan.filter() },
      runtime_{ fieldMap_, plan_.fields(), ctx_ } {
    // populate all reference rows
    for (size_t i = 0, size = data_.getRows(); i < size; ++i) {
      // if we have enough samples, just return
      if (check(i) >= plan_.top()) {
        break;
      }
    }
  }

  virtual ~ReferenceRows() = default;

  virtual const nebula::surface::RowData& next() override {
    auto row = rows_.at(index_++);
    ctx_->reset(accessor_->seek(row));
    return runtime_;
  }

  virtual std::unique_ptr<nebula::surface::RowData> item(size_t index) const override {
    // map external row to internal row due to filter
    auto row = rows_.at(index);

    auto a = data_.makeAccessor();
    a->seek(row);

    // a computed row with a wrapped row only
    return std::make_unique<ComputedRow>(
      fieldMap_, plan_.fields(), std::make_shared<nebula::surface::eval::EvalContext>(std::move(a), scriptData_));
  }

private:
  size_t check(size_t index) {
    ctx_->reset(accessor_->seek(index));

    // if not fullfil the condition
    // ignore valid here - if system can't determine how to act on NULL value
    // we don't know how to make decision here too
    if (!filter_.eval<bool>(*ctx_).value_or(false)) {
      return size_;
    }

    rows_.push_back(index);
    return ++size_;
  }

private:
  const BlockPhase& plan_;
  const nebula::surface::Name2Index fieldMap_;
  const std::shared_ptr<nebula::surface::eval::ScriptData> scriptData_;
  const nebula::memory::Batch& data_;
  std::unique_ptr<nebula::memory::RowAccessor> accessor_;
  std::shared_ptr<nebula::surface::eval::EvalContext> ctx_;
  const nebula::surface::eval::ValueEval& filter_;
  ComputedRow runtime_;

  // filtered row number
  std::vector<size_t> rows_;
};

} // namespace core
} // namespace execution
} // namespace nebula