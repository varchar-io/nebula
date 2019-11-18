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

class ReferenceRows : public nebula::surface::RowCursor {
public:
  explicit ReferenceRows(const BlockPhase& plan, const nebula::memory::Batch& data)
    : nebula::surface::RowCursor(0),
      data_{ data },
      accessor_{ data.makeAccessor() },
      ctx_{ plan.cacheEval() },
      filter_{ plan.filter() },
      runtime_{ plan.outputSchema(), ctx_, plan.fields() } {}

  virtual ~ReferenceRows() = default;

  size_t check(size_t index) {
    ctx_.reset(accessor_->seek(index));

    // if not fullfil the condition
    // ignore valid here - if system can't determine how to act on NULL value
    // we don't know how to make decision here too
    bool valid = true;
    if (!ctx_.eval<bool>(filter_, valid) || !valid) {
      return size_;
    }

    rows_.push_back(index);
    return ++size_;
  }

  virtual const nebula::surface::RowData& next() override {
    auto row = rows_.at(index_++);
    ctx_.reset(accessor_->seek(row));
    return runtime_;
  }

  virtual std::unique_ptr<nebula::surface::RowData> item(size_t index) const override {
    auto row = data_.makeAccessor();
    row->seek(index);
    return row;
  }

private:
  std::vector<size_t> rows_;
  const nebula::memory::Batch& data_;
  std::unique_ptr<nebula::memory::RowAccessor> accessor_;
  nebula::surface::eval::EvalContext ctx_;
  const nebula::surface::eval::ValueEval& filter_;
  ComputedRow runtime_;
};

} // namespace core
} // namespace execution
} // namespace nebula