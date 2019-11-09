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
#include "ReferenceRows.h"
#include "execution/ExecutionPlan.h"
#include "execution/eval/ValueEval.h"
#include "memory/Batch.h"
#include "memory/keyed/HashFlat.h"
#include "surface/DataSurface.h"

/**
 * Block executor is used to apply computing on a single block
 */
namespace nebula {
namespace execution {
namespace core {
/**
 * Block executor defines the smallest compute unit and itself is a row data cursor
 * TODO(cao): consider merging FlatRowCursor here for module reuse.
 * Till then, BlockExecutor will return wrapped FlatRowCursor instead of itself as row cursor.
 */
class BlockExecutor : public nebula::surface::RowCursor {

public:
  BlockExecutor(const nebula::memory::Batch& data, const nebula::execution::BlockPhase& plan)
    : nebula::surface::RowCursor(0), data_{ data }, plan_{ plan } {
    // compute will finish the compute and fill the data state in
    this->compute();
  }
  virtual ~BlockExecutor() = default;

  inline virtual const nebula::surface::RowData& next() override {
    return result_->row(index_++);
  }

  inline virtual std::unique_ptr<nebula::surface::RowData> item(size_t index) const override {
    return result_->crow(index);
  }

  inline std::unique_ptr<nebula::memory::keyed::FlatBuffer> takeResult() {
    auto temp = std::move(result_);
    result_ = nullptr;
    return temp;
  }

private:
  void compute();

private:
  const nebula::memory::Batch& data_;
  const nebula::execution::BlockPhase& plan_;
  std::unique_ptr<nebula::memory::keyed::HashFlat> result_;
};

class SamplesExecutor : public nebula::surface::RowCursor {
public:
  SamplesExecutor(const nebula::memory::Batch& data, const nebula::execution::BlockPhase& plan)
    : nebula::surface::RowCursor(0), data_{ data }, plan_{ plan } {
    // compute will finish the compute and fill the data state in
    this->compute();
  }
  virtual ~SamplesExecutor() = default;

  inline virtual const nebula::surface::RowData& next() override {
    index_++;
    return samples_->next();
  }
  
  inline virtual std::unique_ptr<nebula::surface::RowData> item(size_t index) const override {
    return samples_->item(index);
  }

private:
  void compute();

private:
  const nebula::memory::Batch& data_;
  const nebula::execution::BlockPhase& plan_;
  std::unique_ptr<ReferenceRows> samples_;
};

nebula::surface::RowCursorPtr compute(const nebula::memory::Batch&, const nebula::execution::BlockPhase&);

} // namespace core
} // namespace execution
} // namespace nebula