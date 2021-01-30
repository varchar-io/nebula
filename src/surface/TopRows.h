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

#pragma once

#include <algorithm>
#include <queue>
#include "DataSurface.h"
#include "common/Cursor.h"

/**
 * Implement a sorting and top cutoff wrapper for row cursor to return all values
 */
namespace nebula {
namespace surface {
class TopRows : public RowCursor {
  using Less = std::function<bool(const std::unique_ptr<RowData>&, const std::unique_ptr<RowData>&)>;

public:
  // top rows will pick sorted top N rows, if max is 0, it means we don't apply limit and return all
  TopRows(const RowCursorPtr& rows, size_t max, Less less)
    : RowCursor(max == 0 ? rows->size() : std::min(max, rows->size())),
      heap_(rows->size()),
      rows_{ rows } {
    // build heap / priority queue
    if (less != nullptr) {
      compare_ = [this, func = std::move(less)](size_t left, size_t right) -> bool {
        return func(rows_->item(left), rows_->item(right));
      };

      // fill the vector with its index
      std::iota(heap_.begin(), heap_.end(), 0);
      std::make_heap(heap_.begin(), heap_.end(), compare_);
    }
  }

  // TODO(cao) - might be too expensive if there are many items/rows to iterate on
  virtual const RowData& next() override {
    // stop condition
    index_++;

    // no need heap
    if (!compare_) {
      return rows_->next();
    }

    // fetch the top one and return its value
    std::pop_heap(heap_.begin(), heap_.end(), compare_);
    auto top = heap_.back();
    heap_.pop_back();

    current_ = rows_->item(top);
    return *current_;
  }

  virtual std::unique_ptr<RowData> item(size_t) const override {
    throw NException("Top rows do not support random access.");
  }

private:
  std::vector<size_t> heap_;
  RowCursorPtr rows_;
  std::function<bool(size_t, size_t)> compare_;

  std::unique_ptr<RowData> current_;
}; // namespace surface

} // namespace surface
} // namespace nebula