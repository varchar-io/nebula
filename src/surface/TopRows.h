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

#include <algorithm>
#include <queue>
#include "DataSurface.h"
#include "common/Cursor.h"

/**
 * Implement a sorting and top cutoff wrapper for row cursor to return all values
 */
namespace nebula {
namespace surface {
class TopRows : public nebula::common::Cursor<RowData> {
  using Less = std::function<bool(const RowData&, const RowData&)>;

public:
  TopRows(const RowCursor& rows, size_t max, const Less& less)
    : nebula::common::Cursor<RowData>(std::min(max, rows->size())),
      heap_(rows->size()),
      rows_{ rows },
      less_{ less } {
    // build heap / priority queue
    if (less_ != nullptr) {
      // fill the vector with its index
      std::iota(heap_.begin(), heap_.end(), 0);
      std::make_heap(heap_.begin(), heap_.end(), [this](size_t left, size_t right) -> bool {
        return less_(rows_->item(left), rows_->item(right));
      });
    }
  }

  // TODO(cao) - might be too expensive if there are many items/rows to iterate on
  virtual const RowData& next() override {
    // stop condition
    index_++;

    // no need heap
    if (less_ == nullptr) {
      return rows_->next();
    }

    // fetch the top one and return its value
    std::pop_heap(heap_.begin(), heap_.end(), [this](size_t left, size_t right) -> bool {
      return less_(rows_->item(left), rows_->item(right));
    });
    auto top = heap_.back();
    heap_.pop_back();

    return rows_->item(top);
  }

  virtual const RowData& item(size_t) const override {
    throw NException("To rows do not support random access.");
  }

private:
  std::vector<size_t> heap_;
  RowCursor rows_;
  Less less_;
};

} // namespace surface
} // namespace nebula