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

#include <glog/logging.h>
#include <memory>
#include <vector>

/**
 * A cursor template that help iterating a container.
 * (should we use std::iterator instead?)
 */
namespace nebula {
namespace common {
/**
 * A cursor type is like a iterator
 */
template <typename T>
class Cursor {
public:
  Cursor(size_t size) : index_{ 0 }, size_{ size } {}
  virtual ~Cursor() = default;

  inline bool hasNext() const {
    return index_ < size_;
  }

  // TODO(cao) - might be too expensive if there are many items/rows to iterate on
  virtual const T& next() = 0;

  // a const interface return an unique ptr for secure randome access
  virtual std::unique_ptr<T> item(size_t) const = 0;

  inline size_t size() const {
    return size_;
  }

protected:
  size_t index_;
  size_t size_;
};

template <typename T>
class CompositeCursor : public Cursor<T> {
  using CursorPtr = std::shared_ptr<Cursor<T>>;

public:
  CompositeCursor() : Cursor<T>(0), cursor_{ 0 }, innerIndex_{ 0 } {}
  virtual ~CompositeCursor() = default;

  void combine(CursorPtr another) {
    if (another->size() == 0) {
      return;
    }

    this->size_ += another->size();

    sizes_.push_back(this->size_);
    lists_.push_back(another);
  }

  // TODO(cao) - might be too expensive if there are many items/rows to iterate on
  virtual const T& next() override {
    // boundary check
    {
      auto& current = lists_[cursor_];
      if (innerIndex_ == current->size()) {
        ++cursor_;
        innerIndex_ = 0;
      }
    }

    // always increment for global check
    this->index_++;
    innerIndex_++;
    return lists_[cursor_]->next();
  }

  virtual std::unique_ptr<T> item(size_t index) const override {
    // TODO(cao) - need optimize this for loop out by pre-computing the index and inside offset
    size_t i = 0;
    for (size_t size = sizes_.size(); i < size; ++i) {
      if (index < sizes_.at(i)) {
        break;
      }
    }

    // "i" is the cursor we're going to get data from
    auto offset = i == 0 ? 0 : sizes_.at(i - 1);
    return lists_.at(i)->item(index - offset);
  }

private:
  size_t cursor_;
  size_t innerIndex_;
  std::vector<size_t> sizes_;
  std::vector<CursorPtr> lists_;
};

} // namespace common
} // namespace nebula