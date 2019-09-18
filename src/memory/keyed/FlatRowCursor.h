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
#include "surface/DataSurface.h"

namespace nebula {
namespace memory {
namespace keyed {

class FlatRowCursor : public nebula::surface::RowCursor {
public:
  explicit FlatRowCursor(std::unique_ptr<FlatBuffer> flat)
    : nebula::surface::RowCursor(flat->getRows()),
      flat_{ std::move(flat) } {}

  virtual ~FlatRowCursor() = default;

  virtual const nebula::surface::RowData& next() override {
    return flat_->row(index_++);
  }

  virtual std::unique_ptr<nebula::surface::RowData> item(size_t index) const override {
    return flat_->crow(index);
  }

  inline std::unique_ptr<nebula::memory::keyed::FlatBuffer> takeResult() {
    std::unique_ptr<FlatBuffer> temp = nullptr;
    std::swap(temp, flat_);
    N_ENSURE_NOT_NULL(temp, "should have swapped a solid buffer.");
    return temp;
  }

private:
  std::unique_ptr<FlatBuffer> flat_;
};

} // namespace keyed
} // namespace memory
} // namespace nebula