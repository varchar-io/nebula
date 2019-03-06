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

#include "Batch.h"
#include <numeric>

namespace nebula {
namespace memory {

using nebula::surface::RowData;
using nebula::type::TreeBase;

// add a row into current batch
// and return row ID of this row in current batch
// thread-safe on sync guarded - exclusive lock?
uint32_t Batch::add(const RowData& row) {
  // read data from row data and save it to batch
  auto result = data_->append(row);

  // record the row size
  LOG(INFO) << "Total row size  = " << result;

  return rows_++;
}

// random access to a row - may require internal seek
RowData& Batch::row(uint32_t rowId) {
  return cursor_;
}

} // namespace memory
} // namespace nebula