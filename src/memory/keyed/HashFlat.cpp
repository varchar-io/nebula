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

#include "HashFlat.h"

namespace nebula {
namespace memory {
namespace keyed {

bool HashFlat::update(const nebula::surface::RowData& row, const UpdateCallback& callback) {
  // add it as a new row first
  this->add(row);

  auto newRow = getRows() - 1;
  auto hValue = hash(newRow, keys_);
  Key key{ *this, newRow, hValue };
  auto itr = rowKeys_.find(key);
  if (itr != rowKeys_.end()) {
    // copy the new row data into target
    copy(newRow, std::get<1>(*itr), callback, keys_);

    // rollback the new added row
    rollback();

    return true;
  }

  // a new key
  rowKeys_.insert(key);
  return false;
}

} // namespace keyed
} // namespace memory
} // namespace nebula