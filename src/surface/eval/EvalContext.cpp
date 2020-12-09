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

#include "EvalContext.h"

/**
 * Value evaluation context.
 * It provides same expression evaluation cache.
 * Every evaluation expression has a unique signature. 
 * Also it provides reference return rather than value return comparing to RowData interface. 
 */
namespace nebula {
namespace surface {
namespace eval {

// reset to a new row
void EvalContext::reset(const nebula::surface::Accessor& row) {
  // std::addressof ?
  this->row_ = &row;

  if (UNLIKELY(cache_ != nullptr)) {
    // clear t he evaluation map
    cache_.reset();
  }
}

} // namespace eval
} // namespace surface
} // namespace nebula