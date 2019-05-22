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
#include <unordered_map>
#include "ValueEval.h"
#include "common/Memory.h"
#include "surface/DataSurface.h"
#include "type/Type.h"

/**
 * Value evaluation context.
 * It provides same expression evaluation cache.
 * Every evaluation expression has a unique signature. 
 * Also it provides reference return rather than value return comparing to RowData interface. 
 */
namespace nebula {
namespace execution {
namespace eval {

class EvalContext {
public:
  EvalContext() : slice_{ 1024 } {
    cursor_ = 1;
  }
  virtual ~EvalContext() = default;

  // change reference to row data, clear all cache.
  // and start build cache based on evaluation signautre.
  void reset(const nebula::surface::RowData&);

  // evaluate a value eval object in current context and return value reference.
  template <typename T>
  const T& eval(const nebula::execution::eval::ValueEval& ve, bool& valid) {
    auto sign = ve.signature();

    // if in evaluated list
    auto itr = map_.find(sign);
    if (itr != map_.end()) {
      auto offset = itr->second;
      valid = offset > 0;
      if (!valid) {
        return nebula::type::TypeDetect<T>::value;
      }

      return slice_.read<T>(offset);
    }

    N_ENSURE_NOT_NULL(row_, "reference a row object before evaluation.");
    const auto value = ve.eval<T>(*row_, valid);
    if (!valid) {
      map_[sign] = 0;
      return nebula::type::TypeDetect<T>::value;
    }
    const auto offset = cursor_;
    map_[sign] = offset;
    cursor_ += slice_.write<T>(cursor_, value);

    return slice_.read<T>(offset);
  }

private:
  const nebula::surface::RowData* row_;
  // a signature keyed tuples indicating if this expr evaluated (having entry) or not.
  std::unordered_map<std::string_view, size_t> map_;
  // layout all cached data, when reset, just move the cursor to 1
  // 0 is reserved, any evaluated data points to 0 if it is NULL
  size_t cursor_;
  nebula::common::PagedSlice slice_;
};

} // namespace eval
} // namespace execution
} // namespace nebula