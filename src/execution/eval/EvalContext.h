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
  // if evaluated, the first item indicating if it's evaluated as NULL, ignoring offset and length in the data buffer.
  // otherwise using {offset, length} to fetch the data desired from buffer.
  using Meta = std::pair<bool, std::any>;

public:
  // change reference to row data, clear all cache.
  // and start build cache based on evaluation signautre.
  void reset(const nebula::surface::RowData&);

  // evaluate a value eval object in current context and return value reference.
  template <typename T>
  const T& eval(const nebula::execution::eval::ValueEval& ve, bool& valid) {
    const auto& sign = ve.signature();

    // if in evaluated list
    auto itr = map_.find(sign);
    if (itr != map_.end()) {
      valid = itr->second.first;
      if (!valid) {
        return nebula::type::TypeDetect<T>::value;
      }

      return std::any_cast<const T&>(itr->second.second);
    }

    N_ENSURE_NOT_NULL(row_, "reference a row object before evaluation.");
    const auto value = ve.eval<T>(*row_, valid);
    map_[sign] = Meta{ valid, value };
    if (!valid) {
      return nebula::type::TypeDetect<T>::value;
    }

    return std::any_cast<const T&>(map_.at(sign).second);
  }

private:
  const nebula::surface::RowData* row_;
  // a signature keyed tuples indicating if this expr evaluated (having entry) or not.
  std::unordered_map<std::string, Meta> map_;
};

} // namespace eval
} // namespace execution
} // namespace nebula