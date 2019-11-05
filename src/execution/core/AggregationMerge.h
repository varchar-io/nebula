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

#include "common/Folly.h"
#include "execution/eval/ValueEval.h"
#include "memory/keyed/HashFlat.h"
#include "surface/DataSurface.h"
#include "type/Type.h"

/**
 * A logic wrapper to merge aggregation results shared by aggregators (Node Executor or Server Executor)
 */
namespace nebula {
namespace execution {
namespace core {

using UpdateCallbackType = std::function<bool(size_t column, nebula::type::Kind kind, void* ov, void* nv, void* value)>;
UpdateCallbackType updateCallback(const std::unordered_set<size_t>&,
                                  const std::vector<std::unique_ptr<nebula::execution::eval::ValueEval>>&);

nebula::surface::RowCursorPtr merge(
  folly::ThreadPoolExecutor&,
  const nebula::type::Schema,
  const std::vector<size_t>&,
  const std::vector<std::unique_ptr<nebula::execution::eval::ValueEval>>&,
  const bool,
  const std::vector<folly::Try<nebula::surface::RowCursorPtr>>&);
} // namespace core
} // namespace execution
} // namespace nebula