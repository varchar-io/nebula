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
#include "glog/logging.h"
#include "surface/DataSurface.h"

/**
 * Define nebula execution runtime.
 * Every single node will receive the plan and know what execution unit it needs to run on top of its data range.
 */
namespace nebula {
namespace execution {
// An execution plan that can be serialized and passed around
// protobuf?
class ExecutionPlan {
public:
  void display() const;
  nebula::common::Cursor<nebula::surface::RowData&> execute(const std::string& server);
};
} // namespace execution
} // namespace nebula