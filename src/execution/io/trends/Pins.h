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

#include "meta/MetaService.h"
#include "meta/NNode.h"
#include "meta/Table.h"
#include "type/Serde.h"

/**
 * This module is to build special case for trends which has some hard coded data.
 * Will be deleted after it's done with pilot run.
 */
namespace nebula {
namespace execution {
namespace io {
namespace trends {

class PinsTable : public nebula::meta::Table {
  static constexpr auto NAME = "pin.pins";

public:
  PinsTable() : Table(NAME) {
    // TODO(cao) - let's make date as a number
    // id bigint,
    // board_id bigint,
    // user_id bigint,
    // created_at string => map to _time_ in "Y-M-D H:M:S" format,
    // title string,
    // details string,
    // image_signature string,
    // repins bigint
    schema_ = nebula::type::TypeSerializer::from(
      "ROW<_time_:long, id:long, board_id:long, user_id:long, title:string, details:string, signature:string, repins:long>");
  }

  virtual ~PinsTable() = default;

  // load pins data in current process
  // load pins data as max
  void load(size_t max = 0);
};

} // namespace trends
} // namespace io
} // namespace execution
} // namespace nebula