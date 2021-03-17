/*
 * Copyright 2017-present varchar.io
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

#include "memory/FlatRow.h"

/**
 * Define a row parser interface so that different format can implement its own.
 */
namespace nebula {
namespace storage {

// ROW parser is an interface to provide formatted data parsing from given buffer
class RowParser {
public:
  virtual ~RowParser() = default;

  // has time colummn included already or not
  virtual bool hasTime() const noexcept = 0;

  // parse a buffer into given flatrow
  virtual bool parse(void*, size_t, nebula::memory::FlatRow&) noexcept = 0;

  // client can choose to ask parser to fill all null values with default time to 0
  virtual void nullify(nebula::memory::FlatRow&, size_t = 0) noexcept = 0;
};

} // namespace storage
} // namespace nebula