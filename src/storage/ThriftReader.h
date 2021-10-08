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

#include "RowParser.h"
#include "common/Hash.h"
#include "meta/Table.h"
/**
 * A thrift object reader
 * Similar to JsonReader, we can extend a RowCursor from this single object reader to read a whole binary file.
 */
namespace nebula {
namespace storage {

// Represents a reusable thrift object.
class ThriftRow final : public RowParser {
public:
  ThriftRow(const std::unordered_map<std::string, uint32_t>& columnsMap)
    : hasTime_{ false } {
    // reverse mapping of name -> id
    for (auto itr = columnsMap.begin(); itr != columnsMap.end(); ++itr) {
      fields_.emplace(itr->second, itr->first);
      if (itr->first == nebula::meta::Table::TIME_COLUMN) {
        hasTime_ = true;
      }
    }
  }

  ~ThriftRow() = default;

public:
  virtual inline bool hasTime() const noexcept override {
    return hasTime_;
  }

  virtual bool parse(void*, size_t, nebula::memory::FlatRow&) noexcept override;

  virtual void nullify(nebula::memory::FlatRow& row, size_t time) noexcept override {
    // write everything a null if encoutering an invalid message
    row.reset();
    if (!hasTime_) {
      row.write(nebula::meta::Table::TIME_COLUMN, time);
    }

    for (auto itr = fields_.cbegin(); itr != fields_.cend(); ++itr) {
      row.writeNull(itr->second);
    }
  }

private:
  bool hasTime_;
  // reverse the fields mapping from field ID -> name
  nebula::common::unordered_map<uint32_t, std::string> fields_;
};
} // namespace storage
} // namespace nebula