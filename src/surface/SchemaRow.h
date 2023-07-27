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

#include "DataSurface.h"
#include "common/Hash.h"
#include "type/Type.h"

namespace nebula {
namespace surface {
/**
 * A schema row is a row data interface with known schema
 * And it will forward all its name based calls into index based calls
 */
using Name2Index = nebula::common::unordered_map<std::string, size_t>;
class SchemaRow : public RowData {
public:
  explicit SchemaRow(const Name2Index& map) : fields_{ map } {}
  virtual ~SchemaRow() = default;

public:
  // we can move this utility to `type` package for more widely usage
  inline static Name2Index name2index(const nebula::type::Schema& schema) noexcept {
    // build a field name to data node
    const auto numCols = schema->size();
    Name2Index map;
    map.reserve(numCols);

    for (size_t i = 0; i < numCols; ++i) {
      auto f = schema->childType(i);
      map[f->name()] = i;
    }

    return map;
  }

public:
#define INDEX_FUNC(T, F) \
  virtual T F(IndexType) const override = 0;

#define FORWARD_NAME_2_INDEX(T, F)               \
  T F(const std::string& field) const override { \
    return F(fields_.at(field));                 \
  }

#define COMPOSE(T, F) \
  INDEX_FUNC(T, F)    \
  FORWARD_NAME_2_INDEX(T, F)

  COMPOSE(bool, isNull)
  COMPOSE(bool, readBool)
  COMPOSE(int8_t, readByte)
  COMPOSE(int16_t, readShort)
  COMPOSE(int32_t, readInt)
  COMPOSE(int64_t, readLong)
  COMPOSE(float, readFloat)
  COMPOSE(double, readDouble)
  COMPOSE(int128_t, readInt128)
  COMPOSE(std::string_view, readString)
  COMPOSE(std::unique_ptr<nebula::surface::ListData>, readList)
  COMPOSE(std::unique_ptr<nebula::surface::MapData>, readMap)

#undef COMPOSE
#undef FORWARD_NAME_2_INDEX
#undef INDEX_FUNC

private:
  const Name2Index& fields_;
};
} // namespace surface
} // namespace nebula