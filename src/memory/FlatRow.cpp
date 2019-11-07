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

#include "FlatRow.h"

namespace nebula {
namespace memory {

using nebula::surface::ListData;
using nebula::surface::MapData;

bool FlatRow::isNull(const std::string& field) const {
  auto offset = meta_.at(field);
  // it is null if value at current offset is 0
  return slice_.read<int8_t>(offset) == 0;
}

#define READ_SCALAR(RT, NAME)                        \
  RT FlatRow::NAME(const std::string& field) const { \
    auto offset = meta_.at(field);                   \
    return slice_.read<RT>(offset + 1);              \
  }

READ_SCALAR(bool, readBool)
READ_SCALAR(int8_t, readByte)
READ_SCALAR(int16_t, readShort)
READ_SCALAR(int32_t, readInt)
READ_SCALAR(int64_t, readLong)
READ_SCALAR(float, readFloat)
READ_SCALAR(double, readDouble)
READ_SCALAR(int128_t, readInt128)

#undef READ_SCALAR

std::string_view FlatRow::readString(const std::string& field) const {
  auto offset = meta_.at(field);

  // type check: we can check first byte is string flag
  return slice_.read(offset + 5, slice_.read<int32_t>(offset + 1));
}

std::unique_ptr<ListData> FlatRow::readList(const std::string& field) const {
  auto offset = meta_.at(field);
  auto header = slice_.read<int16_t>(offset);
  auto size = slice_.read<int32_t>(offset + 2);

  // the base offset passed in is data offset without header.
  return std::make_unique<FlatList>(size, header, offset + 6, slice_);
}

std::unique_ptr<MapData> FlatRow::readMap(const std::string&) const {
  throw NException("Map not supported in flat row");
}

//////////////////////////////////////////////////////////////////////////////////////////////////
bool FlatList::isNull(size_t) const {
  // TODO(cao): Currently not supporting nulls in list/vector. Empty string is supported.
  return false;
}

#define READ_SCALAR(RT, NAME)                     \
  RT FlatList::NAME(size_t index) const {         \
    constexpr auto width = sizeof(RT);            \
    N_ENSURE_EQ(type_, width, "not list of #RT"); \
    auto offset = base_ + index * type_;          \
    return slice_.read<RT>(offset);               \
  }

READ_SCALAR(bool, readBool)
READ_SCALAR(int8_t, readByte)
READ_SCALAR(int16_t, readShort)
READ_SCALAR(int32_t, readInt)
READ_SCALAR(int64_t, readLong)
READ_SCALAR(float, readFloat)
READ_SCALAR(double, readDouble)
READ_SCALAR(int128_t, readInt128)

#undef READ_SCALAR

std::string_view FlatList::readString(size_t index) const {
  N_ENSURE_EQ(type_, FlatRow::STRING_FLAG, "expect string list");

  // offset to read length of this string
  auto pos = base_ + index * 4;
  auto offset = slice_.read<int32_t>(pos);
  auto len = slice_.read<int32_t>(pos + 4) - offset;
  N_ENSURE(len >= 0, "offset should be incremental.");

  return slice_.read(offset, len);
}

} // namespace memory
} // namespace nebula