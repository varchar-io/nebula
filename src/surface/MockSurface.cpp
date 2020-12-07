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

#include "MockSurface.h"
#include <cstdint>
#include "common/Evidence.h"
#include "fmt/format.h"

/**
 * Mock some data and test the interface with an app
 */
namespace nebula {
namespace surface {

//------------ Mock Row
// All intrefaces - string type has RVO, copy elision optimization
bool MockRowData::isNull(const std::string&) const {
  // 10% nulls
  // TODO(cao) - revisit nulls handling in both batch and flatbuffer
  // Turn on this and run test ExecutionTest.TestRowCursorSerde
  // return rand_() < 0.1;
  return false;
}

bool MockRowData::readBool(const std::string&) const {
  // half of trues
  return rand_() < 0.5;
}

int8_t MockRowData::readByte(const std::string&) const {
  return std::numeric_limits<int8_t>::max() * rand_();
}

int16_t MockRowData::readShort(const std::string&) const {
  return std::numeric_limits<int16_t>::max() * rand_();
}

int32_t MockRowData::readInt(const std::string&) const {
  return std::numeric_limits<int32_t>::max() * rand_();
}

int64_t MockRowData::readLong(const std::string&) const {
  return std::numeric_limits<int64_t>::max() * rand_();
}

float MockRowData::readFloat(const std::string&) const {
  return rand_();
}

double MockRowData::readDouble(const std::string&) const {
  return rand_();
}

int128_t MockRowData::readInt128(const std::string&) const {
  return std::numeric_limits<int128_t>::max() * rand_();
}

std::string_view MockRowData::readString(const std::string&) const {
  return strings_.at(rand_() * strings_.size());
}

// compound types
std::unique_ptr<ListData> MockRowData::readList(const std::string&) const {
  // copy elision or seg fault?
  return std::make_unique<MockListData>(4, seed_);
}

std::unique_ptr<MapData> MockRowData::readMap(const std::string&) const {
  return std::make_unique<MockMapData>(2, seed_);
}

//------------ Mock Row
// All intrefaces - string type has RVO, copy elision optimization
bool MockRowData::isNull(IndexType) const {
  // 10% nulls
  return rand_() < 0.1;
}

bool MockRowData::readBool(IndexType) const {
  // half of trues
  return rand_() < 0.5;
}

int8_t MockRowData::readByte(IndexType) const {
  return std::numeric_limits<int8_t>::max() * rand_();
}

int16_t MockRowData::readShort(IndexType) const {
  return std::numeric_limits<int16_t>::max() * rand_();
}

int32_t MockRowData::readInt(IndexType) const {
  return std::numeric_limits<int32_t>::max() * rand_();
}

int64_t MockRowData::readLong(IndexType) const {
  return std::numeric_limits<int64_t>::max() * rand_();
}

float MockRowData::readFloat(IndexType) const {
  return rand_();
}

double MockRowData::readDouble(IndexType) const {
  return rand_();
}

int128_t MockRowData::readInt128(IndexType) const {
  return std::numeric_limits<int128_t>::max() * rand_();
}

std::string_view MockRowData::readString(IndexType) const {
  return strings_.at(rand_() * strings_.size());
}

// compound types
std::unique_ptr<ListData> MockRowData::readList(IndexType) const {
  // copy elision or seg fault?
  return std::make_unique<MockListData>(4, seed_);
}

std::unique_ptr<MapData> MockRowData::readMap(IndexType) const {
  return std::make_unique<MockMapData>(2, seed_);
}

//------------ Mock List
bool MockListData::isNull(IndexType) const {
  return rand_() < 0.1;
}

bool MockListData::readBool(IndexType) const {
  return rand_() < 0.3;
}

int8_t MockListData::readByte(IndexType) const {
  return std::numeric_limits<int8_t>::max() * rand_();
}

int16_t MockListData::readShort(IndexType) const {
  return std::numeric_limits<int16_t>::max() * rand_();
}
int32_t MockListData::readInt(IndexType) const {
  return std::numeric_limits<int32_t>::max() * rand_();
}

int64_t MockListData::readLong(IndexType) const {
  return std::numeric_limits<int64_t>::max() * rand_();
}

float MockListData::readFloat(IndexType) const {
  return rand_();
}

double MockListData::readDouble(IndexType) const {
  return rand_();
}

int128_t MockListData::readInt128(IndexType) const {
  return std::numeric_limits<int128_t>::max() * rand_();
}

std::string_view MockListData::readString(IndexType) const {
  return strings_.at(rand_() * strings_.size());
}

//------------ Mock Map
std::unique_ptr<ListData> MockMapData::readKeys() const {
  return std::make_unique<MockListData>(getItems());
}

std::unique_ptr<ListData> MockMapData::readValues() const {
  return std::make_unique<MockListData>(getItems());
}

///////////////////////////////////////////////////////////
std::optional<bool> MockAccessor::readBool(const std::string&) const {
  // half of trues
  return rand_() < 0.5;
}

std::optional<int8_t> MockAccessor::readByte(const std::string&) const {
  return std::numeric_limits<int8_t>::max() * rand_();
}

std::optional<int16_t> MockAccessor::readShort(const std::string&) const {
  return std::numeric_limits<int16_t>::max() * rand_();
}

std::optional<int32_t> MockAccessor::readInt(const std::string&) const {
  return std::numeric_limits<int32_t>::max() * rand_();
}

std::optional<int64_t> MockAccessor::readLong(const std::string&) const {
  return std::numeric_limits<int64_t>::max() * rand_();
}

std::optional<float> MockAccessor::readFloat(const std::string&) const {
  return rand_();
}

std::optional<double> MockAccessor::readDouble(const std::string&) const {
  return rand_();
}

std::optional<int128_t> MockAccessor::readInt128(const std::string&) const {
  return std::numeric_limits<int128_t>::max() * rand_();
}

std::optional<std::string_view> MockAccessor::readString(const std::string&) const {
  return strings_.at(rand_() * strings_.size());
}

} // namespace surface
} // namespace nebula