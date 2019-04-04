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

#include <cstdint>
#include "DataSurface.h"
#include "common/Evidence.h"
#include "fmt/format.h"

/**
 * Mock some data and test the interface with an app
 */
namespace nebula {
namespace surface {

//------------ Mock Row
// All intrefaces - string type has RVO, copy elision optimization
bool MockRowData::isNull(const std::string& field) const {
  // 10% nulls
  return rand_() < 0.1;
}

bool MockRowData::readBool(const std::string& field) const {
  // half of trues
  return rand_() < 0.5;
}
int8_t MockRowData::readByte(const std::string& field) const {
  return std::numeric_limits<int8_t>::max() * rand_();
}
int16_t MockRowData::readShort(const std::string& field) const {
  return std::numeric_limits<int16_t>::max() * rand_();
}
int32_t MockRowData::readInt(const std::string& field) const {
  return std::numeric_limits<int32_t>::max() * rand_();
}
int64_t MockRowData::readLong(const std::string& field) const {
  return std::numeric_limits<int64_t>::max() * rand_();
}
float MockRowData::readFloat(const std::string& field) const {
  return rand_();
}
double MockRowData::readDouble(const std::string& field) const {
  return rand_();
}
std::string MockRowData::readString(const std::string& field) const {
  return std::string(rand_() * 10, 'N');
}

// compound types
std::unique_ptr<ListData> MockRowData::readList(const std::string& field) const {
  // copy elision or seg fault?
  return std::make_unique<MockListData>(4, seed_);
}

std::unique_ptr<MapData> MockRowData::readMap(const std::string& field) const {
  return std::make_unique<MockMapData>(2, seed_);
}

//------------ Mock Row
// All intrefaces - string type has RVO, copy elision optimization
bool MockRowData::isNull(IndexType index) const {
  // 10% nulls
  return rand_() < 0.1;
}

bool MockRowData::readBool(IndexType index) const {
  // half of trues
  return rand_() < 0.5;
}
int8_t MockRowData::readByte(IndexType index) const {
  return std::numeric_limits<int8_t>::max() * rand_();
}
int16_t MockRowData::readShort(IndexType index) const {
  return std::numeric_limits<int16_t>::max() * rand_();
}
int32_t MockRowData::readInt(IndexType index) const {
  return std::numeric_limits<int32_t>::max() * rand_();
}
int64_t MockRowData::readLong(IndexType index) const {
  return std::numeric_limits<int64_t>::max() * rand_();
}
float MockRowData::readFloat(IndexType index) const {
  return rand_();
}
double MockRowData::readDouble(IndexType index) const {
  return rand_();
}
std::string MockRowData::readString(IndexType index) const {
  return std::string(rand_() * 10, 'N');
}

// compound types
std::unique_ptr<ListData> MockRowData::readList(IndexType index) const {
  // copy elision or seg fault?
  return std::make_unique<MockListData>(4, seed_);
}

std::unique_ptr<MapData> MockRowData::readMap(IndexType index) const {
  return std::make_unique<MockMapData>(2, seed_);
}

//------------ Mock List
bool MockListData::isNull(IndexType index) const {
  return rand_() < 0.1;
}

bool MockListData::readBool(IndexType index) const {
  return rand_() < 0.3;
}

int8_t MockListData::readByte(IndexType index) const {
  return std::numeric_limits<int8_t>::max() * rand_();
}

int16_t MockListData::readShort(IndexType index) const {
  return std::numeric_limits<int16_t>::max() * rand_();
}
int32_t MockListData::readInt(IndexType index) const {
  return std::numeric_limits<int32_t>::max() * rand_();
}

int64_t MockListData::readLong(IndexType index) const {
  return std::numeric_limits<int64_t>::max() * rand_();
}

float MockListData::readFloat(IndexType index) const {
  return rand_();
}

double MockListData::readDouble(IndexType index) const {
  return rand_();
}
std::string MockListData::readString(IndexType index) const {
  return std::string(rand_() * 10, 'N');
}

//------------ Mock Map
std::unique_ptr<ListData> MockMapData::readKeys() const {
  return std::make_unique<MockListData>(getItems());
}

std::unique_ptr<ListData> MockMapData::readValues() const {
  return std::make_unique<MockListData>(getItems());
}

} // namespace surface
} // namespace nebula