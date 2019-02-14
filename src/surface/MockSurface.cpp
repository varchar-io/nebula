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
#include "fmt/format.h"

/**
 * Mock some data and test the interface with an app
 */
namespace nebula {
namespace surface {

//------------ Mock Row
// All intrefaces - string type has RVO, copy elision optimization
bool MockRowData::isNull(const std::string& field) {
  return false;
}

bool MockRowData::readBool(const std::string& field) {
  return true;
}
int8_t MockRowData::readByte(const std::string& field) {
  return 8;
}
int16_t MockRowData::readShort(const std::string& field) {
  return 16;
}
int32_t MockRowData::readInt(const std::string& field) {
  return 32;
}
int64_t MockRowData::readLong(const std::string& field) {
  return 64;
}
float MockRowData::readFloat(const std::string& field) {
  return 4.0;
}
double MockRowData::readDouble(const std::string& field) {
  return 8.0;
}
std::string MockRowData::readString(const std::string& field) {
  return "MOCK";
}

// compound types
std::unique_ptr<ListData> MockRowData::readList(const std::string& field) {
  // copy elision or seg fault?
  return std::make_unique<MockListData>(4);
}

std::unique_ptr<MapData> MockRowData::readMap(const std::string& field) {
  return std::make_unique<MockMapData>(2);
}

//------------ Mock List
bool MockListData::isNull(uint32_t index) {
  return false;
}

bool MockListData::readBool(uint32_t index) {
  return index % 2 == 0;
}

int8_t MockListData::readByte(uint32_t index) {
  return 8 * (index + 2);
}

int16_t MockListData::readShort(uint32_t index) {
  return 16 * (index + 2);
}
int32_t MockListData::readInt(uint32_t index) {
  return 32 * (index + 2);
}

int64_t MockListData::readLong(uint32_t index) {
  return 64 * (index + 2);
}

float MockListData::readFloat(uint32_t index) {
  return 4.0 * (index + 2);
}

double MockListData::readDouble(uint32_t index) {
  return 8.0 * (index + 2);
}
std::string MockListData::readString(uint32_t index) {
  return fmt::format("LIST_{0}_MOCK", index);
}

//------------ Mock Map
std::unique_ptr<ListData> MockMapData::readKeys() {
  return std::make_unique<MockListData>(getItems());
}

std::unique_ptr<ListData> MockMapData::readValues() {
  return std::make_unique<MockListData>(getItems());
}

} // namespace surface
} // namespace nebula