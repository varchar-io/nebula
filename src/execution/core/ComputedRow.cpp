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

#include "ComputedRow.h"

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

bool ComputedRow::isNull(IndexType) const {
  // TODO(cao): how to determine nullbility of output field?
  return false;
}

// TODO(cao) - need to implement isNull (maybe cache to ensure evaluate once)
// otherwise - this may return invalid values
#define FORWARD_EVAL_FIELD(TYPE, NAME)               \
  TYPE ComputedRow::NAME(IndexType index) const {    \
    bool valid = true;                               \
    return ctx_->eval<TYPE>(*fields_[index], valid); \
  }

FORWARD_EVAL_FIELD(bool, readBool)
FORWARD_EVAL_FIELD(int8_t, readByte)
FORWARD_EVAL_FIELD(int16_t, readShort)
FORWARD_EVAL_FIELD(int32_t, readInt)
FORWARD_EVAL_FIELD(int64_t, readLong)
FORWARD_EVAL_FIELD(float, readFloat)
FORWARD_EVAL_FIELD(double, readDouble)
FORWARD_EVAL_FIELD(int128_t, readInt128)
FORWARD_EVAL_FIELD(std::string_view, readString)

#undef FORWARD_EVAL_FIELD

std::unique_ptr<nebula::surface::ListData> ComputedRow::readList(IndexType) const {
  throw NException("Not implemented yet");
}

std::unique_ptr<nebula::surface::MapData> ComputedRow::readMap(IndexType) const {
  throw NException("Not implemented yet");
}

} // namespace core
} // namespace execution
} // namespace nebula