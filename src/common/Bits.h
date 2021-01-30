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

#include <glog/logging.h>
#include <memory>

namespace nebula {
namespace common {

// define bits operation on given byte
class Byte {
  static constexpr uint8_t MASK[] = {
    0,
    0xFF >> 7,
    0xFF >> 6,
    0xFF >> 5,
    0xFF >> 4,
    0xFF >> 3,
    0xFF >> 2,
    0xFF >> 1,
    0xFF
  };

public:
  inline static void write(uint8_t* byte, size_t pos, size_t bits, uint8_t value) {
    *byte = ((value & MASK[bits]) << pos) | (*byte & MASK[pos]);
  }

  inline static uint8_t read(uint8_t* byte, size_t pos, size_t bits) {
    return (*byte >> pos) & MASK[bits];
  }

  static void print() {
    for (size_t i = 0; i < sizeof(MASK); ++i) {
      LOG(INFO) << "MASK i=" << i << ", v=" << (int)MASK[i];
    }
  }
};
} // namespace common
} // namespace nebula