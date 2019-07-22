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

#include <stdint.h>

namespace nebula {
namespace memory {
namespace encode {

enum FixedBitSizes {
  ONE = 0,
  TWO,
  THREE,
  FOUR,
  FIVE,
  SIX,
  SEVEN,
  EIGHT,
  NINE,
  TEN,
  ELEVEN,
  TWELVE,
  THIRTEEN,
  FOURTEEN,
  FIFTEEN,
  SIXTEEN,
  SEVENTEEN,
  EIGHTEEN,
  NINETEEN,
  TWENTY,
  TWENTYONE,
  TWENTYTWO,
  TWENTYTHREE,
  TWENTYFOUR,
  TWENTYSIX,
  TWENTYEIGHT,
  THIRTY,
  THIRTYTWO,
  FORTY,
  FORTYEIGHT,
  FIFTYSIX,
  SIXTYFOUR,
  SIZE
};

enum EncodingType {
  SHORT_REPEAT = 0,
  DIRECT = 1,
  PATCHED_BASE = 2,
  DELTA = 3
};

// constant values
static constexpr auto MIN_REPEAT = 3;
static constexpr auto HIST_LEN = 32;
static constexpr auto MAX_LITERAL_SIZE = 512;
static constexpr auto MAX_SHORT_REPEAT_LENGTH = 10;

// Map FBS enum to bit width value.
static constexpr uint8_t FBSToBitWidthMap[FixedBitSizes::SIZE] = {
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
  26, 28, 30, 32, 40, 48, 56, 64
};

// Map bit length i to closest fixed bit width that can contain i bits.
static constexpr uint8_t ClosestFixedBitsMap[65] = {
  1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
  26, 26, 28, 28, 30, 30, 32, 32,
  40, 40, 40, 40, 40, 40, 40, 40,
  48, 48, 48, 48, 48, 48, 48, 48,
  56, 56, 56, 56, 56, 56, 56, 56,
  64, 64, 64, 64, 64, 64, 64, 64
};

// Map bit length i to closest aligned fixed bit width that can contain i bits.
static constexpr uint8_t ClosestAlignedFixedBitsMap[65] = {
  1, 1, 2, 4, 4, 8, 8, 8, 8, 16, 16, 16, 16, 16, 16, 16, 16, 24, 24, 24, 24, 24, 24, 24, 24,
  32, 32, 32, 32, 32, 32, 32, 32,
  40, 40, 40, 40, 40, 40, 40, 40,
  48, 48, 48, 48, 48, 48, 48, 48,
  56, 56, 56, 56, 56, 56, 56, 56,
  64, 64, 64, 64, 64, 64, 64, 64
};

// Map bit width to FBS enum.
static constexpr uint8_t BitWidthToFBSMap[65] = {
  FixedBitSizes::ONE, FixedBitSizes::ONE, FixedBitSizes::TWO, FixedBitSizes::THREE, FixedBitSizes::FOUR,
  FixedBitSizes::FIVE, FixedBitSizes::SIX, FixedBitSizes::SEVEN, FixedBitSizes::EIGHT,
  FixedBitSizes::NINE, FixedBitSizes::TEN, FixedBitSizes::ELEVEN, FixedBitSizes::TWELVE,
  FixedBitSizes::THIRTEEN, FixedBitSizes::FOURTEEN, FixedBitSizes::FIFTEEN, FixedBitSizes::SIXTEEN,
  FixedBitSizes::SEVENTEEN, FixedBitSizes::EIGHTEEN, FixedBitSizes::NINETEEN, FixedBitSizes::TWENTY,
  FixedBitSizes::TWENTYONE, FixedBitSizes::TWENTYTWO, FixedBitSizes::TWENTYTHREE, FixedBitSizes::TWENTYFOUR,
  FixedBitSizes::TWENTYSIX, FixedBitSizes::TWENTYSIX,
  FixedBitSizes::TWENTYEIGHT, FixedBitSizes::TWENTYEIGHT,
  FixedBitSizes::THIRTY, FixedBitSizes::THIRTY,
  FixedBitSizes::THIRTYTWO, FixedBitSizes::THIRTYTWO,
  FixedBitSizes::FORTY, FixedBitSizes::FORTY, FixedBitSizes::FORTY, FixedBitSizes::FORTY,
  FixedBitSizes::FORTY, FixedBitSizes::FORTY, FixedBitSizes::FORTY, FixedBitSizes::FORTY,
  FixedBitSizes::FORTYEIGHT, FixedBitSizes::FORTYEIGHT, FixedBitSizes::FORTYEIGHT, FixedBitSizes::FORTYEIGHT,
  FixedBitSizes::FORTYEIGHT, FixedBitSizes::FORTYEIGHT, FixedBitSizes::FORTYEIGHT, FixedBitSizes::FORTYEIGHT,
  FixedBitSizes::FIFTYSIX, FixedBitSizes::FIFTYSIX, FixedBitSizes::FIFTYSIX, FixedBitSizes::FIFTYSIX,
  FixedBitSizes::FIFTYSIX, FixedBitSizes::FIFTYSIX, FixedBitSizes::FIFTYSIX, FixedBitSizes::FIFTYSIX,
  FixedBitSizes::SIXTYFOUR, FixedBitSizes::SIXTYFOUR, FixedBitSizes::SIXTYFOUR, FixedBitSizes::SIXTYFOUR,
  FixedBitSizes::SIXTYFOUR, FixedBitSizes::SIXTYFOUR, FixedBitSizes::SIXTYFOUR, FixedBitSizes::SIXTYFOUR
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
inline uint32_t getClosestFixedBits(uint32_t n) {
  if (n <= 64) {
    return ClosestFixedBitsMap[n];
  } else {
    return 64;
  }
}

inline uint32_t findClosestNumBits(int64_t value) {
  if (value < 0) {
    return getClosestFixedBits(64);
  }

  uint32_t count = 0;
  while (value != 0) {
    count++;
    value = value >> 1;
  }
  return getClosestFixedBits(count);
}

inline uint32_t encodeBitWidth(uint32_t n) {
  if (n <= 64) {
    return BitWidthToFBSMap[n];
  } else {
    return FixedBitSizes::SIXTYFOUR;
  }
}

inline uint32_t decodeBitWidth(uint32_t n) {
  return FBSToBitWidthMap[n];
}

inline int64_t zigZag(int64_t value) {
  return (value << 1) ^ (value >> 63);
}

inline int64_t unZigZag(uint64_t value) {
  return value >> 1 ^ -(value & 1);
}

inline bool isSafeSubtract(int64_t left, int64_t right) {
  return ((left ^ right) >= 0) || ((left ^ (left - right)) >= 0);
}

inline uint32_t getClosestAlignedFixedBits(uint32_t n) {
  if (n <= 64) {
    return ClosestAlignedFixedBitsMap[n];
  } else {
    return 64;
  }
}

inline uint32_t getOpCode(EncodingType encoding) {
  return static_cast<uint32_t>(encoding << 6);
}

} // namespace encode
} // namespace memory
} // namespace nebula