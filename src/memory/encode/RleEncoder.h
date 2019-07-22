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

#include <vector>
#include "Utils.h"
#include "common/Memory.h"

namespace nebula {
namespace memory {
namespace encode {

struct EncodingOption {
  EncodingType encoding;
  int64_t fixedDelta;
  int64_t gapVsPatchListCount;
  int64_t zigzagLiteralsCount;
  int64_t baseRedLiteralsCount;
  int64_t adjDeltasCount;
  uint32_t zzBits90p;
  uint32_t zzBits100p;
  uint32_t brBits95p;
  uint32_t brBits100p;
  uint32_t bitsDeltaMax;
  uint32_t patchWidth;
  uint32_t patchGapWidth;
  uint32_t patchLength;
  int64_t min;
  bool isFixedDelta;
};

// Implement run length encoding for numbers.
// this code is mostly inspired and copied from Apache ORC implementation.
// with re-organization to fit in nebula code base.
// https://github.com/apache/orc/blob/master/c%2B%2B/src/RleEncoderV2.cc
class RleEncoder {
public:
  // create an encoder with
  // signed: signed value
  // buffer: encoder doesn't own any memory, it writes to external allocated slice
  // alignBp: align bit packing
  RleEncoder(bool isSigned, nebula::common::PagedSlice& buffer, bool alignBp = true)
    : isSigned_{ isSigned },
      alignBp_{ alignBp },
      buffer_{ buffer },
      bufferPos_{ 0 },
      prevDelta_{ 0 },
      fixedRunLength_{ 0 },
      variableRunLength_{ 0 },
      numLiterals_{ 0 } {}
  virtual ~RleEncoder() = default;

  // flush all data out
  void flush();

  // encode 8 bytes in
  void write(int64_t);

private:
  void determineEncoding(EncodingOption& option);
  void computeZigZagLiterals(EncodingOption& option);
  void preparePatchedBlob(EncodingOption& option);

  void writeInts(int64_t* input, uint32_t offset, size_t len, uint32_t bitSize);
  void initializeLiterals(int64_t val);
  void writeValues(EncodingOption& option);
  void writeShortRepeatValues(EncodingOption& option);
  void writeDirectValues(EncodingOption& option);
  void writePatchedBasedValues(EncodingOption& option);
  void writeDeltaValues(EncodingOption& option);
  uint32_t percentileBits(int64_t* data, size_t offset, size_t length, double p, bool reuseHist = false);
  void writeVulong(int64_t val);
  void writeVslong(int64_t val);

  // write a byte
  void writeByte(char);

private:
  const bool isSigned_;
  const bool alignBp_;

  // buffer writing meta
  nebula::common::PagedSlice& buffer_;
  size_t bufferPos_;

  // recording metrics
  int64_t prevDelta_;
  uint32_t fixedRunLength_;
  uint32_t variableRunLength_;
  size_t numLiterals_;

  // temporary data to help encoding
  int32_t histgram_[HIST_LEN];
  int64_t literals_[MAX_LITERAL_SIZE];
  int64_t gapVsPatchList_[MAX_LITERAL_SIZE];
  int64_t zigzagLiterals_[MAX_LITERAL_SIZE];
  int64_t baseRedLiterals_[MAX_LITERAL_SIZE];
  int64_t adjDeltas_[MAX_LITERAL_SIZE];
};

} // namespace encode
} // namespace memory
} // namespace nebula