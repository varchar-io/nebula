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

static constexpr auto UNPACK_MAX = 256;

// Implement run length encoding for numbers.
// this code is mostly inspired and copied from Apache ORC implementation.
// with re-organization to fit in nebula code base.
// https://github.com/apache/orc/blob/master/c%2B%2B/src/RleDecoderV2.cc
class RleDecoder {
public:
  RleDecoder(bool isSigned, nebula::common::PagedSlice& buffer)
    : isSigned_{ isSigned },
      buffer_{ buffer },
      cursor_(0),
      firstByte_(0),
      runLength_(0),
      runRead_(0),
      deltaBase_(0),
      byteSize_(0),
      firstValue_(0),
      prevValue_(0),
      bitSize_(0),
      bitsLeft_(0),
      curByte_(0),
      patchBitSize_(0),
      unpackedIdx_(0),
      patchIdx_(0),
      base_(0),
      curGap_(0),
      curPatch_(0),
      patchMask_(0),
      actualGap_(0) {}
  virtual ~RleDecoder() = default;

  // seek operations
  void seek(uint64_t);
  void skip(uint64_t numValues);
  void next(int64_t*, uint64_t);

private:
  inline void resetReadLongs() {
    bitsLeft_ = 0;
    curByte_ = 0;
  }

  inline void resetRun() {
    resetReadLongs();
    bitSize_ = 0;
  }

  inline unsigned char readByte() {
    return buffer_.read<unsigned char>(cursor_++);
  }

  void adjustGapAndPatch();
  int64_t readLongBE(uint64_t bsz);
  int64_t readVslong();
  uint64_t readVulong();
  uint64_t readLongs(int64_t*, uint64_t, uint64_t, uint64_t);

  // different branch of algos
  uint64_t nextShortRepeats(int64_t*, uint64_t, uint64_t);
  uint64_t nextDirect(int64_t*, uint64_t, uint64_t);
  uint64_t nextPatched(int64_t*, uint64_t, uint64_t);
  uint64_t nextDelta(int64_t*, uint64_t, uint64_t);

private:
  // basic input data to decode
  bool isSigned_;
  nebula::common::PagedSlice& buffer_;
  uint64_t cursor_;

  // decoding state
  unsigned char firstByte_;
  uint64_t runLength_;
  uint64_t runRead_;
  int64_t deltaBase_;     // Used by DELTA
  uint64_t byteSize_;     // Used by SHORT_REPEAT and PATCHED_BASE
  int64_t firstValue_;    // Used by SHORT_REPEAT and DELTA
  int64_t prevValue_;     // Used by DELTA
  uint32_t bitSize_;      // Used by DIRECT, PATCHED_BASE and DELTA
  uint32_t bitsLeft_;     // Used by anything that uses readLongs
  uint32_t curByte_;      // Used by anything that uses readLongs
  uint32_t patchBitSize_; // Used by PATCHED_BASE
  uint64_t unpackedIdx_;  // Used by PATCHED_BASE
  uint64_t patchIdx_;     // Used by PATCHED_BASE
  int64_t base_;          // Used by PATCHED_BASE
  uint64_t curGap_;       // Used by PATCHED_BASE
  int64_t curPatch_;      // Used by PATCHED_BASE
  int64_t patchMask_;     // Used by PATCHED_BASE
  int64_t actualGap_;     // Used by PATCHED_BASE

  // TODO: auto grow this packaged and unpacked
  int64_t unpacked_[UNPACK_MAX];      // Used by PATCHED_BASE
  int64_t unpackedPatch_[UNPACK_MAX]; // Used by PATCHED_BASE
};
} // namespace encode
} // namespace memory
} // namespace nebula