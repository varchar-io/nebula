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

#include "RleDecoder.h"
#include "common/Errors.h"
#include "common/Likely.h"

namespace nebula {
namespace memory {
namespace encode {

void RleDecoder::adjustGapAndPatch() {
  auto val = unpackedPatch_[patchIdx_];
  curGap_ = static_cast<uint64_t>(val) >> patchBitSize_;
  curPatch_ = val & patchMask_;
  actualGap_ = 0;

  // special case: gap is >255 then patch value will be 0.
  // if gap is <=255 then patch value cannot be 0
  while (curGap_ == 255 && curPatch_ == 0) {
    actualGap_ += 255;
    ++patchIdx_;
    val = unpackedPatch_[patchIdx_];
    curGap_ = static_cast<uint64_t>(val >> patchBitSize_);
    curPatch_ = val & patchMask_;
  }

  // add the left over gap
  actualGap_ += curGap_;
}

uint64_t RleDecoder::readLongs(int64_t* data, uint64_t offset, uint64_t len, uint64_t fb) {
  uint64_t ret = 0;

  // TODO: unroll to improve performance
  for (uint64_t i = offset; i < (offset + len); i++) {
    uint64_t result = 0;
    uint64_t bitsLeftToRead = fb;
    while (bitsLeftToRead > bitsLeft_) {
      result <<= bitsLeft_;
      result |= curByte_ & ((1 << bitsLeft_) - 1);
      bitsLeftToRead -= bitsLeft_;
      curByte_ = readByte();
      bitsLeft_ = 8;
    }

    // handle the left over bits
    if (bitsLeftToRead > 0) {
      result <<= bitsLeftToRead;
      bitsLeft_ -= static_cast<uint32_t>(bitsLeftToRead);
      result |= (curByte_ >> bitsLeft_) & ((1 << bitsLeftToRead) - 1);
    }
    data[i] = result;
    ++ret;
  }

  return ret;
}

int64_t RleDecoder::readLongBE(uint64_t bsz) {
  int64_t ret = 0, val;
  uint64_t n = bsz;
  while (n > 0) {
    n--;
    val = readByte();
    ret |= (val << (n * 8));
  }
  return ret;
}

uint64_t RleDecoder::nextShortRepeats(
  int64_t* data,
  uint64_t offset,
  uint64_t numValues) {
  if (runRead_ == runLength_) {
    // extract the number of fixed bytes
    byteSize_ = (firstByte_ >> 3) & 0x07;
    byteSize_ += 1;

    runLength_ = firstByte_ & 0x07;
    // run lengths values are stored only after MIN_REPEAT value is met
    runLength_ += MIN_REPEAT;
    runRead_ = 0;

    // read the repeated value which is store using fixed bytes
    firstValue_ = readLongBE(byteSize_);

    if (isSigned_) {
      firstValue_ = unZigZag(static_cast<uint64_t>(firstValue_));
    }
  }

  uint64_t nRead = std::min(runLength_ - runRead_, numValues);

  for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
    data[pos] = firstValue_;
    ++runRead_;
  }

  return nRead;
}

uint64_t RleDecoder::nextDirect(
  int64_t* data,
  uint64_t offset,
  uint64_t numValues) {
  if (runRead_ == runLength_) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte_ >> 1) & 0x1f;
    bitSize_ = decodeBitWidth(fbo);

    // extract the run length
    runLength_ = static_cast<uint64_t>(firstByte_ & 0x01) << 8;
    runLength_ |= readByte();
    // runs are one off
    runLength_ += 1;
    runRead_ = 0;
  }

  uint64_t nRead = std::min(runLength_ - runRead_, numValues);

  runRead_ += readLongs(data, offset, nRead, bitSize_);

  if (isSigned_) {
    for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
      data[pos] = unZigZag(data[pos]);
    }
  }

  return nRead;
}

uint64_t RleDecoder::nextPatched(
  int64_t* data,
  uint64_t offset,
  uint64_t numValues) {
  if (runRead_ == runLength_) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte_ >> 1) & 0x1f;
    bitSize_ = decodeBitWidth(fbo);

    // extract the run length
    runLength_ = static_cast<uint64_t>(firstByte_ & 0x01) << 8;
    runLength_ |= readByte();
    // runs are one off
    runLength_ += 1;
    runRead_ = 0;

    // extract the number of bytes occupied by base
    uint64_t thirdByte = readByte();
    byteSize_ = (thirdByte >> 5) & 0x07;
    // base width is one off
    byteSize_ += 1;

    // extract patch width
    uint32_t pwo = thirdByte & 0x1f;
    patchBitSize_ = decodeBitWidth(pwo);

    // read fourth byte and extract patch gap width
    uint64_t fourthByte = readByte();
    uint32_t pgw = (fourthByte >> 5) & 0x07;
    // patch gap width is one off
    pgw += 1;

    // extract the length of the patch list
    size_t pl = fourthByte & 0x1f;
    if (pl == 0) {
      throw NException("Corrupt PATCHED_BASE encoded data (pl==0)!");
    }

    // read the next base width number of bytes to extract base value
    base_ = readLongBE(byteSize_);
    int64_t mask = (static_cast<int64_t>(1) << ((byteSize_ * 8) - 1));
    // if mask of base value is 1 then base is negative value else positive
    if ((base_ & mask) != 0) {
      base_ = base_ & ~mask;
      base_ = -base_;
    }

    // TODO: something more efficient than resize
    unpackedIdx_ = 0;
    N_ENSURE(runLength_ <= UNPACK_MAX, "unexpected run length");
    readLongs(unpacked_, 0, runLength_, bitSize_);
    // any remaining bits are thrown out
    resetReadLongs();

    // TODO: something more efficient than resize
    patchIdx_ = 0;
    // TODO: Skip corrupt?
    //    if ((patchBitSize + pgw) > 64 && !skipCorrupt) {
    if ((patchBitSize_ + pgw) > 64) {
      throw NException("Corrupt PATCHED_BASE encoded data (patchBitSize + pgw > 64)!");
    }

    uint32_t cfb = getClosestFixedBits(patchBitSize_ + pgw);

    N_ENSURE(pl <= UNPACK_MAX, "unexpected pack list length");
    readLongs(unpackedPatch_, 0, pl, cfb);

    // any remaining bits are thrown out
    resetReadLongs();

    // apply the patch directly when decoding the packed data
    patchMask_ = ((static_cast<int64_t>(1) << patchBitSize_) - 1);

    adjustGapAndPatch();
  }

  uint64_t nRead = std::min(runLength_ - runRead_, numValues);

  for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
    if (static_cast<int64_t>(unpackedIdx_) != actualGap_) {
      // no patching required. add base to unpacked value to get final value
      data[pos] = base_ + unpacked_[unpackedIdx_];
    } else {
      // extract the patch value
      int64_t patchedVal = unpacked_[unpackedIdx_] | (curPatch_ << bitSize_);

      // add base to patched value
      data[pos] = base_ + patchedVal;

      // increment the patch to point to next entry in patch list
      ++patchIdx_;

      // need real data size in the slice
      if (patchIdx_ < UNPACK_MAX) {
        adjustGapAndPatch();

        // next gap is relative to the current gap
        actualGap_ += unpackedIdx_;
      }
    }

    ++runRead_;
    ++unpackedIdx_;
  }

  return nRead;
}

inline int64_t RleDecoder::readVslong() {
  return unZigZag(readVulong());
}

uint64_t RleDecoder::readVulong() {
  uint64_t ret = 0, b;
  uint64_t offset = 0;
  do {
    b = readByte();
    ret |= (0x7f & b) << offset;
    offset += 7;
  } while (b >= 0x80);
  return ret;
}

uint64_t RleDecoder::nextDelta(
  int64_t* data, uint64_t offset, uint64_t numValues) {
  if (runRead_ == runLength_) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte_ >> 1) & 0x1f;
    if (fbo != 0) {
      bitSize_ = decodeBitWidth(fbo);
    } else {
      bitSize_ = 0;
    }

    // extract the run length
    runLength_ = static_cast<uint64_t>(firstByte_ & 0x01) << 8;
    runLength_ |= readByte();
    ++runLength_; // account for first value
    runRead_ = 0;
    deltaBase_ = 0;

    // read the first value stored as vint
    if (isSigned_) {
      firstValue_ = static_cast<int64_t>(readVslong());
    } else {
      firstValue_ = static_cast<int64_t>(readVulong());
    }

    prevValue_ = firstValue_;

    // read the fixed delta value stored as vint (deltas can be negative even
    // if all number are positive)
    deltaBase_ = static_cast<int64_t>(readVslong());
  }

  uint64_t nRead = std::min(runLength_ - runRead_, numValues);

  uint64_t pos = offset;
  if (runRead_ == 0 && pos < offset + nRead) {
    data[pos++] = firstValue_;
    ++runRead_;
  }

  if (bitSize_ == 0) {
    // add fixed deltas to adjacent values
    for (; pos < offset + nRead; ++pos) {
      prevValue_ = prevValue_ + deltaBase_;
      data[pos] = prevValue_;
      ++runRead_;
    }
  } else {
    if (runRead_ < 2 && pos < offset + nRead) {
      // add delta base and first value
      prevValue_ = firstValue_ + deltaBase_;
      data[pos++] = prevValue_;
      ++runRead_;
    }

    // write the unpacked values, add it to previous value and store final
    // value to result buffer. if the delta base value is negative then it
    // is a decreasing sequence else an increasing sequence
    uint64_t remaining = (offset + nRead) - pos;
    runRead_ += readLongs(data, pos, remaining, bitSize_);

    if (deltaBase_ < 0) {
      for (; pos < offset + nRead; ++pos) {
        prevValue_ = prevValue_ - data[pos];
        data[pos] = prevValue_;
      }
    } else {
      for (; pos < offset + nRead; ++pos) {
        prevValue_ = prevValue_ + data[pos];
        data[pos] = prevValue_;
      }
    }
  }

  return nRead;
}

void RleDecoder::next(int64_t* data, uint64_t numValues) {
  uint64_t nRead = 0;

  while (nRead < numValues) {
    if (UNLIKELY(runRead_ == runLength_)) {
      resetRun();
      firstByte_ = readByte();
    }

    uint64_t offset = nRead, length = numValues - nRead;

    EncodingType enc = static_cast<EncodingType>((firstByte_ >> 6) & 0x03);
    switch (static_cast<int64_t>(enc)) {
    case SHORT_REPEAT:
      nRead += nextShortRepeats(data, offset, length);
      break;
    case DIRECT:
      nRead += nextDirect(data, offset, length);
      break;
    case PATCHED_BASE:
      nRead += nextPatched(data, offset, length);
      break;
    case DELTA:
      nRead += nextDelta(data, offset, length);
      break;
    default:
      throw NException("unknown encoding");
    }
  }
}

void RleDecoder::skip(uint64_t numValues) {
  constexpr uint64_t N = 64;
  int64_t dummy[N];

  while (numValues) {
    uint64_t nRead = std::min(N, numValues);
    next(dummy, nRead);
    numValues -= nRead;
  }
}

void RleDecoder::seek(uint64_t pos) {
  runRead_ = 0;
  runLength_ = 0;

  // only support forward seek
  N_ENSURE(pos > cursor_, "only forward seek");
  // skip ahead the given number of records
  skip(pos - cursor_);
}

} // namespace encode
} // namespace memory
} // namespace nebula