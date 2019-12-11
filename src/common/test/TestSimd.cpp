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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <random>

// #ifndef HWY_TARGET_INCLUDE
// #define HWY_TARGET_INCLUDE "../src/common/test/TestSimd.cpp"
// #include <hwy/foreach_target.h>
// #include <hwy/runtime_dispatch.h>
// #endif

// static_targets.h for single CPU target
// TODO(cao) - will not produce cross-platform bits
#include <hwy/static_targets.h>
#include <hwy/type_traits.h>

/**
 * Test namespace for exploring SIMD instructions.
 * Mostly through project "Highway".
 * Refer https://github.com/google/highway/blob/master/quick_reference.md
 */
namespace nebula {
namespace common {
namespace test {

[[noreturn]] void NotifyFailure(const char* filename, const int line,
                                const size_t bits, const char* type_name,
                                const size_t lane, const char* expected,
                                const char* actual) {
  fprintf(stderr,
          "%s:%d: bits %zu, %s lane %zu mismatch: expected '%s', got '%s'.\n",
          filename, line, bits, type_name, lane, expected, actual);
  __builtin_trap();
}

// Returns end of string (position of '\0').
template <typename T>
inline char* ToString(T value, char* to) {
  char reversed[100];
  char* pos = reversed;
  int64_t before;
  do {
    before = value;
    value /= 10;
    const int64_t mod = before - value * 10;
    *pos++ = "9876543210123456789"[9 + mod];
  } while (value != 0);
  if (before < 0) *pos++ = '-';

  // Reverse the string
  const int num_chars = pos - reversed;
  for (int i = 0; i < num_chars; ++i) {
    to[i] = pos[-1 - i];
  }
  to[num_chars] = '\0';
  return to + num_chars;
}

template <>
inline char* ToString<float>(const float value, char* to) {
  if (!std::isfinite(value)) {
    strncpy(to, "<not-finite>", 13);
    return to + strlen(to);
  }
  const int64_t truncated = static_cast<int64_t>(value);
  char* end = ToString(truncated, to);
  *end++ = '.';
  int64_t frac = static_cast<int64_t>((value - truncated) * 1E8);
  if (frac < 0) frac = -frac;
  return ToString(frac, end);
}

template <>
inline char* ToString<double>(const double value, char* to) {
  if (!std::isfinite(value)) {
    strncpy(to, "<not-finite>", 13);
    return to + strlen(to);
  }
  const int64_t truncated = static_cast<int64_t>(value);
  char* end = ToString(truncated, to);
  *end++ = '.';
  int64_t frac = static_cast<int64_t>((value - truncated) * 1E16);
  if (frac < 0) frac = -frac;
  return ToString(frac, end);
}

// String comparison

template <typename T1, typename T2>
inline bool BytesEqual(const T1* p1, const T2* p2, const size_t size) {
  const uint8_t* bytes1 = reinterpret_cast<const uint8_t*>(p1);
  const uint8_t* bytes2 = reinterpret_cast<const uint8_t*>(p2);
  for (size_t i = 0; i < size; ++i) {
    if (bytes1[i] != bytes2[i]) return false;
  }
  return true;
}

inline bool StringsEqual(const char* s1, const char* s2) {
  while (*s1 == *s2++) {
    if (*s1++ == '\0') return true;
  }
  return false;
}

template <typename T>
void AssertEqual(const T expected, const T actual, const char* filename = "",
                 const int line = -1, const size_t lane = 0,
                 const char* name = nullptr) {
  char expected_buf[100];
  char actual_buf[100];
  ToString(expected, expected_buf);
  ToString(actual, actual_buf);
  // Rely on string comparison to ensure similar floats are "equal".
  if (!StringsEqual(expected_buf, actual_buf)) {
    NotifyFailure(
      filename, line, HWY_BITS, name, lane, expected_buf, actual_buf);
  }
}

#define HWY_ASSERT_EQ(expected, actual) \
  AssertEqual(expected, actual, __FILE__, __LINE__)

class BasicSimd {
  using RandomState = std::mt19937;

public:
  HWY_INLINE uint32_t Random32(RandomState* rng) {
    return static_cast<uint32_t>((*rng)());
  }

  // Prevents the compiler from eliding the computations that led to "output".
  // Works by indicating to the compiler that "output" is being read and modified.
  // The +r constraint avoids unnecessary writes to memory, but only works for
  // built-in types.
  template <class T>
  inline void PreventElision(T&& output) {
#ifndef _MSC_VER
    asm volatile(""
                 : "+r"(output)
                 :
                 : "memory");
#endif
  }

  HWY_NOINLINE HWY_ATTR void floorLog2(const uint8_t* HWY_RESTRICT values,
                                       uint8_t* HWY_RESTRICT log2) {
    // Descriptors for all required data types:
    const HWY_FULL(int32_t) d32;
    const HWY_FULL(float) df;
    const HWY_CAPPED(uint8_t, d32.N) d8;

    const auto u8 = Load(d8, values);
    const auto bits = BitCast(d32, ConvertTo(df, ConvertTo(d32, u8)));
    const auto exponent = hwy::ShiftRight<23>(bits) - Set(d32, 127);
    Store(ConvertTo(d8, exponent), d8, log2);
  }

  HWY_NOINLINE HWY_ATTR void testBasic() {
    const size_t kStep = HWY_FULL(int32_t)::N;
    const size_t kBytes = 32;
    static_assert(kBytes % kStep == 0, "Must be a multiple of kStep");

    uint8_t in[kBytes];
    uint8_t expected[kBytes];
    RandomState rng{ 1234 };
    for (size_t i = 0; i < kBytes; ++i) {
      expected[i] = Random32(&rng) & 7;
      in[i] = 1u << expected[i];
    }
    uint8_t out[32];
    for (size_t i = 0; i < kBytes; i += kStep) {
      floorLog2(in + i, out + i);
    }
    int sum = 0;
    for (size_t i = 0; i < kBytes; ++i) {
      HWY_ASSERT_EQ(expected[i], out[i]);
      sum += out[i];
    }
    PreventElision(sum);
  }
};

TEST(SimdTest, TestArthmetic) {
  LOG(INFO) << "Current CPU HWY BITS=" << HWY_BITS;
  BasicSimd bs;
  bs.testBasic();
}

} // namespace test
} // namespace common
} // namespace nebula