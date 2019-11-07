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

// fortunately we don't need to implement int128, current version of compilers supports it
// TODO(cao) - this may break build if the compiler already defined int128_t someday.
// Probably move it inside namespace.
typedef __int128_t int128_t;
typedef __uint128_t uint128_t;

namespace nebula {
namespace common {

// nebula own implementation of int128?
// struct alignas(16) Int128 {
//   int64_t hi;
//   uint64_t lo;
// };

constexpr uint128_t UINT128_LOW_MASK = UINT64_MAX;
constexpr uint128_t UINT128_HIGH_MASK = UINT128_LOW_MASK << 64;

template <typename T = uint64_t>
inline T low64(const int128_t& v) {
  return static_cast<T>(v);
}

template <typename T = uint64_t>
inline T high64(const int128_t& v) {
  return static_cast<T>(v >> 64);
}

} // namespace common
} // namespace nebula