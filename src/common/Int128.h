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

#include <string>
#include <type_traits>
#include <vector>

// #if __cplusplus > 201703L
// #include <bit>
// #endif
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
// example of endians, value = 0x4F52 to start at address 1000
// big endian: 4F -> address 1000, 52 -> address 1001. (low bytes on high address)
// little-endian: 52 -> 1000, 4F -> 1001. (low bytes on low address)
// #if __cplusplus > 201703L
// constexpr bool isBigEndian = std::endian::native == std::endian::big;
// #else
// constexpr bool isBigEndian = ;
// inline constexpr bool is_system_little_endian() {
//   constexpr auto value{ 0x01 };
//   const void* address = static_cast<const void*>(&value);
//   const unsigned char* least_significant_address = static_cast<const unsigned char*>(address);
//   return (*least_significant_address == 0x01);
// }
// #endif
class Int128_U {
public:
  static constexpr uint128_t UINT128_LOW_MASK = UINT64_MAX;
  static constexpr uint128_t UINT128_HIGH_MASK = UINT128_LOW_MASK << 64;

  template <typename T = int64_t>
  static inline auto low64(const int128_t& v) ->
    typename std::enable_if_t<std::is_integral_v<T>, T> {
    return (T)(*reinterpret_cast<const int64_t*>(&v));
  }

  template <typename T = double>
  static inline auto low64(const int128_t& v) ->
    typename std::enable_if_t<std::is_floating_point_v<T>, T> {
    return (T)(*reinterpret_cast<const double*>(&v));
  }

  template <typename T = int64_t>
  static inline auto high64(const int128_t& v) ->
    typename std::enable_if_t<std::is_integral_v<T>, T> {
    return (T)(*(reinterpret_cast<const int64_t*>(&v) + 1));
  }

  template <typename T = double>
  static inline auto high64(const int128_t& v) ->
    typename std::enable_if_t<std::is_floating_point_v<T>, T> {
    return (T)(*(reinterpret_cast<const double*>(&v) + 1));
  }

  // TODO(cao): I'm assuming this is slow, how can we speed it up.
  template <typename T = int64_t>
  static inline auto high64_add(int128_t& v, T delta) ->
    typename std::enable_if_t<std::is_integral_v<T>, void> {
    // TODO(cao): delta could be floating value, this will lose precision
    // have to use 8 bytes, 8 bytes separation or std::array<char, 16> to re-implement
    auto addr = reinterpret_cast<int64_t*>(&v) + 1;
    *addr += delta;
  }

  template <typename T = double>
  static inline auto high64_add(int128_t& v, T delta) ->
    typename std::enable_if_t<std::is_floating_point_v<T>, void> {
    // TODO(cao): delta could be floating value, this will lose precision
    // have to use 8 bytes, 8 bytes separation or std::array<char, 16> to re-implement
    auto addr = reinterpret_cast<double*>(&v) + 1;
    *addr += delta;
  }

  // TODO(cao): I'm assuming this is slow, how can we speed it up.
  template <typename T = int64_t>
  static inline auto low64_add(int128_t& v, T delta) ->
    typename std::enable_if_t<std::is_integral_v<T>, void> {
    // TODO(cao): delta could be floating value, this will lose precision
    // have to use 8 bytes, 8 bytes separation or std::array<char, 16> to re-implement
    auto addr = reinterpret_cast<int64_t*>(&v);
    *addr += delta;
  }

  template <typename T = double>
  static inline auto low64_add(int128_t& v, T delta) ->
    typename std::enable_if_t<std::is_floating_point_v<T>, void> {
    // TODO(cao): delta could be floating value, this will lose precision
    // have to use 8 bytes, 8 bytes separation or std::array<char, 16> to re-implement
    auto addr = reinterpret_cast<double*>(&v);
    *addr += delta;
  }

  static std::string to_string(const int128_t& i) {
    constexpr auto digits = "0123456789";
    auto x = i < 0 ? -i : i;
    char buffer[128];
    char* ptr = std::end(buffer);
    do {
      --ptr;
      *ptr = digits[x % 10];
      x /= 10;
    } while (x != 0);

    // put the sign if negative value
    if (i < 0) {
      --ptr;
      *ptr = '-';
    }

    auto size = std::end(buffer) - ptr;
    return std::string(ptr, size);
  }
};

} // namespace common
} // namespace nebula

// place this in global namespace so that every file can benefit it
std::ostream& operator<<(std::ostream& os, int128_t i);