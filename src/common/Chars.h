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

#include "Hash.h"

/**
 * Some utility functions related to strings
 */
namespace nebula {
namespace common {
// split a char streams into a vector of strings
class Chars {
public:
  template <bool order = false,
            typename T = std::conditional_t<order,
                                            std::vector<std::string>,
                                            nebula::common::unordered_set<std::string>>>
  static T split(const char* data, size_t size, char delimeter = ',') {
    if (!data || size < 1) {
      return T();
    }

    size_t start = 0;
    size_t end = 0;
    T set;

// a bit ugly: branch based on order (vector vs set)
#define ADD_SEGMENT                                \
  if (start != end) {                              \
    if constexpr (!order) {                        \
      set.emplace(data + start, end - start);      \
    } else {                                       \
      set.emplace_back(data + start, end - start); \
    }                                              \
  }

    while (end < size) {
      // found delimeter, process
      if (*(data + end) == delimeter) {
        ADD_SEGMENT

        // prepare next char
        start = end + 1;
      }

      // move to next char
      ++end;
    }

    // capture the last segment after the last delimeter
    ADD_SEGMENT

#undef ADD_SEGMENT

    return set;
  }

  // in-place converting a string to lower case
  static inline void lower(std::string& str) {
    for (auto itr = str.begin(); itr != str.end(); ++itr) {
      *itr = std::tolower(*itr);
    }
  }

  // make a copy of string in lower case
  static inline std::string lower_copy(const std::string& str) {
    const auto size = str.size();
    std::string copy(size, 0);
    for (size_t i = 0; i < size; ++i) {
      copy.at(i) = std::tolower(str.at(i));
    }
    return copy;
  }

  // convert a name into a path by replacing all spot into '/' as well as started with it
  static inline std::string path(const char* data, size_t size, char spot = '.') noexcept {
    // give it a double size as max capacity
    std::string p(size + 1, '/');

    // copy data into this buffer
    for (size_t i = 0; i < size; ++i) {
      char ch = *(data + i);
      p[i + 1] = ch == spot ? '/' : ch;
    }

    return p;
  }

  // get last section of the given string if splittable by delimeter
  // if no delimmeter found, return original string view
  static std::string_view last(std::string_view orig, char delimeter = '/') {
    auto data = orig.data();
    auto pos = orig.size();
    // check pos-1 to be delimeter and stop
    while (pos > 0 && data[pos - 1] != delimeter) {
      --pos;
    }

    return std::string_view(data + pos, orig.size() - pos);
  }

  // char equals
  static inline bool eq(char a, char b) {
    return a == b;
  }

  // char equlas ignoring case
  static inline bool ieq(char a, char b) {
    return a == b || std::tolower(a) == std::tolower(b);
  }

  // shortcut: two string views have the same value ignoring case
  static inline bool same(std::string_view v1, std::string_view v2, bool ignoreCase = true) {
    return v1.size() == v2.size() && prefix(v1.data(), v1.size(), v2.data(), v2.size(), ignoreCase);
  }

  // shortcut of prefix function in sensitive case
  static bool prefix(const std::string_view text, const std::string_view prefix) {
    const auto size = prefix.size();
    return (size == 0) || (text.size() >= size && std::memcmp(text.data(), prefix.data(), size) == 0);
  }

  // prefix function with char to char comparison
  static bool prefix(const char* src, size_t s_size,
                     const char* target, size_t t_size,
                     bool ignoreCase = true) {
    if (s_size < t_size) {
      return false;
    }

    bool (*eqf)(char a, char b) = ignoreCase ? &ieq : &eq;
    for (size_t i = 0; i < t_size; ++i) {
      if (!(*eqf)(*(src + i), *(target + i))) {
        return false;
      }
    }

    return true;
  }

  static std::string digest(const char* str, size_t size) {
    // sizeof a c-string will include the ending 0, we want to avoid that to be hit in our final code
    static constexpr char CTABLE[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    static constexpr auto STABLE = sizeof(CTABLE) - 1;
    static constexpr auto LENGTH = 6;

    // minimal requirement, otherwise return itself
    if (size < LENGTH) {
      return std::string(str, size);
    }

    std::string token(LENGTH, '0');
    nebula::common::Hasher hash;
    auto step = size / LENGTH;
    size_t i = 0;
    while (i < size) {
      auto code = hash.hash64(str + i, std::min<size_t>(size - i, step));
      token[i / step] = CTABLE[code % STABLE];
      i += step;
    }

    return token;
  }
};
} // namespace common
} // namespace nebula