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
#include <string>

#include "Errors.h"
#include "Hash.h"

/**
 * I do not find a good way to format a dynamic number of named args through fmtlib.
 * So have this simple one to use internally in Nebula.
 * Something should work like:
 *  std::vector<fmt::internal::named_arg<std::string, std::string>> args;
 *  args.reserve(1);
 *  args.emplace_back(fmt::arg("x", "va"));
 *  auto text = fmt::vformat("{x}", fmt::basic_format_args(&args, 1));
 * 
 */

namespace nebula {
namespace common {
// use this util function like:
// auto text = nebula::common::format("I will go to {p} to do {x}", {{"p", "place"}, {"x", "something"}});
// supporting max result size as 1024, expect exception if overflowing.
template <size_t x = 0>
std::string format(const std::string_view fmtstr, const unordered_map<std::string_view, std::string_view> macros) {
  // support only MAX TEXT length for final resul, throw exception if not meet the expectation
  static constexpr auto MAX_TEXT = 1024;
  char buffer[MAX_TEXT];
  size_t bufferPos = 0;

  // go through fmtstr, if meet {, token starts, if meet }, token ends, otherwise mark them as -1
  static constexpr size_t SIZE_T_LIMIT = std::numeric_limits<size_t>::max();
  static constexpr char TOKEN_START = '{';
  static constexpr char TOKEN_END = '}';

  // TODO(cao) - we don't support escape TOKEN symbols such as "{{" or "}}"
  size_t tokenStart = SIZE_T_LIMIT;
  const auto data = fmtstr.data();
  for (size_t i = 0, size = fmtstr.size(); i < size; ++i) {
    // token started, looking for token end
    auto ch = *(data + i);
    if (tokenStart != SIZE_T_LIMIT) {
      if (ch == TOKEN_END) {
        auto token = std::string_view(data + tokenStart + 1, i - tokenStart - 1);
        // looking for the token and memcopy its value into buffer
        auto found = macros.find(token);
        if (found == macros.end()) {
          LOG(INFO) << "Token not found: " << token;
          throw NException(fmt::format("token not found for fmt string: {0}.", token));
        }

        // copy the value into buffer
        const auto& value = found->second;
        const auto size = value.size();
        N_ENSURE(bufferPos + size < MAX_TEXT, "format result is overflowing supported size.");
        std::memcpy(buffer + bufferPos, value.data(), size);
        bufferPos += size;

        // reset the token start for next one
        tokenStart = SIZE_T_LIMIT;
      }
    } else if (ch == TOKEN_START) {
      // mark token to start
      tokenStart = i;
    } else {
      // just copy the char into buffer
      buffer[bufferPos++] = ch;
    }
  }

  // when the loop is done, tokenStart has to be 0 to indicate all tokens are replaced
  N_ENSURE_EQ(tokenStart, SIZE_T_LIMIT, "some open token not replaced - wrong format string.");
  N_ENSURE(bufferPos < MAX_TEXT, "format string is too long.");

  return std::string(buffer, bufferPos);
}

static inline std::string normalize(const std::string& input) {
  static constexpr auto UNDERSCORE = '_';
  std::string buffer;
  char last = '-';
  for (char ch : input) {
    if (std::isdigit(ch) || std::isalpha(ch) || (ch == UNDERSCORE && last != UNDERSCORE)) {
      buffer.append(1, std::tolower(ch));
      last = ch;
    } else if (last != UNDERSCORE) {
      buffer.append(1, UNDERSCORE);
      last = UNDERSCORE;
    }
  }

  return buffer;
}

template <typename T>
static inline void unformat(std::string& input) {
  if constexpr (std::is_arithmetic_v<T>) {
    size_t r = 0, w = 0;
    for (size_t size = input.size(); r < size; ++r) {
      auto ch = input.at(r);

      if constexpr (std::is_integral_v<T>) {
        // dot means end
        if (ch == '.') {
          ++r;
          break;
        }
      }

      // treat comma and space as format - remove them
      // we may want to leverage localization provider to handle this
      if (ch != ',' && ch != ' ') {
        if (w != r) {
          input.at(w) = input.at(r);
        }
        w++;
      }
    }

    if (w < r) {
      input.resize(w);
    }
  }
}

// sheet column name pattern: [A-Z, AA, AB...ZZ]
// get range of the sheet => A1:{LAST_COL}{MAX_ROW}

static inline std::string sheetColName(size_t size) {
  static const auto MAX_SIZE = 26 * 27;
  static const auto BASE = 'A';
  static const auto BAND = 26;

  // maximum column to support is ZZ
  if (--size >= MAX_SIZE) {
    return "ZZ";
  }

  std::string str;

  // maximum support till ZZ
  auto high = size / BAND;
  auto low = size % BAND;
  if (high > 0) {
    str.append(1, BASE + high - 1);
  }
  str.append(1, BASE + low);
  return str;
}

} // namespace common
} // namespace nebula