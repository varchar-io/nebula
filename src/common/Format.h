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

#include <string>
#include <unordered_map>

#include "Errors.h"

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
std::string format(std::string_view fmtstr, std::unordered_map<std::string_view, std::string_view> macros) {
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

} // namespace common
} // namespace nebula