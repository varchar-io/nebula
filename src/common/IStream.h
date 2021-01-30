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

#include <istream>
#include <streambuf>

namespace nebula {
namespace common {

// Implement a simple quick memory buffer input stream
struct IBuf : std::streambuf {
  IBuf(char const* base, size_t size) {
    char* p(const_cast<char*>(base));
    this->setg(p, p, p + size);
  }
};

struct IStream : virtual IBuf, std::istream {
  IStream(char const* base, size_t size)
    : IBuf(base, size),
      std::istream(static_cast<std::streambuf*>(this)) {}
  IStream(std::string_view v)
    : IStream(v.data(), v.size()) {}
};

} // namespace common
} // namespace nebula
