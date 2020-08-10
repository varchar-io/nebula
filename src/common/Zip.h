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

namespace nebula {
namespace common {

enum class ZipFormat {
  UNKNOWN = 0,
  DELTA = 1
};

// define a normal zip content represented by String
class Zip {
public:
  explicit Zip() : format_{ ZipFormat::UNKNOWN } {}
  // copy input data and format
  explicit Zip(const std::string& data, ZipFormat format)
    : data_{ data },
      format_{ format } {}
  explicit Zip(std::string&& data, ZipFormat format)
    : data_{ std::move(data) },
      format_{ format } {}

  inline std::string_view data() const noexcept {
    return data_;
  }

  inline ZipFormat format() const noexcept {
    return format_;
  }

private:
  std::string data_;
  ZipFormat format_;
};

} // namespace common
} // namespace nebula