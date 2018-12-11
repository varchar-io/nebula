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

#include "fmt/format.h"

namespace nebula {
namespace common {
class NebulaException {
public:
  NebulaException(const std::string& msg);
  NebulaException(const std::string& file,
                  const uint32_t line,
                  const std::string& method,
                  const std::string& expr,
                  const std::string& msg) : file_{ file }, line_{ line }, method_{ method }, expr_{ expr }, msg_{ msg } {
  }
  virtual ~NebulaException() = default;

  std::string toString() const {
    return fmt::format("[FILE={0}][LINE={1}][FUNC={2}] Failed on \"{3}\": {4}",
                       file_, line_, method_, expr_, msg_);
  }

private:
  std::string file_;
  uint32_t line_;
  std::string method_;
  std::string expr_;
  std::string msg_;
};

#define N_ENSURE(e, ...)                                                            \
  ({                                                                                \
    auto const& _tmp = (e);                                                         \
    auto msg = fmt::format(",", __VA_ARGS__);                                       \
    _tmp ? _tmp : throw NebulaException(__FILE__, __LINE__, __FUNCTION__, #e, msg); \
  })

#define DWIO_ENSURE_NOT_NULL(p, ...) \
  N_ENSURE(p != nullptr, "[Null pointer] : ", ##__VA_ARGS__);

#define N_ENSURE_NE(l, r, ...)        \
  N_ENSURE(                           \
    l != r,                           \
    "[Range Constraint Violation : ", \
    l,                                \
    "/",                              \
    r,                                \
    "] : ",                           \
    ##__VA_ARGS__);

#define N_ENSURE_EQ(l, r, ...)        \
  N_ENSURE(                           \
    l == r,                           \
    "[Range Constraint Violation : ", \
    l,                                \
    "/",                              \
    r,                                \
    "] : ",                           \
    ##__VA_ARGS__);

#define N_ENSURE_LT(l, r, ...)        \
  N_ENSURE(                           \
    l < r,                            \
    "[Range Constraint Violation : ", \
    l,                                \
    "/",                              \
    r,                                \
    "] : ",                           \
    ##__VA_ARGS__);

#define N_ENSURE_LE(l, r, ...)        \
  N_ENSURE(                           \
    l <= r,                           \
    "[Range Constraint Violation : ", \
    l,                                \
    "/",                              \
    r,                                \
    "] : ",                           \
    ##__VA_ARGS__);

#define N_ENSURE_GT(l, r, ...)        \
  N_ENSURE(                           \
    l > r,                            \
    "[Range Constraint Violation : ", \
    l,                                \
    "/",                              \
    r,                                \
    "] : ",                           \
    ##__VA_ARGS__);

#define N_ENSURE_GE(l, r, ...)        \
  N_ENSURE(                           \
    l >= r,                           \
    "[Range Constraint Violation : ", \
    l,                                \
    "/",                              \
    r,                                \
    "] : ",                           \
    ##__VA_ARGS__);
} // namespace common
} // namespace nebula