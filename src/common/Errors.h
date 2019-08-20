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

#include <fmt/format.h>

namespace nebula {
namespace common {

class NebulaException : public std::exception {
public:
  NebulaException(const std::string& file,
                  const uint32_t line,
                  const std::string& method,
                  const std::string& expr,
                  const std::string& msg);
  virtual ~NebulaException() = default;

  virtual const char* what() const noexcept override {
    return format_.c_str();
  }

private:
  std::string format_;
};

// fetch file name only
using cstr = const char*;
static constexpr size_t length(cstr str) {
  return *str ? 1 + length(str + 1) : 0;
}

static constexpr cstr lslash(cstr str, size_t pos) noexcept {
  while (pos > 0 && *(str + pos) != '/') {
    --pos;
  }
  return (str + pos + 1);
}

static constexpr cstr lslash(cstr str) {
  return lslash(str, length(str));
}
#define __NFILE__ ({constexpr nebula::common::cstr nf__ {nebula::common::lslash(__FILE__)}; nf__; })

#define NException(msg) \
  nebula::common::NebulaException(__NFILE__, __LINE__, __FUNCTION__, "None", msg)

#define THROW_NOT_IMPLEMENTED()                                                                     \
  ({                                                                                                \
    throw nebula::common::NebulaException(__NFILE__, __LINE__, __FUNCTION__, "NotImplemented", ""); \
  })

#define THROW_RUNTIME(MSG)                                                                         \
  ({                                                                                               \
    throw nebula::common::NebulaException(__NFILE__, __LINE__, __FUNCTION__, "RuntimeError", MSG); \
  })

#define N_ENSURE(e, msg)                                                                             \
  ({                                                                                                 \
    auto const& _tmp = (e);                                                                          \
    _tmp ? _tmp : throw nebula::common::NebulaException(__NFILE__, __LINE__, __FUNCTION__, #e, msg); \
  })

#define N_ENSURE_NOT_NULL(p, m) \
  N_ENSURE(p != nullptr, fmt::format("[NULL-{0}]: {1}", #p, m));

#define N_ENSURE_NE(l, r, m)                                               \
  ({                                                                       \
    auto msg = fmt::format("[Range Violation: {0} != {1}]: {2}", l, r, m); \
    N_ENSURE(l != r, msg);                                                 \
  })

#define N_ENSURE_EQ(l, r, m)                                               \
  ({                                                                       \
    auto msg = fmt::format("[Range Violation: {0} == {1}]: {2}", l, r, m); \
    N_ENSURE(l == r, msg);                                                 \
  })

#define N_ENSURE_LT(l, r, m)                                              \
  ({                                                                      \
    auto msg = fmt::format("[Range Violation: {0} < {1}]: {2}", l, r, m); \
    N_ENSURE(l < r, msg);                                                 \
  })

#define N_ENSURE_LE(l, r, m)                                               \
  ({                                                                       \
    auto msg = fmt::format("[Range Violation: {0} <= {1}]: {2}", l, r, m); \
    N_ENSURE(l <= r, msg);                                                 \
  })

#define N_ENSURE_GT(l, r, m)                                              \
  ({                                                                      \
    auto msg = fmt::format("[Range Violation: {0} > {1}]: {2}", l, r, m); \
    N_ENSURE(l > r, msg);                                                 \
  })

#define N_ENSURE_GE(l, r, m)                                               \
  ({                                                                       \
    auto msg = fmt::format("[Range Violation: {0} >= {1}]: {2}", l, r, m); \
    N_ENSURE(l >= r, msg);                                                 \
  })
} // namespace common
} // namespace nebula