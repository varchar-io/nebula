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

#include <functional>

/**
 * Provide a mechanism to clean up a routine when going out of scope.
 * Finally onExit([]() => {LOG(INFO) << "exit"; });
 */
namespace nebula {
namespace common {

class Finally final {
  using Routine = std::function<void()>;

public:
  explicit Finally(Routine ref) : ref_{ std::move(ref) } {}
  Finally(const Finally&) = delete;
  Finally(const Finally&&) = delete;
  Finally& operator=(const Finally&) = delete;
  Finally& operator=(const Finally&&) = delete;
  ~Finally() {
    release();
  }

private:
  inline void release() noexcept {
    Routine ref;
    ref_.swap(ref);
    if (ref) {
      ref();
    }
  }

private:
  Routine ref_;
};

} // namespace common
} // namespace nebula