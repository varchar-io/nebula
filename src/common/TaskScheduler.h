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

#include <folly/io/async/EventBase.h>
#include <folly/io/async/HHWheelTimer.h>

namespace nebula {
namespace common {

// this is a tasks scheduler based folly HHWheelTimer
class TaskScheduler {
public:
  template <typename F>
  void setInterval(size_t ms, F fn) {
    eventBase_.timer().scheduleTimeoutFn(
      [f = std::move(fn), ms, this] {
        f();

        // after the function is done, reschedule it again before exiting.
        // if stopped signals, we don't schedule anymore to allow server shutdown
        if (!stopped_) {
          this->setInterval(ms, std::move(f));
        }
      },
      std::chrono::milliseconds(ms));
  }

  // schedule the function once
  template <typename F>
  void setTimeout(size_t ms, F fn) {
    auto& timer = eventBase_.timer();
    timer.scheduleTimeoutFn(std::move(fn), std::chrono::milliseconds(ms));
  }

  inline void run() {
    stopped_ = false;
    eventBase_.loop();
  }

  inline void stop() {
    stopped_ = true;
  }

private:
  folly::EventBase eventBase_;
  std::atomic<bool> stopped_;
};

} // namespace common
} // namespace nebula