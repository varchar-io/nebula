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

#include <chrono>
#include <functional>
#include <glog/logging.h>
#include <iomanip>
#include <random>
#include <sstream>
#include <string_view>
#include <thread>

/**
 * Evidence is library to provide evidences like time and random sequence generators.
 */
namespace nebula {
namespace common {
class Evidence {
private:
  Evidence() = default;
  ~Evidence() = default;

public: /** only static methods */
  inline static size_t ticks() {
    return std::chrono::system_clock::now().time_since_epoch().count();
  }

  inline static size_t unix_timestamp() {
    static constexpr std::chrono::time_point<std::chrono::system_clock> epoch;
    static constexpr auto x = epoch.time_since_epoch().count();
    return (ticks() - x) / 1000000;
  }

  // given date time string and parsing pattern, return GMT unix time stamp
  static std::time_t time(const std::string_view datetime, const std::string& pattern) {
    std::tm t = {};

    // set buffer to it - can we avoid constructing std::string here?
    std::istringstream value(datetime.data());
    value >> std::get_time(&t, pattern.c_str());
    if (value.fail()) {
      LOG(ERROR) << "Failed to parse time: " << datetime;
      return 0;
    }

    // convert this time into GMT based unixtime stamp
    return timegm(&t);
  }

  // given a date time value, stripe time out to date granularity
  // such as value of (2019-05-15 23:01:34) => value of (2019-05-15 00:00:00)
  static std::time_t date(std::time_t time) {
    auto tm = std::gmtime(&time);
    // caculate how much seconds to stripe
    auto delta = tm->tm_hour * 3600 + tm->tm_min * 60 + tm->tm_sec;
    return time - delta;
  }

  // rand in a range [min, max] inclusively
  template <typename T = int>
  inline static auto rand(size_t min, size_t max, size_t seed = unix_timestamp())
    -> std::function<std::enable_if_t<std::is_integral<T>::value, T>()> {
    auto x = std::bind(std::uniform_int_distribution<T>(min, max), std::mt19937(seed));
    return [=]() mutable {
      return x();
    };
  }

  // rand in a real range of [0,1) for percentiles
  template <typename T = double>
  inline static auto rand(size_t seed = unix_timestamp())
    -> std::function<std::enable_if_t<std::is_floating_point<T>::value, T>()> {
    auto x = std::bind(std::uniform_real_distribution<T>(0, 1), std::mt19937(seed));
    return [x]() mutable {
      return x();
    };
  }

  // wrapper sleep_for
  inline static void sleep(size_t millis) {
    std::this_thread::sleep_for(std::chrono::milliseconds(millis));
  }

  class Duration final {
  public:
    Duration() : tick_{ std::chrono::high_resolution_clock::now() } {}

    void reset() noexcept {
      tick_ = std::chrono::high_resolution_clock::now();
    }

    auto elapsedMs() const noexcept {
      return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - tick_).count();
    }

  private:
    std::chrono::time_point<std::chrono::high_resolution_clock> tick_;
  };
};
} // namespace common
} // namespace nebula