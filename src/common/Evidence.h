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
  static std::time_t time(const std::string& datetime, const std::string& pattern) {
    std::tm t = {};
    std::istringstream value(datetime);
    value >> std::get_time(&t, pattern.c_str());
    if (value.fail()) {
      LOG(ERROR) << "Failed to parse time: " << datetime;
      return 0;
    }

    // convert this time into GMT based unixtime stamp
    return timegm(&t);
  }

  // rand in a range
  inline static auto rand(size_t min, size_t max, size_t seed = unix_timestamp()) -> std::function<size_t()> {
    auto x = std::bind(std::uniform_int_distribution<size_t>(min, max), std::mt19937(seed));
    return [=]() mutable {
      return x();
    };
  }

  // rand in a real range of [0,1) for percentiles
  inline static auto rand(size_t seed = unix_timestamp()) -> std::function<double()> {
    auto x = std::bind(std::uniform_real_distribution<double>(0, 1), std::mt19937(seed));
    return [x]() mutable {
      return x();
    };
  }

  // wrapper sleep_for
  inline static void sleep(size_t millis) {
    std::this_thread::sleep_for(std::chrono::milliseconds(millis));
  }
};
} // namespace common
} // namespace nebula