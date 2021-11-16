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

#include <chrono>
#include <cmath>
#include <ctime>
#include <date/date.h>
#include <functional>
#include <glog/logging.h>
#include <iomanip>
#include <random>
#include <sstream>
#include <string_view>
#include <thread>

#include "Likely.h"

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
  static constexpr auto UNIX_1899_DEC_30 = -2209161600;
  static constexpr auto MINUTE_SECONDS = 60;
  static constexpr auto HOUR_SECONDS = 3600;
  static constexpr auto DAY_SECONDS = HOUR_SECONDS * 24;
  static constexpr auto NANO_BASE = 1000'000'000'000'000'000;
  static constexpr auto MICRO_BASE = 1000'000'000'000'000;
  static constexpr auto MILLI_BASE = 1000'000'000'000;

  // micro-seconds
  inline static size_t ticks() {
    return std::chrono::system_clock::now().time_since_epoch().count();
  }

  template <typename T>
  inline static size_t seconds(const std::chrono::time_point<T>& tp) {
    return std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count();
  }

  template <typename T>
  inline static size_t seconds(const std::chrono::duration<T>& dur) {
    return std::chrono::duration_cast<std::chrono::seconds>(dur).count();
  }

  // TODO(cao): consider using int64_t rather than size_t
  // to support time before unix epoch (1970)
  inline static size_t unix_timestamp() {
    return seconds(std::chrono::system_clock::now());
  }

  // convert a serial number to unix timestamp (seconds)
  inline static int64_t serial_2_unix(double serial) {
    return static_cast<int64_t>(serial * DAY_SECONDS + UNIX_1899_DEC_30);
  }

  //  no matter it is nano, micro, milli, or seconds, convert to seconds
  static int64_t to_seconds(int64_t timestamp) {
    if (timestamp / NANO_BASE > 0L) {
      return timestamp / 1000'000'000;
    }

    if (timestamp / MICRO_BASE > 0L) {
      return timestamp / 1000'000;
    }

    if (timestamp / MILLI_BASE > 0L) {
      return timestamp / 1000;
    }

    // assume seconds
    return timestamp;
  }

  /// copied from internet
  /// struct tm to seconds since Unix epoch
  static size_t my_timegm(struct tm* t) {
    static constexpr int MONTHSPERYEAR = 12;
    static constexpr int cumdays[MONTHSPERYEAR] = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };

    size_t result;
    int64_t year = 1900 + t->tm_year + t->tm_mon / MONTHSPERYEAR;
    result = (year - 1970) * 365 + cumdays[t->tm_mon % MONTHSPERYEAR];
    result += (year - 1968) / 4;
    result -= (year - 1900) / 100;
    result += (year - 1600) / 400;
    if ((year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0) && (t->tm_mon % MONTHSPERYEAR) < 2)
      result--;

    result += t->tm_mday - 1;
    result *= 24;

    result += t->tm_hour;
    result *= 60;

    result += t->tm_min;
    result *= 60;

    result += t->tm_sec;

    if (t->tm_isdst == 1)
      result -= 3600;

    return result;
  }

  // given date time string and parsing pattern, return GMT unix time stamp
  static size_t time(const std::string_view datetime, const std::string& pattern) {
    date::sys_time<std::chrono::microseconds> tp;
    std::istringstream value(datetime.data());
    value >> date::parse(pattern, tp);
    if (value.fail()) {
      LOG(ERROR) << "Failed to parse time: " << datetime;
      return 0;
    }

    return seconds(tp);
  }

  inline static std::time_t now() {
    return std::time(nullptr);
  }

  // given a date time value, stripe time out to date granularity
  // such as value of (2019-05-15 23:01:34) => value of (2019-05-15 00:00:00)
  static std::time_t date(std::time_t time) {
    auto tm = std::gmtime(&time);
    // caculate how much seconds to stripe
    auto delta = tm->tm_hour * 3600 + tm->tm_min * 60 + tm->tm_sec;
    return time - delta;
  }

  static std::string format(const std::time_t& time, const std::string& fmt, bool gmt = true) {
    constexpr auto MAX = 64;
    std::string output(MAX, '\0');
    // fmt spec: https://en.cppreference.com/w/cpp/chrono/c/strftime
    auto size = std::strftime(output.data(),
                              output.size(),
                              fmt.c_str(),
                              gmt ? std::gmtime(&time) : std::localtime(&time));

    if (N_LIKELY(size < MAX)) {
      output.resize(size);
    }

    return output;
  }

  inline static std::string fmt_normal(const std::time_t& time, bool gmt = true) {
    // normal display as date and time
    return format(time, "%c", gmt);
  }

  inline static std::string fmt_extra(const std::time_t& time, bool gmt = true) {
    // normal display with extra info like weekday name and timezone
    return format(time, "%A %c %Z", gmt);
  }

  inline static std::string fmt_ymd_dash(const std::time_t& time, bool gmt = true) {
    // equivalent to %F
    return format(time, "%Y-%m-%d", gmt);
  }

  inline static std::string fmt_ymd_slash(const std::time_t& time, bool gmt = true) {
    return format(time, "%Y/%m/%d", gmt);
  }

  inline static std::string fmt_mdy_slash(const std::time_t& time, bool gmt = true) {
    // equivalent to %D
    return format(time, "%m/%d/%Y", gmt);
  }

  inline static std::string fmt_mdy2_slash(const std::time_t& time, bool gmt = true) {
    // equivalent to %D
    return format(time, "%m/%d/%y", gmt);
  }

  inline static std::string fmt_mdy_dash(const std::time_t& time, bool gmt = true) {
    return format(time, "%m-%d-%Y", gmt);
  }

  inline static std::string fmt_hour(const std::time_t& time, bool gmt = true) {
    return format(time, "%H", gmt);
  }

  inline static std::string fmt_minute(const std::time_t& time, bool gmt = true) {
    return format(time, "%M", gmt);
  }

  inline static std::string fmt_second(const std::time_t& time, bool gmt = true) {
    return format(time, "%S", gmt);
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