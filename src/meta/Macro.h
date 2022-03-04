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

#include <regex>
#include <string>

#include "common/Chars.h"
#include "common/Evidence.h"
#include "common/Format.h"
#include "common/Hash.h"

/**
 * Define macro supported in Nebula.
 */
namespace nebula {
namespace meta {
// type of macros accepted in file path, doesn't rely on time spec
enum PatternMacro : uint8_t {
  // Daily partition /dt={DATE}?
  DAILY = 0x1,
  // hourly partition name /dt={DATE}/hr={HOUR}
  HOURLY = 0x2,
  // minute partition name /dt={DATE}/hr={HOUR}/mi={MINUTE}
  MINUTELY = 0x4,
  // use second level directory name /dt={DATE}/hr={HOUR}/mi={MINUTE}/se={SECOND}
  SECONDLY = 0x8,
  // use directory name in unix timestamp (seconds) /ts={TIMESTAMP}
  TIMESTAMP = 0x16,
  // placeholder for not accepted marcos
  INVALID = 0,
};

class Macro {
  // Every macro requires its parent level to present - all the way up to DAILY
  // valid patterns:
  // daily -> {date}
  // hourly -> {date} and {hour} both present
  // minutely -> {date}, {hour}, and {minute} all present
  // secondly -> {date}, {hour}, {minute}, and {second} all present
  // otherwise, it is invalid
  // timestamp could be present by itself
  static constexpr auto TIMESTAMP = PatternMacro::TIMESTAMP;
  static constexpr auto DAILY = PatternMacro::DAILY;
  static constexpr auto HOURLY = DAILY | PatternMacro::HOURLY;
  static constexpr auto MINUTELY = HOURLY | PatternMacro::MINUTELY;
  static constexpr auto SECONDLY = MINUTELY | PatternMacro::SECONDLY;

  // literal constants
  static constexpr std::string_view V_DATE = "date";
  static constexpr std::string_view V_HOUR = "hour";
  static constexpr std::string_view V_MINUTE = "minute";
  static constexpr std::string_view V_SECOND = "second";
  static constexpr std::string_view V_TIMESTAMP = "timestamp";

  // ALL MACRO (note that, static inline const initializing a static data member is cpp 17 feature)
  static inline const std::regex PATTERN{ "\\{(\\w+)\\}", std::regex_constants::icase };

  // MACRO literal mapping to the entry
  static inline const nebula::common::unordered_map<std::string, PatternMacro> MACRO_MAP{
    { V_DATE, PatternMacro::DAILY },
    { V_HOUR, PatternMacro::HOURLY },
    { V_MINUTE, PatternMacro::MINUTELY },
    { V_SECOND, PatternMacro::SECONDLY },
    { V_TIMESTAMP, PatternMacro::TIMESTAMP }
  };

  // How many seconds in each MACRO unit.
  static inline const nebula::common::unordered_map<PatternMacro, std::regex> MACRO_REGEX{
    { PatternMacro::DAILY, std::regex("\\{date\\}", std::regex_constants::icase) },
    { PatternMacro::HOURLY, std::regex("\\{hour\\}", std::regex_constants::icase) },
    { PatternMacro::MINUTELY, std::regex("\\{minute\\}", std::regex_constants::icase) },
    { PatternMacro::SECONDLY, std::regex("\\{second\\}", std::regex_constants::icase) },
    { PatternMacro::TIMESTAMP, std::regex("\\{timestamp\\}", std::regex_constants::icase) }
  };

  // How many seconds in each MACRO unit.
  static inline const nebula::common::unordered_map<PatternMacro, size_t> MACRO_SECONDS{
    { PatternMacro::DAILY, nebula::common::Evidence::DAY_SECONDS },
    { PatternMacro::HOURLY, nebula::common::Evidence::HOUR_SECONDS },
    { PatternMacro::MINUTELY, nebula::common::Evidence::MINUTE_SECONDS },
    { PatternMacro::MINUTELY, 1 }
  };

  static inline const std::vector<PatternMacro> ALL_TIME_MACROS{
    PatternMacro::DAILY,
    PatternMacro::HOURLY,
    PatternMacro::MINUTELY,
    PatternMacro::SECONDLY
  };

public:
  // seconds of granularity for given macro
  static inline size_t seconds(PatternMacro macro) {
    auto found = MACRO_SECONDS.find(macro);
    if (found != MACRO_SECONDS.end()) {
      return found->second;
    }

    return 0;
  }

  static inline size_t watermark(const common::unordered_map<std::string_view, std::string_view>& p) {
    size_t watermark = 0;
    for (auto itr = p.cbegin(); itr != p.cend(); ++itr) {
      auto key = itr->first;
      if (common::Chars::same(key, V_DATE)) {
        watermark += common::Evidence::time(itr->second, "%Y-%m-%d");
      } else if (common::Chars::same(key, V_HOUR)) {
        watermark += std::stol(std::string(itr->second)) * common::Evidence::HOUR_SECONDS;
      } else if (common::Chars::same(key, V_MINUTE)) {
        watermark += std::stol(std::string(itr->second)) * common::Evidence::MINUTE_SECONDS;
      } else if (common::Chars::same(key, V_SECOND)) {
        watermark += std::stol(std::string(itr->second));
      }
    }

    return watermark;
  }

  static inline bool isTimeMacroString(const std::string& str) {
    return (MACRO_MAP.find(str) != MACRO_MAP.end());
  }

  static inline std::string getTimeStringForMacroString(const std::string& str, const std::time_t& watermark) {
    return getTimeStringForMacro(MACRO_MAP.at(str), watermark);
  }

  static inline std::string getTimeStringForMacro(PatternMacro macro, const std::time_t& watermark, const std::string& defaultStr="") {
    switch (macro) {
      case PatternMacro::TIMESTAMP: {
        return std::to_string(watermark);
      }
      case PatternMacro::DAILY: {
        return nebula::common::Evidence::fmt_ymd_dash(watermark);
      }
      case PatternMacro::HOURLY: {
        return nebula::common::Evidence::fmt_hour(watermark);
      }
      case PatternMacro::MINUTELY: {
        return nebula::common::Evidence::fmt_minute(watermark);
      }
      case PatternMacro::SECONDLY: {
        return nebula::common::Evidence::fmt_second(watermark);
      }
      default: {
        return defaultStr;
      }
    }
  }

  static inline std::string replaceTimeMacro(PatternMacro macro, const std::string& str, const std::time_t& watermark) {
    return std::regex_replace(str, MACRO_REGEX.at(macro), getTimeStringForMacro(macro, watermark));
  }

  // materialize a template with all macros based on the provided watermark
  // e.g
  // "s3://nebula/{DATE}" -> "2020-12-20"
  static inline std::string materialize(PatternMacro macro,
                                        const std::string& holder,
                                        const std::time_t& watermark) {
    if (macro == PatternMacro::INVALID) {
      return holder;
    }
    if (macro == PatternMacro::TIMESTAMP) {
      return replaceTimeMacro(macro, holder, watermark);
    }
    auto str = holder;
    for (const auto& macroToCheck : ALL_TIME_MACROS) {
      if (macro >= macroToCheck) {
        str = replaceTimeMacro(macroToCheck, str, watermark);
      }
    }

    // we have finish replacing all macros
    return str;
  }

  static inline const std::map<std::string, std::vector<std::string>> filteredMacroValuesMap(const std::string& input, const std::map<std::string, std::vector<std::string>>& macroValues) {
    std::map<std::string, std::vector<std::string>> filteredMacroValues;
    for (auto it = macroValues.begin(); it != macroValues.end(); ++it) {
      const auto searchString = fmt::format("{{{}}}", it->first);
      if (input.find(searchString) != std::string::npos) {
        filteredMacroValues.emplace(it->first, it->second);
      }
    }
    return filteredMacroValues;
  }

  static inline const std::vector<nebula::common::unordered_map<std::string_view, std::string_view>> enumerateMacroCombinations(const std::map<std::string, std::vector<std::string>>& filteredMacroValues) {
    std::vector<nebula::common::unordered_map<std::string_view, std::string_view>> macroCombinations;
    for (auto it = filteredMacroValues.begin(); it != filteredMacroValues.end(); ++it) {
      if (it == filteredMacroValues.begin()) {
        for (const auto& val : it->second) {
          nebula::common::unordered_map<std::string_view, std::string_view> newCombination = { { it->first, val } };
          macroCombinations.emplace_back(newCombination);
        }
        continue;
      }
      std::vector<nebula::common::unordered_map<std::string_view, std::string_view>> newMacroCombinations;
      for (const auto& combination : macroCombinations) {
        for (const auto& val : it->second) {
          // intentional copy
          auto newCombination = combination;
          newCombination.emplace(it->first, val);
          newMacroCombinations.emplace_back(newCombination);
        }
      }
      macroCombinations = newMacroCombinations;
    }
    return macroCombinations;
  }

  static inline const std::vector<std::pair<std::string, nebula::common::unordered_map<std::string, std::string>>>
    enumeratePathsWithCustomMacros(const std::string& input, const std::map<std::string, std::vector<std::string>>& macroValues) {
    std::vector<std::pair<std::string, nebula::common::unordered_map<std::string, std::string>>> results;
    const auto& filteredMacroValues = Macro::filteredMacroValuesMap(input, macroValues);
    const auto& macroCombinations = Macro::enumerateMacroCombinations(filteredMacroValues);
    if (macroCombinations.size() == 0) {
      return { std::make_pair(
        input,
        nebula::common::unordered_map<std::string, std::string>()) };
    }

    for (const auto& macroCombination : macroCombinations) {
      // need to manually copy to make string out of string_view
      nebula::common::unordered_map<std::string, std::string> stringMacroCombinations;
      for (const auto& p : macroCombination) {
        stringMacroCombinations.emplace(p.first, p.second);
      }
      results.push_back(
        std::make_pair(
          nebula::common::format(
            input,
            macroCombination,
            // allow missing macros for time macros
            true),
          stringMacroCombinations));
    }
    return results;
  }

  // extract pattern used in a given path
  // for example, "s3://nebula/dt={DATE}/dt={HOUR} -> HOURLY"
  static inline PatternMacro extract(const std::string& input) {
    // match all macros if available
    auto mBegin = std::sregex_iterator(input.begin(), input.end(), PATTERN);
    auto mEnd = std::sregex_iterator();
    uint8_t code = PatternMacro::INVALID;

    for (auto i = mBegin; i != mEnd; ++i) {
      // get the group member to avoid wrapper `{}`
      auto str = i->str(1);
      nebula::common::Chars::lower(str);

      auto found = MACRO_MAP.find(str);
      if (found != MACRO_MAP.end()) {
        code |= found->second;
      }
    }

#define TYPE_CHECK(V)       \
  if (code == V) {          \
    return PatternMacro::V; \
  }

    TYPE_CHECK(TIMESTAMP)
    TYPE_CHECK(DAILY)
    TYPE_CHECK(HOURLY)
    TYPE_CHECK(MINUTELY)
    TYPE_CHECK(SECONDLY)
#undef TYPE_CHECK

    return PatternMacro::INVALID;
  }
};

} // namespace meta
} // namespace nebula