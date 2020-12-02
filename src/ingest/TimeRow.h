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

#include "meta/TableSpec.h"

/**
 * A time row is a row wrapper to provide time value computing based on time spec.
 */
namespace nebula {
namespace ingest {

// row wrapper to translate "date" string into reserved "_time_" column
class TimeRow : public nebula::surface::RowData {
public:
  TimeRow(const nebula::meta::TimeSpec& ts, size_t watermark)
    : timeFunc_{ makeTimeFunc(ts, watermark) } {}
  ~TimeRow() = default;

  const TimeRow& set(const nebula::surface::RowData* row) {
    row_ = row;
    return *this;
  }

// raw date to _time_ columm in ingestion time
#define TRANSFER(TYPE, FUNC)                           \
  TYPE FUNC(const std::string& field) const override { \
    return row_->FUNC(field);                          \
  }

  TRANSFER(bool, readBool)
  TRANSFER(int8_t, readByte)
  TRANSFER(int16_t, readShort)
  TRANSFER(int32_t, readInt)
  TRANSFER(std::string_view, readString)
  TRANSFER(float, readFloat)
  TRANSFER(double, readDouble)
  TRANSFER(int128_t, readInt128)
  TRANSFER(std::unique_ptr<nebula::surface::ListData>, readList)
  TRANSFER(std::unique_ptr<nebula::surface::MapData>, readMap)

  bool isNull(const std::string& field) const override {
    if (UNLIKELY(field == nebula::meta::Table::TIME_COLUMN)) {
      // timestamp in string 2016-07-15 14:38:03
      return false;
    }

    return row_->isNull(field);
  }

  // _time_ is in long type and it's coming from date string
  int64_t readLong(const std::string& field) const override {
    if (UNLIKELY(field == nebula::meta::Table::TIME_COLUMN)) {
      // timestamp in string 2016-07-15 14:38:03
      return timeFunc_(row_);
    }

    return row_->readLong(field);
  }

private:
  // A method to convert time spec into a time function
  std::function<int64_t(const nebula::surface::RowData*)> makeTimeFunc(const nebula::meta::TimeSpec& ts, size_t watermark) {
    // static time spec
    switch (ts.type) {
    case nebula::meta::TimeType::STATIC: {
      return [value = ts.unixTimeValue](const nebula::surface::RowData*) { return value; };
      break;
    }
    case nebula::meta::TimeType::CURRENT: {
      return [](const nebula::surface::RowData*) { return nebula::common::Evidence::unix_timestamp(); };
      break;
    }
    case nebula::meta::TimeType::COLUMN: {
      // TODO(cao) - currently only support string column with time pattern
      // and unix time stamp value as bigint if pattern is absent.
      // ts.pattern is required: time string pattern or special value such as UNIXTIME

      // Note: time column can not be NULL
      // unfortunately if the data has it as null, we return 1 as indicator
      // we can not use 0, because Nebula doesn't allow query time range fall into 0 start/end.
      static constexpr size_t NULL_TIME = 1;
      constexpr auto UNIX_TS = "UNIXTIME";
      constexpr auto UNIX_MS = "UNIXTIME_MS";
      constexpr auto UNIX_NANO = "UNIXTIME_NANO";
      constexpr auto SERIAL_NUMBER = "SERIAL_NUMBER";

      // SERIAL_NUMBER translation
      if (ts.pattern == SERIAL_NUMBER) {
        return [col = ts.colName](const nebula::surface::RowData* r) {
          if (UNLIKELY(r->isNull(col))) {
            return NULL_TIME;
          }

          // TODO(cao): support negative unix_time by replacing size_t type
          return (size_t)nebula::common::Evidence::serial_2_unix(r->readDouble(col));
        };
      }

      auto scale = 0;
      if (ts.pattern == UNIX_TS) {
        scale = 1;
      } else if (ts.pattern == UNIX_MS) {
        scale = 1000;
      } else if (ts.pattern == UNIX_NANO) {
        scale = 1000000000;
      }

      // unix time (bigint) conversion
      if (scale > 0) {
        return [col = ts.colName, scale](const nebula::surface::RowData* r) {
          if (UNLIKELY(r->isNull(col))) {
            return NULL_TIME;
          }

          return (size_t)r->readLong(col) / scale;
        };
      }

      // last option: string of time pattern such as 'yyyy-mm-dd'
      return [col = ts.colName, pattern = ts.pattern](const nebula::surface::RowData* r) {
        if (UNLIKELY(r->isNull(col))) {
          return NULL_TIME;
        }

        return nebula::common::Evidence::time(r->readString(col), pattern);
      };
      break;
    }
    case nebula::meta::TimeType::MACRO: {
      if (nebula::meta::extractPatternMacro(ts.pattern) != nebula::meta::PatternMacro::INVALID) {
        // use partition time as batch watermark
        return [watermark](const nebula::surface::RowData*) {
          return watermark;
        };
      } else {
        return [](const nebula::surface::RowData*) { return 0; };
      }
      break;
    }
    case nebula::meta::TimeType::PROVIDED: {
      return [](const nebula::surface::RowData* r) { return r->readLong(nebula::meta::Table::TIME_COLUMN); };
    }
    default: {
      LOG(ERROR) << "Unsupported time type: " << (int)ts.type;
      return {};
    }
    }
  }

private:
  std::function<int64_t(const nebula::surface::RowData*)> timeFunc_;
  const nebula::surface::RowData* row_;
};

} // namespace ingest
} // namespace nebula