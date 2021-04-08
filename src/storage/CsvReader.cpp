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
#include <sstream>

#include "CsvReader.h"
#include "common/Errors.h"

/**
 * A wrapper for reading csv file and return row cursor
 */
namespace nebula {
namespace storage {

static constexpr auto LF = '\n';
static constexpr auto CR = '\r';
static constexpr auto DQ = '"';

// ending a field by reading comma or LF or CRLF
// returning if the field is last one in current line
int32_t endField(std::istream& str, char ch, const char deli) {
  // end of file will end last field
  if (str.eof()) {
    return 1;
  }

  // line break as CRLF
  if (ch == CR) {
    str.get(ch);
    N_ENSURE(ch == LF, "end line as CRLF");
    return 1;
  }

  // single line break (unix) is fine (RFC4180 states CRLF always though)
  if (ch == LF) {
    return 1;
  }

  N_ENSURE(ch == deli, "has to be delimeter");
  return 0;
}

// form a cell value and return state: -1=no cell,0=non-last field, 1=last
int32_t field(std::istream& str, std::string& cell, const char deli) {
  char ch;
  // 1. do not start with any LF/CR
  while (!str.eof()) {
    str.read(&ch, 1);
    if (ch != LF && ch != CR) {
      break;
    }
  }

  // nothing valid to produce
  if (str.eof()) {
    return -1;
  }

  // seeing first letter as deli produces empty cell
  if (ch == deli) {
    return 0;
  }

  // per RFC4180, a field can be escaped or non-escaped
  const auto escaped = (ch == DQ);

  // add the char if non-escaped
  if (!escaped) {
    cell += ch;
  }

  // read char by char until it meets
  // escaped: ending DQ
  // non-escaped: comma or CRLF
  while (str.read(&ch, 1)) {
    if (escaped) {
      if (ch == DQ) {
        // if next char is also DQ, otherwise
        if (str.get(ch) && ch == DQ) {
          cell += DQ;
          continue;
        } else {
          // ending the field with stream, letter already read and delimeter
          return endField(str, ch, deli);
        }
      }
    } else {
      // for non-escaped case, we need to stop at comma, end line, or end file
      if (str.eof() || ch == CR || ch == LF) {
        return 1;
      }

      // stop at delimeter
      if (ch == deli) {
        break;
      }
    }

    // any other character just enqueue it
    cell += ch;
  }

  // only possible to be last field when it hits end of file
  return str.eof() ? 1 : 0;
}

// follow RFC4180 for CSV rules
void CsvRow::readNext(std::istream& str) {
  data_.clear();
  int state = -1;
  do {
    std::string cell;
    state = field(str, cell, delimiter_);
    if (state != -1 || data_.size() > 0) {
      data_.emplace_back(std::move(cell));
    }
  } while (state == 0);
}

std::istream& operator>>(std::istream& str, CsvRow& data) {
  data.readNext(str);
  return str;
}

} // namespace storage
} // namespace nebula