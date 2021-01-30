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

#include "CsvReader.h"
#include <sstream>

/**
 * A wrapper for reading csv file and return row cursor
 */
namespace nebula {
namespace storage {
void CsvRow::readNext(std::istream& str) {
  std::string line;
  std::getline(str, line);

  std::stringstream lineStream(line);
  std::string cell;

  data_.clear();
  while (std::getline(lineStream, cell, delimiter_)) {
    data_.push_back(folly::trimWhitespace(cell).toString());
  }

  // This checks for a trailing comma with no data after it.
  if (!lineStream && cell.empty()) {
    // If there was a trailing comma then add an empty element.
    data_.push_back("");
  }
}

std::istream& operator>>(std::istream& str, CsvRow& data) {
  data.readNext(str);
  return str;
}

} // namespace storage
} // namespace nebula