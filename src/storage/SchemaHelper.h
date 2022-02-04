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

#include <fmt/format.h>
#include <string>
#include <vector>

#include "common/Hash.h"

// in some scenarios - such as CSV reading and sheet data loading.
// we need to figure out column mapping, here we need some utility to handle some common cases

namespace nebula {
namespace storage {
// if we get a bunch of duplicate names, let's use sequence number to diff them
static void dedup(std::vector<std::string>& columns) {
  nebula::common::unordered_set<std::string> names;
  for (auto itr = columns.begin(); itr != columns.end(); ++itr) {
    auto& origin = *itr;

    // post fix to differentiate
    auto index = 0;
    std::string name = origin;
    // is the name existing already?
    while (names.find(name) != names.end()) {
      name = fmt::format("{0}_{1}", origin, ++index);
    }

    // it's a dupe name
    if (index > 0) {
      *itr = name;
    }

    // put the name into set for next check
    names.emplace(name);
  }
}
} // namespace storage
} // namespace nebula