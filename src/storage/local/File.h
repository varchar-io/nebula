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

#include <dirent.h>
#include <string>
#include <vector>
#include "common/Errors.h"

/**
 * A wrapper for interacting with AWS / S3
 */
namespace nebula {
namespace storage {
namespace local {
class File {
public:
  static std::vector<std::string> list(const std::string& dir) {
    std::vector<std::string> files;
    DIR* d = opendir(dir.c_str());
    if (d) {
      // read all entries
      struct dirent* dp;
      while ((dp = readdir(d))) {
        // only need regular files
        if (dp->d_type == 8) {
          files.push_back(dp->d_name);
        }
      }
      closedir(d);
    }

    // it may be empty
    return files;
  }
};
} // namespace local
} // namespace storage
} // namespace nebula