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

#include "File.h"

// use file system in GNU system for now
// #ifdef __GNUG__
#ifndef __APPLE__
#include <filesystem>
#endif

/**
 * A wrapper for interacting with AWS / S3
 */
namespace nebula {
namespace storage {
namespace local {
std::vector<FileInfo> File::list(const std::string& dir) {
  std::vector<FileInfo> files;
  DIR* d = opendir(dir.c_str());
  if (d) {
    // read all entries
    struct dirent* dp;
    while ((dp = readdir(d))) {
      // only need regular files - 8=REG 4=DIR
      files.emplace_back(dp->d_type == 4,
                         0,
                         dp->d_reclen,
                         std::string(dp->d_name));
    }

    closedir(d);
  }

  // it may be empty
  return files;
}

FileInfo File::info(const std::string& file) {
// #ifdef __GNUG__
#ifndef __APPLE__
  namespace fs = std::filesystem;
  auto s = fs::status(file);
  N_ENSURE(fs::exists(s), "file not exists");

  return FileInfo(
    false,
    // unix timestamp in ms
    0,
    // status.last_write_time().time_since_epoch().count(),
    fs::file_size(file),
    file);
#else
  return FileInfo(false, 0, 0, file);
#endif
}

} // namespace local
} // namespace storage
} // namespace nebula