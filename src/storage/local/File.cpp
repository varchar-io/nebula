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

#include "File.h"

#include <fstream>
#include <unistd.h>

#include "common/Evidence.h"

/**
 * A wrapper for interacting with AWS / S3
 */
namespace nebula {
namespace storage {
namespace local {

namespace fs = std::filesystem;

std::vector<FileInfo> File::list(const std::string& path) {
  std::error_code ec;
  if (fs::is_regular_file(path, ec)) {
    return { info(path) };
  }

  if (ec) {
    return {};
  }

  std::vector<FileInfo> files;
  for (const auto& entry : fs::directory_iterator(path)) {
    if (entry.is_regular_file()) {
      files.emplace_back(false,
                         nebula::common::Evidence::seconds(entry.last_write_time()),
                         entry.file_size(),
                         entry.path(),
                         "");
    }
  }

  // API from <unistd.h>
  // DIR* d = opendir(dir.c_str());
  // if (d) {
  //   // read all entries
  //   struct dirent* dp;
  //   while ((dp = readdir(d))) {
  //     // only need regular files - 8=REG 4=DIR
  //     auto name = std::string(dp->d_name);
  //     if (name == "." || name == "..") {
  //       continue;
  //     }

  //     files.emplace_back(dp->d_type == 4,
  //                        0,
  //                        dp->d_reclen,
  //                        name,
  //                        "");
  //   }

  //   closedir(d);
  // }

  // it may be empty
  return files;
}

FileInfo File::info(const std::string& file) {
  auto s = fs::status(file);
  N_ENSURE(fs::exists(s), "file not exists");

  return FileInfo(
    false,
    // unix timestamp in ms
    0,
    // status.last_write_time().time_since_epoch().count(),
    fs::file_size(file),
    file,
    "");
}

size_t File::read(const std::string& file, char* buf, size_t size) {
  std::ifstream fs(file);
  fs.seekg(0, std::ios::end);
  auto bytes = std::min<size_t>(fs.tellg(), size);
  fs.read(buf, bytes);
  return bytes;
}

std::string File::temp(bool dir) {
  char f[] = "/tmp/nebula.XXXXXX";
  if (dir) {
    auto ret = mkdtemp(f);
    N_ENSURE(ret != NULL, "Failed to create temp dir");
    return ret;
  }

  // temp file
  auto ret = mkstemp(f);
  N_ENSURE(ret != -1, "Failed to create temp file");
  return f;
}

bool File::sync(const std::string& from, const std::string& to, bool recursive) {
  N_ENSURE(!recursive, "Do not support recursive yet.");

  // from and to are both local folder, we just copy all files over
  // support recursive by setting copy_options
  return copy(from, to);
}

void File::rm(const std::string& path) {
  std::filesystem::remove_all(path);
}

} // namespace local
} // namespace storage
} // namespace nebula