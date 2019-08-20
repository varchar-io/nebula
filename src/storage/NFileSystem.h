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

#include "common/Errors.h"

/**
 * Define a file system interface to provide common data access.
 */
namespace nebula {
namespace storage {

// layout
struct FileInfo {
  explicit FileInfo(bool isd, size_t ts, size_t sz, std::string n)
    : isDir{ isd }, timestamp{ ts }, size{ sz }, name{ std::move(n) } {
  }
  bool isDir;
  size_t timestamp;
  size_t size;
  std::string name;

  inline std::string signature() const {
    return fmt::format("{0}_{1}_{2}", name, size, timestamp);
  }
};

class NFileSystem {
public:
  NFileSystem() = default;
  virtual ~NFileSystem() = default;

  // list a folder or a path to get all file info
  virtual std::vector<FileInfo> list(const std::string&) = 0;

  // read a file/object at given offset and length into buffer address provided
  virtual size_t read(const std::string&, const size_t, const size_t, char*) = 0;

  // read a file/object fully into a memory buffer
  virtual size_t read(const std::string&, char*) = 0;

  virtual FileInfo info(const std::string&) = 0;
};
} // namespace storage
} // namespace nebula