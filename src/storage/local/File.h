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
#include "storage/NFileSystem.h"

/**
 * A wrapper for interacting with AWS / S3
 */
namespace nebula {
namespace storage {
namespace local {
class File : public NFileSystem {
public:
  virtual std::vector<FileInfo> list(const std::string& dir) override;

  // read a file/object at given offset and length into buffer address provided
  virtual size_t read(const std::string&, const size_t, const size_t, char*) override {
    throw NException("Not implemented");
  }

  // read a file/object fully into a memory buffer
  virtual size_t read(const std::string&, char*, size_t) override;

  // return file info of given file path
  virtual FileInfo info(const std::string&) override;

  virtual bool copy(const std::string&, const std::string&) override {
    throw NException("Not implemented");
  }

  virtual std::string temp(bool = false) override;

  virtual bool sync(const std::string&, const std::string&, bool = false) override;

  virtual void rm(const std::string&) override;
};

} // namespace local
} // namespace storage
} // namespace nebula