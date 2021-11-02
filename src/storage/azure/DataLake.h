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

#include <azure/storage/files/datalake.hpp>
#include <mutex>

#include "common/Errors.h"
#include "storage/NFileSystem.h"
#include "type/Serde.h"

/**
 * A wrapper for interacting with Azure / Datalake
 */
namespace nebula {
namespace storage {
namespace azure {

class DataLake : public NFileSystem {
public:
  DataLake(const std::string&, const nebula::type::Settings&);
  virtual ~DataLake() = default;

public:
  virtual std::vector<FileInfo> list(const std::string&) override;
  virtual size_t read(const std::string&, char*, size_t) override;
  // file level copy
  virtual bool copy(const std::string&, const std::string&) override;
  // directory level copy
  virtual bool sync(const std::string&, const std::string&, bool recursive = false) override;

  virtual size_t read(const std::string&, const size_t, const size_t, char*) override {
    throw NException("Not implemented");
  }

  virtual FileInfo info(const std::string&) override {
    throw NException("Not implemented");
  }

  virtual std::string temp(bool = false) override {
    throw NException("Not implemented");
  }

  virtual void rm(const std::string&) override {
    throw NException("Not implemented");
  }

private:
  bool download(const std::string&, const std::string&);
  bool upload(const std::string&, const std::string&);

private:
  std::string bucket_;
  std::string endpoint_;
  std::string account_;
  std::string secret_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileSystemClient> client_;

  std::mutex mtx_;
};
} // namespace azure
} // namespace storage
} // namespace nebula
