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

#include <aws/core/Aws.h>
#include <mutex>

#include "common/Errors.h"
#include "storage/NFileSystem.h"

/**
 * A wrapper for interacting with Azure / Datalake
 */
namespace nebula {
namespace storage {
namespace azure {
class Datalake : public NFileSystem {
public:
  Datalake(const std::string& bucket) : bucket_{ bucket } {}
  virtual ~Datalake() = default;

public:
  virtual std::vector<FileInfo> list(const std::string&) override;

  void read(const std::string&, const std::string&);

  virtual size_t read(const std::string&, const size_t, const size_t, char*) override {
    throw NException("Not implemented");
  }

  virtual size_t read(const std::string&, char*, size_t) override;

  virtual FileInfo info(const std::string&) override {
    throw NException("Not implemented");
  }

  virtual bool copy(const std::string&, const std::string&) override {
    throw NException("Not implemented");
  }

  virtual std::string temp(bool = false) override {
    throw NException("Not implemented");
  }

  virtual bool sync(const std::string&, const std::string&, bool recursive = false) override {
    throw NException("Not implemented");
  }

  virtual void rm(const std::string&) override {
    throw NException("Not implemented");
  }

private:
  bool download(const std::string&, const std::string&) override {
    throw NException("Not implemented");
  }

  bool upload(const std::string&, const std::string&) override {
    throw NException("Not implemented");
  }

  void initDatalakeClients();

private:
  std::string bucket_;
};
} // namespace azure
} // namespace storage
} // namespace nebula
