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

#include <aws/core/Aws.h>
#include "common/Errors.h"
#include "storage/NFileSystem.h"

/**
 * A wrapper for interacting with AWS / S3
 */
namespace nebula {
namespace storage {
namespace aws {
class S3 : public NFileSystem {
public:
  S3(const std::string& bucket) : bucket_{ bucket } {
    options_.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    Aws::InitAPI(options_);
  }
  virtual ~S3() {
    Aws::ShutdownAPI(options_);
  }

public:
  // list prefix or objects under the given prefix
  // if obj is true, it will return all objects (max 1K) under given prefix at any level
  // otherwise it will only return sub-prefixes one level down under current prefix
  // TODO(cao): not supporting pagination yet, current one time fetch max keys at 1K
  virtual std::vector<FileInfo> list(const std::string&) override;
  void read(const std::string&, const std::string&);
  // read a file/object at given offset and length into buffer address provided
  virtual size_t read(const std::string&, const size_t, const size_t, char*) override {
    throw NException("Not implemented");
  }

  // read a file/object fully into a memory buffer
  virtual size_t read(const std::string&, char*) override {
    throw NException("Not implemented");
  }

  virtual FileInfo info(const std::string&) override {
    throw NException("Not implemented");
  }

  // download a prefix to a local tmp file
  virtual std::string copy(const std::string&) override;

private:
  Aws::SDKOptions options_;
  std::string bucket_;
};
} // namespace aws
} // namespace storage
} // namespace nebula