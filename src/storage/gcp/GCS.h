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

#include <mutex>

#include "common/Errors.h"
#include "google/cloud/storage/client.h"
#include "storage/NFileSystem.h"

/**
 * A wrapper for interacting with GCS
 */
namespace nebula {
namespace storage {
namespace gcp {

class GCS : public NFileSystem {
  static inline google::cloud::storage::ClientOptions Options() {
    auto opts = google::cloud::storage::ClientOptions::CreateDefaultClientOptions();
    return *opts;
  }

public:
  GCS(const std::string& bucket)
    : bucket_(bucket),
      client_{ Options(), google::cloud::storage::LimitedErrorCountRetryPolicy(3) } {
  }
  virtual ~GCS() = default;

public:
  // list prefix or objects under the given prefix
  // if obj is true, it will return all objects (max 1K) under given prefix at any level
  // otherwise it will only return sub-prefixes one level down under current prefix
  virtual std::vector<FileInfo> list(const std::string&) override;

  void read(const std::string&, const std::string&);

  // read a file/object at given offset and length into buffer address provided
  virtual size_t read(const std::string&, const size_t, const size_t, char*) override;

  // read a file/object fully into a memory buffer
  virtual size_t read(const std::string&, char*, size_t) override;

  virtual FileInfo info(const std::string&) override;

  // download a prefix to a local tmp file
  virtual bool copy(const std::string&, const std::string&) override;

  // create temporary file or dir
  virtual std::string temp(bool = false) override {
    throw NException("Not implemented");
  }

  // sync folder from GCS path (without bucket) to local path
  virtual bool sync(const std::string&, const std::string&, bool recursive = false) override;

  virtual void rm(const std::string&) override;

private:
  // download a gcs fille to local file
  bool download(const std::string&, const std::string&);

  // upload a local file to a gcs file
  bool upload(const std::string&, const std::string&);

private:
  std::string bucket_;
  google::cloud::storage::Client client_;
  std::mutex mutex_;
};

} // namespace gcp
} // namespace storage
} // namespace nebula
