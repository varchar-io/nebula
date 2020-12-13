/*
 * Copyright 2020-present Philip Yu
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

#include "google/cloud/storage/client.h"
#include "storage/NFileSystem.h"

/**
 * A wrapper for interacting with AWS / S3
 */
namespace nebula {
namespace storage {
namespace gcs {
class CloudStorage : public NFileSystem {
public:
  CloudStorage(google::cloud::storage::Client* gcs_client,
               const std::string& bucket) : gcs_client_(gcs_client),
    bucket_(bucket) {}
  virtual ~CloudStorage() = default;

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

  // sync folder from s3 path (without bucket) to local path
  virtual bool sync(const std::string&, const std::string&, bool recursive = false) override;

  // remove a s3 file or s3 prefix
  virtual void rm(const std::string&) override;

private:
  void downloadFile(const std::string&, const std::string&);
  void uploadFile(const std::string&, const std::string&);

private:
  google::cloud::storage::Client* gcs_client_;
  std::string bucket_;

  std::mutex local_file_mutex_;
};
} // namespace gcs
} // namespace storage
} // namespace nebula