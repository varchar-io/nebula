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

#include "GCS.h"

#include <fstream>
#include <glog/logging.h>
#include <google/cloud/storage/client.h>

#include "common/Chars.h"
#include "common/Errors.h"
#include "storage/NFileSystem.h"
#include "storage/local/File.h"

/**
 * A wrapper for interacting with GCS.
 */
namespace nebula {
namespace storage {
namespace gcp {

using google::cloud::StatusOr;
using google::cloud::storage::IfGenerationMatch;
using google::cloud::storage::LimitedErrorCountRetryPolicy;
using google::cloud::storage::ObjectMetadata;
using google::cloud::storage::ObjectReadStream;
using google::cloud::storage::ObjectWriteStream;
using google::cloud::storage::Prefix;

std::vector<FileInfo> GCS::list(const std::string& key) {
  std::vector<FileInfo> objects;
  for (const StatusOr<ObjectMetadata>& meta : client_->ListObjects(bucket_, Prefix(key))) {
    if (meta.ok()) {
      objects.emplace_back(false, 0, meta->size(), meta->name(), bucket_);
    }
  }

  return objects;
}

size_t GCS::read(const std::string& key, char* buf, size_t size) {
  return read(key, 0, size, buf);
}

size_t GCS::read(const std::string& key, const size_t offset, const size_t size, char* buf) {
  ObjectReadStream stream = client_->ReadObject(bucket_, key);
  if (stream.bad()) {
    LOG(ERROR) << "Failed to read object " << bucket_ << "/" << key;
    return -1;
  }

  if (offset > 0) {
    stream.seekg(offset, std::ios::beg);
  }
  stream.read(buf, size);
  return stream.gcount();
}

FileInfo GCS::info(const std::string& key) {
  auto meta = client_->GetObjectMetadata(bucket_, key);
  if (meta.ok()) {
    return FileInfo(false, 0, meta->size(), meta->name(), bucket_);
  }

  LOG(ERROR) << "Fail to get object " << bucket_ << "/" << key;
  throw NException("Failed to get file metadata.");
}

bool GCS::upload(const std::string& key, const std::string& local) {
  StatusOr<ObjectMetadata> metadata = client_->UploadFile(
    local, bucket_, key, IfGenerationMatch(0));
  if (!metadata) {
    LOG(WARNING) << "Failed to upload: " << metadata.status().message();
    return false;
  }

  return true;
}

bool GCS::download(const std::string& key, const std::string& local) {
  // ObjectReadStream will close the stream in destructor.
  google::cloud::Status status = client_->DownloadToFile(bucket_, key, local);
  if (!status.ok()) {
    LOG(WARNING) << "Failed to download: " << status.message();
  }

  return status.ok();
}

bool GCS::copy(const std::string& from, const std::string& to) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (from.at(0) == '/') {
    return upload(to, from);
  } else if (to.at(0) == '/') {
    return download(from, to);
  }

  LOG(WARNING) << "Remote file path format is not supported.";
  return false;
}

bool GCS::sync(const std::string& from, const std::string& to, bool recursive) {
  // need support local to gcs sync as well for writing/backup scenario
  N_ENSURE(!recursive, "support non-recursive sync only for now.");
  if (from.empty() || to.empty()) {
    LOG(WARNING) << "Invalid path: from=" << from << ", to=" << to;
    return false;
  }

  if (from.at(0) == '/') {
    return upload(to, from);
  } else if (to.at(0) == '/') {
    return download(from, to);
  }

  LOG(WARNING) << "Not supporting remote-to-remote sync yet.";
  return false;
}

void GCS::rm(const std::string& key) {
  for (const StatusOr<ObjectMetadata>& meta : client_->ListObjects(bucket_, Prefix(key))) {
    if (meta.ok()) {
      google::cloud::Status status = client_->DeleteObject(bucket_, meta->name());
      if (!status.ok()) {
        LOG(WARNING) << "Faield to delete " << meta->name() << " : " << status.message();
      }
    }
  }
}

} // namespace gcp
} // namespace storage
} // namespace nebula