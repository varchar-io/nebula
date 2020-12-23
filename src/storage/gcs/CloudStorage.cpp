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

#include "CloudStorage.h"

#include <string>
#include <glog/logging.h>
#include <iostream>
#include <unistd.h>
#include <fstream>
#include <mutex>

#include "common/Chars.h"
#include "common/Errors.h"
#include "google/cloud/storage/client.h"
#include "storage/local/File.h"
#include "storage/NFileSystem.h"

/**
 * A wrapper for interacting with GCS.
 */
namespace nebula {
namespace storage {
namespace gcs {

namespace {

using gcs = google::cloud::storage;
using google::cloud::StatusOr;

constexpr int BUFFER_SIZE = 4096;

}

std::vector<FileInfo> CloudStorage::list(const std::string & prefix) {
  std::vector<FileInfo> objects;
  for (const StatusOr<gcs::ObjectMetadata>&
       object_metadata : gcs_client_->ListObjects(bucket_)) {
    if (object_metadata.ok()) {
      if (StartsWith(object_metadata.name(), prefix)) {
        // Change the timestamp later.
        object.emplace_back(false, 0, object_metadata.size(), object_metadata.name(), bucket_);
      }
    }
  }

  return objects;
}

size_t CloudStorage::read(const std::string& object_name, char* buf, size_t size) {
  return read(object_name, 0, size, buf);
}

virtual size_t read(const std::string& object_name, const size_t offset, const size_t size,
                    char* buf) {
  gcs::ObjectReadStream stream = gcs_client_->ReadObject(bucket_, object_name);
  if (stream.bad()) {
    LOG(ERROR) << "Failed to read object " << bucket_ << "/" << object_name;
    return -1;
  }

  if (offset > 0) {
    stream.seekg(offset, std::ios::beg);
  }
  while (stream.read(buf, BUFFER_SIZE)) {
    buf += BUFFER_SIZE;
  }
  return stream.gcount();
}

FileInfo CloudStorage::info(const std::string& object_name) {
  auto object_metadata = gcs_client_->GetObjectMetadata(bucket_, object_name);
  if (object_metadata.ok()) {
    return FileInfo(false, 0, object_metadata.size(), object_metadata.name(), bucket_);
  } else {
    LOG(ERROR) << "Fail to get object " << bucket_ << "/" << object_name;
    throw NException("Failed to get file metadata.");
  }
}


bool CloudStorage::uploadFile(const std::string& object_name,
                              const std::string& local_file) {

  std::ifstream in_stream;
  in_stream.open(local_file);
  if (in_stream.fail()) {
    LOG(ERROR) << "Cannot open local file " << local_file;
    return false;
  }

  gcs::ObjectWriteStream out_stream = gcs_client_->WriteObject(bucket_, object_name);
  char* buffer = new char[BUFFER_SIZE];
  while (in_stream.read(buf, BUFFER_SIZE)) {
    out_stream.write(buf, BUFFER_SIZE);
  }

  delete[] buffer;

  in_stream.close();
  return true;
}

bool CloudStorage::downloadFile(const std::string& object_name,
                                const std::string& local_file) {
  std::ofstream out_stream;
  out_stream.open(local_file);
  if (out_stream.fail()) {
    LOG(ERROR) << "Cannot open local file " << local_file;
    return false;
  }
  // ObjectReadStream will close the stream in destructor.
  gcs::ObjectReadStream in_stream = gcs_client_->ReadObject(bucket_, object_name);
  if (in_stream.bad()) {
    LOG(ERROR) << "Failed to read object " << bucket_ << "/" << object_name;
    return false;
  }

  char* buffer = new char[BUFFER_SIZE];
  while (in_stream.read(buf, BUFFER_SIZE)) {
    out_stream.write(buf, BUFFER_SIZE);
  }

  delete[] buffer;
  out_stream.close();
  return true;
}

bool CloudStorage::copy(const std::string& from, const std::string& to) {
  std::lock_guard<std::mutex> lock(local_file_mutex_);

  if (from.at(0) == '/') {
    return uploadFile(to, from);
  } else if (to.at(0) == '/') {
    return downloadFile(from, to);
  }

  LOG(WARNING) << "Remote file path format is not supported.";
  return false;
}

bool CloudStorage::sync(const std::string& from, const std::string& to, bool recursive) {
  // need support local to S3 sync as well for writing/backup scenario
  N_ENSURE(!recursive, "support non-recursive sync only for now.");
  if (from.empty() || to.empty()) {
    LOG(WARNING) << "Invalid path: from=" << from << ", to=" << to;
    return false;
  }

  if (from.at(0) == '/') {
    uploadFile(from, to);
  } else if (to.at(0) == '/') {
    downloadFile(from, to);
  } else {
    LOG(WARNING) << "Not supporting s3 to s3 sync for now";
    return false;
  }

  return true;
}

void CloudStorage::rm(const std::string & name_or_prefix) {
  for (const StatusOr<gcs::ObjectMetadata>&
       object_metadata : gcs_client_->ListObjects(bucket_)) {
    if (object_metadata.ok()) {
      if (StartsWith(object_metadata.name(), prefix)) {
        gcs_client_->DeleteObject(bucket_, object_name);

      }
    }
  }
}
} // namespace gcs
} // namespace storage
} // namespace nebula
