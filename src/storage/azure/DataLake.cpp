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

#include "DataLake.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include "common/Chars.h"
#include "storage/local/File.h"

/**
 * A wrapper for interacting with Azure / Datalake
 */
namespace nebula {
namespace storage {
namespace azure {

// allow defining literal size like `10_KB`, `1_MB`, etc.
using ull = unsigned long long;

constexpr ull operator"" _KB(ull no) {
  return no * 1024;
}

constexpr ull operator"" _MB(ull no) {
  return no * (1024_KB);
}

using Azure::Storage::StorageSharedKeyCredential;
using Azure::Storage::Files::DataLake::DataLakeDirectoryClient;
using Azure::Storage::Files::DataLake::DataLakeFileClient;
using Azure::Storage::Files::DataLake::DataLakeFileSystemClient;
using Azure::Storage::Files::DataLake::DownloadFileOptions;
using Azure::Storage::Files::DataLake::ListPathsOptions;
using Azure::Storage::Files::DataLake::UploadFileFromOptions;
using Azure::Storage::Files::DataLake::Models::PathHttpHeaders;

PathHttpHeaders GetInterestingHttpHeaders() {
  static PathHttpHeaders result = []() {
    PathHttpHeaders ret;
    ret.CacheControl = "no-cache";
    ret.ContentDisposition = "attachment";
    ret.ContentEncoding = "deflate";
    ret.ContentLanguage = "en-US";
    ret.ContentType = "application/octet-stream";
    return ret;
  }();

  return result;
}

// these credential info is expected to pass from cluster config of a deployment
static constexpr auto KEY_ENDPOINT = "azure.storage.url";
static constexpr auto KEY_ACCOUNT = "azure.storage.account";
static constexpr auto KEY_SECRET = "azure.storage.secret";

// azure data lake bucket is like a long host, for example
// `<file_system>2@<account_name>3.dfs.core.windows.net`
DataLake::DataLake(const std::string& bucket, const nebula::type::Settings& settings)
  : bucket_{ bucket } {
#define KEY_OF_IF_PRESENT(K, M)  \
  {                              \
    auto itr = settings.find(K); \
    if (itr != settings.end()) { \
      M = itr->second;           \
    }                            \
  }

  KEY_OF_IF_PRESENT(KEY_ENDPOINT, endpoint_)
  KEY_OF_IF_PRESENT(KEY_ACCOUNT, account_)
  KEY_OF_IF_PRESENT(KEY_SECRET, secret_)
#undef KEY_OF_IF_PRESENT

  // build a client from given parameters
  auto sharedKeyCredential = std::make_shared<StorageSharedKeyCredential>(account_, secret_);
  client_ = std::make_shared<DataLakeFileSystemClient>(
    fmt::format("{0}/{1}", endpoint_, bucket_), sharedKeyCredential);
}

std::vector<FileInfo> DataLake::list(const std::string& path) {
  std::vector<FileInfo> fileInfos;
  auto dirClient = std::make_shared<DataLakeDirectoryClient>(
    client_->GetDirectoryClient(path));

  // azure datalake will throw if the path not exists
  // different behavior from S3/GCS, wish there is a quick method like `Exists`
  try {
    // opts may contain continuation token since azure return maximum 5K results per request
    ListPathsOptions opts;
    while (true) {
      auto res = dirClient->ListPaths(true, opts);
      for (auto& pathItem : res.Paths) {
        if (!pathItem.IsDirectory) {
          fileInfos.emplace_back(false, 0, pathItem.FileSize, pathItem.Name, bucket_);
        }
      }

      // no more results
      if (!res.NextPageToken.HasValue()) {
        break;
      }

      // assign the continuation token for next page
      opts.ContinuationToken = res.NextPageToken;
    }
  } catch (Azure::Storage::StorageException& e) {
    if (e.ErrorCode == "PathNotFound") {
      VLOG(1) << "Path not found: " << path;
    }
  }

  return fileInfos;
}

size_t DataLake::read(const std::string& remote, char* buf, size_t size) {
  // get file client pointing to the remote file path
  auto fileClient = client_->GetFileClient(remote);
  auto dataLakeClient = std::make_shared<DataLakeFileClient>(fileClient);

  // do the download
  auto res = dataLakeClient->DownloadTo((uint8_t*)buf, size);
  return res.Value.FileSize;
}

bool uploadFile(const std::shared_ptr<DataLakeFileSystemClient> client,
                const std::string& remote,
                const std::string& local) {
  // get file client pointing to the remote file path
  auto fileClient = client->GetFileClient(remote);
  auto dataLakeClient = std::make_shared<DataLakeFileClient>(fileClient);
  dataLakeClient->Create();

  // configure file upload options
  // options.TransferOptions.Concurrency = concurrency;
  UploadFileFromOptions options;
  options.TransferOptions.ChunkSize = 1_MB;
  options.HttpHeaders = GetInterestingHttpHeaders();

  // do the upload
  auto res = dataLakeClient->UploadFrom(local, options);

  VLOG(1) << "Success: upload " << local << " to key=" << remote;
  return res.Value.ETag.HasValue();
}

bool downloadFile(const std::shared_ptr<DataLakeFileSystemClient> client,
                  const std::string& remote,
                  const std::string& local) {
  // get file client pointing to the remote file path
  auto fileClient = client->GetFileClient(remote);
  auto dataLakeClient = std::make_shared<DataLakeFileClient>(fileClient);

  // do the download
  auto res = dataLakeClient->DownloadTo(local);

  VLOG(1) << "Success: download " << remote << " to " << local;
  return res.Value.FileSize > 0;
}

inline bool DataLake::copy(const std::string& from, const std::string& to) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (from.at(0) == '/') {
    return uploadFile(this->client_, to, from);
  } else if (to.at(0) == '/') {
    return downloadFile(this->client_, from, to);
  }

  // only support copy between local and remote
  LOG(WARNING) << "Not supporting azure to azure copy for now";
  return false;
}

bool DataLake::sync(const std::string& from, const std::string& to, bool recursive) {
  // need support local to azure sync as well for writing/backup scenario
  N_ENSURE(!recursive, "support non-recursive sync only for now.");
  if (from.empty() || to.empty()) {
    LOG(WARNING) << "Invalid path: from=" << from << ", to=" << to;
    return false;
  }

  // the sync is interpreted as either upload or download
  if (from.at(0) == '/') {
    return upload(from, to);
  } else if (to.at(0) == '/') {
    return download(from, to);
  }

  // from azure to azure is not allowed
  LOG(WARNING) << "Only support sync between local and remote";
  return false;
}

bool DataLake::download(const std::string& remote, const std::string& local) {
  std::lock_guard<std::mutex> lock(mtx_);
  LOG(INFO) << "Download: from " << remote << " to " << local;

  auto files = list(remote);

  // expect false if specified location is empty
  if (files.size() == 0) {
    LOG(WARNING) << "Failed to download: source dir/prefix is empty=" << remote;
    return false;
  }

  // any single failure will result in whole task failure
  for (auto& f : files) {
    // for each key, let's download it to current to folder
    if (!f.isDir) {
      // get file name
      auto nameOnly = nebula::common::Chars::last(f.name);
      if (!downloadFile(client_, f.name, fmt::format("{0}/{1}", local, nameOnly))) {
        LOG(WARNING) << "Failed to download: file=" << f.name;
        return false;
      }
    }
  }

  return true;
}

bool DataLake::upload(const std::string& local, const std::string& azure) {
  std::lock_guard<std::mutex> lock(mtx_);
  LOG(INFO) << "Upload: from " << local << " to " << azure;

  // need local file system to list files
  nebula::storage::local::File lfs;
  auto files = lfs.list(local);
  // expect false if specified location is empty
  if (files.size() == 0) {
    LOG(WARNING) << "Failed to upload: source dir/prefix is empty=" << local;
    return false;
  }

  // upload all files
  for (auto& f : files) {
    if (!f.isDir) {
      // f.name will be full path from local, we need to
      auto nameOnly = nebula::common::Chars::last(f.name);
      if (!uploadFile(this->client_,
                      fmt::format("{0}/{1}", azure, nameOnly),
                      fmt::format("{0}/{1}", local, nameOnly))) {
        LOG(WARNING) << "Failed to upload: file=" << nameOnly;
        return false;
      }
    }
  }

  return true;
}

} // namespace azure
} // namespace storage
} // namespace nebula
