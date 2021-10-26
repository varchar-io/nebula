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

/**
 * A wrapper for interacting with Azure / Datalake
 */
namespace nebula {
namespace storage {
namespace azure {

using Azure::Storage::StorageSharedKeyCredential;
using Azure::Storage::Files::DataLake::DataLakeFileSystemClient;

// these credential info is expected to pass from cluster config of a deployment
static constexpr auto KEY_ENDPOINT = "azure.storage.url";
static const std::string KEY_ACCOUNT = "azure.storage.account";
static const std::string KEY_SECRET = "azure.storage.secret";

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
  client_ = std::make_shared<DataLakeFileSystemClient>(endpoint_, sharedKeyCredential);
}

std::vector<FileInfo> DataLake::list(const std::string&) {
  return {};
  //   std::vector<FileInfo> fileInfos;
  //   auto directoryClient = getFileSystemClient().GetDirectoryClient(prefix);
  //   await foreach (auto path in directoryClient.GetPathsAsync()) {
  //     fileInfos.push_back(FileInfo(path.isDirectory, path.lastModified, path.contentLength, path.name, prefix));
  //   }
  //   return fileInfos;
}

size_t DataLake::read(const std::string&, char*, size_t) {
  //   auto directoryClient = getFileSystemClient().GetDirectoryClient(prefix);
  //   auto fileClient = directoryClient.GetFileClient(key);
  //   auto result = fileClient.Download();
  //   auto bytes = result.Value.Body->Read(buf, 0, size);
  //   return bytes;
  return 0;
}

inline bool DataLake::copy(const std::string&, const std::string&) {
  return false;
}

bool DataLake::sync(const std::string&, const std::string&, bool) {
  return false;
}

bool DataLake::download(const std::string&, const std::string&) {
  return false;
}

bool DataLake::upload(const std::string&, const std::string&) {
  return false;
}

} // namespace azure
} // namespace storage
} // namespace nebula
