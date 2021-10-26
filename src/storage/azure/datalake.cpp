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

#include "datalake.h"

#include <azure/storage/files/datalake.hpp>

#include <cstdio>
#include <fstream>
#include <glog/logging.h>
#include <iostream>
#include <unistd.h>

#include "common/Chars.h"
#include "storage/local/File.h"

using namespace Azure::Storage::Files::DataLake;

/**
 * A wrapper for interacting with Azure / Datalake
 */
namespace nebula {
namespace storage {
namespace azure {

std::string getEndpointUrl() { return std::getenv("AZURE_STORAGE_ACCOUNT_URL"); }
std::string getAccountName() { return std::getenv("AZURE_STORAGE_ACCOUNT_NAME"); }
std::string getAccountKey() { return std::getenv("AZURE_STORAGE_ACCOUNT_KEY"); }

const DataLakeFileSystemClient& getFileSystemClient() {
    auto sharedKeyCredential = std::make_shared<StorageSharedKeyCredential>(getAccountName(), getAccountKey());
    return DataLakeFileSystemClient(getEndpointUrl(), sharedKeyCredential);
}

std::vector<FileInfo> Datalake::list(const std::string& prefix) {
    std::vector<FileInfo> fileInfos;
    auto directoryClient = getFileSystemClient().GetDirectoryClient(prefix);
    await
    foreach (auto path in directoryClient.GetPathsAsync()) {
        fileInfos.push_back(FileInfo(path.isDirectory, path.lastModified, path.contentLength, path.name, prefix));
    }
    return fileInfos;
}

size_t Datalake::read(const std::string& key, char* buf, size_t size) {
    auto directoryClient = getFileSystemClient().GetDirectoryClient(prefix);
    auto fileClient = directoryClient.GetFileClient(key);
    auto result = fileClient.Download();
    auto bytes = result.Value.Body->Read(buf, 0, size);
    return bytes;
}

} // namespace azure
} // namespace storage
} // namespace nebula
