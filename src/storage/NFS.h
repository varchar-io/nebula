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

#include <fmt/format.h>
#include <uriparser/Uri.h>

#include "NFileSystem.h"
#include "aws/S3.h"
#include "azure/DataLake.h"
#include "gcp/GCS.h"
#include "local/File.h"
#include "type/Serde.h"

/**
 * Define a file system interface to provide common data access.
 */
namespace nebula {
namespace storage {

// create nebula compatible file system from parameters
// 1. protocol: s3, gs, abfs, local
// 2. bucket: if the file system supports it, such as s3/gs, otherwise leave it as empty
// 3. settings: key-value in string types to pass around access info such as credential
std::unique_ptr<NFileSystem> makeFS(const std::string&, const std::string& = "", const nebula::type::Settings& = {});

// Customized nebula URI info after parsing
struct UriInfo {
  std::string schema;
  std::string host;
  // path will have leading and trailing slash striped
  // see parse test
  std::string path;
};

UriInfo parse(const std::string&);

} // namespace storage
} // namespace nebula