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

/**
 * A wrapper for interacting with AWS / S3
 */
namespace nebula {
namespace storage {
namespace aws {
class S3 {
public:
  S3(const std::string& bucket) : bucket_{ bucket } {
    Aws::InitAPI(options_);
  }
  virtual ~S3() {
    Aws::ShutdownAPI(options_);
  }

public:
  std::vector<std::string> list(const std::string&);
  void read(const std::string&, const std::string&);

private:
  Aws::SDKOptions options_;
  std::string bucket_;
};
} // namespace aws
} // namespace storage
} // namespace nebula