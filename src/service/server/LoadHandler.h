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

#include <vector>

#include "ingest/IngestSpec.h"
#include "common/Params.h"
#include "nebula.pb.h"

/**
 * A handler to process different loading demand
 */
namespace nebula {
namespace service {
namespace server {

// any load request will leads to a load result
using LoadResult = std::vector<std::shared_ptr<nebula::ingest::IngestSpec>>;

// load list of ingest specs per request
class LoadHandler {
public:
  LoadHandler() = default;
  virtual ~LoadHandler() = default;

public:
  // load pre-configured template found in cluster config
  LoadResult loadConfigured(const LoadRequest*, LoadError&, std::string&);

  // load a google sheet spec
  LoadResult loadGoogleSheet(const LoadRequest*, LoadError&, std::string&);

  // load a google sheet spec
  LoadResult loadDemand(const LoadRequest*, LoadError&, std::string&);

  // extract watermark from json setting
  size_t extractWatermark(nebula::common::unordered_map<std::string_view, std::string_view> p);
};

} // namespace server
} // namespace service
} // namespace nebula