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

#include <vector>

#include "common/Params.h"
#include "ingest/IngestSpec.h"
#include "nebula.pb.h"

/**
 * A handler to process different loading demand
 */
namespace nebula {
namespace service {
namespace server {

// load list of ingest specs per request
class LoadHandler {
public:
  LoadHandler() = default;
  virtual ~LoadHandler() = default;

public:
  // load pre-configured template found in cluster config
  nebula::meta::TableSpecPtr loadConfigured(const LoadRequest*, LoadError&);

  // load a google sheet spec
  nebula::meta::TableSpecPtr loadGoogleSheet(const LoadRequest*, LoadError&);

  // load a google sheet spec
  nebula::meta::TableSpecPtr loadDemand(const LoadRequest*, LoadError&);
};

} // namespace server
} // namespace service
} // namespace nebula