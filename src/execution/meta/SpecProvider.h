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

#include <mutex>

/**
 *
 * A spec provider is responsible to maintain fresh spec state.
 * And provide spec related queries, such as
 * - (online specs, offline specs) for a given table.
 * - active tables includes all tables that has valid specs.
 *
 * A spec has versions.
 *
 * A spec has storage attribute (online, offline).
 * 1. Node name: means it's online serving by a specific node.
 * 2. Media path: means it's offline stored in external storage, such as S3.
 * 3. A spec should be removed if it's expired (ephemeral) or
 *
 * Design Overview -
 * https://docs.google.com/drawings/d/1yYOjyjGKwi7mMQn3zrZuswokLVrLdMvg724eYUE46Ic
 *
 */

#include "Spec.h"
#include "meta/TableSpec.h"

namespace nebula {
namespace execution {
namespace meta {

class SpecProvider {
  // generate specs for given table at the function execution time
  // if no spec to generate, return empty vector, do not throw
  virtual std::vector<SpecPtr> generate(const std::string&, const nebula::meta::TableSpecPtr&) noexcept = 0;
};

} // namespace meta
} // namespace execution
} // namespace nebula