
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
#include "meta/TableSpec.h"

/**
 * A generic data spec interface that loads one or more data blocks in runtime
 *
 */

namespace nebula {
namespace execution {
namespace meta {

// data spec: defines a logical data unit which could produce multiple data blocks
class Spec {
};

using SpecPtr = std::shared_ptr<Spec>;
} // namespace meta
} // namespace execution
} // namespace nebula