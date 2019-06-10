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

#include "TypeData.h"
#include "TypeMetadata.h"

namespace nebula {
namespace memory {
namespace serde {

/**
 * A factory to create typed data and metadata
 */
class TypeDataFactory {
public:
  static std::unique_ptr<TypeMetadata> createMeta(nebula::type::Kind);
  static std::unique_ptr<TypeDataProxy> createData(nebula::type::Kind, const nebula::meta::Column&, size_t);

private:
  TypeDataFactory() = default;
  virtual ~TypeDataFactory() = default;
};
} // namespace serde
} // namespace memory
} // namespace nebula