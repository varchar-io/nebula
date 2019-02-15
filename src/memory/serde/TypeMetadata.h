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
#include "Type.h"

namespace nebula {
namespace memory {
namespace serde {

/**
 * A metadata serde to desribe metadata for a given type
 */
class TypeMetadata {
private:
  // raw size of this node
  long rawSize_;
};

// type metadata implementation for each type kind
template <nebula::type::Kind KIND>
class TypeMetadataImpl;

using BoolMetadata = TypeMetadataImpl<nebula::type::Kind::BOOLEAN>;
using ByteMetadata = TypeMetadataImpl<nebula::type::Kind::TINYINT>;
using ShortMetadata = TypeMetadataImpl<nebula::type::Kind::SMALLINT>;
using IntMetadata = TypeMetadataImpl<nebula::type::Kind::INTEGER>;
using LongMetadata = TypeMetadataImpl<nebula::type::Kind::BIGINT>;
using FloatMetadata = TypeMetadataImpl<nebula::type::Kind::REAL>;
using DoubleMetadata = TypeMetadataImpl<nebula::type::Kind::DOUBLE>;
using StringMetadata = TypeMetadataImpl<nebula::type::Kind::VARCHAR>;
using ListMetadata = TypeMetadataImpl<nebula::type::Kind::ARRAY>;
using MapMetadata = TypeMetadataImpl<nebula::type::Kind::MAP>;
using StructMetadata = TypeMetadataImpl<nebula::type::Kind::STRUCT>;

template <nebula::type::Kind KIND>
class TypeMetadataImpl : public TypeMetadata {
};

} // namespace serde
} // namespace memory
} // namespace nebula