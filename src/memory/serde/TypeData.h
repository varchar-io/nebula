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
 * A data serde to desribe real data for a given type
 */
class TypeData {
};

// type metadata implementation for each type kind
template <nebula::type::Kind KIND>
class TypeDataImpl;

// we have empty data because compound type node doesn't really hold data
// so instead we don't define data for any compound type
using EmptyData = TypeDataImpl<nebula::type::Kind::INVALID>;
using BoolData = TypeDataImpl<nebula::type::Kind::BOOLEAN>;
using ByteData = TypeDataImpl<nebula::type::Kind::TINYINT>;
using ShortData = TypeDataImpl<nebula::type::Kind::SMALLINT>;
using IntData = TypeDataImpl<nebula::type::Kind::INTEGER>;
using LongData = TypeDataImpl<nebula::type::Kind::BIGINT>;
using FloatData = TypeDataImpl<nebula::type::Kind::REAL>;
using DoubleData = TypeDataImpl<nebula::type::Kind::DOUBLE>;
using StringData = TypeDataImpl<nebula::type::Kind::VARCHAR>;

template <nebula::type::Kind KIND>
class TypeDataImpl : public TypeData {
};

} // namespace serde
} // namespace memory
} // namespace nebula