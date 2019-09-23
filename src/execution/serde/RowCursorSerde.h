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

#include "memory/keyed/FlatBuffer.h"

#include "surface/DataSurface.h"
#include "type/Type.h"

/**
 * This header defines logic to convert a RowCursor to a serializable data format.
 * So that an RowCursor consumer can use this extension methods to do serialization work.
 * 
 * The extension requires namespace scope definition.
 */
namespace nebula {
namespace execution {
namespace serde {

using FlatBufferPtr = std::unique_ptr<nebula::memory::keyed::FlatBuffer>;

FlatBufferPtr asBuffer(nebula::surface::RowCursor& cursor, nebula::type::Schema schema);
void init();

} // namespace serde
} // namespace execution
} // namespace nebula