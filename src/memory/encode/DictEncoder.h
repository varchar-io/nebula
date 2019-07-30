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

#include "common/Memory.h"

namespace nebula {
namespace memory {
namespace encode {
/**
 * Dictionary encoding for text values.
 * It works like this:
 * 1. Reading tokens from the text value separated by <space>
 * 2. It applies only first 255 characters for using one byte store position info.
 * 3. It only works for token which length is longer than 2.
 * 4. An encoded string are stored like this 
 *      [number dict items(0~255)][p0(0-255)][c0(0-65536)][p1(0-255)]...[pN(0-255)]
 */
class DictEncoder {
};
} // namespace encode
} // namespace memory
} // namespace nebula