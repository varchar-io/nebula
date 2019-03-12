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
namespace encode {
/**
 * A base class to encode data streams into another data.
 */
class Encoder {
};

/**
 * Bool encoder - bool value usually ends in a bit map
 */
class BoolEncoder : public Encoder {
};

/**
 * Integer numbers encoder - int width of 1, 2, 4, 8 bytes
 */
class IntEncoder : public Encoder {
};

/**
 * Float number encoder - 4 bytes float or 8 bytes double
 */
class FloatEncoder : public Encoder {
};

/**
 * Byte sequence encoder captures bytes serialized data like strings
 */
class BytesEncoder : public Encoder {
};
} // namespace encode
} // namespace memory
} // namespace nebula