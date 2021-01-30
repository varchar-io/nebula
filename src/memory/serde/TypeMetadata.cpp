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

#include "TypeMetadata.h"

namespace nebula {
namespace memory {
namespace serde {

// define bool histogram method
template <>
size_t TypeMetadata::histogram(bool v) {
  if (v) {
    ++bh_->trueValues;
  }

  return ++(histo_->count);
}

// define varchar histogram method
#define DEFAULT_HISTOGRAM_RECORD(T)   \
  template <>                         \
  size_t TypeMetadata::histogram(T) { \
    return ++(histo_->count);         \
  }

DEFAULT_HISTOGRAM_RECORD(char const*);
DEFAULT_HISTOGRAM_RECORD(const std::string&);
DEFAULT_HISTOGRAM_RECORD(std::string_view);
DEFAULT_HISTOGRAM_RECORD(int128_t);

// TODO(cao) - we can not assume the object type is like this
// we do this for temporary due to we don't use it anyways
// hence we assume complex type to match this signature by providing nullptr
DEFAULT_HISTOGRAM_RECORD(std::nullptr_t);

#undef DEFAULT_HISTOGRAM_RECORD

// define number types
#define NUMBER_TYPE_HISTO(T, P)         \
  template <>                           \
  size_t TypeMetadata::histogram(T v) { \
    if (v < P->v_min) {                 \
      P->v_min = v;                     \
    }                                   \
                                        \
    if (v > P->v_max) {                 \
      P->v_max = v;                     \
    }                                   \
                                        \
    P->v_sum += v;                      \
                                        \
    return ++(histo_->count);           \
  }

NUMBER_TYPE_HISTO(int8_t, ih_)
NUMBER_TYPE_HISTO(int16_t, ih_)
NUMBER_TYPE_HISTO(int32_t, ih_)
NUMBER_TYPE_HISTO(int64_t, ih_)
NUMBER_TYPE_HISTO(float, rh_)
NUMBER_TYPE_HISTO(double, rh_)

#undef NUMBER_TYPE_HISTO

} // namespace serde
} // namespace memory
} // namespace nebula