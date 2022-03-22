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

#include "DataSpec.h"

/**
 * Node manager managing all nebula working nodes
 */
namespace nebula {
namespace meta {

std::string DataSpec::serialize(const DataSpec& spec) noexcept {
  std::stringstream buffer;
  msgpack::pack(buffer, spec);
  buffer.seekg(0);
  return buffer.str();
}

SpecPtr DataSpec::deserialize(const std::string_view str) {
  msgpack::object_handle oh = msgpack::unpack(str.data(), str.size());
  auto spec = oh.get().as<SpecPtr>();
  // reconstruct all splits
  for (auto& split : spec->splits_) {
    split->construct();
  }

  // reconstruct spec too
  spec->construct();

  // now the spec should be well established
  return spec;
}

} // namespace meta
} // namespace nebula