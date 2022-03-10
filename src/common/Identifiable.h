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

#include <string>

namespace nebula {
namespace common {
// provide unified interface to indentify all assets in Nebula
// such as:
//          Nebula Node
//          Nebula Table
//          Nebula Data Spec (Ingest Spec)
//          Nebula Data Block
//          Nebula Column
//          ...
// uniqueness is very important through the system
// But most of the time, we don't use GUID because we're making semantic meaningfulness for each asset.
class Identifiable {
public:
  // an identifiable should always return a unique ID in its breed
  virtual const std::string& id() const = 0;

  // when we serialize a class with computed fields (value cache)
  // usually they are private and need to computed in the construction time
  // this interface is used to be called after object is deserialized
  virtual void construct() = 0;
};

} // namespace common
} // namespace nebula