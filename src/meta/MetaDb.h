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
namespace meta {

// a meta database interface.
class MetaDb {
public:
  virtual ~MetaDb() = default;
  // read a given key, if not found, return false
  // otherwise the object will be filled with the bytes.
  virtual bool read(const std::string&, std::string&) const = 0;

  // write a k-v into the db
  virtual bool write(const std::string&, const std::string&) = 0;

  // backup the current db
  virtual bool backup() noexcept = 0;

  // close a db
  virtual void close() noexcept = 0;
};

class VoidDb final : public MetaDb {
public:
  ~VoidDb() = default;
  inline virtual bool read(const std::string&, std::string&) const override { return true; }
  inline virtual bool write(const std::string&, const std::string&) override { return true; }
  inline virtual bool backup() noexcept override { return true; }
  inline virtual void close() noexcept override {}
};

} // namespace meta
} // namespace nebula