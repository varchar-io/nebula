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

#include "Histogram.h"
#include "type/Type.h"

/**
 * Block is an interface to define what type of interfaces 
 * We can get from a data chunk, it is similar to meta/NBlock 
 * which is a wrapper to build identity of a block, in somewhat sense, we may merge these two.
 */
namespace nebula {
namespace surface {
namespace eval {
class Block {
public:
  virtual ~Block() = default;

public:
  // get the table schema of current data block
  virtual nebula::type::Schema schema() const = 0;

  // get total number of rows in current data block
  virtual size_t getRows() const = 0;

  // get column type
  virtual nebula::type::TypeNode columnType(const std::string&) const = 0;

  // get column histogram
  virtual std::shared_ptr<Histogram> histogram(const std::string&) const = 0;

  // get column partition values - empty if not a partition column
  virtual std::vector<std::any> partitionValues(const std::string&) const = 0;

  // check if a value is probably in the block
  virtual bool probably(const std::string&, std::any) const = 0;
};

} // namespace eval
} // namespace surface
} // namespace nebula