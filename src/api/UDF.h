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

#include "execution/eval/ValueEval.h"
#include "glog/logging.h"
#include "surface/DataSurface.h"
#include "type/Type.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
// define an UDF interface
template <nebula::type::Kind KIND>
class UDF : public nebula::execution::eval::KindEval<KIND> {
public:
  UDF() = default;
  virtual ~UDF() = default;

  // columns referenced
  virtual std::vector<std::string> columns() const = 0;
};

// UDAF is a state ful object
template <nebula::type::Kind KIND>
class UDAF : public nebula::execution::eval::KindEval<KIND> {
public:
  UDAF() = default;
  virtual ~UDAF() = default;

  // columns referenced
  virtual std::vector<std::string> columns() const = 0;

  // partial aggregate
  virtual void partial(const nebula::surface::RowData&) = 0;

  // global aggregate
  virtual void global(const nebula::surface::RowData&) = 0;
};

enum class UDAF_REG {
  MAX,
  MIN,
  AVG,
  COUNT
};

} // namespace api
} // namespace nebula