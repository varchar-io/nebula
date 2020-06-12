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

#include "Query.h"

/**
 * Define serialization functions to ser/de expressions to be transferred through network bounds.
 */
namespace nebula {
namespace api {
namespace dsl {

class Serde {
public:
  // expression serde
  static std::string serialize(const Expression&);
  static std::shared_ptr<Expression> deserialize(const std::string&);

  // custom column object serde
  static std::string serialize(const std::vector<CustomColumn>&);
  static std::vector<CustomColumn> deserialize(const char*, size_t);  
};

} // namespace dsl
} // namespace api
} // namespace nebula