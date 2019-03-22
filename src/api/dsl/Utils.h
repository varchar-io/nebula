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

#include "api/UDAF.h"
#include "common/Errors.h"
#include "glog/logging.h"
#include "meta/Table.h"
#include "type/Tree.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace dsl {

#ifndef THIS_TYPE
#define THIS_TYPE typename std::remove_reference<decltype(*this)>::type
#endif

#ifndef IS_T_LITERAL
#define IS_T_LITERAL(T) std::is_same<char*, std::decay_t<T>>::value
#endif

} // namespace dsl
} // namespace api
} // namespace nebula