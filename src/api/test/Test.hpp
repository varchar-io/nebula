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

#include <mutex>
#include <thread>

namespace nebula {
namespace api {
namespace test {

// get sync test mutex
std::mutex& sync_test_mutex();

// generate data
std::tuple<std::string, int64_t, int64_t> genData(unsigned numBlocks = std::thread::hardware_concurrency());

} // namespace test
} // namespace api
} // namespace nebula