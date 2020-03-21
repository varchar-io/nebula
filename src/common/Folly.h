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

// expliclitly define this for portability?
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS 0x20
#endif

/* A central place to include folly features */
#include <folly/Conv.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/ThreadPoolExecutor.h>
#include <folly/experimental/FunctionScheduler.h>
#include <folly/futures/Future.h>
#include <folly/init/Init.h>