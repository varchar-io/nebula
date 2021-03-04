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

#if defined(__GNUC__)
#define N_LIKELY(x) (__builtin_expect((x), 1))
#define N_UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define N_LIKELY(x) (x)
#define N_UNLIKELY(x) (x)
#endif

#define N_FORCE_INLINE inline __attribute__((always_inline))
#define N_NO_INLINE __attribute__((noinline))
#define N_MAYBE_UNUSED __attribute__((unused))

// define glue to glue two symbols together as one symbol beforee compiling
// such as "GLUE(x, y)" MACRO translated into "xy"
#define N_GLUE(x, y) x##y

// Basic litteral transformation LITERAL(x) => "x"
#define N_LITERAL(x) #x

// get offset of given field in given object.
#ifndef FOFF
#define FOFF(type, field) ((size_t) & ((type*)0)->field)
#endif

// get array length
#ifndef ALEN
#define ALEN(x) (sizeof(x) / sizeof((x)[0]))
#endif
