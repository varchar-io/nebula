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

#include "Like.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {
bool match(const char* sp, const size_t ss, size_t si,
           const char* pp, const size_t ps, size_t pi) {
  // when pattern still have words
  while (pi < ps) {
    char ch = *(pp + pi);

    // if not %, source has to match this
    if (ch != '%') {
      char sch = *(sp + si);
      if (ch != sch) {
        return false;
      }

      // move both source and pattern
      si++;
      pi++;
    } else {
      // if current is a matcher char. it can match anything in source
      size_t pos = si + 1;

      // assuming this % match current char in source
      while (pos <= ss) {
        if (match(sp, ss, pos, pp, ps, pi + 1)) {
          return true;
        }

        pos++;
      }

      // none of above try find a match
      return false;
    }
  }

  // when pattern is all done, can't have remaining source for a match
  return si == ss;
}
} // namespace udf
} // namespace api
} // namespace nebula