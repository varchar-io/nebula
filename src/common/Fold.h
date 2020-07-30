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

#include <cmath>
#include <condition_variable>

#include "Evidence.h"
#include "Folly.h"

/**
 * A fold algorithm based on vector of objects
 * fold algorithm utilize multi-core to speed up with folly thread pool
 * 
 * To reduce thread context switch, number of segments per pass is determined by number of cores.
 * SEGMENTS = std::ceiling(SIZE / CORES)
 * 
 * e.g vector=[0, 1, 2, 3, 4, 5, 6], cores=4, => segments = 2, algo = sum
 * plan: [1 -> 0], [3 -> 2], [5->4], [6] => [1, 1, 5, 3, 9, 5, 6]
 *       [2 -> 0], [6 -> 4] => [6, 1, 5, 3, 15, 5, 6]
 *       [4 -> 0] => [21]
 * return vector[0]
 * 
 */

namespace nebula {
namespace common {

// This is a in-place data update version (lambda yields void)
// function provides logic how to fold "from" to "to".
// we may need different version for slot replacement version
template <typename T>
void fold(folly::ThreadPoolExecutor& pool,
          const std::vector<T>& sources,
          const std::function<void(T& from, T& to)> algo,
          size_t width = 0) noexcept {

  Evidence::Duration duration;
  const auto size = sources.size();

  // nothing to fold
  if (UNLIKELY(size < 2)) {
    return;
  }

  // for width = 1, it indicates using current thread to do all merge
  if (width == 1) {
    auto& to = const_cast<T&>(sources.at(0));
    for (size_t i = 1; i < size; ++i) {
      auto& from = const_cast<T&>(sources.at(i));
      algo(from, to);
    }

    return;
  }

  // calculate the width of each fold at least folding 2
  constexpr size_t MIN_WIDTH = 2;
  if (width == 0) {
    const auto poolSize = pool.numThreads();
    width = std::max((size / poolSize), MIN_WIDTH);
  }

  // pass will tell us how far each fold will apply: width * stage
  auto pass = 1;

  // working on all iterations through multi-fold system
  std::atomic<int> count;
  std::mutex cv_m;
  std::unique_lock<std::mutex> lock(cv_m);
  std::condition_variable cv;

  // given pass and width, the step to look for next step
  size_t step = 0;
  while ((step = std::pow(width, pass - 1)) < size) {
    // reset counter
    count = 0;

    // work on current pass by enqueue all tasks
    size_t i = 0;
    while (i < size) {
      auto end = std::min(i + step * width, size);
      pool.add([step, end, i, &sources, &algo, &count, &cv]() {
        // do the work by folding
        auto& to = const_cast<T&>(sources.at(i));
        for (auto r = i + step; r < end; r += step) {
          auto& from = const_cast<T&>(sources.at(r));
          algo(from, to);
        }

        // after finish the work increase the count
        --count;
        cv.notify_one();
      });

      ++count;

      // next position will be skip pass * width
      i = end;
    }

    // wait for all tasks done in this pass, then move to next pass
    cv.wait(lock, [&count]() { return count == 0; });
    ++pass;
  }

  LOG(INFO) << "Folded in passes=" << pass << " with width=" << width << " using ms=" << duration.elapsedMs();
}

} // namespace common
} // namespace nebula
