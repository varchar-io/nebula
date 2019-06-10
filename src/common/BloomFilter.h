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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <memory>
#include <vector>
#include "Bloom.h"
#include "Evidence.h"

// #include "bf/bloom_filter/basic.hpp"
// #include "cuckoofilter.h"

// DEFINE_double(BF_FPP, 0.001, "false positive rate for the bloom filter");

/**
 * Define a bloom filter module.
 * Internally we're using cuckoo filter as initial implementation.
 */
namespace nebula {
namespace common {
template <typename T, size_t BITS = 16>
class BloomFilter {
public:
  BloomFilter(size_t items) {
    bloom_parameters parameters;
    parameters.projected_element_count = items;
    parameters.false_positive_probability = 0.001;
    parameters.random_seed = Evidence::ticks();
    parameters.compute_optimal_parameters();

    //Instantiate Bloom Filter
    filter_ = std::make_unique<bloom_filter>(parameters);
  }
  virtual ~BloomFilter() = default;

public:
  // add an item in the set, if no more space or other issues
  // return false to indicate this item was not added
  bool add(const T& item) noexcept {
    filter_->insert(item);
    // filter_.add(item);
    // auto x = filter_.Add(item);
    // if (x != cuckoofilter::Ok) {
    //   LOG(INFO) << (int)x;
    // }
    // return x == cuckoofilter::Ok;
    return true;
  }

  // check if given item is probably in the set.
  // return false if absolutely not in the set
  bool probably(const T& item) const noexcept {
    return filter_->contains(item);
    // return filter_.lookup(item) == 1;
    // return filter_.Contain(item) == cuckoofilter::Ok;
  }

  // memory bytes used by this filter
  size_t bytes() const noexcept {
    return filter_->size();
    // return 0;
    // return filter_.SizeInBytes();
  }

private:
  std::unique_ptr<bloom_filter> filter_;
  // cuckoofilter::CuckooFilter<T, BITS> filter_;
};

} // namespace common
} // namespace nebula