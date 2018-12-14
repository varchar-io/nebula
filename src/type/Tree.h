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

#include <any>
#include <vector>
#include "Errors.h"
#include "glog/logging.h"

namespace nebula {
namespace type {

using namespace nebula::common;

// Define a generic tree with NODE data typed as T
// Difference between class and typename - declare template parameter type
// CRTP tree - we can use std::any or std::variant in C++17 to make a heterogenous tree
template <typename T>
class Tree {
public:
  Tree(T data) : data_{ data } {}
  virtual ~Tree() = default;

  /* Basic Tree APIs */
  template <typename R>
  Tree<R>& childAt(size_t index) {
    N_ENSURE(index >= 0 && index < children_.size(), "index out of bound");

    const auto& item = children_[index];
    LOG(INFO) << "type of the item: " << item.type().name();

    return *std::any_cast<std::shared_ptr<Tree<R>>>(item);
  }

  inline size_t size() const {
    return children_.size();
  }

  const T& value() const {
    return data_;
  }

  template <typename R>
  Tree<R>& addChild(std::shared_ptr<Tree<R>> child) {
    children_.push_back(child);
    return childAt<R>(size() - 1);
  }

  template <typename... S>
  size_t addChildren(std::shared_ptr<Tree<S>>... children) {
    auto size = children_.size();

    // C++ 17: fold-expressions
    auto one = [this](auto c) { children_.push_back(c); };
    (one(children), ...);

    // Otherwise we can use this method to do expansion
    // each_arg([this](auto c) { children_.push_back(c); }, children...);
    return children_.size() - size;
  }

protected:
  T data_;
  std::vector<std::any> children_;

private:
  template <typename F, typename... S>
  void each_arg(F f, S&&... s) {
    [](...) {}((f(std::forward<S>(s)), 0)...);
  }
};

} // namespace type
} // namespace nebula