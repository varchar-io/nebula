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

#include <vector>
#include "Errors.h"

namespace nebula {
namespace type {

using namespace nebula::common;

// Define a generic tree with NODE data typed as T
// Difference between class and typename - declare template parameter type
// CRTP tree - we can use std::any or std::variant in C++17 to make a heterogenous tree
template <typename T>
class Tree {
public:
  virtual ~Tree() = default;

  /* Basic Tree APIs */
  template <typename R>
  Tree<R>& childAt(size_t index) {
    N_ENSURE_GE(index, 0, "index out of bound");
    return static_cast<T*>(this)->childAtImpl(index);
  }

  size_t size() const {
    return static_cast<T*>(this)->sizeImpl();
  }

  template <typename R>
  Tree<R>& addChild(std::unique_ptr<Tree<R>> child) {
    return static_cast<T*>(this)->addChildImpl(std::move(child));
  }
};

// how to do specialization so that it works for all scalar types?
template <>
class Tree<int> {
public:
  Tree(int node) : node_{ node } {}

  Tree<int>& childAt(size_t index) {
    N_ENSURE(index >= 0 && index < size(), "index out of bound");
    return children_[index];
  }

  inline size_t size() const {
    return children_.size();
  }

  Tree<int>& addChild(int v) {
    children_.emplace_back(v);
    return children_[size() - 1];
  }

  const int value() const {
    return node_;
  }

protected:
  int node_;
  std::vector<Tree<int>> children_;
};

} // namespace type
} // namespace nebula