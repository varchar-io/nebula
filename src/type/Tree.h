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

// By default ROOT starts with 0 in the type tree
static constexpr size_t ROOT = 0;

using namespace nebula::common;

// Utility to check if an object has id() method
template <typename T, typename = size_t>
struct hasId : std::false_type {
};

template <typename T>
struct hasId<T, decltype(std::declval<T>().id())> : std::true_type {
};

template <typename T>
static auto fetchId(const T& t) -> typename std::enable_if<hasId<typename std::remove_pointer<T>::type>::value, size_t>::type {
  if constexpr (std::is_pointer<T>::value) {
    return t->id();
  } else {
    return t.id();
  }
}

template <typename T>
static auto fetchId(const T& t) -> typename std::enable_if<!hasId<typename std::remove_pointer<T>::type>::value, size_t>::type {
  return static_cast<size_t>(t);
}

// Define a generic tree node type = a shared pointer of a tree base
class TreeBase;
using TreeNode = std::shared_ptr<TreeBase>;

// Tree base for generic reference purpose
class TreeBase {
public:
  TreeBase(size_t id) : node_{ 0 }, id_{ id } {}
  TreeBase(size_t id, const std::vector<TreeNode>& children)
    : node_{ 0 }, id_{ id }, children_(children.begin(), children.end()) {}
  virtual ~TreeBase() = default;

  // generic method
  template <typename D>
  D treeWalk(
    std::function<void(const TreeBase&)> prev,
    std::function<D(const TreeBase&, std::vector<D>&)> post) const {
    // only apply on valid PREV procedure
    if (prev) {
      prev(*this);
    }

    // only apply on valid POST procedure
    if (post) {
      std::vector<D> results;
      for (auto& child : children_) {
        results.push_back(child->template treeWalk<D>(prev, post));
      }

      return post(*this, results);
    } else {
      for (auto& child : children_) {
        child->template treeWalk<D>(prev, {});
      }

      return D();
    }
  }

  inline size_t getId() const {
    return id_;
  }

  inline size_t getNode() const {
    return node_;
  }

  // assign NODE ID to all nodes in the type tree
  // The ID allocation is sequence number in depth-first tree traverse
  // While ROOT has node ID as 0
  // return largest NODE ID = number of nodes in the tree
  size_t assignNodeId(size_t start = ROOT) {
    size_t node = start;
    auto last = treeWalk<size_t>(
      [&node](const auto& v) { const_cast<TreeBase&>(v).node_ = node++; },
      [&node](const auto& v, std::vector<size_t>& children) { return node; });

    N_ENSURE_EQ(node, last, "last node should match");
    return last;
  }

public:
  // an open extension to allow client to augment any data structure.
  // NOT USED internal at all - so no responsibility to manage this piece resource
  std::any ext;

protected:
  // Internal NODE sequence ID
  size_t node_;

  // Arbitrary Id used by client
  size_t id_;

  // all children
  std::vector<TreeNode> children_;
}; // namespace type

// Define a generic tree with NODE data typed as T
// Difference between class and typename - declare template parameter type
// CRTP tree - we can use std::any or std::variant in C++17 to make a heterogenous tree
template <typename T>
class Tree : public TreeBase {
public:
  Tree(T data, const std::vector<TreeNode>& children)
    : TreeBase(fetchId(data), children), data_{ data } {}
  Tree(T data) : TreeBase(fetchId(data)), data_{ data } {}
  virtual ~Tree() = default;

  /* Basic Tree APIs */
  template <typename R>
  Tree<R>& childAt(size_t index) {
    return *std::static_pointer_cast<Tree<R>>(childAt(index));
  }

  TreeNode childAt(size_t index) {
    N_ENSURE(index >= 0 && index < children_.size(), "index out of bound");
    return children_[index];
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

private:
  template <typename F, typename... S>
  void each_arg(F f, S&&... s) {
    [](...) {}((f(std::forward<S>(s)), 0)...);
  }
};

} // namespace type
} // namespace nebula