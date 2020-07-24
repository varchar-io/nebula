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

#include <fmt/format.h>
#include <iostream>
#include <queue>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "Hash.h"

namespace nebula {
namespace common {

// this implements tree path merging algorithm.
// it merges any given path (usually call stack) into a tree with weighted nodes
// for example, [A, B, C] + [A, B, D] => (A.2, B.2, [C.1, D.1])
// a stack is represented by a vector/list.
// TODO(cao) - consider extending type/Tree.h to support this with efficient data structure
// We provide two implementations: HashFrame and VectorFrame using hash table and vector each
// VectorFrame can be optimized further by binary search with maintained order.
template <typename T>
struct HashFrame {
  explicit HashFrame(const T& d, size_t dep, size_t c = 1)
    : data{ d }, depth{ dep }, count{ c } {
  }

  using RAW_PTR = HashFrame<T>*;
  using PTR = typename std::unique_ptr<HashFrame<T>>;
  struct Hash {
  public:
    size_t operator()(const PTR& ptr) const {
      return h_(ptr->data);
    }

  private:
    std::hash<T> h_;
  };

  struct Equal {
  public:
    bool operator()(const PTR& p1, const PTR& p2) const {
      return p1->data == p2->data;
    }
  };

  const T data;
  const size_t depth;
  size_t count;
  unordered_set<PTR, Hash, Equal> children;

  // add child if not found existing
  RAW_PTR addIfNotFound(const T& d, size_t dep) {
    auto ptr = std::make_unique<HashFrame>(d, dep, 0);
    auto found = children.find(ptr);
    if (found != children.end()) {
      return (*found).get();
    } else {
      // insert this new node and set current to it
      return (*children.insert(std::move(ptr)).first).get();
    }
  }

  RAW_PTR find(const T& d) const noexcept {
    auto ptr = std::make_unique<HashFrame>(d, 0);
    auto found = children.find(ptr);

    if (found != children.end()) {
      return (*found).get();
    }

    return nullptr;
  }

  // insert a new child
  void add(PTR p) {
    children.insert(std::move(p));
  }
};

template <typename T>
struct VectorFrame {
  explicit VectorFrame(const T& d, size_t dep, size_t c = 1)
    : data{ d }, depth{ dep }, count{ c } {
  }

  using RAW_PTR = VectorFrame<T>*;
  using PTR = typename std::unique_ptr<VectorFrame<T>>;

  const T data;
  const size_t depth;
  size_t count;
  std::vector<PTR> children;

  // API
  RAW_PTR addIfNotFound(const T& d, size_t dep) noexcept {
    auto found = std::find_if(std::begin(children), std::end(children), [&d](const auto& i) {
      return i->data == d;
    });
    if (found != children.end()) {
      return (*found).get();
    } else {
      // insert this new node and set current to it
      return (*children.insert(children.cend(), std::make_unique<VectorFrame>(d, dep, 0))).get();
    }
  }

  RAW_PTR find(const T& d) const noexcept {
    auto found = std::find_if(std::begin(children), std::end(children), [&d](const auto& i) {
      return i->data == d;
    });

    if (found != children.end()) {
      return (*found).get();
    }

    return nullptr;
  }

  // insert a new child
  void add(PTR p) noexcept {
    children.push_back(std::move(p));
  }
};

template <typename T, bool hash = false>
class StackTree {
  using FT = std::conditional_t<hash, HashFrame<T>, VectorFrame<T>>;
  using ST = typename std::vector<T>;

public:
  // build a stack tree object from a json blob
  StackTree(const std::string_view& json) : rf_{ true } {
    parse(json);
  }

  // construct a new stack tree
  StackTree(bool rootFirst = true)
    : rf_{ rootFirst }, root_{ std::make_unique<FT>(T{}, 0, 0) } {}
  virtual ~StackTree() = default;

public:
  // merge a new stack into current tree
  void merge(const ST& stack) noexcept {
    // root always gets vote
    ++root_->count;

    // layer by layer search, counting if found, otherwise a new frame added
    // for stack, root is at the bottom.
    FT* current = root_.get();

    if (rf_) {
      for (size_t i = 0; i < stack.size(); ++i) {
        current = visit(current, stack.at(i));
      }
    } else {
      for (int i = stack.size() - 1; i >= 0; --i) {
        current = visit(current, stack.at(i));
      }
    }
  }

  // also supports reading line by line from input stream
  void merge(std::istream& text) noexcept {
    if (!rf_) {
      ST lines;
      lines.reserve(64);
      std::string line;
      while (std::getline(text, line)) {
        lines.push_back(std::move(line));
      }

      return merge(lines);
    }

    // otherwise, for root first, we can process without buffer/copy
    // root always gets vote
    ++root_->count;
    FT* current = root_.get();
    std::string line;
    while (std::getline(text, line)) {
      current = visit(current, line);
    }
  }

  void merge(const StackTree& stack) noexcept {
    merge(*root_, *stack.root_);
  }

  // print current tree in JSON format
  // please refer this http://martinspier.io/d3-flame-graph/stacks.json format for visualization readyness
  std::string jsonfy(size_t threshold = 1) const noexcept {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> json(buffer);

    jsonfy(*root_, json, threshold);
    return buffer.GetString();
  }

  // print as lines of strings in alphabatic order - debug/test purpose only
  friend std::ostream& operator<<(std::ostream& os, const StackTree& tree) noexcept {
    std::queue<FT*> q;
    q.push(tree.root_.get());
    std::vector<std::string> lines;
    while (!q.empty()) {
      FT* p = q.front();
      lines.push_back(fmt::format("NODE: d={0}, c={1}, l={2}", p->data, p->count, p->depth));

      // pop current and push all its children
      q.pop();
      for (auto itr = p->children.begin(); itr != p->children.end(); ++itr) {
        q.push((*itr).get());
      }
    }

    // sort the result for deterministic result used in test case
    std::sort(std::begin(lines), std::end(lines));
    for (auto& l : lines) {
      os << l << std::endl;
    }

    return os;
  }

private:
  // jsonfy current object
  void jsonfy(FT& obj, rapidjson::Writer<rapidjson::StringBuffer>& json, size_t threshold) const noexcept {
    // threshold indicates minial value to be serialized
    if (obj.count < threshold) {
      return;
    }

    // write root
    json.StartObject();

    // data field
    json.Key("name");
    if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>) {
      json.String(obj.data.data(), obj.data.size());
    } else if constexpr (std::is_integral_v<T>) {
      json.Int(obj.data);
    } else {
      throw NException("Type not supported for data");
    }

    // count field
    json.Key("value");
    json.Int(obj.count);

    // all children
    if (obj.children.size() > 0) {
      json.Key("children");
      json.StartArray();
      for (auto& item : obj.children) {
        jsonfy(*item, json, threshold);
      }
      json.EndArray();
    }

    // end current object
    json.EndObject();
  }

  std::unique_ptr<FT> parse(const rapidjson::GenericObject<false, rapidjson::Value>& obj, size_t depth) {
    std::unique_ptr<FT> ptr = nullptr;
    if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>) {
      ptr = std::make_unique<FT>(obj["name"].GetString(), depth, obj["value"].GetInt());
    } else if constexpr (std::is_integral_v<T>) {
      ptr = std::make_unique<FT>(obj["name"].GetInt(), depth, obj["value"].GetInt());
    } else {
      throw NException("Type not supported for data");
    }

    if (obj.HasMember("children")) {
      for (auto& child : obj["children"].GetArray()) {
        ptr->add(parse(child.GetObject(), depth + 1));
      }
    }

    return ptr;
  }

  // reference method "jsonfy" for serialization format
  void parse(const std::string_view& json) {
    rapidjson::Document cd;
    if (cd.Parse(json.data(), json.size()).HasParseError()) {
      throw NException(fmt::format("Error parsing stack tree json: {0}", json));
    }

    const auto& root = cd.GetObject();
    root_ = parse(root, 0);
  }

  static FT* visit(FT* current, const T& data) {
    current = current->addIfNotFound(data, current->depth + 1);
    ++current->count;
    return current;
  }

  // copy the whole branch represented by child into target
  static typename FT::PTR copy(const FT& child) {
    auto node = std::make_unique<FT>(child.data, child.depth, child.count);
    for (auto& n : child.children) {
      node->add(copy(*n));
    }

    return node;
  }

  // merge frame source into target
  static void merge(FT& target, const FT& source) {
    N_ENSURE(target.depth == source.depth, "merge nodes at the same level");
    target.count += source.count;

    // check all elements, if found in target, merge that nodes
    // otherwies, build up the whole sub tree in target
    for (auto& node : source.children) {
      auto ptr = target.find(node->data);
      if (ptr) {
        merge(*ptr, *node);
      } else {
        target.add(copy(*node));
      }
    }
  }

private:
  // root first indicates if root shows first (upside down - icicle view)
  bool rf_;
  std::unique_ptr<FT> root_;
};

} // namespace common
} // namespace nebula