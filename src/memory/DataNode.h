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

#include "DataSurface.h"
#include "Memory.h"
#include "Tree.h"
#include "Type.h"
#include "encode/Encoder.h"
#include "glog/logging.h"
#include "serde/TypeData.h"
#include "serde/TypeDataFactory.h"
#include "serde/TypeMetadata.h"

/**
 * A data node holds real memory data for each node in the schema tree
 * 
 */
namespace nebula {
namespace memory {

// define DataTree type = shared pointer of a data node
class DataNode;
using nebula::surface::IndexType;
using DataTree = std::shared_ptr<DataNode>;
using TDataNode = DataNode;
using PDataNode = DataNode*;

class DataNode : public nebula::type::Tree<PDataNode> {
public:
  static DataTree buildDataTree(const nebula::type::Schema&);

public:
  DataNode(const nebula::type::TypeBase& type)
    : type_{ type },
      meta_{ nebula::memory::serde::TypeDataFactory::createMeta(type.k()) },
      data_{ nebula::memory::serde::TypeDataFactory::createData(type.k()) },
      count_{ 0 },
      rawSize_{ 0 },
      nebula::type::Tree<PDataNode>(this) {
    LOG(INFO) << fmt::format("Create data node w/o children [{0}].", type.name());
  }

  DataNode(const nebula::type::TypeBase& type, const std::vector<nebula::type::TreeNode>& children)
    : type_{ type },
      meta_{ nebula::memory::serde::TypeDataFactory::createMeta(type.k()) },
      data_{ nebula::memory::serde::TypeDataFactory::createData(type.k()) },
      count_{ 0 },
      rawSize_{ 0 },
      nebula::type::Tree<DataNode*>(this, children) {
    LOG(INFO) << fmt::format("Create data node [N={0}, T={1}] with children {2}",
                             type.name(), (int)type.k(), children.size());
  }

  virtual ~DataNode() = default;

  inline size_t id() const {
    // TODO(cao): generate identifier for current node
    return 0;
  }

public: // data appending API
  size_t appendNull();

  template <typename T>
  size_t append(T v);

  size_t append(const std::string&);

public: // data reading API
  // use std::optional to simplify the interface
  // instead of
  // if(!isNull(index)) {
  //    type = read()
  // }
  // we can do
  // std::Optional<Type> type = read();
  inline bool isNull(size_t index) {
    return meta_->isNull(index);
  }

  template <typename T>
  T read(size_t index);

public: // basic metadata exposure
  inline size_t entries() const {
    return count_;
  }

  inline size_t rawSize() const {
    return rawSize_;
  }

  inline size_t storageSize() const {
    // count metadata size in?
    // not including children's size - API provide access any node in the tree
    return data_ == nullptr ? 0 : data_->size();
  }

  inline size_t storageAllocation() const {
    return data_ == nullptr ? 0 : data_->capacity();
  }

  // list/map retrieve child's offset and length at some position
  inline std::tuple<IndexType, IndexType> offsetSize(IndexType index) {
    return meta_->offsetSize(index);
  }

private:
  // called for every single value added in current node
  inline size_t cursorAndAdvance() {
    return count_++;
  }

private:
  // pointing to a node in the schema tree which is supposed to be shared.
  // should we make a copy here to be safe?
  const nebula::type::TypeBase& type_;

  // metadata
  std::unique_ptr<nebula::memory::serde::TypeMetadata> meta_;

  // real data
  std::unique_ptr<nebula::memory::serde::TypeDataProxy> data_;

  // number of values in current data node
  size_t count_;

  // raw size of data accumulation
  size_t rawSize_;
};
} // namespace memory
} // namespace nebula