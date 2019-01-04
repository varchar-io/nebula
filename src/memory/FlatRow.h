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

#include <stack>
#include "Memory.h"
#include "Type.h"

// DEFINE_int32(SLICE_SIZE, 64 * 1024, "slice size in bytes");

/**
 * Batch implementation in two modes - single row / multiple rows
 */
namespace nebula {
namespace memory {

using nebula::common::NebulaException;
using nebula::common::PagedSlice;
using nebula::type::Kind;
using nebula::type::Schema;
using nebula::type::Tree;
using nebula::type::TreeBase;
using nebula::type::TreeNode;
using nebula::type::TypeNode;

static constexpr size_t SLICE_SIZE = 1024 * 1024;

/**
 * A flat memory serialization format for a ROW structure.
 * internally it's just a byte array. The data layout like this
 * it's designed to be reused to carry every row data from reader to writer
 * it's optimized for fast access nad not optimized for storage size -
 * so row data memory may be 2x larger than original row data
 * <p>
 * Every value has size prefix, 1byte flag indicating if its null or not
 * Compound types are struct, map and list
 * PRIMITIVES
 * - 1byte flag size + {0, 1,2,4,8} bytes value for fixed (0 if it's null)
 * - 1byte flag + 4bytes size + {0, N} bytes for variable length value.
 * <p>
 * Struct
 * 1byte flag + {4 bytes size if flag=0} + repeat {field}
 * MAP
 * 1byte flag + {4 bytes items if flag=0} + {4 bytes size if not null and having items} + repeat {key, value}
 * List
 * 1byte flag + {4 bytes items if flag=0} + {4 bytes size if not null and having items} + repeat {item}
 */
struct DataMeta;
class FlatRow {
public:
  FlatRow(const Schema& schema) : schema_{ schema } {
    // assign NODES from ROOT 0
    schema_->assignNodeId();

    // start the row struct automatically
    begin(schema_);
  }

  // initialize states for writing a new row
  void init();

  // begin or end any NODE
  void begin(const TypeNode& node, size_t items = 1);

  // end current NODE writing and return total size of current node
  size_t end(const TypeNode& node);

  // seal method to end whole row writing = alias to end(schema_)
  size_t seal() {
    return end(schema_);
  }

  // locate node with given NAME
  TypeNode locate(const std::string& name);

  // write data at given memory offset for specified node
  template <typename T>
  size_t write(const std::string& name, const T& value) {
    // locate the name, check if currently in a un-named context
    // named context - stack top is "struct"
    N_ENSURE(!stack_.empty() && stack_.top()->k() == Kind::STRUCT,
             "only support name lookup in struct context");

    // look up name in the tree
    auto node = locate(name);
    N_ENSURE(!!node, fmt::format("node not found: {0}", name));

    // write value
    return write<T>(value);
  }

  //  write data at current offset
  template <typename T>
  size_t write(const T& value);

private:
  // report when a single value written, a primitive value, a list, a map or a struct
  void onValueWritten() {
    // pick top in the stack and update its expected value
    const auto& parent = stack_.top();
    parent->values--;
  }

private:
  // data containers
  Schema schema_;
  PagedSlice<SLICE_SIZE> slice_;

  // write states
  size_t cursor_;
  std::stack<std::unique_ptr<DataMeta>> stack_;
};

template <typename T>
size_t FlatRow::write(const T& value) {
  LOG(INFO) << "write-value called";

  // report a value written
  onValueWritten();
  return 0;
}

// Data Tree share the same memory slice
// its data field is offset in the slice
struct DataMeta {
  DataMeta(const TypeNode& n, size_t c = 0)
    : node{ n }, values{ c }, offset{ 0 }, length{ 0 } {}
  ~DataMeta() = default;

  // type node
  TypeNode node;

  // number of values in list/map
  size_t values;

  // offset in the memory chunk
  size_t offset;

  // length of all data for this node
  size_t length;
};

} // namespace memory
} // namespace nebula