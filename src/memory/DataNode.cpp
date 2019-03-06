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

#include "DataNode.h"
#include "Errors.h"

DEFINE_int32(META_PAGE_SIZE, 4 * 1024, "page size for meta data");
DEFINE_int32(DATA_PAGE_SIZE, 256 * 1024, "page size for real data");

/**
 * A data node holds real memory data for each node in the schema tree
 * 
 */
namespace nebula {
namespace memory {

using nebula::common::NebulaException;
using nebula::type::Kind;
using nebula::type::Schema;
using nebula::type::TreeNode;
using nebula::type::TypeBase;
using nebula::type::TypeNode;

// global NULL SIZE definition = null value takes 1 byte raw space
static const uint32_t NULL_SIZE = 1;

// static method to build node tree
DataTree DataNode::buildDataTree(const Schema& schema) {
  // traverse the whole schema tree to generate a data tree
  auto dataTree = schema->treeWalk<TreeNode>(
    [](const auto& v) {},
    [](const auto& v, std::vector<TreeNode>& children) {
      return TreeNode(new DataNode(dynamic_cast<const TypeBase&>(v), children));
    });

  return std::static_pointer_cast<DataNode>(dataTree);
}

#define DISPATCH_KIND(size, KIND, object, value) \
  case Kind::KIND: {                             \
    size += object->append(value);               \
    break;                                       \
  }

uint32_t DataNode::append(const nebula::surface::RowData& row) {
  N_ENSURE(type_.k() == Kind::STRUCT, "struct type expected");

  // fill all results - introduce iterator in tree to ease the loops?
  uint32_t size = 0;
  for (size_t i = 0, count = this->size(); i < count; ++i) {
    const auto& child = this->childAt<PDataNode>(i).value();
    const auto kind = child->type_.k();
    const auto& name = child->type_.name();

    // null field
    if (row.isNull(name)) {
      size += child->appendNull();
      continue;
    }

    switch (kind) {
      DISPATCH_KIND(size, BOOLEAN, child, row.readBool(name))
      DISPATCH_KIND(size, TINYINT, child, row.readByte(name))
      DISPATCH_KIND(size, SMALLINT, child, row.readShort(name))
      DISPATCH_KIND(size, INTEGER, child, row.readInt(name))
      DISPATCH_KIND(size, BIGINT, child, row.readLong(name))
      DISPATCH_KIND(size, REAL, child, row.readFloat(name))
      DISPATCH_KIND(size, DOUBLE, child, row.readDouble(name))
      DISPATCH_KIND(size, VARCHAR, child, row.readString(name))
    case Kind::ARRAY: {
      auto list = row.readList(name);
      size += child->append(*list);
      break;
    }
    case Kind::MAP: {
      auto map = row.readMap(name);
      size += child->append(*map);
      break;
    }
    default:
      throw NebulaException(fmt::format("Not supported type: {0}", name));
    }
  }

  // return total raw size of the data added in this node
  return size;
}

#undef DISPATCH_KIND

#define DISPATCH_KIND(KIND, lambda, object, func)                          \
  case Kind::KIND: {                                                       \
    lambda = [&object, &list](auto i) { return object->append(func(i)); }; \
    break;                                                                 \
  }

uint32_t DataNode::append(const nebula::surface::ListData& list) {
  N_ENSURE(type_.k() == Kind::ARRAY, "list/array type expected");
  const auto& child = this->childAt<PDataNode>(0).value();
  auto items = list.getItems();
  const auto kind = child->type_.k();
  uint32_t size = 0;

  std::function<uint32_t(int)> lambda;
  switch (kind) {
    DISPATCH_KIND(BOOLEAN, lambda, child, list.readBool)
    DISPATCH_KIND(TINYINT, lambda, child, list.readByte)
    DISPATCH_KIND(SMALLINT, lambda, child, list.readShort)
    DISPATCH_KIND(INTEGER, lambda, child, list.readInt)
    DISPATCH_KIND(BIGINT, lambda, child, list.readLong)
    DISPATCH_KIND(REAL, lambda, child, list.readFloat)
    DISPATCH_KIND(DOUBLE, lambda, child, list.readDouble)
    DISPATCH_KIND(VARCHAR, lambda, child, list.readString)
  default:
    throw NebulaException(fmt::format("Not supported type: {0}", nebula::type::KIND_NAME(kind)));
  }

  for (auto i = 0; i < items; ++i) {
    // null field
    if (list.isNull(i)) {
      size += child->appendNull();
      continue;
    }

    size += lambda(i);
  }

  // return the raw size just added to current list
  return size;
}

uint32_t DataNode::append(const nebula::surface::MapData& map) {
  LOG(INFO) << "append a map";
  N_ENSURE(type_.k() == Kind::MAP, "map type expected");
  const auto& key = this->childAt<PDataNode>(0).value();
  const auto& value = this->childAt<PDataNode>(1).value();

  auto entries = map.getItems();
  const auto keyKind = key->type_.k();
  const auto valueKind = value->type_.k();
  uint32_t size = 0;

  auto keys = map.readKeys();
  size += key->append(*keys);
  auto values = map.readValues();
  size += value->append(*values);

  // return raw size just added to current map
  return size;
}

uint32_t DataNode::appendNull() {
  // TODO: add a NULL value
  return NULL_SIZE;
}

uint32_t DataNode::append(bool b) {
  N_ENSURE(type_.k() == Kind::BOOLEAN, "bool type expected");

  // TODO: add a bool value

  return nebula::type::BoolType::width;
}

uint32_t DataNode::append(int8_t b) {
  N_ENSURE(type_.k() == nebula::type::ByteType::kind, "byte type expected");

  // TODO: add a byte value

  return nebula::type::ByteType::width;
}

uint32_t DataNode::append(int16_t s) {
  N_ENSURE(type_.k() == nebula::type::ShortType::kind, "short type expected");

  // TODO: add a short value

  return nebula::type::ShortType::width;
}

uint32_t DataNode::append(int32_t i) {
  N_ENSURE(type_.k() == nebula::type::IntType::kind, "int type expected");

  // TODO: add a int value

  return nebula::type::IntType::width;
}

uint32_t DataNode::append(int64_t l) {
  N_ENSURE(type_.k() == nebula::type::LongType::kind, "long type expected");

  // TODO: add a long value

  return nebula::type::LongType::width;
}

uint32_t DataNode::append(float f) {
  N_ENSURE(type_.k() == nebula::type::FloatType::kind, "float type expected");

  // TODO: add a float value

  return nebula::type::FloatType::width;
}

uint32_t DataNode::append(double d) {
  N_ENSURE(type_.k() == nebula::type::DoubleType::kind, "double type expected");

  // TODO: add a double value

  return nebula::type::DoubleType::width;
}

uint32_t DataNode::append(const std::string& str) {
  N_ENSURE(type_.k() == nebula::type::StringType::kind, "string type expected");

  // TODO: add a string value

  // return bytes length of this string in UTF8 encoding
  return str.length();
}

} // namespace memory
} // namespace nebula