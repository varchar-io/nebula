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

/**
 * A data node holds real memory data for each node in the schema tree
 * 
 */
namespace nebula {
namespace memory {

using nebula::common::NebulaException;
using nebula::surface::ListData;
using nebula::surface::MapData;
using nebula::surface::RowData;
using nebula::type::Kind;
using nebula::type::Schema;
using nebula::type::TreeNode;
using nebula::type::TypeBase;
using nebula::type::TypeNode;

// global NULL SIZE definition = null value takes 1 byte raw space
static constexpr size_t NULL_SIZE = 1;

// static method to build node tree
DataTree DataNode::buildDataTree(const Schema& schema) {
  // traverse the whole schema tree to generate a data tree
  auto dataTree = schema->treeWalk<TreeNode>(
    [](const auto& v) {},
    [](const auto& v, std::vector<TreeNode>& children) {
      const auto& t = dynamic_cast<const TypeBase&>(v);
      return TreeNode(new DataNode(t, children));
    });

  return std::static_pointer_cast<DataNode>(dataTree);
}

#define INCREMENT_RAW_SIZE_AND_RETURN() \
  rawSize_ += size;                     \
  return size;

size_t DataNode::appendNull() {
  constexpr static auto NULL_TUPLE = std::make_tuple(0, NULL_SIZE);

  // meta_->set(cursorAndAdvance(), NULL_TUPLE);
  constexpr size_t size = NULL_SIZE;
  auto index = cursorAndAdvance();
  meta_->setNull(index);

  // TODO(cao): trade space for complexity
  // fill holes with void value so that reader doesn't need to skip holes
  // data_ could be null if it's compound type
  data_->addVoid(index);

  INCREMENT_RAW_SIZE_AND_RETURN()
}

template <>
size_t DataNode::append(bool b) {
  N_ENSURE(type_.k() == Kind::BOOLEAN, "bool type expected");

  constexpr size_t size = nebula::type::BoolType::width;
  data_->add(cursorAndAdvance(), b);
  INCREMENT_RAW_SIZE_AND_RETURN()
}

template <>
size_t DataNode::append(int8_t b) {
  N_ENSURE(type_.k() == nebula::type::ByteType::kind, "byte type expected");

  constexpr size_t size = nebula::type::ByteType::width;
  data_->add(cursorAndAdvance(), b);

  INCREMENT_RAW_SIZE_AND_RETURN()
}

template <>
size_t DataNode::append(int16_t s) {
  N_ENSURE(type_.k() == nebula::type::ShortType::kind, "short type expected");

  constexpr size_t size = nebula::type::ShortType::width;
  data_->add(cursorAndAdvance(), s);

  INCREMENT_RAW_SIZE_AND_RETURN()
}

template <>
size_t DataNode::append(int32_t i) {
  N_ENSURE(type_.k() == nebula::type::IntType::kind, "int type expected");

  constexpr size_t size = nebula::type::IntType::width;
  data_->add(cursorAndAdvance(), i);

  INCREMENT_RAW_SIZE_AND_RETURN()
}

template <>
size_t DataNode::append(int64_t l) {
  N_ENSURE(type_.k() == nebula::type::LongType::kind, "long type expected");

  constexpr size_t size = nebula::type::LongType::width;
  data_->add(cursorAndAdvance(), l);

  INCREMENT_RAW_SIZE_AND_RETURN()
}

template <>
size_t DataNode::append(float f) {
  N_ENSURE(type_.k() == nebula::type::FloatType::kind, "float type expected");

  constexpr size_t size = nebula::type::FloatType::width;
  data_->add(cursorAndAdvance(), f);

  INCREMENT_RAW_SIZE_AND_RETURN()
}

template <>
size_t DataNode::append(double d) {
  N_ENSURE(type_.k() == nebula::type::DoubleType::kind, "double type expected");

  constexpr size_t size = nebula::type::DoubleType::width;
  data_->add(cursorAndAdvance(), d);

  INCREMENT_RAW_SIZE_AND_RETURN()
}

size_t DataNode::append(const std::string& str) {
  N_ENSURE(type_.k() == nebula::type::StringType::kind, "string type expected");

  const size_t size = str.size();
  const auto index = cursorAndAdvance();

  data_->add(index, str);
  meta_->setOffsetSize(index, size);

  INCREMENT_RAW_SIZE_AND_RETURN()
}

#define DISPATCH_KIND(KIND, lambda, object, func)                          \
  case Kind::KIND: {                                                       \
    lambda = [&object, &list](auto i) { return object->append(func(i)); }; \
    break;                                                                 \
  }

template <>
size_t DataNode::append(const nebula::surface::ListData& list) {
  N_ENSURE(type_.k() == Kind::ARRAY, "list/array type expected");
  const auto& child = this->childAt<PDataNode>(0).value();
  const auto items = list.getItems();
  const auto kind = child->type_.k();
  size_t size = 0;

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
  const auto index = cursorAndAdvance();
  meta_->setOffsetSize(index, items);
  INCREMENT_RAW_SIZE_AND_RETURN()
}

#undef DISPATCH_KIND

template <>
size_t DataNode::append(const nebula::surface::MapData& map) {
  LOG(INFO) << "append a map";
  N_ENSURE(type_.k() == Kind::MAP, "map type expected");
  const auto& key = this->childAt<PDataNode>(0).value();
  const auto& value = this->childAt<PDataNode>(1).value();

  const auto entries = map.getItems();
  const auto keyKind = key->type_.k();
  const auto valueKind = value->type_.k();
  uint32_t size = 0;

  auto keys = map.readKeys();
  size += key->append<const ListData&>(*keys);
  auto values = map.readValues();
  size += value->append<const ListData&>(*values);

  // return raw size just added to current map
  meta_->setOffsetSize(cursorAndAdvance(), entries);
  INCREMENT_RAW_SIZE_AND_RETURN()
}

#define DISPATCH_KIND(size, KIND, object, value) \
  case Kind::KIND: {                             \
    size += object->append(value);               \
    break;                                       \
  }

template <>
size_t DataNode::append(const nebula::surface::RowData& row) {
  // TODO(cao): NULL row is not supported.
  // Need to modify if we want to support row/struct column type in the future
  N_ENSURE(type_.k() == Kind::STRUCT, "struct type expected");

  // fill all results - introduce iterator in tree to ease the loops?
  size_t size = 0;
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
      size += child->append<const ListData&>(*list);
      break;
    }
    case Kind::MAP: {
      auto map = row.readMap(name);
      size += child->append<const MapData&>(*map);
      break;
    }
    default:
      throw NebulaException(fmt::format("Not supported type: {0}", name));
    }
  }

  // return total raw size of the data added in this node
  INCREMENT_RAW_SIZE_AND_RETURN()
}

#undef DISPATCH_KIND

#undef INCREMENT_RAW_SIZE_AND_RETURN

///////////////////////////////////////////////////////////////////////////////////////////////////

#define TYPE_READ_DELEGATE(TYPE)      \
  template <>                         \
  TYPE DataNode::read(size_t index) { \
    return data_->read<TYPE>(index);  \
  }

TYPE_READ_DELEGATE(bool)
TYPE_READ_DELEGATE(int8_t)
TYPE_READ_DELEGATE(int16_t)
TYPE_READ_DELEGATE(int32_t)
TYPE_READ_DELEGATE(int64_t)
TYPE_READ_DELEGATE(float)
TYPE_READ_DELEGATE(double)

template <>
std::string DataNode::read(size_t index) {
  auto os = meta_->offsetSize(index);
  return data_->read(std::get<0>(os), std::get<1>(os));
}

#undef TYPE_READ_DELEGATE

} // namespace memory
} // namespace nebula