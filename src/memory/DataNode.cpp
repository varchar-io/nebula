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

#include "common/Hash.h"
#include "common/Likely.h"
#include "type/Type.h"

/**
 * A data node holds real memory data for each node in the schema tree
 * 
 */
namespace nebula {
namespace memory {

using nebula::common::Hasher;
using nebula::memory::serde::TypeMetadata;
using nebula::meta::Table;
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
DataTree DataNode::buildDataTree(const Table& table, size_t capacity) {
  // traverse the whole schema tree to generate a data tree
  auto schema = table.schema();
  auto dataTree = schema->walk<TreeNode>(
    {},
    [&table, capacity](const auto& v, const std::vector<TreeNode>& children) {
      const auto& t = dynamic_cast<const TypeBase&>(v);
      return TreeNode(new DataNode(t, table.column(t.name()), capacity, children));
    });

  return std::static_pointer_cast<DataNode>(dataTree);
}

#define INCREMENT_RAW_SIZE_AND_RETURN() \
  rawSize_ += size;                     \
  return size;

size_t DataNode::appendNull() {
  // constexpr static auto NULL_TUPLE = std::make_tuple(0, NULL_SIZE);
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

#define APPEND_SOLID_VALUE(K, N)                                          \
  template <>                                                             \
  size_t DataNode::append(nebula::type::TypeTraits<Kind::K>::CppType v) { \
    N_ENSURE(type_.k() == Kind::K, #N "type expected");                   \
    constexpr size_t size = nebula::type::Type<Kind::K>::width;           \
    data_->add(cursorAndAdvance(), v);                                    \
    meta_->histogram(v);                                                  \
    rawSize_ += size;                                                     \
    return size;                                                          \
  }

APPEND_SOLID_VALUE(BOOLEAN, bool)
APPEND_SOLID_VALUE(TINYINT, byte)
APPEND_SOLID_VALUE(SMALLINT, short)
APPEND_SOLID_VALUE(INTEGER, int)
APPEND_SOLID_VALUE(BIGINT, long)
APPEND_SOLID_VALUE(REAL, float)
APPEND_SOLID_VALUE(DOUBLE, double)

#undef APPEND_SOLID_VALUE

template <>
size_t DataNode::append(int128_t i128) {
  N_ENSURE(type_.k() == nebula::type::Int128Type::kind, "int128 type expected");

  constexpr size_t size = nebula::type::Int128Type::width;
  data_->add(cursorAndAdvance(), i128);

  // histogram
  meta_->histogram(i128);

  INCREMENT_RAW_SIZE_AND_RETURN()
}

template <>
size_t DataNode::append(std::string_view str) {
  N_ENSURE(type_.k() == nebula::type::StringType::kind, "string type expected");
  // if current data node is enabled with dictionary.
  // we check the dictionary to see if the same value already present
  // if yes, we save the mapping.
  const size_t size = str.size();
  const auto index = cursorAndAdvance();

  // histogram
  meta_->histogram(str);

  if (meta_->hasDict()) {
    auto dictIdx = meta_->dictItem(str);
    meta_->setOffsetSize(index, dictIdx);
    INCREMENT_RAW_SIZE_AND_RETURN()
  }

  // this new value will be appended at the data chunk
  // and we record its offset and size
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
    DISPATCH_KIND(INT128, lambda, child, list.readInt128)
    DISPATCH_KIND(VARCHAR, lambda, child, list.readString)
  default:
    throw NException(fmt::format("Not supported type: {0}", TypeBase::kname(kind)));
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

  // histogram recording
  meta_->histogram(nullptr);

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
  uint32_t size = 0;

  auto keys = map.readKeys();
  size += key->append<const ListData&>(*keys);
  auto values = map.readValues();
  size += value->append<const ListData&>(*values);

  // return raw size just added to current map
  meta_->setOffsetSize(cursorAndAdvance(), entries);

  // histogram recording
  meta_->histogram(nullptr);
  INCREMENT_RAW_SIZE_AND_RETURN()
}

// if current column is a partition column
// we save its value into meta and skip writing data stream
#define DISPATCH_KIND(size, KIND, object, value) \
  case Kind::KIND: {                             \
    if (!object->meta_->isPartition()) {         \
      size += object->append(value);             \
    }                                            \
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
      DISPATCH_KIND(size, INT128, child, row.readInt128(name))
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
      throw NException(fmt::format("Not supported type: {0}", name));
    }
  }

  // histogram recording
  meta_->histogram(nullptr);

  // return total raw size of the data added in this node
  INCREMENT_RAW_SIZE_AND_RETURN()
}

#undef DISPATCH_KIND

#undef INCREMENT_RAW_SIZE_AND_RETURN

///////////////////////////////////////////////////////////////////////////////////////////////////

#define TYPE_READ_DELEGATE(TYPE)              \
  template <>                                 \
  TYPE DataNode::read(size_t index) {         \
    if (UNLIKELY(meta_->isRealNull(index))) { \
      return data_->defaultValue<TYPE>();     \
    }                                         \
    return data_->read<TYPE>(index);          \
  }

TYPE_READ_DELEGATE(bool)
TYPE_READ_DELEGATE(int8_t)
TYPE_READ_DELEGATE(int16_t)
TYPE_READ_DELEGATE(int32_t)
TYPE_READ_DELEGATE(int64_t)
TYPE_READ_DELEGATE(float)
TYPE_READ_DELEGATE(double)
TYPE_READ_DELEGATE(int128_t)

template <>
std::string_view DataNode::read(size_t index) {
  if (meta_->isRealNull(index)) {
    return data_->defaultValue<std::string_view>();
  }

  // if with dictionary, we need to check
  // whether this points to another index so using offsetSize
  // instead of offsetSizeDirect
  if (meta_->hasDict()) {
    auto os = meta_->offsetSize(index);
    return meta_->dictItem(os.second);
  }

  auto os = meta_->offsetSize(index);
  return data_->read(os.first, os.second);
}

#undef TYPE_READ_DELEGATE

} // namespace memory
} // namespace nebula