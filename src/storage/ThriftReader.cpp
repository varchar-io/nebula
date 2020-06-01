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

#include "ThriftReader.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <unordered_map>
#include <unordered_set>

#include "common/Evidence.h"
#include "common/Likely.h"

/**
 * Thrift reader to parse payload into a row object
 */
namespace nebula {
namespace storage {

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TCompactProtocol;
using apache::thrift::protocol::TType;
using apache::thrift::transport::TMemoryBuffer;
using nebula::common::Evidence;
using nebula::memory::FlatRow;
using nebula::meta::Table;

constexpr auto LEVEL = 1'000;

void readStruct(uint64_t base,
                TBinaryProtocol& proto,
                std::unordered_map<uint32_t, std::string>& fields,
                std::unordered_set<uint32_t>& fieldsWritten,
                FlatRow& row) {
  // field name?
  std::string name;

  while (true) {
    TType type = apache::thrift::protocol::T_STOP;
    int16_t id = 0;

    proto.readFieldBegin(name, type, id);

    // no more fields to read
    if (id == 0) {
      proto.skip(type);
      proto.readFieldEnd();
      break;
    }

    // adjustment based on base
    auto levelId = id + base;

    // look up which column name this is for
    // TODO(cao): I don't know why the same ID will hit twice here
    // let use the first one before figuring out the reason
    auto w = fieldsWritten.find(levelId);
    auto f = fields.find(levelId);

    // if this is not written yet and it's a desired field
    if (w == fieldsWritten.end() && f != fields.end()) {
      auto name = f->second;

      // time field special handling
      if (UNLIKELY(name == Table::TIME_COLUMN)) {
        int64_t time = Evidence::unix_timestamp();
        proto.readI64(time);
        row.write(name, Evidence::to_seconds(time));
        fieldsWritten.emplace(levelId);
        proto.readFieldEnd();
        continue;
      }

#define TYPE_EXTRACT(T, CT, M)      \
  case TType::T: {                  \
    CT v;                           \
    proto.M(v);                     \
    row.write(name, v);             \
    fieldsWritten.emplace(levelId); \
    break;                          \
  }

      switch (type) {
        TYPE_EXTRACT(T_STRING, std::string, readBinary)
        TYPE_EXTRACT(T_BOOL, bool, readBool)
        TYPE_EXTRACT(T_BYTE, int8_t, readByte)
        TYPE_EXTRACT(T_I16, int16_t, readI16)
        TYPE_EXTRACT(T_I32, int32_t, readI32)
        TYPE_EXTRACT(T_I64, int64_t, readI64)
        TYPE_EXTRACT(T_DOUBLE, double, readDouble)
      case TType::T_LIST: {
        // TODO(cao): we only support size of list for now
        TType elemType = apache::thrift::protocol::T_STOP;
        uint32_t listSize;
        proto.readListBegin(elemType, listSize);
        row.write(name, listSize);
        fieldsWritten.emplace(levelId);
        proto.readListEnd();
        break;
      }
      default: {
        VLOG(1) << "Type not supported at field: " << levelId << " with type=" << type;
        proto.skip(type);
        break;
      }
      }
#undef TYPE_EXTRACT
    } else if (type == TType::T_STRUCT) {
      // support simple nesting
      proto.readStructBegin(name);
      // recursively
      readStruct(levelId * LEVEL, proto, fields, fieldsWritten, row);
      proto.readStructEnd();
    } else {
      proto.skip(type);
    }

    proto.readFieldEnd();
  }
}

bool ThriftRow::parse(void* buf, size_t size, nebula::memory::FlatRow& row) noexcept {
  auto buffer = std::make_shared<TMemoryBuffer>(static_cast<uint8_t*>(buf), size);
  TBinaryProtocol proto(buffer);

  // read all fields
  const auto numFields = fields_.size();
  std::unordered_set<uint32_t> fieldsWritten;
  fieldsWritten.reserve(numFields);

  // TODO(cao): ID path hack, every time it enter into a new level, it times 10K to get next field ID
  // current field;
  readStruct(0, proto, fields_, fieldsWritten, row);

  // in case anything happened, not all fields found from this message
  const auto numWritten = fieldsWritten.size();
  if (UNLIKELY(numWritten < numFields)) {
    for (auto itr = fields_.cbegin(); itr != fields_.cend(); ++itr) {
      if (fieldsWritten.find(itr->first) == fieldsWritten.end()) {
        row.writeNull(itr->second);
      }
    }
  }

  return true;
}

} // namespace storage
} // namespace nebula