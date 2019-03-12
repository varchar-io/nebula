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

#include "TypeData.h"

// initialize data page for each type with heuristic assumptions.
// this can be optimized in the future when the system can learn different category's data statistics.
// it can be allocated by external metadata hints.
DEFINE_int32(META_PAGE_SIZE, 4 * 1024, "page size for meta data");
DEFINE_int32(BOOL_PAGE_SIZE, 1024, "BOOL data page size");
DEFINE_int32(BYTE_PAGE_SIZE, 2 * 1024, "BYTE data page size");
DEFINE_int32(SHORT_PAGE_SIZE, 2 * 1024, "SHORT data page size");
DEFINE_int32(INT_PAGE_SIZE, 4 * 1024, "int data page size");
DEFINE_int32(LONG_PAGE_SIZE, 8 * 1024, "long data page size");
DEFINE_int32(REAL_PAGE_SIZE, 8 * 1024, "real data page size");
DEFINE_int32(BINARY_PAGE_SIZE, 32 * 1024, "string or bytes data page size");
DEFINE_int32(EMPTY_PAGE_SIZE, 8, "for compability only - compound types do not have data");

namespace nebula {
namespace memory {
namespace serde {

#define TYPE_DATA_CONSTR(TYPE, SLICE_PAGE) \
  template <>                              \
  TYPE::TypeDataImpl() : slice_{ (size_t)SLICE_PAGE } {}

TYPE_DATA_CONSTR(BoolData, FLAGS_BOOL_PAGE_SIZE)
TYPE_DATA_CONSTR(ByteData, FLAGS_BYTE_PAGE_SIZE)
TYPE_DATA_CONSTR(ShortData, FLAGS_SHORT_PAGE_SIZE)
TYPE_DATA_CONSTR(IntData, FLAGS_INT_PAGE_SIZE)
TYPE_DATA_CONSTR(LongData, FLAGS_LONG_PAGE_SIZE)
TYPE_DATA_CONSTR(FloatData, FLAGS_REAL_PAGE_SIZE)
TYPE_DATA_CONSTR(DoubleData, FLAGS_REAL_PAGE_SIZE)
TYPE_DATA_CONSTR(StringData, FLAGS_BINARY_PAGE_SIZE)
TYPE_DATA_CONSTR(EmptyData, FLAGS_EMPTY_PAGE_SIZE)

#undef TYPE_DATA_CONSTR

// TODO(cao) - avoid is specific to current uncompressed flat layout - it is not needed with encoder
// which means, encoder should be able to handle position seeking and holes skipping

#define TYPE_ADD_VOID(TYPE, value)       \
  template <>                            \
  void TYPE::addVoid(IndexType index) {  \
    size_ += slice_.write(size_, value); \
  }

TYPE_ADD_VOID(BoolData, false)
TYPE_ADD_VOID(ByteData, (int8_t)0);
TYPE_ADD_VOID(ShortData, (int16_t)0);
TYPE_ADD_VOID(IntData, (int32_t)0);
TYPE_ADD_VOID(LongData, (int64_t)0);
TYPE_ADD_VOID(FloatData, (float)0);
TYPE_ADD_VOID(DoubleData, (double)0);

// string void data
template <>
void StringData::addVoid(IndexType index) {
  size_ += slice_.write(size_, "", 0);
}

#undef TYPE_ADD_VOID

///////////////////////////////////////////////////////////////////////////////////////////////////

template <>
template <>
void BoolData::add(IndexType index, bool value) {
  // TODO(cao): raw data, need to plug in encoder
  size_ += slice_.write(size_, value);
}

template <>
template <>
void ByteData::add(IndexType index, int8_t value) {
  size_ += slice_.write(size_, value);
}

template <>
template <>
void ShortData::add(IndexType index, int16_t value) {
  size_ += slice_.write(size_, value);
}

template <>
template <>
void IntData::add(IndexType index, int32_t value) {
  size_ += slice_.write(size_, value);
}

template <>
template <>
void LongData::add(IndexType index, int64_t value) {
  size_ += slice_.write(size_, value);
}

template <>
template <>
void FloatData::add(IndexType index, float value) {
  size_ += slice_.write(size_, value);
}

template <>
template <>
void DoubleData::add(IndexType index, double value) {
  size_ += slice_.write(size_, value);
}

template <>
template <>
void StringData::add(IndexType index, const std::string& value) {
  size_ += slice_.write(size_, value.data(), value.size());
}

///////////////////////////////////////////////////////////////////////////////////////////////////
template <>
template <>
inline bool BoolData::read(IndexType index) {
  // TODO(cao): raw data, need to plug in encoder
  // for flat memory block, data read offset = index * (data item width)
  // for encoders, it has different meta data way to figure out the data offset.
  return slice_.read<bool>(index);
}

template <>
template <>
inline int8_t ByteData::read(IndexType index) {
  return slice_.read<int8_t>(index);
}

template <>
template <>
inline int16_t ShortData::read(IndexType index) {
  return slice_.read<int16_t>(index << 1);
}

template <>
template <>
inline int32_t IntData::read(IndexType index) {
  return slice_.read<int32_t>(index << 2);
}

template <>
template <>
inline int64_t LongData::read(IndexType index) {
  return slice_.read<int64_t>(index << 3);
}

template <>
template <>
inline float FloatData::read(IndexType index) {
  return slice_.read<float>(index << 2);
}

template <>
template <>
inline double DoubleData::read(IndexType index) {
  return slice_.read<double>(index << 3);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#define HOOK_CONCRETE(TYPE, obj)          \
  if ((obj = dynamic_cast<TYPE*>(raw))) { \
    void_ = [this](IndexType index) {     \
      obj->addVoid(index);                \
    };                                    \
  }

// TODO(cao) - revisit: seems proxy is unncessary if every type can be templated
TypeDataProxy::TypeDataProxy(PTypeData typeData)
  : data_{ std::move(typeData) }, void_{} {
  // perf reason: pay the cost of virtual function once
  // the pointer can be any type -
  // it would be great if base class can infer sub-class's template type??
  auto raw = data_.get();
  HOOK_CONCRETE(BoolData, bd_)
  HOOK_CONCRETE(ByteData, btd_)
  HOOK_CONCRETE(ShortData, sd_)
  HOOK_CONCRETE(IntData, id_)
  HOOK_CONCRETE(LongData, ld_)
  HOOK_CONCRETE(FloatData, fd_)
  HOOK_CONCRETE(DoubleData, dd_)
  HOOK_CONCRETE(StringData, std_)

  // void_ could be null if current data proxy is created for compound
  // N_ENSURE(void_ != nullptr, "type data is not registered.");
}
#undef HOOK_CONCRETE

#define TYPE_ADD_PROXY(TYPE, OBJ)                        \
  template <>                                            \
  void TypeDataProxy::add(IndexType index, TYPE value) { \
    OBJ->add<TYPE>(index, value);                        \
  }

TYPE_ADD_PROXY(bool, bd_)
TYPE_ADD_PROXY(int8_t, btd_)
TYPE_ADD_PROXY(int16_t, sd_)
TYPE_ADD_PROXY(int32_t, id_)
TYPE_ADD_PROXY(int64_t, ld_)
TYPE_ADD_PROXY(float, fd_)
TYPE_ADD_PROXY(double, dd_)

void TypeDataProxy::add(IndexType index, const std::string& value) {
  std_->add<const std::string&>(index, value);
}

#undef TYPE_PROXY

#define TYPE_READ_PROXY(TYPE, OBJ)            \
  template <>                                 \
  TYPE TypeDataProxy::read(IndexType index) { \
    return OBJ->read<TYPE>(index);            \
  }

TYPE_READ_PROXY(bool, bd_)
TYPE_READ_PROXY(int8_t, btd_)
TYPE_READ_PROXY(int16_t, sd_)
TYPE_READ_PROXY(int32_t, id_)
TYPE_READ_PROXY(int64_t, ld_)
TYPE_READ_PROXY(float, fd_)
TYPE_READ_PROXY(double, dd_)

#undef TYPE_READ_PROXY

} // namespace serde
} // namespace memory
} // namespace nebula