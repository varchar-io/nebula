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
#include "common/Evidence.h"

namespace nebula {
namespace surface {

////////////////////////////////////////////////// MOCK DATA //////////////////////////////////////////////////
class MockData {
public:
  MockData(size_t seed = 0)
    : rand_{ nebula::common::Evidence::rand(
      seed == 0 ? nebula::common::Evidence::ticks() : seed) } {}
  virtual ~MockData() = default;

protected:
  std::function<double()> rand_;
};

// Supported compound types
class MockRowData : public RowData, protected MockData {
public:
  MockRowData(size_t seed = 0) : MockData(seed), seed_{ seed } {
    strings_.reserve(32);
    for (auto i = 0; i < 32; ++i) {
      strings_.push_back(std::string(rand_() * 10, 'N'));
    }
  }

public:
  virtual bool isNull(const std::string& field) const override;
  virtual bool readBool(const std::string& field) const override;
  virtual int8_t readByte(const std::string& field) const override;
  virtual int16_t readShort(const std::string& field) const override;
  virtual int32_t readInt(const std::string& field) const override;
  virtual int64_t readLong(const std::string& field) const override;
  virtual float readFloat(const std::string& field) const override;
  virtual double readDouble(const std::string& field) const override;
  virtual int128_t readInt128(const std::string& field) const override;
  virtual std::string_view readString(const std::string& field) const override;

  // compound types
  virtual std::unique_ptr<ListData> readList(const std::string& field) const override;
  virtual std::unique_ptr<MapData> readMap(const std::string& field) const override;

  virtual bool isNull(IndexType index) const override;
  virtual bool readBool(IndexType index) const override;
  virtual int8_t readByte(IndexType index) const override;
  virtual int16_t readShort(IndexType index) const override;
  virtual int32_t readInt(IndexType index) const override;
  virtual int64_t readLong(IndexType index) const override;
  virtual float readFloat(IndexType index) const override;
  virtual double readDouble(IndexType index) const override;
  virtual int128_t readInt128(IndexType index) const override;
  virtual std::string_view readString(IndexType index) const override;

  // compound types
  virtual std::unique_ptr<ListData> readList(IndexType index) const override;
  virtual std::unique_ptr<MapData> readMap(IndexType index) const override;

private:
  size_t seed_;
  std::vector<std::string> strings_;
};

class MockRowCursor : public RowCursor {
public:
  MockRowCursor(size_t size = 8) : RowCursor(size) {
  }

  virtual const RowData& next() override {
    index_++;
    return rowData_;
  }

  virtual std::unique_ptr<RowData> item(size_t) const override {
    throw NException("not support item");
  }

private:
  MockRowData rowData_;
};

class MockListData : public ListData, protected MockData {
public:
  MockListData(IndexType items, size_t seed = 0) : ListData(items), MockData(seed) {
    strings_.reserve(32);
    for (auto i = 0; i < 32; ++i) {
      strings_.push_back(std::string(rand_() * 10, 'N'));
    }
  }
  bool isNull(IndexType index) const override;
  bool readBool(IndexType index) const override;
  std::int8_t readByte(IndexType index) const override;
  int16_t readShort(IndexType index) const override;
  int32_t readInt(IndexType index) const override;
  int64_t readLong(IndexType index) const override;
  float readFloat(IndexType index) const override;
  double readDouble(IndexType index) const override;
  int128_t readInt128(IndexType index) const override;
  std::string_view readString(IndexType index) const override;

private:
  std::vector<std::string> strings_;
};

class MockMapData : public MapData, protected MockData {
public:
  MockMapData(IndexType items, size_t seed = 0) : MapData(items), MockData(seed) {}
  std::unique_ptr<ListData> readKeys() const override;
  std::unique_ptr<ListData> readValues() const override;
};

// Supported compound types
class MockAccessor : public Accessor, protected MockData {
public:
  MockAccessor(size_t seed = 0) : MockData(seed) {
    strings_.reserve(32);
    for (auto i = 0; i < 32; ++i) {
      strings_.push_back(std::string(rand_() * 10, 'N'));
    }
  }

public:
  virtual std::optional<bool> readBool(const std::string& field) const override;
  virtual std::optional<int8_t> readByte(const std::string& field) const override;
  virtual std::optional<int16_t> readShort(const std::string& field) const override;
  virtual std::optional<int32_t> readInt(const std::string& field) const override;
  virtual std::optional<int64_t> readLong(const std::string& field) const override;
  virtual std::optional<float> readFloat(const std::string& field) const override;
  virtual std::optional<double> readDouble(const std::string& field) const override;
  virtual std::optional<int128_t> readInt128(const std::string& field) const override;
  virtual std::optional<std::string_view> readString(const std::string& field) const override;

private:
  std::vector<std::string> strings_;
};

} // namespace surface
} // namespace nebula