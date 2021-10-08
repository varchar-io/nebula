/*
 * Copyright 2017-present varchar.io
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

#include <fstream>
#include <iostream>
#include <rapidjson/istreamwrapper.h>
#include <string>

#include "JsonRow.h"
#include "surface/DataSurface.h"

/**
 * A JSON file reader, the expected file is list of json objects separated by new line.
 * It reads line by line as string, and let JSON parse each line as object
 */
namespace nebula {
namespace storage {

// this reader will assume every line of given file is a row object
// so it's called line json reader, means it will open the file
// and read line by line to ingest each row
class LineJsonReader : public nebula::surface::RowCursor {
  static constexpr size_t SLICE_SIZE = 1024;

public:
  LineJsonReader(
    const std::string& file,
    const nebula::meta::JsonProps& props,
    nebula::type::Schema schema,
    bool nullDefault = true)
    : nebula::surface::RowCursor(0),
      fstream_{ file },
      json_{ schema, props.columnsMap, nullDefault },
      row_{ SLICE_SIZE } {

    // read first line to initialize cursor state
    if (std::getline(fstream_, line_)) {
      ++size_;
    }
  }
  virtual ~LineJsonReader() = default;

public:
  // next row data of JsonRow
  virtual const nebula::surface::RowData& next() override {
    index_++;

    // set it up before returning to client to consume
    row_.reset();
    json_.parse(line_.data(), line_.size(), row_);

    // read next row, if true, then it has data
    if (std::getline(fstream_, line_)) {
      ++size_;
    }

    return row_;
  }

  virtual std::unique_ptr<nebula::surface::RowData> item(size_t) const override {
    throw NException("stream-based JSON Reader does not support random access.");
  }

private:
  std::ifstream fstream_;
  JsonRow json_;
  nebula::memory::FlatRow row_;
  std::string line_;
};

// Object json reader instead will parse the whole file as an JSON object
// it is either an array, or it is an object
class ObjectJsonReader : public nebula::surface::RowCursor {
  static constexpr auto ROOT_ROWS = "[ROOT]";
  static constexpr size_t SLICE_SIZE = 1024;

public:
  ObjectJsonReader(
    const std::string& file,
    const nebula::meta::JsonProps& props,
    nebula::type::Schema schema,
    bool nullDefault = true)
    : nebula::surface::RowCursor(0),
      json_{ schema, props.columnsMap, nullDefault },
      row_{ SLICE_SIZE },
      array_{ nullptr } {
    const auto& rf = props.rowsField;
    N_ENSURE(rf.size() > 0, "rows field has to be set.");

    // parse the file into json object
    std::ifstream ifs{ file };
    rapidjson::IStreamWrapper isw(ifs);
    if (doc_.ParseStream(isw).HasParseError()) {
      throw NException("Failed to parse the json document.");
    }

    if (rf == ROOT_ROWS) {
      array_ = &doc_;
    } else {
      auto root = doc_.GetObject();
      array_ = locate(root, nebula::common::Chars::path(rf.data(), rf.size()));
    }
    N_ENSURE((array_ != nullptr && array_->IsArray()), "rows field should be array");

    // size is array length
    size_ = array_->GetArray().Size();
  }
  virtual ~ObjectJsonReader() = default;

public:
  // next row data of JsonRow
  virtual const nebula::surface::RowData& next() override {
    // set it up before returning to client to consume
    row_.reset();
    auto item = (*array_)[index_++].GetObject();
    json_.fill(item, row_);
    return row_;
  }

  virtual std::unique_ptr<nebula::surface::RowData> item(size_t) const override {
    throw NException("stream-based JSON Reader does not support random access.");
  }

private:
  JsonRow json_;
  nebula::memory::FlatRow row_;

  // json doc and the array pointer
  rapidjson::Document doc_;
  rapidjson::Value* array_;
};

std::unique_ptr<nebula::surface::RowCursor> makeJsonReader(
  const std::string& file,
  const nebula::meta::JsonProps& props,
  nebula::type::Schema schema,
  bool nullDefault = true) {
  // empty rows field - every line of the file is a row object in json
  if (props.rowsField.size() == 0) {
    return std::make_unique<LineJsonReader>(file, props, schema, nullDefault);
  } else {
    return std::make_unique<ObjectJsonReader>(file, props, schema, nullDefault);
  }
}

} // namespace storage
} // namespace nebula