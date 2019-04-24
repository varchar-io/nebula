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

#include <glog/logging.h>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <unordered_map>
#include "execution/BlockManager.h"
#include "memory/Batch.h"
#include "meta/NBlock.h"
#include "meta/Table.h"
#include "nebula.grpc.pb.h"
#include "storage/CsvReader.h"
#include "type/Serde.h"

/**
 * A cursor template that help iterating a container.
 * (should we use std::iterator instead?)
 */
namespace nebula {
namespace service {
class TrendsTable : public nebula::meta::Table {
public:
  TrendsTable() : Table("pin.trends") {
    // TODO(cao) - let's make date as a number
    schema_ = nebula::type::TypeSerializer::from("ROW<query:string, date:string, count:long>");
  }
  virtual ~TrendsTable() = default;
};

// build for specific product such as trends
class V1ServiceImpl final : public V1::Service {
  grpc::Status State(grpc::ServerContext*, const TableStateRequest*, TableStateResponse*);
  TrendsTable table_;

public:
  void loadTrends();
};

} // namespace service
} // namespace nebula