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

#include "Batch.h"
#include <numeric>

DEFINE_int32(BESS_PAGE_SIZE, 1024, "page size for bess encoded data");

namespace nebula {
namespace memory {

using nebula::meta::BessType;
using nebula::meta::Table;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TreeBase;
using nebula::type::TypeBase;

Batch::Batch(const Table& table, size_t capacity, size_t pid)
  : schema_{ table.schema() },
    data_{ DataNode::buildDataTree(table, capacity) },
    pod_{ table.pod() },
    pid_{ pid },
    bess_{ pod_ != nullptr ? (size_t)FLAGS_BESS_PAGE_SIZE : 0 },
    rows_{ 0 },
    fields_{ schema_->size() },
    sealed_{ false } {
  // build a field name to data node
  for (size_t i = 0, size = schema_->size(); i < size; ++i) {
    auto f = dynamic_cast<TypeBase*>(schema_->TreeBase::childAt(i).get());
    fields_[f->name()] = data_->childAt<PDataNode>(i).value();
  }

  // if current batch belongs to a pod, then we can decode spaces for each dimensions
  if (pod_ != nullptr) {
    VLOG(1) << "Locate spaces for given pod: " << pid_;
    spaces_ = pod_->locate(pid_);
    bessBits_ = pod_->bessBits();
  }
}

// add a row into current batch
// and return row ID of this row in current batch
// thread-safe on sync guarded - exclusive lock?
size_t Batch::add(const RowData& row, BessType bess) {
  N_ENSURE(!sealed_, "can not add rows into sealed batch");
  // bess is already calculated by caller just write it out
  // TODO(cao): compress bess value which should be very small
  // width = sum(bits width of each dimension)
  if (pod_ != nullptr) {
    bess_.writeBits(rows_ * bessBits_, bessBits_, bess);
  }

  // read data from row data and save it to batch
  auto result = data_->append<const RowData&>(row);

  // record the row size
  VLOG(1) << "Total row size  = " << result;

  return rows_++;
}

// random access to a row - may require internal seek
std::unique_ptr<RowAccessor> Batch::makeAccessor() const {
  return std::make_unique<RowAccessor>(const_cast<const Batch&>(*this));
}

std::string Batch::state() const {
  // tree walk the whole data tree to collect all storage size
  // raw size is already accumulated during writing path
  // storage size is dynamic depending on adopted encoders
  // SizeMeta: size, allocation
  using SizeMeta = std::tuple<size_t, size_t>;
  auto s = data_->walk<SizeMeta, DataNode>(
    {},
    [](const DataNode& v, const std::vector<SizeMeta>& children) {
      size_t allocation = v.storageAllocation();
      size_t size = v.storageSize();
      for (const auto& c : children) {
        allocation += std::get<0>(c);
        size += std::get<1>(c);
      }

      return std::make_tuple(allocation, size);
    });

  // TODO(cao): output a JSON string
  return fmt::format("[raw: {0}, size: {1}, allocation: {2}, rows: {3}, bess: {4}]",
                     data_->rawSize(), std::get<1>(s), std::get<0>(s), rows_, bess_.size());
}

void Batch::seal() {
  N_ENSURE(!sealed_, "batch is already sealed.");
  sealed_ = true;

  // seal every node
  data_->seal();

  // seal bess as well
  if (pod_) {
    auto bits = (rows_ * bessBits_);
    bess_.seal(bits / 8 + 1);
  }
}

} // namespace memory
} // namespace nebula