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

#include <leveldb/cache.h>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include "common/Evidence.h"
#include "meta/MetaDb.h"
#include "storage/NFS.h"

namespace nebula {
namespace service {
namespace base {

// Db is already common name rather than abbr.
class NativeMetaDb final : public nebula::meta::MetaDb {
public:
  // local: local path for the native DB
  // remote: the remote persist location for backup
  NativeMetaDb(const std::string&, size_t = 0);
  ~NativeMetaDb();

public:
  // read a given key, if not found, return false
  // otherwise the object will be filled with the bytes.
  virtual inline bool read(const std::string& key, std::string& value) const override {
    auto status = db_->Get(ropts_, key, &value);
    return status.ok();
  }

  // write a k-v into the db
  virtual bool write(const std::string& key, const std::string& value) override;

  // backup db data into remote location - called by async scheduler
  virtual bool backup() noexcept override;

  // close DB and move its data to backup folder
  virtual void close() noexcept override;

  inline const std::string& local() const {
    return local_;
  }

  inline const std::string& remote() const {
    return remote_;
  }

private:
  // restore data from remote store
  void restore() noexcept;

  // open DB with a local store
  void open();

private:
  std::string local_;
  std::string remote_;
  // local file system
  std::unique_ptr<nebula::storage::NFileSystem> fs_;
  bool dirty_;
  std::string backup_;

  // leveldb related options
  leveldb::Options options_;
  leveldb::DB* db_;
  leveldb::ReadOptions ropts_;
  leveldb::WriteOptions wopts_;

  // db backup related properties
  // timestamp used for check point for now
  nebula::common::Evidence::Duration tick_;
};

} // namespace base
} // namespace service
} // namespace nebula