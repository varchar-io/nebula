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

#include "NativeMetaDb.h"

#include <fstream>
#include <gflags/gflags.h>

#include "common/Errors.h"

// default 100mb memory cache for the meta db
DEFINE_uint64(DB_CACHE_SIZE, 100 * 1048576, "memory cache size of the db.");
DEFINE_int64(DB_BACKUP_INTERVAL, 5 * 60000, "time interval milliseconds to backup the DB");

/**
 * provide common data operations for service
 */
namespace nebula {
namespace service {
namespace base {

void NativeMetaDb::open() {
  leveldb::Status status = leveldb::DB::Open(options_, local_, &db_);
  N_ENSURE(status.ok(), "Failed to open native meta db");
}

void NativeMetaDb::restore() noexcept {
  auto localFs = nebula::storage::makeFS("local", "");
  auto uriInfo = nebula::storage::parse(remote_);
  if (uriInfo.schema == "s3") {
    // sync all remote data into a local folder
    auto fs = nebula::storage::makeFS("s3", uriInfo.host);

    // find latest version indicated by the version file
    const auto versionKey = fmt::format("{0}/version", uriInfo.path);
    char version[32];
    auto size = fs->read(versionKey, version, sizeof(version));
    if (size > 0) {
      // if existing the version, we download it to local to start with
      auto snapshot = fmt::format("{0}/{1}", uriInfo.path, std::string_view(version, size));
      if (fs->sync(snapshot, local_)) {
        LOG(INFO) << "Reuse metadb saved at: " << snapshot;
      }
    }

    return;
  }

  // supporting other store type as well
  LOG(WARNING) << "Unsupported store location: " << remote_;
}

// local: local path for the native DB
// remote: the remote persist location for backup
NativeMetaDb::NativeMetaDb(
  const std::string& remote,
  size_t cacheSize)
  : remote_{ remote },
    fs_{ nebula::storage::makeFS("local") },
    dirty_{ false } {
  // if cache size is not provided by method call, use the default one
  if (cacheSize == 0) {
    cacheSize = FLAGS_DB_CACHE_SIZE;
  }

  // create a local store
  local_ = fs_->temp(true);

  // create a backup folder (temp folder)
  if (!remote_.empty()) {
    resetBackup();

    // try to restore local_ from remote_
    restore();
  }

  // open the local db
  options_.create_if_missing = true;
  options_.block_cache = leveldb::NewLRUCache(cacheSize);
  open();
}

NativeMetaDb::~NativeMetaDb() {
  // we want this destroy order so not using smart pointers
  if (db_) {
    delete db_;
  }

  if (options_.block_cache) {
    delete options_.block_cache;
  }

  // do one more time backup if dirty data available since last sync
  backup();
}

bool NativeMetaDb::write(const std::string& key, const std::string& value) {
  auto status = db_->Put(wopts_, key, value);
  auto ok = status.ok();
  if (ok) {
    // any successful write will cause the db dirty (in terms of in sync with backup)
    if (remote_.size() > 0 && tick_.elapsedMs() > FLAGS_DB_BACKUP_INTERVAL) {
      // close db
      close();

      // reopen the same db
      open();

      // reset tick
      tick_.reset();
    }
  }

  return ok;
}

bool NativeMetaDb::backup() noexcept {
  // upload data from backup_ to remote_
  if (dirty_) {
    dirty_ = false;
    auto uriInfo = nebula::storage::parse(remote_);
    if (uriInfo.schema == "s3") {
      // sync all remote data into a local folder
      auto fs = nebula::storage::makeFS("s3", uriInfo.host);

      // generate a new unique snapshot
      auto version = std::to_string(nebula::common::Evidence::unix_timestamp());
      auto snapshot = fmt::format("{0}/{1}", uriInfo.path, version);

      // failed to upload this snapshot
      if (!fs->sync(backup_, snapshot)) {
        return false;
      }

      // write this version into version file
      auto versionFile = fmt::format("{0}/version", backup_);
      auto remoteVersion = fmt::format("{0}/version", uriInfo.path);
      std::ofstream ofs;
      ofs.open(versionFile, std::ofstream::out);
      ofs << version;
      ofs.close();

      // upload this file into S3
      auto versionUpdated = fs->copy(versionFile, remoteVersion);

      // clean backup folder for next sync
      resetBackup();
      return versionUpdated;
    }

    // not supported location
    LOG(WARNING) << "Unsupported backup location: " << remote_;
  }

  return false;
}

void NativeMetaDb::resetBackup() {
  if (fs_->list(backup_).size() != 0) {
    fs_->rm(backup_);
    backup_ = fs_->temp(true);
  }
}

void NativeMetaDb::close() noexcept {
  // shut db and copy data to backup folder
  // and kick off async upload process
  // reopen the DB
  if (db_) {
    delete db_;
    db_ = nullptr;
  }

  if (!backup_.empty()) {
    // indicate we can upload backup_ now
    resetBackup();
    dirty_ = fs_->sync(local_, backup_);
    LOG(INFO) << "Prepare local=" << local_ << " to backup=" << backup_ << ", ready=" << dirty_;
  }
}

} // namespace base
} // namespace service
} // namespace nebula