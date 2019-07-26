/// Copyright 2016 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.


#include "rocksdb_admin/application_db_manager.h"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "folly/String.h"
#include "glog/logging.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"


DEFINE_string(rocksdb_dir, "/tmp/",
    "The dir for local rocksdb instances");

namespace admin {

const int kRemoveDBRefWaitMilliSec = 200;

rocksdb::DB* OpenMetaDB() {
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB* db;
  auto s = rocksdb::DB::Open(options, FLAGS_rocksdb_dir + "meta_db", &db);
  CHECK(s.ok()) << "Failed to open meta DB"
                << " with error " << s.ToString();

  return db;
}

ApplicationDBManager::ApplicationDBManager()
    : dbs_(), meta_db_(OpenMetaDB()), dbs_lock_() {}

bool ApplicationDBManager::clearMetaData(const std::string& db_name) {
  rocksdb::WriteOptions options;
  options.sync = true;
  auto s = meta_db_->Delete(options, db_name);
  return s.ok();
}

bool ApplicationDBManager::writeMetaData(
    const std::string& db_name, const std::string& s3_bucket,
    const std::string& s3_path, const int64_t last_kafka_msg_timestamp_ms) {
  DBMetaData meta;
  meta.db_name = db_name;
  meta.set_s3_bucket(s3_bucket);
  meta.set_s3_path(s3_path);
  meta.set_last_kafka_msg_timestamp_ms(last_kafka_msg_timestamp_ms);

  std::string buffer;
  apache::thrift::CompactSerializer::serialize(meta, &buffer);

  rocksdb::WriteOptions options;
  options.sync = true;
  auto s = meta_db_->Put(options, db_name, buffer);
  return s.ok();
}

DBMetaData ApplicationDBManager::getMetaData(const std::string& db_name) {
  DBMetaData meta;
  meta.db_name = db_name;

  std::string buffer;
  rocksdb::ReadOptions options;
  auto s = meta_db_->Get(options, db_name, &buffer);
  if (s.ok()) {
    apache::thrift::CompactSerializer::deserialize(buffer, meta);
  }

  return meta;
}

bool ApplicationDBManager::addDB(const std::string& db_name,
                                 std::unique_ptr<rocksdb::DB> db,
                                 replicator::DBRole role,
                                 std::string* error_message) {
  return addDB(db_name, std::move(db), role, nullptr, error_message);
}

bool ApplicationDBManager::addDB(const std::string& db_name,
                                 std::unique_ptr<rocksdb::DB> db,
                                 replicator::DBRole role,
                                 std::unique_ptr<folly::SocketAddress> up_addr,
                                 std::string* error_message) {
  std::unique_lock<std::shared_mutex> lock(dbs_lock_);
  if (dbs_.find(db_name) != dbs_.end()) {
    if (error_message) {
      *error_message = db_name + " has already been added";
    }
    return false;
  }
  auto rocksdb_ptr = std::shared_ptr<rocksdb::DB>(db.release(),
    [](rocksdb::DB* db){});
  auto application_db_ptr = std::make_shared<ApplicationDB>(db_name,
    std::move(rocksdb_ptr), role, std::move(up_addr));

  dbs_.emplace(db_name, std::move(application_db_ptr));
  return true;
}

const std::shared_ptr<ApplicationDB> ApplicationDBManager::getDB(
    const std::string& db_name,
    std::string* error_message) {
  std::shared_lock<std::shared_mutex> lock(dbs_lock_);
  auto itor = dbs_.find(db_name);
  if (itor == dbs_.end()) {
    if (error_message) {
      *error_message = db_name + " does not exist";
    }
    return nullptr;
  }
  return itor -> second;
}

std::unique_ptr<rocksdb::DB> ApplicationDBManager::removeDB(
    const std::string& db_name,
    std::string* error_message) {
  std::shared_ptr<ApplicationDB> ret;

  {
    std::unique_lock<std::shared_mutex> lock(dbs_lock_);
    auto itor = dbs_.find(db_name);
    if (itor == dbs_.end()) {
      if (error_message) {
        *error_message = db_name + " does not exist";
      }
      return nullptr;
    }
  
    ret = std::move(itor->second);
    dbs_.erase(itor);
  }

  waitOnApplicationDBRef(ret);
  return std::unique_ptr<rocksdb::DB>(ret->db_.get());
}

std::string ApplicationDBManager::DumpDBStatsAsText() {
    std::unordered_map<std::string, std::shared_ptr<ApplicationDB>> dbs_copy;
  {
    std::shared_lock<std::shared_mutex> lock(dbs_lock_);
    dbs_copy = dbs_;
  }

  std::string stats;
  // Add stats for DB size
  // total_sst_file_size db=abc00001: 12345
  // total_sst_file_size db=abc00002: 54321
  uint64_t sz;
  for (const auto& itor : dbs_copy) {
    auto db_name = itor.first;
    auto db = itor.second;
    if (!db->rocksdb()->GetIntProperty(
          rocksdb::DB::Properties::kTotalSstFilesSize, &sz)) {
      LOG(ERROR) << "Failed to get kTotalSstFilesSize for " << db->db_name();
      sz = 0;
    }

    stats += folly::stringPrintf("  total_sst_file_size db=%s: %" PRIu64 "\n",
                                 db->db_name().c_str(), sz);

    DBMetaData meta = this->getMetaData(db_name);
    if (meta.last_kafka_msg_timestamp_ms > 0) {
        stats += folly::stringPrintf(" last_kafka_msg_timestamp_ms db=%s " PRIu64 "\n",
                                     db->db_name().c_str(), meta.last_kafka_msg_timestamp_ms);

    }
  }

  return stats;
}

std::vector<std::string> ApplicationDBManager::getAllDBNames()  {
    std::vector<std::string> db_names;
    std::shared_lock<std::shared_mutex> lock(dbs_lock_);
    db_names.reserve(dbs_.size());
    for (const auto& db : dbs_) {
        db_names.push_back(db.first);
    }
    return db_names;
}

ApplicationDBManager::~ApplicationDBManager() {
  auto itor = dbs_.begin();
  while (itor != dbs_.end()) {
    waitOnApplicationDBRef(itor->second);
    // we want to first remove the ApplicationDB and then release the RocksDB
    // it contains.
    auto tmp = std::unique_ptr<rocksdb::DB>(itor->second->db_.get());
    itor = dbs_.erase(itor);
  }
}

void ApplicationDBManager::waitOnApplicationDBRef(
    const std::shared_ptr<ApplicationDB>& db) {
  while (db.use_count() > 1) {
    LOG(INFO) << db->db_name() << " is still holding by others, wait "
      << kRemoveDBRefWaitMilliSec << " milliseconds";
    std::this_thread::sleep_for(
      std::chrono::milliseconds(kRemoveDBRefWaitMilliSec));
  }
}

}  // namespace admin
