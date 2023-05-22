/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "config.h"
#include "status.h"
#include "storage/batch_extractor.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
class LatestSnapShot {
 public:
  explicit LatestSnapShot(rocksdb::DB *db) : db_(db), snapshot_(db_->GetSnapshot()) {}
  ~LatestSnapShot() { db_->ReleaseSnapshot(snapshot_); }

  LatestSnapShot(const LatestSnapShot &) = delete;
  LatestSnapShot &operator=(const LatestSnapShot &) = delete;

  const rocksdb::Snapshot *GetSnapShot() { return snapshot_; }

 private:
  rocksdb::DB *db_ = nullptr;
  const rocksdb::Snapshot *snapshot_ = nullptr;
};

class Parser {
 public:
  explicit Parser(Engine::Storage *storage, std::vector<int> &slot_list, std::string &output_dir,
                  std::string &namespace_str, MigrationAgent::Config &config)
      : storage_(storage),
        slot_id_encoded_(storage_->IsSlotIdEncoded()),
        output_dir_(output_dir),
        namespace_(namespace_str),
        config_(config) {
    latest_snapshot_ = std::make_unique<LatestSnapShot>(storage->GetDB());
    for (auto slot : slot_list) {
      slot_list_.emplace(slot);
    }
    meta_cf_handle_ = storage_->GetCFHandle(Engine::kMetadataColumnFamilyName);
    subkey_cf_handle_ = storage_->GetCFHandle(Engine::kSubkeyColumnFamilyName);
  }
  ~Parser() = default;

  Status SeekAndDump();
  Status ParseFullDB();
  Status ParseWriteBatch(const std::string &batch_string);

  Status CompactAndMerge();

 protected:
  Engine::Storage *storage_ = nullptr;
  std::unique_ptr<LatestSnapShot> latest_snapshot_;
  bool slot_id_encoded_ = false;
  std::string output_dir_;

  std::set<int> slot_list_;
  std::vector<rocksdb::SstFileWriter *> meta_files_;
  std::vector<rocksdb::SstFileWriter *> subkey_files_;
  Slice namespace_;

  MigrationAgent::Config config_;
  Status parseSimpleKV(const Slice &ns_key, const Slice &value, int expire);
  Status DumpSimpleKV(const Slice &ns_key, const Slice &value, int expire);
  Status DumpComplexKV(const Slice &ns_key, const Metadata &metadata, const Slice &meta_value);
  Status parseComplexKV(const Slice &ns_key, const Metadata &metadata);
  Status parseBitmapSegment(const Slice &ns, const Slice &user_key, int index, const Slice &bitmap);

  rocksdb::ColumnFamilyHandle *meta_cf_handle_;
  rocksdb::ColumnFamilyHandle *subkey_cf_handle_;
};
