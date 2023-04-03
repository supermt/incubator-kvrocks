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

#include <memory>
#include <utility>

#include "db_util.h"
#include "event_util.h"
#include "fmt/format.h"
#include "io_util.h"
#include "slot_migrate.h"
#include "storage/batch_extractor.h"
#include "storage/compact_filter.h"
#include "storage/table_properties_collector.h"
#include "thread_util.h"
#include "time_util.h"
#include "types/redis_stream_base.h"
#include "types/redis_string.h"

Status CompactAndMergeMigrate::SendSnapshot() {
  // This is the function to compact and merge the data
  Status s;
  s = PickSSTs();

  // Ask the writable DB instance to do the compaction
  auto options = storage_->GetDB()->GetOptions();
  rocksdb::CompactionOptions co;
  co.compression = options.compression;
  co.max_subcompactions = svr_->GetConfig()->max_bg_migration;
  storage_->GetDB()->PauseBackgroundWork();
  auto rocks_s = storage_->GetDB()->CompactFiles(co, GetSubkeyCFH(), subkey_compact_sst, options.num_levels - 1, -1,
                                                 &compact_results);

  rocks_s = storage_->GetDB()->CompactFiles(co, GetMetadataCFH(), meta_compact_sst, options.num_levels - 1, -1,
                                            &compact_results);
  storage_->GetDB()->ContinueBackgroundWork();
  if (!rocks_s.ok()) {
    return {Status::NotOK, rocks_s.ToString()};
  }
  if (!s.IsOK()) {
    return s;
  }
  s = SendRemoteSST();
  if (!s.IsOK()) {
    return {s.GetCode(), "Send SST to remote failed, due to: " + s.Msg()};
  }
  return Status::OK();
}

Status CompactAndMergeMigrate::SendRemoteSST() {
  // sending files through socket, we need to first set the connection into blocking mode
  Status s;
  s = Util::SockSetBlocking(slot_job_->slot_fd_, 1);
  if (!s.IsOK()) {
    return s;
  }
  for (auto &file_name : compact_results) {
    uint64_t file_size = 0;
    auto fd = UniqueFD(OpenDataFile(file_name, &file_size));
    if (*fd == -1) {
      return {Status::NotOK, "Failed to open the file: " + file_name};
    }
    s = Util::SockSendFile(slot_job_->slot_fd_, *fd, file_size);
    if (!s.IsOK()) {
      return s;
    } else {
      LOG(INFO) << "[Compact-and-merge] Send File Success, File Name: " << file_name << " file size (bytes) "
                << file_size;
    }
  }

  return s;
}
void CompactAndMergeMigrate::CreateCFHandles() {
  auto options = storage_->GetDB()->GetOptions();
  rocksdb::ColumnFamilyOptions metadata_opts(options);
  rocksdb::BlockBasedTableOptions metadata_table_opts = storage_->InitTableOptions();
  metadata_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(metadata_table_opts));
  metadata_opts.compaction_filter_factory = std::make_shared<Engine::MetadataFilterFactory>(svr_->storage_);
  metadata_opts.disable_auto_compactions = svr_->GetConfig()->RocksDB.disable_auto_compactions;
  // Enable whole key bloom filter in memtable
  metadata_opts.memtable_whole_key_filtering = true;
  metadata_opts.memtable_prefix_bloom_size_ratio = 0.1;
  metadata_opts.table_properties_collector_factories.emplace_back(
      NewCompactOnExpiredTableCollectorFactory(Engine::kMetadataColumnFamilyName, 0.3));
  svr_->storage_->SetBlobDB(&metadata_opts);

  rocksdb::BlockBasedTableOptions subkey_table_opts = storage_->InitTableOptions();

  subkey_table_opts.block_cache =
      rocksdb::NewLRUCache(svr_->GetConfig()->RocksDB.subkey_block_cache_size * MiB, -1, false, 0.75);
  subkey_table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
  subkey_table_opts.cache_index_and_filter_blocks = false;
  subkey_table_opts.cache_index_and_filter_blocks_with_high_priority = true;
  rocksdb::ColumnFamilyOptions subkey_opts(options);
  subkey_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(subkey_table_opts));
  subkey_opts.compaction_filter_factory = std::make_shared<Engine::SubKeyFilterFactory>(svr_->storage_);
  subkey_opts.disable_auto_compactions = svr_->GetConfig()->RocksDB.disable_auto_compactions;
  subkey_opts.table_properties_collector_factories.emplace_back(
      NewCompactOnExpiredTableCollectorFactory(Engine::kSubkeyColumnFamilyName, 0.3));
  svr_->storage_->SetBlobDB(&subkey_opts);

  cf_desc_.emplace_back(rocksdb::kDefaultColumnFamilyName, subkey_opts);
  cf_desc_.emplace_back(Engine::kMetadataColumnFamilyName, metadata_opts);
}
CompactAndMergeMigrate::CompactAndMergeMigrate(Server *svr, int migration_speed, int pipeline_size_limit, int seq_gap,
                                               bool batched)
    : SlotMigrate(svr, migration_speed, pipeline_size_limit, seq_gap, batched) {
  this->CreateCFHandles();
  auto options = svr->storage_->GetDB()->GetOptions();
  rocksdb::Status s =
      rocksdb::DB::OpenForReadOnly(options, svr_->GetConfig()->db_dir, cf_desc_, &cf_handles_, &compact_ptr);
  assert(s.ok());
}
rocksdb::ColumnFamilyHandle *CompactAndMergeMigrate::GetMetadataCFH() { return cf_handles_[1]; }
rocksdb::ColumnFamilyHandle *CompactAndMergeMigrate::GetSubkeyCFH() { return cf_handles_[0]; }
Status CompactAndMergeMigrate::SetMigrationSlots(std::vector<int> &target_slots) {
  if (!migrate_slots_.empty() || this->IsMigrationInProgress()) {
    return {Status::NotOK, "Last Migrate Batch is not finished"};
  }
  migrate_slots_.clear();
  migrate_slots_.insert(migrate_slots_.end(), target_slots.begin(), target_slots.end());
  return Status::OK();
}
Status CompactAndMergeMigrate::MigrateStart(Server *svr, const std::string &node_id, const std::string &dst_ip,
                                            int dst_port, int seq_gap, bool join) {
  migrate_state_ = kMigrateStarted;
  dst_node_ = node_id;
  auto job = std::make_unique<SlotMigrateJob>(migrate_slots_, dst_ip, dst_port, 0, 16, seq_gap);
  LOG(INFO) << "[migrate] Start migrating slots, from slot: " << migrate_slots_.front()
            << " to slot: " << migrate_slots_.back() << ". Slots are moving to" << dst_ip << ":" << dst_port;
  {
    std::lock_guard<std::mutex> guard(job_mutex_);
    slot_job_ = std::move(job);
    job_cv_.notify_one();
  }
  return Status::OK();
}

inline int compare_with_prefix(const std::string &x, rocksdb::Slice &prefix) {
  rocksdb::Slice x_slice(x);
  return memcmp(x_slice.data_, prefix.data_, prefix.size_);
}
inline std::vector<std::string> UniqueVector(std::vector<std::string> &input) {
  std::sort(input.begin(), input.end());
  auto pos = std::unique(input.begin(), input.end());
  input.erase(pos, input.end());
  return input;
}
Status CompactAndMergeMigrate::PickSSTs() {
  std::vector<rocksdb::Slice> slot_prefix_list;
  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  storage_->SetReadOptions(read_options);
  rocksdb::ColumnFamilyHandle *meta_cf_handle = GetMetadataCFH();
  //  auto iter = DBUtil::UniqueIterator(storage_->GetDB()->NewIterator(read_options, meta_cf_handle));
  for (int slot : migrate_slots_) {
    std::string prefix;
    ComposeSlotKeyPrefix(namespace_, slot, &prefix);
    slot_prefix_list.emplace_back(prefix);
  }
  rocksdb::ColumnFamilyMetaData metacf_ssts;
  compact_ptr->GetColumnFamilyMetaData(meta_cf_handle, &metacf_ssts);

  for (const auto &level_stat : metacf_ssts.levels) {
    for (const auto &sst_info : level_stat.files) {
      for (auto prefix : slot_prefix_list) {
        if (compare_with_prefix(sst_info.smallestkey, prefix) < 0 &&
            compare_with_prefix(sst_info.largestkey, prefix) > 0)
          meta_compact_sst.push_back(sst_info.relative_filename);
      }
    }
  }

  rocksdb::ColumnFamilyMetaData subkeycf_ssts;
  compact_ptr->GetColumnFamilyMetaData(GetSubkeyCFH(), &subkeycf_ssts);
  for (const auto &level_stat : subkeycf_ssts.levels) {
    for (const auto &sst_info : level_stat.files) {
      subkey_compact_sst.push_back(sst_info.relative_filename);
    }
  }
  // Erase redundant SSTs
  meta_compact_sst = UniqueVector(meta_compact_sst);
  subkey_compact_sst = UniqueVector(subkey_compact_sst);

  return Status::OK();
}
