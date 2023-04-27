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
  storage_->GetDB()->PauseBackgroundWork();
  s = PickSSTs();
  if (!s.IsOK()) {
    return s;
  }
  // Ask the writable DB instance to do the compaction
  auto options = storage_->GetDB()->GetOptions();
  rocksdb::CompactionOptions co;
  co.compression = options.compression;
  co.max_subcompactions = svr_->GetConfig()->max_bg_migration;
  if (meta_compact_sst_.size() > 0) {
    auto rocks_s = storage_->GetDB()->CompactFiles(co, GetMetadataCFH(), meta_compact_sst_, options.num_levels - 1, -1,
                                                   &compact_results);
    if (!rocks_s.ok()) {
      std::string file_str = "[";
      for (const auto &file : compact_results) {
        file_str = file_str + file + ",";
      }
      file_str.pop_back();
      file_str += "]";
      LOG(ERROR) << "Compaction Failed, meta file list:" << file_str;
      return {Status::NotOK, rocks_s.ToString()};
    }
    s = SendRemoteSST(compact_results, Engine::kMetadataColumnFamilyName);
    if (!s.IsOK()) {
      return {s.GetCode(), "Send SST to remote failed, due to: " + s.Msg()};
    }
  }

  compact_results.clear();
  if (subkey_compact_sst_.size() > 0) {
    auto rocks_s = storage_->GetDB()->CompactFiles(co, GetSubkeyCFH(), subkey_compact_sst_, options.num_levels - 1, -1,
                                                   &compact_results);

    if (!rocks_s.ok()) {
      std::string file_str = "[";
      for (const auto &file : compact_results) {
        file_str = file_str + file + ",";
      }
      file_str.pop_back();
      file_str += "]";
      LOG(ERROR) << "Compaction Failed, meta file list:" << file_str;
      return {Status::NotOK, rocks_s.ToString()};
    }
    s = SendRemoteSST(compact_results, Engine::kSubkeyColumnFamilyName);
    if (!s.IsOK()) {
      return {s.GetCode(), "Send SST to remote failed, due to: " + s.Msg()};
      return s;
    }
  }
  storage_->GetDB()->ContinueBackgroundWork();

  return Status::OK();
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
CompactAndMergeMigrate::CompactAndMergeMigrate(Server *svr, int migration_speed, int pipeline_size_limit, int seq_gap)
    : SlotMigrate(svr, migration_speed, pipeline_size_limit, seq_gap) {
  this->batched_ = true;
}
rocksdb::ColumnFamilyHandle *CompactAndMergeMigrate::GetMetadataCFH() {
  return svr_->storage_->GetCFHandle(Engine::kMetadataColumnFamilyName);
}
rocksdb::ColumnFamilyHandle *CompactAndMergeMigrate::GetSubkeyCFH() {
  return svr_->storage_->GetCFHandle(Engine::kSubkeyColumnFamilyName);
}
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

  for (int slot : migrate_slots_) {
    std::string prefix;
    ComposeSlotKeyPrefix(namespace_, slot, &prefix);
    slot_prefix_list_.push_back(prefix);

    //    std::cout << "Slot prefix: " << prefix << " hex:" << Slice(prefix).ToString(true) << std::endl;
    subkey_prefix_list_.emplace_back(ExtractSubkeyPrefix(Slice(slot_prefix_list_.back())));
    //    std::cout << "Slot prefix: " << subkey_prefix_list_.back()
    //              << " hex:" << Slice(subkey_prefix_list_.back()).ToString(true) << std::endl;
  }

  auto job = std::make_unique<SlotMigrateJob>(migrate_slots_, dst_ip, dst_port, 0, 16, seq_gap);
  LOG(INFO) << "[migrate] Start migrating slots, from slot: " << migrate_slots_.front()
            << " to slot: " << migrate_slots_.back() << ". Slots are moving to " << dst_ip << ":" << dst_port;
  {
    std::lock_guard<std::mutex> guard(job_mutex_);
    slot_job_ = std::move(job);
    job_cv_.notify_one();
  }
  return Status::OK();
}

Status CompactAndMergeMigrate::PickSSTs() {
  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  storage_->SetReadOptions(read_options);
  rocksdb::ColumnFamilyHandle *meta_cf_handle = GetMetadataCFH();
  //  auto iter = DBUtil::UniqueIterator(storage_->GetDB()->NewIterator(read_options, meta_cf_handle));
  rocksdb::ColumnFamilyMetaData metacf_ssts;
  svr_->storage_->GetDB()->GetColumnFamilyMetaData(meta_cf_handle, &metacf_ssts);
  std::cout << "Picking SSTs" << std::endl;

  for (const auto &meta_level_stat : metacf_ssts.levels) {
    for (const auto &meta_sst : meta_level_stat.files) {
      for (Slice prefix : slot_prefix_list_) {
        if (compare_with_prefix(meta_sst.smallestkey, prefix) < 0 &&
            compare_with_prefix(meta_sst.largestkey, prefix) > 0) {
          std::cout << Slice(meta_sst.smallestkey).ToString(true) << " vs. " << prefix.ToString(true) << std::endl;
          meta_compact_sst_.push_back(meta_sst.name);
        }
      }
    }
  }

  rocksdb::ColumnFamilyMetaData subkeycf_ssts;
  svr_->storage_->GetDB()->GetColumnFamilyMetaData(GetSubkeyCFH(), &subkeycf_ssts);
  for (const auto &level_stat : subkeycf_ssts.levels) {
    for (const auto &sst_info : level_stat.files) {
      for (Slice prefix : subkey_prefix_list_) {
        std::cout << Slice(sst_info.smallestkey).ToString(true) << " vs. " << prefix.ToString(true)
                  << " result: " << compare_with_prefix(sst_info.smallestkey, prefix) << std::endl;

        if (compare_with_prefix(sst_info.smallestkey, prefix) < 0 &&
            compare_with_prefix(sst_info.largestkey, prefix) > 0)
          subkey_compact_sst_.push_back(sst_info.name);
      }
    }
  }
  // Erase redundant SSTs
  meta_compact_sst_ = UniqueVector(meta_compact_sst_);
  subkey_compact_sst_ = UniqueVector(subkey_compact_sst_);
  std::string file_str;
  file_str = "meta list: [ ";
  for (auto sst : meta_compact_sst_) {
    file_str = file_str + sst + ',';
  }
  file_str.pop_back();
  file_str += "], subkey list: [ ";
  for (auto sst : subkey_compact_sst_) {
    file_str = file_str + sst + ',';
  }
  file_str.pop_back();
  file_str += "]";
  LOG(INFO) << "Collected SSTables " << file_str;

  return Status::OK();
}
std::string CompactAndMergeMigrate::ExtractSubkeyPrefix(const Slice &slot_prefix) {
  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  storage_->SetReadOptions(read_options);
  auto iter = DBUtil::UniqueIterator(svr_->storage_->GetDB()->NewIterator(read_options, GetMetadataCFH()));

  iter->Seek(slot_prefix);
  Slice first_slot_key = iter->key();
  std::string ns, user_key;
  ExtractNamespaceKey(first_slot_key, &ns, &user_key, true);

  std::string slot_key, prefix_subkey;
  AppendNamespacePrefix(user_key, &slot_key);
  std::string bytes = iter->value().ToString();
  Metadata metadata(kRedisNone, false);
  metadata.Decode(bytes);

  InternalKey(slot_key, "", metadata.version, true).Encode(&prefix_subkey);
  return prefix_subkey;
}
Status CompactAndMergeMigrate::SendRemoteSST(std::vector<std::string> &file_list,
                                             const std::string &column_family_name) {
  Status s;
  std::string file_str;

  for (auto &file_name : file_list) {
    std::string rel_path = Util::Split(file_name, "/").back();
    file_str = file_str + rel_path + ",";
  }
  file_str.pop_back();
  std::string cmds;
  cmds =
      Redis::MultiBulkString({"sst_ingest", "remote", column_family_name, file_str, svr_->cluster_->GetMyId()}, false);
  auto fd = Util::SockConnect(dst_ip_, dst_port_);

  if (!fd.IsOK()) {
    return fd;
  }
  //  s = Util::SockSetBlocking(*fd, 1);
  //  if (!s.IsOK()) {
  //    return s;
  //  }
  s = Util::SockSend(*fd, cmds);
  if (!s.IsOK()) {
    return s.Prefixed("Failed to send command");
  }
  s = CheckResponseOnce(*fd);
  if (!s.IsOK()) {
    return s.Prefixed("Error in receiving ingestion results");
  }

  LOG(INFO) << "[" << this->GetName() << "] Send File Success, total # of files: " << file_list.size()
            << ", file_list: " << file_str;
  return s;
}
