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
#include "rocksdb/sst_file_reader.h"
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
    auto start = Util::GetTimeStampMS();

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
    auto end = Util::GetTimeStampMS();
    LOG(INFO) << "Meta SST compacted, Time cost (ms): " << end - start;
    std::vector<std::string> filtered_sst;

    s = FilterMetaSSTs(compact_results, &filtered_sst);
    if (!s.IsOK()) {
      return s;
    }
    s = SendRemoteSST(filtered_sst, Engine::kMetadataColumnFamilyName);
    if (!s.IsOK()) {
      return {s.GetCode(), "Send SST to remote failed, due to: " + s.Msg()};
    }
  }

  compact_results.clear();
  if (subkey_compact_sst_.size() > 0) {
    auto start = Util::GetTimeStampMS();
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

    auto end = Util::GetTimeStampMS();
    LOG(INFO) << "Subkey SST compacted, Time cost (ms): " << end - start;

    std::vector<std::string> filtered_sst;
    s = FilterSubkeySSTs(compact_results, &filtered_sst);
    if (!s.IsOK()) {
      return s;
    }
    s = SendRemoteSST(filtered_sst, Engine::kSubkeyColumnFamilyName);
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
    subkey_prefix_list_.emplace_back(ExtractSubkeyPrefix(Slice(slot_prefix_list_.back())));
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

  for (const auto &meta_level_stat : metacf_ssts.levels) {
    for (const auto &meta_sst : meta_level_stat.files) {
      for (Slice prefix : slot_prefix_list_) {
        if (compare_with_prefix(meta_sst.smallestkey, prefix) < 0 &&
            compare_with_prefix(meta_sst.largestkey, prefix) > 0) {
          meta_compact_sst_.push_back(meta_sst.name);
        }
      }
    }
  }

  rocksdb::ColumnFamilyMetaData subkeycf_ssts;
  // sort the prefix to avoid repeat searching
  std::sort(subkey_prefix_list_.begin(), subkey_prefix_list_.end());

  svr_->storage_->GetDB()->GetColumnFamilyMetaData(GetSubkeyCFH(), &subkeycf_ssts);
  for (const auto &level_stat : subkeycf_ssts.levels) {
    for (const auto &sst_info : level_stat.files) {
      for (Slice prefix : subkey_prefix_list_) {
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
  std::string source_ssts;
  std::string file_str;
  for (auto &file_name : file_list) {
    source_ssts += ((file_name) + " ");
    file_str += (Util::Split(file_name, "/").back() + ",");
  }
  source_ssts.pop_back();
  file_str.pop_back();
  LOG(INFO) << "Sending files: " << source_ssts;

  switch (svr_->GetConfig()->sst_transport_method) {
    case kNetwork: {
      std::string cmds;
      std::string file_str;

      for (auto &file_name : file_list) {
        std::string rel_path = Util::Split(file_name, "/").back();
        file_str = file_str + rel_path + ",";
      }
      file_str.pop_back();
      cmds = Redis::MultiBulkString({"sst_ingest", "remote", column_family_name, file_str, svr_->cluster_->GetMyId()},
                                    false);

      auto fd = Util::SockConnect(dst_ip_, dst_port_);
      if (!fd.IsOK()) {
        return fd;
      }

      s = Util::SockSend(*fd, cmds);

      if (!s.IsOK()) {
        return s;
      }
      s = CheckResponseOnce(*fd);
      if (!s.IsOK()) {
        return s;
      }

      return Status::OK();
    }
    case kSCP: {
      std::string cmds;

      auto source_space = svr_->GetConfig()->global_migration_sync_dir + "/" + std::to_string(svr_->GetConfig()->port);
      auto target_space = svr_->GetConfig()->global_migration_sync_dir + "/" + std::to_string(dst_port_);
      cmds = "ls " + source_ssts + " |xargs -n 1 basename| parallel -v -j8 rsync -raz " + source_space + "/{} " +
             svr_->GetConfig()->migration_user + "@" + dst_ip_ + ":" + target_space + "/{}";
      LOG(INFO) << cmds;
      int status = system(cmds.c_str());
      if (status < 0) {
        LOG(ERROR) << "Rsync send file error: " << strerror(errno) << '\n';
        return {Status::NotOK, fmt::format("Rsync send file error: {}", strerror(errno))};
      }

      cmds = Redis::MultiBulkString({"sst_ingest", "local", column_family_name, file_str, svr_->cluster_->GetMyId()},
                                    false);

      auto fd = Util::SockConnect(dst_ip_, dst_port_);

      if (!fd.IsOK()) {
        return fd;
      }

      s = Util::SockSend(*fd, cmds);

      if (!s.IsOK()) {
        return s;
      }
      s = CheckResponseOnce(*fd);
      if (!s.IsOK()) {
        return s;
      }
      return Status::OK();
    }

    default:
      return {Status::NotOK,
              "File transportation is invalid" + std::to_string(svr_->GetConfig()->sst_transport_method)};
  }
}
Status CompactAndMergeMigrate::FilterMetaSSTs(const std::vector<std::string> &input_list,
                                              std::vector<std::string> *output_list) {
  auto opt = svr_->storage_->GetDB()->GetOptions();
  rocksdb::SstFileReader sst_file_reader(opt);
  rocksdb::ReadOptions ropts;
  rocksdb::SstFileWriter writer(rocksdb::EnvOptions(opt), opt, GetMetadataCFH());
  uint64_t valid = 0;
  auto start = Util::GetTimeStampMS();

  for (auto &file : input_list) {
    // Create SST reader
    auto ros = sst_file_reader.Open(file);  // rocksdb open status;
    if (!ros.ok()) {
      return {Status::NotOK, "failed on reading" + ros.ToString()};
    }
    auto sst_name = Util::Split(file, "/").back();

    std::string out_name = "";
    switch (svr_->GetConfig()->sst_transport_method) {
      case kNetwork: {
        out_name = file + ".bck";
        break;
      }
      case kSCP: {
        out_name =
            svr_->GetConfig()->global_migration_sync_dir + std::to_string(svr_->GetConfig()->port) + "/" + sst_name;
        break;
      }
      default:
        return {Status::NotOK, "Unknown file transmission"};
    }

    output_list->push_back(out_name);
    LOG(INFO) << "Start filtering, target file: " << out_name;
    ros = writer.Open(out_name);
    if (!ros.ok()) {
      return {Status::NotOK, "failed on writing" + ros.ToString()};
    }
    std::unique_ptr<rocksdb::Iterator> iter(sst_file_reader.NewIterator(ropts));
    iter->SeekToFirst();
    for (; iter->Valid(); iter->Next()) {
      // meta family, with specific prefix
      //      for (const auto &prefix : slot_prefix_list_) {
      //        if (iter->key().starts_with(prefix)) {
      writer.Put(iter->key(), iter->value());
      //          valid++;
      //        }
      //      }
    }
    ros = writer.Finish();
    if (!ros.ok()) {
      return {Status::NotOK, "Write out error, " + ros.ToString()};
    }
    // end of reading file
  }
  auto end = Util::GetTimeStampMS();
  LOG(INFO) << "Meta SST filtered, # of valid entries: " << valid << ", time taken (ms): " << end - start;
  return Status::OK();
}
Status CompactAndMergeMigrate::FilterSubkeySSTs(const std::vector<std::string> &input_list,
                                                std::vector<std::string> *output_list) {
  auto opt = svr_->storage_->GetDB()->GetOptions();
  rocksdb::SstFileReader sst_file_reader(opt);
  rocksdb::ReadOptions ropts;
  rocksdb::SstFileWriter writer(rocksdb::EnvOptions(opt), opt);
  uint64_t valid = 0;
  auto start = Util::GetTimeStampMS();

  for (auto file : input_list) {
    auto ros = sst_file_reader.Open(file);  // rocksdb open status;
    if (!ros.ok()) {
      return {Status::NotOK, "failed on reading" + ros.ToString()};
    }

    auto sst_name = Util::Split(file, "/").back();

    std::string out_name = "";
    switch (svr_->GetConfig()->sst_transport_method) {
      case kNetwork: {
        out_name = file + ".bck";
        break;
      }
      case kSCP: {
        out_name =
            svr_->GetConfig()->global_migration_sync_dir + std::to_string(svr_->GetConfig()->port) + "/" + sst_name;
        break;
      }
      default:
        return {Status::NotOK, "Unknown file transmission"};
    }

    output_list->push_back(out_name);
    ros = writer.Open(out_name);

    std::unique_ptr<rocksdb::Iterator> iter(sst_file_reader.NewIterator(ropts));
    iter->SeekToFirst();
    auto smallest = iter->key();
    std::string smallest_prefix;
    // find the smallest prefix that is larger than this smallest value;
    for (const auto &prefix : subkey_prefix_list_) {
      if (compare_with_prefix(smallest, prefix) < 0) {
        smallest_prefix = prefix;
        break;
      }
    }
    for (; iter->Valid(); iter->Next()) {
      //      auto cur_key = iter->key();
      //      if (compare_with_prefix(cur_key, smallest_prefix) < 0) {
      //        // it's in the range
      //        valid++;
      //        writer.Put(iter->key(), iter->value());
      //      }
      writer.Put(iter->key(), iter->value());
    }  // end of entry scanning

    ros = writer.Finish();
    if (!ros.ok()) {
      return {Status::NotOK, "Write out error, " + ros.ToString()};
    }
  }
  auto end = Util::GetTimeStampMS();
  LOG(INFO) << "Subkey SSTs filtered, # of valid entries: " << valid << ", time taken (ms): " << end - start;
  return Status::OK();
}
