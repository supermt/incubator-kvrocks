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
#include "rocksdb/convenience.h"
#include "slot_migrate.h"
#include "storage/batch_extractor.h"
#include "storage/compact_filter.h"
#include "storage/table_properties_collector.h"
#include "thread_util.h"
#include "time_util.h"
#include "types/redis_stream_base.h"
#include "types/redis_string.h"

LevelMigrate::LevelMigrate(Server *svr, int migration_speed, int pipeline_size_limit, int seq_gap)
    : CompactAndMergeMigrate(svr, migration_speed, pipeline_size_limit, seq_gap) {
  meta_cf_handle_ = storage_->GetCFHandle(Engine::kMetadataColumnFamilyName);
  subkey_cf_handle_ = storage_->GetCFHandle(Engine::kSubkeyColumnFamilyName);
  this->batched_ = true;
}

Status LevelMigrate::SendSnapshot() {
  auto start = Util::GetTimeStampUS();
  rocksdb::CancelAllBackgroundWork(storage_->GetDB(), false);  // wait for current compaction to finish
  auto end = Util::GetTimeStampUS();
  LOG(INFO) << "Wait BG flush job for: " << end - start << " us" << std::endl;
  storage_->GetDB()->PauseBackgroundWork();
  auto src_config = svr_->GetConfig();
  std::string src_info = "127.0.0.1:" + std::to_string(src_config->port) + "@" + src_config->db_dir;
  std::string dst_info =
      dst_ip_ + ":" + std::to_string(dst_port_) + "@" + src_config->global_migration_sync_dir + "/" + dst_node_;
  // we can directly send data to target server
  std::string db_path_abs;
  auto db_ptr = storage_->GetDB();
  db_ptr->GetEnv()->GetAbsolutePath(src_config->db_dir, &db_path_abs);
  std::vector<std::string> slot_prefix_list_;

  for (int slot : slot_job_->slots_) {
    std::string prefix;
    ComposeSlotKeyPrefix(namespace_, slot, &prefix);
    slot_prefix_list_.push_back(prefix);
  }

  rocksdb::ColumnFamilyMetaData metacf_ssts;
  rocksdb::ColumnFamilyMetaData subkeycf_ssts;
  meta_cf_handle_ = storage_->GetCFHandle(Engine::kMetadataColumnFamilyName);
  subkey_cf_handle_ = storage_->GetCFHandle(Engine::kSubkeyColumnFamilyName);
  storage_->GetDB()->GetColumnFamilyMetaData(meta_cf_handle_, &metacf_ssts);
  storage_->GetDB()->GetColumnFamilyMetaData(subkey_cf_handle_, &subkeycf_ssts);
  std::vector<std::string> meta_compact_sst_(0);
  std::vector<std::string> subkey_compact_sst_(0);
  std::map<int, std::vector<std::string>> meta_level_files;
  std::map<int, std::vector<std::string>> subkey_level_files;

  start = Util::GetTimeStampUS();
  for (const auto &level_stat : metacf_ssts.levels) {
    meta_level_files[level_stat.level] = {};
    for (const auto &sst_info : level_stat.files) {
      for (auto prefix : slot_prefix_list_) {
        if (compare_with_prefix(sst_info.smallestkey, prefix) <= 0 &&
            compare_with_prefix(sst_info.largestkey, prefix) >= 0) {
          meta_level_files[level_stat.level].push_back(Util::Split(sst_info.name, "/").back());
          meta_compact_sst_.push_back(sst_info.name);
          break;  // no need for redundant inserting
        }
      }
    }
  }

  for (const auto &level_stat : subkeycf_ssts.levels) {
    subkey_level_files[level_stat.level] = {};
    for (const auto &sst_info : level_stat.files) {
      for (auto prefix : slot_prefix_list_) {
        if (compare_with_prefix(sst_info.smallestkey, prefix) <= 0 &&
            compare_with_prefix(sst_info.largestkey, prefix) >= 0) {
          subkey_level_files[level_stat.level].push_back(Util::Split(sst_info.name, "/").back());
          subkey_compact_sst_.push_back(sst_info.name);
          break;
        }
      }
    }
  }

  if (meta_compact_sst_.empty() || subkey_compact_sst_.empty()) {
    return {Status::NotOK, "No SSTs can be found."};
  }

  std::string meta_sst_str;
  std::vector<std::string> result_ssts;
  for (const auto &s : meta_compact_sst_) {
    auto fn = Util::Split(s, "/").back();
    meta_sst_str += (fn + ",");
    result_ssts.push_back(s);
  }
  meta_sst_str.pop_back();

  std::string sub_sst_str;
  sub_sst_str.clear();
  for (const auto &s : subkey_compact_sst_) {
    auto fn = Util::Split(s, "/").back();
    sub_sst_str += (fn + ",");
    result_ssts.push_back(s);
  }
  sub_sst_str.pop_back();
  end = Util::GetTimeStampUS();

  LOG(INFO) << "Meta SSTs:[" << meta_sst_str << "]";
  LOG(INFO) << "Subkey SSTs:[" << sub_sst_str << "]" << std::endl;
  LOG(INFO) << "SST collected, Time taken(us): " << end - start << std::endl;

  // copy files to remote server
  auto remote_username = svr_->GetConfig()->migration_user;

  std::string source_ssts = "";

  for (const auto &fn : result_ssts) {
    std::string abs_name;
    if (src_config->db_dir[0] == '/') {
      abs_name = src_config->db_dir + fn + " ";
    } else {
      abs_name = db_path_abs + "/" + src_config->db_dir + fn + " ";
    }

    source_ssts += abs_name;
  }
  source_ssts.pop_back();
  LOG(INFO) << "SST waiting for ingestion: " << source_ssts;
  std::string source_space = db_path_abs + "/" + svr_->GetConfig()->db_dir;
  std::string target_space = svr_->GetConfig()->global_migration_sync_dir + "/" + dst_node_;

  std::string worthy_result;
  std::string mkdir_remote_cmd =
      "ssh " + svr_->GetConfig()->migration_user + "@" + dst_ip_ + " mkdir -p -m 777 " + target_space;
  auto s = Util::CheckCmdOutput(mkdir_remote_cmd, &worthy_result);
  LOG(INFO) << "command: " << mkdir_remote_cmd;
  LOG(INFO) << worthy_result;
  std::string migration_cmds = "ls " + source_ssts + " |xargs -n 1 basename| parallel -v -j8 rsync -raz --progress " +
                               source_space + "/{} " + remote_username + "@" + dst_ip_ + ":" + target_space + "/{}";
  LOG(INFO) << migration_cmds;

  std::string file_copy_output;
  s = Util::CheckCmdOutput(migration_cmds, &file_copy_output);
  if (!s.IsOK()) {
    return {Status::NotOK, "Failed on copy file: " + file_copy_output};
  }

  // Start ingestion
  std::string ingest_output;
  std::string target_server_pre = "redis-cli";
  target_server_pre += (" -h " + dst_ip_);
  target_server_pre += (" -p " + std::to_string(dst_port_));

  start = Util::GetTimeStampUS();
  for (const auto &meta_level : meta_level_files) {
    std::string meta_file_str;
    if (meta_level.second.empty()) {
      continue;
    }
    for (auto file : meta_level.second) {
      meta_file_str += (file + ",");
    }
    meta_file_str.pop_back();

    std::string ingestion_command = " CLUSTERX sst_ingest local";
    ingestion_command += (" " + std::string(Engine::kMetadataColumnFamilyName));
    ingestion_command += (" " + meta_file_str);
    ingestion_command += (" " + dst_node_);
    auto level_ingest_cmd = target_server_pre + ingestion_command + " fast " + std::to_string(meta_level.first);
    LOG(INFO) << level_ingest_cmd;
    s = Util::CheckCmdOutput(level_ingest_cmd, &ingest_output);
    if (!s.IsOK()) {
      return s;
    }
  }
  for (const auto &subkey_level : subkey_level_files) {
    std::string subkey_file_str;
    if (subkey_level.second.empty()) {
      continue;
    }

    for (auto file : subkey_level.second) {
      subkey_file_str += (file + ",");
    }
    subkey_file_str.pop_back();

    std::string ingestion_command = " CLUSTERX sst_ingest local";
    ingestion_command += (" " + std::string(Engine::kMetadataColumnFamilyName));
    ingestion_command += (" " + subkey_file_str);
    ingestion_command += (" " + dst_node_);
    auto level_ingest_cmd = target_server_pre + ingestion_command + " fast " + std::to_string(subkey_level.first);
    LOG(INFO) << ingestion_command;
    s = Util::CheckCmdOutput(level_ingest_cmd, &ingestion_command);
    if (!s.IsOK()) {
      return s;
    }
  }

  end = Util::GetTimeStampUS();

  LOG(INFO) << "Level ingestion finished, Time taken(us)" << end - start;

  storage_->GetDB()->ContinueBackgroundWork();
  return Status::OK();
}
