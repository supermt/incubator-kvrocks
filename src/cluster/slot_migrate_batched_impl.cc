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
#include "rocksdb/db.h"
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
  rocksdb::WaitForBackgroundWork(storage_->GetDB());

  auto src_config = svr_->GetConfig();
  std::string src_info = "127.0.0.1:" + std::to_string(src_config->port) + "@" + src_config->db_dir;
  std::string dst_info =
      dst_ip_ + ":" + std::to_string(dst_port_) + "@" + src_config->global_migration_sync_dir + "/" + dst_node_;
  std::string cwd;
  rocksdb::Env::Default()->GetAbsolutePath(src_config->db_dir, &cwd);

  std::string src_uri = cwd;  // src_uri + src_info = absolute path
  if (src_uri.back() != '/') src_uri.push_back('/');
  std::string dst_uri = "/";  // dst_uri + dst_info = absolute path
  std::string migration_agent_path = src_config->migration_agent_location;
  std::string pull_method = std::to_string(pull_method_);
  std::string namespace_str = namespace_;
  std::string migration_user = src_config->migration_user;

  std::string slot_str;
  int i = 0;
  for (auto slot : migrate_slots_) {
    i++;
    slot_str += (std::to_string(slot) + ",");
  }
  slot_str.pop_back();
  auto s = storage_->ReOpenDB(true);  // Set DB to readonly
  if (!s.IsOK()) return s;
  std::string agent_cmd = migration_agent_path + " --src_uri=" + src_uri + " --dst_uri=" + dst_uri +
                          " --src_info=" + src_info + " --dst_info=" + dst_info + " --pull_method=" + pull_method +
                          " --namespace_str=" + namespace_str + " --migration_user=" + migration_user +
                          " --slot_str=" + slot_str;
  LOG(INFO) << "Try migrating using remote commands: " << agent_cmd;
  std::string worthy_result;
  s = Util::CheckCmdOutput(agent_cmd, &worthy_result);
  LOG(INFO) << "Migration agent returns with: " << worthy_result;
  if (!s.IsOK()) {
    rocksdb::WaitForBackgroundWork(storage_->GetDB());
    auto res = storage_->ReOpenDB(false);  // Restore DB to writable
    return s;
  }
  rocksdb::WaitForBackgroundWork(storage_->GetDB());
  s = storage_->ReOpenDB(false);  // Restore DB to writable
  if (!s.IsOK()) return s;
  return Status::OK();
}

CompactAndMergeMigrate::CompactAndMergeMigrate(Server *svr, int migration_speed, int pipeline_size_limit, int seq_gap,
                                               int pull_method)
    : SlotMigrate(svr, migration_speed, pipeline_size_limit, seq_gap), pull_method_(pull_method) {
  this->batched_ = true;
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