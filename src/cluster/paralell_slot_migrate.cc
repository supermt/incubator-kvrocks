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

#include <fcntl.h>

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

ParallelSlotMigrate::ParallelSlotMigrate(Server *svr, int migration_speed, int pipeline_size_limit, int seq_gap)
    : SlotMigrate(svr, migration_speed, pipeline_size_limit, seq_gap) {
  this->batched_ = true;
}
void ParallelSlotMigrate::Loop() {
  while (true) {
    std::unique_lock<std::mutex> ul(job_mutex_);
    while (!IsTerminated() && !slot_job_) {
      job_cv_.wait(ul);
    }
    ul.unlock();

    if (IsTerminated()) {
      Clean();
      return;
    }

    LOG(INFO) << "[migrate] migrate_slot: " << slot_job_->migrate_slot_ << ", dst_ip: " << slot_job_->dst_ip_
              << ", dst_port: " << slot_job_->dst_port_ << ", speed_limit: " << slot_job_->speed_limit_
              << ", pipeline_size_limit: " << slot_job_->pipeline_size_;

    dst_ip_ = slot_job_->dst_ip_;
    dst_port_ = slot_job_->dst_port_;
    migration_speed_ = slot_job_->speed_limit_;
    pipeline_size_limit_ = slot_job_->pipeline_size_;
    seq_gap_limit_ = slot_job_->seq_gap_;

    for (int slot : slot_job_->slots_) {
      slot_job_->migrate_slot_ = slot;
      migrate_slot_ = (int16_t)slot;
      RunStateMachine();
    }

    std::lock_guard<std::mutex> guard(job_mutex_);
    slot_job_->migrate_slot_ = -1;
    slot_job_.reset();
    //    svr_->migration_pool_->enqueue(&SlotMigrate::RunStateMachine, this);
  }
}
Status ParallelSlotMigrate::MigrateStart(Server *svr, const std::string &node_id, const std::string &dst_ip,
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

Status ParallelSlotMigrate::SetMigrationSlots(std::vector<int> &target_slots) {
  if (!migrate_slots_.empty() || this->IsMigrationInProgress()) {
    return {Status::NotOK, "Last Migrate Batch is not finished"};
  }
  migrate_slots_.clear();
  migrate_slots_.insert(migrate_slots_.end(), target_slots.begin(), target_slots.end());
  return Status::OK();
}
void ParallelSlotMigrate::Clean() {
  LOG(INFO) << "[" << GetName() << "] Clean resources of migrating slot " << slot_job_->migrate_slot_;
  if (slot_snapshot_) {
    storage_->GetDB()->ReleaseSnapshot(slot_snapshot_);
    slot_snapshot_ = nullptr;
  }

  state_machine_ = kSlotMigrateNone;
  current_pipeline_size_ = 0;
  wal_begin_seq_ = 0;
  wal_increment_seq_ = 0;
  SetMigrateStopFlag(false);
}
