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

LevelMigrate::LevelMigrate(Server* svr, int migration_speed, int pipeline_size_limit, int seq_gap, bool batched)
    : CompactAndMergeMigrate(svr, migration_speed, pipeline_size_limit, seq_gap, batched) {}
Status LevelMigrate::SendSnapshot() {
  // For each level, pick the SSTs

  compact_ptr->GetColumnFamilyMetaData(GetMetadataCFH(), &metacf_level_stats);
  for (int i = 0; i < storage_->GetDB()->GetOptions().num_levels; i++) {
    auto s = PickSSTForLevel(i);
    if (!s.IsOK()) {
      return s;
    }

    s = SendRemoteSST();
    if (s.IsOK()) {
      return s;
    }
    LOG(INFO) << "Successfully send SSTS at level: " << i << ". # of Meta SSTs: " << pend_sending_sst_meta.size()
              << ". # of Subkey SSTs: " << pend_sending_sst_subkey.size();
  }
  return Status::OK();
}

Status LevelMigrate::PickSSTForLevel(int level) {
  auto meta_level = metacf_level_stats.levels[level];
  auto subkey_level = subkey_stats.levels[level];
  pend_sending_sst_meta.clear();

  for (auto slot_prefix : slot_prefix_list_) {
    for (const auto& file : meta_level.files) {
      // Search through the meta sst list
      if (compare_with_prefix(file.smallestkey, slot_prefix) < 0 &&
          compare_with_prefix(file.largestkey, slot_prefix) > 0) {
        pend_sending_sst_meta.push_back(file.relative_filename);
      }
    }
  }

  for (auto subkey_prefix : subkey_prefix_list_) {
    for (const auto& file : subkey_level.files) {
      if (compare_with_prefix(file.smallestkey, subkey_prefix) < 0 &&
          compare_with_prefix(file.largestkey, subkey_prefix) > 0) {
        pend_sending_sst_subkey.push_back(file.relative_filename);
      }
    }
  }

  return Status::OK();
}
Status LevelMigrate::SendRemoteSST() {
  auto s = CompactAndMergeMigrate::SendRemoteSST(pend_sending_sst_meta, Engine::kMetadataColumnFamilyName);
  if (s.IsOK()) {
    LOG(ERROR) << "Meta SSTs sending error";
    return s;
  }
  s = CompactAndMergeMigrate::SendRemoteSST(pend_sending_sst_subkey, Engine::kSubkeyColumnFamilyName);
  if (s.IsOK()) {
    LOG(ERROR) << "Subkey SSTs sending error";
    return s;
  }
  return s;
}
