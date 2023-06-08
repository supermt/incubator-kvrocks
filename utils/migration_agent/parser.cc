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

#include "parser.h"

#include <assert.h>
#include <glog/logging.h>
#include <rocksdb/write_batch.h>

#include <memory>

#include "cluster/redis_slot.h"
#include "db_util.h"
#include "rocksdb/convenience.h"
#include "rocksdb/sst_file_reader.h"
#include "server/redis_reply.h"
#include "time_util.h"
#include "types/redis_string.h"

Status Parser::ParseFullDB() {
  rocksdb::DB *db_ = storage_->GetDB();
  if (!latest_snapshot_) latest_snapshot_ = std::make_unique<LatestSnapShot>(db_);
  rocksdb::ColumnFamilyHandle *metadata_cf_handle_ = storage_->GetCFHandle(Engine::kMetadataColumnFamilyName);

  rocksdb::ReadOptions read_options;
  read_options.snapshot = latest_snapshot_->GetSnapShot();
  read_options.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(read_options, metadata_cf_handle_));
  Status s;

  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    Metadata metadata(kRedisNone);
    metadata.Decode(iter->value().ToString());
    if (metadata.Expired()) {  // ignore the expired key
      continue;
    }

    if (metadata.Type() == kRedisString) {
      s = parseSimpleKV(iter->key(), iter->value(), metadata.expire);
    } else {
      s = parseComplexKV(iter->key(), metadata);
    }
    if (!s.IsOK()) return s;
  }

  return Status::OK();
}

Status Parser::parseSimpleKV(const Slice &ns_key, const Slice &value, int expire) {
  std::string ns, user_key;
  ExtractNamespaceKey(ns_key, &ns, &user_key, slot_id_encoded_);

  auto command = Redis::Command2RESP(
      {"SET", user_key, value.ToString().substr(Redis::STRING_HDR_SIZE, value.size() - Redis::STRING_HDR_SIZE)});
  Status s;
  //  = writer_->Write(ns, {command});
  //  if (!s.IsOK()) return s;

  if (expire > 0) {
    command = Redis::Command2RESP({"EXPIREAT", user_key, std::to_string(expire)});
    //    s = writer_->Write(ns, {command});
  }

  return s;
}

Status Parser::parseComplexKV(const Slice &ns_key, const Metadata &metadata) {
  RedisType type = metadata.Type();
  if (type < kRedisHash || type > kRedisSortedint) {
    return {Status::NotOK, "unknown metadata type: " + std::to_string(type)};
  }

  std::string ns, user_key;
  ExtractNamespaceKey(ns_key, &ns, &user_key, slot_id_encoded_);
  std::string prefix_key;
  InternalKey(ns_key, "", metadata.version, slot_id_encoded_).Encode(&prefix_key);
  std::string next_version_prefix_key;
  InternalKey(ns_key, "", metadata.version + 1, slot_id_encoded_).Encode(&next_version_prefix_key);

  rocksdb::ReadOptions read_options;
  read_options.snapshot = latest_snapshot_->GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  storage_->SetReadOptions(read_options);

  std::string output;
  auto iter = DBUtil::UniqueIterator(storage_, read_options);
  for (iter->Seek(prefix_key); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(prefix_key)) {
      break;
    }

    InternalKey ikey(iter->key(), slot_id_encoded_);
    std::string sub_key = ikey.GetSubKey().ToString();
    std::string value = iter->value().ToString();
    switch (type) {
      case kRedisHash:
        output = Redis::Command2RESP({"HSET", user_key, sub_key, value});
        break;
      case kRedisSet:
        output = Redis::Command2RESP({"SADD", user_key, sub_key});
        break;
      case kRedisList:
        output = Redis::Command2RESP({"RPUSH", user_key, value});
        break;
      case kRedisZSet: {
        double score = DecodeDouble(value.data());
        output = Redis::Command2RESP({"ZADD", user_key, Util::Float2String(score), sub_key});
        break;
      }
      case kRedisBitmap: {
        int index = std::stoi(sub_key);
        auto s = Parser::parseBitmapSegment(ns, user_key, index, value);
        if (!s.IsOK()) return s.Prefixed("failed to parse bitmap segment");
        break;
      }
      case kRedisSortedint: {
        std::string val = std::to_string(DecodeFixed64(ikey.GetSubKey().data()));
        output = Redis::Command2RESP({"ZADD", user_key, val, val});
        break;
      }
      default:
        break;  // should never get here
    }

    if (type != kRedisBitmap) {
      Status s;
      //      = writer_->Write(ns, {output});
      if (!s.IsOK()) return s.Prefixed(fmt::format("failed to write the '{}' command to AOF", output));
    }
  }

  if (metadata.expire > 0) {
    output = Redis::Command2RESP({"EXPIREAT", user_key, std::to_string(metadata.expire)});
    Status s;
    //    = writer_->Write(ns, {output});
    if (!s.IsOK()) return s.Prefixed("failed to write the EXPIREAT command to AOF");
  }

  return Status::OK();
}

Status Parser::parseBitmapSegment(const Slice &ns, const Slice &user_key, int index, const Slice &bitmap) {
  Status s;
  for (size_t i = 0; i < bitmap.size(); i++) {
    if (bitmap[i] == 0) continue;  // ignore zero byte

    for (int j = 0; j < 8; j++) {
      if (!(bitmap[i] & (1 << j))) continue;  // ignore zero bit

                                              //      s = writer_->Write(
      //          ns.ToString(),
      //          {Redis::Command2RESP({"SETBIT", user_key.ToString(), std::to_string(index * 8 + i * 8 + j), "1"})});
      //      if (!s.IsOK()) return s.Prefixed("failed to write SETBIT command to AOF");
    }
  }
  return Status::OK();
}

Status Parser::ParseWriteBatch(const std::string &batch_string) {
  //  rocksdb::WriteBatch write_batch(batch_string);
  //  WriteBatchExtractor write_batch_extractor(slot_id_encoded_, -1, true);
  //
  //  auto db_status = write_batch.Iterate(&write_batch_extractor);
  //  if (!db_status.ok())
  //    return {Status::NotOK, fmt::format("failed to iterate over the write batch: {}", db_status.ToString())};

  //  auto resp_commands = write_batch_extractor.GetRESPCommands();
  //  for (const auto &iter : *resp_commands) {
  //    Status s;
  //    //    = writer_->Write(iter.first, iter.second);
  //    if (!s.IsOK()) {
  //      LOG(ERROR) << "[kvrocks2redis] Failed to write to AOF from the write batch. Error: " << s.Msg();
  //    }
  //  }

  return Status::OK();
}
Status Parser::SeekAndDump() {
  rocksdb::DB *db_ = storage_->GetDB();
  if (!latest_snapshot_) latest_snapshot_ = std::make_unique<LatestSnapShot>(db_);
  meta_cf_handle_ = storage_->GetCFHandle(Engine::kMetadataColumnFamilyName);
  subkey_cf_handle_ = storage_->GetCFHandle(Engine::kSubkeyColumnFamilyName);

  rocksdb::ReadOptions read_options;
  read_options.snapshot = latest_snapshot_->GetSnapShot();
  read_options.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(read_options, meta_cf_handle_));

  // Create SST file writer
  Status s;
  rocksdb::EnvOptions env_opt;
  rocksdb::Options rocks_opt = db_->GetOptions();
  rocksdb::SstFileWriter *current_meta_sst_writer = new rocksdb::SstFileWriter(env_opt, rocks_opt, meta_cf_handle_);
  rocksdb::SstFileWriter *current_subkey_sst_writer = new rocksdb::SstFileWriter(env_opt, rocks_opt, subkey_cf_handle_);
  int meta_sst_no = 0;
  int subkey_sst_no = 0;

  auto rocks = current_meta_sst_writer->Open(output_dir_ + "/" + std::to_string(meta_sst_no));
  if (!rocks.ok()) return {Status::NotOK, rocks.ToString()};
  meta_files_.push_back(current_meta_sst_writer);

  rocks = current_subkey_sst_writer->Open(output_dir_ + "/" + std::to_string(subkey_sst_no));
  if (!rocks.ok()) return {Status::NotOK, rocks.ToString()};
  subkey_files_.push_back(current_subkey_sst_writer);

  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    Metadata metadata(kRedisNone);
    metadata.Decode(iter->value().ToString());

    if (metadata.Expired()) {  // ignore the expired key
      continue;
    }
    std::string ns, user_key;
    uint16_t slot;
    ExtractNamespaceKey(iter->key(), &ns, &user_key, &slot);
    std::cout << iter->key().ToString(true) << ". Slot id: " << slot << std::endl;
    if (slot_list_.count(slot) == 0) continue;  // it's not in one of our needed slot

    if (metadata.Type() == kRedisString) {
      s = DumpSimpleKV(iter->key(), iter->value(), metadata.expire);
    } else {
      s = DumpComplexKV(iter->key(), metadata, iter->value());
    }
    if (!s.IsOK()) return s;
    if (meta_files_.back()->FileSize() > 128 * 1024 * 1024l) {
      auto rocks = meta_files_.back()->Finish();
      if (!rocks.ok()) return {Status::NotOK, rocks.ToString()};
      meta_sst_no++;
      rocks = current_meta_sst_writer->Open(output_dir_ + "/" + std::to_string(meta_sst_no));
      if (!rocks.ok()) return {Status::NotOK, rocks.ToString()};
      meta_files_.push_back(current_meta_sst_writer);
    }

    if (subkey_files_.back()->FileSize() > 128 * 1024 * 1024l) {
      auto rocks = subkey_files_.back()->Finish();
      if (!rocks.ok()) return {Status::NotOK, rocks.ToString()};
      subkey_sst_no++;
      rocks = current_subkey_sst_writer->Open(output_dir_ + "/" + std::to_string(subkey_sst_no));
      if (!rocks.ok()) return {Status::NotOK, rocks.ToString()};
      subkey_files_.push_back(current_subkey_sst_writer);
    }
  }
  rocks = subkey_files_.back()->Finish();
  if (!rocks.ok()) return {Status::NotOK, rocks.ToString()};
  rocks = meta_files_.back()->Finish();
  if (!rocks.ok()) return {Status::NotOK, rocks.ToString()};

  return Status::OK();
}
Status Parser::DumpSimpleKV(const Slice &ns_key, const Slice &value, int expire) {
  std::string ns, user_key;
  //  ExtractNamespaceKey(ns_key, &ns, &user_key, true);  // Slot id is always encoded
  auto rocks = meta_files_.back()->Put(ns_key, value);
  if (!rocks.ok()) return {Status::NotOK, rocks.ToString()};
  return Status::OK();
}

Status Parser::DumpComplexKV(const Slice &ns_key, const Metadata &metadata, const Slice &meta_value) {
  auto current_meta = meta_files_.back();
  auto current_subkey = subkey_files_.back();
  RedisType type = metadata.Type();
  if (type < kRedisHash || type > kRedisSortedint) {
    return {Status::NotOK, "unknown metadata type: " + std::to_string(type)};
  }
  // All KV here must be in the target slots
  std::string ns, user_key;
  ExtractNamespaceKey(ns_key, &ns, &user_key, true);
  std::string prefix_key;
  InternalKey(ns_key, "", metadata.version, true).Encode(&prefix_key);
  std::string next_version_prefix_key;
  InternalKey(ns_key, "", metadata.version + 1, true).Encode(&next_version_prefix_key);

  current_meta->Put(ns_key, meta_value);

  rocksdb::ReadOptions read_options;
  read_options.snapshot = latest_snapshot_->GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  storage_->SetReadOptions(read_options);

  auto iter = DBUtil::UniqueIterator(storage_, read_options);
  for (iter->Seek(prefix_key); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(prefix_key)) {
      break;
    }
    InternalKey ikey(iter->key(), true);

    std::string sub_key = ikey.GetSubKey().ToString();
    current_subkey->Put(iter->key(), iter->value());
  }
  return Status::OK();
}
Status Parser::CompactAndMerge() {
  std::vector<std::string> slot_prefix_list_;
  // Step 0. Fill namespace if empty

  auto db_ptr = storage_->GetDB();
  rocksdb::ReadOptions read_options;
  storage_->SetReadOptions(read_options);
  if (!latest_snapshot_) latest_snapshot_ = std::make_unique<LatestSnapShot>(db_ptr);
  //  CancelAllBackgroundWork(db_ptr, true);
  db_ptr->PauseBackgroundWork();
  read_options.snapshot = latest_snapshot_->GetSnapShot();
  auto iter = DBUtil::UniqueIterator(storage_, read_options, meta_cf_handle_);

  if (namespace_ == "") {
    iter->SeekToFirst();
    std::string ns, user_key;
    ExtractNamespaceKey(iter->key(), &ns, &user_key, true);
    std::cout << ns.size() << "(bytes), ns data:" << ns << std::endl;
    namespace_ = ns;
  }

  // Step 1. Compose prefix key
  for (int slot : slot_list_) {
    std::string prefix;
    ComposeSlotKeyPrefix(namespace_, slot, &prefix);
    //    std::cout << prefix << "," << slot << "," << Slice(prefix).ToString(true);
    // After ComposeSlotKeyPrefix
    //  +-------------|---------|-------|--------|---|-------|-------+
    // |namespace_size|namespace|slot_id|, therefore we compare only the prefix key

    // This is prefix key: and the subkey is empty
    // +-------------|---------|-------|--------|---|-------|-------+
    // |namespace_size|namespace|slot_id|key_size|key|version|subkey|
    // +-------------|---------|-------|--------|---|-------|-------+
    slot_prefix_list_.push_back(prefix);
  }
  // Therefore we need only the prefix key.
  // Step 2. Find related SSTs.
  // Get level files
  rocksdb::ColumnFamilyMetaData metacf_ssts;
  rocksdb::ColumnFamilyMetaData subkeycf_ssts;
  storage_->GetDB()->GetColumnFamilyMetaData(meta_cf_handle_, &metacf_ssts);
  storage_->GetDB()->GetColumnFamilyMetaData(subkey_cf_handle_, &subkeycf_ssts);
  std::vector<std::string> meta_compact_sst_(0);
  std::vector<std::string> subkey_compact_sst_(0);

  std::vector<std::string> meta_compact_results;
  std::vector<std::string> subkey_compact_results;
  auto options = storage_->GetDB()->GetOptions();
  rocksdb::CompactionOptions co;
  co.compression = options.compression;
  co.max_subcompactions = options.max_background_compactions;

  std::sort(slot_prefix_list_.begin(), slot_prefix_list_.end(),
            [&](const Slice &a, const Slice &b) { return options.comparator->Compare(a, b) < 0; });

  auto start = Util::GetTimeStampMS();
  std::cout << "Finding Meta" << std::endl;
  for (const auto &level_stat : metacf_ssts.levels) {
    for (const auto &sst_info : level_stat.files) {
      for (auto prefix : slot_prefix_list_) {
        if (compare_with_prefix(sst_info.smallestkey, prefix) <= 0 &&
            compare_with_prefix(sst_info.largestkey, prefix) >= 0) {
          meta_compact_sst_.push_back(sst_info.name);
          break;  // no need for redundant inserting
        }
      }
    }
  }
  for (const auto &level_stat : subkeycf_ssts.levels) {
    for (const auto &sst_info : level_stat.files) {
      for (auto prefix : slot_prefix_list_) {
        if (compare_with_prefix(sst_info.smallestkey, prefix) <= 0 &&
            compare_with_prefix(sst_info.largestkey, prefix) >= 0) {
          subkey_compact_sst_.push_back(sst_info.name);
          break;
        }
      }
    }
  }

  if (meta_compact_sst_.empty() || subkey_compact_sst_.empty()) {
    std::cout << "Error: No SST can be found";
    exit(-1);
  }

  std::cout << "Meta SST found:" << meta_compact_sst_.size() << std::endl;
  std::cout << "Finding Subkey" << std::endl;

  std::cout << "Subkey SST found:" << subkey_compact_sst_.size() << std::endl;
  std::string sst_str;
  for (const auto &s : meta_compact_sst_) {
    sst_str += (s + ",");
  }
  sst_str.pop_back();

  std::cout << "Meta SSTs:[" << sst_str << "]" << std::endl;
  sst_str.clear();
  for (const auto &s : subkey_compact_sst_) {
    sst_str += (s + ",");
  }
  sst_str.pop_back();

  std::cout << "Subkey SSTs:[" << sst_str << "]" << std::endl;
  auto end = Util::GetTimeStampMS();
  std::cout << "SST collecting time(ms): " << end - start << std::endl;

  // Step 3. Compact data to remove dead entries

  start = Util::GetTimeStampMS();
  auto compact_s = storage_->GetDB()->CompactFiles(co, meta_cf_handle_, meta_compact_sst_, options.num_levels - 1, -1,
                                                   &meta_compact_results);
  if (!compact_s.ok()) {
    return {Status::NotOK, compact_s.ToString()};
  }

  compact_s = storage_->GetDB()->CompactFiles(co, subkey_cf_handle_, subkey_compact_sst_, options.num_levels - 1, -1,
                                              &subkey_compact_results);
  if (!compact_s.ok()) {
    return {Status::NotOK, compact_s.ToString()};
  }
  db_ptr->ContinueBackgroundWork();
  end = Util::GetTimeStampMS();
  std::cout << "Compaction time(ms): " << end - start << std::endl;
  std::cout << "# compaction input: " << meta_compact_sst_.size() + subkey_compact_sst_.size()
            << " # compaction output: " << meta_compact_results.size() + subkey_compact_results.size() << std::endl;

  // Step 4. Copy the file to the remote, return
  std::set<std::string> result_sets;

  std::string source_ssts = "";
  std::string meta_compact_results_str;
  std::string subkey_compact_results_str;

  start = Util::GetTimeStampMS();

  for (const auto &fn : meta_compact_results) {
    rocksdb::SstFileReader reader(options);
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options, subkey_cf_handle_);
    reader.Open(fn);
    auto read_it = reader.NewIterator(read_options);
    auto out_put_name = fn + ".out";
    writer.Open(out_put_name);
    read_it->SeekToFirst();
    std::string smallest_prefix = slot_prefix_list_.front();
    std::string largest_prefix = slot_prefix_list_.back();
    for (; read_it->Valid(); read_it->Next()) {
      auto current_key = read_it->key();
      if (compare_with_prefix(smallest_prefix, current_key.ToString()) <= 0 &&
          compare_with_prefix(largest_prefix, current_key.ToString()) >= 0) {
        writer.Put(read_it->key(), read_it->value());
      }
    }
    writer.Finish();
    result_sets.emplace(out_put_name);
    meta_compact_results_str += Util::Split(out_put_name, "/").back();
    meta_compact_results_str += ',';
  }
  end = Util::GetTimeStampMS();
  std::cout << "Meta Filtered, time(ms): " << end - start << std::endl;

  start = Util::GetTimeStampMS();
  for (const auto &fn : subkey_compact_results) {
    rocksdb::SstFileReader reader(options);
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options, subkey_cf_handle_);
    reader.Open(fn);
    auto read_it = reader.NewIterator(read_options);
    auto out_put_name = fn + ".out";
    writer.Open(out_put_name);
    read_it->SeekToFirst();
    std::string smallest_prefix = slot_prefix_list_.front();
    std::string largest_prefix = slot_prefix_list_.back();
    for (; read_it->Valid(); read_it->Next()) {
      auto current_key = read_it->key();
      if (compare_with_prefix(smallest_prefix, current_key.ToString()) <= 0 &&
          compare_with_prefix(largest_prefix, current_key.ToString()) >= 0) {
        writer.Put(read_it->key(), read_it->value());
      }
    }
    writer.Finish();
    result_sets.emplace(out_put_name);
    subkey_compact_results_str += Util::Split(out_put_name, "/").back();
    subkey_compact_results_str += ',';
  }
  end = Util::GetTimeStampMS();
  std::cout << "Subkey Filtered, time(ms): " << end - start << std::endl;

  for (const auto &fn : result_sets) {
    source_ssts += (fn + " ");
  }
  meta_compact_results_str.pop_back();
  subkey_compact_results_str.pop_back();

  std::cout << "File collected, Meta: [" << meta_compact_results_str << "], Subkey: [" << subkey_compact_results_str
            << "]" << std::endl;
  std::string source_space = config_.src_db_dir;
  std::string target_space = config_.dst_db_dir;

  std::string mkdir_remote_cmd =
      "ssh " + config_.remote_username + "@" + config_.dst_server_host + " mkdir -p " + config_.dst_db_dir;
  std::cout << mkdir_remote_cmd << std::endl;
  std::string worthy_result;
  Status s = Util::CheckCmdOutput(mkdir_remote_cmd, &worthy_result);
  std::cout << worthy_result << std::endl;
  if (!s.IsOK()) {
    return {Status::NotOK, "Failed creating directory"};
  }

  start = Util::GetTimeStampMS();
  //  std::string migration_cmds = "ls " + source_ssts + " |xargs -n 1 basename| parallel -v -j8 rsync -raz --progress "
  //  +
  //                               source_space + "/{} " + config_.remote_username + "@" + config_.dst_server_host + ":"
  //                               + target_space + "/{}";
  std::string migration_cmds = "ls " + source_ssts + " |xargs -n 1 basename| parallel -v -j8 rsync -raz --progress " +
                               source_space + "/{} " + config_.remote_username + "@" + config_.dst_server_host + ":" +
                               target_space + "/{}";

  //  for (auto migrate_candidate : result_sets) {
  //    std::string migration_cmds = "rsync -raz " + source_space + sst_str + config_.remote_username + "@" +
  //                                 config_.dst_server_host + ":" + target_space;

  end = Util::GetTimeStampMS();
  std::cout << migration_cmds << " Time taken(ms)" << std::endl;
  worthy_result.clear();
  s = Util::CheckCmdOutput(migration_cmds, &worthy_result);

  if (!s.IsOK()) {
    std::cout << "File transmission error!" << s.Msg();
    return {Status::NotOK, "Failed copying files:" + s.Msg()};
  }
  std::cout << worthy_result << std::endl;
  std::cout << "File copy time (ms): " << end - start << std::endl;
  // After copying the files, ingest to target server
  std::string target_server_pre = "redis-cli";
  target_server_pre += (" -h " + config_.dst_server_host);
  target_server_pre += (" -p " + std::to_string(config_.dst_server_port));
  // meta cf first
  std::string ingestion_command = "CLUSTERX sst_ingest local";
  ingestion_command += (" " + std::string(Engine::kMetadataColumnFamilyName));
  ingestion_command += (" " + meta_compact_results_str);
  ingestion_command += (" " + config_.src_server_host);
  auto temp = target_server_pre + ingestion_command + " slow";
  std::cout << ingestion_command << std::endl;
  start = Util::GetTimeStampMS();
  s = Util::CheckCmdOutput(temp, &worthy_result);
  if (!s.IsOK()) {
    return s;
  }
  end = Util::GetTimeStampMS();

  std::cout << "Meta ingestion results: " << worthy_result << "; Time taken(ms): " << end - start << std::endl;
  // sub key cf then
  ingestion_command = "CLUSTERX sst_ingest local";
  ingestion_command += (" " + std::string(Engine::kSubkeyColumnFamilyName));
  ingestion_command += (" " + subkey_compact_results_str);
  ingestion_command += (" " + config_.src_server_host);
  temp = target_server_pre + ingestion_command + " slow";
  start = Util::GetTimeStampMS();
  std::cout << ingestion_command << std::endl;
  s = Util::CheckCmdOutput(temp, &worthy_result);
  if (!s.IsOK()) {
    return s;
  }
  end = Util::GetTimeStampMS();
  std::cout << "Subkey ingestion results: " << worthy_result << "; Time taken(ms): " << end - start << std::endl;
  //  }
  latest_snapshot_.reset();
  return Status::OK();
}
