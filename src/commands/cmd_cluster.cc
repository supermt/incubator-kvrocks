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

#include "cluster/slot_import.h"
#include "commander.h"
#include "error_constants.h"
#include "io_util.h"
#include "scope_exit.h"
#include "thread_util.h"
#include "time_util.h"

namespace Redis {

class CommandCluster : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);

    if (args.size() == 2 && (subcommand_ == "nodes" || subcommand_ == "slots" || subcommand_ == "info"))
      return Status::OK();

    if (subcommand_ == "keyslot" && args_.size() == 3) return Status::OK();

    if (subcommand_ == "import") {
      if (args.size() == 4) {
        slot_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
        auto state = ParseInt<unsigned>(args[3], {kImportStart, kImportNone}, 10);
        if (!state) return {Status::NotOK, "Invalid import state"};
        state_ = static_cast<ImportStatus>(*state);
        return Status::OK();
      } else {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }
    }

    return {Status::RedisParseErr, "CLUSTER command, CLUSTER INFO|NODES|SLOTS|KEYSLOT"};
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!svr->GetConfig()->cluster_enabled) {
      *output = Redis::Error("Cluster mode is not enabled");
      return Status::OK();
    }

    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    if (subcommand_ == "keyslot") {
      auto slot_id = GetSlotNumFromKey(args_[2]);
      *output = Redis::Integer(slot_id);
    } else if (subcommand_ == "slots") {
      std::vector<SlotInfo> infos;
      Status s = svr->cluster_->GetSlotsInfo(&infos);
      if (s.IsOK()) {
        output->append(Redis::MultiLen(infos.size()));
        for (const auto &info : infos) {
          output->append(Redis::MultiLen(info.nodes.size() + 2));
          output->append(Redis::Integer(info.start));
          output->append(Redis::Integer(info.end));
          for (const auto &n : info.nodes) {
            output->append(Redis::MultiLen(3));
            output->append(Redis::BulkString(n.host));
            output->append(Redis::Integer(n.port));
            output->append(Redis::BulkString(n.id));
          }
        }
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "nodes") {
      std::string nodes_desc;
      Status s = svr->cluster_->GetClusterNodes(&nodes_desc);
      if (s.IsOK()) {
        *output = Redis::BulkString(nodes_desc);
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "info") {
      std::string cluster_info;
      Status s = svr->cluster_->GetClusterInfo(&cluster_info);
      if (s.IsOK()) {
        *output = Redis::BulkString(cluster_info);
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "import") {
      Status s;
      batched_import = svr->slot_migrate_->IsBatched();
      //      svr->GetConfig()->migrate_method >= kSeekAndInsertBatched;
      s = svr->cluster_->ImportSlot(conn, static_cast<int>(slot_), state_, batched_import);

      if (s.IsOK()) {
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else {
      *output = Redis::Error("Invalid cluster command options");
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  int64_t slot_ = -1;
  ImportStatus state_ = kImportNone;
  bool batched_import = false;
};
class CommandIngest : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    remote_or_local_ = args[1];
    column_family_name_ = args[2];
    files_str_ = args[3];
    server_id_ = args[4];
    if (remote_or_local_ != "local" && remote_or_local_ != "remote") {
      return {Status::NotOK, "Failed cmd format, it should be like: ingest remote|local file1,file2,file3"};
    }
    return Status::OK();
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    LOG(INFO) << "Receive Ingest command:" << files_str_;
    //    LOG(INFO) << "Start Ingesting files:" << files_str_;
    std::string target_dir = svr->GetConfig()->backup_sync_dir;
    std::vector<std::string> files = Util::Split(files_str_, ",");
    LOG(INFO) << "Ingesting files from: " << remote_or_local_;

    if (remote_or_local_ == "local") {
      std::vector<std::string> ingest_files;
      std::string file_str;
      for (auto file : files) {
        ingest_files.push_back(svr->GetConfig()->migration_sync_dir + std::to_string(svr->GetConfig()->port) + "/" +
                               file);
        file_str += ingest_files.back() + ",";
      }
      file_str.pop_back();
      LOG(INFO) << "Ingesting files: " << file_str;

      auto t = GET_OR_RET(Util::CreateThread("master-repl", [svr, this, ingest_files] {
        auto s = svr->cluster_->IngestFiles(this->column_family_name_, ingest_files);

        if (!s.IsOK()) {
          LOG(ERROR) << "Ingestion Error, reason: " << s.Msg();
          return;
        }
      }));

      //      t.detach();
      t.join();

      //      auto t = GET_OR_RET(svr->cluster_->IngestFiles(column_family_name_, ingest_files));

      //      auto s = svr->cluster_->IngestFiles(column_family_name_, ingest_files);

      *output = Redis::SimpleString("OK");
      return Status::OK();
    } else if (remote_or_local_ == "remote") {
      LOG(INFO) << "Fetching data from remote server: " << server_id_;
      auto start = Util::GetTimeStampMS();
      Status s = Status::OK();
      s = svr->cluster_->FetchFileFromRemote(server_id_, files, svr->GetConfig()->migration_sync_dir);
      auto end = Util::GetTimeStampMS();
      if (!s.IsOK()) {
        LOG(ERROR) << "Fetching data error " << s.Msg();
        *output = Redis::SimpleString("Fetch error");
        return s;
      }
      LOG(INFO) << "Fetched all SST, time taken(ms): " << end - start;
      std::vector<std::string> ingestion_candidates;
      for (auto file : files) {
        ingestion_candidates.push_back(svr->GetConfig()->migration_sync_dir + "/" + file);
      }

      LOG(INFO) << "Start Ingestion";
      s = svr->cluster_->IngestFiles(column_family_name_, ingestion_candidates);

      if (!s.IsOK()) {
        *output = Redis::SimpleString("Ingestion error");
        LOG(ERROR) << "Ingesting data error " << s.Msg();
        return s;
      }
      *output = Redis::SimpleString("OK");
      return Status::OK();
    }
    return {Status::NotOK, "Execution failed"};
  }

 private:
  std::string remote_or_local_;
  std::string files_str_;
  std::string column_family_name_;
  std::string server_id_;
};
class CommandSSTFetch : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    files_str_ = args[1];
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override { return Status::OK(); }

 private:
  std::string files_str_;
};

class CommandClusterX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);

    if (args.size() == 2 && (subcommand_ == "version")) return Status::OK();

    if (subcommand_ == "setnodeid" && args_.size() == 3 && args_[2].size() == kClusterNodeIdLen) return Status::OK();

    if (subcommand_ == "migrate") {
      if (args.size() == 4) {
        slot_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
        dst_node_id_ = args[3];
        return Status::OK();
      } else if (args.size() == 5) {
        slot_ = -1;
        int64_t start = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
        int64_t end = GET_OR_RET(ParseInt<int64_t>(args[3], 10));
        for (int64_t i = start; i < end; i++) {
          slots_.push_back(static_cast<int>(i));
        }
        dst_node_id_ = args[4];
        return Status::OK();
      } else {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }
    }

    if (subcommand_ == "setnodes" && args_.size() >= 4) {
      nodes_str_ = args_[2];

      auto parse_result = ParseInt<int64_t>(args[3].c_str(), 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "Invalid version"};
      }

      set_version_ = *parse_result;

      if (args_.size() == 4) return Status::OK();

      if (args_.size() == 5 && strcasecmp(args_[4].c_str(), "force") == 0) {
        force_ = true;
        return Status::OK();
      }

      return {Status::RedisParseErr, "Invalid setnodes options"};
    }

    // CLUSTERX SETSLOT $SLOT_ID NODE $NODE_ID $VERSION
    // CLUSTERX SETSLOT $SLOT_ID_START $SLOT_ID_END NODE $NODE_ID $VERSION

    if (subcommand_ == "setslot" && args_.size() == 6) {
      auto parse_id = ParseInt<int>(args[2], 10);
      if (!parse_id) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      slot_id_ = *parse_id;

      if (!Cluster::IsValidSlot(slot_id_)) {
        return {Status::RedisParseErr, "Invalid slot id"};
      }

      if (strcasecmp(args_[3].c_str(), "node") != 0) {
        return {Status::RedisParseErr, "Invalid setslot options"};
      }

      if (args_[4].size() != kClusterNodeIdLen) {
        return {Status::RedisParseErr, "Invalid node id"};
      }

      auto parse_version = ParseInt<int64_t>(args[5], 10);
      if (!parse_version) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      if (*parse_version < 0) return {Status::RedisParseErr, "Invalid version"};

      set_version_ = *parse_version;

      return Status::OK();
    }

    return {Status::RedisParseErr, "CLUSTERX command, CLUSTERX VERSION|SETNODEID|SETNODES|SETSLOT|MIGRATE"};
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!svr->GetConfig()->cluster_enabled) {
      *output = Redis::Error("Cluster mode is not enabled");
      return Status::OK();
    }

    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    bool need_persist_nodes_info = false;
    if (subcommand_ == "setnodes") {
      Status s = svr->cluster_->SetClusterNodes(nodes_str_, set_version_, force_);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "setnodeid") {
      Status s = svr->cluster_->SetNodeId(args_[2]);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "setslot") {
      Status s = svr->cluster_->SetSlot(slot_id_, args_[4], set_version_);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else if (subcommand_ == "version") {
      int64_t v = svr->cluster_->GetVersion();
      *output = Redis::BulkString(std::to_string(v));
    } else if (subcommand_ == "migrate") {
      Status s;
      if (slot_ == -1) {
        s = svr->cluster_->MigrateSlots(slots_, dst_node_id_);

      } else {
        s = svr->cluster_->MigrateSlot(static_cast<int>(slot_), dst_node_id_);
      }

      if (s.IsOK()) {
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else {
      *output = Redis::Error("Invalid cluster command options");
    }
    if (need_persist_nodes_info && svr->GetConfig()->persist_cluster_nodes_enabled) {
      return svr->cluster_->DumpClusterNodes(svr->GetConfig()->NodesFilePath());
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  std::string nodes_str_;
  std::string dst_node_id_;
  int64_t set_version_ = 0;
  int64_t slot_ = -1;
  int slot_id_ = -1;
  bool force_ = false;
  std::vector<int> slots_;
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandCluster>("cluster", -2, "cluster no-script", 0, 0, 0),
                        MakeCmdAttr<CommandClusterX>("clusterx", -2, "cluster no-script", 0, 0, 0),
                        MakeCmdAttr<CommandIngest>("sst_ingest", 5, "cluster no-script", 0, 0, 0),
                        MakeCmdAttr<CommandSSTFetch>("fetch_remote_sst", 2, "cluster no-script", 0, 0, 0), )

}  // namespace Redis
