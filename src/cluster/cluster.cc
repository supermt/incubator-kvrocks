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

#include "cluster.h"

#include <config/config_util.h>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <memory>

#include "commands/commander.h"
#include "event_util.h"
#include "fmt/format.h"
#include "io_util.h"
#include "parse_util.h"
#include "replication.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb_crc32c.h"
#include "server/server.h"
#include "string_util.h"
#include "time_util.h"

const char *errInvalidNodeID = "Invalid cluster node id";
const char *errInvalidSlotID = "Invalid slot id";
const char *errSlotOutOfRange = "Slot is out of range";
const char *errInvalidClusterVersion = "Invalid cluster version";
const char *errSlotOverlapped = "Slot distribution is overlapped";
const char *errNoMasterNode = "The node isn't a master";
const char *errClusterNoInitialized = "CLUSTERDOWN The cluster is not initialized";
const char *errInvalidClusterNodeInfo = "Invalid cluster nodes info";
const char *errInvalidImportState = "Invalid import state";

ClusterNode::ClusterNode(std::string id, std::string host, int port, int role, std::string master_id,
                         std::bitset<kClusterSlots> slots)
    : id_(std::move(id)),
      host_(std::move(host)),
      port_(port),
      role_(role),
      master_id_(std::move(master_id)),
      slots_(slots) {}

Cluster::Cluster(Server *svr, std::vector<std::string> binds, int port)
    : svr_(svr), binds_(std::move(binds)), port_(port), size_(0), version_(-1), myself_(nullptr) {
  for (auto &slots_node : slots_nodes_) {
    slots_node = nullptr;
  }
}

// We access cluster without lock, actually we guarantee data-safe by work threads
// ReadWriteLockGuard, CLUSTER command doesn't have 'exclusive' attribute, i.e.
// CLUSTER command can be executed concurrently, but some subcommand may change
// cluster data, so these commands should be executed exclusively, and ReadWriteLock
// also can guarantee accessing data is safe.
bool Cluster::SubCommandIsExecExclusive(const std::string &subcommand) {
  if (strcasecmp("setnodes", subcommand.c_str()) == 0) {
    return true;
  } else if (strcasecmp("setnodeid", subcommand.c_str()) == 0) {
    return true;
  } else if (strcasecmp("setslot", subcommand.c_str()) == 0) {
    return true;
  } else if (strcasecmp("import", subcommand.c_str()) == 0) {
    return true;
  }
  return false;
}

Status Cluster::SetNodeId(const std::string &node_id) {
  if (node_id.size() != kClusterNodeIdLen) {
    return {Status::ClusterInvalidInfo, errInvalidNodeID};
  }

  myid_ = node_id;
  // Already has cluster topology
  if (version_ >= 0 && nodes_.find(node_id) != nodes_.end()) {
    myself_ = nodes_[myid_];
  } else {
    myself_ = nullptr;
  }

  // Set replication relationship
  if (myself_) return SetMasterSlaveRepl();

  return Status::OK();
}

// Set the slot to the node if new version is current version +1. It is useful
// when we scale cluster avoid too many big messages, since we only update one
// slot distribution and there are 16384 slot in our design.
//
// The reason why the new version MUST be +1 of current version is that,
// the command changes topology based on specific topology (also means specific
// version), we must guarantee current topology is exactly expected, otherwise,
// this update may make topology corrupt, so base topology version is very important.
// This is different with CLUSTERX SETNODES commands because it uses new version
// topology to cover current version, it allows kvrocks nodes lost some topology
// updates since of network failure, it is state instead of operation.

Status Cluster::SetSlots(std::vector<int> &slots, const std::string &node_id, int64_t version) {
  Status s;
  for (int slot : slots) {
    s = SetSlot(slot, node_id, version++);
    if (!s.IsOK()) {
      return s;
    }
  }
  return Status::OK();
}

Status Cluster::SetSlot(int slot, const std::string &node_id, int64_t new_version) {
  // Parameters check
  if (new_version <= 0 || new_version != version_ + 1) {
    return {Status::NotOK, errInvalidClusterVersion};
  }

  if (!IsValidSlot(slot)) {
    return {Status::NotOK, errInvalidSlotID};
  }

  if (node_id.size() != kClusterNodeIdLen) {
    return {Status::NotOK, errInvalidNodeID};
  }

  // Get the node which we want to assign a slot into it
  std::shared_ptr<ClusterNode> to_assign_node = nodes_[node_id];
  if (to_assign_node == nullptr) {
    return {Status::NotOK, "No this node in the cluster"};
  }

  if (to_assign_node->role_ != kClusterMaster) {
    return {Status::NotOK, errNoMasterNode};
  }

  // Update version
  version_ = new_version;

  // Update topology
  //  1. Remove the slot from old node if existing
  //  2. Add the slot into to-assign node
  //  3. Update the map of slots to nodes.
  std::shared_ptr<ClusterNode> old_node = slots_nodes_[slot];
  if (old_node != nullptr) {
    old_node->slots_[slot] = false;
  }
  to_assign_node->slots_[slot] = true;
  slots_nodes_[slot] = to_assign_node;

  // Clear data of migrated slot or record of imported slot
  if (old_node == myself_ && old_node != to_assign_node) {
    // If slot is migrated from this node
    if (migrated_slots_.count(slot) > 0) {
      svr_->slot_migrate_->ClearKeysOfSlot(kDefaultNamespace, slot);
      migrated_slots_.erase(slot);
    }
    // If slot is imported into this node
    if (imported_slots_.count(slot) > 0) {
      imported_slots_.erase(slot);
    }
  }

  return Status::OK();
}

// cluster setnodes $all_nodes_info $version $force
// one line of $all_nodes: $node_id $host $port $role $master_node_id $slot_range
Status Cluster::SetClusterNodes(const std::string &nodes_str, int64_t version, bool force) {
  if (version < 0) return {Status::NotOK, errInvalidClusterVersion};

  if (!force) {
    // Low version wants to reset current version
    if (version_ > version) {
      return {Status::NotOK, errInvalidClusterVersion};
    }

    // The same version, it is not needed to update
    if (version_ == version) return Status::OK();
  }

  ClusterNodes nodes;
  std::unordered_map<int, std::string> slots_nodes;
  Status s = ParseClusterNodes(nodes_str, &nodes, &slots_nodes);
  if (!s.IsOK()) return s;

  // Update version and cluster topology
  version_ = version;
  nodes_ = nodes;
  size_ = 0;

  // Update slots to nodes
  for (const auto &n : slots_nodes) {
    slots_nodes_[n.first] = nodes_[n.second];
  }

  // Update replicas info and size
  for (auto &n : nodes_) {
    if (n.second->role_ == kClusterSlave) {
      if (nodes_.find(n.second->master_id_) != nodes_.end()) {
        nodes_[n.second->master_id_]->replicas.push_back(n.first);
      }
    }
    if (n.second->role_ == kClusterMaster && n.second->slots_.count() > 0) {
      size_++;
    }
  }

  // Find myself
  if (myid_.empty() || force) {
    for (auto &n : nodes_) {
      if (n.second->port_ == port_ && std::find(binds_.begin(), binds_.end(), n.second->host_) != binds_.end()) {
        myid_ = n.first;
        break;
      }
    }
  }
  myself_ = nullptr;
  if (!myid_.empty() && nodes_.find(myid_) != nodes_.end()) {
    myself_ = nodes_[myid_];
  }

  // Set replication relationship
  if (myself_) {
    s = SetMasterSlaveRepl();
    if (!s.IsOK()) {
      return s.Prefixed("failed to set master-replica replication");
    }
  }

  // Clear data of migrated slots
  if (!migrated_slots_.empty()) {
    for (auto &it : migrated_slots_) {
      if (slots_nodes_[it.first] != myself_) {
        svr_->slot_migrate_->ClearKeysOfSlot(kDefaultNamespace, it.first);
      }
    }
  }
  // Clear migrated and imported slot info
  migrated_slots_.clear();
  imported_slots_.clear();

  return Status::OK();
}

// Set replication relationship by cluster topology setting
Status Cluster::SetMasterSlaveRepl() {
  if (!svr_) return Status::OK();

  if (!myself_) return Status::OK();

  if (myself_->role_ == kClusterMaster) {
    // Master mode
    auto s = svr_->RemoveMaster();
    if (!s.IsOK()) {
      return s.Prefixed("failed to remove master");
    }
    LOG(INFO) << "MASTER MODE enabled by cluster topology setting";
  } else if (nodes_.find(myself_->master_id_) != nodes_.end()) {
    // Replica mode and master node is existing
    std::shared_ptr<ClusterNode> master = nodes_[myself_->master_id_];
    auto s = svr_->AddMaster(master->host_, master->port_, false);
    if (!s.IsOK()) {
      LOG(WARNING) << "SLAVE OF " << master->host_ << ":" << master->port_
                   << " wasn't enabled by cluster topology setting, encounter error: " << s.Msg();
      return s.Prefixed("failed to add master");
    }
    LOG(INFO) << "SLAVE OF " << master->host_ << ":" << master->port_ << " enabled by cluster topology setting";
  }

  return Status::OK();
}

bool Cluster::IsNotMaster() { return myself_ == nullptr || myself_->role_ != kClusterMaster || svr_->IsSlave(); }

Status Cluster::SetSlotMigrated(int slot, const std::string &ip_port) {
  if (!IsValidSlot(slot)) {
    return {Status::NotOK, errSlotOutOfRange};
  }

  // It is called by slot-migrating thread which is an asynchronous thread.
  // Therefore, it should be locked when a record is added to 'migrated_slots_'
  // which will be accessed when executing commands.
  auto exclusivity = svr_->WorkExclusivityGuard();
  migrated_slots_[slot] = ip_port;
  return Status::OK();
}

Status Cluster::SetSlotImported(int slot) {
  if (!IsValidSlot(slot)) {
    return {Status::NotOK, errSlotOutOfRange};
  }

  // It is called by command 'cluster import'. When executing the command, the
  // exclusive lock has been locked. Therefore, it can't be locked again.
  imported_slots_.insert(slot);
  return Status::OK();
}

Status Cluster::MigrateSlot(int slot, const std::string &dst_node_id) {
  Status s = ValidateMigrateSlot(slot, dst_node_id);
  if (!s.IsOK()) return s;
  const auto dst = nodes_[dst_node_id];
  s = svr_->slot_migrate_->MigrateStart(svr_, dst_node_id, dst->host_, dst->port_, slot,
                                        svr_->GetConfig()->migrate_speed, svr_->GetConfig()->pipeline_size,
                                        svr_->GetConfig()->sequence_gap, true);

  return s;
}

Status Cluster::ImportSlot(Redis::Connection *conn, int slot, int state, bool batched) {
  if (IsNotMaster()) {
    return {Status::NotOK, "Slave can't import slot"};
  }

  if (!IsValidSlot(slot)) {
    return {Status::NotOK, errSlotOutOfRange};
  }
  if (svr_->slot_import_map.count(slot) == 0) {
    svr_->slot_import_map.emplace(slot, std::make_unique<SlotImport>(svr_));
  }

  auto p_slot_import = svr_->slot_import_map[slot].get();

  switch (state) {
    case kImportStart:
      if (!p_slot_import->Start(conn->GetFD(), slot)) {
        return {Status::NotOK, fmt::format("Can't start importing slot {}", slot)};
      }
      // Set link importing
      conn->SetImporting();
      myself_->importing_slots_.insert(slot);
      // Set link error callback
      conn->close_cb_ = [object_ptr = p_slot_import, capture_fd = conn->GetFD()](int fd) {
        object_ptr->StopForLinkError(capture_fd);
      };
      // Stop forbidding writing slot to accept write commands
      if (slot == svr_->slot_migrate_->GetForbiddenSlot()) svr_->slot_migrate_->ReleaseForbiddenSlot();
      LOG(INFO) << "[import] Start importing slot " << slot;
      break;
    case kImportSuccess:
      if (!p_slot_import->Success(slot)) {
        LOG(ERROR) << "[import] Failed to set slot importing success, maybe slot is wrong"
                   << ", received slot: " << slot << ", current slot: " << p_slot_import->GetSlot();
        return {Status::NotOK, fmt::format("Failed to set slot {} importing success", slot)};
      }
      myself_->importing_slots_.erase(slot);
      svr_->slot_import_map.erase(slot);
      LOG(INFO) << "[import] Succeed to import slot " << slot;
      break;
    case kImportFailed:
      if (!p_slot_import->Fail(slot)) {
        LOG(ERROR) << "[import] Failed to set slot importing error, maybe slot is wrong"
                   << ", received slot: " << slot << ", current slot: " << p_slot_import->GetSlot();
        return {Status::NotOK, fmt::format("Failed to set slot {} importing error", slot)};
      }
      myself_->importing_slots_.erase(slot);
      svr_->slot_import_map.erase(slot);
      LOG(INFO) << "[import] Failed to import slot " << slot;
      break;
    default:
      return {Status::NotOK, errInvalidImportState};
  }

  return Status::OK();
}

Status Cluster::GetClusterInfo(std::string *cluster_infos) {
  if (version_ < 0) {
    return {Status::ClusterDown, errClusterNoInitialized};
  }

  cluster_infos->clear();

  int ok_slot = 0;
  for (auto &slots_node : slots_nodes_) {
    if (slots_node != nullptr) ok_slot++;
  }

  *cluster_infos =
      "cluster_state:ok\r\n"
      "cluster_slots_assigned:" +
      std::to_string(ok_slot) +
      "\r\n"
      "cluster_slots_ok:" +
      std::to_string(ok_slot) +
      "\r\n"
      "cluster_slots_pfail:0\r\n"
      "cluster_slots_fail:0\r\n"
      "cluster_known_nodes:" +
      std::to_string(nodes_.size()) +
      "\r\n"
      "cluster_size:" +
      std::to_string(size_) +
      "\r\n"
      "cluster_current_epoch:" +
      std::to_string(version_) +
      "\r\n"
      "cluster_my_epoch:" +
      std::to_string(version_) + "\r\n";

  if (myself_ != nullptr && myself_->role_ == kClusterMaster && !svr_->IsSlave()) {
    // Get migrating status
    std::string migrate_infos;
    svr_->slot_migrate_->GetMigrateInfo(&migrate_infos);
    *cluster_infos += migrate_infos;

    // Get importing status
    std::string import_infos;
    for (const auto &pair : svr_->slot_import_map) {
      pair.second.get()->GetImportInfo(&import_infos);
      *cluster_infos += import_infos;
    }
  }

  return Status::OK();
}

// Format: 1) 1) start slot
//            2) end slot
//            3) 1) master IP
//               2) master port
//               3) node ID
//            4) 1) replica IP
//               2) replica port
//               3) node ID
//          ... continued until done
Status Cluster::GetSlotsInfo(std::vector<SlotInfo> *slots_infos) {
  if (version_ < 0) {
    return {Status::ClusterDown, errClusterNoInitialized};
  }

  slots_infos->clear();

  int start = -1;
  std::shared_ptr<ClusterNode> n = nullptr;
  for (int i = 0; i <= kClusterSlots; i++) {
    // Find start node and slot id
    if (n == nullptr) {
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
      continue;
    }
    // Generate slots info when occur different node with start or end of slot
    if (i == kClusterSlots || n != slots_nodes_[i]) {
      slots_infos->emplace_back(GenSlotNodeInfo(start, i - 1, n));
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
    }
  }

  return Status::OK();
}

SlotInfo Cluster::GenSlotNodeInfo(int start, int end, const std::shared_ptr<ClusterNode> &n) {
  std::vector<SlotInfo::NodeInfo> vn;
  vn.push_back({n->host_, n->port_, n->id_});  // itself

  for (const auto &id : n->replicas) {         // replicas
    if (nodes_.find(id) == nodes_.end()) continue;
    vn.push_back({nodes_[id]->host_, nodes_[id]->port_, nodes_[id]->id_});
  }

  return {start, end, vn};
}

// $node $host:$port@$cport $role $master_id/$- $ping_sent $ping_received
// $version $connected $slot_range
Status Cluster::GetClusterNodes(std::string *nodes_str) {
  if (version_ < 0) {
    return {Status::ClusterDown, errClusterNoInitialized};
  }

  *nodes_str = GenNodesDescription();
  return Status::OK();
}

std::string Cluster::GenNodesDescription() {
  UpdateSlotsInfo();

  auto now = Util::GetTimeStampMS();
  std::string nodes_desc;
  for (const auto &item : nodes_) {
    const std::shared_ptr<ClusterNode> n = item.second;

    std::string node_str;
    // ID, host, port
    node_str.append(n->id_ + " ");
    node_str.append(fmt::format("{}:{}@{} ", n->host_, n->port_, n->port_ + kClusterPortIncr));

    // Flags
    if (n->id_ == myid_) node_str.append("myself,");
    if (n->role_ == kClusterMaster) {
      node_str.append("master - ");
    } else {
      node_str.append("slave " + n->master_id_ + " ");
    }

    // Ping sent, pong received, config epoch, link status
    node_str.append(fmt::format("{} {} {} connected", now - 1, now, version_));

    if (n->role_ == kClusterMaster && n->slots_info_.size() > 0) {
      node_str.append(" " + n->slots_info_);
    }

    nodes_desc.append(node_str + "\n");
  }
  return nodes_desc;
}

void Cluster::UpdateSlotsInfo() {
  int start = -1;
  // reset the previous slots info
  for (const auto &item : nodes_) {
    const std::shared_ptr<ClusterNode> &n = item.second;
    n->slots_info_.clear();
  }

  std::shared_ptr<ClusterNode> n = nullptr;
  for (int i = 0; i <= kClusterSlots; i++) {
    // Find start node and slot id
    if (n == nullptr) {
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
      continue;
    }
    // Generate slots info when occur different node with start or end of slot
    if (i == kClusterSlots || n != slots_nodes_[i]) {
      if (start == i - 1) {
        n->slots_info_ += fmt::format("{} ", start);
      } else {
        n->slots_info_ += fmt::format("{}-{} ", start, i - 1);
      }
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
    }
  }

  for (const auto &item : nodes_) {
    const std::shared_ptr<ClusterNode> n = item.second;
    if (n->slots_info_.size() > 0) n->slots_info_.pop_back();  // Remove last space
  }
}

std::string Cluster::GenNodesInfo() {
  UpdateSlotsInfo();

  std::string nodes_info;
  for (const auto &item : nodes_) {
    const std::shared_ptr<ClusterNode> &n = item.second;
    std::string node_str;
    node_str.append("node ");
    // ID
    node_str.append(n->id_ + " ");
    // Host + Port
    node_str.append(fmt::format("{} {} ", n->host_, n->port_));

    // Role
    if (n->role_ == kClusterMaster) {
      node_str.append("master - ");
    } else {
      node_str.append("slave " + n->master_id_ + " ");
    }

    // Slots
    if (n->role_ == kClusterMaster && n->slots_info_.size() > 0) {
      node_str.append(" " + n->slots_info_);
    }
    nodes_info.append(node_str + "\n");
  }
  return nodes_info;
}

Status Cluster::DumpClusterNodes(const std::string &file) {
  // Parse and validate the cluster nodes string before dumping into file
  std::string tmp_path = file + ".tmp";
  remove(tmp_path.data());
  std::ofstream output_file(tmp_path, std::ios::out);
  output_file << fmt::format("version {}\n", version_);
  output_file << fmt::format("id {}\n", myid_);
  output_file << GenNodesInfo();
  output_file.close();
  if (rename(tmp_path.data(), file.data()) < 0) {
    return {Status::NotOK, fmt::format("rename file encounter error: {}", strerror(errno))};
  }
  return Status::OK();
}

Status Cluster::LoadClusterNodes(const std::string &file_path) {
  if (rocksdb::Env::Default()->FileExists(file_path).IsNotFound()) {
    LOG(INFO) << fmt::format("The cluster nodes file {} is not found. Use CLUSTERX subcommands to specify it.",
                             file_path);
    return Status::OK();
  }

  std::ifstream file;
  file.open(file_path);
  if (!file.is_open()) {
    return {Status::NotOK, fmt::format("error opening the file '{}': {}", file_path, strerror(errno))};
  }

  int64_t version = -1;
  std::string id, nodesInfo;
  std::string line;
  while (!file.eof()) {
    std::getline(file, line);

    auto parsed = ParseConfigLine(line);
    if (!parsed) return parsed.ToStatus().Prefixed("malformed line");
    if (parsed->first.empty() || parsed->second.empty()) continue;

    auto key = parsed->first;
    if (key == "version") {
      auto parse_result = ParseInt<int64_t>(parsed->second, 10);
      if (!parse_result) {
        return {Status::NotOK, errInvalidClusterVersion};
      }
      version = *parse_result;
    } else if (key == "id") {
      id = parsed->second;
      if (id.length() != kClusterNodeIdLen) {
        return {Status::NotOK, errInvalidNodeID};
      }
    } else if (key == "node") {
      nodesInfo.append(parsed->second + "\n");
    } else {
      return {Status::NotOK, fmt::format("unknown key: {}", key)};
    }
  }
  return SetClusterNodes(nodesInfo, version, false);
}

Status Cluster::ParseClusterNodes(const std::string &nodes_str, ClusterNodes *nodes,
                                  std::unordered_map<int, std::string> *slots_nodes) {
  std::vector<std::string> nodes_info = Util::Split(nodes_str, "\n");
  if (nodes_info.size() == 0) {
    return {Status::ClusterInvalidInfo, errInvalidClusterNodeInfo};
  }

  nodes->clear();

  // Parse all nodes
  for (const auto &node_str : nodes_info) {
    std::vector<std::string> fields = Util::Split(node_str, " ");
    if (fields.size() < 5) {
      return {Status::ClusterInvalidInfo, errInvalidClusterNodeInfo};
    }

    // 1) node id
    if (fields[0].size() != kClusterNodeIdLen) {
      return {Status::ClusterInvalidInfo, errInvalidNodeID};
    }

    std::string id = fields[0];

    // 2) host, TODO(@shooterit): check host is valid
    std::string host = fields[1];

    // 3) port
    auto parse_result = ParseInt<uint16_t>(fields[2], 10);
    if (!parse_result) {
      return {Status::ClusterInvalidInfo, "Invalid cluster node port"};
    }

    int port = *parse_result;

    // 4) role
    int role = 0;
    if (strcasecmp(fields[3].c_str(), "master") == 0) {
      role = kClusterMaster;
    } else if (strcasecmp(fields[3].c_str(), "slave") == 0 || strcasecmp(fields[3].c_str(), "replica") == 0) {
      role = kClusterSlave;
    } else {
      return {Status::ClusterInvalidInfo, "Invalid cluster node role"};
    }

    // 5) master id
    std::string master_id = fields[4];
    if ((role == kClusterMaster && master_id != "-") ||
        (role == kClusterSlave && master_id.size() != kClusterNodeIdLen)) {
      return {Status::ClusterInvalidInfo, errInvalidNodeID};
    }

    std::bitset<kClusterSlots> slots;
    if (role == kClusterSlave) {
      if (fields.size() != 5) {
        return {Status::ClusterInvalidInfo, errInvalidClusterNodeInfo};
      } else {
        // Create slave node
        (*nodes)[id] = std::make_shared<ClusterNode>(id, host, port, role, master_id, slots);
        continue;
      }
    }

    // 6) slot info
    auto valid_range = NumericRange<int>{0, kClusterSlots - 1};
    for (unsigned i = 5; i < fields.size(); i++) {
      std::vector<std::string> ranges = Util::Split(fields[i], "-");
      if (ranges.size() == 1) {
        auto parse_start = ParseInt<int>(ranges[0], valid_range, 10);
        if (!parse_start) {
          return {Status::ClusterInvalidInfo, errSlotOutOfRange};
        }

        int start = *parse_start;
        slots.set(start, true);
        if (role == kClusterMaster) {
          if (slots_nodes->find(start) != slots_nodes->end()) {
            return {Status::ClusterInvalidInfo, errSlotOverlapped};
          } else {
            (*slots_nodes)[start] = id;
          }
        }
      } else if (ranges.size() == 2) {
        auto parse_start = ParseInt<int>(ranges[0], valid_range, 10);
        auto parse_stop = ParseInt<int>(ranges[1], valid_range, 10);
        if (!parse_start || !parse_stop || *parse_start >= *parse_stop) {
          return {Status::ClusterInvalidInfo, errSlotOutOfRange};
        }

        int start = *parse_start;
        int stop = *parse_stop;
        for (int j = start; j <= stop; j++) {
          slots.set(j, true);
          if (role == kClusterMaster) {
            if (slots_nodes->find(j) != slots_nodes->end()) {
              return {Status::ClusterInvalidInfo, errSlotOverlapped};
            } else {
              (*slots_nodes)[j] = id;
            }
          }
        }
      } else {
        return {Status::ClusterInvalidInfo, errSlotOutOfRange};
      }
    }

    // Create master node
    (*nodes)[id] = std::make_shared<ClusterNode>(id, host, port, role, master_id, slots);
  }

  return Status::OK();
}

bool Cluster::IsWriteForbiddenSlot(int slot) { return svr_->slot_migrate_->GetForbiddenSlot() == slot; }

Status Cluster::CanExecByMySelf(const Redis::CommandAttributes *attributes, const std::vector<std::string> &cmd_tokens,
                                Redis::Connection *conn) {
  if (!svr_->storage_->GetDB() || svr_->storage_->IsClosing())
    return {Status::RedisExecErr, "TRYAGAIN DB pointer is empty"};
  std::vector<int> keys_indexes;
  auto s = Redis::GetKeysFromCommand(attributes->name, static_cast<int>(cmd_tokens.size()), &keys_indexes);
  // No keys
  if (!s.IsOK()) return Status::OK();

  if (keys_indexes.size() == 0) return Status::OK();

  int slot = -1;
  for (auto i : keys_indexes) {
    if (i >= static_cast<int>(cmd_tokens.size())) break;

    int cur_slot = GetSlotNumFromKey(cmd_tokens[i]);
    if (slot == -1) slot = cur_slot;
    if (slot != cur_slot) {
      return {Status::RedisExecErr, "CROSSSLOT Attempted to access keys that don't hash to the same slot"};
    }
  }
  if (slot == -1) return Status::OK();

  if (slots_nodes_[slot] == nullptr) {
    return {Status::ClusterDown, "CLUSTERDOWN Hash slot not served"};
  }

  if (myself_ && myself_ == slots_nodes_[slot]) {
    // We use central controller to manage the topology of the cluster.
    // Server can't change the topology directly, so we record the migrated slots
    // to move the requests of the migrated slots to the destination node.
    if (migrated_slots_.count(slot) > 0) {  // I'm not serving the migrated slot
      return {Status::RedisExecErr, fmt::format("MOVED {} {}", slot, migrated_slots_[slot])};
    }
    // To keep data consistency, slot will be forbidden write while sending the last incremental data.
    // During this phase, the requests of the migrating slot has to be rejected.
    if (attributes->is_write() && IsWriteForbiddenSlot(slot)) {
      return {Status::RedisExecErr, "TRYAGAIN Can't write to slot being migrated which is in write forbidden phase"};
    }
    SetSlotAccessed(slot);
    return Status::OK();  // I'm serving this slot
  }

  if (myself_ && myself_->importing_slots_.count(slot) && conn->IsImporting()) {
    // While data migrating, the topology of the destination node has not been changed.
    // The destination node has to serve the requests from the migrating slot,
    // although the slot is not belong to itself. Therefore, we record the importing slot
    // and mark the importing connection to accept the importing data.
    //    LOG(INFO) << "This connection is serving Import";
    SetSlotAccessed(slot);
    return Status::OK();  // I'm serving the importing connection
  }

  if (myself_ && imported_slots_.count(slot)) {
    // After the slot is migrated, new requests of the migrated slot will be moved to
    // the destination server. Before the central controller change the topology, the destination
    // server should record the imported slots to accept new data of the imported slots.
    SetSlotAccessed(slot);
    return Status::OK();  // I'm serving the imported slot
  }

  if (myself_ && myself_->role_ == kClusterSlave && !attributes->is_write() &&
      nodes_.find(myself_->master_id_) != nodes_.end() && nodes_[myself_->master_id_] == slots_nodes_[slot]) {
    return Status::OK();  // My master is serving this slot
  }
  //  LOG(INFO) << "Trying to find slot: " << slot << ", Forbidden slot is: " <<
  //  svr_->slot_migrate_->GetForbiddenSlot();
  return {Status::RedisExecErr,
          fmt::format("MOVED {} {}:{}", slot, slots_nodes_[slot]->host_, slots_nodes_[slot]->port_)};
}

Status Cluster::MigrateSlots(std::vector<int> &slots, const std::string &dst_node_id) {
  Status s;
  auto dst = nodes_[dst_node_id];
  std::vector<std::future<Status>> results;
  for (auto it = slots.begin(); it != slots.end(); ++it) {
    s = ValidateMigrateSlot(*it, dst_node_id);
    if (!s.IsOK()) {
      LOG(WARNING) << "Slot " << *it << " is not valid for migration";
      slots.erase(it);
    }
  }
  if (slots.empty()) {
    return {Status::NotOK, "All slots are not valid"};
  }

  switch (svr_->GetConfig()->migrate_method) {
    case kSeekAndInsert: {
      return {Status::NotOK, "This migration method does not support multi-slot migration"};
    }
    case kSeekAndInsertBatched:
    case kSeekAndIngestion:
    case kCompactAndMerge:
    case kLevelMigration: {
      // strange bug, this server can not get the selected migration method
      std::cout << "function: Migrate " << this->svr_->slot_migrate_->GetName() << std::endl;
      s = this->svr_->slot_migrate_->SetMigrationSlots(slots);
      if (!s.IsOK()) return s;
      s = svr_->slot_migrate_->MigrateStart(svr_, dst_node_id, dst->host_, dst->port_, svr_->GetConfig()->sequence_gap,
                                            false);
      if (!s.IsOK()) return s;
      break;
    }
    default:  // kCompactAndMerge = 2, kLevelMigration = 3, these two method do not support single slot migration
      return {Status::NotOK, "This migration method does not support single slot migration"};
  }

  LOG(INFO) << "[cluster migration] Finished slot migration cmds sending, start to set nodes";
  // Migration succeed, set slots.
  return Status::OK();
}
Status Cluster::FetchFileFromRemote(const std::string &server_id, std::vector<std::string> &file_list,
                                    const std::string &temp_dir) {
  if (nodes_.find(server_id) == nodes_.end()) {
    return {Status::NotOK, "Can't find the destination node id"};
  }
  auto target_server = nodes_[server_id];
  auto port = target_server->port_;
  auto ip = target_server->host_;

  size_t concurrency = 4;
  //  if (file_list.size() > 20) {
  //    // Use 4 threads to download files in parallel
  //    concurrency = 4;
  //  }
  std::string file_str = "[ ";
  for (auto &file : file_list) {
    file_str += file + ",";
  }
  file_str.pop_back();
  file_str += "]";

  LOG(INFO) << "Start Fetching Files, # of SSTs:" << file_list.size() << ", max fetching threads: " << concurrency;

  std::atomic<uint32_t> fetch_cnt = {0};
  std::atomic<uint32_t> skip_cnt = {0};
  std::vector<std::future<Status>> results;
  for (size_t tid = 0; tid < concurrency; ++tid) {
    results.push_back(
        std::async(std::launch::async,
                   [this, temp_dir, &file_list, tid, concurrency, &fetch_cnt, &skip_cnt, ip, port]() -> Status {
                     LOG(INFO) << "Fetching file from remote, address: " << ip << ":" << port;
                     int sock_fd = GET_OR_RET(Util::SockConnect(ip, port).Prefixed("connect the server err"));
                     UniqueFD unique_fd{sock_fd};
                     auto s = this->svr_->slot_migrate_->SendAuth(sock_fd);
                     if (!s.IsOK()) {
                       return s.Prefixed("send the auth command err");
                     }
                     std::vector<std::string> fetch_files;
                     for (auto f_idx = tid; f_idx < file_list.size(); f_idx += concurrency) {
                       const auto &f_name = file_list[f_idx];

                       fetch_files.push_back(f_name);
                     }
                     unsigned files_count = file_list.size();
                     fetch_file_callback fn = [&fetch_cnt, &skip_cnt, files_count](const std::string &fetch_file,
                                                                                   const uint32_t fetch_crc) {
                       fetch_cnt.fetch_add(1);
                       uint32_t cur_skip_cnt = skip_cnt.load();
                       uint32_t cur_fetch_cnt = fetch_cnt.load();
                       LOG(INFO) << "[fetch] "
                                 << "Fetched " << fetch_file << ", crc32: " << fetch_crc
                                 << ", skip count: " << cur_skip_cnt << ", fetch count: " << cur_fetch_cnt
                                 << ", progress: " << cur_skip_cnt + cur_fetch_cnt << "/" << files_count;
                     };

                     if (!fetch_files.empty()) {
                       s = this->fetchFiles(sock_fd, temp_dir, fetch_files, fn);
                     }
                     return s;
                   }));
  }

  // Wait till finish
  for (auto &f : results) {
    Status s = f.get();
    if (!s.IsOK()) {
      LOG(ERROR) << "Receive file error: " << s.Msg();
      return s;
    }
  }
  return Status::OK();
}
Status Cluster::IngestFiles(const std::string &column_family, const std::vector<std::string> &files, bool fast_ingest,
                            int target_level) {
  auto cfh = svr_->storage_->GetCFHandle(column_family);
  rocksdb::Options opt = svr_->storage_->GetDB()->GetOptions();
  rocksdb::IngestExternalFileOptions ifo;
  if (fast_ingest) {
    ifo.sub_tier_mode = true;
    ifo.sub_tier_base = target_level;
  } else {
    ifo.allow_global_seqno = true;
    ifo.write_global_seqno = true;
    ifo.verify_checksums_before_ingest = false;
    ifo.ingest_behind = false;
  }

  //  ing_options.move_files = true;
  auto start = Util::GetTimeStampUS();
  // Read and Extract Records

  auto rocks_s = svr_->storage_->GetDB()->IngestExternalFile(cfh, files, ifo);
  auto end = Util::GetTimeStampUS();

  if (!rocks_s.ok()) {
    LOG(INFO) << "Ingestion error, Time taken(us): " << end - start;
    //    return Status::OK();
    return {Status::NotOK, "Ingestion error" + rocks_s.ToString()};
  }
  LOG(INFO) << "Ingestion completed, Time taken(us): " << end - start;
  return Status::OK();
}
int Cluster::OpenDataFileForMigrate(const std::string &remote_file_name, uint64_t *file_size) {
  return svr_->slot_migrate_->OpenDataFile(remote_file_name, file_size);
}
Status Cluster::ValidateMigrateSlot(int slot, const std::string &dst_node_id) {
  if (nodes_.find(dst_node_id) == nodes_.end()) {
    return {Status::NotOK, "Can't find the destination node id"};
  }

  if (!IsValidSlot(slot)) {
    return {Status::NotOK, "slot: " + std::to_string(slot) + " is out of range"};
  }

  if (slots_nodes_[slot] != myself_) {
    return {Status::NotOK, "Can't migrate slot which doesn't belong to me"};
  }

  if (IsNotMaster()) {
    return {Status::NotOK, "Slave can't migrate slot"};
  }

  if (nodes_[dst_node_id]->role_ != kClusterMaster) {
    return {Status::NotOK, "Can't migrate slot to a slave"};
  }

  if (nodes_[dst_node_id] == myself_) {
    return {Status::NotOK, "Can't migrate slot to myself"};
  }
  return Status::OK();
}
Status Cluster::fetchFiles(int sock_fd, const std::string &dir, const std::vector<std::string> &files,
                           const fetch_file_callback &fn) {
  std::string files_str;
  for (const auto &file : files) {
    files_str += file;
    files_str.push_back(',');
  }
  files_str.pop_back();

  const auto fetch_command = Redis::MultiBulkString({"fetch_remote_sst", files_str});
  auto s = Util::SockSend(sock_fd, fetch_command);
  if (!s.IsOK()) return s.Prefixed("send fetch file command");

  UniqueEvbuf evbuf;
  for (const auto &file : files) {
    auto start = Util::GetTimeStampUS();
    LOG(INFO) << "[fetch] Start to fetch file " << file;
    s = fetchFile(sock_fd, evbuf.get(), dir, file, fn);
    if (!s.IsOK()) {
      s = Status(Status::NotOK, "fetch file err: " + s.Msg());
      LOG(WARNING) << "[fetch] Fail to fetch file " << file << ", err: " << s.Msg();
      break;
    }
    auto end = Util::GetTimeStampUS();
    LOG(INFO) << "[fetch] Succeed fetching file " << file << ", Time taken(us): " << end - start;
    // Just for tests
    if (svr_->GetConfig()->fullsync_recv_file_delay) {
      sleep(svr_->GetConfig()->fullsync_recv_file_delay);
    }
  }
  return s;
}
Status Cluster::fetchFile(int sock_fd, evbuffer *evbuf, const std::string &dir, const std::string &file,
                          const fetch_file_callback &fn) {
  size_t file_size = 0;

  // Read file size line
  while (true) {
    UniqueEvbufReadln line(evbuf, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) {
      if (evbuffer_read(evbuf, sock_fd, -1) <= 0) {
        return {Status::NotOK, fmt::format("read size: {}", strerror(errno))};
      }
      continue;
    }
    if (line[0] == '-') {
      std::string msg(line.get());
      return {Status::NotOK, msg};
    }
    file_size = line.length > 0 ? std::strtoull(line.get(), nullptr, 10) : 0;
    break;
  }

  // Write to tmp file
  auto tmp_file = Engine::Storage::ReplDataManager::NewTmpFile(svr_->storage_, dir, file);
  if (!tmp_file) {
    return {Status::NotOK, "unable to create tmp file"};
  }

  size_t remain = file_size;
  uint32_t tmp_crc = 0;
  char data[16 * 1024];
  while (remain != 0) {
    if (evbuffer_get_length(evbuf) > 0) {
      auto data_len = evbuffer_remove(evbuf, data, remain > 16 * 1024 ? 16 * 1024 : remain);
      if (data_len == 0) continue;
      if (data_len < 0) {
        return {Status::NotOK, "read sst file data error"};
      }
      tmp_file->Append(rocksdb::Slice(data, data_len));
      tmp_crc = rocksdb::crc32c::Extend(tmp_crc, data, data_len);
      remain -= data_len;
    } else {
      if (evbuffer_read(evbuf, sock_fd, -1) <= 0) {
        return {Status::NotOK, fmt::format("read sst file: {}", strerror(errno))};
      }
    }
  }
  // move tmp file to file
  auto s = Engine::Storage::ReplDataManager::SwapTmpFile(svr_->storage_, dir, file);
  if (!s.IsOK()) return {Status::NotOK, "Mv Tmp error"};
  return Status::OK();
}
void Cluster::SetSlotAccessed(int slot) { svr_->slot_hotness_map_[slot]++; }
std::string Cluster::GetServerHotness() { return svr_->GetHotnessJson(); }