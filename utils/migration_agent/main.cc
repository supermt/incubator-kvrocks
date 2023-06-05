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

#include <event2/thread.h>
#include <fcntl.h>
#include <getopt.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>

#include <csignal>

#include "config.h"
#include "config/config.h"
#include "io_util.h"
#include "parser.h"
#include "storage/storage.h"
#include "version.h"

const char *kDefaultConfPath = "./migration_agent.conf";

DEFINE_string(work_dir, "/tmp/migration_sync/", "root_directory  or hdfs://");
DEFINE_string(src_uri, "./", "root_directory  or hdfs://");
DEFINE_string(dst_uri, "./", "root_directory  or hdfs://");
DEFINE_string(src_info, "127.0.0.1:40001@node_40001/db/", "");
DEFINE_string(dst_info, "127.0.0.1:40002@node_40002/db/", "");
DEFINE_string(slot_str, "0,1,2,3,4,5,6,7,8,9,10,", "the slot number id list, like: 1,2,3,4");
DEFINE_int64(pull_method, 0, "How to do the pull-based method, 0 for compact-and-merge, 1 for seek-and-ingest");
DEFINE_string(namespace_str, "", "The namespace of remote server");
DEFINE_string(migration_user, "jinghua2", "The user name of remote server");
DEFINE_string(candidate_ssts, "", "");

std::function<void()> hup_handler;

struct Options {
  //  std::string conf_file = kDefaultConfPath;
  std::string src_info = "";  // 127.0.0.1:40001@node1/asdf/
  std::string dst_info = "";  // 127.0.0.1:40002@node1/asdf/
  std::string prefix = "./";  // ./ or hdfs://
  std::string slot_str = "";  // 1,23,45,65

  bool show_usage = false;
};

extern "C" void signal_handler(int sig) {
  if (hup_handler) hup_handler();
}

static void initGoogleLog(const MigrationAgent::Config *config) {
  FLAGS_minloglevel = config->loglevel;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;
  FLAGS_log_dir = config->work_dir;
}

Server *GetServer() { return nullptr; }

int main(int argc, char *argv[]) {
  google::InitGoogleLogging("migration_agent");
  evthread_use_pthreads();

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  std::cout << "Version: " << VERSION << " @" << GIT_COMMIT << std::endl;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  MigrationAgent::Config agent_config;
  auto temp = Util::Split(FLAGS_src_info, "@");
  auto host_and_ip = Util::Split(temp[0], ":");
  agent_config.remote_username = FLAGS_migration_user;
  agent_config.src_server_host = host_and_ip[0];
  {
    auto temp_s = ParseInt<std::uint16_t>(host_and_ip[1]);
    if (!temp_s.IsOK()) exit(-1);
    agent_config.src_server_port = *temp_s;
  }

  agent_config.src_db_dir = FLAGS_src_uri + temp[1];

  temp = Util::Split(FLAGS_dst_info, "@");
  host_and_ip = Util::Split(temp[0], ":");
  agent_config.src_server_host = host_and_ip[0];
  {
    auto temp_s = ParseInt<std::uint16_t>(host_and_ip[1]);
    if (!temp_s.IsOK()) exit(-1);
    agent_config.src_server_port = *temp_s;
  }

  agent_config.dst_db_dir = FLAGS_dst_uri + temp[1];

  agent_config.work_dir = FLAGS_work_dir;
  //      ET_OR_RET(ParseInt<std::uint16_t>(host_and_ip[1]));
  temp = Util::Split(FLAGS_slot_str, ",");
  for (const auto &slot : temp) {
    if (!slot.empty()) agent_config.slot_list.push_back(std::stoi(slot));
  }
  std::cout << agent_config.ToString() << std::endl;

  for (auto str : Util::Split(FLAGS_candidate_ssts, ",")) {
    agent_config.input_files.push_back(str);
  }

  initGoogleLog(&agent_config);

  if (agent_config.src_db_dir.back() != '/') agent_config.src_db_dir += '/';
  if (agent_config.dst_db_dir.back() != '/') agent_config.dst_db_dir += '/';

  Config src_config;
  src_config.db_dir = agent_config.src_db_dir;
  src_config.slot_id_encoded = true;
  src_config.sec_dir = src_config.db_dir + "/tmp/";
  Engine::Storage src_sst_store(&src_config);

  auto s = src_sst_store.Open(FLAGS_pull_method == 1);
  if (!s.IsOK()) {
    std::cout << "Failed to open storage: " << s.Msg();
    LOG(ERROR) << "Failed to open Kvrocks storage: " << s.Msg();
    exit(-1);
  }
  Parser sst_reader(&src_sst_store, agent_config.slot_list, agent_config.work_dir, FLAGS_namespace_str, agent_config);
  //  std::vector<std::string> slot_prefix_list;
  //  for (auto slot_prefix : slot_prefix_list) {
  //  }
  if (FLAGS_pull_method == 0) {
    s = sst_reader.CompactAndMerge();
  } else {
    s = sst_reader.SeekAndDump();
  }

  if (!s.IsOK()) {
    std::cout << "Migration error!" << s.Msg();
    src_sst_store.CloseDB();
    return -1;
  }

  src_sst_store.CloseDB();
  return 0;
}
