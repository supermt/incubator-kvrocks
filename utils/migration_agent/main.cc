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

static void usage(const char *program) {
  std::cout << program << " sync kvrocks to redis\n"
            << "\t-s src_host:src_port@db_path,e.g., 127.0.0.1:40001@node1/asdf/\n"
            << "\t-h help\n";
  exit(0);
}

static Options parseCommandLineOptions(int argc, char **argv) {
  int ch = 0;
  Options opts;
  while ((ch = ::getopt(argc, argv, "c:h")) != -1) {
    switch (ch) {
      case 's': {
        opts.src_info = optarg;
        break;
      }
      case 'd': {
        opts.dst_info = optarg;
        break;
      }
      case 'p': {
        opts.prefix = optarg;
        break;
      }
      case 'l': {
        opts.slot_str = optarg;
        break;
      }
      case 'h': {
        opts.show_usage = true;
        break;
      }
      default:
        usage(argv[0]);
    }
  }
  return opts;
}

static void initGoogleLog(const MigrationAgent::Config *config) {
  FLAGS_minloglevel = config->loglevel;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;
  FLAGS_log_dir = config->work_dir;
}

static Status createPidFile(const std::string &path) {
  int fd = open(path.data(), O_RDWR | O_CREAT | O_EXCL, 0660);
  if (fd < 0) {
    return {Status::NotOK, strerror(errno)};
  }

  std::string pid_str = std::to_string(getpid());
  auto s = Util::Write(fd, pid_str);
  if (!s.IsOK()) {
    return s.Prefixed("failed to write to PID-file");
  }

  close(fd);
  return Status::OK();
}

static void removePidFile(const std::string &path) { std::remove(path.data()); }

Server *GetServer() { return nullptr; }

int main(int argc, char *argv[]) {
  google::InitGoogleLogging("migration_agent");
  evthread_use_pthreads();

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  std::cout << "Version: " << VERSION << " @" << GIT_COMMIT << std::endl;
  auto opts = parseCommandLineOptions(argc, argv);
  if (opts.show_usage) usage(argv[0]);

  MigrationAgent::Config agent_config;
  auto temp = Util::Split(opts.src_info, "@");
  auto host_and_ip = Util::Split(temp[0], ":");
  agent_config.src_server_host = host_and_ip[0];
  {
    auto temp_s = ParseInt<std::uint16_t>(host_and_ip[1]);
    if (!temp_s.IsOK()) exit(-1);
    agent_config.src_server_port = *temp_s;
  }

  agent_config.src_db_dir = temp[1];

  temp = Util::Split(opts.dst_info, "@");
  host_and_ip = Util::Split(temp[0], ":");
  agent_config.src_server_host = host_and_ip[0];
  {
    auto temp_s = ParseInt<std::uint16_t>(host_and_ip[1]);
    if (!temp_s.IsOK()) exit(-1);
    agent_config.src_server_port = *temp_s;
  }

  agent_config.src_db_dir = temp[1];

  agent_config.uri_prefix = opts.prefix;
  //      ET_OR_RET(ParseInt<std::uint16_t>(host_and_ip[1]));
  temp = Util::Split(opts.slot_str, ",");
  for (const auto &slot : temp) {
    if (!slot.empty()) agent_config.slot_list.push_back(std::stoi(slot));
  }
  std::cout << agent_config.ToString() << std::endl;

  initGoogleLog(&agent_config);

  //  // KVrocks Config
  //  Config kvrocks_config;
  //  kvrocks_config.db_dir = config.db_dir;
  //  kvrocks_config.cluster_enabled = config.cluster_enable;
  //  kvrocks_config.slot_id_encoded = config.cluster_enable;
  //
  //  Engine::Storage storage(&kvrocks_config);
  //  auto s = storage.Open(true);
  //  if (!s.IsOK()) {
  //    LOG(ERROR) << "Failed to open Kvrocks storage: " << s.Msg();
  //    exit(1);
  //  }
  //
  //  RedisWriter writer(&config);
  //  Parser parser(&storage, &writer);
  //
  //  Sync sync(&storage, &writer, &parser, &config);
  //  hup_handler = [&sync]() {
  //    if (!sync.IsStopped()) {
  //      LOG(INFO) << "Bye Bye";
  //      sync.Stop();
  //    }
  //  };
  //  sync.Start();
  //
  //  removePidFile(config.pidfile);
  return 0;
}
