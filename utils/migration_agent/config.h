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

#pragma once

#include <map>
#include <string>
#include <vector>

#include "status.h"

namespace MigrationAgent {

struct redis_server {
  std::string host;
  uint32_t port;
  std::string auth;
  int db_number;
};

struct Config {
  std::string ToString();

 public:
  int loglevel = 0;
  bool daemonize = false;

  std::string work_dir = "";
  std::string pidfile = "";
  std::string src_db_dir = "";
  std::string dst_db_dir = "";
  std::string uri_prefix = "";

  std::string src_server_host = "0.0.0.0";
  int src_server_port = 40001;
  std::string dst_server_host = "0.0.0.0";
  int dst_server_port = 40002;
  std::vector<int> slot_list;

 public:
  Status Load(std::string path);
  Config() = default;
  ~Config() = default;

 private:
  std::string path_;
  StatusOr<bool> yesnotoi(const std::string &input);
  Status parseConfigFromString(const std::string &input);
};

}  // namespace MigrationAgent
