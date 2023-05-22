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

#include "config.h"

#include <fmt/format.h>
#include <rocksdb/env.h>
#include <strings.h>

#include <fstream>
#include <iostream>
#include <utility>
#include <vector>

#include "../kvrocks2redis/config.h"
#include "config/config.h"
#include "config/config_util.h"
#include "string_util.h"

namespace MigrationAgent {

static constexpr const char *kLogLevels[] = {"info", "warning", "error", "fatal"};
static constexpr size_t kNumLogLevel = std::size(kLogLevels);

StatusOr<bool> Config::yesnotoi(const std::string &input) {
  if (Util::EqualICase(input, "yes")) {
    return true;
  } else if (Util::EqualICase(input, "no")) {
    return false;
  }
  return {Status::NotOK, "value must be 'yes' or 'no'"};
}

Status Config::parseConfigFromString(const std::string &input) {
  auto [original_key, value] = GET_OR_RET(ParseConfigLine(input));
  if (original_key.empty()) return Status::OK();

  std::vector<std::string> args = Util::Split(value, " \t\r\n");
  auto key = Util::ToLower(original_key);
  size_t size = args.size();

  if (size == 1 && key == "daemonize") {
    daemonize = GET_OR_RET(yesnotoi(args[0]).Prefixed("key 'daemonize'"));
  } else if (size == 1 && key == "data-dir") {
    work_dir = args[0];
    if (work_dir.empty()) {
      return {Status::NotOK, "'data-dir' was not specified"};
    }

    if (work_dir.back() != '/') {
      work_dir += "/";
    }
    pidfile = work_dir + "kvrocks2redis.pid";
  } else if (size == 1 && key == "src_db_dir") {
    src_db_dir = args[0];
    if (src_db_dir.empty()) {
      return {Status::NotOK, "'output-dir' was not specified"};
    }

    if (src_db_dir.back() != '/') {
      src_db_dir += "/";
    }
  } else if (size == 1 && key == "dst_db_dir") {
    dst_db_dir = args[0];
    if (dst_db_dir.empty()) {
      return {Status::NotOK, "'output-dir' was not specified"};
    }

    if (dst_db_dir.back() != '/') {
      dst_db_dir += "/";
    }
  } else if (size == 2 && key == "src_server") {
    src_server_host = args[0];
    src_server_port = GET_OR_RET(ParseInt<std::uint16_t>(args[1]).Prefixed("src_server port number"));
    if (src_server_host.empty()) {
      return {Status::NotOK, "'src_server_host' was not specified"};
    }
  } else if (size == 2 && key == "dst_server") {
    dst_server_host = args[0];
    dst_server_port = GET_OR_RET(ParseInt<std::uint16_t>(args[1]).Prefixed("dst_server port number"));
    if (src_server_host.empty()) {
      return {Status::NotOK, "'dst_server_host' was not specified"};
    }
  } else {
    return {Status::NotOK, "unknown configuration directive or wrong number of arguments"};
  }

  return Status::OK();
}

Status Config::Load(std::string path) {
  path_ = std::move(path);
  std::ifstream file(path_);
  if (!file.is_open()) {
    return {Status::NotOK, fmt::format("failed to open file '{}': {}", path_, strerror(errno))};
  }

  std::string line;
  int line_num = 1;
  while (!file.eof()) {
    std::getline(file, line);
    Status s = parseConfigFromString(line);
    if (!s.IsOK()) {
      return s.Prefixed(fmt::format("at line #L{}", line_num));
    }

    line_num++;
  }

  auto s = rocksdb::Env::Default()->FileExists(work_dir);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return {Status::NotOK, fmt::format("the specified Kvrocks working directory '{}' doesn't exist", work_dir)};
    }
    return {Status::NotOK, s.ToString()};
  }

  s = rocksdb::Env::Default()->FileExists(work_dir);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return {Status::NotOK,
              fmt::format("the specified directory '{}' for intermediate files doesn't exist", work_dir)};
    }
    return {Status::NotOK, s.ToString()};
  }

  return Status::OK();
}
std::string Config::ToString() {
  std::string temp;
  temp += fmt::format("{}:{}, dir:{},", src_server_host, src_server_port, src_db_dir);
  temp += fmt::format("{}:{}, dir:{}", dst_server_host, dst_server_port, dst_db_dir);

  return temp;
}

}  // namespace MigrationAgent
