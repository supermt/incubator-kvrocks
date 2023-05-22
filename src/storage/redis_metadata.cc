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

#include "redis_metadata.h"

#include <rocksdb/env.h>
#include <sys/time.h>

#include <atomic>
#include <cstdlib>
#include <ctime>

#include "cluster/redis_slot.h"
#include "time_util.h"

// 52 bit for microseconds and 11 bit for counter
const int VersionCounterBits = 11;

static std::atomic<uint64_t> version_counter_ = {0};

constexpr const char *kErrMetadataTooShort = "metadata is too short";

InternalKey::InternalKey(Slice input, bool slot_id_encoded) : slot_id_encoded_(slot_id_encoded) {
  uint32_t key_size = 0;
  uint8_t namespace_size = 0;
  GetFixed8(&input, &namespace_size);
  namespace_ = Slice(input.data(), namespace_size);
  input.remove_prefix(namespace_size);
  if (slot_id_encoded_) {
    GetFixed16(&input, &slotid_);
  }
  GetFixed32(&input, &key_size);
  key_ = Slice(input.data(), key_size);
  input.remove_prefix(key_size);
  GetFixed64(&input, &version_);
  sub_key_ = Slice(input.data(), input.size());
}

InternalKey::InternalKey(Slice ns_key, Slice sub_key, uint64_t version, bool slot_id_encoded)
    : slot_id_encoded_(slot_id_encoded) {
  uint8_t namespace_size = 0;
  GetFixed8(&ns_key, &namespace_size);
  namespace_ = Slice(ns_key.data(), namespace_size);
  ns_key.remove_prefix(namespace_size);
  if (slot_id_encoded_) {
    GetFixed16(&ns_key, &slotid_);
  }
  key_ = ns_key;
  sub_key_ = sub_key;
  version_ = version;
}

Slice InternalKey::GetNamespace() const { return namespace_; }

Slice InternalKey::GetKey() const { return key_; }

Slice InternalKey::GetSubKey() const { return sub_key_; }

uint64_t InternalKey::GetVersion() const { return version_; }

void InternalKey::Encode(std::string *out) {
  out->clear();
  size_t pos = 0;
  size_t total = 1 + namespace_.size() + 4 + key_.size() + 8 + sub_key_.size();
  if (slot_id_encoded_) {
    total += 2;
  }
  out->resize(total);
  auto buf = out->data();
  EncodeFixed8(buf + pos, static_cast<uint8_t>(namespace_.size()));
  pos += 1;
  memcpy(buf + pos, namespace_.data(), namespace_.size());
  pos += namespace_.size();
  if (slot_id_encoded_) {
    EncodeFixed16(buf + pos, slotid_);
    pos += 2;
  }
  EncodeFixed32(buf + pos, static_cast<uint32_t>(key_.size()));
  pos += 4;
  memcpy(buf + pos, key_.data(), key_.size());
  pos += key_.size();
  EncodeFixed64(buf + pos, version_);
  pos += 8;
  memcpy(buf + pos, sub_key_.data(), sub_key_.size());
  // pos += sub_key_.size();
}

bool InternalKey::operator==(const InternalKey &that) const {
  if (key_ != that.key_) return false;
  if (sub_key_ != that.sub_key_) return false;
  return version_ == that.version_;
}
int InternalKey::GetSlotID() const { return slotid_; }
void ExtractNamespaceKey(Slice ns_key, std::string *ns, std::string *key, uint16_t *slot_id) {
  uint8_t namespace_size = 0;
  GetFixed8(&ns_key, &namespace_size);
  *ns = ns_key.ToString().substr(0, namespace_size);
  ns_key.remove_prefix(namespace_size);

  GetFixed16(&ns_key, slot_id);

  *key = ns_key.ToString();
}
void ExtractNamespaceKey(Slice ns_key, std::string *ns, std::string *key, bool slot_id_encoded) {
  uint8_t namespace_size = 0;
  GetFixed8(&ns_key, &namespace_size);
  *ns = ns_key.ToString().substr(0, namespace_size);
  ns_key.remove_prefix(namespace_size);

  if (slot_id_encoded) {
    uint16_t slot_id = 0;
    GetFixed16(&ns_key, &slot_id);
  }

  *key = ns_key.ToString();
}

void ComposeNamespaceKey(const Slice &ns, const Slice &key, std::string *ns_key, bool slot_id_encoded) {
  ns_key->clear();

  PutFixed8(ns_key, static_cast<uint8_t>(ns.size()));
  ns_key->append(ns.data(), ns.size());

  if (slot_id_encoded) {
    auto slot_id = GetSlotNumFromKey(key.ToString());
    PutFixed16(ns_key, slot_id);
  }

  ns_key->append(key.data(), key.size());
}

void ComposeSlotKeyPrefix(const Slice &ns, int slotid, std::string *output) {
  output->clear();

  PutFixed8(output, static_cast<uint8_t>(ns.size()));
  output->append(ns.data(), ns.size());

  PutFixed16(output, static_cast<uint16_t>(slotid));
}

Metadata::Metadata(RedisType type, bool generate_version)
    : flags((uint8_t)0x0f & type), expire(0), version(0), size(0) {
  if (generate_version) version = generateVersion();
}

rocksdb::Status Metadata::Decode(const std::string &bytes) {
  // flags(1byte) + expire (4byte)
  if (bytes.size() < 5) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }
  Slice input(bytes);
  GetFixed8(&input, &flags);
  GetFixed32(&input, reinterpret_cast<uint32_t *>(&expire));
  if (Type() != kRedisString) {
    if (input.size() < 12) return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
    GetFixed64(&input, &version);
    GetFixed32(&input, &size);
  }
  return rocksdb::Status::OK();
}

void Metadata::Encode(std::string *dst) {
  PutFixed8(dst, flags);
  PutFixed32(dst, (uint32_t)expire);
  if (Type() != kRedisString) {
    PutFixed64(dst, version);
    PutFixed32(dst, size);
  }
}

void Metadata::InitVersionCounter() {
  // use random position for initial counter to avoid conflicts,
  // when the slave was promoted as master and the system clock may backoff
  srand(static_cast<unsigned>(Util::GetTimeStamp()));
  version_counter_ = static_cast<uint64_t>(std::rand());
}

uint64_t Metadata::generateVersion() {
  uint64_t timestamp = Util::GetTimeStampUS();
  uint64_t counter = version_counter_.fetch_add(1);
  return (timestamp << VersionCounterBits) + (counter % (1 << VersionCounterBits));
}

bool Metadata::operator==(const Metadata &that) const {
  if (flags != that.flags) return false;
  if (expire != that.expire) return false;
  if (Type() != kRedisString) {
    if (size != that.size) return false;
    if (version != that.version) return false;
  }
  return true;
}

RedisType Metadata::Type() const { return static_cast<RedisType>(flags & (uint8_t)0x0f); }

int32_t Metadata::TTL() const {
  if (expire <= 0) {
    return -1;
  }
  auto now = Util::GetTimeStamp();
  if (expire < now) {
    return -2;
  }
  return int32_t(expire - now);
}

timeval Metadata::Time() const {
  auto t = version >> VersionCounterBits;
  struct timeval created_at {
    static_cast<uint32_t>(t / 1000000), static_cast<int32_t>(t % 1000000)
  };
  return created_at;
}

bool Metadata::ExpireAt(int64_t expired_ts) const {
  if (Type() != kRedisString && Type() != kRedisStream && size == 0) {
    return true;
  }
  if (expire <= 0) {
    return false;
  }
  return expire < expired_ts;
}

bool Metadata::Expired() const { return ExpireAt(Util::GetTimeStamp()); }

ListMetadata::ListMetadata(bool generate_version)
    : Metadata(kRedisList, generate_version), head(UINT64_MAX / 2), tail(head) {}

void ListMetadata::Encode(std::string *dst) {
  Metadata::Encode(dst);
  PutFixed64(dst, head);
  PutFixed64(dst, tail);
}

rocksdb::Status ListMetadata::Decode(const std::string &bytes) {
  Slice input(bytes);
  GetFixed8(&input, &flags);
  GetFixed32(&input, reinterpret_cast<uint32_t *>(&expire));
  if (Type() != kRedisString) {
    if (input.size() < 12) return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
    GetFixed64(&input, &version);
    GetFixed32(&input, &size);
  }
  if (Type() == kRedisList) {
    if (input.size() < 16) return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
    GetFixed64(&input, &head);
    GetFixed64(&input, &tail);
  }
  return rocksdb::Status::OK();
}

void StreamMetadata::Encode(std::string *dst) {
  Metadata::Encode(dst);

  PutFixed64(dst, last_generated_id.ms);
  PutFixed64(dst, last_generated_id.seq);

  PutFixed64(dst, recorded_first_entry_id.ms);
  PutFixed64(dst, recorded_first_entry_id.seq);

  PutFixed64(dst, max_deleted_entry_id.ms);
  PutFixed64(dst, max_deleted_entry_id.seq);

  PutFixed64(dst, first_entry_id.ms);
  PutFixed64(dst, first_entry_id.seq);

  PutFixed64(dst, last_entry_id.ms);
  PutFixed64(dst, last_entry_id.seq);

  PutFixed64(dst, entries_added);
}

rocksdb::Status StreamMetadata::Decode(const std::string &bytes) {
  // flags(1byte) + expire (4byte)
  if (bytes.size() < 5) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  Slice input(bytes);
  GetFixed8(&input, &flags);
  GetFixed32(&input, reinterpret_cast<uint32_t *>(&expire));

  if (input.size() < 12) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  GetFixed64(&input, &version);
  GetFixed32(&input, &size);

  if (input.size() < 88) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  GetFixed64(&input, &last_generated_id.ms);
  GetFixed64(&input, &last_generated_id.seq);

  GetFixed64(&input, &recorded_first_entry_id.ms);
  GetFixed64(&input, &recorded_first_entry_id.seq);

  GetFixed64(&input, &max_deleted_entry_id.ms);
  GetFixed64(&input, &max_deleted_entry_id.seq);

  GetFixed64(&input, &first_entry_id.ms);
  GetFixed64(&input, &first_entry_id.seq);

  GetFixed64(&input, &last_entry_id.ms);
  GetFixed64(&input, &last_entry_id.seq);

  GetFixed64(&input, &entries_added);

  return rocksdb::Status::OK();
}
