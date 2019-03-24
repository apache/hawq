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
 */

#ifndef STORAGE_SRC_STORAGE_COMMON_STRING_H_
#define STORAGE_SRC_STORAGE_COMMON_STRING_H_

#include <cassert>
#include <string>

#include "dbcommon/utils/cutils.h"

namespace storage {

#define STRING_INIT_RESERVE_BYTES 1024

class String {
 public:
  String() {
    assert(reserved_ > 0);
    data_ = dbcommon::cnmalloc(reserved_);
  }

  virtual ~String() { dbcommon::cnfree(data_); }

  String &operator=(const String &) = delete;

  void append(const char *value, uint32_t sz) {
    enlarge(sz);
    assert(size_ + sz <= reserved_);
    memcpy(&data_[size_], value, sz);
    size_ += sz;
  }

  void append(const char *value, uint32_t pos, uint32_t sz) {
    enlarge(sz);
    assert(size_ + sz <= reserved_);
    memcpy(&data_[size_], value + pos, sz);
    size_ += sz;
  }

  void appendChar(char value) {
    enlarge(1);
    assert(size_ + 1 <= reserved_);
    *(reinterpret_cast<char *>(&data_[size_])) = value;
    size_ += 1;
  }

  char *data() const { return data_; }

  uint32_t size() const { return size_; }

  void reset() { size_ = 0; }

  std::string substr(uint32_t pos, uint32_t len) {
    std::string str;
    for (uint32_t i = pos, end = pos + len; i < end; ++i) str += data_[i];
    return str;
  }

 private:
  void enlarge(uint32_t needed) {
    needed += size_;
    if (needed <= reserved_) return;

    uint32_t newLen = 2 * reserved_;
    while (needed > newLen) newLen *= 2;

    data_ = dbcommon::cnrealloc(data_, sizeof(char) * newLen);
    reserved_ = newLen;
  }

 private:
  char *data_ = nullptr;
  uint32_t size_ = 0;
  uint32_t reserved_ = STRING_INIT_RESERVE_BYTES;
};
}  // namespace storage

#endif  // STORAGE_SRC_STORAGE_COMMON_STRING_H_
