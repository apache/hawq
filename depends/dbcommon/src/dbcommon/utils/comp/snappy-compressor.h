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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_COMP_SNAPPY_COMPRESSOR_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_COMP_SNAPPY_COMPRESSOR_H_

#include <snappy.h>

#include <cassert>
#include <string>

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/comp/compressor.h"

namespace dbcommon {

class SnappyCompressor : public Compressor {
 public:
  SnappyCompressor() {}
  ~SnappyCompressor() {}

  uint64_t compress(const char *src, uint64_t srcSize, char *dest,
                    uint64_t destSize) override {
    assert(src != nullptr && srcSize > 0 && dest != nullptr && destSize > 0);

    assert(destSize >= snappy::MaxCompressedLength(srcSize));

    size_t ret = 0;
    snappy::RawCompress(src, srcSize, dest, &ret);
    return ret;
  }

  uint64_t decompress(const char *src, uint64_t srcSize, char *dest,
                      uint64_t destSize) override {
    assert(src != nullptr && srcSize > 0 && dest != nullptr && destSize > 0);

    size_t ret = 0;
    bool success = snappy::GetUncompressedLength(src, srcSize, &ret);
    if (!success) {
      LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE,
                "invalid input compressed data");
    }

    assert(ret <= destSize);

    success = snappy::RawUncompress(src, srcSize, dest);
    if (!success) {
      LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE,
                "invalid input compressed data");
    }
    return ret;
  }

  uint64_t maxCompressedLength(uint64_t srcSize) override {
    assert(srcSize > 0);

    return snappy::MaxCompressedLength(srcSize);
  }

  void compress(const char *src, size_t size, std::string *output) override {
    assert(nullptr != src && size >= sizeof(int32_t));

    snappy::Compress(src, size, output);
  }

  void uncompress(const char *src, size_t size, std::string *output) override {
    assert(nullptr != src && size >= sizeof(int32_t));

    snappy::Uncompress(src, size, output);
  }
};
}  // namespace dbcommon

#endif  // DBCOMMON_UTILS_COMP_SNAPPY_COMPRESSOR_H_
