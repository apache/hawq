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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_COMP_LZ4_COMPRESSOR_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_COMP_LZ4_COMPRESSOR_H_

#include <lz4.h>
#include <lz4hc.h>

#include <cassert>
#include <string>

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/comp/compressor.h"

namespace dbcommon {

class LZ4Compressor : public Compressor {
 public:
  LZ4Compressor() {}
  ~LZ4Compressor() {}

  uint64_t compress(const char *src, uint64_t srcSize, char *dest,
                    uint64_t destSize) override;
  uint64_t decompress(const char *src, uint64_t srcSize, char *dest,
                      uint64_t destSize) override;
  uint64_t maxCompressedLength(uint64_t srcSize) override;

  void compress(const char *src, size_t size, std::string *output) override;
  void uncompress(const char *src, size_t size, std::string *output) override;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_COMP_LZ4_COMPRESSOR_H_
