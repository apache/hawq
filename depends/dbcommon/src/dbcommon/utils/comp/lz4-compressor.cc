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

#include "dbcommon/utils/comp/lz4-compressor.h"

#include "dbcommon/log/logger.h"

namespace dbcommon {

uint64_t LZ4Compressor::compress(const char *src, uint64_t srcSize, char *dest,
                                 uint64_t destSize) {
  assert(src != nullptr && srcSize > 0 && dest != nullptr && destSize > 0);
  assert(destSize >= LZ4_compressBound(srcSize));
  int res = LZ4_compress_default(src, dest, srcSize, destSize);
  if (res == 0) {
    LOG_ERROR(ERRCODE_SYSTEM_ERROR,
              "failed to compress using LZ4, original size %llu", srcSize);
  }
  return res;
}

uint64_t LZ4Compressor::decompress(const char *src, uint64_t srcSize,
                                   char *dest, uint64_t destSize) {
  assert(src != nullptr && srcSize > 0 && dest != nullptr && destSize > 0);
  int res = LZ4_decompress_safe(src, dest, srcSize, destSize);
  if (res < 0) {
    LOG_ERROR(ERRCODE_SYSTEM_ERROR,
              "failed to decompress using LZ4, original size %llu, code %d",
              srcSize, res);
  }
  return res;
}

uint64_t LZ4Compressor::maxCompressedLength(uint64_t srcSize) {
  assert(srcSize > 0);
  int res = LZ4_compressBound(srcSize);
  if (res == 0) {
    LOG_ERROR(ERRCODE_SYSTEM_ERROR,
              "failed to get compress bound using LZ4, original size %llu",
              srcSize);
  }
  return res;
}

void LZ4Compressor::compress(const char *src, size_t size,
                             std::string *output) {
  int uncompressedSize = size;
  int compressedSize = maxCompressedLength(uncompressedSize);
  assert(compressedSize > 0);  // otherwise, maxCompressedLength should raise
                               // exception
  output->resize(sizeof(int) + compressedSize);
  memcpy(const_cast<char *>(output->data()), &uncompressedSize, sizeof(int));
  compressedSize = LZ4_compress_default(
      src, const_cast<char *>(output->data()) + sizeof(int), uncompressedSize,
      compressedSize);
  if (compressedSize == 0) {
    LOG_ERROR(ERRCODE_SYSTEM_ERROR,
              "failed to compress using LZ4, original size %llu", size);
  }
  output->resize(sizeof(int) + compressedSize);  // shrink to actual size
}

void LZ4Compressor::uncompress(const char *src, size_t size,
                               std::string *output) {
  int uncompressedSize = 0;
  memcpy(&uncompressedSize, src, sizeof(int));
  output->resize(uncompressedSize);
  int res =
      LZ4_decompress_safe(src + sizeof(int), const_cast<char *>(output->data()),
                          size - sizeof(int), uncompressedSize);
  if (res < 0) {
    LOG_ERROR(ERRCODE_SYSTEM_ERROR,
              "failed to decompress using LZ4, original size %llu, code %d",
              uncompressedSize, res);
  }
}

}  // namespace dbcommon
