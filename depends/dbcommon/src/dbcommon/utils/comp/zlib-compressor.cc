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

#include "dbcommon/utils/comp/zlib-compressor.h"

#include <zlib.h>
#include <cassert>
#include <cerrno>

#include "dbcommon/log/logger.h"

namespace dbcommon {

void ZlibCompressor::compress(const char *src, size_t size,
                              std::string *output) {
  int level = 3;
  uLongf compressedSize;
  uLongf uncompressedSize = size;
  int status;

  compressedSize = ::compressBound(uncompressedSize);  // worst case
  output->resize(compressedSize + sizeof(int));

  // store uncompressed size in the beginning.
  memcpy(const_cast<char *>(output->data()), &uncompressedSize, sizeof(int));

  status = compress2(reinterpret_cast<Bytef *>(
                         const_cast<char *>(output->data()) + sizeof(int)),
                     &compressedSize, reinterpret_cast<const Bytef *>(src),
                     uncompressedSize, level);
  if (status != Z_OK)
    LOG_ERROR(ERRCODE_SYSTEM_ERROR, "compression failed: %s %d", zError(status),
              status);

  output->resize(compressedSize + sizeof(int));
}

void ZlibCompressor::uncompress(const char *src, size_t size,
                                std::string *output) {
  assert(NULL != src && size >= sizeof(int32_t) && "invalid input");

  // get the uncompressed data size.
  int32_t bufSize;
  ::memcpy(&bufSize, src, sizeof(int32_t));

  // resize the output buffer.
  output->resize(bufSize);

  uLongf outputSize = bufSize;
  int status = ::uncompress(
      const_cast<Bytef *>(reinterpret_cast<const Bytef *>(output->data())),
      &outputSize,
      const_cast<Bytef *>(
          reinterpret_cast<const Bytef *>((src + sizeof(int32_t)))),
      size - sizeof(int32_t));

  if (status != Z_OK) {
    LOG_ERROR(
        ERRCODE_SYSTEM_ERROR,
        "Uncompress failed: %s (errno=%d), compressed len %d, uncompressed %d",
        zError(status), static_cast<int>(status), static_cast<int>(size),
        static_cast<int>(outputSize));
  }
}
}  // namespace dbcommon
