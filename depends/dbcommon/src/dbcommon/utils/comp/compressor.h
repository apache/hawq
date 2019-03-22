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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_COMP_COMPRESSOR_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_COMP_COMPRESSOR_H_

#include <string>

namespace dbcommon {

class Compressor {
 public:
  Compressor() {}
  virtual ~Compressor() {}

  // Compress srt to dest
  // @param src The input data
  // @param srcSize The input size
  // @param dest The output buffer (must have been allocated)
  // @param destSize The output buffer size
  // @return The final compressed result size
  virtual uint64_t compress(const char *src, uint64_t srcSize, char *dest,
                            uint64_t destSize) = 0;

  // Compress srt to dest
  // @param src The input data
  // @param srcSize The input size
  // @param dest The output buffer (must have been allocated)
  // @param destSize The output buffer size
  // @return The final decompressed result size
  virtual uint64_t decompress(const char *src, uint64_t srcSize, char *dest,
                              uint64_t destSize) = 0;

  // Compute the maximal size of the compressed representation of
  // input data that is "source_bytes" bytes in length;
  // This function is used to compute the space allocated for compress function
  // @param srcSize The input raw size
  // @return The maximal size after compression
  virtual uint64_t maxCompressedLength(uint64_t srcSize) = 0;

  virtual void compress(const char *src, size_t size, std::string *output) = 0;
  virtual void uncompress(const char *src, size_t size,
                          std::string *output) = 0;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_COMP_COMPRESSOR_H_
