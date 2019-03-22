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

#include "gtest/gtest.h"

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/comp/lz4-compressor.h"

namespace dbcommon {

TEST(TestLz4Compress, BasicCompressUncompress) {
  std::string testData = "abcdefghijklmnopqrstuvwxyz";
  LZ4Compressor comp;
  std::string compressedData;
  comp.compress(testData.data(), testData.size(), &compressedData);
  std::string uncompressedData;
  comp.uncompress(compressedData.data(), compressedData.size(),
                  &uncompressedData);
  EXPECT_STREQ(uncompressedData.c_str(), testData.c_str());
}

TEST(TestLz4Compress, BasicCompressDecompress) {
  std::string testData = "abcdefghijklmnopqrstuvwxyz";
  LZ4Compressor comp;
  std::string compressedData;
  int compressedSize = comp.maxCompressedLength(testData.size());
  compressedData.resize(compressedSize);
  uint64_t res =
      comp.compress(testData.data(), testData.size(),
                    const_cast<char *>(compressedData.data()), compressedSize);
  EXPECT_NE(0, res);
  compressedData.resize(res);

  std::string decompressedData;
  decompressedData.resize(testData.size());
  comp.decompress(compressedData.data(), compressedData.size(),
                  const_cast<char *>(decompressedData.data()), testData.size());
  EXPECT_STREQ(decompressedData.c_str(), testData.c_str());
}

}  // namespace dbcommon
