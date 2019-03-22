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

#include "dbcommon/log/exception.h"
#include "dbcommon/log/logger.h"
#include "dbcommon/utils/byte-buffer.h"

namespace dbcommon {

TEST(TestByteBuffer, TestWrite) {
  ByteBuffer buffer(true);
  for (int i = 0; i < 1001; i++) {
    buffer.append(i);
  }

  EXPECT_EQ(buffer.size(), 1001 * sizeof(int));

  for (int i = 0; i < 1001; i++) {
    int ret = buffer.get<int>(i);
    EXPECT_EQ(i, ret);
  }

  EXPECT_EQ(buffer.size(), 1001 * sizeof(int));
}

TEST(TestByteBuffer, TestBytes) {
  ByteBuffer buffer(true);

  int size = 0;
  for (int i = 0; i < 1024; i++) {
    std::string s = std::to_string(i);
    size += s.length();
    buffer.append(s.data(), s.length());
  }

  EXPECT_EQ(buffer.size(), size);

  int pos = 0;
  for (int i = 0; i < 1024; i++) {
    std::string x = std::to_string(i);
    std::string s(buffer.read(pos, x.length()), x.length());
    pos += s.length();
    EXPECT_EQ(s, x);
  }

  EXPECT_EQ(buffer.size(), size);
}

TEST(TestByteBuffer, TestReadWriteAT) {
  ByteBuffer buffer(true);

  for (int i = 0; i < 8001; i++) {
    buffer.append<int>(i);
  }

  for (int i = 0; i < 8001; i++) {
    int ret = buffer.get<int>(i);
    EXPECT_EQ(ret, i);
  }

  for (int i = 0; i < 8001; i++) {
    buffer.set<int>(i, i - 1);
  }

  for (int i = 0; i < 8001; i++) {
    int ret = buffer.get<int>(i);
    EXPECT_EQ(ret, i - 1);
  }
}

}  // namespace dbcommon
