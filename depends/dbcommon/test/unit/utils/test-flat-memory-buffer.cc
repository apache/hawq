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

#include "dbcommon/utils/flat-memory-buffer.h"

#include "gtest/gtest.h"

namespace dbcommon {

TEST(TestFlatMemBuf, TestResize) {
  using Type = double;
  FlatMemBuf<Type, 256> buf;

  ASSERT_EQ(0, buf.getMemUsed());

  buf.resize(5);
  EXPECT_EQ(nextPowerOfTwo(sizeof(Type) * 5), buf.getMemUsed());
  *buf.ptrAt(3) = 233;
  EXPECT_EQ(233, *buf.ptrAt(3));

  buf.resize(9);
  EXPECT_EQ(nextPowerOfTwo(sizeof(Type) * 9), buf.getMemUsed());

  buf.resize(buf.BlkSize + 7);
  EXPECT_EQ(256 + nextPowerOfTwo(sizeof(Type) * 7), buf.getMemUsed());
  *buf.ptrAt(buf.BlkSize) = 666;
  EXPECT_EQ(666, *buf.ptrAt(buf.BlkSize));

  buf.resize(buf.BlkSize * 13);
  EXPECT_EQ(256 * 13, buf.getMemUsed());

  EXPECT_EQ(233, *buf.ptrAt(3));
  EXPECT_EQ(666, *buf.ptrAt(buf.BlkSize));

  buf.resize(2);
  EXPECT_EQ(256 * 13, buf.getMemUsed());
}

}  // namespace dbcommon

