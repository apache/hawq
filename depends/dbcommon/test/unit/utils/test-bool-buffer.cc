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
#include "dbcommon/utils/bool-buffer.h"
#include "dbcommon/utils/byte-buffer.h"

namespace dbcommon {

TEST(TestBoolBuffer, TestWriteBool) {
  BoolBuffer buffer(true);

  for (int i = 0; i < 5001; i++) {
    buffer.append(true);
  }

  EXPECT_EQ(buffer.size(), 5001);

  for (int i = 0; i < 5001; i++) {
    bool ret = buffer.get(i);
    EXPECT_EQ(ret, true);
  }

  EXPECT_EQ(buffer.size(), 5001);

  buffer.resize(0);

  EXPECT_EQ(buffer.size(), 0);

  for (int i = 0; i < 5001; i++) {
    buffer.append(false);
  }

  EXPECT_EQ(buffer.size(), 5001);

  for (int i = 0; i < 5001; i++) {
    bool ret = buffer.get(i);
    EXPECT_EQ(ret, false);
  }

  EXPECT_EQ(buffer.size(), 5001);
}

TEST(TestBoolBuffer, TestWriteBoolDiff) {
  BoolBuffer buffer(true);

  for (int i = 0; i < 5001; i++) {
    if (i % 2 == 0)
      buffer.append(true);
    else
      buffer.append(false);
  }

  EXPECT_EQ(buffer.size(), 5001);

  for (int i = 0; i < 5001; i++) {
    bool ret = buffer.get(i);
    if (i % 2 == 0)
      EXPECT_EQ(ret, true);
    else
      EXPECT_EQ(ret, false);
  }

  EXPECT_EQ(buffer.size(), 5001);
}

TEST(TestBoolBuffer, TestReadWriteBoolAT) {
  BoolBuffer buffer(true);

  for (int i = 0; i < 5000; i++) {
    buffer.append(i % 2 ? true : false);
  }

  for (int i = 0; i < 5000; i++) {
    bool ret = buffer.get(i);
    EXPECT_EQ(ret, i % 2 ? true : false);
  }

  for (int i = 0; i < 5000; i++) {
    buffer.set(i, i % 2 ? false : true);
  }

  for (int i = 0; i < 5000; i++) {
    bool ret = buffer.get(i);
    EXPECT_EQ(ret, i % 2 ? false : true);
  }
}

TEST(TestBoolBuffer, TestSetBitData) {
  {
    BoolBuffer buffer(true);
    char data[] = {1};
    buffer.setBitData(data, 1);
    EXPECT_EQ(1, buffer.size());
    EXPECT_TRUE(buffer.get(0));
    EXPECT_EQ(sizeof(data) / sizeof(data[0]), buffer.bitDataSizeInBytes());
  }

  {
    BoolBuffer buffer(true);
    char data[] = {2};
    buffer.setBitData(data, 2);
    EXPECT_EQ(2, buffer.size());
    EXPECT_FALSE(buffer.get(0));
    EXPECT_TRUE(buffer.get(1));
    EXPECT_EQ(sizeof(data) / sizeof(data[0]), buffer.bitDataSizeInBytes());
  }

  {
    BoolBuffer buffer(true);
    char data[] = {6};
    buffer.setBitData(data, 8);
    EXPECT_EQ(8, buffer.size());
    EXPECT_FALSE(buffer.get(0));
    EXPECT_TRUE(buffer.get(1));
    EXPECT_TRUE(buffer.get(2));
    EXPECT_FALSE(buffer.get(3));
    EXPECT_FALSE(buffer.get(4));
    EXPECT_FALSE(buffer.get(5));
    EXPECT_FALSE(buffer.get(6));
    EXPECT_FALSE(buffer.get(7));
    EXPECT_EQ(sizeof(data) / sizeof(data[0]), buffer.bitDataSizeInBytes());
  }

  {
    BoolBuffer buffer(true);
    char data[] = {6, 1};
    buffer.setBitData(data, 9);
    EXPECT_EQ(9, buffer.size());
    EXPECT_FALSE(buffer.get(0));
    EXPECT_TRUE(buffer.get(1));
    EXPECT_TRUE(buffer.get(2));
    EXPECT_FALSE(buffer.get(3));
    EXPECT_FALSE(buffer.get(4));
    EXPECT_FALSE(buffer.get(5));
    EXPECT_FALSE(buffer.get(6));
    EXPECT_FALSE(buffer.get(7));
    EXPECT_TRUE(buffer.get(8));
    EXPECT_EQ(sizeof(data) / sizeof(data[0]), buffer.bitDataSizeInBytes());
  }

  {
    BoolBuffer buffer(true);
    char data[] = {6, 6, 1};
    buffer.setBitData(data, 19);
    EXPECT_EQ(19, buffer.size());
    EXPECT_FALSE(buffer.get(0));
    EXPECT_TRUE(buffer.get(1));
    EXPECT_TRUE(buffer.get(2));
    EXPECT_FALSE(buffer.get(3));
    EXPECT_FALSE(buffer.get(4));
    EXPECT_FALSE(buffer.get(5));
    EXPECT_FALSE(buffer.get(6));
    EXPECT_FALSE(buffer.get(7));

    EXPECT_FALSE(buffer.get(8));
    EXPECT_TRUE(buffer.get(9));
    EXPECT_TRUE(buffer.get(10));
    EXPECT_FALSE(buffer.get(11));
    EXPECT_FALSE(buffer.get(12));
    EXPECT_FALSE(buffer.get(13));
    EXPECT_FALSE(buffer.get(14));
    EXPECT_FALSE(buffer.get(15));

    EXPECT_TRUE(buffer.get(16));
    EXPECT_FALSE(buffer.get(17));
    EXPECT_FALSE(buffer.get(18));
    EXPECT_EQ(sizeof(data) / sizeof(data[0]), buffer.bitDataSizeInBytes());
  }
}

TEST(TestBoolBuffer, TestGetBitData) {
  {
    BoolBuffer buffer(true);
    buffer.append(true);
    const char *data = buffer.getBitData();
    EXPECT_TRUE(memcmp(data, "\0x1", 1));
  }

  {
    BoolBuffer buffer(true);
    buffer.append(false);
    buffer.append(true);
    buffer.append(true);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    const char *data = buffer.getBitData();
    EXPECT_TRUE(memcmp(data, "\0x6", 1));
  }

  {
    BoolBuffer buffer(true);
    buffer.append(false);
    buffer.append(true);
    buffer.append(true);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(true);
    const char *data = buffer.getBitData();
    EXPECT_TRUE(memcmp(data, "\0x6\0x1", 2));
  }

  {
    BoolBuffer buffer(true);
    buffer.append(false);
    buffer.append(true);
    buffer.append(true);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(true);
    buffer.append(true);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(false);
    buffer.append(true);
    const char *data = buffer.getBitData();
    EXPECT_TRUE(memcmp(data, "\0x6\0x6\0x1", 3));
  }
}

TEST(TestBoolBuffer, TestBitData) {
  BoolBuffer buffer(true);
  char data[] = {1, 2, 3};
  size_t size = sizeof(data) / sizeof(data[0]) * CHAR_BIT - 1;
  buffer.setBitData(data, size);

  const char *res = buffer.getBitData();

  for (size_t i = 0; i < sizeof(data) / sizeof(data[0]) - 1; ++i) {
    EXPECT_EQ(data[i], res[i]);
  }

  char a = data[sizeof(data) / sizeof(data[0]) - 1];
  char b = res[sizeof(data) / sizeof(data[0]) - 1];

  for (size_t i = 0; i < CHAR_BIT - 1; ++i) {
    EXPECT_EQ((a & (1 << i)), (b & (1 << i)));
  }
}

}  // namespace dbcommon
