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

#include "dbcommon/utils/int-util.h"

#include "gtest/gtest.h"

namespace dbcommon {
class TestVarIntEncodeDecode : public testing::TestWithParam<uint64_t> {};

TEST_P(TestVarIntEncodeDecode, TestMostSignificanBit) {
  char buf[10];
  uint32_t outsize = encodeMsbVarint(GetParam(), buf);
  uint64_t integer;
  uint32_t size;
  std::tie(integer, size) = decodeMsbVarint(buf);
  EXPECT_EQ(GetParam(), integer);
  EXPECT_EQ(outsize, size);
}

TEST_P(TestVarIntEncodeDecode, TestUnsignedVarint) {
  char buf[10];
  uint32_t outsize = encodeUnsignedVarint(GetParam(), buf);
  uint64_t integer;
  uint32_t size;
  std::tie(integer, size) = decodeUnsignedVarint(buf);
  EXPECT_EQ(GetParam(), integer);
  EXPECT_EQ(outsize, size);
}

INSTANTIATE_TEST_CASE_P(TestVarIntEncodeDecode, TestVarIntEncodeDecode,
                        ::testing::ValuesIn(std::vector<uint64_t>{
                            4,
                            127,
                            128,
                            233,
                            666,
                            4546,
                            14122,
                            38324,
                            65536,
                            0x10000,
                            0x100000000,
                            0x100000000,
                            0x100FA0000,
                            0x100000000,
                            0x100003400,
                            0x100000000EA,
                            0x10230100000,
                            0x1000000000000,
                            0x12345678E0000,
                            0x10D00000E0000,
                            0x100000000000000,
                            0x1,
                            0x12,
                            0x123,
                            0x1234,
                            0x12345,
                            0x123456,
                            0x1234567,
                            0x12345678,
                            0x123456789,
                            0x1234567890,
                            0x1234567890A,
                            0x1234567890AB,
                            0x1234567890ABC,
                            0x1234567890ABCD,
                            0x1234567890ABCDE,
                            0x1234567890ABCDEF,
                        }));

}  // namespace dbcommon
