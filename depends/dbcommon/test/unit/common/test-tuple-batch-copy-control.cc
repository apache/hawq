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

#include "dbcommon/common/vector.h"
#include "dbcommon/log/debug-logger.h"
#include "dbcommon/testutil/tuple-batch-utils.h"
#include "dbcommon/type/type-kind.h"
#include "gtest/gtest.h"

namespace dbcommon {

class TestCopyControlOnTupleBatch : public testing::TestWithParam<std::string> {
 public:
  TestCopyControlOnTupleBatch() {
    auto schemaStr = TestWithParam::GetParam();
    LOG_TESTING("%s", schemaStr.c_str());
    tupleDesc = tbu.generateTupleDesc(schemaStr);
  }
  TupleBatchUtility tbu;
  TupleDesc::uptr tupleDesc;
};
INSTANTIATE_TEST_CASE_P(
    TestCopyControl, TestCopyControlOnTupleBatch,
    ::testing::Values(
        "schema: boolean int8 int16 int32 int64 float double "
        "decimal decimal_new "
        "bpchar bpchar(10) varchar varchar(5) string binary timestamp "
        "time date"));

TEST_P(TestCopyControlOnTupleBatch, TestClone) {
  auto srcTB = tbu.generateTupleBatchRandom(*tupleDesc, 0, 666, true);
  auto clonedTB = srcTB->clone();
  EXPECT_EQ(srcTB->toString(), clonedTB->toString());
}

TEST_P(TestCopyControlOnTupleBatch, TestSerializeAndDeserialize) {
  auto srcTB = tbu.generateTupleBatchRandom(*tupleDesc, 0, 666, true);
  std::string serializedStr;
  srcTB->serialize(&serializedStr, 0);
  TupleBatch::uptr deserializedTB(new TupleBatch);
  deserializedTB->deserialize(serializedStr);
  EXPECT_EQ(srcTB->toString(), deserializedTB->toString());
}

TEST_P(TestCopyControlOnTupleBatch, TestAppend) {
  auto dstTB = tbu.generateTupleBatch(*tupleDesc, 0, 666, false);
  auto srcTB0 = tbu.generateTupleBatch(*tupleDesc, 0, 333, false);
  auto srcTB1 = tbu.generateTupleBatch(*tupleDesc, 333, 333, false);
  srcTB0->append(srcTB1.get());
  EXPECT_EQ(dstTB->toString(), srcTB0->toString());
}

}  // namespace dbcommon
