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

#include <errno.h>
#include <stdlib.h>

#include "dbcommon/log/exception.h"
#include "dbcommon/log/logger.h"
#include "gtest/gtest.h"
#include "storage/format/orc/vector.h"
#include "storage/testutil/file-utils.h"

namespace orc {

TEST(TestOrcVector, IntegerVectors) {
  dbcommon::MemoryPool *pool = dbcommon::getDefaultPool();

  LongVectorBatch lv(100, *pool);
  EXPECT_EQ(lv.toString(), "Integer vector <0 of 100>");
  lv.resize(1000);
  EXPECT_EQ(lv.toString(), "Integer vector <0 of 1000>");
  EXPECT_EQ(lv.getWidth(), 8);
  EXPECT_EQ(lv.getType(), ORCTypeKind::LONG);

  EXPECT_EQ(lv.hasVariableLength(), false);
}

TEST(TestOrcVector, BytesVectorBatch) {
  dbcommon::MemoryPool *pool = dbcommon::getDefaultPool();

  StringVectorBatch sv(100, *pool);
  EXPECT_EQ(sv.toString(), "String vector <0 of 100>");
  sv.resize(1000);
  EXPECT_EQ(sv.toString(), "String vector <0 of 1000>");
  EXPECT_EQ(sv.getWidth(), 0);
  EXPECT_EQ(sv.getType(), ORCTypeKind::STRING);
  EXPECT_EQ(sv.getData(), (char *)sv.data.data());

  EXPECT_EQ(sv.hasVariableLength(), true);
}

TEST(TestOrcVector, StructVectorBatch) {
  dbcommon::MemoryPool *pool = dbcommon::getDefaultPool();

  StructVectorBatch vec(100, *pool);

  std::unique_ptr<LongVectorBatch> cvec(new LongVectorBatch(100, *pool));
  vec.fields.push_back(cvec.release());

  EXPECT_THROW(vec.getWidth(), dbcommon::TransactionAbortException);
  EXPECT_EQ(vec.getType(), ORCTypeKind::STRUCT);
  EXPECT_THROW(vec.getData(), dbcommon::TransactionAbortException);
  EXPECT_THROW(vec.buildVector(), dbcommon::TransactionAbortException);

  EXPECT_EQ(vec.hasVariableLength(), false);

  EXPECT_EQ(vec.toString(),
            "Struct vector <0 of 100; Integer vector <0 of 100>; >");
  vec.resize(1000);
  EXPECT_EQ(vec.toString(),
            "Struct vector <0 of 1000; Integer vector <0 of 1000>; >");
}

TEST(TestOrcVector, ListVectorBatch) {
  dbcommon::MemoryPool *pool = dbcommon::getDefaultPool();

  ListVectorBatch vec(100, *pool);

  EXPECT_THROW(vec.getWidth(), dbcommon::TransactionAbortException);
  EXPECT_EQ(vec.getType(), ORCTypeKind::LIST);
  EXPECT_THROW(vec.getData(), dbcommon::TransactionAbortException);
  EXPECT_THROW(vec.buildVector(), dbcommon::TransactionAbortException);
  EXPECT_EQ(vec.hasVariableLength(), true);

  EXPECT_EQ(vec.toString(), "List vector < with 0 of 100>");
  vec.resize(1000);
  EXPECT_EQ(vec.toString(), "List vector < with 0 of 1000>");
}

TEST(TestOrcVector, MapVectorBatch) {
  dbcommon::MemoryPool *pool = dbcommon::getDefaultPool();

  MapVectorBatch vec(100, *pool);

  EXPECT_THROW(vec.getWidth(), dbcommon::TransactionAbortException);
  EXPECT_EQ(vec.getType(), ORCTypeKind::MAP);
  EXPECT_THROW(vec.getData(), dbcommon::TransactionAbortException);
  EXPECT_THROW(vec.buildVector(), dbcommon::TransactionAbortException);
  EXPECT_EQ(vec.hasVariableLength(), true);

  EXPECT_EQ(vec.toString(), "Map vector <,  with 0 of 100>");
  vec.resize(1000);
  EXPECT_EQ(vec.toString(), "Map vector <,  with 0 of 1000>");
}

TEST(TestOrcVector, UnionVectorBatch) {
  dbcommon::MemoryPool *pool = dbcommon::getDefaultPool();

  UnionVectorBatch vec(100, *pool);
  std::unique_ptr<LongVectorBatch> cvec(new LongVectorBatch(100, *pool));
  vec.children.push_back(cvec.release());

  EXPECT_THROW(vec.getWidth(), dbcommon::TransactionAbortException);
  EXPECT_EQ(vec.getType(), ORCTypeKind::UNION);
  EXPECT_THROW(vec.getData(), dbcommon::TransactionAbortException);
  EXPECT_THROW(vec.buildVector(), dbcommon::TransactionAbortException);
  EXPECT_EQ(vec.hasVariableLength(), false);

  EXPECT_EQ(vec.toString(),
            "Union vector <Integer vector <0 of 100>; with 0 of 100>");
  vec.resize(1000);
  EXPECT_EQ(vec.toString(),
            "Union vector <Integer vector <0 of 100>; with 0 of 1000>");
}

TEST(TestOrcVector, Decimal64VectorBatch) {
  dbcommon::MemoryPool *pool = dbcommon::getDefaultPool();

  Decimal64VectorBatch vec(100, *pool);

  EXPECT_EQ(vec.getWidth(), 24);
  EXPECT_EQ(vec.getType(), ORCTypeKind::DECIMAL);
  EXPECT_EQ(vec.getData(), (char *)(vec.values.data()));
  EXPECT_EQ(vec.getAuxiliaryData(), (char *)(vec.highbitValues.data()));
  // EXPECT_THROW(vec.buildVector(), dbcommon::TransactionAbortException);
  EXPECT_EQ(vec.hasVariableLength(), false);

  EXPECT_EQ(vec.toString(), "Decimal64 vector  with 0 of 100>");
  vec.resize(1000);
  EXPECT_EQ(vec.toString(), "Decimal64 vector  with 0 of 1000>");
}

TEST(TestOrcVector, Decimal128VectorBatch) {
  dbcommon::MemoryPool *pool = dbcommon::getDefaultPool();

  Decimal128VectorBatch vec(100, *pool);

  EXPECT_EQ(vec.getWidth(), 24);
  EXPECT_EQ(vec.getType(), ORCTypeKind::DECIMAL);
  EXPECT_EQ(vec.getData(), (char *)(vec.lowbitValues.data()));
  EXPECT_EQ(vec.getAuxiliaryData(), (char *)(vec.highbitValues.data()));
  // EXPECT_THROW(vec.buildVector(), dbcommon::TransactionAbortException);
  EXPECT_EQ(vec.hasVariableLength(), false);

  EXPECT_EQ(vec.toString(), "Decimal128 vector  with 0 of 100>");
  vec.resize(1000);
  EXPECT_EQ(vec.toString(), "Decimal128 vector  with 0 of 1000>");
}

TEST(TestOrcVector, DateVectors) {
  dbcommon::MemoryPool *pool = dbcommon::getDefaultPool();

  DateVectorBatch lv(100, *pool);
  EXPECT_EQ(lv.toString(), "Integer vector <0 of 100>");
  lv.resize(1000);
  EXPECT_EQ(lv.toString(), "Integer vector <0 of 1000>");
  EXPECT_EQ(lv.getWidth(), 4);
  EXPECT_EQ(lv.getType(), ORCTypeKind::DATE);

  EXPECT_EQ(lv.hasVariableLength(), false);
}

TEST(TestOrcVector, DISABLED_TimestampVectorBatch) {
  dbcommon::MemoryPool *pool = dbcommon::getDefaultPool();

  TimestampVectorBatch vec(100, *pool);

  EXPECT_THROW(vec.getWidth(), dbcommon::TransactionAbortException);
  EXPECT_EQ(vec.getType(), ORCTypeKind::TIMESTAMP);
  EXPECT_THROW(vec.getData(), dbcommon::TransactionAbortException);
  EXPECT_THROW(vec.buildVector(), dbcommon::TransactionAbortException);
  EXPECT_THROW(vec.hasVariableLength(), dbcommon::TransactionAbortException);

  EXPECT_EQ(vec.toString(), "Timestamp vector <0 of 100>");
  vec.resize(1000);
  EXPECT_EQ(vec.toString(), "Timestamp vector <0 of 1000>");
}
}  // namespace orc
