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

#include <sys/time.h>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/testutil/tuple-batch-utils.h"
#include "dbcommon/type/typebase.h"
#include "gtest/gtest.h"

namespace dbcommon {

TEST(TestTupleBatch, TestBasic) {
  TupleDesc desc;
  desc.add("id", TypeKind::INTID);
  desc.add("name", TypeKind::STRINGID);

  TupleBatch batch(desc, true);
  EXPECT_EQ(2, batch.getNumOfColumns());
  EXPECT_EQ(0, batch.getNumOfRows());

#define NUM_ROWS 1001
  TupleBatchWriter &writer = batch.getTupleBatchWriter();
  for (int i = 0; i < batch.getNumOfColumns(); i++) {
    for (int j = 0; j < NUM_ROWS; j++) {
      writer[i]->append(std::to_string(j), false);
    }
  }
  batch.incNumOfRows(NUM_ROWS);

  EXPECT_EQ(2, batch.getNumOfColumns());
  EXPECT_EQ(NUM_ROWS, batch.getNumOfRows());

  const TupleBatchReader &reader = batch.getTupleBatchReader();
  for (int i = 0; i < NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader[0]->read(i, &null));
    EXPECT_EQ(null, false);
  }

  for (int i = 0; i < NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader[1]->read(i, &null));
    ASSERT_EQ(null, false);
  }
}

TEST(TestTupleBatch, TestBatchAppend) {
  TupleDesc desc;
  desc.add("id", TypeKind::INTID);
  desc.add("name", TypeKind::STRINGID);

  // generate the first batch
  TupleBatch batch1(desc, true);
  EXPECT_EQ(2, batch1.getNumOfColumns());
  EXPECT_EQ(0, batch1.getNumOfRows());

  TupleBatchWriter &writer1 = batch1.getTupleBatchWriter();
  for (int i = 0; i < batch1.getNumOfColumns(); i++) {
    for (int j = 0; j < NUM_ROWS; j++) {
      writer1[i]->append(std::to_string(j), false);
    }
  }
  batch1.incNumOfRows(NUM_ROWS);

  EXPECT_EQ(2, batch1.getNumOfColumns());
  EXPECT_EQ(NUM_ROWS, batch1.getNumOfRows());
  EXPECT_EQ(batch1.isValid(), true);

  // generate the second batch
  TupleBatch batch2(desc, true);
  EXPECT_EQ(2, batch2.getNumOfColumns());
  EXPECT_EQ(0, batch2.getNumOfRows());

  TupleBatchWriter &writer2 = batch2.getTupleBatchWriter();
  for (int i = 0; i < batch2.getNumOfColumns(); i++) {
    for (int j = NUM_ROWS; j < 2 * NUM_ROWS; j++) {
      writer2[i]->append(std::to_string(j), false);
    }
  }
  batch2.incNumOfRows(NUM_ROWS);

  EXPECT_EQ(2, batch1.getNumOfColumns());
  EXPECT_EQ(NUM_ROWS, batch1.getNumOfRows());

  // verify the first batch
  const TupleBatchReader &reader1 = batch1.getTupleBatchReader();
  for (int i = 0; i < NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader1[0]->read(i, &null));
    EXPECT_EQ(null, false);
  }

  for (int i = 0; i < NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader1[1]->read(i, &null));
    ASSERT_EQ(null, false);
  }

  EXPECT_EQ(batch2.isValid(), true);

  // append batch2 to batch1
  batch1.append(&batch2);
  EXPECT_EQ(batch1.getNumOfRows(), 2 * NUM_ROWS);
  EXPECT_EQ(batch1.isValid(), true);
  EXPECT_EQ(batch2.isValid(), true);

  // verifty the first batch
  const TupleBatchReader &reader2 = batch1.getTupleBatchReader();
  for (int i = 0; i < 2 * NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader2[0]->read(i, &null));
    EXPECT_EQ(null, false);
  }

  for (int i = 0; i < 2 * NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader2[1]->read(i, &null));
    ASSERT_EQ(null, false);
  }

  batch1.append(&batch2);
  EXPECT_EQ(batch1.getNumOfRows(), 3 * NUM_ROWS);

  EXPECT_EQ(batch1.isValid(), true);
  EXPECT_EQ(batch2.isValid(), true);
}

TEST(TestTupleBatch, TestArrayLength) {
  TupleDesc desc;
  desc.add("id", TypeKind::INTID);
  desc.add("name", TypeKind::STRINGID);

  TupleBatch batch(desc, true);
  EXPECT_EQ(2, batch.getNumOfColumns());
  EXPECT_EQ(0, batch.getNumOfRows());

#undef NUM_ROWS
#define NUM_ROWS 1001
  int totalLen = 0;
  TupleBatchWriter &writer = batch.getTupleBatchWriter();
  for (int i = 0; i < batch.getNumOfColumns(); i++) {
    for (int j = 0; j < NUM_ROWS; j++) {
      writer[i]->append(std::to_string(j), false);
      if (i == 1) totalLen += (std::to_string(j).length());
    }
  }
  batch.incNumOfRows(NUM_ROWS);
  EXPECT_EQ(2, batch.getNumOfColumns());
  EXPECT_EQ(NUM_ROWS, batch.getNumOfRows());

  const TupleBatchReader &reader = batch.getTupleBatchReader();
  EXPECT_EQ(reader[0]->getNullBitMapNumBytesPlain(),
            BoolBuffer::bitmapBitLengthToByteLength(NUM_ROWS));
  EXPECT_EQ(reader[0]->getValueBuffer()->size(), NUM_ROWS * sizeof(int));

  EXPECT_EQ(reader[1]->getNullBitMapNumBytesPlain(),
            BoolBuffer::bitmapBitLengthToByteLength(NUM_ROWS));
  EXPECT_EQ(reader[1]->getValueBuffer()->size(), totalLen);

  for (int i = 0; i < NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader[0]->read(i, &null));
    EXPECT_EQ(null, false);
  }

  for (int i = 0; i < NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader[1]->read(i, &null));
    ASSERT_EQ(null, false);
  }

  batch.reset();

  EXPECT_EQ(reader[0]->getNullBitMapNumBytesPlain(), 0);
  EXPECT_EQ(reader[0]->getValueBuffer()->size(), 0);

  EXPECT_EQ(reader[1]->getNullBitMapNumBytesPlain(), 0);
  EXPECT_EQ(reader[1]->getValueBuffer()->size(), 0);

  for (int i = 0; i < batch.getNumOfColumns(); i++) {
    for (int j = 0; j < NUM_ROWS; j++) {
      writer[i]->append(std::to_string(j), false);
    }
  }
  batch.incNumOfRows(NUM_ROWS);

  EXPECT_EQ(2, batch.getNumOfColumns());
  EXPECT_EQ(NUM_ROWS, batch.getNumOfRows());

  for (int i = 0; i < NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader[0]->read(i, &null));
    EXPECT_EQ(null, false);
  }

  for (int i = 0; i < NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader[1]->read(i, &null));
    ASSERT_EQ(null, false);
  }

  EXPECT_EQ(batch.isValid(), true);
}

TEST(TestTupleBatch, TestBatchReplicated) {
  TupleDesc desc;
  desc.add("id", TypeKind::INTID);
  desc.add("name", TypeKind::STRINGID);

  // generate the first batch
  TupleBatch batch(desc, true);
  const int kNumRows = 100;

  TupleBatchWriter &writer = batch.getTupleBatchWriter();
  for (int i = 0; i < batch.getNumOfColumns(); i++) {
    for (int j = 0; j < kNumRows; j++) {
      writer[i]->append(std::to_string(j), false);
    }
  }
  batch.incNumOfRows(kNumRows);

  EXPECT_EQ(2, batch.getNumOfColumns());
  EXPECT_EQ(100, batch.getNumOfRows());

  TupleBatch::uptr replicated1 = batch.replicateRowsToTB(5, 10);
  const TupleBatchReader &reader = replicated1->getTupleBatchReader();
  for (int i = 0; i < 10; i++) {
    bool null1;
    bool null2;
    EXPECT_EQ(std::to_string(5), reader[0]->read(i, &null1));
    EXPECT_EQ(std::to_string(5), reader[1]->read(i, &null2).c_str());
    ASSERT_EQ(null1, false);
    ASSERT_EQ(null2, false);
  }

  EXPECT_EQ(10, replicated1->getNumOfRows());
}

TEST(TestTupleBatch, TestBatchConcat) {
  const int kNumRows = 100;

  TupleDesc desc;
  desc.add("id", TypeKind::INTID);
  desc.add("name", TypeKind::STRINGID);

  // generate the first batch
  TupleBatch::uptr batch1(new TupleBatch(desc, true));
  TupleBatchWriter &writer1 = batch1->getTupleBatchWriter();
  for (int i = 0; i < batch1->getNumOfColumns(); i++) {
    for (int j = 0; j < kNumRows; j++) {
      writer1[i]->append(std::to_string(j), false);
    }
  }
  batch1->incNumOfRows(kNumRows);
  EXPECT_EQ(2, batch1->getNumOfColumns());
  EXPECT_EQ(100, batch1->getNumOfRows());

  // generate the second batch
  TupleBatch::uptr batch2(new TupleBatch(desc, true));
  TupleBatchWriter &writer2 = batch2->getTupleBatchWriter();
  for (int i = 0; i < batch2->getNumOfColumns(); i++) {
    for (int j = 0; j < kNumRows; j++) {
      writer2[i]->append(std::to_string(kNumRows - j), false);
    }
  }
  batch2->incNumOfRows(kNumRows);
  EXPECT_EQ(2, batch2->getNumOfColumns());
  EXPECT_EQ(100, batch2->getNumOfRows());

  batch1->concatTB(std::move(batch2));
  const TupleBatchReader &reader = batch1->getTupleBatchReader();
  for (int i = 0; i < 100; i++) {
    bool null1;
    bool null2;
    bool null3;
    EXPECT_EQ(std::to_string(i), reader[0]->read(i, &null1));
    EXPECT_EQ(std::to_string(100 - i), reader[2]->read(i, &null2));
    EXPECT_EQ(std::to_string(100 - i), reader[3]->read(i, &null3));
    ASSERT_EQ(null1, false);
    ASSERT_EQ(null2, false);
    ASSERT_EQ(null3, false);
  }

  EXPECT_EQ(100, batch1->getNumOfRows());
}

TEST(TestTupleBatch, TestSelectedList) {
#undef NUM_ROWS
#define NUM_ROWS 1000
  TupleBatchUtility tbu;
  TupleDesc::uptr desc = tbu.generateTupleDesc("is");
  TupleBatch::uptr batch = tbu.generateTupleBatch(*desc, 0, NUM_ROWS);

  EXPECT_EQ(2, batch->getNumOfColumns());
  EXPECT_EQ(NUM_ROWS, batch->getNumOfRows());

  const TupleBatchReader &reader = batch->getTupleBatchReader();
  for (int i = 0; i < NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader[0]->read(i, &null));
    EXPECT_EQ(null, false);
  }

  for (int i = 0; i < NUM_ROWS; i++) {
    bool null;
    EXPECT_EQ(std::to_string(i), reader[1]->read(i, &null));
    ASSERT_EQ(null, false);
  }

  SelectList sl;
  sl.push_back(0);
  sl.push_back(2);
  batch->setSelected(sl);
  EXPECT_EQ(2, batch->getNumOfRows());

  batch->materialize();
  EXPECT_EQ(2, batch->getNumOfRows());
  EXPECT_EQ(nullptr, batch->getSelected());

  const TupleBatchReader &readerNew = batch->getTupleBatchReader();
  bool null;
  EXPECT_EQ(std::to_string(0), readerNew[0]->read(0, &null));
  EXPECT_EQ(null, false);
  EXPECT_EQ(std::to_string(2), readerNew[0]->read(1, &null));
  EXPECT_EQ(null, false);

  EXPECT_EQ(std::to_string(0), readerNew[1]->read(0, &null));
  EXPECT_EQ(null, false);
  EXPECT_EQ(std::to_string(2), readerNew[1]->read(1, &null));
  EXPECT_EQ(null, false);
}

TEST(TestTupleBatch, TestSelectedListAppend) {
#undef NUM_ROWS
#define NUM_ROWS 1000
  TupleBatchUtility tbu;
  TupleDesc::uptr desc = tbu.generateTupleDesc("is");
  TupleBatch::uptr batch = tbu.generateTupleBatch(*desc, 0, NUM_ROWS);

  EXPECT_EQ(2, batch->getNumOfColumns());
  EXPECT_EQ(NUM_ROWS, batch->getNumOfRows());

  SelectList sl;
  sl.push_back(0);
  sl.push_back(2);
  batch->setSelected(sl);
  EXPECT_EQ(2, batch->getNumOfRows());

  TupleBatch::uptr batch1 = tbu.generateTupleBatch(*desc, 0, 9);
  EXPECT_EQ("0,0\n1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n",
            batch1->toString());
  EXPECT_EQ(9, batch1->getNumOfRows());
  EXPECT_EQ(true, batch1->isValid());

  batch->append(batch1.get());

  EXPECT_EQ(true, batch->isValid());
  EXPECT_EQ(11, batch->getNumOfRows());
  EXPECT_EQ("0,0\n2,2\n0,0\n1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n",
            batch->toString());

  SelectList sl1;
  sl1.push_back(0);
  sl1.push_back(2);
  batch1->setSelected(sl1);
  EXPECT_EQ(2, batch1->getNumOfRows());
  EXPECT_EQ(9, batch1->getNumOfRowsPlain());
  EXPECT_EQ(true, batch1->isValid());

  batch->append(batch1.get());
  EXPECT_EQ(13, batch->getNumOfRows());
  EXPECT_EQ("0,0\n2,2\n0,0\n1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n0,0\n2,2\n",
            batch->toString());
  EXPECT_EQ(true, batch->isValid());

  batch->materialize();
  EXPECT_EQ(13, batch->getNumOfRows());
  EXPECT_EQ(true, batch->isValid());
  EXPECT_EQ("0,0\n2,2\n0,0\n1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n0,0\n2,2\n",
            batch->toString());
}

TEST(TestTupleBatch, TestSelectedListAppend2) {
#undef NUM_ROWS
#define NUM_ROWS 2
  TupleBatchUtility tbu;
  TupleDesc::uptr desc = tbu.generateTupleDesc("is");
  TupleBatch::uptr batch = tbu.generateTupleBatch(*desc, 0, NUM_ROWS);

  EXPECT_EQ(2, batch->getNumOfColumns());
  EXPECT_EQ(NUM_ROWS, batch->getNumOfRows());

  TupleBatch::uptr batch1 = tbu.generateTupleBatch(*desc, 0, 9);
  EXPECT_EQ("0,0\n1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n",
            batch1->toString());
  EXPECT_EQ(9, batch1->getNumOfRows());

  batch->append(batch1.get());

  EXPECT_EQ(11, batch->getNumOfRows());
  EXPECT_EQ("0,0\n1,1\n0,0\n1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n",
            batch->toString());

  SelectList sl1;
  sl1.push_back(0);
  sl1.push_back(2);
  batch1->setSelected(sl1);
  EXPECT_EQ(2, batch1->getNumOfRows());

  batch->append(batch1.get());
  EXPECT_EQ(13, batch->getNumOfRows());
  EXPECT_EQ("0,0\n1,1\n0,0\n1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n0,0\n2,2\n",
            batch->toString());
  EXPECT_EQ(true, batch->isValid());

  batch->materialize();
  EXPECT_EQ(13, batch->getNumOfRows());
  EXPECT_EQ(true, batch->isValid());
  EXPECT_EQ("0,0\n1,1\n0,0\n1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n0,0\n2,2\n",
            batch->toString());
}

TEST(TestTupleBatch, TestMaterialization) {
#undef NUM_ROWS
#define NUM_ROWS 2
  TupleBatchUtility tbu;
  TupleDesc::uptr desc = tbu.generateTupleDesc("is");
  TupleBatch::uptr batch = tbu.generateTupleBatch(*desc, 0, NUM_ROWS);

  EXPECT_EQ(2, batch->getNumOfColumns());
  EXPECT_EQ(NUM_ROWS, batch->getNumOfRows());
  EXPECT_EQ("0,0\n1,1\n", batch->toString());

  batch->materialize();

  EXPECT_EQ("0,0\n1,1\n", batch->toString());
  EXPECT_EQ(NUM_ROWS, batch->getNumOfRows());

  SelectList sl1;
  sl1.push_back(0);
  batch->setSelected(sl1);
  EXPECT_EQ(1, batch->getNumOfRows());
  EXPECT_EQ("0,0\n", batch->toString());

  batch->materialize();

  EXPECT_EQ(1, batch->getNumOfRows());
  EXPECT_EQ("0,0\n", batch->toString());

  batch = tbu.generateTupleBatch(*desc, 0, NUM_ROWS);
  TupleBatch::uptr batchRef(new TupleBatch(*desc, false));

  Vector *v0 = batch->getColumn(0);
  Vector *v0ref = batchRef->getColumn(0);
  v0ref->setValue(v0->getValue(), v0->getValueBuffer()->size());
  v0ref->setNullBits(v0->getNullBits(), v0->getNullsSize());

  Vector *v1 = batch->getColumn(1);
  Vector *v1ref = batchRef->getColumn(1);
  v1ref->setValue(v1->getValue(), v1->getValueBuffer()->size());
  v1ref->setLengths(v1->getLengths(), v1->getNumOfRowsPlain());
  v1ref->setNullBits(v1->getNullBits(), v1->getNullsSize());
  v1ref->computeValPtrs();

  batchRef->setNumOfRows(batch->getNumOfRows());

  EXPECT_EQ(2, batchRef->getNumOfRows());
  EXPECT_EQ("0,0\n1,1\n", batchRef->toString());

  batchRef->materialize();
  EXPECT_EQ(2, batchRef->getNumOfRows());
  EXPECT_EQ("0,0\n1,1\n", batchRef->toString());
}

TEST(TestTupleBatch, TestAllSelected) {
#undef NUM_ROWS
#define NUM_ROWS 2
  TupleBatchUtility tbu;
  TupleDesc::uptr desc = tbu.generateTupleDesc("is");
  TupleBatch::uptr batch = tbu.generateTupleBatch(*desc, 0, NUM_ROWS);

  EXPECT_EQ(2, batch->getNumOfColumns());
  EXPECT_EQ(NUM_ROWS, batch->getNumOfRows());
  EXPECT_EQ("0,0\n1,1\n", batch->toString());

  SelectList sl1;
  sl1.push_back(0);
  sl1.push_back(1);

  batch->setSelected(sl1);
  EXPECT_EQ(2, batch->getNumOfRows());
  EXPECT_EQ("0,0\n1,1\n", batch->toString());

  batch->materialize();
  EXPECT_EQ(2, batch->getNumOfRows());
  EXPECT_EQ("0,0\n1,1\n", batch->toString());
}

TEST(TestTupleBatch, TestAppendBoundaryCase) {
  TupleBatchUtility tbu;
  TupleDesc::uptr desc = tbu.generateTupleDesc("is");
  TupleBatch::uptr batch = tbu.generateTupleBatch(*desc, 0, 0);

  EXPECT_EQ(2, batch->getNumOfColumns());
  EXPECT_EQ(0, batch->getNumOfRows());
  EXPECT_EQ("", batch->toString());

  TupleBatch::uptr batch1 = tbu.generateTupleBatch(*desc, 0, 0);

  batch->append(batch1.get());
  EXPECT_EQ(2, batch->getNumOfColumns());
  EXPECT_EQ(0, batch->getNumOfRows());
  EXPECT_EQ("", batch->toString());

  TupleBatch::uptr batch2 = tbu.generateTupleBatch(*desc, 0, 1);
  batch->append(batch2.get());
  EXPECT_EQ(2, batch->getNumOfColumns());
  EXPECT_EQ(1, batch->getNumOfRows());
  EXPECT_EQ("0,0\n", batch->toString());
}

TEST(TestTupleBatch, TestSerializeAndDeserializeOwnData) {
  TupleBatchUtility tbu;
  // STRUCT not test
  TupleDesc::uptr desc = tbu.generateTupleDesc("is");

  bool useSelectList[2] = {false, true};
  bool hasNull[2] = {false, true};
  std::string expected[2] = {"0,0\n1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n",
                             "0,0\n2,2\n"};
  std::string expectedNull[2] = {"", ",\n"};
  int length[2][2] = {{9, 10}, {2, 3}};
  int lengthPlain[2][2] = {{9, 10}, {9, 10}};

  int i, j;
  for (i = 0; i < 2; i++)
    for (j = 0; j < 2; j++) {
      TupleBatch::uptr batch1 = tbu.generateTupleBatch(*desc, 0, 9, hasNull[j]);
      TupleBatch::uptr batch2(new TupleBatch(*desc, true));
      if (useSelectList[i]) {
        SelectList sl;
        sl.push_back(0);
        sl.push_back(2);
        if (hasNull[j]) sl.push_back(9);
        batch1->setSelected(sl);
      }
      std::string tmp;
      EXPECT_EQ(length[i][j], batch1->getNumOfRows());
      EXPECT_EQ(lengthPlain[i][j], batch1->getNumOfRowsPlain());
      batch1->serialize(&tmp, 0);
      batch2->deserialize(tmp);
      EXPECT_EQ(expected[i].append(expectedNull[j]), batch2->toString());
      EXPECT_EQ(length[i][j], batch1->getNumOfRows());
      EXPECT_EQ(length[i][j], batch2->getNumOfRows());
      EXPECT_EQ(length[i][j], batch2->getNumOfRowsPlain());
      EXPECT_EQ(true, batch2->isValid());
    }
}

TEST(TestTupleBatch, TestSerializeAndDeserializeNotOwnData) {
  TupleBatchUtility tbu;
  // STRUCT not test
  TupleDesc::uptr desc = tbu.generateTupleDesc("is");

  bool useSelectList[2] = {false, true};
  bool hasNull[2] = {false, true};
  std::string expected[2] = {"0,0\n1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n",
                             "0,0\n2,2\n"};
  std::string expectedNull[2] = {"", ",\n"};
  int length[2][2] = {{9, 10}, {2, 3}};
  int lengthPlain[2][2] = {{9, 10}, {9, 10}};

  int i, j;
  for (i = 0; i < 2; i++)
    for (j = 0; j < 2; j++) {
      TupleBatch::uptr batch1(new TupleBatch(*desc, false));
      TupleBatch::uptr batch2(new TupleBatch(*desc, true));
      TupleBatch::uptr batch1_buf =
          tbu.generateTupleBatch(*desc, 0, 9, hasNull[j]);

      Vector *v0_buf = batch1_buf->getColumn(0);
      Vector *v0 = batch1->getColumn(0);
      v0->setValue(v0_buf->getValue(), v0_buf->getValueBuffer()->size());
      v0->setNullBits(v0_buf->getNullBits(), v0_buf->getNullsSize());

      Vector *v1_buf = batch1_buf->getColumn(1);
      Vector *v1 = batch1->getColumn(1);
      v1->setValPtrs(v1_buf->getValPtrs(), v1_buf->getNumOfRowsPlain());
      v1->setLengths(v1_buf->getLengths(), v1_buf->getNumOfRowsPlain());
      v1->setNullBits(v1_buf->getNullBits(), v1_buf->getNullsSize());

      batch1->setNumOfRows(batch1_buf->getNumOfRows());

      if (useSelectList[i]) {
        SelectList sl;
        sl.push_back(0);
        sl.push_back(2);
        if (hasNull[j]) sl.push_back(9);
        batch1->setSelected(sl);
      }
      std::string tmp;
      EXPECT_EQ(length[i][j], batch1->getNumOfRows());
      EXPECT_EQ(lengthPlain[i][j], batch1->getNumOfRowsPlain());
      batch1->serialize(&tmp, 0);
      batch2->deserialize(tmp);
      EXPECT_EQ(expected[i].append(expectedNull[j]), batch2->toString());
      EXPECT_EQ(length[i][j], batch1->getNumOfRows());
      EXPECT_EQ(length[i][j], batch2->getNumOfRows());
      EXPECT_EQ(length[i][j], batch2->getNumOfRowsPlain());
      EXPECT_EQ(true, batch2->isValid());
    }
}

TEST(TestTupleBatch, TestSysColumns) {
  // STEP 1. prepare tuple batch
  const int tupleCount = 3;
  auto tupleDesc = TupleBatchUtility::generateTupleDesc("thil");
  TupleBatch tb(*tupleDesc, true, true);
  for (int i = 0; i < tupleCount; i++) {
    int j = 1;
    for (auto &sysWriter : tb.getTupleBatchSysWriter()) {
      sysWriter->append(std::to_string((i + 1) * j++), false);
    }
    for (auto &writer : tb.getTupleBatchWriter()) {
      writer->append(std::to_string((i + 1) * j++), false);
    }
    tb.incNumOfRows(1);
  }
  LOG_INFO(
      "Tuple batch has %u tuple with %u system column and %u user column:\n%s",
      tb.getNumOfRows(), tb.getNumOfSysColumns(), tb.getNumOfColumns(),
      tb.toString().c_str());

  // STEP 2. clone tuple batch
  std::unique_ptr<TupleBatch> ctb = std::move(tb.clone());
  EXPECT_EQ(tb.toString(), ctb->toString());

  // STEP 3. serialize and deserialize tuple batch
  std::string stb, sctb;
  tb.serialize(&stb, 0);
  ctb->serialize(&sctb, 0);
  EXPECT_EQ(stb, sctb);

  std::unique_ptr<TupleBatch> dtb(new TupleBatch(*tupleDesc, true));
  dtb->deserialize(stb);
  EXPECT_EQ(tb.toString(), dtb->toString());
}

TEST(TestTupleBatch, TestPermutate) {
  std::vector<uint32_t> projIdxs = {1, 3, 0, 2};
  auto tupleDesc = TupleBatchUtility::generateTupleDesc("thil");
  TupleBatch tupleBatch(*tupleDesc, true);
  int i = 0;
  for (auto &writer : tupleBatch.getTupleBatchWriter()) {
    writer->append(std::to_string(i++), false);
  }
  tupleBatch.setNumOfRows(1);
  tupleBatch.permutate(projIdxs);
  EXPECT_EQ("2,0,3,1\n", tupleBatch.toString());

  auto scalar = tupleBatch.getRow(0);
  scalar->permutate(projIdxs);
  EXPECT_EQ("3,2,1,0\n", scalar->toString());
}

}  // namespace dbcommon
