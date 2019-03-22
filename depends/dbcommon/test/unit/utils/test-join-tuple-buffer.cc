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

#include <numeric>

#include "dbcommon/utils/join-tuple-buffer.h"

#include "dbcommon/hash/hash-keys.h"
#include "dbcommon/hash/native-hash-table.h"
#include "dbcommon/log/debug-logger.h"
#include "dbcommon/testutil/tuple-batch-utils.h"
#include "gtest/gtest.h"

namespace dbcommon {

typedef NativeJoinHashTable HashTable;

TEST(TestJoinTupleBuffer, TestInnerJoin) {
  auto outerTupleDesc =
      TupleBatchUtility::generateTupleDesc("schema: int32 int32");
  auto innerTupleDesc =
      TupleBatchUtility::generateTupleDesc("schema: int32 string");
  auto outerTb = TupleBatchUtility::generateTupleBatch(*outerTupleDesc,
                                                       "1 0\n"
                                                       "2 1\n"
                                                       "3 1\n"
                                                       "4 2\n"
                                                       "5 0\n"
                                                       "6 4");
  auto innerTb = TupleBatchUtility::generateTupleBatch(*innerTupleDesc,
                                                       "0 a\n"
                                                       "1 b\n"
                                                       "1 c\n"
                                                       "2 d\n"
                                                       "2 e\n"
                                                       "2 f");

  {
    LOG_TESTING("Basic operation");

    HashTable hashtable(*innerTupleDesc, {1}, 0);
    hashtable.setupJoin(true, *outerTupleDesc, {2}, *innerTupleDesc, {1});
    hashtable.insert(innerTb.get());

    hashtable.resetInput(*outerTupleDesc, {2});
    for (auto iter = 0; iter < 5; iter++) {
      // iterate to testing reset and pipeline
      size_t num = 0;
      void* ptr = 0;
      auto tmp = hashtable.retrieveInnerJoin(
          outerTb.get(), hashtable.check(outerTb.get()), &num, &ptr);
      EXPECT_EQ(
          "1,0\n"
          "2,1\n"
          "2,1\n"
          "3,1\n"
          "3,1\n"
          "4,2\n"
          "4,2\n"
          "4,2\n"
          "5,0\n",
          tmp.first->toString());
      EXPECT_EQ(
          "0,a\n"
          "1,b\n"
          "1,c\n"
          "1,b\n"
          "1,c\n"
          "2,d\n"
          "2,f\n"
          "2,e\n"
          "0,a\n",
          tmp.second->toString());
      EXPECT_EQ(6, num);
    }
    EXPECT_DOUBLE_EQ(DEFAULT_SIZE_PER_HASHKEY_BLK +
                         DEFAULT_SIZE_PER_HASHJOIN_BLK +
                         DEFAULT_NUMBER_TUPLES_PER_BATCH *
                             (4 + DEFAULT_RESERVED_SIZE_OF_STRING) +
                         DEFAULT_SIZE_PER_HASH_CHAIN_BLK * 2,
                     hashtable.getMemUsed());
  }

  {
    LOG_TESTING("Large inner table");

    HashTable hashtable(*innerTupleDesc, {1}, 0);
    hashtable.setupJoin(true, *outerTupleDesc, {2}, *innerTupleDesc, {1});
    for (auto iter = 0; iter < 2001; iter++) hashtable.insert(innerTb.get());

    hashtable.resetInput(*outerTupleDesc, {2});
    size_t num = 0;
    void* ptr = 0;
    auto tmp = hashtable.retrieveInnerJoin(
        outerTb.get(), hashtable.check(outerTb.get()), &num, &ptr);
    EXPECT_EQ(DEFAULT_NUMBER_TUPLES_PER_BATCH, tmp.first->getNumOfRows());
    EXPECT_EQ(1, num);
    EXPECT_NE(nullptr, ptr);
    while (num != 6) {
      tmp = hashtable.retrieveInnerJoin(
          outerTb.get(), hashtable.check(outerTb.get()), &num, &ptr);
    }
    EXPECT_EQ(9 * 2001 % DEFAULT_NUMBER_TUPLES_PER_BATCH,
              tmp.first->getNumOfRows());
  }
}

TEST(TestJoinTupleBuffer, TestRetrieveTuple) {
  auto outerTupleDesc =
      TupleBatchUtility::generateTupleDesc("schema: int32 int32");
  auto innerTupleDesc =
      TupleBatchUtility::generateTupleDesc("schema: int32 string");
  auto outerTb = TupleBatchUtility::generateTupleBatch(*outerTupleDesc,
                                                       "1 0\n"
                                                       "2 1\n"
                                                       "3 1\n"
                                                       "4 2\n"
                                                       "5 0\n"
                                                       "6 4");
  auto innerTb = TupleBatchUtility::generateTupleBatch(*innerTupleDesc,
                                                       "0 a\n"
                                                       "1 b\n"
                                                       "1 c\n"
                                                       "2 d\n"
                                                       "2 e\n"
                                                       "2 f");

  HashTable hashtable(*innerTupleDesc, {1}, 0);
  hashtable.setupJoin(true, *outerTupleDesc, {2}, *innerTupleDesc, {1});
  hashtable.insert(innerTb.get());

  hashtable.resetInput(*outerTupleDesc, {2});
  for (auto iter = 0; iter < 5; iter++) {
    // iterate to testing reset and pipeline
    size_t num = 0;
    void* ptr = 0;
    auto groupNos = hashtable.check(outerTb.get());

    auto t1 = reinterpret_cast<JoinKey*>(groupNos[0]);
    EXPECT_EQ(",a\n", hashtable.retrieveInnerTuple(t1)->toString());
    EXPECT_EQ(nullptr, t1->next);

    auto t2 = reinterpret_cast<JoinKey*>(groupNos[1]);
    EXPECT_EQ(",b\n", hashtable.retrieveInnerTuple(t2)->toString());
    t2 = t2->next;
    EXPECT_EQ(",c\n", hashtable.retrieveInnerTuple(t2)->toString());
    EXPECT_EQ(nullptr, t2->next);

    auto t3 = reinterpret_cast<JoinKey*>(groupNos[2]);
    EXPECT_EQ(",b\n", hashtable.retrieveInnerTuple(t3)->toString());
    t3 = t3->next;
    EXPECT_EQ(",c\n", hashtable.retrieveInnerTuple(t3)->toString());
    EXPECT_EQ(nullptr, t3->next);

    auto t4 = reinterpret_cast<JoinKey*>(groupNos[3]);
    EXPECT_EQ(",d\n", hashtable.retrieveInnerTuple(t4)->toString());
    t4 = t4->next;
    EXPECT_EQ(",f\n", hashtable.retrieveInnerTuple(t4)->toString());
    t4 = t4->next;
    EXPECT_EQ(",e\n", hashtable.retrieveInnerTuple(t4)->toString());
    EXPECT_EQ(nullptr, t4->next);

    auto t5 = reinterpret_cast<JoinKey*>(groupNos[4]);
    EXPECT_EQ(",a\n", hashtable.retrieveInnerTuple(t5)->toString());
    EXPECT_EQ(nullptr, t5->next);

    EXPECT_EQ(NOT_IN_HASHTABLE, groupNos[5]);
  }
}

TEST(TestJoinTupleBuffer, TestInnerTableStore) {
  auto typeUtil = dbcommon::TypeUtil::instance();
  std::set<TypeKind> supportedTypes = {
      // integer
      TypeKind::TINYINTID,
      TypeKind::SMALLINTID,
      TypeKind::INTID,
      TypeKind::BIGINTID,
      // float point
      TypeKind::FLOATID,
      TypeKind::DOUBLEID,  // TypeKind::DECIMALID,
      // date/time
      TypeKind::TIMESTAMPID,  // TypeKind::TIMESTAMPTZID,
      TypeKind::DATEID,
      TypeKind::TIMEID,  // TypeKind::TIMETZID, TypeKind::INTERVALID,
      // string
      TypeKind::STRINGID,
      TypeKind::VARCHARID,
      TypeKind::CHARID,
      // misc
      TypeKind::BOOLEANID,
      TypeKind::BINARYID,
  };

  for (auto typeKind : supportedTypes) {
    auto typeName = typeUtil->getTypeEntryById(typeKind)->name;
    LOG_TESTING("%s", typeName.c_str());
    for (bool hasNull : std::vector<bool>{false, true})
      for (bool withSelectList : std::vector<bool>{false, true}) {
        LOG_TESTING("TB: %s nulls\t%s select list", (hasNull ? "has" : "no"),
                    (withSelectList ? "with" : "without"));
        auto innerTupleDesc = TupleBatchUtility::generateTupleDesc(
            "schema: " + typeName + " int32");
        auto innerTb = TupleBatchUtility::generateTupleBatchRandom(
            *innerTupleDesc, 0, 100, hasNull, withSelectList);

        HashTable hashtable(*innerTupleDesc, {2}, 0);
        hashtable.setupJoin(true, *innerTupleDesc, {2}, *innerTupleDesc, {2});
        hashtable.insert(innerTb.get());

        size_t num = 0;
        void* ptr = 0;
        auto tmp = hashtable.retrieveInnerJoin(
            innerTb.get(), hashtable.check(innerTb.get()), &num, &ptr);
        // EXPECT_EQ(innerTb->toString().size(), tmp.second->toString().size());
      }
  }
}

}  // namespace dbcommon
