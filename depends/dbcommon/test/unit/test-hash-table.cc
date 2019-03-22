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

#include <map>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/hash/hash-keys.h"
#include "dbcommon/hash/native-hash-table.h"
#include "dbcommon/log/debug-logger.h"
#include "dbcommon/testutil/tuple-batch-utils.h"
#include "gtest/gtest.h"

namespace dbcommon {

/*
 * In this test, we are going to check all the functions in native-hash-table.h
 * group-keys.h, and the related checkGroupByKey/checkGroupByKeys in Vector.
 * We need to check the combination of (with nulls/no nulls) * (direct
 * encoding/dictionary encoding) * (with select list/ without select list).
 */

class TestHashTable : public ::testing::Test {
 public:
  TestHashTable() {
    desc = TupleBatchUtility::generateTupleDesc(
        "schema: "
        "boolean "
        "int8 int16 int32 int64 "
        "float double decimal decimal_new "
        "varchar varchar(5) string "
        "binary "
        "timestamp time date");
  }

  void test(std::vector<uint64_t> grpIdxs) {
    std::string hashkey;
    for (auto idx : grpIdxs) {
      hashkey = hashkey + ' ' +
                TypeUtil::instance()
                    ->getTypeEntryById(desc->getColumnType(idx - 1))
                    ->name;
    }
    LOG_TESTING("hash key: %s", hashkey.c_str());
    hashTable.reset(new HashTable(*desc, grpIdxs, 1));
    testInsert(grpIdxs, true, true);
    testInsert(grpIdxs, true, false);
    testInsert(grpIdxs, false, true);
    testInsert(grpIdxs, false, false);

    testCheck(grpIdxs, true, true);
    testCheck(grpIdxs, true, false);
    testCheck(grpIdxs, true, true);
    testCheck(grpIdxs, false, false);
  }

 private:
  typedef NativeAggHashTable HashTable;

  void resetTupleBatch(bool hasNull, bool withSelectList) {
    batch = TupleBatchUtility::generateTupleBatchRandom(*desc, 1, 20, hasNull,
                                                        withSelectList);
  }

  void resetAnsForInsert(std::vector<uint64_t> grpIdxs) {
    groupKeys.clear();
    ans.clear();
    for (int rowIdx = 0; rowIdx < batch->getNumOfRows(); rowIdx++) {
      std::string groupKey = "";
      for (auto grpIdx : grpIdxs) {
        bool null;
        std::string tmp = batch->vectors[grpIdx - 1]->read(rowIdx, &null);
        if (!null) groupKey += tmp;
        groupKey += null ? '$' : '@';
        // LOG_INFO("%s", groupKey.c_str());
      }
      if (groupKeys.find(groupKey) != groupKeys.end()) {
        ans.push_back(groupKeys[groupKey]);
      } else {
        ans.push_back(groupKeys.size());
        groupKeys[groupKey] = ans.back();
      }
    }
  }

  void testInsert(std::vector<uint64_t> grpIdxs, bool hasNull,
                  bool withSelectList) {
    LOG_TESTING("Insert TB: %s nulls\t%s select list", (hasNull ? "has" : "no"),
                (withSelectList ? "with" : "without"));
    resetTupleBatch(hasNull, withSelectList);
    resetAnsForInsert(grpIdxs);
    hashTable->reset();
    for (int iter = 0; iter < 2; iter++) {
      std::vector<uint64_t> ret = hashTable->insert(batch.get());
      EXPECT_EQ(ans, ret);
    }
  }

  void resetAnsForCheck(std::vector<uint64_t> grpIdxs) {
    ans.clear();
    for (int rowIdx = 0; rowIdx < batch->getNumOfRows(); rowIdx++) {
      std::string groupKey = "";
      for (auto grpIdx : grpIdxs) {
        bool null;
        std::string tmp = batch->vectors[grpIdx - 1]->read(rowIdx, &null);
        if (!null) groupKey += tmp;
        groupKey += null ? '$' : '@';
        // LOG_INFO("%s", groupKey.c_str());
      }
      if (groupKeys.find(groupKey) != groupKeys.end()) {
        ans.push_back(groupKeys[groupKey]);
      } else {
        ans.push_back(NOT_IN_HASHTABLE);
      }
    }
  }

  void testCheck(std::vector<uint64_t> grpIdxs, bool hasNull,
                 bool withSelectList) {
    LOG_TESTING("Check TB: %s nulls\t%s select list", (hasNull ? "has" : "no"),
                (withSelectList ? "with" : "without"));
    resetTupleBatch(hasNull, withSelectList);
    resetAnsForCheck(grpIdxs);
    std::vector<uint64_t> ret = hashTable->check(batch.get());
    EXPECT_EQ(ans, ret);
  }

  dbcommon::TupleDesc::uptr desc;
  dbcommon::TupleBatch::uptr batch;
  std::map<std::string, uint64_t> groupKeys;
  std::vector<uint64_t> ans;
  std::unique_ptr<HashTable> hashTable;
};

TEST_F(TestHashTable, TestHashOnMutipleColumn) {
  for (uint64_t i = 1; i <= desc->getNumOfColumns(); i++)
    for (uint64_t j = i + 1; j <= desc->getNumOfColumns(); j++)
      for (uint64_t k = j + 1; k <= desc->getNumOfColumns(); k++)
        test({i, j, k});
}

TEST_F(TestHashTable, TestHashOnSingleColumn) {
  for (uint64_t i = 1; i <= desc->getNumOfColumns(); i++) test({i});
}

// Also test internal NormalAccess and QuickAccess at the same time.
TEST_F(TestHashTable, TestResize) {
  dbcommon::TupleBatchUtility tbu;
  dbcommon::TupleDesc::uptr desc = (std::move(tbu.generateTupleDesc("ifs")));
  std::unique_ptr<HashTable> hashTable(new HashTable(*desc, {1, 2, 3}, 1));

  {
    dbcommon::TupleBatch::uptr batch(new dbcommon::TupleBatch(*desc, true));
    dbcommon::TupleBatchWriter &writer = batch->getTupleBatchWriter();
    std::vector<uint64_t> ans;
    for (int i = 0; i < DEFAULT_NUMBER_TUPLES_PER_BATCH; i++) {
      writer[0]->append(std::to_string(i), false);
      writer[1]->append("3.14", false);
      writer[2]->append("hah", false);
      ans.push_back(i);
    }
    batch->incNumOfRows(DEFAULT_NUMBER_TUPLES_PER_BATCH);
    std::vector<uint64_t> ret = hashTable->insert(batch.get());
    EXPECT_EQ(ans, ret);
    EXPECT_LE(
        DEFAULT_SIZE_PER_HASHKEY_BLK + DEFAULT_SIZE_PER_HASH_CHAIN_BLK * 2,
        hashTable->getMemUsed());
  }

  {  // after inserted one tuple batch, resize is coming
    dbcommon::TupleBatch::uptr batch(new dbcommon::TupleBatch(*desc, true));
    dbcommon::TupleBatchWriter &writer = batch->getTupleBatchWriter();
    std::vector<uint64_t> ans;
    for (int i = 0; i < DEFAULT_NUMBER_TUPLES_PER_BATCH; i++) {
      writer[0]->append("0", false);
      writer[1]->append("3.14", false);
      writer[2]->append(std::to_string(i), false);
      ans.push_back(i + DEFAULT_NUMBER_TUPLES_PER_BATCH);
    }
    batch->incNumOfRows(DEFAULT_NUMBER_TUPLES_PER_BATCH);
    std::vector<uint64_t> ret = hashTable->insert(batch.get());
    EXPECT_EQ(ans, ret);
    ret = hashTable->insert(batch.get());
    EXPECT_EQ(ans, ret);
    EXPECT_LE(
        DEFAULT_SIZE_PER_HASHKEY_BLK + DEFAULT_SIZE_PER_HASH_CHAIN_BLK * 2,
        hashTable->getMemUsed());
  }
}

TEST_F(TestHashTable, TestGetHashkeys) {
  TupleBatchUtility tbu;
  auto desc = tbu.generateTupleDesc(
      "schema: "
      "boolean "
      "int8 int16 int32 int64 "
      "float double decimal_new "
      "bpchar bpchar(10) varchar varchar(5) string "
      "binary "
      "timestamp time date");
  std::vector<uint64_t> grpIdxs(desc->getNumOfColumns());
  std::iota(grpIdxs.begin(), grpIdxs.end(), 1);
  NativeAggHashTable aggHashtable(*desc, grpIdxs, 0);
  NativeJoinHashTable joinHashtable(*desc, grpIdxs, 0);
  joinHashtable.setupJoin(true, *desc, grpIdxs, *desc, grpIdxs);

  auto tb = tbu.generateTupleBatchRandom(*desc, 0, 10, true);
  aggHashtable.insert(tb.get());
  joinHashtable.insert(tb.get());
  auto agghashkeys = aggHashtable.getHashKeys();
  auto joinhashkeys = joinHashtable.getHashKeys();
  EXPECT_EQ(agghashkeys->toString(), joinhashkeys->toString());
}

}  // namespace dbcommon
