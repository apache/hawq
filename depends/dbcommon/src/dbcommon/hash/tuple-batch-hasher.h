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

#ifndef DBCOMMON_SRC_DBCOMMON_HASH_TUPLE_BATCH_HASHER_H_
#define DBCOMMON_SRC_DBCOMMON_HASH_TUPLE_BATCH_HASHER_H_

#include <vector>

#include "dbcommon/common/tuple-batch.h"

namespace dbcommon {

class TupleBatchHasher {
 public:
  // Construct a tuple batch hasher.
  TupleBatchHasher() {}

  // Construct a tuple batch hasher with given hash keys.
  // @param keys column indexes (1 based) of hash key.
  TupleBatchHasher(const dbcommon::TupleDesc &inputTupleDesc,
                   const std::vector<uint64_t> &keys) {
    setKeys(inputTupleDesc, keys);
  }

  // Set hash keys
  // @param keys column indexes (1 based) of hash key.
  void setKeys(const dbcommon::TupleDesc &inputTupleDesc,
               const std::vector<uint64_t> &keys) {
    if (keys.size() == 1) {
      auto type = inputTupleDesc.getColumnType(keys[0] - 1);
      useSeed = (type == TypeKind::FLOATID || type == TypeKind::DOUBLEID ||
                 type == TypeKind::DECIMALNEWID);
    }
    this->keys = keys;
  }

  void setSegsNum(int32_t segsnum) { segsNum = segsnum; }

  const std::vector<uint64_t> &getKeys() const { return keys; }

  int32_t getSegsNum() { return segsNum; }

  // Calculate hash value for the given tuple batch on given keys.
  // @param batch given tuple batch.
  // @return a vector of hash value for each tuple in tuple batch.
  std::vector<uint64_t> &hash(const dbcommon::TupleBatch *batch) {
    static_assert(sizeof(size_t) == sizeof(int64_t),
                  "can only work on 64 bit system");

    size_t size = batch->getNumOfRows();

    assert(keys.size() > 0 && "invalid input");

    // this seed is used to make hash value uniformly distributed,
    // especially for group by only one column.
    const uint64_t seed = *reinterpret_cast<const uint64_t *>("ChiYangG");
    uint64_t indexHead;
    if (useSeed) {
      indexHead = 0;
      retval.resize(size);
      std::fill(retval.begin(), retval.end(), seed);
    } else {
      indexHead = 1;
      retval.resize(size);
      batch->getColumn(keys[0] - 1)->hash(retval);
    }

    tmpval.resize(size);
    for (size_t i = indexHead; i < keys.size(); ++i) {
      batch->getColumn(keys[i] - 1)->hash(tmpval);

#pragma clang loop unroll_count(4)
      for (size_t i = 0; i < retval.size(); ++i) {
        // this comes from Hash128to64 in cityhash
        const uint64_t kMul = 0x9ddfea08eb382d69ULL;
        uint64_t a = (tmpval[i] ^ retval[i]) * kMul;
        a ^= (a >> 47);
        uint64_t b = (retval[i] ^ a) * kMul;
        b ^= (b >> 47);
        b *= kMul;
        retval[i] = b;
      }
    }

    return retval;
  }

  // Calculate hash value for the given tuple batch on given keys.
  // @param batch given tuple batch.
  // @return a vector of hash value for each tuple in tuple batch.
  std::vector<uint64_t> &cdbhash(const dbcommon::TupleBatch *batch) {
    static_assert(sizeof(size_t) == sizeof(int64_t),
                  "can only work on 64 bit system");

    size_t size = batch->getNumOfRows();

    assert(keys.size() > 0 && "invalid input");

    retval.resize(size);

    for (size_t i = 0; i < size; i++) retval[i] = cdbhash_init();

    for (size_t i = 0; i < keys.size(); ++i) {
      batch->getColumn(keys[i] - 1)->cdbHash(retval);
    }

    if (!magmaTable) {
      if (ispowof2(segsNum)) {
        for (size_t i = 0; i < size; i++) {
          retval[i] = FASTMOD(static_cast<uint32_t>(retval[i]),
                              static_cast<uint32_t>(segsNum));
        }
      } else {
        for (size_t i = 0; i < size; i++) {
          retval[i] %= segsNum;
        }
      }
    } else {
      for (size_t i = 0; i < size; ++i) {
        retval[i] = rangevseg[retval[i] %= rangeNum_];
      }
    }

    return retval;
  }

  int32_t cdbhashNoKey() { return rrIndex_++ % segsNum; }

  void setRangeVseg(int val) { rangevseg.push_back(val); }

  void setMagmaTable(bool magma) { magmaTable = magma; }

  void setMagmaRangeNum(int rangeNum) { rangeNum_ = rangeNum; }

 protected:
  bool useSeed = false;  // determine whether to use the shuffle seed
  bool magmaTable = false;
  int rangeNum_ = 0;  // magma table range num
  int32_t segsNum = 0;
  std::vector<uint64_t> keys;
  std::vector<uint64_t> retval;
  std::vector<uint64_t> tmpval;    // intermediate result for each column
  std::vector<int32_t> rangevseg;  // use for magma rangid---->vseg
  int32_t rrIndex_ = 0;            // round robin index

 private:
  // returns 1 is the input int is a power of 2 and 0 otherwize.
  int32_t ispowof2(int32_t segsNum) { return !(segsNum & (segsNum - 1)); }
};

}  // namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_HASH_TUPLE_BATCH_HASHER_H_
