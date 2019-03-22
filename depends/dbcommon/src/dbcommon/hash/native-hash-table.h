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

#ifndef DBCOMMON_SRC_DBCOMMON_HASH_NATIVE_HASH_TABLE_H_
#define DBCOMMON_SRC_DBCOMMON_HASH_NATIVE_HASH_TABLE_H_

#include <cmath>
#include <memory>
#include <numeric>
#include <set>
#include <utility>
#include <vector>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/hash/hash-keys.h"
#include "dbcommon/hash/tuple-batch-hasher.h"
#include "dbcommon/utils/flat-memory-buffer.h"
#include "dbcommon/utils/int-util.h"
#include "dbcommon/utils/macro.h"

#define MAX_LOAD_FACTOR 0.5
namespace dbcommon {

// TODO(chiyang):
// 1. add direct hash for short length field
class NativeHashTable {
 public:
  virtual ~NativeHashTable() {
#ifdef CHECK_COLLISIONS
    LOG_INFO(
        "\n\n"
        "**** Native Hash Table Debug Info **** \n"
        "* Size of buckets:       %llu \n"
        "* Capability of buckets: %llu \n"
        "* Number of distinct hash key:     %llu \n"
        "* Number of distinct hash value:   %lu \n"
        "* Number of using hash bucket:     %llu \n"
        "* Number of buckets collision:     %llu \n"
        "* Number of hash value collision:  %llu \n"
        "************************************** \n\n",
        buckets_.size(), capability_, getNumberOfGroups(), hashValueSet_.size(),
        numOfHashChain_, numOfHashBucketsCollision_, numOfHashValueCollision_);
#endif
  }

  uint64_t getNumberOfGroups() { return chainBuf_.size(); }

  virtual void resetInput(const dbcommon::TupleDesc &inputTupleDesc,
                          const std::vector<uint64_t> &hashColIdxs) = 0;

  virtual void reset() = 0;

  virtual std::vector<uint64_t> &insert(const dbcommon::TupleBatch *batch) = 0;

  virtual std::vector<uint64_t> &check(const dbcommon::TupleBatch *batch) = 0;

  virtual std::unique_ptr<dbcommon::TupleBatch> getHashKeys() = 0;

  virtual double getMemUsed() = 0;

 protected:
  // @param desc Tuple description of input tuple batch.
  // @param grpColIdx Determine which fields to construct hash key.
  NativeHashTable(const dbcommon::TupleDesc &inputTupleDesc,
                  const std::vector<uint64_t> &grpColIdx,
                  uint64_t estedNumOfGroup) {
    uint64_t size = estedNumOfGroup * 2;
    // A large enough capability helps to reduce cost on calling
    // insertOneElemement instead of insertOneElemementQuick(not safe to quick
    // access only the first memory block of buckets when potential rehash
    // exist) when the exact group size is small.
    size = (size < DEFAULT_NUMBER_TUPLES_PER_BATCH)
               ? DEFAULT_NUMBER_TUPLES_PER_BATCH
               : size;
    size = static_cast<uint64_t>(std::ceil(size / MAX_LOAD_FACTOR));
    size = nextPowerOfTwo(size);

    // The numOfGroup might be overestimated
    if (size > HashBucketBuf::BlkSize) size = HashBucketBuf::BlkSize;

    initBuckets(size);
    estimatedBucketSize_ = size;
    batchhasher_.setKeys(inputTupleDesc, grpColIdx);
    groupNos_.resize(DEFAULT_NUMBER_TUPLES_PER_BATCH);
    hashCellPtrs_.resize(DEFAULT_NUMBER_TUPLES_PER_BATCH);
  }

  // Insert the batch into the hash table and
  // @return Corresponding groupkeyNo for each tuple
  template <typename HASHKEYS>
  std::vector<uint64_t> &insert(HASHKEYS &hashkeys_,  // NOLINT
                                const dbcommon::TupleBatch *batch) {
    auto N = batch->getNumOfRows();

    if (N == 0) {
      groupNos_.resize(0);
      return groupNos_;
    }

    if (std::is_same<HASHKEYS, JoinHashKeys>::value) {
      const_cast<TupleBatch *>(batch)->permutate(externalToInternalIdxMap_);
    }

    std::vector<uint64_t> &hashValue_ = batchhasher_.hash(batch);
    groupNos_.resize(N);
    hashCellPtrs_.resize(N);

    fetchBuckets(hashValue_);

    hashkeys_.HASHKEYS::check(batch, hashCellPtrs_, groupNos_);

    if (chainBuf_.size() + N > capability_) rescale();

    uint64_t *__restrict__ const hashValue = &hashValue_[0];
    uint64_t *__restrict__ const groupNos = &groupNos_[0];
    const void **__restrict__ const hashCellPtrs = &hashCellPtrs_[0];
    if (buckets_.enableQuickAccess()) {
      HashBucketBuf::QuickAccessor accessor(buckets_);
      for (uint64_t i = 0; i < N; ++i) {
        if (hashCellPtrs[i] == nullptr) {
          HashCell **bucket = accessor.at(hashValue[i] & mask_);
          HashCell *cell = *bucket;
          groupNos[i] = insert(hashkeys_, batch, i, hashValue[i], bucket, cell);
        } else {
          if (std::is_same<HASHKEYS, JoinHashKeys>::value) {
            hashkeys_.HASHKEYS::insert(batch, i, groupNos_[i]);
          }
        }
      }
    } else {
      for (uint64_t i = 0; i < N; ++i) {
        HashBucketBuf::NormalAccessor accessor(buckets_);
        if (hashCellPtrs[i] == nullptr) {
          HashCell **bucket = accessor.at(hashValue[i] & mask_);
          HashCell *cell = *bucket;
          groupNos[i] = insert(hashkeys_, batch, i, hashValue[i], bucket, cell);
        } else {
          if (std::is_same<HASHKEYS, JoinHashKeys>::value) {
            hashkeys_.HASHKEYS::insert(batch, i, groupNos[i]);
          }
        }
      }
    }

    if (std::is_same<HASHKEYS, JoinHashKeys>::value) {
      const_cast<TupleBatch *>(batch)->permutate(internalToExternalIdxMap_);
    }

    return groupNos_;
  }

  // Retrieve TupleBatch's corresponding groupNO inside the hash table.
  // @return Corresponding groupkeyNo for each tuple
  template <typename HASHKEYS>
  std::vector<uint64_t> &check(HASHKEYS &hashkeys_,  // NOLINT
                               const dbcommon::TupleBatch *batch) {
    auto N = batch->getNumOfRows();

    if (N == 0) {
      groupNos_.resize(0);
      return groupNos_;
    }

    std::vector<uint64_t> &hashValue_ = batchhasher_.hash(batch);
    groupNos_.resize(N);
    hashCellPtrs_.resize(N);

    fetchBuckets(hashValue_);

    hashkeys_.HASHKEYS::check(batch, hashCellPtrs_, groupNos_);

    uint64_t *__restrict__ const hashValue = &hashValue_[0];
    uint64_t *__restrict__ const groupNos = &groupNos_[0];
    const void **__restrict__ const hashCellPtrs = &hashCellPtrs_[0];
    if (buckets_.enableQuickAccess()) {
      HashBucketBuf::QuickAccessor accessor(buckets_);
      for (uint64_t i = 0; i < N; ++i) {
        if (hashCellPtrs[i] == nullptr) {
          HashCell **bucket = accessor.at(hashValue[i] & mask_);
          HashCell *cell = *bucket;
          groupNos[i] =
              cell ? check(hashkeys_, batch, i, hashValue[i], cell->next)
                   : NOT_IN_HASHTABLE;
        }
      }
    } else {
      for (uint64_t i = 0; i < N; ++i) {
        HashBucketBuf::NormalAccessor accessor(buckets_);
        if (hashCellPtrs[i] == nullptr) {
          HashCell **bucket = accessor.at(hashValue[i] & mask_);
          HashCell *cell = *bucket;
          groupNos[i] =
              cell ? check(hashkeys_, batch, i, hashValue[i], cell->next)
                   : NOT_IN_HASHTABLE;
        }
      }
    }

    return groupNos_;
  }

  typedef FlatMemBuf<HashCell *, DEFAULT_SIZE_PER_HASH_CHAIN_BLK> HashBucketBuf;

  void fetchBuckets(const std::vector<uint64_t> &hashValue_) {
    // hint for compiler optimization
    auto N = hashValue_.size();
    const uint64_t *__restrict__ const hashValue = &hashValue_[0];
    uint64_t *__restrict__ const groupNos = &groupNos_[0];
    const void **__restrict__ const hashCellPtrs = &hashCellPtrs_[0];
    memset(hashCellPtrs, 0, N * sizeof(void *));

    auto mask = mask_;
    if (buckets_.enableQuickAccess()) {
      HashBucketBuf::QuickAccessor accessor(buckets_);
      for (uint64_t i = 0; i < N; i++) {
        auto hashcell = *accessor.at(hashValue[i] & mask);
        if (hashcell) {
          groupNos[i] = hashcell->groupNo;
          hashCellPtrs[i] = hashcell->data;
        }
      }
    } else {
      for (uint64_t i = 0; i < N; i++) {
        HashBucketBuf::NormalAccessor accessor(buckets_);
        auto hashcell = *accessor.at(hashValue[i] & mask);
        if (hashcell) {
          groupNos[i] = hashcell->groupNo;
          hashCellPtrs[i] = hashcell->data;
        }
      }
    }
  }

  template <typename HASHKEYS>
  inline uint64_t insert(HASHKEYS &hashkeys_,  // NOLINT
                         const dbcommon::TupleBatch *batch, uint64_t index,
                         uint64_t hashValue, HashCell **bucket,
                         HashCell *curCell) {
    while (nullptr != curCell) {
      if (hashValue == curCell->hashVal) {
        // specified the call of subclass to avoid virtual function call
        if (hashkeys_.HASHKEYS::check(batch, index, curCell)) {
          if (std::is_same<HASHKEYS, JoinHashKeys>::value) {
            hashkeys_.HASHKEYS::insert(batch, index, curCell->groupNo);
          }
          return curCell->groupNo;
        }
#ifdef CHECK_COLLISIONS
        numOfHashValueCollision_++;
#endif
      }
#ifdef CHECK_COLLISIONS
      if (hashValue != curCell->hashVal) numOfHashBucketsCollision_++;
#endif
      curCell = curCell->next;
    }

    curCell = hashkeys_.HASHKEYS::add(batch, index, hashValue);
    updateHashChain(bucket, curCell);

    chainBuf_.push_back(curCell);
    return curCell->groupNo;
  }

  template <typename HASHKEYS>
  inline uint64_t check(HASHKEYS &hashkeys_,  // NOLINT
                        const dbcommon::TupleBatch *batch, uint64_t index,
                        uint64_t hashValue, HashCell *curCell) {
    while (nullptr != curCell) {
      if (hashValue == curCell->hashVal) {
        // specified the call of subclass to avoid virtual function call
        if (hashkeys_.HASHKEYS::check(batch, index, curCell))
          return curCell->groupNo;
#ifdef CHECK_COLLISIONS
        numOfHashValueCollision_++;
#endif
      }
#ifdef CHECK_COLLISIONS
      if (hashValue != curCell->hashVal) numOfHashBucketsCollision_++;
#endif
      curCell = curCell->next;
    }
    return NOT_IN_HASHTABLE;
  }

  void initBuckets(uint64_t size) {
    buckets_.resize(size);
    mask_ = size - 1;
    capability_ = size * MAX_LOAD_FACTOR;
    buckets_.memZero(size);  // set to nullptr
  }

  inline void updateHashChain(HashCell **bktPtr, HashCell *addedCellPtr) {
#ifdef CHECK_COLLISIONS
    if (*bktPtr == nullptr) {
      numOfHashChain_++;
    }
    hashValueSet_.insert(addedCellPtr->hashVal);
#endif
    addedCellPtr->next = *bktPtr;
    *bktPtr = addedCellPtr;
  }

  void rescale() {
#ifdef CHECK_COLLISIONS
    numOfHashChain_ = 0;
#endif
    uint64_t oldSize = mask_ + 1;
    initBuckets(oldSize * 2);
    if (buckets_.enableQuickAccess()) {
      HashBucketBuf::QuickAccessor chainBufAccessor(chainBuf_);
      HashBucketBuf::QuickAccessor bucketAccessor(buckets_);
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < chainBuf_.size(); i++) {
        HashCell **iter = chainBufAccessor.at(i);
        HashCell **bkt = bucketAccessor.at((*iter)->hashVal & mask_);
        updateHashChain(bkt, *iter);
      }
    } else {
      HashBucketBuf::NormalAccessor chainBufAccessor(chainBuf_);
      HashBucketBuf::NormalAccessor bucketAccessor(buckets_);
#pragma clang loop unroll_count(4)
      for (uint64_t i = 0; i < chainBuf_.size(); i++) {
        HashCell **iter = chainBufAccessor.at(i);
        HashCell **bkt = bucketAccessor.at((*iter)->hashVal & mask_);
        updateHashChain(bkt, *iter);
      }
    }
  }

  uint64_t estimatedBucketSize_ = 0;
  uint64_t mask_ = 0;  // equal to the size of buckets - 1
  uint64_t capability_ = 0;

  HashBucketBuf buckets_;
  HashBucketBuf chainBuf_;

  TupleBatchHasher batchhasher_;  // tuple batch hash function

  // return buffer for each TupleBatch
  std::vector<uint64_t> groupNos_;
  // intermediate buffer for each TupleBatch
  std::vector<const void *> hashCellPtrs_;

  bool storeInnerTable_ = false;

  std::vector<uint32_t> internalToExternalIdxMap_, externalToInternalIdxMap_;

#ifdef CHECK_COLLISIONS
  // different hash key but identical hash value
  uint64_t numOfHashValueCollision_ = 0;
  // different hash value but identical hash bucket
  uint64_t numOfHashBucketsCollision_ = 0;
  uint64_t numOfHashChain_ = 0;
  std::set<uint64_t> hashValueSet_;
#endif
};

class NativeAggHashTable final : public NativeHashTable {
 public:
  NativeAggHashTable(const dbcommon::TupleDesc &inputTupleDesc,
                     const std::vector<uint64_t> &grpColIdx,
                     uint64_t estedNumOfGroup)
      : NativeHashTable(inputTupleDesc, grpColIdx, estedNumOfGroup),
        hashkeys_(inputTupleDesc, grpColIdx) {}

  void resetInput(const dbcommon::TupleDesc &inputTupleDesc,
                  const std::vector<uint64_t> &hashColIdxs) override {
    batchhasher_.setKeys(inputTupleDesc, hashColIdxs);
    hashkeys_.resetInput(inputTupleDesc, hashColIdxs);
  }

  void reset() override {
    initBuckets(estimatedBucketSize_);
    chainBuf_.resize(0);
    hashkeys_.reset();
  }

  std::vector<uint64_t> &insert(const dbcommon::TupleBatch *batch) override {
    return NativeHashTable::insert(hashkeys_, batch);
  }

  std::vector<uint64_t> &check(const dbcommon::TupleBatch *batch) override {
    return NativeHashTable::check(hashkeys_, batch);
  }

  std::unique_ptr<dbcommon::TupleBatch> getHashKeys() override {
    return hashkeys_.getHashkeys();
  }

  double getMemUsed() override {
    return buckets_.getMemUsed() + chainBuf_.getMemUsed() +
           hashkeys_.getMemUsed();
  }

 private:
  AggHashKeys hashkeys_;  // hash keys container
};

class NativeJoinHashTable final : public NativeHashTable {
 public:
  NativeJoinHashTable(const dbcommon::TupleDesc &inputTupleDesc,
                      const std::vector<uint64_t> &grpColIdx,
                      uint64_t estedNumOfGroup)
      : NativeHashTable(inputTupleDesc, grpColIdx, estedNumOfGroup),
        hashkeys_(inputTupleDesc, grpColIdx) {}

  void resetInput(const dbcommon::TupleDesc &inputTupleDesc,
                  const std::vector<uint64_t> &hashColIdxs) override {
    batchhasher_.setKeys(inputTupleDesc, hashColIdxs);
    hashkeys_.resetInput(inputTupleDesc, hashColIdxs);
  }

  void reset() override {
    initBuckets(estimatedBucketSize_);
    chainBuf_.resize(0);
    hashkeys_.reset();
  }

  std::vector<uint64_t> &insert(const dbcommon::TupleBatch *batch) override {
    return NativeHashTable::insert(hashkeys_, batch);
  }

  std::vector<uint64_t> &check(const dbcommon::TupleBatch *batch) override {
    return NativeHashTable::check(hashkeys_, batch);
  }

  std::unique_ptr<dbcommon::TupleBatch> getHashKeys() override;

  double getMemUsed() override {
    return buckets_.getMemUsed() + chainBuf_.getMemUsed() +
           hashkeys_.getMemUsed();
  }

  // @param[in/out] num The groupNo to be retrieved.
  // @param[in/out] ptr The tuple to be retrieved.
  // @return <outerTb, innerTb>
  std::pair<TupleBatch::uptr, TupleBatch::uptr> retrieveInnerJoin(
      const TupleBatch *outerTbInput, const std::vector<uint64_t> &groupNos,
      size_t *num, void **ptr);

  TupleBatch *retrieveInnerTuple(JoinKey *ptr);

  void setupJoin(bool storeInnerTable, const TupleDesc &outerTupleDesc,
                 const std::vector<uint64_t> &outerHashColIdxs,
                 const TupleDesc &innerTupleDesc,
                 const std::vector<uint64_t> &innerHashColIdxs);

 private:
  JoinHashKeys hashkeys_;  // hash keys container
  size_t retrievedHashekeyCount_ = 0;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_HASH_NATIVE_HASH_TABLE_H_
