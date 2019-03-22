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

#ifndef DBCOMMON_SRC_DBCOMMON_HASH_HASH_KEYS_H_
#define DBCOMMON_SRC_DBCOMMON_HASH_HASH_KEYS_H_

#include <cstring>
#include <deque>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/type/type-util.h"
#include "dbcommon/utils/block-memory-buffer.h"
#include "dbcommon/utils/byte-buffer.h"
#include "dbcommon/utils/flat-memory-buffer.h"
#include "dbcommon/utils/join-tuple-buffer.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

#define NOT_IN_HASHTABLE uint64_t(-1)
struct alignas(8) HashCell {
  uint64_t hashVal;
  uint64_t groupNo;  // start from 0
  HashCell *next;
  char data[0];  // pointer to compact hash key data
};
static_assert(sizeof(HashCell) == 24, "");
static_assert(sizeof(JoinKey *) == sizeof(uint64_t), "");

// HashKeys stores all the hash keys in the hash table.
// It has been proved that NSM get better performance than DSM.
class HashKeys {
 public:
  HashKeys() {}

  // @param hashColIdxs Column index start from 1
  HashKeys(const dbcommon::TupleDesc &inputTupleDesc,
           const std::vector<uint64_t> &hashColIdxs) {
    resetInput(inputTupleDesc, hashColIdxs);
  }

  // Change the description of input TB.
  void resetInput(const dbcommon::TupleDesc &inputTupleDesc,
                  const std::vector<uint64_t> &hashColIdxs) {
    assert(hashColIdxs_.empty() || hashColIdxs.size() == hashColIdxs_.size());
    inputTupleDesc_ = inputTupleDesc;
    hashColIdxs_.clear();
    for (uint64_t i : hashColIdxs) hashColIdxs_.push_back(i - 1);
    isVarLenCol_.resize(hashColIdxs.size(), false);
    for (uint32_t i = 0; i < hashColIdxs.size(); i++) {
      dbcommon::TypeKind type =
          inputTupleDesc.getColumnType(hashColIdxs[i] - 1);
      if (type == STRINGID || type == CHARID || type == VARCHARID ||
          type == BINARYID || type == DECIMALID || type == STRUCTEXID ||
          type == IOBASETYPEID) {
        isVarLenCol_[i] = true;
      }
    }
  }

  void reset() {
    numOfGroups_ = 0;
    hashKeyBuf_.reset();
  }

  // Return hash keys in the form of tuple batch.
  virtual std::unique_ptr<dbcommon::TupleBatch> getHashkeys() {
    return nullptr;
  }

  // @return HashCell that contains hash key
  inline HashCell *add(const dbcommon::TupleBatch *batch, uint64_t index,
                       uint64_t hashVal) {
    assert(index < batch->getNumOfRows());

    ++numOfGroups_;
    HashCell *cell = makeTuple(batch, index, hashVal, numOfGroups_ - 1);

    return cell;
  }

  // Check whether a single tuple match the content in the HashCell.
  bool check(const dbcommon::TupleBatch *batch, uint64_t index,
             HashCell *hashcell) {
    const char *dataPtr = hashcell->data;
    uint64_t step;  // serve as a mark also
    for (uint64_t i : hashColIdxs_) {
      dbcommon::Vector *vec = batch->getColumn(i);
      step = vec->checkGroupByKey(index, dataPtr);
      if (step == 0) return false;
      dataPtr = dbcommon::alignedAddress(dataPtr + step);
    }

    return true;
  }

  // Check each tuple inside tuple batch match the content in the HashCell.
  // @param[in] batch
  // @param[in/out] hashCells' element will be nullptr if and only if not match
  // @groupNos[out] corresponding groupNo if matched, otherwise undefined
  void check(const dbcommon::TupleBatch *batch,
             std::vector<const void *> &__restrict__ hashCells,
             std::vector<uint64_t> &__restrict__ groupNos) {
    for (uint64_t i : hashColIdxs_) {
      dbcommon::Vector *vec = batch->getColumn(i);
      vec->checkGroupByKeys(hashCells);
    }
  }

  void insert(const TupleBatch *innerTb, uint64_t idx, uintptr_t groupNo) {}

  uint64_t getNumOfGroups() { return numOfGroups_; }

  double getMemUsed() { return hashKeyBuf_.getMemUsed(); }

 protected:
  // @return a pointer to a new HashCell that contains the hash key
  inline HashCell *makeTuple(const dbcommon::TupleBatch *batch, uint64_t index,
                             uint64_t hashVal, uint64_t groupNo) {
    // Step 1: Construct HashCell header
    auto tmpHashCell =
        HashCell({.hashVal = hashVal, .groupNo = groupNo, .next = nullptr});
    hashKeyBuf_.appendRawData(tmpHashCell);

    // Step 2: Construct each field of HashCell.data
    for (uint64_t i = 0; i < hashColIdxs_.size(); i++) {
      dbcommon::Vector *vec = batch->getColumn(hashColIdxs_[i]);
      uint64_t len;
      bool isNull;
      const char *dataPtr;

      dataPtr = vec->read(index, &len, &isNull);

      if (isVarLenCol_[i])
        hashKeyBuf_.appendVarLen(isNull, len, dataPtr);
      else
        hashKeyBuf_.appendFixedLen(isNull, len, dataPtr);
      hashKeyBuf_.paddingField();
    }
    auto ret = hashKeyBuf_.currentTuplePtr();
    hashKeyBuf_.paddingTuple();

    // Step 3: return
    return const_cast<HashCell *>(reinterpret_cast<const HashCell *>(ret));
  }

  uint64_t numOfGroups_ = 0;
  // serve as a memory block to store tuple in NSM
  BlockMemBuf<DEFAULT_SIZE_PER_HASHKEY_BLK> hashKeyBuf_;

  dbcommon::TupleDesc inputTupleDesc_;
  std::vector<uint64_t> hashColIdxs_;  // index start from 0
  std::vector<bool> isVarLenCol_;      // check variable-length in hashColIdx
};

class AggHashKeys final : public HashKeys {
 public:
  AggHashKeys() {}

  AggHashKeys(const dbcommon::TupleDesc &inputTupleDesc,
              const std::vector<uint64_t> &hashColIdxs)
      : HashKeys(inputTupleDesc, hashColIdxs) {}

  inline HashCell *add(const dbcommon::TupleBatch *batch, uint64_t index,
                       uint64_t hashVal) {
    assert(index < batch->getNumOfRows());

    if ((numOfGroups_ / DEFAULT_NUMBER_TUPLES_PER_BATCH) >=
        hashkeyTBs_.size()) {
      hashkeyTBs_.push_back(std::unique_ptr<dbcommon::TupleBatch>(
          new dbcommon::TupleBatch(inputTupleDesc_, true)));
      currHashkeyTB_ = hashkeyTBs_.back().get();
    }
    ++numOfGroups_;
    currHashkeyTB_->append(*batch, index);

    HashCell *cell = makeTuple(batch, index, hashVal, numOfGroups_ - 1);

    return cell;
  }

  // Return the Keys in group by clause within one tuple batch
  std::unique_ptr<dbcommon::TupleBatch> getHashkeys() override {
    if (hashkeyTBs_.empty()) {
      return nullptr;
    } else {
      std::unique_ptr<dbcommon::TupleBatch> retval =
          std::move(hashkeyTBs_.front());
      hashkeyTBs_.pop_front();
      return std::move(retval);
    }
  }

  void reset() {
    HashKeys::reset();
    hashkeyTBs_.clear();
  }

  double getMemUsed() {
    double memUsed = hashKeyBuf_.getMemUsed();
    for (const auto &tb : hashkeyTBs_) memUsed += tb->getMemUsed();
    return memUsed;
  }

 private:
  std::deque<std::unique_ptr<dbcommon::TupleBatch>> hashkeyTBs_;
  dbcommon::TupleBatch *currHashkeyTB_ = nullptr;
};

class JoinHashKeys final : public HashKeys {
 public:
  JoinHashKeys() {}

  JoinHashKeys(const dbcommon::TupleDesc &inputTupleDesc,
               const std::vector<uint64_t> &hashColIdxs)
      : HashKeys(inputTupleDesc, hashColIdxs) {}

  // Change the description of input TB.
  void resetInput(const dbcommon::TupleDesc &inputTupleDesc,
                  const std::vector<uint64_t> &hashColIdxs) {
    assert(hashColIdxs_.empty() || hashColIdxs.size() == hashColIdxs_.size());
    inputTupleDesc_ = inputTupleDesc;
    hashColIdxs_.clear();
    for (uint64_t i : hashColIdxs) hashColIdxs_.push_back(i - 1);
    isVarLenCol_.resize(inputTupleDesc.getNumOfColumns(), false);
    for (uint32_t i = 0; i < hashColIdxs.size(); i++) {
      dbcommon::TypeKind type =
          inputTupleDesc.getColumnType(hashColIdxs[i] - 1);
      if (type == STRINGID || type == CHARID || type == VARCHARID ||
          type == BINARYID || type == DECIMALID || type == STRUCTEXID ||
          type == IOBASETYPEID) {
        isVarLenCol_[i] = true;
      }
    }
    for (uint32_t i = hashColIdxs.size(); i < inputTupleDesc.getNumOfColumns();
         i++) {
      dbcommon::TypeKind type = inputTupleDesc.getColumnType(i);
      if (type == STRINGID || type == CHARID || type == VARCHARID ||
          type == BINARYID || type == DECIMALID || type == STRUCTEXID ||
          type == IOBASETYPEID) {
        isVarLenCol_[i] = true;
      }
    }
  }

  // @return HashCell that contains hash key
  inline HashCell *add(const dbcommon::TupleBatch *batch, uint64_t index,
                       uint64_t hashVal) {
    assert(index < batch->getNumOfRows());

    ++numOfGroups_;

    // Step 1: Construct HashCell header
    auto tmpHashCell = HashCell(
        {.hashVal = hashVal, .groupNo = NOT_IN_HASHTABLE, .next = nullptr});
    hashKeyBuf_.appendRawData(tmpHashCell);

    // Step 2: Construct each field of HashCell's HashKey data
    for (uint64_t i = 0; i < hashColIdxs_.size(); i++) {
      dbcommon::Vector *vec = batch->getColumn(hashColIdxs_[i]);
      uint64_t len;
      bool isNull;
      const char *dataPtr;

      dataPtr = vec->read(index, &len, &isNull);

      if (isVarLenCol_[i])
        hashKeyBuf_.appendVarLen(isNull, len, dataPtr);
      else
        hashKeyBuf_.appendFixedLen(isNull, len, dataPtr);
      hashKeyBuf_.paddingField();
    }
    auto joinKeyOffset = hashKeyBuf_.currentTupleSize();
    hashKeyBuf_.appendRawData<JoinKey *>(nullptr);

    // Step 3: Construct each field of HashCell's JoinKey data
    for (uint64_t i = hashColIdxs_.size(); i < isVarLenCol_.size(); i++) {
      dbcommon::Vector *vec = batch->getColumn(i);
      uint64_t len;
      bool isNull;
      const char *dataPtr;

      dataPtr = vec->read(index, &len, &isNull);

      if (isVarLenCol_[i])
        hashKeyBuf_.appendVarLen(isNull, len, dataPtr);
      else
        hashKeyBuf_.appendFixedLen(isNull, len, dataPtr);
      // hashKeyBuf_.paddingField();
    }

    // Step 4: set offset
    auto ret = const_cast<HashCell *>(
        reinterpret_cast<const HashCell *>(hashKeyBuf_.currentTuplePtr()));
    ret->groupNo = reinterpret_cast<uintptr_t>(ret) + joinKeyOffset;
    hashKeyBuf_.paddingTuple();

    return const_cast<HashCell *>(reinterpret_cast<const HashCell *>(ret));
  }

  void reset() { HashKeys::reset(); }

  void setup(const TupleDesc &outerTupleDesc,
             const std::vector<uint64_t> &outerHashColIdxs,
             const TupleDesc &innerTupleDesc,
             const std::vector<uint64_t> &innerHashColIdxs) {
    joinBuffer_.setup(outerTupleDesc, outerHashColIdxs, innerTupleDesc,
                      innerHashColIdxs);
  }

  void insert(const TupleBatch *tb, uint64_t idx, uintptr_t groupNo) {
    joinBuffer_.insert(tb, idx, reinterpret_cast<JoinKey *>(groupNo));
  }

  std::pair<TupleBatch::uptr, TupleBatch::uptr> retrieveInnerJoin(
      const TupleBatch *outerTbInput, const std::vector<uint64_t> &groupNos,
      size_t *num, void **ptr) {
    return joinBuffer_.retrieveInnerJoin(outerTbInput, groupNos, num, ptr);
  }

  TupleBatch *retrieveInnerTuple(JoinKey *ptr) {
    return joinBuffer_.retrieveInnerTuple(ptr);
  }

  double getMemUsed() {
    return hashKeyBuf_.getMemUsed() + joinBuffer_.getMemUsed();
  }

  std::unique_ptr<TupleBatch> retrieveHashkeys(
      std::vector<const void *> &__restrict__ hashCells);

 private:
  JoinTupleBuffer joinBuffer_;
};

}  // namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_HASH_HASH_KEYS_H_
