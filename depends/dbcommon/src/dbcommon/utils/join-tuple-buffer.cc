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

#include "dbcommon/utils/join-tuple-buffer.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <utility>

#include "dbcommon/hash/hash-keys.h"
#include "dbcommon/type/decimal.h"

namespace dbcommon {

JoinTupleBuffer::JoinTupleBuffer(
    const TupleDesc &outerTupleDesc,
    const std::vector<uint64_t> &outerHashColIdxs,
    const TupleDesc &innerTupleDesc,
    const std::vector<uint64_t> &innerHashColIdxs) {
  scalarTb_.isScalarTupleBatch_ = true;
  scalarTb_.numOfRowsPlain = 1;
  setup(outerTupleDesc, outerHashColIdxs, innerTupleDesc, innerHashColIdxs);
}

void JoinTupleBuffer::setup(const TupleDesc &outerTupleDesc,
                            const std::vector<uint64_t> &outerHashColIdxs,
                            const TupleDesc &innerTupleDesc,
                            const std::vector<uint64_t> &innerHashColIdxs) {
  assert(outerHashColIdxs.size() == innerHashColIdxs.size());
  outerHashColIdxs_ = outerHashColIdxs;
  innerHashColIdxs_ = innerHashColIdxs;
  outerTupleDesc_ = outerTupleDesc;
  internalToExternalIdxMap_.resize(innerTupleDesc.getNumOfColumns());
  externalToInternalIdxMap_.resize(innerTupleDesc.getNumOfColumns());
  std::iota(internalToExternalIdxMap_.begin(), internalToExternalIdxMap_.end(),
            0);
  auto fixedLens = innerTupleDesc.getFixedLengths();
  std::vector<bool> isHashkey(innerTupleDesc.getNumOfColumns(), false);
  for (auto idx : innerHashColIdxs) isHashkey[idx - 1] = true;
  std::stable_sort(
      internalToExternalIdxMap_.begin(), internalToExternalIdxMap_.end(),
      [&](const uint32_t &x, const uint32_t &y) -> bool {  // NOLINT
        return (isHashkey[x] == false && isHashkey[y] == false &&
                fixedLens[x] > fixedLens[y]) ||
               (isHashkey[x] > isHashkey[y]);
      });
  for (auto i = 0; i < internalToExternalIdxMap_.size(); i++) {
    externalToInternalIdxMap_[internalToExternalIdxMap_[i]] = i;
    addColumn(
        innerTupleDesc.getColumnType(internalToExternalIdxMap_[i]),
        innerTupleDesc.getColumnTypeModifier(internalToExternalIdxMap_[i]),
        fixedLens[internalToExternalIdxMap_[i]]);
  }
  hashColNum_ = innerHashColIdxs.size();
  fixedLenColNum_ = hashColNum_;
  while (fixedLenColNum_ < innerTupleDesc.getNumOfColumns() &&
         fixedLens[internalToExternalIdxMap_[fixedLenColNum_]])
    fixedLenColNum_++;
  totalColNum_ = innerTupleDesc.getNumOfColumns();
  scalarTb_.scalars_.resize(totalColNum_);
}

void JoinTupleBuffer::insert(const TupleBatch *tb, uint64_t tupleIdx,
                             JoinKey *joinkey) {
  // This statement intend to reduce data dependency
  auto joinkeyChain = joinkey->next;

  // Step 1: Construct HashCell header
  auto tmpTuple = JoinKey({.next = nullptr});
  innerTableData_.appendRawData(tmpTuple);

  // Step 2: Construct each field of HashCell.data
  uint64_t colIdx = hashColNum_;
  for (; colIdx < fixedLenColNum_; colIdx++) {
    dbcommon::Vector *vec = tb->getColumn(colIdx);
    uint64_t len;
    bool isNull;
    const char *dataPtr;

    dataPtr = vec->read(tupleIdx, &len, &isNull);
    innerTableData_.appendFixedLen(isNull, len, dataPtr);
  }
  for (; colIdx < totalColNum_; colIdx++) {
    dbcommon::Vector *vec = tb->getColumn(colIdx);
    uint64_t len;
    bool isNull;
    const char *dataPtr;

    dataPtr = vec->read(tupleIdx, &len, &isNull);
    innerTableData_.appendVarLen(isNull, len, dataPtr);
  }

  // Step 3: Update data chain
  auto tuple = const_cast<JoinKey *>(
      reinterpret_cast<const JoinKey *>(innerTableData_.currentTuplePtr()));
  innerTableData_.paddingTuple();
  tuple->next = joinkeyChain;
  joinkey->next = tuple;
}

std::pair<TupleBatch::uptr, TupleBatch::uptr>
JoinTupleBuffer::retrieveInnerJoin(const TupleBatch *outerTbInput,
                                   const std::vector<uint64_t> &groupNos,
                                   size_t *num, void **ptr) {
  reset();

  const auto size = groupNos.size();
  auto idx = *num;
  JoinKey *cur = nullptr;
  if (*ptr) {
    cur = reinterpret_cast<JoinKey *>(*ptr);
  } else {
    if (groupNos[idx] != NOT_IN_HASHTABLE)
      cur = reinterpret_cast<JoinKey *>(groupNos[idx]);
  }

  SelectList sel;
  sel.reserve(DEFAULT_NUMBER_TUPLES_PER_BATCH);
  if (auto originSel = outerTbInput->getSelected()) {
    while (idx < size) {
      while (cur) {
        if (tupleIdx_ >= DEFAULT_NUMBER_TUPLES_PER_BATCH) goto end;
        sel.push_back((*originSel)[idx]);
        retrieveInnerTableValue(cur->data);
        cur = cur->next;
      }
      idx++;
      if (idx < size && groupNos[idx] != NOT_IN_HASHTABLE) {
        cur = reinterpret_cast<JoinKey *>(groupNos[idx]);
      } else {
        cur = nullptr;
      }
    }

  } else {
    while (idx < size) {
      while (cur) {
        if (tupleIdx_ >= DEFAULT_NUMBER_TUPLES_PER_BATCH) goto end;
        sel.push_back(idx);
        retrieveInnerTableValue(cur->data);
        cur = cur->next;
      }
      idx++;
      if (idx < size && groupNos[idx] != NOT_IN_HASHTABLE) {
        cur = reinterpret_cast<JoinKey *>(groupNos[idx]);
      } else {
        cur = nullptr;
      }
    }
  }
end:
  *num = idx;
  *ptr = cur;
  auto innerTb = getInnerTb();
  if (innerTb) {
    assert(innerTb->getNumOfRows() == sel.size());
    auto outerTb = outerTbInput->cloneSelected(&sel);
    outerTb->setNumOfRows(innerTb->getNumOfRows());
    for (auto i = 0; i < innerHashColIdxs_.size(); i++) {
      assert(innerTb->getColumn(innerHashColIdxs_[i] - 1) == nullptr);
      innerTb->setColumn(innerHashColIdxs_[i] - 1,
                         outerTb->getColumn(outerHashColIdxs_[i] - 1)->clone(),
                         false);
    }
    return std::make_pair(std::move(outerTb), std::move(innerTb));
  } else {
    return std::make_pair(nullptr, nullptr);
  }
}

TupleBatch *JoinTupleBuffer::retrieveInnerTuple(JoinKey *ptr) {
  reset();
  retrieveInnerTableValue(ptr->data);
  auto innerTb = getInnerTb();
  assert(innerTb->getNumOfRows() == 1);
  // XXX(chiyang): Not set hash attribute here
  for (auto i = 0; i < hashColNum_; i++) scalarTb_.scalars_[i].isnull = true;
  for (auto i = hashColNum_; i < totalColNum_; i++)
    innerTb->getColumn(i)->readPlainScalar(0, &(scalarTb_.scalars_[i]));
  return &scalarTb_;
}

double JoinTupleBuffer::getMemUsed() {
  double memUsed = innerTableData_.getMemUsed();
  for (const auto info : vecMemoryInfos_) memUsed += info.cap;
  return memUsed;
}

TupleBatch::uptr JoinTupleBuffer::getInnerTb() {
  auto tupleCount = tupleIdx_;
  assert(tupleCount <= DEFAULT_NUMBER_TUPLES_PER_BATCH);
  if (tupleCount == 0) return nullptr;
  TupleBatch::uptr outTB(new TupleBatch(innerTupleDesc_.getNumOfColumns()));

  for (uint32_t colIdx = hashColNum_;
       colIdx < innerTupleDesc_.getNumOfColumns(); colIdx++) {
    std::unique_ptr<dbcommon::Vector> vec = dbcommon::Vector::BuildVector(
        innerTupleDesc_.getColumnType(colIdx), false,
        innerTupleDesc_.getColumnTypeModifier(colIdx));

    vec->setHasNull(true);
    bool *__restrict__ nullVec = vecBasicInfos_[colIdx].nullVec;
    vec->getNullBuffer()->setBools(reinterpret_cast<char *>(nullVec),
                                   tupleCount);

    char *valVec = vecBasicInfos_[colIdx].valVec;
    switch (vec->getTypeKind()) {
      case TypeKind::BOOLEANID:
      case TypeKind::TINYINTID:
        vec->setValue(valVec, tupleCount * sizeof(uint8_t));
        break;
      case TypeKind::SMALLINTID:
        vec->setValue(valVec, tupleCount * sizeof(uint16_t));
        break;
      case TypeKind::INTID:
      case TypeKind::DATEID:
      case TypeKind::FLOATID:
        vec->setValue(valVec, tupleCount * sizeof(uint32_t));
        break;
      case TypeKind::BIGINTID:
      case TypeKind::DOUBLEID:
      case TypeKind::TIMEID:
        vec->setValue(valVec, tupleCount * sizeof(uint64_t));
        break;
      case TypeKind::TIMESTAMPID: {
        // xxx(chiyang): performance missing
        vec = dbcommon::Vector::BuildVector(TypeKind::TIMESTAMPID, true, -1);
        Timestamp *timestamps = reinterpret_cast<Timestamp *>(valVec);
        vec->setHasNull(true);
        bool *__restrict__ nulls = vecBasicInfos_[colIdx].nullVec;
        for (int j = 0; j < tupleCount; j++) {
          vec->append(reinterpret_cast<char *>(&timestamps[j]),
                      sizeof(Timestamp), nulls[j]);
        }
        break;
      }
      case TypeKind::TIMESTAMPTZID: {
        // xxx(chiyang): performance missing
        vec = dbcommon::Vector::BuildVector(TypeKind::TIMESTAMPTZID, true, -1);
        Timestamp *timestamps = reinterpret_cast<Timestamp *>(valVec);
        vec->setHasNull(true);
        bool *__restrict__ nulls = vecBasicInfos_[colIdx].nullVec;
        for (int j = 0; j < tupleCount; j++) {
          vec->append(reinterpret_cast<char *>(&timestamps[j]),
                      sizeof(Timestamp), nulls[j]);
        }
        break;
      }
      case TypeKind::DECIMALNEWID: {
        vec = dbcommon::Vector::BuildVector(TypeKind::DECIMALNEWID, true, -1);
        DecimalVar *decimal = reinterpret_cast<DecimalVar *>(valVec);
        vec->setHasNull(true);
        bool *__restrict__ nulls = vecBasicInfos_[colIdx].nullVec;
        for (int j = 0; j < tupleCount; j++) {
          vec->append(reinterpret_cast<char *>(&decimal[j]), sizeof(DecimalVar),
                      nulls[j]);
        }
        break;
      }

      case TypeKind::CHARID:
      case TypeKind::STRINGID:
      case TypeKind::VARCHARID:
      case TypeKind::BINARYID:
      case TypeKind::DECIMALID:
      case TypeKind::IOBASETYPEID:
      case TypeKind::STRUCTEXID: {
        assert(vecMemoryInfos_[colIdx].size <= vecMemoryInfos_[colIdx].cap);
        vec->setLengths(vecMemoryInfos_[colIdx].lenVec, tupleCount);
        vec->setValue(vecBasicInfos_[colIdx].valVec,
                      vecMemoryInfos_[colIdx].size);
        vec->setDirectEncoding(true);
        vec->computeValPtrs();
        break;
      }

      default:
        LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Unhandled typeKind %d",
                  vec->getTypeKind());
    }  // end of switch
    outTB->setColumn(colIdx, std::move(vec), false);
  }  // end of each column
  assert(outTB->getNumOfColumns() == innerTupleDesc_.getNumOfColumns());
  outTB->setNumOfRows(tupleCount);
  outTB->permutate(internalToExternalIdxMap_);
  return std::move(outTB);
}

void JoinTupleBuffer::addColumn(TypeKind type, int64_t typeMod,
                                uint32_t fixedLen) {
  innerTupleDesc_.add("", type, typeMod, true);
  uint32_t maxLen = fixedLen;

  // Compromise to compilation issues
  auto memFreeWrapper = [](void *p) { dbcommon::cnfree(p); };

  MemBlkOwner nullVec(dbcommon::cnmalloc(DEFAULT_NUMBER_TUPLES_PER_BATCH),
                      memFreeWrapper);
  nullVecHolders_.push_back(std::move(nullVec));

  if (type == TypeKind::CHARID || type == TypeKind::VARCHARID ||
      type == TypeKind::STRINGID || type == TypeKind::BINARYID) {
    maxLen = DEFAULT_RESERVED_SIZE_OF_STRING;
  }
  MemBlkOwner valVec(
      dbcommon::cnmalloc(DEFAULT_NUMBER_TUPLES_PER_BATCH * maxLen),
      memFreeWrapper);
  valVecHolders_.push_back(std::move(valVec));

  switch (type) {
    case TypeKind::BOOLEANID:
    case TypeKind::TINYINTID:
    case TypeKind::SMALLINTID:
    case TypeKind::INTID:
    case TypeKind::DATEID:
    case TypeKind::FLOATID:
    case TypeKind::BIGINTID:
    case TypeKind::DOUBLEID:
    case TypeKind::TIMEID:
    case TypeKind::TIMESTAMPID:
    case TypeKind::TIMESTAMPTZID:
    case TypeKind::DECIMALNEWID:
      lenVecHolders_.push_back(MemBlkOwner(nullptr, nullptr));
      valPtrVecHolders_.push_back(MemBlkOwner(nullptr, nullptr));
      break;

    case TypeKind::CHARID: {
      MemBlkOwner lenVec(dbcommon::cnmalloc(DEFAULT_NUMBER_TUPLES_PER_BATCH *
                                            sizeof(uint64_t)),
                         memFreeWrapper);
      MemBlkOwner valPtrVec(dbcommon::cnmalloc(DEFAULT_NUMBER_TUPLES_PER_BATCH *
                                               sizeof(uint64_t)),
                            memFreeWrapper);
      lenVecHolders_.push_back(std::move(lenVec));
      valPtrVecHolders_.push_back(std::move(valPtrVec));
      break;
    }

    case TypeKind::STRINGID:
    case TypeKind::VARCHARID:
    case TypeKind::BINARYID:
    case TypeKind::DECIMALID:
    case TypeKind::IOBASETYPEID:
    case TypeKind::STRUCTEXID: {
      MemBlkOwner lenVec(dbcommon::cnmalloc(DEFAULT_NUMBER_TUPLES_PER_BATCH *
                                            sizeof(uint64_t)),
                         memFreeWrapper);
      lenVecHolders_.push_back(std::move(lenVec));
      valPtrVecHolders_.push_back(MemBlkOwner(nullptr, nullptr));
      break;
    }

    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Unhandled typeKind %d", type);
  }

  VecBasic vecBasic = {fixedLen,
                       reinterpret_cast<bool *>(nullVecHolders_.back().get()),
                       reinterpret_cast<char *>(valVecHolders_.back().get())};
  vecBasicInfos_.push_back(vecBasic);

  ValVecProp valVecProp = {
      0, (DEFAULT_NUMBER_TUPLES_PER_BATCH * maxLen),
      reinterpret_cast<uint64_t *>(lenVecHolders_.back().get())};
  vecMemoryInfos_.push_back(valVecProp);
}

void JoinTupleBuffer::reset() {
  tupleIdx_ = 0;
  for (int i = 0; i < vecMemoryInfos_.size(); i++) vecMemoryInfos_[i].size = 0;
}

inline void JoinTupleBuffer::setVarWidthField(uint32_t colIdx,
                                              const char *valPtr,
                                              uint32_t valLen) {
  if (vecMemoryInfos_[colIdx].cap < vecMemoryInfos_[colIdx].size + valLen) {
    vecMemoryInfos_[colIdx].cap =
        (valLen >= vecMemoryInfos_[colIdx].cap ? valLen
                                               : vecMemoryInfos_[colIdx].cap) *
        2;
    valVecHolders_[colIdx].reset(dbcommon::cnrealloc(
        valVecHolders_[colIdx].release(), vecMemoryInfos_[colIdx].cap));
    vecBasicInfos_[colIdx].valVec = valVecHolders_[colIdx].get();
  }

  memcpy(vecBasicInfos_[colIdx].valVec + vecMemoryInfos_[colIdx].size, valPtr,
         valLen);
  vecMemoryInfos_[colIdx].size += valLen;
  (vecMemoryInfos_[colIdx].lenVec)[tupleIdx_] = valLen;
}

inline void JoinTupleBuffer::setFixedWidthField(char *dst, uint64_t idx,
                                                const char *src,
                                                uint64_t width) {
  if (width == sizeof(uint64_t)) {
    reinterpret_cast<uint64_t *>(dst)[idx] =
        (*reinterpret_cast<const uint64_t *>(src));
  } else if (width == sizeof(uint32_t)) {
    reinterpret_cast<uint32_t *>(dst)[idx] =
        (*reinterpret_cast<const uint32_t *>(src));
  } else if (width == sizeof(uint16_t)) {
    reinterpret_cast<uint16_t *>(dst)[idx] =
        (*reinterpret_cast<const uint16_t *>(src));
  } else if (width == sizeof(uint8_t)) {
    reinterpret_cast<uint8_t *>(dst)[idx] =
        (*reinterpret_cast<const uint8_t *>(src));
  } else {
    memcpy(dst + idx * width, src, width);
  }
}

void JoinTupleBuffer::retrieveInnerTableValue(const char *valPtr) {
  // hint for compiler optimize
  uint64_t offset = 0;
  uint64_t tupleIdx = tupleIdx_;
  const VecBasic *__restrict__ vecBasics = vecBasicInfos_.data();

  // Step 1. Decode Front fixed-length column
  uint64_t colIdx = hashColNum_;
  for (uint64_t valFixWidthColNum = fixedLenColNum_; colIdx < valFixWidthColNum;
       colIdx++) {
    assert(vecBasics[colIdx].valWidth != 0);

    // decode and set null mark
    bool isNull = *reinterpret_cast<const bool *>(valPtr + offset);
    bool *__restrict__ nullVec = vecBasics[colIdx].nullVec;
    nullVec[tupleIdx] = isNull;

    // set data
    uint32_t width = vecBasics[colIdx].valWidth;
    offset += width;  // padding for null
    char *__restrict__ valVec = vecBasics[colIdx].valVec;
    setFixedWidthField(valVec, tupleIdx, valPtr + offset, width);
    offset += width;
  }

  // Step 2. Decode back variable-length column
  for (uint64_t VarWidthColNum = totalColNum_; colIdx < VarWidthColNum;
       colIdx++) {
    assert(vecBasics[colIdx].valWidth == 0);

    // decode data length
    uint64_t fieldDataLen =
        *reinterpret_cast<const uint64_t *>(valPtr + offset);
    offset += sizeof(uint64_t);

    // decode and set null mark
    bool isNull = *reinterpret_cast<const bool *>(valPtr + offset);
    bool *__restrict__ nullVec = vecBasics[colIdx].nullVec;
    nullVec[tupleIdx] = isNull;
    offset += 1;

    // set data
    char *__restrict__ valVec = vecBasics[colIdx].valVec;
    setVarWidthField(colIdx, valPtr + offset, fieldDataLen);
    offset += fieldDataLen;
  }
  tupleIdx_++;
  assert(tupleIdx_ <= DEFAULT_NUMBER_TUPLES_PER_BATCH);
}

void JoinTupleBuffer::retrieveInnerTableNull() {
  for (uint64_t colIdx = hashColNum_; colIdx < totalColNum_; colIdx++) {
    bool *__restrict__ nullVec = vecBasicInfos_[colIdx].nullVec;
    nullVec[tupleIdx_] = true;
  }
  tupleIdx_++;
  assert(tupleIdx_ <= DEFAULT_NUMBER_TUPLES_PER_BATCH);
}

}  // namespace dbcommon
