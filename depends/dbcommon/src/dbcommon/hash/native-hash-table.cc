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

#include "dbcommon/hash/native-hash-table.h"

namespace dbcommon {

std::unique_ptr<dbcommon::TupleBatch> NativeJoinHashTable::getHashKeys() {
  auto groupCount = hashkeys_.getNumOfGroups();
  if (retrievedHashekeyCount_ >= groupCount) return nullptr;

  uint64_t size =
      DEFAULT_NUMBER_TUPLES_PER_BATCH + retrievedHashekeyCount_ <= groupCount
          ? DEFAULT_NUMBER_TUPLES_PER_BATCH
          : groupCount - retrievedHashekeyCount_;
  hashCellPtrs_.resize(size);
  HashBucketBuf::NormalAccessor accessor(chainBuf_);
  for (auto i = 0; i < size; i++) {
    hashCellPtrs_[i] = (*accessor.at(i + retrievedHashekeyCount_))->data;
  }
  retrievedHashekeyCount_ += size;

  return hashkeys_.retrieveHashkeys(hashCellPtrs_);
}

std::pair<TupleBatch::uptr, TupleBatch::uptr>
NativeJoinHashTable::retrieveInnerJoin(const TupleBatch *outerTbInput,
                                       const std::vector<uint64_t> &groupNos,
                                       size_t *num, void **ptr) {
  auto ret = hashkeys_.retrieveInnerJoin(outerTbInput, groupNos, num, ptr);
  if (ret.second) ret.second->permutate(internalToExternalIdxMap_);
  return ret;
}

TupleBatch *NativeJoinHashTable::retrieveInnerTuple(JoinKey *ptr) {
  auto ret = hashkeys_.retrieveInnerTuple(ptr);
  ret->permutate(internalToExternalIdxMap_);
  return ret;
}

void NativeJoinHashTable::setupJoin(
    bool storeInnerTable, const TupleDesc &outerTupleDesc,
    const std::vector<uint64_t> &outerHashColIdxs,
    const TupleDesc &innerTupleDesc,
    const std::vector<uint64_t> &innerHashColIdxs) {
  storeInnerTable_ = storeInnerTable;
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
  }
  dbcommon::TupleDesc newInnerTupleDesc;
  for (auto i = 0; i < innerTupleDesc.getNumOfColumns(); i++) {
    newInnerTupleDesc.add(
        "", innerTupleDesc.getColumnType(internalToExternalIdxMap_[i]),
        innerTupleDesc.getColumnTypeModifier(internalToExternalIdxMap_[i]));
  }
  std::vector<uint64_t> newInnerHashColIdxs(innerHashColIdxs.size());
  for (auto i = 0; i < innerHashColIdxs.size(); i++) {
    newInnerHashColIdxs[i] =
        externalToInternalIdxMap_[innerHashColIdxs[i] - 1] + 1;
  }
  // std::iota(newInnerHashColIdxs.begin(), newInnerHashColIdxs.end(), 1);
  hashkeys_.setup(outerTupleDesc, outerHashColIdxs, newInnerTupleDesc,
                  newInnerHashColIdxs);
  resetInput(newInnerTupleDesc, newInnerHashColIdxs);
}

}  // namespace dbcommon
