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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_JOIN_TUPLE_BUFFER_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_JOIN_TUPLE_BUFFER_H_

#include <utility>
#include <vector>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/utils/block-memory-buffer.h"

namespace dbcommon {

struct JoinKey {
  JoinKey *next;
  char data[0];
};

// JoinTupleBuffer is used to generate intermediate result for join operation.
// It organize tuples as a series of tuple chains, which contains a series of
// tuple. You can view it as a two-dimensional array for tuples.
//
// JoinTupleBuffer materialize its tuple in the form like that in HashKey.
// However, it reorder the field according to the field length, which leads to
// better memory IO.
//
// There is a tight coupling between TupleBuffer and HashjoinExecNode.
class JoinTupleBuffer {
 public:
  JoinTupleBuffer() {
    scalarTb_.isScalarTupleBatch_ = true;
    scalarTb_.numOfRowsPlain = 1;
  }

  explicit JoinTupleBuffer(const TupleDesc &outerTupleDesc,
                           const std::vector<uint64_t> &outerHashColIdxs,
                           const TupleDesc &innerTupleDesc,
                           const std::vector<uint64_t> &innerHashColIdxs);

  void setup(const TupleDesc &outerTupleDesc,
             const std::vector<uint64_t> &outerHashColIdxs,
             const TupleDesc &innerTupleDesc,
             const std::vector<uint64_t> &innerHashColIdxs);

  void insert(const TupleBatch *innerTb, uint64_t tupleIdx, JoinKey *joinkey);

  // @param[in/out] num The groupNo to be retrieved.
  // @param[in/out] ptr The tuple to be retrieved.
  // @return <outerTb, innerTb>
  std::pair<TupleBatch::uptr, TupleBatch::uptr> retrieveInnerJoin(
      const TupleBatch *outerTbInput, const std::vector<uint64_t> &groupNos,
      size_t *num, void **ptr);

  TupleBatch *retrieveInnerTuple(JoinKey *ptr);

  double getMemUsed();

 private:
  void addColumn(dbcommon::TypeKind type, int64_t typeMod, uint32_t fixLen);

  void reset();

  void setVarWidthField(uint32_t colIdx, const char *valPtr, uint32_t valLen);

  void setFixedWidthField(char *dst, uint64_t idx, const char *src,
                          uint64_t width);

  void retrieveInnerTableValue(const char *valPtr);

  void retrieveInnerTableNull();

  TupleBatch::uptr getInnerTb();

  std::vector<uint32_t> internalToExternalIdxMap_, externalToInternalIdxMap_;
  std::vector<uint64_t> outerHashColIdxs_, innerHashColIdxs_;
  TupleDesc outerTupleDesc_, innerTupleDesc_;
  uint32_t hashColNum_ = 0;
  uint32_t fixedLenColNum_ = 0;
  uint32_t totalColNum_ = 0;

  BlockMemBuf<DEFAULT_SIZE_PER_HASHJOIN_BLK> innerTableData_;

  uint32_t tupleIdx_ = 0;

  // lenVecHolders_ is only used for variable width field
  // valPtrVecHolders_ is only used for CHAR(n) when getRawTupleBatch()
  std::vector<MemBlkOwner> nullVecHolders_, valVecHolders_, lenVecHolders_,
      valPtrVecHolders_;

  // basic info for a vector
  struct VecBasic {
    const uint32_t valWidth;
    bool *__restrict__ nullVec;
    char *__restrict__ valVec;
  };
  std::vector<VecBasic> vecBasicInfos_;

  // memory info for a vector, only useful for variable width field
  struct ValVecProp {
    uint32_t size;
    uint32_t cap;
    uint64_t *__restrict__ lenVec;
  };
  std::vector<ValVecProp> vecMemoryInfos_;

  // for retriveInnerTuple
  TupleBatch scalarTb_;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_JOIN_TUPLE_BUFFER_H_
