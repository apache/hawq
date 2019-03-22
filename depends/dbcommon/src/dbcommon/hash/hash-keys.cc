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

#include "dbcommon/hash/hash-keys.h"

namespace dbcommon {

std::unique_ptr<TupleBatch> JoinHashKeys::retrieveHashkeys(
    std::vector<const void *> &__restrict__ hashCells) {
  std::unique_ptr<TupleBatch> batch(new TupleBatch(inputTupleDesc_, true));
  for (uint64_t colIdx : hashColIdxs_) {
    dbcommon::Vector *vec = batch->getColumn(colIdx);
    for (auto tupleIdx = 0; tupleIdx < hashCells.size(); tupleIdx++) {
      const char *hashCell =
          reinterpret_cast<const char *>(hashCells[tupleIdx]);
      if (inputTupleDesc_.getFixedLengths()[colIdx]) {
        bool null = *reinterpret_cast<const bool *>(hashCell);
        uint64_t len = inputTupleDesc_.getFixedLengths()[colIdx];
        if (len == 24) {
          hashCell += 24;
        } else {
          hashCell += len >= 8 ? 8 : len;
        }
        vec->append(hashCell, len, null);
        hashCell += len;
      } else {
        uint64_t len = *reinterpret_cast<const uint64_t *>(hashCell);
        hashCell += sizeof(uint64_t);
        bool null = *reinterpret_cast<const bool *>(hashCell);
        hashCell += sizeof(bool);
        vec->append(hashCell, len, null);
        hashCell += len;
      }
      hashCells[tupleIdx] = dbcommon::alignedAddress(hashCell);
    }
  }
  batch->setNumOfRows(hashCells.size());
  for (auto colIdx = 0; colIdx < inputTupleDesc_.getNumOfColumns(); colIdx++) {
    if (batch->getColumn(colIdx)->getNumOfRows() == 0)
      batch->setColumn(colIdx, std::unique_ptr<Vector>(nullptr), true);
  }

  return batch;
}

}  // namespace dbcommon
