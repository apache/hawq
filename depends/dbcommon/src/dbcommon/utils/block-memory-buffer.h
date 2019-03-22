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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_BLOCK_MEMORY_BUFFER_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_BLOCK_MEMORY_BUFFER_H_

#include <memory>
#include <utility>
#include <vector>

#include "dbcommon/nodes/datum.h"
#include "dbcommon/utils/cutils.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

// BlockMemBuf serves as an unlimited size append-only memory buffer to store
// a series of records. BlockMemBuf ensure each its record, which is no larger
// than a block, stay in a continuous memory area.
// Each record should be encoded and decoded in their own way, whose size is no
// larger than MEMBLKSIZE.
template <std::size_t MEMBLKSIZE>
class BlockMemBuf {
 public:
  template <class T>
  inline void appendRawData(T data) {
    if (currentMemBlkPtr_ + sizeof(T) > currentMemBlkBorder_) allocNewMemBlk();
    *reinterpret_cast<T *>(currentMemBlkPtr_) = data;
    currentMemBlkPtr_ += sizeof(T);
  }

  inline void appendRawData(const char *data, uint64_t len) {
    if (currentMemBlkPtr_ + len > currentMemBlkBorder_) allocNewMemBlk();
    memcpy(currentMemBlkPtr_, data, len);
    currentMemBlkPtr_ += len;
  }

  // Append fixed length attribute in the form of
  // (null, padding,               data)
  // ([1B], [(sizeof(data) - 1)B], []) = ([(sizeof(data) * 2)B])
  inline void appendFixedLen(bool isNull, uint64_t len, const char *dataPtr) {
    assert(len == 24 || len == 16 || len == 8 || len == 4 || len == 2 ||
           len == 1);
    if (len == 24) {
      appendPaddingNull(isNull, len);
      appendRawData(*reinterpret_cast<const uint64_t *>(dataPtr));
      appendRawData(
          *reinterpret_cast<const uint64_t *>(dataPtr + sizeof(uint64_t)));
      appendRawData(*reinterpret_cast<const uint64_t *>(
          dataPtr + sizeof(uint64_t) + sizeof(uint64_t)));
      return;
    }
    appendPaddingNull(isNull, len >= 8 ? 8 : len);
    switch (len) {
      case 16:
        appendRawData(*reinterpret_cast<const Timestamp *>(dataPtr));
        break;
      case 8:
        appendRawData(*reinterpret_cast<const uint64_t *>(dataPtr));
        break;
      case 4:
        appendRawData(*reinterpret_cast<const uint32_t *>(dataPtr));
        break;
      case 2:
        appendRawData(*reinterpret_cast<const uint16_t *>(dataPtr));
        break;
      case 1:
        appendRawData(*reinterpret_cast<const uint8_t *>(dataPtr));
        break;
    }
  }

  // Append variable length attribute in the form of
  // (length, null, data)
  // ([8B],   [1B], [])
  inline void appendVarLen(bool isNull, uint64_t len, const char *dataPtr) {
    if (isNull)
      appendRawData(uint64_t(0));  // set len to zero, important!!!
    else
      appendRawData(len);
    appendPaddingNull(isNull, 1);
    if (isNull) return;
    if (currentMemBlkPtr_ + len > currentMemBlkBorder_) allocNewMemBlk();
    // xxx should align dataPtr to 8 byte and then fetch by 8 byte
    while (len > 7) {
      *reinterpret_cast<uint64_t *>(currentMemBlkPtr_) =
          *reinterpret_cast<const uint64_t *>(dataPtr);
      dataPtr += 8;
      currentMemBlkPtr_ += 8;
      len -= 8;
    }
    while (len > 0) {
      *reinterpret_cast<char *>(currentMemBlkPtr_) =
          *reinterpret_cast<const char *>(dataPtr);
      dataPtr++;
      currentMemBlkPtr_++;
      len--;
    }
  }

  // performance: padding field helps to reduce CPU fetching data cost
  inline void paddingField() {
    currentMemBlkPtr_ = dbcommon::alignedAddress(currentMemBlkPtr_);
  }

  // Pad current record and generate a new record.
  inline void paddingTuple() {
    currentMemBlkPtr_ = dbcommon::alignedAddress(currentMemBlkPtr_);
    currentTuplePtr_ = currentMemBlkPtr_;
  }

  const char *currentTuplePtr() {
    assert(currentMemBlkPtr_ >= currentTuplePtr_);
    assert(currentMemBlkPtr_ <= currentMemBlkBorder_);
    return currentTuplePtr_;
  }

  uint64_t currentTupleSize() { return currentMemBlkPtr_ - currentTuplePtr_; }

  void reset() {
    memBlkList_.clear();
    currentTuplePtr_ = nullptr;
    currentMemBlkPtr_ = nullptr;
    currentMemBlkBorder_ = nullptr;
  }

  double getMemUsed() { return MEMBLKSIZE * memBlkList_.size(); }

 private:
  void allocNewMemBlk() {
    assert(currentMemBlkPtr_ >= currentTuplePtr_);
    assert(currentMemBlkPtr_ <= currentMemBlkBorder_);
    MemBlkOwner tmpOwner(MemBlkOwner({(cnmalloc(MEMBLKSIZE)), cnfree}));
    char *newPtr = reinterpret_cast<char *>(tmpOwner.get());
    memcpy(newPtr, currentTuplePtr_, currentMemBlkPtr_ - currentTuplePtr_);
    currentMemBlkPtr_ = newPtr + (currentMemBlkPtr_ - currentTuplePtr_);
    currentTuplePtr_ = newPtr;
    currentMemBlkBorder_ = newPtr + MEMBLKSIZE;
    memBlkList_.push_back(std::move(tmpOwner));
  }

  // Append null in the form of
  // (null, padding)
  // ([1B], [(paddingLen - 1)B])
  inline void appendPaddingNull(bool data, uint64_t paddingLen) {
    assert(paddingLen == 24 || paddingLen == 16 || paddingLen == 8 ||
           paddingLen == 4 || paddingLen == 2 || paddingLen == 1);
    if (currentMemBlkPtr_ + paddingLen > currentMemBlkBorder_) allocNewMemBlk();
    *reinterpret_cast<bool *>(currentMemBlkPtr_) = data;
    currentMemBlkPtr_ += paddingLen;
  }

  std::vector<MemBlkOwner> memBlkList_;
  const char *currentTuplePtr_ = nullptr;
  char *currentMemBlkPtr_ = nullptr;
  const char *currentMemBlkBorder_ = nullptr;
};
}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_BLOCK_MEMORY_BUFFER_H_
