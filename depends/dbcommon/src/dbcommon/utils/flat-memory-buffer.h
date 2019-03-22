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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_FLAT_MEMORY_BUFFER_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_FLAT_MEMORY_BUFFER_H_

#include <cassert>
#include <memory>
#include <utility>
#include <vector>

#include "dbcommon/utils/cutils.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

// A FlatMemBuf is used to avoid Out of Memory when acquire for large memory,
// and reduce needless realloc. Users can use it as an array of elements.
//
// FlatMemBuf works like a single-level page table. The second level is a set of
// memory block(size specified by user) and the first level is an array of the
// pointers that points to the memory block in second level.
//
// Theoretically, accessing each element require two memory access. However,
// only one memory block exists when the size of FlatMemBuf is small, so we
// introduce two different accessor for FlatMemBuf.
//
// Be familiar with the implementation of this class, otherwise bug-prone
template <typename TYPE, std::size_t MEMBLKSIZE>
class FlatMemBuf {
 public:
  class NormalAccessor {
   public:
    explicit NormalAccessor(const FlatMemBuf &fmb)
        : memBlkPtrList(fmb.memBlkPtrList) {}

    force_inline TYPE *at(uint64_t index) {
      return reinterpret_cast<TYPE *>(memBlkPtrList[index >> ShiftLen]) +
             (index & Mask);
    }

   private:
    char **const memBlkPtrList;
  };

  class QuickAccessor {
   public:
    explicit QuickAccessor(const FlatMemBuf &fmb) : memBlkPtr(fmb.memBlkPtr0) {}

    force_inline TYPE *at(uint64_t index) {
      return reinterpret_cast<TYPE *>(memBlkPtr) + index;
    }

   private:
    char *const memBlkPtr;
  };

  FlatMemBuf() {
    static_assert((MEMBLKSIZE & (MEMBLKSIZE - 1)) == 0, "");
    static_assert(((sizeof(TYPE) & (sizeof(TYPE) - 1)) == 0),
                  "supposed sizeof(TYPE) to be power of two");
    reserve(BlkSize);
    memBlkPtr0 = memBlkPtrListVec[0];
  }

  force_inline void reserve(uint64_t size) {
    while ((size + BlkSize - 1) / BlkSize > memBlkList.size()) {
      // Exactly, each MemBlkOwner contains extra spare memory beyond the
      // specified MEMBLKSIZE, in order to align memory address into the
      // multiple of the cache line size, which helps to reduce cache miss when
      // size_ is small.
      // For example, the number of the groups in TPCH-Q1 is 4 and
      // sizeof(AggGroupValue) is 16. There is only 64 byte memory keeping being
      // accessed, which fits into one cache line perfectly.
      MemBlkOwner tmpOwner(MemBlkOwner(
          {(cnmalloc(MEMBLKSIZE + DEFAULT_CACHE_LINE_SIZE - 1)), cnfree}));
      char *tmpPtr =
          alignedAddressInto<DEFAULT_CACHE_LINE_SIZE>(tmpOwner.get());
      assert(((uint64_t)tmpPtr & (DEFAULT_CACHE_LINE_SIZE - 1)) == 0);
      memBlkPtrListVec.push_back(tmpPtr);
      memBlkPtrList = &memBlkPtrListVec[0];
      memBlkList.push_back(std::move(tmpOwner));
    }
  }

  force_inline void resize(uint64_t size) {
    reserve(size);
    size_ = size;
  }

  force_inline uint64_t size() { return size_; }

  force_inline TYPE dataAt(uint64_t index) { return *ptrAt(index); }

  force_inline TYPE *getPtrAtBlk0() {
    return reinterpret_cast<TYPE *>(memBlkPtr0);
  }

  force_inline void **getPtrList() {
    return reinterpret_cast<void **>(memBlkPtrList);
  }

  // todo Refactor this function to c++ style
  void fetch(uint64_t size, uint64_t mask, uint64_t *idx, TYPE *ret) {
    if (size_ <= BlkSize) {
      // performance: reduce memory access for memBlkPtr0
      TYPE *tmpPtr = reinterpret_cast<TYPE *>(memBlkPtr0);
      for (uint64_t i = 0; i < size; i++) ret[i] = tmpPtr[idx[i] & mask];
    } else {
      for (uint64_t i = 0; i < size; i++) ret[i] = *ptrAt(idx[i] & mask);
    }
  }

  force_inline void push_back(const TYPE &data) {
    reserve(size_ + 1);
    size_++;
    *ptrAt(size_ - 1) = data;
  }

  // Set the front elements in range of [0, size) to zero.
  void memZero(uint64_t size) {
    assert(size <= size_);
    uint64_t idx = 0;
    while (size >= BlkSize) {
      memset(memBlkPtrListVec[idx], 0, MEMBLKSIZE);
      idx++;
      size -= BlkSize;
    }
    if (size > 0) memset(memBlkPtrListVec[idx], 0, size * sizeof(TYPE));
  }

  // Initialize [begin, end) to zero.
  void memZero(uint64_t begin, uint64_t end) {
    assert(begin <= end && end <= size_);
    uint64_t beginBlkIdx = begin / BlkSize;
    uint64_t endBlkIdx = end / BlkSize;

    // Specialized initialization for single block
    if (beginBlkIdx == endBlkIdx) {
      memset(memBlkPtrListVec[beginBlkIdx] + (begin % BlkSize) * sizeof(TYPE),
             0, (end - begin) * sizeof(TYPE));
      return;
    }

    // Initialize leftmost leftover
    memset(memBlkPtrListVec[beginBlkIdx] + (begin % BlkSize) * sizeof(TYPE), 0,
           MEMBLKSIZE - (begin % BlkSize) * sizeof(TYPE));

    // Initialize intermediate blocks
    beginBlkIdx++;
    while (beginBlkIdx < endBlkIdx) {
      memset(memBlkPtrListVec[beginBlkIdx], 0, MEMBLKSIZE);
      beginBlkIdx++;
    }

    // Initialize rightmost leftover
    if (end % BlkSize > 0)
      memset(memBlkPtrListVec[endBlkIdx], 0, (end % BlkSize) * sizeof(TYPE));
  }

  force_inline TYPE *ptrAt(uint64_t index) {
    assert(index < size_);
    return reinterpret_cast<TYPE *>(memBlkPtrList[index >> ShiftLen]) +
           (index & Mask);
  }

  force_inline TYPE *ptrAtQuick(uint64_t index) {
    assert(index < BlkSize);
    return reinterpret_cast<TYPE *>(memBlkPtr0) + index;
  }

  bool enableQuickAccess() { return size_ <= BlkSize; }

  static const uint64_t BlkSize = MEMBLKSIZE / sizeof(TYPE);

  double getMemUsed() { return MEMBLKSIZE * memBlkList.size(); }

 private:
  // the maximum number of the elements that contained in a memory block
  static const uint64_t Mask = BlkSize - 1;
  static const uint64_t ShiftLen = 64 - __builtin_clzll(Mask);

  std::vector<MemBlkOwner> memBlkList;
  std::vector<char *> memBlkPtrListVec;
  char **memBlkPtrList = nullptr;
  char *memBlkPtr0 = nullptr;  // performance: for small scale
  uint64_t size_ = 0;
};

}  // namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_FLAT_MEMORY_BUFFER_H_
