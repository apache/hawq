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
#include "dbcommon/utils/int-util.h"
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
  }

  void reserve(uint64_t size) {
    if (sizeof(TYPE) * size <= getMemUsed()) return;

    auto filledBlockCount = sizeof(TYPE) * size / MEMBLKSIZE;
    auto remainedMemSize = nextPowerOfTwo(sizeof(TYPE) * size % MEMBLKSIZE);
    if (!memBlkList.empty() && filledBlockCount) {
      if (filledBlockCount >= memBlkList.size() && lastBlkSize_ != MEMBLKSIZE) {
        // enlarge previous block
        auto blk = std::move(memBlkList.back());
        memBlkPtrListVec.pop_back();
        memBlkList.pop_back();
        auto newBlk = allocate(blk.release(), MEMBLKSIZE);
        memBlkPtrListVec.push_back(newBlk.first);
        memBlkPtrList = &memBlkPtrListVec[0];
        memBlkList.push_back(std::move(newBlk.second));
        lastBlkSize_ = MEMBLKSIZE;
      }
    }
    while (memBlkList.size() < filledBlockCount) {
      auto newBlk = allocate(nullptr, MEMBLKSIZE);
      memBlkPtrListVec.push_back(newBlk.first);
      memBlkPtrList = &memBlkPtrListVec[0];
      memBlkList.push_back(std::move(newBlk.second));
      lastBlkSize_ = MEMBLKSIZE;
    }
    assert(memBlkPtrListVec.size() == memBlkList.size());
    assert(memBlkList.size() >= filledBlockCount);
    if (memBlkList.size() == (filledBlockCount + (remainedMemSize ? 1 : 0))) {
      if (remainedMemSize && remainedMemSize < MEMBLKSIZE) {
        // enlarge previous block
        auto blk = std::move(memBlkList.back());
        memBlkPtrListVec.pop_back();
        memBlkList.pop_back();
        auto newBlk = allocate(blk.release(), remainedMemSize);
        memBlkPtrListVec.push_back(newBlk.first);
        memBlkPtrList = &memBlkPtrListVec[0];
        memBlkList.push_back(std::move(newBlk.second));
        lastBlkSize_ = remainedMemSize;
      }
    } else {
      // allocate new block
      assert(remainedMemSize);
      auto newBlk = allocate(nullptr, remainedMemSize);
      memBlkPtrListVec.push_back(newBlk.first);
      memBlkPtrList = &memBlkPtrListVec[0];
      memBlkList.push_back(std::move(newBlk.second));
      lastBlkSize_ = remainedMemSize;
    }

    if (!memBlkPtrListVec.empty()) {
      memBlkPtr0 = memBlkPtrListVec[0];
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

  double getMemUsed() {
    assert(getMemAllocated() >= sizeof(TYPE) * size());
    return sizeof(TYPE) * size();
  }

  double getMemAllocated() {
    double ret = 0;
    ret += memBlkList.size() > 1 ? MEMBLKSIZE * (memBlkList.size() - 1) : 0;
    ret += lastBlkSize_;
    return ret;
  }

  void reset() {
    lastBlkSize_ = 0;
    memBlkList.clear();
    memBlkPtrListVec.clear();
    memBlkPtrList = nullptr;
    memBlkPtr0 = nullptr;
    size_ = 0;
  }

 private:
  std::pair<char *, MemBlkOwner> allocate(char *ptr, size_t size) {
    // Exactly, each MemBlkOwner contains extra spare memory beyond the
    // specified MEMBLKSIZE, in order to align memory address into the
    // multiple of the cache line size, which helps to reduce cache miss when
    // size_ is small.
    // For example, the number of the groups in TPCH-Q1 is 4 and
    // sizeof(AggGroupValue) is 16. There is only 64 byte memory keeping being
    // accessed, which fits into one cache line perfectly.
    // TODO(chiyang): add back the alignment of cache line size
    MemBlkOwner tmpOwner(MemBlkOwner({(cnrealloc(ptr, size)), cnfree}));
    char *tmpPtr = tmpOwner.get();
    return std::make_pair(tmpPtr, std::move(tmpOwner));
  }

  // the maximum number of the elements that contained in a memory block
  static const uint64_t Mask = BlkSize - 1;
  static const uint64_t ShiftLen = 64 - __builtin_clzll(Mask);

  uint64_t lastBlkSize_ = 0;
  std::vector<MemBlkOwner> memBlkList;
  std::vector<char *> memBlkPtrListVec;
  char **memBlkPtrList = nullptr;
  char *memBlkPtr0 = nullptr;  // performance: for small scale
  uint64_t size_ = 0;          // number of stored unit type
};

}  // namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_FLAT_MEMORY_BUFFER_H_
