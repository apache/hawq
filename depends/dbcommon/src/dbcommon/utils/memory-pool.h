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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_MEMORY_POOL_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_MEMORY_POOL_H_

#include <execinfo.h>
#include <unistd.h>
#include <cassert>
#include <limits>
#include <mutex>  // NOLINT
#include <string>
#include <unordered_map>

#include "dbcommon/utils/cutils.h"

namespace dbcommon {

typedef struct MemAddr {
  void *memPtr;  // allocated raw memory ptr
  size_t size;
} MemAddr;

// aligned memory ptr
typedef std::unordered_map<void *, MemAddr> MemAddrMap;

class MemoryPool {
 public:
  MemoryPool() {
    freeMem.clear();
    activeMem.clear();
  }
  ~MemoryPool() { reset(); }

  void reset() {
    // for limit, activeMem is not empty when memory pool destruct
    for (auto iter = activeMem.begin(); iter != activeMem.end(); ++iter) {
      dbcommon::cnfree(iter->second.memPtr);
    }
    for (auto iter = freeMem.begin(); iter != freeMem.end(); ++iter) {
      dbcommon::cnfree(iter->second.memPtr);
    }
    freeMem.clear();
    activeMem.clear();
    space = 0;
  }

  size_t getSpace() { return space; }

  template <typename T>
  T *malloc(size_t size) {
    size_t newSize = kBlockSize;
    while (newSize < size) newSize *= 2;
    size_t align = alignof(T);
    if (newSize - size < align) newSize += align;

    void *p = nullptr;
    size_t pSize = kMaxSize;
    for (auto iter = freeMem.begin(); iter != freeMem.end(); ++iter) {
      if (iter->second.size >= newSize && iter->second.size < pSize) {
        p = iter->first;
        pSize = iter->second.size;
      }
    }

    if (p == nullptr) {
      p = dbcommon::cnmalloc(newSize);
    } else {
      freeMem.erase(p);
      newSize = pSize;
    }
    space = newSize;
    void *ret = p;
    std::align(align, size, ret, space);
    activeMem[ret] = MemAddr{p, space};
    return reinterpret_cast<T *>(ret);
  }

  template <typename T>
  T *realloc(void *p, size_t size) {
    auto iter = activeMem.find(p);
    if (iter->second.size >= size) {
      space = iter->second.size;
      return reinterpret_cast<T *>(p);
    }

    size_t newSize = kBlockSize;
    while (newSize < size) newSize *= 2;
    size_t align = alignof(T);
    if (newSize - size < align) newSize += align;

    void *ret = dbcommon::cnrealloc(p, newSize);
    space = newSize;
    if (ret == p) {
      activeMem[ret].size = space;
    } else {
      activeMem.erase(p);
      p = ret;
      ret = p;
      std::align(align, size, ret, space);
      activeMem[ret] = MemAddr{p, space};
    }

    return reinterpret_cast<T *>(ret);
  }

  void free(void *p) {
    auto iter = activeMem.find(p);
    freeMem.insert(*iter);
    activeMem.erase(iter);
  }

 public:
  std::string debugStr() {
    std::string str;
    str += "freeMemNum|activeMemNum(" + std::to_string(freeMem.size()) + "|" +
           std::to_string(activeMem.size()) + "):";
    for (auto iter = freeMem.begin(); iter != freeMem.end(); ++iter) {
      std::stringstream strm;
      strm << iter->second.memPtr;
      str += strm.str() + "-" + std::to_string(iter->second.size) + ";";
    }
    str += "****";
    for (auto iter = activeMem.begin(); iter != activeMem.end(); ++iter) {
      std::stringstream strm;
      strm << iter->second.memPtr;
      str += strm.str() + "-" + std::to_string(iter->second.size) + ";";
    }
    return str;
  }

  std::string callstackStr() {
    std::string str;
    int nptrs;
    void *buffer[1000];
    char **strings;
    nptrs = backtrace(buffer, 1000);
    strings = backtrace_symbols(buffer, nptrs);
    str += "\n";
    for (int i = 0; i < nptrs; i++) {
      str += strings[i];
      str += "\n";
    }
    return str;
  }

 private:
  const uint64_t kBlockSize = 1024;
  const uint64_t kMaxSize = std::numeric_limits<uint64_t>::max();
  MemAddrMap freeMem;
  MemAddrMap activeMem;
  size_t space = 0;
};

MemoryPool *getDefaultPool();

class ConcurrentMemoryPool : private MemoryPool {
 public:
  void reset() {
    std::lock_guard<std::mutex> lockGuard(lock_);
    MemoryPool::reset();
  }

  size_t getSpace() {
    std::lock_guard<std::mutex> lockGuard(lock_);
    return MemoryPool::getSpace();
  }

  template <typename T>
  T *malloc(size_t size) {
    std::lock_guard<std::mutex> lockGuard(lock_);
    return MemoryPool::malloc<T>(size);
  }

  template <typename T>
  T *realloc(void *p, size_t size) {
    std::lock_guard<std::mutex> lockGuard(lock_);
    return MemoryPool::realloc<T>(p, size);
  }

  void free(void *p) {
    std::lock_guard<std::mutex> lockGuard(lock_);
    return MemoryPool::free(p);
  }

 private:
  std::mutex lock_;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_MEMORY_POOL_H_
