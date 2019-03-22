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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_CUTILS_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_CUTILS_H_

#include <execinfo.h>

#include <cstdlib>
#include <memory>

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

char* cnmalloc(size_t size);

char* cnrealloc(void* ptr, size_t size);

inline void cnfree(void* ptr) { ::free(ptr); }

typedef std::unique_ptr<char, decltype(cnfree)*> MemBlkOwner;

// Pad the source address to 8 byte aligned.
static force_inline const char* alignedAddress(const char* src) {
  src += (8 - ((uint64_t)src & 0x7)) & 0x7;
  return src;
}
static force_inline char* alignedAddress(char* src) {
  src += (8 - ((uint64_t)src & 0x7)) & 0x7;
  return src;
}

template <uint64_t size>
static force_inline const char* alignedAddressInto(const char* src) {
  src += (size - ((uint64_t)src & (size - 1))) & (size - 1);
  return src;
}
template <uint64_t size>
static force_inline char* alignedAddressInto(char* src) {
  src += (size - ((uint64_t)src & (size - 1))) & (size - 1);
  return src;
}

}  // namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_CUTILS_H_
