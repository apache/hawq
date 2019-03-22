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

#include "dbcommon/utils/cutils.h"

namespace dbcommon {

static void outputCallStack() {
  int nptrs = 0;
  void* buffer[64];
  char** strings;

  nptrs = backtrace(buffer, 64);
  LOG_INFO("backtrace() returned %d addresses\n", nptrs);

  strings = backtrace_symbols(buffer, nptrs);
  if (strings == nullptr) {
    return;
  }

  for (int j = 0; j < nptrs; j++) {
    LOG_INFO("callstack(%d) %s\n", j, strings[j]);
  }

  free(strings);
}

char* cnmalloc(size_t size) {
  if (size > 1024LU * 1024 * 1024) {
    outputCallStack();
  }

  char* ret = static_cast<char*>(::malloc(size));
  if (ret == nullptr) {
    LOG_ERROR(ERRCODE_OUT_OF_MEMORY,
              "cnmalloc() failed to allocate memory, size %zu", size);
  }
  return ret;
}

char* cnrealloc(void* ptr, size_t size) {
  if (size > 1024LU * 1024 * 1024) {
    outputCallStack();
  }

  char* ret = static_cast<char*>(::realloc(ptr, size));

  if (ret == nullptr && size > 0) {
    LOG_ERROR(ERRCODE_OUT_OF_MEMORY,
              "cnrealloc() failed to allocate memory, size %zu", size);
  }
  return ret;
}

}  // namespace dbcommon
