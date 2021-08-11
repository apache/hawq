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

#ifndef EXECUTOR_SRC_EXECUTOR_CWRAPPER_CACHED_RESULT_H_
#define EXECUTOR_SRC_EXECUTOR_CWRAPPER_CACHED_RESULT_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

__attribute__((weak)) typedef struct CachedResultC CachedResultC;
__attribute__((weak)) typedef struct ExecutorC ExecutorC;
__attribute__((weak)) typedef struct ExecutorCatchedError ExecutorCatchedError;

__attribute__((weak)) void CacheFileCleanUp() {}

__attribute__((weak)) void CachedResultReset(CachedResultC *cr, bool enable) {}

__attribute__((weak)) bool CachedResultIsRead(CachedResultC *cr) {}

__attribute__((weak)) bool CachedResultIsWrite(CachedResultC *cr) {}

__attribute__((weak)) ExecutorC *ExecutorNewInstance(const char *listenHost,
                                                     const char *icType,
                                                     uint32_t *listenPort) {}

__attribute__((weak)) ExecutorCatchedError *ExecutorGetLastError(
    ExecutorC *exec) {}

__attribute__((weak)) void MagmaFormatC_CancelMagmaClient() {}
__attribute__((weak)) void MagmaClientC_CancelMagmaClient() {}

#ifdef __cplusplus
}
#endif

#endif  // EXECUTOR_SRC_EXECUTOR_CWRAPPER_CACHED_RESULT_H_
