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

#ifndef EXECUTOR_SRC_EXECUTOR_CWRAPPER_EXECUTOR_C_H_
#define EXECUTOR_SRC_EXECUTOR_CWRAPPER_EXECUTOR_C_H_

#include <stdint.h>

#include "storage/cwrapper/orc-format-c.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ERROR_MESSAGE_BUFFER_SIZE
#define ERROR_MESSAGE_BUFFER_SIZE 4096
#endif

__attribute__((weak)) struct ExecutorC;

__attribute__((weak)) typedef struct ExecutorC ExecutorC;

__attribute__((weak)) typedef struct ExecutorCatchedError {
  int errCode;
  char errMessage[ERROR_MESSAGE_BUFFER_SIZE];
} ExecutorCatchedError;

__attribute__((weak)) struct MyInstrumentation;
__attribute__((weak)) typedef struct MyInstrumentation MyInstrumentation;

__attribute__((weak)) typedef struct StorageFormatCallback {
  uint64_t tupcount;
  uint64_t eof;
  uint64_t uncompressed_eof;
  uint64_t processedTupleCount;
} StorageFormatCallback;

__attribute__((weak)) typedef struct StorageFormatC StorageFormatC;

__attribute__((weak)) StorageFormatCallback StorageFormatDumpCallbackC(
    StorageFormatC *fmt);

__attribute__((weak)) void ORCFormatFreeStorageFormatC(StorageFormatC **fmt) {}

#ifdef __cplusplus
}
#endif

#endif  // EXECUTOR_SRC_EXECUTOR_CWRAPPER_EXECUTOR_C_H_
