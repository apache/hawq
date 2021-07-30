///////////////////////////////////////////////////////////////////////////////
// Copyright 2019, Oushu Inc.
// All rights reserved.
//
// Author:
///////////////////////////////////////////////////////////////////////////////

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
