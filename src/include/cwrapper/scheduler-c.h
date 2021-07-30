///////////////////////////////////////////////////////////////////////////////
// Copyright 2016, Oushu Inc.
// All rights reserved.
//
// Author:
///////////////////////////////////////////////////////////////////////////////
#ifndef SCHEDULER_SRC_SCHEDULER_CWRAPPER_SCHEDULER_C_H_
#define SCHEDULER_SRC_SCHEDULER_CWRAPPER_SCHEDULER_C_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ERROR_MESSAGE_BUFFER_SIZE
#define ERROR_MESSAGE_BUFFER_SIZE 4096
#endif

__attribute__((weak)) struct SchedulerC;
__attribute__((weak)) struct UnivPlanC;

__attribute__((weak)) typedef struct SchedulerC SchedulerC;

__attribute__((weak)) typedef struct SchedulerCatchedError {
  int errCode;
  char errMessage[ERROR_MESSAGE_BUFFER_SIZE];
} SchedulerCatchedError;

__attribute__((weak)) typedef struct SchedulerStats {
  // in millisecond
  int rpcServerSetupTime;
  int setupListenerPortTime;
  int dispatchTaskTime;
  int waitTaskCompleteTime;
} SchedulerStats;

#ifdef __cplusplus
}
#endif

#endif  // SCHEDULER_SRC_SCHEDULER_CWRAPPER_SCHEDULER_C_H_
