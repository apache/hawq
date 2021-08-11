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
