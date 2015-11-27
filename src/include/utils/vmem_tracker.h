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
/*-------------------------------------------------------------------------
 *
 * vmem_tracker.h
 *	  This file contains declarations for vmem tracking functions.
 *
 */
#ifndef VMEMTRACKER_H
#define VMEMTRACKER_H

#include "nodes/nodes.h"

/*
 * The cleanupCountdown in the SessionState determines how many
 * processes we need to cleanup to declare a session clean. If it
 * reaches 0, we mark the session clean. However, -1 indicates
 * that the session is either done cleaning previous runaway event
 * or it never started a cleaning.
 */
#define CLEANUP_COUNTDOWN_BEFORE_RUNAWAY -1

typedef enum MemoryAllocationStatus
{
	MemoryAllocation_Success,
	MemoryFailure_VmemExhausted,
	MemoryFailure_SystemMemoryExhausted,
	MemoryFailure_QueryMemoryExhausted
} MemoryAllocationStatus;

typedef int64 EventVersion;

extern int runaway_detector_activation_percent;

extern int32 VmemTracker_ConvertVmemChunksToMB(int chunks);
extern int32 VmemTracker_ConvertVmemMBToChunks(int mb);
extern int64 VmemTracker_ConvertVmemChunksToBytes(int chunks);
extern int32 VmemTracker_GetReservedVmemChunks(void);
extern int64 VmemTracker_GetReservedVmemBytes(void);
extern int64 VmemTracker_GetMaxReservedVmemChunks(void);
extern int64 VmemTracker_GetMaxReservedVmemMB(void);
extern int64 VmemTracker_GetMaxReservedVmemBytes(void);
extern int64 VmemTracker_GetVmemLimitBytes(void);
extern int32 VmemTracker_GetVmemLimitChunks(void);
extern int32 VmemTracker_GetAvailableVmemMB(void);
extern int64 VmemTracker_GetAvailableVmemBytes(void);
extern int32 VmemTracker_GetAvailableQueryVmemMB(void);
extern void VmemTracker_ShmemInit(void);
extern void VmemTracker_Init(void);
extern void VmemTracker_Shutdown(void);
extern void VmemTracker_ResetMaxVmemReserved(void);
extern MemoryAllocationStatus VmemTracker_ReserveVmem(int64 newly_requested);
extern void VmemTracker_ReleaseVmem(int64 to_be_freed_requested);
extern void VmemTracker_RequestWaiver(int64 waiver_bytes);
extern int64 VmemTracker_Fault(int32 reason, int64 arg);

int VmemTracker_GetPhysicalMemQuotaInMB(void);
int VmemTracker_GetPhysicalMemQuotaInChunks(void);
int VmemTracker_GetDynamicMemoryQuotaSema(void);
int VmemTracker_SetDynamicMemoryQuotaSema(int value);

extern int32 RedZoneHandler_GetRedZoneLimitChunks(void);
extern int32 RedZoneHandler_GetRedZoneLimitMB(void);
extern bool RedZoneHandler_IsVmemRedZone(void);
extern void RedZoneHandler_DetectRunawaySession(void);
extern void RunawayCleaner_RunawayCleanupDoneForSession(void);
extern void RunawayCleaner_RunawayCleanupDoneForProcess(bool ignoredCleanup);
extern void RedZoneHandler_LogVmemUsageOfAllSessions(void);

extern void IdleTracker_ActivateProcess(void);
extern void IdleTracker_DeactivateProcess(void);

#endif   /* VMEMTRACKER_H */
