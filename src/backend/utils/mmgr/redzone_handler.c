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
 * redzone_handler.c
 *	 Implementation of the red-zone handler that detects when the system
 *	 is running low in vmem (i.e., the system is in red-zone). The red-zone
 *	 handler identifies the session that consumes most vmem and asks it
 *	 to gracefully release its memory.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/atomic.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "utils/vmem_tracker.h"
#include "utils/session_state.h"

/* External dependencies within the runaway cleanup framework */
extern bool vmemTrackerInited;
extern volatile int32 *segmentVmemChunks;
extern volatile int32 *segmentVmemQuotaChunks;
extern volatile EventVersion *CurrentVersion;
extern volatile EventVersion *latestRunawayVersion;
extern void RunawayCleaner_StartCleanup(void);
extern int32 VmemTracker_ConvertVmemMBToChunks(int mb);

#define SHMEM_RUNAWAY_DETECTOR_MUTEX "SHMEM_RUNAWAY_DETECTOR_MUTEX"
#define INVALID_SESSION_ID -1

/* The runaway detector activates if the used vmem exceeds this percentage of the vmem quota */
int	runaway_detector_activation_percent = 95;

/* The last number of chunks for segment vmem quota */
static int lastSegmentVmemQuotaChunks = -1;

/* The number of memory chunks for red zone */
static int redZoneChunks = -1;

/*
 * A shared memory binary flag (0 or 1) that identifies one process at-a-time as runaway
 * detector. At red-zone each process tries to determine runaway query, but only the first
 * process that succeeds to set this counter to 1 becomes the detector.
 */
volatile uint32 *isRunawayDetector = NULL;

void RedZoneHandler_ShmemInit(void);
void RedZoneHandler_ReactivateRunawayDetector(void);

/*
 * Returns the red-zone cut-off in "chunks" unit
 */
int32
RedZoneHandler_GetRedZoneLimitChunks()
{
	/*
	 * runaway_detector_activation_percent = 100% is reserved for not enforcing runaway
	 * detection by setting the redZoneChunks to an artificially high value. Also, during
	 * gpinitsystem we may start a QD without initializing the hawq_re_memory_overcommit_max.
	 * This may result in 0 vmem protect limit. In such case, we ensure that the
	 * redZoneChunks is set to a large value.
	 */
	if (runaway_detector_activation_percent != 100)
	{
		/*
		 * Calculate red zone threshold in MB, and then convert MB to "chunks"
		 * using chunk size for efficient comparison to detect red zone
		 */
		if (lastSegmentVmemQuotaChunks != *segmentVmemQuotaChunks)
		{
			lastSegmentVmemQuotaChunks = *segmentVmemQuotaChunks;
			redZoneChunks = (int)(lastSegmentVmemQuotaChunks *
                                  ((float) runaway_detector_activation_percent) /
                                  ((float) 100));
		}
	}

	/* 0 means disable red-zone completely */
	if (redZoneChunks == 0)
	{
		redZoneChunks = INT32_MAX;
	}

	return redZoneChunks;
}

/*
 * Returns the red-zone cut-off in "MB" unit
 */
int32
RedZoneHandler_GetRedZoneLimitMB()
{
	return VmemTracker_ConvertVmemChunksToMB(redZoneChunks);
}

/*
 * Initializes the red zone handler's shared memory states.
 */
void
RedZoneHandler_ShmemInit()
{
	Assert(!vmemTrackerInited);

	bool		alreadyInShmem = false;

	isRunawayDetector = (uint32 *)
								ShmemInitStruct(SHMEM_RUNAWAY_DETECTOR_MUTEX,
										sizeof(int32),
										&alreadyInShmem);
	Assert(alreadyInShmem || !IsUnderPostmaster);

	Assert(NULL != isRunawayDetector);

	if(!IsUnderPostmaster)
	{
		*isRunawayDetector = 0;
	}
}

/*
 * Returns true if the system is in red-zone (too little VMEM)
 */
bool
RedZoneHandler_IsVmemRedZone()
{
	if (!vmemTrackerInited)
	{
		return false;
	}

	int32 redZoneChunks = RedZoneHandler_GetRedZoneLimitChunks();
	Assert(redZoneChunks > 0);
	return *segmentVmemChunks > redZoneChunks;
}

/*
 * Finds and notifies the top vmem consuming session.
 */
static void
RedZoneHandler_FlagTopConsumer()
{
	if (!vmemTrackerInited)
	{
		return;
	}

	Assert(NULL != MySessionState);

	bool success = compare_and_swap_32((uint32*) isRunawayDetector, 0, 1);

	/* If successful then this process must be the runaway detector */
	AssertImply(success, 1 == *isRunawayDetector);

	/*
	 * Someone already determined the runaway query, so nothing to do. This
	 * will also prevent re-entry to this method by a cleaning session.
	 */
	if (!success)
	{
		return;
	}

	/*
	 * Grabbing a shared lock prevents others to modify the SessionState
	 * data structure, therefore ensuring that we don't flag someone
	 * who was already dying. A shared lock is enough as we access the
	 * data structure in a read-only manner.
	 */
	LWLockAcquire(SessionStateLock, LW_SHARED);

	int32 maxVmem = 0;
	int32 maxActiveVmem = 0;
	SessionState *maxActiveVmemSessionState = NULL;
	SessionState *maxVmemSessionState = NULL;

	SessionState *curSessionState = AllSessionStateEntries->usedList;

	while (curSessionState != NULL)
	{
		Assert(INVALID_SESSION_ID != curSessionState->sessionId);

		int32 curVmem = curSessionState->sessionVmem;

		Assert(maxActiveVmem <= maxVmem);

		if (curVmem > maxActiveVmem)
		{
			if (curVmem > maxVmem)
			{
				maxVmemSessionState = curSessionState;
				maxVmem = curVmem;
			}

			/*
			 * Only consider sessions with at least 1 active process. As we
			 * are *not* grabbings locks, this does not guarantee that by the
			 * time we finish walking all sessions the chosen session will
			 * still have active process.
			 */
			if  (curSessionState->activeProcessCount > 0)
			{
				maxActiveVmemSessionState = curSessionState;
				maxActiveVmem = curVmem;
			}
		}

		curSessionState = curSessionState->next;
	}

	if (NULL != maxActiveVmemSessionState)
	{
		SpinLockAcquire(&maxActiveVmemSessionState->spinLock);

		/*
		 * Now that we grabbed lock, make sure we have at least 1 active process
		 * before flagging this session for termination
		 */
		if (0 < maxActiveVmemSessionState->activeProcessCount)
		{
			/*
			 * First update the runaway event detection version so that
			 * an active process of the runaway session is forced to clean up before
			 * it deactivates. As we grabbed the spin lock, no process of the runaway
			 * session can deactivate unless we release the lock. The other sessions
			 * don't care what global runaway version they observe as the runaway
			 * event is not pertinent to them.
			 *
			 * We don't need any lock here as the runaway detector is singleton,
			 * and only the detector can update this variable.
			 */
			*latestRunawayVersion = *CurrentVersion + 1;
			/*
			 * Make sure that the runaway event version is not shared with any other
			 * processes, and not shared with any other deactivation/reactivation version
			 */
			*CurrentVersion = *CurrentVersion + 2;

			Assert(CLEANUP_COUNTDOWN_BEFORE_RUNAWAY == maxActiveVmemSessionState->cleanupCountdown);
			/*
			 * Determine how many processes need to cleanup to mark the session clean.
			 */
			maxActiveVmemSessionState->cleanupCountdown = maxActiveVmemSessionState->activeProcessCount;

			if (maxActiveVmemSessionState == maxVmemSessionState)
			{
				/* Finally signal the runaway process for cleanup */
				maxActiveVmemSessionState->runawayStatus = RunawayStatus_PrimaryRunawaySession;
			}
			else
			{
				maxActiveVmemSessionState->runawayStatus = RunawayStatus_SecondaryRunawaySession;
			}

			/* Save the amount of vmem session was holding when it was flagged as runaway */
			maxActiveVmemSessionState->sessionVmemRunaway = maxActiveVmemSessionState->sessionVmem;

			/* Save the command count currently running in the runaway session */
			maxActiveVmemSessionState->commandCountRunaway = gp_command_count;
		}
		else
		{
			/*
			 * Failed to find any viable runaway session. Reset runaway detector flag
			 * for another round of runaway determination at a later time. As we couldn't
			 * find any runaway session, the CurrentVersion is not changed.
			 */
			*isRunawayDetector = 0;
		}

		SpinLockRelease(&maxActiveVmemSessionState->spinLock);
	}
	else
	{
		/*
		 * No active session to mark as runaway. So, reenable the runaway detection process
		 */
		*isRunawayDetector = 0;
	}

	LWLockRelease(SessionStateLock);
}

/*
 * In a red-zone this method identifies the top vmem consuming session,
 * and requests it to cleanup. If the red-zone handler determines itself
 * as the runaway session, it also starts the cleanup.
 */
void
RedZoneHandler_DetectRunawaySession()
{
	/*
	 * InterruptHoldoffCount > 0 indicates we are in a sensitive code path that doesn't
	 * like a control flow disruption as may happen from a pending die/cancel interrupt.
	 * As we may eventually ERROR out from this method (during RunawayCleaner_StartCleanup)
	 * we want to make sure that HOLD_INTERRUPTS() was not called (i.e., InterruptHoldoffCount == 0).
	 *
	 * What happens if we don't check for InterruptHoldoffCount? One example is LWLockAcquire()
	 * which calls HOLD_INTERRUPTS() to ensure that no unexpected control
	 * flow disruption happens because of FATAL/ERROR as done from die/cancel interrupt
	 * handler. If we ignore InterruptHoldoffCount, the PGSemaphoreLock() (called from LWLockAcquire)
	 * would call CHECK_FOR_INTERRUPTS() and we may throw ERROR if the current session is a runaway.
	 * Unfortunately, LWLockAcquire shares the semaphore with the regular lock manager and
	 * ProcWaitForSignal. Therefore, LWLockAcquire may wake up multiple times during its wait
	 * for a semaphore which may not relate to an actual LWLock release. This requires LWLockAcquire
	 * to keep track of how many of those false wake events it has consumed (by decrementing semaphore
	 * when it shouldn't have done so) and LWLockAcquire rollback the semaphore decrements for
	 * the irrelevant wake up events by re-incrementing once it actually acquires the lock.
	 * Therefore, an unexpected control flow out of the LWLockAcquire before it properly rolled back
	 * may prevent the LWLockAcquire to rollback the false wake events. Although we do call LWLockRelease
	 * during an error handling, that doesn't guarantee that the falsely consumed semaphore wake
	 * events would be rolled back (i.e., semaphore does not get re-incremented during error handling) as
	 * done at the end of LWLockAcquire. This may cause the semaphore to never wake up other waiting
	 * processes and therefore may cause other processes to hang perpetually.
	 */
	if (!RedZoneHandler_IsVmemRedZone() || InterruptHoldoffCount > 0 ||
			CritSectionCount > 0)
	{
		return;
	}

	/* We don't support runaway detection/termination from non-owner thread */
	Assert(MemoryProtection_IsOwnerThread());
	Assert(gp_mp_inited);

	RedZoneHandler_FlagTopConsumer();
	RunawayCleaner_StartCleanup();
}

/*
 * Saves VMEM usage of all the sessions into log
 */
void
RedZoneHandler_LogVmemUsageOfAllSessions()
{
	if (!vmemTrackerInited)
	{
		return;
	}

	Assert(NULL != MySessionState);

	/*
	 * Grabbing a shared lock ensures that the data structure is not
	 * modified while we are reading. Shared lock is enough as we
	 * are only reading and not modifying the SessionState data structure
	 */
	LWLockAcquire(SessionStateLock, LW_SHARED);

	SessionState *curSessionState = AllSessionStateEntries->usedList;

	PG_TRY();
	{
		/* Write the header for the subsequent lines of memory usage information */
		write_stderr("session_state: session_id, is_runaway, qe_count, active_qe_count, dirty_qe_count, vmem_mb, runaway_vmem_mb, runaway_command_cnt\n");

		while (curSessionState != NULL)
		{
			Assert(INVALID_SESSION_ID != curSessionState->sessionId);

			write_stderr("session_state: %d, %d, %d, %d, %d, %d, %d, %d\n", curSessionState->sessionId,
					curSessionState->runawayStatus != RunawayStatus_NotRunaway, curSessionState->pinCount,
					curSessionState->activeProcessCount, curSessionState->cleanupCountdown, VmemTracker_ConvertVmemChunksToMB(curSessionState->sessionVmem),
					VmemTracker_ConvertVmemChunksToMB(curSessionState->sessionVmemRunaway), curSessionState->commandCountRunaway);

			curSessionState = curSessionState->next;
		}
	}
	PG_CATCH();
	{
		LWLockRelease(SessionStateLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	LWLockRelease(SessionStateLock);
}
