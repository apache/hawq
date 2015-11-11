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
 * runaway_cleaner.c
 *	 Implementation of the runaway cleaner that checks if a session is marked
 *	 as runaway (i.e., consuming too much vmem) by the red-zone handler
 *	 (redzone_handler.c). The runaway cleaner cleans up such session by triggering
 *	 an elog(ERROR, ...) which rolls back transaction and releases memory. Once
 *	 cleanup is finished, the runaway cleaner also informs the red zone handler
 *	 so that a new runaway session can be chosen if necessary.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/atomic.h"
#include "cdb/cdbvars.h"
#include "utils/vmem_tracker.h"
#include "utils/session_state.h"
#include "utils/faultinjector.h"
#include "miscadmin.h"

/* External dependencies within the runaway cleanup framework */
extern bool vmemTrackerInited;
extern bool isProcessActive;
extern EventVersion activationVersion;
extern EventVersion deactivationVersion;
extern volatile uint32 *isRunawayDetector;
extern volatile EventVersion *latestRunawayVersion;

/*
 * The cleanupCountdown in the SessionState determines how many
 * processes we need to cleanup to declare a session clean. If it
 * reaches 0, we mark the session clean. However, -1 indicates
 * that the session is either done cleaning previous runaway event
 * or it never started a cleaning.
 */
#define CLEANUP_COUNTDOWN_BEFORE_RUNAWAY -1

/* The runaway version for which this process started cleaning up */
static EventVersion beginCleanupRunawayVersion = 0;

/* The runaway version for which this process finished cleaning up */
static EventVersion endCleanupRunawayVersion = 0;

void RunawayCleaner_Init(void);
void RunawayCleaner_StartCleanup(void);
bool RunawayCleaner_IsCleanupInProgress(void);

/*
 * Initializes the per-process states of the runaway cleaner.
 */
void
RunawayCleaner_Init()
{
	beginCleanupRunawayVersion = 0;
	endCleanupRunawayVersion = 0;
}

/* Returns true if the current process should start a runaway cleanup */
static bool
RunawayCleaner_ShouldStartRunawayCleanup()
{
	if (NULL != MySessionState && MySessionState->runawayStatus != RunawayStatus_NotRunaway &&
			beginCleanupRunawayVersion != *latestRunawayVersion)
	{
		AssertImply(isProcessActive, activationVersion >= deactivationVersion);
		AssertImply(!isProcessActive, deactivationVersion >= activationVersion);

		/*
		 * We are marked as runaway. Therefore, if the runaway event happened before deactivation,
		 * we must have a version counter increment
		 */
		AssertImply(*latestRunawayVersion < deactivationVersion && !isProcessActive, activationVersion < deactivationVersion);

		if (isProcessActive && *latestRunawayVersion > activationVersion)
		{
			/* Active process and the runaway event came after the activation */
			return true;
		}
		else if (!isProcessActive && *latestRunawayVersion < deactivationVersion &&
				*latestRunawayVersion > activationVersion)
		{
			/*
			 * The process is deactivated, but there is a pending runaway event before
			 * the deactivation for which this process never cleaned up
			 */
			return true;
		}
	}

	return false;
}
/*
 * Starts a runaway cleanup by triggering an ERROR if the VMEM tracker is active
 * and a commit is not already in progress. Otherwise, it marks the process as clean
 */
void
RunawayCleaner_StartCleanup()
{
	/*
	 * Cleanup can be attempted from multiple places, such as before deactivating
	 * a process (if a pending runaway event) or periodically from CHECK_FOR_INTERRUPTS
	 * (indirectly via RedZoneHandler_DetectRunaway). We don't carry multiple cleanup
	 * for a single runaway event. Every time we *start* a cleanup process, we set the
	 * beginCleanupRunawayVersion to the runaway version for which we started cleaning
	 * up. Later on, if we reenter this method (e.g., another CHECK_FOR_INTERRUPTS()
	 * during cleanup), we can observe that the cleanup already started from this runaway
	 * event, and therefore we skip duplicate cleanup
	 */
	if (RunawayCleaner_ShouldStartRunawayCleanup())
	{
		Assert(beginCleanupRunawayVersion < *latestRunawayVersion);
		Assert(endCleanupRunawayVersion < *latestRunawayVersion);
		/* We don't want to cleanup multiple times for same runaway event */
		beginCleanupRunawayVersion = *latestRunawayVersion;

		if (CritSectionCount == 0 && InterruptHoldoffCount == 0 && vmemTrackerInited &&
			gp_command_count > 0 /* Cleaning up QEs that are not executing a valid command
			may cause the QD to get stuck [MPP-24950] */ &&
			/* Super user is terminated only when it's the primary runaway consumer (i.e., the top consumer) */
			(!superuser() || MySessionState->runawayStatus == RunawayStatus_PrimaryRunawaySession))
		{
#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
			RunawayCleanup,
			DDLNotSpecified,
			"",  // databaseName
			""); // tableName
#endif

			ereport(ERROR, (errmsg("Canceling query because of high VMEM usage. Used: %dMB, available %dMB, red zone: %dMB",
					VmemTracker_ConvertVmemChunksToMB(MySessionState->sessionVmem), VmemTracker_GetAvailableVmemMB(),
					RedZoneHandler_GetRedZoneLimitMB()), errprintstack(true)));
		}

		/*
		 * If we cannot error out because of a critical section or because we are a super user
		 * or for some other reason (such as the QE is not running any valid command, i.e.,
		 * gp_command_count is not positive) simply declare this process as clean
		 */
		RunawayCleaner_RunawayCleanupDoneForProcess(true /* ignoredCleanup */);
	}
}

/*
 * Resets the runaway flag and enables runaway detector.
 *
 * Note: this method should not need any additional locks.
 * Either the MySessionState entry is being released, and
 * we already have a lock on SessionState, and therefore,
 * no new runaway detector can run until the lock is released.
 *
 * Alternatively, we may reset this while still in a live
 * session. In such case, our runaway event versioning should
 * ensure that every process of this session would do another round
 * of cleanup if it is detected as a runaway session again.
 */
void
RunawayCleaner_RunawayCleanupDoneForSession()
{
	Assert(NULL != MySessionState);
	if (MySessionState->runawayStatus != RunawayStatus_NotRunaway)
	{
		/* The last runaway cleanup should have finished */
		Assert(endCleanupRunawayVersion == beginCleanupRunawayVersion);
		Assert(endCleanupRunawayVersion == *latestRunawayVersion);
		Assert(CLEANUP_COUNTDOWN_BEFORE_RUNAWAY == MySessionState->cleanupCountdown);

		MySessionState->runawayStatus = RunawayStatus_NotRunaway;
		MySessionState->sessionVmemRunaway = 0;
		MySessionState->commandCountRunaway = 0;

		/*
		 * Reset the exclusive runaway detector flag so that
		 * another runaway detector can be chosen
		 */
		*isRunawayDetector = 0;
	}
}

/*
 * Marks the current process as clean. If all the processes are marked
 * as clean for this session (i.e., cleanupCountdown == 0 in the
 * MySessionState) then we reset session's runaway status as well as
 * the runaway detector flag (i.e., a new runaway detector can run).
 *
 * Parameters:
 * 		ignoredCleanup: whether the cleanup was ignored, i.e., no elog(ERROR, ...)
 * 		was thrown. In such case a deactivated process is not reactivated as the
 * 		deactivation didn't get interrupted.
 */
void
RunawayCleaner_RunawayCleanupDoneForProcess(bool ignoredCleanup)
{
	/*
	 * We don't do anything if we don't have an ongoing cleanup, or we already finished
	 * cleanup once for the current runaway event
	 */
	if (beginCleanupRunawayVersion != *latestRunawayVersion ||
			endCleanupRunawayVersion == beginCleanupRunawayVersion)
	{
		/* Either we never started cleanup, or we already finished */
		return;
	}

	/* Disable repeating call */
	endCleanupRunawayVersion = beginCleanupRunawayVersion;

	Assert(NULL != MySessionState);
	/*
	 * As the current cleanup holds leverage on the  cleanupCountdown,
	 * the session must stay as runaway at least until the current
	 * process marks itself clean
	 */
	Assert(MySessionState->runawayStatus != RunawayStatus_NotRunaway);

	/* We only cleanup if we were active when the runaway event happened */
	Assert((!isProcessActive && *latestRunawayVersion < deactivationVersion &&
			*latestRunawayVersion > activationVersion) ||
			(*latestRunawayVersion > activationVersion &&
			(activationVersion >= deactivationVersion && isProcessActive)));

	/*
	 * We don't reactivate if the process is already active or a deactivated
	 * process never errored out during deactivation (i.e., failed to complete
	 * deactivation)
	 */
	if (!isProcessActive && !ignoredCleanup)
	{
		Assert(1 == *isRunawayDetector);
		Assert(0 < MySessionState->cleanupCountdown);
		/*
		 * As the process threw ERROR instead of going into ReadCommand() blocking
		 * state, we have to reactivate the process from its current Deactivated
		 * state
		 */
		IdleTracker_ActivateProcess();
	}

	Assert(0 < MySessionState->cleanupCountdown);
#if USE_ASSERT_CHECKING
	int cleanProgress =
#endif
			gp_atomic_add_32(&MySessionState->cleanupCountdown, -1);
	Assert(0 <= cleanProgress);

	bool finalCleaner = compare_and_swap_32((uint32*) &MySessionState->cleanupCountdown,
			0, CLEANUP_COUNTDOWN_BEFORE_RUNAWAY);

	if (finalCleaner)
	{
		/*
		 * The final cleaner is responsible to reset the runaway flag,
		 * and enable the runaway detection process.
		 */
		RunawayCleaner_RunawayCleanupDoneForSession();
	}
}

/*
 * Returns true if a cleanup is in progress (i.e., endCleanupRunawayVersion
 * is smaller than beginCleanupRunawayVersion).
 */
bool
RunawayCleaner_IsCleanupInProgress()
{
	Assert(endCleanupRunawayVersion <= beginCleanupRunawayVersion);
	return endCleanupRunawayVersion < beginCleanupRunawayVersion;
}
