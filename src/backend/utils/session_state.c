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

#include "postgres.h"

#include "utils/atomic.h"
#include "utils/memutils.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "utils/session_state.h"
#include "utils/vmem_tracker.h"

#define INVALID_SESSION_ID -1
#define SHMEM_SESSION_STATE_ARRAY "Session State Array"

/* Pointer to SessionStateArray instance in shared memory */
volatile SessionStateArray *AllSessionStateEntries = NULL;
/* Number of entries in the SessionStateArray */
static int SessionStateArrayEntryCount = 0;
/*
 * A pointer to the entry in the array of SessionState that corresponds
 * to the SessionState of this session.
 */
volatile SessionState *MySessionState = NULL;
/* Have we initialized the sessionState pointer */
bool sessionStateInited = false;

/* For checking if the process is deactivated before calling SessionState_Release */
extern bool isProcessActive;

/* Returns the size of the shared memory to allocate SessionStateArray */
/*
 * Grabs one entry in the sessionStateArray for current session.
 * If the current session already has an entry, it just returns the
 * pointer to the previously grabbed entry.
 */
static SessionState*
SessionState_Acquire(int sessionId)
{
	LWLockAcquire(SessionStateLock, LW_EXCLUSIVE);

	SessionState *cur = AllSessionStateEntries->usedList;

	while (cur != NULL && cur->sessionId != sessionId)
	{
		Assert(INVALID_SESSION_ID != cur->sessionId);
		cur = cur->next;
	}

	if (NULL == cur && NULL == AllSessionStateEntries->freeList)
	{
		LWLockRelease(SessionStateLock);
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
						errmsg("Too many sessions."),
						errdetail("Could not acquire resources for additional sessions."),
						errhint("Disconnect some sessions and try again.")));
	}

	SessionState *acquired = cur;

	/*
	 * Nothing was acquired for this session from any other processes. Therefore,
	 * acquire a new entry, and reset its properties.
	 */
	if (NULL == acquired)
	{
		acquired = AllSessionStateEntries->freeList;
		Assert(INVALID_SESSION_ID == acquired->sessionId &&
				acquired->runawayStatus == RunawayStatus_NotRunaway &&
				0 == acquired->pinCount &&
				CLEANUP_COUNTDOWN_BEFORE_RUNAWAY == acquired->cleanupCountdown &&
				0 == acquired->activeProcessCount &&
				0 == acquired->sessionVmem &&
				0 == acquired->spinLock &&
				0 == acquired->sessionVmemRunaway &&
				0 == acquired->commandCountRunaway &&
				!acquired->isModifiedSessionId);

		AllSessionStateEntries->freeList = acquired->next;

		acquired->next = AllSessionStateEntries->usedList;
		AllSessionStateEntries->usedList = acquired;
		AllSessionStateEntries->numSession++;
		Assert(AllSessionStateEntries->numSession <= AllSessionStateEntries->maxSession);

		acquired->sessionId = sessionId;
		acquired->runawayStatus = RunawayStatus_NotRunaway;
		acquired->sessionVmemRunaway = 0;
		acquired->commandCountRunaway = 0;
		acquired->pinCount = 0;
		acquired->sessionVmem = 0;
		acquired->cleanupCountdown = CLEANUP_COUNTDOWN_BEFORE_RUNAWAY;
		acquired->activeProcessCount = 0;

#ifdef USE_ASSERT_CHECKING
		acquired->isModifiedSessionId = false;
#endif

		/*
		 * Make sure that the lock is reset to released. Note: this doesn't
		 * have a matching SpinLockAcquire. We are just resetting the lock
		 * as part of initialization
		 */
		SpinLockRelease(&acquired->spinLock);
	}

	Assert(NULL != acquired);
	int pinCount = gp_atomic_add_32((volatile int *) &acquired->pinCount, 1);

	ereport(gp_sessionstate_loglevel, (errmsg("SessionState_Acquire: pinCount: %d, activeProcessCount: %d",
			pinCount, acquired->activeProcessCount), errprintstack(true)));

	LWLockRelease(SessionStateLock);

	return acquired;
}

/*
 * Releases the pinCount of a SessionState entry. If the pinCount
 * drops to 0, it puts the entry back to the freeList for reuse.
 */
static void
SessionState_Release(SessionState *acquired)
{
	if (!sessionStateInited)
	{
		Assert(NULL == acquired);
		return;
	}

	Assert(NULL != acquired);
	Assert(0 < acquired->pinCount);
	Assert(acquired->sessionId == gp_session_id || acquired->isModifiedSessionId);

	LWLockAcquire(SessionStateLock, LW_EXCLUSIVE);

	Assert(!isProcessActive);
	Assert(acquired->activeProcessCount < acquired->pinCount);

	int pinCount = gp_atomic_add_32((volatile int *) &acquired->pinCount, -1);

	ereport(gp_sessionstate_loglevel, (errmsg("SessionState_Release: pinCount: %d, activeProcessCount: %d",
			pinCount, acquired->activeProcessCount), errprintstack(true)));

	    /* Before this point the process should have been deactivated */
	Assert(acquired->activeProcessCount <= acquired->pinCount);
	Assert(0 <= acquired->pinCount);

	if (0 == acquired->pinCount)
	{
		RunawayCleaner_RunawayCleanupDoneForSession();

		acquired->sessionId = INVALID_SESSION_ID;

		Assert(0 == acquired->sessionVmem);
		Assert(acquired->runawayStatus == RunawayStatus_NotRunaway);
		Assert(CLEANUP_COUNTDOWN_BEFORE_RUNAWAY == acquired->cleanupCountdown);
		Assert(0 == acquired->activeProcessCount);

		acquired->sessionVmem = 0;
		acquired->runawayStatus = RunawayStatus_NotRunaway;
		acquired->sessionVmemRunaway = 0;
		acquired->commandCountRunaway = 0;
		acquired->cleanupCountdown = CLEANUP_COUNTDOWN_BEFORE_RUNAWAY;
		acquired->activeProcessCount = 0;

#ifdef USE_ASSERT_CHECKING
		acquired->isModifiedSessionId = false;
#endif

		SessionState *cur = AllSessionStateEntries->usedList;
		SessionState *prev = NULL;

		while (cur != acquired && cur != NULL)
		{
			prev = cur;
			cur = cur->next;
		}

		Assert(cur == acquired);

		/* grabbed is at the head of used list */
		if (NULL == prev)
		{
			Assert(AllSessionStateEntries->usedList == acquired);
			AllSessionStateEntries->usedList = acquired->next;
		}
		else
		{
			prev->next = cur->next;
		}

		acquired->next = AllSessionStateEntries->freeList;
		AllSessionStateEntries->freeList = acquired;
		AllSessionStateEntries->numSession--;
		Assert(AllSessionStateEntries->numSession >= 0);
	}

	LWLockRelease(SessionStateLock);
}

/* Returns the size of the SessionState array */
Size
SessionState_ShmemSize()
{
	SessionStateArrayEntryCount = MaxBackends;

	Size size = offsetof(SessionStateArray, data);
	size = add_size(size, mul_size(sizeof(SessionState),
			SessionStateArrayEntryCount));

	return size;
}

/* Allocates the shared memory SessionStateArray */
void
SessionState_ShmemInit()
{
	bool	found = false;

	Size shmemSize = SessionState_ShmemSize();
	AllSessionStateEntries = (SessionStateArray *)
				ShmemInitStruct(SHMEM_SESSION_STATE_ARRAY, shmemSize, &found);

	Assert(found || !IsUnderPostmaster);

	if (!IsUnderPostmaster)
	{
		MemSet(AllSessionStateEntries, 0, shmemSize);

		/*
		 * We're the first - initialize.
		 */
		AllSessionStateEntries->numSession = 0;
		AllSessionStateEntries->maxSession = SessionStateArrayEntryCount;

		AllSessionStateEntries->sessions = (SessionState *)&AllSessionStateEntries->data;

		/* Every entry of the array is free at this time */
		AllSessionStateEntries->freeList = AllSessionStateEntries->sessions;
		AllSessionStateEntries->usedList = NULL;

		/*
		 * Set all the entries' sessionId to invalid. Also, set the next pointer
		 * to point to the next entry in the array.
		 */
		SessionState *prev = &AllSessionStateEntries->sessions[0];
		prev->sessionId = INVALID_SESSION_ID;
		prev->cleanupCountdown = CLEANUP_COUNTDOWN_BEFORE_RUNAWAY;

		for (int i = 1; i < AllSessionStateEntries->maxSession; i++)
		{
			SessionState *cur = &AllSessionStateEntries->sessions[i];

			cur->sessionId = INVALID_SESSION_ID;
			cur->cleanupCountdown = CLEANUP_COUNTDOWN_BEFORE_RUNAWAY;
			prev->next = cur;

			prev = cur;
		}

		prev->next = NULL;
	}
}

/* Initialize the SessionState for current session */
void
SessionState_Init()
{
	Assert(NULL == MySessionState);

	if (INVALID_SESSION_ID == gp_session_id)
	{
		return;
	}

	Assert(!sessionStateInited);

	MySessionState = SessionState_Acquire(gp_session_id);

	Assert(NULL != MySessionState);

	sessionStateInited = true;
}

/* Shutdown the SessionState for current session */
void
SessionState_Shutdown()
{
	Assert(INVALID_SESSION_ID == gp_session_id || NULL != MySessionState);

	Assert(INVALID_SESSION_ID == gp_session_id || sessionStateInited);

	SessionState_Release((SessionState *)MySessionState);

	MySessionState = NULL;

	sessionStateInited = false;
}

/*
 * Returns true if the SessionState entry is acquired by a session
 */
bool
SessionState_IsAcquired(SessionState *sessionState)
{
	return sessionState->sessionId != INVALID_SESSION_ID;
}
