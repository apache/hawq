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

/*
 * testutils.c
 * 		Implementation of testing utilities.
 *
 */

#include <sys/time.h>
#include <sys/resource.h>

#include "postgres.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "utils/elog.h"
#include "utils/testutils.h"

#ifdef USE_TEST_UTILS

#define MAX_FRAME_DEPTH   64
#define USECS_IN_MSEC     1000
#define MSECS_IN_SEC      1000

/* holds return addresses of frames in stack */
static void *stackAddressesLastCFI[MAX_FRAME_DEPTH];

/* stack depth */
static uint32 stackDepthLastCFI = 0;

/* elapsed user time */
static struct timeval userTimeLastCFI;

/* static functions */
static int32 timeElapsedMs(struct timeval *tvLast, struct timeval *tvFirst);
static bool IsValidReportLevel(int level);

/*
 * Reset time slice
 */
void
TimeSliceReset()
{
	struct rusage ru;

	if (0 != getrusage(RUSAGE_SELF, &ru))
	{
		elog(ERROR, "Time slicer: Failed to retrieve user time");
	}

	userTimeLastCFI = ru.ru_utime;
	stackDepthLastCFI = gp_backtrace(stackAddressesLastCFI, MAX_FRAME_DEPTH);
}


/*
 * Check if time slice since last check-for-interrupts (CFI) has been exceeded
 */
void
TimeSliceCheck(const char *file, int line)
{
	Assert(gp_test_time_slice);
	Assert(IsValidReportLevel(gp_test_time_slice_report_level));

	struct rusage ru;
	int32 elapsedMs = 0;

	/* CFI is disabled inside critical sections */
	if (0 != InterruptHoldoffCount || 0 != CritSectionCount)
	{
		return;
	}

	/* get current user time */
	if (getrusage(RUSAGE_SELF, &ru) != 0)
	{
		elog(ERROR, "Time slicer: Failed to retrieve user time");
	}

	elapsedMs = timeElapsedMs(&ru.ru_utime, &userTimeLastCFI);

	Assert(0 <= elapsedMs);

	/* check elapsed time since last CFI  */
	if (gp_test_time_slice_interval < elapsedMs)
	{
		void *stackAddressesCurrent[MAX_FRAME_DEPTH];
		uint32 stackDepthCurrent = gp_backtrace(stackAddressesCurrent, MAX_FRAME_DEPTH);

		char *stackTraceLastCFI = gp_stacktrace(stackAddressesLastCFI, stackDepthLastCFI);
		char *stackTraceCurrent = gp_stacktrace(stackAddressesCurrent, stackDepthCurrent);

		/* report time slice violation error */
		ereport(gp_test_time_slice_report_level,
				(errmsg("Time slice of %d ms exceeded at (%s:%d), last CFI before %d ms.\n"
						"Stack trace of last CFI:\n%s\n"
						"Current stack trace:\n%s\n",
						gp_test_time_slice_interval,
						file,
						line,
						elapsedMs,
						stackTraceLastCFI,
						stackTraceCurrent)));
	}

	/* reset time slice */
	userTimeLastCFI = ru.ru_utime;
	stackDepthLastCFI = gp_backtrace(stackAddressesLastCFI, MAX_FRAME_DEPTH);
}


/*
 * Get elapsed time between two timestamps in ms
 */
static int32
timeElapsedMs(struct timeval *tvLast, struct timeval *tvFirst)
{
	return (tvLast->tv_sec - tvFirst->tv_sec) * MSECS_IN_SEC +
			(tvLast->tv_usec - tvFirst->tv_usec) / USECS_IN_MSEC;
}

/*
 * Check if report level is valid
 */
static bool
IsValidReportLevel(int level)
{
	return (level >= NOTICE && level <= PANIC);
}


#ifdef USE_TEST_UTILS_X86

/*
 * check if lightweight lock(s) are held;
 * print stack trace where lock(s) got acquired and error out;
 */
void
LWLockHeldDetect(const void *pv, int lockmode)
{
	Assert(gp_test_deadlock_hazard);
	Assert(IsValidReportLevel(gp_test_deadlock_hazard_report_level));

	const LOCKTAG *locktag = (const LOCKTAG *) pv;

	if (0 < LWLocksHeld())
	{
		void *stackAddressesCurrent[MAX_FRAME_DEPTH];
		uint32 stackDepthCurrent = gp_backtrace(stackAddressesCurrent, MAX_FRAME_DEPTH);
		char *stackTraceCurrent = gp_stacktrace(stackAddressesCurrent, stackDepthCurrent);

		const char *stackTraces = LWLocksHeldStackTraces();
		Assert(NULL != stackTraces);

		/* report time slice violation error */
		ereport(gp_test_deadlock_hazard_report_level,
				(errmsg("Attempting to acquire database lock (%s:%d:%d:%d:%d) while holding lightweight lock (%d:%p).\n"
						"Stack trace(s) where lightweight lock(s) got acquired:\n%s\n"
						"Current stack trace:\n%s\n",
						GetLockmodeName(locktag->locktag_lockmethodid, lockmode),
						locktag->locktag_field1,
						locktag->locktag_field2,
						locktag->locktag_field3,
						locktag->locktag_field4,
						LWLockHeldLatestId(),
						LWLockHeldLatestCaller(),
						stackTraces,
						stackTraceCurrent)));
	}
}

#endif /* USE_TEST_UTILS_X86 */

#endif /* USE_TEST_UTILS */

/* EOF */
