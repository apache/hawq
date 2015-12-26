/*
 * gptime
 *   define time-related functions used in GPDB
 * 
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
 *
 * IDENTIFICATION
 *   $Id$
 */
#include "c.h"

#include <sys/time.h>

#ifdef HAVE_LIBRT
#include <time.h>
#endif

#include "pgtime.h"

/*
 * gp_set_monotonic_begin_time: set the beginTime and endTime to the current
 * time.
 */
void
gp_set_monotonic_begin_time(GpMonotonicTime *time)
{
	time->beginTime.tv_sec = 0;
	time->beginTime.tv_usec = 0;
	time->endTime.tv_sec = 0;
	time->endTime.tv_usec = 0;
	
	gp_get_monotonic_time(time);

	time->beginTime.tv_sec = time->endTime.tv_sec;
	time->beginTime.tv_usec = time->endTime.tv_usec;
}


/*
 * gp_get_monotonic_time
 *    This function returns the time in the monotonic order.
 *
 * The new time is stored in time->endTime, which has a larger value than
 * the original value. The original endTime is lost.
 *
 * This function is intended for computing elapsed time between two
 * calls. It is not for getting the system time.
 */
void
gp_get_monotonic_time(GpMonotonicTime *time)
{
	struct timeval newTime;
	int status;

#if HAVE_LIBRT
	/* Use clock_gettime to return monotonic time value. */
	struct timespec ts;
	status = clock_gettime(CLOCK_MONOTONIC, &ts);

	newTime.tv_sec = ts.tv_sec;
	newTime.tv_usec = ts.tv_nsec / 1000;

#else

	gettimeofday(&newTime, NULL);
	status = 0; /* gettimeofday always succeeds. */

#endif

	if (status == 0 &&
		timeCmp(&time->endTime, &newTime) < 0)
	{
		time->endTime.tv_sec = newTime.tv_sec;
		time->endTime.tv_usec = newTime.tv_usec;
	}

	else
	{
		time->endTime.tv_usec = time->endTime.tv_usec + 1;

		time->endTime.tv_sec = time->endTime.tv_sec +
			(time->endTime.tv_usec / USECS_PER_SECOND);
		time->endTime.tv_usec = time->endTime.tv_usec % USECS_PER_SECOND;
	}
}
