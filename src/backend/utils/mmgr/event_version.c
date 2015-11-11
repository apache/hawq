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
 * event_version.c
 *	 Implementation of the event version provider. This module does not provide
 *	 any API. Instead it just sets up shared memory variables so that other
 *	 modules can track and update event versions as necessary. Event versions
 *	 are used to provide a temporal ordering of runaway events and cleanup events,
 *	 as well as the idle and the activation events of different processes.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "utils/vmem_tracker.h"

/* External dependencies within the runaway cleanup framework */
extern bool vmemTrackerInited;

#define SHMEM_EVENT_VERSION_PROVIDER "The shared counter for event version provider"
#define SHMEM_RUNAWAY_EVENT_VERSION "Most recent runaway detection version"

/*
 * A shared memory counter that provides a set of monotonically
 * increasing values. The counter is only incremented by the runaway
 * detector, at the time of a new runaway event. In fact, at the time
 * of runaway event, the detector would increment it by 2, using the
 * skipped value as the version of the runaway event. This ensures
 * that the runaway version doesn't overlap with any other version
 * as used by other processes on the segment as activation and
 * deactivation version.
 *
 * The current version would be used by processes during activation
 * or deactivation to identify when it becmes idle/active.
 */
volatile EventVersion *CurrentVersion = NULL;

/* The event version of the latest runaway event */
volatile EventVersion *latestRunawayVersion = 0;

void EventVersion_ShmemInit(void);

/*
 * Initializes the event version provider's shared memory states.
 */
void
EventVersion_ShmemInit()
{
	Assert(!vmemTrackerInited);

	bool		alreadyInShmem = false;

	CurrentVersion = (EventVersion *)
								ShmemInitStruct(SHMEM_EVENT_VERSION_PROVIDER,
										sizeof(EventVersion),
										&alreadyInShmem);
	Assert(alreadyInShmem || !IsUnderPostmaster);

	latestRunawayVersion = (EventVersion *)
								ShmemInitStruct(SHMEM_RUNAWAY_EVENT_VERSION,
										sizeof(EventVersion),
										&alreadyInShmem);
	Assert(alreadyInShmem || !IsUnderPostmaster);

	Assert(NULL != CurrentVersion);
	Assert(NULL != latestRunawayVersion);

	if(!IsUnderPostmaster)
	{
		*latestRunawayVersion = 0;
		/*
		 * As no runaway event has happened yet, we must make sure that
		 * the CurrentVersion is larger than latestRunawayVersion
		 */
		*CurrentVersion = *latestRunawayVersion + 1;
	}
}
