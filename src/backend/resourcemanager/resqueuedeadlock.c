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

#include "resqueuedeadlock.h"
#include "dynrm.h"
#include "utils/simplestring.h"

void initializeResqueueDeadLockDetector(ResqueueDeadLockDetector detector,
										void                    *queuetrack)
{
	Assert( detector != NULL );
	detector->ResqueueTrack = (DynResourceQueueTrack)queuetrack;
	initializeHASHTABLE(&(detector->Sessions),
						PCONTEXT,
						HASHTABLE_SLOT_VOLUME_DEFAULT,
						HASHTABLE_SLOT_VOLUME_DEFAULT_MAX,
						HASHTABLE_KEYTYPE_CHARARRAY,
						NULL);
	detector->InUseTotalCore      = 0;
	detector->InUseTotalMemoryMB  = 0;
	detector->LockedTotalCore     = 0;
	detector->LockedTotalMemoryMB = 0;
}

int addSessionInUseResource(ResqueueDeadLockDetector detector,
							int64_t 				 sessionid,
							uint32_t 				 memorymb,
							double 					 core)
{
	/* Build key */
	SimpArray key;
	setSimpleArrayRef(&key, (char *)&sessionid, sizeof(int64_t));

	/* Check if the session id exists. */
	PAIR pair = getHASHTABLENode(&(detector->Sessions), &key);
	if ( pair == NULL ) {
		return RESQUEMGR_NO_SESSIONID;
	}

	SessionTrack sessiontrack = (SessionTrack)(pair->Value);
	Assert( sessiontrack != NULL );
	Assert( !sessiontrack->Locked );
	sessiontrack->InUseTotalMemoryMB += memorymb;
	sessiontrack->InUseTotalCore     += core;

	detector->InUseTotalMemoryMB += memorymb;
	detector->InUseTotalCore     += core;

	return FUNC_RETURN_OK;
}

int minusSessionInUserResource(ResqueueDeadLockDetector detector,
							   int64_t 					sessionid,
							   uint32_t 				memorymb,
							   double 					core)
{
	/* Build key */
	SimpArray key;
	setSimpleArrayRef(&key, (char *)&sessionid, sizeof(int64_t));

	/* Check if the session id exists. */
	PAIR pair = getHASHTABLENode(&(detector->Sessions), &key);
	if ( pair == NULL ) {
		return RESQUEMGR_NO_SESSIONID;
	}

	detector->InUseTotalMemoryMB -= memorymb;
	detector->InUseTotalCore     -= core;

	SessionTrack sessiontrack = (SessionTrack)(pair->Value);
	Assert( sessiontrack != NULL );

	sessiontrack->InUseTotalMemoryMB -= memorymb;
	sessiontrack->InUseTotalCore     -= core;

	Assert(detector->InUseTotalCore >= 0 && detector->InUseTotalMemoryMB >= 0);
	Assert(sessiontrack->InUseTotalCore >= 0 && sessiontrack->InUseTotalMemoryMB >= 0);

	/* If the session has no resource used, remove the session tracker. */
	if ( sessiontrack->InUseTotalMemoryMB == 0 &&
		 sessiontrack->InUseTotalCore == 0	) {
		rm_pfree(PCONTEXT, sessiontrack);
		/* Remove from hash table. */
		removeHASHTABLENode(&(detector->Sessions), &key);
	}

	return FUNC_RETURN_OK;
}

int removeSession(ResqueueDeadLockDetector detector, int64_t sessionid)
{
	/* Build key */
	SimpArray key;
	setSimpleArrayRef(&key, (char *)&sessionid, sizeof(int64_t));

	/* Check if the session id exists. */
	PAIR pair = getHASHTABLENode(&(detector->Sessions), &key);
	if ( pair == NULL ) {
		return RESQUEMGR_NO_SESSIONID;
	}

	SessionTrack sessiontrack = (SessionTrack)(pair->Value);
	detector->InUseTotalMemoryMB -= sessiontrack->InUseTotalMemoryMB;
	detector->InUseTotalCore     -= sessiontrack->InUseTotalCore;

	rm_pfree(PCONTEXT, sessiontrack);

	/* Remove from hash table. */
	removeHASHTABLENode(&(detector->Sessions), &key);

	return FUNC_RETURN_OK;
}

void createAndLockSessionResource(ResqueueDeadLockDetector detector,
								  int64_t 				   sessionid)
{
	SessionTrack curstrack = NULL;

	/* Build key */
	SimpArray key;
	setSimpleArrayRef(&key, (char *)&sessionid, sizeof(int64_t));

	/* Check if the session id exists. */
	PAIR pair = getHASHTABLENode(&(detector->Sessions), &key);
	if ( pair == NULL ) {
		curstrack = (SessionTrack)rm_palloc0(PCONTEXT, sizeof(SessionTrackData));
		curstrack->SessionID 		  = sessionid;
		curstrack->InUseTotalCore     = 0;
		curstrack->InUseTotalMemoryMB = 0;
		curstrack->Locked			  = false;
		/* Add to the detector. */
		setHASHTABLENode(&(detector->Sessions), &key, curstrack, false);
	}
	else {
		curstrack = (SessionTrack)(pair->Value);
	}

	Assert( curstrack != NULL );
	Assert( !curstrack->Locked );
	curstrack->Locked = true;
	detector->LockedTotalCore     += curstrack->InUseTotalCore;
	detector->LockedTotalMemoryMB += curstrack->InUseTotalMemoryMB;

	elog(DEBUG3, "Locked session "INT64_FORMAT "Left %d MB",
				 sessionid,
				 detector->LockedTotalMemoryMB);
}

void unlockSessionResource(ResqueueDeadLockDetector detector,
						   int64_t 				    sessionid)
{
	/* Build key */
	SimpArray key;
	setSimpleArrayRef(&key, (char *)&sessionid, sizeof(int64_t));

	/* Check if the session id exists. */
	PAIR pair = getHASHTABLENode(&(detector->Sessions), &key);

	if ( pair != NULL ) {
		SessionTrack sessiontrack = (SessionTrack)(pair->Value);
		Assert(sessiontrack != NULL);
		Assert(sessiontrack->Locked);
		detector->LockedTotalCore     -= sessiontrack->InUseTotalCore;
		detector->LockedTotalMemoryMB -= sessiontrack->InUseTotalMemoryMB;
		sessiontrack->Locked = false;

		elog(DEBUG3, "Unlocked session "INT64_FORMAT "Left %d MB",
					 sessionid,
					 detector->LockedTotalMemoryMB);
	}

	Assert(detector->LockedTotalCore >= 0 && detector->LockedTotalMemoryMB >= 0);
}

SessionTrack findSession(ResqueueDeadLockDetector detector,
						 int64_t 				  sessionid)
{
	/* Build key */
	SimpArray key;
	setSimpleArrayRef(&key, (char *)&sessionid, sizeof(int64_t));

	/* Check if the session id exists. */
	PAIR pair = getHASHTABLENode(&(detector->Sessions), &key);
	if ( pair != NULL ) {
		return (SessionTrack)(pair->Value);
	}
	return NULL;
}

void resetResourceDeadLockDetector(ResqueueDeadLockDetector detector)
{
	List 	 *allss = NULL;
	ListCell *cell	= NULL;
	getAllPAIRRefIntoList(&(detector->Sessions), &allss);
	foreach(cell, allss)
	{
		SessionTrack strack = (SessionTrack)(((PAIR)lfirst(cell))->Value);
		rm_pfree(PCONTEXT, strack);
	}
	freePAIRRefList(&(PCONTRACK->Connections), &allss);
	clearHASHTABLE(&(detector->Sessions));

	detector->InUseTotalMemoryMB 	= 0;
	detector->InUseTotalCore		= 0.0;
	detector->LockedTotalMemoryMB	= 0;
	detector->LockedTotalCore		= 0.0;
}
