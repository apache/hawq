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
#include "resqueuemanager.h"

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

	resetResourceBundleData(&(detector->InUseTotal), 0, 0.0, 0);
	resetResourceBundleData(&(detector->LockedTotal), 0, 0.0, 0);
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
	addResourceBundleData(&(sessiontrack->InUseTotal), memorymb, core);
	addResourceBundleData(&(detector->InUseTotal), memorymb, core);

	elog(DEBUG3, "Deadlock detector adds in-use %d MB from session "INT64_FORMAT", "
				 "has %d MB in use %d MB locked.",
				 memorymb,
				 sessionid,
				 detector->InUseTotal.MemoryMB,
				 detector->LockedTotal.MemoryMB);

	return FUNC_RETURN_OK;
}

int minusSessionInUseResource(ResqueueDeadLockDetector	detector,
							  int64_t					sessionid,
							  uint32_t 					memorymb,
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

	minusResourceBundleData(&(detector->InUseTotal), memorymb, core);

	SessionTrack sessiontrack = (SessionTrack)(pair->Value);
	Assert( sessiontrack != NULL );

	minusResourceBundleData(&(sessiontrack->InUseTotal), memorymb, core);

	Assert(detector->InUseTotal.Core >= 0.0 &&
		   detector->InUseTotal.MemoryMB >= 0);
	Assert(sessiontrack->InUseTotal.Core >= 0.0 &&
		   sessiontrack->InUseTotal.MemoryMB >= 0);

	/* If the session has no resource used, remove the session tracker. */
	if ( sessiontrack->InUseTotal.MemoryMB == 0 &&
		 sessiontrack->InUseTotal.Core == 0.0 )
	{
		rm_pfree(PCONTEXT, sessiontrack);
		removeHASHTABLENode(&(detector->Sessions), &key);
	}

	elog(DEBUG3, "Deadlock detector reduces in-use %d MB from session "INT64_FORMAT", "
				 "has %d MB in use %d MB locked.",
				 memorymb,
				 sessionid,
				 detector->InUseTotal.MemoryMB,
				 detector->LockedTotal.MemoryMB);

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
	if ( pair == NULL )
	{
		curstrack = (SessionTrack)rm_palloc0(PCONTEXT, sizeof(SessionTrackData));
		curstrack->SessionID = sessionid;
		curstrack->Locked	 = false;
		resetResourceBundleData(&(curstrack->InUseTotal), 0, 0.0, 0);

		/* Add to the detector. */
		setHASHTABLENode(&(detector->Sessions), &key, curstrack, false);
	}
	else
	{
		curstrack = (SessionTrack)(pair->Value);
	}

	Assert( curstrack != NULL );
	Assert( !curstrack->Locked );
	curstrack->Locked = true;
	addResourceBundleDataByBundle(&(detector->LockedTotal),
								  &(curstrack->InUseTotal));

	elog(DEBUG3, "Deadlock detector locked session "INT64_FORMAT
				 ", has %d MB in use %d MB locked",
				 sessionid,
				 detector->InUseTotal.MemoryMB,
				 detector->LockedTotal.MemoryMB);
}

void unlockSessionResource(ResqueueDeadLockDetector detector,
						   int64_t 				    sessionid)
{
	/* Build key */
	SimpArray key;
	setSimpleArrayRef(&key, (char *)&sessionid, sizeof(int64_t));

	/* Check if the session id exists. */
	PAIR pair = getHASHTABLENode(&(detector->Sessions), &key);

	if ( pair != NULL )
	{
		SessionTrack sessiontrack = (SessionTrack)(pair->Value);
		Assert(sessiontrack != NULL);
		Assert(sessiontrack->Locked);
		minusResourceBundleDataByBundle(&(detector->LockedTotal),
									    &(sessiontrack->InUseTotal));
		sessiontrack->Locked = false;

		elog(DEBUG3, "Deadlock detector unlocked session "INT64_FORMAT
						 ", has %d MB in use %d MB locked",
						 sessionid,
						 detector->InUseTotal.MemoryMB,
						 detector->LockedTotal.MemoryMB);
	}

	Assert(detector->LockedTotal.Core >= 0.0 &&
		   detector->LockedTotal.MemoryMB >= 0);
}

SessionTrack findSession(ResqueueDeadLockDetector detector,
						 int64_t 				  sessionid)
{
	/* Build key */
	SimpArray key;
	setSimpleArrayRef(&key, (char *)&sessionid, sizeof(int64_t));

	/* Check if the session id exists. */
	PAIR pair = getHASHTABLENode(&(detector->Sessions), &key);
	if ( pair != NULL )
	{
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

	resetResourceBundleData(&(detector->InUseTotal), 0, 0.0, 0);
	resetResourceBundleData(&(detector->LockedTotal), 0, 0.0, 0);
}

void copyResourceDeadLockDetectorWithoutLocking(ResqueueDeadLockDetector source,
												ResqueueDeadLockDetector target)
{
	Assert(source != NULL);
	Assert(target != NULL);
	resetResourceDeadLockDetector(target);
	target->ResqueueTrack = source->ResqueueTrack;
	addResourceBundleDataByBundle(&(target->InUseTotal), &(source->InUseTotal));

	List 	 *allss = NULL;
	ListCell *cell	= NULL;
	getAllPAIRRefIntoList(&(source->Sessions), &allss);
	foreach(cell, allss)
	{
		SessionTrack strack = (SessionTrack)(((PAIR)lfirst(cell))->Value);
		SessionTrack newstrack = rm_palloc0(PCONTEXT, sizeof(SessionTrackData));
		newstrack->SessionID = strack->SessionID;
		newstrack->Locked	 = false;
		resetResourceBundleDataByBundle(&(newstrack->InUseTotal),
										&(strack->InUseTotal));
		/* Add to the detector. */
		SimpArray key;
		setSimpleArrayRef(&key, (char *)&(newstrack->SessionID), sizeof(int64_t));
		setHASHTABLENode(&(target->Sessions), &key, newstrack, false);
	}
}
