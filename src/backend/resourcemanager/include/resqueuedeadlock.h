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

#ifndef DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_QUEUE_DEADLOCK_DETECTOR_H
#define DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_QUEUE_DEADLOCK_DETECTOR_H
#include "envswitch.h"
#include "rmcommon.h"
#include "utils/hashtable.h"

/*------------------------------------------------------------------------------
 *
 * +------------------------------+                         +------------------+
 * | ResqueueDeadLockDetectorData |---hashlist ref (1:N)--->| SessionTrackData |
 * +------------------------------+                         +------------------+
 *
 * ResqueueDeadLockDetectorData tracks all current active sessions assigned to
 * one resource queue. Each HAWQ resource queue has one detector bound. The total
 * locked resource quantity is tracked in this instance.
 *
 * SessionTrackData tracks the resource usage quantity of one session.
 *
 * THE IDEA of detecting resource deadlock: When processing current resource
 * request at the head of the waiting queue, the requested minimum resource
 * quota plus locked resource can not be more than the maximum limit of this
 * resource queue.
 *
 *------------------------------------------------------------------------------
 * resource negotiation actions that updates this detector.
 *
 * Register connection : No action.
 *
 * Acquire resource (wait in queue) : Create new session tracker if the session
 * 									  does not exist (set zero resource in used.).
 * 									  Lock the resource usage in the detector.
 *
 * Dispatch resource successfully : Unlock the session, add resource usage in the
 * 									session.
 *
 * Dispatch resource unsuccessfully : Trigger deadlock detection. No change in
 * 									  session trackers and detectors.
 *
 * Return resource : Minus resource usage in the session. The session should not
 * 					 be in locked status.
 * 					 If the session has zero resource in use after the return
 * 					 action, drop the session tracker.
 *
 * WHEN deadlock is detected : Find the queued resource request from tail to the
 * 							   head one by one, cancel all requests that have
 * 							   nonzero resource locked until the dead lock is
 * 							   unlocked. The unlock condition is that the first
 * 							   request at the head position of the waiting queue
 * 							   must has minimum request quantity smaller than
 * 							   maximum resource limit of the resource queue minus
 * 							   locked resource quantity.
 * 							   When one request is cancelled, its corresponding
 * 							   session's locked resource is unlocked.
 *
 * When a connection track is cancelled with resource occupied :
 * 							   Minus resource usage in the session, the session
 * 							   maybe locked or not locked.
 *
 *------------------------------------------------------------------------------
 */

struct SessionTrackData
{
	int64_t				SessionID;
	ResourceBundleData 	InUseTotal;
	bool				Locked;
};

typedef struct SessionTrackData  SessionTrackData;
typedef struct SessionTrackData *SessionTrack;

struct ResqueueDeadLockDetectorData
{
	HASHTABLEData		Sessions;					/* Hash of SessionTrack. */
	ResourceBundleData	InUseTotal;
	ResourceBundleData	LockedTotal;
	void			   *ResqueueTrack;
};

typedef struct ResqueueDeadLockDetectorData  ResqueueDeadLockDetectorData;
typedef struct ResqueueDeadLockDetectorData *ResqueueDeadLockDetector;

void initializeResqueueDeadLockDetector(ResqueueDeadLockDetector detector,
										void                    *queuetrack);

int createSession(ResqueueDeadLockDetector detector,
				  int64_t 				   sessionid,
				  SessionTrack			  *sessiontrack);

int addSessionInUseResource(ResqueueDeadLockDetector detector,
							int64_t 				 sessionid,
							uint32_t 				 memorymb,
							double 					 core);

int minusSessionInUseResource(ResqueueDeadLockDetector	detector,
							  int64_t					sessionid,
							  uint32_t 					memorymb,
							  double 					core);

void createAndLockSessionResource(ResqueueDeadLockDetector detector,
								  int64_t 				   sessionid);

void unlockSessionResource(ResqueueDeadLockDetector detector,
						   int64_t 				    sessionid);

SessionTrack findSession(ResqueueDeadLockDetector detector,
						 int64_t 				  sessionid);

void resetResourceDeadLockDetector(ResqueueDeadLockDetector detector);

void copyResourceDeadLockDetectorWithoutLocking(ResqueueDeadLockDetector source,
												ResqueueDeadLockDetector target);
#endif /* DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_QUEUE_DEADLOCK_DETECTOR_H */
