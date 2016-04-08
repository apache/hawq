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

#include "dynrm.h"
#include "communication/rmcomm_MessageServer.h"
#include "communication/rmcomm_MessageHandler.h"
#include "communication/rmcomm_QD_RM_Protocol.h"

void cutReferenceOfConnTrackAndCommBuffer(AsyncCommMessageHandlerContext context);
void removeResourceRequestInConnHavingRequestsInternal(int32_t 	 connid,
													   List    **requests);

/* Initialize connection track manager. */
void initializeConnectionTrackManager(void)
{
	initializeHASHTABLE(&(PCONTRACK->Connections),
						  PCONTEXT,
						  HASHTABLE_SLOT_VOLUME_DEFAULT,
						  HAWQRM_QD_CONNECTION_MAX_CAPABILITY * 2,
						  HASHTABLE_KEYTYPE_UINT32,
						  NULL);

	PCONTRACK->FreeConnIDs			= NULL;
	PCONTRACK->ConnHavingRequests 	= NULL;
	PCONTRACK->ConnToRetry			= NULL;
	PCONTRACK->ConnToSend			= NULL;

	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	for ( int i = 0 ; i < HAWQRM_QD_CONNECTION_MAX_CAPABILITY ; ++i )
	{
		PCONTRACK->FreeConnIDs = lappend_int(PCONTRACK->FreeConnIDs, i);
	}
	MEMORY_CONTEXT_SWITCH_BACK
}

/* Use connection id. */
int useConnectionID(int32_t *connid)
{
	/* Ensure that we have potential enough connection IDs to utilize. */
	if ( PCONTRACK->FreeConnIDs == NULL )
	{
		*connid = INVALID_CONNID;
		return CONNTRACK_CONNID_FULL;
	}
	*connid = lfirst_int(list_head(PCONTRACK->FreeConnIDs));
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->FreeConnIDs = list_delete_first(PCONTRACK->FreeConnIDs);
	MEMORY_CONTEXT_SWITCH_BACK
	elog(DEBUG3, "Resource manager uses connection track ID %d", *connid);
	return FUNC_RETURN_OK;
}

/* Return connection id. */
int returnConnectionID(int32_t connid)
{
	int value = connid;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->FreeConnIDs = lappend_int(PCONTRACK->FreeConnIDs, value);
	MEMORY_CONTEXT_SWITCH_BACK
	elog(DEBUG3, "Resource manager returned connection track ID %d", connid);
	return FUNC_RETURN_OK;
}


int getInUseConnectionTrack(uint32_t connid, ConnectionTrack *track)
{
	PAIR pair = getHASHTABLENode( &(PCONTRACK->Connections),
								  TYPCONVERT(void *,connid));
	if ( pair == NULL )
	{
		return CONNTRACK_NO_CONNID;
	}

	*track = (ConnectionTrack)(pair->Value);
	return FUNC_RETURN_OK;
}

/*
 * Create empty connection track instance.
 */
void createEmptyConnectionTrack(ConnectionTrack *track)
{
	/* Create new entry in connection track. */
	(*track) = rm_palloc0(PCONTEXT, sizeof(ConnectionTrackData));

	(*track)->RequestTime	 			= 0;
	(*track)->RegisterTime   			= 0;
	(*track)->ResRequestTime 			= 0;
	(*track)->ResAllocTime	 			= 0;
	(*track)->LastActTime	 			= 0;
	(*track)->HeadQueueTime				= 0;

	(*track)->MessageSize	 	 		= 0;
	(*track)->MessageMark1   	 		= 0;
	(*track)->MessageMark2   	 		= 0;
	(*track)->MessageID	     	 		= 0;
	initializeSelfMaintainBuffer(&((*track)->MessageBuff), PCONTEXT);
	(*track)->MessageReceiveTime 		= 0;

	(*track)->ConnID 		 			= INVALID_CONNID;
	(*track)->QueueID			 		= 0;
	(*track)->Progress		 	 		= CONN_PP_INFO_NOTSET;
	(*track)->ResponseSent	 	 		= false;
	(*track)->SessionID					= -1;

	(*track)->SegMemoryMB	 	 		= -1;
	(*track)->SegCore		 	 		= -1.0;
	(*track)->SegIOBytes				= 0;
	(*track)->SegNum			 		= -1;
	(*track)->SegNumMin					= -1;
	(*track)->SegNumActual				= -1;
	(*track)->SegNumEqual				= 0;
	(*track)->SegPreferredHostCount 	= 0;
	(*track)->SegPreferredHostNames 	= NULL;
	(*track)->SegPreferredScanSizeMB 	= NULL;
	(*track)->SliceSize					= 0;
	(*track)->IOBytes					= 0;
	(*track)->MaxSegCountFixed			= 0;
	(*track)->MinSegCountFixed			= 0;
	(*track)->VSegLimitPerSeg			= -1;
	(*track)->VSegLimit					= -1;
	(*track)->StatVSegMemoryMB			= 0;
	(*track)->StatNVSeg					= 0;
	(*track)->Resource					= NULL;

	(*track)->QueueTrack		 		= NULL;
	(*track)->User				 		= NULL;

	(*track)->isOld						= false;

	(*track)->troubledByFragment		= false;
	(*track)->troubledByFragmentTimestamp = 0;

	(*track)->CommBuffer				= NULL;
}

void freeUsedConnectionTrack(ConnectionTrack track)
{
	if ( track->SegPreferredHostNames != NULL )
	{
		rm_pfree(PCONTEXT, track->SegPreferredHostNames);
	}

	/* The connection tracker to be freed must not contain allocated resource. */
	Assert(list_length(track->Resource) == 0);

	destroySelfMaintainBuffer(&(track->MessageBuff));
	resetSelfMaintainBuffer(&(track->MessageBuff));
	rm_pfree(PCONTEXT, track);
}

/*
 * Use one empty connection track.
 *
 * Basically, the recycled connection track instance is utilized, and new
 * connection id is fetched.
 */
int useConnectionTrack(ConnectionTrack *track)
{
	int 			res 	= FUNC_RETURN_OK;
	ConnectionTrack result 	= NULL;

	*track = NULL;
	/* Ensure that we have potential enough connection IDs to utilize. */
	if ( PCONTRACK->FreeConnIDs == NULL )
	{
		return CONNTRACK_CONNID_FULL;
	}

	createEmptyConnectionTrack(&result);
	*track = result;

	/* The track must be initialized or recycled correctly. */
	Assert((*track)->ConnID == -1);
	Assert((*track)->Progress == CONN_PP_INFO_NOTSET);

    *track = result;

    elog(DEBUG5, "HAWQ RM :: New connection track %lx.", (unsigned long)(result));

	return res;
}

void trackConnectionTrack(ConnectionTrack track)
{
	Assert(track != NULL);
	Assert(track->ConnID != INVALID_CONNID);

	setHASHTABLENode( &(PCONTRACK->Connections),
					  TYPCONVERT(void *,track->ConnID),
					  (void *)(track),
					  false ); 					/* Should be no old values. */
}

/*
 * Return connection track instance which will be recycled.
 */
void returnConnectionTrack(ConnectionTrack track)
{
	Assert(track != NULL);

	if ( track->ConnID != -1 )
	{
		elog(DEBUG5, "Resource manager returned connection track with Conn ID %d, "
					 "addr %lx %d conn track left.",
					 track->ConnID,
					 (unsigned long)track,
					 PCONTRACK->Connections.NodeCount-1);

		/* Return connection ID firstly. */
		returnConnectionID(track->ConnID);
		/* Remove index from HASHTABLE. */
		removeHASHTABLENode( &(PCONTRACK->Connections),
							 TYPCONVERT(void *, track->ConnID));
		track->ConnID = -1;
	}

	freeSegPreferredHostInfo(track);

	/* Recycle the track. */
	freeUsedConnectionTrack(track);
}

int retrieveConnectionTrack(ConnectionTrack track, int32_t connid)
{
	/*
	 * If this connection track has no pre-set connection id, the in-use one can
	 * be retrieved, otherwise, this connection track can only receive messages
	 * sent through the same connection id.
	 *
	 * If the RPC is stateless, no need to call this function.
	 */
	Assert( track->ConnID == -1 || track->ConnID == connid );
	if ( track->ConnID != -1 )
	{
		return FUNC_RETURN_OK;
	}

	int 			res   = FUNC_RETURN_OK;
	ConnectionTrack oldct = NULL;
	res = getInUseConnectionTrack(connid, &oldct);
	if ( res != FUNC_RETURN_OK )
	{
		elog(WARNING, "Resource manager received invalid Conn ID %d.", connid);
		transformConnectionTrackProgress(track, CONN_PP_TRANSFORM_ERROR);
		return res;
	}

	Assert( oldct != NULL );

	track->ConnID   				= oldct->ConnID;
	track->Progress 				= oldct->Progress;
	track->QueueID  				= oldct->QueueID;
	track->QueueTrack 				= oldct->QueueTrack;
	track->RegisterTime 			= oldct->RegisterTime;
	track->ResAllocTime 			= oldct->ResAllocTime;
	track->ResRequestTime 			= oldct->ResRequestTime;
	track->LastActTime				= oldct->LastActTime;

	/* Move old resource list to new connection tracker. */
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	track->Resource = list_concat(track->Resource, oldct->Resource);
	MEMORY_CONTEXT_SWITCH_BACK
	oldct->Resource = NULL;

	track->ResponseSent     		= oldct->ResponseSent;
	track->MaxSegCountFixed	  		= oldct->MaxSegCountFixed;
	track->MinSegCountFixed			= oldct->MinSegCountFixed;
	track->VSegLimitPerSeg			= oldct->VSegLimitPerSeg;
	track->VSegLimit				= oldct->VSegLimit;
	track->StatNVSeg				= oldct->StatNVSeg;
	track->StatVSegMemoryMB			= oldct->StatVSegMemoryMB;
	track->SegNumEqual				= oldct->SegNumEqual;
	track->SliceSize				= oldct->SliceSize;
	track->SegIOBytes				= oldct->SegIOBytes;
	track->IOBytes					= oldct->IOBytes;
	track->SegCore					= oldct->SegCore;
	track->SegMemoryMB      		= oldct->SegMemoryMB;
	track->SegNum           		= oldct->SegNum;
	track->SegNumActual     		= oldct->SegNumActual;
	track->SegNumMin				= oldct->SegNumMin;
	track->SegPreferredHostCount 	= oldct->SegPreferredHostCount;

	track->isOld					= oldct->isOld;

	track->troubledByFragment		= oldct->troubledByFragment;
	track->troubledByFragmentTimestamp = oldct->troubledByFragmentTimestamp;

	track->SessionID     			= oldct->SessionID;
	track->User 					= oldct->User;
	memcpy(track->UserID, oldct->UserID, sizeof(track->UserID));

	oldct->ConnID = INVALID_CONNID; /* Avoid recycling connection id resource */
	returnConnectionTrack(oldct);

	/* Update connection track hash table. */
	trackConnectionTrack(track);

	elog(DEBUG3, "Resource manager fetched existing connection track ID=%d, "
				 "Progress=%d.",
				 track->ConnID,
				 track->Progress);

	return FUNC_RETURN_OK;
}

void setConnectionTrackMessageBuffer(ConnectionTrack 	track,
									 char 			   *content,
									 int 				size)
{
	Assert(track != NULL);
	resetSelfMaintainBuffer(&(track->MessageBuff));
	appendSelfMaintainBuffer(&(track->MessageBuff), content, size);
}

bool canTransformConnectionTrackProgress(ConnectionTrack track,
									  	 enum CONN_PROCESS_PROGRESSES progress)
{
	Assert(track != NULL);

	switch(progress) {
	case CONN_PP_INFO_NOTSET:
		break;
	case CONN_PP_ESTABLISHED:
		return track->Progress == CONN_PP_INFO_NOTSET ||
			   track->Progress == CONN_PP_REGISTER_DONE;
	case CONN_PP_REGISTER_DONE:
		return track->Progress == CONN_PP_ESTABLISHED ||
			   track->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT ||
			   track->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_DONE ||
			   track->Progress == CONN_PP_DDL_REQUEST_ACCEPTED;
	case CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT:
		return track->Progress == CONN_PP_REGISTER_DONE;
	case CONN_PP_RESOURCE_QUEUE_ALLOC_DONE:
		return track->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT;
	case CONN_PP_DDL_REQUEST_ACCEPTED:
		return track->Progress == CONN_PP_REGISTER_DONE;

	case CONN_PP_REGISTER_FAIL:
		return track->Progress == CONN_PP_ESTABLISHED;
	case CONN_PP_RESOURCE_ACQUIRE_FAIL:
		return track->Progress == CONN_PP_REGISTER_DONE;
	case CONN_PP_RESOURCE_QUEUE_ALLOC_FAIL:
		return track->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT;
	case CONN_PP_TIMEOUT_FAIL:
		return track->Progress == CONN_PP_ESTABLISHED ||
			   track->Progress == CONN_PP_REGISTER_DONE ||
			   track->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT ||
			   track->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_DONE;
	case CONN_PP_TRANSFORM_ERROR:
		return true;
	default:
		Assert(false);
	}
	return false;
}

/**
 * Transform connection track to new progress, the validity is checked.
 */
void transformConnectionTrackProgress(ConnectionTrack track,
									  enum CONN_PROCESS_PROGRESSES progress)
{
	Assert(track != NULL);

	Assert(canTransformConnectionTrackProgress(track, progress));
	track->Progress = progress;
}

void addMessageToConnTrack(AsyncCommMessageHandlerContext	context,
						   uint16_t							messageid,
						   uint8_t							mark1,
						   uint8_t							mark2,
						   char 						   *buffer,
						   uint32_t							buffersize)
{
	/* Create a new connection track instance to save received message. */
	ConnectionTrack conntrack = NULL;
	createEmptyConnectionTrack(&conntrack);

	conntrack->MessageID    = messageid;
	conntrack->MessageMark1 = mark1;
	conntrack->MessageMark2 = mark2;
	conntrack->MessageSize  = buffersize;
	resetSelfMaintainBuffer(&(conntrack->MessageBuff));
	appendSelfMaintainBuffer(&(conntrack->MessageBuff), buffer, buffersize);

	/* Let connection track instance able to find the socket connection. */
	conntrack->CommBuffer = context->AsyncBuffer;
	/* Let comm buffer instance able to find connection track. */
	context->UserData = conntrack;

	/* Start from a established connection. */
	transformConnectionTrackProgress(conntrack, CONN_PP_ESTABLISHED);

	conntrack->RequestTime = gettime_microsec();
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnHavingRequests = lappend(PCONTRACK->ConnHavingRequests,
											conntrack);
	MEMORY_CONTEXT_SWITCH_BACK
}

void sentMessageFromConnTrack(AsyncCommMessageHandlerContext context)
{
	ConnectionTrack conntrack = (ConnectionTrack)(context->UserData);
	conntrack->ResponseSent = true;

	/* Clean up the connection between connection track and comm buffer. */
	cutReferenceOfConnTrackAndCommBuffer(context);
}
void hasCommErrorInConnTrack(AsyncCommMessageHandlerContext context)
{
	/* This is a call back function, nothing to do. */
	cutReferenceOfConnTrackAndCommBuffer(context);
}

void cleanupConnTrack(AsyncCommMessageHandlerContext context)
{
	cutReferenceOfConnTrackAndCommBuffer(context);
}

void cutReferenceOfConnTrackAndCommBuffer(AsyncCommMessageHandlerContext context)
{
	ConnectionTrack conntrack = (ConnectionTrack)(context->UserData);
	bool returnconn = false;

	if ( conntrack != NULL && conntrack->ConnID == -1 )
	{
		elog(DEBUG3, "Resource manager returns connection track with no conn id set.");
		returnconn = true;
	}
	else if ( conntrack != NULL &&
		      (conntrack->Progress == CONN_PP_ESTABLISHED||
		       conntrack->Progress > CONN_PP_FAILS) )
	{
		elog(DEBUG3, "Resource manager returns connection track due to removable "
					 "status. %d",
					 conntrack->Progress);
		returnconn = true;
	}
	else if ( conntrack != NULL )
	{
		/* Cut the reference between connection track and rmcomm buffer. */
		conntrack->CommBuffer = NULL;
		context->UserData = NULL;
	}

	if ( returnconn )
	{
		Assert(conntrack != NULL);
		returnConnectionTrack(conntrack);
		context->UserData = NULL;
	}
}

/*
 * The main processing loop for all computations.
 */
void processSubmittedRequests(void)
{
	ConnectionTrack  ct    		= NULL;

	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	while( list_length(PCONTRACK->ConnHavingRequests) > 0 )
	{
		ct = (ConnectionTrack)lfirst(list_head(PCONTRACK->ConnHavingRequests));
		PCONTRACK->ConnHavingRequests = list_delete_first(PCONTRACK->ConnHavingRequests);
		RMMessageHandlerType handler = getMessageHandler(ct->MessageID);
		Assert(handler != NULL);
		if ( !handler((void **)&ct) )
		{
			PCONTRACK->ConnToRetry = lappend(PCONTRACK->ConnToRetry, ct);
		}
	}

	if ( list_length(PCONTRACK->ConnToRetry) > 0 )
	{
		elog(DEBUG3, "Resource manager retries %d requests in next loop.",
				  	 list_length(PCONTRACK->ConnToRetry));
	}

	while( list_length(PCONTRACK->ConnToRetry) > 0 )
	{
		void *move = lfirst(list_head(PCONTRACK->ConnToRetry));
		PCONTRACK->ConnToRetry = list_delete_first(PCONTRACK->ConnToRetry);
		PCONTRACK->ConnHavingRequests = lappend(PCONTRACK->ConnHavingRequests, move);
	}
	MEMORY_CONTEXT_SWITCH_BACK
}

void buildSegPreferredHostInfo(ConnectionTrack track)
{
	if ( track->SegPreferredHostCount > 0 )
	{
		track->SegPreferredScanSizeMB =
				(int64_t *)(track->MessageBuff.Buffer +
							sizeof(RPCRequestHeadAcquireResourceFromRMData));

		track->SegPreferredHostNames =
				(char **)rm_palloc0(PCONTEXT,
									sizeof(char *)*track->SegPreferredHostCount);

		track->SegPreferredHostNames[0] =
				track->MessageBuff.Buffer +
				sizeof(RPCRequestHeadAcquireResourceFromRMData) +
				sizeof(int64_t) * track->SegPreferredHostCount;

		for ( int i = 1 ; i < track->SegPreferredHostCount ; i++ )
		{
			/* Point to each host name strings. */
			track->SegPreferredHostNames[i] = track->SegPreferredHostNames[i-1] +
											  strlen(track->SegPreferredHostNames[i-1]) + 1;
		}
	}
}

void freeSegPreferredHostInfo(ConnectionTrack track)
{
	if (track->SegPreferredHostNames != NULL)
	{
		rm_pfree(PCONTEXT, track->SegPreferredHostNames);
	}
	track->SegPreferredHostNames = NULL;
}

void setAllAllocatedResourceInConnectionTracksOld(void)
{
	List 	 *allconns = NULL;
	ListCell *cell	   = NULL;
	getAllPAIRRefIntoList(&(PCONTRACK->Connections), &allconns);

	foreach(cell, allconns)
	{
		ConnectionTrack conntrack = (ConnectionTrack)(((PAIR)lfirst(cell))->Value);
		if (!conntrack->isOld &&
		    conntrack->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_DONE )
		{
			conntrack->isOld = true;

			Assert( conntrack->QueueTrack != NULL );
			DynResourceQueueTrack quetrack = (DynResourceQueueTrack)
											 conntrack->QueueTrack;
			minusResourceBundleData(&(quetrack->TotalUsed),
									conntrack->SegMemoryMB * conntrack->SegNumActual,
									conntrack->SegCore     * conntrack->SegNumActual);
			Assert(quetrack->TotalUsed.MemoryMB >= 0 &&
				   quetrack->TotalUsed.Core >= 0);
			elog(DEBUG3, "Resource manager moved resource to old from in-use "
						 "resource counter (%d MB, %lf CORE), "
						 "current in-use (%d MB, %lf CORE).",
						 conntrack->SegMemoryMB * conntrack->SegNumActual,
						 conntrack->SegCore     * conntrack->SegNumActual,
						 quetrack->TotalUsed.MemoryMB,
						 quetrack->TotalUsed.Core);
		}
	}
	freePAIRRefList(&(PCONTRACK->Connections), &allconns);
}

void dumpConnectionTracks(const char *filename)
{
	if ( filename == NULL )
	{
		return;
	}
	FILE *fp = fopen(filename, "w");

	fprintf(fp, "Number of free connection ids : %d\n",
				list_length(PCONTRACK->FreeConnIDs));
	fprintf(fp, "Number of connection tracks having requests to handle : %d\n",
				list_length(PCONTRACK->ConnHavingRequests));
	fprintf(fp, "Number of connection tracks having responses to send : %d\n",
				list_length(PCONTRACK->ConnToSend));

	/* Output each connection track. */

	/* The output format:
	 * SOCK(client=ClientAddr:ClientPort:time=ConnTime),
	 * CONN(id=ConnID:user=UserID:queue=QueueName:prog=Progress:RegisterTime:\
	 * 		lastact=LastActTime),
	 * ALLOC(sessionid, segmemorymb, segcore, segnum, segnummin, segnumact, \
	 * 		 segsplitsize, slicesize, fixsegsize, resreqtime, resalloctime),
	 * LOC(hostnum, (hostname,splitnum)+)
	 * RESOURCE(Resource),
	 * MessageSize:MessageID:MessageBuffSize:MessageRecvTime,

	 * Current communication status ( from async comm buffer )
	 */
	HASHTABLE conns = &(PCONTRACK->Connections);
	for ( int i = 0 ; i < conns->SlotVolume ; ++i )
	{
		List     *slot = conns->Slots[i];
		ListCell *cell = NULL;

		foreach(cell, slot)
		{
			ConnectionTrack conn = (ConnectionTrack)(((PAIR)lfirst(cell))->Value);

			if ( conn->CommBuffer != NULL )
			{
				fprintf(fp, "SOCK(client=%s:%d:time=%s),",
							conn->CommBuffer->ClientAddrDotStr,
							conn->CommBuffer->ClientAddrPort,
							format_time_microsec(conn->RequestTime));
			}
			else
			{
				fprintf(fp, "SOCK(client=DISCONNECTED:time=NOTIME),");
			}
			fprintf(fp, "CONN(id=%d:user=%s:",
						conn->ConnID,
						conn->UserID);
			if ( conn->QueueTrack == NULL )
			{
				fprintf(fp, "queue=NULL:");
			}
			else
			{
				DynResourceQueueTrack quetrack = (DynResourceQueueTrack)
												 (conn->QueueTrack);
				fprintf(fp, "queue=%s:", quetrack->QueueInfo->Name);
			}
			fprintf(fp, "prog=%d:time=%s:lastact=%s:headqueue=%s),",
						conn->Progress,
						format_time_microsec(conn->RegisterTime),
						format_time_microsec(conn->LastActTime),
						conn->HeadQueueTime == 0 ?
							"NOT HEAD" :
							format_time_microsec(conn->HeadQueueTime));

			fprintf(fp, "ALLOC(session="INT64_FORMAT":"
						"resource=(%d MB, %lf CORE)x(%d:min=%d:act=%d):"
						"slicesize=%d:"
						"io bytes size="INT64_FORMAT":"
						"vseg limit per seg=%d:"
						"vseg limit per query=%d:"
						"fixsegsize=%d:"
						"reqtime=%s:"
						"alloctime=%s:"
						"stmt=%d MB x %d),",
						conn->SessionID,
						conn->SegMemoryMB, conn->SegCore,
						conn->SegNum, conn->SegNumMin, conn->SegNumActual,
						conn->SliceSize,
						conn->IOBytes,
						conn->VSegLimitPerSeg,
						conn->VSegLimit,
						conn->MinSegCountFixed,
						format_time_microsec(conn->ResRequestTime),
						format_time_microsec(conn->ResAllocTime),
						conn->StatVSegMemoryMB,
						conn->StatNVSeg);

			fprintf(fp, "LOC(size=%d", conn->SegPreferredHostCount);
			if ( conn->SegPreferredHostCount <= 0 )
			{
				fprintf(fp, "),");
			}
			else
			{
				for ( int i = 0 ; i < conn->SegPreferredHostCount ; ++i )
				{
					fprintf(fp, ":host(%s:"INT64_FORMAT")",
							    conn->SegPreferredHostNames[i],
								conn->SegPreferredScanSizeMB[i]);
				}
				fprintf(fp,"),");
			}

			fprintf(fp, "RESOURCE(hostsize=%d", list_length(conn->Resource));
			if ( list_length(conn->Resource) == 0 )
			{
				fprintf(fp, "),");
			}
			else
			{
				ListCell *cell = NULL;
				foreach(cell, conn->Resource)
				{
					VSegmentCounterInternal vsegcnt = (VSegmentCounterInternal)
													  lfirst(cell);
					fprintf(fp, ":host(%s,%d,%s)",
								GET_SEGRESOURCE_HOSTNAME(vsegcnt->Resource),
								vsegcnt->VSegmentCount,
								(vsegcnt->HDFSNameIndex < conn->SegPreferredHostCount ?
								 conn->SegPreferredHostNames[vsegcnt->HDFSNameIndex] :
								 "NONE"));
				}
				fprintf(fp, "),");
			}

			fprintf(fp, "MSG(id=%d:size=%d:contsize=%d:recvtime=%s, client=%s:%d),",
						conn->MessageID,
						conn->MessageSize,
						conn->MessageBuff.Cursor+1,
						format_time_microsec(conn->MessageReceiveTime),
						conn->CommBuffer == NULL ?
							"UNKNOWNHOST" :
							conn->CommBuffer->ClientAddrDotStr,
						conn->CommBuffer == NULL ?
							0:
							conn->CommBuffer->ClientAddrPort);

			fprintf(fp, "COMMSTAT(");
			if ( conn->CommBuffer == NULL )
			{
				fprintf(fp, "disconnected");
			}
			else
			{
				fprintf(fp, "fd=%d:readbuffer=%d:writebuffer=%d buffers:toclose=%s:"
							"forceclose=%s",
							conn->CommBuffer->FD,
							conn->CommBuffer->ReadBuffer.Cursor+1,
							list_length(conn->CommBuffer->WriteBuffer),
							(conn->CommBuffer->toClose ? "true" : "false"),
							(conn->CommBuffer->forcedClose ? "true" : "false"));
			}
			fprintf(fp, ")\n");
		}
	}
	fclose(fp);
}

/*
 * Build response message into Connection Track instance.
 */
void buildResponseIntoConnTrack(ConnectionTrack      conntrack,
				   	   	   	    char 				*content,
								uint32_t 			 size,
								uint8_t  			 mark1,
								uint8_t  			 mark2,
								uint16_t 			 messageid)
{
	elog(DEBUG3, "Resource manager built message id %d, size %d", messageid, size);
	conntrack->MessageID    = messageid; /* Message id.      */
	conntrack->MessageMark1 = mark1;
	conntrack->MessageMark2 = mark2;
	conntrack->MessageSize  = size;
	conntrack->ResponseSent = false;
	setConnectionTrackMessageBuffer(conntrack, content, size);
}

void copyAllocWaitingConnectionTrack(ConnectionTrack source,
									 ConnectionTrack target)
{
	target->RequestTime					= source->RequestTime;
	target->RegisterTime   				= source->RegisterTime;
	target->ResRequestTime 				= source->ResRequestTime;
	target->ResAllocTime	 			= 0;
	target->LastActTime	 				= source->LastActTime;
	target->HeadQueueTime				= source->HeadQueueTime;

	target->MessageSize	 	 			= source->MessageSize;
	target->MessageMark1   	 			= source->MessageMark1;
	target->MessageMark2   	 			= source->MessageMark2;
	target->MessageID	     	 		= source->MessageID;
	appendSelfMaintainBuffer(&(target->MessageBuff),
							 SMBUFF_CONTENT(&(source->MessageBuff)),
							 getSMBContentSize(&(source->MessageBuff)));
	target->MessageReceiveTime 			= source->MessageReceiveTime;

	target->ConnID 		 				= source->ConnID;
	memcpy(target->UserID, source->UserID, sizeof(target->UserID));
	target->QueueID			 			= source->QueueID;

	target->Progress		 	 		= source->Progress;
	target->ResponseSent	 	 		= source->ResponseSent;
	target->SessionID					= source->SessionID;

	target->SegMemoryMB	 	 			= -1;
	target->SegCore		 	 			= -1.0;
	target->SegIOBytes					= 0;
	target->SegNum			 			= -1;
	target->SegNumMin					= -1;
	target->SegNumActual				= -1;
	target->SegNumEqual					= 0;
	target->SegPreferredHostCount 		= source->SegPreferredHostCount;
	target->SegPreferredHostNames 		= NULL;
	target->SegPreferredScanSizeMB 		= NULL;
	target->SliceSize					= source->SliceSize;
	target->IOBytes						= source->IOBytes;
	target->MaxSegCountFixed			= source->MaxSegCountFixed;
	target->MinSegCountFixed			= source->MinSegCountFixed;
	target->VSegLimitPerSeg				= source->VSegLimitPerSeg;
	target->VSegLimit					= source->VSegLimit;
	target->StatVSegMemoryMB			= source->StatVSegMemoryMB;
	target->StatNVSeg					= source->StatNVSeg;
	target->Resource					= NULL;

	target->QueueTrack		 			= source->QueueTrack;
	target->User				 		= source->User;

	target->isOld						= false;

	target->troubledByFragment		= false;
	target->troubledByFragmentTimestamp = 0;

	target->CommBuffer				= NULL;

	/*
	 * We have copied the resource request content, so we can call this API to
	 * build up preferred host list.
	 */
	buildSegPreferredHostInfo(target);
}

void copyResourceQuotaConnectionTrack(ConnectionTrack source,
									  ConnectionTrack target)
{
	target->SegMemoryMB		= source->SegMemoryMB;
	target->SegCore			= source->SegCore;
	target->SegIOBytes		= source->SegIOBytes;
	target->SegNum			= source->SegNum;
	target->SegNumMin		= source->SegNumMin;
	target->SegNumEqual		= source->SegNumEqual;
}

void removeResourceRequestInConnHavingReqeusts(int32_t connid)
{
	removeResourceRequestInConnHavingRequestsInternal(connid,
													  &(PCONTRACK->ConnHavingRequests));
	removeResourceRequestInConnHavingRequestsInternal(connid,
													  &(PCONTRACK->ConnToRetry));
}

void removeResourceRequestInConnHavingRequestsInternal(int32_t 	 connid,
													   List    **requests)
{
	ConnectionTrack  ct		  = NULL;
	ListCell		*cell	  = NULL;
	ListCell		*prevcell = NULL;

	foreach(cell, (*requests))
	{
		ct = (ConnectionTrack)lfirst(cell);
		elog(LOG, "Request id %d", ct->MessageID);
		if ( ct->MessageID == REQUEST_QD_ACQUIRE_RESOURCE )
		{
			/* Try to get the connection id in this reqeust. */
			RPCRequestHeadAcquireResourceFromRM request =
				SMBUFF_HEAD(RPCRequestHeadAcquireResourceFromRM,
							&(ct->MessageBuff));
			if ( request->ConnID == connid )
			{
				elog(WARNING, "Resource manager finds ConnID %d in request pending "
							  "list should be cancelled.",
							  request->ConnID);
				if ( ct->CommBuffer!= NULL )
				{
					forceCloseFileDesc(ct->CommBuffer);
				}
				MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
				(*requests) = list_delete_cell((*requests), cell, prevcell);
				MEMORY_CONTEXT_SWITCH_BACK
				break;
			}
		}
		prevcell = cell;
	}
}
