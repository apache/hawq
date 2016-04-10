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
#include "envswitch.h"
#include "utils/linkedlist.h"
#include "utils/memutilities.h"
#include "utils/network_utils.h"
#include "utils/kvproperties.h"
#include "communication/rmcomm_MessageHandler.h"
#include "communication/rmcomm_QD2RM.h"
#include "communication/rmcomm_QD_RM_Protocol.h"
#include "communication/rmcomm_RMSEG_RM_Protocol.h"
#include "communication/rmcomm_RM2RMSEG.h"
#include "resourceenforcer/resourceenforcer.h"

#include "resourcemanager.h"

/*
 * The MAIN ENTRY of request handler.
 * The implementation of all request handlers are :
 * 		requesthandler.c 		: handlers for QD-RM resource negotiation RPC
 * 		requesthandler_ddl.c	: handlers for QD-RM DDL manipulations
 * 		requesthandler_RMSEG.c	: handlers for QE-RMSEG, RM-RMSEG RPC
 */

#define RETRIEVE_CONNTRACK(res, connid, conntrack)                      	   \
	if ( (*conntrack)->ConnID == INVALID_CONNID ) {                            \
		res = retrieveConnectionTrack((*conntrack), connid);				   \
		if ( res != FUNC_RETURN_OK ) {                                         \
			elog(LOG, "Not valid resource context with id %d.", connid);  	   \
			goto sendresponse;                                                 \
		}                                                                      \
		elog(DEBUG5, "HAWQ RM :: Fetched existing connection track "           \
					 "ID=%d, Progress=%d.",                                    \
					 (*conntrack)->ConnID,                                     \
					 (*conntrack)->Progress);                                  \
	}

/*
 * Handle the request of REGISTER CONNECTION.
 */
bool handleRMRequestConnectionReg(void **arg)
{
	static char 			errorbuf[ERRORMESSAGE_SIZE];
	int						res			= FUNC_RETURN_OK;
	ConnectionTrack 		conntrack	= (ConnectionTrack )(*arg);
	SelfMaintainBufferData 	responsedata;
	RPCResponseRegisterConnectionInRMByStrData response;

	initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

	/* Parse request. */
	strncpy(conntrack->UserID,
			SMBUFF_CONTENT(&(conntrack->MessageBuff)),
			sizeof(conntrack->UserID)-1);
	conntrack->UserID[sizeof(conntrack->UserID)-1] = '\0';

	/* Handle the request. */
	res = registerConnectionByUserID(conntrack, errorbuf, sizeof(errorbuf));
	if ( res == FUNC_RETURN_OK )
	{
		/* Allocate connection id and track this connection. */
		res = useConnectionID(&(conntrack->ConnID));
		if ( res == FUNC_RETURN_OK )
		{
			trackConnectionTrack(conntrack);
			elog(LOG, "ConnID %d. Resource manager tracked connection.",
					  conntrack->ConnID);
			response.Result = FUNC_RETURN_OK;
			response.ConnID = conntrack->ConnID;
		}
		else
		{
			Assert( res == CONNTRACK_CONNID_FULL );
			snprintf(errorbuf, sizeof(errorbuf),
					 "cannot accept more resource context instance");
			elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
			/* No connection id resource. Return occupation in resource queue. */
			returnConnectionToQueue(conntrack, false);
			response.Result = res;
			response.ConnID = INVALID_CONNID;
		}
	}
	else
	{
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		response.Result = res;
		response.ConnID = INVALID_CONNID;
	}

	/* Build response. */
	appendSMBVar(&responsedata, response);
	if ( response.Result != FUNC_RETURN_OK )
	{
		appendSMBStr(&responsedata, errorbuf);
		appendSelfMaintainBufferTill64bitAligned(&responsedata);
	}

	buildResponseIntoConnTrack(conntrack,
				   	   	   	   SMBUFF_CONTENT(&responsedata),
							   getSMBContentSize(&responsedata),
							   conntrack->MessageMark1,
							   conntrack->MessageMark2,
							   RESPONSE_QD_CONNECTION_REG);

	destroySelfMaintainBuffer(&responsedata);
	elog(DEBUG3, "ConnID %d. One connection register result %d.",
				 conntrack->ConnID,
				 response.Result);

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

/**
 * Handle the request of REGISTER CONNECTION.
 */
bool handleRMRequestConnectionRegByOID(void **arg)
{
	static char 		   errorbuf[ERRORMESSAGE_SIZE];
	int		   			   res			= FUNC_RETURN_OK;
	ConnectionTrack 	   conntrack	= (ConnectionTrack )(*arg);
	bool				   exist		= false;
	SelfMaintainBufferData responsedata;
	RPCResponseRegisterConnectionInRMByOIDData response;

	initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

	RPCRequestRegisterConnectionInRMByOID request =
		SMBUFF_HEAD(RPCRequestRegisterConnectionInRMByOID,
					&(conntrack->MessageBuff));

	/* Get user name from oid. */
	UserInfo reguser = getUserByUserOID(request->UseridOid, &exist);
	if ( !exist )
	{
		res = RESQUEMGR_NO_USERID;
		snprintf(errorbuf, sizeof(errorbuf),
				 "user oid " INT64_FORMAT "does not exist",
				 request->UseridOid);
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		goto exit;
	}
	else
	{
		/* Set user id string into connection track. */
		strncpy(conntrack->UserID, reguser->Name, sizeof(conntrack->UserID)-1);
	}

	/* Handle the request. */
	res = registerConnectionByUserID(conntrack, errorbuf, sizeof(errorbuf));

	if ( res == FUNC_RETURN_OK )
	{
		/* Allocate connection id and track this connection. */
		res = useConnectionID(&(conntrack->ConnID));
		if ( res == FUNC_RETURN_OK )
		{
			trackConnectionTrack(conntrack);
			elog(RMLOG, "ConnID %d. Resource manager tracked connection.",
					  	conntrack->ConnID);
			response.Result = FUNC_RETURN_OK;
			response.ConnID = conntrack->ConnID;
		}
		else {
			Assert( res == CONNTRACK_CONNID_FULL );
			snprintf(errorbuf, sizeof(errorbuf),
					 "cannot accept more resource context instance");
			elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
			/* No connection id resource. Return occupation in resource queue. */
			returnConnectionToQueue(conntrack, false);
			response.Result = res;
			response.ConnID = INVALID_CONNID;
		}
	}
	else {
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		response.Result = res;
		response.ConnID = INVALID_CONNID;
	}

exit:
	/* Build message saved in the connection track instance. */
	appendSMBVar(&responsedata, response);
	if ( response.Result != FUNC_RETURN_OK )
	{
		appendSMBStr(&responsedata, errorbuf);
		appendSelfMaintainBufferTill64bitAligned(&responsedata);
	}

	buildResponseIntoConnTrack(conntrack,
				   	   	   	   SMBUFF_CONTENT(&responsedata),
							   getSMBContentSize(&responsedata),
							   conntrack->MessageMark1,
							   conntrack->MessageMark2,
							   RESPONSE_QD_CONNECTION_REG_OID);
	destroySelfMaintainBuffer(&responsedata);
	elog(DEBUG3, "ConnID %d. One connection register result %d (OID).",
				 conntrack->ConnID,
				 response.Result);

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

/*
 * Handle UNREGISTER request.
 */
bool handleRMRequestConnectionUnReg(void **arg)
{
	static char 	 errorbuf[ERRORMESSAGE_SIZE];
	int      		 res	 	= FUNC_RETURN_OK;
	ConnectionTrack *conntrack 	= (ConnectionTrack *)arg;

	RPCRequestHeadUnregisterConnectionInRM request =
		SMBUFF_HEAD(RPCRequestHeadUnregisterConnectionInRM,
					&((*conntrack)->MessageBuff));

	elog(DEBUG3, "ConnID %d. Try to unregister.", request->ConnID);

	res = retrieveConnectionTrack((*conntrack), request->ConnID);
	if ( res != FUNC_RETURN_OK )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "the resource context is invalid or timed out");
		elog(WARNING, "ConnID %d. %s", request->ConnID, errorbuf);
		goto sendresponse;
	}
	elog(DEBUG3, "ConnID %d. Fetched existing connection track, progress=%d.",
				 (*conntrack)->ConnID,
				 (*conntrack)->Progress);

	/* Get connection ID. */
	request = SMBUFF_HEAD(RPCRequestHeadUnregisterConnectionInRM,
			  	  	  	  &((*conntrack)->MessageBuff));

	/*
	 * If this connection is waiting for resource allocated, cancel the request
	 * from resource queue.
	 */
	if ( (*conntrack)->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT )
	{
		cancelResourceAllocRequest((*conntrack), errorbuf, false);
		transformConnectionTrackProgress(conntrack, CONN_PP_REGISTER_DONE);
	}
	/* If this connection has resource allocated, return the resource. */
	else if ( (*conntrack)->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_DONE )
	{
		returnResourceToResQueMgr((*conntrack));
	}
	/*
	 * If this connection has acquire resource not processed yet, we should
	 * remove that now. In this case, this connection should have registered.
	 */
	else if ( (*conntrack)->Progress == CONN_PP_REGISTER_DONE )
	{
		elog(WARNING, "Resource manager finds possible not handled resource "
					  "request from ConnID %d.",
					  request->ConnID);
		removeResourceRequestInConnHavingReqeusts(request->ConnID);
	}
	else if ( !canTransformConnectionTrackProgress((*conntrack), CONN_PP_ESTABLISHED) )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "wrong resource context status for unregistering, %d",
				 (*conntrack)->Progress);

		elog(WARNING, "ConnID %d. %s", request->ConnID, errorbuf);
		res = REQUESTHANDLER_WRONG_CONNSTAT;
		goto sendresponse;
	}

	returnConnectionToQueue(*conntrack, false);

	elog(LOG, "ConnID %d. Connection is unregistered.", (*conntrack)->ConnID);

sendresponse:
	{
		SelfMaintainBufferData responsedata;
		initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

		RPCResponseUnregisterConnectionInRMData response;
		response.Result   = res;
		response.Reserved = 0;
		appendSMBVar(&responsedata, response);

		if ( response.Result != FUNC_RETURN_OK )
		{
			appendSMBStr(&responsedata, errorbuf);
			appendSelfMaintainBufferTill64bitAligned(&responsedata);
		}

		/* Build message saved in the connection track instance. */
		buildResponseIntoConnTrack((*conntrack),
							 	   SMBUFF_CONTENT(&responsedata),
								   getSMBContentSize(&responsedata),
								   (*conntrack)->MessageMark1,
								   (*conntrack)->MessageMark2,
								   RESPONSE_QD_CONNECTION_UNREG);
		destroySelfMaintainBuffer(&responsedata);

		if ( res == CONNTRACK_NO_CONNID )
		{
			transformConnectionTrackProgress((*conntrack),
											 CONN_PP_TRANSFORM_ERROR);
		}

		(*conntrack)->ResponseSent = false;
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, *conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
	}

	return true;
}

/**
 * Handle the request of ACQUIRE QUERY RESOURCE
 **/
bool handleRMRequestAcquireResource(void **arg)
{
	static char		 errorbuf[ERRORMESSAGE_SIZE];
	int				 res		= FUNC_RETURN_OK;
	ConnectionTrack *conntrack	= (ConnectionTrack *)arg;
	uint64_t		 reqtime	= gettime_microsec();

	/* If we run in YARN mode, we expect that we should try to get at least one
	 * available segment, and this requires at least once global resource manager
	 * cluster report returned.
	 */
	if ( reqtime - DRMGlobalInstance->ResourceManagerStartTime <=
		 rm_nocluster_timeout * 1000000LL &&
		 PRESPOOL->RBClusterReportCounter == 0 )
	{
		elog(DEBUG3, "Resource manager defers the resource request.");
		return false;
	}

	/*
	 * If resource queue has no concrete capacity set yet, no need to handle
	 * the request.
	 */
	if ( PQUEMGR->RootTrack->QueueInfo->ClusterMemoryMB <= 0 )
	{
		elog(DEBUG3, "Resource manager defers the resource request because the "
					 "resource queues have no valid resource capacities yet.");
		return false;
	}

	RPCRequestHeadAcquireResourceFromRM request =
		SMBUFF_HEAD(RPCRequestHeadAcquireResourceFromRM,
					&((*conntrack)->MessageBuff));

	elog(DEBUG3, "ConnID %d. Acquires query resource. Session id "INT64_FORMAT,
				 request->ConnID,
				 request->SessionID);

	res = retrieveConnectionTrack((*conntrack), request->ConnID);
	if ( res != FUNC_RETURN_OK )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "the resource context may be timed out");
		elog(WARNING, "ConnID %d. %s", request->ConnID, errorbuf);
		goto sendresponse;
	}
	elog(DEBUG3, "ConnID %d. Fetched existing connection track, progress=%d.",
				 (*conntrack)->ConnID,
				 (*conntrack)->Progress);

	request = SMBUFF_HEAD(RPCRequestHeadAcquireResourceFromRM,
						  &((*conntrack)->MessageBuff));
	if ( (*conntrack)->Progress != CONN_PP_REGISTER_DONE )
	{
		res = REQUESTHANDLER_WRONG_CONNSTAT;
		snprintf(errorbuf, sizeof(errorbuf),
				 "the resource context status is invalid");
		elog(WARNING, "ConnID %d. %s", (*conntrack)->ConnID, errorbuf);
		goto sendresponse;
	}

	/*--------------------------------------------------------------------------
	 * We firstly check if the cluster has too many unavailable segments, which
	 * is measured by rm_rejectrequest_nseg_limit. The expected cluster size is
	 * loaded from counting hosts in $GPHOME/etc/slaves. Resource manager rejects
	 * query  resource requests at once if currently there are too many segments
	 * unavailable.
	 *--------------------------------------------------------------------------
	 */
	Assert(PRESPOOL->SlavesHostCount > 0);
	int rejectlimit = ceil(PRESPOOL->SlavesHostCount * rm_rejectrequest_nseg_limit);
	int unavailcount = PRESPOOL->SlavesHostCount - PRESPOOL->AvailNodeCount;
	if ( unavailcount > rejectlimit )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "%d of %d segments %s unavailable, exceeds %.1f%% defined in GUC hawq_rm_rejectrequest_nseg_limit. "
				 "The allocation request is rejected.",
				 unavailcount,
				 PRESPOOL->SlavesHostCount,
				 unavailcount == 1 ? "is" : "are",
				 rm_rejectrequest_nseg_limit*100.0);
		elog(WARNING, "ConnID %d. %s", (*conntrack)->ConnID, errorbuf);
		res = RESOURCEPOOL_TOO_MANY_UAVAILABLE_HOST;
		goto sendresponse;
	}

	/* Get scan size. */
	request = SMBUFF_HEAD(RPCRequestHeadAcquireResourceFromRM,
			  	  	  	  &((*conntrack)->MessageBuff));

	(*conntrack)->SliceSize				= request->SliceSize;
	(*conntrack)->IOBytes				= request->IOBytes;
	(*conntrack)->SegPreferredHostCount = request->NodeCount;
	(*conntrack)->MaxSegCountFixed      = request->MaxSegCountFix;
	(*conntrack)->MinSegCountFixed      = request->MinSegCountFix;
	(*conntrack)->SessionID				= request->SessionID;
	(*conntrack)->VSegLimitPerSeg		= request->VSegLimitPerSeg;
	(*conntrack)->VSegLimit				= request->VSegLimit;
	(*conntrack)->StatVSegMemoryMB		= request->StatVSegMemoryMB;
	(*conntrack)->StatNVSeg				= request->StatNVSeg;

	/* Get preferred nodes. */
	buildSegPreferredHostInfo((*conntrack));

	elog(RMLOG, "ConnID %d. Session ID " INT64_FORMAT " "
				"Scanning "INT64_FORMAT" io bytes "
				"by %d slices with %d preferred segments. "
				"Expect %d vseg (MIN %d). "
				"Each segment has maximum %d vseg. "
				"Query has maximum %d vseg. "
				"Statement quota %d MB x %d vseg",
				(*conntrack)->ConnID,
				(*conntrack)->SessionID,
				(*conntrack)->IOBytes,
				(*conntrack)->SliceSize,
				(*conntrack)->SegPreferredHostCount,
				(*conntrack)->MaxSegCountFixed,
				(*conntrack)->MinSegCountFixed,
				(*conntrack)->VSegLimitPerSeg,
				(*conntrack)->VSegLimit,
				(*conntrack)->StatVSegMemoryMB,
				(*conntrack)->StatNVSeg);

	if ( (*conntrack)->StatNVSeg > 0 )
	{
		elog(LOG, "ConnID %d. Statement level resource quota is active. "
				  "Expect resource ( %d MB ) x %d.",
				  (*conntrack)->ConnID,
				  (*conntrack)->StatVSegMemoryMB,
				  (*conntrack)->StatNVSeg);
	}

	res = acquireResourceFromResQueMgr((*conntrack), errorbuf, sizeof(errorbuf));
	if ( res != FUNC_RETURN_OK )
	{
		goto sendresponse;
	}
	(*conntrack)->ResRequestTime = reqtime;
	(*conntrack)->LastActTime    = (*conntrack)->ResRequestTime;

	return true;

sendresponse:
	{
		/* Send error message. */
		RPCResponseAcquireResourceFromRMERRORData errresponse;
		errresponse.Result   = res;
		errresponse.Reserved = 0;

		SelfMaintainBufferData responsedata;
		initializeSelfMaintainBuffer(&responsedata, PCONTEXT);
		appendSMBVar(&responsedata, errresponse);
		appendSMBStr(&responsedata, errorbuf);
		appendSelfMaintainBufferTill64bitAligned(&responsedata);

		buildResponseIntoConnTrack((*conntrack),
								   SMBUFF_CONTENT(&responsedata),
								   getSMBContentSize(&responsedata),
								   (*conntrack)->MessageMark1,
								   (*conntrack)->MessageMark2,
								   RESPONSE_QD_ACQUIRE_RESOURCE);
		destroySelfMaintainBuffer(&responsedata);

		transformConnectionTrackProgress((*conntrack), CONN_PP_TRANSFORM_ERROR);

		(*conntrack)->ResponseSent = false;
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, *conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
	}
	return true;
}


/**
 * Handle RETURN RESOURCE request.
 *
 */
bool handleRMRequestReturnResource(void **arg)
{
	static char		errorbuf[ERRORMESSAGE_SIZE];
	int      		res		    = FUNC_RETURN_OK;
	ConnectionTrack *conntrack  = (ConnectionTrack *)arg;

	RPCRequestHeadReturnResource request =
		SMBUFF_HEAD(RPCRequestHeadReturnResource, &((*conntrack)->MessageBuff));

	elog(DEBUG3, "ConnID %d. Returns query resource.", request->ConnID);

	res = retrieveConnectionTrack((*conntrack), request->ConnID);
	if ( res != FUNC_RETURN_OK )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "the resource context may be timed out");
		elog(WARNING, "ConnID %d. %s", request->ConnID, errorbuf);
		goto sendresponse;
	}
	elog(DEBUG3, "ConnID %d. Fetched existing connection track, progress=%d.",
				 (*conntrack)->ConnID,
				 (*conntrack)->Progress);

	/* Get connection ID. */
	request = SMBUFF_HEAD(RPCRequestHeadReturnResource,
						  &((*conntrack)->MessageBuff));

	if ( (*conntrack)->Progress != CONN_PP_RESOURCE_QUEUE_ALLOC_DONE )
	{
		res = REQUESTHANDLER_WRONG_CONNSTAT;
		snprintf(errorbuf, sizeof(errorbuf),
				 "the resource context status is invalid");
		elog(WARNING, "ConnID %d. %s", (*conntrack)->ConnID, errorbuf);
		goto sendresponse;
	}

	/* Return the resource. */
	returnResourceToResQueMgr(*conntrack);

	elog(LOG, "ConnID %d. Returned resource.", (*conntrack)->ConnID);

sendresponse:
	{
		SelfMaintainBufferData responsedata;
		initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

		RPCResponseHeadReturnResourceData response;
		response.Result   = res;
		response.Reserved = 0;
		appendSMBVar(&responsedata, response);

		if ( response.Result != FUNC_RETURN_OK )
		{
			appendSMBStr(&responsedata, errorbuf);
			appendSelfMaintainBufferTill64bitAligned(&responsedata);
		}

		buildResponseIntoConnTrack((*conntrack),
	                               SMBUFF_CONTENT(&responsedata),
								   getSMBContentSize(&responsedata),
								   (*conntrack)->MessageMark1,
								   (*conntrack)->MessageMark2,
								   RESPONSE_QD_RETURN_RESOURCE );
		destroySelfMaintainBuffer(&responsedata);
		if ( res == CONNTRACK_NO_CONNID )
		{
			transformConnectionTrackProgress((*conntrack), CONN_PP_TRANSFORM_ERROR);
		}

		(*conntrack)->ResponseSent = false;
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, *conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
	}
	return true;
}

/*
 * Handle I AM ALIVE request.
 */
bool handleRMSEGRequestIMAlive(void **arg)
{
	ConnectionTrack conntrack = (ConnectionTrack)(*arg);
	elog(RMLOG, "Resource manager receives segment heart-beat information.");

	SelfMaintainBufferData machinereport;
	initializeSelfMaintainBuffer(&machinereport,PCONTEXT);
	SegStat segstat = (SegStat)(SMBUFF_CONTENT(&(conntrack->MessageBuff)) +
								sizeof(RPCRequestHeadIMAliveData));

	generateSegStatReport(segstat, &machinereport);

	elog(RMLOG, "Resource manager received segment machine information, %s",
				SMBUFF_CONTENT(&machinereport));
	destroySelfMaintainBuffer(&machinereport);

	/* Get hostname and ip address from the connection's sockaddr */
	char*    		fts_client_ip     = NULL;
	uint32_t 		fts_client_ip_len = 0;
	struct hostent* fts_client_host   = NULL;
	struct in_addr 	fts_client_addr;

	Assert(conntrack->CommBuffer != NULL);
	fts_client_ip = conntrack->CommBuffer->ClientAddrDotStr;
	fts_client_ip_len = strlen(fts_client_ip);
	inet_aton(fts_client_ip, &fts_client_addr);
	fts_client_host = gethostbyaddr(&fts_client_addr, 4, AF_INET);
	Assert(fts_client_host != NULL);

	/* Get the received machine id instance start address. */
	SegInfo fts_client_seginfo = (SegInfo)(SMBUFF_CONTENT(&(conntrack->MessageBuff)) +
										   sizeof(RPCRequestHeadIMAliveData) +
										   offsetof(SegStatData, Info));

	/* Build new machine id with inserted ip address and modified host name. */
	SelfMaintainBufferData newseginfo;
	initializeSelfMaintainBuffer(&newseginfo, PCONTEXT);

	/* Copy machine id header. */
	prepareSelfMaintainBuffer(&newseginfo, sizeof(SegInfoData), false);
	memcpy(SMBUFF_CONTENT(&(newseginfo)),
		   fts_client_seginfo, sizeof(SegInfoData));
	jumpforwardSelfMaintainBuffer(&newseginfo, sizeof(SegInfoData));

	/* Put client ip address's offset and attribute */
	uint16_t addroffset = sizeof(SegInfoData) +
						  sizeof(uint32_t) *
						  (((fts_client_seginfo->HostAddrCount + 1 + 1) >> 1) << 1);

	uint16_t addrattr = HOST_ADDRESS_CONTENT_STRING;

	SegInfo newseginfoptr = SMBUFF_HEAD(SegInfo, &newseginfo);
	newseginfoptr->AddressAttributeOffset = sizeof(SegInfoData);
	newseginfoptr->AddressContentOffset   = addroffset;
	newseginfoptr->HostAddrCount 		  = fts_client_seginfo->HostAddrCount + 1;

	uint32_t addContentOffset= addroffset;

	appendSMBVar(&newseginfo,addroffset);
	appendSMBVar(&newseginfo,addrattr);

	elog(RMLOG, "Resource manager received IMAlive message, this segment's IP "
				"address count: %d",
				fts_client_seginfo->HostAddrCount);

	/* iterate all the offset/attribute in machineIdData from client */
	for (int i = 0; i < fts_client_seginfo->HostAddrCount; i++) {

		/* Read old address offset data. */
		addroffset = *(uint16_t *)((char *)fts_client_seginfo +
								   fts_client_seginfo->AddressAttributeOffset +
								   i*sizeof(uint32_t));
		/*
		 * Adjust address offset by counting the size of one AddressString.
		 * Adding new address attribute and offset content can also causing more
		 * space enlarged. We have to count it.
		 */
		addroffset += __SIZE_ALIGN64(sizeof(uint32_t) +
									 fts_client_ip_len + 1) +
					  (addContentOffset - fts_client_seginfo->AddressContentOffset);

		appendSMBVar(&newseginfo,addroffset);

		/* Read old address attribute data. */
		addrattr = *(uint16_t *)((char *)fts_client_seginfo +
								 fts_client_seginfo->AddressAttributeOffset +
								 i*sizeof(uint32_t) +
								 sizeof(uint16_t));
		/* No need to adjust the value. */
		appendSMBVar(&newseginfo,addrattr);
	}

	/* We may have to add '\0' pads to make the block of address offset and
	 * attribute 64-bit aligned. */
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	/* Put the connection's client ip into the first position */
	appendSMBVar(&newseginfo,fts_client_ip_len);
	appendSMBStr(&newseginfo,fts_client_ip);
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	elog(RMLOG, "Resource manager received IMAlive message, "
				"this segment's IP address: %s\n",
				fts_client_ip);

	/* Put other ip addresses' content directly. */
	appendSelfMaintainBuffer(&newseginfo,
							 (char *)fts_client_seginfo +
							 	 	 fts_client_seginfo->AddressContentOffset,
							 fts_client_seginfo->HostNameOffset -
							 fts_client_seginfo->AddressContentOffset);

	/* fill in hostname */
	newseginfoptr = SMBUFF_HEAD(SegInfo, &(newseginfo));
	newseginfoptr->HostNameOffset = getSMBContentSize(&newseginfo);
	appendSMBStr(&newseginfo,fts_client_host->h_name);
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	newseginfoptr = SMBUFF_HEAD(SegInfo, &(newseginfo));
	newseginfoptr->HostNameLen = strlen(fts_client_host->h_name);
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	/* fill in failed temporary directory string */
	if (fts_client_seginfo->FailedTmpDirLen != 0)
	{
		newseginfoptr->FailedTmpDirOffset = getSMBContentSize(&newseginfo);
		newseginfoptr->FailedTmpDirLen = strlen(GET_SEGINFO_FAILEDTMPDIR(fts_client_seginfo));
		appendSMBStr(&newseginfo, GET_SEGINFO_FAILEDTMPDIR(fts_client_seginfo));
		elog(RMLOG, "Resource manager received IMAlive message, "
					"failed temporary directory:%s",
					GET_SEGINFO_FAILEDTMPDIR(fts_client_seginfo));
		appendSelfMaintainBufferTill64bitAligned(&newseginfo);
	}
	else
	{
		newseginfoptr->FailedTmpDirOffset = 0;
		newseginfoptr->FailedTmpDirLen = 0;
	}

	newseginfoptr->Size      = getSMBContentSize(&newseginfo);
	/* reported by segment, set GRM host/rack as NULL */
	newseginfoptr->GRMHostNameLen = 0;
	newseginfoptr->GRMHostNameOffset = 0;
	newseginfoptr->GRMRackNameLen = 0;
	newseginfoptr->GRMRackNameOffset = 0;

	elog(RMLOG, "Resource manager received IMAlive message, "
				"this segment's hostname: %s\n",
				fts_client_host->h_name);

	/* build segment status information instance and add to resource pool */
	SegStat newsegstat = (SegStat)
						 rm_palloc0(PCONTEXT,
								  	offsetof(SegStatData, Info) +
									getSMBContentSize(&newseginfo));
	/* Copy old segment status information. */
	memcpy((char *)newsegstat, segstat, offsetof(SegStatData, Info));
	/* Copy new segment info information. */
	memcpy((char *)newsegstat + offsetof(SegStatData, Info),
		   SMBUFF_CONTENT(&newseginfo),
		   getSMBContentSize(&newseginfo));

	destroySelfMaintainBuffer(&newseginfo);

	newsegstat->ID 				 = SEGSTAT_ID_INVALID;

	RPCRequestHeadIMAlive header = SMBUFF_HEAD(RPCRequestHeadIMAlive,
												&(conntrack->MessageBuff));
	newsegstat->FailedTmpDirNum  = header->TmpDirBrokenCount;
	newsegstat->RMStartTimestamp = header->RMStartTimestamp;
	newsegstat->StatusDesc = 0;
	newsegstat->Reserved   = 0;

	bool capstatchanged = false;
	if ( addHAWQSegWithSegStat(newsegstat, &capstatchanged) != FUNC_RETURN_OK )
	{
		/* Should be a duplicate host. */
		rm_pfree(PCONTEXT, newsegstat);
	}

	if ( capstatchanged )
	{
		/* Refresh resource queue capacities. */
		refreshResourceQueueCapacity(false);
		refreshActualMinGRMContainerPerSeg();
		/* Recalculate all memory/core ratio instances' limits. */
		refreshMemoryCoreRatioLimits();
		/* Refresh memory/core ratio level water mark. */
		refreshMemoryCoreRatioWaterMark();
	}

	/* Send the response. */
	RPCResponseIMAliveData response;
	response.Result   = FUNC_RETURN_OK;
	response.Reserved = 0;
	buildResponseIntoConnTrack(conntrack,
						 	   (char *)&response,
							   sizeof(response),
							   conntrack->MessageMark1,
							   conntrack->MessageMark2,
							   RESPONSE_RM_IMALIVE);

	elog(DEBUG3, "Resource manager accepted segment machine.");

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

bool handleRMRequestAcquireResourceQuota(void **arg)
{
	static char		 errorbuf[ERRORMESSAGE_SIZE];
	int      		 res		= FUNC_RETURN_OK;
	ConnectionTrack  conntrack  = (ConnectionTrack)(*arg);
	bool			 exist		= false;
	uint64_t		 reqtime	= gettime_microsec();
	/* If we run in YARN mode, we expect that we should try to get at least one
	 * available segment, and this requires at least once global resource manager
	 * cluster report returned.
	 */
	if ( reqtime - DRMGlobalInstance->ResourceManagerStartTime <=
		 rm_nocluster_timeout * 1000000LL &&
		 PRESPOOL->RBClusterReportCounter == 0 )
	{
		elog(DEBUG3, "Resource manager defers the resource request.");
		return false;
	}

	/*
	 * If resource queue has no concrete capacity set yet, no need to handle
	 * the request.
	 */
	if ( PQUEMGR->RootTrack->QueueInfo->ClusterMemoryMB <= 0 )
	{
		elog(DEBUG3, "Resource manager defers the resource request because the "
					 "resource queues have no valid resource capacities yet.");
		return false;
	}

	Assert(PRESPOOL->SlavesHostCount > 0);
	int rejectlimit = ceil(PRESPOOL->SlavesHostCount * rm_rejectrequest_nseg_limit);
	int unavailcount = PRESPOOL->SlavesHostCount - PRESPOOL->AvailNodeCount;
	if ( unavailcount > rejectlimit )
	{
		snprintf(errorbuf, sizeof(errorbuf),
				 "%d of %d segments %s unavailable, exceeds %.1f%% defined in "
				 "GUC hawq_rm_rejectrequest_nseg_limit. The resource quota "
				 "request is rejected.",
				 unavailcount,
				 PRESPOOL->SlavesHostCount,
				 unavailcount == 1 ? "is" : "are",
				 rm_rejectrequest_nseg_limit*100.0);
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		res = RESOURCEPOOL_TOO_MANY_UAVAILABLE_HOST;
		goto errorexit;
	}

	RPCRequestHeadAcquireResourceQuotaFromRMByOID request =
		SMBUFF_HEAD(RPCRequestHeadAcquireResourceQuotaFromRMByOID,
					&(conntrack->MessageBuff));

	/* Get user name from oid. */
	UserInfo reguser = getUserByUserOID(request->UseridOid, &exist);
	if ( !exist )
	{
		res = RESQUEMGR_NO_USERID;
		snprintf(errorbuf, sizeof(errorbuf),
				 "user oid " INT64_FORMAT "does not exist",
				 request->UseridOid);
		elog(WARNING, "ConnID %d. %s", conntrack->ConnID, errorbuf);
		goto errorexit;
	}
	else
	{
		/* Set user id string into connection track. */
		strncpy(conntrack->UserID, reguser->Name, sizeof(conntrack->UserID)-1);
	}

	conntrack->MaxSegCountFixed = request->MaxSegCountFix;
	conntrack->MinSegCountFixed = request->MinSegCountFix;
	conntrack->VSegLimitPerSeg	= request->VSegLimitPerSeg;
	conntrack->VSegLimit		= request->VSegLimit;
	conntrack->StatVSegMemoryMB	= request->StatVSegMemoryMB;
	conntrack->StatNVSeg		= request->StatNVSeg;

	elog(RMLOG, "ConnID %d. User "INT64_FORMAT" acquires query resource quota. "
			  	"Expect %d vseg (MIN %d). "
				"Each segment has maximum %d vseg. "
				"Query has maximum %d vseg. "
				"Statement quota %d MB x %d vseg",
				conntrack->ConnID,
				request->UseridOid,
				request->MaxSegCountFix,
				request->MinSegCountFix,
				request->VSegLimitPerSeg,
				request->VSegLimit,
				request->StatVSegMemoryMB,
				request->StatNVSeg);

	if ( conntrack->StatNVSeg > 0 )
	{
		elog(LOG, "ConnID %d. Statement level resource quota is active. "
				  "Expect resource ( %d MB ) x %d.",
				  conntrack->ConnID,
				  conntrack->StatVSegMemoryMB,
				  conntrack->StatNVSeg);
	}

	res = acquireResourceQuotaFromResQueMgr(conntrack, errorbuf, sizeof(errorbuf));

	if ( res == FUNC_RETURN_OK )
	{
		RPCResponseHeadAcquireResourceQuotaFromRMByOIDData response;

		DynResourceQueueTrack queuetrack = getQueueTrackByQueueOID(reguser->QueueOID);
		if ( queuetrack != NULL )
		{
			memcpy(response.QueueName,
				   queuetrack->QueueInfo->Name,
				   sizeof(response.QueueName));
		}
		else {
			response.QueueName[0]='\0';
		}

		response.Reserved1   = 0;
		response.Result      = res;
		response.SegCore     = conntrack->SegCore;
		response.SegMemoryMB = conntrack->SegMemoryMB;
		response.SegNum	     = conntrack->SegNum;
		response.SegNumMin   = conntrack->SegNumMin;
		response.Reserved2   = 0;

		buildResponseIntoConnTrack( conntrack,
									(char *)&response,
									sizeof(response),
									conntrack->MessageMark1,
									conntrack->MessageMark2,
									RESPONSE_QD_ACQUIRE_RESOURCE_QUOTA);
		conntrack->ResponseSent = false;
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
		return true;
	}
	else
	{
		elog(WARNING, "%s", errorbuf);
	}
errorexit:
	{
		SelfMaintainBufferData responsedata;
		initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

		RPCResponseHeadAcquireResourceQuotaFromRMByOIDERRORData response;
		response.Result   = res;
		response.Reserved = 0;
		buildResponseIntoConnTrack(conntrack,
								   SMBUFF_CONTENT(&responsedata),
								   getSMBContentSize(&responsedata),
								   conntrack->MessageMark1,
								   conntrack->MessageMark2,
								   RESPONSE_QD_ACQUIRE_RESOURCE_QUOTA);
		destroySelfMaintainBuffer(&responsedata);
		conntrack->ResponseSent = false;
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
	}
	return true;
}

bool handleRMRequestRefreshResource(void **arg)
{
	int      		 res		= FUNC_RETURN_OK;
	ConnectionTrack  conntrack  = (ConnectionTrack)(*arg);
	ConnectionTrack  oldct		= NULL;
	uint64_t		 curmsec    = gettime_microsec();

	RPCRequestHeadRefreshResourceHeartBeat request =
		SMBUFF_HEAD(RPCRequestHeadRefreshResourceHeartBeat,
					&(conntrack->MessageBuff));

	uint32_t *connids = (uint32_t *)
						(SMBUFF_CONTENT(&(conntrack->MessageBuff)) +
						 sizeof(RPCRequestHeadRefreshResourceHeartBeatData));

	elog(DEBUG3, "Resource manager refreshes %d ConnIDs.", request->ConnIDCount);

	for ( int i = 0 ; i < request->ConnIDCount ; ++i )
	{
		/* Find connection track identified by ConnID */
		res = getInUseConnectionTrack(connids[i], &oldct);
		if ( res == FUNC_RETURN_OK )
		{
			oldct->LastActTime = curmsec;
			elog(RMLOG, "Refreshed resource context connection id %d", connids[i]);
		}
		else
		{
			elog(WARNING, "Can not find resource context connection id %d for "
						  "resource refreshing.",
					      connids[i]);
		}
	}

	/* Temporarily, we ignore the wrong conn id inputs. */
	RPCResponseRefreshResourceHeartBeatData response;
	response.Result   = FUNC_RETURN_OK;
	response.Reserved = 0;

	buildResponseIntoConnTrack( conntrack,
								(char *)&response,
								sizeof(response),
								conntrack->MessageMark1,
								conntrack->MessageMark2,
								RESPONSE_QD_REFRESH_RESOURCE);
	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK
	return true;
}

bool handleRMRequestSegmentIsDown(void **arg)
{
	int      		 res		 = FUNC_RETURN_OK;
	ConnectionTrack  conntrack   = (ConnectionTrack)(*arg);
	/* Get host name that is down. */
	char 			*hostname    = SMBUFF_CONTENT(&(conntrack->MessageBuff));
	int				 hostnamelen = 0;
	int32_t		 	 segid       = SEGSTAT_ID_INVALID;

	while( (hostname - SMBUFF_CONTENT(&(conntrack->MessageBuff)) <
			getSMBContentSize(&(conntrack->MessageBuff))) &&
			*hostname != '\0' )
	{
		hostnamelen = strlen(hostname);
		res = getSegIDByHostName(hostname, hostnamelen, &segid);
		if ( res == FUNC_RETURN_OK )
		{
			/* Get resource info of the expected host. */
			SegResource segres = getSegResource(segid);
			Assert( segres != NULL );

			if ( !IS_SEGSTAT_FTSAVAILABLE(segres->Stat) )
			{
				elog(WARNING, "Resource manager does not probe the status of "
							  "host %s because it is down already.",
							  hostname);
			}
			else if ( segres->RUAlivePending )
			{
				elog(LOG, "Resource manager does not probe the status of host %s "
						  "because it is in RUAlive pending status already.",
						  hostname);
			}
			else
			{
				elog(RMLOG, "Resource manager probes the status of host %s by "
							"sending RUAlive request.",
							hostname);

				res = sendRUAlive(hostname);
				/* IN THIS CASE, the segment is considered as down. */
				if (res != FUNC_RETURN_OK)
				{
					/*----------------------------------------------------------
					 * This call makes resource manager able to adjust queue and
					 * mem/core trackers' capacity.
					 *----------------------------------------------------------
					 */
					setSegResHAWQAvailability(segres,
											  RESOURCE_SEG_STATUS_UNAVAILABLE);

					/* Make resource pool remove unused containers */
					returnAllGRMResourceFromSegment(segres);
					/* Set the host down in gp_segment_configuration table */
					segres->Stat->StatusDesc |= SEG_STATUS_FAILED_PROBING_SEGMENT;
					if (Gp_role != GP_ROLE_UTILITY)
					{
						SimpStringPtr description = build_segment_status_description(segres->Stat);
						update_segment_status(segres->Stat->ID + REGISTRATION_ORDER_OFFSET,
											  SEGMENT_STATUS_DOWN,
											  (description->Len > 0)?description->Str:"");
						add_segment_history_row(segres->Stat->ID + REGISTRATION_ORDER_OFFSET,
												hostname,
												description->Str);

						freeSimpleStringContent(description);
						rm_pfree(PCONTEXT, description);
					}

					/* Set the host down. */
					elog(LOG, "Resource manager sets host %s from up to down "
							  "due to not reaching host.", hostname);
				}
				else
				{
					elog(RMLOG, "Resource manager triggered RUAlive request to "
								"host %s.",
								hostname);
				}
			}
		}
		else {
			elog(WARNING, "Resource manager cannot find host %s to check status, "
						  "skip it.",
						  hostname);
		}

		hostname = hostname + strlen(hostname) + 1; /* Try next */
	}

	refreshResourceQueueCapacity(false);
	refreshActualMinGRMContainerPerSeg();

	RPCResponseSegmentIsDownData response;
	response.Result   = res;
	response.Reserved = 0;
	buildResponseIntoConnTrack( conntrack,
								(char *)&response,
								sizeof(response),
								conntrack->MessageMark1,
								conntrack->MessageMark2,
								RESPONSE_QD_SEGMENT_ISDOWN);
	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

bool handleRMRequestDumpResQueueStatus(void **arg)
{
    ConnectionTrack conntrack   = (ConnectionTrack)(*arg);
    RPCResponseResQueueStatus   response = NULL;
    int responseLen = sizeof(RPCResponseResQueueStatusData) 
                    + sizeof(ResQueueStatusData) * (list_length(PQUEMGR->Queues) - 1);

    response = (RPCResponseResQueueStatus)palloc(responseLen);

    response->Result 	= FUNC_RETURN_OK;
    response->queuenum 	= list_length(PQUEMGR->Queues);

    int i = 0;
    ListCell *cell = NULL;
    foreach(cell, PQUEMGR->Queues)
    {
    	DynResourceQueueTrack quetrack = lfirst(cell);

        sprintf(response->queuedata[i].name, "%s", quetrack->QueueInfo->Name);
        response->queuedata[i].segmem     = quetrack->QueueInfo->SegResourceQuotaMemoryMB;
        response->queuedata[i].segcore    = quetrack->QueueInfo->SegResourceQuotaVCore;
        response->queuedata[i].segsize    = quetrack->ClusterSegNumber;
        response->queuedata[i].segsizemax = quetrack->ClusterSegNumberMax;
        response->queuedata[i].inusemem   = quetrack->TotalUsed.MemoryMB;
        response->queuedata[i].inusecore  = quetrack->TotalUsed.Core;
        response->queuedata[i].holders    = quetrack->NumOfRunningQueries;
        response->queuedata[i].waiters    = quetrack->QueryResRequests.NodeCount;

        /* Generate if resource queue paused dispatching resource. */
        if ( quetrack->troubledByFragment )
        {
        	response->queuedata[i].pausedispatch = 'R';
        }
        else if ( quetrack->pauseAllocation )
        {
        	response->queuedata[i].pausedispatch = 'T';
        }
        else
        {
        	response->queuedata[i].pausedispatch = 'F';
        }
        response->queuedata[i].reserved[0] = '\0';
        response->queuedata[i].reserved[1] = '\0';
        response->queuedata[i].reserved[2] = '\0';
        response->queuedata[i].reserved[3] = '\0';
        response->queuedata[i].reserved[4] = '\0';
        response->queuedata[i].reserved[5] = '\0';
        response->queuedata[i].reserved[6] = '\0';
        i++; 
    }
    
    buildResponseIntoConnTrack(conntrack,
                               (char *)response,
                               responseLen,
                               conntrack->MessageMark1,
                               conntrack->MessageMark2,
                               RESPONSE_QD_DUMP_RESQUEUE_STATUS);
    conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK
    return true;
}

bool handleRMRequestDumpStatus(void **arg)
{
	static char errorbuf[ERRORMESSAGE_SIZE];
    ConnectionTrack conntrack   = (ConnectionTrack)(*arg);
    RPCResponseDumpStatusData response;

    RPCRequestDumpStatus request = SMBUFF_HEAD(RPCRequestDumpStatus,
    										   &(conntrack->MessageBuff));

    elog(DEBUG3, "Resource manager dump type %u data to file %s",
    			 request->type,
				 request->dump_file);

    response.Result   = FUNC_RETURN_OK;
    response.Reserved = 0;
    switch (request->type)
    {
    case 1:
        dumpConnectionTracks(request->dump_file);
        break;
    case 2:
        dumpResourceQueueStatus(request->dump_file);
        break;
    case 3:
        dumpResourcePoolHosts(request->dump_file);
        break;
    default:
        Assert(false);
        break;
    }

    SelfMaintainBufferData responsedata;
    initializeSelfMaintainBuffer(&responsedata, PCONTEXT);

    appendSMBVar(&responsedata, response);

    buildResponseIntoConnTrack(conntrack,
                               SMBUFF_CONTENT(&responsedata),
							   getSMBContentSize(&responsedata),
							   conntrack->MessageMark1,
                               conntrack->MessageMark2,
                               RESPONSE_QD_DUMP_STATUS);
    destroySelfMaintainBuffer(&responsedata);

    conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

    return true;
}

bool handleRMRequestDummy(void **arg)
{
    ConnectionTrack 	 conntrack = (ConnectionTrack)(*arg);
    RPCResponseDummyData response;
    response.Result = FUNC_RETURN_OK;
    response.Reserved = 0;
    buildResponseIntoConnTrack(conntrack,
                               (char *)&response,
                               sizeof(response),
                               conntrack->MessageMark1,
                               conntrack->MessageMark2,
                               RESPONSE_DUMMY);
    conntrack->ResponseSent = false;
    MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
    PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
    MEMORY_CONTEXT_SWITCH_BACK

    return true;
}

bool handleRMRequestQuotaControl(void **arg)
{
	ConnectionTrack conntrack = (ConnectionTrack)(*arg);
	RPCRequestQuotaControl request =
		SMBUFF_HEAD(RPCRequestQuotaControl, &(conntrack->MessageBuff));
	Assert(request->Phase >= 0 && request->Phase < QUOTA_PHASE_COUNT);
	bool oldvalue = PRESPOOL->pausePhase[request->Phase];
	PRESPOOL->pausePhase[request->Phase] = request->Pause;
	if ( oldvalue != PRESPOOL->pausePhase[request->Phase] )
	{
		elog(LOG, "Resource manager resource quota life cycle pause setting %d "
				  "changes to %s",
				  request->Phase,
				  PRESPOOL->pausePhase[request->Phase]?"paused":"resumed");
	}

	RPCResponseQuotaControlData response;
	response.Result 	= FUNC_RETURN_OK;
	response.Reserved	= 0;

    buildResponseIntoConnTrack(conntrack,
                               (char *)&response,
                               sizeof(response),
                               conntrack->MessageMark1,
                               conntrack->MessageMark2,
							   RESPONSE_QD_QUOTA_CONTROL);
    conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}
