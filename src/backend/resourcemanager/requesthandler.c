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

/******************************************************************************
 * Global Variables
 ******************************************************************************/
extern char *UnixSocketDir;		  /* Reference from global configure.         */

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
	ConnectionTrack conntrack = (ConnectionTrack )(*arg);
	RPCResponseHeadRegisterConnectionInRMByStrData response;

	/* Parse request. */
	strncpy(conntrack->UserID,
			conntrack->MessageBuff.Buffer,
			sizeof(conntrack->UserID)-1);
	conntrack->UserID[sizeof(conntrack->UserID)-1] = '\0';


	/* Handle the request. */
	int res = registerConnectionByUserID(conntrack);
	if ( res == FUNC_RETURN_OK )
	{
		/* Allocate connection id and track this connection. */
		res = useConnectionID(&(conntrack->ConnID));
		if ( res == FUNC_RETURN_OK )
		{
			trackConnectionTrack(conntrack);
			elog(DEBUG5, "Resource manager tracked connection, ID=%d, Progress=%d.",
					     conntrack->ConnID,
					     conntrack->Progress);
			response.Result = FUNC_RETURN_OK;
			response.ConnID = conntrack->ConnID;
		}
		else
		{
			/* No connection id resource. Return occupation in resource queue. */
			returnConnectionToQueue(conntrack, false);
			elog(LOG, "Resource manager can not accept more connections.");
			response.Result = res;
			response.ConnID = INVALID_CONNID;
		}
	}
	else
	{
		response.Result = res;
		response.ConnID = INVALID_CONNID;
	}

	buildResponseIntoConnTrack( conntrack,
				   	   	   	    (char *)&response,
								sizeof(response),
								conntrack->MessageMark1,
								conntrack->MessageMark2,
								RESPONSE_QD_CONNECTION_REG);

	elog(DEBUG3, "One connection is registered. ConnID=%d, result=%d\n",
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
	int		   		res 	  = FUNC_RETURN_OK;
	ConnectionTrack conntrack = (ConnectionTrack )(*arg);
	bool			exist	  = false;

	RPCResponseHeadRegisterConnectionInRMByOIDData response;
	RPCRequestHeadRegisterConnectionInRMByOID request =
		(RPCRequestHeadRegisterConnectionInRMByOID)
		(conntrack->MessageBuff.Buffer);

	/* Get user name from oid. */
	UserInfo reguser = getUserByUserOID(request->UseridOid, &exist);
	if ( !exist ) {
		elog(LOG, "User oid "INT64_FORMAT" is not found. Temporarily set to "
				  "defaultuser.",
				  request->UseridOid);
		memcpy(conntrack->UserID, "defaultuser", sizeof("defaultuser"));
	}
	else {
		elog(DEBUG5, "User %s with oid "INT64_FORMAT" is found for registering.",
				     reguser->Name,
					 request->UseridOid);

		/* Set user id string into connection track. */
		strncpy(conntrack->UserID, reguser->Name, sizeof(conntrack->UserID)-1);
	}

	/* Handle the request. */
	res = registerConnectionByUserID(conntrack);

	if ( res == FUNC_RETURN_OK ) {
		/* Allocate connection id and track this connection. */
		res = useConnectionID(&(conntrack->ConnID));
		if ( res == FUNC_RETURN_OK ) {
			trackConnectionTrack(conntrack);
			elog(DEBUG5, "Resource manager tracked connection, ID=%d, Progress=%d.",
					     conntrack->ConnID,
					     conntrack->Progress);
			response.Result = FUNC_RETURN_OK;
			response.ConnID = conntrack->ConnID;
		}
		else {
			/* No connection id resource. Return occupation in resource queue. */
			returnConnectionToQueue(conntrack, false);
			elog(LOG, "Resource manager can not accept more connections.");
			response.Result = res;
			response.ConnID = INVALID_CONNID;
		}
	}
	else {
		response.Result = res;
		response.ConnID = INVALID_CONNID;
	}

	/* Build message saved in the connection track instance. */
	buildResponseIntoConnTrack( conntrack,
				   	   	   	    (char *)&response,
								sizeof(response),
								conntrack->MessageMark1,
								conntrack->MessageMark2,
								RESPONSE_QD_CONNECTION_REG_OID);

	elog(DEBUG3, "One connection is registered through OID. ConnID=%d, result=%d\n",
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
	int      		 res	 	= FUNC_RETURN_OK;
	ConnectionTrack *conntrack 	= (ConnectionTrack *)arg;

	RPCRequestHeadUnregisterConnectionInRM request =
		(RPCRequestHeadUnregisterConnectionInRM)((*conntrack)->MessageBuff.Buffer);
	elog(DEBUG5, "HAWQ RM :: Connection id %d try to unregister.",
				 request->ConnID);

	if ( (*conntrack)->ConnID == INVALID_CONNID )
	{
		res = retrieveConnectionTrack((*conntrack), request->ConnID);
		if ( res != FUNC_RETURN_OK )
		{
			elog(LOG, "Not valid resource context with id %d.", request->ConnID);
			goto sendresponse;
		}
		elog(DEBUG5, "HAWQ RM :: Fetched existing connection track "
					 "ID=%d, Progress=%d.",
					 (*conntrack)->ConnID,
					 (*conntrack)->Progress);
	}

	/* Get connection ID. */
	request = (RPCRequestHeadUnregisterConnectionInRM)
			  ((*conntrack)->MessageBuff.Buffer);

	elog(DEBUG5, "HAWQ RM :: Connection id %d unregisters connection.",
				 request->ConnID);

	if ( !canTransformConnectionTrackProgress((*conntrack), CONN_PP_ESTABLISHED) )
	{
		elog(DEBUG5, "HAWQ RM :: Wrong connection status for unregistering. "
					"Current connection status is %d.",
					(*conntrack)->Progress);
		res = REQUESTHANDLER_WRONG_CONNSTAT;
		goto sendresponse;
	}

	returnConnectionToQueue(*conntrack, false);

	elog(DEBUG3, "One connection is unregistered. ConnID=%d", (*conntrack)->ConnID);

sendresponse:
	{
		RPCResponseHeadUnregisterConnectionInRMData response;
		response.Result   = res;
		response.Reserved = 0;
		/* Build message saved in the connection track instance. */
		buildResponseIntoConnTrack((*conntrack),
							 	   (char *)&response,
								   sizeof(response),
								   (*conntrack)->MessageMark1,
								   (*conntrack)->MessageMark2,
								   RESPONSE_QD_CONNECTION_UNREG);

		if ( res == CONNTRACK_NO_CONNID ) {
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
 *
 * The previous connection status can be:
 * 		CONN_REGISTER_DONE 				: Register is done.
 *
 *		RESOURCE_QUEUE_ALLOC_WAIT		: Request is already submitted, skip
 *										  current content and keep retrieved
 *										  request.
 *
 *		RESOURCE_QUEUE_ALLOC_DONE		: Request is processed,but the response
 *										  is not sent due to some errors. Keep
 *										  retrieved request and allocation. Send
 *										  the response directly.
 *
 * Request format:
 *		uint32_t 	conn_id
 *		uint32_t 	scansizemb
 *		uint32_t 	preferred node count ( can be 0 )
 *		uint32_t 	seg count fix
 *		int64_t*n 	node scan size
 *		char	 	hostnames splited by '\0'
 *		uint8_t		pad if hostnames are not 64bit aligned.
 *
 * Response format:
 * 		No response if everything goes well. The request is only submitted to
 * 		resource queues to wait for the resource.
 *
 * 		If some error rises, this function build response as below.
 *
 * 		uint32_t error code
 * 		uint32_t reserved.
 *
 * Return:
 * 		FUNC_RETURN_OK		Succeed submitted the allocation request.
 * 		CONNTRACK_NO_CONNID Wrong connection track id.
 *
 **/
bool handleRMRequestAcquireResource(void **arg)
{
	/*
	 * If the request is received before refreshing resource queue capacity,
	 * temporarily ignore this request and let framework through back to the
	 * request list.
	 */
	if ( PQUEMGR->RootTrack != NULL &&
		 PQUEMGR->RootTrack->ClusterSegNumberMax == 0 )
	{
		return false;
	}

	int      		 res		= FUNC_RETURN_OK;
	ConnectionTrack *conntrack  = (ConnectionTrack *)arg;

	RPCRequestHeadAcquireResourceFromRM request =
		(RPCRequestHeadAcquireResourceFromRM)((*conntrack)->MessageBuff.Buffer);

	elog(DEBUG5, "HAWQ RM :: Connection id %d acquires query resource. "
				 "Session id "INT64_FORMAT,
				 request->ConnID,
				 request->SessionID);

	if ( (*conntrack)->ConnID == INVALID_CONNID )
	{
		res = retrieveConnectionTrack((*conntrack), request->ConnID);
		if ( res != FUNC_RETURN_OK )
		{
			elog(LOG, "Not valid resource context with id %d.", request->ConnID);
			goto sendresponse;
		}
		elog(DEBUG5, "HAWQ RM :: Fetched existing connection track "
					 "ID=%d, Progress=%d.",
					 (*conntrack)->ConnID,
					 (*conntrack)->Progress);
	}

	request = (RPCRequestHeadAcquireResourceFromRM)((*conntrack)->MessageBuff.Buffer);
	if ( (*conntrack)->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_DONE )
	{
		elog(DEBUG5, "HAWQ RM :: The connection track already has allocated "
					 "resource. Send again. ConnID=%d",
					 request->ConnID);
		goto sendagain;
	}
	else if ( (*conntrack)->Progress == CONN_PP_RESOURCE_QUEUE_ALLOC_WAIT )
	{
		elog(DEBUG5, "HAWQ RM :: The connection track already accepted "
					 "acquire resource request. Ignore. ConnID=%d",
					 request->ConnID);
		goto sendignore;
	}
	else if ( (*conntrack)->Progress != CONN_PP_REGISTER_DONE )
	{
		elog(DEBUG5, "HAWQ RM :: Wrong connection status for acquiring resource. "
					 "Current connection status is %d.",
					 (*conntrack)->Progress);
		res = REQUESTHANDLER_WRONG_CONNSTAT;
		goto sendresponse;
	}

	/* Check if HAWQ has enough alive segments. */
	int unavailcount = PRESPOOL->SlavesHostCount - PRESPOOL->AvailNodeCount;
	if ( unavailcount >= rm_rejectrequest_nseg_limit )
	{
		elog(WARNING, "Resource manager finds %d segments not available yet, all "
					  "resource allocation requests are rejected.",
					  unavailcount);
		res = RESOURCEPOOL_TOO_MANY_UAVAILABLE_HOST;
		goto sendresponse;
	}

	/* Get scan size. */
	request = (RPCRequestHeadAcquireResourceFromRM)
			  ((*conntrack)->MessageBuff.Buffer);

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

	elog(DEBUG3, "Expect resource. ConnID=%d, Scanning "INT64_FORMAT" io bytes "
				 "by %d slices with %d preferred segments. Each segment has "
				 "maximum %d vseg, query has maximum %d vseg.",
				 (*conntrack)->ConnID,
				 (*conntrack)->IOBytes,
				 (*conntrack)->SliceSize,
				 (*conntrack)->SegPreferredHostCount,
				 (*conntrack)->VSegLimitPerSeg,
				 (*conntrack)->VSegLimit);

	if ( (*conntrack)->StatNVSeg > 0 )
	{
		elog(LOG, "Statement level resource quota is active. "
				  "ConnID=%d, Expect resource. "
				  "Total %d vsegs, each vseg has %d MB memory quota.",
				  (*conntrack)->ConnID,
				  (*conntrack)->StatNVSeg,
				  (*conntrack)->StatVSegMemoryMB);
	}

	res = acquireResourceFromResQueMgr((*conntrack));
	if ( res != FUNC_RETURN_OK )
	{
		goto sendresponse;
	}
	(*conntrack)->ResRequestTime = gettime_microsec();
	(*conntrack)->LastActTime    = (*conntrack)->ResRequestTime;
sendignore:
	return true;  /* No need to send response now. The resource will be dispatched
					 when dispatching resource for each resource queue. */
sendresponse:
	{
		RPCResponseAcquireResourceFromRMERRORData errresponse;
		/* Send error message. */
		errresponse.Result   = res;
		errresponse.Reserved = 0;

		buildResponseIntoConnTrack( (*conntrack),
									(char *)&errresponse,
									sizeof(errresponse),
									(*conntrack)->MessageMark1,
									(*conntrack)->MessageMark2,
									RESPONSE_QD_ACQUIRE_RESOURCE);

		if ( res == CONNTRACK_NO_CONNID )
		{
			transformConnectionTrackProgress((*conntrack), CONN_PP_TRANSFORM_ERROR);
		}

		(*conntrack)->ResponseSent = false;
		{
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, *conntrack);
			MEMORY_CONTEXT_SWITCH_BACK
		}
		return true;
	}

sendagain:
	/* Let resource allocation response be sent again. */
	buildAcquireResourceResponseMessage((*conntrack));
	(*conntrack)->ResponseSent = false;
	{
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, *conntrack);
		MEMORY_CONTEXT_SWITCH_BACK
	}
	/* TODO: A problem here... The message buffer has been refreshed to a request,
	 *  	 the response must be built again before sending out. */
	return true;
}


/**
 * Handle RETURN RESOURCE request.
 *
 */
bool handleRMRequestReturnResource(void **arg)
{
	int      		res		    = FUNC_RETURN_OK;
	ConnectionTrack *conntrack  = (ConnectionTrack *)arg;

	RPCRequestHeadReturnResource request =
		(RPCRequestHeadReturnResource)((*conntrack)->MessageBuff.Buffer);

	elog(DEBUG5, "HAWQ RM :: Connection id %d returns query resource.",
				 request->ConnID);

	if ( (*conntrack)->ConnID == INVALID_CONNID )
	{
		res = retrieveConnectionTrack((*conntrack), request->ConnID);
		if ( res != FUNC_RETURN_OK )
		{
			elog(WARNING, "Not valid resource context with id %d.", request->ConnID);
			goto sendresponse;
		}
		elog(DEBUG5, "HAWQ RM :: Fetched existing connection track "
					 "ID=%d, Progress=%d.",
					 (*conntrack)->ConnID,
					 (*conntrack)->Progress);
	}

	/* Get connection ID. */
	request = (RPCRequestHeadReturnResource)
			  ((*conntrack)->MessageBuff.Buffer);
	elog(DEBUG5, "HAWQ RM :: Connection id %d returns query resource.",
				 request->ConnID);

	if ( (*conntrack)->Progress == CONN_PP_REGISTER_DONE )
	{
		elog(DEBUG5, "HAWQ RM :: The resource has been returned or has not been "
					 "acquired.");
		goto sendresponse;
	}
	else if ( (*conntrack)->Progress != CONN_PP_RESOURCE_QUEUE_ALLOC_DONE )
	{
		elog(DEBUG5, "HAWQ RM :: Wrong connection status for acquiring resource. "
					 "Current connection status is %d.",
					 (*conntrack)->Progress);
		res = REQUESTHANDLER_WRONG_CONNSTAT;
		goto sendresponse;
	}

	/* Return the resource. */
	returnResourceToResQueMgr(*conntrack);

	elog(DEBUG3, "Return resource. ConnID=%d", (*conntrack)->ConnID);

sendresponse:
	{
		elog(DEBUG3, "Return resource result %d.", res);

		RPCResponseHeadReturnResourceData response;
		response.Result   = res;
		response.Reserved = 0;
		buildResponseIntoConnTrack( (*conntrack),
	                                (char *)&response,
									sizeof(response),
									(*conntrack)->MessageMark1,
									(*conntrack)->MessageMark2,
									RESPONSE_QD_RETURN_RESOURCE );

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
	elog(DEBUG5, "Resource manager receives segment heart-beat information.");

	SelfMaintainBufferData machinereport;
	initializeSelfMaintainBuffer(&machinereport,PCONTEXT);
	SegStat segstat = (SegStat)(conntrack->MessageBuff.Buffer +
  	  	    					sizeof(RPCRequestHeadIMAliveData));
	generateSegStatReport(segstat, &machinereport);

	elog(DEBUG3, "Resource manager received segment machine information. %s",
				 machinereport.Buffer);

	destroySelfMaintainBuffer(&machinereport);

	/* Get hostname and ip address from the connection's sockaddr */
	char*    		fts_client_ip     = NULL;
	uint32_t 		fts_client_ip_len = 0;
	struct in_addr 	fts_client_addr;
	struct hostent* fts_client_host   = NULL;

	fts_client_ip = conntrack->ClientAddrDotStr;
	fts_client_ip_len = strlen(fts_client_ip);
	inet_aton(fts_client_ip, &fts_client_addr);
	fts_client_host = gethostbyaddr(&fts_client_addr, 4, AF_INET);
	Assert(fts_client_host != NULL);

	/* Get the received machine id instance start address. */
	SegInfo fts_client_seginfo = (SegInfo)(conntrack->MessageBuff.Buffer +
										   sizeof(RPCRequestHeadIMAliveData) +
										   offsetof(SegStatData, Info));

	/* Build new machine id with inserted ip address and modified host name. */
	SelfMaintainBufferData newseginfo;
	initializeSelfMaintainBuffer(&newseginfo, PCONTEXT);

	/* Copy machine id header. */
	prepareSelfMaintainBuffer(&newseginfo, sizeof(SegInfoData), false);
	memcpy(newseginfo.Buffer, fts_client_seginfo, sizeof(SegInfoData));
	jumpforwardSelfMaintainBuffer(&newseginfo, sizeof(SegInfoData));

	/* Put client ip address's offset and attribute */
	uint16_t addroffset = sizeof(SegInfoData) +
						  sizeof(uint32_t) *
						  (((fts_client_seginfo->HostAddrCount + 1 + 1) >> 1) << 1);

	uint16_t addrattr = HOST_ADDRESS_CONTENT_STRING;

	SegInfo newseginfoptr = (SegInfo)(newseginfo.Buffer);
	newseginfoptr->AddressAttributeOffset = sizeof(SegInfoData);
	newseginfoptr->AddressContentOffset = addroffset;
	newseginfoptr->HostAddrCount = fts_client_seginfo->HostAddrCount + 1;

	uint32_t addContentOffset= addroffset;

	appendSMBVar(&newseginfo,addroffset);
	appendSMBVar(&newseginfo,addrattr);

	elog(DEBUG5, "Resource manager received IMAlive message, this segment's IP "
				 "address count: %d\n",
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

	elog(DEBUG5, "Resource manager received IMAlive message, "
				 "this segment's IP address: %s\n",
				 fts_client_ip);

	/* Put other ip addresses' content directly. */
	appendSelfMaintainBuffer(&newseginfo,
							 (char *)fts_client_seginfo +
							 	 	 fts_client_seginfo->AddressContentOffset,
							 fts_client_seginfo->HostNameOffset -
							 fts_client_seginfo->AddressContentOffset);

	/* fill in hostname */
	newseginfoptr = (SegInfo)(newseginfo.Buffer);
	newseginfoptr->HostNameOffset = newseginfo.Cursor+1;
	appendSMBStr(&newseginfo,fts_client_host->h_name);
	appendSelfMaintainBufferTill64bitAligned(&newseginfo);

	newseginfoptr = (SegInfo)(newseginfo.Buffer);
	newseginfoptr->HostNameLen = strlen(fts_client_host->h_name);
	newseginfoptr->Size      = newseginfo.Cursor+1;
	/* reported by segment, set GRM host/rack as NULL */
	newseginfoptr->GRMHostNameLen = 0;
	newseginfoptr->GRMHostNameOffset = 0;
	newseginfoptr->GRMRackNameLen = 0;
	newseginfoptr->GRMRackNameOffset = 0;

	elog(DEBUG5, "Resource manager received IMAlive message, "
				 "this segment's hostname: %s\n",
				 fts_client_host->h_name);

	/* build segment status information instance and add to resource pool */
	SegStat newsegstat = (SegStat)
						 rm_palloc0(PCONTEXT,
								  	offsetof(SegStatData, Info) +
									newseginfo.Cursor + 1);
	/* Copy old segment status information. */
	memcpy((char *)newsegstat, segstat, offsetof(SegStatData, Info));
	/* Copy new segment info information. */
	memcpy((char *)newsegstat + offsetof(SegStatData, Info),
		   newseginfo.Buffer,
		   newseginfo.Cursor+1);

	destroySelfMaintainBuffer(&newseginfo);

	newsegstat->ID 				= SEGSTAT_ID_INVALID;
	newsegstat->GRMAvailable 	= RESOURCE_SEG_STATUS_UNSET;
	newsegstat->FTSAvailable 	= RESOURCE_SEG_STATUS_AVAILABLE;

	if ( addHAWQSegWithSegStat(newsegstat) != FUNC_RETURN_OK ) {
		/* Should be a duplciate host. */
		rm_pfree(PCONTEXT, newsegstat);
	}

	/* Refresh resource queue capacities. */
	refreshResourceQueuePercentageCapacity();
	/* Recalculate all memory/core ratio instances' limits. */
	refreshMemoryCoreRatioLimits();
	/* Refresh memory/core ratio level water mark. */
	refreshMemoryCoreRatioWaterMark();

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

	/*
	 * If the request is received before refreshing resource queue capacity,
	 * temporarily ignore this request and let framework through back to the
	 * request list.
	 */
	if ( PQUEMGR->RootTrack != NULL &&
		 PQUEMGR->RootTrack->ClusterSegNumberMax == 0 ) {
		return false;
	}

	int      		 res		= FUNC_RETURN_OK;
	ConnectionTrack  conntrack  = (ConnectionTrack)(*arg);
	bool			 exist		= false;

	RPCRequestHeadAcquireResourceQuotaFromRMByOID request =
		(RPCRequestHeadAcquireResourceQuotaFromRMByOID)
		(conntrack->MessageBuff.Buffer);

	elog(DEBUG5, "HAWQ RM :: User "INT64_FORMAT" acquires query resource quota "
				 "with %d split to process. Fixed segment number %d.",
				 request->UseridOid,
				 request->MaxSegCountFix,
				 request->MinSegCountFix);

	/* Get user name from oid. */
	UserInfo reguser = getUserByUserOID(request->UseridOid, &exist);
	if ( !exist ) {
		elog(LOG, "User oid "INT64_FORMAT" is not found. Temporarily set to "
				  "defaultuser.",
				  request->UseridOid);
		memcpy(conntrack->UserID, "defaultuser", sizeof("defaultuser"));
	}
	else {
		elog(DEBUG5, "User %s with oid "INT64_FORMAT" is found for acquiring "
					 "resource.",
				     reguser->Name,
					 request->UseridOid);

		/* Set user id string into connection track. */
		strncpy(conntrack->UserID, reguser->Name, sizeof(conntrack->UserID)-1);
	}

	conntrack->MaxSegCountFixed = request->MaxSegCountFix;
	conntrack->MinSegCountFixed = request->MinSegCountFix;
	conntrack->VSegLimitPerSeg	= request->VSegLimitPerSeg;
	conntrack->VSegLimit		= request->VSegLimit;

	res = acquireResourceQuotaFromResQueMgr(conntrack);

	if ( res == FUNC_RETURN_OK ) {
		RPCResponseHeadAcquireResourceQuotaFromRMByOIDData response;

		DynResourceQueueTrack queuetrack =
			getQueueTrackByQueueOID(reguser->QueueOID, &exist);
		if ( exist ) {
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
	}
	else {
		RPCResponseHeadAcquireResourceQuotaFromRMByOIDERRORData errresponse;
		errresponse.Result   = res;
		errresponse.Reserved = 0;
		buildResponseIntoConnTrack( conntrack,
									(char *)&errresponse,
									sizeof(errresponse),
									conntrack->MessageMark1,
									conntrack->MessageMark2,
									RESPONSE_QD_ACQUIRE_RESOURCE_QUOTA);
	}
	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK
	return true;
}

bool handleRMRequestRefreshResource(void **arg)
{
	int      		 res		= FUNC_RETURN_OK;
	ConnectionTrack  conntrack  = (ConnectionTrack)(*arg);
	ConnectionTrack  oldct		= NULL;
	uint64_t		 curmsec    = gettime_microsec();

	RPCRequestHeadRefreshResourceHeartBeat request =
		(RPCRequestHeadRefreshResourceHeartBeat)
		(conntrack->MessageBuff.Buffer);

	uint32_t *connids = (uint32_t *)
						(conntrack->MessageBuff.Buffer +
						 sizeof(RPCRequestHeadRefreshResourceHeartBeatData));

	elog(DEBUG3, "Resource manager refreshes %d ConnIDs.", request->ConnIDCount);

	for ( int i = 0 ; i < request->ConnIDCount ; ++i )
	{
		/* Find connection track identified by ConnID */
		res = getInUseConnectionTrack(connids[i], &oldct);
		if ( res == FUNC_RETURN_OK )
		{
			oldct->LastActTime = curmsec;
			elog(DEBUG3, "Refreshed resource of connection id %d", connids[i]);
		}
		else
		{
			elog(DEBUG3, "Can not find connection id %d for resource refreshing.",
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
	char 			*hostname    = conntrack->MessageBuff.Buffer;
	int				 hostnamelen = 0;
	int32_t		 	 segid       = SEGSTAT_ID_INVALID;

	while( (hostname - conntrack->MessageBuff.Buffer <= conntrack->MessageBuff.Cursor) &&
		   *hostname != '\0' ) {

		hostnamelen = strlen(hostname);
		res = getSegIDByHostName(hostname, hostnamelen, &segid);
		if ( res == FUNC_RETURN_OK )  {

			/* Get resourceinfo of the expected host. */
			SegResource segres = getSegResource(segid);
			Assert( segres != NULL );

			if ( !IS_SEGSTAT_FTSAVAILABLE(segres->Stat) ) {
				elog(LOG, "Resource manager does not probe the status of host %s "
						  "because it is down already.",
						  hostname);
			}
			else if ( segres->RUAlivePending ) {
				elog(LOG, "Resource manager does not probe the status of host %s "
						  "because it is in RUAlive pending status already.",
						  hostname);
			}
			else {

				elog(DEBUG3, "Resource manager probes the status of host %s by "
						 	 "sending RUAlive request.",
							 hostname);

		        res = sendRUAlive(hostname);
		        /* IN THIS CASE, the segment is considered as down. */
		        if (res != FUNC_RETURN_OK) {

		        	/*
		        	 * This call makes resource manager able to adjust queue and
		        	 * mem/core trackers' capacity.
		        	 */
		        	setSegResHAWQAvailability(segres, RESOURCE_SEG_STATUS_UNAVAILABLE);

		        	/* Make resource pool remove unused containers */
		        	returnAllGRMResourceFromSegment(segres);
		        	/* Set the host down in gp_segment_configuration table */
		        	update_segment_status(segres->Stat->ID + REGISTRATION_ORDER_OFFSET, SEGMENT_STATUS_DOWN);
		        	/* Set the host down. */
		        	elog(LOG, "Resource manager sets host %s from up to down "
		        			  "due to not reaching host.", hostname);
		        }
		        else {
		        	elog(DEBUG3, "Resource manager triggers RUAlive request to "
		        				 "host %s.",
								 hostname);
		        }
			}
		}
		else {
			elog(LOG, "Resource manager cannot find host %s to check status, "
					  "skip it.",
					  hostname);
		}
		hostname = hostname + strlen(hostname) + 1; /* Try next */
	}

	refreshResourceQueuePercentageCapacity();

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

bool handleRMRequestTmpDir(void **arg)
{
	int      		 res		= FUNC_RETURN_OK;
    ConnectionTrack conntrack   = (ConnectionTrack)(*arg);

    RPCResponseTmpDirForQDData response;

    if (DRMGlobalInstance->NextLocalHostTempDirIdx < 0) 
    {
        response.Result       = RM_STATUS_BAD_TMPDIR;
        response.tmpdir[0]    = '\0';
        response.Reserved     = 0;

        buildResponseIntoConnTrack(conntrack,
                                   (char *)&response,
                                   sizeof(response),
                                   conntrack->MessageMark1,
                                   conntrack->MessageMark2,
                                   RESPONSE_QD_TMPDIR);

        elog(LOG, "handleRMRequestTmpDir, no existing tmp dirs in the "
                     "master resource manager");
    }
    else
    {
        response.Result       = res;
        response.Reserved     = 0;
        
        SimpStringPtr tmpdir = (SimpStringPtr)
        getDQueueNodeDataByIndex(&DRMGlobalInstance->LocalHostTempDirectoriesForQD, DRMGlobalInstance->NextLocalHostTempDirIdxForQD);

        DRMGlobalInstance->NextLocalHostTempDirIdxForQD =
                    (DRMGlobalInstance->NextLocalHostTempDirIdxForQD + 1) %
                     getDQueueLength(&DRMGlobalInstance->LocalHostTempDirectoriesForQD);
        
        memset(response.tmpdir, 0, sizeof(response.tmpdir));
        memcpy(response.tmpdir, tmpdir->Str, tmpdir->Len);

        buildResponseIntoConnTrack(conntrack,
                                   (char *)&response,
                                   sizeof(response),
                                   conntrack->MessageMark1,
                                   conntrack->MessageMark2,
                                   RESPONSE_QD_TMPDIR);
        
        elog(LOG, "handleRMRequestTmpDir, get temporary directory:%s from master resource manager", tmpdir->Str);
    }

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

    response->Result 	= 0;
    response->queuenum 	= list_length(PQUEMGR->Queues);

    int i = 0;
    ListCell *cell = NULL;
    foreach(cell, PQUEMGR->Queues)
    {
    	DynResourceQueueTrack quetrack = lfirst(cell);

        sprintf(response->queuedata[i].name, "%s", quetrack->QueueInfo->Name);
        response->queuedata[i].segmem = quetrack->QueueInfo->SegResourceQuotaMemoryMB;
        response->queuedata[i].segcore = quetrack->QueueInfo->SegResourceQuotaVCore;
        response->queuedata[i].segsize = quetrack->ClusterSegNumber;
        response->queuedata[i].segsizemax = quetrack->ClusterSegNumberMax;
        response->queuedata[i].inusemem = quetrack->TotalUsed.MemoryMB;
        response->queuedata[i].inusecore = quetrack->TotalUsed.Core;
        response->queuedata[i].holders = quetrack->NumOfRunningQueries;
        response->queuedata[i].waiters = quetrack->QueryResRequests.NodeCount;

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
    ConnectionTrack conntrack   = (ConnectionTrack)(*arg);
    RPCResponseDumpStatusData response;

    uint32_t type = *((uint32_t *)(conntrack->MessageBuff.Buffer + 0));
    char *dump_file  = (char  *)(conntrack->MessageBuff.Buffer + 8);

    elog(LOG, "handleRMRequestDumpStatus type:%u dump_file:%s", type, dump_file);

    switch (type)
    {
    case 1:
        dumpConnectionTracks(dump_file);
        response.Result       = FUNC_RETURN_OK;
        break;
    case 2:
        dumpResourceQueueStatus(dump_file);
        response.Result       = FUNC_RETURN_OK;
        break;
    case 3:
        dumpResourcePoolHosts(dump_file);
        response.Result       = FUNC_RETURN_OK;
        break;
    default:
        response.Result       = RM_STATUS_BAD_DUMP_TYPE;
        break;
    }

    response.Reserved     = 0;

    buildResponseIntoConnTrack(conntrack,
                                   (char *)&response,
                                   sizeof(response),
                                   conntrack->MessageMark1,
                                   conntrack->MessageMark2,
                                   RESPONSE_QD_DUMP_STATUS);

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
