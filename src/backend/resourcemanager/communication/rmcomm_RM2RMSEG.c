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

#include "communication/rmcomm_RM2RMSEG.h"
#include "communication/rmcomm_Message.h"
#include "communication/rmcomm_MessageHandler.h"
#include "communication/rmcomm_RM_RMSEG_Protocol.h"
#include "dynrm.h"
#include "resourcepool.h"
#include "resourcebroker/resourcebroker_API.h"

#include "utils/memutilities.h"
#include "utils/simplestring.h"
#include "utils/linkedlist.h"

#define DRMRM2RMSEG_MEMORY_CONTEXT_NAME  "RM to RMSEG communication"

void receivedRUAliveResponse(AsyncCommMessageHandlerContext  context,
							 uint16_t						 messageid,
							 uint8_t						 mark1,
							 uint8_t						 mark2,
							 char 							*buffer,
							 uint32_t						 buffersize);
void sentRUAlive(AsyncCommMessageHandlerContext context);
void sentRUAliveError(AsyncCommMessageHandlerContext context);
void sentRUAliveCleanUp(AsyncCommMessageHandlerContext context);

void recvIncreaseMemoryQuotaResponse(AsyncCommMessageHandlerContext	context,
                                     uint16_t						messageid,
									 uint8_t						mark1,
									 uint8_t						mark2,
									 char							*buffer,
									 uint32_t						buffersize);
void sentIncreaseMemoryQuota(AsyncCommMessageHandlerContext context);
void sentIncreaseMemoryQuotaError(AsyncCommMessageHandlerContext context);
void sentIncreaseMemoryQuotaCleanup(AsyncCommMessageHandlerContext context);

void recvDecreaseMemoryQuotaResponse(AsyncCommMessageHandlerContext	context,
                                    uint16_t                        messageid,
                                    uint8_t                         mark1,
                                    uint8_t                         mark2,
                                    char                            *buffer,
                                    uint32_t                        buffersize);
void sentDecreaseMemoryQuota(AsyncCommMessageHandlerContext context);
void sentDecreaseMemoryQuotaError(AsyncCommMessageHandlerContext context);
void sentDecreaseMemoryQuotaCleanup(AsyncCommMessageHandlerContext context);

void processContainersAfterIncreaseMemoryQuota(GRMContainerSet ctns, bool accepted);
void processContainersAfterDecreaseMemoryQuota(GRMContainerSet cts, bool kicked);

/******************************************************************************
 *
 * Global Variables.
 *
 * Postmaster side global variables saving the data not necessarily always sent
 * from resource manager.
 *
 ******************************************************************************/

MemoryContext			RM2RMSEG_CommContext;
RM2RMSEGContextData		RM2RMSEG_Context;

void initializeRM2RMSEGComm(void)
{

	/* Ask for new memory context for this postmaster side memory consumption.*/
	MEMORY_CONTEXT_SWITCH_TO(TopMemoryContext)

	RM2RMSEG_CommContext = NULL;

	RM2RMSEG_CommContext = AllocSetContextCreate( CurrentMemoryContext,
											      DRMRM2RMSEG_MEMORY_CONTEXT_NAME,
											      ALLOCSET_DEFAULT_MINSIZE,
											      ALLOCSET_DEFAULT_INITSIZE,
											      ALLOCSET_DEFAULT_MAXSIZE);
	Assert( RM2RMSEG_CommContext != NULL );
	MEMORY_CONTEXT_SWITCH_BACK

	/* Create communication context. */
	RM2RMSEG_Context.RM2RMSEG_Conn_FD = -1;

	initializeSelfMaintainBuffer(&(RM2RMSEG_Context.SendBuffer),
								 RM2RMSEG_CommContext);
	initializeSelfMaintainBuffer(&(RM2RMSEG_Context.RecvBuffer),
								 RM2RMSEG_CommContext);
}

int cleanupRM2RMSEGComm(void)
{
	destroySelfMaintainBuffer(&(RM2RMSEG_Context.SendBuffer));
	destroySelfMaintainBuffer(&(RM2RMSEG_Context.RecvBuffer));
	return FUNC_RETURN_OK;
}

int sendRUAlive(char *seghostname)
{
	int 			res 			= FUNC_RETURN_OK;
	AsyncCommBuffer	newcommbuffer 	= NULL;
	SegResource 	segres			= NULL;
	int32_t 		segid 			= SEGSTAT_ID_INVALID;

	Assert(seghostname != NULL);

	res = getSegIDByHostName(seghostname, strlen(seghostname), &segid);
	if ( res != FUNC_RETURN_OK )
	{
		elog(WARNING, "Resource manager host %s not registered when send "
					  "RUAlive message.",
					  seghostname);
		return res;
	}

	segres = getSegResource(segid);
	Assert(segres != NULL);

	bool oldstat = setSegResRUAlivePending(segres, true);
	if( oldstat != false )
	{
		Assert(false);
	}

	/* Build request. */
	SelfMaintainBufferData tosend;
	initializeSelfMaintainBuffer(&tosend, PCONTEXT);

	RPCRequestRUAliveData request;
	request.Reserved = 0;
	appendSMBVar(&tosend, request);

	AsyncCommMessageHandlerContext context =
			(AsyncCommMessageHandlerContext)
			rm_palloc0(AsyncCommContext,
					   sizeof(AsyncCommMessageHandlerContextData));

	context->inMessage				 = false;
	context->MessageRecvReadyHandler = NULL;
	context->MessageRecvedHandler	 = receivedRUAliveResponse;
	context->MessageSendReadyHandler = NULL;
	context->MessageSentHandler		 = sentRUAlive;
	context->MessageErrorHandler 	 = sentRUAliveError;
	context->MessageCleanUpHandler	 = sentRUAliveCleanUp;
	context->UserData                = (void *)segres;

	/* Connect to HAWQ RM server */
	res = registerAsyncConnectionFileDesc(seghostname,
										  rm_segment_port,
										  ASYNCCOMM_READBYTES | ASYNCCOMM_WRITEBYTES,
										  &AsyncCommBufferHandlersMessage,
										  context,
										  &newcommbuffer);
	if ( res != FUNC_RETURN_OK )
	{
		rm_pfree(AsyncCommContext, context);
		elog(WARNING, "Fail to register asynchronous connection for sending "
					  "RUAlive message. %d", res);
		return res;
	}

	buildMessageToCommBuffer(newcommbuffer,
							 SMBUFF_CONTENT(&tosend),
							 getSMBContentSize(&tosend),
							 REQUEST_RM_RUALIVE,
							 0,
							 0);
	destroySelfMaintainBuffer(&tosend);
	context->AsyncBuffer = newcommbuffer;
	InitHandler_Message(newcommbuffer);
	return FUNC_RETURN_OK;
}

void receivedRUAliveResponse(AsyncCommMessageHandlerContext  context,
							 uint16_t						 messageid,
							 uint8_t						 mark1,
							 uint8_t						 mark2,
							 char 							*buffer,
							 uint32_t						 buffersize)
{
	Assert(context != NULL && buffer != NULL);

	SegResource 		segres	 	  = (SegResource)(context->UserData);
	RPCResponseRUAlive  response 	  = (RPCResponseRUAlive)buffer;
	bool               	responsevalid = true;

	if (messageid != RESPONSE_RM_RUALIVE ||
		buffersize != sizeof(RPCResponseRUAliveData) )
	{
		elog(WARNING, "Resource manager received wrong response for RUAlive request "
				      "sent to host %s. Set this host down.",
				      GET_SEGRESOURCE_HOSTNAME(segres));
		responsevalid = false;
	}
	else
	{
		elog(DEBUG3, "Resource manager received host %s RUAlive response message, "
			         "response is %d.",
					 GET_SEGRESOURCE_HOSTNAME(segres),
				     response->Result);
	}

	/*
	 * If the response is wrong, and the host is now marked available, mark this
	 * host as down now.
	 */
	if ( !responsevalid || response->Result != FUNC_RETURN_OK )
	{
		if (IS_SEGSTAT_FTSAVAILABLE(segres->Stat))
		{

			/*
			 * This call makes resource manager able to adjust queue and mem/core
			 * trackers' capacity.
			 */
			setSegResHAWQAvailability(segres, RESOURCE_SEG_STATUS_UNAVAILABLE);
			/*
			 * This call makes resource pool remove unused containers.
			 */
			returnAllGRMResourceFromSegment(segres);

			segres->Stat->StatusDesc |= SEG_STATUS_FAILED_PROBING_SEGMENT;
			/* Set the host down in gp_segment_configuration table */
			if (Gp_role != GP_ROLE_UTILITY)
			{
				SimpStringPtr description = build_segment_status_description(segres->Stat);
				update_segment_status(segres->Stat->ID + REGISTRATION_ORDER_OFFSET,
									  SEGMENT_STATUS_DOWN,
									  (description->Len > 0)?description->Str:"");
				add_segment_history_row(segres->Stat->ID + REGISTRATION_ORDER_OFFSET,
										GET_SEGRESOURCE_HOSTNAME(segres),
										description->Str);

				freeSimpleStringContent(description);
				rm_pfree(PCONTEXT, description);
			}
			/* Set the host down. */
			elog(WARNING, "Resource manager sets host %s from up to down "
					  	  "due to not getting valid RUAlive response.",
					  GET_SEGRESOURCE_HOSTNAME(segres));

			refreshResourceQueueCapacity(false);
			refreshActualMinGRMContainerPerSeg();
		}
		else {
			elog(DEBUG3, "Resource manager find host %s is down already.",
						 GET_SEGRESOURCE_HOSTNAME(segres));
		}
	}

	setSegResRUAlivePending(segres, false);
	closeFileDesc(context->AsyncBuffer);
}

void sentRUAlive(AsyncCommMessageHandlerContext context)
{
	Assert(context != NULL);
	SegResource segres = (SegResource)(context->UserData);
	elog(DEBUG3, "Resource manager sent successfully RUAlive request to host %s.",
				 GET_SEGRESOURCE_HOSTNAME(segres));
}

void sentRUAliveError(AsyncCommMessageHandlerContext context)
{
	SegResource segres = (SegResource)(context->UserData);
	Assert(segres != NULL);

	setSegResRUAlivePending(segres, false);

	if ( IS_SEGSTAT_FTSAVAILABLE(segres->Stat) )
	{
		/*
		 * This call makes resource manager able to adjust queue and mem/core
		 * trackers' capacity.
		 */
		setSegResHAWQAvailability(segres, RESOURCE_SEG_STATUS_UNAVAILABLE);
		/*
		 * This call makes resource pool remove unused containers.
		 */
		returnAllGRMResourceFromSegment(segres);
		segres->Stat->StatusDesc |= SEG_STATUS_COMMUNICATION_ERROR;
		/* Set the host down in gp_segment_configuration table */
		if (Gp_role != GP_ROLE_UTILITY)
		{
			SimpStringPtr description = build_segment_status_description(segres->Stat);
			update_segment_status(segres->Stat->ID + REGISTRATION_ORDER_OFFSET,
								  SEGMENT_STATUS_DOWN,
								  (description->Len > 0)?description->Str:"");
			add_segment_history_row(segres->Stat->ID + REGISTRATION_ORDER_OFFSET,
									GET_SEGRESOURCE_HOSTNAME(segres),
									description->Str);

			freeSimpleStringContent(description);
			rm_pfree(PCONTEXT, description);
		}
		/* Set the host down. */
		elog(LOG, "Resource manager sets host %s from up to down "
				  "due to communication error.",
				  GET_SEGRESOURCE_HOSTNAME(segres));

		refreshResourceQueueCapacity(false);
		refreshActualMinGRMContainerPerSeg();
	}
}

void sentRUAliveCleanUp(AsyncCommMessageHandlerContext context)
{
	/* Do nothing. */
}


/******************************************************************************
 * Increase/Decrease Memory Quota.
 ******************************************************************************/
int increaseMemoryQuota(char *seghostname, GRMContainerSet containerset)
{
    int             res			= FUNC_RETURN_OK;
    AsyncCommBuffer commbuffer	= NULL;
    GRMContainerSet newctns		= NULL;

    Assert( seghostname != NULL );

    /* Move resource containers that should be returned to communication context.*/
    newctns = createGRMContainerSet();
    moveGRMContainerSetContainerList(newctns, containerset);

    /* Build request */
    SelfMaintainBufferData tosend;
    initializeSelfMaintainBuffer(&tosend, PCONTEXT);

    RPCRequestUpdateMemoryQuotaData request;
    request.MemoryQuotaDelta = newctns->Allocated.MemoryMB;

    GRMContainer firstcont = getGRMContainerSetContainerFirst(newctns);
    Assert(firstcont != NULL);

    request.MemoryQuotaTotalPending = firstcont->Resource->Allocated.MemoryMB +
    								  newctns->Allocated.MemoryMB +
									  firstcont->Resource->OldInuse.MemoryMB;

    elog(LOG, "Resource manager increase host %s memory quota "UINT64_FORMAT" MB, "
    		  "Expected total memory quota after increasing is " UINT64_FORMAT" MB. "
			  "Include %d MB old in-use memory quota.",
			  seghostname,
    		  request.MemoryQuotaDelta,
			  request.MemoryQuotaTotalPending,
			  firstcont->Resource->OldInuse.MemoryMB);

    appendSMBVar(&tosend, request);

    /* Set content to send and add to AsyncComm framework */
    AsyncCommMessageHandlerContext context =
            (AsyncCommMessageHandlerContext)rm_palloc0(AsyncCommContext,
            sizeof(AsyncCommMessageHandlerContextData));
    context->inMessage               = false;
    context->UserData                = newctns;
    context->MessageRecvReadyHandler = NULL;
    context->MessageRecvedHandler    = recvIncreaseMemoryQuotaResponse;
    context->MessageSendReadyHandler = NULL;
    context->MessageSentHandler      = sentIncreaseMemoryQuota;
    context->MessageErrorHandler     = sentIncreaseMemoryQuotaError;
    context->MessageCleanUpHandler   = sentIncreaseMemoryQuotaCleanup;

    elog(DEBUG3, "Created AsyncComm Message context for Async Conn.");

	res = registerAsyncConnectionFileDesc(seghostname,
										  rm_segment_port,
										  ASYNCCOMM_READBYTES | ASYNCCOMM_WRITEBYTES,
										  &AsyncCommBufferHandlersMessage,
										  context,
										  &commbuffer);
    if ( res != FUNC_RETURN_OK )
    {
        elog(LOG, "Resource manager failed to set connection to segment host %s "
        		  "on port %d to increase memory quota.",
                  seghostname,
				  rm_segment_port);
        processContainersAfterIncreaseMemoryQuota(containerset, false);
		freeGRMContainerSet(newctns);
        rm_pfree(AsyncCommContext, context);
        return res;
    }
    else
    {
    	elog(DEBUG3, "Resource manager succeeded set connection to segment host %s "
    				 "on port %d to increase memory quota.",
     			     seghostname,
					 rm_segment_port);
    }

    buildMessageToCommBuffer(commbuffer,
                             tosend.Buffer,
                             tosend.Cursor+1,
                             REQUEST_RM_INCREASE_MEMORY_QUOTA,
                             0,
                             0);
    destroySelfMaintainBuffer(&tosend);

    context->AsyncBuffer = commbuffer;

	InitHandler_Message(commbuffer);

    return FUNC_RETURN_OK;
}

void recvIncreaseMemoryQuotaResponse(AsyncCommMessageHandlerContext	context,
                                     uint16_t                       messageid,
                                     uint8_t                        mark1,
                                     uint8_t                        mark2,
                                     char                           *buffer,
                                     uint32_t                       buffersize)
{
    RPCResponseUpdateMemoryQuota rsp  = (RPCResponseUpdateMemoryQuota)buffer;
	GRMContainerSet         	 ctns = (GRMContainerSet)(context->UserData);
	bool						 acceptedcontainer = false;

    GRMContainer firstcont = getGRMContainerSetContainerFirst(ctns);
    Assert(firstcont != NULL);

	if ( messageid != RESPONSE_RM_INCREASE_MEMORY_QUOTA ||
         buffersize != sizeof(RPCResponseUpdateMemoryQuotaData) )
    {
    	elog(LOG, "Resource manager received wrong response for increasing "
    			  "memory quota request from host %s",
				  GET_SEGRESOURCE_HOSTNAME(firstcont->Resource));
    }
	else if ( rsp->Result == FUNC_RETURN_OK )
	{
		elog(LOG, "Host %s successfully increased memory quota.",
				  GET_SEGRESOURCE_HOSTNAME(firstcont->Resource));
		acceptedcontainer = true;
	}
	else
	{
        elog(LOG, "Host %s failed to increase memory quota due to remote error. "
        		  "error code %d",
				  GET_SEGRESOURCE_HOSTNAME(firstcont->Resource),
        		  rsp->Result);
	}

    processContainersAfterIncreaseMemoryQuota(ctns, acceptedcontainer);
    closeFileDesc(context->AsyncBuffer);
}

void sentIncreaseMemoryQuota(AsyncCommMessageHandlerContext context)
{
}

void sentIncreaseMemoryQuotaError(AsyncCommMessageHandlerContext context)
{
	GRMContainerSet	ctns = (GRMContainerSet)(context->UserData);
	processContainersAfterIncreaseMemoryQuota(ctns, false);
}

void sentIncreaseMemoryQuotaCleanup(AsyncCommMessageHandlerContext context)
{
    /* Free user data in message */
	GRMContainerSet	ctns = (GRMContainerSet)(context->UserData);

	if ( ctns->Containers == NULL )
	{
		elog(DEBUG3, "Resource manager succeeded cleaning up data for increasing "
				     "memory quota message");
	}
	else
	{
		elog(WARNING, "Resource manager failed to cleanup data for increasing memory "
				  "quota message");
		processContainersAfterIncreaseMemoryQuota(ctns, false);
	}

	Assert( ctns->Containers == NULL );
	rm_pfree(PCONTEXT, (GRMContainerSet)(context->UserData));
}

int decreaseMemoryQuota(char 			*seghostname,
					    GRMContainerSet  rescontainerset)
{
    int             res			= FUNC_RETURN_OK;
    AsyncCommBuffer commbuffer	= NULL;
    GRMContainerSet ctns		= NULL;

    Assert( seghostname != NULL );
    Assert( rescontainerset->Containers != NULL );

    /* Move resource containers that should be returned to communication context.*/
    ctns = createGRMContainerSet();
    moveGRMContainerSetContainerList(ctns, rescontainerset);

    /* Build request */
    SelfMaintainBufferData tosend;
    initializeSelfMaintainBuffer(&tosend, PCONTEXT);

    RPCRequestUpdateMemoryQuotaData request;

    request.MemoryQuotaDelta = ctns->Allocated.MemoryMB;

    GRMContainer firstcont = getGRMContainerSetContainerFirst(ctns);
    Assert( firstcont != NULL );

    request.MemoryQuotaTotalPending = firstcont->Resource->Allocated.MemoryMB +
    								  firstcont->Resource->OldInuse.MemoryMB;

    Assert( request.MemoryQuotaTotalPending >= 0 );

    elog(LOG, "Resource manager decrease host %s memory quota "UINT64_FORMAT" MB, "
    		  "Expected total memory quota after decreasing is " UINT64_FORMAT" MB. "
			  "Include %d MB old in-use memory quota.",
			  seghostname,
    		  request.MemoryQuotaDelta,
			  request.MemoryQuotaTotalPending,
			  firstcont->Resource->OldInuse.MemoryMB);

    appendSMBVar(&tosend, request);

    /* Set content to send and add to AsyncComm framework */
    AsyncCommMessageHandlerContext context =
            (AsyncCommMessageHandlerContext)rm_palloc0(AsyncCommContext,
            sizeof(AsyncCommMessageHandlerContextData));
    context->inMessage               = false;
    context->UserData                = ctns;
    context->MessageRecvReadyHandler = NULL;
    context->MessageRecvedHandler    = recvDecreaseMemoryQuotaResponse;
    context->MessageSendReadyHandler = NULL;
    context->MessageSentHandler      = sentDecreaseMemoryQuota;
    context->MessageErrorHandler     = sentDecreaseMemoryQuotaError;
    context->MessageCleanUpHandler   = sentDecreaseMemoryQuotaCleanup;

	res = registerAsyncConnectionFileDesc(seghostname,
										  rm_segment_port,
										  ASYNCCOMM_READBYTES | ASYNCCOMM_WRITEBYTES,
										  &AsyncCommBufferHandlersMessage,
										  context,
										  &commbuffer);
    if ( res != FUNC_RETURN_OK )
    {
        elog(LOG, "Resource manager failed to set connection to segment host %s "
        		  "on port %d to decrease memory quota.",
                  seghostname,
				  rm_segment_port);
		processContainersAfterDecreaseMemoryQuota(ctns, false);
		freeGRMContainerSet(ctns);
		rm_pfree(AsyncCommContext, context);
        return res;
    }
    else
    {
    	elog(DEBUG3, "Resource manager succeeded set connection to segment host %s "
    				 "on port %d to decrease memory quota.",
     			     seghostname,
					 rm_segment_port);
    }

    buildMessageToCommBuffer(commbuffer,
                             tosend.Buffer,
                             tosend.Cursor+1,
                             REQUEST_RM_DECREASE_MEMORY_QUOTA,
                             0,
                             0);
    destroySelfMaintainBuffer(&tosend);

    context->AsyncBuffer = commbuffer;

	InitHandler_Message(commbuffer);

    return FUNC_RETURN_OK;
}

void recvDecreaseMemoryQuotaResponse(AsyncCommMessageHandlerContext	context,
                                     uint16_t                       messageid,
                                     uint8_t                        mark1,
                                     uint8_t                        mark2,
                                     char                           *buffer,
                                     uint32_t                       buffersize)
{
    RPCResponseUpdateMemoryQuota rsp  = (RPCResponseUpdateMemoryQuota)buffer;
	GRMContainerSet         	 ctns = (GRMContainerSet)(context->UserData);
	bool						 kickedcontainer = false;

	GRMContainer firstcont = getGRMContainerSetContainerFirst(ctns);
	Assert( firstcont != NULL );

    if ( messageid != RESPONSE_RM_DECREASE_MEMORY_QUOTA ||
         buffersize != sizeof(RPCResponseUpdateMemoryQuotaData) )
    {
    	elog(LOG, "Resource manager received wrong response for decreasing "
    			  "memory quota request from host %s.",
				  GET_SEGRESOURCE_HOSTNAME(firstcont->Resource));
    }
    else if ( rsp->Result == FUNC_RETURN_OK )
    {
    	elog(LOG, "Host %s successfully decreased memory quota.",
    			  GET_SEGRESOURCE_HOSTNAME(firstcont->Resource));
    	kickedcontainer = true;
    }
    else
    {
        elog(LOG, "Host %s failed to decrease memory quota due to remote error. "
        		  "Error code %d",
				  GET_SEGRESOURCE_HOSTNAME(firstcont->Resource),
        		  rsp->Result);
    }
    processContainersAfterDecreaseMemoryQuota(ctns, kickedcontainer);
    closeFileDesc(context->AsyncBuffer);
}

void sentDecreaseMemoryQuota(AsyncCommMessageHandlerContext context)
{
}

void sentDecreaseMemoryQuotaError(AsyncCommMessageHandlerContext context)
{
	GRMContainerSet	ctns = (GRMContainerSet)(context->UserData);
	processContainersAfterDecreaseMemoryQuota(ctns, false);
}

void sentDecreaseMemoryQuotaCleanup(AsyncCommMessageHandlerContext context)
{
    /* Free user data in message */
	GRMContainerSet	ctns = (GRMContainerSet)(context->UserData);

	if ( ctns->Containers == NULL )
	{
		elog(DEBUG3, "Resource manager succeeded cleaning up data for decrease memory "
				     "quota message");
	}
	else
	{
		elog(LOG, "Resource manager failed to cleanup data for decrease memory "
				  "quota message");
		processContainersAfterDecreaseMemoryQuota(ctns, false);
	}

	freeGRMContainerSet(ctns);
}

void processContainersAfterIncreaseMemoryQuota(GRMContainerSet ctns, bool accepted)
{
	GRMContainer ctn = NULL;

    /* Handle the containers. */
    if ( accepted )
    {
    	moveGRMContainerSetToAccepted(ctns);
    }
    else
    {
    	while( ctns->Containers != NULL )
		{
    		ctn = popGRMContainerSetContainerList(ctns);
    		ctn->Life += 1;

    		PRESPOOL->AddPendingContainerCount--;
    		elog(LOG, "AddPendingContainerCount minused 1, current value %d",
    				  PRESPOOL->AddPendingContainerCount);
    		/*
    		 * Add container to ToKickContainers if lifetime is not too long.
    		 * If the resource manager is not in clean up phase, directly drop
    		 * the container.
    		 */
    		if( !isCleanGRMResourceStatus() &&
    			ctn->Life < RESOURCE_CONTAINER_MAX_LIFETIME )
    		{
    			addGRMContainerToToBeAccepted(ctn);
    		}
    		/* Add container to KickedContainers if lifetime is long enough */
    		else
    		{
    			removePendingResourceRequestInRootQueue(ctn->MemoryMB, ctn->Core, false);
    			addGRMContainerToKicked(ctn);
    		}
    	}
    }
}

void processContainersAfterDecreaseMemoryQuota(GRMContainerSet ctns, bool kicked)
{
	GRMContainer ctn = NULL;

    /* Handle the containers. */
    if ( kicked ) {
    	moveGRMContainerSetToKicked(ctns);
    }
    else {
    	while( ctns->Containers != NULL )
    	{
    		ctn = popGRMContainerSetContainerList(ctns);
    		ctn->Life += 1;

    		PRESPOOL->RetPendingContainerCount--;
    		/* Add container to ToKickContainers if lifetime is not too long */
    		if( ctn->Life < RESOURCE_CONTAINER_MAX_LIFETIME )
    		{
    			/* Clear decrease pending quota because we will add back. */
    			minusResourceBundleData(&(ctn->Resource->DecPending),
    									ctn->MemoryMB,
										ctn->Core);
    			Assert( ctn->Resource->DecPending.MemoryMB >= 0 );
    			Assert( ctn->Resource->DecPending.Core >= 0 );

    			addGRMContainerToToBeKicked(ctn);
    		}
    		/* Add container to KickedContainers if lifetime is long enough */
    		else
    		{
    			addGRMContainerToKicked(ctn);
    		}
    	}
    }
}
