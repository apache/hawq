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

#include "communication/rmcomm_RMSEG2RM.h"
#include "communication/rmcomm_Message.h"
#include "communication/rmcomm_MessageHandler.h"
#include "communication/rmcomm_RMSEG_RM_Protocol.h"
#include "dynrm.h"
#include "cdb/cdbtmpdir.h"
#include "utils/memutilities.h"
#include "utils/simplestring.h"
#include "utils/linkedlist.h"

#define DRMRMSEG2RM_MEMORY_CONTEXT_NAME  "RMSEG to RM communication"

void receivedIMAliveResponse(AsyncCommMessageHandlerContext  context,
							 uint16_t						 messageid,
							 uint8_t						 mark1,
							 uint8_t						 mark2,
							 char 							*buffer,
							 uint32_t						 buffersize);
void sentIMAlive(AsyncCommMessageHandlerContext context);
void sentIMAliveError(AsyncCommMessageHandlerContext context);
void sentIMAliveCleanUp(AsyncCommMessageHandlerContext context);
/******************************************************************************
 *
 * Global Variables.
 *
 * Postmaster side global variables saving the data not necessarily always sent
 * from resource manager.
 *
 ******************************************************************************/

MemoryContext			RMSEG2RM_CommContext;
RMSEG2RMContextData		RMSEG2RM_Context;

void initializeRMSEG2RMComm(void)
{
	/* Ask for new memory context for this postmaster side memory consumption.*/
	MEMORY_CONTEXT_SWITCH_TO(TopMemoryContext)

	RMSEG2RM_CommContext 	= NULL;

	RMSEG2RM_CommContext = AllocSetContextCreate( CurrentMemoryContext,
											      DRMRMSEG2RM_MEMORY_CONTEXT_NAME,
											      ALLOCSET_DEFAULT_MINSIZE,
											      ALLOCSET_DEFAULT_INITSIZE,
											      ALLOCSET_DEFAULT_MAXSIZE );
	Assert( RMSEG2RM_CommContext != NULL );
	MEMORY_CONTEXT_SWITCH_BACK

	/* Create communication context. */
	RMSEG2RM_Context.RMSEG2RM_Conn_FD = -1;

	initializeSelfMaintainBuffer(&(RMSEG2RM_Context.SendBuffer),
								 RMSEG2RM_CommContext);
	initializeSelfMaintainBuffer(&(RMSEG2RM_Context.RecvBuffer),
								 RMSEG2RM_CommContext);
}

int cleanupRMSEG2RMComm(void)
{
	destroySelfMaintainBuffer(&(RMSEG2RM_Context.SendBuffer));
	destroySelfMaintainBuffer(&(RMSEG2RM_Context.RecvBuffer));

	return FUNC_RETURN_OK;
}

/******************************************************************************
 * I aM Alive.
 *
 * Request:
 *         |<----------- 64 bits (8 bytes) ----------->|
 *         +----------+--------------------------------+
 *         |  TDC     |  BDC     |     Reserved        |
 * 		   +----------+----------+---------------------+
 *         |                                           |
 *         |             Machine ID info               |
 *         |                                           |
 * 		   +-------------------------------------------+     _____ 64bit aligned
 *
 * Response:
 *         |<----------- 64 bits (8 bytes) ----------->|
 *         +---------------------+---------------------+
 *         |  heartbeat result   |    Reserved   	   |
 * 		   +---------------------+---------------------+     _____ 64bit aligned
 *
 ******************************************************************************/
int sendIMAlive(int  *errorcode,
				char *errorbuf,
				int	  errorbufsize)
{
	int 				res 					= FUNC_RETURN_OK;
	AsyncCommBuffer		newcommbuffer			= NULL;

	Assert( DRMGlobalInstance->LocalHostStat != NULL );

	/* Build request. */
	SelfMaintainBufferData tosend;
	initializeSelfMaintainBuffer(&tosend, PCONTEXT);

	RPCRequestHeadIMAliveData requesthead;
	requesthead.TmpDirCount 	  = TmpDirNum;
	requesthead.TmpDirBrokenCount = DRMGlobalInstance->LocalHostStat->FailedTmpDirNum;
	requesthead.Reserved		  = 0;

	appendSMBVar(&tosend, requesthead);
	appendSelfMaintainBuffer(&tosend,
							 (char *)(DRMGlobalInstance->LocalHostStat),
							 offsetof(SegStatData, Info) +
							 DRMGlobalInstance->LocalHostStat->Info.Size);

	/* Set content to send and add to AsyncComm framework. */
	AsyncCommMessageHandlerContext context =
			rm_palloc0(AsyncCommContext,
					   sizeof(AsyncCommMessageHandlerContextData));
	context->inMessage				 = false;
	context->UserData  				 = NULL;
	context->MessageRecvReadyHandler = NULL;
	context->MessageRecvedHandler 	 = receivedIMAliveResponse;
	context->MessageSendReadyHandler = NULL;
	context->MessageSentHandler		 = sentIMAlive;
	context->MessageErrorHandler 	 = sentIMAliveError;
	context->MessageCleanUpHandler	 = sentIMAliveCleanUp;

	/* Connect to HAWQ RM server */

	res = registerAsyncConnectionFileDesc(NULL,
										  DRMGlobalInstance->SendToStandby?
										  standby_addr_host:
										  master_addr_host,
										  rm_master_port,
										  ASYNCCOMM_READBYTES | ASYNCCOMM_WRITEBYTES,
										  &AsyncCommBufferHandlersMessage,
										  context,
										  &newcommbuffer);
	if ( res != FUNC_RETURN_OK )
	{
		rm_pfree(AsyncCommContext, context);
		elog(WARNING, "Fail to register asynchronous connection for sending "
					  "IMAlive message. %d", res);
		return res;
	}

	buildMessageToCommBuffer(newcommbuffer,
							 tosend.Buffer,
							 tosend.Cursor + 1,
							 REQUEST_RM_IMALIVE,
							 0,
							 0);

	destroySelfMaintainBuffer(&tosend);

	context->AsyncBuffer = newcommbuffer;

	InitHandler_Message(newcommbuffer);

	return FUNC_RETURN_OK;
}



int sendIShutdown(void)
{
	return FUNC_RETURN_OK;
}

void receivedIMAliveResponse(AsyncCommMessageHandlerContext  context,
							 uint16_t						 messageid,
							 uint8_t						 mark1,
							 uint8_t						 mark2,
							 char 							*buffer,
							 uint32_t						 buffersize)
{
	RPCResponseIMAlive response = (RPCResponseIMAlive)buffer;
	if ( messageid != RESPONSE_RM_IMALIVE ||
		 buffersize != sizeof(RPCResponseIMAliveData) ) {
		elog(WARNING, "Segment's resource manager received wrong response for heart-beat request.");
		return;
	}
	if ( response->Result == FUNC_RETURN_OK ) {
		elog(DEBUG5, "Segment's resource manager gets response of heart-beat request successfully.");
	}
	else {
		elog(LOG, "Segment's resource manager gets error code %d as the response of "
				  "heart-beat request.",
				  response->Result);
	}

	context->AsyncBuffer->toClose     = true;
	context->AsyncBuffer->forcedClose = false;
}

void sentIMAlive(AsyncCommMessageHandlerContext context)
{
	/* Do nothing. */
}

void sentIMAliveError(AsyncCommMessageHandlerContext context)
{
	if(DRMGlobalInstance->SendToStandby)
		elog(WARNING, "Segment's resource manager sending IMAlive message switches from standby to master");
	else
		elog(WARNING, "Segment's resource manager sending IMAlive message switches from master to standby");
	DRMGlobalInstance->SendToStandby = !DRMGlobalInstance->SendToStandby;
}

void sentIMAliveCleanUp(AsyncCommMessageHandlerContext context)
{
	/* Do nothing. */
}
