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

#include "envswitch.h"
#include "communication/rmcomm_SyncComm.h"

#include <pthread.h>
#include "miscadmin.h"
#include "rmcommon.h"
#include "communication/rmcomm_AsyncComm.h"
#include "communication/rmcomm_Message.h"
#include "utils/network_utils.h"

enum SYNC_RPC_COMM_STATUS {
	SYNC_RPC_COMM_IDLE,
	SYNC_RPC_COMM_SENDING,
	SYNC_RPC_COMM_RECVING
};

struct SyncRPCContextData {
	uint16_t  					ExpMessageID;
	SelfMaintainBuffer			RecvBuffer;
	uint8_t						RecvMark1;
	uint8_t						RecvMark2;
	enum SYNC_RPC_COMM_STATUS	CommStatus;
	int							Result;
};
typedef struct SyncRPCContextData  SyncRPCContextData;
typedef struct SyncRPCContextData *SyncRPCContext;

void initializeSyncRPContent(SyncRPCContext 	content,
							 SelfMaintainBuffer smb,
							 uint16_t	 		expmsgid);

AsyncCommMessageHandlerContext createMessageHandlerContext(SyncRPCContext content);

void receivedSyncRPCResponse(AsyncCommMessageHandlerContext  context,
							 uint16_t						 messageid,
							 uint8_t						 mark1,
							 uint8_t						 mark2,
							 char 							*buffer,
							 uint32_t						 buffersize);
void sentSyncRPCRequest(AsyncCommMessageHandlerContext context);
void sentSyncRPCRequestError(AsyncCommMessageHandlerContext context);
void sentSyncRPCRequestCleanUp(AsyncCommMessageHandlerContext context);

int callSyncRPCRemote(const char     	   *hostname,
					  uint16_t              port,
		  	  	  	  const char 	 	   *sendbuff,
					  int   		  		sendbuffsize,
					  uint16_t		  		sendmsgid,
					  uint16_t 		  		exprecvmsgid,
					  SelfMaintainBuffer 	recvsmb,
					  char				   *errorbuf,
					  int					errorbufsize)
{
	int 							fd 			  = -1;
	int 							res 		  = FUNC_RETURN_OK;
	AsyncCommBuffer					newcommbuffer = NULL;
	AsyncCommMessageHandlerContext 	context 	  = NULL;
	SyncRPCContextData 				userdata;

	/* Connect to the server side. */
	res = connectToServerRemote(hostname, port, &fd);
	if ( res != FUNC_RETURN_OK )
	{
		snprintf(errorbuf, errorbufsize,
				 "failed to connect to remote socket server %s:%d",
				 hostname,
				 port);
		elog(LOG, "%s", errorbuf);
		goto exit;
	}

	initializeSyncRPContent(&userdata, recvsmb, exprecvmsgid);
	context = createMessageHandlerContext(&userdata);

	res = registerFileDesc(fd,
						   ASYNCCOMM_READBYTES | ASYNCCOMM_WRITEBYTES,
						   &AsyncCommBufferHandlersMessage,
						   context,
						   &newcommbuffer);
	if ( res != FUNC_RETURN_OK )
	{
		rm_pfree(AsyncCommContext, context);

		snprintf(errorbuf, errorbufsize,
				 "failed to register socket connection fd %d connected to %s:%d "
				 "for resource rpc communication",
				 fd,
				 hostname, port);

		elog(LOG, "%s", errorbuf);
		goto exit;
	}

	buildMessageToCommBuffer(newcommbuffer, sendbuff, sendbuffsize, sendmsgid, 0, 0);

	context->AsyncBuffer = newcommbuffer;

	InitHandler_Message(newcommbuffer);

	/* Wait for the complete of the communication. */
	while( true )
	{
		processAllCommFileDescs();
		CHECK_FOR_INTERRUPTS();
		if ( userdata.CommStatus == SYNC_RPC_COMM_IDLE )
		{
			break;
		}
	}

	res = userdata.Result;

	/* Close and cleanup */
	unresigsterFileDesc(fd);
	elog(DEBUG3, "Result of synchronous RPC. %d", res);

	if ( res == FUNC_RETURN_OK )
	{
		returnAliveConnectionRemoteByHostname(&fd, hostname, port);
	}
	else
	{
		closeConnectionRemote(&fd);
	}

	if (res != FUNC_RETURN_OK)
	{
	  elog(LOG, "Sync RPC framework (inet) finds exception raised.");

	  switch(res)
	  {
	  case COMM2RM_CLIENT_FAIL_SEND:
		  snprintf(errorbuf, errorbufsize, "failed to send content");
		  break;
	  case COMM2RM_CLIENT_FAIL_RECV:
		  snprintf(errorbuf, errorbufsize, "failed to receive content");
		  break;
	  case REQUESTHANDLER_WRONGMESSAGE:
		  snprintf(errorbuf, errorbufsize, "wrong message was received");
		  break;
	  default:
		  Assert(false);
	  }
	}
	return res;
exit:
	elog(LOG, "Close fd %d at once", fd);
	closeConnectionRemote(&fd);
	return res;
}

void receivedSyncRPCResponse(AsyncCommMessageHandlerContext  context,
							 uint16_t						 messageid,
							 uint8_t						 mark1,
							 uint8_t						 mark2,
							 char 							*buffer,
							 uint32_t						 buffersize)
{
	SyncRPCContext userdata = (SyncRPCContext)(context->UserData);
	Assert(userdata != NULL);

	userdata->CommStatus = SYNC_RPC_COMM_IDLE;

	if ( userdata->ExpMessageID == messageid ) {
		resetSelfMaintainBuffer(userdata->RecvBuffer);
		appendSelfMaintainBuffer(userdata->RecvBuffer, buffer, buffersize);
		userdata->RecvMark1 = mark1;
		userdata->RecvMark2 = mark2;
		userdata->Result    = FUNC_RETURN_OK;
	}
	else {
		userdata->Result    = REQUESTHANDLER_WRONGMESSAGE;
	}
}

void sentSyncRPCRequest(AsyncCommMessageHandlerContext context)
{
	SyncRPCContext userdata = (SyncRPCContext)(context->UserData);
	Assert(userdata != NULL);
	userdata->CommStatus = SYNC_RPC_COMM_RECVING;
	userdata->Result = COMM2RM_CLIENT_FAIL_RECV;
}

void sentSyncRPCRequestError(AsyncCommMessageHandlerContext context)
{
	elog(DEBUG5, "Sync RPC sending error is detected.");
	SyncRPCContext userdata = (SyncRPCContext)(context->UserData);
	Assert(userdata != NULL);
	userdata->CommStatus = SYNC_RPC_COMM_IDLE;
}

void sentSyncRPCRequestCleanUp(AsyncCommMessageHandlerContext context)
{
	elog(DEBUG5, "Sync RPC clean up is called.");
	SyncRPCContext userdata = (SyncRPCContext)(context->UserData);
	Assert(userdata != NULL);
	userdata->CommStatus = SYNC_RPC_COMM_IDLE;
}

void initializeSyncRPCComm(void)
{
	initializeAsyncComm();
}

void initializeSyncRPContent(SyncRPCContext 	content,
							 SelfMaintainBuffer smb,
							 uint16_t	 		expmsgid)
{
	content->ExpMessageID = expmsgid;
	content->RecvBuffer   = smb;
	content->RecvMark1    = 0;
	content->RecvMark2    = 0;
	content->CommStatus   = SYNC_RPC_COMM_SENDING;
	content->Result       = COMM2RM_CLIENT_FAIL_SEND;
}

AsyncCommMessageHandlerContext createMessageHandlerContext(SyncRPCContext content)
{
	AsyncCommMessageHandlerContext context = NULL;

	context = rm_palloc0(AsyncCommContext, sizeof(AsyncCommMessageHandlerContextData));
	context->inMessage				 = false;
	context->UserData  				 = NULL;
	context->MessageRecvReadyHandler = NULL;
	context->MessageRecvedHandler 	 = receivedSyncRPCResponse;
	context->MessageSendReadyHandler = NULL;
	context->MessageSentHandler		 = sentSyncRPCRequest;
	context->MessageErrorHandler 	 = sentSyncRPCRequestError;
	context->MessageCleanUpHandler	 = sentSyncRPCRequestCleanUp;
	context->UserData 				 = (void *)content;
	return context;
}
