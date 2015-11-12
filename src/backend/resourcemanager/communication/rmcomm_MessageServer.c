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

#include "communication/rmcomm_MessageServer.h"
#include "communication/rmcomm_Message.h"
#include "conntrack.h"
#include "utils/network_utils.h"

/*
 * This implementation of the message server makes connection trackers to save
 * data.
 */
AsyncCommBufferHandlersData AsyncCommBufferHandlersMsgServer = {
	NULL,
	ReadReadyHandler_MsgServer,
	NULL,
	NULL,
	NULL,
	ErrorHandler_MsgServer,
	CleanUpHandler_MsgServer
};

AsyncCommMessageHandlerContext createConnTrackHandlerContext(ConnectionTrack newtrack);

void ReadReadyHandler_MsgServer(AsyncCommBuffer buffer)
{
	int					res				= FUNC_RETURN_OK;
	ConnectionTrack 	newtrack 		= NULL;
	ConnectionTrackData tmptrackdata;
	AsyncCommBuffer     newcommbuffer   = NULL;

	/* For each new client connection, we use one new connection tracker. */
	res = useConnectionTrack(&newtrack);
	if ( res == FUNC_RETURN_OK && canRegisterFileDesc() )
	{
		/* The connection track progress must be at initial state. */
		Assert(newtrack->Progress == CONN_PP_INFO_NOTSET);
		/* Accept connection. */
		newtrack->ClientAddrLen = sizeof(newtrack->ClientAddr);
		newtrack->ClientSocket  = accept(buffer->FD,
										 (struct sockaddr *)&(newtrack->ClientAddr),
										 &(newtrack->ClientAddrLen));
		if ( newtrack->ClientSocket == -1 )
		{
			elog(WARNING, "Resource manager socket accept error is detected. "
						  "This connection is to be closed. errno %d",
						  errno);
			/* Return connection track. */
			returnConnectionTrack(newtrack);
		}
		else
		{
			/* Set client connection address and port. */
			strncpy(newtrack->ClientAddrDotStr,
					SOCKADDR(&(newtrack->ClientAddr)),
					sizeof(newtrack->ClientAddrDotStr)-1);
			newtrack->ClientAddrPort = SOCKPORT(&(newtrack->ClientAddr));
			newtrack->ConnectTime    = gettime_microsec();
			newtrack->LastActTime    = newtrack->ConnectTime;

			/* Create AsyncComm Message handler instance. */
			AsyncCommMessageHandlerContext context =
										createConnTrackHandlerContext(newtrack);

			/* Add new client fd into AsyncComm manager. */
			res = registerFileDesc(newtrack->ClientSocket,
								   NULL,
								   ASYNCCOMM_READBYTES | ASYNCCOMM_WRITEBYTES,
								   &AsyncCommBufferHandlersMessage,
								   context,
								   &newcommbuffer);
			if ( res != FUNC_RETURN_OK )
			{
				elog(WARNING, "Resource manager can not track client FD %d. %d",
							  newtrack->ClientSocket,
							  res);
				closeConnectionRemote(&newtrack->ClientSocket);
				rm_pfree(AsyncCommContext, context);
				returnConnectionTrack(newtrack);
				return;
			}

			/* Make the connection tracker able to reference AsyncComm buffer */
			newtrack->CommBuffer = newcommbuffer;
			context->AsyncBuffer = newcommbuffer;

			transformConnectionTrackProgress(newtrack, CONN_PP_ESTABLISHED);

			elog(DEBUG3, "Resource manager accepted one client connected from "
						 "%s:%d FD %d. connection track %lx\n",
						 newtrack->ClientAddrDotStr,
						 newtrack->ClientAddrPort,
						 newtrack->ClientSocket,
						 (unsigned long)newtrack);

			/* Call callback function to initialize context for message handlers. */
			InitHandler_Message(newcommbuffer);
		}
	}
	else
	{
		/* Accept but close the connection. */
		tmptrackdata.ClientAddrLen = sizeof(tmptrackdata.ClientAddr);
		tmptrackdata.ClientSocket  = accept(buffer->FD,
											(struct sockaddr *)&(tmptrackdata.ClientAddr),
											&(tmptrackdata.ClientAddrLen));
		if ( tmptrackdata.ClientSocket != -1 )
		{
			elog(WARNING, "Resource manager cannot add more connections. Accept "
						  "but close the connection. FD %d.",
						  tmptrackdata.ClientSocket);
			closeConnectionRemote(&tmptrackdata.ClientSocket);
		}
		if ( newtrack != NULL )
		{
			returnConnectionTrack(newtrack);
		}
	}
}

void ErrorHandler_MsgServer(AsyncCommBuffer buffer)
{
	elog(WARNING, "Resource manager socket server has error raised.");
	/* AsyncComm framework will close this fd consequently. */
}

void CleanUpHandler_MsgServer(AsyncCommBuffer buffer)
{
	elog(LOG, "Clean up handler in message server is called.");
	/* No need to clean up anything. */
}

/* Register message handlers for clien FDs. */
AsyncCommMessageHandlerContext createConnTrackHandlerContext(ConnectionTrack newtrack)
{
	AsyncCommMessageHandlerContext result =
			rm_palloc0(AsyncCommContext,
					   sizeof(AsyncCommMessageHandlerContextData));

	result->inMessage				= false;
	result->UserData  				= newtrack;
	result->MessageRecvReadyHandler = NULL;
	result->MessageRecvedHandler 	= addNewMessageToConnTrack;
	result->MessageSendReadyHandler = NULL;
	result->MessageSentHandler		= sentMessageFromConnTrack;
	result->MessageErrorHandler 	= hasCommErrorInConnTrack;
	result->MessageCleanUpHandler	= cleanupConnTrack;

	elog(DEBUG3, "Created AsyncComm Message context.");

	return result;
}

