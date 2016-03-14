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

AsyncCommMessageHandlerContext createConnTrackHandlerContext(void);

void ReadReadyHandler_MsgServer(AsyncCommBuffer buffer)
{
	int					res				= FUNC_RETURN_OK;
	ConnectionTrack 	newtrack 		= NULL;
	ConnectionTrackData tmptrackdata;
	AsyncCommBuffer     newcommbuffer   = NULL;
	struct sockaddr_in	clientaddr;
	socklen_t			clientaddrlen	= sizeof(clientaddr);
	int					fd				= -1;

	/* Always accept the connection. */
	fd = accept(buffer->FD, (struct sockaddr *)&clientaddr, &clientaddrlen);
	if ( fd == -1 )
	{
		elog(WARNING, "Resource manager socket accept error is detected. errno %d",
					  errno);
		return;
	}

	/* Create AsyncComm Message handler instance. */
	AsyncCommMessageHandlerContext context = createConnTrackHandlerContext();

	/* Add new client fd into AsyncComm manager. */
	res = registerFileDesc(fd,
						   ASYNCCOMM_READBYTES | ASYNCCOMM_WRITEBYTES,
						   &AsyncCommBufferHandlersMessage,
						   context,
						   &newcommbuffer);
	if ( res != FUNC_RETURN_OK )
	{
		Assert(newcommbuffer == NULL);
		/* close the connection and cleanup. */
		closeConnectionRemote(&fd);
		rm_pfree(AsyncCommContext, context);
		return;
	}

	assignFileDescClientAddressInfo(newcommbuffer,
									NULL,
									0,
									&clientaddr,
									clientaddrlen);

	/* Let context able to track comm buffer. */
	context->AsyncBuffer = newcommbuffer;

	/* Call callback function to initialize context for message handlers. */
	InitHandler_Message(newcommbuffer);

	elog(DEBUG3, "Resource manager accepted one client connected from "
				 "%s:%d as FD %d.\n",
				 newcommbuffer->ClientAddrDotStr,
				 newcommbuffer->ClientAddrPort,
				 newcommbuffer->FD);
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
AsyncCommMessageHandlerContext createConnTrackHandlerContext(void)
{
	AsyncCommMessageHandlerContext result =
			rm_palloc0(AsyncCommContext,
					   sizeof(AsyncCommMessageHandlerContextData));

	result->inMessage				= false;
	result->UserData  				= NULL;
	result->MessageRecvReadyHandler = NULL;
	result->MessageRecvedHandler 	= addMessageToConnTrack;
	result->MessageSendReadyHandler = NULL;
	result->MessageSentHandler		= sentMessageFromConnTrack;
	result->MessageErrorHandler 	= hasCommErrorInConnTrack;
	result->MessageCleanUpHandler	= cleanupConnTrack;

	elog(DEBUG3, "Created AsyncComm Message context.");

	return result;
}

