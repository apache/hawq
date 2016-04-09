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

#include "communication/rmcomm_Message.h"
#include "communication/rmcomm_MessageHandler.h"

AsyncCommBufferHandlersData AsyncCommBufferHandlersMessage = {
	NULL,
	ReadReadyHandler_Message,
	ReadPostHandler_Message,
	WriteReadyHandler_Message,
	WritePostHandler_Message,
	ErrorHandler_Message,
	CleanUpHandler_Message
};

void InitHandler_Message(AsyncCommBuffer buffer)
{

}

void ReadReadyHandler_Message(AsyncCommBuffer buffer)
{
	AsyncCommMessageHandlerContext context = (AsyncCommMessageHandlerContext)
											 (buffer->UserData);
	if ( context->MessageRecvReadyHandler != NULL )
	{
		context->MessageRecvReadyHandler(context);
	}
}

void ReadPostHandler_Message(AsyncCommBuffer buffer)
{
	AsyncCommMessageHandlerContext context = (AsyncCommMessageHandlerContext)
											 (buffer->UserData);
	int msgsize = getSMBContentSize(&(buffer->ReadBuffer));
	if ( !context->inMessage )
	{
		/* Check if having message head. */
		if ( msgsize >= DRM_MSGFRAME_HEADTAGSIZE )
		{
			char *p = buffer->ReadBuffer.Buffer;
			if ( DRM_MSGFRAME_HEADTAG_MATCHED(p) )
			{
				context->inMessage = true;
			}
			else
			{
				/*
				 * This protocol does not allow content not wrapped by message
				 * framework, therefore, here we force to close the connection.
				 * No more processing is needed.
				 */
				forceCloseFileDesc(buffer);
				elog(WARNING, "AsyncComm framework received invalid message head. "
							  "Close the connection FD %d.",
							  buffer->FD);
				return;
			}
		}
		else
		{
			/* Not a complete message frame head tag. Wait for new content. */
			return;
		}
	}

	/*
	 * If we are in one message, and its header was completed received, we parse
	 * the header to get the expected message content.
	 */
	if ( context->inMessage && msgsize >= DRM_MSGFRAME_HEADSIZE )
	{
		/* Check if achieve the end of message. */
		RMMessageHead header = (RMMessageHead)(buffer->ReadBuffer.Buffer);
		if ( DRM_MSGFRAME_TOTALSIZE(header) <= msgsize )
		{
			char *p = buffer->ReadBuffer.Buffer + header->MessageSize +
					  DRM_MSGFRAME_HEADSIZE;
			if ( DRM_MSGFRAME_TAILTAG_MATCHED(p) )
			{
				/* Skip heart-beat message log. */
				elog(RMLOG, "AsyncComm framework receives message %d from FD %d",
							header->MessageID,
							buffer->FD);

				/* Get complete message and call the handler. */
				if ( context->MessageRecvedHandler != NULL )
				{
					/* Check and perform error injection action. */
					performMessageForErrorAction(header->MessageID,
												 true,	/* read content.      */
												 true,	/* before processing. */
												 buffer);

					if ( CAN_PROCESS_RECEIVED(buffer->forceErrorAction) )
					{
						header = (RMMessageHead)(buffer->ReadBuffer.Buffer);
						context->MessageRecvedHandler(context,
													  header->MessageID,
													  header->Mark1,
													  header->Mark2,
													  buffer->ReadBuffer.Buffer +
														  DRM_MSGFRAME_HEADSIZE,
													  header->MessageSize);
					}

					/* Check and perform error injection action. */
					performMessageForErrorAction(header->MessageID,
												 true,	/* read content.      */
												 false,	/* after processing.  */
												 buffer);
				}
				/* Shift out this message */
				shiftLeftSelfMaintainBuffer(&(buffer->ReadBuffer),
											DRM_MSGFRAME_TOTALSIZE(header));
				/* Set out of the message */
				context->inMessage = false;
			}
			else
			{
				/* We get wrong message content. */
				elog(WARNING, "AsyncComm framework received wrong message tail "
							  "content.");
				forceCloseFileDesc(buffer);
			}
		}
	}
}

void WriteReadyHandler_Message(AsyncCommBuffer buffer)
{
	AsyncCommMessageHandlerContext context = (AsyncCommMessageHandlerContext)
											 (buffer->UserData);

	if ( context->MessageSendReadyHandler != NULL )
	{
		context->MessageSendReadyHandler(context);
	}

	performErrorActionForWritingMessage(buffer, true);

}

void WritePostHandler_Message(AsyncCommBuffer buffer)
{
	AsyncCommMessageHandlerContext context = (AsyncCommMessageHandlerContext)
											 (buffer->UserData);
	int msgsize = getSMBContentSize(&(buffer->ReadBuffer));
	if ( msgsize == 0 )
	{
		Assert( context->MessageSentHandler != NULL );
		context->MessageSentHandler(context);
	}

	elog(DEBUG3, "Write post handler for client message is called.");

	performErrorActionForWritingMessage(buffer, false);
}

void ErrorHandler_Message(AsyncCommBuffer buffer)
{
	AsyncCommMessageHandlerContext context = (AsyncCommMessageHandlerContext)
											 (buffer->UserData);
	Assert( context->MessageErrorHandler != NULL );
	context->MessageErrorHandler(context);
}

void CleanUpHandler_Message(AsyncCommBuffer buffer)
{
	AsyncCommMessageHandlerContext context = (AsyncCommMessageHandlerContext)
											 (buffer->UserData);
	Assert( context->MessageCleanUpHandler != NULL );
	context->MessageCleanUpHandler(context);
	rm_pfree(AsyncCommContext, context);
	elog(DEBUG3, "Freed AsyncComm Message context.");
}

void buildMessageToCommBuffer(AsyncCommBuffer   buffer,
							  const char 	   *content,
							  int   	 		contentsize,
							  uint16_t 	 		messageid,
							  uint8_t    		mark1,
							  uint8_t    		mark2)
{
	static char messagehead[DRM_MSGFRAME_HEADSIZE]   = DRM_MSGFRAME_HEADTAG_STR;
	static char messagetail[DRM_MSGFRAME_TAILSIZE+1] = DRM_MSGFRAME_TAILTAG_STR;
	RMMessageHead phead = (RMMessageHead)messagehead;
	RMMessageTail ptail = (RMMessageTail)messagetail;

	/* Append message head. */
	phead->Mark1       = mark1;
	phead->Mark2       = mark2;
	phead->MessageID   = messageid;
	phead->MessageSize = contentsize;

	SelfMaintainBuffer newbuff = createSelfMaintainBuffer(AsyncCommContext);

	appendSelfMaintainBuffer(newbuff, (char *)phead, sizeof(*phead));
	/* Append actual message content. */
	appendSelfMaintainBuffer(newbuff, (char *)content, contentsize);
	/* Append message tail. */
	appendSelfMaintainBuffer(newbuff, (char *)ptail, sizeof(*ptail));

	addMessageContentToCommBuffer(buffer, newbuff);
}
