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

#include "communication/rmcomm_MessageHandler.h"
#include "dynrm.h"
#include "communication/rmcomm_MessageProtocol.h"

int getMessageErrorInjectAction(const char *actionstr);

struct MessageIDPairData
{
	int ID;
	char IDStr[64];
};

typedef struct MessageIDPairData  MessageIDPairData;
typedef struct MessageIDPairData *MessageIDPair;

MessageIDPairData MessageIDPairs[] = {
	{REQUEST_QD_DDL_MANIPULATERESQUEUE, "REQUEST_QD_DDL_MANIPULATERESQUEUE"},
	{REQUEST_QD_DDL_MANIPULATEROLE, 	"REQUEST_QD_DDL_MANIPULATEROLE"},

	{REQUEST_QD_CONNECTION_REG,			"REQUEST_QD_CONNECTION_REG"},
	{REQUEST_QD_CONNECTION_UNREG,		"REQUEST_QD_CONNECTION_UNREG"},
	{REQUEST_QD_ACQUIRE_RESOURCE,		"REQUEST_QD_ACQUIRE_RESOURCE"},
	{REQUEST_QD_RETURN_RESOURCE,		"REQUEST_QD_RETURN_RESOURCE"},
	{REQUEST_QD_ACQUIRE_RESOURCE_PROB,	"REQUEST_QD_ACQUIRE_RESOURCE_PROB"},
	{REQUEST_QD_CONNECTION_REG_OID,		"REQUEST_QD_CONNECTION_REG_OID"},
	{REQUEST_QD_ACQUIRE_RESOURCE_QUOTA, "REQUEST_QD_ACQUIRE_RESOURCE_QUOTA"},
	{REQUEST_QD_REFRESH_RESOURCE,		"REQUEST_QD_REFRESH_RESOURCE"},
	{REQUEST_QD_SEGMENT_ISDOWN,			"REQUEST_QD_SEGMENT_ISDOWN"},
    {REQUEST_QD_TMPDIR,					"REQUEST_QD_TMPDIR"},
    {REQUEST_QD_DUMP_STATUS,			"REQUEST_QD_DUMP_STATUS"},
    {REQUEST_QD_DUMP_RESQUEUE_STATUS,	"REQUEST_QD_DUMP_RESQUEUE_STATUS"},
	{REQUEST_DUMMY,						"REQUEST_DUMMY"},
	{REQUEST_QD_QUOTA_CONTROL,			"REQUEST_QD_QUOTA_CONTROL"},

	{REQUEST_RM_RUALIVE,				"REQUEST_RM_RUALIVE"},
	{REQUEST_RM_IMALIVE,				"REQUEST_RM_IMALIVE"},
	{REQUEST_QE_MOVETOCGROUP,			"REQUEST_QE_MOVETOCGROUP"},
	{REQUEST_QE_MOVEOUTCGROUP,			"REQUEST_QE_MOVEOUTCGROUP"},
	{REQUEST_QE_SETWEIGHTCGROUP,		"REQUEST_QE_SETWEIGHTCGROUP"},
	{REQUEST_RM_INCREASE_MEMORY_QUOTA,	"REQUEST_RM_INCREASE_MEMORY_QUOTA"},
	{REQUEST_RM_DECREASE_MEMORY_QUOTA,	"REQUEST_RM_DECREASE_MEMORY_QUOTA"},
	{REQUEST_RM_TMPDIR,					"REQUEST_RM_TMPDIR"},

	{RESPONSE_QD_DDL_MANIPULATERESQUEUE,"RESPONSE_QD_DDL_MANIPULATERESQUEUE"},
	{RESPONSE_QD_DDL_MANIPULATEROLE,	"RESPONSE_QD_DDL_MANIPULATEROLE"},

	{RESPONSE_QD_CONNECTION_REG,		"RESPONSE_QD_CONNECTION_REG"},
	{RESPONSE_QD_CONNECTION_UNREG,		"RESPONSE_QD_CONNECTION_UNREG"},
	{RESPONSE_QD_ACQUIRE_RESOURCE,		"RESPONSE_QD_ACQUIRE_RESOURCE"},
	{RESPONSE_QD_RETURN_RESOURCE,		"RESPONSE_QD_RETURN_RESOURCE"},
	{RESPONSE_QD_ACQUIRE_RESOURCE_PROB, "RESPONSE_QD_ACQUIRE_RESOURCE_PROB"},
	{RESPONSE_QD_CONNECTION_REG_OID,	"RESPONSE_QD_CONNECTION_REG_OID"},
	{RESPONSE_QD_ACQUIRE_RESOURCE_QUOTA,"RESPONSE_QD_ACQUIRE_RESOURCE_QUOTA"},
	{RESPONSE_QD_REFRESH_RESOURCE,		"RESPONSE_QD_REFRESH_RESOURCE"},
	{RESPONSE_QD_SEGMENT_ISDOWN,		"RESPONSE_QD_SEGMENT_ISDOWN"},
	{RESPONSE_QD_TMPDIR,				"RESPONSE_QD_TMPDIR"},
    {RESPONSE_QD_DUMP_STATUS,			"RESPONSE_QD_DUMP_STATUS"},
    {RESPONSE_QD_DUMP_RESQUEUE_STATUS,	"RESPONSE_QD_DUMP_RESQUEUE_STATUS"},
	{RESPONSE_DUMMY,					"RESPONSE_DUMMY"},
	{RESPONSE_QD_QUOTA_CONTROL,			"RESPONSE_QD_QUOTA_CONTROL"},

	{RESPONSE_RM_RUALIVE,				"RESPONSE_RM_RUALIVE"},
	{RESPONSE_RM_IMALIVE,				"RESPONSE_RM_IMALIVE"},
	{RESPONSE_QE_MOVETOCGROUP,			"RESPONSE_QE_MOVETOCGROUP"},
	{RESPONSE_QE_MOVEOUTCGROUP,			"RESPONSE_QE_MOVEOUTCGROUP"},
	{RESPONSE_QE_SETWEIGHTCGROUP,		"RESPONSE_QE_SETWEIGHTCGROUP"},
	{RESPONSE_RM_INCREASE_MEMORY_QUOTA,	"RESPONSE_RM_INCREASE_MEMORY_QUOTA"},
	{RESPONSE_RM_DECREASE_MEMORY_QUOTA,	"RESPONSE_RM_DECREASE_MEMORY_QUOTA"},
	{RESPONSE_RM_TMPDIR,				"RESPONSE_RM_TMPDIR"},

	{RESPONSE_QE_SETWEIGHTCGROUP,		"RESPONSE_QE_SETWEIGHTCGROUP"},

	/* Never add items after this. */
	{-1,								"END"}
};

RMMessageHandlerErrorPolicyData MessageErrorAction[ERRINJ_MAXIMUM_COUNT] = {
	{ERRINJ_NO_ACTION, 				 "ERRINJ_NO_ACTION", 				ERRINJ_ACTION_UNSET},
	{ERRINJ_DISCONNECT_BEFORE_PROC,  "ERRINJ_DISCONNECT_BEFORE_PROC",	ERRINJ_ACTION_RECV},
	{ERRINJ_DISCONNECT_AFTER_PROC, 	 "ERRINJ_DISCONNECT_AFTER_PROC",	ERRINJ_ACTION_RECV},
	{ERRINJ_DISCONNECT_BEFORE_SEND,  "ERRINJ_DISCONNECT_BEFORE_SEND",	ERRINJ_ACTION_SEND},
	{ERRINJ_DISCONNECT_AFTER_SEND,   "ERRINJ_DISCONNECT_AFTER_SEND",	ERRINJ_ACTION_SEND},
	{ERRINJ_PARTIAL_SEND,			 "ERRINJ_PARTIAL_SEND",				ERRINJ_ACTION_SEND},
	{ERRINJ_PARTIAL_SEND_FRAME_TAIL, "ERRINJ_PARTIAL_SEND_FRAME_TAIL",	ERRINJ_ACTION_SEND},
	{ERRINJ_PARTIAL_SEND_FRAME_HEAD, "ERRINJ_PARTIAL_SEND_FRAME_HEAD",	ERRINJ_ACTION_SEND},
	{ERRINJ_PARTIAL_RECV,			 "ERRINJ_PARTIAL_RECV",				ERRINJ_ACTION_RECV},
	{ERRINJ_PARTIAL_RECV_FRAME_TAIL, "ERRINJ_PARTIAL_RECV_FRAME_TAIL",	ERRINJ_ACTION_RECV},
	{ERRINJ_PARTIAL_RECV_FRAME_HEAD, "ERRINJ_PARTIAL_RECV_FRAME_HEAD",	ERRINJ_ACTION_RECV}
};

RMMessageHandlerData RMMessageHandlerArray[MESSAGE_MAXIMUM_COUNT];
bool initializedRMMessageHandlerArray = false;

void initializeMessageHandlers(void)
{
	if ( initializedRMMessageHandlerArray )
	{
		return;
	}

	for ( int i = 0 ; i < MESSAGE_MAXIMUM_COUNT ; ++i )
	{
		RMMessageHandlerArray[i].ID 		  = i;
		RMMessageHandlerArray[i].IDStr[0] 	  = '\0';
		RMMessageHandlerArray[i].Handler 	  = NULL;
		RMMessageHandlerArray[i].RecvCounter  = 0;
		RMMessageHandlerArray[i].SendCounter = 0;
		RMMessageHandlerArray[i].ErrorInject.ErrorAction  = ERRINJ_NO_ACTION;
		RMMessageHandlerArray[i].ErrorInject.RecvThread   = -1;
		RMMessageHandlerArray[i].ErrorInject.SendThread  = -1;

		for ( int pairidx = 0 ; MessageIDPairs[pairidx].ID >= 0 ; pairidx++ )
		{
			if ( MessageIDPairs[pairidx].ID == i )
			{
				strncpy(RMMessageHandlerArray[i].IDStr,
						MessageIDPairs[pairidx].IDStr,
						sizeof(RMMessageHandlerArray[i].IDStr)-1);
				break;
			}
		}
	}

	initializedRMMessageHandlerArray = true;
}

void registerMessageHandler(int messageid, RMMessageHandlerType handler)
{
	initializeMessageHandlers();

	Assert( messageid >= 0  && messageid < MESSAGE_MAXIMUM_COUNT );
	RMMessageHandlerArray[messageid].Handler = handler;
	elog(DEBUG3, "Registered message handler for message %s(%d)",
				 RMMessageHandlerArray[messageid].IDStr,
				 messageid);
}

RMMessageHandlerType getMessageHandler(int messageid)
{
	initializeMessageHandlers();

	Assert( messageid >= 0  && messageid < MESSAGE_MAXIMUM_COUNT );
	return RMMessageHandlerArray[messageid].Handler;
}

int getMessageIDbyMessageIDStr(const char *messageidstr)
{
	for ( int i = 0 ; i < MESSAGE_MAXIMUM_COUNT ; ++i )
	{
		if ( strcmp(RMMessageHandlerArray[i].IDStr, messageidstr) == 0 )
		{
			elog(DEBUG3, "Found message id string matched. %s %d",
						 messageidstr, i);
			return i;
		}
	}
	return -1;
}

int getMessageErrorInjectAction(const char *actionstr)
{
	initializeMessageHandlers();

	for ( int i = 0 ; i < ERRINJ_MAXIMUM_COUNT ; ++i )
	{
		if ( strcmp(actionstr, MessageErrorAction[i].ErrorActionStr) == 0 )
		{
			return i;
		}
	}
	return -1;
}

void setMessageErrorInject(const char *messageidstr,
						   const char *actionstr,
						   int 		   countthread)
{
	initializeMessageHandlers();

	int messageid = getMessageIDbyMessageIDStr(messageidstr);

	if ( messageid >= 0 && messageid < MESSAGE_MAXIMUM_COUNT )
	{
		int erraction = getMessageErrorInjectAction(actionstr);
		if ( erraction < 0 )
		{
			elog(WARNING, "Ignore unrecognized error injection action %s",
						  actionstr);
			return;
		}
		RMMessageHandlerArray[messageid].ErrorInject.ErrorAction = erraction;

		if ( MessageErrorAction[erraction].ActionType == ERRINJ_ACTION_RECV )
		{
			RMMessageHandlerArray[messageid].ErrorInject.RecvThread  = countthread;
		}
		else if ( MessageErrorAction[erraction].ActionType == ERRINJ_ACTION_SEND )
		{
			RMMessageHandlerArray[messageid].ErrorInject.SendThread = countthread;
		}

		elog(LOG, "Set error injection for message %s, action %s, "
				  "thread recv %d, send %d.",
				  messageidstr,
				  actionstr,
				  RMMessageHandlerArray[messageid].ErrorInject.RecvThread,
				  RMMessageHandlerArray[messageid].ErrorInject.SendThread);
	}
	else
	{
		elog(WARNING, "Ignore unrecognized message id %s for setting error "
					  "injection action",
					  messageidstr);
	}
}

int countMessageForErrorAction(int messageid, bool read, bool beforeaction)
{
	initializeMessageHandlers();

	RMMessageHandler msghandler = &RMMessageHandlerArray[messageid];
	if ( beforeaction )
	{
		msghandler->RecvCounter += read ? 1 : 0;
		msghandler->SendCounter += read ? 0 : 1;

		elog(DEBUG3, "%s message %d count is updated to recv: %d send: %d.",
					 read ? "Recv " : "Send ",
					 messageid,
					 msghandler->RecvCounter,
					 msghandler->SendCounter);
	}

	if ( msghandler->ErrorInject.ErrorAction == ERRINJ_NO_ACTION )
	{
		return ERRINJ_NO_ACTION;
	}

	if (( read && msghandler->RecvCounter == msghandler->ErrorInject.RecvThread) ||
		(!read && msghandler->SendCounter == msghandler->ErrorInject.SendThread))
	{
		return msghandler->ErrorInject.ErrorAction;
	}

	return ERRINJ_NO_ACTION;
}

void performMessageForErrorAction(int 			  messageid,
								  bool 			  read,
								  bool 			  beforeproc,
								  AsyncCommBuffer buffer)
{
	initializeMessageHandlers();

	/* Get action. */
	buffer->forceErrorAction = countMessageForErrorAction(messageid,
														  read,
														  beforeproc);
	if ( buffer->forceErrorAction != ERRINJ_NO_ACTION )
	{
		elog(LOG, "Error inject action %s(%d) is triggered.",
				  MessageErrorAction[buffer->forceErrorAction].ErrorActionStr,
				  buffer->forceErrorAction);
	}
}

void performErrorActionForWritingMessage(AsyncCommBuffer buffer, bool beforeact)
{
	initializeMessageHandlers();

	/* Expect the message head is saved in the first send buffer. */
	SelfMaintainBuffer firstbuff = getFirstWriteBuffer(buffer);
	if ( firstbuff == NULL )
	{
		return;
	}

	RMMessageHead head = (RMMessageHead)(firstbuff->Buffer);
	performMessageForErrorActionForWritingMessageByID(head->MessageID,
													  beforeact,
													  buffer);
}

void performMessageForErrorActionForWritingMessageByID(int  		   messageid,
													   bool 		   beforeact,
													   AsyncCommBuffer buffer)
{
	buffer->forceErrorAction = countMessageForErrorAction(messageid,
														  false,
														  beforeact);
	if ( buffer->forceErrorAction != ERRINJ_NO_ACTION )
	{
		elog(LOG, "Error inject action %s(%d) is triggered.",
				  MessageErrorAction[buffer->forceErrorAction].ErrorActionStr,
				  buffer->forceErrorAction);
	}
	/* Perform action here. */
	switch(buffer->forceErrorAction)
	{
	case ERRINJ_DISCONNECT_BEFORE_PROC:
	case ERRINJ_DISCONNECT_BEFORE_SEND:
		if ( beforeact )
		{
			forceCloseFileDesc(buffer);
		}
		break;
	case ERRINJ_DISCONNECT_AFTER_SEND:
	case ERRINJ_DISCONNECT_AFTER_PROC:
		if ( !beforeact )
		{
			forceCloseFileDesc(buffer);
		}
		break;
	case ERRINJ_PARTIAL_SEND:
		/* only send half of message content. */
		if ( beforeact )
		{
			/* Get the first send buffer and cut content directly. */
			SelfMaintainBuffer sendbuffer = getFirstWriteBuffer(buffer);
			if ( sendbuffer != NULL &&
				 buffer->WriteContentSize == buffer->WriteContentOriginalSize )
			{
				RMMessageHead head = (RMMessageHead)sendbuffer->Buffer;
				int sizeleft = sizeof(RMMessageHeadData) +
							   (head->MessageSize >> 1);
				resetSelfMaintainBufferCursor(sendbuffer, sizeleft-1);
				buffer->WriteContentSize = sizeleft;
				elog(LOG, "Cut send content size from %d to %d after cutting "
						  "message.",
						  buffer->WriteContentOriginalSize,
						  buffer->WriteContentSize);
			}
			closeFileDesc(buffer);
		}
		break;
	case ERRINJ_PARTIAL_SEND_FRAME_TAIL:
		/* only send half of message tail. */
		if ( beforeact )
		{
			/* Get the first send buffer and cut content directly. */
			SelfMaintainBuffer sendbuffer = getFirstWriteBuffer(buffer);
			if ( sendbuffer != NULL &&
				 buffer->WriteContentSize == buffer->WriteContentOriginalSize )
			{
				RMMessageHead head = (RMMessageHead)sendbuffer->Buffer;
				int sizeleft = sizeof(RMMessageHeadData) +
							   head->MessageSize +
							   (sizeof(RMMessageTailData) >> 1);
				resetSelfMaintainBufferCursor(sendbuffer, sizeleft-1);
				buffer->WriteContentSize = sizeleft;
				elog(LOG, "Cut send content size from %d to %d after cutting "
						  "message frame tail tag.",
						  buffer->WriteContentOriginalSize,
						  buffer->WriteContentSize);

			}
			closeFileDesc(buffer);
		}
		break;
	case ERRINJ_PARTIAL_SEND_FRAME_HEAD:
		/* only send half of message head. */
		if ( beforeact )
		{
			/* Get the first send buffer and cut content directly. */
			SelfMaintainBuffer sendbuffer = getFirstWriteBuffer(buffer);
			if ( sendbuffer != NULL &&
				 buffer->WriteContentSize == buffer->WriteContentOriginalSize )
			{
				int sizeleft = sizeof(RMMessageHeadData) >> 1;
				resetSelfMaintainBufferCursor(sendbuffer, sizeleft-1);
				buffer->WriteContentSize = sizeleft;
				elog(LOG, "Cut send content size from %d to %d after cutting "
						  "message frame tail head.",
						  buffer->WriteContentOriginalSize,
						  buffer->WriteContentSize);
			}
			closeFileDesc(buffer);
		}
		break;
	case ERRINJ_PARTIAL_RECV:

		break;
	case ERRINJ_PARTIAL_RECV_FRAME_TAIL:
		break;
	case ERRINJ_PARTIAL_RECV_FRAME_HEAD:
		break;
	}
}
