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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MSGHANDLER_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MSGHANDLER_H

#include "envswitch.h"
#include "utils/simplestring.h"
#include "communication/rmcomm_AsyncComm.h"

enum RM_MESSAGE_ID {
	/* DDL request from QD to RM */
	REQUEST_QD_DDL_BEGINTAG									= 0,
	REQUEST_QD_DDL_MANIPULATERESQUEUE,
	REQUEST_QD_DDL_MANIPULATEROLE,

	/* Resource request from QD to RM */
	REQUEST_QD_BEGINTAG										= 256,
	REQUEST_QD_CONNECTION_REG,
	REQUEST_QD_CONNECTION_UNREG,
	REQUEST_QD_ACQUIRE_RESOURCE,
	REQUEST_QD_RETURN_RESOURCE,
	REQUEST_QD_ACQUIRE_RESOURCE_PROB,
	REQUEST_QD_CONNECTION_REG_OID,
	REQUEST_QD_ACQUIRE_RESOURCE_QUOTA,
	REQUEST_QD_REFRESH_RESOURCE,
	REQUEST_QD_SEGMENT_ISDOWN,
    REQUEST_QD_TMPDIR,
    REQUEST_QD_DUMP_STATUS,
    REQUEST_QD_DUMP_RESQUEUE_STATUS,
	REQUEST_DUMMY,
	REQUEST_QD_QUOTA_CONTROL,

	/* Request between RM and RMSEG */
	REQUEST_RM_BEGINTAG										= 512,
	REQUEST_RM_RUALIVE,
	REQUEST_RM_IMALIVE,
	REQUEST_QE_MOVETOCGROUP,
	REQUEST_QE_MOVEOUTCGROUP,
	REQUEST_QE_SETWEIGHTCGROUP,
	REQUEST_RM_INCREASE_MEMORY_QUOTA,
	REQUEST_RM_DECREASE_MEMORY_QUOTA,
	REQUEST_RM_TMPDIR,

	/* DDL response from RM to QD */
	RESPONSE_QD_DDL_BEGINTAG								= 2048,
	RESPONSE_QD_DDL_MANIPULATERESQUEUE,
	RESPONSE_QD_DDL_MANIPULATEROLE,

	/* Resource response from RM to QD */
	RESPONSE_QD_BEGINTAG									= 2304,
	RESPONSE_QD_CONNECTION_REG,
	RESPONSE_QD_CONNECTION_UNREG,
	RESPONSE_QD_ACQUIRE_RESOURCE,
	RESPONSE_QD_RETURN_RESOURCE,
	RESPONSE_QD_ACQUIRE_RESOURCE_PROB,
	RESPONSE_QD_CONNECTION_REG_OID,
	RESPONSE_QD_ACQUIRE_RESOURCE_QUOTA,
	RESPONSE_QD_REFRESH_RESOURCE,
	RESPONSE_QD_SEGMENT_ISDOWN,
	RESPONSE_QD_TMPDIR,
    RESPONSE_QD_DUMP_STATUS,
    RESPONSE_QD_DUMP_RESQUEUE_STATUS,
	RESPONSE_DUMMY,
	RESPONSE_QD_QUOTA_CONTROL,

	/* Response between RM and RMSEG */
	RESPONSE_RM_BEGINTAG									= 2560,
	RESPONSE_RM_RUALIVE,
	RESPONSE_RM_IMALIVE,
	RESPONSE_QE_MOVETOCGROUP,
	RESPONSE_QE_MOVEOUTCGROUP,
	RESPONSE_QE_SETWEIGHTCGROUP,
	RESPONSE_RM_INCREASE_MEMORY_QUOTA,
	RESPONSE_RM_DECREASE_MEMORY_QUOTA,
	RESPONSE_RM_TMPDIR,

	/* Never add items after this. */
	MESSAGE_MAXIMUM_COUNT = 4096
};

enum RM_MESSAGE_HANDLER_ERROR_INJECT_ACTION
{
	ERRINJ_NO_ACTION,
	/* Disconnect actively after receiving message, but before process it. */
	ERRINJ_DISCONNECT_BEFORE_PROC,
	/* Disconnect actively after process it. */
	ERRINJ_DISCONNECT_AFTER_PROC,
	/* Disconnect actively before sending message out. */
	ERRINJ_DISCONNECT_BEFORE_SEND,
	/* Disconnect actively after sending message out. */
	ERRINJ_DISCONNECT_AFTER_SEND,
	/* Send partial content containing complete message head. Then actively close.*/
	ERRINJ_PARTIAL_SEND,
	/* Send partial content containing partial message tail. Then actively close. */
	ERRINJ_PARTIAL_SEND_FRAME_TAIL,
	/* Send partial content containing partial message head. Then actively close. */
	ERRINJ_PARTIAL_SEND_FRAME_HEAD,
	/* Receive partial content containing complete message head. Then actively close. */
	ERRINJ_PARTIAL_RECV,
	/* Receive partial content containing partial message tail. Then actively close. */
	ERRINJ_PARTIAL_RECV_FRAME_TAIL,
	/* Receive partial content containing partial message head. Then actively close. */
	ERRINJ_PARTIAL_RECV_FRAME_HEAD,

	/* Never add items after this. */
	ERRINJ_MAXIMUM_COUNT
};

enum RM_MESSAGE_HANDLER_ERROR_INJECT_ACTION_TYPE
{
	ERRINJ_ACTION_UNSET,
	ERRINJ_ACTION_RECV,
	ERRINJ_ACTION_SEND
};

struct RMMessageHandlerErrorPolicyData
{
	int  ErrorAction;
	char ErrorActionStr[128];
	enum RM_MESSAGE_HANDLER_ERROR_INJECT_ACTION_TYPE ActionType;
};

typedef struct RMMessageHandlerErrorPolicyData  RMMessageHandlerErrorPolicyData;
typedef struct RMMessageHandlerErrorPolicyData *RMMessageHandlerErrorPolicy;

struct RMMessageHandlerErrorInjectData
{
	int	RecvThread;
	int	SendThread;
	int ErrorAction;
};

typedef struct RMMessageHandlerErrorInjectData  RMMessageHandlerErrorInjectData;
typedef struct RMMessageHandlerErrorInjectData *RMMessageHandlerErrorInject;

typedef bool (* RMMessageHandlerType)(void **);

struct RMMessageHandlerData
{
	int 					ID;
	char					IDStr[64];
	RMMessageHandlerType	Handler;
	int						RecvCounter;
	int						SendCounter;

	/* Error injection */
	RMMessageHandlerErrorInjectData	ErrorInject;
};

typedef struct RMMessageHandlerData  RMMessageHandlerData;
typedef struct RMMessageHandlerData *RMMessageHandler;

void initializeMessageHandlers(void);
void registerMessageHandler(int messageid, RMMessageHandlerType handler);
RMMessageHandlerType getMessageHandler(int messageid);
int getMessageIDbyMessageIDStr(const char *messageidstr);

void setMessageErrorInject(const char *messageidstr,
						   const char *actionstr,
						   int 		   countthread);

int countMessageForErrorAction(int messageid, bool read, bool beforeaction);

/* Process the message already recognized directly. */
void performMessageForErrorAction(int 			  messageid,
								  bool 			  read,
								  bool 			  beforeproc,
								  AsyncCommBuffer buffer);

/* Recognize the message to send and perform the action. */
void performErrorActionForWritingMessage(AsyncCommBuffer buffer, bool beforeaction);
void performMessageForErrorActionForWritingMessageByID(int  		   messageid,
													   bool 		   beforeact,
													   AsyncCommBuffer buffer);

#define CAN_PROCESS_RECEIVED(erract)										   \
	((erract) == ERRINJ_NO_ACTION ||										   \
	 (erract) == ERRINJ_DISCONNECT_AFTER_PROC ||							   \
	 (erract) == ERRINJ_PARTIAL_RECV ||										   \
	 (erract) == ERRINJ_PARTIAL_RECV_FRAME_HEAD ||							   \
	 (erract) == ERRINJ_PARTIAL_RECV_FRAME_TAIL)

#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MSGHANDLER_H*/
