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

#include "resourcemanager/communication/rmcomm_QE2RMSEG.h"

bool QE2RMSEG_Initialized = false;

void initializeQE2RMSEGComm(void);

void initializeQE2RMSEGComm(void)
{
	if ( !QE2RMSEG_Initialized )
	{
		initializeSyncRPCComm();
		QE2RMSEG_Initialized = true;
	}
}

/**
 * Move the QE PID to corresponding CGroup
 */
int
MoveToCGroupForQE(TimestampTz masterStartTime,
				  int connId,
				  int segId,
				  int procId,
				  char *errorbuf,
				  int errorbufsize)
{
#ifdef __linux
	initializeQE2RMSEGComm();

	int			res			= FUNC_RETURN_OK;

	const char	*serverHost	= "127.0.0.1";
	uint16_t	serverPort	= rm_segment_port;

	SelfMaintainBuffer sendBuffer = createSelfMaintainBuffer(CurrentMemoryContext);
	SelfMaintainBuffer recvBuffer = createSelfMaintainBuffer(CurrentMemoryContext);

	/* Build request */
	RPCRequestMoveToCGroupData request;
	request.MasterStartTime = masterStartTime;
	request.ConnID          = connId;
	request.SegmentID		= segId;
	request.ProcID			= procId;
	appendSMBVar(sendBuffer, request);

	/* Send request */
	res = callSyncRPCRemote(serverHost,
	                        serverPort,
	                        sendBuffer->Buffer,
	                        sendBuffer->Cursor+1,
	                        REQUEST_QE_MOVETOCGROUP,
	                        RESPONSE_QE_MOVETOCGROUP,
	                        recvBuffer,
							errorbuf,
							errorbufsize);

	deleteSelfMaintainBuffer(sendBuffer);
	deleteSelfMaintainBuffer(recvBuffer);
	return res;
#endif
	return FUNC_RETURN_OK;
}

/**
 * Move the QE PID out of corresponding CGroup
 */
int
MoveOutCGroupForQE(TimestampTz masterStartTime,
				   int connId,
				   int segId,
				   int procId,
				   char *errorbuf,
				   int errorbufsize)
{
#ifdef __linux
	initializeQE2RMSEGComm();

	int			res			= FUNC_RETURN_OK;

	const char	*serverHost	= "127.0.0.1";
	uint16_t	serverPort	= rm_segment_port;

	SelfMaintainBuffer sendBuffer = createSelfMaintainBuffer(CurrentMemoryContext);
	SelfMaintainBuffer recvBuffer = createSelfMaintainBuffer(CurrentMemoryContext);

	/* Build request */
	RPCRequestMoveOutCGroupData request;
	request.MasterStartTime = masterStartTime;
	request.ConnID          = connId;
	request.SegmentID		= segId;
	request.ProcID			= procId;
	appendSMBVar(sendBuffer, request);

	/* Send request */
	res = callSyncRPCRemote(serverHost,
	                        serverPort,
	                        sendBuffer->Buffer,
	                        sendBuffer->Cursor+1,
	                        REQUEST_QE_MOVEOUTCGROUP,
	                        RESPONSE_QE_MOVEOUTCGROUP,
	                        recvBuffer,
							errorbuf,
							errorbufsize);

	deleteSelfMaintainBuffer(sendBuffer);
	deleteSelfMaintainBuffer(recvBuffer);
	return res;
#endif
	return FUNC_RETURN_OK;
}
  
/**
 * Set CPU share weight for corresponding CGroup
 */
int
SetWeightCGroupForQE(TimestampTz masterStartTime,
					 int connId,
					 int segId,
					 QueryResource *resource,
					 int procId,
					 char *errorbuf,
					 int errorbufsize)
{
#ifdef __linux
	initializeQE2RMSEGComm();

	int			res			= FUNC_RETURN_OK;

	const char	*serverHost	= "127.0.0.1";
	uint16_t	serverPort	= rm_segment_port;

	SelfMaintainBuffer sendBuffer = createSelfMaintainBuffer(CurrentMemoryContext);
	SelfMaintainBuffer recvBuffer = createSelfMaintainBuffer(CurrentMemoryContext);

	/* Build request */
	RPCRequestSetWeightCGroupData request;
	request.MasterStartTime = masterStartTime;
	request.ConnID			= connId;
	request.SegmentID		= segId;
	request.ProcID			= procId;
	request.Weight			= resource->segment_vcore;
	appendSMBVar(sendBuffer, request);

	/* Send request */
	res = callSyncRPCRemote(serverHost,
	                        serverPort,
	                        sendBuffer->Buffer,
	                        sendBuffer->Cursor+1,
	                        REQUEST_QE_SETWEIGHTCGROUP,
	                        RESPONSE_QE_SETWEIGHTCGROUP,
	                        recvBuffer,
							errorbuf,
							errorbufsize);

	deleteSelfMaintainBuffer(sendBuffer);
	deleteSelfMaintainBuffer(recvBuffer);
	return res;
#endif
	return FUNC_RETURN_OK;
}

