/*-------------------------------------------------------------------------
 *
 * walredoserver.c
 *	  Process under QD postmaster that manages redo of the WAL by the standby.
 *
 *
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
 *
 *-------------------------------------------------------------------------
 */
#ifndef WALREDOSERVER_H
#define WALREDOSERVER_H

typedef enum WalRedoRequestCommand
{
	NewCheckpointLocation = 0,
	Quiesce,
	MaxWalRedoRequestCommand /* must always be last */
} WalRedoRequestCommand;

typedef struct WalRedoRequest
{
	WalRedoRequestCommand	command;

	XLogRecPtr 	newCheckpointLocation;
} WalRedoRequest;

typedef enum WalRedoResponseCommand
{
	Quiesced = 0,
	RedoUnexpectedError,
	MaxWalRedoResponseCommand /* must always be last */
} WalRedoResponseCommand;

typedef struct WalRedoResponse
{
	WalRedoResponseCommand	response;
	
} WalRedoResponse;

extern int	walredoserver_start(void);
extern bool IsWalRedoProcess(void);
extern int  WalRedoServerShmemSize(void);
extern void WalRedoServerShmemInit(void);
extern bool WalRedoServerClientConnect(void);
extern bool WalRedoServerClientSendRequest(WalRedoRequest* walRedoRequest);
extern bool WalRedoServerClientReceiveResponse(WalRedoResponse* walRedoResponse, struct timeval *timeout);
extern bool WalRedoServerClientPollResponse(WalRedoResponse* walRedoResponse, bool *pollResponseReceived);
extern bool  WalRedoServerNewCheckpointLocation(XLogRecPtr *newCheckpointLocation);
extern bool WalRedoServerQuiesce(void);
extern char* WalRedoRequestCommandToString(WalRedoRequestCommand command);
extern char* WalRedoResponseCommandToString(WalRedoResponseCommand command);

#endif   /* WALREDOSERVER_H */


