/*-------------------------------------------------------------------------
 *
 * walsendserver.c
 *	  Process under QD postmaster that manages sending WAL to the standby.
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
#ifndef WALSENDSERVER_H
#define WALSENDSERVER_H

#include "access/xlogdefs.h"

typedef enum WalSendRequestCommand
{
	PositionToEnd = 0,
	Catchup,
	WriteWalPages,
	FlushWalPages,
	CloseForShutdown,
	MaxWalSendRequestCommand /* must always be last */
} WalSendRequestCommand;

typedef struct WalSendRequest
{
	WalSendRequestCommand	command;

	/*
	 * Data for WriteWalPages.
	 */
	int			startidx;	/* Index to the start page the in WAL buffer pool */
	int			npages;		/* Number of pages to write */

	TimeLineID	timeLineID;
	uint32 		logId;
	uint32 		logSeg;
	uint32 		logOff;
	bool 		haveNewCheckpointLocation;
	XLogRecPtr 	newCheckpointLocation;

	/*
	 * Data for MirrorQDEnabled.
	 */
	XLogRecPtr	flushedLocation;
} WalSendRequest;

typedef struct WalSendResponse
{
	bool	ok;
	
} WalSendResponse;

extern int	walsendserver_start(void);
extern bool IsWalSendProcess(void);

extern void WalSendServerShmemInit(void);
extern int WalSendServerShmemSize(void);
extern void WalSendServerGetClientTimeout(struct timeval *timeout);
extern bool WalSendServerClientConnect(bool complain);
extern bool WalSendServerClientSendRequest(WalSendRequest* walSendRequest);
extern bool WalSendServerClientReceiveResponse(WalSendResponse* walSendResponse, struct timeval *timeout);
extern char* WalSendRequestCommandToString(WalSendRequestCommand command);


#endif   /* WALSENDSERVER_H */

