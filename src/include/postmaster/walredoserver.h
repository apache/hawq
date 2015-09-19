/*-------------------------------------------------------------------------
 *
 * walredoserver.c
 *	  Process under QD postmaster that manages redo of the WAL by the standby.
 *
 *
 * Copyright (c) 2007-2008, Greenplum inc
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


