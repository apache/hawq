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
#include "postgres.h"


#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <netinet/in.h>
#include "utils/elog.h"


#include "miscadmin.h"
#include "libpq/pqsignal.h"
#include "cdb/cdbvars.h"
#include "commands/sequence.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "catalog/pg_control.h"
#include "postmaster/service.h"
#include "postmaster/walredoserver.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/pmsignal.h"			/* PostmasterIsAlive */
#include "storage/sinval.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"
#include "cdb/cdbutil.h"
#include "cdb/cdblink.h"
#include "access/xlog_internal.h"
#include "utils/memutils.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "cdb/cdblogsync.h"


/*=========================================================================
 * FUNCTIONS PROTOTYPES
 */


#ifdef EXEC_BACKEND
static pid_t walredoserver_forkexec(void);
#endif

static void WalRedoServerInitContext(void);

/*
 * The following are standard procedure signatures needed by
 * the Service shared library.
 */
static void WalRedoServer_ServiceRequestShutdown(SIGNAL_ARGS);
static void WalRedoServer_ServiceEarlyInit(void);
static void WalRedoServer_ServicePostgresInit(void);
static void WalRedoServer_ServiceInit(int listenerPort);
static bool WalRedoServer_ServiceShutdownRequested(void);
static bool WalRedoServer_ServiceRequest(ServiceCtrl *serviceCtrl, int sockfd, uint8 *request);
static void WalRedoServer_ServiceShutdown(void);


/*=========================================================================
 * GLOBAL STATE VARIABLES
 */

static bool am_walredoserver = false;

static ServiceConfig WalRedoServer_ServiceConfig =
	{"WALRDO",
	"WAL Redo Server", "WAL Redo Server process", "walredoserver",
	sizeof(WalRedoRequest), sizeof(WalRedoResponse),
	NULL,	// Default client timeout.
	WalRedoServer_ServiceRequestShutdown,
	WalRedoServer_ServiceEarlyInit,
	WalRedoServer_ServicePostgresInit,
	WalRedoServer_ServiceInit,
	WalRedoServer_ServiceShutdownRequested,
	WalRedoServer_ServiceRequest,
	WalRedoServer_ServiceShutdown,
	NULL};	// No PostmasterDied callback.

static ServiceCtrl WalRedoServer_ServiceCtrl;

/*=========================================================================
 * GLOBAL STATE VARIABLES
 */

typedef struct WalRedoServerShmem
{
	int			listenerPort;
}	WalRedoServerShmem;


static WalRedoServerShmem *WalRedoServerShared = NULL;

/*=========================================================================
 * VISIBLE FUNCTIONS
 */

int 
WalRedoServerShmemSize(void)
{
	
	return sizeof(WalRedoServerShmem);
}


void 
WalRedoServerShmemInit(void)
{
	bool		found;
	
	
	/* Create or attach to the SharedSnapshot shared structure */
	WalRedoServerShared = (WalRedoServerShmem *) ShmemInitStruct("WAL Redo Server", WalRedoServerShmemSize(), &found);

	if (!found)
	{
		WalRedoServerShared->listenerPort = -1;
	}
}

bool 
WalRedoServerNewCheckpointLocation(XLogRecPtr *newCheckpointLocation)
{
	WalRedoRequest walRedoRequest;
	WalRedoResponse walRedoResponse;
	bool connected;
	bool successful;
	bool pollResponseReceived;

	MemSet(&walRedoRequest, 0, sizeof(walRedoRequest));
	walRedoRequest.command = NewCheckpointLocation;
	walRedoRequest.newCheckpointLocation= *newCheckpointLocation;

	connected = WalRedoServerClientConnect();
	if (!connected)
	{
		elog(LOG, "cannot connect to WAL Redo server)");

		return false;
	}

	successful = WalRedoServerClientSendRequest(&walRedoRequest);

	if (successful)
	{
		/*
		 * See if the optional redo server error message was sent...
		 */
		successful = WalRedoServerClientPollResponse(&walRedoResponse, &pollResponseReceived);
		if (pollResponseReceived)
		{
			Assert(walRedoResponse.response == RedoUnexpectedError);
			elog(LOG,"redo encountered an unexpected error");
			successful = false;
		}
	}

	return successful;
}

bool
WalRedoServerQuiesce(void)
{
	WalRedoRequest walRedoRequest;
	WalRedoResponse walRedoResponse;
	bool connected;
	bool successful;

	elog(LOG,"quiesce");
	
	MemSet(&walRedoRequest, 0, sizeof(walRedoRequest));
	walRedoRequest.command = Quiesce;

	connected = WalRedoServerClientConnect();
	if (!connected)
	{
		elog(LOG, "cannot connect to WAL Redo server)");

		return false;
	}

	successful = WalRedoServerClientSendRequest(&walRedoRequest);
	if (successful)
	{
		struct timeval clientTimeout;

		ServiceGetClientTimeout(&WalRedoServer_ServiceConfig, &clientTimeout);

		/*
		 * See if we received a quiesced response.
		 */
		successful = WalRedoServerClientReceiveResponse(&walRedoResponse, &clientTimeout);
		if (successful)
		{
//			elog(LOG,"received response %s",
//				 WalRedoResponseCommandToString(walRedoResponse.response));
		}
	}

	return successful;
}

/*
 * Main entry point for walredoserver controller process.
 *
 * This code is heavily based on pgarch.c, q.v.
 */
int
walredoserver_start(void)
{

	pid_t		WalRedoServerPID;

	
#ifdef EXEC_BACKEND
	switch ((WalRedoServerPID = walredoserver_forkexec()))
#else
	switch ((WalRedoServerPID = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork walredoserver process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			ServiceInit(&WalRedoServer_ServiceConfig, &WalRedoServer_ServiceCtrl);
			ServiceMain(&WalRedoServer_ServiceCtrl);
			break;
#endif
		default:
			return (int) WalRedoServerPID;
	}

	
	/* shouldn't get here */
	return 0;
}

/*=========================================================================
 * HELPER FUNCTIONS
 */


#ifdef EXEC_BACKEND
/*
 * walredoserver_forkexec()
 *
 * Format up the arglist for the serqserver process, then fork and exec.
 */
static pid_t
walredoserver_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkwalredoserver";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif   /* EXEC_BACKEND */

bool walredo_shutdown_requested=false;

static void
WalRedoServer_ServiceRequestShutdown(SIGNAL_ARGS)
{
	walredo_shutdown_requested = true;
}

static void
WalRedoServer_ServiceEarlyInit(void)
{
	// UNDONE: Kludge that allow initialization of components for recovery purposes.
	SetProcessingMode(BootstrapProcessing);
}

static void
WalRedoServer_ServicePostgresInit(void)
{
	/* See InitPostgres()... */
    InitProcess();	
	InitBufferPoolBackend();
	InitXLOGAccess();

}

static void
WalRedoServer_ServiceInit(int listenerPort)
{
	if (WalRedoServerShared == NULL)
		elog(FATAL,"WAL Redo server shared memory not initialized");
	
	WalRedoServerShared->listenerPort = listenerPort;
	
	am_walredoserver = true;

	WalRedoServerInitContext();
}

static bool
WalRedoServer_ServiceShutdownRequested(void)
{
	if (walredo_shutdown_requested)
		elog((Debug_print_qd_mirroring ? LOG : DEBUG5),"shutdown requested is true");
	return walredo_shutdown_requested;
}

static void
WalRedoServer_ServiceShutdown(void)
{
	// Empty.
}

/*
 * Process local static that contains the connection to the WAL Redo server.
 */
static ServiceClient WalRedoServerClient = {NULL,-1};

bool
WalRedoServerClientConnect(void)
{
	bool connected;
	
	if (WalRedoServerShared == NULL)
		elog(FATAL,"WAL Redo server shared memory not initialized");

	connected = ServiceClientConnect(&WalRedoServer_ServiceConfig, 
									WalRedoServerShared->listenerPort,
									&WalRedoServerClient,
									/* complain */ true);

	return connected;
}

bool
WalRedoServerClientSendRequest(WalRedoRequest* walRedoRequest)
{
	bool successful;
	
	successful = ServiceClientSendRequest(&WalRedoServerClient, walRedoRequest, sizeof(WalRedoRequest));

	return successful;
}

bool
WalRedoServerClientReceiveResponse(WalRedoResponse* walRedoResponse, struct timeval *timeout)
{
	bool successful;
	
	successful = ServiceClientReceiveResponse(&WalRedoServerClient, walRedoResponse, sizeof(WalRedoResponse), timeout);

	return successful;
}

bool
WalRedoServerClientPollResponse(WalRedoResponse* walRedoResponse, bool *pollResponseReceived)
{
	bool successful;
	
	successful = ServiceClientPollResponse(&WalRedoServerClient, walRedoResponse, sizeof(WalRedoResponse), pollResponseReceived);

	return successful;
}

static bool	   		RedoQuiesce;
static bool	   		RedoServerError;
static XLogRecPtr 	RedoCheckPointLoc;
static CheckPoint 	RedoCheckpoint;

static void
WalRedoServerInitContext(void)
{
	RedoQuiesce = false;
	RedoServerError = false;
	memset(&RedoCheckPointLoc, 0, sizeof(XLogRecPtr));
	memset(&RedoCheckpoint, 0, sizeof(CheckPoint));
}

static bool WalRedoServer_ServiceRequest(ServiceCtrl *serviceCtrl, int sockfd, uint8 *request)
{
	WalRedoRequest *walRedoRequest = (WalRedoRequest*)request;
	WalRedoResponse walRedoResponse;
	bool result;

	//*** Missing: On normal shutdown, disable and disconnect.

	result = true;	// Assume.

	/* 
	 * Use a TRY block to catch unexpected errors that bubble up to this level
	 * and disable QD mirroring.
	 */
	PG_TRY();
	{
		if (Debug_print_qd_mirroring)
			elog(LOG, "request command %d = '%s'",
			     walRedoRequest->command, 
			     WalRedoRequestCommandToString(walRedoRequest->command));

		switch (walRedoRequest->command)
		{
		case NewCheckpointLocation:
			{
				if (!RedoQuiesce && !RedoServerError)
				{
					elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"new checkpoint location %s",
						 XLogLocationToString(&walRedoRequest->newCheckpointLocation));

					PG_TRY();
					{
						cdb_perform_redo(&RedoCheckPointLoc, &RedoCheckpoint, &walRedoRequest->newCheckpointLocation);
					}
					PG_CATCH();
					{
						/* 
						 * Report the error related to reading the primary's WAL
						 * to the server log 
						 */
						EmitErrorReport();
						FlushErrorState();

						elog(NOTICE,"error occurred during redo");
						RedoServerError = true;
						
						walRedoResponse.response = RedoUnexpectedError;

						result = ServiceProcessRespond(serviceCtrl, sockfd, (uint8*)&walRedoResponse, sizeof(walRedoResponse));

					}
					PG_END_TRY();
				}
				break;
			}

		case Quiesce:
			RedoQuiesce = true;
			
			walRedoResponse.response = Quiesced;

			result = ServiceProcessRespond(serviceCtrl, sockfd, (uint8*)&walRedoResponse, sizeof(walRedoResponse));
			break;

		default:
			elog(ERROR, "Unknown WalRedoRequestCommand %d", walRedoRequest->command);
		}
	}
	PG_CATCH();
	{
		/* 
		 * Report the unexpected error.
		 */
		EmitErrorReport();
		FlushErrorState();

		elog(NOTICE,"disabling redo (unexpected error encountered)");

		result = false;
	}
	PG_END_TRY();

	return result;
}

char* 
WalRedoRequestCommandToString(WalRedoRequestCommand command)
{
	switch (command)
	{
	case NewCheckpointLocation:
		return "NewCheckpointLocation";

	case Quiesce:
		return "Quiesce";

	default:
		return "Unknown";
	}
}

char* 
WalRedoResponseCommandToString(WalRedoResponseCommand command)
{
	switch (command)
	{
	case Quiesced:
		return "Quiesced";

	case RedoUnexpectedError:
		return "RedoUnexpectedError";

	default:
		return "Unknown";
	}
}

