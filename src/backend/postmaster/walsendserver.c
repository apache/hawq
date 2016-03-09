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
#include "postgres.h"
#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <netinet/in.h>


#include "miscadmin.h"
#include "libpq/pqsignal.h"
#include "cdb/cdbvars.h"
#include "commands/sequence.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/service.h"
#include "postmaster/walsendserver.h"
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

// To avoid pulling in cdbfts.h
extern bool isQDMirroringEnabled(void);	
extern void disableQDMirroring_TooFarBehind(char *detail);
extern void disableQDMirroring_ConnectionError(char *detail, char *errorMessage);
extern void disableQDMirroring_UnexpectedError(char *detail);
extern void disableQDMirroring_ShutDown(void);
extern void enableQDMirroring(char *message, char *detail);

/*=========================================================================
 * FUNCTIONS PROTOTYPES
 */


#ifdef EXEC_BACKEND
static pid_t walsendserver_forkexec(void);
#endif


static void WalSendServerGetStandbyTimeout(struct timeval *timeout);
static void WalSendServerInitContext(void);
static void WalSendServerDoRequest(WalSendRequest *walSendRequest);

/*
 * The following are standard procedure signatures needed by
 * the Service shared library.
 */
static void WalSendServer_ServiceClientTimeout(struct timeval *timeout);
static void WalSendServer_ServiceRequestShutdown(SIGNAL_ARGS);
static void WalSendServer_ServicePostgresInit(void);
static void WalSendServer_ServiceInit(int listenerPort);
static bool WalSendServer_ServiceShutdownRequested(void);
static bool WalSendServer_ServiceRequest(ServiceCtrl *serviceCtrl, int sockfd, uint8 *request);
static void WalSendServer_ServiceShutdown(void);
static void WalSendServer_ServicePostmasterDied(void);


/*=========================================================================
 * GLOBAL STATE VARIABLES
 */

static bool am_walsendserver = false;

static ServiceConfig WalSendServer_ServiceConfig =
	{"WALSND",
	"WAL Send Server", "WAL Send Server process", "walsendserver",
	sizeof(WalSendRequest), sizeof(WalSendResponse),
	WalSendServer_ServiceClientTimeout,
	WalSendServer_ServiceRequestShutdown,
	NULL,		// No EarlyInit.
	WalSendServer_ServicePostgresInit,
	WalSendServer_ServiceInit,
	WalSendServer_ServiceShutdownRequested,
	WalSendServer_ServiceRequest,
	WalSendServer_ServiceShutdown,
	WalSendServer_ServicePostmasterDied};

static ServiceCtrl WalSendServer_ServiceCtrl;


/*=========================================================================
 * VISIBLE FUNCTIONS
 */

typedef struct WalSendServerShmem
{
	int	listenerPort;
	
} WalSendServerShmem;

static WalSendServerShmem *WalSendServerShared = NULL;

int WalSendServerShmemSize(void)
{
	return MAXALIGN(sizeof(WalSendServerShmem));
}


void WalSendServerShmemInit(void)
{
	bool		found;
	
	
	/* Create or attach to the SharedSnapshot shared structure */
	WalSendServerShared = (WalSendServerShmem *) ShmemInitStruct("WAL Send Server", WalSendServerShmemSize(), &found);
	if (!WalSendServerShared)
		elog(FATAL, "could not initialize WAL Send server shared memory");
	
	if (!found)
		WalSendServerShared->listenerPort = -1;
}

/*
 * Main entry point for walsendserver controller process.
 *
 * This code is heavily based on pgarch.c, q.v.
 */
int
walsendserver_start(void)
{

	pid_t		WalSendServerPID;

	
#ifdef EXEC_BACKEND
	switch ((WalSendServerPID = walsendserver_forkexec()))
#else
	switch ((WalSendServerPID = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork walsendserver process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			ServiceInit(&WalSendServer_ServiceConfig, &WalSendServer_ServiceCtrl);
			ServiceMain(&WalSendServer_ServiceCtrl);
			break;
#endif
		default:
			return (int) WalSendServerPID;
	}

	
	/* shouldn't get here */
	return 0;
}


/*=========================================================================
 * HELPER FUNCTIONS
 */


#ifdef EXEC_BACKEND
/*
 * walsendserver_forkexec()
 *
 * Format up the arglist for the serqserver process, then fork and exec.
 */
static pid_t
walsendserver_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkwalsendserver";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif   /* EXEC_BACKEND */

bool walsend_shutdown_requested=false;

/*
 * The following are standard procedure signatures needed by
 * the Service shared library.
 */

static void
WalSendServer_ServiceClientTimeout(struct timeval *timeout)
{
	timeout->tv_sec = WalSendClientTimeout / 1000;
	timeout->tv_usec = (WalSendClientTimeout % 1000) * 1000;
}

static void
WalSendServer_ServiceRequestShutdown(SIGNAL_ARGS)
{
	walsend_shutdown_requested = true;
}

static void
WalSendServer_ServicePostgresInit(void)
{
	/* See InitPostgres()... */
    InitProcess();	
	InitBufferPoolBackend();
	InitXLOGAccess();

}

static void
WalSendServer_ServiceInit(int listenerPort)
{
	if (WalSendServerShared == NULL)
		elog(FATAL,"WAL Send server shared memory not initialized");
	
	WalSendServerShared->listenerPort = listenerPort;
	
	am_walsendserver = true;

	WalSendServerInitContext();
}

static bool
WalSendServer_ServiceShutdownRequested(void)
{
	if (walsend_shutdown_requested)
		elog((Debug_print_qd_mirroring ? LOG : DEBUG5),"shutdown requested is true");
	return walsend_shutdown_requested;
}

static void
WalSendServer_ServiceShutdown(void)
{
	PG_TRY();
	{
		disableQDMirroring_ShutDown();
		if (disconnectMirrorQD_SendClose())
			elog(LOG,"Master mirror disconnected");
	}
	PG_CATCH();
	{
		/* 
		 * Report the error related to reading the primary's WAL
		 * to the server log 
		 */
	    if (!elog_demote(NOTICE))
    	{
    		elog(LOG,"unable to demote error");
        	PG_RE_THROW();
    	}
		EmitErrorReport();
		FlushErrorState();
	}
	PG_END_TRY();
}

static void
WalSendServer_ServicePostmasterDied(void)
{
	if (!isQDMirroringNotConfigured() &&
		!isQDMirroringNotKnownYet() &&
		!isQDMirroringDisabled())
	{
		/*
		 * Only complain if we were trying to do work.
		 */
		ereport(LOG,
				(errmsg("exiting because postmaster has died")));
		proc_exit(1);
	}
}

//==============================================================================

static void
WalSendServerGetStandbyTimeout(struct timeval *timeout)
{
	struct timeval standbyTimeout = {30,0};		// 30 seconds.

	*timeout = standbyTimeout;
}

void
WalSendServerGetClientTimeout(struct timeval *timeout)
{
	ServiceGetClientTimeout(&WalSendServer_ServiceConfig, timeout);

	// Here we need to set client timeout greater than the server timeout, otherwise this timeout
	// will be triggered before the connection between WalSendServer and standby timeout trigger,
	// it will lead to the QD process report FATAL, and all processes on master restart.
	timeout->tv_sec *= 2;
	timeout->tv_usec *= 2;
}

/*
 * Process local static that contains the connection to the WAL send server.
 */
static ServiceClient WalSendServerClient = {NULL,-1};

bool
WalSendServerClientConnect(bool complain)
{
	bool connected;
	
	if (WalSendServerShared == NULL)
		elog(FATAL,"WAL Send server shared memory not initialized");

	connected = ServiceClientConnect(&WalSendServer_ServiceConfig, 
									WalSendServerShared->listenerPort,
									&WalSendServerClient,
									/* complain */ complain);

	return connected;
}


bool
WalSendServerClientSendRequest(WalSendRequest* walSendRequest)
{
	bool successful;
	
	successful = ServiceClientSendRequest(&WalSendServerClient, walSendRequest, sizeof(WalSendRequest));

	return successful;
}

bool
WalSendServerClientReceiveResponse(WalSendResponse* walSendResponse, struct timeval *timeout)
{
	bool successful;
	
	successful = ServiceClientReceiveResponse(&WalSendServerClient, walSendResponse, sizeof(WalSendResponse), timeout);

	return successful;
}

static XLogRecPtr 	originalEndLocation;
static XLogRecPtr 	currentEndLocation;
static uint32 		periodicLen;
static uint32 		periodicReportLen;
static XLogRecPtr 	periodicLocation;

static char		   *saveBuffer = NULL;
static uint32		saveBufferLen = 0;
static uint32       writeLogId = 0;
static uint32       writeLogSeg = 0;
static uint32       writeLogOff = 0;

static void
WalSendServerInitContext(void)
{
	memset(&originalEndLocation, 0, sizeof(XLogRecPtr));
	memset(&currentEndLocation, 0, sizeof(XLogRecPtr));
	
	periodicLen = 0;
	periodicReportLen = 10 * 1024 * 1024;	// 10 megabytes.
	if (!Debug_print_qd_mirroring)
		periodicReportLen *= 10;		// 100 megabytes.
	
	memset(&periodicLocation, 0, sizeof(XLogRecPtr));

}

static void
WalSendServerDoRequest(WalSendRequest *walSendRequest)
{
	bool successful;
	struct timeval standbyTimeout;

	WalSendServerGetStandbyTimeout(&standbyTimeout);
	
	switch (walSendRequest->command)
	{
	case PositionToEnd:
		elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "PositionToEnd");

		successful = write_position_to_end(&originalEndLocation,
			                               NULL, &walsend_shutdown_requested);
		if (successful)
			elog(LOG,"Standby master returned transaction log end location %s",
				 XLogLocationToString(&originalEndLocation));
		else
		{
			disableQDMirroring_ConnectionError(
				"Unable to connect to standby master and determine transaction log end location",
				GetStandbyErrorString());
			disconnectMirrorQD_SendClose();
		}
		break;
		
	case Catchup:
		elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "Catchup");

        if (isQDMirroringCatchingUp())
    	{
    		bool tooFarBehind = false;

			elog(LOG,"Current master transaction log is flushed through location %s",
				 XLogLocationToString(&walSendRequest->flushedLocation));
			
			if (XLByteLT(originalEndLocation, walSendRequest->flushedLocation))
			{
				/*
				 * Standby master is behind the primary.  Send catchup WAL.
				 */
				 
				/* 
				 * Use a TRY block to catch errors from our attempt to read
				 * the primary's WAL.  Errors from sending to the standby
				 * come up as a boolean return (successful).
				 */
				PG_TRY();
				{
					successful = XLogCatchupQDMirror(
									&originalEndLocation, 
									&walSendRequest->flushedLocation,
									&standbyTimeout,
									&walsend_shutdown_requested);
				}
				PG_CATCH();
				{
					/* 
					 * Report the error related to reading the primary's WAL
					 * to the server log 
					 */
					 
					/* 
					 * But first demote the error to something much less
					 * scary.
					 */
				    if (!elog_demote(WARNING))
			    	{
			    		elog(LOG,"unable to demote error");
			        	PG_RE_THROW();
			    	}
					
					EmitErrorReport();
					FlushErrorState();

					successful = false;
					tooFarBehind = true;
				}
				PG_END_TRY();
				
				if (successful)
				{
					elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
						 "catchup send from standby end %s through primary flushed location %s",
						 XLogLocationToString(&originalEndLocation),
						 XLogLocationToString2(&walSendRequest->flushedLocation));
				}

			}
			else if (XLByteEQ(originalEndLocation, walSendRequest->flushedLocation))
			{
				elog((Debug_print_qd_mirroring ? LOG : DEBUG5),"Mirror was already caught up");
				successful = true;
			}
			else
			{
				elog(WARNING,"Standby master transaction log location %s is beyond the current master end location %s",
				     XLogLocationToString(&originalEndLocation),
				     XLogLocationToString2(&walSendRequest->flushedLocation));
				successful = false;
			}
			
			if (successful)
			{
				char detail[200];
				int count;
				
				count = snprintf(
							 detail, sizeof(detail),
							 "Transaction log copied from locations %s through %s to the standby master",
						     XLogLocationToString(&originalEndLocation),
						     XLogLocationToString2(&walSendRequest->flushedLocation));
				if (count >= sizeof(detail))
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("format command string failure")));
				}

				enableQDMirroring("Master mirroring is now synchronized", detail);

				currentEndLocation = walSendRequest->flushedLocation;

				periodicLen = 0;
				periodicLocation = currentEndLocation;
			}
			else
			{
				if (tooFarBehind)
				{
					disableQDMirroring_TooFarBehind(
						"The current master was unable to synchronize the standby master "
						"because the transaction logs on the current master were recycled.  "
						"A gpinitstandby (at an appropriate time) will be necessary to copy "
						"over the whole master database to the standby master so it may be synchronized");
				}
				else
				{
					disableQDMirroring_ConnectionError(
						"Connection to the standby master was lost during transaction log catchup",
						GetStandbyErrorString());
				}
				disconnectMirrorQD_SendClose();
			}
		}
		else if (isQDMirroringDisabled())
		{
			elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "Master Mirror Send: Master mirroring not catching-up (state is disabled)");
		}
		else
		{
			elog(ERROR,"unexpected master mirroring state %s",
				 QDMirroringStateString());
		}
		
		break;
		
	case WriteWalPages:
		if (Debug_print_qd_mirroring)
			elog(LOG, "WriteWalPages");
		
        if (isQDMirroringEnabled())
        {
			char	   *from;
			Size		nbytes;
			bool		more= false;

			/*
			 * For now, save copy of data until flush.  This could be
			 * optimized.
			 */
			if (saveBuffer == NULL)
			{
				uint32 totalBufferLen = XLOGbuffers * XLOG_BLCKSZ;
				
				saveBuffer = malloc(totalBufferLen);
				if (saveBuffer == NULL)
					elog(ERROR,"Could not allocate buffer for xlog data (%d bytes)",
					     totalBufferLen);
				
				saveBufferLen = 0;
			}

			XLogGetBuffer(walSendRequest->startidx, walSendRequest->npages,
				          &from, &nbytes);

			if (saveBufferLen == 0)
			{
				more = false;
				writeLogId = walSendRequest->logId;
				writeLogSeg = walSendRequest->logSeg;
				writeLogOff = walSendRequest->logOff;

				memcpy(saveBuffer, from, nbytes);
				saveBufferLen = nbytes;
			}
			else
			{
				more = true;
				memcpy(&saveBuffer[saveBufferLen], from, nbytes);
				saveBufferLen += nbytes;
			}
			
			if (Debug_print_qd_mirroring)
				elog(LOG,
					 "Master Mirror Send: WriteWalPages (%s) startidx %d, npages %d, timeLineID %d, logId %u, logSeg %u, logOff 0x%X, nbytes 0x%X",
					 (more ? "more" : "new"),
					 walSendRequest->startidx,
					 walSendRequest->npages,
					 walSendRequest->timeLineID,
					 walSendRequest->logId,
					 walSendRequest->logSeg,
					 walSendRequest->logOff,
					 (int)nbytes);
    	}

	case FlushWalPages:
		if (Debug_print_qd_mirroring)
			elog(LOG, "FlushWalPages");
		
        if (isQDMirroringEnabled())
        {
			char 		cmd[MAXFNAMELEN + 50];

			if (saveBufferLen == 0)
				successful = true;
			else
			{
				if (snprintf(cmd, sizeof(cmd),"xlog %d %d %d %d", 
							 writeLogId, writeLogSeg, writeLogOff, 
							 (int)saveBufferLen) >= sizeof(cmd))
					elog(ERROR,"could not create cmd for qd mirror logid %d seg %d", 
					     writeLogId, writeLogSeg);
				
				successful = write_qd_sync(cmd, saveBuffer, saveBufferLen, 
							               &standbyTimeout,
							               &walsend_shutdown_requested);
				if (successful)
				{
					XLogRecPtr oldEndLocation;
					
					oldEndLocation = currentEndLocation;

					currentEndLocation.xlogid = writeLogId;
					currentEndLocation.xrecoff = writeLogSeg * XLogSegSize + writeLogOff;
					if (currentEndLocation.xrecoff >= XLogFileSize)
					{
						(currentEndLocation.xlogid)++;
						currentEndLocation.xrecoff = 0;
					}

					if (XLByteLT(oldEndLocation,currentEndLocation))
					{
						periodicLen += saveBufferLen;
						if (periodicLen > periodicReportLen)
						{
							elog(LOG,
								 "Master mirroring periodic report: %d bytes successfully send to standby master for locations %s through %s",
								 periodicLen,
								 XLogLocationToString(&periodicLocation),
								 XLogLocationToString2(&currentEndLocation));

							periodicLen = 0;
							periodicLocation = currentEndLocation;
						}
					}
					else
					{
						if (Debug_print_qd_mirroring)
							elog(LOG,
							     "Send to Master mirror successful.  New end location %s (old %s)",
							     XLogLocationToString(&currentEndLocation),
							     XLogLocationToString2(&oldEndLocation));
					}
				}
				else
				{
					disableQDMirroring_ConnectionError(
						"Connection to the standby master was lost attempting to send new transaction log",
						GetStandbyErrorString());
					disconnectMirrorQD_SendClose();
				}

				/*
				 * Reset so WriteWalPages can fill the buffer again.
				 */
				saveBufferLen = 0;
				writeLogId = 0;
				writeLogSeg = 0;
				writeLogOff = 0;
			}
			
			if (successful && walSendRequest->haveNewCheckpointLocation)
			{
				uint32 logid;
				uint32 seg;
				uint32 offset;
				
	        	elog((Debug_print_qd_mirroring ? LOG : DEBUG5),"New previous checkpoint location %s",
				     XLogLocationToString(&walSendRequest->newCheckpointLocation));
				XLByteToSeg(walSendRequest->newCheckpointLocation, logid, seg);
				offset = walSendRequest->newCheckpointLocation.xrecoff % XLogSegSize;
				
				if (snprintf(cmd, sizeof(cmd),"new_checkpoint_location %d %d %d", 
							 logid, seg, offset) >= sizeof(cmd))
					elog(ERROR,"could not create cmd for qd mirror logid %d seg %d offset %d", 
					     logid, seg, offset);
				
				successful = write_qd_sync(cmd, NULL, 0, 
					                       NULL, &walsend_shutdown_requested);
				if (successful)
				{
					elog((Debug_print_qd_mirroring ? LOG : DEBUG5),"Send of new checkpoint location to master mirror successful");
				}
				else
				{
					disableQDMirroring_ConnectionError(
						"Connection to the standby master was lost attempting to send new checkpoint location",
						GetStandbyErrorString());
					disconnectMirrorQD_SendClose();
				}
			}
			
        }
		else if (isQDMirroringDisabled())
		{
			elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "Master Mirror Send: Master mirroring not enabled");
		}
		else
		{
			elog(ERROR,"unexpected master mirroring state %s",
				 QDMirroringStateString());
		}
		
		break;

	case CloseForShutdown:
		if (Debug_print_qd_mirroring)
			elog(LOG, "CloseForShutdown");

		/*
		 * Do the work we would normally do when signaled to stop.
		 */
		WalSendServer_ServiceShutdown();
		break;

	default:
		elog(ERROR, "Unknown WalSendRequestCommand %d", walSendRequest->command);
	}

}

static bool WalSendServer_ServiceRequest(ServiceCtrl *serviceCtrl, int sockfd, uint8 *request)
{
	WalSendRequest *walSendRequest = (WalSendRequest*)request;
	WalSendResponse walSendResponse;
	bool result = false;

	/* 
	 * Use a TRY block to catch unexpected errors that bubble up to this level
	 * and disable QD mirroring.
	 */
	PG_TRY();
	{
		if (Debug_print_qd_mirroring)
			elog(LOG, "request command %d = '%s'",
			     walSendRequest->command, 
			     WalSendRequestCommandToString(walSendRequest->command));

		WalSendServerDoRequest(walSendRequest);

		/*
		 * Currently, all requests need a response.
		 */
		walSendResponse.ok = true;

		result = ServiceProcessRespond(serviceCtrl, sockfd, (uint8*)&walSendResponse, sizeof(walSendResponse));
	}
	PG_CATCH();
	{
		/* 
		 * Report the unexpected error.
		 */
		EmitErrorReport();
		FlushErrorState();

		disableQDMirroring_UnexpectedError(
			"An unexpected error encountered.  Please report this problem to Greenplum");

		result = false;
	}
	PG_END_TRY();

	return result;
}

char* 
WalSendRequestCommandToString(WalSendRequestCommand command)
{
	switch (command)
	{
	case PositionToEnd:
		return "PositionToEnd";

	case Catchup:
		return "Catchup";

	case WriteWalPages:
		return "WriteWalPages";
		
	case FlushWalPages:
		return "FlushWalPages";

	case CloseForShutdown:
		return "CloseForShutdown";
		
	default:
		return "Unknown";
	}
}
