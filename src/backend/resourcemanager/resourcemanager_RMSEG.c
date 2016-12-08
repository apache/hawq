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

#include "envswitch.h"
#include "dynrm.h"

#include "resourcemanager/resourcemanager.h"
#include "communication/rmcomm_AsyncComm.h"
#include "communication/rmcomm_MessageHandler.h"
#include "communication/rmcomm_MessageServer.h"
#include "communication/rmcomm_RMSEG2RM.h"
#include "resourceenforcer/resourceenforcer.h"
#include "cdb/cdbtmpdir.h"

int ResManagerMainSegment2ndPhase(void)
{
	int res = FUNC_RETURN_OK;

	elog(DEBUG5, "HAWQ RM SEG is triggered.");

	/* Register message handlers */
	registerMessageHandler(REQUEST_QE_MOVETOCGROUP, handleQEMoveToCGroup);
	registerMessageHandler(REQUEST_QE_MOVEOUTCGROUP, handleQEMoveOutCGroup);
	registerMessageHandler(REQUEST_QE_SETWEIGHTCGROUP, handleQESetWeightCGroup);
	registerMessageHandler(REQUEST_RM_INCREASE_MEMORY_QUOTA, handleRMIncreaseMemoryQuota);
	registerMessageHandler(REQUEST_RM_DECREASE_MEMORY_QUOTA, handleRMDecreaseMemoryQuota);
	registerMessageHandler(REQUEST_RM_RUALIVE, handleRMSEGRequestRUAlive);


	/**************************************************************************
	 * New socket facility poll based server.
	 **************************************************************************/
	res = initializeSocketServer_RMSEG();
	if ( res != FUNC_RETURN_OK ) {
		elog(FATAL, "Fail to initialize socket server.");
	}

	/*
	 * Resource enforcement: initialize, cleanup, and rebuild CGroup hash table
	 * when segment resource manager starts/restarts
	 */
	initCGroupThreads();

	InitFileAccess();

	/*
	 * Notify postmaster that HAWQ RM is ready. Ignore the possible problem that
	 * the parent process quits. HAWQ RM will automatically detect if its parent
	 * dies, then HAWQ RM should exit normally.
	 */
	kill(DRMGlobalInstance->ParentPID, SIGUSR2);
	elog(LOG, "HAWQ RM SEG process works now.");

    /* Start request handler to provide services. */
    res = MainHandlerLoop_RMSEG();

    elog(RMLOG, "HAWQ RM SEG server goes into exit phase.");
    return res;

}

int  initializeSocketServer_RMSEG(void)
{
	int 		res		= FUNC_RETURN_OK;
	int 		netres 	= 0;
	char 	   *allip   = "0.0.0.0";
	pgsocket 	RMListenSocket[HAWQRM_SERVER_PORT_COUNT];

	for ( int i = 0 ; i < HAWQRM_SERVER_PORT_COUNT ; ++i ) {
		RMListenSocket[i] = PGINVALID_SOCKET;
	}

	/* Listen normal socket addresses. */
	netres = StreamServerPort( AF_UNSPEC,
							   allip,
							   rm_segment_port,
							   NULL,
							   RMListenSocket,
							   HAWQRM_SERVER_PORT_COUNT);

	/* If there are multiple segments in one host, which is common in old imp.
	 * We can not make all segments work. So, if HAWQ RM SEG fails to start
	 * socket server by listening the port, we accept this case and make it
	 * silent. HAWQ RM will not recognize this segment and will not assign
	 * tasks. */
	if ( netres != STATUS_OK ) {
		res = REQUESTHANDLER_FAIL_START_SOCKET_SERVER;
		elog( LOG,  "cannot create socket server. HostName=%s, Port=%d",
				    allip,
					rm_segment_port);
		return res;
	}

	/* Initialize array for polling all file descriptors. */
	initializeAsyncComm();
	int 			validfdcount = 0;
	AsyncCommBuffer newbuffer    = NULL;
	for ( int i = 0 ; i < HAWQRM_SERVER_PORT_COUNT ; ++i ) {
		if (RMListenSocket[i] != PGINVALID_SOCKET) {
			netres = registerFileDesc(RMListenSocket[i],
									  ASYNCCOMM_READ,
									  &AsyncCommBufferHandlersMsgServer,
									  NULL,
									  &newbuffer);
			if ( netres != FUNC_RETURN_OK ) {
				res = REQUESTHANDLER_FAIL_START_SOCKET_SERVER;
				elog(LOG, "Resource manager cannot track socket server.");
				break;
			}
			validfdcount++;

			InitHandler_Message(newbuffer);
		}
	}

	if ( res != FUNC_RETURN_OK ) {
		for ( int i = 0 ; i < HAWQRM_SERVER_PORT_COUNT ; ++i ) {
			if ( RMListenSocket[i] != PGINVALID_SOCKET ) close(RMListenSocket[i]);
		}
		return res;
	}

	elog(DEBUG5, "HAWQ RM SEG :: Starts accepting resource request. "
				 "Listening normal socket port %s:%d. "
				 "Total listened %d FDs.",
				allip,
				rm_segment_port,
				validfdcount);
	return res;

}

int MainHandlerLoop_RMSEG(void)
{
	int 		res 	  = FUNC_RETURN_OK;
	uint64_t    curtime   = 0;
	int			errorcode = FUNC_RETURN_OK;
	char		errorbuf[1024];

	DRMGlobalInstance->ResourceManagerStartTime = gettime_microsec();
	while( DRMGlobalInstance->ResManagerMainKeepRun ) {

		if (!PostmasterIsAlive(true)) {
			DRMGlobalInstance->ResManagerMainKeepRun = false;
			elog(LOG, "Postmaster is not alive, resource manager exits");
			break;
		}

		/* PART1. Handle socket server inputs. */
		res = processAllCommFileDescs();
		if ( res != FUNC_RETURN_OK ) {
			/*
			 * The possible error here is the failure of poll(), we won't keep
			 * running HAWQ RM any longer, graceful quit is requested.
			 */
			DRMGlobalInstance->ResManagerMainKeepRun = false;
			elog(LOG, "System error cause resource manager not possible to track "
					  "network communications.");
		}

		/* PART2. Handle all BE submitted requests. */
		processSubmittedRequests();

		if ( curtime - DRMGlobalInstance->TmpDirLastCheckTime >
			1000000LL * rm_segment_tmpdir_detect_interval )
		{
			checkAndBuildFailedTmpDirList();
			DRMGlobalInstance->TmpDirLastCheckTime = gettime_microsec();
		}

		/* PART3. Fresh local host info and send IMAlive message to resource
		 * 		  manager server.											  */
		curtime = gettime_microsec();
		if ( DRMGlobalInstance->LocalHostStat == NULL ||
			 curtime - DRMGlobalInstance->LocalHostLastUpdateTime >
			 1000000LL * rm_segment_config_refresh_interval )
		{
			refreshLocalHostInstance();
			checkLocalPostmasterStatus();
		}

		if ( DRMGlobalInstance->SendIMAlive )
		{
			 if (DRMGlobalInstance->LocalHostStat != NULL &&
			     curtime - DRMGlobalInstance->HeartBeatLastSentTime >
				 1000000LL * rm_segment_heartbeat_interval )
			 {
				 sendIMAlive(&errorcode, errorbuf, sizeof(errorbuf));
				 DRMGlobalInstance->HeartBeatLastSentTime = gettime_microsec();
			 }
		}

		/* PART4. Send responses back to the clients. */
		sendResponseToClients();

		/* PART5. Resource enforcement work thread quit */
		if (g_enforcement_thread_quited) {
			elog(ERROR, "Resource enforcement thread quited");
		}
	}

	elog(RMLOG, "Resource manager main event handler exits.");

	return res;
}

/*
 *  Check if this temporary directory is OK to read or write.
 *  If not, it's probably due to disk error.
 */
bool CheckTmpDirAvailable(char *path)
{
	FILE  *tmp = NULL;
	bool  ret = true;
	char* fname = NULL;
	char* testfile = "/checktmpdir.log";

	/* open a file to check if
	 * this temporary directory is OK.
	 */
	fname = palloc0(strlen(path) + strlen(testfile) + 1);
	strncpy(fname, path, strlen(path));
	strncpy(fname + strlen(path), testfile, strlen(testfile));
	tmp = fopen(fname, "w");
	if (tmp == NULL)
	{
		elog(LOG, "Can't open file:%s when check temporary directory: %s",
				  fname,
				  strerror(errno));
		ret = false;
	}

	pfree(fname);
	if (tmp != NULL)
		fclose(tmp);
	return ret;
}

/*
 * Check the status of each temporary directory,
 * and build a list of failed temporary directories.
 */
void checkAndBuildFailedTmpDirList(void)
{
	uint64_t starttime = gettime_microsec();
	destroyTmpDirList(DRMGlobalInstance->LocalHostFailedTmpDirList);
	DRMGlobalInstance->LocalHostFailedTmpDirList = NULL;

	DQUEUE_LOOP_BEGIN(&DRMGlobalInstance->LocalHostTempDirectories, iter, SimpStringPtr, value)
		if (!CheckTmpDirAvailable(value->Str))
		{
			char *failedDir = pstrdup(value->Str);
			DRMGlobalInstance->LocalHostFailedTmpDirList =
					lappend(DRMGlobalInstance->LocalHostFailedTmpDirList, failedDir);
		}
	DQUEUE_LOOP_END
	uint64_t endtime = gettime_microsec();
	elog(LOG, "checkAndBuildFailedTmpDirList finished checking temporary "
			  "directory, which costs " UINT64_FORMAT " us",
			  endtime - starttime);
}
