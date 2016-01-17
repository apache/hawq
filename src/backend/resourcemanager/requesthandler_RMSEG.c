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
#include "time.h"

#include "resourcemanager/resourcemanager.h"
#include "communication/rmcomm_RMSEG2RM.h"
#include "communication/rmcomm_RM2RMSEG.h"
#include "communication/rmcomm_MessageHandler.h"
#include "communication/rmcomm_QE_RMSEG_Protocol.h"
#include "communication/rmcomm_RM_RMSEG_Protocol.h"
#include "utils/simplestring.h"
#include "utils/linkedlist.h"

#include "utils/palloc.h"

#include "gp-libpq-fe.h"
#include "gp-libpq-int.h"

#include "resourceenforcer/resourceenforcer_message.h"

#define RMSEG_INBUILDHOST SMBUFF_HEAD(SegStat, &localsegstat)

/* UINT32_T.MAX_VALUE = 4294967295 */
#define MAX_DIGITS_OF_UINT32_T 10

/* Format of cgroup timestamp: YYYYMMDD_HHMMSS */
#define LENTH_CGROUP_TIMESTAMP 15

char *buildCGroupNameString(int64 masterStartTime, uint32 connId);

/*
 * Refresh localhost information, this is used to build message for IMAlive.
 */
int refreshLocalHostInstance(void)
{
	SimpString failedTmpDirStr;
	initSimpleString(&failedTmpDirStr, PCONTEXT);

	/* Get local host name. */
	SimpString hostname;
	initSimpleString(&hostname, PCONTEXT);
	getLocalHostName(&hostname);
	elog(DEBUG3, "Segment resource manager reads localhost name %s",
				 hostname.Str);

	/* Read local host interface addresses. */
	DQueueData addresses;
	initializeDQueue(&addresses, PCONTEXT);
	getLocalHostAllIPAddressesAsStrings(&addresses);
	DQUEUE_LOOP_BEGIN(&addresses, iter, HostAddress, addr)
		elog(DEBUG3, "Segment resource manager reads local host address %s",
					 addr->Address + 4);
	DQUEUE_LOOP_END

	/* Get a list of failed temporary directory */
	List* failedTmpDir = getFailedTmpDirList();
	uint16_t failedTmpDirNum = list_length(failedTmpDir);
	if (failedTmpDirNum > 0)
	{
		SelfMaintainBufferData buf;
		initializeSelfMaintainBuffer(&buf, PCONTEXT);
		uint16_t idx = 0;
		ListCell *lc = NULL;
		foreach(lc, failedTmpDir)
		{
			elog(LOG, "Get a failed temporary directory list for IMAlive message: %s",
					  (char *)lfirst(lc));
			appendSelfMaintainBuffer(&buf, (char *)lfirst(lc), strlen((char*)lfirst(lc)));
			if (idx != failedTmpDirNum -1)
			{
				appendSelfMaintainBuffer(&buf, ",", 1);
			}
			idx++;
		}
		static char zeropad = '\0';
		appendSMBVar(&buf, zeropad);
		setSimpleStringNoLen(&failedTmpDirStr, buf.Buffer);
		elog(LOG, "Segment resource manager build failed tmp list string:%s", failedTmpDirStr.Str);
		destroySelfMaintainBuffer(&buf);
	}

	bool shouldupdate = false;
	if ( DRMGlobalInstance->LocalHostStat == NULL )
	{
		elog(LOG, "Segment resource manager discovers localhost configuration "
				  "the first time.");
		shouldupdate = true;
	}
	else
	{
		SegInfo info = &(DRMGlobalInstance->LocalHostStat->Info);

		/* Check if the hostname changes. */
		if ( SimpleStringComp( &hostname, GET_SEGINFO_HOSTNAME(info)) != 0 )
		{
			elog(LOG, "Segment resource manager changes localhost name "
					  "from %s to %s.",
					  GET_SEGINFO_HOSTNAME(info),
					  hostname.Str);
			shouldupdate = true;
		}

		/* Check if has the same count of addresses. */
		if ( !shouldupdate && info->HostAddrCount != addresses.NodeCount)
		{
			elog(LOG, "Segment resource manager changes number of host "
					  "addresses from %d to %d.",
					  info->HostAddrCount,
					  addresses.NodeCount);
			shouldupdate = true;
		}

		/* Check if the addresses are all not changed. */
		if( !shouldupdate )
		{
			int dummyindex = 0;
			/* Check if the host addresses change. */
			DQUEUE_LOOP_BEGIN(&addresses, iter, HostAddress, addr)
				if ( findSegInfoHostAddrStr(info,
											(AddressString)(addr->Address),
											&dummyindex) != FUNC_RETURN_OK ) {
					elog(LOG, "Segment resource manager finds new host address %s",
							  ((AddressString)(addr->Address))->Address);
					shouldupdate = true;
					break;
				}
			DQUEUE_LOOP_END
		}

		/* Check if the failed temporary directory are changed. */
		if( !shouldupdate &&
				DRMGlobalInstance->LocalHostStat->FailedTmpDirNum != failedTmpDirNum)
		{
			elog(LOG, "Segment resource manager changes number of failed "
					  "temporary from %d to %d.",
					  DRMGlobalInstance->LocalHostStat->FailedTmpDirNum,
					  failedTmpDirNum);
			shouldupdate = true;
		}

		if ( !shouldupdate && failedTmpDirNum != 0 )
		{
			if (strcmp(GET_SEGINFO_FAILEDTMPDIR(info), failedTmpDirStr.Str) != 0)
			{
				elog(LOG, "Segment resource manager finds failed temporary directory change "
						  "from %s to %s", GET_SEGINFO_FAILEDTMPDIR(info), failedTmpDirStr.Str);
				shouldupdate = true;
			}
		}
	}

	if ( shouldupdate )
	{

		elog(LOG, "Segment resource manager Build/update local host information.");

		/* Rebuild segment stat content. */
		SelfMaintainBufferData localsegstat;
		initializeSelfMaintainBuffer(&localsegstat, PCONTEXT);
		prepareSelfMaintainBuffer(&localsegstat, sizeof(SegStatData), true);

		RMSEG_INBUILDHOST->GRMTotalMemoryMB = 0;
		RMSEG_INBUILDHOST->GRMTotalCore     = 0.0;
		RMSEG_INBUILDHOST->FTSTotalMemoryMB = DRMGlobalInstance->SegmentMemoryMB;
		RMSEG_INBUILDHOST->FTSTotalCore     = DRMGlobalInstance->SegmentCore;
		RMSEG_INBUILDHOST->GRMAvailable    = RESOURCE_SEG_STATUS_UNSET;
		RMSEG_INBUILDHOST->FTSAvailable    = RESOURCE_SEG_STATUS_AVAILABLE;
		RMSEG_INBUILDHOST->ID				= SEGSTAT_ID_INVALID;
		RMSEG_INBUILDHOST->FailedTmpDirNum  = failedTmpDirNum;

		RMSEG_INBUILDHOST->Info.master	= 0;			 /* I'm a segment. 	  */
		RMSEG_INBUILDHOST->Info.standby	= 0;			 /* I'm a segment. 	  */
		RMSEG_INBUILDHOST->Info.port	= PostPortNumber;/* hawq-site.xml conf*/
		RMSEG_INBUILDHOST->Info.alive	= 1;			 /* I'm always alive. */

		jumpforwardSelfMaintainBuffer(&localsegstat, sizeof(SegStatData));

		/* add address of offset and addtributes. */
		RMSEG_INBUILDHOST->Info.HostAddrCount = addresses.NodeCount;

		uint16_t addroffset = sizeof(SegInfoData) +
							  sizeof(uint32_t) * (((addresses.NodeCount + 1) >> 1) << 1);
		uint16_t addrattr = 0;
		addrattr |= HOST_ADDRESS_CONTENT_STRING;

		RMSEG_INBUILDHOST->Info.AddressAttributeOffset = sizeof(SegInfoData);
		RMSEG_INBUILDHOST->Info.AddressContentOffset   = addroffset;

		DQUEUE_LOOP_BEGIN(&addresses, iter, HostAddress, addr)
			appendSMBVar(&localsegstat, addroffset);
			appendSMBVar(&localsegstat, addrattr);
			addroffset += addr->AddressSize;
		DQUEUE_LOOP_END

		appendSelfMaintainBufferTill64bitAligned(&localsegstat);

		/* add address content. */
		DQUEUE_LOOP_BEGIN(&addresses, iter, HostAddress, addr)
			appendSelfMaintainBuffer(&localsegstat, addr->Address, addr->AddressSize);
		DQUEUE_LOOP_END

		/* add host name. */
		RMSEG_INBUILDHOST->Info.HostNameOffset = localsegstat.Cursor + 1 -
												 offsetof(SegStatData, Info);
		appendSelfMaintainBuffer(&localsegstat, hostname.Str, hostname.Len+1);
		appendSelfMaintainBufferTill64bitAligned(&localsegstat);

		RMSEG_INBUILDHOST->Info.HostNameLen = hostname.Len;

		/* segment reports, set GRM host/rack as NULL */
		RMSEG_INBUILDHOST->Info.GRMHostNameLen 	  = 0;
		RMSEG_INBUILDHOST->Info.GRMHostNameOffset = 0;
		RMSEG_INBUILDHOST->Info.GRMRackNameLen 	  = 0;
		RMSEG_INBUILDHOST->Info.GRMRackNameOffset = 0;

		/* add failed temporary directory */
		if (failedTmpDirNum == 0)
		{
			RMSEG_INBUILDHOST->Info.FailedTmpDirOffset = 0;
			RMSEG_INBUILDHOST->Info.FailedTmpDirLen = 0;
		}
		else
		{
			RMSEG_INBUILDHOST->Info.FailedTmpDirOffset = localsegstat.Cursor + 1 -
					 									 offsetof(SegStatData, Info);
			appendSelfMaintainBuffer(&localsegstat, failedTmpDirStr.Str, failedTmpDirStr.Len+1);
			appendSelfMaintainBufferTill64bitAligned(&localsegstat);
			RMSEG_INBUILDHOST->Info.FailedTmpDirLen = failedTmpDirStr.Len;
			elog(LOG, "Segment resource manager builds tmp dir:%s",
					  GET_SEGINFO_FAILEDTMPDIR(&RMSEG_INBUILDHOST->Info));
		}

		/* get total size of this machine id. */
		RMSEG_INBUILDHOST->Info.Size = localsegstat.Cursor + 1 - offsetof(SegStatData, Info);

		/* Save new content. */
		if ( DRMGlobalInstance->LocalHostStat != NULL )
		{
			rm_pfree(PCONTEXT, DRMGlobalInstance->LocalHostStat);
		}

		DRMGlobalInstance->LocalHostStat = rm_palloc0(PCONTEXT, localsegstat.Cursor+1);
		memcpy(DRMGlobalInstance->LocalHostStat, localsegstat.Buffer, localsegstat.Cursor+1);

		SelfMaintainBufferData machinereport;
		initializeSelfMaintainBuffer(&machinereport,PCONTEXT);
		generateSegStatReport(DRMGlobalInstance->LocalHostStat, &machinereport);

		elog(LOG, "Built localhost information. %s", machinereport.Buffer);

		destroySelfMaintainBuffer(&machinereport);
		destroySelfMaintainBuffer(&localsegstat);
	}

	/* Update update time. */
	DRMGlobalInstance->LocalHostLastUpdateTime = gettime_microsec();

	/* Cleanup. */
	freeSimpleStringContent(&hostname);
	DQUEUE_LOOP_BEGIN(&addresses, iter, HostAddress, addr)
		freeHostAddress(addresses.Context, addr);
	DQUEUE_LOOP_END
	removeAllDQueueNodes(&addresses);
	cleanDQueue(&addresses);
	destroyTmpDirList(failedTmpDir);
	freeSimpleStringContent(&failedTmpDirStr);

	return FUNC_RETURN_OK;
}

/**
 * HAWQ RM server asks HAWQ RM segment if it is alive.
 */
bool handleRMSEGRequestRUAlive(void **arg)
{
	int	libpqres = CONNECTION_OK;
	PGconn *conn = NULL;
	char conninfo[1024];
	int retry = 5;

	/* Check if can connect postmaster */
	sprintf(conninfo,
			"options='-c gp_session_role=UTILITY' dbname=template1 port=%d connect_timeout=60",
			seg_addr_port);
	while (retry > 0) {
		retry--;
		conn = PQconnectdb(conninfo);
		if ((libpqres = PQstatus(conn)) != CONNECTION_OK) {
			if (retry == 0) {
				elog(LOG, "Segment's postmaster is down, PQconnectdb result : %d, %s",
						  libpqres,
						  PQerrorMessage(conn));
				/* Don't send IMAlive anymore */
				DRMGlobalInstance->SendIMAlive = false;
			}
			else {
				pg_usleep(500000);
				continue;
			}
		}
		else {
			elog(DEBUG3, "Segment's postmaster is healthy.");
			break;
		}
	}

	PQfinish(conn);

	/* send response */
	RPCResponseRUAliveData response;
	response.Result   = libpqres == CONNECTION_OK ? FUNC_RETURN_OK : LIBPQ_CONN_ERROR;
	response.Reserved = 0;
	ConnectionTrack conntrack = (ConnectionTrack)(*arg);
	buildResponseIntoConnTrack(conntrack,
						   	   (char*)&response,
							   sizeof(response),
							   conntrack->MessageMark1,
							   conntrack->MessageMark2,
							   RESPONSE_RM_RUALIVE);

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

void checkLocalPostmasterStatus(void)
{
	int libpqres = CONNECTION_OK;
	PGconn *conn = NULL;
	char conninfo[1024];

	if ( DRMGlobalInstance->SendIMAlive ) {
		return;
	}

	/* Check if can connect postmaster */
	sprintf(conninfo, "options='-c gp_session_role=UTILITY' "
					  "dbname=template1 port=%d connect_timeout=60",
					  seg_addr_port);
	conn = PQconnectdb(conninfo);
	if ((libpqres = PQstatus(conn)) != CONNECTION_OK) {
		elog(LOG, "Segment postmaster is down, libpb conn result : %d, %s",
				  libpqres,
				  PQerrorMessage(conn));
		/* Don't send IMAlive anymore */
		if ( DRMGlobalInstance->SendIMAlive ) {
			DRMGlobalInstance->SendIMAlive = false;
			elog(LOG, "Segment postmaster is unhealthy, "
					  "resource manager pauses sending heart-beat.");
		}
	}
	else {
		if ( !DRMGlobalInstance->SendIMAlive ) {
			DRMGlobalInstance->SendIMAlive = true;
			elog(LOG, "Segment postmaster is healthy, "
					  "resource manager restore sending heat-beat again.");
		}
	}
	PQfinish(conn);
}

static void
checkTmpDirTable(uint32_t session_id, bool session_exists)
{
    if (!session_exists)
    {
        // New Session
        uint32_t *data = rm_palloc0(DRMGlobalInstance->Context, sizeof(uint32_t));
        *data = session_id;
        insertDQueueHeadNode(&DRMGlobalInstance->TmpDirLRUList, (void *)data);
    }

    if (hash_get_num_entries(DRMGlobalInstance->LocalTmpDirTable) >=
    	DRMGlobalInstance->TmpDirTableCapacity)
    {
        // Delete LRU session
        bool            found;
        TmpDirKey       tmpdir_key;
        TmpDirEntry     *tmpdir_entry;

        uint32_t *data = (uint32_t *)removeDQueueTailNode(&DRMGlobalInstance->TmpDirLRUList);
        tmpdir_key.session_id = *data;
        rm_pfree(DRMGlobalInstance->Context, data);
        tmpdir_entry = hash_search(DRMGlobalInstance->LocalTmpDirTable,
                                   (void *)&tmpdir_key,
								   HASH_REMOVE,
								   &found);
        if (found)
        {
        	hash_destroy(tmpdir_entry->qe_tmp_dirs);
        }
    }
    else
    {
    	// Update LRU data
    	DQueueNode node = DRMGlobalInstance->TmpDirLRUList.Head;
    	while (node != NULL)
    	{
    		if (*(uint32_t *)node->Data == session_id)
    		{
    			break;
    		}
    		node = node->Next;
    	}

    	if (node && node != DRMGlobalInstance->TmpDirLRUList.Head)
    	{
    		insertDQueueHeadNode(&DRMGlobalInstance->TmpDirLRUList, removeDQueueNode(&DRMGlobalInstance->TmpDirLRUList, node));
    	}
    }
}


bool handleRMSEGRequestTmpDir(void **arg)
{
    ConnectionTrack conntrack 	= (ConnectionTrack)(*arg);
	char			response[8];
    uint32_t 		session_id 	= 0;
    uint32_t 		command_id 	= 0;
    int32_t 		qeidx 		= -1;
    TmpDirKey       tmpdir_key;
    TmpDirEntry    *tmpdir_entry;
    QETmpDirKey     qe_tmpdir_key;
    QETmpDirEntry  *qe_tmpdir_entry;
    bool 			session_found;
    bool 			new_command;
    bool 			qeidx_found;
    SelfMaintainBufferData sendbuff;

    if (DRMGlobalInstance->NextLocalHostTempDirIdx < 0
        || DRMGlobalInstance->NextLocalHostTempDirIdxForQD < 0) {

        *(uint32_t *)response       = RM_STATUS_BAD_TMPDIR;
        *(uint32_t *)(response + 4) = 0;

        buildResponseIntoConnTrack(conntrack,
					   	   	       response,
								   sizeof(response),
								   conntrack->MessageMark1,
								   conntrack->MessageMark2,
								   RESPONSE_RM_TMPDIR);

        elog(LOG, "handleRMSEGRequestTmpDir, no existing tmp dirs in the "
        			 "segment resource manager");
    } else {

        session_id = *((uint32_t *)(conntrack->MessageBuff.Buffer + 0));
        command_id = *((uint32_t *)(conntrack->MessageBuff.Buffer + 4));
        qeidx      = *((int32_t  *)(conntrack->MessageBuff.Buffer + 8));
        initializeSelfMaintainBuffer(&sendbuff, PCONTEXT);

        if (qeidx == -1)
        {
            // QE on master
            int tmpdir_idx = 0;
            if (session_id > 0)
            {
                tmpdir_idx = session_id % getDQueueLength(&DRMGlobalInstance->LocalHostTempDirectoriesForQD);
            }
           
            SimpStringPtr tmpdir = (SimpStringPtr)
                getDQueueNodeDataByIndex(&DRMGlobalInstance->LocalHostTempDirectoriesForQD, tmpdir_idx);

            appendSMBSimpStr(&sendbuff, tmpdir);
            appendSelfMaintainBufferTill64bitAligned(&sendbuff);
           
            char tmpdir_string[TMPDIR_MAX_LENGTH] = {0};
            memset(tmpdir_string, 0, TMPDIR_MAX_LENGTH);
            memcpy(tmpdir_string, tmpdir->Str, tmpdir->Len);

            elog(LOG, "handleRMSEGRequestTmpDir session_id:%u command_id:%u qe_idx:%d tmpdir:%s",
                	 session_id,
					 command_id,
					 qeidx,
					 tmpdir_string);
        }
        else
        {
            // QE on segment
            tmpdir_key.session_id = session_id;
            qe_tmpdir_key.qeidx = qeidx;

            tmpdir_entry = (TmpDirEntry *)
                           hash_search(DRMGlobalInstance->LocalTmpDirTable,
                                       (void *)&tmpdir_key,
                                       HASH_ENTER,
                                       &session_found);
            if (!session_found)
            {
                // New Session
                new_command = true;
            }
            else
            {
                // Existing Session
                if (command_id == tmpdir_entry->command_id)
                {
                    // In the same command
                    new_command = false;
                }
                else
                {
                    // New command
                    new_command = true;
                    hash_destroy(tmpdir_entry->qe_tmp_dirs);
                }
            }

            if (new_command)
            {
                tmpdir_entry->command_id = command_id;
                HASHCTL ctl;
                ctl.keysize = sizeof(QETmpDirKey);
                ctl.entrysize = sizeof(QETmpDirEntry);
                ctl.hcxt = DRMGlobalInstance->Context;
                tmpdir_entry->qe_tmp_dirs =
                        hash_create("Executor session temporary directory table",
                                    16,
                                    &ctl,
                                    HASH_ELEM);
            }

            qe_tmpdir_entry = (QETmpDirEntry *)hash_search(tmpdir_entry->qe_tmp_dirs,
                                                           (void *)&qe_tmpdir_key,
                                                           HASH_ENTER,
                                                           &qeidx_found);

            if (!qeidx_found)
            {
                // New QE
                SimpStringPtr tmpdir =
                        (SimpStringPtr)
                        getDQueueNodeDataByIndex(&DRMGlobalInstance->LocalHostTempDirectories, DRMGlobalInstance->NextLocalHostTempDirIdx);
                DRMGlobalInstance->NextLocalHostTempDirIdx=
                        (DRMGlobalInstance->NextLocalHostTempDirIdx + 1) %
                         getDQueueLength(&DRMGlobalInstance->LocalHostTempDirectories);
                memset(qe_tmpdir_entry->tmpdir, 0, TMPDIR_MAX_LENGTH);
                memcpy(qe_tmpdir_entry->tmpdir, tmpdir->Str, tmpdir->Len);
            }

            checkTmpDirTable(session_id, session_found);

            appendSMBStr(&sendbuff, qe_tmpdir_entry->tmpdir);
            appendSelfMaintainBufferTill64bitAligned(&sendbuff);

            elog(LOG, "handleRMSEGRequestTmpDir session_id:%u command_id:%u qe_idx:%d tmpdir:%s",
                	 session_id,
					 command_id,
					 qeidx,
					 qe_tmpdir_entry->tmpdir);
        }
        
        buildResponseIntoConnTrack(conntrack,
                                   sendbuff.Buffer,
                                   sendbuff.Cursor + 1,
                                   conntrack->MessageMark1,
                                   conntrack->MessageMark2,
                                   RESPONSE_RM_TMPDIR);
        destroySelfMaintainBuffer(&sendbuff);
    }

    conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

    return true;
}

char *buildCGroupNameString(int64 masterStartTime, uint32 connId)
{
	int tLength = strlen(timestamptz_to_str(masterStartTime));
	char *tTime = (char *)rm_palloc0(PCONTEXT, tLength+1);
	strcpy(tTime, timestamptz_to_str(masterStartTime));

	char *sTime = (char *)rm_palloc0(PCONTEXT, LENTH_CGROUP_TIMESTAMP+1);
	sprintf(sTime, "%.4s%.2s%.2s_%.2s%.2s%.2s",
			   	   tTime,
			   	   tTime + 5,
			   	   tTime + 8,
			   	   tTime + 11,
			   	   tTime + 14,
			   	   tTime + 17);

	/* CGroup name follows the format: hawq-YYMMDD_HHMMSS-connCONNECTIONID */
	char *cgroupName = (char*)rm_palloc0(PCONTEXT,
									  	 sizeof("hawq-") -1 + strlen(sTime) +
									  	 sizeof("-conn") -1 + MAX_DIGITS_OF_UINT32_T + 1);
	sprintf(cgroupName, "hawq-%s-conn%d", sTime, connId);

	rm_pfree(PCONTEXT, sTime);
	rm_pfree(PCONTEXT, tTime);

	return cgroupName;
}
/**
 * Handle QE MoveToGroup function call.
 */
bool handleQEMoveToCGroup(void **arg)
{
	ConnectionTrack conntrack = (ConnectionTrack)(*arg);
	Assert(conntrack != NULL);

	RPCResponseMoveToCGroupData	response;

	int					segmentPid;
	TimestampTz			masterStartTime;
	uint32_t			connId;
	int					segId;

	RPCRequestMoveToCGroup request = (RPCRequestMoveToCGroup)
									 (conntrack->MessageBuff.Buffer);
	masterStartTime = request->MasterStartTime;
	connId			= request->ConnID;
	segId			= request->SegmentID;
	segmentPid		= request->ProcID;

	elog(DEBUG1, "Resource enforcer moves QE to CGroup: "
	             "masterStartTime = %s, connId = %d, segId = %d, procId = %d",
	             timestamptz_to_str(masterStartTime),
	             connId, segmentPid, segmentPid);

	response.Result = FUNC_RETURN_OK;
	response.Reserved = 0;

	buildResponseIntoConnTrack(conntrack,
	                           (char *)&response,
	                           sizeof(response),
	                           conntrack->MessageMark1,
	                           conntrack->MessageMark2,
	                           RESPONSE_QE_MOVETOCGROUP);

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	/* Prepare cgroupName from masterStartTime, connId and segId information */
	char *cgroupName = buildCGroupNameString(masterStartTime, connId);

	/* Move To CGroup in thread */
	ResourceEnforcementRequest *task = (ResourceEnforcementRequest *)malloc(sizeof(ResourceEnforcementRequest));
	task->type = MOVETO;
	task->pid = segmentPid;
	memset(task->cgroup_name, 0, sizeof(task->cgroup_name));
	strncpy(task->cgroup_name, cgroupName, strlen(cgroupName)+1);
	enqueue(g_queue_cgroup, (void *)task);

	rm_pfree(PCONTEXT, cgroupName);

	return true;
}

/**
 * Handle QE MoveOutGroup function call.
 */
bool handleQEMoveOutCGroup(void **arg)
{
	ConnectionTrack conntrack = (ConnectionTrack)(*arg);
	Assert(conntrack != NULL);

	RPCResponseMoveOutCGroupData response;

	int				segmentPid;
	TimestampTz		masterStartTime;
	uint32_t		connId;
	int				segId;

	RPCRequestMoveOutCGroup request = (RPCRequestMoveOutCGroup)
									  (conntrack->MessageBuff.Buffer);
	masterStartTime = request->MasterStartTime;
	connId			= request->ConnID;
	segId			= request->SegmentID;
	segmentPid		= request->ProcID;

	elog(DEBUG1, "Resource enforcer moves QE out from CGroup: "
	             "masterStartTime = %s, connId = %d, segId = %d, procId = %d",
	             timestamptz_to_str(masterStartTime),
	             connId, segmentPid, segmentPid);

	response.Result = FUNC_RETURN_OK;
	response.Reserved = 0;

	buildResponseIntoConnTrack(conntrack,
	                           (char *)&response,
	                           sizeof(response),
	                           conntrack->MessageMark1,
	                           conntrack->MessageMark2,
	                           RESPONSE_QE_MOVEOUTCGROUP);

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	/* Prepare cgroupName from masterStartTime, connId and segId information */
	char *cgroupName = buildCGroupNameString(masterStartTime, connId);

    /* Move Out CGroup in thread */
    ResourceEnforcementRequest *task = (ResourceEnforcementRequest *)
    								   malloc(sizeof(ResourceEnforcementRequest));
    task->type = MOVEOUT;
    task->pid = segmentPid;
    memset(task->cgroup_name, 0, sizeof(task->cgroup_name));
    strncpy(task->cgroup_name, cgroupName, strlen(cgroupName)+1);
    enqueue(g_queue_cgroup, (void *)task);

	rm_pfree(PCONTEXT, cgroupName);

	return true;
}

/**
 * Handle QE SetWeightGroup function call.
 */
bool handleQESetWeightCGroup(void **arg)
{
	ConnectionTrack conntrack = (ConnectionTrack)(*arg);
	Assert(conntrack != NULL);

	TimestampTz			masterStartTime;
	uint32_t			connId;
	int					segId;
	int					segmentPid;
	double					weight;

	RPCRequestSetWeightCGroup request = (RPCRequestSetWeightCGroup)
									    (conntrack->MessageBuff.Buffer);
	masterStartTime = request->MasterStartTime;
	connId			= request->ConnID;
	segId			= request->SegmentID;
	segmentPid		= request->ProcID;
	weight			= request->Weight;

	elog(DEBUG1, "Resource enforcer sets weight for QE in CGroup: "
	             "masterStartTime = %s, connId = %d, segId = %d, "
	             "procId = %d, weight = %lf",
	             timestamptz_to_str(masterStartTime),
	             connId, segmentPid, segmentPid, weight);

	/* Prepare cgroupName from masterStartTime, connId and segId information */
	char *cgroupName = buildCGroupNameString(masterStartTime, connId);

	/* Build request instance and add request into the queue. */
	ResourceEnforcementRequest *task = (ResourceEnforcementRequest *)
	                                   malloc(sizeof(ResourceEnforcementRequest));

	if ( task == NULL ) {
		elog(ERROR, "Resource enforcer fails to malloc "
		            "resource enforcement request instance");
	}
	task->type = SETWEIGHT;
	task->pid = segmentPid;
	memset(task->cgroup_name, 0, sizeof(task->cgroup_name));
	strncpy(task->cgroup_name, cgroupName, strlen(cgroupName)+1);
	task->query_resource.vcore = weight;
	if (enqueue(g_queue_cgroup, (void *)task) == -1 ) {
		elog(ERROR, "Resource enforcer fails to add "
		            "resource enforcement request into task queue");
	}

	RPCResponseSetWeightCGroupData	response;
	response.Result   = FUNC_RETURN_OK;
	response.Reserved = 0;

	buildResponseIntoConnTrack(conntrack,
	                           (char *)&response,
	                           sizeof(response),
	                           conntrack->MessageMark1,
	                           conntrack->MessageMark2,
	                           RESPONSE_QE_SETWEIGHTCGROUP);

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	rm_pfree(PCONTEXT, cgroupName);

	return true;
}


/**
 * Handle IncreaseMemQuota request from resource manager server
 */
bool handleRMIncreaseMemoryQuota(void **arg)
{
	ConnectionTrack conntrack = (ConnectionTrack)(*arg);
	Assert( conntrack != NULL );

	uint64	memquotadelta = 0;
	uint64	memquotatotal = 0;
	int		res = FUNC_RETURN_OK;

	/* Parse request */
	RPCRequestUpdateMemoryQuota request = (RPCRequestUpdateMemoryQuota)
										  (conntrack->MessageBuff.Buffer);
	memquotadelta = request->MemoryQuotaDelta;
	memquotatotal = request->MemoryQuotaTotalPending;

	elog(LOG, "Resource enforcer increases memory quota to: "
	          "total memory quota = "INT64_FORMAT" MB, "
	          "delta memory quota = "INT64_FORMAT" MB",
	          memquotatotal, memquotadelta);

	/* Update memory quota */
	uint64_t umq_beg_time = 0;
	uint64_t umq_end_time = 0;

	if (DEBUG5 >= log_min_messages)
	{
		umq_beg_time = gettime_microsec();
	}

	res = gp_update_mem_quota(memquotatotal);

	if (DEBUG5 >= log_min_messages)
	{
		umq_end_time = gettime_microsec();
	}
	elog(DEBUG1, "Resource enforcer increases memory quota "
	             "in "UINT64_FORMAT" us to: ",
	             "total memory quota = "INT64_FORMAT" MB, ",
	             "delta memory quota = "INT64_FORMAT" MB",
	             umq_end_time - umq_beg_time,
	             memquotatotal, memquotadelta);

	/* Build response */
	RPCResponseUpdateMemoryQuotaData response;
	if ( res == FUNC_RETURN_OK )
	{
		response.Result = FUNC_RETURN_OK;
		response.Reserved = 0;

		buildResponseIntoConnTrack(conntrack,
                       	   	   	   (char *)&response,
								   sizeof(response),
								   conntrack->MessageMark1,
								   conntrack->MessageMark2,
								   RESPONSE_RM_INCREASE_MEMORY_QUOTA);

	}
	else
	{
		response.Result = RESENFORCER_FAIL_UPDATE_MEMORY_QUOTA;
		response.Reserved = 0;

		buildResponseIntoConnTrack(conntrack,
                       	   	   	   (char *)&response,
								   sizeof(response),
								   conntrack->MessageMark1,
								   conntrack->MessageMark2,
								   RESPONSE_RM_INCREASE_MEMORY_QUOTA);

		elog(ERROR, "Resource enforcer fails to increase memory quota to: ",
		            "total memory quota = "INT64_FORMAT" MB, "
		            "delta memory quota = "INT64_FORMAT" MB",
		            memquotatotal, memquotadelta);
	}

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK


	return true;
}


/**
 * Handle DecreaseMemQuota request from resource manager server
 */
bool handleRMDecreaseMemoryQuota(void **arg)
{
	ConnectionTrack conntrack = (ConnectionTrack)(*arg);
	Assert( conntrack != NULL );

	uint64	memquotadelta = 0;
	uint64	memquotatotal = 0;
	int		res = FUNC_RETURN_OK;

	/* Parse request */
	RPCRequestUpdateMemoryQuota request = (RPCRequestUpdateMemoryQuota)
										  (conntrack->MessageBuff.Buffer);
	memquotadelta = request->MemoryQuotaDelta;
	memquotatotal = request->MemoryQuotaTotalPending;

	elog(LOG, "Resource enforcer decreases memory quota to: "
	          "memory quota total = "INT64_FORMAT" MB, "
	          "memory quota delta = "INT64_FORMAT" MB",
	          memquotatotal, memquotadelta);

	/* Update memory quota */
	uint64_t umq_beg_time = 0;
	uint64_t umq_end_time = 0;

	if (DEBUG5 >= log_min_messages)
	{
		umq_beg_time = gettime_microsec();
	}

	res = gp_update_mem_quota(memquotatotal);

	if (DEBUG5 >= log_min_messages)
	{
		umq_end_time = gettime_microsec();
	}
	elog(DEBUG1, "Resource enforcer decreases memory quota "
	             "in "UINT64_FORMAT" us to: ",
	             "total memory quota = "INT64_FORMAT" MB, ",
	             "delta memory quota = "INT64_FORMAT" MB",
	             umq_end_time - umq_beg_time,
	             memquotatotal, memquotadelta);

	/* Build response */
	RPCResponseUpdateMemoryQuotaData response;
	if ( res == FUNC_RETURN_OK )
	{
		response.Result = FUNC_RETURN_OK;
		response.Reserved = 0;

		buildResponseIntoConnTrack(conntrack,
                       	   	   	   (char *)&response,
								   sizeof(response),
								   conntrack->MessageMark1,
								   conntrack->MessageMark2,
								   RESPONSE_RM_DECREASE_MEMORY_QUOTA);

	}
	else
	{
		response.Result = RESENFORCER_FAIL_UPDATE_MEMORY_QUOTA;
		response.Reserved = 0;

		buildResponseIntoConnTrack(conntrack,
                       	   	   	   (char *)&response,
								   sizeof(response),
								   conntrack->MessageMark1,
								   conntrack->MessageMark2,
								   RESPONSE_RM_DECREASE_MEMORY_QUOTA);

		elog(ERROR, "Resource enforcer fails to decrease memory quota to: ",
		            "total memory quota = "INT64_FORMAT" MB, "
		            "delta memory quota = "INT64_FORMAT" MB",
		            memquotatotal, memquotadelta);
	}

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

