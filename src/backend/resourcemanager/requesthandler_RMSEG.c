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
	uint16_t failedTmpDirNum =
								list_length(DRMGlobalInstance->LocalHostFailedTmpDirList);
	if (failedTmpDirNum > 0)
	{
		SelfMaintainBufferData buf;
		initializeSelfMaintainBuffer(&buf, PCONTEXT);
		uint16_t idx = 0;
		ListCell *lc = NULL;
		foreach(lc, DRMGlobalInstance->LocalHostFailedTmpDirList)
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
						  "from '%s' to '%s'", GET_SEGINFO_FAILEDTMPDIR(info), failedTmpDirStr.Str);
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
		RMSEG_INBUILDHOST->FTSAvailable     = RESOURCE_SEG_STATUS_AVAILABLE;
		RMSEG_INBUILDHOST->Info.ID          = SEGSTAT_ID_INVALID;
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
	while (retry > 0)
	{
		retry--;
		conn = PQconnectdb(conninfo);
		if ((libpqres = PQstatus(conn)) != CONNECTION_OK)
		{
			if (retry == 0)
			{
				elog(LOG, "Segment receives RUAlive from master "
						  "and postmaster is down, PQconnectdb result : %d, %s",
						  libpqres,
						  PQerrorMessage(conn));
				/* Don't send IMAlive anymore */
				DRMGlobalInstance->SendIMAlive = false;
			}
			else
			{
				PQfinish(conn);
				pg_usleep(500000);
				continue;
			}
		}
		else
		{
			elog(LOG, "Segment receives RUAlive from master and postmaster is healthy.");
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
	             "in "UINT64_FORMAT" us to: "
	             "total memory quota = "UINT64_FORMAT" MB, "
	             "delta memory quota = "UINT64_FORMAT" MB",
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

		elog(WARNING, "Resource enforcer fails to increase memory quota to: "
		              "total memory quota = "UINT64_FORMAT" MB, "
		              "delta memory quota = "UINT64_FORMAT" MB",
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
	             "in "UINT64_FORMAT" us to: "
	             "total memory quota = "UINT64_FORMAT" MB, "
	             "delta memory quota = "UINT64_FORMAT" MB",
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

		elog(WARNING, "Resource enforcer fails to decrease memory quota to: "
		              "total memory quota = "UINT64_FORMAT" MB, "
		              "delta memory quota = "UINT64_FORMAT" MB",
		              memquotatotal, memquotadelta);
	}

	conntrack->ResponseSent = false;
	MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	PCONTRACK->ConnToSend = lappend(PCONTRACK->ConnToSend, conntrack);
	MEMORY_CONTEXT_SWITCH_BACK

	return true;
}

