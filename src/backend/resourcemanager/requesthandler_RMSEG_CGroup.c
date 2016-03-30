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

/* UINT32_T.MAX_VALUE = 4294967295 */
#define MAX_DIGITS_OF_UINT32_T 10

/* Format of cgroup timestamp: YYYYMMDD_HHMMSS */
#define LENTH_CGROUP_TIMESTAMP 15

char *buildCGroupNameString(int64 masterStartTime, uint32 connId);


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

