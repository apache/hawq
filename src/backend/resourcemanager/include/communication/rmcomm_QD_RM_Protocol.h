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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_QD_RM_PROTOCOL_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_QD_RM_PROTOCOL_H

#include "resourcemanager/envswitch.h"

#define RPC_PROTOCOL_STRUCT_BEGIN(protoname)								   \
	struct protoname##Data {

#define RPC_PROTOCOL_STRUCT_END(protoname)									   \
	};																		   \
	typedef struct protoname##Data		protoname##Data;					   \
	typedef struct protoname##Data	   *protoname;

/*
 *------------------------------------------------------------------------------
 * QD to RM.
 *
 * Notify RM to update resource queue definitions.
 *
 * Request format:
 *		uint32_t 		conn_id
 *		uint16_t		manipulation action (1=create, 2=alter, 3=drop)
 *		uint16_t		with attribute length
 *		a list of strings:
 *			<resource queue name> \0
 *			<attribute name> \0 <attribute value> \0  -> with attributes.
 *			<attribute name> \0 <attribute value> \0  -> with attributes.
 *			<attribute name> \0						  -> without attributes.
 *			<attribute name> \0
 *		append multiple \0 to make 64-bit aligned.
 *
 * Protocol of argument 'options'.
 *
 * 	There are always one defnode named 'withliststart' and one defnode named
 * 	'withoutliststart'. the node 'withliststart' starts a list of all options to
 * 	update into the target queue, then the node 'withoutliststart' starts a list
 * 	of options to remove from the target queue.
 *
 * 	"withliststart"-> opt_1 w/ value_1 -> opt_2 w/ value2 ->...->
 * 	"withoutliststart"->opt_n -> opt_n+1
 *
 * Response format (SUCCEED):
 * 		uint32_t		return code
 * 		uint8_t			action count
 * 		uint8_t			reserved[3]
 * 		uint32_t		action data size
 * 		uint8_t			action code (1=create,2=alter,3=drop)
 * 		uint8_t 		column count
 * 		uint8_t			reserved[2]
 * 		int64_t			queue oid
 * 		uint8_t			column index x column count
 *		column new value \0 column new value \0 ...
 *		append multiple \0  to make 64-bit aligned.
 *
 * Response format (ERROR):
 * 		uint32_t 		return code
 * 		uint32_t 		reserved
 * 		string of error. (optional)
 *		append multiple \0 to make 64-bit aligned.
 *------------------------------------------------------------------------------
 */

enum ManipulateResQueue_Action_Enum {
	MANIPULATE_RESQUEUE_UNSET = 0,
	MANIPULATE_RESQUEUE_CREATE,
	MANIPULATE_RESQUEUE_ALTER,
	MANIPULATE_RESQUEUE_DROP
};

RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestHeadManipulateResQueue)
	uint32_t	ConnID;
	uint16_t	ManipulateAction;
	uint16_t	WithAttrLength;
RPC_PROTOCOL_STRUCT_END(RPCRequestHeadManipulateResQueue)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseHeadManipulateResQueue)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseHeadManipulateResQueue)

enum ManipulateRoleResQueue_Action_Enum {
	MANIPULATE_ROLE_RESQUEUE_UNSET = 0,
	MANIPULATE_ROLE_RESQUEUE_CREATE,
	MANIPULATE_ROLE_RESQUEUE_ALTER,
	MANIPULATE_ROLE_RESQUEUE_DROP
};

RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestHeadManipulateRole)
	Oid		 RoleOID;
	Oid		 QueueOID;
	uint16_t Action;
	int8_t	 isSuperUser;
	int8_t   Reserved[5];
	char	 Name[64];
RPC_PROTOCOL_STRUCT_END(RPCRequestHeadManipulateRole)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseHeadManipulateRole)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseHeadManipulateRole)

enum CatResQueueAction_Action_Enum {
	CATRESQUEUE_ACTION_ACTION_UNSET = 0,
	CATRESQUEUE_ACTION_ACTION_INSERT,
	CATRESQUEUE_ACTION_ACTION_UPDATE,
	CATRESQUEUE_ACTION_ACTION_DELETE
};

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseHeadManipulateResQueueERROR)
	RPCResponseHeadManipulateResQueueData	Result;
	char									ErrorText[1];
RPC_PROTOCOL_STRUCT_END(RPCResponseHeadManipulateResQueueERROR)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseHeadManipulateRoleERROR)
	RPCResponseHeadManipulateRoleData		Result;
	char									ErrorText[1];
RPC_PROTOCOL_STRUCT_END(RPCResponseHeadManipulateRoleERROR)

/*******************************************************************************
 * Protocol of registerConnection In RM By Str.
 ******************************************************************************/

/*
 * Response format:
 * 		uint32_t 		return code
 * 		int32_t 		conn_id
 * 		char			error message [in case failure]
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseRegisterConnectionInRMByStr)
	uint32_t	Result;
	int32_t		ConnID;
RPC_PROTOCOL_STRUCT_END(RPCResponseRegisterConnectionInRMByStr)

/*******************************************************************************
 * Protocol of registerConnection In RM By OID.
 ******************************************************************************/
/*
 * Request format:
 *		uint64_t 		useridoid;
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestRegisterConnectionInRMByOID)
	int64_t     UseridOid;
RPC_PROTOCOL_STRUCT_END(RPCRequestRegisterConnectionInRMByOID)

/*
 * Response format:
 * 		uint32_t 		return code
 * 		uint32_t 		conn_id
 * 		char			error message [in case failure]
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseRegisterConnectionInRMByOID)
	uint32_t	Result;
	uint32_t	ConnID;
RPC_PROTOCOL_STRUCT_END(RPCResponseRegisterConnectionInRMByOID)

/*******************************************************************************
 * Protocol of UnregisterConnection In RM.
 ******************************************************************************/
/*
 * Request format:
 *		uint32_t 		conn_id
 *		uint32_t		reserved
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestHeadUnregisterConnectionInRM)
	uint32_t 		ConnID;
	uint32_t		Reserved;
RPC_PROTOCOL_STRUCT_END(RPCRequestHeadUnregisterConnectionInRM)

/*
 * Response format:
 * 		uint32_t 		return code
 * 		uint32_t		reserved
 * 		char			error message [in case failure]
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseUnregisterConnectionInRM)
	uint32_t		Result;
	uint32_t		Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseUnregisterConnectionInRM)


/*******************************************************************************
 * Protocol of AcquireResource From RM.
 ******************************************************************************/
/*
 * Request format:
 * 		uint64_t		session id
 *		uint32_t 		conn_id
 *		uint32_t 		preferred node count N ( can be 0 )
 *		uint32_t		maximum vseg count expected
 *		uint32_t 		minimum vseg count expected
 *		int32_t 		slicesize
 *		uint32_t		vseg per seg limit
 *		uint32_t		vseg limit
 *		uint32_t		statement vseg memory quota in MB
 *		uint32_t		statement vseg count
 *		uint32_t		reserved
 *		int64_t			estimated io size
 *		int64_t * N     node IO scan size array
 *		char	 		hostnames array
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestHeadAcquireResourceFromRM)
	int64_t			SessionID;
	uint32_t 		ConnID;
	uint32_t		NodeCount;
	uint32_t		MaxSegCountFix;
	uint32_t		MinSegCountFix;
	int32_t			SliceSize;
	uint32_t		VSegLimitPerSeg;
	uint32_t		VSegLimit;
	uint32_t		StatVSegMemoryMB;
	uint32_t		StatNVSeg;
	uint32_t		Reserved;
	int64_t			IOBytes;
RPC_PROTOCOL_STRUCT_END(RPCRequestHeadAcquireResourceFromRM)

/*
 * Response format:
 * 		uint32_t return code
 * 		uint32_t reserved1
 * 		uint32_t seg number
 * 		uint32_t seg memory MB
 * 		double   seg core
 * 		uint32_t physical host count
 * 		a list of hdfs hostname index array ( uint32_t array, 64-bit aligned )
 * 		a list of host information offset   ( uint32_t array, 64-bit aligned )
 * 		a list of resource information including machine info.
 * 			host 1
 * 			host 2
 * 			...
 * 			host m
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseHeadAcquireResourceFromRM)
	uint32_t	Result;
	uint32_t	Reserved1;
	uint32_t	SegCount;
	uint32_t	SegMemoryMB;
	double 		SegCore;
	uint32_t	HostCount;
	uint32_t 	Reserved2;
RPC_PROTOCOL_STRUCT_END(RPCResponseHeadAcquireResourceFromRM)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseAcquireResourceFromRMERROR)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseAcquireResourceFromRMERROR)

/*******************************************************************************
 * Protocol of AcquireResourceQuota From RM by OID.
 ******************************************************************************/
/*
 * Request format:
 * 		int64_t  	user oid
 *		uint32_t 	scansize_split
 *		uint32_t	segment count fixed
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestHeadAcquireResourceQuotaFromRMByOID)
	int64_t			UseridOid;		/* Decide the target resource queue. */
	uint32_t		MaxSegCountFix;
	uint32_t		MinSegCountFix;
	uint32_t		VSegLimitPerSeg;
	uint32_t		VSegLimit;
	uint32_t		StatVSegMemoryMB;
	uint32_t		StatNVSeg;
RPC_PROTOCOL_STRUCT_END(RPCRequestHeadAcquireResourceQuotaFromRMByOID)

/*
 * Response format:
 * 		uint32_t return code
 * 		uint32_t reserved1
 * 		uint32_t seg number
 * 		uint32_t seg memory MB
 * 		double   seg core
 * 		queue name string
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseHeadAcquireResourceQuotaFromRMByOID)
	uint32_t	Result;
	uint32_t	Reserved1;
	uint32_t	SegNum;
	uint32_t    SegNumMin;
	uint32_t	SegMemoryMB;
	uint32_t    Reserved2;
	double 		SegCore;
	char		QueueName[64];
RPC_PROTOCOL_STRUCT_END(RPCResponseHeadAcquireResourceQuotaFromRMByOID)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseHeadAcquireResourceQuotaFromRMByOIDERROR)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseHeadAcquireResourceQuotaFromRMByOIDERROR)

/*******************************************************************************
 * Protocol of ReturnResource.
 ******************************************************************************/
/*
 * Request format:
 *		uint32_t 		conn_id
 *		uint32_t		reserved (0)
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestHeadReturnResource)
	uint32_t 		ConnID;
	uint32_t		Reserved;
RPC_PROTOCOL_STRUCT_END(RPCRequestHeadReturnResource)

/*
 * Response format:
 * 		uint32_t return code
 */

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseHeadReturnResource)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseHeadReturnResource)

/*******************************************************************************
 * Protocol of GetClusterSize.
 ******************************************************************************/
/*
 * Response format:
 * 		uint32_t return code
 * 		uint32_t cluster size
 */

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseHeadGetClusterSize)
	uint32_t	Result;
	uint32_t	ClusterSize;
RPC_PROTOCOL_STRUCT_END(RPCResponseHeadGetClusterSize)

/*******************************************************************************
 * Protocol of SegmentIsDown.
 ******************************************************************************/
/*
 * Response format:
 * 		uint32_t return code
 * 		uint32_t reserved
 */

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseSegmentIsDown)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseSegmentIsDown)

/*******************************************************************************
 * Protocol of RefreshResourceHeartBeat.
 ******************************************************************************/
/*
 * Request format:
 * 		uint32_t connid count
 * 		uint32_t reserved
 * 		int32_t  connid 1
 * 		int32_t  contid 2
 * 		...
 */

RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestHeadRefreshResourceHeartBeat)
	int32_t		ConnIDCount;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCRequestHeadRefreshResourceHeartBeat)

/*
 * Response format:
 * 		uint32_t result
 * 		uint32_t reserved
 */
RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseRefreshResourceHeartBeat)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseRefreshResourceHeartBeat)

RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestTmpDirForQD)
	uint64_t    Reserved;
RPC_PROTOCOL_STRUCT_END(RPCRequestTmpDirForQD)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseTmpDirForQD)
	uint32_t	Result;
	uint32_t	Reserved;
    char        tmpdir[1024];
RPC_PROTOCOL_STRUCT_END(RPCResponseTmpDirForQD)


RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestDumpStatus)
	uint32_t    type;
	uint32_t    Reserved;
    char        dump_file[1024];
RPC_PROTOCOL_STRUCT_END(RPCRequestDumpStatus)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseDumpStatus)
    uint32_t    Result;
	uint32_t    Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseDumpStatus)


RPC_PROTOCOL_STRUCT_BEGIN(ResQueueStatus)
    char        name[128];
    int32_t     segmem;
    int32_t     segsize;
    int32_t     segsizemax;
    int32_t     inusemem;
    int32_t     holders;
    int32_t     waiters;
    char		pausedispatch;
    char		reserved[7];
    double      segcore;
    double      inusecore;
RPC_PROTOCOL_STRUCT_END(ResQueueStatus)

RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestResQueueStatus)
    uint64_t    Reserved;
RPC_PROTOCOL_STRUCT_END(RPCRequestResQueueStatus)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseResQueueStatus)
    uint32_t            Result;
    uint32_t            queuenum;
    ResQueueStatusData  queuedata[1];
RPC_PROTOCOL_STRUCT_END(RPCResponseResQueueStatus)

/*******************************************************************************
 * Protocol of Segment Resource Quota Control Request.
 ******************************************************************************/
RPC_PROTOCOL_STRUCT_BEGIN(RPCRequestQuotaControl)
	uint32_t	Pause;
	uint32_t	Phase;
RPC_PROTOCOL_STRUCT_END(RPCRequestQuotaControl)

RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseQuotaControl)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseQuotaControl)
/*******************************************************************************
 * Protocol of Dummy Request.
 ******************************************************************************/
RPC_PROTOCOL_STRUCT_BEGIN(RPCResponseDummy)
	uint32_t	Result;
	uint32_t	Reserved;
RPC_PROTOCOL_STRUCT_END(RPCResponseDummy)
#endif /*RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_QD_RM_PROTOCOL_H*/
