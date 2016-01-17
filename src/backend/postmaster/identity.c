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
/*-------------------------------------------------------------------------
 *
 * identity.c
 *	  Provides the process identity management. One of the most important usage
 *	  of identity is to support unique tag. And there are two identity, one is
 *	  static one used to locate server(phasical segment). The other one is used
 *	  to run the work(query).
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "postmaster/identity.h"

#include "lib/stringinfo.h"				/* serialize */
#include "miscadmin.h"					/* IsBootstrapProcessingMode() */
#include "storage/proc.h"				/* MyProc */
#include "executor/execdesc.h"			/* AllocateResource */
#include "cdb/cdbutil.h"				/* QueryResource */
#include "optimizer/cost.h"
#include "utils/guc.h"
#include "commands/defrem.h"			/* defGetInt64() */

#include "resourcemanager/envswitch.h"
#include "resourcemanager/dynrm.h"
#include "resourcemanager/communication/rmcomm_QE_RMSEG_Protocol.h"
#include "resourcemanager/errorcode.h"
#include "resourcemanager/resourcemanager.h"
#include "resourcemanager/utils/memutilities.h"
#include "resourcemanager/communication/rmcomm_MessageHandler.h"
#include "resourcemanager/communication/rmcomm_SyncComm.h"
#include "resourcemanager/communication/rmcomm_QD2RM.h"
#include "cdb/cdbtmpdir.h"

#ifndef SHOULD_REMOVE
#include "cdb/cdbvars.h"
#endif

typedef enum SegmentRole
{
	SEGMENT_ROLE_INVALID,
	SEGMENT_ROLE_INITDB,
	SEGMENT_ROLE_MASTER,
	SEGMENT_ROLE_STANDBY,
	SEGMENT_ROLE_SEGMENT,
	SEGMENT_ROLE_STANDALONE
} SegmentRole;

typedef struct SegmentIdentity
{
	SegmentRole	role;

	/*
	 * There are two level identitiers for each process. One is Segment name,
	 * which is used to locate the physical server, another one is Query
	 * executor id, used to track the query workers.
	 *
	 * Process 'ps' state and log should output all of them.
	 *
	 * Segment name is default to 'role + hostname'.
	 */
	char		name[SEGMENT_IDENTITY_NAME_LENGTH];

	/* Allocate during register. */
	int			id;

	/* Cache our self information. */
	bool		master_address_set;
	char		master_host[SEGMENT_IDENTITY_NAME_LENGTH];
	int			master_port;

	SegmentFunctionList	function;
	ProcessIdentity		pid;
} SegmentIdentity;

static SegmentIdentity SegmentId = { SEGMENT_ROLE_INVALID };

static void DebugSegmentIdentity(struct SegmentIdentity *id);
static void DebugProcessIdentity(struct ProcessIdentity *id);
static bool	DeserializeProcessIdentity(struct ProcessIdentity *id, const char *str);
static void GetLocalTmpDirFromRM(char *host, uint16_t port, int session_id, int command_id, int qeidx);

static void
GetLocalTmpDirFromRM(char *host,
					 uint16_t port,
					 int session_id,
					 int command_id,
					 int qeidx)
{
    int res = FUNC_RETURN_OK;
    char errorbuf[ERRORMESSAGE_SIZE] = "";
    SelfMaintainBuffer sendBuffer = createSelfMaintainBuffer(PCONTEXT);
    SelfMaintainBuffer recvBuffer = createSelfMaintainBuffer(PCONTEXT);

    RPCRequestGetTmpDirFromRMSEGData request;
    request.SessionID = session_id;
    request.CommandID = command_id;
    request.Reserved1 = 0;
    request.QEIndex	  = qeidx;
    request.Reserved2 = 0;

    appendSMBVar(sendBuffer, request);

    res = callSyncRPCRemote(host,
    						port,
							sendBuffer->Buffer,
							sendBuffer->Cursor + 1,
							REQUEST_RM_TMPDIR,
							RESPONSE_RM_TMPDIR,
							recvBuffer,
							errorbuf,
							sizeof(errorbuf));

    deleteSelfMaintainBuffer(sendBuffer);

    if ( res != FUNC_RETURN_OK ) {
    	elog(ERROR, "Fail to get temp dir from resource manager (port %d). "
    			    "Error %d",
					port,
					res);
    }

	LocalTempPath = pstrdup(recvBuffer->Buffer);
    deleteSelfMaintainBuffer(recvBuffer);

    if (LocalTempPath)
    {
        elog(LOG, "GetLocalTmpDirFromRM session_id:%d command_id:%d qe_idx:%d tmpdir:%s",
                session_id, command_id, qeidx, LocalTempPath);    
    }
    else
    {
        elog(LOG, "GetLocalTmpDirFromRM session_id:%d command_id:%d qe_idx:%d tmpdir not config",
                session_id, command_id, qeidx); 
    }
}

static void
SetSegmentRole(const char *name, SegmentIdentity *segment)
{
	SegmentRole	role = SEGMENT_ROLE_INVALID;

	if (IsBootstrapProcessingMode())
		role = SEGMENT_ROLE_INITDB;
	else if (strcmp("segment", name) == 0 || strcmp("mirrorless", name) == 0)
		role =  SEGMENT_ROLE_SEGMENT;
	else if (strcmp("master", name) == 0)
		role = SEGMENT_ROLE_MASTER;
	else if (strcmp("standby", name) == 0)
		role = SEGMENT_ROLE_STANDBY;
	else
		elog(FATAL, "Invalid role: %s!", name);

	segment->role = role;
}

static void
SetupSegmentFunction(SegmentIdentity *segment)
{
	Assert(segment);
	MemSet(&segment->function, 0, sizeof(segment->function));

	switch (segment->role)
	{
		case SEGMENT_ROLE_INITDB:
			break;
		case SEGMENT_ROLE_MASTER:
			segment->function.module_motion = true;
			break;
		case SEGMENT_ROLE_STANDBY:
			segment->function.module_log_sync = true;
			break;
		case SEGMENT_ROLE_SEGMENT:
			segment->function.login_as_default = true;
			segment->function.module_motion = true;
			break;
		default:
			Assert(false);
			break;
	}
}

static void
SetupSegmentName(SegmentIdentity *segment)
{
}

static void
UnsetProcessIdentity(SegmentIdentity *segment)
{
	segment->pid.init = false;
}

void
SetSegmentIdentity(const char *name)
{
	SetSegmentRole(name, &SegmentId);
	SetupSegmentName(&SegmentId);
	SetupSegmentFunction(&SegmentId);
	UnsetProcessIdentity(&SegmentId);
}

bool
IsOnMaster(void)
{
	return SegmentId.role == SEGMENT_ROLE_MASTER;
}

SegmentFunctionList *
GetSegmentFunctionList(void)
{
	return &SegmentId.function;
}

ProcessFunctionList *
GetProcessFunctionList(void)
{
	return &SegmentId.pid.function;
}

static void
GenerateProcessIdentityLabel(ProcessIdentity *id)
{
	Assert(id->init);
}

#define	PI_SER_START_TOKEN	"ProcessIdentity_Begin_"
#define PI_SER_SLICE_TOKEN	"slice_"
#define PI_SER_IDX_TOKEN	"idx_"
#define	PI_SER_GANG_TOKEN	"gang_"
#define PI_SER_WRITER_TOKEN	"writer_"
#define PI_SER_CMD_TOKEN	"cmd_"
#define PI_SER_END_TOKEN	"End_ProcessIdentity"

const char *
SerializeProcessIdentity(ProcessIdentity *id, int *msg_len)
{
	StringInfoData	str;

#define put_token_int(token, val) \
do { \
	appendStringInfo(&str, token "%d" "_", (val)); \
} while (0)

#define put_token_bool(token, val) \
do { \
	appendStringInfo(&str, token "%s" "_", (val) ? "t" : "f"); \
} while (0)
	
	/* Should not happen, but return NULL instead of error! */
	if (!id->init)
		return NULL;

	/* Prepare to serialize */
	initStringInfo(&str);
	appendStringInfo(&str, PI_SER_START_TOKEN);

	/* serialize the data from here */
	put_token_int(PI_SER_SLICE_TOKEN, id->slice_id);
	put_token_int(PI_SER_IDX_TOKEN,  id->id_in_slice);
	put_token_int(PI_SER_GANG_TOKEN, id->gang_member_num);
	put_token_int(PI_SER_CMD_TOKEN, id->command_count);
	put_token_bool(PI_SER_WRITER_TOKEN, id->is_writer);

	/* End of serialize */
	appendStringInfo(&str, PI_SER_END_TOKEN);

	*msg_len = str.len;
	return str.data;
}

static bool
DeserializeProcessIdentity(ProcessIdentity *id, const char *str)
{
	const char	*p;

	Assert(id);
	Assert(str);

	id->init = false;
	p = str;

#define consume_token(token)	\
do { \
	if (strncmp(p, (token), strlen(token)) != 0) \
		goto error; \
	p += strlen(token); \
} while (0)

#define consume_int(val) \
do { \
	char *end; \
	(val) = strtol(p, &end, 10); \
	if (p == end) \
		goto error; \
	p = end; \
	if (*p != '_') \
		goto error; \
	p++; /* skip the '_' */ \
} while (0)

#define consume_bool(val) \
do { \
	if (*p == 't') \
		(val) = true; \
	else if (*p == 'f') \
		(val) = false; \
	else \
		goto error; \
	p++; \
	if (*p != '_') \
		goto error; \
	p++; \
} while (0)

	consume_token(PI_SER_START_TOKEN);
	consume_token(PI_SER_SLICE_TOKEN);
	consume_int(id->slice_id);
	consume_token(PI_SER_IDX_TOKEN);
	consume_int(id->id_in_slice);
	consume_token(PI_SER_GANG_TOKEN);
	consume_int(id->gang_member_num);
	consume_token(PI_SER_CMD_TOKEN);
	consume_int(id->command_count);
	consume_token(PI_SER_WRITER_TOKEN);
	consume_bool(id->is_writer);
	consume_token(PI_SER_END_TOKEN);

	return true;

error:
	return false;
}

void
SetupDispatcherIdentity(int segmentNum)
{
	SegmentId.pid.slice_id = currentSliceId;
	SegmentId.pid.id_in_slice = MASTER_CONTENT_ID;
	SegmentId.pid.gang_member_num = segmentNum;
	SegmentId.pid.command_count = gp_command_count;
	SegmentId.pid.is_writer = Gp_is_writer;

	SegmentId.pid.init = true;
	GenerateProcessIdentityLabel(&SegmentId.pid);
}

bool
SetupProcessIdentity(const char *str)
{
	bool ret = false;

	elog(DEBUG1, "SetupProcessIdentity: receive msg: %s", str);
	ret = DeserializeProcessIdentity(&SegmentId.pid, str);

	DebugSegmentIdentity(&SegmentId);
	DebugProcessIdentity(&SegmentId.pid);

	GpIdentity.segindex = SegmentId.pid.id_in_slice;
	currentSliceId = SegmentId.pid.slice_id;

	SetConfigOption("gp_is_writer", SegmentId.pid.is_writer ? "true" : "false",
					PGC_POSTMASTER, PGC_S_OVERRIDE);

	MyProc->mppIsWriter = SegmentId.pid.is_writer;
	gp_command_count = SegmentId.pid.command_count;
	TempPath = "/tmp";
	SegmentId.pid.init = true;
	GenerateProcessIdentityLabel(&SegmentId.pid);

    if (Gp_role == GP_ROLE_EXECUTE)
    {
        initializeDRMInstanceForQE();
        
        if (get_tmpdir_from_rm)
        {
            /* If QE is under one segment. */
            if ( GetQEIndex() != -1 ) {
                GetLocalTmpDirFromRM("127.0.0.1",//DRMGlobalInstance->SocketLocalHostName.Str,
                                     rm_segment_port,
                                     gp_session_id,
                                     gp_command_count,
                                     GetQEIndex());
            }
            /* QE is under master. */
            else {
                GetLocalTmpDirFromRM("127.0.0.1",//DRMGlobalInstance->SocketLocalHostName.Str,
                                     rm_master_port,
                                     gp_session_id,
                                     gp_command_count,
                                     GetQEIndex());
            }

            elog(DEBUG1, "Get temporary directory from segment resource manager, %s",
        		    LocalTempPath);
        }
        else
        {
            getSegmentLocalTmpDirFromShmem(gp_session_id, gp_command_count, GetQEIndex());
            elog(DEBUG1, "getSegmentLocalTmpDirFromShmem session_id:%d command_id:%d qeidx:%d tmpdir:%s", gp_session_id, gp_command_count, GetQEIndex(), LocalTempPath);
        }
    }

	return ret;
}

bool
AmIMaster(void)
{
	return SegmentId.role == SEGMENT_ROLE_MASTER;
}

bool
AmIStandby(void)
{
	return SegmentId.role == SEGMENT_ROLE_STANDBY;
}

bool
AmISegment(void)
{
	return SegmentId.role == SEGMENT_ROLE_SEGMENT;
}

int
GetQEIndex(void)
{
	return SegmentId.pid.init ? SegmentId.pid.id_in_slice : GpIdentity.segindex;
}

int
GetQEGangNum(void)
{
	return SegmentId.pid.init ? SegmentId.pid.gang_member_num : 0;
}

int
GetUtilPartitionNum(void)
{
	return default_segment_num;
}

int
GetPlannerSegmentNum(void)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		return 1;

	if (gp_segments_for_planner > 0)
	{
		return gp_segments_for_planner;
	}

	return GetUtilPartitionNum();
}
/** 
  *	Read Relation Option from statement WITH options
  */
int GetRelOpt_bucket_num_fromOptions(List *options, int default_val)
{
	int bucketnum =0;
	ListCell   *cell;

	/* Scan list to see if "bucketnum" was included */
	if(options)
		foreach(cell, options)
		{
			DefElem    *def = (DefElem *) lfirst(cell);

			if (pg_strcasecmp(def->defname, "bucketnum") == 0)
			{
				bucketnum = (int) defGetInt64(def);
				break;
			}	
		}
	return ((bucketnum>0)?bucketnum : (default_val));
}
/** 
  *	Read Relation Option from catalog Relation
  */
int 
GetRelOpt_bucket_num_fromRel(Relation relation, int default_val)
{
	int bucket_num =0;
	
	if(relation && relation->rd_options)  
		bucket_num = ((StdRdOptions *) (relation)->rd_options)->bucket_num;

	return ((bucket_num>0)?bucket_num : (default_val));
}
/** 
  *	Read Relation Option from catalog Relation RangeVar
  */
int 
GetRelOpt_bucket_num_fromRangeVar(const RangeVar* rel_rv, int default_val)
{
	int ret_val=0;
	Relation rel;
	
	rel = try_relation_openrv(rel_rv, AccessExclusiveLock, true);
	ret_val = GetRelOpt_bucket_num_fromRel(rel, default_val);
	if(rel) relation_close(rel, NoLock);

	return ret_val;
}

int
GetRandomDistPartitionNum(void)
{
	return default_segment_num;
}

int
GetHashDistPartitionNum(void)
{
	return default_segment_num;
}

int
GetExternalTablePartitionNum(void)
{
	return default_segment_num;
}

int
GetAllWorkerHostNum(void)
{
	List *segments = GetSegmentList();
	int num = list_length(segments);

	list_free(segments);

	return num;
}

static void
DebugSegmentIdentity(SegmentIdentity *id)
{
}

static void
DebugProcessIdentity(ProcessIdentity *id)
{
	if (!id->init)
	{
		elog(DEBUG1, "ProcessIdentity is not init");
	}

	elog(DEBUG1, "ProcessIdentity: "
				"slice %d "
				"id %d "
				"gang num %d "
				"writer %s",
				id->slice_id,
				id->id_in_slice,
				id->gang_member_num,
				id->is_writer ? "t" : "f");
}

/*
 * get master's local transaction id.
 * in hawq, there is no distributed transaction.
 * master's transaction is dispatched from master to segments.
 */
TransactionId
GetMasterTransactionId(void)
{
	if (Gp_role == GP_ROLE_DISPATCH)
		return GetCurrentTransactionId();

	return SegmentId.pid.xid;
}

/*
 * set dispatched master's transaction id on QE
 */
void
SetMasterTransactionId(TransactionId xid)
{
	Assert(Gp_role == GP_ROLE_EXECUTE);

	SegmentId.pid.xid = xid;
}

void
SetMasterAddress(char *address, int port)
{
	if (SegmentId.master_address_set)
		return;

	StrNCpy(SegmentId.master_host, address, sizeof(SegmentId.master_host));
	SegmentId.master_port = port;
	SegmentId.master_address_set = true;
}

void
GetMasterAddress(char **address, int *port)
{
	if (!SegmentId.master_address_set)
	{
	  Segment *master = GetMasterSegment();
	  char *address = master->hostip ? master->hostip : master->hostname;
	  SetMasterAddress(address, master->port);
	  FreeSegment(master);
	}

	*address = SegmentId.master_host;
	*port = SegmentId.master_port;
}

bool
IsWriter(void)
{
  return SegmentId.pid.init ? SegmentId.pid.is_writer : false;
}
