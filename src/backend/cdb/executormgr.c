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

/*
 * executormgr.c
 *
 *
 */
#include "postgres.h"

#include "cdb/dispatcher.h"
#include "cdb/executormgr.h"
#include "cdb/poolmgr.h"

#include "cdb/cdbconn.h"	/* SegmentDatabaseDescriptor */
#include "cdb/cdbgang.h"	/* Gang */

#include "catalog/pg_authid.h"	/* TODO:BOOTSTRAP_USER_ID remove! */
#include "cdb/cdbdisp.h"		/* TODO: DispatchCommandQueryParms */
#include "cdb/cdbdispatchresult.h"	/* TODO: CdbDispatchResult & cdbdisp_makeDispatchResults */
#include "cdb/cdbvars.h"		/* TODO: gp_commond_count */
#include "miscadmin.h"			/* TODO: MyDatabaseId */
#include "libpq/libpq-be.h"			/* TODO: MyProcPort */
#include "commands/dbcommands.h"	/* TODO: get_database_dts */
#include "utils/lsyscache.h"	/* TODO: get_rolname */
#include "utils/guc_tables.h"	/* TODO: manipulate gucs */
#include "portability/instr_time.h"	/* Monitor the dispatcher performance */

typedef enum ExecutorMgrConstant {
	EXECUTORMGR_CANCEL_ERROR_BUFFER_SIZE = 256,
} ExecutorMgrConstant;

typedef enum QueryExecutorState {
	QES_UNINIT,			/* Uninit state */
	QES_DISPATCHABLE,	/* Init state */
	QES_RUNNING,		/* Work dispatched */
	QES_STOP,			/* No more data */
} QueryExecutorState;

typedef enum QueryExecutorHealth {
	QEH_NA,
	QEH_GOOD,
	QEH_CANCEL,
	QEH_ERROR,
} QueryExecutorHealth;

typedef struct QueryExecutor {
	/* Executor State */
	bool			takeovered;		/* Someone outside the dispacher will release the executor. */
	SegmentDatabaseDescriptor	*desc;
	struct Gang		*refGang;
	QueryExecutorState	state;
	QueryExecutorHealth	health;

	/* Error State */
	struct CdbDispatchResult	*refResult;	/* TODO: libpq is ugly, should invent a new connection manager. */

	/* Reference to dispatch data. */
	struct DispatchSlice	*refSlice;
	struct DispatchTask		*refTask;

	/* Serialized identity state */
	const char	*identity_msg;
	int			identity_msg_len;

	instr_time	time_dispatch_begin;
	instr_time	time_dispatch_end;
	instr_time	time_connect_begin;
	instr_time	time_connect_end;
  instr_time  time_consume_begin;
  instr_time  time_consume_end;
  instr_time  time_free_begin;
  instr_time  time_free_end;
} QueryExecutor;

typedef struct ExecutorCache {
	bool				init;
	MemoryContext		ctx;
	struct PoolMgrState	*pool;
	struct PoolMgrState *entrydb_pool; // pool for entry db connection
	int		cached_num;
	int		allocated_num;
	int		takeover_num;
} ExecutorCache;

static ExecutorCache	executor_cache;
#define GetSegmentHashKey(segment)	(((segment)->master) ? "master" : (segment)->hostname)


static void executormgr_catch_error(QueryExecutor *executor);
static void executormgr_destory(SegmentDatabaseDescriptor *desc);
static struct CdbDispatchResult	*executormgr_get_executor_result(QueryExecutor *executor);

void
executormgr_setup_env(MemoryContext ctx)
{
	MemoryContext old;

	if (executor_cache.init)
		return;

	executor_cache.pool = poolmgr_create_pool(ctx, (PoolMgrCleanCallback) executormgr_destory);
	executor_cache.entrydb_pool = poolmgr_create_pool(ctx, (PoolMgrCleanCallback) executormgr_destory);
	executor_cache.ctx = ctx;
	executor_cache.init = true;

	/* TODO: Setup dispatcher information. But should remove in the future. */
	old = MemoryContextSwitchTo(ctx);
	MyProcPort->dboid = MyDatabaseId;
	MyProcPort->dbdtsoid = get_database_dts(MyDatabaseId);
	MyProcPort->bootstrap_user = get_rolname(BOOTSTRAP_SUPERUSERID);
	MyProcPort->encoding = GetDatabaseEncoding();
	MemoryContextSwitchTo(old);
}

void
executormgr_cleanup_env(void)
{
	if (!executor_cache.init)
		return;

	if (executor_cache.allocated_num != 0)
		elog(WARNING, "%d segments was allocated but not returned during cleanup executor manager.", executor_cache.allocated_num);
	if (executor_cache.takeover_num != 0)
		elog(WARNING, "%d segments was takeovered but not returned during cleanup executor manager.", executor_cache.takeover_num);

	poolmgr_drop_pool(executor_cache.pool);
	poolmgr_drop_pool(executor_cache.entrydb_pool);
}

static void
executormgr_count_executors_num(SegmentDatabaseDescriptor *desc, int *count)
{
	*count += 1;
}

int
executormgr_get_cached_executor_num(void)
{
	int		ret = 0;

	Assert(executor_cache.init);

	poolmgr_iterate(executor_cache.pool, NULL, (PoolMgrIterateCallback) executormgr_count_executors_num, (PoolIterateArg) &ret);
	return ret;
}

int
executormgr_get_cached_executor_num_onentrydb(void)
{
  int   ret = 0;

  Assert(executor_cache.init);

  poolmgr_iterate(executor_cache.entrydb_pool, NULL,
                  (PoolMgrIterateCallback) executormgr_count_executors_num,
                  (PoolIterateArg) &ret);
  return ret;
}

QueryExecutor *
executormgr_create_executor(void)
{
	QueryExecutor	*executor = palloc0(sizeof(QueryExecutor));

	INSTR_TIME_SET_ZERO(executor->time_dispatch_begin);
	INSTR_TIME_SET_ZERO(executor->time_dispatch_end);
	INSTR_TIME_SET_ZERO(executor->time_connect_begin);
	INSTR_TIME_SET_ZERO(executor->time_connect_end);
  INSTR_TIME_SET_ZERO(executor->time_consume_begin);
  INSTR_TIME_SET_ZERO(executor->time_consume_end);
  INSTR_TIME_SET_ZERO(executor->time_free_begin);
  INSTR_TIME_SET_ZERO(executor->time_free_end);

	return executor;
}

/*
 * executormgr_bind_executor_task
 *	For each task in slice, allocate an executor and bind it to task.
 */
bool
executormgr_bind_executor_task(struct DispatchData *data,
							QueryExecutor *executor,
							SegmentDatabaseDescriptor *desc,
							struct DispatchTask *task,
							struct DispatchSlice *slice)
{
  Assert(desc != NULL);

  executor->state = QES_UNINIT;
	executor->desc = desc;
	executor->state = QES_DISPATCHABLE;
	executor->health = QEH_NA;

	/* setup the owner of the executor */
	executor->takeovered = false;

	/* setup payload */
	executor->refSlice = slice;
	executor->refTask = task;

	/* TODO: set result slot */
	executor->refResult = cdbdisp_makeResult(dispatch_get_results(data),
											executor->desc,
											dispatch_get_task_identity(task)->slice_id);

	if (executor->refResult == NULL)
	  return false;

	/* Transfer any connection errors from segdbDesc. */
	executormgr_merge_error(executor);

	return true;
}

void
executormgr_serialize_executor_state(struct DispatchData *data,
							QueryExecutor *executor,
							struct DispatchTask *task,
							struct DispatchSlice *slice)
{
	executor->identity_msg = SerializeProcessIdentity(dispatch_get_task_identity(task), &executor->identity_msg_len);	
}

void
executormgr_unbind_executor_task(struct DispatchData *data,
							QueryExecutor *executor,
							struct DispatchTask *task,
							struct DispatchSlice *slice)
{
	if (executor->state == QES_UNINIT)
		return;

	/* Return executors */
	TIMING_BEGIN(executor->time_free_begin);
	if (!executor->takeovered)
		executormgr_free_executor(executor->desc);
	executor->state = QES_UNINIT;
	TIMING_END(executor->time_free_end);
}

void
executormgr_get_statistics(QueryExecutor *executor,
					instr_time *time_connect_begin,
					instr_time *time_connect_end,
					instr_time *time_dispatch_begin,
					instr_time *time_dispatch_end,
					instr_time *time_consume_begin,
          instr_time *time_consume_end,
          instr_time *time_free_begin,
          instr_time *time_free_end)
{
	INSTR_TIME_ASSIGN(*time_connect_begin, executor->time_connect_begin);
	INSTR_TIME_ASSIGN(*time_connect_end, executor->time_connect_end);
	INSTR_TIME_ASSIGN(*time_dispatch_begin, executor->time_dispatch_begin);
	INSTR_TIME_ASSIGN(*time_dispatch_end, executor->time_dispatch_end);
  INSTR_TIME_ASSIGN(*time_consume_begin, executor->time_consume_begin);
  INSTR_TIME_ASSIGN(*time_consume_end, executor->time_consume_end);
  INSTR_TIME_ASSIGN(*time_free_begin, executor->time_free_begin);
  INSTR_TIME_ASSIGN(*time_free_end, executor->time_free_end);
}

void
executormgr_get_executor_connection_info(QueryExecutor *executor,
							char **address, int *port, int *pid)
{
	if (address)
		*address = pstrdup(executor->desc->segment->hostip);

	if (Gp_interconnect_type == INTERCONNECT_TYPE_UDP)
		*port = (executor->desc->motionListener >> 16) & 0x0ffff;
	else
		*port = (executor->desc->motionListener & 0x0ffff);

	*pid = executor->desc->backendPid;
}

int
executormgr_get_executor_slice_id(QueryExecutor *executor)
{
	return dispatch_get_task_identity(executor->refTask)->slice_id;
}

/*
 * executormgr_is_stop
 */
bool
executormgr_is_stop(QueryExecutor *executor)
{
	return executor->state == QES_STOP;
}

bool
executormgr_has_error(QueryExecutor *executor)
{
	return executor->health == QEH_ERROR;
}

static struct CdbDispatchResult *
executormgr_get_executor_result(QueryExecutor *executor)
{
	return executor->refResult;
}

int
executormgr_get_fd(QueryExecutor *executor)
{
	return PQsocket(executor->desc->conn);
}

bool
executormgr_cancel(QueryExecutor *executor)
{
	PGconn			*conn = executor->desc->conn;
	PGcancel		*cn;
	char			errbuf[EXECUTORMGR_CANCEL_ERROR_BUFFER_SIZE];
	bool			success;

	cn = PQgetCancel(conn);
	MemSet(errbuf, 0, sizeof(errbuf));

	success = (PQcancel(cn, errbuf, sizeof(errbuf)) != 0);

#if 0
	if (success)
	{
		executor->state = QES_STOP;
		executor->health = QEH_CANCEL;
	}
	else
	{
		/* TODO: log error? how to deal with connection error. */
		executormgr_catch_error(executor);
	}
#endif

	{
		write_log("function executormgr_cancel calling executormgr_catch_error");
		executormgr_catch_error(executor);
	}
	PQfreeCancel(cn);
	return success;
}

static bool
executormgr_validate_conn(PGconn *conn)
{
  if (conn == NULL)
    return false;
  if (!dispatch_validate_conn(conn->sock))
  {
    printfPQExpBuffer(&conn->errorMessage,
              libpq_gettext(
                  "server closed the connection unexpectedly\n"
             "\tThis probably means the server terminated abnormally\n"
                 "\tbefore or while processing the request.\n"));
    conn->status = CONNECTION_BAD;
    closesocket(conn->sock);
    conn->sock = -1;
    return false;
  }
  return true;
}

/*
 * executormgr_is_dispatchable
 *	Return the true iff executor can receive query.
 */
static bool
executormgr_is_dispatchable(QueryExecutor *executor)
{
	PGconn						*conn = executor->desc->conn;

	Assert(executor->state == QES_DISPATCHABLE);

	if (PQisBusy(conn))
	{
		/* TODO: dead code? */
		if (!executormgr_discard(executor) && PQisBusy(conn))
			return false;
	}

	if (!executormgr_validate_conn(conn) || PQstatus(conn) == CONNECTION_BAD)
	{
		write_log("function executormgr_is_dispatchable meets error, connection is bad.");
		executormgr_catch_error(executor);
		return false;
	}

	return true;
}

/*
 * executormgr_dispatch_and_run
 *	Dispatch data and run the query.
 */
bool
executormgr_dispatch_and_run(struct DispatchData *data, QueryExecutor *executor)
{
	PGconn		*conn = executor->desc->conn;
	char		*query = NULL;
	int			query_len;
	DispatchCommandQueryParms	*parms = dispatcher_get_QueryParms(data);

	if (!executormgr_is_dispatchable(executor))
	  goto error;

	TIMING_BEGIN(executor->time_dispatch_begin);
	query = PQbuildGpQueryString(parms->strCommand, parms->strCommandlen,
								parms->serializedQuerytree, parms->serializedQuerytreelen,
								parms->serializedPlantree, parms->serializedPlantreelen,
								parms->serializedParams, parms->serializedParamslen,
								parms->serializedSliceInfo, parms->serializedSliceInfolen,
								NULL, 0,
								executor->identity_msg, executor->identity_msg_len,
								parms->serializedQueryResource, parms->serializedQueryResourcelen,
								0,
								gp_command_count,
								executormgr_get_executor_slice_id(executor),
								parms->rootIdx,
								parms->seqServerHost, parms->seqServerHostlen, parms->seqServerPort,
								parms->primary_gang_id,
								GetCurrentStatementStartTimestamp(),
								GetSessionUserId(),	/* For external tools who want this info on segments. */
								IsAuthenticatedUserSuperUser(),
								BOOTSTRAP_SUPERUSERID,
								true,
								BOOTSTRAP_SUPERUSERID,
								&query_len);

	if (PQsendGpQuery_shared(conn, query, query_len) == 0)
		goto error;

  if (Debug_print_execution_detail) {
    instr_time  time;
    INSTR_TIME_SET_CURRENT(time);
    write_log("The time after dispatching : %s to conn %s, isNonBlocking %d, number of chars waiting"
        "in buffer %d", query, conn->gpqeid, PQisnonblocking(conn), conn->outCount);
  }

	TIMING_END(executor->time_dispatch_end);
	free(query);
	executor->state = QES_RUNNING;
	executor->health = QEH_GOOD;
	executor->refResult->hasDispatched = true;

	return true;

error:
	if (query)
		free(query);
	write_log("function executormgr_dispatch_and_run meets error.");
	executormgr_catch_error(executor);
	return false;
}

/*
 * executormgr_consume
 *	If there are data available for executor, use this interface to consume data.
 *	Return false if there is an error. Need to check executor state if returns
 *	true.
 */
bool
executormgr_consume(QueryExecutor *executor)
{
	PGconn			*conn = executor->desc->conn;
	int				rc;
	bool			done = false;

	TIMING_BEGIN(executor->time_consume_begin);
	CdbDispatchResult	*resultSlot = executormgr_get_executor_result(executor);

	if ((rc = PQconsumeInput(conn)) == 0)
		goto connection_error;

	while (!PQisBusy(conn))
	{
		PGresult		*result;
		ExecStatusType	status_type;
		int				result_index;

		if (PQstatus(conn) == CONNECTION_BAD)
			goto connection_error;
		result_index = cdbdisp_numPGresult(resultSlot);
		result = PQgetResult(conn);

		/* Normal command finished */
		if (!result)
		{
			done = true;
			break;
		}

		/* Transfer the result to resultSlot, so we don't need to cleanup! */
		cdbdisp_appendResult(resultSlot, result);

		status_type = PQresultStatus(result);
		if (status_type == PGRES_COMMAND_OK ||
			status_type == PGRES_TUPLES_OK ||
			status_type == PGRES_COPY_IN ||
			status_type == PGRES_COPY_OUT)
		{
			resultSlot->okindex = result_index;
			if (result->numRejected > 0)
				resultSlot->numrowsrejected += result->numRejected;
			if (status_type == PGRES_COPY_IN || status_type == PGRES_COPY_OUT)
			{
				done = true;
				break;
			}
		}
		else
		{
			char	*sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
			int		errcode = 0;

			/* TODO: error? */
			if (sqlstate && strlen(sqlstate) == 5)
				errcode = cdbdisp_sqlstate_to_errcode(sqlstate);
			cdbdisp_seterrcode(errcode, result_index, resultSlot);
			goto connection_error;
		}
	}

	if (done)
		executor->state = QES_STOP;

  TIMING_END(executor->time_consume_end);
	return true;

connection_error:
	/* Let caller deal with connection error. */
	write_log("function executormgr_consume meets error, connection is bad.");
	executormgr_catch_error(executor);
	return false;
}

/*
 * executormgr_discard
 *	Discard the useless results in a executor.
 */
bool
executormgr_discard(QueryExecutor *executor)
{
	PGresult		*result;
	PGconn			*conn = executor->desc->conn;
	bool			ret;

	result = PQgetResult(conn);
	ret = result != NULL;
	PQclear(result);

	return ret;
}

/*
 * executormgr_catch_error
 *	Currently, only connection error is an executor error.
 */
static void
executormgr_catch_error(QueryExecutor *executor)
{
	PGconn			*conn = executor->desc->conn;
	char			*msg;
	int       errCode = 0;
	if (executor->refResult->errcode != 0)
	  errCode = executor->refResult->errcode;

	msg = PQerrorMessage(conn);

	if (msg && (strcmp("", msg) != 0) && (executor->refResult->errcode == 0)) {
	  errCode = ERRCODE_GP_INTERCONNECTION_ERROR;
	}

	PQExpBufferData selfDesc;
	initPQExpBuffer(&selfDesc);
	appendPQExpBuffer(&selfDesc, "(seg%d %s:%d)",
	                  executor->desc->segment->segindex,
	                  executor->desc->segment->hostname,
	                  executor->desc->segment->port);

  if (!executor->refResult->error_message) {
    cdbdisp_appendMessage(
        executor->refResult,
        LOG,
        errCode,
        "%s %s: %s",
        (executor->state == QES_DISPATCHABLE ?
            "Error dispatching to" :
            (executor->state == QES_RUNNING ?
                "Query Executor Error in" : "Error in ")),
        (executor->desc->whoami && strcmp(executor->desc->whoami, "") != 0) ?
            executor->desc->whoami : selfDesc.data,
        msg ? msg : "unknown error");
  }

  termPQExpBuffer(&selfDesc);
  PQfinish(conn);

	executor->desc->conn = NULL;

	executor->state = QES_STOP;
	executor->health = QEH_ERROR;
}

void
executormgr_merge_error(QueryExecutor *executor)
{
	if (executor->state == QES_UNINIT)
		return;
	if (executormgr_is_executor_error(executor))
		cdbdisp_mergeConnectionErrors(executor->refResult, executor->desc);
}

void
executormgr_merge_error_for_dispatcher(
    QueryExecutor *executor, int *errHostSize,
    int *errNum, char ***errHostInfo)
{
  if (executor->state == QES_UNINIT)
    return;
  int errCode = executor->refResult->errcode;
  if (ERRCODE_GP_INTERCONNECTION_ERROR == errCode
      || ERRCODE_ADMIN_SHUTDOWN == errCode) {
    // add host to *errHostInfo
    char *host = executor->desc->segment->hostname;
    int hostNameLen = strlen(host);
    (*errHostInfo)[*errNum] = (char*)palloc0(hostNameLen + 1);
    strcpy((*errHostInfo)[*errNum], host);
    (*errNum)++;
    if(*errNum == *errHostSize) {
      (*errHostSize) *= 2;
      (*errHostInfo) = (char**) repalloc((*errHostInfo),
                                         *errHostSize * sizeof(char *));
    }
  }
  if (executormgr_is_executor_error(executor)) {
    cdbdisp_mergeConnectionErrors(executor->refResult, executor->desc);
  }
}

bool
executormgr_is_executor_error(QueryExecutor *executor) {
  if (executor->desc->errcode || executor->desc->error_message.len)
    return true;
  else
    return false;
}

bool
executormgr_is_executor_valid(QueryExecutor *executor) {
  return executor->desc != NULL;
}

SegmentDatabaseDescriptor *
executormgr_takeover_segment_conns(QueryExecutor *executor)
{
	executor->takeovered = true;
	executor_cache.takeover_num++;
	executor_cache.allocated_num--;

	return executor->desc;
}

void
executormgr_free_takeovered_segment_conn(SegmentDatabaseDescriptor *desc)
{
	executor_cache.takeover_num--;
	executor_cache.allocated_num++;
	executormgr_free_executor(desc);
}

static SegmentDatabaseDescriptor *
executormgr_allocate_any_executor(bool is_writer, bool is_entrydb)
{
  // get executor from pool and check whether the connection is valid, keep
  // running until finding a valid one or the pool becomes NULL
  struct PoolMgrState *executor_pool =
      is_entrydb ? executor_cache.entrydb_pool : executor_cache.pool;
  SegmentDatabaseDescriptor *desc = poolmgr_get_random_item(executor_pool);
  while (desc != NULL && !executormgr_validate_conn(desc->conn)) {
    desc = poolmgr_get_random_item(executor_pool);
  }
  return desc;
}

static SegmentDatabaseDescriptor *
executormgr_allocate_executor_by_name(const char *name, bool is_writer)
{
  // get executor from pool and check whether the connection is valid, keep
  // running until finding a valid one or the pool becomes NULL
  SegmentDatabaseDescriptor *desc =
      poolmgr_get_item_by_name(executor_cache.pool, name);
  while (desc != NULL && !executormgr_validate_conn(desc->conn)) {
    desc = poolmgr_get_item_by_name(executor_cache.pool, name);
  }
  return desc;
}	

/*
 * executormgr_allocate_executor
 *	Allocate an executor for specific slice/task.
 */
SegmentDatabaseDescriptor *
executormgr_allocate_executor(Segment *segment, bool is_writer, bool is_entrydb)
{
	SegmentDatabaseDescriptor *ret;

	if (is_entrydb || (segment != NULL && segment->master))
	  ret = executormgr_allocate_any_executor(is_writer, true);
	else if (segment == NULL)
		ret = executormgr_allocate_any_executor(is_writer, false);
	else
		ret = executormgr_allocate_executor_by_name(GetSegmentHashKey(segment), is_writer);
	if (!ret)
		return NULL;

	executor_cache.allocated_num++;
	executor_cache.cached_num--;
	return ret;
}

/*
 * executormgr_free_executor
 *	Free an executor.
 */
void
executormgr_free_executor(SegmentDatabaseDescriptor *desc)
{
	executor_cache.allocated_num--;
	if (!desc->conn)
	{
		/* executor has connection error, remove it. */
		executormgr_destory(desc);
		return;
	}

	desc->conn->asyncStatus = PGASYNC_IDLE;
//	pqClearAsyncResult(desc->conn);
	if (desc->segment->master)
	  poolmgr_put_item(executor_cache.entrydb_pool, GetSegmentHashKey(desc->segment), desc);
	else
	  poolmgr_put_item(executor_cache.pool, GetSegmentHashKey(desc->segment), desc);

	executor_cache.cached_num++;
}

static bool
executormgr_add_address(SegmentDatabaseDescriptor *segdbDesc, PQExpBuffer str)
{
	Segment	*segment = segdbDesc->segment;

    /*
     * If init buffer failed, set segdbDesc.errcode, err_message, conn set null and return,
     * the caller cdbgang.c will later check the status of segdbDesc and output to user.
     */
    if(str->maxlen == 0)
    {
    	segdbDesc->errcode = ERRCODE_OUT_OF_MEMORY;
    	appendPQExpBuffer(&segdbDesc->error_message,
			  "Master unable to connect, malloc memory structure failure");
    	segdbDesc->conn = NULL;
    	return false;
    }

	/*
	 * On the master, we must use UNIX domain sockets for security -- as it can
	 * be authenticated. See MPP-15802.
	 */
	if (!segment->master)
	{
		/*
		 * First we pick the cached hostip if we have it.
		 *
		 * If we don't have a cached hostip, we use the host->address,
		 * if we don't have that we fallback to host->hostname.
		 */
		if (segment->hostip != NULL)
		{
	        appendPQExpBuffer(str, "hostaddr=%s ", segment->hostip);
		}
	    else if (segment->hostname == NULL)
		{
	        appendPQExpBufferStr(str, "host='' " );
		}
	    else if (isdigit(segment->hostname[0]))
		{
	        appendPQExpBuffer(str, "hostaddr=%s ", segment->hostname);
		}
	    else
		{
	        appendPQExpBuffer(str, "host=%s ", segment->hostname);
		}
	}

    appendPQExpBuffer(str, "port=%u ", segment->port);

	/* 
	 * XXX: PQconnectdb() doesn't handle embedded quotes (') but they can be
	 * in a valid database name.
	 */
    if (MyProcPort->database_name)
        appendPQExpBuffer(str, "dbname='%s' ", MyProcPort->database_name);

    appendPQExpBuffer(str, "user='%s' ", MyProcPort->user_name);

    appendPQExpBuffer(str, "connect_timeout=%d ", gp_segment_connect_timeout);

	appendPQExpBuffer(str, "dboid=%u ", MyProcPort->dboid);
	appendPQExpBuffer(str, "dbdtsoid=%u ", MyProcPort->dbdtsoid);
	appendPQExpBuffer(str, "bootstrap_user=%s ", MyProcPort->bootstrap_user);
	appendPQExpBuffer(str, "encoding=%d ", MyProcPort->encoding);

	return true;
}

static bool
addOneOption(PQExpBufferData *buffer, struct config_generic * guc)
{
	Assert(guc && (guc->flags & GUC_GPDB_ADDOPT));
	switch (guc->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *bguc = (struct config_bool *) guc;

				appendPQExpBuffer(buffer, " -c %s=%s", guc->name,
								  *(bguc->variable) ? "true" : "false"
					);
				return true;
			}
		case PGC_INT:
			{
				struct config_int *iguc = (struct config_int *) guc;

				appendPQExpBuffer(buffer, " -c %s=%d", guc->name, *iguc->variable);
				return true;
			}
		case PGC_REAL:
			{
				struct config_real *rguc = (struct config_real *) guc;

				appendPQExpBuffer(buffer, " -c %s=%f", guc->name, *rguc->variable);
				return true;
			}
		case PGC_STRING:
			{
				struct config_string *sguc = (struct config_string *) guc;
				const char *str = *sguc->variable;
				unsigned int	 j, start, size;
				char			*temp, *new_temp;

				size = 256;
				temp = malloc(size + 8);
				if (temp == NULL)
					return false;

				j = 0;
				for (start = 0; start < strlen(str); ++start)
				{
					if (j == size)
					{
						size *= 2;
						new_temp = realloc(temp, size + 8);
						if (new_temp == NULL)
						{
							free(temp);
							return false;
						}
						temp = new_temp;
					}

					if (str[start] == ' ')
					{
						temp[j++] = '\\';
						temp[j++] = '\\';
					} else if (str[start] == '"' || str[start] == '\'')
						temp[j++] = '\\';

					temp[j++] = str[start];
				}

				temp[j] = '\0';
				appendPQExpBuffer(buffer, " -c %s=%s", guc->name, temp);
				free(temp);

				return true;
			}
	}

	Assert(!"Invalid guc var type");
	return false;
}


static bool
executormgr_add_guc(PQExpBuffer str, bool is_superuser)
{
	struct config_generic **gucs = get_guc_variables();
	int						ngucs = get_num_guc_variables();
	int						i;

	appendPQExpBufferStr(str, "options='");

	/*
	 * Transactions are tricky.
	 * Here is the copy and pasted code, and we know they are working.
	 * The problem, is that QE may ends up with different iso level, but
	 * postgres really does not have read uncommited and repeated read.
	 * (is this true?) and they are mapped.
	 *
	 * Put these two gucs in the generic framework works (pass make installcheck-good)
	 * if we make assign_defaultxactisolevel and assign_XactIsoLevel correct take
	 * string "readcommitted" etc.	(space stripped).  However, I do not
	 * want to change this piece of code unless I know it is broken.
	 */
	if (DefaultXactIsoLevel == XACT_SERIALIZABLE)
		appendPQExpBuffer(str, " -c default_transaction_isolation=serializable");

	if (XactIsoLevel == XACT_SERIALIZABLE)
		appendPQExpBuffer(str, " -c transaction_isolation=serializable");

	for (i = 0; i < ngucs; ++i)
	{
		struct config_generic *guc = gucs[i];

		if ((guc->flags & GUC_GPDB_ADDOPT) &&
			(guc->context == PGC_USERSET || is_superuser))
		{
			if (!addOneOption(str, guc))
				return false;
		}
	}

	/* Add the default database/user gucs */
	if (MyProcPort->override_options.len)
	{
		appendPQExpBuffer(str, " %s", MyProcPort->override_options.data);
	}

	appendPQExpBuffer(str, "' ");

	return true;
}

static bool
executormgr_add_static_state(PQExpBuffer str, bool is_writer)
{
	char	*master_host;
	int		master_port;

	appendPQExpBuffer(str, "gpqeid=%d;", gp_session_id);

#ifdef HAVE_INT64_TIMESTAMP
	appendPQExpBuffer(str, INT64_FORMAT ";", PgStartTime);
#else
#ifndef _WIN32
	appendPQExpBuffer(str, "%.14a;", PgStartTime);
#else
	appendPQExpBuffer(str, "%g;", PgStartTime);
#endif
#endif

	GetMasterAddress(&master_host, &master_port);
	/* dispatcher host name for external table */
	appendPQExpBuffer(str, "%s;", master_host);
	/* dispatcher host port for external table */
	appendPQExpBuffer(str, "%d;", master_port);
	appendPQExpBuffer(str, "%s;", is_writer ? "true" : "false");

	/* change last semicolon to space */
	Assert(str->data[str->len - 1] == ';');
	str->data[str->len - 1] = ' ';

	return true;
}

SegmentDatabaseDescriptor *
executormgr_prepare_connect(Segment *segment, bool is_writer)
{
	SegmentDatabaseDescriptor	*desc;
	Segment						*long_lived_segment;

	/* Executor have to exist for a long period. */
	long_lived_segment = CopySegment(segment, executor_cache.ctx);
	desc = MemoryContextAlloc(executor_cache.ctx, sizeof(*desc));
	cdbconn_initSegmentDescriptor(desc, long_lived_segment);

	executor_cache.allocated_num++;
	return desc;
}

bool
executormgr_connect(SegmentDatabaseDescriptor *desc, QueryExecutor *executor,
					bool is_writer, bool is_superuser)
{
	PQExpBufferData buffer;

	/* Build the connection string. */
	initPQExpBuffer(&buffer);
	if (buffer.maxlen == 0)
		goto error;

	if (!executormgr_add_address(desc, &buffer))
		goto error;

	if (!executormgr_add_guc(&buffer, is_superuser))
		goto error;

	if (!executormgr_add_static_state(&buffer, is_writer))
		goto error;

	TIMING_BEGIN(executor->time_connect_begin);
	if (!cdbconn_doConnect(desc, buffer.data))
		goto error;
	TIMING_END(executor->time_connect_end);

	free(buffer.data);
	return true;

error:
	termPQExpBuffer(&buffer);
	return false;
}

static void
executormgr_destory(SegmentDatabaseDescriptor *desc)
{
	PQfinish(desc->conn);
	desc->conn = NULL;
	cdbconn_termSegmentDescriptor(desc);
	FreeSegment(desc->segment);
	pfree(desc);
}

