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

#include "postgres.h"

#include "cdb/executormgr_new.h"

#include "catalog/pg_authid.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/dispatcher.h"
#include "cdb/dispatcher_new.h"
#include "cdb/executormgr.h"
#include "cdb/poolmgr.h"
#include "commands/dbcommands.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"
#include "utils/faultinjector.h"

typedef enum MyQueryExecutorState {
  MYQES_UNINIT,  /* Uninit state */
  MYQES_INIT,    /* Init state */
  MYQES_RUNNING, /* Work dispatched */
  MYQES_STOPPED, /* No more data */
} QueryExecutorState;

typedef struct MyQueryExecutor {
  bool isWriter;
  bool isSuperuser;
  struct MyDispatchTask *refTask;
  SegmentDatabaseDescriptor *desc;
  struct CdbDispatchResult *refResult;

  const char *idMsg;
  int idMsgLen;

  int execIndex;

  QueryExecutorState state;

  // instrument
  instr_time connectBegin;
  instr_time connectEnd;
  instr_time dispatchBegin;
  instr_time dispatchEnd;
  int dataSize;
} MyQueryExecutor;

typedef struct MyExecutorCache {
  MemoryContext ctx;
  struct PoolMgrState *qePool;
  struct PoolMgrState *entryDbPool;
  int cachedNum;
} MyExecutorCache;

static MyExecutorCache executorCache;

static void executormgr_destory(SegmentDatabaseDescriptor *desc) {
  PQfinish(desc->conn);
  desc->conn = NULL;
  cdbconn_termSegmentDescriptor(desc);
  FreeSegment(desc->segment);
  pfree(desc);
}

static bool executormgr_validate_conn(PGconn *conn) {
  if (conn == NULL) return false;
  if (!dispatch_validate_conn(conn->sock)) {
    printfPQExpBuffer(
        &conn->errorMessage,
        libpq_gettext("server closed the connection unexpectedly\n"
                      "\tThis probably means the server terminated abnormally\n"
                      "\tbefore or while processing the request.\n"));
    conn->status = CONNECTION_BAD;
    closesocket(conn->sock);
    conn->sock = -1;
    return false;
  }
  return true;
}

static SegmentDatabaseDescriptor *executormgr_allocate_any_executor(
    bool entryDb) {
  struct PoolMgrState *pool =
      entryDb ? executorCache.entryDbPool : executorCache.qePool;
  SegmentDatabaseDescriptor *desc = poolmgr_get_random_item(pool);
  while (desc != NULL && !executormgr_validate_conn(desc->conn)) {
    desc = poolmgr_get_random_item(pool);
  }
  return desc;
}

static SegmentDatabaseDescriptor *executormgr_allocate_executor_by_name(
    const char *name) {
  SegmentDatabaseDescriptor *desc =
      poolmgr_get_item_by_name(executorCache.qePool, name);
  while (desc != NULL && !executormgr_validate_conn(desc->conn)) {
    desc = poolmgr_get_item_by_name(executorCache.qePool, name);
  }
  return desc;
}

static void executormgr_freeExecutor(struct SegmentDatabaseDescriptor *desc) {
  if (!desc->conn) {
    executormgr_destory(desc);
    return;
  }

  desc->conn->asyncStatus = PGASYNC_IDLE;
  if (desc->segment && desc->segment->master)
    poolmgr_put_item(executorCache.entryDbPool, getSegmentKey(desc->segment),
                     desc);
  else if (desc->segment == NULL)
    poolmgr_put_item(executorCache.qePool, "segment", desc);
  else
    poolmgr_put_item(executorCache.qePool, getSegmentKey(desc->segment), desc);

  ++executorCache.cachedNum;
}

static bool executormgr_executorHasErr(struct MyQueryExecutor *qe) {
  return qe->desc->errcode || qe->desc->error_message.len;
}

void executormgr_setupEnv(MemoryContext ctx) {
  executorCache.qePool =
      poolmgr_create_pool(ctx, (PoolMgrCleanCallback)executormgr_destory);
  executorCache.entryDbPool =
      poolmgr_create_pool(ctx, (PoolMgrCleanCallback)executormgr_destory);
  executorCache.ctx = ctx;

  MemoryContext old = MemoryContextSwitchTo(ctx);
  MyProcPort->dboid = MyDatabaseId;
  MyProcPort->dbdtsoid = get_database_dts(MyDatabaseId);
  MyProcPort->bootstrap_user = get_rolname(BOOTSTRAP_SUPERUSERID);
  MyProcPort->encoding = GetDatabaseEncoding();
  MemoryContextSwitchTo(old);
}

void executormgr_cleanupEnv() {
  poolmgr_drop_pool(executorCache.qePool);
  poolmgr_drop_pool(executorCache.entryDbPool);
}

struct SegmentDatabaseDescriptor *executormgr_allocateExecutor(
    struct Segment *segment, bool entryDb) {
  SegmentDatabaseDescriptor *ret = NULL;

  if (entryDb || (segment != NULL && segment->master))
    ret = executormgr_allocate_any_executor(true);
  else if (segment == NULL)
    ret = executormgr_allocate_any_executor(false);
  else
    ret = executormgr_allocate_executor_by_name(getSegmentKey(segment));

  if (ret) --executorCache.cachedNum;

  return ret;
}

void executormgr_unbindExecutor(struct MyQueryExecutor *qe) {
  executormgr_freeExecutor(qe->desc);
}

struct SegmentDatabaseDescriptor *executormgr_prepareConnect(
    struct Segment *segment) {
  Segment *savedSegment =
      segment ? CopySegment(segment, executorCache.ctx) : NULL;
  SegmentDatabaseDescriptor *desc =
      MemoryContextAlloc(executorCache.ctx, sizeof(*desc));
  cdbconn_initSegmentDescriptor(desc, savedSegment);
  return desc;
}

struct MyQueryExecutor *executormgr_makeQueryExecutor(
    struct Segment *segment, bool isWriter, struct MyDispatchTask *task,
    int sliceIndex) {
  MyQueryExecutor *qe = palloc0(sizeof(MyQueryExecutor));

  qe->isWriter = isWriter;
  qe->isSuperuser = superuser_arg(GetAuthenticatedUserId());
  qe->refTask = task;
  setTaskRefQE(task, qe);
  qe->refResult =
      cdbdisp_makeResult(getDispatchResults(task), NULL, sliceIndex);

  qe->state = MYQES_INIT;
  qe->refResult->hasDispatched = true;

  return qe;
}

void executormgr_setQueryExecutorIndex(struct MyQueryExecutor *qe, int index) {
  qe->execIndex = index;
}

void executormgr_setQueryExecutorIdMsg(struct MyQueryExecutor *qe, char *idMsg,
                                       int idMsgLen) {
  qe->idMsg = idMsg;
  qe->idMsgLen = idMsgLen;
}

void executormgr_getConnectInfo(struct MyQueryExecutor *qe, char **address,
                                int *port, int *myPort, int *pid) {
  if (address) *address = pstrdup(qe->desc->segment->hostip);

  if (Gp_interconnect_type == INTERCONNECT_TYPE_UDP)
    *port = (qe->desc->motionListener >> 16) & 0x0ffff;
  else
    *port = (qe->desc->motionListener & 0x0ffff);

  *myPort = (qe->desc->my_listener & 0x0ffff);

  *pid = qe->desc->backendPid;
}

bool executormgr_main_doconnect(struct MyQueryExecutor *qe) {
  PQExpBufferData connMsg;
  initPQExpBuffer(&connMsg);
  uint32 qeNum =
      list_length(list_nth(getTaskPerSegmentList(qe->refTask), qe->execIndex));
  uint32 n32 = htonl(qeNum);
  appendBinaryPQExpBuffer(&connMsg, (char *)&n32, 4);
  executormgr_add_address(qe->desc, true, &connMsg);
  executormgr_add_guc(&connMsg, qe->isSuperuser);
  executormgr_add_static_state(&connMsg, qe->isWriter);

  PQExpBufferData buffer;
  /* Build the connection string. */
  initPQExpBuffer(&buffer);
  if (buffer.maxlen == 0) goto error;

  if (!qe->desc->conn) {
    if (!executormgr_add_address(qe->desc, false, &buffer)) goto error;

    if (!executormgr_addDispGuc(&buffer, qe->isSuperuser)) goto error;

    if (!executormgr_add_static_state(&buffer, qe->isWriter)) goto error;
  }

  TIMING_BEGIN(qe->connectBegin);
  if (!cdbconn_main_doconnect(qe->desc, buffer.data, &connMsg)) goto error;
  TIMING_END(qe->connectEnd);

  free(connMsg.data);
  free(buffer.data);
  return true;

error:
  termPQExpBuffer(&connMsg);
  termPQExpBuffer(&buffer);
  return false;
}

bool executormgr_main_run(struct MyQueryExecutor *qe) {
  if (!executormgr_idDispatchable(qe)) return false;

  DispatchCommandQueryParms *parms = getQueryParms(qe->refTask);
  int queryLen;
  char *query = PQbuildGpNewQueryString(
      parms->strCommand, parms->strCommandlen, parms->serializedQuerytree,
      parms->serializedQuerytreelen, parms->serializedPlantree,
      parms->serializedPlantreelen, parms->serializedParams,
      parms->serializedParamslen, parms->serializedSliceInfo,
      parms->serializedSliceInfolen, NULL, 0, qe->idMsg, qe->idMsgLen,
      parms->serializedQueryResource, parms->serializedQueryResourcelen,
      parms->serializedCommonPlan, parms->serializedCommonPlanLen, 0,
      gp_command_count, parms->rootIdx, parms->seqServerHost,
      parms->seqServerHostlen, parms->seqServerPort, parms->primary_gang_id,
      GetCurrentStatementStartTimestamp(),
      GetSessionUserId(), /* For external tools who want this info on segments.
                           */
      IsAuthenticatedUserSuperUser(), BOOTSTRAP_SUPERUSERID, true,
      BOOTSTRAP_SUPERUSERID, &queryLen);

  TIMING_BEGIN(qe->dispatchBegin);
  PGconn *conn = qe->desc->conn;
  if (!PQsendGpQuery_shared(conn, query, queryLen)) return false;
  TIMING_END(qe->dispatchEnd);

  free(query);
  qe->dataSize = queryLen;
  qe->state = MYQES_RUNNING;
  return true;
}

bool executormgr_main_consumeData(struct MyQueryExecutor *qe) {
  PGconn *conn = qe->desc->conn;
  int rc;
  bool done = false;
  struct MyQueryExecutor *myQe = qe;

#ifdef FAULT_INJECTOR
  // expect FaultInjectorType: FaultInjectorTypeDispatchError,
  FaultInjectorType_e ret =
      FaultInjector_InjectFaultIfSet(MainDispatchConsumeData, DDLNotSpecified,
                                     "",   // databaseName
                                     "");  // tableName
  if (ret == FaultInjectorTypeDispatchError) goto error;
#endif

  if ((rc = PQconsumeInput(conn)) == 0) goto error;
  while (!PQisBusy(conn)) {
    /* Normal command finished */
    if (conn->asyncStatus == PGASYNC_IDLE) {
      done = true;
      break;
    }
    if (PQstatus(conn) == CONNECTION_BAD) goto error;

    if (conn->dispBuffer.len != 0) {
      int qeIndex;
      cdbdisp_deserializeDispatchResult(NULL, &qeIndex, &conn->dispBuffer);
      myQe = getTaskRefQE((struct MyDispatchTask *)(list_nth(
          list_nth(getTaskPerSegmentList(qe->refTask), qe->execIndex),
          qeIndex)));
      struct CdbDispatchResult *refResult = myQe->refResult;
      cdbdisp_deserializeDispatchResult(refResult, &qeIndex, &conn->dispBuffer);
      conn->asyncStatus = PGASYNC_BUSY;
      if (refResult->errcode != 0) {
        cdbdisp_seterrcode(refResult->errcode, -1, refResult);
        goto error;
      }
    } else {
      PQgetResult(conn);
      write_log("main dispatcher got error msg from proxy dispatcher: %s",
                conn->errorMessage.data);
      goto error;
    }
  }

  if (done) qe->state = MYQES_STOPPED;

  return true;

error:
  executormgr_catchError(myQe, true);
  return false;
}

bool executormgr_proxy_consumeData(struct MyQueryExecutor *qe) {
  PGconn *conn = qe->desc->conn;
  bool done = false;
  CdbDispatchResult *resultSlot = qe->refResult;

#ifdef FAULT_INJECTOR
  // expect FaultInjectorType: FaultInjectorTypeDispatchError,
  // FaultInjectorQuietExit
  FaultInjectorType_e ret =
      FaultInjector_InjectFaultIfSet(ProxyDispatchConsumeData, DDLNotSpecified,
                                     "",   // databaseName
                                     "");  // tableName
  if (ret == FaultInjectorTypeDispatchError) goto error;
#endif

  if (!PQconsumeInput(conn)) goto error;

  while (!PQisBusy(conn)) {
    if (PQstatus(conn) == CONNECTION_BAD) goto error;

    int resultIndex = cdbdisp_numPGresult(resultSlot);
    PGresult *result = PQgetResult(conn);

    /* Normal command finished */
    if (!result) {
      done = true;
      break;
    }

    cdbdisp_appendResult(resultSlot, result);

    ExecStatusType statusType = PQresultStatus(result);
    if (statusType == PGRES_COMMAND_OK || statusType == PGRES_TUPLES_OK ||
        statusType == PGRES_COPY_IN || statusType == PGRES_COPY_OUT) {
      resultSlot->okindex = resultIndex;
      if (result->numRejected > 0)
        resultSlot->numrowsrejected += result->numRejected;
      if (statusType == PGRES_COPY_IN || statusType == PGRES_COPY_OUT) {
        done = true;
        break;
      }
    } else {
      char *sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
      int errCode = 0;
      if (sqlstate && strlen(sqlstate) == 5)
        errCode = cdbdisp_sqlstate_to_errcode(sqlstate);
      cdbdisp_seterrcode(errCode, resultIndex, resultSlot);
      goto error;
    }
  }

  if (done) {
    qe->state = MYQES_STOPPED;
    executormgr_sendback(qe);
  }

  return true;

error:
  return false;
}

bool executormgr_proxy_doconnect(struct MyQueryExecutor *qe) {
#ifdef FAULT_INJECTOR
  // expect FaultInjectorType: FaultInjectorTypeDispatchError
  FaultInjectorType_e ret =
      FaultInjector_InjectFaultIfSet(ProxyDispatcherConnect, DDLNotSpecified,
                                     "",   // databaseName
                                     "");  // tableName
  if (ret == FaultInjectorTypeDispatchError) goto error;
#endif

  if (!cdbconn_proxy_doconnect(qe->desc, getTaskConnMsg(qe->refTask)))
    goto error;

  return true;

error:
  return false;
}

bool executormgr_proxy_run(struct MyQueryExecutor *qe, char **msg, int32 *len) {
  PGconn *conn = qe->desc->conn;

  if (!executormgr_idDispatchable(qe)) return false;

  if (*msg == NULL) {
    char *connMsg = getTaskConnMsg(qe->refTask);
    *len = ntohl(*((int32 *)(connMsg + 1))) + 1;
    *msg = malloc(*len);
    memcpy(*msg, connMsg, *len);
  }

  // fill in executor index
  int execId = htonl(qe->execIndex);
  memcpy(*msg + 5, &execId, 4);

  if (!PQsendGpQuery_shared(conn, *msg, *len)) return false;

  qe->state = MYQES_RUNNING;
  return true;
}

struct SegmentDatabaseDescriptor *executormgr_getSegDesc(
    struct MyQueryExecutor *qe) {
  if (!qe->desc) {
    qe->desc = palloc0(sizeof(SegmentDatabaseDescriptor));
    cdbconn_initSegmentDescriptor(qe->desc, NULL);
  }
  return qe->desc;
}

void executormgr_setSegDesc(struct MyQueryExecutor *qe,
                            struct SegmentDatabaseDescriptor *desc) {
  qe->desc = desc;
}

bool executormgr_isStopped(struct MyQueryExecutor *qe) {
  return qe->state == MYQES_STOPPED;
}

int executormgr_getFd(struct MyQueryExecutor *qe) {
  return PQsocket(qe->desc->conn);
}

static bool executormgr_cleanCachedExecutorFilter(PoolItem item) {
  SegmentDatabaseDescriptor *desc = (SegmentDatabaseDescriptor *)item;
  return desc->conn->asyncStatus == PGASYNC_IDLE;
}

bool executormgr_hasCachedExecutor() { return executorCache.cachedNum > 0; }

void executormgr_cleanCachedExecutor() {
  if (executormgr_hasCachedExecutor()) {
    poolmgr_clean(executorCache.qePool,
                  (PoolMgrIterateFilter)executormgr_cleanCachedExecutorFilter);
    poolmgr_clean(executorCache.entryDbPool,
                  (PoolMgrIterateFilter)executormgr_cleanCachedExecutorFilter);
  }
}

bool executormgr_main_cancel(struct MyQueryExecutor *qe) {
  PGconn *conn = qe->desc->conn;
  PGcancel *cn = PQgetCancel(conn);

  char errbuf[256];
  MemSet(errbuf, 0, sizeof(errbuf));
  bool success = (PQcancel(cn, errbuf, sizeof(errbuf)) != 0);
  if (!success) {
    write_log("executormgr_main_cancel cancel failed, %s.", errbuf);
  }
  PQfreeCancel(cn);

  PQfinish(conn);
  qe->desc->conn = NULL;
  qe->state = MYQES_STOPPED;

  return success;
}

bool executormgr_proxy_cancel(struct MyQueryExecutor *qe, bool cancelRequest) {
  bool success = true;
  PGconn *conn = qe->desc->conn;

  if (cancelRequest) {
    PGcancel *cn = PQgetCancel(conn);
    char errbuf[256];
    MemSet(errbuf, 0, sizeof(errbuf));
    success = (PQcancel(cn, errbuf, sizeof(errbuf)) != 0);
    if (!success) {
      write_log("executormgr_proxy_cancel cancel failed, %s.", errbuf);
    }
    PQfreeCancel(cn);
  }

  PQfinish(conn);
  qe->desc->conn = NULL;
  qe->state = MYQES_STOPPED;

  return success;
}

void executormgr_setErrCode(struct MyQueryExecutor *qe) {
  CdbDispatchResult *resultSlot = qe->refResult;
  if (resultSlot->errcode == ERRCODE_SUCCESSFUL_COMPLETION ||
      resultSlot->errcode == ERRCODE_INTERNAL_ERROR)
    cdbdisp_seterrcode(ERRCODE_INTERNAL_ERROR, -1, resultSlot);
}

void executormgr_mergeDispErr(struct MyQueryExecutor *qe, int *errHostNum,
                              char ***errHostInfo) {
  if (!qe->desc) return;

  int errCode = qe->desc->errcode ? qe->desc->errcode : qe->refResult->errcode;
  if (ERRCODE_GP_INTERCONNECTION_ERROR == errCode ||
      ERRCODE_ADMIN_SHUTDOWN == errCode) {
    char *hostInfo = qe->desc->segment->hostname;
    (*errHostInfo)[*errHostNum] = (char *)palloc0(strlen(hostInfo) + 1);
    strcpy((*errHostInfo)[*errHostNum], hostInfo);
    ++*errHostNum;
  }

  if (executormgr_executorHasErr(qe))
    cdbdisp_mergeConnectionErrors(qe->refResult, qe->desc);
}

void executormgr_catchError(struct MyQueryExecutor *qe, bool formatError) {
  PGconn *conn = qe->desc->conn;
  int errCode = qe->refResult->errcode;
  char *errMsg = PQerrorMessage(conn);
  if (errMsg && (strcmp("", errMsg) != 0)) {
    if (errCode == 0) errCode = ERRCODE_GP_INTERCONNECTION_ERROR;
    if (!qe->refResult->error_message) {
      cdbdisp_appendMessage(qe->refResult, LOG, errCode, "%s",
                            errMsg ? errMsg : "unknown error");
    }
  }
  if (formatError && qe->refResult->error_message) {
    char *msgBegin = malloc(qe->refResult->error_message->len + 1);
    memset(msgBegin, 0, qe->refResult->error_message->len + 1);
    char *msgBeginBackup = msgBegin;
    memcpy(msgBegin, qe->refResult->error_message->data,
           qe->refResult->error_message->len);
    char *msgEnd = msgBegin + qe->refResult->error_message->len;
    char *msgPos = msgBegin;
    const char *errPrefix = "ERROR:  ";
    const char *ctxPrefix = "CONTEXT:  ";
    const char *detailPrefix = "DETAIL:  ";
    resetPQExpBuffer(qe->refResult->error_message);
    bool errOut = false;
    while (msgBegin < msgEnd) {
      if ((msgPos = strchr(msgBegin, '\n')) == NULL) msgPos = msgEnd;
      if (strncmp(errPrefix, msgBegin, strlen(errPrefix)) == 0) {
        errOut = true;
        appendBinaryPQExpBuffer(qe->refResult->error_message,
                                msgBegin + strlen(errPrefix),
                                msgPos - msgBegin - strlen(errPrefix));
        appendPQExpBuffer(qe->refResult->error_message,
                          "  (seg%d "
                          "slice%d %s:%d pid=%d)",
                          getTaskSegId(qe->refTask),
                          getTaskSliceId(qe->refTask),
                          qe->desc->segment->hostname, qe->desc->segment->port,
                          qe->desc->backendPid);
        if (msgPos != msgEnd)
          appendPQExpBufferChar(qe->refResult->error_message, *msgPos);
        else
          break;
      } else if (strncmp(detailPrefix, msgBegin, strlen(detailPrefix)) == 0) {
        appendBinaryPQExpBuffer(qe->refResult->error_message,
                                msgBegin + strlen(detailPrefix),
                                msgPos - msgBegin - strlen(detailPrefix));
        if (msgPos != msgEnd)
          appendPQExpBufferChar(qe->refResult->error_message, *msgPos);
        else
          break;
      } else if (strncmp(ctxPrefix, msgBegin, strlen(ctxPrefix)) == 0) {
        appendBinaryPQExpBuffer(qe->refResult->error_message,
                                msgBegin + strlen(ctxPrefix),
                                msgEnd - msgBegin - strlen(ctxPrefix));
        break;
      } else if (!errOut) {
        appendBinaryPQExpBuffer(qe->refResult->error_message, msgBegin,
                                msgEnd - msgBegin);
        appendPQExpBuffer(qe->refResult->error_message,
                          "  (seg%d "
                          "slice%d %s:%d pid=%d)",
                          getTaskSegId(qe->refTask),
                          getTaskSliceId(qe->refTask),
                          qe->desc->segment->hostname, qe->desc->segment->port,
                          qe->desc->backendPid);
        break;
      }
      msgBegin = msgPos + 1;
    }
    free(msgBeginBackup);
  }

  executormgr_setErrCode(qe);
}

bool executormgr_idDispatchable(struct MyQueryExecutor *qe) {
  PGconn *conn = qe->desc->conn;
  if (!executormgr_validate_conn(conn) || PQstatus(conn) == CONNECTION_BAD) {
    write_log("%s: bad connection.", __func__);
    return false;
  }

  return true;
}

void executormgr_getStats(struct MyQueryExecutor *qe, instr_time *connectBegin,
                          instr_time *connectEnd, instr_time *dispatchBegin,
                          instr_time *dispatchEnd, int *dataSize) {
  INSTR_TIME_ASSIGN(*connectBegin, qe->connectBegin);
  INSTR_TIME_ASSIGN(*connectEnd, qe->connectEnd);
  INSTR_TIME_ASSIGN(*dispatchBegin, qe->dispatchBegin);
  INSTR_TIME_ASSIGN(*dispatchEnd, qe->dispatchEnd);
  *dataSize = qe->dataSize;
}

void executormgr_sendback(struct MyQueryExecutor *qe) {
  PQExpBufferData buf;
  initPQExpBuffer(&buf);
  cdbdisp_serializeDispatchResult(qe->refResult, qe->execIndex, &buf);
  pq_putmessage('V', buf.data, buf.len);
  pq_flush();
  termPQExpBuffer(&buf);
}

void debugProxyDispTaskDetails(struct List *tasks) {
  int threadNum = list_length(tasks);
  int threadId = 1;
  ListCell *lc1 = NULL;
  ListCell *lc2 = NULL;
  StringInfoData str;
  initStringInfo(&str);
  foreach (lc1, tasks) {
    List *qes = (List *)lfirst(lc1);
    resetStringInfo(&str);
    appendStringInfo(&str,
                     "ProxyDisp-Thr"
                     "%d"
                     "/"
                     "%d"
                     "-"
                     "%d"
                     ":",
                     threadId, threadNum, list_length(qes));
    foreach (lc2, qes) {
      MyQueryExecutor *qe = (MyQueryExecutor *)lfirst(lc2);
      appendStringInfo(&str,
                       " "
                       "%d",
                       qe->execIndex);
    }
    appendStringInfoString(&str, ";");
    elog(LOG, "%s", str.data);
    ++threadId;
  }
}

void debugMainDispTaskDetails(struct List *tasks) {
  int threadNum = list_length(tasks);
  int threadId = 1;
  ListCell *lc1 = NULL;
  ListCell *lc2 = NULL;
  StringInfoData str;
  initStringInfo(&str);
  foreach (lc1, tasks) {
    List *qes = (List *)lfirst(lc1);
    resetStringInfo(&str);
    appendStringInfo(&str,
                     "MainDisp-Thr"
                     "%d"
                     "/"
                     "%d"
                     "-"
                     "%d"
                     ":",
                     threadId, threadNum, list_length(qes));
    foreach (lc2, qes) {
      MyQueryExecutor *qe = (MyQueryExecutor *)lfirst(lc2);
      appendStringInfo(&str,
                       " "
                       "%s",
                       taskSegToString(qe->refTask));
    }
    appendStringInfoString(&str, ";");
    elog(LOG, "%s", str.data);
    ++threadId;
  }
}
