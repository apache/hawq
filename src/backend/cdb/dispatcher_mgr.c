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

/* for poll */
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#include <math.h>

#include "cdb/dispatcher_mgr.h"

#include "cdb/cdbconn.h"
#include "cdb/dispatcher.h"
#include "cdb/dispatcher_new.h"
#include "cdb/executormgr_new.h"
#include "cdb/workermgr.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "utils/faultinjector.h"

#include "magma/cwrapper/magma-client-c.h"

#define DISPMGR_POLL_TIME 2000

typedef struct MyQueryExecutorGroup {
  List *qes;
  struct pollfd *fds;
} MyQueryExecutorGroup;

List *groupTaskRoundRobin(List *executors, int numPerThread) {
  double execNum = list_length(executors);
  double remainingExecNum = execNum;
  List *tasks = NIL;
  int threadNum = ceil(execNum / numPerThread);

  /* Create the List of List of info. */
  for (int i = 0; i < threadNum; ++i) {
    List *task = NIL;
    int avgNum = ceil(remainingExecNum / (threadNum - i));
    for (int j = 0; j < avgNum && remainingExecNum > 0; ++j) {
      task = lappend(task, list_nth(executors, i + threadNum * j));
      --remainingExecNum;
    }

    tasks = lappend(tasks, task);
  }

  return tasks;
}

List *makeQueryExecutorGroup(List *task, bool pollFd) {
  List *qeGroup = NIL;
  ListCell *lc = NULL;
  foreach (lc, task) {
    MyQueryExecutorGroup *g = palloc(sizeof(MyQueryExecutorGroup));
    g->qes = (List *)lfirst(lc);
    if (pollFd)
      g->fds = palloc0(sizeof(struct pollfd) * list_length(g->qes));
    else
      g->fds = NULL;
    qeGroup = lappend(qeGroup, g);
  }
  return qeGroup;
}

void mainDispatchFuncConnect(struct MyQueryExecutorGroup *qeGrp,
                             struct WorkerMgrState *state) {
  struct MyQueryExecutor *myQe = NULL;

  ListCell *lc;
  foreach (lc, qeGrp->qes) {
    myQe = lfirst(lc);

#ifdef FAULT_INJECTOR
    // expect FaultInjectorType: FaultInjectorTypeDispatchError
    FaultInjectorType_e ret = FaultInjector_InjectFaultIfSet(
                                  MainDispatchConnect,
                                  DDLNotSpecified,
                                  "",  // databaseName
                                  ""); // tableName
    if(ret == FaultInjectorTypeDispatchError) goto error;
#endif

    if (workermgr_should_query_stop(state)) goto error;

    if (!executormgr_main_doconnect(myQe)) goto error;
  }
  return;

error:
  workermgr_set_state_cancel(state);
  executormgr_setErrCode(myQe);
}

void mainDispatchFuncRun(struct MyQueryExecutorGroup *qeGrp,
                         struct WorkerMgrState *state) {
  struct MyQueryExecutor *myQe = NULL;
  bool catchProxyErr = true;

#ifdef FAULT_INJECTOR
    // expect FaultInjectorType: FaultInjectorTypeDispatchError
    FaultInjectorType_e ret = FaultInjector_InjectFaultIfSet(
                                  MainDispatchSendPlan,
                                  DDLNotSpecified,
                                  "",  // databaseName
                                  ""); // tableName
    if(ret == FaultInjectorTypeDispatchError) goto error;
#endif

  ListCell *lc;
  foreach (lc, qeGrp->qes) {
    myQe = lfirst(lc);

    if (workermgr_should_query_stop(state)) {
      write_log("%s: query is canceled prior to dispatched.", __func__);
      goto error;
    }

    if (!executormgr_main_run(myQe)) {
      write_log("%s: query can't be dispatched.", __func__);
      goto error;
    }
  }

  while (true) {
    if (workermgr_should_query_stop(state)) {
      write_log("%s: query is canceled while polling executors.", __func__);
      goto error;
    }

    int nfds = 0;
    ListCell *cell = NULL;
    foreach (cell, qeGrp->qes) {
      myQe = lfirst(cell);
      if (executormgr_isStopped(myQe)) continue;
      qeGrp->fds[nfds].fd = executormgr_getFd(myQe);
      qeGrp->fds[nfds].events = POLLIN;
      ++nfds;
    }
    if (nfds == 0) return;

    int n = poll(qeGrp->fds, nfds, DISPMGR_POLL_TIME);

    if (n < 0) {
      if (SOCK_ERRNO == EINTR) continue;
      /*
       * System call poll error is only caused by program bug or system
       * resources unavailable. In this case, fail the query is okay.
       */
      write_log("%s: poll failed with errno: %d. ", __func__, SOCK_ERRNO);
      goto error;
    }

    if (n == 0) {
      /*
       * Network problem and long time query may get here.
       * Check network problem is expensive.
       * Long time query only concerns cancel query, check it later in the
       * loop start.
       */
      continue;
    }

    int idx = 0;
    foreach (cell, qeGrp->qes) {
      myQe = lfirst(cell);
      if (executormgr_isStopped(myQe)) continue;
      if (!(qeGrp->fds[idx++].revents & POLLIN)) continue;
      if (!executormgr_main_consumeData(myQe)) {
        catchProxyErr = false;
        write_log("%s: fail to consume data. ", __func__);
        goto error;
      }
    }
  }

  return;

error:
  workermgr_set_state_cancel(state);

  if (catchProxyErr) executormgr_catchError(myQe, true);

  foreach (lc, qeGrp->qes) {
    struct MyQueryExecutor *qe = lfirst(lc);
    if (!executormgr_isStopped(qe)) executormgr_main_cancel(qe);
  }

  // if we can detect dispatcher error in new interconnect,
  // the code below should be removed then.
  MagmaClientC_CancelMagmaClient();
  MagmaFormatC_CancelMagmaClient();
  if (MyNewExecutor != NULL) MyExecutorSetCancelQuery(MyNewExecutor);
}

void proxyDispatchFuncConnect(struct MyQueryExecutorGroup *qeGrp,
                              struct WorkerMgrState *state) {
  struct MyQueryExecutor *myQe = NULL;

  ListCell *lc;
  foreach (lc, qeGrp->qes) {
    myQe = lfirst(lc);

    if (workermgr_should_query_stop(state)) goto error;

    if (!executormgr_proxy_doconnect(myQe)) {
      write_log("%s: failed to startup new qe.", __func__);
      goto error;
    }
  }
  return;

error:
  workermgr_set_state_cancel(state);
  executormgr_setErrCode(myQe);
}

void proxyDispatchFuncRun(struct MyQueryExecutorGroup *qeGrp,
                          struct WorkerMgrState *state) {
  struct MyQueryExecutor *myQe = NULL;
  int retries = 0;

  ListCell *lc;
  char *msgCopy = NULL;
  int32 len;
  foreach (lc, qeGrp->qes) {
    myQe = lfirst(lc);

#ifdef FAULT_INJECTOR
    // expect FaultInjectorType: FaultInjectorTypeDispatchError
    FaultInjectorType_e ret = FaultInjector_InjectFaultIfSet(
                                  ProxyDispatchSendPlan,
                                  DDLNotSpecified,
                                  "",  // databaseName
                                  ""); // tableName
    if(ret == FaultInjectorTypeDispatchError) goto error;
#endif

    if (workermgr_should_query_stop(state)) {
      write_log("%s: query is canceled prior to dispatched.", __func__);
      goto error;
    }

    if (!executormgr_proxy_run(myQe, &msgCopy, &len)) {
      write_log("%s: query can't be dispatched.", __func__);
      if (msgCopy) free(msgCopy);
      goto error;
    }
  }
  if (msgCopy) free(msgCopy);

  while (true) {
    if (workermgr_should_query_stop(state)) {
      write_log("%s: query is canceled while polling executors.", __func__);
      goto error;
    }

    int nfds = 0;
    ListCell *cell = NULL;
    foreach (cell, qeGrp->qes) {
      myQe = lfirst(cell);
      if (executormgr_isStopped(myQe)) continue;
      qeGrp->fds[nfds].fd = executormgr_getFd(myQe);
      qeGrp->fds[nfds].events = POLLIN;
      ++nfds;
    }
    if (nfds == 0) return;

    int n = poll(qeGrp->fds, nfds, DISPMGR_POLL_TIME);

    if (n < 0) {
      if (SOCK_ERRNO == EINTR) continue;
      /*
       * System call poll error is only caused by program bug or system
       * resources unavailable. In this case, fail the query is okay.
       */
      write_log("%s: poll is failed with errno(%d). ", __func__, SOCK_ERRNO);
      goto error;
    }

    if (n == 0) {
      /*
       * Network problem and long time query may get here.
       * Check network problem is expensive.
       * Long time query only concerns cancel query, check it later in the
       * loop start.
       */
      ++retries;
      // every 4*DISPMGR_POLL_TIME to check qd alive
      if (((retries & 0x3) == 0) && !dispatch_validate_conn(MyProcPort->sock))
        goto error;
      else
        continue;
    }

    int idx = 0;
    foreach (cell, qeGrp->qes) {
      myQe = lfirst(cell);
      if (executormgr_isStopped(myQe)) continue;
      if (!(qeGrp->fds[idx++].revents & POLLIN)) continue;
      if (!executormgr_proxy_consumeData(myQe)) {
        write_log("%s: fail to consume data. ", __func__);
        goto error;
      }
    }
  }

  return;

error:
  workermgr_set_state_cancel(state);

  /*
   * Previously query failed, probably in this executor. We need to
   * set error code here if it has not been set although the executor
   * is probably fine. This let the main process for the query proceed
   * to cancel the query in its thread also, without waiting for a long
   * time. We expect the error code have been set previously before jumping
   * to error_cleanup. The code below could be the last defence.
   */
  executormgr_catchError(myQe, false);
  executormgr_sendback(myQe);

  foreach (lc, qeGrp->qes) {
    struct MyQueryExecutor *qe = lfirst(lc);
    executormgr_proxy_cancel(qe, !executormgr_isStopped(qe));
  }
}
