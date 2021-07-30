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
 * ic_new.c
 *	   new interconnect code
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq/ip.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/debugbreak.h"

#include "cdb/cdbvars.h"
#include "cdb/ml_ipc.h"

#include "executor/cwrapper/cached-result.h"
#include "executor/cwrapper/executor-c.h"
#include "magma/cwrapper/magma-client-c.h"
#include "optimizer/newPlanner.h"
#include "storage/cwrapper/magma-format-c.h"

void InitNewInterconnect() {
  if (!enableOushuDbExtensiveFeatureSupport()) return;
  char myname[128];
  char *localname = NULL;
  if (Gp_role == GP_ROLE_DISPATCH ||
      (MyProcPort->laddr.addr.ss_family != AF_INET &&
       MyProcPort->laddr.addr.ss_family != AF_INET6))
    localname = "";
  else {
    getnameinfo((const struct sockaddr *)&(MyProcPort->laddr.addr),
                MyProcPort->laddr.salen, myname, sizeof(myname), NULL, 0,
                NI_NUMERICHOST);
    localname = myname;
  }
  my_listener_port = 0;

  MyNewExecutor = ExecutorNewInstance(localname, show_new_interconnect_type(),
                                      &my_listener_port);
  ExecutorCatchedError *err = ExecutorGetLastError(MyNewExecutor);
  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    elog(ERROR, "failed to init new interconnect. %s (%d)", err->errMessage,
         err->errCode);
  }

  if (Gp_role == GP_ROLE_DISPATCH) MyCachedResult = CachedResultNewInstance();
}

void CleanUpNewInterconnect() {
  if (MyNewExecutor == NULL) return;
  ExecutorFreeInstance(&MyNewExecutor);
  if (MyNewExecutor != NULL) {
    ExecutorCatchedError *err = ExecutorGetLastError(MyNewExecutor);
    elog(ERROR, "failed to cleanup new interconnect. %s (%d)", err->errMessage,
         err->errCode);
  }

  if (Gp_role == GP_ROLE_DISPATCH) CachedResultFreeInstance(&MyCachedResult);
}

void ResetRpcClientInstance() {
  if (!enableOushuDbExtensiveFeatureSupport()) {
    hawq_init_with_magma = false;
  }
  if (hawq_init_with_magma == false) return;
  MagmaClientC_CleanupClients();
  MagmaFormatC_CleanupClients();
}
