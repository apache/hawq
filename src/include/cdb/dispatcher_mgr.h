////////////////////////////////////////////////////////////////////////////////
// Copyright 2019, Oushu Inc.
// All rights reserved.
//
// Author:
////////////////////////////////////////////////////////////////////////////////

#ifndef DISPATCHER_MGR_H
#define DISPATCHER_MGR_H

#include "nodes/pg_list.h"

struct MyQueryExecutorGroup;
struct WorkerMgrState;

extern List *groupTaskRoundRobin(List *executors, int numPerThread);

extern List *makeQueryExecutorGroup(List *task, bool pollFd);

extern void mainDispatchFuncConnect(struct MyQueryExecutorGroup *qeGrp,
                                    struct WorkerMgrState *state);

extern void mainDispatchFuncRun(struct MyQueryExecutorGroup *qeGrp,
                                struct WorkerMgrState *state);

extern void proxyDispatchFuncConnect(struct MyQueryExecutorGroup *qeGrp,
                                     struct WorkerMgrState *state);

extern void proxyDispatchFuncRun(struct MyQueryExecutorGroup *qeGrp,
                                 struct WorkerMgrState *state);

#endif /* DISPATCHER_MGR_H */
