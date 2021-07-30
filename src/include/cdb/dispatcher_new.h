////////////////////////////////////////////////////////////////////////////////
// Copyright 2019, Oushu Inc.
// All rights reserved.
//
// Author:
////////////////////////////////////////////////////////////////////////////////

#ifndef DISPATCHER_NEW_H
#define DISPATCHER_NEW_H

#include "optimizer/newPlanner.h"
#include "postmaster/identity.h"

struct DispatchDataResult;
struct List;
struct MainDispatchData;
struct MyDispatchTask;
struct MyQueryExecutor;
struct ProxyDispatchData;
struct QueryContextInfo;
struct QueryDesc;
struct QueryResource;

// main dispatcher
extern struct MainDispatchData *mainDispatchInit(
    struct QueryResource *resource);
extern void mainDispatchPrepare(struct MainDispatchData *data,
                                struct QueryDesc *queryDesc, bool newPlanner);
extern void mainDispatchRun(struct MainDispatchData *data,
                            CommonPlanContext *ctx, bool newPlanner);
extern void mainDispatchWait(struct MainDispatchData *data, bool requestCancel);
extern void mainDispatchCleanUp(struct MainDispatchData **data);
extern struct CdbDispatchResults *mainDispatchGetResults(
    struct MainDispatchData *data);
extern bool mainDispatchHasError(struct MainDispatchData *data);
extern int mainDispatchGetSegNum(struct MainDispatchData *data);
extern void mainDispatchCatchError(struct MainDispatchData **data);
extern void mainDispatchPrintStats(StringInfo buf,
                                   struct MainDispatchData *data);

// proxy dispatcher
extern void proxyDispatchInit(int qeNum, char *msg,
                              struct ProxyDispatchData **data);
extern void proxyDispatchPrepare(struct ProxyDispatchData *data);
extern void sendSegQEDetails(struct ProxyDispatchData *data);
extern void proxyDispatchRun(struct ProxyDispatchData *data, char *connMsg);
extern void proxyDispatchWait(struct ProxyDispatchData *data);
extern void proxyDispatchCleanUp(struct ProxyDispatchData **data);

// dispatch statement
extern void mainDispatchStmtNode(struct Node *node,
                                 struct QueryContextInfo *ctx,
                                 struct QueryResource *resource,
                                 struct DispatchDataResult *result);

// utils
extern List *getTaskPerSegmentList(struct MyDispatchTask *task);
extern void setTaskPerSegmentList(struct MyDispatchTask *task, struct List *l);
extern void setTaskRefQE(struct MyDispatchTask *task,
                         struct MyQueryExecutor *qe);
extern int getTaskSliceId(struct MyDispatchTask *task);
extern int getTaskSegId(struct MyDispatchTask *task);
extern struct MyQueryExecutor *getTaskRefQE(struct MyDispatchTask *task);
extern struct DispatchCommandQueryParms *getQueryParms(
    struct MyDispatchTask *task);
extern char *getTaskConnMsg(struct MyDispatchTask *task);
extern struct CdbDispatchResults *getDispatchResults(
    struct MyDispatchTask *task);

// for debug
extern const char *taskIdToString(struct MyDispatchTask *task);
extern const char *taskSegToString(struct MyDispatchTask *task);

#endif /* DISPATCHER_NEW_H */
