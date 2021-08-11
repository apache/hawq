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
////////////////////////////////////////////////////////////////////////////////

#ifndef EXECUTORMGR_NEW_H
#define EXECUTORMGR_NEW_H

#include "portability/instr_time.h"

struct List;
struct MyDispatchTask;
struct MyQueryExecutor;
struct Segment;
struct SegmentDatabaseDescriptor;

extern void executormgr_setupEnv(MemoryContext ctx);

extern void executormgr_cleanupEnv();

extern struct SegmentDatabaseDescriptor *executormgr_allocateExecutor(
    struct Segment *segment, bool entryDb);

extern void executormgr_unbindExecutor(struct MyQueryExecutor *qe);

extern struct SegmentDatabaseDescriptor *executormgr_prepareConnect(
    struct Segment *segment);

extern struct MyQueryExecutor *executormgr_makeQueryExecutor(
    struct Segment *segment, bool isWriter, struct MyDispatchTask *task,
    int sliceIndex);

extern void executormgr_setQueryExecutorIndex(struct MyQueryExecutor *qe,
                                              int index);

extern void executormgr_setQueryExecutorIdMsg(struct MyQueryExecutor *qe,
                                              char *idMsg, int idMsgLen);

extern void executormgr_getConnectInfo(struct MyQueryExecutor *qe,
                                       char **address, int *port, int *myPort,
                                       int *pid);

extern bool executormgr_main_doconnect(struct MyQueryExecutor *qe);

extern bool executormgr_main_run(struct MyQueryExecutor *qe);

extern bool executormgr_main_consumeData(struct MyQueryExecutor *qe);

extern bool executormgr_proxy_doconnect(struct MyQueryExecutor *qe);

extern bool executormgr_proxy_run(struct MyQueryExecutor *qe, char **msg,
                                  int32 *len);

extern bool executormgr_proxy_consumeData(struct MyQueryExecutor *qe);

extern struct SegmentDatabaseDescriptor *executormgr_getSegDesc(
    struct MyQueryExecutor *qe);
extern void executormgr_setSegDesc(struct MyQueryExecutor *qe,
                                   struct SegmentDatabaseDescriptor *desc);
extern bool executormgr_isStopped(struct MyQueryExecutor *qe);
extern int executormgr_getFd(struct MyQueryExecutor *qe);

extern bool executormgr_hasCachedExecutor();
extern void executormgr_cleanCachedExecutor();

extern bool executormgr_main_cancel(struct MyQueryExecutor *qe);
extern bool executormgr_proxy_cancel(struct MyQueryExecutor *qe,
                                     bool cancelRequest);

extern void executormgr_setErrCode(struct MyQueryExecutor *qe);

extern void executormgr_mergeDispErr(struct MyQueryExecutor *qe,
                                     int *errHostNum, char ***errHostInfo);

extern void executormgr_catchError(struct MyQueryExecutor *qe,
                                   bool formatError);

extern bool executormgr_idDispatchable(struct MyQueryExecutor *qe);

extern void executormgr_getStats(struct MyQueryExecutor *qe,
                                 instr_time *connectBegin,
                                 instr_time *connectEnd,
                                 instr_time *dispatchBegin,
                                 instr_time *dispatchEnd, int *dataSize);

extern void executormgr_sendback(struct MyQueryExecutor *qe);

// for debug
extern void debugProxyDispTaskDetails(struct List *tasks);
extern void debugMainDispTaskDetails(struct List *tasks);

#endif /* EXECUTORMGR_NEW_H */
