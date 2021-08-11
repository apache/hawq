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
///////////////////////////////////////////////////////////////////////////////

#ifndef SRC_INCLUDE_CDB_SCHEDULER_H_
#define SRC_INCLUDE_CDB_SCHEDULER_H_

#include "cdb/cdbdef.h"
#include "executor/execdesc.h"
#include "optimizer/newPlanner.h"

#include "dbcommon/utils/instrument.h"

/*
 * SchedulerState
 *  Change and check state should in a wise way.
 */
typedef enum SchedulerState {
  SS_INIT,  /* Not running */
  SS_RUN,   /* Running */
  SS_OK,    /* Running without error */
  SS_ERROR, /* Running encounter error */
  SS_DONE,  /* Cleanup and Error consumed */
} SchedulerState;

typedef struct SchedulerSliceStats {
  MyInstrumentation **instr;
  int iStatInst;
  int nStatInst;
} SchedulerSliceStats;

typedef struct ScheduleWorker {
  struct Segment *segment;
  int receiverPort;
  ProcessIdentity id;
} ScheduleWorker;

typedef struct ScheduleSlice {
  ScheduleWorker *workers;
  int workers_num;
} ScheduleSlice;

typedef struct ScheduleMetaData {
  /* Share Information */
  int all_slices_num; /* EXPLAIN needs all slices num to store ANALYZE stats */
  int used_slices_num;
  ScheduleSlice *slices;
} ScheduleMetaData;

typedef struct SchedulerData {
  SchedulerState state;

  /* Original Information */
  struct QueryDesc *queryDesc;
  int localSliceId;

  /* Resource that we can use. */
  QueryResource *resource;
  CommonPlan *newPlan;

  /* Readonly for worker manager */
  ScheduleMetaData job;

  /* Statistics */
  int segmentNum; /* the number of segments for this query */
  char **hosts;
  int *stageNo;
  int stageNum; /* not include root slice and initplan */
  int **ports;
  int totalStageNum; /* include root slice and initplan */
  uint64_t insertRows;
  /* Statistics */
  MyInstrumentation **myInstrument;
  SchedulerSliceStats *slices;
} SchedulerData;

extern bool scheduler_plan_support_check(QueryDesc *queryDesc);

extern void scheduler_prepare_for_new_query(QueryDesc *queryDesc,
                                            const char *queryId, int planId);
extern void scheduler_run(struct SchedulerData *data, CommonPlanContext *ctx);
extern void scheduler_getResult(struct SchedulerData *data);
extern void scheduler_wait(struct SchedulerData *data);
extern void scheduler_cleanup(struct SchedulerData *data);
extern void scheduler_catch_error(struct SchedulerData *data);
extern void scheduler_receive_computenode_stats(SchedulerData *data,
                                                struct PlanState *planstate);
extern void scheduler_print_stats(struct SchedulerData *data, StringInfo buf);
extern CdbVisitOpt myinstrument_walk_node(
    MyInstrumentation *instr,
    CdbVisitOpt (*walker)(MyInstrumentation *instr, void *context),
    void *context);

#endif  // SRC_INCLUDE_CDB_SCHEDULER_H_
