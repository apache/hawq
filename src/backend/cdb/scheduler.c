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

#include "cdb/cdbgang.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/scheduler.h"
#include "commands/variable.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"

#include "scheduler/cwrapper/scheduler-c.h"

#define CDB_MOTION_LOST_CONTACT_STRING \
  "Interconnect error master lost contact with segment."

typedef struct {
  int sliceIndex;
  int children;
  Slice *slice;
} sliceVec;

static int compare_slice_order(const void *aa, const void *bb) {
  sliceVec *a = (sliceVec *) aa;
  sliceVec *b = (sliceVec *) bb;

  if (a->slice == NULL)
    return 1;
  if (b->slice == NULL)
    return -1;

  /* sort the writer gang slice first, because he sets the shared snapshot */
  if (a->slice->is_writer && !b->slice->is_writer)
    return -1;
  else if (b->slice->is_writer && !a->slice->is_writer)
    return 1;

  if (a->children == b->children)
    return 0;
  else if (a->children > b->children)
    return 1;
  else
    return -1;
}
static void mark_bit(char *bits, int nth) {
  int nthbyte = nth >> 3;
  char nthbit = 1 << (nth & 7);
  bits[nthbyte] |= nthbit;
}
static void or_bits(char *dest, char *src, int n) {
  int i;

  for (i = 0; i < n; i++)
    dest[i] |= src[i];
}

static int count_bits(char *bits, int nbyte) {
  int i;
  int nbit = 0;
  int bitcount[] = { 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4 };

  for (i = 0; i < nbyte; i++) {
    nbit += bitcount[bits[i] & 0x0F];
    nbit += bitcount[(bits[i] >> 4) & 0x0F];
  }

  return nbit;
}

static int markbit_dep_children(SliceTable *sliceTable, int sliceIdx,
                                sliceVec *sliceVec, int bitmasklen, char *bits) {
  ListCell *sublist;
  Slice *slice = (Slice *) list_nth(sliceTable->slices, sliceIdx);

  foreach (sublist, slice->children)
  {
    int childIndex = lfirst_int(sublist);
    char *newbits = palloc0(bitmasklen);

    markbit_dep_children(sliceTable, childIndex, sliceVec, bitmasklen, newbits);
    or_bits(bits, newbits, bitmasklen);
    mark_bit(bits, childIndex);
    pfree(newbits);
  }

  sliceVec[sliceIdx].sliceIndex = sliceIdx;
  sliceVec[sliceIdx].children = count_bits(bits, bitmasklen);
  sliceVec[sliceIdx].slice = slice;

  return sliceVec[sliceIdx].children;
}
static int count_dependent_children(SliceTable *sliceTable, int sliceIndex,
                                    sliceVec *sliceVector, int len) {
  int ret = 0;
  int bitmasklen = (len + 7) >> 3;
  char *bitmask = palloc0(bitmasklen);

  ret = markbit_dep_children(sliceTable, sliceIndex, sliceVector, bitmasklen,
                             bitmask);
  pfree(bitmask);

  return ret;
}

static int fillSliceVector(SliceTable *sliceTbl, int rootIdx,
                           sliceVec *sliceVector, int sliceLim) {
  int top_count;

  /* count doesn't include top slice add 1 */
  top_count = 1
      + count_dependent_children(sliceTbl, rootIdx, sliceVector, sliceLim);

  qsort(sliceVector, sliceLim, sizeof(sliceVec), compare_slice_order);

  return top_count;
}

static void scheduler_serialize_common_plan(SchedulerData *data,
                                            CommonPlanContext *ctx);
static void scheduler_init_instrumentation(MyInstrumentation **myInstrument,
                                           MyInstrumentation *instr,
                                           struct PlanState *planstate,
                                           int stageNo, int segmentNum);
static CdbVisitOpt convertMyInstruemntToSliceStat(MyInstrumentation *instr,
                                                  void *context);

bool scheduler_plan_support_check(QueryDesc *queryDesc) {
  SliceTable *sliceTbl = queryDesc->estate->es_sliceTable;

  if(sliceTbl->doInstrument){
    return false;
  }

  if(sliceTbl->nInitPlans){
    return false;
  }

  ListCell *cell = NULL;
  foreach(cell, sliceTbl->slices)
  {
    Slice *slice = (Slice *) lfirst(cell);

    if (slice->directDispatch.isDirectDispatch
        || slice->gangSize == 1 && slice->gangType != GANGTYPE_UNALLOCATED) {
      return false;
    }
  }

  return true;
}

void scheduler_prepare_for_new_query(QueryDesc *queryDesc, const char *queryId,
                                     int planId) {
  // Initialize scheduler when first query
  if (MyScheduler == NULL) {
    // MyScheduler = SchedulerNew(magma_port_segment + 4);
    SchedulerSetupRpcServer(MyScheduler, GetMasterSegment()->hostip);
  }

  queryDesc->estate->scheduler_data = palloc0(sizeof(SchedulerData));
  SchedulerData *scheduler_data = queryDesc->estate->scheduler_data;
  scheduler_data->state = SS_INIT;
  scheduler_data->segmentNum = list_length(queryDesc->resource->segments);
  scheduler_data->resource = queryDesc->resource;
  scheduler_data->queryDesc = queryDesc;

  SliceTable *sliceTbl = queryDesc->estate->es_sliceTable;
  scheduler_data->localSliceId = sliceTbl->localSlice;

  sliceVec *sliceVector = NULL;
  int i;
  int slice_num;

  sliceVector = palloc0(list_length(sliceTbl->slices) * sizeof(*sliceVector));
  slice_num = fillSliceVector(sliceTbl, RootSliceIndex(queryDesc->estate),
                              sliceVector, list_length(sliceTbl->slices));

  scheduler_data->job.used_slices_num = slice_num;
  scheduler_data->job.all_slices_num = list_length(sliceTbl->slices);
  scheduler_data->job.slices = palloc0(
      scheduler_data->job.used_slices_num * sizeof(ScheduleSlice));

  for (i = 0; i < scheduler_data->job.used_slices_num; i++) {
    scheduler_data->job.slices[i].workers_num = list_length(
        scheduler_data->resource->segments);
    scheduler_data->job.slices[i].workers = palloc0(
        scheduler_data->job.slices[i].workers_num * sizeof(ScheduleWorker));
  }

  for (i = 0; i < slice_num; i++) {
    Slice *slice = sliceVector[i].slice;
    bool is_writer = false;

    is_writer = (i == 0 && !queryDesc->extended_query);
    // Not all of slices need to dispatch, such as root slice!
    // root slice is at end of sliceVector
    if (slice->gangType == GANGTYPE_UNALLOCATED) {
      scheduler_data->job.used_slices_num--;
      continue;
    }

    int j;

    // Assign the id from 0.
    for (j = 0; j < scheduler_data->job.slices[i].workers_num; j++) {
      ScheduleWorker *worker = &scheduler_data->job.slices[i].workers[j];

      worker->id.id_in_slice = j;
      worker->id.slice_id = slice->sliceIndex;
      worker->id.gang_member_num = list_length(
          scheduler_data->resource->segments);
      worker->id.command_count = gp_command_count;
      worker->id.is_writer = is_writer;
      worker->segment = list_nth(scheduler_data->resource->segments,
                                 worker->id.id_in_slice);
      worker->id.init = true;
    }
  }

  pfree(sliceVector);

  // get all stageNo from plan tree
  struct Plan *planTree = queryDesc->plannedstmt->planTree;
  scheduler_data->stageNo = palloc0(
      sizeof(int) * scheduler_data->job.used_slices_num);
  scheduler_data->stageNum = scheduler_data->job.used_slices_num;
  scheduler_data->totalStageNum = scheduler_data->job.all_slices_num;

  for (int i = 0; i < scheduler_data->job.used_slices_num; i++) {
    scheduler_data->stageNo[i] = scheduler_data->job.slices[i].workers[0].id
        .slice_id;
  }

//  bool *isInitPlan =
//      palloc0(sizeof(bool) * list_length(queryDesc->plannedstmt->subplans));
//  if (planId == 0) {
//    get_all_stageno_from_plantree(planTree, scheduler_data->stageNo,
//                                  &scheduler_data->stageNum, isInitPlan);
//    ListCell *lc;
//    int i = 0;
//    foreach (lc, queryDesc->plannedstmt->subplans) {
//      Plan *subplan = (Plan *)lfirst(lc);
//      if (!isInitPlan[i])
//        get_all_stageno_from_plantree(subplan, scheduler_data->stageNo,
//                                      &scheduler_data->stageNum, isInitPlan);
//      i++;
//    }
//  } else {
//    Plan *subplan =
//        (Plan *)list_nth(queryDesc->plannedstmt->subplans, planId - 1);
//    get_all_stageno_from_plantree(subplan, scheduler_data->stageNo,
//                                  &scheduler_data->stageNum, isInitPlan);
//  }
//  pfree(isInitPlan);

  // get all hosts from resource
  ListCell *lc;
  i = 0;
  scheduler_data->hosts = palloc0(sizeof(char *) * scheduler_data->segmentNum);
  int *lens = palloc0(sizeof(int) * scheduler_data->segmentNum);
  foreach (lc, scheduler_data->resource->segments)
  {
    Segment *seg = (Segment *) lfirst(lc);
    scheduler_data->hosts[i] = seg->hostname;
    lens[i] = strlen(seg->hostname);
    i++;
  }

  SchedulerPrepareForNewQuery(MyScheduler, queryId, scheduler_data->hosts, lens,
                              scheduler_data->segmentNum,
                              scheduler_data->stageNo,
                              scheduler_data->stageNum);
  pfree(lens);
  SchedulerCatchedError *err = SchedulerGetLastError(MyScheduler);
  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    scheduler_data->state = SS_ERROR;
    int errCode = err->errCode;
    ereport(
        ERROR,
        (errcode(errCode), errmsg("failed to prepare scheduler for new query. %s (%d)", err->errMessage, errCode)));
  }

  scheduler_data->totalStageNum = queryDesc->estate->es_sliceTable->nInitPlans
      + queryDesc->estate->es_sliceTable->nMotions + 1;
  scheduler_data->ports = palloc0(
      sizeof(int *) * scheduler_data->totalStageNum);
  for (i = 0;
      i
          < queryDesc->estate->es_sliceTable->nInitPlans
              + queryDesc->estate->es_sliceTable->nMotions + 1; i++)
    scheduler_data->ports[i] = palloc0(
        sizeof(int) * scheduler_data->segmentNum);
  for (i = 0; i < scheduler_data->segmentNum; i++)
    for (int j = 0; j < scheduler_data->stageNum; j++)
      scheduler_data->ports[scheduler_data->stageNo[j]][i] =
          SchedulerGetListenPort(MyScheduler, i, scheduler_data->stageNo[j]);
  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    scheduler_data->state = SS_ERROR;
    int errCode = err->errCode;
    ereport(
        ERROR,
        (errcode(errCode), errmsg("failed to get scheduler listen port. %s (%d)", err->errMessage, errCode)));
  }
}

void scheduler_run(SchedulerData *data, CommonPlanContext *ctx) {
  if (!data)
    return;

  scheduler_serialize_common_plan(data, ctx);
  ScheduleRun(MyScheduler, ctx->univplan);
  univPlanFreeInstance(&ctx->univplan);
  data->state = SS_RUN;
  SchedulerCatchedError *err = SchedulerGetLastError(MyScheduler);
  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    SchedulerCancelQuery(MyScheduler);

    if (MyNewExecutor != NULL)
      MyExecutorSetCancelQuery(MyNewExecutor);

    data->state = SS_ERROR;
    int errCode = err->errCode;
    ereport(
        ERROR,
        (errcode(errCode), errmsg("failed to run scheduler. %s (%d)", err->errMessage, errCode)));
  }
}

void scheduler_getResult(struct SchedulerData *data) {
  getTasksResult(MyScheduler, &data->insertRows);
}

void scheduler_wait(SchedulerData *data) {
  if (!data || data->state == SS_OK || data->state == SS_ERROR)
    return;

  SchedulerWaitAllTasksComplete(MyScheduler);
  SchedulerCatchedError *err = SchedulerGetLastError(MyScheduler);
  data->state = SS_OK;
  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    data->state = SS_ERROR;
    int errCode = err->errCode;
    ereport(ERROR, (errcode(errCode), errmsg("%s", err->errMessage)));
  }
}

static void scheduler_serialize_common_plan(SchedulerData *data,
                                            CommonPlanContext *ctx) {
  PlannedStmt *stmt = data->queryDesc ? data->queryDesc->plannedstmt : NULL;
  if (stmt) {
    univPlanSetDoInstrument(
        ctx->univplan, data->queryDesc->estate->es_sliceTable->doInstrument);
    univPlanSetNCrossLevelParams(ctx->univplan, stmt->nCrossLevelParams);

    // add interconnect info
    for (int i = 0;
        i
            < data->queryDesc->estate->es_sliceTable->nInitPlans
                + data->queryDesc->estate->es_sliceTable->nMotions + 1; ++i) {
      Slice *slice = list_nth(data->queryDesc->estate->es_sliceTable->slices,
                              i);
      ListCell *lc;
      uint32_t listenerNum;
      char **addr;
      int32 *port;
      if (!slice->primaryProcesses) {
        if (data->ports[i][0] == 0)
          listenerNum = 0;
        else if (slice->directDispatch.isDirectDispatch)
          listenerNum = 1;
        else if (slice->gangSize == 1)
          listenerNum = 1;
        else
          listenerNum = data->segmentNum;
        addr = palloc0(listenerNum * sizeof(char *));
        port = palloc0(listenerNum * sizeof(int32));
        for (int j = 0; j < listenerNum; j++) {
          addr[j] = data->hosts[j];
          port[j] = data->ports[i][j];
        }
      } else {
        listenerNum = slice->primaryProcesses->length;
        addr = palloc0(listenerNum * sizeof(char *));
        port = palloc0(listenerNum * sizeof(int32));
        int index = 0;
        foreach (lc, slice->primaryProcesses)
        {
          CdbProcess *proc = (CdbProcess *) lfirst(lc);
          if (proc->listenerAddr)
            addr[index] = pstrdup(proc->listenerAddr);
          else
            addr[index] = data->resource->master->hostip;
          port[index] = data->ports[i][index];
          if (port[index] == 0)
            port[index] = proc->myListenerPort;
          ++index;
        }
      }
      // scheduler no use, just set listener to -1
      univPlanReceiverAddListeners(ctx->univplan, listenerNum, -1, addr, port);
      pfree(addr);
      pfree(port);
    }

    univPlanFixVarType(ctx->univplan);

    univPlanAddGuc(ctx->univplan, "work_file_dir", rm_seg_tmp_dirs);
    univPlanAddGuc(ctx->univplan, "enable_partitioned_hashagg",
                   new_executor_enable_partitioned_hashagg_mode);
    univPlanAddGuc(ctx->univplan, "enable_partitioned_hashjoin",
                   new_executor_enable_partitioned_hashjoin_mode);
    univPlanAddGuc(ctx->univplan, "enable_external_sort",
                   new_executor_enable_external_sort_mode);
    univPlanAddGuc(ctx->univplan, "new_scheduler", new_scheduler_mode);
    char *timezone_str = show_timezone();
    univPlanAddGuc(ctx->univplan, "timezone_string", timezone_str);

    char partitioned_hash_recursive_depth_limit[4];
    sprintf(partitioned_hash_recursive_depth_limit, "%d",
            new_executor_partitioned_hash_recursive_depth_limit);
    univPlanAddGuc(ctx->univplan, "partitioned_hash_recursive_depth_limit",
                   partitioned_hash_recursive_depth_limit);

    univPlanStagize(ctx->univplan);

    // elog(DEBUG1, "common plan: %s",
    // univPlanGetJsonFormatedPlan(ctx->univplan));

    // serialize common plan for QD executes
    int len;
    const char *val = univPlanSerialize(ctx->univplan, &len, true);
    data->newPlan = (CommonPlan *) palloc0(sizeof(CommonPlan));
    data->newPlan->len = len;
    data->newPlan->str = (char *) palloc0(len);
    memcpy(data->newPlan->str, val, len);

    data->queryDesc->newPlan = data->newPlan;

    int numSlicesToDispatch = data->queryDesc->plannedstmt->planTree
        ->nMotionNodes;
    uint64 planSizeInKb = ((uint64) len * (uint64) numSlicesToDispatch)
        / (uint64) 1024;
    elog(LOG,
    "Dispatch new plan instead of old plan, size to "
    "scheduler: " UINT64_FORMAT "KB",
    planSizeInKb);
  }
}

void scheduler_cleanup(SchedulerData *data) {
  if (!data)
    return;

  if (data->stageNo) {
    pfree(data->stageNo);
    data->stageNo = NULL;
  }
  if (data->hosts) {
    pfree(data->hosts);
    data->hosts = NULL;
  }
  if (data->ports) {
    for (int i = 0;
        i
            < data->queryDesc->estate->es_sliceTable->nInitPlans
                + data->queryDesc->estate->es_sliceTable->nMotions + 1; i++) {
      pfree(data->ports[i]);
      data->ports[i] = NULL;
    }
    pfree(data->ports);
    data->ports = NULL;
  }
  if (data->slices) {
    for (int i = 0; i < data->totalStageNum; i++) {
      for (int j = data->slices[i].nStatInst / data->segmentNum; j > 0; j--)
        pfree(data->slices[i].instr[j - 1]);
    }
    pfree(data->slices);
    data->slices = NULL;
  }
  if (data->myInstrument) {
    pfree(data->myInstrument);
    data->myInstrument = NULL;
  }
  pfree(data);
}

/*
 * scheduler_catch_error
 *  Cancel the workermgr work and report error based on bug on schedulerer or
 *  executors.
 */
void scheduler_catch_error(SchedulerData *data) {
  if (!data)
    return;

  int qderrcode = elog_geterrcode();
  bool useQeError = false;
  SchedulerCatchedError *qeError = SchedulerGetLastError(MyScheduler);

  /* Init state means no thread and data is not correct! */
  if (!data->state == SS_INIT) {
    /* Have to figure out exception source */
    SchedulerCollectError(MyScheduler);
    qeError = SchedulerGetLastError(MyScheduler);

    SchedulerCancelQuery(MyScheduler);
  }

  /*
   * When a QE stops executing a command due to an error, as a
   * consequence there can be a cascade of interconnect errors
   * (usually "sender closed connection prematurely") thrown in
   * downstream processes (QEs and QD).  So if we are handling
   * an interconnect error, and a QE hit a more interesting error,
   * we'll let the QE's error report take precedence.
   */
  if (qderrcode == ERRCODE_GP_INTERCONNECTION_ERROR) {
    bool qd_lost_flag = false;
    char *qderrtext = elog_message();

    if (qderrtext && strcmp(qderrtext, CDB_MOTION_LOST_CONTACT_STRING) == 0)
      qd_lost_flag = true;

    if (qeError->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
      if (qd_lost_flag && qeError->errCode == ERRCODE_GP_INTERCONNECTION_ERROR)
        useQeError = true;
      else if (qeError->errCode != ERRCODE_GP_INTERCONNECTION_ERROR)
        useQeError = true;
    }
  }

  if (useQeError) {
    /* Too bad, our gang got an error. */
    PG_TRY()
          ;
          {
            ereport(
                ERROR,
                (errcode(qeError->errCode), errmsg("%s", qeError->errMessage)));
          }PG_CATCH();
        {
        }PG_END_TRY();
  }
}

void scheduler_print_stats(struct SchedulerData *data, StringInfo buf) {
  if (!data)
    return;
  SchedulerStats *stats = SchedulerGetStats(MyScheduler);
  appendStringInfo(buf, "New Scheduler statistics:\n");
  if (stats->rpcServerSetupTime)
    appendStringInfo(buf, "  setup rpc server: %dms;",
                     stats->rpcServerSetupTime);
  appendStringInfo(buf, "  setup listener port: %dms;",
                   stats->setupListenerPortTime);
  appendStringInfo(buf, "  dispatch task: %dms;", stats->dispatchTaskTime);
  appendStringInfo(buf, "  wait task complete: %dms;\n",
                   stats->waitTaskCompleteTime);
}

void scheduler_receive_computenode_stats(SchedulerData *data,
                                         struct PlanState *planstate) {
  SchedulerRecveiveAllTaskStats(MyScheduler);
  SchedulerCatchedError *err = SchedulerGetLastError(MyScheduler);
  data->state = SS_OK;
  if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
    data->state = SS_ERROR;
    int errCode = err->errCode;
    ereport(ERROR, (errcode(errCode), errmsg("%s", err->errMessage)));
  }
  data->myInstrument = palloc0(
      sizeof(MyInstrumentation *) * data->totalStageNum);
  int i;
  for (i = 0; i < data->totalStageNum; i++) {
    data->myInstrument[i] = palloc0(
        sizeof(MyInstrumentation) * data->segmentNum);
  }
  scheduler_init_instrumentation(data->myInstrument, data->myInstrument[0],
                                 planstate, 0, data->segmentNum);
  data->slices = palloc0(sizeof(SchedulerSliceStats) * data->totalStageNum);
  for (i = 0; i < data->stageNum * data->segmentNum; i++) {
    int32_t stageNo;
    int32_t segmentNo;
    SchedulerGetOneTaskInfo(MyScheduler, i, &stageNo, &segmentNo);
    SchedulerGetOneTaskStats(MyScheduler, i,
                             &data->myInstrument[stageNo][segmentNo],
                             &data->slices[stageNo].nStatInst);
  }
  for (i = 0; i < data->totalStageNum; i++) {
    data->slices[i].instr = palloc0(
        sizeof(MyInstrumentation *) * data->slices[i].nStatInst);
    for (int j = 0; j < data->segmentNum; j++)
      if (data->slices[i].nStatInst > 0)
        myinstrument_walk_node(&data->myInstrument[i][j],
                               convertMyInstruemntToSliceStat,
                               &data->slices[i]);
  }
}

static void scheduler_init_instrumentation(MyInstrumentation **myInstrument,
                                           MyInstrumentation *instr,
                                           struct PlanState *planstate,
                                           int stageNo, int segmentNum) {
  int segNum = 1;
  switch (nodeTag(planstate->plan)) {
    case T_Motion: {
      Motion *m = (Motion *) planstate->plan;
      stageNo = m->motionID;
      instr = myInstrument[stageNo];
      segNum = segmentNum;
      break;
    }
  }
  if (planstate->lefttree) {
    for (int i = 0; i < segNum; i++) {
      instr[i].leftTree = palloc0(sizeof(MyInstrumentation));
      scheduler_init_instrumentation(myInstrument, instr[i].leftTree,
                                     planstate->lefttree, stageNo, segmentNum);
    }
  }
  if (planstate->righttree) {
    for (int i = 0; i < segNum; i++) {
      instr[i].rightTree = palloc0(sizeof(MyInstrumentation));
      scheduler_init_instrumentation(myInstrument, instr[i].rightTree,
                                     planstate->righttree, stageNo, segmentNum);
    }
  }
  if (nodeTag(planstate->plan) == T_Append) {
    AppendState *appendstate = (AppendState *) planstate;
    ListCell *lc;
    for (int i = 0; i < segNum; i++) {
      MyInstrumentation *subInstr = &instr[i];
      int appendNum = list_length(((Append *) planstate->plan)->appendplans);
      for (int j = 0; j < appendNum; j++) {
        PlanState *appendplanstate = appendstate->appendplans[j];
        subInstr->subTree = palloc0(sizeof(MyInstrumentation));
        scheduler_init_instrumentation(myInstrument, subInstr->subTree,
                                       appendplanstate, stageNo, segmentNum);
        subInstr = subInstr->subTree;
      }
    }
  }
}

static CdbVisitOpt convertMyInstruemntToSliceStat(MyInstrumentation *instr,
                                                  void *context) {
  SchedulerSliceStats *sliceState = (SchedulerSliceStats *) context;
  sliceState->instr[sliceState->iStatInst] = instr;
  sliceState->iStatInst++;
  return CdbVisit_Walk;
}

CdbVisitOpt myinstrument_walk_node(
    MyInstrumentation *instr,
    CdbVisitOpt (*walker)(MyInstrumentation *instr, void *context),
    void *context) {
  CdbVisitOpt whatnext;

  if (instr == NULL)
    return CdbVisit_Walk;

  whatnext = walker(instr, context);
  if (whatnext == CdbVisit_Walk) {
    if (instr->leftTree && whatnext == CdbVisit_Walk)
      whatnext = myinstrument_walk_node(instr->leftTree, walker, context);
    if (instr->rightTree && whatnext == CdbVisit_Walk)
      whatnext = myinstrument_walk_node(instr->rightTree, walker, context);
    if (instr->subTree && whatnext == CdbVisit_Walk)
      whatnext = myinstrument_walk_node(instr->subTree, walker, context);
    if (instr->subplan && whatnext == CdbVisit_Walk) {
      whatnext = myinstrument_walk_node(instr->subplan, walker, context);
    }
    if (instr->subplanSibling && whatnext == CdbVisit_Walk) {
      whatnext = myinstrument_walk_node(instr->subplanSibling, walker, context);
    }
  } else if (whatnext == CdbVisit_Skip) {
    whatnext = CdbVisit_Walk;
  }

  Assert(whatnext != CdbVisit_Skip);
  return whatnext;
}
