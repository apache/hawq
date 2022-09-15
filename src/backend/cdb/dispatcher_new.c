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

#include "cdb/dispatcher_new.h"

#include "cdb/cdbconn.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbrelsize.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbvars.h"
#include "cdb/dispatcher.h"  // to involve DispatchDataResult
#include "cdb/dispatcher_mgr.h"
#include "cdb/executormgr.h"
#include "cdb/executormgr_new.h"
#include "cdb/poolmgr.h"
#include "cdb/workermgr.h"
#include "commands/variable.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "optimizer/newPlanner.h"
#include "resourcemanager/communication/rmcomm_QD2RM.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "univplan/cwrapper/univplan-c.h"

/*=========================================================================
 * global states
 */
static MemoryContext DispatchDataContext;
#define DISPATCH_INIT_VALUE 0
static int DispatchInitCount = DISPATCH_INIT_VALUE;

typedef struct MyDispatchTask {
  ProcessIdentity id;
  struct Segment *segment;
  bool entryDB;
  void *refDispatchData;
  struct MyQueryExecutor *refQE;
} MyDispatchTask;

typedef struct MyDispatchTaskGroup {
  int taskNum;
  MyDispatchTask *task;
} MyDispatchTaskGroup;

typedef struct DispatchMetaData {
  int all_slices_num;
  int used_slices_num;
  MyDispatchTaskGroup
      *slices;  // dispatch task grouped by slice from top to bottom
  struct PoolMgrState *dispatchTaskMap;  // segment => dispatch task list
  List *mainDispatchTaskList;            // main dispatch task list
  List *taskPerSegment;  // qes dispatch task in one segment, list of list
} DispatchMetaData;

typedef struct CommonDispatchData {
  struct CdbDispatchResults *results;
} CommonDispatchData;

typedef struct MainDispatchData {
  CommonDispatchData cdata;
  QueryDesc *queryDesc;
  DispatchCommandQueryParms *pQueryParms;
  SliceTable *sliceTable;
  QueryResource *resource;
  CommonPlan *newPlan;
  void *workerMgrState;
  DispatchMetaData metadata;
  char *qdListenerAddr;

  // instrument
  instr_time beginTime;
} MainDispatchData;

typedef struct ProxyDispatchData {
  CommonDispatchData cdata;
  void *workerMgrState;
  char *connMsg;
  MyDispatchTaskGroup *tasks;
} ProxyDispatchData;

typedef enum MyDispStmtType { MDST_STRING, MDST_NODE } MyDispStmtType;

typedef struct MyDispStmt {
  MyDispStmtType type;

  // string type
  const char *string;
  const char *serializeQuerytree;
  int serializeLenQuerytree;

  // node type
  struct Node *node;
  struct QueryContextInfo *ctx;
} MyDispStmt;

static void calcConstValue(struct QueryDesc *queryDesc) {
  bool isSRI = false;
  PlannedStmt *stmt = queryDesc->plannedstmt;
  if (queryDesc->operation == CMD_INSERT) {
    Assert(stmt->commandType == CMD_INSERT);

    /* We might look for constant input relation (instead of SRI), but I'm
     * afraid that wouldn't scale.
     */
    isSRI = IsA(stmt->planTree, Result) && stmt->planTree->lefttree == NULL;
  }

  if (!isSRI) clear_relsize_cache();

  if (queryDesc->operation == CMD_INSERT ||
      queryDesc->operation == CMD_SELECT ||
      queryDesc->operation == CMD_UPDATE ||
      queryDesc->operation == CMD_DELETE) {
    MemoryContext oldContext;

    oldContext = CurrentMemoryContext;
    if (stmt->qdContext) /* Temporary! See comment in PlannedStmt. */
    {
      oldContext = MemoryContextSwitchTo(stmt->qdContext);
    } else /* MPP-8382: memory context of plan tree should not change */
    {
      MemoryContext mc = GetMemoryChunkContext(stmt->planTree);
      oldContext = MemoryContextSwitchTo(mc);
    }

    stmt->planTree = (Plan *)exec_make_plan_constant(stmt, isSRI);

    MemoryContextSwitchTo(oldContext);
  }
}

static void remove_subquery_in_RTEs(Node *node) {
  if (node == NULL) {
    return;
  }

  if (IsA(node, RangeTblEntry)) {
    RangeTblEntry *rte = (RangeTblEntry *)node;
    if (RTE_SUBQUERY == rte->rtekind && NULL != rte->subquery) {
      /*
       * replace subquery with a dummy subquery
       */
      rte->subquery = makeNode(Query);
    }

    return;
  }

  if (IsA(node, List)) {
    List *list = (List *)node;
    ListCell *lc = NULL;
    foreach (lc, list) { remove_subquery_in_RTEs((Node *)lfirst(lc)); }
  }
}

static DispatchCommandQueryParms *dispatcher_fill_query_param(
    const char *strCommand, char *serializeQuerytree, int serializeLenQuerytree,
    char *serializePlantree, int serializeLenPlantree, char *serializeParams,
    int serializeLenParams, char *serializeSliceInfo, int serializeLenSliceInfo,
    int rootIdx, QueryResource *resource) {
  DispatchCommandQueryParms *queryParms;
  Segment *master = NULL;

  if (resource) {
    master = resource->master;
  }

  queryParms = palloc0(sizeof(*queryParms));

  queryParms->strCommand = strCommand;
  queryParms->strCommandlen = strCommand ? strlen(strCommand) + 1 : 0;
  queryParms->serializedQuerytree = serializeQuerytree;
  queryParms->serializedQuerytreelen = serializeLenQuerytree;
  queryParms->serializedPlantree = serializePlantree;
  queryParms->serializedPlantreelen = serializeLenPlantree;
  queryParms->serializedParams = serializeParams;
  queryParms->serializedParamslen = serializeLenParams;
  queryParms->serializedSliceInfo = serializeSliceInfo;
  queryParms->serializedSliceInfolen = serializeLenSliceInfo;
  queryParms->rootIdx = rootIdx;

  /* sequence server info */
  if (master) {
    queryParms->seqServerHost = pstrdup(master->hostip);
    queryParms->seqServerHostlen = strlen(master->hostip) + 1;
  }
  queryParms->seqServerPort = seqServerCtl->seqServerPort;

  queryParms->primary_gang_id =
      0; /* We are relying on the slice table to provide gang ids */

  /*
   * in hawq, there is no distributed transaction
   */
  queryParms->serializedDtxContextInfo = NULL;
  queryParms->serializedDtxContextInfolen = 0;

  return queryParms;
}

typedef struct {
  int sliceIndex;
  int children;
  Slice *slice;
} sliceVec;

static int compare_slice_order(const void *aa, const void *bb) {
  sliceVec *a = (sliceVec *)aa;
  sliceVec *b = (sliceVec *)bb;

  if (a->slice == NULL) return 1;
  if (b->slice == NULL) return -1;

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

  for (i = 0; i < n; i++) dest[i] |= src[i];
}

static int count_bits(char *bits, int nbyte) {
  int i;
  int nbit = 0;
  int bitcount[] = {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4};

  for (i = 0; i < nbyte; i++) {
    nbit += bitcount[bits[i] & 0x0F];
    nbit += bitcount[(bits[i] >> 4) & 0x0F];
  }

  return nbit;
}

static int markbit_dep_children(SliceTable *sliceTable, int sliceIdx,
                                sliceVec *sliceVec, int bitmasklen,
                                char *bits) {
  ListCell *sublist;
  Slice *slice = (Slice *)list_nth(sliceTable->slices, sliceIdx);

  foreach (sublist, slice->children) {
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
  top_count =
      1 + count_dependent_children(sliceTbl, rootIdx, sliceVector, sliceLim);

  qsort(sliceVector, sliceLim, sizeof(sliceVec), compare_slice_order);

  return top_count;
}

static void dispatch_init_env() {
  ++DispatchInitCount;
  if (DispatchDataContext) return;

  DispatchDataContext = AllocSetContextCreate(
      TopMemoryContext, "Dispatch Data Context", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

  executormgr_setupEnv(TopMemoryContext);
}

static void mainDispatchEndEnv(MainDispatchData *data) {
  cdbdisp_destroyDispatchResults(data->cdata.results);
  data->cdata.results = NULL;

  ListCell *cell = NULL;
  int index = 0;
  foreach (cell, data->metadata.mainDispatchTaskList) {
    MyDispatchTask *task = lfirst(cell);
    executormgr_unbindExecutor(task->refQE);
    // free all segdesc which are only pure qes
    List *taskPerSegment = list_nth(data->metadata.taskPerSegment, index);
    ListCell *lc = NULL;
    foreach (lc, taskPerSegment) {
      MyDispatchTask *qeTask = lfirst(lc);
      if (task == qeTask) continue;
      SegmentDatabaseDescriptor *desc = executormgr_getSegDesc(qeTask->refQE);
      cdbconn_termSegmentDescriptor(desc);
    }
    ++index;
  }

  if (--DispatchInitCount == DISPATCH_INIT_VALUE)
    MemoryContextResetAndDeleteChildren(DispatchDataContext);
}

static void proxyDispatchEndEnv(ProxyDispatchData *data) {
  cdbdisp_destroyDispatchResults(data->cdata.results);
  data->cdata.results = NULL;

  ListCell *cell = NULL;
  for (int i = 0; i < data->tasks->taskNum; ++i) {
    MyDispatchTask *task = &data->tasks->task[i];
    executormgr_unbindExecutor(task->refQE);
  }

  if (--DispatchInitCount == DISPATCH_INIT_VALUE)
    MemoryContextResetAndDeleteChildren(DispatchDataContext);
}

static void assignTasks(MainDispatchData *data) {
  sliceVec *sliceVector = NULL;
  int i;
  int slice_num;
  int totalTaskNum = 0;

  /*
   * Dispatch order is top-bottom, this will reduce the useless pkts sends
   * from scan nodes.
   */
  sliceVector =
      palloc0(list_length(data->sliceTable->slices) * sizeof(*sliceVector));
  slice_num =
      fillSliceVector(data->sliceTable, data->pQueryParms->rootIdx, sliceVector,
                      list_length(data->sliceTable->slices));

  data->metadata.used_slices_num = slice_num;
  data->metadata.all_slices_num = list_length(data->sliceTable->slices);

  data->metadata.slices =
      palloc0(data->metadata.used_slices_num * sizeof(MyDispatchTaskGroup));
  for (i = 0; i < data->metadata.used_slices_num; i++) {
    data->metadata.slices[i].taskNum = list_length(data->resource->segments);
    data->metadata.slices[i].task =
        palloc0(data->metadata.slices[i].taskNum * sizeof(MyDispatchTask));
    totalTaskNum += data->metadata.slices[i].taskNum;
  }

  data->cdata.results = cdbdisp_makeDispatchResults(
      totalTaskNum, data->metadata.all_slices_num, false);

  for (i = 0; i < slice_num; i++) {
    Slice *slice = sliceVector[i].slice;
    bool is_writer = false;

    is_writer = (i == 0 && !data->queryDesc->extended_query);
    /* Not all of slices need to dispatch, such as root slice! */
    if (slice->gangType == GANGTYPE_UNALLOCATED) {
      data->metadata.used_slices_num--;
      continue;
    }

    if (slice->directDispatch.isDirectDispatch) {
      MyDispatchTask *task = &data->metadata.slices[i].task[0];

      data->metadata.slices[i].taskNum = 1;

      task->id.id_in_slice = linitial_int(slice->directDispatch.contentIds);
      task->id.slice_id = slice->sliceIndex;
      task->id.gang_member_num = list_length(data->resource->segments);
      task->id.command_count = gp_command_count;
      task->id.is_writer = is_writer;
      task->segment = list_nth(data->resource->segments, task->id.id_in_slice);
      task->id.init = true;
      task->refDispatchData = data;
      poolmgr_put_item(data->metadata.dispatchTaskMap,
                       getSegmentKey(task->segment), task);
      executormgr_makeQueryExecutor(task->segment, task->id.is_writer, task,
                                    task->id.slice_id);

    } else if (slice->gangSize == 1) {
      MyDispatchTask *task = &data->metadata.slices[i].task[0];

      /* Run some single executor case. Should merge with direct dispatch! */
      data->metadata.slices[i].taskNum = 1;

      /* TODO: I really want to remove the MASTER_CONTENT_ID. */
      if (slice->gangType == GANGTYPE_ENTRYDB_READER) {
        task->id.id_in_slice = MASTER_CONTENT_ID;
        task->segment = data->resource->master;
        task->entryDB = true;
      } else {
        task->id.id_in_slice = 0;
        task->segment = linitial(data->resource->segments);
      }

      task->id.slice_id = slice->sliceIndex;
      task->id.gang_member_num = list_length(data->resource->segments);
      task->id.command_count = gp_command_count;
      task->id.is_writer = is_writer;
      task->id.init = true;
      task->refDispatchData = data;
      poolmgr_put_item(data->metadata.dispatchTaskMap,
                       getSegmentKey(task->segment), task);
      executormgr_makeQueryExecutor(task->segment, task->id.is_writer, task,
                                    task->id.slice_id);
    } else {
      int j;

      /* Assign the id from 0. */
      for (j = 0; j < data->metadata.slices[i].taskNum; j++) {
        MyDispatchTask *task = &data->metadata.slices[i].task[j];

        task->id.id_in_slice = j;
        task->id.slice_id = slice->sliceIndex;
        task->id.gang_member_num = list_length(data->resource->segments);
        task->id.command_count = gp_command_count;
        task->id.is_writer = is_writer;
        task->segment =
            list_nth(data->resource->segments, task->id.id_in_slice);
        task->id.init = true;
        task->refDispatchData = data;
        poolmgr_put_item(data->metadata.dispatchTaskMap,
                         getSegmentKey(task->segment), task);
        executormgr_makeQueryExecutor(task->segment, task->id.is_writer, task,
                                      task->id.slice_id);
      }
    }
  }

  pfree(sliceVector);
}

static void gatherTaskPerSegment(List *item, List **mainDispatchTaskList) {
  MyDispatchTask *task = lfirst(list_head(item));
  executormgr_setQueryExecutorIndex(task->refQE,
                                    list_length(*mainDispatchTaskList));
  List *taskPerSeg = getTaskPerSegmentList(task);
  taskPerSeg = lappend(taskPerSeg, item);
  setTaskPerSegmentList(task, taskPerSeg);
  *mainDispatchTaskList = lappend(*mainDispatchTaskList, task);
}

static void decodeQEDetails(MainDispatchData *data) {
  int segNum = list_length(data->metadata.mainDispatchTaskList);
  for (int i = 0; i < segNum; ++i) {
    MyDispatchTask *perSegTask =
        list_nth(data->metadata.mainDispatchTaskList, i);
    int4 *msg = (int4 *)(executormgr_getSegDesc(perSegTask->refQE)
                             ->conn->dispBuffer.data);
    List *taskPerSegment = list_nth(data->metadata.taskPerSegment, i);
    int index = 0;
    ListCell *lc = NULL;
    foreach (lc, taskPerSegment) {
      MyDispatchTask *qeTask = lfirst(lc);
      SegmentDatabaseDescriptor *desc = executormgr_getSegDesc(qeTask->refQE);
      desc->motionListener = (int4)ntohl(*(msg + index++));
      desc->my_listener = (int4)ntohl(*(msg + index++));
      desc->backendPid = (int4)ntohl(*(msg + index++));
      if (!desc->segment) desc->segment = qeTask->segment;
    }
    if (!data->qdListenerAddr && !perSegTask->entryDB) {
      int4 len = (int4)ntohl(*(msg + index++));
      data->qdListenerAddr = (char *)palloc(len);
      memcpy(data->qdListenerAddr, (char *)(msg + index), len);
    }
  }
}

static void dispatcher_serialize_state(MainDispatchData *data) {
  SliceTable *sliceTable =
      data->queryDesc ? data->queryDesc->estate->es_sliceTable : NULL;

  // reset all slice primaryProcesses except QD
  if (sliceTable) {
    ListCell *lc = NULL;
    foreach (lc, sliceTable->slices) {
      Slice *slice = lfirst(lc);
      if (slice->gangType != GANGTYPE_UNALLOCATED)
        slice->primaryProcesses = NIL;
      else {
        CdbProcess *proc = lfirst(list_head(slice->primaryProcesses));
        if (data->qdListenerAddr)
          proc->listenerAddr = data->qdListenerAddr;
        else
          proc->listenerAddr =
              data->resource->master->hostip;  // query is not dispatched,
                                               // interconnect will not be setup
      }
    }
  }

  int segNum = list_length(data->metadata.mainDispatchTaskList);
  for (int i = 0; i < segNum; ++i) {
    StringInfoData buf;
    initStringInfo(&buf);
    MyDispatchTask *perSegTask =
        list_nth(data->metadata.mainDispatchTaskList, i);
    List *taskPerSegment = list_nth(data->metadata.taskPerSegment, i);
    ListCell *lc = NULL;
    foreach (lc, taskPerSegment) {
      MyDispatchTask *qeTask = lfirst(lc);
      newSerializeProcessIdentity(&qeTask->id, &buf);
    }
    executormgr_setQueryExecutorIdMsg(perSegTask->refQE, buf.data, buf.len);
  }

  if (sliceTable) {
    // sort primaryProcesses order by vseg id asc
    for (int m = 0; m < data->metadata.used_slices_num; ++m) {
      MyDispatchTaskGroup *taskGroup = data->metadata.slices + m;
      for (int n = 0; n < taskGroup->taskNum; ++n) {
        MyDispatchTask *qeTask = taskGroup->task + n;
        Slice *planSlice = list_nth(sliceTable->slices, qeTask->id.slice_id);
        CdbProcess *proc = makeNode(CdbProcess);
        executormgr_getConnectInfo(qeTask->refQE, &proc->listenerAddr,
                                   &proc->listenerPort, &proc->myListenerPort,
                                   &proc->pid);
        proc->contentid = qeTask->id.id_in_slice;
        planSlice->primaryProcesses =
            lappend(planSlice->primaryProcesses, proc);
      }
    }

    int sliceInfoLen;
    char *sliceInfo = serializeNode((Node *)sliceTable, &sliceInfoLen, NULL);
    data->pQueryParms->serializedSliceInfo = sliceInfo;
    data->pQueryParms->serializedSliceInfolen = sliceInfoLen;
  }
}

static void aggregateQueryResource(QueryResource *queryRes) {
  if (queryRes && queryRes->segments && (list_length(queryRes->segments) > 0)) {
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;
    Segment *seg1 = NULL;
    Segment *seg2 = NULL;
    int nseg = 0;
    int i = 0;

    queryRes->numSegments = list_length(queryRes->segments);
    queryRes->segment_vcore_agg = NULL;
    queryRes->segment_vcore_agg = palloc0(sizeof(int) * queryRes->numSegments);
    queryRes->segment_vcore_writer =
        palloc0(sizeof(int) * queryRes->numSegments);

    foreach (lc1, queryRes->segments) {
      int j = 0;
      nseg = 0;
      seg1 = (Segment *)lfirst(lc1);
      queryRes->segment_vcore_writer[i] = i;
      foreach (lc2, queryRes->segments) {
        seg2 = (Segment *)lfirst(lc2);
        if (strcmp(seg1->hostname, seg2->hostname) == 0) {
          nseg++;
          if (j < queryRes->segment_vcore_writer[i]) {
            queryRes->segment_vcore_writer[i] = j;
          }
        }
        j++;
      }

      queryRes->segment_vcore_agg[i++] = nseg;
    }
  }
}

static void dispatcher_serialize_query_resource(MainDispatchData *data) {
  char *serializedQueryResource = NULL;
  int serializedQueryResourcelen = 0;
  QueryResource *queryResource = data->resource;

  if (queryResource) {
    aggregateQueryResource(queryResource);
    serializedQueryResource =
        serializeNode((Node *)queryResource, &serializedQueryResourcelen, NULL);
    data->pQueryParms->serializedQueryResource = serializedQueryResource;
    data->pQueryParms->serializedQueryResourcelen = serializedQueryResourcelen;

    if (queryResource->segment_vcore_agg) {
      pfree(queryResource->segment_vcore_agg);
    }
    if (queryResource->segment_vcore_writer) {
      pfree(queryResource->segment_vcore_writer);
    }
  }
}

static void dispatcher_serialize_common_plan(MainDispatchData *data,
                                             CommonPlanContext *ctx) {
  PlannedStmt *stmt = data->queryDesc ? data->queryDesc->plannedstmt : NULL;
  if (stmt) {
    univPlanSetDoInstrument(
        ctx->univplan, data->queryDesc->estate->es_sliceTable->doInstrument);
    univPlanSetNCrossLevelParams(ctx->univplan, stmt->nCrossLevelParams);
    univPlanSetCmdType(ctx->univplan, (UnivPlanCCmdType)stmt->commandType);

    // add interconnect info
    for (int i = 0;
         i < data->queryDesc->estate->es_sliceTable->nInitPlans +
                 data->queryDesc->estate->es_sliceTable->nMotions + 1;
         ++i) {
      Slice *slice =
          list_nth(data->queryDesc->estate->es_sliceTable->slices, i);

      // if parent slice supports direct dispatch, should dispatch listener id
      // set default value -1
      int execId = -1;
      if (slice->parentIndex != -1) {
        Slice *parentSlice = list_nth(data->queryDesc->estate->es_sliceTable->slices, slice->parentIndex);
        if (parentSlice->directDispatch.isDirectDispatch) {
          List *contentIds = parentSlice->directDispatch.contentIds;
          Assert(list_length(contentIds) == 1);
          execId = linitial_int(contentIds);
        }
      }
      uint32_t listenerNum = list_length(slice->primaryProcesses);
      char **addr = palloc(listenerNum * sizeof(char *));
      int32 *port = palloc(listenerNum * sizeof(int32));
      int index = 0;
      ListCell *lc;
      foreach (lc, slice->primaryProcesses) {
        CdbProcess *proc = (CdbProcess *)lfirst(lc);
        addr[index] = pstrdup(proc->listenerAddr);
        port[index] = proc->myListenerPort;
        ++index;
      }
      univPlanReceiverAddListeners(ctx->univplan, listenerNum, execId, addr, port);
      pfree(port);
    }
    univPlanFixVarType(ctx->univplan);

    char numberStrBuf[20];

    univPlanAddGuc(ctx->univplan, "work_file_dir", rm_seg_tmp_dirs);
    univPlanAddGuc(ctx->univplan, "runtime_filter_mode",
                   new_executor_runtime_filter_mode);
    univPlanAddGuc(ctx->univplan, "enable_partitioned_hashagg",
                   new_executor_enable_partitioned_hashagg_mode);
    univPlanAddGuc(ctx->univplan, "enable_partitioned_hashjoin",
                   new_executor_enable_partitioned_hashjoin_mode);
    univPlanAddGuc(ctx->univplan, "enable_external_sort",
                   new_executor_enable_external_sort_mode);
    univPlanAddGuc(ctx->univplan, "filter_pushdown",
                   orc_enable_filter_pushdown);
    univPlanAddGuc(ctx->univplan, "magma_enable_shm", magma_enable_shm);
    sprintf(numberStrBuf, "%d", magma_shm_limit_per_block * 1024);
    univPlanAddGuc(ctx->univplan, "magma_shm_limit_per_block", numberStrBuf);

    char *timezone_str = show_timezone();
    univPlanAddGuc(ctx->univplan, "timezone_string", timezone_str);

    sprintf(numberStrBuf, "%d",
            new_executor_partitioned_hash_recursive_depth_limit);
    univPlanAddGuc(ctx->univplan, "partitioned_hash_recursive_depth_limit",
                   numberStrBuf);
    sprintf(numberStrBuf, "%d", new_executor_external_sort_memory_limit_size_mb);
    univPlanAddGuc(ctx->univplan, "external_sort_memory_limit_size_mb",
                   numberStrBuf);

    univPlanAddGuc(ctx->univplan, "new_interconnect_type",
                   show_new_interconnect_type());

    univPlanStagize(ctx->univplan);

    if (client_min_messages == DEBUG1) {
      elog(DEBUG1, "common plan:\n%s",
           univPlanGetJsonFormatedPlan(ctx->univplan));
    }

    // serialize and compress
    int len;
    const char *val = univPlanSerialize(ctx->univplan, &len, true);
    data->pQueryParms->serializedCommonPlan = (char *)palloc(len);
    memcpy(data->pQueryParms->serializedCommonPlan, val, len);
    data->pQueryParms->serializedCommonPlanLen = len;

    // prepare common plan for QD execute
    data->newPlan = (CommonPlan *)palloc(sizeof(CommonPlan));
    data->newPlan->len = len;
    data->newPlan->str = (char *)palloc(len);
    memcpy(data->newPlan->str, val, len);
    univPlanFreeInstance(&ctx->univplan);

    data->queryDesc->newPlan = data->newPlan;

    int numSlicesToDispatch =
        data->queryDesc->plannedstmt->planTree->nMotionNodes;
    uint64 planSizeInKb =
        ((uint64)len * (uint64)numSlicesToDispatch) / (uint64)1024;
    elog(LOG,
         "Dispatch new plan instead of old plan, size to "
         "dispatch: " UINT64_FORMAT "KB",
         planSizeInKb);
  }
}

static void assignTasksForStmt(struct MainDispatchData *data) {
  data->metadata.dispatchTaskMap =
      poolmgr_create_pool(DispatchDataContext, NULL);

  int entryDBSegNum = 0;
  int totalTaskNum = 0;
  data->metadata.used_slices_num = entryDBSegNum + 1;
  data->metadata.all_slices_num = entryDBSegNum + 1;
  data->metadata.slices =
      palloc0(data->metadata.used_slices_num * sizeof(MyDispatchTaskGroup));

  data->metadata.slices[entryDBSegNum].taskNum =
      list_length(data->resource->segments);
  data->metadata.slices[entryDBSegNum].task = palloc0(
      data->metadata.slices[entryDBSegNum].taskNum * sizeof(MyDispatchTask));
  totalTaskNum += data->metadata.slices[entryDBSegNum].taskNum;

  data->cdata.results = cdbdisp_makeDispatchResults(
      totalTaskNum, data->metadata.all_slices_num, false);

  for (int i = 0; i < data->metadata.slices[entryDBSegNum].taskNum; ++i) {
    MyDispatchTask *task = &data->metadata.slices[entryDBSegNum].task[i];

    task->id.id_in_slice = i;
    task->id.slice_id = entryDBSegNum;
    task->id.gang_member_num = list_length(data->resource->segments);
    task->id.command_count = gp_command_count;
    task->id.is_writer = true;
    task->segment = list_nth(data->resource->segments, i);
    task->id.init = true;
    task->entryDB = task->segment->master;
    task->refDispatchData = data;
    poolmgr_put_item(data->metadata.dispatchTaskMap,
                     getSegmentKey(task->segment), task);
    executormgr_makeQueryExecutor(task->segment, task->id.is_writer, task,
                                  task->id.slice_id);
  }

  poolmgr_iterate_item_entry(
      data->metadata.dispatchTaskMap,
      (PoolMgrIterateCallback)gatherTaskPerSegment,
      (PoolIterateArg)&data->metadata.mainDispatchTaskList);

  data->pQueryParms->primary_gang_id = -1;
}

static void mainDispatchStmtNodePrepare(struct MainDispatchData *data,
                                        struct Node *node,
                                        struct QueryContextInfo *ctx) {
  MemoryContext old = MemoryContextSwitchTo(DispatchDataContext);

  // construct an utility query node
  Query *q = makeNode(Query);
  q->commandType = CMD_UTILITY;
  q->utilityStmt = node;
  q->contextdisp = ctx;
  q->querySource = QSRC_ORIGINAL;
  /*
   * We must set q->canSetTag = true.  False would be used to hide a command
   * introduced by rule expansion which is not allowed to return its
   * completion status in the command tag (PQcmdStatus/PQcmdTuples). For
   * example, if the original unexpanded command was SELECT, the status
   * should come back as "SELECT n" and should not reflect other commands
   * inserted by rewrite rules.  True means we want the status.
   */
  q->canSetTag = true;

  int serializedQueryTreeLen;
  char *serializedQueryTree = serializeNode((Node *)q, &serializedQueryTreeLen,
                                            NULL /*uncompressed_size*/);

  data->pQueryParms = dispatcher_fill_query_param(
      debug_query_string, serializedQueryTree, serializedQueryTreeLen, NULL, 0,
      NULL, 0, NULL, 0, 0, data->resource);

  assignTasksForStmt(data);

  MemoryContextSwitchTo(old);
}

static void dispatchStmt(MyDispStmt *stmt, QueryResource *resource,
                         struct DispatchDataResult *result) {
  MainDispatchData *data = mainDispatchInit(resource);

  if (stmt->type == MDST_NODE)
    mainDispatchStmtNodePrepare(data, stmt->node, stmt->ctx);
  else
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("feature not suppported"),
             errdetail("dispatch statement string is not supported yet.")));

  PG_TRY();
  {
    CommonPlanContext ctx;
    bool newPlanner = can_convert_common_plan(data->queryDesc, &ctx);
    mainDispatchRun(data, &ctx, newPlanner);
    mainDispatchWait(data, false);
    /* index stmt need to update catalog */
    if (stmt->node != NULL && IsA(stmt->node, IndexStmt))
    {
      IndexStmt *idxStmt = (IndexStmt *)(stmt->node);
      CdbDispatchResults *pr = mainDispatchGetResults((DispatchDataResult *) data);
      cdbdisp_handleModifiedOrcIndexCatalogOnSegments(&(idxStmt->allidxinfos), pr, UpdateCatalogOrcIndexModifiedOnSegments);
    }
    if (result && !mainDispatchHasError(data)) {
      int entryDBSegNum = 0;
      int segNum = list_length(resource->segments) + entryDBSegNum;
      initStringInfo(&result->errbuf);
      result->result = cdbdisp_returnResults(
          segNum, data->cdata.results, &result->errbuf, &result->numresults);
      // cdbdisp_returnResults freed the memory
      data->cdata.results = NULL;
    }
    mainDispatchCleanUp(&data);
  }
  PG_CATCH();
  {
    mainDispatchWait(data, true);
    mainDispatchCleanUp(&data);
    PG_RE_THROW();
  }
  PG_END_TRY();
}

static mainDispatchDumpError(struct MainDispatchData *data, bool throwError) {
  struct CdbDispatchResults *dispResults = data->cdata.results;

  StringInfoData buf;
  initStringInfo(&buf);
  cdbdisp_dumpDispatchResults(dispResults, &buf, false);

  // If buf is null, try to find meaningful error info from resultArray
  if (buf.len == 0) {
    for (int i = 0; i < dispResults->resultCount; ++i) {
      if (dispResults->resultArray[i].error_message) {
        cdbdisp_dumpDispatchResult(&(dispResults->resultArray[i]), false, &buf);
        break;
      }
    }
  }

  PG_TRY();
  {
    ereport(ERROR, (errcode(dispResults->errcode), errOmitLocation(true),
                    errmsg("%s", buf.data)));
  }
  PG_CATCH();
  {
    pfree(buf.data);
    if (throwError) PG_RE_THROW();
  }
  PG_END_TRY();
}

static void mainDispatchCollectError(struct MainDispatchData *data) {
  MemoryContext old = MemoryContextSwitchTo(DispatchDataContext);

  int errHostNum = 0;
  char **errHostInfo = (char **)palloc0(
      list_length(data->metadata.mainDispatchTaskList) * sizeof(char *));

  ListCell *cell = NULL;
  foreach (cell, data->metadata.mainDispatchTaskList) {
    MyDispatchTask *task = lfirst(cell);
    executormgr_mergeDispErr(task->refQE, &errHostNum, &errHostInfo);
  }

  if (errHostNum > 0 && mainDispatchHasError(data))
    sendFailedNodeToResourceManager(errHostNum, errHostInfo);

  for (int i = 0; i < errHostNum; i++) {
    pfree(errHostInfo[i]);
  }
  pfree(errHostInfo);

  MemoryContextSwitchTo(old);
}

static debugTaskSegDetails(struct MainDispatchData *data) {
  int segNum = list_length(data->metadata.mainDispatchTaskList);
  int segId = 1;
  StringInfoData str;
  initStringInfo(&str);
  for (int i = 0; i < segNum; ++i) {
    MyDispatchTask *perSegTask =
        list_nth(data->metadata.mainDispatchTaskList, i);
    List *taskPerSegment = list_nth(data->metadata.taskPerSegment, i);
    resetStringInfo(&str);
    appendStringInfo(&str,
                     "MainDisp-Seg"
                     "%d"
                     "/"
                     "%d"
                     "-"
                     "%d"
                     "("
                     "%s"
                     ")"
                     ":",
                     segId, segNum, list_length(taskPerSegment),
                     taskSegToString(perSegTask));
    ListCell *lc = NULL;
    foreach (lc, taskPerSegment) {
      MyDispatchTask *qeTask = lfirst(lc);
      appendStringInfo(&str,
                       " "
                       "%s",
                       taskIdToString(qeTask));
    }
    appendStringInfoString(&str, ";");
    elog(LOG, "%s", str.data);
    ++segId;
  }
}

MainDispatchData *mainDispatchInit(QueryResource *resource) {
  dispatch_init_env();

  MemoryContext old = MemoryContextSwitchTo(DispatchDataContext);
  MainDispatchData *data = palloc0(sizeof(MainDispatchData));
  MemoryContextSwitchTo(old);
  data->resource = resource;

  return data;
}

void mainDispatchPrepare(struct MainDispatchData *data,
                         struct QueryDesc *queryDesc, bool newPlanner) {
  MemoryContext old = MemoryContextSwitchTo(DispatchDataContext);

  /*
   * Let's evaluate STABLE functions now, so we get consistent values on the QEs
   *
   * Also, if this is a single-row INSERT statement, let's evaluate
   * nextval() and currval() now, so that we get the QD's values, and a
   * consistent value for everyone
   *
   */
  calcConstValue(queryDesc);

  /*
   *  MPP-20785:
   *  remove subquery field from RTE's since it is not needed during query
   *  execution, this is an optimization to reduce size of serialized plan
   *  before dispatching
   */
  remove_subquery_in_RTEs((Node *)(queryDesc->plannedstmt->rtable));

  /*
   * serialized plan tree. Note that we're called for a single
   * slice tree (corresponding to an initPlan or the main plan), so the
   * parameters are fixed and we can include them in the prefix.
   */
  char *splan;
  int splan_len, splan_len_uncompressed;
  if (newPlanner) {
    // new excutor does not need old plan serialized
    struct Plan *tmpPlan = queryDesc->plannedstmt->planTree;
    queryDesc->plannedstmt->planTree = NULL;
    splan = serializeNode((Node *)queryDesc->plannedstmt, &splan_len,
                          &splan_len_uncompressed);
    queryDesc->plannedstmt->planTree = tmpPlan;
  } else {
    splan = serializeNode((Node *)queryDesc->plannedstmt, &splan_len,
                          &splan_len_uncompressed);
  }

  /* compute the total uncompressed size of the query plan for all slices */
  int num_slices = queryDesc->plannedstmt->planTree->nMotionNodes + 1;
  uint64 plan_size_in_kb =
      ((uint64)splan_len_uncompressed * (uint64)num_slices) / (uint64)1024;

  // elog(DEBUG1, "Query plan size to dispatch: " UINT64_FORMAT "KB",
  // plan_size_in_kb);

  if (0 < gp_max_plan_size && plan_size_in_kb > gp_max_plan_size) {
    ereport(
        ERROR,
        (errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
         (errmsg("Query plan size limit exceeded, current size: " UINT64_FORMAT
                 "KB, max allowed size: %dKB",
                 plan_size_in_kb, gp_max_plan_size),
          errhint("Size controlled by gp_max_plan_size"))));
  }
  Assert(splan != NULL && splan_len > 0 && splan_len_uncompressed > 0);

  // limit plan slice number
  if (0 < gp_max_plan_slice && num_slices > gp_max_plan_slice) {
    ereport(
        ERROR,
        (errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
         (errmsg("Query plan slice number limit exceeded, current: " UINT64_FORMAT
                 ", max allowed: %d",
                 num_slices, gp_max_plan_slice),
          errhint("Slice number controlled by gp_max_plan_slice"))));
  }

  char *sparams;
  int sparams_len;
  if (queryDesc->params != NULL && queryDesc->params->numParams > 0) {
    ParamListInfoData *pli;
    ParamExternData *pxd;
    StringInfoData parambuf;
    Size length;
    int plioff;
    int32 iparam;

    /* Allocate buffer for params */
    initStringInfo(&parambuf);

    /* Copy ParamListInfoData header and ParamExternData array */
    pli = queryDesc->params;
    length = (char *)&pli->params[pli->numParams] - (char *)pli;
    plioff = parambuf.len;
    Assert(plioff == MAXALIGN(plioff));
    appendBinaryStringInfo(&parambuf, pli, length);

    /* Copy pass-by-reference param values. */
    for (iparam = 0; iparam < queryDesc->params->numParams; iparam++) {
      int16 typlen;
      bool typbyval;

      /* Recompute pli each time in case parambuf.data is repalloc'ed */
      pli = (ParamListInfoData *)(parambuf.data + plioff);
      pxd = &pli->params[iparam];

      /* Does pxd->value contain the value itself, or a pointer? */
      get_typlenbyval(pxd->ptype, &typlen, &typbyval);
      if (!typbyval) {
        char *s = DatumGetPointer(pxd->value);

        if (pxd->isnull || !PointerIsValid(s)) {
          pxd->isnull = true;
          pxd->value = 0;
        } else {
          length = datumGetSize(pxd->value, typbyval, typlen);

          /* MPP-1637: we *must* set this before we
           * append. Appending may realloc, which will
           * invalidate our pxd ptr. (obviously we could
           * append first if we recalculate pxd from the new
           * base address) */
          pxd->value = Int32GetDatum(length);

          appendBinaryStringInfo(&parambuf, &iparam, sizeof(iparam));
          appendBinaryStringInfo(&parambuf, s, length);
        }
      }
    }
    sparams = parambuf.data;
    sparams_len = parambuf.len;
  } else {
    sparams = NULL;
    sparams_len = 0;
  }

  data->pQueryParms = dispatcher_fill_query_param(
      queryDesc->sourceText, NULL, 0, splan, splan_len, sparams, sparams_len,
      NULL, 0, RootSliceIndex(queryDesc->estate), queryDesc->resource);
  data->queryDesc = queryDesc;
  data->sliceTable = queryDesc->estate->es_sliceTable;
  data->metadata.dispatchTaskMap =
      poolmgr_create_pool(DispatchDataContext, NULL);

  assignTasks(data);

  poolmgr_iterate_item_entry(
      data->metadata.dispatchTaskMap,
      (PoolMgrIterateCallback)gatherTaskPerSegment,
      (PoolIterateArg)&data->metadata.mainDispatchTaskList);

  MemoryContextSwitchTo(old);
}

void mainDispatchRun(struct MainDispatchData *data, CommonPlanContext *ctx,
                     bool newPlanner) {
  // no need to dispatch
  if (!data->metadata.mainDispatchTaskList) return;

  INSTR_TIME_SET_CURRENT(data->beginTime);
  MemoryContext old = MemoryContextSwitchTo(DispatchDataContext);

  List *executorList = NIL;
  ListCell *cell = NULL;
  foreach (cell, data->metadata.mainDispatchTaskList) {
    MyDispatchTask *task = lfirst(cell);
    struct SegmentDatabaseDescriptor *desc =
        executormgr_allocateExecutor(task->segment, task->entryDB);
    if (!desc) desc = executormgr_prepareConnect(task->segment);
    executormgr_setSegDesc(task->refQE, desc);
    executorList = lappend(executorList, task->refQE);
  }
  List *tasks =
      groupTaskRoundRobin(executorList, main_disp_connections_per_thread);

  if (Debug_print_dispatcher_detail) {
    debugTaskSegDetails(data);
    debugMainDispTaskDetails(tasks);
  }

  tasks = makeQueryExecutorGroup(tasks, true);

  data->workerMgrState = workermgr_create_workermgr_state(list_length(tasks));

  bool hasError = false;
  PG_TRY();
  {
    workermgr_submit_job(data->workerMgrState, tasks,
                         (WorkerMgrTaskCallback)mainDispatchFuncConnect);
    workermgr_wait_job(data->workerMgrState);
  }
  PG_CATCH();
  {
    workermgr_cancel_job(data->workerMgrState);
    FlushErrorState();
    hasError = true;
  }
  PG_END_TRY();
  if (hasError) goto error;

  CHECK_FOR_INTERRUPTS();

  if (!mainDispatchHasError(data)) {
    decodeQEDetails(data);

    dispatcher_serialize_state(data);
    dispatcher_serialize_query_resource(data);
    if (newPlanner) dispatcher_serialize_common_plan(data, ctx);

    workermgr_submit_job(data->workerMgrState, tasks,
                         (WorkerMgrTaskCallback)mainDispatchFuncRun);
  } else {
    MemoryContextSwitchTo(old);
    ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                    errmsg(CDB_MOTION_LOST_CONTACT_STRING)));
  }

  MemoryContextSwitchTo(old);

  return;

error:
  MemoryContextSwitchTo(old);
  elog(ERROR, "MainDispatcher error out");
}

void mainDispatchWait(struct MainDispatchData *data, bool requestCancel) {
  if (!data->workerMgrState) return;

  if (requestCancel)
    workermgr_cancel_job(data->workerMgrState);
  else
    workermgr_wait_job(data->workerMgrState);

  CHECK_FOR_INTERRUPTS();

  mainDispatchCollectError(data);
}

void mainDispatchCleanUp(struct MainDispatchData **data) {
  if (*data == NULL) return;

  if (mainDispatchHasError(*data)) {
    if (!proc_exit_inprogress) mainDispatchDumpError(*data, true);
  } else {
    if (Debug_print_dispatcher_detail) {
      StringInfoData str;
      initStringInfo(&str);
      mainDispatchPrintStats(&str, *data);
      elog(LOG, "%s", str.data);
    }
    mainDispatchEndEnv(*data);
    *data = NULL;
  }
}

struct CdbDispatchResults *mainDispatchGetResults(
    struct MainDispatchData *data) {
  return data->cdata.results;
}

bool mainDispatchHasError(struct MainDispatchData *data) {
  return data->cdata.results && data->cdata.results->errcode;
}

int mainDispatchGetSegNum(struct MainDispatchData *data) {
  return list_length(data->resource->segments);
}

void mainDispatchCatchError(struct MainDispatchData **data) {
  if ((*data)->workerMgrState) {
    workermgr_cancel_job((*data)->workerMgrState);
    mainDispatchCollectError(*data);
  }

  int qdErrCode = elog_geterrcode();
  bool useQeError = false;
  struct CdbDispatchResults *dispResults = (*data)->cdata.results;

  if (qdErrCode == ERRCODE_GP_INTERCONNECTION_ERROR) {
    char *qdErrMsg = elog_message();

    if (dispResults->errcode) {
      if (dispResults->errcode != ERRCODE_GP_INTERCONNECTION_ERROR)
        useQeError = true;
    }
  }

  if (useQeError) mainDispatchDumpError(*data, false);

  mainDispatchEndEnv(*data);
  *data = NULL;
}

void mainDispatchPrintStats(StringInfo buf, struct MainDispatchData *data) {
  if (!data->metadata.mainDispatchTaskList) return;

  instr_time totalTime, connectTime, dispatchTime;
  instr_time firstConnectBegin, lastConnectEnd;
  instr_time firstDispatchBegin, lastDispatchEnd;
  instr_time maxConnectTime, minConnectTime, totalConnectTime;
  instr_time maxDispatchTime, minDispatchTime, totalDispatchTime;
  INSTR_TIME_SET_ZERO(totalTime);
  INSTR_TIME_SET_ZERO(connectTime);
  INSTR_TIME_SET_ZERO(dispatchTime);

  int segNum = list_length(data->metadata.mainDispatchTaskList);
  int qeNum = 0;
  int dataSize = 0;
  for (int i = 0; i < segNum; ++i) {
    instr_time connectBegin;
    instr_time connectEnd;
    instr_time connectDiff;
    instr_time dispatchBegin;
    instr_time dispatchEnd;
    instr_time dispatchDiff;
    MyDispatchTask *perSegTask =
        list_nth(data->metadata.mainDispatchTaskList, i);
    executormgr_getStats(perSegTask->refQE, &connectBegin, &connectEnd,
                         &dispatchBegin, &dispatchEnd, &dataSize);
    INSTR_TIME_SET_ZERO(connectDiff);
    INSTR_TIME_SET_ZERO(dispatchDiff);
    INSTR_TIME_ACCUM_DIFF(connectDiff, connectEnd, connectBegin);
    INSTR_TIME_ACCUM_DIFF(dispatchDiff, dispatchEnd, dispatchBegin);

    if (i == 0) {
      INSTR_TIME_ASSIGN(firstConnectBegin, connectBegin);
      INSTR_TIME_ASSIGN(lastConnectEnd, connectEnd);
      INSTR_TIME_ASSIGN(firstDispatchBegin, dispatchBegin);
      INSTR_TIME_ASSIGN(lastDispatchEnd, dispatchEnd);
      INSTR_TIME_ASSIGN(maxConnectTime, connectDiff);
      INSTR_TIME_ASSIGN(minConnectTime, connectDiff);
      INSTR_TIME_ASSIGN(totalConnectTime, connectDiff);
      INSTR_TIME_ASSIGN(maxDispatchTime, dispatchDiff);
      INSTR_TIME_ASSIGN(minDispatchTime, dispatchDiff);
      INSTR_TIME_ASSIGN(totalDispatchTime, dispatchDiff);
    } else {
      if (INSTR_TIME_GREATER_THAN(firstConnectBegin, connectBegin))
        INSTR_TIME_ASSIGN(firstConnectBegin, connectBegin);
      if (INSTR_TIME_LESS_THAN(lastConnectEnd, connectEnd))
        INSTR_TIME_ASSIGN(lastConnectEnd, connectEnd);
      if (INSTR_TIME_GREATER_THAN(firstDispatchBegin, dispatchBegin))
        INSTR_TIME_ASSIGN(firstDispatchBegin, dispatchBegin);
      if (INSTR_TIME_LESS_THAN(lastDispatchEnd, dispatchEnd))
        INSTR_TIME_ASSIGN(lastDispatchEnd, dispatchEnd);
      if (INSTR_TIME_LESS_THAN(maxConnectTime, connectDiff))
        INSTR_TIME_ASSIGN(maxConnectTime, connectDiff);
      if (INSTR_TIME_GREATER_THAN(minConnectTime, connectDiff))
        INSTR_TIME_ASSIGN(minConnectTime, connectDiff);
      if (INSTR_TIME_LESS_THAN(maxDispatchTime, dispatchDiff))
        INSTR_TIME_ASSIGN(maxDispatchTime, dispatchDiff);
      if (INSTR_TIME_GREATER_THAN(minDispatchTime, dispatchDiff))
        INSTR_TIME_ASSIGN(minDispatchTime, dispatchDiff);
      INSTR_TIME_ADD(totalConnectTime, connectDiff);
      INSTR_TIME_ADD(totalDispatchTime, dispatchDiff);
    }
    qeNum += list_length(list_nth(data->metadata.taskPerSegment, i));
  }

  INSTR_TIME_ACCUM_DIFF(totalTime, lastDispatchEnd, data->beginTime);
  INSTR_TIME_ACCUM_DIFF(connectTime, lastConnectEnd, firstConnectBegin);
  INSTR_TIME_ACCUM_DIFF(dispatchTime, lastDispatchEnd, firstDispatchBegin);

  appendStringInfo(buf, "Dispatcher statistics:\n");
  appendStringInfo(buf, "  proxy num: %d; qe num: %d; data size: %.1fKB;",
                   segNum, qeNum, (double)dataSize / 1024);
  appendStringInfo(buf,
                   " dispatcher time(total/connection/dispatch data): (%.3f "
                   "ms/%.3f ms/%.3f ms).\n",
                   INSTR_TIME_GET_MILLISEC(totalTime),
                   INSTR_TIME_GET_MILLISEC(connectTime),
                   INSTR_TIME_GET_MILLISEC(dispatchTime));
  appendStringInfo(
      buf, "  proxy connect time(max/min/avg): (%.3f ms/%.3f ms/%.3f ms);",
      INSTR_TIME_GET_MILLISEC(maxConnectTime),
      INSTR_TIME_GET_MILLISEC(minConnectTime),
      INSTR_TIME_GET_MILLISEC(totalConnectTime) / segNum);
  appendStringInfo(
      buf, "  proxy dispatch time(max/min/avg): (%.3f ms/%.3f ms/%.3f ms).\n",
      INSTR_TIME_GET_MILLISEC(maxDispatchTime),
      INSTR_TIME_GET_MILLISEC(minDispatchTime),
      INSTR_TIME_GET_MILLISEC(totalDispatchTime) / segNum);
}

void proxyDispatchInit(int qeNum, char *msg, struct ProxyDispatchData **data) {
  if (*data) proxyDispatchEndEnv(*data);

  dispatch_init_env();

  MemoryContext old = MemoryContextSwitchTo(DispatchDataContext);
  *data = palloc0(sizeof(ProxyDispatchData));
  (*data)->tasks = palloc0(sizeof(MyDispatchTaskGroup));
  (*data)->tasks->taskNum = qeNum;
  (*data)->tasks->task = palloc0(qeNum * sizeof(MyDispatchTask));
  (*data)->connMsg = msg;
  MemoryContextSwitchTo(old);
}

void proxyDispatchPrepare(struct ProxyDispatchData *data) {
  MemoryContext old = MemoryContextSwitchTo(DispatchDataContext);

  data->cdata.results =
      cdbdisp_makeDispatchResults(data->tasks->taskNum, -1, false);

  List *executorList = NIL;
  for (int i = 0; i < data->tasks->taskNum; ++i) {
    MyDispatchTask *task = &data->tasks->task[i];
    task->refDispatchData = data;
    struct MyQueryExecutor *qe =
        executormgr_makeQueryExecutor(NULL, task->id.is_writer, task, -1);
    executormgr_setQueryExecutorIndex(qe, i);
    struct SegmentDatabaseDescriptor *desc =
        executormgr_allocateExecutor(NULL, false);
    if (!desc) {
      desc = executormgr_prepareConnect(NULL);
      executorList = lappend(executorList, qe);
    }
    executormgr_setSegDesc(qe, desc);
  }

  if (executorList != NIL) {
    List *tasks =
        groupTaskRoundRobin(executorList, proxy_disp_connections_per_thread);
    tasks = makeQueryExecutorGroup(tasks, false);

    struct WorkerMgrState *state =
        workermgr_create_workermgr_state(list_length(tasks));

    workermgr_submit_job(state, tasks,
                         (WorkerMgrTaskCallback)proxyDispatchFuncConnect);
    workermgr_wait_job(state);
  }

  if(proxyDispatchHasError(data)){
    MemoryContextSwitchTo(old);
    ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                    errmsg("proxy dispatcher failed to connect to segment")));
  }

  MemoryContextSwitchTo(old);
}

void sendSegQEDetails(struct ProxyDispatchData *data) {
  MemoryContext old = MemoryContextSwitchTo(DispatchDataContext);

  StringInfoData buf;
  pq_beginmessage(&buf, 'V');
  for (int i = 0; i < data->tasks->taskNum; ++i) {
    MyDispatchTask *task = &data->tasks->task[i];
    SegmentDatabaseDescriptor *desc = executormgr_getSegDesc(task->refQE);
    pq_sendint(&buf, (int32)desc->motionListener, sizeof(int32));
    pq_sendint(&buf, (int32)desc->my_listener, sizeof(int32));
    pq_sendint(&buf, (int32)desc->backendPid, sizeof(int32));
  }
  /*
   * Send back qd listener address. This guarantees that the outgoing
   * connections will connect to an address that is reachable in the event when
   * the master can not be reached by segments through the network interface
   * recorded in the catalog.
   */
  char *listenerAddr = MyProcPort->remote_host;
  if (strcmp(listenerAddr, "[local]") == 0) listenerAddr = "127.0.0.1";
  pq_sendint(&buf, strlen(listenerAddr) + 1, sizeof(int32));
  pq_sendbytes(&buf, listenerAddr, strlen(listenerAddr) + 1);

  pq_endmessage(&buf);

  MemoryContextSwitchTo(old);
}

void proxyDispatchRun(ProxyDispatchData *data, char *connMsg) {
  MemoryContext old = MemoryContextSwitchTo(DispatchDataContext);

  List *executorList = NIL;
  for (int i = 0; i < data->tasks->taskNum; ++i) {
    MyDispatchTask *task = &data->tasks->task[i];
    executorList = lappend(executorList, task->refQE);
  }

  List *tasks =
      groupTaskRoundRobin(executorList, proxy_disp_connections_per_thread);

  if (Debug_print_dispatcher_detail) debugProxyDispTaskDetails(tasks);

  tasks = makeQueryExecutorGroup(tasks, true);

  data->connMsg = connMsg;
  data->workerMgrState = workermgr_create_workermgr_state(list_length(tasks));

  workermgr_submit_job(data->workerMgrState, tasks,
                       (WorkerMgrTaskCallback)proxyDispatchFuncRun);

  MemoryContextSwitchTo(old);
}

void proxyDispatchWait(struct ProxyDispatchData *data) {
  if (data->workerMgrState) workermgr_wait_job(data->workerMgrState);
}

void proxyDispatchCleanUp(struct ProxyDispatchData **data) {
  if (*data == NULL) return;

  proxyDispatchEndEnv(*data);

  *data = NULL;
}

bool proxyDispatchHasError(struct ProxyDispatchData *data) {
  return data->cdata.results && data->cdata.results->errcode;
}

void mainDispatchStmtNode(struct Node *node, struct QueryContextInfo *ctx,
                          struct QueryResource *resource,
                          struct DispatchDataResult *result) {
  MyDispStmt stmt;
  MemSet(&stmt, 0, sizeof(stmt));
  stmt.type = MDST_NODE;
  stmt.node = node;
  stmt.ctx = ctx;
  if (result) MemSet(result, 0, sizeof(*result));
  dispatchStmt(&stmt, resource, result);
}

List *getTaskPerSegmentList(struct MyDispatchTask *task) {
  return ((MainDispatchData *)task->refDispatchData)->metadata.taskPerSegment;
}

extern void setTaskPerSegmentList(struct MyDispatchTask *task, struct List *l) {
  ((MainDispatchData *)task->refDispatchData)->metadata.taskPerSegment = l;
}

void setTaskRefQE(struct MyDispatchTask *task, struct MyQueryExecutor *qe) {
  task->refQE = qe;
}

int getTaskSliceId(struct MyDispatchTask *task) { return task->id.slice_id; }

int getTaskSegId(struct MyDispatchTask *task) { return task->id.id_in_slice; }

struct MyQueryExecutor *getTaskRefQE(struct MyDispatchTask *task) {
  return task->refQE;
}

DispatchCommandQueryParms *getQueryParms(struct MyDispatchTask *task) {
  return ((MainDispatchData *)task->refDispatchData)->pQueryParms;
}

char *getTaskConnMsg(struct MyDispatchTask *task) {
  return ((ProxyDispatchData *)task->refDispatchData)->connMsg;
}

struct CdbDispatchResults *getDispatchResults(struct MyDispatchTask *task) {
  return ((CommonDispatchData *)task->refDispatchData)->results;
}

void checkQdError(void *dispatchData) {
  if (Gp_role == GP_ROLE_DISPATCH) {
    MainDispatchData *data = (MainDispatchData *)dispatchData;
    if (data && mainDispatchHasError(data))
      ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                      errmsg(CDB_MOTION_LOST_CONTACT_STRING)));
  }
}

const char *taskIdToString(struct MyDispatchTask *task) {
  StringInfoData str;
  initStringInfo(&str);
  appendStringInfo(&str,
                   "s"
                   "%d"
                   "_v"
                   "%d",
                   task->id.slice_id, task->id.id_in_slice);
  return str.data;
}

const char *taskSegToString(struct MyDispatchTask *task) {
  StringInfoData str;
  initStringInfo(&str);
  if (task->entryDB)
    appendStringInfo(&str, "%s", "EntryDB");
  else
    appendStringInfo(&str, "%s", task->segment->hostname);
  return str.data;
}
