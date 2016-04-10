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
 *
 * cdbsharedstorageop.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/appendonlytid.h"
#include "catalog/heap.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbmirroredappendonly.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbsharedstorageop.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/dispatcher.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbpartition.h"
#include "nodes/parsenodes.h"
#include "optimizer/prep.h"
#include "storage/lmgr.h"

/*
 * Create a sharedStorageOpTask instance
 */
SharedStorageOpTasks *CreateSharedStorageOpTasks(void) {
  SharedStorageOpTasks *retval;
  retval = palloc(sizeof(SharedStorageOpTasks));
  retval->numTasks = 0;
  retval->sizeTasks = 1;
  retval->tasks = palloc0(sizeof(SharedStorageOpTask));
  return retval;
}

/*
 * Destory a sharedStorageOpTask instance
 */
void DropSharedStorageOpTasks(SharedStorageOpTasks *task) {
  if (NULL != task) {
    if (task->tasks) {
      int i;
      for (i = 0; i < task->numTasks; ++i) {
        if (task->tasks[i].relname) {
          pfree(task->tasks[i].relname);
        }
      }
      pfree(task->tasks);
    }
    pfree(task);
  }
}

/*
 * Before add a task, add the operation to delete pending.
 */
void SharedStorageOpPreAddTask(RelFileNode *relFileNode, int32 segmentFileNum,
                               const char *relationName,
                               ItemPointer persistentTid,
                               int64 *persistentSerialNum) {

  smgrcreatepending(relFileNode, segmentFileNum,
                    PersistentFileSysRelStorageMgr_AppendOnly,
                    PersistentFileSysRelBufpoolKind_None, (char *) relationName,
                    persistentTid, persistentSerialNum,
                    /*isLocalBuf*/false,
                    /*bufferPoolBulkLoad*/false,
                    /*flushToXLog*/true);
}

/*
 * add a task, it will be performed on all segments.
 */
void SharedStorageOpAddTask(const char * relname, RelFileNode *node,
                            int32 segno, ItemPointer persistentTid,
                            int64 persistentSerialNum,
                            SharedStorageOpTasks *tasks) {
  Assert(NULL != node && NULL != tasks);
  Assert(tasks->sizeTasks >= tasks->numTasks);

  RelFileNode *n;

  if (tasks->sizeTasks == tasks->numTasks) {
    tasks->tasks = repalloc(tasks->tasks,
                            tasks->sizeTasks * sizeof(SharedStorageOpTask) * 2);
    tasks->sizeTasks *= 2;
  }

  n = &tasks->tasks[tasks->numTasks].node;
  n->dbNode = node->dbNode;
  n->relNode = node->relNode;
  n->spcNode = node->spcNode;

  tasks->tasks[tasks->numTasks].segno = segno;
  tasks->tasks[tasks->numTasks].relname = palloc(strlen(relname) + 1);
  strcpy(tasks->tasks[tasks->numTasks].relname, relname);
  ItemPointerCopy(persistentTid, &tasks->tasks[tasks->numTasks].persistentTid);
  tasks->tasks[tasks->numTasks].persistentSerialNum = persistentSerialNum;

  tasks->numTasks++;
}

/*
 * Do the tasks on all segments.
 */
void PerformSharedStorageOpTasks(SharedStorageOpTasks *tasks,
                                 enum SharedStorageOp op) {
  int i, j = 0;

  if (tasks->numTasks == 0)
    return;

  SharedStorageOpStmt *stat = makeNode(SharedStorageOpStmt);

  stat->op = op;

  stat->relFileNode = palloc(sizeof(RelFileNode) * tasks->numTasks);
  stat->segmentFileNum = palloc(sizeof(int) * tasks->numTasks);
  stat->relationName = palloc(sizeof(char *) * tasks->numTasks);

  QueryContextInfo *contextdisp = CreateQueryContextInfo();

  for (i = 0, j = 0; i < tasks->numTasks; ++i) {
    SharedStorageOpTask *task = tasks->tasks + i;
    prepareDispatchedCatalogTablespace(contextdisp, task->node.spcNode);
    stat->relFileNode[j] = task->node;
    stat->segmentFileNum[j] = task->segno;
    stat->relationName[j] = task->relname;

    ++j;
  }

  stat->numTasks = j;

  FinalizeQueryContextInfo(contextdisp);

  int gp_segments_for_planner_before = gp_segments_for_planner;

  QueryResource *resource =
      AllocateResource(QRL_INHERIT, 0, 0, tasks->numTasks, 1, NULL, 0);
  DispatchDataResult result;
  dispatch_statement_node((Node *) stat, contextdisp, resource, &result);
  dispatch_free_result(&result);
  FreeResource(resource);
  DropQueryContextInfo(contextdisp);

  pfree(stat->relFileNode);
  pfree(stat->relationName);
  pfree(stat->segmentFileNum);
  pfree(stat);

  gp_segments_for_planner = gp_segments_for_planner_before;
}

/*
 * Do all the tasks on masters
 */
void PerformSharedStorageOpTasksOnMaster(SharedStorageOpTasks *tasks,
                                         enum SharedStorageOp op) {
  int i;

  switch (op) {
    case Op_CreateSegFile:
      for (i = 0; i < tasks->numTasks; i++) {
        int error = 0;
        MirroredAppendOnly_Create(&tasks->tasks[i].node, tasks->tasks[i].segno,
                                  tasks->tasks[i].relname, &error);

        if (error != 0) {
          ereport(
              ERROR,
              (errcode_for_file_access(), errmsg("could not create relation file '%s', relation name '%s', segno '%d': %s", relpath(tasks->tasks[i].node), tasks->tasks[i].relname, tasks->tasks[i].segno, strerror(error))));
        }
      }
      break;
    case Op_OverWriteSegFile:
      for (i = 0; i < tasks->numTasks; i++) {
        int error = 0;
        AppendOnly_Overwrite(&tasks->tasks[i].node, tasks->tasks[i].segno,
                             &error);

        if (error != 0) {
          ereport(
              ERROR,
              (errcode_for_file_access(), errmsg("could not create relation with overwrite mode: file '%s', relation name '%s', segno '%d': %s", relpath(tasks->tasks[i].node), tasks->tasks[i].relname, tasks->tasks[i].segno, strerror(error))));
        }
      }
      break;
    default:
      Insist(!"unexpected shared storage op type.");
  }
}

/*
 * after shared storage operation complete, update persistent table.
 */
void PostPerformSharedStorageOpTasks(SharedStorageOpTasks *tasks) {
  int i;

  Relation gp_relation_node;

  Assert(NULL != tasks);

  if (tasks->numTasks == 0)
    return;

  gp_relation_node = heap_open(GpRelfileNodeRelationId, RowExclusiveLock);

  for (i = 0; i < tasks->numTasks; ++i) {
    SharedStorageOpTask * task = &tasks->tasks[i];
    InsertGpRelfileNodeTuple(gp_relation_node,
    /* relationId */0,
                             task->relname, task->node.relNode, task->segno,
                             /* updateIndex */true,
                             &task->persistentTid, task->persistentSerialNum);
  }
  heap_close(gp_relation_node, RowExclusiveLock);
}

/*
 * Create db directory if necessary.
 */
void TransactionCreateDatabaseDir(Oid dbid, Oid tsp) {
  DbDirNode justInTimeDbDirNode;

  /* "Fault-in" the database directory in a tablespace if it doesn't exist yet.*/

  justInTimeDbDirNode.tablespace = tsp;
  justInTimeDbDirNode.database = dbid;

  Assert(Gp_role != GP_ROLE_EXECUTE);

  MirroredFileSysObj_JustInTimeDbDirCreate(&justInTimeDbDirNode);
}

/*
 * Execute shared storage operation on segments.
 */
void PerformSharedStorageOp(SharedStorageOpStmt * stat) {
  Assert(NULL != stat);

  int i;
  int error = 0;
  Assert(Gp_role != GP_ROLE_DISPATCH);

  switch (stat->op) {
    case Op_CreateSegFile:
      for (i = 0; i < stat->numTasks; ++i) {
        /*
         * This interface should pass a state object to let the following
         * code access task.
         */
        if (GpIdentity.segindex == i) {
          MirroredAppendOnly_Create(&stat->relFileNode[i],
                                    stat->segmentFileNum[i],
                                    stat->relationName[i], &error);

          if (error != 0) {
            ereport(
                ERROR,
                (errcode_for_file_access(), errmsg("could not create relation file '%s', relation name '%s': %s", relpath(stat->relFileNode[i]), stat->relationName[i], strerror(error)), errdetail("%s", HdfsGetLastError())));
          }
        }
      }
      break;
    case Op_OverWriteSegFile:
      for (i = 0; i < stat->numTasks; ++i) {
        /*
         * This interface should pass a state object to let the following
         * code access task.
         */
        if (GpIdentity.segindex == i) {
          AppendOnly_Overwrite(&stat->relFileNode[i], stat->segmentFileNum[i],
                               &error);

          if (error != 0) {
            ereport(
                ERROR,
                (errcode_for_file_access(), errmsg("could not create relation with overwrite mode: file '%s', relation name '%s': %s", relpath(stat->relFileNode[i]), stat->relationName[i], strerror(error)), errdetail("%s", HdfsGetLastError())));
          }
        }
      }
      break;
    default:
      Insist(!"unexpected shared storage op type.");
  }
}

static void LockSegfilesOnMasterForSingleRel(Relation rel, int32 segno) {
  Insist(Gp_role == GP_ROLE_DISPATCH);

  /*
   * do not lock segfile with content id = -1
   */
  /*
   for (i = 1; i < rel->rd_segfile0_count; ++i)
   {
   if (RelationIsAoRows(rel) || RelationIsParquet(rel))
   {
   LockRelationAppendOnlySegmentFile(&rel->rd_node, segno,
   AccessExclusiveLock, false, i - 1);
   }
   }
   */
  {
    if (RelationIsAoRows(rel) || RelationIsParquet(rel)) {
      LockRelationAppendOnlySegmentFile(&rel->rd_node, segno,
      AccessExclusiveLock,
                                        false);
    }
  }

}

void LockSegfilesOnMaster(Relation rel, int32 segno) {
  List *children = NIL;
  ListCell *lc;

  children = find_all_inheritors(rel->rd_id);

  foreach(lc, children)
  {
    Relation child_rel = rel;
    Oid oid = lfirst_oid(lc);

    if (rel_is_partitioned(oid))
      continue;

    /* open/close the child relation. */
    if (oid != rel->rd_id)
      child_rel = heap_open(oid, AccessShareLock);

    LockSegfilesOnMasterForSingleRel(child_rel, segno);

    if (oid != rel->rd_id)
      heap_close(child_rel, AccessShareLock);
  }
}
