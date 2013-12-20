/*-------------------------------------------------------------------------
 *
 * cdbsharedstorageop.c
 *
 * Copyright (c) 2009-2013, Greenplum inc
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
#include "cdb/cdbsrlz.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbpartition.h"
#include "nodes/parsenodes.h"
#include "optimizer/prep.h"
#include "storage/lmgr.h"

/*
 * Create a sharedStorageOpTask instance
 */
SharedStorageOpTasks *CreateSharedStorageOpTasks(void)
{
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
void DropSharedStorageOpTasks(SharedStorageOpTasks *task)
{
	if (NULL != task)
	{
		if (task->tasks)
		{
			int i;
			for (i = 0; i < task->numTasks; ++i)
			{
				if (task->tasks[i].relname)
				{
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
		int32 contentid, const char *relationName, ItemPointer persistentTid,
		int64 *persistentSerialNum){

	smgrcreatepending(relFileNode,
				segmentFileNum,
				contentid,
				PersistentFileSysRelStorageMgr_AppendOnly,
				PersistentFileSysRelBufpoolKind_None,
				MirroredObjectExistenceState_NotMirrored,
				MirroredRelDataSynchronizationState_None,
				(char *)relationName,
				persistentTid,
				persistentSerialNum,
				/*isLocalBuf*/ false,
				/*bufferPoolBulkLoad*/ false,
				/*flushToXLog*/ true);
}

/*
 * add a task, it will be performed on all segments.
 */
void SharedStorageOpAddTask(const char * relname,
		RelFileNode *node,
		int32 segno, int32 contentid, ItemPointer persistentTid,
		int64 persistentSerialNum, SharedStorageOpTasks *tasks)
{
	Assert(NULL != node && NULL != tasks);
	Assert(tasks->sizeTasks >= tasks->numTasks);

	RelFileNode *n;

	if (tasks->sizeTasks == tasks->numTasks)
	{
		tasks->tasks =
				repalloc(tasks->tasks, tasks->sizeTasks * sizeof(SharedStorageOpTask) * 2);
		tasks->sizeTasks *= 2;
	}

	n = &tasks->tasks[tasks->numTasks].node;
	n->dbNode = node->dbNode;
	n->relNode = node->relNode;
	n->spcNode = node->spcNode;

	tasks->tasks[tasks->numTasks].contentid = contentid;
	tasks->tasks[tasks->numTasks].segno = segno;
	tasks->tasks[tasks->numTasks].relname = palloc(strlen(relname) + 1);
	strcpy(tasks->tasks[tasks->numTasks].relname, relname);
	ItemPointerCopy(persistentTid, &tasks->tasks[tasks->numTasks].persistentTid);
	tasks->tasks[tasks->numTasks].persistentSerialNum = persistentSerialNum;

	tasks->numTasks ++;
}

/*
 * Do the tasks on all segments.
 */
void PerformSharedStorageOpTasks(SharedStorageOpTasks *tasks)
{
	int i, j, masterWorkCount = 0, masterWorkSize = 1;

	char *serializedQuerytree;
	int serializedQuerytree_len;
	CdbDispatcherState ds =	{ NULL, NULL };

	if (tasks->numTasks == 0)
		return;

	SharedStorageOpTask **masterWork = palloc(sizeof(SharedStorageOpTask*));

	Query *q = makeNode(Query);
	SharedStorageOpStmt *stat = makeNode(SharedStorageOpStmt);

	q->commandType = CMD_UTILITY;
	q->utilityStmt = (Node *)stat;
	q->querySource = QSRC_ORIGINAL;
	q->canSetTag = true;

	stat->op = Op_CreateSegFile;

	stat->relFileNode = palloc(sizeof(RelFileNode) * tasks->numTasks);
	stat->segmentFileNum = palloc(sizeof(int) * tasks->numTasks);
	stat->relationName = palloc(sizeof(char *) * tasks->numTasks);
	stat->contentid = palloc(sizeof(int) * tasks->numTasks);

	q->contextdisp = CreateQueryContextInfo();

	for (i = 0, j = 0; i < tasks->numTasks; ++i) {
		SharedStorageOpTask *task = tasks->tasks + i;

		if (MASTER_CONTENT_ID != task->contentid)
		{
			prepareDispatchedCatalogTablespace(q->contextdisp, task->node.spcNode);

			stat->relFileNode[j] = task->node;
			stat->segmentFileNum[j] = task->segno;
			stat->relationName[j] = task->relname;
			stat->contentid[j] = task->contentid;

			++j;
		}
		else
		{
			if (masterWorkCount == masterWorkSize)
			{
				masterWork =
						repalloc(masterWork, masterWorkSize * 2 * sizeof(SharedStorageOpTask *));
				masterWorkSize *= 2;
			}
			masterWork[masterWorkCount++] = task;
		}
	}

	stat->numTasks = j;

	CloseQueryContextInfo(q->contextdisp);
	serializedQuerytree = serializeNode((Node *) q, &serializedQuerytree_len, NULL /*uncompressed_size_out*/);
	Assert(serializedQuerytree != NULL);

	DropQueryContextInfo(q->contextdisp);

	PG_TRY();
	{

		cdbdisp_dispatchCommand("PERFORM SHARED STORAGE OPERATION ON SEGMENTS",
				serializedQuerytree, serializedQuerytree_len,
				true /* cancelonError */, false /* need2phase */,
				false /* withSnapshot */, &ds);

		for (i = 0; i < masterWorkCount; ++i)
		{
			int error = 0;
			MirroredAppendOnly_Create(&masterWork[i]->node, masterWork[i]->segno,
					MASTER_CONTENT_ID, masterWork[i]->relname, &error);

			if (error != 0)
			{
				ereport(ERROR,
					(errcode_for_file_access(),
						errmsg("could not create relation file '%s', relation name '%s', contentid: %d: %s",
							relpath(stat->relFileNode[i]),
							stat->relationName[i], GpIdentity.segindex,
							strerror(error))));
			}
		}

		pfree(masterWork);

		cdbdisp_finishCommand(&ds, NULL, NULL);
	}
	PG_CATCH();
	{
		/* Something happend, clean up after ourselves */
		CdbCheckDispatchResult((struct CdbDispatcherState *)&ds, true);

		cdbdisp_destroyDispatchResults(ds.primaryResults);
		cdbdisp_destroyDispatchThreads(ds.dispatchThreads);

		PG_RE_THROW();
		/* not reached */
	}
	PG_END_TRY();

	pfree(stat->contentid);
	pfree(stat->relFileNode);
	pfree(stat->relationName);
	pfree(stat->segmentFileNum);

	pfree(stat);
}

/*
 * after shared storage operation complete, update persistent table.
 */
void PostPerformSharedStorageOpTasks(SharedStorageOpTasks *tasks)
{
	int i;

	Relation gp_relation_node;

	Assert(NULL != tasks);

	if (tasks->numTasks == 0)
		return;

	gp_relation_node = heap_open(GpRelationNodeRelationId, RowExclusiveLock);

	for (i = 0 ; i < tasks->numTasks; ++i) {
		SharedStorageOpTask * task = &tasks->tasks[i];
		InsertGpRelationNodeTuple(
						gp_relation_node,
						/* relationId */ 0,
						task->relname,
						task->node.relNode,
						task->segno,
						task->contentid,
						/* updateIndex */ true,
						&task->persistentTid,
						task->persistentSerialNum);
	}
	heap_close(gp_relation_node, RowExclusiveLock);
}

/*
 * Create db directory if necessary.
 */
void TransactionCreateDatabaseDir(Oid dbid, Oid tsp)
{
	DbDirNode justInTimeDbDirNode;

	/* "Fault-in" the database directory in a tablespace if it doesn't exist yet.*/

	justInTimeDbDirNode.tablespace = tsp;
	justInTimeDbDirNode.database = dbid;

	Assert(Gp_role != GP_ROLE_EXECUTE);

	int i;
	for (i = 0; i <= GetTotalSegmentsNumber(); ++i)
	{
		MirroredFileSysObj_JustInTimeDbDirCreate(i - 1, &justInTimeDbDirNode);
	}
}

/*
 * Execute shared storage operation on segments.
 */
void PerformSharedStorageOp(SharedStorageOpStmt * stat)
{
	Assert(NULL != stat);

	int i;
	int error = 0;
	Assert(Gp_role != GP_ROLE_DISPATCH);

	switch (stat->op)
	{
	case Op_CreateSegFile:
		for (i = 0; i < stat->numTasks; ++i)
		{
			if (GpIdentity.segindex == stat->contentid[i])
			{
				MirroredAppendOnly_Create(&stat->relFileNode[i], stat->segmentFileNum[i],
						GpIdentity.segindex, stat->relationName[i], &error);

				if (error != 0)
				{
					ereport(ERROR,
						(errcode_for_file_access(),
							errmsg("could not create relation file '%s', relation name '%s', contentid: %d: %s",
								relpath(stat->relFileNode[i]),
								stat->relationName[i], GpIdentity.segindex,
								strerror(error))));
				}
			}
		}
		break;
	default:
		Insist(!"unexpected shared storage op type.");
	}
}

static void LockSegfilesOnMasterForSingleRel(Relation rel, int32 segno)
{
	int i, j;

	Insist(Gp_role == GP_ROLE_DISPATCH);

	/*
	 * do not lock segfile with content id = -1
	 */
	for (i = 1; i < rel->rd_segfile0_count; ++i)
	{
		if (RelationIsAoRows(rel))
		{
			LockRelationAppendOnlySegmentFile(&rel->rd_node, segno,
					AccessExclusiveLock, false, i - 1);
		}
		else if (RelationIsAoCols(rel))
		{
			for (j = 0; j < rel->rd_att->natts; ++j)
			{
				LockRelationAppendOnlySegmentFile(&rel->rd_node,
						j * AOTupleId_MultiplierSegmentFileNum + segno,
						AccessExclusiveLock, false, i - 1);
			}
		}
	}
}

void LockSegfilesOnMaster(Relation rel, int32 segno)
{
	List *children = NIL;
	ListCell *lc;

	children = find_all_inheritors(rel->rd_id);

	foreach(lc, children)
	{
		Relation	child_rel = rel;
		Oid			oid = lfirst_oid(lc);

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
