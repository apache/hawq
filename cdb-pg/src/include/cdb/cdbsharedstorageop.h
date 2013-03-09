/*-------------------------------------------------------------------------
 *
 * cdbsharedstorageop.h
 *
 * Copyright (c) 2009-2013, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBSHAREDSTORAGEOP_H
#define CDBSHAREDSTORAGEOP_H

#include "utils/relcache.h"
#include "nodes/parsenodes.h"

struct SharedStorageOpTask
{
	RelFileNode node;
	int32 segno;
	int32 contentid;
	char * relname;
	ItemPointerData persistentTid;
	int64 persistentSerialNum;
};
typedef struct SharedStorageOpTask SharedStorageOpTask;

struct SharedStorageOpTasks
{
	SharedStorageOpTask *tasks;
	int numTasks;
	int sizeTasks;
};
typedef struct SharedStorageOpTasks SharedStorageOpTasks;

extern SharedStorageOpTasks *CreateSharedStorageOpTasks(void);

extern void DropSharedStorageOpTasks(SharedStorageOpTasks *task);

extern void SharedStorageOpPreAddTask(RelFileNode *relFileNode, int32 segmentFileNum,
		int32 contentid, const char *relationName, ItemPointer persistentTid,
		int64 *persistentSerialNum);

extern void SharedStorageOpAddTask(const char * relname, RelFileNode *node,
		int32 segno, int32 contentid, ItemPointer persistentTid,
		int64 persistentSerialNum, SharedStorageOpTasks *tasks);

extern void TransactionCreateDatabaseDir(Oid dbid, Oid tsp);

extern void PerformSharedStorageOpTasks(SharedStorageOpTasks *tasks);

extern void PostPerformSharedStorageOpTasks(SharedStorageOpTasks *tasks);

extern void PerformSharedStorageOp(SharedStorageOpStmt * stat);

extern void LockSegfilesOnMaster(Relation rel, int32 segno);
#endif /* CDBSHAREDSTORAGEOP_H */
