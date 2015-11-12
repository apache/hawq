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
 * cdbsharedstorageop.h
 *
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
		const char *relationName, ItemPointer persistentTid,
		int64 *persistentSerialNum);

extern void SharedStorageOpAddTask(const char * relname, RelFileNode *node,
		int32 segno, ItemPointer persistentTid,
		int64 persistentSerialNum, SharedStorageOpTasks *tasks);

extern void TransactionCreateDatabaseDir(Oid dbid, Oid tsp);

extern void PerformSharedStorageOpTasks(SharedStorageOpTasks *tasks, enum SharedStorageOp op);

extern void PerformSharedStorageOpTasksOnMaster(SharedStorageOpTasks *tasks, enum SharedStorageOp op);

extern void PostPerformSharedStorageOpTasks(SharedStorageOpTasks *tasks);

extern void PerformSharedStorageOp(SharedStorageOpStmt * stat);

extern void LockSegfilesOnMaster(Relation rel, int32 segno);
#endif /* CDBSHAREDSTORAGEOP_H */
