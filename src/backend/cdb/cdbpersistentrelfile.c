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
 * cdbpersistentrelfile.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"

#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "catalog/gp_persistent.h"
#include "cdb/cdbsharedoidsearch.h"
#include "access/persistentfilesysobjname.h"
#include "cdb/cdbdirectopen.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbpersistentdatabase.h"
#include "cdb/cdbpersistentrelfile.h"
#include "storage/itemptr.h"
#include "utils/hsearch.h"
#include "storage/shmem.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "utils/guc.h"
#include "storage/smgr.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "commands/tablespace.h"

#include "cdb/cdbvars.h"


/*
 * This module is for generic relation file create and drop.
 *
 * For create, it makes the file-system create of an empty file fully transactional so
 * the relation file will be deleted even on system crash.  The relation file could be a heap,
 * index, or append-only (row- or column-store).
 */

typedef struct PersistentRelfileSharedData
{

	PersistentFileSysObjSharedData		fileSysObjSharedData;

} PersistentRelfileSharedData;

#define PersistentRelfileData_StaticInit {PersistentFileSysObjData_StaticInit}

typedef struct PersistentRelfileData
{

	PersistentFileSysObjData		fileSysObjData;

} PersistentRelfileData;


/*
 * Global Variables
 */
PersistentRelfileSharedData	*persistentRelfileSharedData = NULL;

PersistentRelfileData	persistentRelfileData = PersistentRelfileData_StaticInit;

static void PersistentRelfile_VerifyInitScan(void)
{
	if (persistentRelfileSharedData == NULL)
		elog(PANIC, "Persistent relation information shared-memory not setup");

	PersistentFileSysObj_VerifyInitScan();
}

// -----------------------------------------------------------------------------
// Helpers 
// -----------------------------------------------------------------------------

inline static void XLogRecPtr_Zero(XLogRecPtr *xlogLoc)
{
	MemSet(xlogLoc, 0, sizeof(XLogRecPtr));
}

void PersistentRelfile_FlushXLog(void)
{
	PersistentFileSysObj_FlushXLog();
}

extern void PersistentRelfile_Reset(void)
{
	// Currently, nothing to do.
}


//------------------------------------------------------------------------------

int64 PersistentRelfile_MyHighestSerialNum(void)
{
	return PersistentFileSysObj_MyHighestSerialNum(
							PersistentFsObjType_RelationFile);
}

int64 PersistentRelfile_CurrentMaxSerialNum(void)
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	int64 value;

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	value = PersistentFileSysObj_CurrentMaxSerialNum(
							PersistentFsObjType_RelationFile);

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;

	return value;
}

static Oid persistentRelfileCheckTablespace;
static int32 persistentRelfileCheckTablespaceUseCount;
static RelFileNode persistentRelfileCheckTablespaceRelFileNode;

static bool PersistentRelfile_CheckTablespaceScanTupleCallback(
	ItemPointer 			persistentTid,
	int64					persistentSerialNum,
	Datum					*values)
{
	RelFileNode		relFileNode;
	int32			segmentFileNum;

	PersistentFileSysRelStorageMgr relationStorageManager;

	PersistentFileSysState	state;

	PersistentFileSysRelBufpoolKind relBufpoolKind;

	TransactionId			parentXid;
	int64					serialNum;
	ItemPointerData			previousFreeTid;
	bool					sharedStorage;

	GpPersistentRelfileNode_GetValues(
									values,
									&relFileNode.spcNode,
									&relFileNode.dbNode,
									&relFileNode.relNode,
									&segmentFileNum,
									&relationStorageManager,
									&state,
									&relBufpoolKind,
									&parentXid,
									&serialNum,
									&previousFreeTid,
									&sharedStorage);

	if (state == PersistentFileSysState_Created &&
		relFileNode.spcNode == persistentRelfileCheckTablespace)
	{
		persistentRelfileCheckTablespaceUseCount++;
		if (persistentRelfileCheckTablespaceUseCount == 1)
		{
			memcpy(&persistentRelfileCheckTablespaceRelFileNode, &relFileNode, sizeof(RelFileNode));
		}
	}

	return true;	// Continue.
}

void PersistentRelfile_CheckTablespace(
	Oid				tablespace,

	int32			*useCount,

	RelFileNode		*exampleRelFileNode)
{
	persistentRelfileCheckTablespace = tablespace;
	persistentRelfileCheckTablespaceUseCount = 0;

	MemSet(&persistentRelfileCheckTablespaceRelFileNode, 0, sizeof(RelFileNode));

	PersistentFileSysObj_Scan(
		PersistentFsObjType_RelationFile,
		PersistentRelfile_CheckTablespaceScanTupleCallback);

	*useCount = persistentRelfileCheckTablespaceUseCount;
	memcpy(exampleRelFileNode, &persistentRelfileCheckTablespaceRelFileNode, sizeof(RelFileNode));
}

// -----------------------------------------------------------------------------
// State Change 
// -----------------------------------------------------------------------------

/*
 * Indicate we intend to create a relation file as part of the current transaction.
 *
 * An XLOG IntentToCreate record is generated that will guard the subsequent file-system
 * create in case the transaction aborts.
 *
 * After 1 or more calls to this routine to mark intention about relation files that are going
 * to be created, call ~_DoPendingCreates to do the actual file-system creates.  (See its
 * note on XLOG flushing).
 */
void PersistentRelfile_AddCreatePending(
	RelFileNode 		*relFileNode,
				/* The tablespace, database, and relation OIDs for the create. */

	int32				segmentFileNum,

	PersistentFileSysRelStorageMgr relStorageMgr,

	PersistentFileSysRelBufpoolKind relBufpoolKind,

	bool				bufferPoolBulkLoad,

	char				*relationName,

	ItemPointer			persistentTid,
				/* Resulting TID of the gp_persistent_relation_files tuple for the relation. */

	int64				*serialNum,
				/* Resulting serial number for the relation.  Distinquishes the uses of the tuple. */

	bool 				flushToXLog,
				/* When true, the XLOG record for this change will be flushed to disk. */
	bool				isLocalBuf)
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	PersistentFileSysObjName fsObjName;

	XLogRecPtr mirrorBufpoolResyncCkptLoc;
	ItemPointerData previousFreeTid;

	Datum values[Natts_gp_persistent_relfile_node];

	if(RelFileNode_IsEmpty(relFileNode))
		elog(ERROR, "Invalid RelFileNode (0,0,0)");

	MemSet(&previousFreeTid, 0, sizeof(ItemPointerData));
	MemSet(&mirrorBufpoolResyncCkptLoc, 0, sizeof(XLogRecPtr));

	if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
			     "Skipping persistent relation '%s' because we are before persistence work",
				 relpath(*relFileNode));

		MemSet(persistentTid, 0, sizeof(ItemPointerData));
		*serialNum = 0;

		return;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentRelfile_VerifyInitScan();

	PersistentFileSysObjName_SetRelationFile(
										&fsObjName, 
										relFileNode,
										segmentFileNum,
										is_tablespace_shared);

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	GpPersistentRelfileNode_SetDatumValues(
										values,
										relFileNode->spcNode,
										relFileNode->dbNode,
										relFileNode->relNode,
										segmentFileNum,
										relStorageMgr,
										(bufferPoolBulkLoad ?
												PersistentFileSysState_BulkLoadCreatePending :
												PersistentFileSysState_CreatePending),
										relBufpoolKind,
										GetTopTransactionId(),
										/* persistentSerialNum */ 0,	// This will be set by PersistentFileSysObj_AddTuple.
										&previousFreeTid,
										is_tablespace_shared(relFileNode->spcNode));

	PersistentFileSysObj_AddTuple(
							PersistentFsObjType_RelationFile,
							values,
							flushToXLog,
							persistentTid,
							serialNum);
		
	/*
	 * This XLOG must be generated under the persistent write-lock.
	 */
#ifdef MASTER_MIRROR_SYNC

	mmxlog_log_create_relfilenode(
						relFileNode->spcNode,
						relFileNode->dbNode,
						relFileNode->relNode,
						segmentFileNum,
						persistentTid, serialNum);
#endif

	#ifdef FAULT_INJECTOR
			FaultInjector_InjectFaultIfSet(
										   FaultBeforePendingDeleteRelationEntry,
										   DDLNotSpecified,
										   "",  // databaseName
										   ""); // tableName
	#endif

	/*
	 * MPP-18228
	 * To make adding 'Create Pending' entry to persistent table and adding
	 * to the PendingDelete list atomic
	 */
	PendingDelete_AddCreatePendingRelationEntry(
								&fsObjName,
								persistentTid,
								serialNum,
								relStorageMgr,
								relationName,
								isLocalBuf,
								bufferPoolBulkLoad);


	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent relation: Add '%s', relation name '%s' in state 'Create Pending', relation storage manager '%s', serial number " INT64_FORMAT " at TID %s",
			 PersistentFileSysObjName_ObjectName(&fsObjName),
			 relationName,
			 PersistentFileSysRelStorageMgr_Name(relStorageMgr),
			 *serialNum,
			 ItemPointerToString(persistentTid));
}

void PersistentRelfile_AddCreated(
	RelFileNode 		*relFileNode,
				/* The tablespace, database, and relation OIDs for the create. */

	int32				segmentFileNum,

	PersistentFileSysRelStorageMgr relStorageMgr,

	PersistentFileSysRelBufpoolKind relBufpoolKind,

	char				*relationName,

	ItemPointer			persistentTid,
				/* Resulting TID of the gp_persistent_rel_files tuple for the relation. */

	int64				*persistentSerialNum,
				/* Resulting serial number for the relation.  Distinquishes the uses of the tuple. */

	bool 				flushToXLog)
				/* When true, the XLOG record for this change will be flushed to disk. */

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	PersistentFileSysObjName fsObjName;

	XLogRecPtr mirrorBufpoolResyncCkptLoc;
	ItemPointerData previousFreeTid;

	Datum values[Natts_gp_persistent_relfile_node];

	if(RelFileNode_IsEmpty(relFileNode))
		elog(ERROR, "Invalid RelFileNode (0,0,0)");

	MemSet(&previousFreeTid, 0, sizeof(ItemPointerData));
	MemSet(&mirrorBufpoolResyncCkptLoc, 0, sizeof(XLogRecPtr));

	if (!Persistent_BeforePersistenceWork())
		elog(ERROR, "We can only add to persistent meta-data when special states");

	// Verify PersistentFileSysObj_BuildInitScan has been called.
	PersistentRelfile_VerifyInitScan();

	PersistentFileSysObjName_SetRelationFile(
										&fsObjName, 
										relFileNode,
										segmentFileNum,
										is_tablespace_shared);

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	GpPersistentRelfileNode_SetDatumValues(
										values,
										relFileNode->spcNode,
										relFileNode->dbNode,
										relFileNode->relNode,
										segmentFileNum,
										relStorageMgr,
										PersistentFileSysState_Created,
										relBufpoolKind,
										InvalidTransactionId,
										/* persistentSerialNum */ 0,	// This will be set by PersistentFileSysObj_AddTuple.
										&previousFreeTid,
										is_tablespace_shared(relFileNode->spcNode));

	PersistentFileSysObj_AddTuple(
							PersistentFsObjType_RelationFile,
							values,
							flushToXLog,
							persistentTid,
							persistentSerialNum);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent relation: Add '%s', relation name '%s', in state 'Created', relation storage manager '%s', , serial number " INT64_FORMAT " at TID %s",
			 PersistentFileSysObjName_ObjectName(&fsObjName),
			 relationName,
			 PersistentFileSysRelStorageMgr_Name(relStorageMgr),
			 *persistentSerialNum,
			 ItemPointerToString(persistentTid));
}

// -----------------------------------------------------------------------------
// Transaction End  
// -----------------------------------------------------------------------------

/*
 * Indicate the transaction commited and the relation is officially created.
 */
void PersistentRelfile_FinishBufferPoolBulkLoad(
	RelFileNode 		*relFileNode,
				/* The tablespace, database, and relation OIDs for the created relation. */

	ItemPointer			persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the relation. */

	int64				persistentSerialNum)
				/* Serial number for the relation.  Distinquishes the uses of the tuple. */

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	PersistentFileSysObjName fsObjName;

	PersistentFileSysObjStateChangeResult stateChangeResult;
	
	if(RelFileNode_IsEmpty(relFileNode))
		elog(ERROR, "Invalid RelFileNode (0,0,0)");

	if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
			     "Skipping persistent relation '%s' because we are before persistence work",
				 relpath(*relFileNode));

		return;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentRelfile_VerifyInitScan();

	PersistentFileSysObjName_SetRelationFile(
										&fsObjName, 
										relFileNode,
										/* segmentFileNum */ 0,
										is_tablespace_shared);

	// Do this check after skipping out if in bootstrap mode.
	if (PersistentStore_IsZeroTid(persistentTid))
		elog(ERROR, "TID for persistent '%s' tuple for 'Created' is invalid (0,0)",
			 PersistentFileSysObjName_TypeAndObjectName(&fsObjName));

	if (persistentSerialNum == 0)
		elog(ERROR, "Persistent '%s' serial number for 'Created' is invalid (0)",
			 PersistentFileSysObjName_TypeAndObjectName(&fsObjName));

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	stateChangeResult =
		PersistentFileSysObj_StateChange(
								&fsObjName,
								persistentTid,
								persistentSerialNum,
								PersistentFileSysState_CreatePending,
								/* retryPossible */ false,
								/* flushToXlog */ false,
								/* oldState */ NULL,
								/* verifiedActionCallback */ NULL);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent relation: '%s' changed state from 'Bulk Load Create Pending' to 'Create Pending', serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(&fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));
}

/*
 * Indicate the transaction commited and the relation is officially created.
 */
void PersistentRelfile_Created(
	PersistentFileSysObjName *fsObjName,
				/* The tablespace, database, and relation OIDs for the created relation. */

	ItemPointer			persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the relation. */

	int64				persistentSerialNum,
				/* Serial number for the relation.  Distinquishes the uses of the tuple. */

	bool				retryPossible)

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	RelFileNode 		*relFileNode = &fsObjName->variant.rel.relFileNode;

	PersistentFileSysObjStateChangeResult stateChangeResult;
	
	if(RelFileNode_IsEmpty(relFileNode))
		elog(ERROR, "Invalid RelFileNode (0,0,0)");

	if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
			     "Skipping persistent relation '%s' because we are before persistence work",
				 relpath(*relFileNode));

		return;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentRelfile_VerifyInitScan();

	// Do this check after skipping out if in bootstrap mode.
	if (PersistentStore_IsZeroTid(persistentTid))
		elog(ERROR, "TID for persistent '%s' tuple for 'Created' is invalid (0,0)",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName));

	if (persistentSerialNum == 0)
		elog(ERROR, "Persistent '%s' serial number for 'Created' is invalid (0)",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName));

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	stateChangeResult =
		PersistentFileSysObj_StateChange(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								PersistentFileSysState_Created,
								retryPossible,
								/* flushToXlog */ false,
								/* oldState */ NULL,
								/* verifiedActionCallback */ NULL);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent relation: '%s' changed state from 'Create Pending' to 'Created', serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));
}
/*
 * Indicate we intend to drop a relation file as part of the current transaction.
 *
 * This relation file to drop will be listed inside a commit, distributed commit, a distributed 
 * prepared, and distributed commit prepared XOG records.
 *
 * For any of the commit type records, once that XLOG record is flushed then the actual
 * file-system delete will occur.  The flush guarantees the action will be retried after system
 * crash.
 */
PersistentFileSysObjStateChangeResult PersistentRelfile_MarkDropPending(
	PersistentFileSysObjName *fsObjName,
				/* The tablespace, database, and relation OIDs for the drop. */

	ItemPointer			persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the relation. */

	int64				persistentSerialNum,
				/* Serial number for the relation.  Distinquishes the uses of the tuple. */

	bool				retryPossible)

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	RelFileNode 		*relFileNode = &fsObjName->variant.rel.relFileNode;

	PersistentFileSysState oldState;

	PersistentFileSysObjStateChangeResult stateChangeResult;
	
	if(RelFileNode_IsEmpty(relFileNode))
		elog(ERROR, "Invalid RelFileNode (0,0,0)");

	if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
			     "Skipping persistent relation '%s' because we are before persistence work",
				 relpath(*relFileNode));

		return false;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentRelfile_VerifyInitScan();

	// Do this check after skipping out if in bootstrap mode.
	if (PersistentStore_IsZeroTid(persistentTid))
		elog(ERROR, "TID for persistent '%s' tuple for mark DROP pending is invalid (0,0)",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName));

	if (persistentSerialNum == 0)
		elog(ERROR, "Persistent '%s' serial number for mark DROP pending is invalid (0)",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName));

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	stateChangeResult =
		PersistentFileSysObj_StateChange(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								PersistentFileSysState_DropPending,
								retryPossible,
								/* flushToXlog */ false,
								&oldState,
								/* verifiedActionCallback */ NULL);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent relation: '%s' changed state from '%s' to 'Drop Pending', serial number " INT64_FORMAT " TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 PersistentFileSysObjState_Name(oldState),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));

	return stateChangeResult;
}

/*
 * Indicate we are aborting the create of a relation file.
 *
 * This state will make sure the relation gets dropped after a system crash.
 */
PersistentFileSysObjStateChangeResult PersistentRelfile_MarkAbortingCreate(
	PersistentFileSysObjName *fsObjName,
				/* The tablespace, database, and relation OIDs for the aborting create. */

	ItemPointer			persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the relation. */

	int64				persistentSerialNum,
				/* Serial number for the relation.  Distinquishes the uses of the tuple. */

	bool				retryPossible)

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	RelFileNode 		*relFileNode = &fsObjName->variant.rel.relFileNode;

	PersistentFileSysObjStateChangeResult stateChangeResult;
	
	if(RelFileNode_IsEmpty(relFileNode))
		elog(ERROR, "Invalid RelFileNode (0,0,0)");

	if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "Skipping persistent relation '%s' because we are before persistence work",
				 relpath(*relFileNode));

		return false;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentRelfile_VerifyInitScan();

	// Do this check after skipping out if in bootstrap mode.
	if (PersistentStore_IsZeroTid(persistentTid))
		elog(ERROR, "TID for persistent '%s' tuple for mark DROP pending is invalid (0,0)",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName));

	if (persistentSerialNum == 0)
		elog(ERROR, "Persistent '%s' serial number for mark DROP pending is invalid (0)",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName));

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;
	
	stateChangeResult =
		PersistentFileSysObj_StateChange(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								PersistentFileSysState_AbortingCreate,
								retryPossible,
								/* flushToXlog */ false,
								/* oldState */ NULL,
								/* verifiedActionCallback */ NULL);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent relation: '%s' changed state from 'Create Pending' to 'Aborting Create', serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));

	return stateChangeResult;
}

static void
PersistentRelfile_DroppedVerifiedActionCallback(
	PersistentFileSysObjName 	*fsObjName,

	ItemPointer 				persistentTid,
			/* TID of the gp_persistent_rel_files tuple for the relation. */

	int64						persistentSerialNum,
			/* Serial number for the relation.	Distinquishes the uses of the tuple. */

	PersistentFileSysObjVerifyExpectedResult verifyExpectedResult)
{
	RelFileNode *relFileNode = PersistentFileSysObjName_GetRelFileNodePtr(fsObjName);
	int32 segmentFileNum = PersistentFileSysObjName_GetSegmentFileNum(fsObjName);

	switch (verifyExpectedResult)
	{
	case PersistentFileSysObjVerifyExpectedResult_DeleteUnnecessary:
	case PersistentFileSysObjVerifyExpectedResult_StateChangeAlreadyDone:
	case PersistentFileSysObjVerifyExpectedResult_ErrorSuppressed:
		break;
	
	case PersistentFileSysObjVerifyExpectedResult_StateChangeNeeded:
		/*
		 * This XLOG must be generated under the persistent write-lock.
		 */
#ifdef MASTER_MIRROR_SYNC
		mmxlog_log_remove_relfilenode(
							relFileNode->spcNode, 
							relFileNode->dbNode, 
							relFileNode->relNode,
							segmentFileNum,
							persistentTid, persistentSerialNum);
#endif
				
		break;
	
	default:
		elog(ERROR, "Unexpected persistent object verify expected result: %d",
			 verifyExpectedResult);
	}
}

/*
 * Indicate we physically removed the relation file.
 */
void PersistentRelfile_Dropped(
	PersistentFileSysObjName *fsObjName,
				/* The tablespace, database, and relation OIDs for the dropped relation. */

	ItemPointer			persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the relation. */

	int64				persistentSerialNum)
				/* Serial number for the relation.  Distinquishes the uses of the tuple. */

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	RelFileNode 		*relFileNode = &fsObjName->variant.rel.relFileNode;

	PersistentFileSysState oldState;

	PersistentFileSysObjStateChangeResult stateChangeResult;
	
	if(RelFileNode_IsEmpty(relFileNode))
		elog(ERROR, "Invalid RelFileNode (0,0,0)");

	if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
			     "Skipping persistent relation '%s' because we are before persistence work",
				 relpath(*relFileNode));

		return;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentRelfile_VerifyInitScan();

	// Do this check after skipping out if in bootstrap mode.
	if (PersistentStore_IsZeroTid(persistentTid))
		elog(ERROR, "TID for persistent '%s' tuple for mark DROP pending is invalid (0,0)",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName));

	if (persistentSerialNum == 0)
		elog(ERROR, "Persistent '%s' serial number for mark DROP pending is invalid (0)",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName));

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	stateChangeResult =
		PersistentFileSysObj_StateChange(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								PersistentFileSysState_Free,
								/* retryPossible */ false,
								/* flushToXlog */ false,
								&oldState,
								PersistentRelfile_DroppedVerifiedActionCallback);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent relation: '%s' changed state from '%s' to (Free), serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 PersistentFileSysObjState_Name(oldState),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));
}

// -----------------------------------------------------------------------------
// Shmem
// -----------------------------------------------------------------------------

static Size PersistentRelfile_SharedDataSize(void)
{
	return MAXALIGN(sizeof(PersistentRelfileSharedData));
}

/*
 * Return the required shared-memory size for this module.
 */
Size PersistentRelfile_ShmemSize(void)
{
	Size size = 0;

	/* The shared-memory structure. */
	size = add_size(size, PersistentRelfile_SharedDataSize());

	return size;
}

/*
 * Initialize the shared-memory for this module.
 */
void PersistentRelfile_ShmemInit(void)
{
	bool found;

	/* Create the shared-memory structure. */
	persistentRelfileSharedData =
		(PersistentRelfileSharedData *)
						ShmemInitStruct("Mirrored Rel File Data",
										PersistentRelfile_SharedDataSize(),
										&found);

	if (!found)
	{
		PersistentFileSysObj_InitShared(
						&persistentRelfileSharedData->fileSysObjSharedData);
	}

	PersistentFileSysObj_Init(
						&persistentRelfileData.fileSysObjData,
						&persistentRelfileSharedData->fileSysObjSharedData,
						PersistentFsObjType_RelationFile,
						/* scanTupleCallback */ NULL);

	Assert(persistentRelfileSharedData != NULL);
}
