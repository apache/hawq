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
 * cdbpersistentrelation.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlogmm.h"
#include "cdb/cdbpersistentrelation.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "commands/tablespace.h"

typedef struct PersistentRelationSharedData
{
	PersistentFileSysObjSharedData fileSysObjSharedData;
} PersistentRelationSharedData;

#define PersistentRelationData_StaticInit {PersistentFileSysObjData_StaticInit}

typedef struct PersistentRelationData
{
	PersistentFileSysObjData fileSysObjData;
} PersistentRelationData;

typedef struct RelationDirEntryKey
{
	Oid tablespaceOid;
	Oid databaseOid;
	Oid relfilenodeOid;
} RelationDirEntryKey;

typedef struct RelationDirEntryData
{
	RelationDirEntryKey key;

	PersistentFileSysState state;
	int64 persistentSerialNum;
	ItemPointerData persistentTid;
} RelationDirEntryData;

typedef RelationDirEntryData *RelationDirEntry;

/*
 * Global Variables
 */
PersistentRelationSharedData *persistentRelationSharedData = NULL;
static HTAB *persistentRelationSharedHashTable = NULL;

PersistentRelationData persistentRelationData = PersistentRelationData_StaticInit;

/*
 * Variables for checking table space.
 */
static Oid persistentRelationCheckTablespace;
static int32 persistentRelationCheckTablespaceUseCount;
static RelFileNode persistentRelationCheckTablespaceRelationNode;

static bool PersistentRelation_CheckTablespaceScanTupleCallback(
    ItemPointer persistentTid,
    int64 persistentSerialNum,
    Datum *values)
{
  RelFileNode relFileNode;

  PersistentFileSysState state;
  int32 reserved;
  TransactionId parentXid;
  int64 serialNum;
  ItemPointerData previousFreeTid;

  bool sharedStorage;

  GpPersistentRelationNode_GetValues(
                                      values,
                                      &relFileNode.spcNode,
                                      &relFileNode.dbNode,
                                      &relFileNode.relNode,
                                      &state,
                                      &reserved,
                                      &parentXid,
                                      &serialNum,
                                      &previousFreeTid,
                                      &sharedStorage);

  if (state == PersistentFileSysState_Created &&
      relFileNode.spcNode == persistentRelationCheckTablespace)
  {
    persistentRelationCheckTablespaceUseCount++;
    if (persistentRelationCheckTablespaceUseCount == 1)
    {
      memcpy(&persistentRelationCheckTablespaceRelationNode, &relFileNode, sizeof(RelFileNode));
    }
  }

  return true; // Continue;
}

void PersistentRelation_CheckTablespace(
    Oid tablespace,
    int32 *useCount,
    RelFileNode *exampleRelationNode)
{
  persistentRelationCheckTablespace = tablespace;
  persistentRelationCheckTablespaceUseCount = 0;

  MemSet(&persistentRelationCheckTablespaceRelationNode, 0, sizeof(RelFileNode));

  PersistentFileSysObj_Scan(
      PersistentFsObjType_RelationDir,
      PersistentRelation_CheckTablespaceScanTupleCallback);

  *useCount = persistentRelationCheckTablespaceUseCount;
  memcpy(exampleRelationNode, &persistentRelationCheckTablespaceRelationNode, sizeof(RelFileNode));
}

static void PersistentRelation_VerifyInitScan(void)
{
	if (persistentRelationSharedData == NULL)
	{
		elog(PANIC, "Persistent relation information shared-memory not setup");
	}

	PersistentFileSysObj_VerifyInitScan();
}

/*
 * Return the hash entry for a relation.
 */
static RelationDirEntry
PersistentRelation_FindEntryUnderLock(
	RelFileNode *relFileNode)
{
	bool found;

	RelationDirEntry relationDirEntry;
	RelationDirEntryKey key;

	elog(DEBUG1, "PersistentRelation_FindEntryUnderLock: Relation (%u/%u/%u)",
			relFileNode->spcNode,
			relFileNode->dbNode,
			relFileNode->relNode);

	if (persistentRelationSharedHashTable == NULL)
	{
		elog(PANIC, "Persistent relation information shared-memory not setup");
	}

	key.tablespaceOid = relFileNode->spcNode;
	key.databaseOid = relFileNode->dbNode;
	key.relfilenodeOid = relFileNode->relNode;

	relationDirEntry = (RelationDirEntry)
							hash_search(persistentRelationSharedHashTable,
										(void *)&key,
										HASH_FIND,
										&found);

	if (!found)
	{
		return NULL;
	}

	return relationDirEntry;
}

static RelationDirEntry
PersistentRelation_CreateEntryUnderLock(
	RelFileNode *relFileNode)
{
	bool found;

	RelationDirEntry relationDirEntry;
	RelationDirEntryKey key;

	elog(DEBUG1, "PersistentRelation_CreateEntryUnderLock: relation (%u/%u/%u)",
			relFileNode->spcNode,
			relFileNode->dbNode,
			relFileNode->relNode);

	if (persistentRelationSharedHashTable == NULL)
	{
		elog(PANIC, "Persistent relation information shared-memory not setup");
	}

	key.tablespaceOid = relFileNode->spcNode;
	key.databaseOid = relFileNode->dbNode;
	key.relfilenodeOid = relFileNode->relNode;

	relationDirEntry = (RelationDirEntry)
								hash_search(persistentRelationSharedHashTable,
											(void *)&key,
											HASH_ENTER,
											&found);

	if (relationDirEntry == NULL)
	{
		return NULL;
	}

	relationDirEntry->state = PersistentFileSysState_Free;
	relationDirEntry->persistentSerialNum = 0;
	MemSet(&relationDirEntry->persistentTid, 0, sizeof(ItemPointerData));

	return relationDirEntry;
}

static void
PersistentRelation_RemoveEntryUnderLock(
	RelationDirEntry relationDirEntry)
{
	RelationDirEntry removeRelationDirEntry;

	if (persistentRelationSharedHashTable == NULL)
	{
		elog(PANIC, "Persistent relation information shared-memory not setup");
	}

	removeRelationDirEntry = (RelationDirEntry)
									hash_search(persistentRelationSharedHashTable,
												(void *) &relationDirEntry->key,
												HASH_REMOVE,
												NULL);

	if (removeRelationDirEntry == NULL)
	{
		elog(ERROR, "Trying to delete entry that does not exist");
	}
}

PersistentFileSysState
PersistentRelation_GetState(
	RelFileNode *relFileNode)
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	RelationDirEntry relationDirEntry;

	PersistentFileSysState state;

	PersistentRelation_VerifyInitScan();

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	relationDirEntry = PersistentRelation_FindEntryUnderLock(relFileNode);

	if (relationDirEntry == NULL)
	{
		elog(ERROR, "Did not find persistent relation entry %u/%u/%u",
				relFileNode->spcNode,
				relFileNode->dbNode,
				relFileNode->relNode);
	}

	state = relationDirEntry->state;

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;

	return state;
}

static bool
PersistentRelation_ScanTupleCallback(
	ItemPointer persistentTid,
	int64 persistentSerialNum,
	Datum *values)
{
	RelFileNode relFileNode;

	PersistentFileSysState state;
	int32 reserved;
	TransactionId parentXid;
	int64 serialNum;
	ItemPointerData previousFreeTid;

	RelationDirEntry relationDirEntry;
	bool sharedStorage;

	GpPersistentRelationNode_GetValues(
								values,
								&relFileNode.spcNode,
								&relFileNode.dbNode,
								&relFileNode.relNode,
								&state,
								&reserved,
								&parentXid,
								&serialNum,
								&previousFreeTid,
								&sharedStorage);

	if (state == PersistentFileSysState_Free)
	{
		if (Debug_persistent_print)
		{
			elog(Persistent_DebugPrintLevel(),
				"PersistentRelation_ScanTupleCallback: TID %s, serial number " INT64_FORMAT " is free",
				ItemPointerToString2(persistentTid),
				persistentSerialNum);
		}

		return true;
	}

	relationDirEntry = PersistentRelation_CreateEntryUnderLock(&relFileNode);

	if (relationDirEntry == NULL)
	{
		elog(ERROR, "Out of shared-memory for persistent relations");
	}

	relationDirEntry->state = state;
	relationDirEntry->persistentSerialNum = serialNum;
	relationDirEntry->persistentTid = *persistentTid;

	if (Debug_persistent_print)
	{
		elog(Persistent_DebugPrintLevel(),
			"PersistentRelation_ScanTupleCallback: tablespace %u, database %u, relation %u, state %s, TID %s, serial number " INT64_FORMAT,
			relFileNode.spcNode,
			relFileNode.dbNode,
			relFileNode.relNode,
			PersistentFileSysObjState_Name(state),
			ItemPointerToString2(persistentTid),
			persistentSerialNum);
	}

	return true;
}

void PersistentRelation_Reset(void)
{
	HASH_SEQ_STATUS stat;

	RelationDirEntry relationDirEntry;

	hash_seq_init(&stat, persistentRelationSharedHashTable);

	while (true)
	{
		RelationDirEntry removeRelationDirEntry;
		PersistentFileSysObjName fsObjName;
		RelFileNode relFileNode;

		relationDirEntry = hash_seq_search(&stat);
		if (relationDirEntry == NULL)
		{
			break;
		}

		relFileNode.spcNode = relationDirEntry->key.tablespaceOid;
		relFileNode.dbNode = relationDirEntry->key.databaseOid;
		relFileNode.relNode = relationDirEntry->key.relfilenodeOid;

		PersistentFileSysObjName_SetRelationDir(
										&fsObjName,
										&relFileNode,
										is_tablespace_shared);

		if (Debug_persistent_print)
		{
			elog(Persistent_DebugPrintLevel(),
				"Persistent relation directory: Resetting '%s' serial number " INT64_FORMAT " at TID %s",
				PersistentFileSysObjName_ObjectName(&fsObjName),
				relationDirEntry->persistentSerialNum,
				ItemPointerToString(&relationDirEntry->persistentTid));
		}

		removeRelationDirEntry = (RelationDirEntry)
											hash_search(persistentRelationSharedHashTable,
													(void *) &relationDirEntry->key,
													HASH_REMOVE,
													NULL);

		if (removeRelationDirEntry == NULL)
		{
			elog(ERROR, "Trying to delete entry that does not exist");
		}
	}
}

void
PersistentRelation_LookupTidAndSerialNum(
	RelFileNode *relFileNode,
	ItemPointer persistentTid,
	int64 *persistentSerialNum)
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	RelationDirEntry relationDirEntry;

	PersistentRelation_VerifyInitScan();

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	relationDirEntry = PersistentRelation_FindEntryUnderLock(relFileNode);

	if (relationDirEntry == NULL)
	{
		elog(ERROR, "Did not find persistent relation entry %u/%u/%u",
				relFileNode->spcNode,
				relFileNode->dbNode,
				relFileNode->relNode);
	}

	*persistentTid = relationDirEntry->persistentTid;
	*persistentSerialNum = relationDirEntry->persistentSerialNum;

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;
}

// -----------------------------------------------------------------------------
// Shmem
// -----------------------------------------------------------------------------

static Size PersistentRelation_SharedDataSize(void)
{
	return MAXALIGN(sizeof(PersistentRelationSharedData));
}

/*
 * Return the required shared-memory size for this module.
 */
Size PersistentRelation_ShmemSize(void)
{
	Size size;

	/* The hash table of persistent relations */
	size = hash_estimate_size((Size)gp_max_relations,
							sizeof(RelationDirEntryData));

	/* The shared-memory structure. */
	size = add_size(size, PersistentRelation_SharedDataSize());

	elog(LOG, "PersistentRelation_ShmemSize: %zu = "
			"gp_max_relations: %d "
			"* sizeof(RelationDirEntryData): %zu "
			"+ PersistentRelation_SharedDataSize(): %zu",
			size,
			gp_max_relations,
			sizeof(RelationDirEntryData),
			PersistentRelation_SharedDataSize());

	return size;
}

/*
 * PersistentRelation_HashTableInit
 *
 * Create or find shared-memory hash table.
 */
static bool
PersistentRelation_HashTableInit(void)
{
	HASHCTL info;
	int hash_flags;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(RelationDirEntryKey);
	info.entrysize = sizeof(RelationDirEntryData);
	info.hash = tag_hash;
	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	persistentRelationSharedHashTable =
							ShmemInitHash("Persistent Relation Hash",
										gp_max_relations,
										gp_max_relations,
										&info,
										hash_flags);

	if (persistentRelationSharedHashTable == NULL)
	{
		return false;
	}

	return true;
}

/*
 * Initialize the shared-memory for this module.
 */
void PersistentRelation_ShmemInit(void)
{
	bool found;
	bool ok;

	/* Create the shared-memory structure. */
	persistentRelationSharedData =
			(PersistentRelationSharedData *)
							ShmemInitStruct("Mirrored Relation Data",
											PersistentRelation_SharedDataSize(),
											&found);

	if (!found)
	{
		PersistentFileSysObj_InitShared(
						&persistentRelationSharedData->fileSysObjSharedData);
	}

	/* Create or find our shared-memory hash table. */
	ok = PersistentRelation_HashTableInit();
	if (!ok)
	{
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("Not enough shared memory for persistent relation hash table")));
	}

	PersistentFileSysObj_Init(
						&persistentRelationData.fileSysObjData,
						&persistentRelationSharedData->fileSysObjSharedData,
						PersistentFsObjType_RelationDir,
						PersistentRelation_ScanTupleCallback);

	Assert(persistentRelationSharedData != NULL);
	Assert(persistentRelationSharedHashTable != NULL);
}

void PersistentRelation_MarkCreatePending(
		RelFileNode *relFileNode,
		ItemPointer persistentTid,
		int64 *persistentSerialNum,
		bool flushToXLog)
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	PersistentFileSysObjName fsObjName;

	RelationDirEntry relationDirEntry;

	ItemPointerData previousFreeTid;
	Datum values[Natts_gp_persistent_relation_node];

	if (RelFileNode_IsEmpty(relFileNode))
	{
		elog(ERROR, "Invalid RelFileNode (0,0,0)");
	}

	MemSet(&previousFreeTid, 0, sizeof(ItemPointerData));

	if (Persistent_BeforePersistenceWork())
	{
		if (Debug_persistent_print)
		{
			elog(Persistent_DebugPrintLevel(),
				"Skipping persistent relation '%s' because we are before persistence work",
				relpath(*relFileNode));
		}

		*persistentSerialNum = 0;
		/*
		 * The initdb process will load the persistent table once we out
		 * of bootstrap mode.
		 */
		return;
	}

	PersistentRelation_VerifyInitScan();

	PersistentFileSysObjName_SetRelationDir(
							&fsObjName,
							relFileNode,
							is_tablespace_shared);

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	relationDirEntry = PersistentRelation_CreateEntryUnderLock(relFileNode);

	if (relationDirEntry == NULL)
	{
		/* If out of shared memory, no need to promote to PANIC. */
		WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("Out of shared-memory for persistent relations"),
				errhint("You may need to increase the gp_max_relations value"),
				errOmitLocation(true)));
	}

	relationDirEntry->state = PersistentFileSysState_CreatePending;

	GpPersistentRelationNode_SetDatumValues(
									values,
									relFileNode->spcNode,
									relFileNode->dbNode,
									relFileNode->relNode,
									PersistentFileSysState_CreatePending,
									/* reserved */ 0,
									/* parentXid */ GetTopTransactionId(),
									/* persistentSerialNum */ 0, // This will be set by PersistentFileSysObj_AddTuple.
									&previousFreeTid,
									is_tablespace_shared(relFileNode->spcNode));

	PersistentFileSysObj_AddTuple(
							PersistentFsObjType_RelationDir,
							values,
							flushToXLog,
							&relationDirEntry->persistentTid,
							&relationDirEntry->persistentSerialNum);

	*persistentTid = relationDirEntry->persistentTid;
	*persistentSerialNum = relationDirEntry->persistentSerialNum;

	/*
	 * This XLOG must be generated under the persistent write-lock.
	 */
#ifdef MASTER_MIRROR_SYNC
	mmxlog_log_create_relation(
						relFileNode->spcNode,
						relFileNode->dbNode,
						relFileNode->relNode,
						persistentTid, *persistentSerialNum);
#endif

	/*
	 * MPP-18228
	 * To make adding 'Create Pending' entry to persistent table and
	 * adding to the PendingDelete list atomic
	 */
	PendingDelete_AddCreatePendingEntryWrapper(
					&fsObjName,
					persistentTid,
					*persistentSerialNum);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;
}

void PersistentRelation_AddCreated(
		RelFileNode *relFileNode,
		ItemPointer persistentTid,
		int64 *persistentSerialNum,
		bool flushToXLog)
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	PersistentFileSysObjName fsObjName;

	RelationDirEntry relationDirEntry;

	ItemPointerData previousFreeTid;
	Datum values[Natts_gp_persistent_relation_node];

	if (RelFileNode_IsEmpty(relFileNode))
	{
		elog(ERROR, "Invalid RelFileNode (0,0,0)");
	}

	MemSet(&previousFreeTid, 0, sizeof(ItemPointerData));

	if (!Persistent_BeforePersistenceWork())
	{
		elog(ERROR, "We can only add to persistent meta-data when special states");
	}

	/* Verify PersistentFileSysObj_BuildInitScan has been called */
	PersistentRelation_VerifyInitScan();

	PersistentFileSysObjName_SetRelationDir(
										&fsObjName,
										relFileNode,
										is_tablespace_shared);

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	relationDirEntry = PersistentRelation_CreateEntryUnderLock(relFileNode);

	if (relationDirEntry == NULL)
	{
		/* If out of shared memory, no need to promote to PANIC. */
		WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("Out of shared-memory for persistent relations"),
				errhint("You may need to increase the gp_max_relations value"),
				errOmitLocation(true)));
	}

	relationDirEntry->state = PersistentFileSysState_Created;

	GpPersistentRelationNode_SetDatumValues(
										values,
										relFileNode->spcNode,
										relFileNode->dbNode,
										relFileNode->relNode,
										PersistentFileSysState_Created,
										/* reserved */ 0,
										/* parentXid */ InvalidTransactionId,
										/* persistentSerialNum */ 0,
										&previousFreeTid,
										is_tablespace_shared(relFileNode->spcNode));

	PersistentFileSysObj_AddTuple(
								PersistentFsObjType_RelationDir,
								values,
								flushToXLog,
								&relationDirEntry->persistentTid,
								&relationDirEntry->persistentSerialNum);

	*persistentTid = relationDirEntry->persistentTid;
	*persistentSerialNum = relationDirEntry->persistentSerialNum;

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
	{
		elog(Persistent_DebugPrintLevel(),
			"Persistent relation: Add '%s', in state 'Created', serial number " INT64_FORMAT " at TID %s",
			PersistentFileSysObjName_ObjectName(&fsObjName),
			*persistentSerialNum,
			ItemPointerToString(persistentTid));
	}
}

void PersistentRelation_Created(
		PersistentFileSysObjName *fsObjName,
		ItemPointer persistentTid,
		int64 persistentSerialNum,
		bool retryPossible)
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	RelFileNode *relFileNode = &fsObjName->variant.rel.relFileNode;

	RelationDirEntry relationDirEntry;

	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (RelFileNode_IsEmpty(relFileNode))
	{
		elog(ERROR, "Invalid RelFileNode (0,0,0)");
	}

	if (Persistent_BeforePersistenceWork())
	{
		if (Debug_persistent_print)
		{
			elog(Persistent_DebugPrintLevel(),
				"Skipping persistent relation '%s' because we are before persistence work",
				relpath(*relFileNode));
		}

		/*
		 * The initdb process will load the persistent table once we out of bootstrap mode.
		 */
		return;
	}

	PersistentRelation_VerifyInitScan();

	/*
	 * Do this check after skipping out if in bootstrap mode.
	 */
	if (PersistentStore_IsZeroTid(persistentTid))
	{
		elog(ERROR, "TID for persistent '%s' tuple for 'Created is invalid (0,0)",
			PersistentFileSysObjName_TypeAndObjectName(fsObjName));
	}

	if (persistentSerialNum == 0)
	{
		elog(ERROR, "Persistent '%s' serial number for 'Created' is invalid (0)",
			PersistentFileSysObjName_TypeAndObjectName(fsObjName));
	}

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	relationDirEntry = PersistentRelation_FindEntryUnderLock(relFileNode);

	if (relationDirEntry == NULL)
	{
		elog(ERROR, "Did not find persistent relation entry %u/%u/%u",
				relFileNode->spcNode,
				relFileNode->dbNode,
				relFileNode->relNode);
	}

	if (relationDirEntry->state != PersistentFileSysState_CreatePending)
	{
		elog(ERROR, "Persistent relation entry %u/%u/%u expected to be in 'Create Pending' state (actual state '%s')",
				relFileNode->spcNode,
				relFileNode->dbNode,
				relFileNode->relNode,
				PersistentFileSysObjState_Name(relationDirEntry->state));
	}

	stateChangeResult = PersistentFileSysObj_StateChange(
											fsObjName,
											persistentTid,
											persistentSerialNum,
											PersistentFileSysState_Created,
											retryPossible,
											/* flushToXLog */ false,
											/* oldState */ NULL,
											/* verifiedActionCallback */ NULL);

	relationDirEntry->state = PersistentFileSysState_Created;

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
	{
		elog(Persistent_DebugPrintLevel(),
			"Persistent relation: '%s' changed state from 'Create Pending' to 'Created', serial number " INT64_FORMAT "at TID %s (State-Change result '%s')",
			PersistentFileSysObjName_ObjectName(fsObjName),
			persistentSerialNum,
			ItemPointerToString(persistentTid),
			PersistentFileSysObjStateChangeResult_Name(stateChangeResult));
	}
	return ;
}

PersistentFileSysObjStateChangeResult PersistentRelation_MarkDropPending(
		PersistentFileSysObjName *fsObjName,
		ItemPointer persistentTid,
		int64 persistentSerialNum,
		bool retryPossible)
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	RelFileNode *relFileNode = &fsObjName->variant.rel.relFileNode;

	RelationDirEntry relationDirEntry;

	PersistentFileSysState oldState;

	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (RelFileNode_IsEmpty(relFileNode))
	{
		elog(ERROR, "Invalid RelFileNode (0,0,0)");
	}

	if (Persistent_BeforePersistenceWork())
	{
		if (Debug_persistent_print)
		{
			elog(Persistent_DebugPrintLevel(),
				"Skipping persistent relation '%s' because we are before persistence work",
				relpath(*relFileNode));

			/*
			 * The initdb process will load the persistent table once we out of bootstrap mode.
			 */
			return PersistentFileSysObjStateChangeResult_None;
		}
	}

	PersistentRelation_VerifyInitScan();

	/*
	 * Do this check after skipping out if in bootstrap mode.
	 */
	if (PersistentStore_IsZeroTid(persistentTid))
	{
		elog(ERROR, "TID for persisent '%s' tuple for mark DROP pending is invalid (0,0)",
			PersistentFileSysObjName_TypeAndObjectName(fsObjName));
	}

	if (persistentSerialNum == 0)
	{
		elog(ERROR, "Persistent '%s' serial number for mark DROP pending is valid (0)",
			PersistentFileSysObjName_TypeAndObjectName(fsObjName));
	}

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	relationDirEntry = PersistentRelation_FindEntryUnderLock(relFileNode);

	if (relationDirEntry == NULL)
	{
		elog(ERROR, "Did not find persistent relation entry %u/%u/%u",
				relFileNode->spcNode,
				relFileNode->dbNode,
				relFileNode->relNode);
	}

	if ((relationDirEntry->state != PersistentFileSysState_CreatePending) &&
		(relationDirEntry->state != PersistentFileSysState_Created))
	{
		elog(ERROR, "Persistent relation entry %u/%u/%u expected to be in 'Create Pending' or 'Created' state (actual state '%s')",
				relFileNode->spcNode,
				relFileNode->dbNode,
				relFileNode->relNode,
				PersistentFileSysObjState_Name(relationDirEntry->state));
	}

	stateChangeResult = PersistentFileSysObj_StateChange(
											fsObjName,
											persistentTid,
											persistentSerialNum,
											PersistentFileSysState_DropPending,
											retryPossible,
											/* flushToXLog */ false,
											&oldState,
											/* verfifiedActionCallback */ NULL);

	relationDirEntry->state = PersistentFileSysState_DropPending;

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
	{
		elog(Persistent_DebugPrintLevel(),
			"Persistent relation: '%s' changed state from '%s' to 'Drop Pending', serial number " INT64_FORMAT " TID %s (State-Change result '%s')",
			PersistentFileSysObjName_ObjectName(fsObjName),
			PersistentFileSysObjState_Name(oldState),
			persistentSerialNum,
			ItemPointerToString(persistentTid),
			PersistentFileSysObjStateChangeResult_Name(stateChangeResult));
	}

	return stateChangeResult;
}

PersistentFileSysObjStateChangeResult PersistentRelation_MarkAbortingCreate(
		PersistentFileSysObjName *fsObjName,
		ItemPointer persistentTid,
		int64 persistentSerialNum,
		bool retryPossible)
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	RelFileNode *relFileNode = &fsObjName->variant.rel.relFileNode;

	RelationDirEntry relationDirEntry;

	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (RelFileNode_IsEmpty(relFileNode))
	{
		elog(ERROR, "Invalid RelFileNode (0,0,0)");
	}

	if (Persistent_BeforePersistenceWork())
	{
		if (Debug_persistent_print)
		{
			elog(Persistent_DebugPrintLevel(),
				"Skipping persistent relation '%s' because we are before persistence work",
				relpath(*relFileNode));

			/*
			 * The initdb process will load the persistent table once we out of bootstrap mode.
			 */
			return PersistentFileSysObjStateChangeResult_None;
		}
	}

	PersistentRelation_VerifyInitScan();

	/*
	 * Do this check after skipping out if in bootstrap mode.
	 */
	if (PersistentStore_IsZeroTid(persistentTid))
	{
		elog(ERROR, "TID for persistent '%s' tuple for mark DROP pending is invalid (0,0)",
			PersistentFileSysObjName_TypeAndObjectName(fsObjName));
	}

	if (persistentSerialNum == 0)
	{
		elog(ERROR, "Persistent '%s' serial number for mark DROP pending is invalid (0)",
			PersistentFileSysObjName_TypeAndObjectName(fsObjName));
	}

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	relationDirEntry = PersistentRelation_FindEntryUnderLock(relFileNode);

	if (relationDirEntry == NULL)
	{
		elog(ERROR, "Did not find persistent relation entry %u/%u/%u",
			relFileNode->spcNode,
			relFileNode->dbNode,
			relFileNode->relNode);
	}

	if (relationDirEntry->state != PersistentFileSysState_CreatePending)
	{
		elog(ERROR, "Persistent relation entry %u/%u/%u expected to be in 'Create Pending' (actual state '%s')",
			relFileNode->spcNode,
			relFileNode->dbNode,
			relFileNode->relNode,
			PersistentFileSysObjState_Name(relationDirEntry->state));
	}

	stateChangeResult = PersistentFileSysObj_StateChange(
											fsObjName,
											persistentTid,
											persistentSerialNum,
											PersistentFileSysState_AbortingCreate,
											retryPossible,
											/* flushToXLog */ false,
											/* oldState */ NULL,
											/* verifiedActionCallback */ NULL);

	relationDirEntry->state = PersistentFileSysState_AbortingCreate;

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
	{
		elog(Persistent_DebugPrintLevel(),
			"Persistent relation: '%s' changed state from 'Create Pending' to 'Aborting Create', serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			PersistentFileSysObjName_ObjectName(fsObjName),
			persistentSerialNum,
			ItemPointerToString(persistentTid),
			PersistentFileSysObjStateChangeResult_Name(stateChangeResult));
	}

	return stateChangeResult;
}

static void
PersistentRelation_DroppedVerifiedActionCallback(
		PersistentFileSysObjName *fsObjName,
		ItemPointer persistentTid,
		int64 persistentSerialNum,
		PersistentFileSysObjVerifyExpectedResult verifyExpectedResult)
{
	RelFileNode *relFileNode = &fsObjName->variant.rel.relFileNode;

	switch(verifyExpectedResult)
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
		mmxlog_log_remove_relation(relFileNode->spcNode, relFileNode->dbNode, relFileNode->relNode,
				persistentTid, persistentSerialNum);
#endif
		break;

	default:
		elog(ERROR, "Unexpected persistent object verify expected result: %d",
			verifyExpectedResult);
	}
}

void PersistentRelation_Dropped(
		PersistentFileSysObjName *fsObjName,
		ItemPointer persistentTid,
		int64 persistentSerialNum)
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	RelFileNode *relFileNode = &fsObjName->variant.rel.relFileNode;

	RelationDirEntry relationDirEntry;

	PersistentFileSysState oldState;

	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (RelFileNode_IsEmpty(relFileNode))
	{
		elog(ERROR, "Invalid RelFileNode (0,0,0)");
	}

	if (Persistent_BeforePersistenceWork())
	{
		if (Debug_persistent_print)
		{
			elog(Persistent_DebugPrintLevel(),
				"Skipping persistent relation '%s' because we are before persistence work",
				relpath(*relFileNode));
		}

		/*
		 * The initdb process will load the persistent table once we out of bootstrap mode.
		 */
	}

	PersistentRelation_VerifyInitScan();

	/*
	 * Do this check after skipping out if in bootstrap mode.
	 */
	if (PersistentStore_IsZeroTid(persistentTid))
	{
		elog(ERROR, "TID for persistent '%s' tuple for mark DROP pending is invalid (0,0)",
			PersistentFileSysObjName_TypeAndObjectName(fsObjName));
	}

	if (persistentSerialNum == 0)
	{
		elog(ERROR, "Persistent '%s' serial number for mark DROP pending is invalid (0)",
			PersistentFileSysObjName_TypeAndObjectName(fsObjName));
	}

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	relationDirEntry = PersistentRelation_FindEntryUnderLock(relFileNode);

	if (relationDirEntry == NULL)
	{
		elog(ERROR, "Did not find persistent relation entry %u/%u/%u",
				relFileNode->spcNode,
				relFileNode->dbNode,
				relFileNode->relNode);
	}

	if ((relationDirEntry->state != PersistentFileSysState_DropPending) &&
		(relationDirEntry->state != PersistentFileSysState_AbortingCreate))
	{
		elog(ERROR, "Persistent relation entry %u/%u/%u expected to be in 'Drop Pending' or 'Aborting Create' (actual state '%s')",
				relFileNode->spcNode,
				relFileNode->dbNode,
				relFileNode->relNode,
				PersistentFileSysObjState_Name(relationDirEntry->state));
	}
	stateChangeResult = PersistentFileSysObj_StateChange(
											fsObjName,
											persistentTid,
											persistentSerialNum,
											PersistentFileSysState_Free,
											/* retryPossible */ false,
											/* flushToXLog */ false,
											&oldState,
											PersistentRelation_DroppedVerifiedActionCallback);

	relationDirEntry->state = PersistentFileSysState_Free;

	PersistentRelation_RemoveEntryUnderLock(relationDirEntry);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
	{
		elog(Persistent_DebugPrintLevel(),
			"Persistent relation: '%s' changed state from '%s' to (Free), serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			PersistentFileSysObjName_ObjectName(fsObjName),
			PersistentFileSysObjState_Name(oldState),
			persistentSerialNum,
			ItemPointerToString(persistentTid),
			PersistentFileSysObjStateChangeResult_Name(stateChangeResult));
	}
}
