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
 * cdbpersistenttablespace.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"

#include "access/persistentfilesysobjname.h"
#include "access/xlogmm.h"
#include "catalog/catalog.h"
#include "catalog/gp_persistent.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_filespace.h"
#include "cdb/cdbsharedoidsearch.h"
#include "cdb/cdbdirectopen.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbpersistentfilespace.h"
#include "cdb/cdbpersistenttablespace.h"
#include "cdb/cdbdispatchedtablespaceinfo.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "postmaster/postmaster.h"
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
#include "commands/filespace.h"
#include "commands/tablespace.h"

typedef struct PersistentTablespaceSharedData
{
	
	PersistentFileSysObjSharedData		fileSysObjSharedData;

} PersistentTablespaceSharedData;

#define PersistentTablespaceData_StaticInit {PersistentFileSysObjData_StaticInit}

typedef struct PersistentTablespaceData
{

	PersistentFileSysObjData		fileSysObjData;

} PersistentTablespaceData;

typedef struct TablespaceDirEntryKey
{
	Oid	tablespaceOid;
} TablespaceDirEntryKey;

typedef struct TablespaceDirEntryData
{
	TablespaceDirEntryKey	key;

	Oid						filespaceOid;

	PersistentFileSysState	state;
	int64					persistentSerialNum;
	ItemPointerData 		persistentTid;
	
} TablespaceDirEntryData;
typedef TablespaceDirEntryData *TablespaceDirEntry;


/*
 * Global Variables
 */
PersistentTablespaceSharedData	*persistentTablespaceSharedData = NULL;
static HTAB *persistentTablespaceSharedHashTable = NULL;

PersistentTablespaceData	persistentTablespaceData = PersistentTablespaceData_StaticInit;

static void PersistentTablespace_VerifyInitScan(void)
{
	if (persistentTablespaceSharedData == NULL)
		elog(PANIC, "Persistent tablespace information shared-memory not setup");

	PersistentFileSysObj_VerifyInitScan();
}

/*
 * Return the hash entry for a tablespace.
 */
static TablespaceDirEntry
PersistentTablespace_FindEntryUnderLock(
	Oid			tablespaceOid)
{
	bool			found;

	TablespaceDirEntry	tablespaceDirEntry;

	TablespaceDirEntryKey key;

	elog(DEBUG1, "PersistentTablespace_FindEntryUnderLock: tablespace %d", tablespaceOid);

	if (persistentTablespaceSharedHashTable == NULL)
		elog(PANIC, "Persistent tablespace information shared-memory not setup");

	key.tablespaceOid = tablespaceOid;

	tablespaceDirEntry = 
			(TablespaceDirEntry) 
					hash_search(persistentTablespaceSharedHashTable,
								(void *) &key,
								HASH_FIND,
								&found);
	if (!found)
		return NULL;

	return tablespaceDirEntry;
}

static TablespaceDirEntry
PersistentTablespace_CreateEntryUnderLock(
	Oid			filespaceOid,
	Oid 		tablespaceOid)
{
	bool			found;

	TablespaceDirEntry	tablespaceDirEntry;

	TablespaceDirEntryKey key;

	elog(DEBUG1, "PersistentTablespace_CreateEntryUnderLock: tablespace %d", tablespaceOid);

	if (persistentTablespaceSharedHashTable == NULL)
		elog(PANIC, "Persistent tablespace information shared-memory not setup");

	key.tablespaceOid = tablespaceOid;

	tablespaceDirEntry = 
			(TablespaceDirEntry) 
					hash_search(persistentTablespaceSharedHashTable,
								(void *) &key,
								HASH_ENTER_NULL,
								&found);

	if (tablespaceDirEntry == NULL)
		return NULL;

	tablespaceDirEntry->filespaceOid = filespaceOid;
	
	tablespaceDirEntry->state = 0;
	tablespaceDirEntry->persistentSerialNum = 0;
	MemSet(&tablespaceDirEntry->persistentTid, 0, sizeof(ItemPointerData));
	
	return tablespaceDirEntry;
}

static void
PersistentTablespace_RemoveEntryUnderLock(
	TablespaceDirEntry	tablespaceDirEntry)
{
	TablespaceDirEntry	removeTablespaceDirEntry;

	if (persistentTablespaceSharedHashTable == NULL)
		elog(PANIC, "Persistent tablespace information shared-memory not setup");

	removeTablespaceDirEntry = 
				(TablespaceDirEntry) 
						hash_search(persistentTablespaceSharedHashTable,
									(void *) &tablespaceDirEntry->key,
									HASH_REMOVE,
									NULL);

	if (removeTablespaceDirEntry == NULL)
		elog(ERROR, "Trying to delete entry that does not exist");
}

PersistentFileSysState
PersistentTablespace_GetState(
	Oid		tablespaceOid)
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	TablespaceDirEntry tablespaceDirEntry;

	PersistentFileSysState	state;

	/*
	 * pg_default and pg_global always exist, but do not have entries in
	 * gp_persistent_tablespace_node.
	 */
	if (tablespaceOid == DEFAULTTABLESPACE_OID ||
		tablespaceOid == GLOBALTABLESPACE_OID) 
	{ 
		return PersistentFileSysState_Created; 
	}

	PersistentTablespace_VerifyInitScan();

	// NOTE: Since we are not accessing data in the Buffer Pool, we don't need to
	// acquire the MirroredLock.

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	tablespaceDirEntry = 
				PersistentTablespace_FindEntryUnderLock(
												tablespaceOid);
	if (tablespaceDirEntry == NULL)
		elog(ERROR, "Did not find persistent tablespace entry %u", 
			 tablespaceOid);

	state = tablespaceDirEntry->state;

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;

	return state;
}

// -----------------------------------------------------------------------------
// Scan 
// -----------------------------------------------------------------------------

static bool PersistentTablespace_ScanTupleCallback(
	ItemPointer 			persistentTid,
	int64					persistentSerialNum,
	Datum					*values)
{
	Oid		filespaceOid;
	Oid		tablespaceOid;
	
	PersistentFileSysState	state;
	int32					reserved;
	TransactionId			parentXid;
	int64					serialNum;
	ItemPointerData			previousFreeTid;
	
	TablespaceDirEntry tablespaceDirEntry;
	bool					sharedStorage;

	GpPersistentTablespaceNode_GetValues(
									values,
									&filespaceOid,
									&tablespaceOid,
									&state,
									&reserved,
									&parentXid,
									&serialNum,
									&previousFreeTid,
									&sharedStorage);

	if (state == PersistentFileSysState_Free)
	{
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "PersistentTablespace_ScanTupleCallback: TID %s, serial number " INT64_FORMAT " is free",
				 ItemPointerToString2(persistentTid),
				 persistentSerialNum);
		return true;	// Continue.
	}
	
	tablespaceDirEntry = 
		PersistentTablespace_CreateEntryUnderLock(filespaceOid, tablespaceOid);

	if (tablespaceDirEntry == NULL)
		elog(ERROR, "Out of shared-memory for persistent tablespaces");

	tablespaceDirEntry->state = state;
	tablespaceDirEntry->persistentSerialNum = serialNum;
	tablespaceDirEntry->persistentTid = *persistentTid;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "PersistentTablespace_ScanTupleCallback: tablespace %u, filespace %u, state %s, TID %s, serial number " INT64_FORMAT,
			 tablespaceOid,
			 filespaceOid,
			 PersistentFileSysObjState_Name(state),
			 ItemPointerToString2(persistentTid),
			 persistentSerialNum);

	return true;	// Continue.
}

//------------------------------------------------------------------------------

void PersistentTablespace_Reset(void)
{
	HASH_SEQ_STATUS stat;

	TablespaceDirEntry tablespaceDirEntry;

	hash_seq_init(&stat, persistentTablespaceSharedHashTable);

	while (true)
	{
		TablespaceDirEntry removeTablespaceDirEntry;

		PersistentFileSysObjName fsObjName;
		
		tablespaceDirEntry = hash_seq_search(&stat);
		if (tablespaceDirEntry == NULL)
			break;

		/* TODO: when will this code be executed? */
		PersistentFileSysObjName_SetTablespaceDir(
										&fsObjName,
										tablespaceDirEntry->key.tablespaceOid,
										is_tablespace_shared);

		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "Persistent tablespace directory: Resetting '%s' serial number " INT64_FORMAT " at TID %s",
				 PersistentFileSysObjName_ObjectName(&fsObjName),
				 tablespaceDirEntry->persistentSerialNum,
				 ItemPointerToString(&tablespaceDirEntry->persistentTid));

		removeTablespaceDirEntry = 
					(TablespaceDirEntry) 
							hash_search(persistentTablespaceSharedHashTable,
										(void *) &tablespaceDirEntry->key,
										HASH_REMOVE,
										NULL);
		
		if (removeTablespaceDirEntry == NULL)
			elog(ERROR, "Trying to delete entry that does not exist");
	}
}

extern void PersistentTablespace_LookupTidAndSerialNum(
	Oid 		tablespaceOid,
				/* The tablespace OID for the lookup. */

	ItemPointer		persistentTid,
				/* TID of the gp_persistent_tablespace_node tuple for the rel file */

	int64			*persistentSerialNum)
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	TablespaceDirEntry tablespaceDirEntry;

	PersistentTablespace_VerifyInitScan();

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	tablespaceDirEntry = 
				PersistentTablespace_FindEntryUnderLock(
												tablespaceOid);
	if (tablespaceDirEntry == NULL)
		elog(ERROR, "Did not find persistent tablespace entry %u", 
			 tablespaceOid);

	*persistentTid = tablespaceDirEntry->persistentTid;
	*persistentSerialNum = tablespaceDirEntry->persistentSerialNum;

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;
}

// -----------------------------------------------------------------------------
// Helpers 
// -----------------------------------------------------------------------------

static void PersistentTablespace_AddTuple(
	TablespaceDirEntry tablespaceDirEntry,
	TransactionId 	parentXid,
	bool			flushToXLog)
{
	Oid filespaceOid = tablespaceDirEntry->filespaceOid;
	Oid tablespaceOid = tablespaceDirEntry->key.tablespaceOid;

	ItemPointerData previousFreeTid;

	Datum values[Natts_gp_persistent_tablespace_node];

	MemSet(&previousFreeTid, 0, sizeof(ItemPointerData));

	GpPersistentTablespaceNode_SetDatumValues(
								values,
								filespaceOid,
								tablespaceOid,
								tablespaceDirEntry->state,
								parentXid,
								/* persistentSerialNum */ 0,	// This will be set by PersistentFileSysObj_AddTuple.
								&previousFreeTid,
								is_tablespace_shared(tablespaceOid));

	PersistentFileSysObj_AddTuple(
							PersistentFsObjType_TablespaceDir,
							values,
							flushToXLog,
							&tablespaceDirEntry->persistentTid,
							&tablespaceDirEntry->persistentSerialNum);
}

RC4PersistentTablespaceGetFilespaces PersistentTablespace_TryGetFilespacePath(
	Oid 		tablespaceOid,
	char **filespaceLocation,
	Oid *filespaceOid)
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	TablespaceDirEntry tablespaceDirEntry;

	RC4PersistentTablespaceGetFilespaces result;

	*filespaceLocation = NULL;
	*filespaceOid = InvalidOid;

	if (IsBuiltinTablespace(tablespaceOid))
	{
		/*
		 * Optimize out the common cases.
		 */
		 return RC4PersistentTablespaceGetFilespaces_Ok;
	}

#ifdef MASTER_MIRROR_SYNC
	/*
	 * Can't rely on persistent tables or memory structures on the standby so
	 * get it from the cache maintained by the master mirror sync code
	 */
	if (GPStandby())
	{
		/*
		 * Standby can not access the shared storage, so the contentid must be
		 * MASTER_CONTENT_ID.
		 */
		if (!mmxlog_tablespace_get_filespace(
									tablespaceOid,
									filespaceOid))
		{
			if (!Debug_persistent_recovery_print)
			{
				// Print this information when we are not doing other tracing.
				mmxlog_print_tablespaces(
									LOG,
									"Standby Get Filespace for Tablespace");
			}
			return RC4PersistentTablespaceGetFilespaces_TablespaceNotFound;
		}

		if (!mmxlog_filespace_get_path(
									*filespaceOid,
									filespaceLocation))
		{
			if (!Debug_persistent_recovery_print)
			{
				// Print this information when we are not doing other tracing.
				mmxlog_print_filespaces(
									LOG,
									"Standby Get Filespace Location");
			}
			return RC4PersistentTablespaceGetFilespaces_FilespaceNotFound;
		}

		return RC4PersistentTablespaceGetFilespaces_Ok;
	}
#endif

	/*
	 * MPP-10111 - There is a point during gpexpand where we need to bring
	 * the database up to fix the filespace locations for a segment.  At
	 * this point in time the old filespace locations are wrong and we should
	 * not trust anything currently stored there.  If the guc is set we
	 * prevent the lookup of a any non builtin filespaces.
	 */
	if (gp_before_filespace_setup)
		elog(ERROR, "can not lookup tablespace location: gp_before_filespace_setup=true");

	/*
	 * Important to make this call AFTER we check if we are the Standby Master.
	 */
	PersistentTablespace_VerifyInitScan();

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	tablespaceDirEntry =
		PersistentTablespace_FindEntryUnderLock(
										tablespaceOid);
	if (tablespaceDirEntry == NULL)
	{
		result = RC4PersistentTablespaceGetFilespaces_TablespaceNotFound;
	}
	else
	{
		*filespaceOid = tablespaceDirEntry->filespaceOid;

		if (!PersistentFilespace_TryGetFilespacePathUnderLock(
													tablespaceDirEntry->filespaceOid,
													filespaceLocation))
		{
			result = RC4PersistentTablespaceGetFilespaces_FilespaceNotFound;			
		}
		else
		{
			result = RC4PersistentTablespaceGetFilespaces_Ok;
		}
	}

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;

	return result;
}

void PersistentTablespace_GetFilespacePath(
	Oid 		tablespaceOid,
	bool 		needDispatchedTablespaceInfo,
	char		**filespaceLocation)
{
	RC4PersistentTablespaceGetFilespaces tablespaceGetFilespaces;

	Oid filespaceOid;

	/*
	 * Do not call PersistentTablepace_VerifyInitScan here to allow 
	 * PersistentTablespace_TryGetFilespacePath to handle the Standby Master
	 * special case.
	 */


	if (Gp_role != GP_ROLE_EXECUTE || IsBootstrapProcessingMode() || !needDispatchedTablespaceInfo)
	{
		tablespaceGetFilespaces =
				PersistentTablespace_TryGetFilespacePath(
														tablespaceOid,
														filespaceLocation,
														&filespaceOid);
	}
	else
	{
		bool found;
		DispatchedFilespace_GetPathForTablespace(
				tablespaceOid, filespaceLocation, &found);
		if (!found)
			tablespaceGetFilespaces = RC4PersistentTablespaceGetFilespaces_TablespaceNotFound;
		else
			tablespaceGetFilespaces = RC4PersistentTablespaceGetFilespaces_Ok;
	}

	switch (tablespaceGetFilespaces)
	{
	case RC4PersistentTablespaceGetFilespaces_TablespaceNotFound:
		ereport(ERROR, 
				(errcode(ERRCODE_CDB_INTERNAL_ERROR),
				 errmsg("Unable to find entry for tablespace OID = %u when getting filespace directory paths",
						tablespaceOid)));
		break;
			
	case RC4PersistentTablespaceGetFilespaces_FilespaceNotFound:
		ereport(ERROR, 
				(errcode(ERRCODE_CDB_INTERNAL_ERROR),
				 errmsg("Unable to find entry for filespace OID = %u when forming filespace directory paths for tablespace OID = %u",
				 		filespaceOid,
						tablespaceOid)));
		break;
					
	case RC4PersistentTablespaceGetFilespaces_Ok:
		// Go below and pass back the result.
		break;
		
	default:
		elog(ERROR, "Unexpected tablespace filespace fetch result: %d",
			 tablespaceGetFilespaces);
	}
}

Oid
PersistentTablespace_GetFileSpaceOid(Oid tablespaceOid)
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	TablespaceDirEntry tablespaceDirEntry;
	Oid filespace = InvalidOid;

	if (tablespaceOid == GLOBALTABLESPACE_OID ||
		tablespaceOid == DEFAULTTABLESPACE_OID)
	{
		/*
		 * Optimize out the common cases.
		 */
		 return SYSTEMFILESPACE_OID;
	}

	PersistentTablespace_VerifyInitScan();

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	tablespaceDirEntry =
		PersistentTablespace_FindEntryUnderLock(
										tablespaceOid);
	if (tablespaceDirEntry == NULL)
		elog(ERROR, "Did not find persistent tablespace entry %u", 
			 tablespaceOid);

	filespace = tablespaceDirEntry->filespaceOid;

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;

	return filespace;
}

// -----------------------------------------------------------------------------
// State Change 
// -----------------------------------------------------------------------------

/*
 * Indicate we intend to create a tablespace file as part of the current transaction.
 *
 * An XLOG IntentToCreate record is generated that will guard the subsequent file-system
 * create in case the transaction aborts.
 *
 * After 1 or more calls to this routine to mark intention about tablespace files that are going
 * to be created, call ~_DoPendingCreates to do the actual file-system creates.  (See its
 * note on XLOG flushing).
 */
void PersistentTablespace_MarkCreatePending(
	Oid 		filespaceOid,
				/* The filespace where the tablespace lives. */

	Oid 		tablespaceOid,
				/* The tablespace OID for the create. */

	ItemPointer		persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64			*persistentSerialNum,


	bool			flushToXLog)
				/* When true, the XLOG record for this change will be flushed to disk. */

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	PersistentFileSysObjName fsObjName;

	TablespaceDirEntry tablespaceDirEntry;

	if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "Skipping persistent tablespace %u because we are before persistence work",
				 tablespaceOid);

		return;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentTablespace_VerifyInitScan();

	PersistentFileSysObjName_SetTablespaceDir(&fsObjName,tablespaceOid,is_tablespace_shared);

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	tablespaceDirEntry = 
				PersistentTablespace_CreateEntryUnderLock(
												filespaceOid,
												tablespaceOid);
	if (tablespaceDirEntry == NULL)
	{
		/* If out of shared memory, no need to promote to PANIC. */
		WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("Out of shared-memory for persistent tablespaces"),
				 errhint("You may need to increase the gp_max_tablespaces value"),
				 errOmitLocation(true)));
	}

	tablespaceDirEntry->state = PersistentFileSysState_CreatePending;

	PersistentTablespace_AddTuple(
							tablespaceDirEntry,
							/* parentXid */ GetTopTransactionId(),
							flushToXLog);

	*persistentTid = tablespaceDirEntry->persistentTid;
	*persistentSerialNum = tablespaceDirEntry->persistentSerialNum;
		
	/*
	 * This XLOG must be generated under the persistent write-lock.
	 */
#ifdef MASTER_MIRROR_SYNC
	mmxlog_log_create_tablespace(
						filespaceOid,
						tablespaceOid,
						persistentTid, *persistentSerialNum);
#endif

	#ifdef FAULT_INJECTOR
			FaultInjector_InjectFaultIfSet(
										   FaultBeforePendingDeleteTablespaceEntry,
										   DDLNotSpecified,
										   "",  // databaseName
										   ""); // tableName
	#endif

	/*
	 * MPP-18228
	 * To make adding 'Create Pending' entry to persistent table and adding
	 * to the PendingDelete list atomic
	 */
	PendingDelete_AddCreatePendingEntryWrapper(
						&fsObjName,
						persistentTid,
						*persistentSerialNum);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

}

// -----------------------------------------------------------------------------
// Rebuild tablespace persistent table 'gp_persistent_tablespace_node'
// -----------------------------------------------------------------------------

void PersistentTablespace_AddCreated(
									  Oid 		filespaceOid,
									  /* The filespace where the tablespace lives. */
											
									  Oid 		tablespaceOid,
									  /* The tablespace OID to be added. */
											
									  bool			flushToXLog)
									  /* When true, the XLOG record for this change will be flushed to disk. */

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;
	
	PersistentFileSysObjName fsObjName;
	
	ItemPointerData		persistentTid;
	int64				persistentSerialNum;	
	TablespaceDirEntry	tablespaceDirEntry;
	
	/*if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "Skipping persistent tablespace %u because we are before persistence work",
				 tablespaceOid);
		
		return;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}*/
	
	PersistentTablespace_VerifyInitScan();
	
	PersistentFileSysObjName_SetTablespaceDir(&fsObjName,tablespaceOid,is_tablespace_shared);

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;
	
	tablespaceDirEntry = 
	PersistentTablespace_CreateEntryUnderLock(
											  filespaceOid,
											  tablespaceOid);
	if (tablespaceDirEntry == NULL)
	{
		/* If out of shared memory, no need to promote to PANIC. */
		WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("Out of shared-memory for persistent tablespaces"),
				 errhint("You may need to increase the gp_max_tablespaces value"),
				 errOmitLocation(true)));
	}
	
	tablespaceDirEntry->state = PersistentFileSysState_Created;
	
	PersistentTablespace_AddTuple(
								  tablespaceDirEntry,
								  InvalidTransactionId,
								  flushToXLog);

	persistentTid = tablespaceDirEntry->persistentTid;
	persistentSerialNum = tablespaceDirEntry->persistentSerialNum;
		
	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;
	
	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent tablespace directory: Add '%s' in state 'Created', serial number " INT64_FORMAT " at TID '%s' ",
			 PersistentFileSysObjName_ObjectName(&fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(&persistentTid));
}

// -----------------------------------------------------------------------------
// Transaction End  
// -----------------------------------------------------------------------------

/*
 * Indicate the transaction commited and the tablespace is officially created.
 */
void PersistentTablespace_Created(
	PersistentFileSysObjName *fsObjName,
				/* The tablespace OID for the create. */

	ItemPointer		persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64			persistentSerialNum,
				/* Serial number for the tablespace.	Distinquishes the uses of the tuple. */

	bool			retryPossible)

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	Oid 		tablespaceOid = fsObjName->variant.tablespaceOid;

	TablespaceDirEntry tablespaceDirEntry;

	PersistentFileSysObjStateChangeResult stateChangeResult;
	
	if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "Skipping persistent tablespace %u because we are before persistence work",
				 tablespaceOid);

		return;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentTablespace_VerifyInitScan();

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	tablespaceDirEntry = 
				PersistentTablespace_FindEntryUnderLock(
												tablespaceOid);
	if (tablespaceDirEntry == NULL)
		elog(ERROR, "Did not find persistent tablespace entry %u", 
			 tablespaceOid);

	if (tablespaceDirEntry->state != PersistentFileSysState_CreatePending)
		elog(ERROR, "Persistent tablespace entry %u expected to be in 'Create Pending' state (actual state '%s')", 
			 tablespaceOid,
			 PersistentFileSysObjState_Name(tablespaceDirEntry->state));

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

	tablespaceDirEntry->state = PersistentFileSysState_Created;

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent tablespace directory: '%s' changed state from 'Create Pending' to 'Created', serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));
}

/*
 * Indicate we intend to drop a tablespace file as part of the current transaction.
 *
 * This tablespace file to drop will be listed inside a commit, distributed commit, a distributed 
 * prepared, and distributed commit prepared XOG records.
 *
 * For any of the commit type records, once that XLOG record is flushed then the actual
 * file-system delete will occur.  The flush guarantees the action will be retried after system
 * crash.
 */
PersistentFileSysObjStateChangeResult PersistentTablespace_MarkDropPending(
	PersistentFileSysObjName *fsObjName,
				/* The tablespace OID for the drop. */

	ItemPointer		persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64			persistentSerialNum,
				/* Serial number for the tablespace.	Distinquishes the uses of the tuple. */

	bool			retryPossible)

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	Oid 		tablespaceOid = fsObjName->variant.tablespaceOid;

	TablespaceDirEntry tablespaceDirEntry;

	PersistentFileSysObjStateChangeResult stateChangeResult;
	
	if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "Skipping persistent tablespace %u because we are before persistence work",
				 tablespaceOid);

		return false;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentTablespace_VerifyInitScan();

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	tablespaceDirEntry = 
				PersistentTablespace_FindEntryUnderLock(
												tablespaceOid);
	if (tablespaceDirEntry == NULL)
		elog(ERROR, "Did not find persistent tablespace entry %u", 
			 tablespaceOid);

	if (tablespaceDirEntry->state != PersistentFileSysState_CreatePending &&
		tablespaceDirEntry->state != PersistentFileSysState_Created)
		elog(ERROR, "Persistent tablespace entry %u expected to be in 'Create Pending' or 'Created' state (actual state '%s')", 
			 tablespaceOid,
			 PersistentFileSysObjState_Name(tablespaceDirEntry->state));

	stateChangeResult =
		PersistentFileSysObj_StateChange(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								PersistentFileSysState_DropPending,
								retryPossible,
								/* flushToXlog */ false,
								/* oldState */ NULL,
								/* verifiedActionCallback */ NULL);

	tablespaceDirEntry->state = PersistentFileSysState_DropPending;
	
	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent tablespace directory: '%s' changed state from 'Create Pending' to 'Aborting Create', serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));

	return stateChangeResult;
}

/*
 * Indicate we are aborting the create of a tablespace file.
 *
 * This state will make sure the tablespace gets dropped after a system crash.
 */
PersistentFileSysObjStateChangeResult PersistentTablespace_MarkAbortingCreate(
	PersistentFileSysObjName *fsObjName,
				/* The tablespace OID for the aborting create. */
							
	ItemPointer		persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64			persistentSerialNum,
				/* Serial number for the tablespace.	Distinquishes the uses of the tuple. */

	bool			retryPossible)
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	Oid 		tablespaceOid = fsObjName->variant.tablespaceOid;

	TablespaceDirEntry tablespaceDirEntry;

	PersistentFileSysObjStateChangeResult stateChangeResult;
	
	if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "Skipping persistent tablespace %u because we are before persistence work",
				 tablespaceOid);

		return false;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentTablespace_VerifyInitScan();

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	tablespaceDirEntry = 
				PersistentTablespace_FindEntryUnderLock(
												tablespaceOid);
	if (tablespaceDirEntry == NULL)
		elog(ERROR, "Did not find persistent tablespace entry %u", 
			 tablespaceOid);

	if (tablespaceDirEntry->state != PersistentFileSysState_CreatePending)
		elog(ERROR, "Persistent tablespace entry %u expected to be in 'Create Pending' (actual state '%s')", 
			 tablespaceOid,
			 PersistentFileSysObjState_Name(tablespaceDirEntry->state));

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

	tablespaceDirEntry->state = PersistentFileSysState_AbortingCreate;
		
	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent tablespace directory: '%s' changed state from 'Create Pending' to 'Aborting Create', serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));

	return stateChangeResult;
}

static void
PersistentTablespace_DroppedVerifiedActionCallback(
	PersistentFileSysObjName 	*fsObjName,

	ItemPointer 				persistentTid,
			/* TID of the gp_persistent_rel_files tuple for the relation. */

	int64						persistentSerialNum,
			/* Serial number for the relation.	Distinquishes the uses of the tuple. */

	PersistentFileSysObjVerifyExpectedResult verifyExpectedResult)
{
	Oid tablespaceOid = PersistentFileSysObjName_GetTablespaceDir(fsObjName);

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
		mmxlog_log_remove_tablespace(tablespaceOid,persistentTid, persistentSerialNum);
#endif
				
		break;
	
	default:
		elog(ERROR, "Unexpected persistent object verify expected result: %d",
			 verifyExpectedResult);
	}
}

/*
 * Indicate we physically removed the tablespace file.
 */
void PersistentTablespace_Dropped(
	PersistentFileSysObjName *fsObjName,
				/* The tablespace OID for the dropped tablespace. */
										
	ItemPointer		persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64			persistentSerialNum)
				/* Serial number for the tablespace.	Distinquishes the uses of the tuple. */

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	Oid 		tablespaceOid = fsObjName->variant.tablespaceOid;

	TablespaceDirEntry tablespaceDirEntry;

	PersistentFileSysState oldState;

	PersistentFileSysObjStateChangeResult stateChangeResult;
	
	if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "Skipping persistent tablespace %u because we are before persistence work",
				 tablespaceOid);

		return;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentTablespace_VerifyInitScan();

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	tablespaceDirEntry = 
				PersistentTablespace_FindEntryUnderLock(
												tablespaceOid);
	if (tablespaceDirEntry == NULL)
		elog(ERROR, "Did not find persistent tablespace entry %u", 
			 tablespaceOid);

	if (tablespaceDirEntry->state != PersistentFileSysState_DropPending &&
		tablespaceDirEntry->state != PersistentFileSysState_AbortingCreate)
		elog(ERROR, "Persistent tablespace entry %u expected to be in 'Drop Pending' or 'Aborting Create' (actual state '%s')", 
			 tablespaceOid,
			 PersistentFileSysObjState_Name(tablespaceDirEntry->state));

	stateChangeResult =
		PersistentFileSysObj_StateChange(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								PersistentFileSysState_Free,
								/* retryPossible */ false,
								/* flushToXlog */ false,
								&oldState,
								PersistentTablespace_DroppedVerifiedActionCallback);

	tablespaceDirEntry->state = PersistentFileSysState_Free;

	PersistentTablespace_RemoveEntryUnderLock(tablespaceDirEntry);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent tablespace directory: '%s' changed state from '%s' to (Free), serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 PersistentFileSysObjState_Name(oldState),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));
}

// -----------------------------------------------------------------------------
// Shmem
// -----------------------------------------------------------------------------

static Size PersistentTablespace_SharedDataSize(void)
{
	return MAXALIGN(sizeof(PersistentTablespaceSharedData));
}


/*
 * Return the required shared-memory size for this module.
 */
Size PersistentTablespace_ShmemSize(void)
{
	Size		size;

	/* The hash table of persistent tablespaces */
	size = hash_estimate_size((Size)gp_max_tablespaces,
							  sizeof(TablespaceDirEntryData));

	/* The shared-memory structure. */
	size = add_size(size, PersistentTablespace_SharedDataSize());

	elog(LOG, "PersistentTablespace_ShmemSize: %zu = "
			  "gp_max_tablespaces: %d "
			  "* sizeof(TablespaceDirEntryData): %zu "
			  "+ PersistentTablespace_SharedDataSize(): %zu",
			  size,
			  gp_max_tablespaces,
			  sizeof(TablespaceDirEntryData),
			  PersistentTablespace_SharedDataSize());

	return size;
}

/*
 * PersistentTablespace_HashTableInit
 *
 * Create or find shared-memory hash table.
 */
static bool
PersistentTablespace_HashTableInit(void)
{
	HASHCTL			info;
	int				hash_flags;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(TablespaceDirEntryKey);
	info.entrysize = sizeof(TablespaceDirEntryData);
	info.hash = tag_hash;
	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	persistentTablespaceSharedHashTable = 
						ShmemInitHash("Persistent Tablespace Hash",
								   gp_max_tablespaces,
								   gp_max_tablespaces,
								   &info,
								   hash_flags);

	if (persistentTablespaceSharedHashTable == NULL)
		return false;

	return true;
}
						
/*
 * Initialize the shared-memory for this module.
 */
void PersistentTablespace_ShmemInit(void)
{
	bool found;
	bool ok;

	/* Create the shared-memory structure. */
	persistentTablespaceSharedData = 
		(PersistentTablespaceSharedData *)
						ShmemInitStruct("Persistent Tablespace Data",
										PersistentTablespace_SharedDataSize(),
										&found);

	if (!found)
	{
		PersistentFileSysObj_InitShared(
						&persistentTablespaceSharedData->fileSysObjSharedData);
	}

	/* Create or find our shared-memory hash table. */
	ok = PersistentTablespace_HashTableInit();
	if (!ok)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("Not enough shared memory for persistent tablespace hash table")));

	PersistentFileSysObj_Init(
						&persistentTablespaceData.fileSysObjData,
						&persistentTablespaceSharedData->fileSysObjSharedData,
						PersistentFsObjType_TablespaceDir,
						PersistentTablespace_ScanTupleCallback);


	Assert(persistentTablespaceSharedData != NULL);
	Assert(persistentTablespaceSharedHashTable != NULL);
}

/*
 * Pass shared data back to the caller. See add_tablespace_data() for why we do
 * it like this.
 */
#ifdef MASTER_MIRROR_SYNC /* annotation to show that this is just for mmsync */
void
get_tablespace_data(tspc_agg_state **tas, char *caller)
{
	HASH_SEQ_STATUS stat;
	TablespaceDirEntry tde;

	int maxCount;

	Assert(*tas == NULL);

	mmxlog_add_tablespace_init(tas, &maxCount);

	hash_seq_init(&stat, persistentTablespaceSharedHashTable);

	while ((tde = hash_seq_search(&stat)) != NULL)
	{
		mmxlog_add_tablespace(
				 tas, &maxCount, 
				 tde->filespaceOid, tde->key.tablespaceOid,
				 caller);
	}

}
#endif
