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
 * cdbpersistentfilespace.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/persistentfilesysobjname.h"
#include "access/transam.h"
#include "access/xlogmm.h"
#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_filespace.h"
#include "catalog/pg_filespace_entry.h"
#include "catalog/pg_database.h"
#include "catalog/gp_persistent.h"
#include "catalog/gp_segment_config.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "postmaster/postmaster.h"
#include "postmaster/primary_mirror_mode.h"
#include "storage/ipc.h"
#include "storage/itemptr.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/faultinjector.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbsharedoidsearch.h"
#include "cdb/cdbdirectopen.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbpersistentfilespace.h"
#include "commands/filespace.h"

typedef struct PersistentFilespaceSharedData
{

	PersistentFileSysObjSharedData		fileSysObjSharedData;

} PersistentFilespaceSharedData;

#define PersistentFilespaceData_StaticInit {PersistentFileSysObjData_StaticInit}

typedef struct PersistentFilespaceData
{

	PersistentFileSysObjData		fileSysObjData;

} PersistentFilespaceData;

typedef struct FilespaceDirEntryKey
{
	Oid	filespaceOid;
} FilespaceDirEntryKey;

typedef struct FilespaceDirEntryData
{
	FilespaceDirEntryKey	key;

	char	locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen];

	PersistentFileSysState	state;

	int64					persistentSerialNum;
	ItemPointerData 		persistentTid;
} FilespaceDirEntryData;
typedef FilespaceDirEntryData *FilespaceDirEntry;


/*
 * Global Variables
 */
PersistentFilespaceSharedData	*persistentFilespaceSharedData = NULL;
static HTAB *persistentFilespaceSharedHashTable = NULL;

PersistentFilespaceData	persistentFilespaceData = PersistentFilespaceData_StaticInit;

static void PersistentFilespace_VerifyInitScan(void)
{
	if (persistentFilespaceSharedData == NULL)
		elog(PANIC, "Persistent filespace information shared-memory not setup");

	PersistentFileSysObj_VerifyInitScan();
}

/*
 * Return the hash entry for a filespace.
 */
static FilespaceDirEntry
PersistentFilespace_FindDirUnderLock(
	Oid			filespaceOid)
{
	bool			found;

	FilespaceDirEntry	filespaceDirEntry;

	FilespaceDirEntryKey key;

	elog(DEBUG1, "PersistentFilespace_FindDirUnderLock: filespace %d", filespaceOid);

	if (persistentFilespaceSharedHashTable == NULL)
		elog(PANIC, "Persistent filespace information shared-memory not setup");

	key.filespaceOid = filespaceOid;

	filespaceDirEntry =
			(FilespaceDirEntry)
					hash_search(persistentFilespaceSharedHashTable,
								(void *) &key,
								HASH_FIND,
								&found);
	if (!found)
		return NULL;

	return filespaceDirEntry;
}

static FilespaceDirEntry
PersistentFilespace_CreateDirUnderLock(
	Oid			filespaceOid)
{
	bool			found;

	FilespaceDirEntry	filespaceDirEntry;

	FilespaceDirEntryKey key;

	elog(DEBUG1, "PersistentFilespace_CreateDirUnderLock: filespace %d", filespaceOid);

	if (persistentFilespaceSharedHashTable == NULL)
		elog(PANIC, "Persistent filespace information shared-memory not setup");

	key.filespaceOid = filespaceOid;

	filespaceDirEntry =
			(FilespaceDirEntry)
					hash_search(persistentFilespaceSharedHashTable,
								(void *) &key,
								HASH_ENTER_NULL,
								&found);

	if (filespaceDirEntry == NULL)
		return NULL;

	filespaceDirEntry->state = 0;
	filespaceDirEntry->persistentSerialNum = 0;
	MemSet(&filespaceDirEntry->persistentTid, 0, sizeof(ItemPointerData));
    memset(filespaceDirEntry->locationBlankPadded1, ' ', FilespaceLocationBlankPaddedWithNullTermLen);
    filespaceDirEntry->locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen-1]='\0';

	return filespaceDirEntry;
}

static void
PersistentFilespace_RemoveDirUnderLock(
	FilespaceDirEntry	filespaceDirEntry)
{
	FilespaceDirEntry	removeFilespaceDirEntry;

	if (persistentFilespaceSharedHashTable == NULL)
		elog(PANIC, "Persistent filespace information shared-memory not setup");

	removeFilespaceDirEntry =
				(FilespaceDirEntry)
						hash_search(persistentFilespaceSharedHashTable,
									(void *) &filespaceDirEntry->key,
									HASH_REMOVE,
									NULL);

	if (removeFilespaceDirEntry == NULL)
		elog(ERROR, "Trying to delete entry that does not exist");
}


// -----------------------------------------------------------------------------
// Scan
// -----------------------------------------------------------------------------

static bool PersistentFilespace_ScanTupleCallback(
	ItemPointer 			persistentTid,
	int64					persistentSerialNum,
	Datum					*values)
{
	Oid		filespaceOid;

	int16	dbId1;
	char	locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen];

	PersistentFileSysState	state;
	int32					reserved;
	TransactionId			parentXid;
	int64					serialNum;
	ItemPointerData			previousFreeTid;

	FilespaceDirEntry filespaceDirEntry;
	bool					sharedStorage;

	GpPersistentFilespaceNode_GetValues(
									values,
									&filespaceOid,
									&dbId1,
									locationBlankPadded1,
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
				 "PersistentFilespace_ScanTupleCallback: TID %s, serial number " INT64_FORMAT " is free",
				 ItemPointerToString2(persistentTid),
				 persistentSerialNum);
		return true;	// Continue.
	}

	filespaceDirEntry =
			PersistentFilespace_CreateDirUnderLock(
											filespaceOid);

	if (filespaceDirEntry == NULL)
		elog(ERROR, "Out of shared-memory for persistent filespaces");

	memcpy(filespaceDirEntry->locationBlankPadded1, locationBlankPadded1, FilespaceLocationBlankPaddedWithNullTermLen);

	filespaceDirEntry->state = state;
	filespaceDirEntry->persistentSerialNum = serialNum;
	filespaceDirEntry->persistentTid = *persistentTid;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(),
			 "PersistentFilespace_ScanTupleCallback: filespace %u, dbId1 %d, state '%s', TID %s, serial number " INT64_FORMAT,
			 filespaceOid,
			 dbId1,
			 PersistentFileSysObjState_Name(state),
			 ItemPointerToString2(persistentTid),
			 persistentSerialNum);

	return true;	// Continue.
}

//------------------------------------------------------------------------------

void PersistentFilespace_Reset(void)
{
	HASH_SEQ_STATUS stat;

	FilespaceDirEntry filespaceDirEntry;

	hash_seq_init(&stat, persistentFilespaceSharedHashTable);

	while (true)
	{
		FilespaceDirEntry removeFilespaceDirEntry;

		PersistentFileSysObjName fsObjName;

		filespaceDirEntry = hash_seq_search(&stat);
		if (filespaceDirEntry == NULL)
			break;

		PersistentFileSysObjName_SetFilespaceDir(
										&fsObjName,
										filespaceDirEntry->key.filespaceOid,
										is_filespace_shared);

		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(),
				 "Persistent filespace directory: Resetting '%s' serial number " INT64_FORMAT " at TID %s",
				 PersistentFileSysObjName_ObjectName(&fsObjName),
				 filespaceDirEntry->persistentSerialNum,
				 ItemPointerToString(&filespaceDirEntry->persistentTid));

		removeFilespaceDirEntry =
					(FilespaceDirEntry)
							hash_search(persistentFilespaceSharedHashTable,
										(void *) &filespaceDirEntry->key,
										HASH_REMOVE,
										NULL);

		if (removeFilespaceDirEntry == NULL)
			elog(ERROR, "Trying to delete entry that does not exist");
	}
}

extern void PersistentFilespace_LookupTidAndSerialNum(
	Oid 			filespaceOid,
	ItemPointer		persistentTid,
	int64			*persistentSerialNum)
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	FilespaceDirEntry filespaceDirEntry;

	PersistentFilespace_VerifyInitScan();

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	filespaceDirEntry =
				PersistentFilespace_FindDirUnderLock(
												filespaceOid);
	if (filespaceDirEntry == NULL)
		elog(ERROR, "Did not find persistent filespace entry %u",
			 filespaceOid);

	*persistentTid = filespaceDirEntry->persistentTid;
	*persistentSerialNum = filespaceDirEntry->persistentSerialNum;

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;
}


// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

static void PersistentFilespace_AddTuple(
	FilespaceDirEntry filespaceDirEntry,

	int64			createMirrorDataLossTrackingSessionNum,

	int32			reserved,

	TransactionId 	parentXid,

	bool			flushToXLog)
				/* When true, the XLOG record for this change will be flushed to disk. */
{
	Oid filespaceOid = filespaceDirEntry->key.filespaceOid;

	ItemPointerData previousFreeTid;

	Datum values[Natts_gp_persistent_filespace_node];

	MemSet(&previousFreeTid, 0, sizeof(ItemPointerData));

	GpPersistentFilespaceNode_SetDatumValues(
										values,
										filespaceOid,
										filespaceDirEntry->locationBlankPadded1,
										filespaceDirEntry->state,
										reserved,
										parentXid,
										/* persistentSerialNum */ 0,	// This will be set by PersistentFileSysObj_AddTuple.
										&previousFreeTid,
										is_filespace_shared(filespaceDirEntry->key.filespaceOid));

	PersistentFileSysObj_AddTuple(
							PersistentFsObjType_FilespaceDir,
							values,
							flushToXLog,
							&filespaceDirEntry->persistentTid,
							&filespaceDirEntry->persistentSerialNum);
}

static void PersistentFilespace_BlankPadCopyLocation(
	char locationBlankPadded[FilespaceLocationBlankPaddedWithNullTermLen],
	char *location)
{
	int len;
	int blankPadLen;

	if (location != NULL)
	{
		len = strlen(location);
		if (len > FilespaceLocationBlankPaddedWithNullTermLen - 1)
			elog(ERROR, "Location '%s' is too long (found %d characaters -- expected no more than %d characters)",
				 location,
			     len,
			     FilespaceLocationBlankPaddedWithNullTermLen - 1);
	}
	else
		len = 0;

	if (len > 0)
		memcpy(locationBlankPadded, location, len);

	blankPadLen = FilespaceLocationBlankPaddedWithNullTermLen - 1 - len;
	if (blankPadLen > 0)
		MemSet(&locationBlankPadded[len], ' ', blankPadLen);

	locationBlankPadded[FilespaceLocationBlankPaddedWithNullTermLen - 1] = '\0';
}

void PersistentFilespace_ConvertBlankPaddedLocation(
	char 		**filespaceLocation,

	char 		*locationBlankPadded,

	bool		isPrimary)
{
	char *firstBlankChar;
	int len;

	firstBlankChar = strchr(locationBlankPadded, ' ');
	if (firstBlankChar != NULL)
	{
		len = firstBlankChar - locationBlankPadded;
		if (len == 0)
		{
			if (isPrimary)
				elog(ERROR, "Expecting non-empty primary location");

			*filespaceLocation = NULL;
			return;
		}

		*filespaceLocation = (char*)palloc(len + 1);
		memcpy(*filespaceLocation, locationBlankPadded, len);
		(*filespaceLocation)[len] = '\0';
	}
	else
	{
		/*
		 * Whole location is non-blank characters...
		 */
		len = strlen(locationBlankPadded) + 1;
		if (len != FilespaceLocationBlankPaddedWithNullTermLen)
			elog(ERROR, "Incorrect format for blank-padded filespace location");

		*filespaceLocation = pstrdup(locationBlankPadded);
	}
}

bool PersistentFilespace_TryGetFilespacePathUnderLock(
	Oid 		filespaceOid,
	char		**filespaceLocation)
{
	FilespaceDirEntry filespaceDirEntry;

	char *primaryBlankPadded = NULL;

	*filespaceLocation = NULL;

#ifdef MASTER_MIRROR_SYNC
	/*
	 * Can't rely on persistent tables or memory structures on the standby so
	 * get it from the cache maintained by the master mirror sync code
	 */
	if (GPStandby())
	{
		return mmxlog_filespace_get_path(
									filespaceOid,
									filespaceLocation);
	}
#endif

	/*
	 * Important to make this call AFTER we check if we are the Standby Master.
	 */
	PersistentFilespace_VerifyInitScan();

	filespaceDirEntry =
				PersistentFilespace_FindDirUnderLock(
												filespaceOid);
	if (filespaceDirEntry == NULL)
		return false;

	/*
	 * The persistent_filespace_node_table contains the paths for both the
	 * primary and mirror nodes, and the table is the same on both sides of the
	 * mirror.  When it was first created the primary put its location first,
	 * but we don't know if we were the primary when it was created or not.  To
	 * determine which path corresponds to this node we compare our dbid to the
	 * one stored in the table.
	 */
	primaryBlankPadded = filespaceDirEntry->locationBlankPadded1;

	/* These should both have been populated by one of the cases above */
	Assert(primaryBlankPadded);

	PersistentFilespace_ConvertBlankPaddedLocation(
											filespaceLocation,
											primaryBlankPadded,
											/* isPrimary */ true);

	return true;
}

void PersistentFilespace_GetLocation(
	Oid 		filespaceOid,
	char **filespaceLocation)
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	/*
	 * Do not call PersistentFilespace_VerifyInitScan here to allow
	 * PersistentFilespace_TryGetFilespacePathUnderLock to handle the Standby Master
	 * special case.
	 */

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	if (!PersistentFilespace_TryGetFilespacePathUnderLock(
											filespaceOid,
											filespaceLocation))
	{
		elog(ERROR, "Did not find persistent filespace entry %u", filespaceOid);
	}

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;
}

// -----------------------------------------------------------------------------
// State Change
// -----------------------------------------------------------------------------

/*
 * Indicate we intend to create a filespace file as part of the current transaction.
 *
 * An XLOG IntentToCreate record is generated that will guard the subsequent file-system
 * create in case the transaction aborts.
 *
 * After 1 or more calls to this routine to mark intention about filespace files that are going
 * to be created, call ~_DoPendingCreates to do the actual file-system creates.  (See its
 * note on XLOG flushing).
 */
void PersistentFilespace_MarkCreatePending(
	Oid 		filespaceOid,
	char 		*filespaceLocation,
	ItemPointer		persistentTid,
	int64			*persistentSerialNum,
	bool			flushToXLog)
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	PersistentFileSysObjName fsObjName;

	FilespaceDirEntry filespaceDirEntry;

	if (Persistent_BeforePersistenceWork())
	{
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(),
				 "Skipping persistent filespace %u because we are before persistence work",
				 filespaceOid);

		return;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentFilespace_VerifyInitScan();

	PersistentFileSysObjName_SetFilespaceDir(&fsObjName,filespaceOid,is_filespace_shared);

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	filespaceDirEntry =
				PersistentFilespace_CreateDirUnderLock(filespaceOid);
	if (filespaceDirEntry == NULL)
	{
		/* If out of shared memory, no need to promote to PANIC. */
		WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("Out of shared-memory for persistent filespaces"),
				 errhint("You may need to increase the gp_max_filespaces value"),
				 errOmitLocation(true)));
	}

	PersistentFilespace_BlankPadCopyLocation(
										filespaceDirEntry->locationBlankPadded1,
										filespaceLocation);

	filespaceDirEntry->state = PersistentFileSysState_CreatePending;

	PersistentFilespace_AddTuple(
							filespaceDirEntry,
							/* createMirrorDataLossTrackingSessionNum */ 0,
							/* reserved */ 0,
							/* parentXid */ GetTopTransactionId(),
							flushToXLog);

	*persistentTid = filespaceDirEntry->persistentTid;
	*persistentSerialNum = filespaceDirEntry->persistentSerialNum;

	/*
	 * This XLOG must be generated under the persistent write-lock.
	 */
#ifdef MASTER_MIRROR_SYNC
	mmxlog_log_create_filespace(filespaceOid,persistentTid, *persistentSerialNum);
#endif


	#ifdef FAULT_INJECTOR
			FaultInjector_InjectFaultIfSet(
										   FaultBeforePendingDeleteFilespaceEntry,
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

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(),
		     "Persistent filespace directory: Add '%s' in state 'Created', serial number " INT64_FORMAT " at TID %s",
			 PersistentFileSysObjName_ObjectName(&fsObjName),
			 *persistentSerialNum,
			 ItemPointerToString(persistentTid));
}

// -----------------------------------------------------------------------------
// Rebuild filespace persistent table 'gp_persistent_filespace_node'
// -----------------------------------------------------------------------------
void PersistentFilespace_AddCreated(
        Oid 		filespaceOid,
        /* The filespace OID to be added. */

        bool			flushToXLog)
        /* When true, the XLOG record for this change will be flushed to disk. */
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;
	
	PersistentFileSysObjName fsObjName;
	
	ItemPointerData		persistentTid;
	int64				persistentSerialNum;	
	FilespaceDirEntry	filespaceDirEntry;
	
	/*if (Persistent_BeforePersistenceWork())
	{	
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "Skipping persistent filespace %u because we are before persistence work",
				 filespaceOid);
		
		return;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}*/
	
	PersistentFilespace_VerifyInitScan();
	
	PersistentFileSysObjName_SetFilespaceDir(&fsObjName,filespaceOid,is_filespace_shared);

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;
	
	filespaceDirEntry = PersistentFilespace_CreateDirUnderLock(filespaceOid);
	if (filespaceDirEntry == NULL)
	{
		/* If out of shared memory, no need to promote to PANIC. */
		WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("Out of shared-memory for persistent filespaces"),
				 errhint("You may need to increase the gp_max_filespaces value"),
				 errOmitLocation(true)));
	}
    // if it is a new generated one, we need to set info from pg_filespace_entry
    if(filespaceDirEntry->persistentSerialNum==0 || strlen(filespaceDirEntry->locationBlankPadded1)==0)
    {
        Relation pg_fs_entry_rel;
        HeapScanDesc scandesc;
        HeapTuple tuple;
        ScanKeyData entry[1];	
        bool		isNull;
        Datum locDatum;
        char *loc;

        /* Lookup the information for the current pg_filespace_entry */
        pg_fs_entry_rel = heap_open(FileSpaceEntryRelationId, AccessShareLock);

        ScanKeyInit(&entry[0],
                    Anum_pg_filespace_entry_fsefsoid,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(filespaceOid));

        scandesc = heap_beginscan(pg_fs_entry_rel, SnapshotNow, 1, entry);
        tuple = heap_getnext(scandesc, ForwardScanDirection);

        /* We assume that there can be at most one matching tuple */
        if (!HeapTupleIsValid(tuple))
        {
            elog(ERROR, "filespace %u could not be found in pg_filespace_entry", filespaceOid);
        }
        locDatum = heap_getattr(tuple, 
                                    Anum_pg_filespace_entry_fselocation,
                                    pg_fs_entry_rel->rd_att, 
                                    &isNull);

        loc = TextDatumGetCString(locDatum);
        //convert location with blank padded
        memset(filespaceDirEntry->locationBlankPadded1, ' ', FilespaceLocationBlankPaddedWithNullTermLen);
        filespaceDirEntry->locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen-1]='\0';
        memcpy(filespaceDirEntry->locationBlankPadded1, loc, strlen(loc));
	
        if(isNull)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("internal error: filespace '%u' has no name defined",
                        filespaceOid)));

        heap_endscan(scandesc);
        heap_close(pg_fs_entry_rel, AccessShareLock);
    }

	filespaceDirEntry->state = PersistentFileSysState_Created;
	
    PersistentFilespace_AddTuple(
            filespaceDirEntry,
            /* createMirrorDataLossTrackingSessionNum */ 0,
            /* reserved */ 0,
            /* parentXid */ InvalidTransactionId,
            flushToXLog);

	persistentTid = filespaceDirEntry->persistentTid;
	persistentSerialNum = filespaceDirEntry->persistentSerialNum;
		
	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;
	
	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
		     "Persistent filespace directory: Add '%s' in state 'Created', serial number " INT64_FORMAT " at TID '%s' ",
			 PersistentFileSysObjName_ObjectName(&fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(&persistentTid));
}

// -----------------------------------------------------------------------------
// Transaction End
// -----------------------------------------------------------------------------

/*
 * Indicate the transaction commited and the filespace is officially created.
 */
void PersistentFilespace_Created(
	PersistentFileSysObjName *fsObjName,
				/* The filespace OID for the create. */

	ItemPointer		persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64			persistentSerialNum,
				/* Serial number for the filespace.	Distinquishes the uses of the tuple. */

	bool			retryPossible)

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	Oid 		filespaceOid = fsObjName->variant.filespaceOid;

	FilespaceDirEntry filespaceDirEntry;

	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (Persistent_BeforePersistenceWork())
	{
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(),
				 "Skipping persistent filespace %u because we are before "
				 "persistence work",
				 filespaceOid);
		/*
		 * The initdb process will load the persistent table once we out of
		 * bootstrap mode.
		 */
		return;
	}

	PersistentFilespace_VerifyInitScan();

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	filespaceDirEntry =
				PersistentFilespace_FindDirUnderLock(
												filespaceOid);
	if (filespaceDirEntry == NULL)
		elog(ERROR, "did not find persistent filespace entry %u",
			 filespaceOid);

	if (filespaceDirEntry->state != PersistentFileSysState_CreatePending)
		elog(ERROR, "persistent filespace entry %u expected to be in "
			 "'Create Pending' state (actual state '%s')",
			 filespaceOid,
			 PersistentFileSysObjState_Name(filespaceDirEntry->state));

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

	filespaceDirEntry->state = PersistentFileSysState_Created;

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(),
		     "Persistent filespace directory: '%s' changed state from 'Create Pending' to 'Created', serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));
}

/*
 * Indicate we intend to drop a filespace file as part of the current transaction.
 *
 * This filespace file to drop will be listed inside a commit, distributed commit, a distributed
 * prepared, and distributed commit prepared XOG records.
 *
 * For any of the commit type records, once that XLOG record is flushed then the actual
 * file-system delete will occur.  The flush guarantees the action will be retried after system
 * crash.
 */
PersistentFileSysObjStateChangeResult PersistentFilespace_MarkDropPending(
	PersistentFileSysObjName *fsObjName,
				/* The filespace OID for the drop. */

	ItemPointer		persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64			persistentSerialNum,
				/* Serial number for the filespace.	Distinquishes the uses of the tuple. */

	bool			retryPossible)

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	Oid 		filespaceOid = fsObjName->variant.filespaceOid;

	FilespaceDirEntry filespaceDirEntry;

	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (Persistent_BeforePersistenceWork())
	{
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(),
				 "Skipping persistent filespace %u because we are before persistence work",
				 filespaceOid);

		return false;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentFilespace_VerifyInitScan();

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	filespaceDirEntry =
				PersistentFilespace_FindDirUnderLock(
												filespaceOid);
	if (filespaceDirEntry == NULL)
		elog(ERROR, "Did not find persistent filespace entry %u",
			 filespaceOid);

	if (filespaceDirEntry->state != PersistentFileSysState_CreatePending &&
		filespaceDirEntry->state != PersistentFileSysState_Created)
		elog(ERROR, "Persistent filespace entry %u expected to be in 'Create Pending' or 'Created' state (actual state '%s')",
			 filespaceOid,
			 PersistentFileSysObjState_Name(filespaceDirEntry->state));

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

	filespaceDirEntry->state = PersistentFileSysState_DropPending;

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(),
		     "Persistent filespace directory: '%s' changed state from 'Create Pending' to 'Drop Pending', serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));

	return stateChangeResult;
}

/*
 * Indicate we are aborting the create of a filespace file.
 *
 * This state will make sure the filespace gets dropped after a system crash.
 */
PersistentFileSysObjStateChangeResult PersistentFilespace_MarkAbortingCreate(
	PersistentFileSysObjName *fsObjName,
				/* The filespace OID for the aborting create. */

	ItemPointer		persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64			persistentSerialNum,
				/* Serial number for the filespace.	Distinquishes the uses of the tuple. */

	bool			retryPossible)

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	Oid 		filespaceOid = fsObjName->variant.filespaceOid;

	FilespaceDirEntry filespaceDirEntry;

	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (Persistent_BeforePersistenceWork())
	{
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(),
				 "Skipping persistent filespace %u because we are before persistence work",
				 filespaceOid);

		return false;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentFilespace_VerifyInitScan();

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	filespaceDirEntry =
				PersistentFilespace_FindDirUnderLock(
												filespaceOid);
	if (filespaceDirEntry == NULL)
		elog(ERROR, "Did not find persistent filespace entry %u", filespaceOid);

	if (filespaceDirEntry->state != PersistentFileSysState_CreatePending)
		elog(ERROR, "Persistent filespace entry %u expected to be in 'Create Pending' (actual state '%s')",
			 filespaceOid,
			 PersistentFileSysObjState_Name(filespaceDirEntry->state));

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

	filespaceDirEntry->state = PersistentFileSysState_AbortingCreate;

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(),
		     "Persistent filespace directory: '%s' changed state from 'Create Pending' to 'Aborting Create', serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));

	return stateChangeResult;
}

static void
PersistentFilespace_DroppedVerifiedActionCallback(
	PersistentFileSysObjName 	*fsObjName,

	ItemPointer 				persistentTid,
			/* TID of the gp_persistent_rel_files tuple for the relation. */

	int64						persistentSerialNum,
			/* Serial number for the relation.	Distinquishes the uses of the tuple. */

	PersistentFileSysObjVerifyExpectedResult verifyExpectedResult)
{
	Oid filespaceOid = PersistentFileSysObjName_GetFilespaceDir(fsObjName);

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
		mmxlog_log_remove_filespace(filespaceOid,persistentTid, persistentSerialNum);
#endif

		break;

	default:
		elog(ERROR, "Unexpected persistent object verify expected result: %d",
			 verifyExpectedResult);
	}
}

/*
 * Indicate we physically removed the filespace file.
 */
void PersistentFilespace_Dropped(
	PersistentFileSysObjName *fsObjName,
				/* The filespace OID for the dropped filespace. */

	ItemPointer		persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64			persistentSerialNum)
				/* Serial number for the filespace.	Distinquishes the uses of the tuple. */

{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	Oid 		filespaceOid = fsObjName->variant.filespaceOid;

	FilespaceDirEntry filespaceDirEntry;

	PersistentFileSysState oldState;

	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (Persistent_BeforePersistenceWork())
	{
		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(),
				 "Skipping persistent filespace %u because we are before persistence work",
				 filespaceOid);

		return;	// The initdb process will load the persistent table once we out of bootstrap mode.
	}

	PersistentFilespace_VerifyInitScan();

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	filespaceDirEntry =
				PersistentFilespace_FindDirUnderLock(
												filespaceOid);
	if (filespaceDirEntry == NULL)
		elog(ERROR, "Did not find persistent filespace entry %u",
			 filespaceOid);

	if (filespaceDirEntry->state != PersistentFileSysState_DropPending &&
		filespaceDirEntry->state != PersistentFileSysState_AbortingCreate)
		elog(ERROR, "Persistent filespace entry %u expected to be in 'Drop Pending' or 'Aborting Create' (actual state '%s')",
			 filespaceOid,
			 PersistentFileSysObjState_Name(filespaceDirEntry->state));

	stateChangeResult =
		PersistentFileSysObj_StateChange(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								PersistentFileSysState_Free,
								/* retryPossible */ false,
								/* flushToXlog */ false,
								&oldState,
								PersistentFilespace_DroppedVerifiedActionCallback);

	filespaceDirEntry->state = PersistentFileSysState_Free;

	PersistentFilespace_RemoveDirUnderLock(filespaceDirEntry);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(),
		     "Persistent filespace directory: '%s' changed state from '%s' to (Free), serial number " INT64_FORMAT " at TID %s (State-Change result '%s')",
			 PersistentFileSysObjName_ObjectName(fsObjName),
			 PersistentFileSysObjState_Name(oldState),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 PersistentFileSysObjStateChangeResult_Name(stateChangeResult));
}

// -----------------------------------------------------------------------------
// Shmem
// -----------------------------------------------------------------------------

static Size PersistentFilespace_SharedDataSize(void)
{
	return MAXALIGN(sizeof(PersistentFilespaceSharedData));
}

/*
 * Return the required shared-memory size for this module.
 */
Size PersistentFilespace_ShmemSize(void)
{
	Size		size;

	/* The hash table of persistent filespaces */
	size = hash_estimate_size((Size)gp_max_filespaces,
							  sizeof(FilespaceDirEntryData));

	/* The shared-memory structure. */
	size = add_size(size, PersistentFilespace_SharedDataSize());

	elog(LOG, "PersistentFilespace_ShmemSize: %zu = "
			  "gp_max_filespaces: %d "
			  "* sizeof(FilespaceDirEntryData): %zu "
			  "+ PersistentFilespace_SharedDataSize(): %zu",
			  size,
			  gp_max_filespaces,
			  sizeof(FilespaceDirEntryData),
			  PersistentFilespace_SharedDataSize());

	return size;
}

/*
 * PersistentFilespace_HashTableInit
 *
 * Create or find shared-memory hash table.
 */
static bool
PersistentFilespace_HashTableInit(void)
{
	HASHCTL			info;
	int				hash_flags;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(FilespaceDirEntryKey);
	info.entrysize = sizeof(FilespaceDirEntryData);
	info.hash = tag_hash;
	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	persistentFilespaceSharedHashTable =
						ShmemInitHash("Persistent Filespace Hash",
								   gp_max_filespaces,
								   gp_max_filespaces,
								   &info,
								   hash_flags);

	if (persistentFilespaceSharedHashTable == NULL)
		return false;

	return true;
}

/*
 * Initialize the shared-memory for this module.
 */
void PersistentFilespace_ShmemInit(void)
{
	bool found;
	bool ok;

	/* Create the shared-memory structure. */
	persistentFilespaceSharedData =
		(PersistentFilespaceSharedData *)
						ShmemInitStruct("Persistent Filespace Data",
										PersistentFilespace_SharedDataSize(),
										&found);

	if (!found)
	{
		PersistentFileSysObj_InitShared(
						&persistentFilespaceSharedData->fileSysObjSharedData);
	}

	/* Create or find our shared-memory hash table. */
	ok = PersistentFilespace_HashTableInit();
	if (!ok)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("Not enough shared memory for persistent filespace hash table")));

	PersistentFileSysObj_Init(
						&persistentFilespaceData.fileSysObjData,
						&persistentFilespaceSharedData->fileSysObjSharedData,
						PersistentFsObjType_FilespaceDir,
						PersistentFilespace_ScanTupleCallback);


	Assert(persistentFilespaceSharedData != NULL);
	Assert(persistentFilespaceSharedHashTable != NULL);
}

/*
 * Note that this can go away when we do away with master mirror sync by WAL.
 */
#ifdef MASTER_MIRROR_SYNC /* annotation to show that this is just for mmsync */
void
get_filespace_data(fspc_agg_state **fas, char *caller)
{
	HASH_SEQ_STATUS stat;
	FilespaceDirEntry fde;

	int maxCount;

	Assert(*fas == NULL);

	mmxlog_add_filespace_init(fas, &maxCount);

	hash_seq_init(&stat, persistentFilespaceSharedHashTable);

	while ((fde = hash_seq_search(&stat)) != NULL)
	{
		mmxlog_add_filespace(
				 fas, &maxCount,
				 fde->key.filespaceOid,
				 fde->locationBlankPadded1,
                 caller);
	}

}
#endif
