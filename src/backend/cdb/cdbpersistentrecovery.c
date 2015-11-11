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
 * cdbpersistentrecovery.c
 *
 *-------------------------------------------------------------------------
 */
#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>

#include "postgres.h"
#include "cdb/cdbdoublylinked.h"
#include "access/persistentfilesysobjname.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "catalog/gp_persistent.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbpersistentrelfile.h"
#include "cdb/cdbpersistentrecovery.h"
#include "access/heapam.h"
#include "catalog/pg_tablespace.h"
#include "access/xlog_internal.h"
#include "catalog/catalog.h"
#include "cdb/cdbdoublylinked.h"
#include "utils/hsearch.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "cdb/cdbdirectopen.h"
#include "utils/guc.h"
#include "storage/smgr.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbresynchronizechangetracking.h"
#include "access/twophase.h"

static bool
PersistentRecovery_RedoRelationExists(
	ItemPointer		persistentTid,
	
	int64 			persistentSerialNum,
	
	RelFileNode 	*relFileNode);	

typedef struct XactEntryData
{
	TransactionId	xid;

	XactInfoKind	infoKind;

	DoublyLinkedHead	fsObjEntryList;
} XactEntryData;
typedef XactEntryData *XactEntry;

static HTAB *xactHashTable = NULL;

Pass2RecoveryHashShmem_s *pass2RecoveryHashShmem = NULL;

Size
Pass2Recovery_ShmemSize(void)
{
	Size    size;

	size = hash_estimate_size(
				(Size)GP_MAX_PASS2RECOVERY_ABORTINGCREATE,
				sizeof(Pass2RecoveryHashEntry_s));

	size = add_size(size, sizeof(Pass2RecoveryHashShmem_s));
	
	return size;	
}

/* Initialize hash table of AbortingCreate entries in shared memory */
void
Pass2Recovery_ShmemInit(void)
{
	HASHCTL         info;
	int             hash_flags;
	bool    foundPtr;

	pass2RecoveryHashShmem = 
			(Pass2RecoveryHashShmem_s *)
			ShmemInitStruct("pass2 recovery abortingcreate hash",
							sizeof(Pass2RecoveryHashShmem_s),
							&foundPtr);
	if (pass2RecoveryHashShmem == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				(errmsg("not enough shared memory for pass2 recovery"))));
	}

	if (!foundPtr) {
		MemSet(pass2RecoveryHashShmem,
			   0,
			   sizeof(Pass2RecoveryHashShmem_s));
	}

	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(Pass2RecoveryHashEntry_s);
	info.hash = tag_hash;
	hash_flags = (HASH_ELEM | HASH_FUNCTION);
	
	pass2RecoveryHashShmem->hash = 
			ShmemInitHash("pass2 recovery hash",
						  GP_MAX_PASS2RECOVERY_ABORTINGCREATE,
						  GP_MAX_PASS2RECOVERY_ABORTINGCREATE,
						  &info,
						  hash_flags);				  

	if (pass2RecoveryHashShmem->hash == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				(errmsg("not enough shared memory for pass2 recovery"))));
	}
}

static Pass2RecoveryHashEntry_s*
Pass2Recovery_InsertHashEntry(
							Oid		objid,
							bool 	*exists)
{
	bool	foundPtr;
	Pass2RecoveryHashEntry_s *entry;
	Assert(pass2RecoveryHashShmem->hash != NULL);
	entry = (Pass2RecoveryHashEntry_s *) hash_search(
										pass2RecoveryHashShmem->hash,
										(void *) &objid,
										HASH_ENTER_NULL,
										&foundPtr);
	if (entry == NULL) {
		*exists = FALSE;
		return entry;
	}
	
	if (foundPtr) {
		*exists = TRUE;
	} else {
		*exists = FALSE;
	}

	return entry;	
}				

static void
PersistentRecovery_XactHashTableInit(void)
{
	HASHCTL			info;
	int				hash_flags;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(TransactionId);
	info.entrysize = sizeof(XactEntryData);
	info.hash = tag_hash;

	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	xactHashTable = hash_create("XactEntry", 10, &info, hash_flags);
}

static XactEntry PersistentRecovery_FindOrCreateXactEntry(
	TransactionId	xid,

	bool			*found)
{
	XactEntry xactEntry;

	if (xactHashTable == NULL)
		PersistentRecovery_XactHashTableInit();

	xactEntry = 
			(XactEntry) 
					hash_search(xactHashTable,
								(void *) &xid,
								HASH_ENTER,
								found);

	if (!*found)
	{
		DoublyLinkedHead_Init(&xactEntry->fsObjEntryList);
	}

	return xactEntry;
}

#if suppress
static XactEntry PersistentRecovery_FindXactEntry(
	TransactionId	xid)
{
	XactEntry xactEntry;
	bool found;

	Assert (xactHashTable != NULL);

	xactEntry = 
			(XactEntry) 
					hash_search(xactHashTable,
								(void *) &xid,
								HASH_FIND,
								&found);
	if (!found)
		return NULL;

	return xactEntry;
}
#endif

typedef struct FsObjEntryKey
{
	ItemPointerData				persistentTid;
} FsObjEntryKey;

typedef struct FsObjEntryData
{
	FsObjEntryKey				key;

	PersistentFileSysObjName	fsObjName;

	PersistentFileSysRelStorageMgr relStorageMgr;

	int64						persistentSerialNum;
	
	PersistentFileSysState		state;

	TransactionId				xid;

	bool						updateNeeded;

	PersistentFileSysObjStateChangeResult stateChangeResult;
	
	DoubleLinks					xactLinks;

} FsObjEntryData;
typedef FsObjEntryData *FsObjEntry;

static char *
FsObjEntryToBuffer(char *buffer, int maxBufferLen, FsObjEntry fsObjEntry)
{
	int snprintfResult;

	if (fsObjEntry->state != PersistentFileSysState_Free)	
		snprintfResult = 
			snprintf(
				buffer,
				maxBufferLen,
				"%s: state '%s' , transaction %u, relation storage manager '%s', persistent serial number " INT64_FORMAT ", TID %s",
				PersistentFileSysObjName_TypeAndObjectName(&fsObjEntry->fsObjName),
				PersistentFileSysObjState_Name(fsObjEntry->state),
				fsObjEntry->xid,
				PersistentFileSysRelStorageMgr_Name(fsObjEntry->relStorageMgr),
				fsObjEntry->persistentSerialNum,
				ItemPointerToString(&fsObjEntry->key.persistentTid));
	else
		snprintfResult = 
			snprintf(
				buffer,
				maxBufferLen,
			    "Free entry as free number " INT64_FORMAT ", TID %s",
				fsObjEntry->persistentSerialNum,
				ItemPointerToString(&fsObjEntry->key.persistentTid));

	Assert(snprintfResult >= 0);
	Assert(snprintfResult < maxBufferLen);

	return buffer;
}

#define MAX_FS_OBJ_ENTRY_BUFFER 400
static char fsObjEntryBuffer[MAX_FS_OBJ_ENTRY_BUFFER];
static char fsObjEntryBuffer2[MAX_FS_OBJ_ENTRY_BUFFER];

static char *
FsObjEntryToString(FsObjEntry fsObjEntry)
{
	return FsObjEntryToBuffer(fsObjEntryBuffer, MAX_FS_OBJ_ENTRY_BUFFER, fsObjEntry);
}

static char *
FsObjEntryToString2(FsObjEntry fsObjEntry)
{
	return FsObjEntryToBuffer(fsObjEntryBuffer2, MAX_FS_OBJ_ENTRY_BUFFER, fsObjEntry);
}

static HTAB **fsObjHashTable = NULL;

static void
PersistentRecovery_FsObjHashTableInit(void)
{
	HASHCTL			info;
	int				hash_flags;

	PersistentFsObjType fsObjType;

	char name[20];

	fsObjHashTable = (HTAB**)palloc(CountPersistentFsObjType * sizeof(HTAB*));

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(FsObjEntryKey);
	info.entrysize = sizeof(FsObjEntryData);
	info.hash = tag_hash;

	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	for (fsObjType = PersistentFsObjType_First;
	     fsObjType <= PersistentFsObjType_Last;
		 fsObjType++)
 	{
 		sprintf(name, "FsObjEntry_%d", fsObjType);
		fsObjHashTable[fsObjType] = hash_create(name, 10, &info, hash_flags);
 	}

}

static FsObjEntry PersistentRecovery_FindOrCreateFsObjEntry(
	PersistentFsObjType fsObjType,
	
	ItemPointer			persistentTid,

	bool				*found)
{
	FsObjEntry fsObjEntry;

	FsObjEntryKey	key;

	if (fsObjHashTable == NULL)
		PersistentRecovery_FsObjHashTableInit();

	MemSet(&key, 0, sizeof(FsObjEntryKey));
	key.persistentTid = *persistentTid;

	fsObjEntry = 
				(FsObjEntry) 
						hash_search(fsObjHashTable[fsObjType],
									(void *) &key,
									HASH_ENTER,
									found);

	Assert(ItemPointerCompare(&fsObjEntry->key.persistentTid, persistentTid) == 0);

	if (!*found)
	{
		DoubleLinks_Init(&fsObjEntry->xactLinks);
		fsObjEntry->persistentSerialNum = 0;
		MemSet(&fsObjEntry->fsObjName, 0, sizeof(PersistentFileSysObjName));
		fsObjEntry->relStorageMgr = PersistentFileSysRelStorageMgr_None;
		fsObjEntry->xid = InvalidTransactionId;
		fsObjEntry->state = -1;
		fsObjEntry->updateNeeded = false;
		fsObjEntry->stateChangeResult = PersistentFileSysObjStateChangeResult_StateChangeOk;
	}

	return fsObjEntry;
}

static void PersistentRecovery_AddEndXactFsObj(
	TransactionId 						xid,

	XactInfoKind						infoKind,

	PersistentEndXactFileSysActionInfo	*fileSysActionInfo,

	PersistentFileSysState				state)
{
	FsObjEntry 	fsObjEntry;
	FsObjEntryData prevFsObjEntry;

	bool found;

	/*
	 * Create Persistent Change entry keyed on TID.
	 */
	fsObjEntry = 
		PersistentRecovery_FindOrCreateFsObjEntry(
									fileSysActionInfo->fsObjName.type, 
									&fileSysActionInfo->persistentTid, 
									&found);
	if (found)
	{
		memcpy(&prevFsObjEntry, fsObjEntry, sizeof(FsObjEntryData));
	}

	fsObjEntry->fsObjName = fileSysActionInfo->fsObjName;

	if (fileSysActionInfo->fsObjName.type == PersistentFsObjType_RelationFile)
	{
		if (!PersistentFileSysRelStorageMgr_IsValid(fileSysActionInfo->relStorageMgr))
			elog(ERROR, "Relation storage manager for persistent '%s' for Crash Recovery is invalid (%d)",
				 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
				 fileSysActionInfo->relStorageMgr);

		fsObjEntry->relStorageMgr = fileSysActionInfo->relStorageMgr;
	}
	else
	{
		// Not a 'Relation File'.
		fsObjEntry->relStorageMgr = PersistentFileSysRelStorageMgr_None;
	}
		
	fsObjEntry->persistentSerialNum = fileSysActionInfo->persistentSerialNum;
	fsObjEntry->xid = xid;
	fsObjEntry->state = state;

	if (found)
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
			     "%s Record (transaction %u): Overwritting %s with %s",
				 XactInfoKind_Name(infoKind),
				 xid,
				 FsObjEntryToString(&prevFsObjEntry),
				 FsObjEntryToString2(fsObjEntry));
	}
	else
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
			     "%s Record (transaction %u): Adding %s",
				 XactInfoKind_Name(infoKind),
				 xid,
				 FsObjEntryToString(fsObjEntry));
	}
}

bool
PersistentRecovery_ShouldHandlePass1XLogRec(
	XLogRecPtr		*beginLoc,

	XLogRecPtr 		*lsn,
	
	XLogRecord 		*record)
{
	int					relationChangeInfoArrayCount;
	int					arrlen = ChangeTracking_GetInfoArrayDesiredMaxLength(record->xl_rmid, 
																			 record->xl_info);
	RelationChangeInfo 	relationChangeInfoArray[arrlen];
	
	if (Debug_persistent_recovery_print)
		ChangeTracking_PrintRelationChangeInfo(
											record->xl_rmid,
											record->xl_info,
											(void*)XLogRecGetData(record), 
											lsn,
											/* weAreGeneratingXLogNow */ false,
											/* printSkipIssuesOnly */ false);

	/*
	 * Gather vital peristence information from the XLOG record about relations ONLY.
	 */
	ChangeTracking_GetRelationChangeInfoFromXlog(
									  record->xl_rmid,
									  record->xl_info,
									  (void*)XLogRecGetData(record), 
									  relationChangeInfoArray,
									  &relationChangeInfoArrayCount,
									  arrlen);
	if (relationChangeInfoArrayCount == 0)
	{
		/*
		 * A non-data change XLOG record.
		 */
		return false;
	}
	else
	{
		RelFileNode *xlogRelFileNode;

		xlogRelFileNode = &relationChangeInfoArray[0].relFileNode;
		
		if(RelFileNode_IsEmpty(xlogRelFileNode))
			elog(ERROR, "Invalid RelFileNode (0,0,0)");
		
		if (GpPersistent_IsPersistentRelation(xlogRelFileNode->relNode) 
			&& (xlogRelFileNode->relNode != GpGlobalSequenceRelationId))
			/* MPP-17181: since the fix for MPP-17181 will always sync
			 * newly allocated sequence numbers of gp_global_sequence
			 * to disk before persistent table use them, replay of XLOG
			 * records for gp_global_sequence will be skipped, otherwise
			 * replay of stale xlog records might corrupt valid on-disk
			 * gp_global_sequence relation file.
			 */
		{
			if (Debug_persistent_recovery_print)
				elog(PersistentRecovery_DebugPrintLevel(), 
					 "PersistentRecovery_ShouldHandlePass1XLogRec: persistent meta-data (returning true) %u/%u/%u",
					 xlogRelFileNode->spcNode,
					 xlogRelFileNode->dbNode,
					 xlogRelFileNode->relNode);
			return true;
		}
		else
		{
			if (Debug_persistent_recovery_print)
				elog(PersistentRecovery_DebugPrintLevel(), 
					 "PersistentRecovery_ShouldHandlePass1XLogRec: regular table (returning false) %u/%u/%u",
					 xlogRelFileNode->spcNode,
					 xlogRelFileNode->dbNode,
					 xlogRelFileNode->relNode);
			return false;
		}
	}

	return false;	// Not reached.
}

void
PersistentRecovery_HandlePass2XLogRec(
	XLogRecPtr		*beginLoc,

	XLogRecPtr 		*lsn,
	
	XLogRecord 		*record)
{
	RmgrId rmid = record->xl_rmid;

	if (rmid == RM_HEAP_ID)
	{
		RelFileNode relFileNode;
		
		if (!heap_getrelfilenode(record, &relFileNode))
			elog(ERROR, "No relfilenode");

		if (relFileNode.spcNode != GLOBALTABLESPACE_OID)
			return;

		if (relFileNode.relNode != GpPersistentRelfileNodeRelationId &&
			relFileNode.relNode != GpPersistentRelationNodeRelationId &&
			relFileNode.relNode != GpPersistentDatabaseNodeRelationId &&
			relFileNode.relNode != GpPersistentTablespaceNodeRelationId &&
			relFileNode.relNode != GpPersistentFilespaceNodeRelationId)
			return;
	}
	else if (rmid == RM_XACT_ID)
	{
		TransactionId xid;

		XactEntry xactEntry;
		bool found;
		
		PersistentEndXactRecObjects persistentObjects;
		TransactionId *subXids;
		int subXidCount;
		XactInfoKind infoKind;
		int i;
		PersistentFileSysState newState;

		/*
		 * The XLOG record is for transaction module...
		 */
		if (!xact_redo_get_info(
							record,
							&infoKind,
							&xid,
							&persistentObjects,
							&subXids,
							&subXidCount))
			return;

		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "%s Record (transaction %u)",
				 XactInfoKind_Name(infoKind),
				 xid);
		
		/*
		 * We keep track of comitted, aborted, and prepared transactions so 'Create Pending' 
		 * objects can be attached to these transaction entries and not get automatically
		 * aborted by Crash Recovery.
		 */
		xactEntry = 
			PersistentRecovery_FindOrCreateXactEntry(xid, &found);
		xactEntry->infoKind = infoKind;
		
		if (infoKind == XACT_INFOKIND_COMMIT)
		{
			for (i = 0; i < persistentObjects.typed.fileSysActionInfosCount; i++)
			{
				PersistentEndXactFileSysActionInfo	*fileSysActionInfo =
							&persistentObjects.typed.fileSysActionInfos[i];

				PersistentEndXactFileSysAction action;
				
				action = fileSysActionInfo->action;

				switch (action)
				{
					case PersistentEndXactFileSysAction_Create:
						newState = PersistentFileSysState_Created;
						break;
					case PersistentEndXactFileSysAction_Drop:
						newState = PersistentFileSysState_DropPending;
						break;
					case PersistentEndXactFileSysAction_AbortingCreateNeeded:
						newState = PersistentFileSysState_AbortingCreate;
						break;
					default:
						elog(ERROR, "Unexpected persistent end transaction file-system action: %d",
							 action);
						newState = PersistentFileSysState_Free;		// Not reached.
						break;
				}
			
				PersistentRecovery_AddEndXactFsObj(
												xid,
												XACT_INFOKIND_COMMIT,
												fileSysActionInfo,
												newState);
			}
		}
		else if (infoKind == XACT_INFOKIND_ABORT)
		{
			for (i = 0; i < persistentObjects.typed.fileSysActionInfosCount; i++)
			{
				PersistentEndXactFileSysActionInfo	*fileSysActionInfo =
							&persistentObjects.typed.fileSysActionInfos[i];

				PersistentEndXactFileSysAction action;

				action = fileSysActionInfo->action;

				switch (action)
				{
					case PersistentEndXactFileSysAction_Create:
						newState = PersistentFileSysState_AbortingCreate;
						break;
					case PersistentEndXactFileSysAction_Drop:
						continue;
					case PersistentEndXactFileSysAction_AbortingCreateNeeded:
						newState = PersistentFileSysState_AbortingCreate;
						break;
					default:
						elog(ERROR, "Unexpected persistent end transaction file-system action: %d",
							 action);
						newState = PersistentFileSysState_Free;		// Not reached.
				}

				/* MPP-16881: adding AbortingCreate objid to shared memory
 				 * hash table, which will be used in Pass3 recovery to 
				 * clean up gp_fastsequence.
				 */
				if (fileSysActionInfo->fsObjName.type == PersistentFsObjType_RelationFile)
				{
					bool exists;
					Pass2RecoveryHashEntry_s *entry;
					RelFileNode *relFileNode = PersistentFileSysObjName_GetRelFileNodePtr(&fileSysActionInfo->fsObjName);
					entry = Pass2Recovery_InsertHashEntry(
													relFileNode->relNode,
													&exists);
					if (entry == NULL)
						elog(WARNING, 
							 "Pass2Recovery_InsertHashEntry"
							 " failed to insert AbortingCreate entry into"
							 " shared memory hash table, there might be"
							 " entries in gp_fastsequence left uncleaned,"
							 " it could cause inconsistency between"
							 " pg_class and gp_fastsequence.");
				}
			
				PersistentRecovery_AddEndXactFsObj(
												xid,
												XACT_INFOKIND_ABORT,
												fileSysActionInfo,
												newState);
			}
		}
		else
		{
			Assert(infoKind == XACT_INFOKIND_PREPARE);

			/*
			 * Since we don't know if this transaction will commit or abort, we
			 * only keep the transaction entry.
			 */
		}
	}

}

bool
PersistentRecovery_ShouldHandlePass3XLogRec(
	XLogRecPtr		*beginLoc,
	XLogRecPtr 		*lsn,
	XLogRecord 		*record)
{
	int					relationChangeInfoArrayCount;
	int					arrlen = ChangeTracking_GetInfoArrayDesiredMaxLength(record->xl_rmid,
																			 record->xl_info);
	RelationChangeInfo 	relationChangeInfoArray[arrlen];
	
	RelFileNode xlogRelFileNode;
	ItemPointerData xlogPersistentTid;
	int64 xlogPersistentSerialNum;

	bool exists;

	if (Debug_persistent_recovery_print)
		ChangeTracking_PrintRelationChangeInfo(
											record->xl_rmid,
											record->xl_info,
											(void*)XLogRecGetData(record), 
											lsn,
											/* weAreGeneratingXLogNow */ false,
											/* printSkipIssuesOnly */ false);

	/*
	 * Gather vital peristence information from the XLOG record about relations ONLY.
	 */
	ChangeTracking_GetRelationChangeInfoFromXlog(
									  record->xl_rmid,
									  record->xl_info,
									  (void*)XLogRecGetData(record), 
									  relationChangeInfoArray,
									  &relationChangeInfoArrayCount,
									  arrlen);

	if (relationChangeInfoArrayCount == 0)
	{
		/*
		 * Special case truncate because it is not considered a FileRep "page change"....
		 */
		if (record->xl_rmid == RM_SMGR_ID)
		{
			if (!smgrgetpersistentinfo(
									record,
									&xlogRelFileNode,
									&xlogPersistentTid,
									&xlogPersistentSerialNum))
			{
				if (Debug_persistent_recovery_print)
					elog(PersistentRecovery_DebugPrintLevel(), 
						 "PersistentRecovery_ShouldHandlePass3XLogRec: truncate %u/%u/%u, serial number " INT64_FORMAT ", TID %s",
						  xlogRelFileNode.spcNode,
						  xlogRelFileNode.dbNode,
						  xlogRelFileNode.relNode,
						  xlogPersistentSerialNum,
						  ItemPointerToString(&xlogPersistentTid));

				return true;
			}

			// Otherwise, fall through with persistent information.
		}
		else
		{
			/*
			 * A non-data change XLOG record.
			 */
			if (Debug_persistent_recovery_print)
				elog(PersistentRecovery_DebugPrintLevel(), 
					 "PersistentRecovery_ShouldHandlePass3XLogRec: non-data change XLOG record");

			return true;
		}
	}
	else
	{

		xlogRelFileNode = relationChangeInfoArray[0].relFileNode;
		xlogPersistentTid = relationChangeInfoArray[0].persistentTid;
		xlogPersistentSerialNum = relationChangeInfoArray[0].persistentSerialNum;
	}
	
	if (GpPersistent_IsPersistentRelation(xlogRelFileNode.relNode))
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "PersistentRecovery_ShouldHandlePass3XLogRec: skip persistent meta-data %u/%u/%u",
				 xlogRelFileNode.spcNode,
				 xlogRelFileNode.dbNode,
				 xlogRelFileNode.relNode);
		return false;
	}

	if (GpPersistent_SkipXLogInfo(xlogRelFileNode.relNode))
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "PersistentRecovery_ShouldHandlePass3XLogRec: other special relation %u/%u/%u",
				 xlogRelFileNode.spcNode,
				 xlogRelFileNode.dbNode,
				 xlogRelFileNode.relNode);
		return true;
	}
		
	/*
	 * Further qualify using the RelFileNode.
	 */
	exists = PersistentRecovery_RedoRelationExists(
												&xlogPersistentTid,
												xlogPersistentSerialNum,
												&xlogRelFileNode);

	if (exists)
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "PersistentRecovery_ShouldHandlePass3XLogRec: match %u/%u/%u, serial number " INT64_FORMAT ", TID %s",
				  xlogRelFileNode.spcNode,
				  xlogRelFileNode.dbNode,
				  xlogRelFileNode.relNode,
				  xlogPersistentSerialNum,
				  ItemPointerToString(&xlogPersistentTid));
	}
	else
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "PersistentRecovery_ShouldHandlePass3XLogRec: relation has been dropped or the create aborted %u/%u/%u, serial number " INT64_FORMAT ", TID %s",
				  xlogRelFileNode.spcNode,
				  xlogRelFileNode.dbNode,
				  xlogRelFileNode.relNode,
				  xlogPersistentSerialNum,
				  ItemPointerToString(&xlogPersistentTid));
	}

	return exists;
}


static void
PersistentRecovery_PrintXactAndFsObjs(void)
{
	HASH_SEQ_STATUS iterateStatus;
	XactEntry xactEntry;
	FsObjEntry fsObjEntry;

	elog(PersistentRecovery_DebugPrintLevel(), 
	     "Entering PersistentRecovery_PrintXactAndFsObjs");

	if (xactHashTable == NULL)
	{
		elog(PersistentRecovery_DebugPrintLevel(), 
			 "Entering PersistentRecovery_PrintXactAndFsObjs -- no entries");
		return;
	}
	
	hash_seq_init(&iterateStatus, xactHashTable);

	while (true)
	{
		xactEntry = 
				(XactEntry)
						hash_seq_search(&iterateStatus);
		if (xactEntry == NULL)
			break;


		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Summary REDO: Transaction %u (%s)",
			 xactEntry->xid,
			 XactInfoKind_Name(xactEntry->infoKind));

		fsObjEntry = 
			(FsObjEntry) DoublyLinkedHead_First(
									offsetof(FsObjEntryData, xactLinks),
									&xactEntry->fsObjEntryList);
		while (fsObjEntry != NULL)
		{
			elog(PersistentRecovery_DebugPrintLevel(), 
			     "Summary REDO: %s",
				 FsObjEntryToString(fsObjEntry));

			fsObjEntry = 
				(FsObjEntry) DoublyLinkedHead_Next(
										offsetof(FsObjEntryData, xactLinks),
										&xactEntry->fsObjEntryList,
										fsObjEntry);
		}
	}
	
	elog(PersistentRecovery_DebugPrintLevel(), 
	     "Exiting PersistentRecovery_PrintXactAndFsObjs");

}

// -----------------------------------------------------------------------------


static void
PersistentRecovery_AddScanEntry(
	PersistentFsObjType 	fsObjType,

	ItemPointer				persistentTid,

	PersistentFileSysObjName	*fsObjName,

	PersistentFileSysRelStorageMgr relStorageMgr,

	PersistentFileSysState	state,
	
	TransactionId			parentXid,
	
	int64					persistentSerialNum)
{

	FsObjEntry fsObjEntry;
	bool found;

	if (state == PersistentFileSysState_Free)
	{
		fsObjEntry = 
			PersistentRecovery_FindOrCreateFsObjEntry(fsObjType, persistentTid, &found);

		if (found)
		{
			if (Debug_persistent_recovery_print)
				elog(PersistentRecovery_DebugPrintLevel(), 
				     "Scan REDO: Overwriting %s as free", 
					 FsObjEntryToString(fsObjEntry));

		}
		else
		{
			if (Debug_persistent_recovery_print)
				elog(PersistentRecovery_DebugPrintLevel(), 
				     "Scan REDO: Free entry with no end transaction work: %s",
					 FsObjEntryToString(fsObjEntry));
		}
		
		fsObjEntry->state = PersistentFileSysState_Free;
		
		fsObjEntry->updateNeeded = false;		// Already in terminal condition.

		return;
	}

	/*
	 * Create Persistent Change entry.
	 */
	fsObjEntry = 
		PersistentRecovery_FindOrCreateFsObjEntry(
										fsObjType,
										persistentTid,
										&found);
	if (!found)
	{
		fsObjEntry->fsObjName = *fsObjName;
		fsObjEntry->relStorageMgr = relStorageMgr;
		fsObjEntry->persistentSerialNum = persistentSerialNum;
		fsObjEntry->xid = parentXid;
		fsObjEntry->state = state;
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
			     "Scan REDO: Add %s", FsObjEntryToString(fsObjEntry));
		return;
	}

	if (fsObjEntry->persistentSerialNum == persistentSerialNum)
	{
		if (fsObjEntry->state < state)
		{
			if (Debug_persistent_recovery_print)
				elog(PersistentRecovery_DebugPrintLevel(), 
				     "Scan REDO: Newer %s than %s state, transaction %u",
					 FsObjEntryToString(fsObjEntry),
					 PersistentFileSysObjState_Name(state),
					 parentXid);

			fsObjEntry->state = state;
			fsObjEntry->xid = parentXid;
		}
		else if (fsObjEntry->state > state)
		{
			if (Debug_persistent_recovery_print)
				elog(PersistentRecovery_DebugPrintLevel(), 
				     "Scan REDO: Update %s to state '%s', transaction %u, serial number " INT64_FORMAT,
					 FsObjEntryToString(fsObjEntry),
					 PersistentFileSysObjState_Name(state),
					 parentXid,
					 persistentSerialNum);

			fsObjEntry->updateNeeded = true;
		}
		else
		{
				/*
				* During crash recovery when we drop objects, we should skip the objects whose
				* mirror existence state is MirrorDropRemains. It will be dropped during resync
				*/
		}
	}
	else if (fsObjEntry->persistentSerialNum < persistentSerialNum)
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
			     "Scan REDO: Overwriting obsolete %s with '%s', relation storage manager '%s', state '%s', transaction %u, serial number " INT64_FORMAT,
				 FsObjEntryToString(fsObjEntry),
				 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
				 PersistentFileSysRelStorageMgr_Name(relStorageMgr),
				 PersistentFileSysObjState_Name(state),
				 parentXid,
				 persistentSerialNum);

		fsObjEntry->fsObjName = *fsObjName;
		fsObjEntry->relStorageMgr = relStorageMgr;
		fsObjEntry->state = state;
		fsObjEntry->xid = parentXid;
		fsObjEntry->persistentSerialNum = persistentSerialNum;
	}
	else
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
			     "Scan REDO: Wrong state %s", FsObjEntryToString(fsObjEntry));
	}
}

static int	persistentRecoveryCount = 0;

static void PersistentRecovery_ScanTuple(
	PersistentFsObjType 	fsObjType,
	ItemPointer 			persistentTid,
	int64					persistentSerialNum,
	Datum					*values)
{
	PersistentFileSysObjName	fsObjName;
	
	PersistentFileSysState	state;

	PersistentFileSysRelStorageMgr relStorageMgr;

	TransactionId			parentXid;
	int64					serialNum;

	GpPersistent_GetCommonValues(
							fsObjType,
							values,
							&fsObjName,
							&state,
							&parentXid,
							&serialNum);
	Assert(serialNum == persistentSerialNum);

	if (state != PersistentFileSysState_Free &&
		fsObjType == PersistentFsObjType_RelationFile)
	{
		relStorageMgr = 
			(PersistentFileSysRelStorageMgr)
							DatumGetInt16(
									values[Anum_gp_persistent_relfile_node_relation_storage_manager - 1]);
		
		if (!PersistentFileSysRelStorageMgr_IsValid(relStorageMgr))
			elog(ERROR, "Relation storage manager for persistent '%s' for Crash Recovery is invalid (%d)",
				 PersistentFileSysObjName_TypeAndObjectName(&fsObjName),
				 relStorageMgr);
	}
	else
	{
		// 'Free' entry or not a 'Relation File'.
		relStorageMgr = PersistentFileSysRelStorageMgr_None;
	}
	
	PersistentRecovery_AddScanEntry(
							fsObjType,
							persistentTid,
							&fsObjName,
							relStorageMgr,
							state,
							parentXid,
							persistentSerialNum);

	persistentRecoveryCount++;
}

static void
PersistentRecovery_ScanType(
	PersistentFsObjType fsObjType)
{
	PersistentFileSysObjData *fileSysObjData;
	PersistentFileSysObjSharedData	*fileSysObjSharedData;

	PersistentStoreScan storeScan;

	Datum *values;
	 
	ItemPointerData			 persistentTid;
	int64					 persistentSerialNum;

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Entering PersistentRecovery_ScanType %s",
		     PersistentFileSysObjName_TypeName(fsObjType));

	persistentRecoveryCount = 0;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);
		 
	values = (Datum*)palloc(fileSysObjData->storeData.numAttributes * sizeof(Datum));
		 
	PersistentStore_BeginScan(
						&fileSysObjData->storeData,
						&fileSysObjSharedData->storeSharedData,
						&storeScan);

	while (PersistentStore_GetNext(
							&storeScan,
							values,
							&persistentTid,
							&persistentSerialNum))
	{
		PersistentRecovery_ScanTuple(
								fsObjType,
								&persistentTid,
								persistentSerialNum,
								values);
	}

	pfree(values);
	
	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Exiting PersistentRecovery_ScanType %s, count %d",
		     PersistentFileSysObjName_TypeName(fsObjType),
		     persistentRecoveryCount);
}

void
PersistentRecovery_Scan(void)
{
	PersistentFsObjType fsObjType;

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Entering PersistentRecovery_Scan");

	for (fsObjType = PersistentFsObjType_First; 
		 fsObjType <= PersistentFsObjType_Last;
		 fsObjType++)
	 	PersistentRecovery_ScanType(fsObjType);

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Exiting PersistentRecovery_Scan");
}

static void
PersistentRecovery_AttachFsObjTypeToXact(
	PersistentFsObjType fsObjType)
{
	HASH_SEQ_STATUS iterateStatus;
	FsObjEntry fsObjEntry;
	XactEntry xactEntry;
	bool found;

	Assert (fsObjHashTable != NULL);
	
	hash_seq_init(&iterateStatus, fsObjHashTable[fsObjType]);

	while (true)
	{
		fsObjEntry = 
				(FsObjEntry)
						hash_seq_search(&iterateStatus);
		if (fsObjEntry == NULL)
			break;

		if (fsObjEntry->xid == InvalidTransactionId)
			continue;

		xactEntry = 
			PersistentRecovery_FindOrCreateXactEntry(
											fsObjEntry->xid, 
											&found);
		if (!found)
			xactEntry->infoKind = XACT_INFOKIND_NONE;

		DoublyLinkedHead_AddFirst(
						offsetof(FsObjEntryData, xactLinks),
						&xactEntry->fsObjEntryList,
						fsObjEntry);
	}
}

typedef struct RecordCrashTransactionAbortRecordErrContext
{
	TransactionId 		xid;

	PersistentEndXactFileSysActionInfo	*fileSysActionInfos;
	
	int createPendingCount;
} RecordCrashTransactionAbortRecordErrContext;

static void PersistentRecovery_RecordCrashTransactionAbortRecordErrContext(void *arg)
{
	RecordCrashTransactionAbortRecordErrContext	*errContext = (RecordCrashTransactionAbortRecordErrContext*) arg;

	PersistentEndXactFileSysActionInfo	*firstFileSysActionInfo;

	Assert(errContext->createPendingCount > 0);
	firstFileSysActionInfo = &errContext->fileSysActionInfos[0];

	errcontext( 
		 "Record abort transaction record for crashed transaction %u with %d 'Create Pending' file-system objects "
		 "(first file-system object %s, persistent serial number " INT64_FORMAT ", TID %s)",
		 errContext->xid,
		 errContext->createPendingCount,
		 PersistentFileSysObjName_TypeAndObjectName(&firstFileSysActionInfo->fsObjName),
		 firstFileSysActionInfo->persistentSerialNum,
		 ItemPointerToString(&firstFileSysActionInfo->persistentTid));
}

static void
PersistentRecovery_HandlePrepareBeforeCheckpoint(void)
{
	HASH_SEQ_STATUS iterateStatus;
	XactEntry xactEntry;

	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Entering PersistentRecovery_HandlePrepareBeforeCheckpoint");
	}

	if (xactHashTable == NULL)
	{
		if (Debug_persistent_recovery_print)
		{
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "Entering PersistentRecovery_HandlePrepareBeforeCheckpoint -- no entries");
		}
		return;
	}
	
	hash_seq_init(&iterateStatus, xactHashTable);

	while (true)
	{
		xactEntry = 
				(XactEntry)
						hash_seq_search(&iterateStatus);
		if (xactEntry == NULL)
			break;

		if (xactEntry->infoKind == XACT_INFOKIND_NONE)
		{
			XLogRecPtr preparedLoc;

			if (TwoPhaseFindRecoverPostCheckpointPreparedTransactionsMapEntry(
																		xactEntry->xid,
																		&preparedLoc,
																		"PersistentRecovery_HandlePrepareBeforeCheckpoint"))
			{
				xactEntry->infoKind = XACT_INFOKIND_PREPARE;

				if (Debug_persistent_recovery_print)
				{
					elog(PersistentRecovery_DebugPrintLevel(), 
						 "Prepare Before Checkpoint: Transaction %u (None) found and marked prepared (location %s)",
						 xactEntry->xid,
						 XLogLocationToString(&preparedLoc));
				}
			}
			else
			{
				if (Debug_persistent_recovery_print)
				{
					elog(PersistentRecovery_DebugPrintLevel(), 
						 "Prepare Before Checkpoint: Transaction %u (None) not found in checkpoint prepared list",
						 xactEntry->xid);
				}
			}
		}
		else
		{
			if (Debug_persistent_recovery_print)
			{
				elog(PersistentRecovery_DebugPrintLevel(), 
					 "Prepare Before Checkpoint: Transaction %u (%s)",
					 xactEntry->xid,
					 XactInfoKind_Name(xactEntry->infoKind));
			}
		}
	}
	
	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Exiting PersistentRecovery_HandlePrepareBeforeCheckpoint");
	}

}

void
PersistentRecovery_CrashAbort(void)
{
	PersistentFsObjType fsObjType;

	HASH_SEQ_STATUS iterateStatus;
	FsObjEntry fsObjEntry;
	XactEntry xactEntry;
	int xactCount;

	ErrorContextCallback errcontext;

	RecordCrashTransactionAbortRecordErrContext recordCrashTransactionAbortRecordErrContext;
	
	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Entering PersistentRecovery_CrashAbort");

	if (fsObjHashTable == NULL)
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
			     "Exiting PersistentRecovery_CrashAbort (no relation hash table)");
		return;
	}

	for (fsObjType = PersistentFsObjType_First; 
		 fsObjType <= PersistentFsObjType_Last;
		 fsObjType++)
		PersistentRecovery_AttachFsObjTypeToXact(fsObjType);

	PersistentRecovery_HandlePrepareBeforeCheckpoint();

	if (Debug_persistent_recovery_print)
	{
		PersistentRecovery_PrintXactAndFsObjs();
	}

	hash_seq_init(&iterateStatus, xactHashTable);

	xactCount = 0;
	while (true)
	{
		bool needsAbort;
		int createPendingCount;
		int e;
		PersistentEndXactFileSysActionInfo	*fileSysActionInfos;
		PersistentEndXactRecObjects persistentObjects;
		bool abortSuccessfullyRecorded;

		xactEntry = 
				(XactEntry)
						hash_seq_search(&iterateStatus);
		if (xactEntry == NULL)
			break;

		xactCount++;

		if (xactEntry->infoKind != XACT_INFOKIND_NONE)
		{
			Assert(xactEntry->infoKind == XACT_INFOKIND_PREPARE ||
				   xactEntry->infoKind == XACT_INFOKIND_COMMIT ||
				   xactEntry->infoKind == XACT_INFOKIND_ABORT);

			/*
			 * If it is Prepared, we don't know yet what to do with the transaction.
			 *
			 * If it is known Committed or Aborted because we found an XLOG Commit or
			 * or Abort, we may have updates that will be handled later.
			 */

			if (Debug_persistent_recovery_print)
				elog(PersistentRecovery_DebugPrintLevel(), 
					 "PersistentRecovery_CrashAbort: Skipping transaction %u (state '%s')",
					 xactEntry->xid,
					 XactInfoKind_Name(xactEntry->infoKind));

			continue;
		}

		/*
		 * We don't know the status of the transaction until we examine the
		 * persistent node states.
		 */
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "PersistentRecovery_CrashAbort: Checking transaction %u (state '%s')",
				 xactEntry->xid,
				 XactInfoKind_Name(xactEntry->infoKind));

		needsAbort = false;

		fsObjEntry = 
			(FsObjEntry) DoublyLinkedHead_First(
									offsetof(FsObjEntryData, xactLinks),
									&xactEntry->fsObjEntryList);
									
		createPendingCount = 0;
		while (fsObjEntry != NULL)
		{
			if (fsObjEntry->state == PersistentFileSysState_BulkLoadCreatePending ||
				fsObjEntry->state == PersistentFileSysState_CreatePending)
			{
				needsAbort = true;
				
				if (Debug_persistent_recovery_print)
					elog(PersistentRecovery_DebugPrintLevel(), 
 						 "Needs-Abort REDO: (New state 'Aborting Create') %s",
 						 FsObjEntryToString(fsObjEntry));

				createPendingCount++;
			}
			else if (fsObjEntry->state != PersistentFileSysState_Free)
			{
				if (needsAbort)
					elog(ERROR, "Found persistent '%s' entry among 'Create Pending' entries",
						 FsObjEntryToString(fsObjEntry));
				break;
			}

			fsObjEntry = 
				(FsObjEntry) DoublyLinkedHead_Next(
										offsetof(FsObjEntryData, xactLinks),
										&xactEntry->fsObjEntryList,
										fsObjEntry);
		}

		if (!needsAbort)
		{
			if (Debug_persistent_recovery_print)
				elog(PersistentRecovery_DebugPrintLevel(), 
					 "PersistentRecovery_CrashAbort: Skipping transaction %u due to no 'Create Pending' objects",
					 xactEntry->xid);

			Assert(createPendingCount == 0);
			continue;
		}

		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "PersistentRecovery_CrashAbort: Found %d 'Create Pending' persistent file-system objects for transaction %u",
				 createPendingCount,
				 xactEntry->xid);

		fileSysActionInfos = 
			(PersistentEndXactFileSysActionInfo*)
				palloc0(createPendingCount * sizeof(PersistentEndXactFileSysActionInfo));
		
		fsObjEntry = 
			(FsObjEntry) DoublyLinkedHead_First(
									offsetof(FsObjEntryData, xactLinks),
									&xactEntry->fsObjEntryList);
		e = 0;
		while (fsObjEntry != NULL)
		{
			if (fsObjEntry->state == PersistentFileSysState_BulkLoadCreatePending ||
				fsObjEntry->state == PersistentFileSysState_CreatePending)
			{
				Assert(e < createPendingCount);

				fileSysActionInfos[e].action = PersistentEndXactFileSysAction_Create;
				fileSysActionInfos[e].fsObjName = fsObjEntry->fsObjName;
				fileSysActionInfos[e].relStorageMgr = fsObjEntry->relStorageMgr;
				fileSysActionInfos[e].persistentTid = fsObjEntry->key.persistentTid;
				fileSysActionInfos[e].persistentSerialNum = fsObjEntry->persistentSerialNum;
				e++;
			}

			fsObjEntry = 
				(FsObjEntry) DoublyLinkedHead_Next(
										offsetof(FsObjEntryData, xactLinks),
										&xactEntry->fsObjEntryList,
										fsObjEntry);
		}

		PersistentEndXactRec_Init(&persistentObjects);
		PersistentEndXactRec_AddFileSysActionInfos(
										&persistentObjects,
										EndXactRecKind_Abort,
										fileSysActionInfos, 
										createPendingCount);

		
		/* Setup error traceback support for ereport() */
		recordCrashTransactionAbortRecordErrContext.xid = xactEntry->xid;
		recordCrashTransactionAbortRecordErrContext.fileSysActionInfos = fileSysActionInfos;
		recordCrashTransactionAbortRecordErrContext.createPendingCount = createPendingCount;

		errcontext.callback = PersistentRecovery_RecordCrashTransactionAbortRecordErrContext;
		errcontext.arg = (void *) &recordCrashTransactionAbortRecordErrContext;
		errcontext.previous = error_context_stack;
		error_context_stack = &errcontext;
		
		abortSuccessfullyRecorded =
			RecordCrashTransactionAbortRecord(
									xactEntry->xid, 
									&persistentObjects);
		
		/* Pop the error context stack */
		error_context_stack = errcontext.previous;
		
		pfree(fileSysActionInfos);

		fsObjEntry = 
			(FsObjEntry) DoublyLinkedHead_First(
									offsetof(FsObjEntryData, xactLinks),
									&xactEntry->fsObjEntryList);

		while (fsObjEntry != NULL)
		{
			if (fsObjEntry->state == PersistentFileSysState_BulkLoadCreatePending ||
				fsObjEntry->state == PersistentFileSysState_CreatePending)
			{
				if (Debug_persistent_recovery_print)
					elog(PersistentRecovery_DebugPrintLevel(), 
					     "PersistentRecovery_CrashAbort: Set state of %s to 'Aborting Create'", 
					     FsObjEntryToString(fsObjEntry));

				if (abortSuccessfullyRecorded)
				{
					fsObjEntry->updateNeeded = true;
					fsObjEntry->state = PersistentFileSysState_AbortingCreate;
					if (fsObjEntry->fsObjName.type == PersistentFsObjType_RelationFile)
					{
						bool exists;
						Pass2RecoveryHashEntry_s *entry;
						RelFileNode *relFileNode = 
										PersistentFileSysObjName_GetRelFileNodePtr(
												 			&fsObjEntry->fsObjName);
						entry = Pass2Recovery_InsertHashEntry(
													relFileNode->relNode,
													&exists);
						if (entry == NULL)
							elog(WARNING, 
							 	 "Pass2Recovery_InsertHashEntry"
								 " failed to insert AbortingCreate entry into"
								 " shared memory hash table, there might be"
								 " entries in gp_fastsequence left uncleaned,"
								 " it could cause inconsistency between"
								 " pg_class and gp_fastsequence.");
					}
				}
				else
				{
					elog(WARNING, "Not setting the state of '%s' to 'Aborting Create'",
						 FsObjEntryToString(fsObjEntry));
				}
				
			}
			else if (fsObjEntry->state != PersistentFileSysState_Free)
				elog(ERROR, "Unexpected persistent file-system state: %d",
					 fsObjEntry->state);

			fsObjEntry = 
				(FsObjEntry) DoublyLinkedHead_Next(
										offsetof(FsObjEntryData, xactLinks),
										&xactEntry->fsObjEntryList,
										fsObjEntry);
		}

	}

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
			 "Exiting PersistentRecovery_CrashAbort (transaction count %d)", xactCount);
}


static void
PersistentRecovery_UpdateType(
	PersistentFsObjType fsObjType)
{
	HASH_SEQ_STATUS iterateStatus;
	FsObjEntry fsObjEntry;
	PersistentFileSysState	state;
	
	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Entering PersistentRecovery_UpdateType %s",
		     PersistentFileSysObjName_TypeName(fsObjType));

	hash_seq_init(&iterateStatus, fsObjHashTable[fsObjType]);

	persistentRecoveryCount = 0;

	while (true)
	{
		fsObjEntry = 
				(FsObjEntry)
						hash_seq_search(&iterateStatus);
		if (fsObjEntry == NULL)
			break;

		if (fsObjEntry->updateNeeded)
		{
			persistentRecoveryCount++;

			if (Debug_persistent_recovery_print)
				elog(PersistentRecovery_DebugPrintLevel(), 
				     "End-Xact-Update-Needed REDO: %s", FsObjEntryToString(fsObjEntry));

			state = fsObjEntry->state;

			switch (state)
			{
				case PersistentFileSysState_Created:
					PersistentFileSysObj_Created(
									&fsObjEntry->fsObjName,
									&fsObjEntry->key.persistentTid,
									fsObjEntry->persistentSerialNum,
									/* retryPossible */ false);
					break;
				
				case PersistentFileSysState_AbortingCreate:
					fsObjEntry->stateChangeResult =
						PersistentFileSysObj_MarkAbortingCreate(
									&fsObjEntry->fsObjName,
									&fsObjEntry->key.persistentTid,
									fsObjEntry->persistentSerialNum,
									/* retryPossible */ false);
					break;

				case PersistentFileSysState_DropPending:
					fsObjEntry->stateChangeResult =
						PersistentFileSysObj_MarkDropPending(
									&fsObjEntry->fsObjName,
									&fsObjEntry->key.persistentTid,
									fsObjEntry->persistentSerialNum,
									/* retryPossible */ false);
					break;
				default:
					elog(ERROR, "Unexpected persistent file-system state: %d",
						 state);
			}
		}
	}
	
	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Exiting PersistentRecovery_UpdateType %s, count %d",
		     PersistentFileSysObjName_TypeName(fsObjType),
		     persistentRecoveryCount);
}

void
PersistentRecovery_Update(void)
{
	PersistentFsObjType fsObjType;

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Entering PersistentRecovery_Update");

	if (fsObjHashTable == NULL)
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "Exiting PersistentRecovery_Update (no relation hash table)");
		return;
	}

	for (fsObjType = PersistentFsObjType_First; 
		 fsObjType <= PersistentFsObjType_Last;
		 fsObjType++)
	 	PersistentRecovery_UpdateType(fsObjType);
	
	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
			 "Exiting PersistentRecovery_Update");
}

static void
PersistentRecovery_DropType(
	PersistentFsObjType fsObjType)
{
	HASH_SEQ_STATUS iterateStatus;
	FsObjEntry fsObjEntry;
	PersistentFileSysState	state;

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Entering PersistentRecovery_DropType %s",
		     PersistentFileSysObjName_TypeName(fsObjType));

	hash_seq_init(&iterateStatus, fsObjHashTable[fsObjType]);

	persistentRecoveryCount = 0;

	while (true)
	{
		fsObjEntry = 
				(FsObjEntry)
						hash_seq_search(&iterateStatus);
		if (fsObjEntry == NULL)
			break;

		state = fsObjEntry->state;
		
		if (state == PersistentFileSysState_AbortingCreate ||
			state == PersistentFileSysState_DropPending)
		{
			if (fsObjEntry->stateChangeResult != PersistentFileSysObjStateChangeResult_StateChangeOk)
			{
				if (fsObjEntry->stateChangeResult == PersistentFileSysObjStateChangeResult_ErrorSuppressed)
				{
					elog(WARNING, 
						 "Crash recovery skipping drop for %s with State-Change result '%s'", 
						 FsObjEntryToString(fsObjEntry),
						 PersistentFileSysObjStateChangeResult_Name(fsObjEntry->stateChangeResult));
				}
				else if (Debug_persistent_recovery_print)
				{
					elog(PersistentRecovery_DebugPrintLevel(), 
						 "Drop REDO: Skipping drop for %s with State-Change result '%s'", 
						 FsObjEntryToString(fsObjEntry),
						 PersistentFileSysObjStateChangeResult_Name(fsObjEntry->stateChangeResult));
				}
				continue;
			}

			persistentRecoveryCount++;

			if (Debug_persistent_recovery_print)
				elog(PersistentRecovery_DebugPrintLevel(), 
				     "Drop REDO: %s", FsObjEntryToString(fsObjEntry));

			if (fsObjType == PersistentFsObjType_RelationFile &&
				!PersistentFileSysRelStorageMgr_IsValid(fsObjEntry->relStorageMgr))
				elog(ERROR, "Relation storage manager for persistent '%s' for Crash Recovery is invalid (%d)",
					 PersistentFileSysObjName_TypeAndObjectName(&fsObjEntry->fsObjName),
					 fsObjEntry->relStorageMgr);
			
			PersistentFileSysObj_DropObject(
							&fsObjEntry->fsObjName,
							fsObjEntry->relStorageMgr,
							/* relationName */ NULL,		// Ok to be NULL -- we don't know the name here.
							&fsObjEntry->key.persistentTid,
							fsObjEntry->persistentSerialNum,
							/* ignoreNonExistence */ true,
							Debug_persistent_recovery_print,
							PersistentRecovery_DebugPrintLevel());
		}
	}

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Exiting PersistentRecovery_DropType %s, count %d",
		     PersistentFileSysObjName_TypeName(fsObjType),
		     persistentRecoveryCount);

}

void
PersistentRecovery_Drop(void)
{
	PersistentFsObjType fsObjType;

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Entering PersistentRecovery_Scan");

	for (fsObjType = PersistentFsObjType_First; 
		 fsObjType <= PersistentFsObjType_Last;
		 fsObjType++)
	 	PersistentRecovery_DropType(fsObjType);

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Exiting PersistentRecovery_Scan");
}

typedef struct RedoRelationEntryDataKey
{

	ItemPointerData	persistentTid;

} RedoRelationEntryDataKey;

typedef struct RedoRelationEntryData
{
	RedoRelationEntryDataKey	key;

	RelFileNode		relFileNode;

	int64			persistentSerialNum;

	// UNDONE: mirrorExistence
} RedoRelationEntryData;
typedef RedoRelationEntryData *RedoRelationEntry;

void
PersistentRecovery_SerializeRedoRelationFile(
	int redoRelationFile)
{
	HASH_SEQ_STATUS iterateStatus;
	FsObjEntry fsObjEntry;

	RedoRelationEntryData redoRelationEntryData;

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Entering PersistentRecovery_SerializeRedoRelationFile");

	hash_seq_init(
			&iterateStatus, 
			fsObjHashTable[PersistentFsObjType_RelationFile]);

	persistentRecoveryCount = 0;

	while (true)
	{
		int writeLen;

		fsObjEntry = 
				(FsObjEntry)
						hash_seq_search(&iterateStatus);
		if (fsObjEntry == NULL)
			break;

		if (fsObjEntry->state == PersistentFileSysState_Free ||
			fsObjEntry->state == PersistentFileSysState_AbortingCreate ||
			fsObjEntry->state == PersistentFileSysState_DropPending)
			continue;

		Assert(fsObjEntry->state == PersistentFileSysState_BulkLoadCreatePending ||
			   fsObjEntry->state == PersistentFileSysState_CreatePending ||
			   fsObjEntry->state == PersistentFileSysState_Created);

		MemSet(&redoRelationEntryData.key, 0, sizeof(RedoRelationEntryDataKey));	// Zero out any padding.
		redoRelationEntryData.key.persistentTid = fsObjEntry->key.persistentTid;
		
		redoRelationEntryData.relFileNode = PersistentFileSysObjName_GetRelFileNode(&fsObjEntry->fsObjName);
		redoRelationEntryData.persistentSerialNum = fsObjEntry->persistentSerialNum;

		writeLen = write(redoRelationFile, &redoRelationEntryData, sizeof(RedoRelationEntryData));
		if (writeLen != sizeof(RedoRelationEntryData))
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write redo relation file : %m")));
		}
		persistentRecoveryCount++;
		
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "PersistentRecovery_SerializeRedoRelationFile: %u/%u/%u, serial number " INT64_FORMAT ", TID %s",
			     redoRelationEntryData.relFileNode.spcNode,
			     redoRelationEntryData.relFileNode.dbNode,
			     redoRelationEntryData.relFileNode.relNode,
			     redoRelationEntryData.persistentSerialNum,
			     ItemPointerToString(&redoRelationEntryData.key.persistentTid));
	}
	
	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Exiting PersistentRecovery_SerializeRedoRelationFile count %d",
		     persistentRecoveryCount);
}

static HTAB* redoRelationHashTable = NULL;

static void
PersistentRecovery_RedoRelationHashTableInit(void)
{
	HASHCTL			info;
	int				hash_flags;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(RedoRelationEntryDataKey);
	info.entrysize = sizeof(RedoRelationEntryData);
	info.hash = tag_hash;

	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	redoRelationHashTable = hash_create("Redo Relation", 10, &info, hash_flags);
}

static RedoRelationEntry
PersistentRecovery_CreateRedoRelationEntry(
	RedoRelationEntry redoRelationEntry)
{
	RedoRelationEntry entry;

	bool found;

	if(RelFileNode_IsEmpty(&redoRelationEntry->relFileNode))
		elog(ERROR, "Invalid RelFileNode (0,0,0)");

	if (redoRelationHashTable == NULL)
		PersistentRecovery_RedoRelationHashTableInit();

	entry = 
			(RedoRelationEntry) 
					hash_search(redoRelationHashTable,
								(void *) &redoRelationEntry->key,
								HASH_ENTER,
								&found);

	if (found)
	{
		Assert(entry != NULL);
		elog(ERROR, 
			 "Duplicate redo relation entry: existing (%u/%u/%u, serial number " INT64_FORMAT ", TID %s), "
			 "new (%u/%u/%u, serial number " INT64_FORMAT ", TID %s)",
		     entry->relFileNode.spcNode,
		     entry->relFileNode.dbNode,
		     entry->relFileNode.relNode,
			 entry->persistentSerialNum,
		     ItemPointerToString(&entry->key.persistentTid),
			 redoRelationEntry->relFileNode.spcNode,
			 redoRelationEntry->relFileNode.dbNode,
			 redoRelationEntry->relFileNode.relNode,
		     redoRelationEntry->persistentSerialNum,
		     ItemPointerToString2(&redoRelationEntry->key.persistentTid));
	}

	entry->relFileNode = redoRelationEntry->relFileNode;
	entry->persistentSerialNum = redoRelationEntry->persistentSerialNum;

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
			 "Created redo relation entry: %u/%u/%u, serial number " INT64_FORMAT ", TID %s",
			 entry->relFileNode.spcNode,
			 entry->relFileNode.dbNode,
			 entry->relFileNode.relNode,
			 entry->persistentSerialNum,
			 ItemPointerToString(&entry->key.persistentTid));

	return entry;
}

static RedoRelationEntry
PersistentRecovery_FindRedoRelationEntry(
	ItemPointer 	persistentTid)
{
	RedoRelationEntryDataKey key;

	RedoRelationEntry foundEntry;

	bool found;

	if (redoRelationHashTable == NULL)
		PersistentRecovery_RedoRelationHashTableInit();


	MemSet(&key, 0, sizeof(RedoRelationEntryDataKey));
	key.persistentTid = *persistentTid;

	foundEntry = 
			(RedoRelationEntry) 
					hash_search(redoRelationHashTable,
								(void *) &key,
								HASH_FIND,
								&found);
	if (!found)
		return NULL;

	return foundEntry;
}


void
PersistentRecovery_DeserializeRedoRelationFile(
	int redoRelationFile)
{
	RedoRelationEntryData redoRelationEntryData;

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Entering PersistentRecovery_DeserializeRedoRelationFile");

	while(true)
	{
		int readLen;

		errno = 0;
		readLen = read(redoRelationFile, &redoRelationEntryData, sizeof(RedoRelationEntryData));

		if (readLen == 0)
			break;
		else if (readLen != sizeof(RedoRelationEntryData) && errno == 0)
			elog(ERROR, "Bad redo relation entry length (expected %d and found %d)",
			     (int)sizeof(RedoRelationEntryData), readLen);
		else if (errno != 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("error reading redo relation file: %m")));
		}
		
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "PersistentRecovery_DeserializeRedoRelationFile: %u/%u/%u, serial number " INT64_FORMAT ", TID %s",
			     redoRelationEntryData.relFileNode.spcNode,
			     redoRelationEntryData.relFileNode.dbNode,
			     redoRelationEntryData.relFileNode.relNode,
			     redoRelationEntryData.persistentSerialNum,
			     ItemPointerToString(&redoRelationEntryData.key.persistentTid));

		if(RelFileNode_IsEmpty(&redoRelationEntryData.relFileNode))
			elog(ERROR, "Invalid RelFileNode (0,0,0)");

		PersistentRecovery_CreateRedoRelationEntry(&redoRelationEntryData);
	}
	
	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(), 
		     "Exiting PersistentRecovery_DeserializeRedoRelationFile count %d",
		     persistentRecoveryCount);
}

static bool
PersistentRecovery_RedoRelationExists(
	ItemPointer		persistentTid,
	
	int64 			persistentSerialNum,
	
	RelFileNode 	*relFileNode)	
{
	RedoRelationEntry redoRelationEntry;

	redoRelationEntry = PersistentRecovery_FindRedoRelationEntry(persistentTid);
	if (redoRelationEntry == NULL)
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "PersistentRecovery_RedoRelationExists: TID %s is not in the currently 'Created' set (serial number " INT64_FORMAT ", %u/%u/%u)",
				 ItemPointerToString(persistentTid),
				 persistentSerialNum,
				 relFileNode->spcNode,
				 relFileNode->dbNode,
				 relFileNode->relNode);
		return false;
	}
	if (redoRelationEntry->persistentSerialNum != persistentSerialNum)
	{
		if (Debug_persistent_recovery_print)
			elog(PersistentRecovery_DebugPrintLevel(), 
				 "PersistentRecovery_RedoRelationExists: TID %s entry from the 'Created' set has different serial number (serial number " INT64_FORMAT ", %u/%u/%u) indicating XLOG for persistent obsolete file-system object (obsolete serial number " INT64_FORMAT ", %u/%u/%u)",
				 ItemPointerToString(persistentTid),
				 redoRelationEntry->persistentSerialNum,
				 redoRelationEntry->relFileNode.spcNode,
				 redoRelationEntry->relFileNode.dbNode,
				 redoRelationEntry->relFileNode.relNode,
				 persistentSerialNum,
 				 relFileNode->spcNode,
 				 relFileNode->dbNode,
 				 relFileNode->relNode);
		return false;
	}

	return true;
}

