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
 * cdbpersistentfilesysobj.c
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
#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "catalog/gp_persistent.h"
#include "catalog/gp_segment_config.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbpersistentfilespace.h"
#include "cdb/cdbpersistenttablespace.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbdirectopen.h"
#include "cdb/cdbresynchronizechangetracking.h"
#include "cdb/cdbmirroredbufferpool.h"
#include "cdb/cdbmirroredappendonly.h"
#include "cdb/cdbpersistentrecovery.h"
#include "cdb/cdbpersistentrelfile.h"
#include "cdb/cdbpersistentrelation.h"
#include "cdb/cdbpersistentdatabase.h"
#include "cdb/cdbglobalsequence.h"
#include "commands/filespace.h"
#include "commands/tablespace.h"

#include "storage/itemptr.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"

#include "cdb/cdbvars.h"

typedef struct PersistentFileSysObjPrivateSharedMemory
{
	bool	needInitScan;
} PersistentFileSysObjPrivateSharedMemory;

typedef struct ReadTupleForUpdateInfo
{
	PersistentFsObjType 		fsObjType;

	PersistentFileSysObjName 	*fsObjName;

	ItemPointerData				persistentTid;

	int64						persistentSerialNum;
} ReadTupleForUpdateInfo;

#define READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE \
	ErrorContextCallback readTupleForUpdateErrContext; \
	ReadTupleForUpdateInfo readTupleForUpdateInfo;

#define READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHNAME(name, tid, serialNum) \
{ \
	readTupleForUpdateInfo.fsObjType = (name)->type; \
    readTupleForUpdateInfo.fsObjName = (name); \
	readTupleForUpdateInfo.persistentTid = *(tid); \
	readTupleForUpdateInfo.persistentSerialNum = serialNum; \
\
	/* Setup error traceback support for ereport() */ \
	readTupleForUpdateErrContext.callback = PersistentFileSysObj_ReadForUpdateErrContext; \
	readTupleForUpdateErrContext.arg = (void *) &readTupleForUpdateInfo; \
	readTupleForUpdateErrContext.previous = error_context_stack; \
	error_context_stack = &readTupleForUpdateErrContext; \
}

#define READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHTYPE(type, tid, serialNum) \
{ \
	readTupleForUpdateInfo.fsObjType = (type); \
    readTupleForUpdateInfo.fsObjName = NULL; \
	readTupleForUpdateInfo.persistentTid = *(tid); \
	readTupleForUpdateInfo.persistentSerialNum = (serialNum); \
\
	/* Setup error traceback support for ereport() */ \
	readTupleForUpdateErrContext.callback = PersistentFileSysObj_ReadForUpdateErrContext; \
	readTupleForUpdateErrContext.arg = (void *) &readTupleForUpdateInfo; \
	readTupleForUpdateErrContext.previous = error_context_stack; \
	error_context_stack = &readTupleForUpdateErrContext; \
}


#define READTUPLE_FOR_UPDATE_ERRCONTEXT_POP \
{ \
	/* Pop the error context stack */ \
	error_context_stack = readTupleForUpdateErrContext.previous; \
}

static PersistentFileSysObjPrivateSharedMemory *persistentFileSysObjPrivateSharedData = NULL;

void PersistentFileSysObj_InitShared(
	PersistentFileSysObjSharedData 	*fileSysObjSharedData)
{
	PersistentStore_InitShared(&fileSysObjSharedData->storeSharedData);
}

static PersistentFileSysObjData* fileSysObjDataArray[CountPersistentFsObjType];

static PersistentFileSysObjSharedData* fileSysObjSharedDataArray[CountPersistentFsObjType];

static void PersistentFileSysObj_PrintRelationFile(
	int						elevel,
	char					*prefix,
	ItemPointer 			persistentTid,
	Datum					*values)
{
	RelFileNode		relFileNode;
	int32			segmentFileNum;

	PersistentFileSysRelStorageMgr relationStorageManager;

	PersistentFileSysState	state;

	PersistentFileSysRelBufpoolKind relBufpoolKind;

	TransactionId			parentXid;
	int64					persistentSerialNum;
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
									&persistentSerialNum,
									&previousFreeTid,
									&sharedStorage);

	if (state == PersistentFileSysState_Free)
	{
		elog(elevel,
			 "%s gp_persistent_relation_node %s: Free. Free order number " INT64_FORMAT ", previous free TID %s",
			 prefix,
			 ItemPointerToString(persistentTid),
			 persistentSerialNum,
			 ItemPointerToString2(&previousFreeTid));
	}
#if 0
	else if (relationStorageManager == PersistentFileSysRelStorageMgr_BufferPool)
	{
		elog(elevel,
			 "%s gp_persistent_relation_node %s: %u/%u/%u, segment file #%d, contentid %d, relation storage manager '%s', persistent state '%s', create mirror data loss tracking session num " INT64_FORMAT ", "
			 "mirror existence state '%s', data synchronization state '%s', "
			 "Buffer Pool (marked for scan incremental resync = %s, "
			 "resync changed page count " INT64_FORMAT ", "
			 "resync checkpoint loc %s, resync checkpoint block num %u), "
			 "relation buffer pool kind %u, parent xid %u, "
			 "persistent serial num " INT64_FORMAT ", previous free TID %s",
			 prefix,
			 ItemPointerToString(persistentTid),
			 relFileNode.spcNode,
			 relFileNode.dbNode,
			 relFileNode.relNode,
			 segmentFileNum,
			 contentid,
			 PersistentFileSysRelStorageMgr_Name(relationStorageManager),
			 PersistentFileSysObjState_Name(state),
			 createMirrorDataLossTrackingSessionNum,
			 MirroredObjectExistenceState_Name(mirrorExistenceState),
			 MirroredRelDataSynchronizationState_Name(mirrorDataSynchronizationState),
			 (mirrorBufpoolMarkedForScanIncrementalResync ? "true" : "false"),
			 mirrorBufpoolResyncChangedPageCount,
			 XLogLocationToString(&mirrorBufpoolResyncCkptLoc),
			 (unsigned int)mirrorBufpoolResyncCkptBlockNum,
			 relBufpoolKind,
			 parentXid,
			 persistentSerialNum,
			 ItemPointerToString2(&previousFreeTid));
	}
	else
	{
		Assert(relationStorageManager == PersistentFileSysRelStorageMgr_AppendOnly);
		elog(elevel,
			 "%s gp_persistent_relation_node %s: %u/%u/%u, segment file #%d, contentid %d, relation storage manager '%s', persistent state '%s', create mirror data loss tracking session num " INT64_FORMAT ", "
			 "mirror existence state '%s', data synchronization state '%s', "
			 "Append-Only (loss EOF " INT64_FORMAT ", new EOF " INT64_FORMAT ", equal = %s), "
			 "relation buffer pool kind %u, parent xid %u, "
			 "persistent serial num " INT64_FORMAT ", previous free TID %s",
			 prefix,
			 ItemPointerToString(persistentTid),
			 relFileNode.spcNode,
			 relFileNode.dbNode,
			 relFileNode.relNode,
			 segmentFileNum,
			 contentid,
			 PersistentFileSysRelStorageMgr_Name(relationStorageManager),
			 PersistentFileSysObjState_Name(state),
			 createMirrorDataLossTrackingSessionNum,
			 MirroredObjectExistenceState_Name(mirrorExistenceState),
			 MirroredRelDataSynchronizationState_Name(mirrorDataSynchronizationState),
			 mirrorAppendOnlyLossEof,
			 mirrorAppendOnlyNewEof,
			 ((mirrorAppendOnlyLossEof == mirrorAppendOnlyNewEof) ? "true" : "false"),
			 relBufpoolKind,
			 parentXid,
			 persistentSerialNum,
			 ItemPointerToString2(&previousFreeTid));
	}
#endif
}

static void PersistentFileSysObj_PrintRelationDir(
				int elevel,
				char *prefix,
				ItemPointer persistentTid,
				Datum *values)
{
	RelFileNode relFileNode;
	PersistentFileSysState state;

	int32 reserved;
	TransactionId parentXid;
	int64 persistentSerialNum;
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
									&persistentSerialNum,
									&previousFreeTid,
									&sharedStorage);

	if (PersistentFileSysState_Free == state)
	{
		elog(elevel,
				"%s gp_persistent_relation_node %s: Free. Free order number " INT64_FORMAT ", previous free TID %s",
				prefix,
				ItemPointerToString(persistentTid),
				persistentSerialNum,
				ItemPointerToString2(&previousFreeTid));
	}
	else
	{
		elog(elevel,
				"%s gp_persistent_relation_node %s: %u/%u/%u, persistent state '%s', "
				"reserved %u, parent xid %u, "
				"persistent serial num " INT64_FORMAT ", previous free TID %s",
				prefix,
				ItemPointerToString(persistentTid),
				relFileNode.spcNode,
				relFileNode.dbNode,
				relFileNode.relNode,
				PersistentFileSysObjState_Name(state),
				reserved,
				parentXid,
				persistentSerialNum,
				ItemPointerToString2(&previousFreeTid));
	}
}

static void PersistentFileSysObj_PrintDatabaseDir(
	int						elevel,
	char					*prefix,
	ItemPointer 			persistentTid,
	Datum					*values)
{
	DbDirNode		dbDirNode;

	PersistentFileSysState	state;

	int32					reserved;
	TransactionId			parentXid;
	int64					persistentSerialNum;
	ItemPointerData			previousFreeTid;
	bool					sharedStorage;

	GpPersistentDatabaseNode_GetValues(
									values,
									&dbDirNode.tablespace,
									&dbDirNode.database,
									&state,
									&reserved,
									&parentXid,
									&persistentSerialNum,
									&previousFreeTid,
									&sharedStorage);

	if (state == PersistentFileSysState_Free)
	{
		elog(elevel,
			 "%s gp_persistent_database_node %s: Free. Free order number " INT64_FORMAT ", previous free TID %s",
			 prefix,
			 ItemPointerToString(persistentTid),
			 persistentSerialNum,
			 ItemPointerToString2(&previousFreeTid));
	}
	else
	{
		elog(elevel,
			 "%s gp_persistent_database_node %s: %u/%u, persistent state '%s', "
			 "reserved %u, parent xid %u, "
			 "persistent serial num " INT64_FORMAT ", previous free TID %s",
			 prefix,
			 ItemPointerToString(persistentTid),
			 dbDirNode.tablespace,
			 dbDirNode.database,
			 PersistentFileSysObjState_Name(state),
			 reserved,
			 parentXid,
			 persistentSerialNum,
			 ItemPointerToString2(&previousFreeTid));
	}
}

static void PersistentFileSysObj_PrintTablespaceDir(
	int						elevel,
	char					*prefix,
	ItemPointer 			persistentTid,
	Datum					*values)
{
	Oid		filespaceOid;
	Oid		tablespaceOid;

	PersistentFileSysState	state;

	int32					reserved;
	TransactionId			parentXid;
	int64					persistentSerialNum;
	ItemPointerData			previousFreeTid;
	bool					sharedStorage;

	GpPersistentTablespaceNode_GetValues(
									values,
									&filespaceOid,
									&tablespaceOid,
									&state,
									&reserved,
									&parentXid,
									&persistentSerialNum,
									&previousFreeTid,
									&sharedStorage);

	if (state == PersistentFileSysState_Free)
	{
		elog(elevel,
			 "%s gp_persistent_tablespace_node %s: Free. Free order number " INT64_FORMAT ", previous free TID %s",
			 prefix,
			 ItemPointerToString(persistentTid),
			 persistentSerialNum,
			 ItemPointerToString2(&previousFreeTid));
	}
	else
	{
		elog(elevel,
			 "%s gp_persistent_tablespace_node %s: tablespace %u (filespace %u), persistent state '%s'"
			 "reserved %u, parent xid %u, "
			 "persistent serial num " INT64_FORMAT ", previous free TID %s",
			 prefix,
			 ItemPointerToString(persistentTid),
			 tablespaceOid,
			 filespaceOid,
			 PersistentFileSysObjState_Name(state),
			 reserved,
			 parentXid,
			 persistentSerialNum,
			 ItemPointerToString2(&previousFreeTid));
	}
}

static void PersistentFileSysObj_PrintFilespaceDir(
	int						elevel,
	char					*prefix,
	ItemPointer 			persistentTid,
	Datum					*values)
{
	Oid		filespaceOid;

	int16	dbId1;
	char	locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen];

	PersistentFileSysState	state;

	int32					reserved;
	TransactionId			parentXid;
	int64					persistentSerialNum;
	ItemPointerData			previousFreeTid;
	bool					sharedStorage;

	GpPersistentFilespaceNode_GetValues(
									values,
									&filespaceOid,
									&dbId1,
									locationBlankPadded1,
									&state,
									&reserved,
									&parentXid,
									&persistentSerialNum,
									&previousFreeTid,
									&sharedStorage);

	if (state == PersistentFileSysState_Free)
	{
		elog(elevel,
			 "%s gp_persistent_filespace_node %s: Free. Free order number " INT64_FORMAT ", previous free TID %s",
			 prefix,
			 ItemPointerToString(persistentTid),
			 persistentSerialNum,
			 ItemPointerToString2(&previousFreeTid));
	}
	else
	{
		char *filespaceLocation1;

		PersistentFilespace_ConvertBlankPaddedLocation(
												&filespaceLocation1,
												locationBlankPadded1,
												/* isPrimary */ false);	// Always say false so we don't error out here.
		elog(elevel,
			 "%s gp_persistent_filespace_node %s: filespace %u, #1 (dbid %u, location '%s'), persistent state '%s', "
			 "reserved %u, parent xid %u, "
			 "persistent serial num " INT64_FORMAT ", previous free TID %s",
			 prefix,
			 ItemPointerToString(persistentTid),
			 filespaceOid,
			 dbId1,
			 (filespaceLocation1 == NULL ? "<empty>" : filespaceLocation1),
			 PersistentFileSysObjState_Name(state),
			 reserved,
			 parentXid,
			 persistentSerialNum,
			 ItemPointerToString2(&previousFreeTid));

		if (filespaceLocation1 != NULL)
			pfree(filespaceLocation1);
	}
}

static void PersistentFileSysObj_InitOurs(
	PersistentFileSysObjData 	*fileSysObjData,

	PersistentFileSysObjSharedData 	*fileSysObjSharedData,

	PersistentFsObjType			fsObjType,

	int 						attNumPersistentState,

	int 						attNumParentXid)
{
	if (fsObjType < PersistentFsObjType_First ||
		fsObjType > PersistentFsObjType_Last)
		elog(ERROR, "Persistent file-system object type %d out-of-range",
		     fsObjType);

	fileSysObjDataArray[fsObjType] = fileSysObjData;
	fileSysObjSharedDataArray[fsObjType] = fileSysObjSharedData;

	fileSysObjData->fsObjType = fsObjType;
	fileSysObjData->attNumPersistentState = attNumPersistentState;
	fileSysObjData->attNumParentXid = attNumParentXid;
}

void PersistentFileSysObj_Init(
	PersistentFileSysObjData 			*fileSysObjData,

	PersistentFileSysObjSharedData 		*fileSysObjSharedData,

	PersistentFsObjType					fsObjType,

	PersistentStoreScanTupleCallback	scanTupleCallback)
{
	PersistentStoreData *storeData = &fileSysObjData->storeData;

	fileSysObjData->fsObjType = fsObjType;

	switch (fsObjType)
	{
	case PersistentFsObjType_RelationFile:
		PersistentStore_Init(
						storeData,
						"gp_persistent_relfile_node",
						GpGlobalSequence_PersistentRelfile,
						DirectOpen_GpPersistentRelfileNodeOpenShared,
						DirectOpen_GpPersistentRelfileNodeClose,
						scanTupleCallback,
						PersistentFileSysObj_PrintRelationFile,
						Natts_gp_persistent_relfile_node,
						Anum_gp_persistent_relfile_node_persistent_serial_num,
						Anum_gp_persistent_relfile_node_previous_free_tid);
		PersistentFileSysObj_InitOurs(
						fileSysObjData,
						fileSysObjSharedData,
						fsObjType,
						Anum_gp_persistent_relfile_node_persistent_state,
						Anum_gp_persistent_relfile_node_parent_xid);
		break;

	case PersistentFsObjType_RelationDir:
		PersistentStore_Init(
						storeData,
						"gp_persistent_relation_node",
						GpGlobalSequence_PersistentRelation,
						DirectOpen_GpPersistentRelationNodeOpenShared,
						DirectOpen_GpPersistentRelationNodeClose,
						scanTupleCallback,
						PersistentFileSysObj_PrintRelationDir,
						Natts_gp_persistent_relation_node,
						Anum_gp_persistent_relation_node_persistent_serial_num,
						Anum_gp_persistent_relation_node_previous_free_tid);
		PersistentFileSysObj_InitOurs(
						fileSysObjData,
						fileSysObjSharedData,
						fsObjType,
						Anum_gp_persistent_relation_node_persistent_state,
						Anum_gp_persistent_relation_node_parent_xid);
		break;

	case PersistentFsObjType_DatabaseDir:
		PersistentStore_Init(
						storeData,
						"gp_persistent_database_node",
						GpGlobalSequence_PersistentDatabase,
						DirectOpen_GpPersistentDatabaseNodeOpenShared,
						DirectOpen_GpPersistentDatabaseNodeClose,
						scanTupleCallback,
						PersistentFileSysObj_PrintDatabaseDir,
						Natts_gp_persistent_database_node,
						Anum_gp_persistent_database_node_persistent_serial_num,
						Anum_gp_persistent_database_node_previous_free_tid);
		PersistentFileSysObj_InitOurs(
						fileSysObjData,
						fileSysObjSharedData,
						fsObjType,
						Anum_gp_persistent_database_node_persistent_state,
						Anum_gp_persistent_database_node_parent_xid);
		break;

	case PersistentFsObjType_TablespaceDir:
		PersistentStore_Init(
						storeData,
						"gp_persistent_tablespace_node",
						GpGlobalSequence_PersistentTablespace,
						DirectOpen_GpPersistentTableSpaceNodeOpenShared,
						DirectOpen_GpPersistentTableSpaceNodeClose,
						scanTupleCallback,
						PersistentFileSysObj_PrintTablespaceDir,
						Natts_gp_persistent_tablespace_node,
						Anum_gp_persistent_tablespace_node_persistent_serial_num,
						Anum_gp_persistent_tablespace_node_previous_free_tid);
		PersistentFileSysObj_InitOurs(
						fileSysObjData,
						fileSysObjSharedData,
						fsObjType,
						Anum_gp_persistent_tablespace_node_persistent_state,
						Anum_gp_persistent_tablespace_node_parent_xid);
		break;

	case PersistentFsObjType_FilespaceDir:
		PersistentStore_Init(
						storeData,
						"gp_persistent_filespace_node",
						GpGlobalSequence_PersistentFilespace,
						DirectOpen_GpPersistentFileSpaceNodeOpenShared,
						DirectOpen_GpPersistentFileSpaceNodeClose,
						scanTupleCallback,
						PersistentFileSysObj_PrintFilespaceDir,
						Natts_gp_persistent_filespace_node,
						Anum_gp_persistent_filespace_node_persistent_serial_num,
						Anum_gp_persistent_filespace_node_previous_free_tid);
		PersistentFileSysObj_InitOurs(
						fileSysObjData,
						fileSysObjSharedData,
						fsObjType,
						Anum_gp_persistent_filespace_node_persistent_state,
						Anum_gp_persistent_filespace_node_parent_xid);
		break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjType);
	}
}

/*
 * Reset the persistent shared-memory free list heads and shared-memory hash tables.
 */
void PersistentFileSysObj_Reset(void)
{
	PersistentFsObjType	fsObjType;

	for (fsObjType = PersistentFsObjType_First;
		 fsObjType <= PersistentFsObjType_Last;
		 fsObjType++)
	{
		PersistentFileSysObjData		 *fileSysObjData;
		PersistentFileSysObjSharedData  *fileSysObjSharedData;

		PersistentFileSysObj_GetDataPtrs(
									 fsObjType,
									 &fileSysObjData,
									 &fileSysObjSharedData);

		PersistentStore_ResetFreeList(
						 &fileSysObjData->storeData,
						 &fileSysObjSharedData->storeSharedData);

		switch (fsObjType)
		{
		case PersistentFsObjType_RelationFile:
			PersistentRelfile_Reset();
			break;

		case PersistentFsObjType_RelationDir:
			PersistentRelation_Reset();
			break;

		case PersistentFsObjType_DatabaseDir:
			PersistentDatabase_Reset();
			break;

		case PersistentFsObjType_TablespaceDir:
			PersistentTablespace_Reset();
			break;

		case PersistentFsObjType_FilespaceDir:
			/*
			 * Filespace persistent table is not reset (PersistentFilespace_Reset(); is not called)
			 * since it cannot be re-built on segments.
			 * 'gp_filespace' table does not exist on the segments by design.
			 */
			break;

		default:
			elog(ERROR, "Unexpected persistent file-system object type: %d",
				 fsObjType);
		}
	}
}

void PersistentFileSysObj_GetDataPtrs(
	PersistentFsObjType				fsObjType,

	PersistentFileSysObjData		**fileSysObjData,

	PersistentFileSysObjSharedData	**fileSysObjSharedData)
{
	if (fsObjType < PersistentFsObjType_First ||
		fsObjType > PersistentFsObjType_Last)
		elog(PANIC, "Persistent file-system object type %d out-of-range",
		     fsObjType);

	*fileSysObjData = fileSysObjDataArray[fsObjType];
	if (*fileSysObjData == NULL)
		elog(PANIC, "Persistent file-system object type %d memory not initialized",
		     fsObjType);
	if ((*fileSysObjData)->fsObjType != fsObjType)
		elog(PANIC, "Persistent file-system data's type %d doesn't match expected %d",
		     (*fileSysObjData)->fsObjType, fsObjType);

	*fileSysObjSharedData = fileSysObjSharedDataArray[fsObjType];
	if (*fileSysObjSharedData == NULL)
		elog(PANIC, "Persistent file-system object type %d shared-memory not initialized",
		     fsObjType);
}

static void PersistentFileSysObj_DoInitScan(void)
{
	PersistentFsObjType				fsObjType;

	PersistentFileSysObjData		*fileSysObjData;
	PersistentFileSysObjSharedData	*fileSysObjSharedData;

	Assert(persistentFileSysObjPrivateSharedData->needInitScan);

	for (fsObjType = PersistentFsObjType_First;
		 fsObjType <= PersistentFsObjType_Last;
		 fsObjType++)
	{
		 PersistentFileSysObj_GetDataPtrs(
									 fsObjType,
									 &fileSysObjData,
									 &fileSysObjSharedData);
		 PersistentStore_InitScanUnderLock(
						 &fileSysObjData->storeData,
						 &fileSysObjSharedData->storeSharedData);
	}

	 persistentFileSysObjPrivateSharedData->needInitScan = false;
}

void PersistentFileSysObj_BuildInitScan(void)
{
	if (persistentFileSysObjPrivateSharedData == NULL)
		elog(PANIC, "Persistent File-System Object shared-memory not initialized");

	if (!persistentFileSysObjPrivateSharedData->needInitScan)
	{
		// gp_persistent_build_db can be called multiple times.
		return;
	}

	PersistentFileSysObj_DoInitScan();
}

void PersistentFileSysObj_StartupInitScan(void)
{
	if (persistentFileSysObjPrivateSharedData == NULL)
		elog(PANIC, "Persistent File-System Object shared-memory not initialized");

	/*
	 * We only scan persistent meta-data from with Startup phase 1.
	 * Build is only done in special states.
	 */
	if (!persistentFileSysObjPrivateSharedData->needInitScan)
	{
		elog(PANIC, "We only scan persistent meta-data once during Startup Pass 1");
	}

	PersistentFileSysObj_DoInitScan();
}

void PersistentFileSysObj_VerifyInitScan(void)
{
	if (persistentFileSysObjPrivateSharedData == NULL)
		elog(PANIC, "Persistent File-System Object shared-memory not initialized");

	if (persistentFileSysObjPrivateSharedData->needInitScan)
	{
		elog(PANIC, "Persistent File-System Object meta-data not scanned into memory");
	}
}

void PersistentFileSysObj_FlushXLog(void)
{
	PersistentStore_FlushXLog();
}

void PersistentFileSysObj_Scan(
	PersistentFsObjType			fsObjType,

	PersistentStoreScanTupleCallback	scanTupleCallback)
{
	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	PersistentStore_Scan(
					&(fileSysObjDataArray[fsObjType]->storeData),
					&(fileSysObjSharedDataArray[fsObjType]->storeSharedData),
					scanTupleCallback);

}

int64 PersistentFileSysObj_MyHighestSerialNum(
	PersistentFsObjType 	fsObjType)
{
	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	return PersistentStore_MyHighestSerialNum(
								&fileSysObjData->storeData);
}

int64 PersistentFileSysObj_CurrentMaxSerialNum(
	PersistentFsObjType 	fsObjType)
{
	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	return PersistentStore_CurrentMaxSerialNum(
								&fileSysObjSharedData->storeSharedData);
}

PersistentTidIsKnownResult PersistentFileSysObj_TidIsKnown(
	PersistentFsObjType 	fsObjType,

	ItemPointer 			persistentTid,

	ItemPointer 			maxTid)
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	PersistentTidIsKnownResult result;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	result = PersistentStore_TidIsKnown(
								&fileSysObjSharedData->storeSharedData,
								persistentTid,
								maxTid);

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;

	return result;
}

void PersistentFileSysObj_UpdateTuple(
	PersistentFsObjType		fsObjType,

	ItemPointer 			persistentTid,
				/* TID of the stored tuple. */

	Datum 					*values,

	bool					flushToXLog)
				/* When true, the XLOG record for this change will be flushed to disk. */
{
	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	PersistentStore_UpdateTuple(
							&(fileSysObjDataArray[fsObjType]->storeData),
							&(fileSysObjSharedDataArray[fsObjType]->storeSharedData),
							persistentTid,
							values,
							flushToXLog);
}

void PersistentFileSysObj_ReplaceTuple(
	PersistentFsObjType		fsObjType,

	ItemPointer 			persistentTid,
				/* TID of the stored tuple. */

	HeapTuple				tuple,

	Datum 					*newValues,

	bool					*replaces,

	bool					flushToXLog)
				/* When true, the XLOG record for this change will be flushed to disk. */
{
	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	PersistentStore_ReplaceTuple(
							&(fileSysObjDataArray[fsObjType]->storeData),
							&(fileSysObjSharedDataArray[fsObjType]->storeSharedData),
							persistentTid,
							tuple,
							newValues,
							replaces,
							flushToXLog);
}


void PersistentFileSysObj_ReadTuple(
	PersistentFsObjType			fsObjType,

	ItemPointer					readTid,

	Datum						*values,

	HeapTuple					*tupleCopy)
{
	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	if (PersistentStore_IsZeroTid(readTid))
		elog(ERROR, "TID for fetch persistent '%s' tuple is invalid (0,0)",
			 PersistentFileSysObjName_TypeName(fsObjType));

	PersistentStore_ReadTuple(
						&(fileSysObjDataArray[fsObjType]->storeData),
						&(fileSysObjSharedDataArray[fsObjType]->storeSharedData),
						readTid,
						values,
						tupleCopy);
}

void PersistentFileSysObj_AddTuple(
	PersistentFsObjType			fsObjType,

	Datum						*values,

	bool						flushToXLog,
				/* When true, the XLOG record for this change will be flushed to disk. */

	ItemPointer 				persistentTid,
				/* TID of the stored tuple. */

	int64					*persistentSerialNum)
{
	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	PersistentStore_AddTuple(
						&(fileSysObjDataArray[fsObjType]->storeData),
						&(fileSysObjSharedDataArray[fsObjType]->storeSharedData),
						values,
						flushToXLog,
						persistentTid,
						persistentSerialNum);
}

void PersistentFileSysObj_FreeTuple(
	PersistentFileSysObjData 		*fileSysObjData,

	PersistentFileSysObjSharedData 	*fileSysObjSharedData,

	PersistentFsObjType			fsObjType,

	ItemPointer 				persistentTid,
				/* TID of the stored tuple. */

	bool						flushToXLog)
				/* When true, the XLOG record for this change will be flushed to disk. */
{
	Datum *freeValues;

	ItemPointerData previousFreeTid;

	Assert(fsObjType >= PersistentFsObjType_First);
	Assert(fsObjType <= PersistentFsObjType_Last);

	freeValues = (Datum*)palloc(fileSysObjData->storeData.numAttributes * sizeof(Datum));

	MemSet(&previousFreeTid, 0, sizeof(ItemPointerData));

	switch (fsObjType)
	{
	case PersistentFsObjType_RelationFile:
		{
			XLogRecPtr mirrorBufpoolResyncCkptLoc;

			MemSet(&mirrorBufpoolResyncCkptLoc, 0, sizeof(XLogRecPtr));
			GpPersistentRelfileNode_SetDatumValues(
												freeValues,
												/* tablespaceOid */ 0,
												/* databaseOid */ 0,
												/* relFileNodeOid */ 0,
												/* segmentFileNum */ 0,
												PersistentFileSysRelStorageMgr_None,
												PersistentFileSysState_Free,
												PersistentFileSysRelBufpoolKind_None,
												InvalidTransactionId,
												/* persistentSerialNum */ 0,
												&previousFreeTid,
												false);
		}
		break;

	case PersistentFsObjType_RelationDir:
		{
			GpPersistentRelationNode_SetDatumValues(
											freeValues,
											/* tablespaceOid */ 0,
											/* databaseOid */ 0,
											/* relfilenodeOid */ 0,
											PersistentFileSysState_Free,
											/* reserved */ 0,
											InvalidTransactionId,
											/* persistentSerialNum */ 0,
											&previousFreeTid,
											false);
		}
		break;

	case PersistentFsObjType_DatabaseDir:
		GpPersistentDatabaseNode_SetDatumValues(
											freeValues,
											/* tablespaceOid */ 0,
											/* databaseOid */ 0,
											PersistentFileSysState_Free,
											/* reserved */ 0,
											InvalidTransactionId,
											/* persistentSerialNum */ 0,
											&previousFreeTid,
											false);
		break;

	case PersistentFsObjType_TablespaceDir:
		GpPersistentTablespaceNode_SetDatumValues(
											freeValues,
											/* filespaceOid */ 0,
											/* tablespaceOid */ 0,
											PersistentFileSysState_Free,
											InvalidTransactionId,
											/* persistentSerialNum */ 0,
											&previousFreeTid,
											false);
		break;

	case PersistentFsObjType_FilespaceDir:
		{
			char	locationBlankFilled[FilespaceLocationBlankPaddedWithNullTermLen];

			MemSet(locationBlankFilled, ' ', FilespaceLocationBlankPaddedWithNullTermLen - 1);
			locationBlankFilled[FilespaceLocationBlankPaddedWithNullTermLen - 1] = '\0';

			GpPersistentFilespaceNode_SetDatumValues(
												freeValues,
												/* filespaceOid */ 0,
												locationBlankFilled,
												PersistentFileSysState_Free,
												/* reserved */ 0,
												InvalidTransactionId,
												/* persistentSerialNum */ 0,
												&previousFreeTid,
												false);
		}
		break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjType);
	}

	PersistentStore_FreeTuple(
						&fileSysObjData->storeData,
						&fileSysObjSharedData->storeSharedData,
						persistentTid,
						freeValues,
						flushToXLog);
}

static void
PersistentFileSysObj_ReadForUpdateErrContext(void *arg)
{
	ReadTupleForUpdateInfo *info = (ReadTupleForUpdateInfo*) arg;

	if (info->fsObjName != NULL)
	{
		errcontext(
			 "Reading tuple TID %s for possible update to persistent file-system object %s, persistent serial number " INT64_FORMAT,
			 ItemPointerToString(&info->persistentTid),
			 PersistentFileSysObjName_TypeAndObjectName(info->fsObjName),
			 info->persistentSerialNum);
	}
	else
	{
		errcontext(
			 "Reading tuple TID %s for possible update to persistent file-system object type %s, persistent serial number " INT64_FORMAT,
			 ItemPointerToString(&info->persistentTid),
			 PersistentFileSysObjName_TypeName(info->fsObjType),
			 info->persistentSerialNum);
	}
}

/*
 * errcontext_persistent_relation_state_change
 *
 * Add an errcontext() line showing the expected relation path, serial number,
 * expected state(s), next state, and persistent TID.
 */
static int
errcontext_persistent_relation_state_change(
	PersistentFileSysObjName	*fsObjName,

	ItemPointer 			persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64					serialNum,

	PersistentFileSysState	expectedState1,

	PersistentFileSysState	expectedState2,

	PersistentFileSysState	expectedState3,

	PersistentFileSysState	nextState)

{
	char states[100];

	if ((int)expectedState2 == -1)
		sprintf(states, "state '%s'",
		        PersistentFileSysObjState_Name(expectedState1));
	else if ((int)expectedState3 == -1)
		sprintf(states, "states '%s' or '%s'",
			    PersistentFileSysObjState_Name(expectedState1),
			    PersistentFileSysObjState_Name(expectedState2));
	else
		sprintf(states, "states '%s', '%s', or '%s'",
			    PersistentFileSysObjState_Name(expectedState1),
			    PersistentFileSysObjState_Name(expectedState2),
			    PersistentFileSysObjState_Name(expectedState3));

	errcontext(
		 "State change is for persistent '%s' to go from %s to state '%s' with serial number " INT64_FORMAT " at TID %s",
		 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
		 states,
		 PersistentFileSysObjState_Name(nextState),
		 serialNum,
		 ItemPointerToString(persistentTid));

	return 0;
}

static PersistentFileSysObjVerifyExpectedResult PersistentFileSysObj_VerifyExpected(
	PersistentFileSysObjData 		*fileSysObjData,

	PersistentFileSysObjSharedData 	*fileSysObjSharedData,

	PersistentFileSysObjName		*fsObjName,

	ItemPointer 					persistentTid,
								/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64							serialNum,

	Datum 							*values,

	PersistentFileSysState			expectedState1,

	PersistentFileSysState			expectedState2,

	PersistentFileSysState			expectedState3,

	PersistentFileSysState			nextState,

	bool							retryPossible,

	PersistentFileSysState			*oldState)
{
	PersistentFsObjType				fsObjType;

	PersistentFileSysObjName 		actualFsObjName;
	PersistentFileSysState			actualState;

	TransactionId				actualParentXid;
	int64						actualSerialNum;

	bool expectedStateMatch;

	int elevel;

	elevel = (gp_persistent_statechange_suppress_error ? WARNING : ERROR);

	fsObjType = fsObjName->type;

	if (Debug_persistent_print)
		(*fileSysObjData->storeData.printTupleCallback)(
									Persistent_DebugPrintLevel(),
									"VEFIFY EXPECTED",
									persistentTid,
									values);

	GpPersistent_GetCommonValues(
							fsObjType,
							values,
							&actualFsObjName,
							&actualState,
							&actualParentXid,
							&actualSerialNum);

	if (oldState != NULL)
		*oldState = actualState;

	expectedStateMatch = (actualState == expectedState1);
	if (!expectedStateMatch && ((int)expectedState2 != -1))
		expectedStateMatch = (actualState == expectedState2);
	if (!expectedStateMatch && ((int)expectedState3 != -1))
		expectedStateMatch = (actualState == expectedState3);

	if (actualState == PersistentFileSysState_Free)
	{
		if ((nextState == PersistentFileSysState_AbortingCreate ||
			 nextState == PersistentFileSysState_DropPending)
			 &&
			 retryPossible)
		{
			return PersistentFileSysObjVerifyExpectedResult_DeleteUnnecessary;
		}

		ereport(elevel,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("Did not expect to find a 'Free' entry"),
				 errcontext_persistent_relation_state_change(
								 						fsObjName,
								 						persistentTid,
								 						serialNum,
								 						expectedState1,
								 						expectedState2,
								 						expectedState3,
								 						nextState)));
		return PersistentFileSysObjVerifyExpectedResult_ErrorSuppressed;
	}

	if (actualSerialNum != serialNum)
	{
		if ((nextState == PersistentFileSysState_AbortingCreate ||
			 nextState == PersistentFileSysState_DropPending)
			 &&
			 retryPossible)
		{
			return PersistentFileSysObjVerifyExpectedResult_DeleteUnnecessary;
		}

		ereport(elevel,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("Found different serial number " INT64_FORMAT " than expected (persistent file-system object found is '%s', state '%s')",
				        actualSerialNum,
						PersistentFileSysObjName_TypeAndObjectName(&actualFsObjName),
				        PersistentFileSysObjState_Name(actualState)),
				 errcontext_persistent_relation_state_change(
								 						fsObjName,
								 						persistentTid,
								 						serialNum,
								 						expectedState1,
								 						expectedState2,
														expectedState3,
								 						nextState)));
		return PersistentFileSysObjVerifyExpectedResult_ErrorSuppressed;
	}

	if (!expectedStateMatch)
	{
		if (actualState == nextState
			&&
			retryPossible)
		{
			return PersistentFileSysObjVerifyExpectedResult_StateChangeAlreadyDone;
		}

		ereport(elevel,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("Found persistent file-system object in unexpected state '%s'",
						PersistentFileSysObjState_Name(actualState)),
				 errcontext_persistent_relation_state_change(
														fsObjName,
														persistentTid,
														serialNum,
														expectedState1,
														expectedState2,
														expectedState3,
														nextState)));
		return PersistentFileSysObjVerifyExpectedResult_ErrorSuppressed;
	}

	return PersistentFileSysObjVerifyExpectedResult_StateChangeNeeded;
}

extern char *PersistentFileSysObjStateChangeResult_Name(
	PersistentFileSysObjStateChangeResult result)
{
	switch (result)
	{
	case PersistentFileSysObjStateChangeResult_DeleteUnnecessary:
		return "Delete Unnecessary";

	case PersistentFileSysObjStateChangeResult_StateChangeOk:
		return "State-Change OK";

	case PersistentFileSysObjStateChangeResult_ErrorSuppressed:
		return "Error Suppressed";

	default:
		return "Unknown";
	}
}

PersistentFileSysObjStateChangeResult PersistentFileSysObj_StateChange(
	PersistentFileSysObjName 	*fsObjName,

	ItemPointer					persistentTid,

	int64						persistentSerialNum,

	PersistentFileSysState		nextState,

	bool						retryPossible,

	bool						flushToXLog,

	PersistentFileSysState		*oldState,

	PersistentFileSysObjVerifiedActionCallback verifiedActionCallback)
{
	READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE;

	PersistentFsObjType				fsObjType;

	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	Datum *values;

	HeapTuple tupleCopy;

	PersistentFileSysState	expectedState1;
	PersistentFileSysState	expectedState2 = -1;	// -1 means no second possibility.
	PersistentFileSysState	expectedState3 = -1;	// -1 means no third possibility.

	PersistentFileSysObjVerifyExpectedResult verifyExpectedResult;

	PersistentFileSysState localOldState;
	bool resetParentXid;

	if (PersistentStore_IsZeroTid(persistentTid))
		elog(ERROR, "TID for persistent '%s' state-change tuple is invalid (0,0)",
			 PersistentFileSysObjName_TypeName(fsObjName->type));

	if (persistentSerialNum == 0)
		elog(ERROR, "Persistent '%s' serial number for state-change is invalid (0)",
			 PersistentFileSysObjName_TypeName(fsObjName->type));

	if (oldState != NULL)
		*oldState = (PersistentFileSysState)-1;

	fsObjType = fsObjName->type;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	values = (Datum*)palloc(fileSysObjData->storeData.numAttributes * sizeof(Datum));

	READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHNAME(fsObjName, persistentTid, persistentSerialNum);

	PersistentFileSysObj_ReadTuple(
							fsObjType,
							persistentTid,
							values,
							&tupleCopy);

	READTUPLE_FOR_UPDATE_ERRCONTEXT_POP;

	switch (nextState)
	{
	case PersistentFileSysState_CreatePending:
		Assert (fsObjType == PersistentFsObjType_RelationFile);

		expectedState1 = PersistentFileSysState_BulkLoadCreatePending;
		break;

	case PersistentFileSysState_Created:
		expectedState1 = PersistentFileSysState_CreatePending;

		if (fsObjType == PersistentFsObjType_DatabaseDir)
			expectedState2 = PersistentFileSysState_JustInTimeCreatePending;
		break;

	case PersistentFileSysState_DropPending:
		expectedState1 = PersistentFileSysState_Created;
		break;

	case PersistentFileSysState_AbortingCreate:
		expectedState1 = PersistentFileSysState_CreatePending;
		if (fsObjType == PersistentFsObjType_RelationFile)
		{
			expectedState2 = PersistentFileSysState_BulkLoadCreatePending;
		}
		break;

	case PersistentFileSysState_Free:
		expectedState1 = PersistentFileSysState_DropPending;
		expectedState2 = PersistentFileSysState_AbortingCreate;
		if (fsObjType == PersistentFsObjType_DatabaseDir)
			expectedState3 = PersistentFileSysState_JustInTimeCreatePending;
		break;

	default:
		elog(ERROR, "Unexpected persistent object next state: %d",
			 nextState);
        expectedState1 = MaxPersistentFileSysState; /* to appease optimized compilation */
	}

	verifyExpectedResult =
		PersistentFileSysObj_VerifyExpected(
									fileSysObjData,
									fileSysObjSharedData,
									fsObjName,
									persistentTid,
									persistentSerialNum,
									values,
									expectedState1,
									expectedState2,
									expectedState3,
									nextState,
									retryPossible,
									&localOldState);
	if (oldState != NULL)
		*oldState = localOldState;

	if (verifiedActionCallback != NULL)
	{
		(*verifiedActionCallback)(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								verifyExpectedResult);
	}

	switch (verifyExpectedResult)
	{
	case PersistentFileSysObjVerifyExpectedResult_DeleteUnnecessary:
		return PersistentFileSysObjStateChangeResult_DeleteUnnecessary;

	case PersistentFileSysObjVerifyExpectedResult_StateChangeAlreadyDone:
		return PersistentFileSysObjStateChangeResult_StateChangeOk;

	case PersistentFileSysObjVerifyExpectedResult_StateChangeNeeded:
		break;

	case PersistentFileSysObjVerifyExpectedResult_ErrorSuppressed:
		return PersistentFileSysObjStateChangeResult_ErrorSuppressed;

	default:
		elog(ERROR, "Unexpected persistent object verify expected result: %d",
			 verifyExpectedResult);
	}

	resetParentXid = false;	// Assume.

	switch (nextState)
	{
	case PersistentFileSysState_Created:
	case PersistentFileSysState_DropPending:
	case PersistentFileSysState_AbortingCreate:
		resetParentXid = true;
		break;

	case PersistentFileSysState_CreatePending:
	case PersistentFileSysState_Free:
		break;

	default:
		elog(ERROR, "Unexpected persistent object next state: %d",
			 nextState);
	}

	if (nextState != PersistentFileSysState_Free)
	{
		Datum *newValues;
		bool *replaces;

		newValues = (Datum*)palloc0(fileSysObjData->storeData.numAttributes * sizeof(Datum));
		replaces = (bool*)palloc0(fileSysObjData->storeData.numAttributes * sizeof(bool));

		replaces[fileSysObjData->attNumPersistentState - 1] = true;
		newValues[fileSysObjData->attNumPersistentState - 1] = Int16GetDatum(nextState);

		if (resetParentXid)
		{
			replaces[fileSysObjData->attNumParentXid - 1] = true;
			newValues[fileSysObjData->attNumParentXid - 1] = Int32GetDatum(InvalidTransactionId);
		}

		PersistentFileSysObj_ReplaceTuple(
									fsObjType,
									persistentTid,
									tupleCopy,
									newValues,
									replaces,
									flushToXLog);

		pfree(newValues);
		pfree(replaces);
	}
	else
	{
		heap_freetuple(tupleCopy);

		PersistentFileSysObj_FreeTuple(
									fileSysObjData,
									fileSysObjSharedData,
									fsObjType,
									persistentTid,
									flushToXLog);
	}

	if (Debug_persistent_print)
	{
		PersistentFileSysObj_ReadTuple(
								fsObjType,
								persistentTid,
								values,
								&tupleCopy);

		(*fileSysObjData->storeData.printTupleCallback)(
									Persistent_DebugPrintLevel(),
									"STATE CHANGE",
									persistentTid,
									values);

		heap_freetuple(tupleCopy);
	}

	pfree(values);

	return PersistentFileSysObjStateChangeResult_StateChangeOk;
}

void PersistentFileSysObj_RepairDelete(
	PersistentFsObjType			fsObjType,

	ItemPointer					persistentTid)
{
	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	Datum *values;

	HeapTuple tupleCopy;

	PersistentFileSysObjName		fsObjName;
	PersistentFileSysState			state;

	TransactionId					parentXid;
	int64							serialNum;

	if (PersistentStore_IsZeroTid(persistentTid))
		elog(ERROR, "TID for persistent repair delete tuple is invalid (0,0)");

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	values = (Datum*)palloc(fileSysObjData->storeData.numAttributes * sizeof(Datum));

	PersistentFileSysObj_ReadTuple(
							fsObjType,
							persistentTid,
							values,
							&tupleCopy);

	GpPersistent_GetCommonValues(
							fsObjType,
							values,
							&fsObjName,
							&state,
							&parentXid,
							&serialNum);
	if (state == PersistentFileSysState_Free)
		elog(ERROR,
		     "Persistent object for TID %s is already free",
			 ItemPointerToString(persistentTid));

	heap_freetuple(tupleCopy);

	PersistentFileSysObj_FreeTuple(
								fileSysObjData,
								fileSysObjSharedData,
								fsObjType,
								persistentTid,
								/* flushToXLog */ true);

	pfree(values);
}

void PersistentFileSysObj_Created(
	PersistentFileSysObjName 	*fsObjName,

	ItemPointer				persistentTid,

	int64					persistentSerialNum,

	bool					retryPossible)
{
	if (!Persistent_BeforePersistenceWork())
	{
		if (PersistentStore_IsZeroTid(persistentTid))
			elog(ERROR, "TID for persistent '%s' tuple for created is invalid (0,0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));

		if (persistentSerialNum == 0)
			elog(ERROR, "Persistent '%s' serial number for created is invalid (0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));
	}

	switch (fsObjName->type)
	{
	case PersistentFsObjType_RelationFile:
		{
			//RelFileNode *relFileNode = PersistentFileSysObjName_GetRelFileNodePtr(fsObjName);
			//int32 segmentFileNum = PersistentFileSysObjName_GetSegmentFileNum(fsObjName);

			PersistentRelfile_Created(
								//relFileNode,
								//segmentFileNum,
								fsObjName,
								persistentTid,
								persistentSerialNum,
								retryPossible);
		}
		break;

	case PersistentFsObjType_RelationDir:
		{
			PersistentRelation_Created(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								retryPossible);
		}
		break;

	case PersistentFsObjType_DatabaseDir:
		PersistentDatabase_Created(
							//&fsObjName->variant.dbDirNode,
							fsObjName,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_TablespaceDir:
		PersistentTablespace_Created(
							//fsObjName->variant.tablespaceOid,
							fsObjName,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_FilespaceDir:
		PersistentFilespace_Created(
							//fsObjName->variant.filespaceOid,
							fsObjName,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;


	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjName->type);
	}
}

PersistentFileSysObjStateChangeResult PersistentFileSysObj_MarkAbortingCreate(
	PersistentFileSysObjName 	*fsObjName,

	ItemPointer				persistentTid,

	int64					persistentSerialNum,

	bool					retryPossible)
{
	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (!Persistent_BeforePersistenceWork())
	{
		if (PersistentStore_IsZeroTid(persistentTid))
			elog(ERROR, "TID for persistent '%s' tuple for mark aborting CREATE is invalid (0,0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));

		if (persistentSerialNum == 0)
			elog(ERROR, "Persistent '%s' serial number for mark aborting CREATE is invalid (0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));
	}

	switch (fsObjName->type)
	{
	case PersistentFsObjType_RelationFile:
		{
			//RelFileNode *relFileNode = PersistentFileSysObjName_GetRelFileNodePtr(fsObjName);
			//int32 segmentFileNum = PersistentFileSysObjName_GetSegmentFileNum(fsObjName);

			stateChangeResult =
				PersistentRelfile_MarkAbortingCreate(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								retryPossible);
		}
		break;

	case PersistentFsObjType_RelationDir:
		{
			stateChangeResult =
					PersistentRelation_MarkAbortingCreate(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								retryPossible);
		}
		break;

	case PersistentFsObjType_DatabaseDir:
		stateChangeResult =
			PersistentDatabase_MarkAbortingCreate(
							fsObjName,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_TablespaceDir:
		stateChangeResult =
			PersistentTablespace_MarkAbortingCreate(
							fsObjName,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_FilespaceDir:
		stateChangeResult =
			PersistentFilespace_MarkAbortingCreate(
							fsObjName,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjName->type);
		stateChangeResult = PersistentFileSysObjStateChangeResult_None;
	}

	return stateChangeResult;
}

PersistentFileSysObjStateChangeResult PersistentFileSysObj_MarkDropPending(
	PersistentFileSysObjName 	*fsObjName,

	ItemPointer				persistentTid,

	int64					persistentSerialNum,

	bool					retryPossible)
{
	PersistentFileSysObjStateChangeResult stateChangeResult;

	if (!Persistent_BeforePersistenceWork())
	{
		if (PersistentStore_IsZeroTid(persistentTid))
			elog(ERROR, "TID for persistent '%s' tuple for mark DROP pending is invalid (0,0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));

		if (persistentSerialNum == 0)
			elog(ERROR, "Persistent '%s' serial number for mark DROP pending is invalid (0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));
	}

	switch (fsObjName->type)
	{
	case PersistentFsObjType_RelationFile:
		{
			//RelFileNode *relFileNode = PersistentFileSysObjName_GetRelFileNodePtr(fsObjName);
			//int32 segmentFileNum = PersistentFileSysObjName_GetSegmentFileNum(fsObjName);

			stateChangeResult =
				PersistentRelfile_MarkDropPending(
								fsObjName,
								//relFileNode,
								//segmentFileNum,
								persistentTid,
								persistentSerialNum,
								retryPossible);
		}
		break;

	case PersistentFsObjType_RelationDir:
		{
			stateChangeResult =
					PersistentRelation_MarkDropPending(
								fsObjName,
								persistentTid,
								persistentSerialNum,
								retryPossible);
		}
		break;

	case PersistentFsObjType_DatabaseDir:
		stateChangeResult =
			PersistentDatabase_MarkDropPending(
							fsObjName,
							//&fsObjName->variant.dbDirNode,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_TablespaceDir:
		stateChangeResult =
			PersistentTablespace_MarkDropPending(
							//fsObjName->variant.tablespaceOid,
							fsObjName,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	case PersistentFsObjType_FilespaceDir:
		stateChangeResult =
			PersistentFilespace_MarkDropPending(
							//fsObjName->variant.tablespaceOid,
							fsObjName,
							persistentTid,
							persistentSerialNum,
							retryPossible);
		break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjName->type);
		stateChangeResult = PersistentFileSysObjStateChangeResult_None;
	}

	return stateChangeResult;
}

/*
 * Error context callback for errors occurring during PersistentFileSysObj_DropObject().
 */
static void
PersistentFileSysObj_DropObjectErrorCallback(void *arg)
{
	PersistentFileSysObjName *fsObjName = (PersistentFileSysObjName *) arg;

	errcontext("Dropping file-system object -- %s",
		       PersistentFileSysObjName_TypeAndObjectName(fsObjName));
}


void PersistentFileSysObj_DropObject(
	PersistentFileSysObjName 	*fsObjName,

	PersistentFileSysRelStorageMgr relStorageMgr,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */

	ItemPointer					persistentTid,

	int64						persistentSerialNum,

	bool 						ignoreNonExistence,

	bool						debugPrint,

	int							debugPrintLevel)
{
	READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE;

	PersistentFsObjType				fsObjType;

	PersistentFileSysObjData 		*fileSysObjData;
	PersistentFileSysObjSharedData 	*fileSysObjSharedData;

	Datum *values;

	HeapTuple tupleCopy;

	PersistentFileSysObjName 		actualFsObjName;
	PersistentFileSysState		  	actualState;

	TransactionId					actualParentXid;
	int64							actualSerialNum;

	ErrorContextCallback errcontext;

	/* Setup error traceback support for ereport() */
	errcontext.callback = PersistentFileSysObj_DropObjectErrorCallback;
	errcontext.arg = (void *) fsObjName;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	if (!Persistent_BeforePersistenceWork())
	{
		if (PersistentStore_IsZeroTid(persistentTid))
			elog(ERROR, "TID for persistent '%s' tuple for DROP is invalid (0,0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));

		if (persistentSerialNum == 0)
			elog(ERROR, "Persistent '%s' serial number for DROP is invalid (0)",
				 PersistentFileSysObjName_TypeName(fsObjName->type));
	}

	fsObjType = fsObjName->type;

	PersistentFileSysObj_GetDataPtrs(
								fsObjType,
								&fileSysObjData,
								&fileSysObjSharedData);

	values = (Datum*)palloc(fileSysObjData->storeData.numAttributes * sizeof(Datum));

	READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHNAME(fsObjName, persistentTid, persistentSerialNum);

	PersistentFileSysObj_ReadTuple(
							fsObjType,
							persistentTid,
							values,
							&tupleCopy);

	READTUPLE_FOR_UPDATE_ERRCONTEXT_POP;

	GpPersistent_GetCommonValues(
							fsObjType,
							values,
							&actualFsObjName,
							&actualState,
							&actualParentXid,
							&actualSerialNum);

	pfree(values);

	heap_freetuple(tupleCopy);

	if (actualState == PersistentFileSysState_Free)
		elog(ERROR, "Expected persistent entry '%s' not to be in 'Free' state for DROP at TID %s",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
			 ItemPointerToString(persistentTid));

	if (persistentSerialNum != actualSerialNum)
		elog(ERROR, "Expected persistent entry '%s' serial number mismatch for DROP (expected " INT64_FORMAT ", found " INT64_FORMAT "), at TID %s",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
			 persistentSerialNum,
			 actualSerialNum,
			 ItemPointerToString(persistentTid));


	if (debugPrint)
		elog(debugPrintLevel,
			 "PersistentFileSysObj_DropObject: before drop of %s, persistent serial number " INT64_FORMAT ", TID %s",
			 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid));

	switch (fsObjType)
	{
	case PersistentFsObjType_RelationFile:
		{
			RelFileNode *relFileNode = PersistentFileSysObjName_GetRelFileNodePtr(fsObjName);
			int32 segmentFileNum = PersistentFileSysObjName_GetSegmentFileNum(fsObjName);

			if (!PersistentFileSysRelStorageMgr_IsValid(relStorageMgr))
				elog(ERROR, "Relation storage manager for persistent '%s' tuple for DROP is invalid (%d)",
					 PersistentFileSysObjName_TypeName(fsObjName->type),
					 relStorageMgr);

			MirroredFileSysObj_DropRelFile(
								relFileNode,
								segmentFileNum,
								relStorageMgr,
								relationName,
								/* isLocalBuf */ false,
								ignoreNonExistence);


			if (debugPrint)
				elog(debugPrintLevel,
					 "PersistentFileSysObj_DropObject: after drop of %s, persistent serial number " INT64_FORMAT ", TID %s",
					 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
					 persistentSerialNum,
					 ItemPointerToString(persistentTid));

			PersistentRelfile_Dropped(
								fsObjName,
								persistentTid,
								persistentSerialNum);
		}
		break;

	case PersistentFsObjType_RelationDir:
		{
			RelFileNode *relFileNode = &fsObjName->variant.rel.relFileNode;

			MirroredFileSysObj_DropRelationDir(
								relFileNode,
								ignoreNonExistence);

			if (debugPrint)
			{
				elog(debugPrintLevel,
					"PersistentFileSysObj_DropObject: after drop of %s, persistent serial number " INT64_FORMAT ", TID %s",
					PersistentFileSysObjName_TypeAndObjectName(fsObjName),
					persistentSerialNum,
					ItemPointerToString(persistentTid));
			}

			PersistentRelation_Dropped(
								fsObjName,
								persistentTid,
								persistentSerialNum);
		}
		break;

	case PersistentFsObjType_DatabaseDir:
		{
			DbDirNode *dbDirNode = &fsObjName->variant.dbDirNode;

			MirroredFileSysObj_DropDbDir(
								dbDirNode,
								ignoreNonExistence);

			if (debugPrint)
				elog(debugPrintLevel,
					 "PersistentFileSysObj_DropObject: after drop of %s, persistent serial number " INT64_FORMAT ", TID %s",
					 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
					 persistentSerialNum,
					 ItemPointerToString(persistentTid));

			PersistentDatabase_Dropped(
								fsObjName,
								persistentTid,
								persistentSerialNum);
		}
		break;

	case PersistentFsObjType_TablespaceDir:
		MirroredFileSysObj_DropTablespaceDir(
							fsObjName->variant.tablespaceOid,
							ignoreNonExistence);

		if (debugPrint)
			elog(debugPrintLevel,
				 "PersistentFileSysObj_DropObject: after drop of %s, persistent serial number " INT64_FORMAT ", TID %s",
				 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
				 persistentSerialNum,
				 ItemPointerToString(persistentTid));

		PersistentTablespace_Dropped(
							fsObjName,
							persistentTid,
							persistentSerialNum);
		break;

	case PersistentFsObjType_FilespaceDir:
		{
			char *filespaceLocation;

			PersistentFilespace_GetLocation(
								fsObjName->variant.filespaceOid,
								&filespaceLocation);

			MirroredFileSysObj_DropFilespaceDir(
								fsObjName->variant.filespaceOid,
								filespaceLocation,
								ignoreNonExistence);

			if (debugPrint)
				elog(debugPrintLevel,
					 "PersistentFileSysObj_DropObject: after drop of %s, persistent serial number " INT64_FORMAT ", TID %s",
					 PersistentFileSysObjName_TypeAndObjectName(fsObjName),
					 persistentSerialNum,
					 ItemPointerToString(persistentTid));

			PersistentFilespace_Dropped(
								fsObjName,
								persistentTid,
								persistentSerialNum);

			if (filespaceLocation != NULL)
				pfree(filespaceLocation);
		}
		break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjName->type);
	}

	/* Pop the error context stack */
	error_context_stack = errcontext.previous;
}

void PersistentFileSysObj_EndXactDrop(
	PersistentFileSysObjName 	*fsObjName,

	PersistentFileSysRelStorageMgr relStorageMgr,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */

	ItemPointer				persistentTid,

	int64					persistentSerialNum,

	bool					ignoreNonExistence)
{
	PersistentFileSysObj_DropObject(
							fsObjName,
							relStorageMgr,
							relationName,
							persistentTid,
							persistentSerialNum,
							ignoreNonExistence,
							Debug_persistent_print,
							Persistent_DebugPrintLevel());

	// clean up alive connections that are used for deleting hdfs objects
	cleanup_hdfs_handlers_for_dropping();
}

void PersistentFileSysObj_UpdateRelationBufpoolKind(
	RelFileNode							*relFileNode,

	int32								segmentFileNum,

	ItemPointer 						persistentTid,

	int64								persistentSerialNum,

	PersistentFileSysRelBufpoolKind 	relBufpoolKind)
{
	WRITE_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	READTUPLE_FOR_UPDATE_ERRCONTEXT_DECLARE;

	HeapTuple tupleCopy;

	PersistentFileSysObjName		actualFsObjName;
	PersistentFileSysState			state;

	TransactionId					parentXid;
	int64							serialNum;

	Datum values[Natts_gp_persistent_relfile_node];
	Datum newValues[Natts_gp_persistent_relfile_node];
	bool replaces[Natts_gp_persistent_relfile_node];

	WRITE_PERSISTENT_STATE_ORDERED_LOCK;

	READTUPLE_FOR_UPDATE_ERRCONTEXT_PUSHTYPE(PersistentFsObjType_RelationFile, persistentTid, persistentSerialNum);

	PersistentFileSysObj_ReadTuple(
							PersistentFsObjType_RelationFile,
							persistentTid,
							values,
							&tupleCopy);

	READTUPLE_FOR_UPDATE_ERRCONTEXT_POP;

	GpPersistent_GetCommonValues(
							PersistentFsObjType_RelationFile,
							values,
							&actualFsObjName,
							&state,
							&parentXid,
							&serialNum);
	if (persistentSerialNum != serialNum)
		elog(ERROR, "Serial number mismatch for update (expected " INT64_FORMAT ", found " INT64_FORMAT "), at TID %s",
			 persistentSerialNum,
			 serialNum,
			 ItemPointerToString(persistentTid));

	MemSet(newValues, 0, Natts_gp_persistent_relfile_node * sizeof(Datum));
	MemSet(replaces, false, Natts_gp_persistent_relfile_node * sizeof(bool));

	replaces[Anum_gp_persistent_relfile_node_relation_bufpool_kind - 1] = true;
	newValues[Anum_gp_persistent_relfile_node_relation_bufpool_kind - 1] =
													Int32GetDatum(relBufpoolKind);

	PersistentFileSysObj_ReplaceTuple(
								PersistentFsObjType_RelationFile,
								persistentTid,
								tupleCopy,
								newValues,
								replaces,
								/* flushToXLog */ false);

	WRITE_PERSISTENT_STATE_ORDERED_UNLOCK;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(),
			 "PersistentFileSysObj_UpdateRelationBufpoolKind: %u/%u/%u, segment file #%d, set relation bufpool kind '%s' -- persistent serial num " INT64_FORMAT ", TID %s",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 segmentFileNum,
			 PersistentFileSysRelBufpoolKind_Name(relBufpoolKind),
			 persistentSerialNum,
			 ItemPointerToString(persistentTid));
}

void PersistentFileSysObj_PreparedEndXactAction(
	TransactionId 					preparedXid,

	const char 						*gid,

	PersistentEndXactRecObjects 	*persistentObjects,

	bool							isCommit,

	int								prepareAppendOnlyIntentCount)
{
	PersistentFileSysObjStateChangeResult *stateChangeResults;

	int i;

	if (Debug_persistent_print ||
		Debug_persistent_appendonly_commit_count_print)
		elog(Persistent_DebugPrintLevel(),
			 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): commit %s, file-system action count %d, shared-memory Append-Only intent count %d, xlog Append-Only mirror resync EOFs count %d",
			 gid,
			 preparedXid,
			 (isCommit ? "true" : "false"),
			 persistentObjects->typed.fileSysActionInfosCount,
			 prepareAppendOnlyIntentCount,
			 persistentObjects->typed.appendOnlyMirrorResyncEofsCount);

	stateChangeResults =
			(PersistentFileSysObjStateChangeResult*)
					palloc0(persistentObjects->typed.fileSysActionInfosCount * sizeof(PersistentFileSysObjStateChangeResult));

	/*
	 * We need to complete this work, or let Crash Recovery complete it.
	 */
	START_CRIT_SECTION();

	/*
	 * End transaction State-Changes.
	 */
	for (i = 0; i < persistentObjects->typed.fileSysActionInfosCount; i++)
	{
		PersistentEndXactFileSysActionInfo	*fileSysActionInfo =
					&persistentObjects->typed.fileSysActionInfos[i];

		PersistentEndXactFileSysAction action;

		action = fileSysActionInfo->action;

		switch (action)
		{
		case PersistentEndXactFileSysAction_Create:
			if (isCommit)
			{
				/*
				 * 'Create Pending' --> 'Created'.
				 */
				if (Debug_persistent_print)
					elog(Persistent_DebugPrintLevel(),
						 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, 'Create Pending' --> 'Created'",
						 gid,
						 preparedXid,
						 i,
						 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
						 fileSysActionInfo->persistentSerialNum,
						 ItemPointerToString(&fileSysActionInfo->persistentTid));

				PersistentFileSysObj_Created(
										&fileSysActionInfo->fsObjName,
										&fileSysActionInfo->persistentTid,
										fileSysActionInfo->persistentSerialNum,
										/* retryPossible */ true);
			}
			else
			{
				/*
				 * 'Create Pending' --> 'Aborting Create'.
				 */
				if (Debug_persistent_print)
					elog(Persistent_DebugPrintLevel(),
						 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, 'Create Pending' --> 'Aborting Create'",
						 gid,
						 preparedXid,
						 i,
						 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
						 fileSysActionInfo->persistentSerialNum,
						 ItemPointerToString(&fileSysActionInfo->persistentTid));

				stateChangeResults[i] =
					PersistentFileSysObj_MarkAbortingCreate(
											&fileSysActionInfo->fsObjName,
											&fileSysActionInfo->persistentTid,
											fileSysActionInfo->persistentSerialNum,
											/* retryPossible */ true);
			}
			break;

		case PersistentEndXactFileSysAction_Drop:
			if (isCommit)
			{
				/*
				 * 'Created' --> 'Drop Pending'.
				 */
				if (Debug_persistent_print)
					elog(Persistent_DebugPrintLevel(),
						 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, 'Created' --> 'Drop Pending'",
						 gid,
						 preparedXid,
						 i,
						 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
						 fileSysActionInfo->persistentSerialNum,
						 ItemPointerToString(&fileSysActionInfo->persistentTid));

				stateChangeResults[i] =
					PersistentFileSysObj_MarkDropPending(
										&fileSysActionInfo->fsObjName,
										&fileSysActionInfo->persistentTid,
										fileSysActionInfo->persistentSerialNum,
										/* retryPossible */ true);
			}
			break;

		case PersistentEndXactFileSysAction_AbortingCreateNeeded:
			if (Debug_persistent_print)
				elog(Persistent_DebugPrintLevel(),
					 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, 'Create Pending' --> 'Aborting Create'",
					 gid,
					 preparedXid,
					 i,
					 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
					 fileSysActionInfo->persistentSerialNum,
					 ItemPointerToString(&fileSysActionInfo->persistentTid));

			stateChangeResults[i] =
				PersistentFileSysObj_MarkAbortingCreate(
									&fileSysActionInfo->fsObjName,
									&fileSysActionInfo->persistentTid,
									fileSysActionInfo->persistentSerialNum,
									/* retryPossible */ true);
			break;

		default:
			elog(ERROR, "Unexpected persistent end transaction file-system action: %d",
				 action);
		}
	}

	/*
	 * Make the above State-Changes permanent.
	 */
	PersistentFileSysObj_FlushXLog();

	/*
	 * Do the physically drop of the file-system objects.
	 */
	for (i = 0; i < persistentObjects->typed.fileSysActionInfosCount; i++)
	{
		PersistentEndXactFileSysActionInfo	*fileSysActionInfo =
					&persistentObjects->typed.fileSysActionInfos[i];

		PersistentEndXactFileSysAction action;

		bool dropPending;
		bool abortingCreate;

		action = fileSysActionInfo->action;

		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(),
				 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, "
				 "looking at persistent end transaction action '%s' where isCommit %s and State-Change result '%s'",
				 gid,
				 preparedXid,
				 i,
				 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
				 fileSysActionInfo->persistentSerialNum,
				 ItemPointerToString(&fileSysActionInfo->persistentTid),
				 PersistentEndXactFileSysAction_Name(action),
				 (isCommit ? "true" : "false"),
				 PersistentFileSysObjStateChangeResult_Name(stateChangeResults[i]));

		dropPending = false;		// Assume.
		abortingCreate = false;		// Assume.

		switch (action)
		{
		case PersistentEndXactFileSysAction_Create:
			if (!isCommit)
			{
				abortingCreate = true;
			}
#ifdef FAULT_INJECTOR
				if (FaultInjector_InjectFaultIfSet(
											   isCommit ?
											   FinishPreparedTransactionCommitPass1FromCreatePendingToCreated :
											   FinishPreparedTransactionAbortPass1FromCreatePendingToAbortingCreate,
											   DDLNotSpecified,
												   "" /* databaseName */,
												   "" /* tableName */) == TRUE)
					goto injectfaultexit;  // skip pass2
#endif
			break;

		case PersistentEndXactFileSysAction_Drop:
			if (isCommit)
			{
				dropPending = true;
#ifdef FAULT_INJECTOR
				if (FaultInjector_InjectFaultIfSet(
											   FinishPreparedTransactionCommitPass1FromDropInMemoryToDropPending,
											   DDLNotSpecified,
											   "" /* databaseName */,
											   "" /* tableName */) == TRUE)
				goto injectfaultexit;  // skip pass2
#endif
			}
			break;

		case PersistentEndXactFileSysAction_AbortingCreateNeeded:
			abortingCreate = true;
#ifdef FAULT_INJECTOR
				if (FaultInjector_InjectFaultIfSet(
											   isCommit ?
											   FinishPreparedTransactionCommitPass1AbortingCreateNeeded:
											   FinishPreparedTransactionAbortPass1AbortingCreateNeeded,
											   DDLNotSpecified,
											   "" /* databaseName */,
											   "" /* tableName */) == TRUE)
				goto injectfaultexit;  // skip pass2
#endif
			break;

		default:
			elog(ERROR, "Unexpected persistent end transaction file-system action: %d",
				 action);
		}

		if (abortingCreate || dropPending)
		{
			if (stateChangeResults[i] == PersistentFileSysObjStateChangeResult_StateChangeOk)
			{
				if (Debug_persistent_print)
					elog(Persistent_DebugPrintLevel(),
						 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, "
						 "going to drop object where abortingCreate %s or dropPending %s",
						 gid,
						 preparedXid,
						 i,
						 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
						 fileSysActionInfo->persistentSerialNum,
						 ItemPointerToString(&fileSysActionInfo->persistentTid),
						 (abortingCreate ? "true" : "false"),
						 (dropPending ? "true" : "false"));

				PersistentFileSysObj_EndXactDrop(
										&fileSysActionInfo->fsObjName,
										fileSysActionInfo->relStorageMgr,
										/* relationName */ NULL,		// Ok to be NULL -- we don't know the name here.
										&fileSysActionInfo->persistentTid,
										fileSysActionInfo->persistentSerialNum,
										/* ignoreNonExistence */ abortingCreate);
			}
			else
			{
				if (Debug_persistent_print)
					elog(Persistent_DebugPrintLevel(),
						 "PersistentFileSysObj_PreparedEndXactAction (distributed transaction id %s, local prepared xid %u): [%d] %s, persistent serial num " INT64_FORMAT ", TID %s, "
						 "skipping to drop object where abortingCreate %s or dropPending %s with State-Change result '%s'",
						 gid,
						 preparedXid,
						 i,
						 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName),
						 fileSysActionInfo->persistentSerialNum,
						 ItemPointerToString(&fileSysActionInfo->persistentTid),
						 (abortingCreate ? "true" : "false"),
						 (dropPending ? "true" : "false"),
						 PersistentFileSysObjStateChangeResult_Name(stateChangeResults[i]));
			}
		}

#ifdef FAULT_INJECTOR
		switch (action)
		{
			case PersistentEndXactFileSysAction_Create:
				if (FaultInjector_InjectFaultIfSet(
												   isCommit ?
												   FinishPreparedTransactionCommitPass2FromCreatePendingToCreated :
												   FinishPreparedTransactionAbortPass2FromCreatePendingToAbortingCreate,
												   DDLNotSpecified,
												   "" /* databaseName */,
												   "" /* tableName */) == TRUE)
					goto injectfaultexit;  // skip physical drop in pass2

				break;

			case PersistentEndXactFileSysAction_Drop:
				if (isCommit)
				{
					if (FaultInjector_InjectFaultIfSet(
												   FinishPreparedTransactionCommitPass2FromDropInMemoryToDropPending,
												   DDLNotSpecified,
												   "" /* databaseName */,
												   "" /* tableName */) == TRUE)
					goto injectfaultexit;  // skip physical drop in pass2
				}
				break;

			case PersistentEndXactFileSysAction_AbortingCreateNeeded:
				if (FaultInjector_InjectFaultIfSet(
											   isCommit ?
											   FinishPreparedTransactionCommitPass2AbortingCreateNeeded :
											   FinishPreparedTransactionAbortPass2AbortingCreateNeeded,
											   DDLNotSpecified,
											   "" /* databaseName */,
											   "" /* tableName */) == TRUE)
				goto injectfaultexit;  // skip physical drop in pass2
				break;

			default:
				break;
		}

#endif
	}

	goto jumpoverinjectfaultexit;

#ifdef FAULT_INJECTOR
injectfaultexit:
#endif
jumpoverinjectfaultexit:

	PersistentFileSysObj_FlushXLog();

	END_CRIT_SECTION();

	pfree(stateChangeResults);
}

bool PersistentFileSysObj_ScanForRelation(
	RelFileNode 		*relFileNode,
				/* The tablespace, database, and relation OIDs for the create. */

	int32				segmentFileNum,

	ItemPointer			persistentTid,
				/* Resulting TID of the gp_persistent_rel_files tuple for the relation. */

	int64				*persistentSerialNum)
				/* Resulting serial number for the relation.  Distinquishes the uses of the tuple. */
{
	READ_PERSISTENT_STATE_ORDERED_LOCK_DECLARE;

	PersistentFileSysObjData *fileSysObjData;
	PersistentFileSysObjSharedData	*fileSysObjSharedData;

	PersistentStoreScan storeScan;

	Datum values[Natts_gp_persistent_relfile_node];

	bool found;

	if(RelFileNode_IsEmpty(relFileNode))
		elog(ERROR, "Invalid RelFileNode (0,0,0)");

	if (Persistent_BeforePersistenceWork())
	{
		elog(ERROR,
		     "Cannot scan for persistent relation %u/%u/%u, segment file #%d because we are before persistence work",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 segmentFileNum);
	}

	PersistentFileSysObj_VerifyInitScan();

	PersistentFileSysObj_GetDataPtrs(
								PersistentFsObjType_RelationFile,
								&fileSysObjData,
								&fileSysObjSharedData);

	found = false;
	MemSet(persistentTid, 0, sizeof(ItemPointerData));
	*persistentSerialNum = 0;

	READ_PERSISTENT_STATE_ORDERED_LOCK;

	PersistentStore_BeginScan(
						&fileSysObjData->storeData,
						&fileSysObjSharedData->storeSharedData,
						&storeScan);

	while (PersistentStore_GetNext(
							&storeScan,
							values,
							persistentTid,
							persistentSerialNum))
	{
		RelFileNode 					candidateRelFileNode;
		int32 							candidateSegmentFileNum;
		PersistentFileSysRelStorageMgr	relationStorageManager;
		PersistentFileSysState			persistentState;
		PersistentFileSysRelBufpoolKind relBufpoolKind;
		TransactionId					parentXid;
		int64							serialNum;
		ItemPointerData					previousFreeTid;
		bool							sharedStorage;

		GpPersistentRelfileNode_GetValues(
										values,
										&candidateRelFileNode.spcNode,
										&candidateRelFileNode.dbNode,
										&candidateRelFileNode.relNode,
										&candidateSegmentFileNum,
										&relationStorageManager,
										&persistentState,
										&relBufpoolKind,
										&parentXid,
										&serialNum,
										&previousFreeTid,
										&sharedStorage);

		if (persistentState != PersistentFileSysState_BulkLoadCreatePending &&
			persistentState != PersistentFileSysState_CreatePending &&
			persistentState != PersistentFileSysState_Created)
			continue;

		if (RelFileNodeEquals(candidateRelFileNode, *relFileNode) &&
			candidateSegmentFileNum == segmentFileNum)
		{
			found = true;
			break;
		}

		READ_PERSISTENT_STATE_ORDERED_UNLOCK;

		READ_PERSISTENT_STATE_ORDERED_LOCK;

	}

	READ_PERSISTENT_STATE_ORDERED_UNLOCK;

	PersistentStore_EndScan(&storeScan);

	if (found && Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(),
		     "Scan found persistent relation %u/%u/%u, segment file #%d with serial number " INT64_FORMAT " at TID %s",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 segmentFileNum,
			 *persistentSerialNum,
			 ItemPointerToString(persistentTid));

	return found;
}

static void
PersistentFileSysObj_StartupIntegrityCheckRelation(void)
{
	PersistentFileSysObjData *fileSysObjData;
	PersistentFileSysObjSharedData	*fileSysObjSharedData;

	PersistentStoreScan storeScan;

	Datum values[Natts_gp_persistent_relfile_node];

	ItemPointerData persistentTid;
	int64			persistentSerialNum;

	PersistentFileSysObj_GetDataPtrs(
								PersistentFsObjType_RelationFile,
								&fileSysObjData,
								&fileSysObjSharedData);


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
		RelFileNode 					relFileNode;
		int32 							segmentFileNum;

		PersistentFileSysRelStorageMgr	relationStorageManager;
		PersistentFileSysState			persistentState;
		PersistentFileSysRelBufpoolKind relBufpoolKind;
		TransactionId					parentXid;
		int64							serialNum;
		ItemPointerData					previousFreeTid;
		bool							sharedStorage;

		DbDirNode dbDirNode;

		GpPersistentRelfileNode_GetValues(
										values,
										&relFileNode.spcNode,
										&relFileNode.dbNode,
										&relFileNode.relNode,
										&segmentFileNum,
										&relationStorageManager,
										&persistentState,
										&relBufpoolKind,
										&parentXid,
										&serialNum,
										&previousFreeTid,
										&sharedStorage);

		if (persistentState == PersistentFileSysState_Free)
			continue;

		/*
		 * Verify this relation has a database directory parent.
		 */
		dbDirNode.tablespace = relFileNode.spcNode;
		dbDirNode.database = relFileNode.dbNode;

		if (dbDirNode.database == 0)
		{
			/*
			 * We don't represent the shared database directory in
			 * gp_persistent_database_node since it cannot be dropped.
			 */
			Assert(dbDirNode.tablespace == GLOBALTABLESPACE_OID);
			continue;
		}

		/* We support stateless segments, so we don't need do the sanity check for segments. */
		if (!PersistentDatabase_DbDirExistsUnderLock(&dbDirNode))
		{
			elog(LOG,
				 "Database directory not found for relation %u/%u/%u",
				 relFileNode.spcNode,
				 relFileNode.dbNode,
				 relFileNode.relNode);
		}
	}

	PersistentStore_EndScan(&storeScan);
}

static void PersistentFileSysObj_StartupIntegrityCheckPrintDbDirs(void)
{
	DbDirNode dbDirNode;
	PersistentFileSysState state;

	ItemPointerData persistentTid;
	int64 persistentSerialNum;

	PersistentDatabase_DirIterateInit();
	while (PersistentDatabase_DirIterateNext(
									&dbDirNode,
									&state,
									&persistentTid,
									&persistentSerialNum))
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "STARTUP INTEGRITY: Database database %u/%u in state '%s'",
			 dbDirNode.tablespace,
			 dbDirNode.database,
			 PersistentFileSysObjState_Name(state));
	}
}

void PersistentFileSysObj_StartupIntegrityCheck(void)
{
	/*
	 * Show the existing database directories.
	 */
	if (Debug_persistent_recovery_print)
	{
		PersistentFileSysObj_StartupIntegrityCheckPrintDbDirs();
	}

	/*
	 * Verify each relation has a database directory parent.
	 */
	PersistentFileSysObj_StartupIntegrityCheckRelation();

	/*
	 * Show the existing tablespace directories.
	 */
	if (Debug_persistent_recovery_print)
	{
//		PersistentFileSysObj_StartupIntegrityCheckPrintTablespaceDirs();
	}

}

static Size PersistentFileSysObj_SharedDataSize(void)
{
	return MAXALIGN(sizeof(PersistentFileSysObjPrivateSharedMemory));
}

/*
 * Return the required shared-memory size for this module.
 */
Size PersistentFileSysObj_ShmemSize(void)
{
	Size size = 0;

	/* The shared-memory structure. */
	size = add_size(size, PersistentFileSysObj_SharedDataSize());

	return size;
}

/*
 * Initialize the shared-memory for this module.
 */
void PersistentFileSysObj_ShmemInit(void)
{
	bool found;

	/* Create the shared-memory structure. */
	persistentFileSysObjPrivateSharedData =
		(PersistentFileSysObjPrivateSharedMemory *)
						ShmemInitStruct("Persist File-Sys Data",
										PersistentFileSysObj_SharedDataSize(),
										&found);

	if (!found)
	{
		persistentFileSysObjPrivateSharedData->needInitScan = true;
	}
}

/* 
 * Check for inconsistencies in global sequence number and then update to the right value if necessary 
 * WARNING : This is a special routine called during repair only under guc gp_persistent_repair_global_sequence
 */
void 
PersistentFileSysObj_DoGlobalSequenceScan(void)
{

	PersistentFsObjType             fsObjType;
	int64                   	globalSequenceNum;

	PersistentFileSysObjData        *fileSysObjData;
	PersistentFileSysObjSharedData  *fileSysObjSharedData;

	Assert(!persistentFileSysObjPrivateSharedData->needInitScan);

	if (gp_persistent_repair_global_sequence)
	{
	
		for (fsObjType = PersistentFsObjType_First;
			fsObjType <= PersistentFsObjType_Last;
			fsObjType++)
		{
                 	PersistentFileSysObj_GetDataPtrs(
                                                                         fsObjType,
                                                                         &fileSysObjData,
                                                                         &fileSysObjSharedData);

			globalSequenceNum = GlobalSequence_Current(fileSysObjData->storeData.gpGlobalSequence);

			if (globalSequenceNum < fileSysObjSharedData->storeSharedData.maxInUseSerialNum)
			{
				/*
				 * We seem to have a corruption problem.
				 *
				 * Use the gp_persistent_repair_global_sequence GUC to get the system up.
				 */
				GlobalSequence_Set(fileSysObjData->storeData.gpGlobalSequence, fileSysObjSharedData->storeSharedData.maxInUseSerialNum);

				elog(LOG, "Repaired global sequence number " INT64_FORMAT " so use scanned maximum value " INT64_FORMAT " ('%s')",
                                 	globalSequenceNum,
                                 	fileSysObjSharedData->storeSharedData.maxInUseSerialNum,
                                 	fileSysObjData->storeData.tableName);
			}

		}
	}
}
