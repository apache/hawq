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
 * gp_persistent.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "storage/fd.h"

#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "catalog/gp_persistent.h"
#include "cdb/cdbsharedoidsearch.h"
#include "storage/itemptr.h"
#include "storage/shmem.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "utils/guc.h"
#include "storage/smgr.h"
#include "storage/ipc.h"
#include "utils/builtins.h"

void GpPersistentRelfileNode_GetRelfileInfo(
	char		relkind,			/* see RELKIND_xxx constants */
	char		relstorage,			/* see RELSTORAGE_xxx constants */
	Oid			relam,				/* index access method; 0 if not an index */

	PersistentFileSysRelStorageMgr			*relfileStorageManager,
	PersistentFileSysRelBufpoolKind			*relfileBufpoolKind)
{
	// Assume.
	*relfileStorageManager = PersistentFileSysRelStorageMgr_BufferPool;
	*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_None;

	switch(relkind)
	{
	case RELKIND_INDEX:				/* secondary index */
		if (relam == BTREE_AM_OID)
		{
			*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_Btree;
		}
		else if (relam == BITMAP_AM_OID)
		{
			*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_BitMap;
		}
		else
		{
			*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_UnknownIndex;
		}
		break;

	case RELKIND_RELATION:			/* ordinary cataloged heap or Append-Only */
		if (relstorage_is_ao(relstorage))
		{
			*relfileStorageManager = PersistentFileSysRelStorageMgr_AppendOnly;
			Assert(*relfileBufpoolKind == PersistentFileSysRelBufpoolKind_None);
		}
		else if (relstorage == 'h')
		{
			*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_Heap;
		}
		else
		{
			*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_UnknownRelStorage;
		}
		break;

	case RELKIND_SEQUENCE:			/* SEQUENCE relation */
		*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_Sequence;
		break;

	case RELKIND_TOASTVALUE:		/* moved off huge values */
		*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_Toast;
		break;

	case RELKIND_AOSEGMENTS:		/* AO segment files and eof's */
		*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_AppendOnlySeginfo;
		break;

	case RELKIND_AOBLOCKDIR:		/* AO block directory */
		*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_AppendOnlyBlockDirectory;
		break;

	case RELKIND_UNCATALOGED:		/* temporary heap */
		*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_UncatalogedHeap;
		break;

	case RELKIND_VIEW:				/* view */
	case RELKIND_COMPOSITE_TYPE:	/* composite type */
		*relfileStorageManager = PersistentFileSysRelStorageMgr_None;
		break;

	default:
		*relfileBufpoolKind = PersistentFileSysRelBufpoolKind_UnknownRelKind;
		break;

	}
}

void GpPersistentRelfileNode_GetValues(
	Datum									*values,

	Oid 									*tablespaceOid,
	Oid 									*databaseOid,
	Oid 									*relfilenodeOid,
	int32									*segmentFileNum,
	PersistentFileSysRelStorageMgr			*relationStorageManager,
	PersistentFileSysState					*persistentState,
	PersistentFileSysRelBufpoolKind 		*relBufpoolKind,
	TransactionId							*parentXid,
	int64									*persistentSerialNum,
	ItemPointerData 						*previousFreeTid,
	bool									*sharedStorage)
{
	*tablespaceOid = DatumGetObjectId(values[Anum_gp_persistent_relfile_node_tablespace_oid - 1]);

	*databaseOid = DatumGetObjectId(values[Anum_gp_persistent_relfile_node_database_oid - 1]);

	*relfilenodeOid = DatumGetObjectId(values[Anum_gp_persistent_relfile_node_relfilenode_oid - 1]);

	*segmentFileNum = DatumGetInt32(values[Anum_gp_persistent_relfile_node_segment_file_num - 1]);

	*relationStorageManager = (PersistentFileSysRelStorageMgr)DatumGetInt16(values[Anum_gp_persistent_relfile_node_relation_storage_manager - 1]);

	*persistentState = (PersistentFileSysState)DatumGetInt16(values[Anum_gp_persistent_relfile_node_persistent_state - 1]);

	*relBufpoolKind = (PersistentFileSysRelBufpoolKind)DatumGetInt32(values[Anum_gp_persistent_relfile_node_relation_bufpool_kind - 1]);

	*parentXid = (TransactionId)DatumGetInt32(values[Anum_gp_persistent_relfile_node_parent_xid - 1]);

	*persistentSerialNum = DatumGetInt64(values[Anum_gp_persistent_relfile_node_persistent_serial_num - 1]);

	*previousFreeTid = *((ItemPointer) DatumGetPointer(values[Anum_gp_persistent_relfile_node_previous_free_tid - 1]));

	*sharedStorage = true;
}

void GpPersistentRelfileNode_SetDatumValues(
	Datum							*values,

	Oid 							tablespaceOid,
	Oid 							databaseOid,
	Oid 							relfilenodeOid,
	int32							segmentFileNum,
	PersistentFileSysRelStorageMgr	relationStorageManager,
	PersistentFileSysState			persistentState,
	PersistentFileSysRelBufpoolKind relBufpoolKind,
	TransactionId					parentXid,
	int64							persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							sharedStorage)
{
	if (persistentState != PersistentFileSysState_Free && !PersistentFileSysRelStorageMgr_IsValid(relationStorageManager))
		elog(ERROR, "Invalid value for relation storage manager (%d)",
		     relationStorageManager);

	values[Anum_gp_persistent_relfile_node_tablespace_oid - 1] =
									ObjectIdGetDatum(tablespaceOid);
	values[Anum_gp_persistent_relfile_node_database_oid - 1] =
									ObjectIdGetDatum(databaseOid);
	values[Anum_gp_persistent_relfile_node_relfilenode_oid - 1] =
									ObjectIdGetDatum(relfilenodeOid);

	values[Anum_gp_persistent_relfile_node_segment_file_num - 1] =
									Int32GetDatum(segmentFileNum);

	values[Anum_gp_persistent_relfile_node_relation_storage_manager - 1] =
									Int16GetDatum(relationStorageManager);

	values[Anum_gp_persistent_relfile_node_persistent_state - 1] =
									Int16GetDatum(persistentState);

	values[Anum_gp_persistent_relfile_node_relation_bufpool_kind - 1] =
									Int32GetDatum((int32)relBufpoolKind);

	values[Anum_gp_persistent_relfile_node_parent_xid - 1] =
									Int32GetDatum(parentXid);

	values[Anum_gp_persistent_relfile_node_persistent_serial_num - 1] =
									Int64GetDatum(persistentSerialNum);

	values[Anum_gp_persistent_relfile_node_previous_free_tid - 1] =
									PointerGetDatum(previousFreeTid);
}

void GpPersistentRelationNode_GetValues(
		Datum *values,
		Oid *tablespaceOid,
		Oid *databaseOid,
		Oid *relfilenodeOid,
		PersistentFileSysState *persistentState,
		int32 *reserved,
		TransactionId *parentXid,
		int64 *persistentSerialNum,
		ItemPointerData *previousFreeTid,
		bool *sharedStorage)
{
	*tablespaceOid = DatumGetObjectId(values[Anum_gp_persistent_relation_node_tablespace_oid - 1]);

	*databaseOid = DatumGetObjectId(values[Anum_gp_persistent_relation_node_database_oid - 1]);

	*relfilenodeOid = DatumGetObjectId(values[Anum_gp_persistent_relation_node_relfilenode_oid - 1]);

	*persistentState = DatumGetInt16(values[Anum_gp_persistent_relation_node_persistent_state - 1]);

	*reserved = DatumGetInt32(values[Anum_gp_persistent_relation_node_reserved - 1]);

	*parentXid = (TransactionId)DatumGetInt32(values[Anum_gp_persistent_relation_node_parent_xid - 1]);

	*persistentSerialNum = DatumGetInt64(values[Anum_gp_persistent_relation_node_persistent_serial_num - 1]);

	*previousFreeTid = *((ItemPointer) DatumGetPointer(values[Anum_gp_persistent_relation_node_previous_free_tid - 1]));

	*sharedStorage = true;

}

void GpPersistentRelationNode_SetDatumValues(
								Datum *values,
								Oid tablespaceOid,
								Oid databaseOid,
								Oid relfilenodeOid,
								PersistentFileSysState persistentState,
								int32 reserved,
								TransactionId parentXid,
								int64 persistentSerialNum,
								ItemPointerData *previousFreeTid,
								bool sharedStorage)
{
	values[Anum_gp_persistent_relation_node_tablespace_oid - 1] = ObjectIdGetDatum(tablespaceOid);
	values[Anum_gp_persistent_relation_node_database_oid - 1] = ObjectIdGetDatum(databaseOid);
	values[Anum_gp_persistent_relation_node_relfilenode_oid - 1] = ObjectIdGetDatum(relfilenodeOid);
	values[Anum_gp_persistent_relation_node_persistent_state - 1] = Int16GetDatum(persistentState);
	values[Anum_gp_persistent_relation_node_reserved - 1] = Int32GetDatum(reserved);
	values[Anum_gp_persistent_relation_node_parent_xid - 1] = Int32GetDatum(parentXid);
	values[Anum_gp_persistent_relation_node_persistent_serial_num - 1] = Int64GetDatum(persistentSerialNum);
	values[Anum_gp_persistent_relation_node_previous_free_tid - 1] = PointerGetDatum(previousFreeTid);
}

void GpPersistentDatabaseNode_GetValues(
	Datum							*values,
	Oid 							*tablespaceOid,
	Oid 							*databaseOid,
	PersistentFileSysState			*persistentState,
	int32							*reserved,
	TransactionId					*parentXid,
	int64							*persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							*sharedStorage)
{
    *tablespaceOid = DatumGetObjectId(values[Anum_gp_persistent_database_node_tablespace_oid - 1]);

    *databaseOid = DatumGetObjectId(values[Anum_gp_persistent_database_node_database_oid - 1]);

    *persistentState = DatumGetInt16(values[Anum_gp_persistent_database_node_persistent_state - 1]);

	*reserved = DatumGetInt32(values[Anum_gp_persistent_database_node_reserved - 1]);

	*parentXid = (TransactionId)DatumGetInt32(values[Anum_gp_persistent_database_node_parent_xid - 1]);

    *persistentSerialNum = DatumGetInt64(values[Anum_gp_persistent_database_node_persistent_serial_num - 1]);

    *previousFreeTid = *((ItemPointer) DatumGetPointer(values[Anum_gp_persistent_database_node_previous_free_tid - 1]));

	*sharedStorage = true;

}

void GpPersistentDatabaseNode_SetDatumValues(
	Datum							*values,

	Oid 							tablespaceOid,
	Oid 							databaseOid,
	PersistentFileSysState			persistentState,
	int32							reserved,
	TransactionId					parentXid,
	int64							persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							sharedStorage)
{
	values[Anum_gp_persistent_database_node_tablespace_oid - 1] = 
									ObjectIdGetDatum(tablespaceOid);
	values[Anum_gp_persistent_database_node_database_oid - 1] = 
									ObjectIdGetDatum(databaseOid);

	values[Anum_gp_persistent_database_node_persistent_state - 1] = 
									Int16GetDatum(persistentState);

	values[Anum_gp_persistent_database_node_reserved - 1] = 
									Int32GetDatum(reserved);

	values[Anum_gp_persistent_database_node_parent_xid - 1] = 
									Int32GetDatum(parentXid);

	values[Anum_gp_persistent_database_node_persistent_serial_num - 1] = 
									Int64GetDatum(persistentSerialNum);

	values[Anum_gp_persistent_database_node_previous_free_tid - 1] =
									PointerGetDatum(previousFreeTid);
}

void GpPersistentTablespaceNode_GetValues(
	Datum							*values,

	Oid 							*filespaceOid,
	Oid 							*tablespaceOid,
	PersistentFileSysState			*persistentState,
	int32							*reserved,
	TransactionId					*parentXid,
	int64							*persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							*sharedStorage)
{
    *filespaceOid = DatumGetObjectId(values[Anum_gp_persistent_tablespace_node_filespace_oid - 1]);

    *tablespaceOid = DatumGetObjectId(values[Anum_gp_persistent_tablespace_node_tablespace_oid - 1]);

    *persistentState = DatumGetInt16(values[Anum_gp_persistent_tablespace_node_persistent_state - 1]);

	*reserved = DatumGetInt32(values[Anum_gp_persistent_tablespace_node_reserved - 1]);

	*parentXid = (TransactionId)DatumGetInt32(values[Anum_gp_persistent_tablespace_node_parent_xid - 1]);

    *persistentSerialNum = DatumGetInt64(values[Anum_gp_persistent_tablespace_node_persistent_serial_num - 1]);

    *previousFreeTid = *((ItemPointer) DatumGetPointer(values[Anum_gp_persistent_tablespace_node_previous_free_tid - 1]));

	*sharedStorage = true;

}

void GpPersistentTablespaceNode_SetDatumValues(
	Datum							*values,

	Oid 							filespaceOid,
	Oid 							tablespaceOid,
	PersistentFileSysState			persistentState,
	TransactionId					parentXid,
	int64							persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							sharedStorage)
{

	values[Anum_gp_persistent_tablespace_node_filespace_oid - 1] = 
									ObjectIdGetDatum(filespaceOid);

	values[Anum_gp_persistent_tablespace_node_tablespace_oid - 1] = 
									ObjectIdGetDatum(tablespaceOid);

	values[Anum_gp_persistent_tablespace_node_persistent_state - 1] = 
									Int16GetDatum(persistentState);

	values[Anum_gp_persistent_tablespace_node_reserved - 1] = 
									Int32GetDatum(0);

	values[Anum_gp_persistent_tablespace_node_parent_xid - 1] = 
									Int32GetDatum(parentXid);

	values[Anum_gp_persistent_tablespace_node_persistent_serial_num - 1] = 
									Int64GetDatum(persistentSerialNum);

	values[Anum_gp_persistent_tablespace_node_previous_free_tid - 1] =
									PointerGetDatum(previousFreeTid);
}

void GpPersistentFilespaceNode_GetValues(
	Datum							*values,
	Oid 							*filespaceOid,
	int16							*dbId1,
	char							locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen],
	PersistentFileSysState			*persistentState,
	int32							*reserved,
	TransactionId					*parentXid,
	int64							*persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							*sharedStorage)
{
	char *locationPtr;
	int locationLen;

    *filespaceOid = DatumGetObjectId(values[Anum_gp_persistent_filespace_node_filespace_oid - 1]);

    *dbId1 = DatumGetInt16(values[Anum_gp_persistent_filespace_node_db_id - 1]);

	locationPtr = TextDatumGetCString(values[Anum_gp_persistent_filespace_node_location - 1]);;
	locationLen = strlen(locationPtr);
	if (locationLen != FilespaceLocationBlankPaddedWithNullTermLen - 1)
		elog(ERROR, "Expected filespace location to be %d characters and found %d",
			 FilespaceLocationBlankPaddedWithNullTermLen - 1,
			 locationLen);
			 
	memcpy(locationBlankPadded1, locationPtr, FilespaceLocationBlankPaddedWithNullTermLen);

    *persistentState = DatumGetInt16(values[Anum_gp_persistent_filespace_node_persistent_state - 1]);

	*reserved = DatumGetInt32(values[Anum_gp_persistent_filespace_node_reserved - 1]);

	*parentXid = (TransactionId)DatumGetInt32(values[Anum_gp_persistent_filespace_node_parent_xid - 1]);

    *persistentSerialNum = DatumGetInt64(values[Anum_gp_persistent_filespace_node_persistent_serial_num - 1]);

    *previousFreeTid = *((ItemPointer) DatumGetPointer(values[Anum_gp_persistent_filespace_node_previous_free_tid - 1]));

	*sharedStorage = true;

}

void GpPersistentFilespaceNode_SetDatumValues(
	Datum							*values,

	Oid 							filespaceOid,
	char							locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen],
	PersistentFileSysState			persistentState,
	int32							reserved,
	TransactionId					parentXid,
	int64							persistentSerialNum,
	ItemPointerData 				*previousFreeTid,
	bool							sharedStorage)
{
	int locationLen;

	locationLen = strlen(locationBlankPadded1);
	if (locationLen != FilespaceLocationBlankPaddedWithNullTermLen - 1)
		elog(ERROR, "Expected filespace location to be %d characters and found %d",
			 FilespaceLocationBlankPaddedWithNullTermLen - 1,
			 locationLen);

	values[Anum_gp_persistent_filespace_node_filespace_oid - 1] = 
									ObjectIdGetDatum(filespaceOid);

	values[Anum_gp_persistent_filespace_node_db_id - 1] =
									ObjectIdGetDatum(0);

	values[Anum_gp_persistent_filespace_node_location - 1] =
									CStringGetTextDatum(locationBlankPadded1);

	values[Anum_gp_persistent_filespace_node_persistent_state - 1] = 
									Int16GetDatum(persistentState);

	values[Anum_gp_persistent_filespace_node_reserved - 1] = 
									Int32GetDatum(reserved);

	values[Anum_gp_persistent_filespace_node_parent_xid - 1] = 
									Int32GetDatum(parentXid);

	values[Anum_gp_persistent_filespace_node_persistent_serial_num - 1] = 
									Int64GetDatum(persistentSerialNum);

	values[Anum_gp_persistent_filespace_node_previous_free_tid - 1] =
									PointerGetDatum(previousFreeTid);
}

void GpPersistent_GetCommonValues(
	PersistentFsObjType 			fsObjType,
	Datum							*values,
	PersistentFileSysObjName		*fsObjName,
	PersistentFileSysState			*persistentState,
	TransactionId					*parentXid,
	int64							*persistentSerialNum)
{
	int32 reserved;
	ItemPointerData previousFreeTid;
	bool sharedStorage;

	switch (fsObjType)
	{
	case PersistentFsObjType_RelationFile:
		{
			RelFileNode 					relFileNode;
			int32 							segmentFileNum;

			PersistentFileSysRelStorageMgr	relationStorageManager;
			PersistentFileSysRelBufpoolKind relBufpoolKind;

			GpPersistentRelfileNode_GetValues(
											values,
											&relFileNode.spcNode,
											&relFileNode.dbNode,
											&relFileNode.relNode,
											&segmentFileNum,
											&relationStorageManager,
											persistentState,
											&relBufpoolKind,
											parentXid,
											persistentSerialNum,
											&previousFreeTid,
											&sharedStorage);

			PersistentFileSysObjName_SetRelationFile(
												fsObjName,
												&relFileNode,
												segmentFileNum,
												NULL);
			fsObjName->hasInited = true;
			fsObjName->sharedStorage = sharedStorage;
			
		}
		break;

	case PersistentFsObjType_RelationDir:
		{
			RelFileNode relFileNode;

			GpPersistentRelationNode_GetValues(
									values,
									&relFileNode.spcNode,
									&relFileNode.dbNode,
									&relFileNode.relNode,
									persistentState,
									&reserved,
									parentXid,
									persistentSerialNum,
									&previousFreeTid,
									&sharedStorage);

			PersistentFileSysObjName_SetRelationDir(
									fsObjName,
									&relFileNode,
									NULL);
			fsObjName->hasInited = true;
			fsObjName->sharedStorage = sharedStorage;
		}
		break;

	case PersistentFsObjType_DatabaseDir:
		{
			DbDirNode dbDirNode;

			GpPersistentDatabaseNode_GetValues(
									values,
									&dbDirNode.tablespace,
									&dbDirNode.database,
									persistentState,
									&reserved,
									parentXid,
									persistentSerialNum,
									&previousFreeTid,
									&sharedStorage);

			PersistentFileSysObjName_SetDatabaseDir(
											fsObjName,
											dbDirNode.tablespace,
											dbDirNode.database,
											NULL);
			fsObjName->hasInited = true;
			fsObjName->sharedStorage = sharedStorage;
		}
		break;

	case PersistentFsObjType_TablespaceDir:
		{
			Oid tablespaceOid;
			Oid filespaceOid;

			GpPersistentTablespaceNode_GetValues(
											values,
											&filespaceOid,
											&tablespaceOid,
											persistentState,
											&reserved,
											parentXid,
											persistentSerialNum,
											&previousFreeTid,
											&sharedStorage);

			PersistentFileSysObjName_SetTablespaceDir(
												fsObjName, 
												tablespaceOid,
												NULL);
			fsObjName->hasInited = true;
			fsObjName->sharedStorage = sharedStorage;
		}
	break;
	
	case PersistentFsObjType_FilespaceDir:
		{
			Oid filespaceOid;
			int16 dbId1;
			char locationBlankPadded1[FilespaceLocationBlankPaddedWithNullTermLen];

			GpPersistentFilespaceNode_GetValues(
											values,
											&filespaceOid,
											&dbId1,
											locationBlankPadded1,
											persistentState,
											&reserved,
											parentXid,
											persistentSerialNum,
											&previousFreeTid,
											&sharedStorage);

			PersistentFileSysObjName_SetFilespaceDir(
												fsObjName, 
												filespaceOid,
												NULL);
			fsObjName->hasInited = true;
			fsObjName->sharedStorage = sharedStorage;
		}
	break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 fsObjType);
	}
		
}

void GpRelfileNode_GetValues(
	Datum							*values,

	Oid 							*relfilenodeOid,
	int32							*segmentFileNum,
	ItemPointer		 				persistentTid,
	int64							*persistentSerialNum)
{
	*relfilenodeOid = DatumGetObjectId(values[Anum_gp_relfile_node_relfilenode_oid - 1]);

	*segmentFileNum = DatumGetInt32(values[Anum_gp_relfile_node_segment_file_num - 1]);

	*persistentTid = *((ItemPointer) DatumGetPointer(values[Anum_gp_relfile_node_persistent_tid - 1]));

	*persistentSerialNum = DatumGetInt64(values[Anum_gp_relfile_node_persistent_serial_num - 1]);
}

void GpRelfileNode_SetDatumValues(
	Datum							*values,

	Oid 							relfilenodeOid,
	int32							segmentFileNum,
	ItemPointer		 				persistentTid,
	int64							persistentSerialNum)
{
	values[Anum_gp_relfile_node_relfilenode_oid - 1] =
									ObjectIdGetDatum(relfilenodeOid);

	values[Anum_gp_relfile_node_segment_file_num - 1] =
									Int32GetDatum(segmentFileNum);

	values[Anum_gp_relfile_node_persistent_tid - 1] =
									PointerGetDatum(persistentTid);
	
	values[Anum_gp_relfile_node_persistent_serial_num - 1] =
									Int64GetDatum(persistentSerialNum);
}

