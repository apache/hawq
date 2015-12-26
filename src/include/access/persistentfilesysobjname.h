/*-------------------------------------------------------------------------
 *
 * persistentfilesysobjname.h
 *
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
 *
 *-------------------------------------------------------------------------
 */
#ifndef PERSISTENTFILESYSOBJNAME_H
#define PERSISTENTFILESYSOBJNAME_H

#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"
#include "storage/dbdirnode.h"
#include "storage/tablespacedirnode.h"

typedef enum PersistentFsObjType
{
	PersistentFsObjType_First = 0,	// Must start at 0 for 0-based indexing.
	PersistentFsObjType_RelationFile = PersistentFsObjType_First,
	PersistentFsObjType_RelationDir = 1,
	PersistentFsObjType_DatabaseDir = 2,
	PersistentFsObjType_TablespaceDir = 3,
	PersistentFsObjType_FilespaceDir = 4,
	PersistentFsObjType_Last = PersistentFsObjType_FilespaceDir,
	CountPersistentFsObjType = PersistentFsObjType_Last - PersistentFsObjType_First + 1
} PersistentFsObjType;

inline static bool PersistentFsObjType_IsValid(
	PersistentFsObjType	fsObjType)
{
	return (fsObjType >= PersistentFsObjType_First &&
		    fsObjType <= PersistentFsObjType_Last);
}

typedef union PersistentFileSysObjNameVariant
{
	struct rel
	{
		RelFileNode 	relFileNode;

		int32 segmentFileNum;
	} rel; 

	DbDirNode		dbDirNode;

	Oid 	tablespaceOid;
	
	Oid 	filespaceOid;

} PersistentFileSysObjNameVariant;

typedef struct PersistentFileSysObjName
{
	PersistentFsObjType		type;

	PersistentFileSysObjNameVariant variant;
	/* If tablespace is a shared storage, we skip mirroring checking. */
	bool	hasInited;
	bool	sharedStorage;
} PersistentFileSysObjName;

inline static void PersistentFileSysObjName_SetRelationFile(
	PersistentFileSysObjName	*fsObjName,
	RelFileNode					*relFileNode,
	int32						segmentFileNum,
	bool 						(*getSharedStorage) (Oid))
{
	MemSet(fsObjName, 0, sizeof(PersistentFileSysObjName));
	fsObjName->type = PersistentFsObjType_RelationFile;
	memcpy(&(fsObjName->variant.rel.relFileNode), relFileNode, sizeof(RelFileNode));
	fsObjName->variant.rel.segmentFileNum = segmentFileNum;

	if (fsObjName->hasInited || !getSharedStorage)
		return;
	fsObjName->hasInited = true;
	fsObjName->sharedStorage = getSharedStorage(relFileNode->spcNode);
}

inline static RelFileNode PersistentFileSysObjName_GetRelFileNode(
	PersistentFileSysObjName	*fsObjName)
{
	Assert(fsObjName->type == PersistentFsObjType_RelationFile);

	return fsObjName->variant.rel.relFileNode;
}

inline static RelFileNode *PersistentFileSysObjName_GetRelFileNodePtr(
	PersistentFileSysObjName	*fsObjName)
{
	Assert(fsObjName->type == PersistentFsObjType_RelationFile);

	return &fsObjName->variant.rel.relFileNode;
}

inline static int32 PersistentFileSysObjName_GetSegmentFileNum(
	PersistentFileSysObjName	*fsObjName)
{
	Assert(fsObjName->type == PersistentFsObjType_RelationFile);

	return fsObjName->variant.rel.segmentFileNum;
}

inline static void PersistentFileSysObjName_SetDatabaseDir(
	PersistentFileSysObjName	*fsObjName,
	Oid							tablespaceOid,
	Oid							databaseOid,
	bool 						(*getSharedStorage) (Oid))
{
	MemSet(fsObjName, 0, sizeof(PersistentFileSysObjName));
	fsObjName->type = PersistentFsObjType_DatabaseDir;
	fsObjName->variant.dbDirNode.tablespace = tablespaceOid;
	fsObjName->variant.dbDirNode.database = databaseOid;
	if (fsObjName->hasInited || !getSharedStorage)
		return;
	fsObjName->hasInited = true;
	fsObjName->sharedStorage = getSharedStorage(tablespaceOid);
}

inline static void PersistentFileSysObjName_SetRelationDir(
	PersistentFileSysObjName *fsObjName,
	RelFileNode *relFileNode,
	bool (*getSharedStorage)(Oid))
{
	MemSet(fsObjName, 0, sizeof(PersistentFileSysObjName));
	fsObjName->type = PersistentFsObjType_RelationDir;
	memcpy(&(fsObjName->variant.rel.relFileNode), relFileNode, sizeof(RelFileNode));

	if (fsObjName->hasInited || !getSharedStorage)
	{
		return;
	}
	fsObjName->hasInited = true;
	fsObjName->sharedStorage = getSharedStorage(relFileNode->spcNode);
}

inline static DbDirNode *PersistentFileSysObjName_GetDbDirNodePtr(
	PersistentFileSysObjName	*fsObjName)
{
	Assert(fsObjName->type == PersistentFsObjType_DatabaseDir);

	return &fsObjName->variant.dbDirNode;
}

inline static void PersistentFileSysObjName_SetTablespaceDir(
	PersistentFileSysObjName	*fsObjName,
	Oid							tablespaceOid,
	bool 						(*getSharedStorage) (Oid))
{
	MemSet(fsObjName, 0, sizeof(PersistentFileSysObjName));
	fsObjName->type = PersistentFsObjType_TablespaceDir;
	fsObjName->variant.tablespaceOid = tablespaceOid;
	if (fsObjName->hasInited || !getSharedStorage)
		return;
	fsObjName->hasInited = true;
	fsObjName->sharedStorage = getSharedStorage(tablespaceOid);
}

inline static Oid PersistentFileSysObjName_GetTablespaceDir(
	PersistentFileSysObjName	*fsObjName)
{
	Assert(fsObjName->type == PersistentFsObjType_TablespaceDir);

	return fsObjName->variant.tablespaceOid;
}

inline static void PersistentFileSysObjName_SetFilespaceDir(
	PersistentFileSysObjName	*fsObjName,
	Oid							filespaceOid,
	bool 						(*getSharedStorage) (Oid))
{
	MemSet(fsObjName, 0, sizeof(PersistentFileSysObjName));
	fsObjName->type = PersistentFsObjType_FilespaceDir;
	fsObjName->variant.filespaceOid = filespaceOid;
	if (fsObjName->hasInited || !getSharedStorage)
		return;
	fsObjName->hasInited = true;
	fsObjName->sharedStorage = getSharedStorage(filespaceOid);
}

inline static Oid PersistentFileSysObjName_GetFilespaceDir(
	PersistentFileSysObjName	*fsObjName)
{
	Assert(fsObjName->type == PersistentFsObjType_FilespaceDir);

	return fsObjName->variant.filespaceOid;
}

/*
 * The file kinds of a persistent file-system object.
 */
typedef enum PersistentFileSysRelStorageMgr
{
	PersistentFileSysRelStorageMgr_None = 0,
	PersistentFileSysRelStorageMgr_BufferPool = 1,
	PersistentFileSysRelStorageMgr_AppendOnly = 2,
	MaxPersistentFileSysRelStorageMgr /* must always be last */
} PersistentFileSysRelStorageMgr;

inline static bool PersistentFileSysRelStorageMgr_IsValid(
	PersistentFileSysRelStorageMgr	relStorageMgr)
{
	return (relStorageMgr == PersistentFileSysRelStorageMgr_BufferPool ||
		    relStorageMgr == PersistentFileSysRelStorageMgr_AppendOnly);
}


/*
 * The Buffer Pool relation kinds of a persistent file-system object.
 */
typedef enum PersistentFileSysRelBufpoolKind
{
	PersistentFileSysRelBufpoolKind_None = 0,
	PersistentFileSysRelBufpoolKind_Heap = 1,
	PersistentFileSysRelBufpoolKind_UnknownRelStorage = 2,
	PersistentFileSysRelBufpoolKind_AppendOnlySeginfo = 3,
	PersistentFileSysRelBufpoolKind_AppendOnlyBlockDirectory = 4,
	PersistentFileSysRelBufpoolKind_Btree = 5,
	PersistentFileSysRelBufpoolKind_BitMap = 6,
	PersistentFileSysRelBufpoolKind_UnknownIndex = 7,
	PersistentFileSysRelBufpoolKind_Sequence = 8,
	PersistentFileSysRelBufpoolKind_Toast = 9,
	PersistentFileSysRelBufpoolKind_UncatalogedHeap = 10,
	PersistentFileSysRelBufpoolKind_UnknownRelKind = 11,
	MaxPersistentFileSysRelBufpoolKind /* must always be last */
} PersistentFileSysRelBufpoolKind;

/*
 * The states of a persistent file-system object.
 */
typedef enum PersistentFileSysState
{
	PersistentFileSysState_Free = 0,
	PersistentFileSysState_CreatePending = 1,
	PersistentFileSysState_Created = 2,
	PersistentFileSysState_DropPending = 3,
	PersistentFileSysState_AbortingCreate = 4,
	PersistentFileSysState_JustInTimeCreatePending = 5,
	PersistentFileSysState_BulkLoadCreatePending,
	MaxPersistentFileSysState /* must always be last */
} PersistentFileSysState;


/*
 * This module is for generic relation file create and drop.
 *
 * For create, it makes the file-system create of an empty file fully transactional so
 * the relation file will be deleted even on system crash.  The relation file could be a heap,
 * index, or append-only (row- or column-store).
 */

// -----------------------------------------------------------------------------
// Helper
// -----------------------------------------------------------------------------

extern char *PersistentFileSysObjName_ObjectName(
	const PersistentFileSysObjName		*name);

extern char *PersistentFileSysObjName_TypeName(
		PersistentFsObjType		type);

extern char *PersistentFileSysObjName_TypeAndObjectName(
		const PersistentFileSysObjName		*name);

extern int PersistentFileSysObjName_Compare(
	const PersistentFileSysObjName		*name1,
	const PersistentFileSysObjName		*name2);

extern char *PersistentFileSysObjState_Name(
	PersistentFileSysState state);

extern char *PersistentFileSysRelStorageMgr_Name(
	PersistentFileSysRelStorageMgr relStorageMgr);

extern char *PersistentFileSysRelBufpoolKind_Name(
		PersistentFileSysRelBufpoolKind relBufpoolKind);

#endif   /* PERSISTENTFILESYSOBJNAME_H */

