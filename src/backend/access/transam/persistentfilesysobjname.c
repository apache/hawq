/*-------------------------------------------------------------------------
 *
 * persistentfilesysobjname.c
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
#include "postgres.h"
#include "miscadmin.h"
#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"

#include "catalog/catalog.h"
#include "catalog/gp_persistent.h"
#include "access/persistentfilesysobjname.h"
#include "storage/itemptr.h"
#include "utils/hsearch.h"
#include "storage/shmem.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "catalog/pg_tablespace.h"
#include "utils/guc.h"


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

char *PersistentFileSysObjName_ObjectName(
	const PersistentFileSysObjName		*name)
{
	char path[MAXPGPATH + 30];

	/*
	 * We don't use relpath or GetDatabasePath since they include the filespace directory.
	 *
	 * We just want the shorter version for display purposes.
	 */
	switch (name->type)
	{
	case PersistentFsObjType_RelationFile:	
		if (name->variant.rel.relFileNode.spcNode == GLOBALTABLESPACE_OID)
		{
			/* Shared system relations live in {datadir}/global */
			sprintf(path, "global/%u/%u",
					name->variant.rel.relFileNode.relNode,
					name->variant.rel.segmentFileNum);
		}
		else if (name->variant.rel.relFileNode.spcNode == DEFAULTTABLESPACE_OID)
		{
			/* The default tablespace is {datadir}/base */
			sprintf(path, "base/%u/%u/%u",
					name->variant.rel.relFileNode.dbNode,
					name->variant.rel.relFileNode.relNode,
					name->variant.rel.segmentFileNum);
		}
		else
		{
			/* All other tablespaces are accessed via filespace locations */
			sprintf(path, "%u/%u/%u/%u",
					 name->variant.rel.relFileNode.spcNode,
					 name->variant.rel.relFileNode.dbNode,
					 name->variant.rel.relFileNode.relNode,
					 name->variant.rel.segmentFileNum);
		}
		break;
	case PersistentFsObjType_RelationDir:
		if (name->variant.rel.relFileNode.spcNode == GLOBALTABLESPACE_OID)
		{
			/* Shared system relations live in {datadir}/global */
			sprintf(path, "global/%u",
					name->variant.rel.relFileNode.relNode);
		}
		else if (name->variant.rel.relFileNode.spcNode == DEFAULTTABLESPACE_OID)
		{
			/* The default tablespace is {datadir}/base */
			sprintf(path, "base/%u/%u",
					name->variant.rel.relFileNode.dbNode,
					name->variant.rel.relFileNode.relNode);
		}
		else
		{
			/* All other tablespaces are accessed via filespace locations. */
			sprintf(path, "%u/%u/%u",
					name->variant.rel.relFileNode.spcNode,
					name->variant.rel.relFileNode.dbNode,
					name->variant.rel.relFileNode.relNode);
		}
		break;
	case PersistentFsObjType_DatabaseDir:	
		if (name->variant.dbDirNode.tablespace == GLOBALTABLESPACE_OID)
		{
			/* Shared system relations live in {datadir}/global */
			strcpy(path, "global");
		}
		else if (name->variant.dbDirNode.tablespace == DEFAULTTABLESPACE_OID)
		{
			/* The default tablespace is {datadir}/base */
			sprintf(path, "base/%u",
					name->variant.dbDirNode.database);
		}
		else
		{
			/* All other tablespaces are accessed via filespace locations */
			sprintf(path, "%u/%u",
					 name->variant.dbDirNode.tablespace,
					 name->variant.dbDirNode.database);
		}
		break;
	case PersistentFsObjType_TablespaceDir:
		sprintf(path, "%u", name->variant.tablespaceOid);
		break;
	case PersistentFsObjType_FilespaceDir:
		sprintf(path, "%u", name->variant.filespaceOid);
		break;

	default:
		elog(ERROR, "Unexpected persistent file-system object type: %d",
			 name->type);
		return pstrdup("Unknown");
	}

	return pstrdup(path);
}

char *PersistentFileSysObjName_TypeName(
	const PersistentFsObjType		type)
{
	switch (type)
	{
	case PersistentFsObjType_RelationFile: 		return "Relation File";
	case PersistentFsObjType_RelationDir:		return "Relation Directory";
	case PersistentFsObjType_DatabaseDir: 		return "Database Directory";
	case PersistentFsObjType_TablespaceDir: 		return "Tablespace Directory";
	case PersistentFsObjType_FilespaceDir: 		return "Filespace Directory";
	default:
		return "Unknown";
	}
}

char *PersistentFileSysObjName_TypeAndObjectName(
	const PersistentFileSysObjName		*name)
{
	char *typeName;
	char *objectName;

	char resultLen;
	char *result;

	typeName = PersistentFileSysObjName_TypeName(name->type);
	objectName = PersistentFileSysObjName_ObjectName(name);

	resultLen = strlen(typeName) + 4 + strlen(objectName) + 1;
	result = (char*)palloc(resultLen);

	snprintf(result, resultLen, "%s: '%s'", typeName, objectName);

	pfree(objectName);

	return result;
	
}


int PersistentFileSysObjName_Compare(
	const PersistentFileSysObjName		*name1,
	const PersistentFileSysObjName		*name2)
{
	int compareLen = 0;
	int cmp;

	if (name1->type == name2->type)
	{		
		switch (name1->type)
		{
		case PersistentFsObjType_RelationFile:	
			compareLen = offsetof(PersistentFileSysObjName,variant.rel.segmentFileNum) + sizeof(int32);
			break;
		case PersistentFsObjType_RelationDir:
			compareLen = sizeof(RelFileNode);
			break;
		case PersistentFsObjType_DatabaseDir:	
			compareLen = sizeof(DbDirNode);
			break;
		case PersistentFsObjType_TablespaceDir: 
			compareLen = sizeof(Oid);
			break;
		case PersistentFsObjType_FilespaceDir:	
			compareLen = sizeof(Oid);
			break;
		default:
			elog(ERROR, "Unexpected persistent file-system object type: %d",
				 name1->type);
			return 0;
		}

		cmp = memcmp(
					&name1->variant, 
					&name2->variant,
					compareLen);
		if (cmp == 0)
		{
			/*
			 * Handle segmentFileNum for 'Relation File's.
			 */
			if (name1->variant.rel.segmentFileNum == name2->variant.rel.segmentFileNum)
				return 0;
			else if (name1->variant.rel.segmentFileNum > name2->variant.rel.segmentFileNum)
				return 1;
			else
				return -1;
		}
		else if (cmp > 0)
			return 1;
		else
			return -1;
	}
	else if (name1->type > name2->type)
		return 1;
	else
		return -1;
}

char *PersistentFileSysObjState_Name(
	PersistentFileSysState state)
{
	switch (state)
	{
	case PersistentFileSysState_Free: 			return "Free";
	case PersistentFileSysState_CreatePending: 	return "Create Pending";
	case PersistentFileSysState_Created: 		return "Created";
	case PersistentFileSysState_DropPending: 	return "Drop Pending";
	case PersistentFileSysState_AbortingCreate: return "Aborting Create";

	case PersistentFileSysState_JustInTimeCreatePending: return "Just-In-Time Create Pending";

	case PersistentFileSysState_BulkLoadCreatePending: 	return "Bulk Load Create Pending";

	default:
		return "Unknown";
	}
}

char *PersistentFileSysRelStorageMgr_Name(
	PersistentFileSysRelStorageMgr relStorageMgr)
{
	switch (relStorageMgr)
	{
	case PersistentFileSysRelStorageMgr_None: 		return "None";
	case PersistentFileSysRelStorageMgr_BufferPool:	return "Buffer Pool";
	case PersistentFileSysRelStorageMgr_AppendOnly:	return "Append-Only";
		
	default:
		return "Unknown";
	}
}

char *PersistentFileSysRelBufpoolKind_Name(
	PersistentFileSysRelBufpoolKind relBufpoolKind)
{
	switch (relBufpoolKind)
	{
	case PersistentFileSysRelBufpoolKind_None:						return "None";
	case PersistentFileSysRelBufpoolKind_Heap:						return "Heap";
	case PersistentFileSysRelBufpoolKind_UnknownRelStorage:			return "UnknownRelStorage";
	case PersistentFileSysRelBufpoolKind_AppendOnlySeginfo:			return "AppendOnlySeginfo";
	case PersistentFileSysRelBufpoolKind_AppendOnlyBlockDirectory:	return "AppendOnlyBlockDirectory";
	case PersistentFileSysRelBufpoolKind_Btree:						return "Btree";
	case PersistentFileSysRelBufpoolKind_BitMap:					return "BitMap";
	case PersistentFileSysRelBufpoolKind_UnknownIndex:				return "UnknownIndex";
	case PersistentFileSysRelBufpoolKind_Sequence:					return "Sequence";
	case PersistentFileSysRelBufpoolKind_Toast:						return "Toast";
	case PersistentFileSysRelBufpoolKind_UncatalogedHeap:			return "UncatalogedHeap";
	case PersistentFileSysRelBufpoolKind_UnknownRelKind:			return "UnknownRelKind";

	default:
		return "Unknown";
	}
}
