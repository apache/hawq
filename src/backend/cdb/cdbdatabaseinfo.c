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
 * cdbdatabaseinfo.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include "storage/fd.h"
#include "storage/relfilenode.h"
#include "storage/dbdirnode.h"
#include "cdb/cdbdatabaseinfo.h"
#include "catalog/catalog.h"
#include "access/relscan.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "catalog/pg_tablespace.h"
#include "cdb/cdbpersistentdatabase.h"
#include "cdb/cdbdirectopen.h"
#include "catalog/pg_appendonly.h"
#include "access/aosegfiles.h"
#include "access/appendonlytid.h"
#include "utils/guc.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "commands/dbcommands.h"


/*-------------------------------------------------------------------------
 * Local static type declarations
 *------------------------------------------------------------------------- */

/* hash table entry for relation ids */
typedef struct RelationIdEntry
{
	Oid			 relationId;  	/* key */
	DbInfoRel 	*dbInfoRel;   	/* pointer */
} RelationIdEntry;


/*-------------------------------------------------------------------------
 * Debugging functions
 *------------------------------------------------------------------------- */

#ifdef suppress

/*
 * DatabaseInfo_Check()
 *   Validate that the DatabaseInfo returned is consistent.
 */
void DatabaseInfo_Check(DatabaseInfo *info)
{
	int sr;
	int rsf;

	/*
	 * Compare Stored Relations to Relation Segment Files.
	 */
	sr = 0;
	rsf = 0;
	while (true)
	{
		int cmp;

		cmp = TablespaceRelFile_Compare(
						&info->pgClassStoredRelations[sr].tablespaceRelFile,
						&info->relSegFiles[rsf].tablespaceRelFile);
		if (cmp != 0)
		{
			elog(WARNING, "In database %u, stored relation doesn't match relation file on disk "
						"(stored tablespace %u, disk file tablespace %u, "
						"stored relation %u, disk file relation %u)",
						info->database,
						info->pgClassStoredRelations[sr].tablespaceRelFile.tablespace,
						info->relSegFiles[rsf].tablespaceRelFile.tablespace,
						info->pgClassStoredRelations[sr].tablespaceRelFile.relation,
						info->relSegFiles[rsf].tablespaceRelFile.relation);
			return;
		}

		/*
		 * Skip multiple relation segment files.
		 */
		while (true)
		{
			rsf++;
			if (rsf >= info->relSegFilesCount)
				break;
			if (TablespaceRelFile_Compare(
						&info->pgClassStoredRelations[sr].tablespaceRelFile,
						&info->relSegFiles[rsf].tablespaceRelFile) != 0)
				break;
		}

		sr++;

		if (sr >= info->pgClassStoredRelationsCount ||
			rsf >= info->relSegFilesCount)
		{
			if (sr < info->pgClassStoredRelationsCount)
			{
				elog(WARNING, "In database %u, extra stored relation (tablespace %u, relation %u)",
				     info->database,
					 info->pgClassStoredRelations[sr].tablespaceRelFile.tablespace,
					 info->pgClassStoredRelations[sr].tablespaceRelFile.relation);
				return;
			}

			if (rsf < info->relSegFilesCount)
			{
				elog(WARNING, "In database %u, extra relation file on disk (tablespace %u, relation %u)",
				     info->database,
					 info->relSegFiles[rsf].tablespaceRelFile.tablespace,
					 info->relSegFiles[rsf].tablespaceRelFile.relation);
				return;
			}
			break;
		}
	}
}

/*
 * DatabaseInfo_Trace()
 *   Output debugging information about the DatabaseInfo
 */
void DatabaseInfo_Trace(DatabaseInfo *info)
{
	int t;
	int sr;
	int rsf;
	int grn;
	int m;

	for (t = 0; t < info->tablespacesCount; t++)
		elog(WARNING, "Database Info: Tablespace #%d is %u",
			 t, info->tablespaces[t]);

	for (sr = 0; sr < info->pgClassStoredRelationsCount; sr++)
		elog(WARNING, "Database Info: Stored relation (tablespace %u, relation %u, isBufferPoolRealtion %s, TID %s)",
			 info->pgClassStoredRelations[sr].tablespaceRelFile.tablespace, 
			 info->pgClassStoredRelations[sr].tablespaceRelFile.relation,
			 (info->pgClassStoredRelations[sr].isBufferPoolRelation ? "true" : "false"),
			 ItemPointerToString(&info->pgClassStoredRelations[sr].pgClassTid));

	for (rsf = 0; rsf < info->relSegFilesCount; rsf++)
		elog(WARNING, "Database Info: Relation segment file (tablespace %u, relation %u, segment file num %d)",
			 info->relSegFiles[rsf].tablespaceRelFile.tablespace, 
			 info->relSegFiles[rsf].tablespaceRelFile.relation, 
			 info->relSegFiles[rsf].segmentFileNum);

	for (grn = 0; grn < info->gpRelationNodesCount; grn++)
		elog(WARNING, "Database Info: Tablespace %u, relation %u node information (persistent TID %s, perstent serial number " INT64_FORMAT ")",
			 info->gpRelationNodes[grn].tablespaceRelFile.tablespace, 
			 info->gpRelationNodes[grn].tablespaceRelFile.relation, 
			 ItemPointerToString(&info->gpRelationNodes[grn].persistentTid),
			 info->gpRelationNodes[grn].persistentSerialNum);

	for (m = 0; m < info->miscEntriesCount; m++)
		elog(WARNING, "Database Info: Misc entry #%d (tablespace %u, directory = %s, name '%s')",
			 m, 
			 info->miscEntries[m].tablespace,
			 (info->miscEntries[m].isDir ? "true" : "false"),
			 info->miscEntries[m].name);
}


/*
 * DatabaseInfo_FindDbInfoRel()
 *   Lookup an entry in the info hash table.
 *   
 * Note: called nowhere in the source, purely available for debugging.
 */
static DbInfoRel *DatabaseInfo_FindDbInfoRel(
	HTAB 				*dbInfoRelHashTable,
	
	Oid					relfilenodeOid)
{
	DbInfoRel *dbInfoRel;

	bool found;
	
	dbInfoRel = 
			(DbInfoRel*) 
					hash_search(dbInfoRelHashTable,
								(void *) &relfilenodeOid,
								HASH_FIND,
								&found);
	if (!found)
	{
		elog(ERROR, "pg_class entry (relfilenode %u) not found",
			 relfilenodeOid);
		return NULL;
	}

	return dbInfoRel;
}

#endif

/*-------------------------------------------------------------------------
 * Local static function definitions
 *------------------------------------------------------------------------- */

/*
 * DatabaseInfo_Grow()
 *   Used for extensible arrays.
 *
 * XXX - Why not just use repalloc?
 */
static void DatabaseInfo_Grow(
	void 	**array,
	int32	arrayCount,
	int32	*arrayMaxCount,
	int32	elementLen)
{
	void *newArray;
	
	(*arrayMaxCount) *= 2;
	newArray = palloc((*arrayMaxCount)*elementLen);
	memcpy(
		newArray, 
		(*array), 
		arrayCount*elementLen);
	pfree(*array);
	*array = newArray;
}

/*
 * DatabaseInfo_DbInfoRelHashTableInit()
 *    Construct a hash table of DbInfoRel
 */
static HTAB*
DatabaseInfo_DbInfoRelHashTableInit()
{
	HASHCTL			info;
	int				hash_flags;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(DbInfoRel);
	info.hash = tag_hash;

	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	return hash_create("DbInfoRel", 100, &info, hash_flags);
}

/*
 * DatabaseInfo_RelationIdHashTableInit()
 *    Construct a hash table of RelationIdEntry
 */
static HTAB*
DatabaseInfo_RelationIdHashTableInit()
{
	HASHCTL			info;
	int				hash_flags;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(RelationIdEntry);
	info.hash = tag_hash;

	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	return hash_create("RelationId", 100, &info, hash_flags);
}


/*
 * DatabaseInfo_PgAppendOnlyHashTableInit()
 *    Construct a hash table of PgAppendOnlyHashEntry
 */
static HTAB *
DatabaseInfo_PgAppendOnlyHashTableInit()
{
	HASHCTL			info;
	int				hash_flags;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(PgAppendOnlyHashEntry);
	info.hash = tag_hash;

	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	return hash_create("PgAppendOnly", 100, &info, hash_flags);
}


/*
 * DatabaseInfo_AddRelationId()
 *   Add an entry to a dbInfoRel hash table
 */
static void DatabaseInfo_AddRelationId(
	HTAB 		*relationIdHashTable,
	DbInfoRel 	*dbInfoRel)
{
	RelationIdEntry *relationIdEntry;

	bool found;
	
	relationIdEntry = 
			(RelationIdEntry*) 
					hash_search(relationIdHashTable,
								(void *) &dbInfoRel->relationOid,
								HASH_ENTER,
								&found);
	if (found)
	{
		elog(ERROR, "Duplicate pg_class entry (relation id %u)",
			 dbInfoRel->relationOid);
	}

	relationIdEntry->dbInfoRel = dbInfoRel;
}

/*
 * DatabaseInfo_FindRelationId()
 *   Lookup an entry to a dbInfoRel hash table
 */
static DbInfoRel *DatabaseInfo_FindRelationId(
	HTAB 		*dbInfoRelHashTable,
	Oid			 relationId)
{
	RelationIdEntry *relationIdEntry;

	bool found;
	
	relationIdEntry = 
			(RelationIdEntry*) 
					hash_search(dbInfoRelHashTable,
								(void *) &relationId,
								HASH_FIND,
								&found);
	if (!found)
	{
		elog(ERROR, "pg_class entry (relation id %u) not found",
			 relationId);
		return NULL;
	}

	return relationIdEntry->dbInfoRel;
}

/*
 * DatabaseInfo_AddPgAppendOnly()
 *   Add an entry to a pgAppendOnly hash table.
 */
static void DatabaseInfo_AddPgAppendOnly(
	HTAB 				*pgAppendOnlyHashTable,	
	Oid					 relationId,
	AppendOnlyEntry 	*aoEntry)
{
	PgAppendOnlyHashEntry *pgAppendOnlyHashEntry;

	bool found;
	
	pgAppendOnlyHashEntry = 
			(PgAppendOnlyHashEntry*) 
					hash_search(pgAppendOnlyHashTable,
								(void *) &relationId,
								HASH_ENTER,
								&found);
	if (found)
		elog(ERROR, "More than one pg_appendonly entry (relation id %u)",
			 relationId);

	pgAppendOnlyHashEntry->aoEntry = aoEntry;
}


/*
 * DatabaseInfo_FindPgAppendOnly()
 *   Lookup an entry to a pgAppendOnly hash table.
 */
static AppendOnlyEntry *DatabaseInfo_FindPgAppendOnly(
	HTAB 				*pgAppendOnlyHashTable,
	
	Oid					relationId)
{
	PgAppendOnlyHashEntry *pgAppendOnlyHashEntry;

	bool found;
	
	pgAppendOnlyHashEntry = 
			(PgAppendOnlyHashEntry*) 
					hash_search(pgAppendOnlyHashTable,
								(void *) &relationId,
								HASH_FIND,
								&found);
	if (!found)
	{
		elog(ERROR, "pg_appendonly entry (relation id %u) not found",
			 relationId);
		return NULL;
	}

	return pgAppendOnlyHashEntry->aoEntry;
}


/*
 * DatabaseInfo_AddTablespace()
 *   Add a tablespace to the DatabaseInfo
 */
static void DatabaseInfo_AddTablespace(
	DatabaseInfo 		*info,
	Oid 				tablespace)
{
	int t;

	t = 0;
	while (true)
	{
		if (t >= info->tablespacesCount)
		{
			Assert(t == info->tablespacesCount);
			if (t >= info->tablespacesMaxCount)
			{
				DatabaseInfo_Grow(
								(void**)&info->tablespaces,
								info->tablespacesCount,
								&info->tablespacesMaxCount,
								sizeof(Oid));
			}
			info->tablespaces[info->tablespacesCount++] = tablespace;
			break;
		}
		
		if (info->tablespaces[t] == tablespace)
			break;
	
		t++;
	}
}

static void DatabaseInfo_AddExtraSegmentFile(
	DatabaseInfo 		*info,
	Oid 				tablespace,
	Oid					relfilenode,
	int32				segmentFileNum,
	int64				eof)
{
	DbInfoExtraSegmentFile	*dbInfoExtraSegmentFile;

	if (info->extraSegmentFilesCount>= info->extraSegmentFilesMaxCount)
	{
		DatabaseInfo_Grow(
						(void**)&info->extraSegmentFiles,
						info->extraSegmentFilesCount,
						&info->extraSegmentFilesMaxCount,
						sizeof(DbInfoExtraSegmentFile));
	}

	dbInfoExtraSegmentFile = 
					&info->extraSegmentFiles[info->extraSegmentFilesCount];
	info->extraSegmentFilesCount++;
	
	dbInfoExtraSegmentFile->relfilenode = relfilenode;
	dbInfoExtraSegmentFile->segmentFileNum = segmentFileNum;
	dbInfoExtraSegmentFile->tablespaceOid = tablespace;
	dbInfoExtraSegmentFile->eof = eof;
}

static void DatabaseInfo_AddAppendOnlyCatalogSegmentInfo(
	DbInfoRel 				*dbInfoRel,
	int4					contentid,
	int32 					segmentFileNum,
	int64					logicalEof)
{
	DbInfoAppendOnlyCatalogSegmentInfo 	*appendOnlyCatalogSegmentInfo;

	if (dbInfoRel->appendOnlyCatalogSegmentInfoCount >= dbInfoRel->appendOnlyCatalogSegmentInfoMaxCount)
	{
		DatabaseInfo_Grow(
						(void**)&dbInfoRel->appendOnlyCatalogSegmentInfo,
						dbInfoRel->appendOnlyCatalogSegmentInfoCount,
						&dbInfoRel->appendOnlyCatalogSegmentInfoMaxCount,
						sizeof(DbInfoAppendOnlyCatalogSegmentInfo));
	}

	appendOnlyCatalogSegmentInfo = &dbInfoRel->appendOnlyCatalogSegmentInfo[dbInfoRel->appendOnlyCatalogSegmentInfoCount];
	dbInfoRel->appendOnlyCatalogSegmentInfoCount++;

	appendOnlyCatalogSegmentInfo->segmentFileNum = segmentFileNum;
	appendOnlyCatalogSegmentInfo->logicalEof = logicalEof;

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "DatabaseInfo_AddAppendOnlyCatalogSegmentInfo: relation id %u, relation name %s, relfilenode %u, segment file #%d, EOF " INT64_FORMAT,
			 dbInfoRel->relationOid,
			 dbInfoRel->relname,
			 dbInfoRel->relfilenodeOid,
			 segmentFileNum,
			 logicalEof);

}


static void DatabaseInfo_AddPgClassStoredRelation(
	DatabaseInfo 		*info,
	HTAB 				*dbInfoRelHashTable,
	HTAB				*relationIdHashTable,
	Oid 				relfilenode,
	ItemPointer			pgClassTid,
	Oid					relationOid,
	char				*relname,
	Oid					reltablespace,
	char				relkind,
	char				relstorage,
	Oid					relam,
	int					relnatts)
{
	DbInfoRel	*dbInfoRel;
	bool		 found;

	dbInfoRel = 
			(DbInfoRel*) 
					hash_search(dbInfoRelHashTable,
								(void *) &relfilenode,
								HASH_ENTER,
								&found);
	if (found)
		elog(ERROR, "More than one pg_class entry ('%s' %u and '%s' %u) references the same relfilenode %u",
			 dbInfoRel->relname,
			 dbInfoRel->relationOid,
			 relname,
			 relationOid,
			 relfilenode);

	dbInfoRel->inPgClass = true;
	dbInfoRel->pgClassTid = *pgClassTid;
	dbInfoRel->relationOid = relationOid;
	dbInfoRel->relname = pstrdup(relname);
	dbInfoRel->reltablespace = reltablespace;
	dbInfoRel->relkind = relkind;
	dbInfoRel->relstorage = relstorage;
	dbInfoRel->relam = relam;
	dbInfoRel->relnatts = relnatts;

	dbInfoRel->gpRelationNodesMaxCount = 1;
	dbInfoRel->gpRelationNodes = 
			palloc0(dbInfoRel->gpRelationNodesMaxCount * sizeof(DbInfoGpRelationNode));
	dbInfoRel->gpRelationNodesCount = 0;
	
	dbInfoRel->appendOnlyCatalogSegmentInfoMaxCount= 1;
	dbInfoRel->appendOnlyCatalogSegmentInfo = 
			palloc0(dbInfoRel->appendOnlyCatalogSegmentInfoMaxCount * sizeof(DbInfoAppendOnlyCatalogSegmentInfo));
	dbInfoRel->appendOnlyCatalogSegmentInfoCount = 0;
	
	dbInfoRel->physicalSegmentFilesMaxCount = 1;
	dbInfoRel->physicalSegmentFiles = 
			palloc0(dbInfoRel->physicalSegmentFilesMaxCount * sizeof(DbInfoSegmentFile));
	dbInfoRel->physicalSegmentFilesCount = 0;

	DatabaseInfo_AddRelationId(
						relationIdHashTable,
						dbInfoRel);
}

static bool DatabaseInfo_AddGpRelationNode(
	DatabaseInfo		*info,
	HTAB 				*dbInfoRelHashTable,
	Oid					relfilenode,
	int32				segmentFileNum,
	ItemPointer			persistentTid,
	int64				persistentSerialNum,
	ItemPointer			gpRelationNodeTid)
{
	DbInfoRel *dbInfoRel;
	bool found;

	DbInfoGpRelationNode *dbInfoGpRelationNode;

	dbInfoRel = 
			(DbInfoRel*) 
					hash_search(dbInfoRelHashTable,
								(void *) &relfilenode,
								HASH_FIND,
								&found);

	//Changes to solve MPP-16346
	if(!dbInfoRel)
			return found;

	if (found)
	{
		if (dbInfoRel->gpRelationNodesCount >= dbInfoRel->gpRelationNodesMaxCount)
		{
			DatabaseInfo_Grow(
							(void**)&dbInfoRel->gpRelationNodes,
							dbInfoRel->gpRelationNodesCount,
							&dbInfoRel->gpRelationNodesMaxCount,
							sizeof(DbInfoGpRelationNode));
		}

		dbInfoGpRelationNode = 
						&dbInfoRel->gpRelationNodes[dbInfoRel->gpRelationNodesCount];
		dbInfoRel->gpRelationNodesCount++;
	}
	else
	{
		if (info->parentlessGpRelationNodesCount >= dbInfoRel->physicalSegmentFilesMaxCount)
		{
			DatabaseInfo_Grow(
							(void**)&info->parentlessGpRelationNodes,
							info->parentlessGpRelationNodesCount,
							&info->parentlessGpRelationNodesMaxCount,
							sizeof(DbInfoGpRelationNode));
		}

		dbInfoGpRelationNode = 
						&info->parentlessGpRelationNodes[info->parentlessGpRelationNodesCount];
		info->parentlessGpRelationNodesCount++;
	}
	
	dbInfoGpRelationNode->gpRelationNodeTid = *gpRelationNodeTid;
	dbInfoGpRelationNode->relfilenodeOid = relfilenode;
	dbInfoGpRelationNode->segmentFileNum = segmentFileNum;
	dbInfoGpRelationNode->persistentTid = *persistentTid;
	dbInfoGpRelationNode->persistentSerialNum = persistentSerialNum;
	dbInfoGpRelationNode->logicalEof = 0;	// This will obtained from the other sources later (e.g. aoseg ).

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "DatabaseInfo_AddGpRelationNode: gp_relation_node TID %s, relfilenode %u, segment file #%d, persistent serial number " INT64_FORMAT ", persistent TID %s",
			 ItemPointerToString(gpRelationNodeTid),
			 relfilenode,
			 segmentFileNum,
			 persistentSerialNum,
			 ItemPointerToString(persistentTid));
	
	return found;
}

static void DatabaseInfo_AddMiscEntry(
	DatabaseInfo		 	*info,
	Oid 					tablespace,
	bool					isDir,
	char					*name)
{
	MiscEntry 	*miscEntry;

	if (info->miscEntriesCount >= info->miscEntriesMaxCount)
	{
		DatabaseInfo_Grow(
						(void**)&info->miscEntries,
						info->miscEntriesCount,
						&info->miscEntriesMaxCount,
						sizeof(MiscEntry));
	}

	miscEntry = &info->miscEntries[info->miscEntriesCount];
	info->miscEntriesCount++;
	
	miscEntry->tablespace = tablespace;
	miscEntry->isDir = isDir;
	miscEntry->name = pstrdup(name);

}

static void DatabaseInfo_AddPhysicalSegmentFile(
	DbInfoRel 				*dbInfoRel,
	int32 					segmentFileNum,
	int64					eof)
{
	DbInfoSegmentFile 	*dbInfoSegmentFile;

	if (dbInfoRel->physicalSegmentFilesCount >= dbInfoRel->physicalSegmentFilesMaxCount)
	{
		DatabaseInfo_Grow(
						(void**)&dbInfoRel->physicalSegmentFiles,
						dbInfoRel->physicalSegmentFilesCount,
						&dbInfoRel->physicalSegmentFilesMaxCount,
						sizeof(DbInfoSegmentFile));
	}

	dbInfoSegmentFile = &dbInfoRel->physicalSegmentFiles[dbInfoRel->physicalSegmentFilesCount];
	dbInfoRel->physicalSegmentFilesCount++;
	
	dbInfoSegmentFile->segmentFileNum = segmentFileNum;
	dbInfoSegmentFile->eof = eof;

}

static void DatabaseInfo_AddRelSegFile(
	DatabaseInfo 			*info,
	HTAB 					*dbInfoRelHashTable,
	Oid 					tablespace,
	Oid						relfilenode,
	int32					segmentFileNum,
	int64					eof)
{
	DbInfoRel	*dbInfoRel;
	bool		 found;

	/* Lookup the relfilenode in our catalog cache */
	dbInfoRel = (DbInfoRel*) \
		hash_search(dbInfoRelHashTable, 
					(void *) &relfilenode,
					HASH_FIND,
					&found);

	/* 
	 * If the relfilenode doesn't exist in the catalog then add it to the list
	 * of orphaned relfilenodes.
	 */
	if (!found || dbInfoRel->reltablespace != tablespace)
	{
		DatabaseInfo_AddExtraSegmentFile(
									info,
									tablespace,
									relfilenode,
									segmentFileNum,
									eof);
		return;
	}

	DatabaseInfo_AddPhysicalSegmentFile(
										dbInfoRel,
										segmentFileNum,
										eof);
}

static void DatabaseInfo_AddFile(
	DatabaseInfo		 	*info,
	HTAB 					*dbInfoRelHashTable,
	Oid 					tablespace,
	char					*dbDirPath,
	char					*name)
{
	int64 eof;
	int itemCount;
	Oid relfilenode;
	uint32 segmentFileNum;
	char path[MAXPGPATH];
	int	fileFlags = O_RDONLY | PG_BINARY;
	int	fileMode = 0400;
						/* File mode is S_IRUSR 00400 user has read permission */
	File file;

	sprintf(path, "%s/%s", dbDirPath, name);
	
	/*
	 * Open the file for read.
	 */	
	file = PathNameOpenFile(path, fileFlags, fileMode);
	if(file < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("Could not open segment file '%s'", 
						path)));
	}
	eof = FileSeek(file, 0L, SEEK_END);
	if (eof < 0) {
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("Could not seek to end of file \"%s\" : %m",
						path)));
	}
	FileClose(file);

	itemCount = sscanf(name, "%u.%u", &relfilenode, &segmentFileNum);

	// UNDONE: sscanf is a rather poor scanner.
	// UNDONE: For right now, just assume properly named files....
	if (itemCount == 0)
	{
		DatabaseInfo_AddMiscEntry(info, tablespace, false, name);
		return;
	}
	else if (itemCount == 1)
		segmentFileNum = 0;
	else
		Assert(itemCount == 2);

	DatabaseInfo_AddRelSegFile(info, dbInfoRelHashTable, tablespace, relfilenode, segmentFileNum, eof);
}

/*
 * DatabaseInfo_Scan()
 *   Scans the file-system to fill the DatabaseInfo with:
 *     - miscEntry             : non-relation database files
 *     - physicalSegmentFiles  : relation segment files
 */
static void 
DatabaseInfo_Scan(
	DatabaseInfo 		*info,
	HTAB 				*dbInfoRelHashTable,
	Oid 				 tablespace,
	Oid 				 database)
{
	char				*dbDirPath;
	DIR					*xldir;
	struct dirent		*xlde;
	char				 fromfile[MAXPGPATH];

	/* Lookup the database path and allocate a directory scan structure */
	dbDirPath = GetDatabasePath(
						(tablespace == GLOBALTABLESPACE_OID ? 0 : database), 
						tablespace);
	
	xldir = AllocateDir(dbDirPath);
	if (xldir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("Could not open database directory \"%s\": %m", dbDirPath)));

	/* Scan through the directory */
	while ((xlde = ReadDir(xldir, dbDirPath)) != NULL)
	{
		if (strcmp(xlde->d_name, ".") == 0 ||
			strcmp(xlde->d_name, "..") == 0)
			continue;

		/* Odd... On snow leopard, we get back "/" as a subdir, which is wrong. Ingore it */
		if (xlde->d_name[0] == '/' && xlde->d_name[1] == '\0')
			continue;

		snprintf(fromfile, MAXPGPATH, "%s/%s", dbDirPath, xlde->d_name);

		if(IsLocalPath(fromfile)){//on local disk
			struct stat fst;
			
			if (lstat(fromfile, &fst) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", fromfile)));

			if (S_ISDIR(fst.st_mode))
			{
				DatabaseInfo_AddMiscEntry(
										info, 
										tablespace,
										/* isDir */ true, 
										xlde->d_name);
			}
			else if (S_ISREG(fst.st_mode))
			{
				DatabaseInfo_AddFile(
									info,
									dbInfoRelHashTable,
									tablespace,
									dbDirPath,
									xlde->d_name);
			}
		}else{//on hdfs
			int ret = HdfsIsDirOrFile(fromfile);
			if(ret == -1){//fetch info error
				ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("error to fetch info for path \"%s\": %m", fromfile)));
			}else if(ret == 0){//direcotry
				DatabaseInfo_AddMiscEntry(
										info,
										tablespace,
										/* isDir */ true,
										xlde->d_name);
			}else if(ret == 1){//file
				DatabaseInfo_AddFile(
									info,
									dbInfoRelHashTable,
									tablespace,
									dbDirPath,
									xlde->d_name);
			}
		}
	}

	FreeDir(xldir);
}

/*
 * A compare function for 2 RelationSegmentFile.
 */
static int
DbInfoRelPtrArray_Compare(const void *entry1, const void *entry2)
{
	const DbInfoRel *dbInfoRel1 = *((DbInfoRel**)entry1);
	const DbInfoRel *dbInfoRel2 = *((DbInfoRel**)entry2);

	if (dbInfoRel1->relfilenodeOid == dbInfoRel2->relfilenodeOid)
		return 0;
	else if (dbInfoRel1->relfilenodeOid > dbInfoRel2->relfilenodeOid)
		return 1;
	else
		return -1;
}

static int
DbInfoGpRelationNode_Compare(const void *entry1, const void *entry2)
{
	const DbInfoGpRelationNode *info1 = (DbInfoGpRelationNode*) entry1;
	const DbInfoGpRelationNode *info2 = (DbInfoGpRelationNode*) entry2;

	if (info1->relfilenodeOid == info2->relfilenodeOid)
	{
		if (info1->segmentFileNum == info2->segmentFileNum)
			return 0;
		else if (info1->segmentFileNum > info2->segmentFileNum)
			return 1;
		else
			return -1;
	}
	else if (info1->relfilenodeOid > info2->relfilenodeOid)
		return 1;
	else
		return -1;
}

static int
DbInfoSegmentFile_Compare(const void *entry1, const void *entry2)
{
	const DbInfoSegmentFile *info1 = (DbInfoSegmentFile *) entry1;
	const DbInfoSegmentFile *info2 = (DbInfoSegmentFile *) entry2;

	if (info1->segmentFileNum == info2->segmentFileNum)
		return 0;
	else if (info1->segmentFileNum > info2->segmentFileNum)
		return 1;
	else
		return -1;
}

static int
DbInfoAppendOnlyCatalogSegmentInfo_Compare(const void *entry1, const void *entry2)
{
	const DbInfoAppendOnlyCatalogSegmentInfo *info1 = 
		(DbInfoAppendOnlyCatalogSegmentInfo *) entry1;
	const DbInfoAppendOnlyCatalogSegmentInfo *info2 = 
		(DbInfoAppendOnlyCatalogSegmentInfo *) entry2;

	if (info1->segmentFileNum == info2->segmentFileNum)
		return 0;
	else if (info1->segmentFileNum > info2->segmentFileNum)
		return 1;
	else
		return -1;
}

static void
DatabaseInfo_CollectGpRelationNode(
	DatabaseInfo 		*info,
	HTAB				*dbInfoRelHashTable)
{
	HeapScanDesc		scan;
	Relation			gp_relfile_node_rel;
	HeapTuple			tuple;

	gp_relfile_node_rel =
			DirectOpen_GpRelfileNodeOpen(
							info->defaultTablespace, 
							info->database);
	scan = heap_beginscan(gp_relfile_node_rel, SnapshotNow, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool			nulls[Natts_gp_relfile_node];
		Datum			values[Natts_gp_relfile_node];

		Oid				relfilenode;
		int32			segmentFileNum;
		ItemPointerData	persistentTid;
		int64			persistentSerialNum;
		
		heap_deform_tuple(tuple, RelationGetDescr(gp_relfile_node_rel), values, nulls);

		GpRelfileNode_GetValues(
							values,
							&relfilenode,
							&segmentFileNum,
							&persistentTid,
							&persistentSerialNum);
		
		if (!DatabaseInfo_AddGpRelationNode(
									info,
									dbInfoRelHashTable,
									relfilenode,
									segmentFileNum,
									&persistentTid,
									persistentSerialNum,
									&tuple->t_self))
		{
			elog(WARNING, "Did not find matching pg_class entry for gp_relation_node entry relfilenode %u (parentless!!!)",
				 relfilenode);
		}
	}
	heap_endscan(scan);

	DirectOpen_GpRelfileNodeClose(gp_relfile_node_rel);
}

static void
DatabaseInfo_HandleAppendOnly(
	DatabaseInfo		*info,
	HTAB				*dbInfoRelHashTable,
	HTAB				*relationIdHashTable,
	HTAB				*pgAppendOnlyHashTable)
{
	HASH_SEQ_STATUS iterateStatus;

	hash_seq_init(&iterateStatus, dbInfoRelHashTable);

	while (true)
	{
		DbInfoRel *dbInfoRel;

		dbInfoRel = 
				(DbInfoRel*)
						hash_seq_search(&iterateStatus);
		if (dbInfoRel == NULL)
			break;
	
		if (dbInfoRel->relstorage == RELSTORAGE_AOROWS)
		{
			AppendOnlyEntry 	*aoEntry;
			DbInfoRel 			*aosegDbInfoRel;
			int i;
				
			aoEntry = DatabaseInfo_FindPgAppendOnly(
											pgAppendOnlyHashTable,
											dbInfoRel->relationOid);
			if (Debug_persistent_print)
				elog(Persistent_DebugPrintLevel(), 
					 "DatabaseInfo_AddPgClassStoredRelation: Append-Only entry for relation id %u, relation name %s, "
				     "blocksize %d, pagesize %d, safefswritesize %d, compresslevel %d, major version %d, minor version %d, "
				     " checksum %s, compresstype %s, columnstore %s, segrelid %u, segidxid %u, blkdirrelid %u, blkdiridxid %u",
				     dbInfoRel->relationOid,
				     dbInfoRel->relname,
					 aoEntry->blocksize,
					 aoEntry->pagesize,
					 aoEntry->safefswritesize,
					 aoEntry->compresslevel,
					 aoEntry->majorversion,
					 aoEntry->minorversion,
					 (aoEntry->checksum ? "true" : "false"),
					 (aoEntry->compresstype ? aoEntry->compresstype : "NULL"),
					 (aoEntry->columnstore ? "true" : "false"),
					 aoEntry->segrelid,
					 aoEntry->segidxid,
					 aoEntry->blkdirrelid,
					 aoEntry->blkdiridxid);

			/*
			 * Translate the ao[cs]seg relation id to relfilenode.
			 */

			aosegDbInfoRel = DatabaseInfo_FindRelationId(
												relationIdHashTable,
												aoEntry->segrelid);
			Assert(aosegDbInfoRel != NULL);

			if (dbInfoRel->relstorage == RELSTORAGE_AOROWS)
			{
				FileSegInfo **aoSegfileArray;
				int totalAoSegFiles;

				Relation pg_aoseg_rel;

				pg_aoseg_rel = 
						DirectOpen_PgAoSegOpenDynamic(
											aoEntry->segrelid,
											MyDatabaseTableSpace,
											info->database,
											aosegDbInfoRel->relfilenodeOid);
				
				aoSegfileArray = 
						GetAllFileSegInfo_pg_aoseg_rel(
												dbInfoRel->relname, 
												aoEntry,
												pg_aoseg_rel,
												SnapshotNow, 
                                                -1,
												&totalAoSegFiles);
				for (i = 0; i < totalAoSegFiles; i++)
				{
					DatabaseInfo_AddAppendOnlyCatalogSegmentInfo(
															dbInfoRel,
															aoSegfileArray[i]->content,
															aoSegfileArray[i]->segno,
															aoSegfileArray[i]->eof);
				}

				DirectOpen_PgAoSegClose(pg_aoseg_rel);
			}			
		}
	}
}

static void
DatabaseInfo_CollectPgAppendOnly(
	DatabaseInfo 		*info,
	HTAB				*pgAppendOnlyHashTable)
{
	Relation	pg_appendonly_rel;

	HeapScanDesc scan;
	HeapTuple	tuple;

	pg_appendonly_rel = 
				DirectOpen_PgAppendOnlyOpen(
									info->defaultTablespace, 
									info->database);
	scan = heap_beginscan(pg_appendonly_rel, SnapshotNow, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool			nulls[Natts_pg_appendonly];
		Datum			values[Natts_pg_appendonly];
				
		AppendOnlyEntry *aoEntry;

		Oid 			relationId;
				
		heap_deform_tuple(tuple, RelationGetDescr(pg_appendonly_rel), values, nulls);

		aoEntry = GetAppendOnlyEntryFromTuple(
									pg_appendonly_rel,
									RelationGetDescr(pg_appendonly_rel),
									tuple,
									&relationId);

		Assert(aoEntry != NULL);

		if (Debug_persistent_print)
			elog(Persistent_DebugPrintLevel(), 
				 "DatabaseInfo_Collect: Append-Only entry for relation id %u, "
				 "blocksize %d, pagesize %d, safefswritesize %d, compresslevel %d, major version %d, minor version %d, "
				 " checksum %s, compresstype %s, columnstore %s, segrelid %u, segidxid %u, blkdirrelid %u, blkdiridxid %u",
				 relationId,
				 aoEntry->blocksize,
				 aoEntry->pagesize,
				 aoEntry->safefswritesize,
				 aoEntry->compresslevel,
				 aoEntry->majorversion,
				 aoEntry->minorversion,
				 (aoEntry->checksum ? "true" : "false"),
				 (aoEntry->compresstype ? aoEntry->compresstype : "NULL"),
				 (aoEntry->columnstore ? "true" : "false"),
				 aoEntry->segrelid,
				 aoEntry->segidxid,
				 aoEntry->blkdirrelid,
				 aoEntry->blkdiridxid);

		DatabaseInfo_AddPgAppendOnly(
								pgAppendOnlyHashTable,
								relationId,
								aoEntry);
	}
	heap_endscan(scan);

	DirectOpen_PgAppendOnlyClose(pg_appendonly_rel);
	
}

static void
DatabaseInfo_CollectPgClass(
	DatabaseInfo 		*info,
	HTAB				*dbInfoRelHashTable,
	HTAB				*relationIdHashTable,
	int					*count)
{
	Relation	pg_class_rel;

	HeapScanDesc scan;
	HeapTuple	tuple;

	/*
	 * Iterate through all the relations of the database and determine which
	 * database directories are active.  I.e. Fill up the tablespaces array.
	 */
	*count = 0;
	pg_class_rel = 
			DirectOpen_PgClassOpen(
							info->defaultTablespace, 
							info->database);
	scan = heap_beginscan(pg_class_rel, SnapshotNow, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Oid 			relationOid;

		Form_pg_class	form_pg_class;

		Oid 			reltablespace;

		char			relkind;
		char			relstorage;

		int 			relnatts;

		relationOid = HeapTupleGetOid(tuple);

		form_pg_class = (Form_pg_class) GETSTRUCT(tuple);

		reltablespace = form_pg_class->reltablespace;

		if (reltablespace == 0)
		{
			if (relstorage_is_ao(form_pg_class->relstorage))
				reltablespace = get_database_dts(info->database);
			else
				reltablespace = info->defaultTablespace;
		}

		/*
		 *	Skip non-storage relations.
		 */
		relkind = form_pg_class->relkind;

		if (relkind == RELKIND_VIEW ||
			relkind == RELKIND_COMPOSITE_TYPE)
			continue;

		relstorage = form_pg_class->relstorage;

		if (relstorage == RELSTORAGE_EXTERNAL)
			continue;

		relnatts = form_pg_class->relnatts;

		DatabaseInfo_AddTablespace(
									info, 
									reltablespace);

		DatabaseInfo_AddPgClassStoredRelation(
											info,
											dbInfoRelHashTable,
											relationIdHashTable,
											form_pg_class->relfilenode,
											&tuple->t_self,
											relationOid,
											form_pg_class->relname.data,
											reltablespace,
											relkind,
											relstorage,
											form_pg_class->relam,
											relnatts);

		(*count)++;
	}
	heap_endscan(scan);

	DirectOpen_PgClassClose(pg_class_rel);

}

/*
 * DatabaseInfo_SortRelArray()
 *   Builds the sorted RelArray structure based on a RelHash
 */
static void
DatabaseInfo_SortRelArray(
	DatabaseInfo		*info, 
	HTAB				*dbInfoRelHashTable,
	int					 count)
{
	HASH_SEQ_STATUS		  iterateStatus;
	DbInfoRel			**dbInfoRelPtrArray;
	int					  d;

	/* This function will populate the dbInfoRelArray */
	Assert(info->dbInfoRelArray == NULL);

	/* Construct an array of pointers by scanning through the hash table */
	dbInfoRelPtrArray = (DbInfoRel**) palloc(sizeof(DbInfoRel*) * count);
	hash_seq_init(&iterateStatus, dbInfoRelHashTable);
	for (d = 0; d < count; d++)
	{
		dbInfoRelPtrArray[d] = (DbInfoRel*) hash_seq_search(&iterateStatus);

		/* should have as many entries in the hash scan as "count" */
		if (dbInfoRelPtrArray[d] == NULL)
			elog(ERROR, "insufficient #/entries in dbInfoRelHashTable");
	}

	/* double check that the hash contained the right number of elements */
	if (hash_seq_search(&iterateStatus) != NULL)
		elog(ERROR, "too many entries in dbInfoRelHashTable");
	
	/* sort the pointer array */
	qsort(dbInfoRelPtrArray,
		  count, 
		  sizeof(DbInfoRel*),
		  DbInfoRelPtrArray_Compare);
		   
	/*
	 * Finally convert the sorted pointer array into a sorted record array.
	 */
	info->dbInfoRelArray = (DbInfoRel*) palloc(sizeof(DbInfoRel)*count);
	for (d = 0; d < count; d++)
	{
		info->dbInfoRelArray[d] = *(dbInfoRelPtrArray[d]);
		
		/*
		 * For each record in the array we have three lists:
		 *   - gpRelationNodes
		 *   - appendOnlyCatalogSegmentInfo
		 *   - physicalSegmentFiles 
		 *
		 * All three of which need to be sorted on segmentFileNum otherwise
		 * we will not be able to merge the lists correctly.
		 *
		 * XXX - this seems like a bad design, it seems like we have three
		 * sources of information on the same thing, which should be able
		 * to be satisfied with a single Hash rather than trying to keep 
		 * around three different lists and have code spread throughout the
		 * source trying to deal with merging the lists.
		 */
		if (info->dbInfoRelArray[d].gpRelationNodes)
			qsort(info->dbInfoRelArray[d].gpRelationNodes,
				  info->dbInfoRelArray[d].gpRelationNodesCount,
				  sizeof(DbInfoGpRelationNode),
				  DbInfoGpRelationNode_Compare);
		if (info->dbInfoRelArray[d].appendOnlyCatalogSegmentInfo)
			qsort(info->dbInfoRelArray[d].appendOnlyCatalogSegmentInfo,
				  info->dbInfoRelArray[d].appendOnlyCatalogSegmentInfoCount,
				  sizeof(DbInfoAppendOnlyCatalogSegmentInfo),
				  DbInfoAppendOnlyCatalogSegmentInfo_Compare);
		if (info->dbInfoRelArray[d].physicalSegmentFiles)
			qsort(info->dbInfoRelArray[d].physicalSegmentFiles,
				  info->dbInfoRelArray[d].physicalSegmentFilesCount,
				  sizeof(DbInfoSegmentFile),
				  DbInfoSegmentFile_Compare);
	}
	info->dbInfoRelArrayCount = count;

	/* Release the temporary pointer array and return */
	pfree(dbInfoRelPtrArray);
	return;
}



/*-------------------------------------------------------------------------
 * Exported function definitions
 *------------------------------------------------------------------------- */
DatabaseInfo *
DatabaseInfo_Collect(
	Oid			database,
	Oid 		defaultTablespace,
	bool        collectGpRelationNodeInfo,
	bool		collectAppendOnlyCatalogSegmentInfo,
	bool		scanFileSystem)
{
	DatabaseInfo		 *info;	
	HTAB				 *dbInfoRelHashTable;
	HTAB				 *relationIdHashTable;
	HTAB				 *pgAppendOnlyHashTable;
	int					  count;
	int					  t;

	/* Create local hash tables */
	dbInfoRelHashTable	  = DatabaseInfo_DbInfoRelHashTableInit();
	relationIdHashTable	  = DatabaseInfo_RelationIdHashTableInit();
	pgAppendOnlyHashTable = DatabaseInfo_PgAppendOnlyHashTableInit();

	/* Setup an initial empty DatabaseInfo */
	info = (DatabaseInfo*)palloc0(sizeof(DatabaseInfo));
	info->database							  = database;
	info->defaultTablespace					  = defaultTablespace;
	info->collectGpRelationNodeInfo           = collectGpRelationNodeInfo;
	info->collectAppendOnlyCatalogSegmentInfo = collectAppendOnlyCatalogSegmentInfo;

	/* 
	 * Allocate the extensible arrays:
	 *   - tablespaces
	 *   - miscEntries
	 *   - extraSegmentFiles
	 *   - parentlessGpRelationNodes
	 */
	info->tablespacesMaxCount = 10;
	info->tablespaces		  = palloc0(info->tablespacesMaxCount*sizeof(Oid));

	info->miscEntriesMaxCount = 50;
	info->miscEntries		  = palloc0(info->miscEntriesMaxCount*sizeof(MiscEntry));

	info->extraSegmentFilesMaxCount = 10;
	info->extraSegmentFiles	  = \
		palloc0(info->extraSegmentFilesMaxCount*sizeof(DbInfoExtraSegmentFile));

	info->parentlessGpRelationNodesMaxCount = 10;
	info->parentlessGpRelationNodes =  \
		palloc0(info->parentlessGpRelationNodesMaxCount*sizeof(DbInfoGpRelationNode));

	/* 
	 * During the init process, the tempalte0 and postgres are a little special,
	 * they exist only on the local. But the others will be on hdfs and local!.
	 * XXX:mat3: I'll change this later.
	 */
	if (defaultTablespace != DEFAULTTABLESPACE_OID)
		DatabaseInfo_AddTablespace(info, DEFAULTTABLESPACE_OID);

	/* 
	 * Start Collecting information: 
	 *   - from pg_class
	 *   - from pg_appendonly [if specified]
	 *   - from gp_relation_node [if specified]
	 *   - from file system
	 */
	DatabaseInfo_CollectPgClass(info, dbInfoRelHashTable, relationIdHashTable, &count);
	DatabaseInfo_CollectPgAppendOnly(info, pgAppendOnlyHashTable);

	if (info->collectAppendOnlyCatalogSegmentInfo)
	{
		/*
		 * We need the dbInfoRel hash table to translate pg_appendonly.segrelid
		 * to the ao[cs]seg relfilenode.
		 */
		DatabaseInfo_HandleAppendOnly(info, 
									  dbInfoRelHashTable, 
									  relationIdHashTable, 
									  pgAppendOnlyHashTable);
	}

	/*
	 * Note: this information has not yet been populated when this function is
	 * called during bootstrap or as part of upgrade.  In this case we will be
	 * using the results of this function in order to build the gp_relation
	 * table.
	 */
	if (info->collectGpRelationNodeInfo)
	{
		DatabaseInfo_CollectGpRelationNode(info, dbInfoRelHashTable);
	}

	/*
	 * Scan each used directory for its relation segment files and misc
	 * files/dirs as found within the filesystem.  This /may/ contain some files
	 * not referenced in gp_relation_node that are from crashed backends, but
	 * in general should agree with the set of entries in gp_relation_node.
	 *
	 * Files not present in gp_relation_node will not be mirrored and probably
	 * require removal to maintain database/filesystem consistency.
	 */
	if (scanFileSystem)
	{
		for (t = 0; t < info->tablespacesCount; t++)
		{
			DatabaseInfo_Scan(info, dbInfoRelHashTable, info->tablespaces[t], database);
		}
	}

	/* Convert the dbInfoRelHash into array and sort it. */
	DatabaseInfo_SortRelArray(info, dbInfoRelHashTable, count);
	
	/* Cleanup memory */
	hash_destroy(dbInfoRelHashTable);
	hash_destroy(relationIdHashTable);
	hash_destroy(pgAppendOnlyHashTable);

	/* Return the built DatabaseInfo */
	return info;
}

void 
DatabaseInfo_AlignAppendOnly(
	DatabaseInfo 			*info,

	DbInfoRel 				*dbInfoRel)
{
	int a;
	int g;

	/*
	 * Process the ao[cs]seg entries against the gp_relation_node entries.
	 */
	g = 0;
	for (a = 0; a < dbInfoRel->appendOnlyCatalogSegmentInfoCount; a++)
	{
		DbInfoAppendOnlyCatalogSegmentInfo	*appendOnlyCatalogSegmentInfo =
									&dbInfoRel->appendOnlyCatalogSegmentInfo[a];

		while (true)
		{
			if (g >= dbInfoRel->gpRelationNodesCount)
			{
				if (appendOnlyCatalogSegmentInfo->logicalEof > 0)
					elog(ERROR, "Append-Only relation '%s' segment file #%d has data (logical EOF " INT64_FORMAT ") in the aoseg entry but no gp_relation_node entry!",
						 dbInfoRel->relname,
						 appendOnlyCatalogSegmentInfo->segmentFileNum,
						 appendOnlyCatalogSegmentInfo->logicalEof);

				// Otherwise, ignore ao[cs]seg entries with EOF == 0 and no gp_relation_node entry.
				break;
			}

			if (dbInfoRel->gpRelationNodes[g].segmentFileNum < appendOnlyCatalogSegmentInfo->segmentFileNum)
			{
				if (dbInfoRel->gpRelationNodes[g].segmentFileNum == 0)
				{
					/*
					 * Segment file #0 with always have a gp_relation_node entry, but often doesn't have an aoseg entry.
					 */
					g++;
					continue;
				}
				
				elog(ERROR, "Append-Only relation '%s' gp_relation_node entry for segment file #%d without an aoseg entry (case #1)",
					 dbInfoRel->relname,
					 dbInfoRel->gpRelationNodes[g].segmentFileNum);
			}
			else if (dbInfoRel->gpRelationNodes[g].segmentFileNum == appendOnlyCatalogSegmentInfo->segmentFileNum)
			{
				dbInfoRel->gpRelationNodes[g].logicalEof = appendOnlyCatalogSegmentInfo->logicalEof;
				g++;
				break;
			}
			else
			{
				Assert (dbInfoRel->gpRelationNodes[g].segmentFileNum > appendOnlyCatalogSegmentInfo->segmentFileNum);
				elog(ERROR, "Append-Only relation '%s' gp_relation_node entry for segment file #%d without an aoseg entry",
					 dbInfoRel->relname,
					 dbInfoRel->gpRelationNodes[g].segmentFileNum);
			}
			g++;	// Not reached.  Protect against overly smart compliers looking at exit conditions...
		}
	}

	/*
	 * Drain remaining gp_relation_node entries...
	 */
	while (true)
	{
		if (g >= dbInfoRel->gpRelationNodesCount)
			break;

		if (dbInfoRel->gpRelationNodes[g].segmentFileNum > 0)
			elog(ERROR, "Append-Only relation '%s' gp_relation_node entry for segment file #%d without an aoseg entry (case #2)",
				 dbInfoRel->relname,
				 dbInfoRel->gpRelationNodes[g].segmentFileNum);

		g++;
	}
}
