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
 * cdbmirroredfilesysobj.c
 *	  Create and drop mirrored files and directories.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#ifndef WIN32
#include <sys/fcntl.h>
#else
#include <io.h>
#endif 
#include <sys/file.h>
#include <unistd.h>

#include <signal.h>

#include "cdb/cdbmirroredfilesysobj.h"
#include "storage/smgr.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "catalog/gp_persistent.h"
#include "cdb/cdbpersistentdatabase.h"
#include "cdb/cdbpersistenttablespace.h"
#include "cdb/cdbpersistentfilespace.h"
#include "cdb/cdbpersistentrelation.h"
#include "cdb/cdbutil.h"
#include "storage/lwlock.h"
#include "miscadmin.h"
#include "postmaster/primary_mirror_mode.h"
#include "cdb/cdbfilerep.h"
#include "catalog/catalog.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "utils/guc.h"
#include "cdb/cdbmirroredappendonly.h"
#include "commands/filespace.h"
#include "commands/tablespace.h"

#include "cdb/cdbvars.h"

void MirroredFileSysObj_TransactionCreateFilespaceDir(
	Oid					filespaceOid,
	char				*filespaceLocation,
	bool createDir)
{
	PersistentFileSysObjName	fsObjName;
	ItemPointerData 			persistentTid;
	int64						persistentSerialNum;
	int							ioError;

	Assert(filespaceLocation != NULL);

	PersistentFileSysObjName_SetFilespaceDir(
										&fsObjName,
										filespaceOid,
										is_filespace_shared);

	LWLockAcquire(MirroredLock, LW_SHARED);

	/*
	 * We write our intention or 'Create Pending' persistent information before we do
	 * any create filespace work on either the primary or mirror.
	 */
	smgrcreatefilespacedirpending(
							filespaceOid, 
							filespaceLocation, 
							&persistentTid,
							&persistentSerialNum, 
							/* flushToXLog */ true);

	/*
	 * Synchronous primary and mirror create tablespace directory.
	 */
	if (createDir)
	{
    smgrcreatefilespacedir(filespaceLocation, &ioError);
    if (ioError != 0)
    {
      LWLockRelease(MirroredLock);
      // UNDONE: Need equiv. of GetDatabasePath here for filespace.
      ereport(ERROR,
          (errcode_for_file_access(),
           errmsg("could not create filespace directory %s: %s",
              filespaceLocation,
              strerror(ioError)),
              errdetail("%s", HdfsGetLastError())));
    }

    LWLockRelease(MirroredLock);
	}
}

void MirroredFileSysObj_ScheduleDropFilespaceDir(
	Oid			filespaceOid,
	bool		sharedStorage)
{
	ItemPointerData				persistentTid;
	int64						persistentSerialNum;

	PersistentFilespace_LookupTidAndSerialNum(
										filespaceOid,
										&persistentTid,
										&persistentSerialNum);

	smgrschedulermfilespacedir(
						filespaceOid,
						&persistentTid,
						persistentSerialNum,
						sharedStorage);
}

void MirroredFileSysObj_DropFilespaceDir(
	Oid							filespaceOid,
	char						*filespaceLocation,
	bool 						ignoreNonExistence)
{
	Assert(filespaceLocation != NULL);

	smgrdormfilespacedir(
				filespaceOid, 
				filespaceLocation,
				ignoreNonExistence);

}

void MirroredFileSysObj_TransactionCreateTablespaceDir(
	TablespaceDirNode	*tablespaceDirNode,
	ItemPointer 		persistentTid,
	int64				*persistentSerialNum)
{
	PersistentFileSysObjName	fsObjName;
	int ioError;

	PersistentFileSysObjName_SetTablespaceDir(
										&fsObjName,
										tablespaceDirNode->tablespace,
										NULL);
	fsObjName.sharedStorage = is_filespace_shared(tablespaceDirNode->filespace);

	LWLockAcquire(MirroredLock, LW_SHARED);

	/*
	 * We write our intention or 'Create Pending' persistent information before we do
	 * any create tablespace directory work on either the primary or mirror.
	 */
	smgrcreatetablespacedirpending(
							tablespaceDirNode, 
							persistentTid, 
							persistentSerialNum, 
							fsObjName.sharedStorage,
							/* flushToXLog */ true);

	/*
	 * Synchronous primary and mirror create tablespace directory.
	 */
	smgrcreatetablespacedir(
								tablespaceDirNode->tablespace,
								&ioError);
	if (ioError != 0)
	{
		LWLockRelease(MirroredLock);
		// UNDONE: Need equiv. of GetDatabasePath here for tablespace.
		ereport(ERROR,
			(errcode_for_file_access(),
			 errmsg("could not create tablespace directory %u: %s",
					tablespaceDirNode->tablespace,
					strerror(ioError))));
	}

	LWLockRelease(MirroredLock);
}

void MirroredFileSysObj_ScheduleDropTablespaceDir(
	Oid			tablespaceOid,
	bool		sharedStorage)
{
	ItemPointerData persistentTid;
	int64 persistentSerialNum;

	PersistentTablespace_LookupTidAndSerialNum(
										tablespaceOid,
										&persistentTid,
										&persistentSerialNum);

	smgrschedulermtablespacedir(
						tablespaceOid,
						&persistentTid,
						persistentSerialNum,
						sharedStorage);
}

void MirroredFileSysObj_DropTablespaceDir(
	Oid							tablespaceOid,
	bool 						ignoreNonExistence)
{
	smgrdormtablespacedir(
				tablespaceOid,
				ignoreNonExistence);
}

void MirroredFileSysObj_TransactionCreateDbDir(
	DbDirNode			*dbDirNode,

	ItemPointer 		persistentTid,
				/* Output: The TID of the gp_persistent_relation_node tuple. */

	int64				*persistentSerialNum)
				/* Output: The serial number of the gp_persistent_relation_node tuple. */

{
	PersistentFileSysObjName	fsObjName;
	int primaryError;

	PersistentFileSysObjName_SetDatabaseDir(
										&fsObjName,
										dbDirNode->tablespace,
										dbDirNode->database,
										is_tablespace_shared);

	LWLockAcquire(MirroredLock, LW_SHARED);

	/*
	 * We write our intention or 'Create Pending' persistent information before we do
	 * any create database directory work on either the primary or mirror.
	 */
	smgrcreatedbdirpending(
				dbDirNode,
				persistentTid, 
				persistentSerialNum, 
				/* flushToXLog */ true);

	/*
	 * Synchronous primary and mirror create database directory.
	 */
	smgrcreatedbdir(dbDirNode, false, &primaryError);
	if (primaryError != 0)
	{
		LWLockRelease(MirroredLock);
		ereport(ERROR,
			(errcode_for_file_access(),
			 errmsg("could not create database directory %s: %s",
					GetDatabasePath(dbDirNode->database, dbDirNode->tablespace),
					strerror(primaryError))));
	}

	LWLockRelease(MirroredLock);
}

void MirroredFileSysObj_ScheduleDropDbDir(
	DbDirNode			*dbDirNode,
	ItemPointer		 	persistentTid,
	int64 				persistentSerialNum,
	bool				sharedStorage)
{
	Assert(dbDirNode != NULL);

	smgrschedulermdbdir(
					dbDirNode,
					persistentTid,
					persistentSerialNum,
					sharedStorage);

}

void MirroredFileSysObj_DropDbDir(
	DbDirNode					*dbDirNode,
	bool 						ignoreNonExistence)
{
	Assert(dbDirNode != NULL);

	smgrdormdbdir(
			dbDirNode,
			ignoreNonExistence);
}

void MirroredFileSysObj_JustInTimeDbDirCreate(
	DbDirNode			*justInTimeDbDirNode)
{
	PersistentFileSysObjName	fsObjName;

	ItemPointerData 		persistentTid;
	int64					persistentSerialNum;

	int		ioError;

	Assert(justInTimeDbDirNode != NULL);

	if (justInTimeDbDirNode->tablespace == GLOBALTABLESPACE_OID ||
		(justInTimeDbDirNode->tablespace == DEFAULTTABLESPACE_OID &&
		 justInTimeDbDirNode->database == TemplateDbOid))
	{
		/*
		 * Optimize out the common cases.
		 */
		 return;
	}

	PersistentFileSysObjName_SetDatabaseDir(
										&fsObjName,
										justInTimeDbDirNode->tablespace,
										justInTimeDbDirNode->database,
										is_tablespace_shared);

	/*
	 * Acquire TablespaceCreateLock to ensure that no DROP TABLESPACE
	 * or MirroredFileSysObj_JustInTimeDbDirCreate is running concurrently.
	 */
	LWLockAcquire(TablespaceCreateLock, LW_EXCLUSIVE);

	/* Prevent cancel/die interrupt while doing this multi-step */
	HOLD_INTERRUPTS();

	if (PersistentDatabase_DirIsCreated(justInTimeDbDirNode))
	{
		RESUME_INTERRUPTS();
		LWLockRelease(TablespaceCreateLock);
		return;
	}

	LWLockAcquire(MirroredLock, LW_SHARED);

	if (PersistentTablespace_GetState(justInTimeDbDirNode->tablespace) == PersistentFileSysState_CreatePending)
	{
		/*
		 * This tablespace was created in our explicit (i.e. BEGIN ... END) transaction.
		 *
		 * Otherwise, if would not be visible to us.
		 *
		 * Do a regular transaction database directory create.
		 */
		 
		/*
		 * We write our intention or 'Create Pending' persistent information before we do
		 * any create database directory work on either the primary or mirror.
		 */
		smgrcreatedbdirpending(
						justInTimeDbDirNode,
						&persistentTid, 
						&persistentSerialNum, 
						/* flushToXLog */ true);
		
		/*
		 * Synchronous primary and mirror create database directory.
		 */
		smgrcreatedbdir(justInTimeDbDirNode, false, &ioError);
		if (ioError != 0)
		{
			RESUME_INTERRUPTS();
			LWLockRelease(MirroredLock);
			LWLockRelease(TablespaceCreateLock);
			ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create database directory %u/%u: %s",
						justInTimeDbDirNode->tablespace,
						justInTimeDbDirNode->database,
						strerror(ioError))));
		}
	}
	else
	{
		/*
		 * Do Just-In-Time non-transaction database directory create.
		 */
		smgrcreatedbdirjustintime(
							justInTimeDbDirNode,
							&persistentTid, 
							&persistentSerialNum, 
							&ioError);
		if (ioError != 0)
		{
			RESUME_INTERRUPTS();
			LWLockRelease(MirroredLock);
			LWLockRelease(TablespaceCreateLock);
			ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create just-in-time database directory %u/%u: %s",
						justInTimeDbDirNode->tablespace,
						justInTimeDbDirNode->database,
						strerror(ioError))));
		}
	}


	RESUME_INTERRUPTS();

	LWLockRelease(MirroredLock);
	LWLockRelease(TablespaceCreateLock);
}

void MirroredFileSysObj_TransactionCreateRelationDir(
	RelFileNode *relFileNode,
	bool doJustInTimeDirCreate,
	ItemPointer persistentTid,
	int64 *persistentSerialNum)
{
	PersistentFileSysObjName fsObjName;
	int primaryError;

	if (doJustInTimeDirCreate)
	{
		DbDirNode justInTimeDbDirNode;

		/*
		 * "Fault-in" the database directory in a tablespace if it doesn't exist yet.
		 */
		justInTimeDbDirNode.tablespace = relFileNode->spcNode;
		justInTimeDbDirNode.database = relFileNode->dbNode;
		MirroredFileSysObj_JustInTimeDbDirCreate(&justInTimeDbDirNode);
	}

	PersistentFileSysObjName_SetRelationDir(
			&fsObjName,
			relFileNode,
			is_tablespace_shared);

	LWLockAcquire(MirroredLock, LW_SHARED);

	/*
	 * We write our intention to 'Create Pending' persistent information
	 * before we do any create relation directory on either the primary
	 * or mirror.
	 */
	smgrcreaterelationdirpending(
			relFileNode,
			persistentTid,
			persistentSerialNum,
			/* flushToXLog */ true);

	/*
	 * Synchronous primary and mirror create relation directory.
	 */
	smgrcreaterelationdir(relFileNode, &primaryError);
	if (primaryError != 0)
	{
		LWLockRelease(MirroredLock);
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not create relation directory %s: %s",
						relpath(*relFileNode),
						strerror(primaryError))));
	}
	LWLockRelease(MirroredLock);
}

void MirroredFileSysObj_ScheduleDropRelationDir(
	RelFileNode *relFileNode,
	bool sharedStorage)
{
	ItemPointerData persistentTid;
	int64 persistentSerialNum;

	Assert(relFileNode != NULL);

	PersistentRelation_LookupTidAndSerialNum(relFileNode, &persistentTid, &persistentSerialNum);

	smgrschedulermrelationdir(
			relFileNode,
			&persistentTid,
			persistentSerialNum,
			sharedStorage);
}

void MirroredFileSysObj_DropRelationDir(
	RelFileNode *relFileNode,
	bool ignoreNonExistence)
{
	Assert(relFileNode != NULL);

	smgrdormrelationdir(
			relFileNode,
			ignoreNonExistence);

}

void MirroredFileSysObj_TransactionCreateBufferPoolFile(
	SMgrRelation 			smgrOpen,

	PersistentFileSysRelBufpoolKind relBufpoolKind,

	bool 					isLocalBuf,

	char					*relationName,

	bool					doJustInTimeDirCreate,

	bool					bufferPoolBulkLoad,

	ItemPointer 			persistentTid,

	int64					*persistentSerialNum)
{
	PersistentFileSysObjName	fsObjName;
	int primaryError;

	Assert(relationName != NULL);
	Assert(persistentTid != NULL);
	Assert(persistentSerialNum != NULL);

	if (doJustInTimeDirCreate)
	{
		DbDirNode justInTimeDbDirNode;
		
		/*
		 * "Fault-in" the database directory in a tablespace if it doesn't exist yet.
		 */
		justInTimeDbDirNode.tablespace = smgrOpen->smgr_rnode.spcNode;
		justInTimeDbDirNode.database = smgrOpen->smgr_rnode.dbNode;
		MirroredFileSysObj_JustInTimeDbDirCreate(&justInTimeDbDirNode);
	}

	PersistentFileSysObjName_SetRelationFile(
										&fsObjName,
										&smgrOpen->smgr_rnode,
										/* segmentFileNum */ 0,
										is_tablespace_shared);


	LWLockAcquire(MirroredLock, LW_SHARED);

	/*
	 * We write our intention or 'Create Pending' persistent information before we do
	 * any create relation work on either the primary or mirror.
	 */
	smgrcreatepending(
				&smgrOpen->smgr_rnode,
				/* segmentFileNum */ 0,
				PersistentFileSysRelStorageMgr_BufferPool,
				relBufpoolKind,
				relationName,
				persistentTid, 
				persistentSerialNum, 
				isLocalBuf,
				bufferPoolBulkLoad,
				/* flushToXLog */ true);

	/*
	 * Synchronous primary and mirror create relation.
	 */
	smgrcreate(
			smgrOpen,
			isLocalBuf,
			relationName,
			/* ignoreAlreadyExists */ false,
			&primaryError);
	if (primaryError != 0)
	{
		LWLockRelease(MirroredLock);
		ereport(ERROR,
			(errcode_for_file_access(),
			 errmsg("could not create relation file '%s', relation name '%s': %s",
					relpath(smgrOpen->smgr_rnode),
					relationName,
					strerror(primaryError))));
	}

	LWLockRelease(MirroredLock);

	if (Debug_persistent_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(Persistent_DebugPrintLevel(), 
			 "MirroredFileSysObj_TransactionCreateBufferPoolFile: %u/%u/%u, relation name '%s', bulk load %s"
		     ", persistent serial number " INT64_FORMAT " at TID %s",
			 smgrOpen->smgr_rnode.spcNode,
			 smgrOpen->smgr_rnode.dbNode,
			 smgrOpen->smgr_rnode.relNode,
			 (relationName == NULL ? "<null>" : relationName),
			 (bufferPoolBulkLoad ? "true" : "false"),
			 *persistentSerialNum,
			 ItemPointerToString(persistentTid));

		SUPPRESS_ERRCONTEXT_POP();
	}
}

/*
 * in hawq, we acturely do not do any mirror related work.
 */
void MirroredFileSysObj_TransactionCreateAppendOnlyFile(
	RelFileNode 			*relFileNode,

	int32					segmentFileNum,

	char					*relationName,

	bool					doJustInTimeDirCreate,

	ItemPointer 			persistentTid,
				/* Output: The TID of the gp_persistent_relation_node tuple. */

	int64					*persistentSerialNum)
				/* Output: The serial number of the gp_persistent_relation_node tuple. */
{
	int primaryError;

	Assert(relationName != NULL);
	Assert(persistentTid != NULL);
	Assert(persistentSerialNum != NULL);

	if (doJustInTimeDirCreate)
	{
		DbDirNode justInTimeDbDirNode;
		

		 /* "Fault-in" the database directory in a tablespace if it doesn't exist yet.*/

		justInTimeDbDirNode.tablespace = relFileNode->spcNode;
		justInTimeDbDirNode.database = relFileNode->dbNode;
		MirroredFileSysObj_JustInTimeDbDirCreate(&justInTimeDbDirNode);
	}

	/*
	 * We write our intention or 'Create Pending' persistent information before we do
	 * any create relation work on either the primary or mirror.
	 */
	smgrcreatepending(relFileNode,
					segmentFileNum,
					PersistentFileSysRelStorageMgr_AppendOnly,
					PersistentFileSysRelBufpoolKind_None,
					relationName,
					persistentTid,
					persistentSerialNum,
					/*isLocalBuf*/ false,
					/*bufferPoolBulkLoad*/ false,
					/*flushToXLog*/ true);

	/*
	 * Synchronous primary and mirror create relation.
	 */
	MirroredAppendOnly_Create(
							relFileNode,
							segmentFileNum,
							relationName,
							&primaryError);
	if (primaryError != 0)
	{
		ereport(ERROR,
			(errcode_for_file_access(),
			 errmsg("could not create relation file '%s', relation name '%s': %s",
					relpath(*relFileNode),
					relationName,
					strerror(primaryError)),
			 errdetail("%s", HdfsGetLastError())));
	}
}

void MirroredFileSysObj_ScheduleDropBufferPoolRel(
	Relation 				relation)
{
	/*
	 * use MASTER_CONTENT_ID for heap table and index even on segments.
	 */
	if (!relation->rd_relationnodeinfo.isPresent)
		RelationFetchGpRelationNode(relation);
	Assert(relation->rd_relationnodeinfo.isPresent);
	
	/* IMPORANT:
	 * ----> Relcache invalidation can close an open smgr <------
	 *
	 * So, DO NOT add other calls (such as RelationFetchSegFile0GpRelationNode above or 
	 * even elog which may invoke heap_open to gather its errcontext between here 
	 * and the smgrclose below.
	 *
	 * The DANGER is heap_open may process a relcache invalidation and close our
	 * relation's smgr open...
	 */
	RelationOpenSmgr(relation);
	Assert(relation->rd_smgr != NULL);
	smgrscheduleunlink(
					&relation->rd_node,
					/* segmentFileNum */ 0,
					PersistentFileSysRelStorageMgr_BufferPool,
					relation->rd_isLocalBuf,
					relation->rd_rel->relname.data,
					&relation->rd_relationnodeinfo.persistentTid,
					relation->rd_relationnodeinfo.persistentSerialNum);

	/* Now close the file and throw away the hashtable entry */
	smgrclose(relation->rd_smgr);
	Assert(relation->rd_smgr == NULL);	// Since RelationOpenSmgr linked the two together.

	/* IMPORANT:
	 * ----> Relcache invalidation can close an open smgr <------
	 *
	 * See above.
	 */


	/*
	 * Only do this elog tracing outside the smgr usage above.
	 */
	if (Debug_persistent_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(Persistent_DebugPrintLevel(), 
			 "MirroredFileSysObj_ScheduleDropBufferPoolRel: %u/%u/%u, relation name '%s'",
			 relation->rd_node.spcNode,
			 relation->rd_node.dbNode,
			 relation->rd_node.relNode,
			 relation->rd_rel->relname.data);

		SUPPRESS_ERRCONTEXT_POP();
	}

}

void MirroredFileSysObj_ScheduleDropBufferPoolFile(
	RelFileNode 				*relFileNode,

	bool 						isLocalBuf,

	char						*relationName,

	ItemPointer 				persistentTid,

	int64						persistentSerialNum)
{
	Assert(persistentTid != NULL);

	smgrscheduleunlink(
					relFileNode,
					/* segmentFileNum */ 0,
					PersistentFileSysRelStorageMgr_BufferPool,
					isLocalBuf,
					relationName,
					persistentTid, 
					persistentSerialNum);

	if (Debug_persistent_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(Persistent_DebugPrintLevel(), 
			 "MirroredFileSysObj_ScheduleDropBufferPoolFile: %u/%u/%u, relation name '%s'",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 relationName);

		SUPPRESS_ERRCONTEXT_POP();
	}

}

void MirroredFileSysObj_ScheduleDropAppendOnlyFile(
	RelFileNode 				*relFileNode,

	int32						segmentFileNum,

	char						*relationName,

	ItemPointer 				persistentTid,

	int64						persistentSerialNum)
{
	Assert(persistentTid != NULL);

	if (Debug_persistent_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(Persistent_DebugPrintLevel(), 
			 "MirroredFileSysObj_ScheduleDropAppendOnlyFile: %u/%u/%u, relation name '%s'",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 relationName);

		SUPPRESS_ERRCONTEXT_POP();
	}

	smgrscheduleunlink(
					relFileNode,
					segmentFileNum,
					PersistentFileSysRelStorageMgr_AppendOnly,
					/* isLocalBuf */ false,
					relationName,
					persistentTid, 
					persistentSerialNum);
}

void MirroredFileSysObj_DropRelFile(
	RelFileNode 				*relFileNode,

	int32						segmentFileNum,

	PersistentFileSysRelStorageMgr relStorageMgr,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */

	bool 						isLocalBuf,
	bool 						ignoreNonExistence)
{
	int primaryError;

	switch (relStorageMgr)
	{
	case PersistentFileSysRelStorageMgr_BufferPool:
		smgrdounlink(
				relFileNode, 
				isLocalBuf,
				relationName,
				/* isRedo */ false,
				ignoreNonExistence);
		break;

	case PersistentFileSysRelStorageMgr_AppendOnly:
		/* Shared storage never loss data! */
		MirroredAppendOnly_Drop(
					relFileNode,
					segmentFileNum,
					relationName,
					&primaryError);
		if (ignoreNonExistence && primaryError == ENOENT)
			primaryError = 0;

		// UNDONE: This needs to be an error.
		if (primaryError != 0)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove relation %u/%u/%u (segment file #%d): %s",
							relFileNode->spcNode,
							relFileNode->dbNode,
							relFileNode->relNode,
							segmentFileNum,
							strerror(primaryError))));
		break;

	default:
		elog(ERROR, "unexpected relation storage manager %d", relStorageMgr);
	}
}
