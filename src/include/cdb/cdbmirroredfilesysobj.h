/*-------------------------------------------------------------------------
 *
 * cdbmirroredfsobj.h
 *	  Create and drop mirrored files and directories.
 *
 * Copyright (c) 2009-2010, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBMIRROREDFSOBJ_H
#define CDBMIRROREDFSOBJ_H

#include "postgres.h"
#include "storage/relfilenode.h"
#include "storage/dbdirnode.h"
#include "storage/smgr.h"

extern void MirroredFileSysObj_TransactionCreateFilespaceDir(
	Oid			filespaceOid,
	char		*filespaceLocation,
	bool createDir);

extern void MirroredFileSysObj_ScheduleDropFilespaceDir(
	Oid			filespaceOid,
	bool		sharedStorage);

extern void MirroredFileSysObj_DropFilespaceDir(
	Oid							filespaceOid,
	char						*filespaceLocation,
	bool 						ignoreNonExistence);

extern void MirroredFileSysObj_TransactionCreateTablespaceDir(
	TablespaceDirNode	*tablespaceDirNode,

	ItemPointer 		persistentTid,
				/* Output: The TID of the gp_persistent_tablespace_node tuple. */

	int64				*persistentSerialNum);
				/* Output: The serial number of the gp_persistent_tablespace_node tuple. */

extern void MirroredFileSysObj_ScheduleDropTablespaceDir(
	Oid							tablespaceOid,
	bool						sharedStorage);

extern void MirroredFileSysObj_DropTablespaceDir(
	Oid							tablespaceOid,
	bool 						ignoreNonExistence);
	
extern void MirroredFileSysObj_TransactionCreateDbDir(
	DbDirNode			*dbDirNode,

	ItemPointer 		persistentTid,
				/* Output: The TID of the gp_persistent_database_node tuple. */

	int64				*persistentSerialNum);
				/* Output: The serial number of the gp_persistent_database_node tuple. */

extern void MirroredFileSysObj_ScheduleDropDbDir(
	DbDirNode		*dbDirNode,
	ItemPointer		 persistentTid,
	int64 			persistentSerialNum,
	bool			sharedStorage);

extern void MirroredFileSysObj_DropDbDir(
	DbDirNode					*dbDirNode,

	bool 						ignoreNonExistence);

extern void MirroredFileSysObj_TransactionCreateRelationDir(
	RelFileNode *relFileNode,
	bool doJustInTimeDirCreate,
	ItemPointer persistentTid,
	int64 *persistentSerialNum);

extern void MirroredFileSysObj_ScheduleDropRelationDir(
	RelFileNode *relFileNode,
	bool sharedStorage);

extern void MirroredFileSysObj_DropRelationDir(
	RelFileNode *relFileNode,
	bool ignoreNonExistence);

extern void MirroredFileSysObj_TransactionCreateBufferPoolFile(
	SMgrRelation 			smgrOpen,

	PersistentFileSysRelBufpoolKind relBufpoolKind,

	bool 					isLocalBuf,

	char					*relationName,

	bool					doJustInTimeDirCreate,

	bool					bufferPoolBulkLoad,

	ItemPointer 			persistentTid,

	int64					*persistentSerialNum);

extern void MirroredFileSysObj_TransactionCreateAppendOnlyFile(
	RelFileNode 			*relFileNode,

	int32					segmentFileNum,

	char					*relationName,

	bool					doJustInTimeDirCreate,

	ItemPointer 			persistentTid,

	int64					*persistentSerialNum);
extern void MirroredFileSysObj_ScheduleDropBufferPoolRel(
	Relation 				relation);
extern void MirroredFileSysObj_ScheduleDropBufferPoolFile(
	RelFileNode 				*relFileNode,

	bool 						isLocalBuf,

	char						*relationName,

	ItemPointer 				persistentTid,

	int64						persistentSerialNum);

extern void MirroredFileSysObj_ScheduleDropAppendOnlyFile(
	RelFileNode 				*relFileNode,

	int32						segmentFileNum,

	char						*relationName,

	ItemPointer 				persistentTid,

	int64						persistentSerialNum);

extern void MirroredFileSysObj_DropRelFile(
	RelFileNode 				*relFileNode,

	int32						segmentFileNum,

	PersistentFileSysRelStorageMgr relStorageMgr,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */

	bool 						isLocalBuf,
	
	bool 						ignoreNonExistence);

#endif   /* CDBMIRROREDFSOBJ_H */
