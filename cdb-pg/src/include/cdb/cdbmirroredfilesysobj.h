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

extern void MirroredFileSysObj_ValidateFilespaceDir(
	char *mirrorFilespaceLocation);

extern void MirroredFileSysObj_TransactionCreateFilespaceDir(
	Oid			filespaceOid,

	int16		primaryDbId,
	
	char		*primaryFilespaceLocation,
				/* 
				 * The primary filespace directory path.  NOT Blank padded.
				 * Just a NULL terminated string.
				 */
	
	int16		mirrorDbId,
	
	char		*mirrorFilespaceLocation,

	ItemPointer 		persistentTid,
				/* Output: The TID of the gp_persistent_filespace_node tuple. */

	int64				*persistentSerialNum);
				/* Output: The serial number of the gp_persistent_filespace_node tuple. */

extern void MirroredFileSysObj_CreateFilespaceDir(
	Oid 						filespaceOid,
	
	char						*primaryFilespaceLocation,
								/* 
								 * The primary filespace directory path.  NOT Blank padded.
								 * Just a NULL terminated string.
								 */
	
	char						*mirrorFilespaceLocation,
	
	StorageManagerMirrorMode	mirrorMode,
	
	bool						ignoreAlreadyExists,
	
	int 						*primaryError,
	
	bool 						*mirrorDataLossOccurred);

extern void MirroredFileSysObj_ScheduleDropFilespaceDir(
	int4		contentid,
	Oid			filespaceOid,
	bool		sharedStorage);

extern void MirroredFileSysObj_DropFilespaceDir(
	Oid							filespaceOid,

	char						*primaryFilespaceLocation,
								/* 
								 * The primary filespace directory path.  NOT Blank padded.
								 * Just a NULL terminated string.
								 */
	
	char						*mirrorFilespaceLocation,
	
	bool						primaryOnly,

	bool					 	mirrorOnly,

	bool 						ignoreNonExistence,

	bool						*mirrorDataLossOccurred);

extern void MirroredFileSysObj_TransactionCreateTablespaceDir(
	int4				contentid,
	TablespaceDirNode	*tablespaceDirNode,

	ItemPointer 		persistentTid,
				/* Output: The TID of the gp_persistent_tablespace_node tuple. */

	int64				*persistentSerialNum);
				/* Output: The serial number of the gp_persistent_tablespace_node tuple. */

extern void MirroredFileSysObj_CreateTablespaceDir(
	int4						contentid,
	Oid 						tablespaceOid,
	
	StorageManagerMirrorMode	mirrorMode,
	
	bool						ignoreAlreadyExists,
	
	int 						*primaryError,
	
	bool 						*mirrorDataLossOccurred);

extern void MirroredFileSysObj_ScheduleDropTablespaceDir(
	int4						contentid,
	Oid							tablespaceOid,
	bool						sharedStorage);

extern void MirroredFileSysObj_DropTablespaceDir(
	int4						contentid,
	Oid							tablespaceOid,
	
	bool						primaryOnly,

	bool					 	mirrorOnly,

	bool 						ignoreNonExistence,

	bool						*mirrorDataLossOccurred);
	
extern void MirroredFileSysObj_TransactionCreateDbDir(
	int4				contentid,
	DbDirNode			*dbDirNode,

	ItemPointer 		persistentTid,
				/* Output: The TID of the gp_persistent_database_node tuple. */

	int64				*persistentSerialNum);
				/* Output: The serial number of the gp_persistent_database_node tuple. */

extern void MirroredFileSysObj_CreateDbDir(
	int4						contentid,
	DbDirNode					*dbDirNode,
	
	StorageManagerMirrorMode	mirrorMode,
	
	bool						ignoreAlreadyExists,
	
	int 						*primaryError,
	
	bool 						*mirrorDataLossOccurred);

extern void MirroredFileSysObj_ScheduleDropDbDir(
	int4			contentid,
	DbDirNode		*dbDirNode,
	
	ItemPointer		 persistentTid,

	int64 			persistentSerialNum,
	bool			sharedStorage);

extern void MirroredFileSysObj_DropDbDir(
	int4						contentid,

	DbDirNode					*dbDirNode,
	
	bool						primaryOnly,

	bool					 	mirrorOnly,

	bool 						ignoreNonExistence,

	bool						*mirrorDataLossOccurred);

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

	int32					contentid,

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

	int32						contentid,

	char						*relationName,

	ItemPointer 				persistentTid,

	int64						persistentSerialNum);

extern void MirroredFileSysObj_DropRelFile(
	RelFileNode 				*relFileNode,

	int32						segmentFileNum,

	int32						contentid,

	PersistentFileSysRelStorageMgr relStorageMgr,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */

	bool 						isLocalBuf,
	
	bool  						primaryOnly,

	bool 						ignoreNonExistence,

	bool						*mirrorDataLossOccurred);

extern MirroredObjectExistenceState mirror_existence_state(void);

#endif   /* CDBMIRROREDFSOBJ_H */
