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
 * cdbmirroredfsobj.h
 *	  Create and drop mirrored files and directories.
 *
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
