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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * smgr.h
 *	  storage manager switch public interface declarations.
 *
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/storage/smgr.h,v 1.55.2.1 2007/01/27 20:15:47 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef SMGR_H
#define SMGR_H

#include "access/xlog.h"
#include "fmgr.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/dbdirnode.h"
#include "access/persistentendxactrec.h"

/*
 * smgr.c maintains a table of SMgrRelation objects, which are essentially
 * cached file handles.  An SMgrRelation is created (if not already present)
 * by smgropen(), and destroyed by smgrclose().  Note that neither of these
 * operations imply I/O, they just create or destroy a hashtable entry.
 * (But smgrclose() may release associated resources, such as OS-level file
 * descriptors.)
 *
 * An SMgrRelation may have an "owner", which is just a pointer to it from
 * somewhere else; smgr.c will clear this pointer if the SMgrRelation is
 * closed.	We use this to avoid dangling pointers from relcache to smgr
 * without having to make the smgr explicitly aware of relcache.  There
 * can't be more than one "owner" pointer per SMgrRelation, but that's
 * all we need.
 */
typedef struct SMgrRelationData
{
	/* rnode is the hashtable lookup key, so it must be first! */
	RelFileNode smgr_rnode;		/* relation physical identifier */

	/* pointer to owning pointer, or NULL if none */
	struct SMgrRelationData **smgr_owner;

	/* additional public fields may someday exist here */

	/*
	 * Fields below here are intended to be private to smgr.c and its
	 * submodules.	Do not touch them from elsewhere.
	 */
	int			smgr_which;		/* storage manager selector */

	struct _MdMirVec *md_mirvec;		/* for md.c; NULL if not open */
} SMgrRelationData;

typedef SMgrRelationData *SMgrRelation;

extern void smgrinit(void);
extern SMgrRelation smgropen(RelFileNode rnode);
extern void smgrsetowner(SMgrRelation *owner, SMgrRelation reln);
extern void smgrclose(SMgrRelation reln);
extern void smgrcloseall(void);
extern void smgrclosenode(RelFileNode rnode);
extern void smgrcreatefilespacedirpending(
	Oid 							filespaceOid,
	char							*filespaceLocation,
	ItemPointer 					persistentTid,
	int64							*persistentSerialNum,
	bool							flushToXLog);
extern void smgrcreatefilespacedir(
	char						*filespaceLocation,
	int 						*ioError);
extern void smgrcreatetablespacedirpending(
	TablespaceDirNode				*tablespaceDirNode,
	ItemPointer 					persistentTid,
	int64							*persistentSerialNum,
	bool							sharedStorage,

	bool							flushToXLog);
extern void smgrcreatetablespacedir(
	Oid 						tablespaceOid,
	int 						*ioError);
extern void smgrcreatedbdirjustintime(
	DbDirNode					*justInTimeDbDirNode,
	ItemPointer 				persistentTid,
	int64						*persistentSerialNum,
	int 						*primaryError);
extern void smgrcreatedbdirpending(
	DbDirNode						*dbDirNode,
	ItemPointer 					persistentTid,
	int64							*persistentSerialNum,
	bool							flushToXLog);
extern void smgrcreatedbdir(
	DbDirNode					*dbDirNode,
	bool						ignoreAlreadyExists,
	int 						*primaryError);
extern void smgrcreaterelationdirpending(
	RelFileNode *relFileNode,
	ItemPointer persistentTid,
	int64 *persistentSerialNum,
	bool flushToXLog);
extern void smgrcreaterelationdir(
	RelFileNode *relFileNode,
	int *primaryError);

extern void smgrcreatepending(
	RelFileNode 					*relFileNode,

	int32							segmentFileNum,

	PersistentFileSysRelStorageMgr relStorageMgr,

	PersistentFileSysRelBufpoolKind relBufpoolKind,

	char							*relationName,

	ItemPointer 					persistentTid,

	int64							*persistentSerialNum,

	bool							isLocalBuf,

	bool							bufferPoolBulkLoad,

	bool							flushToXLog);
extern void smgrcreate(
	SMgrRelation 				reln,

	bool						isLocalBuf, 

	char						*relationName,
	bool						ignoreAlreadyExists,

	int 						*primaryError);
extern void smgrrecreate(
	RelFileNode 				*relFileNode,

	bool						isLocalBuf, 

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */

	int 						*primaryError);
extern void smgrscheduleunlink(
	RelFileNode 	*relFileNode,

	int32			segmentFileNum,

	PersistentFileSysRelStorageMgr relStorageMgr,

	bool			isLocalBuf,

	char			*relationName,

	ItemPointer 	persistentTid,

	int64			persistentSerialNum);
extern void smgrdounlink(
	RelFileNode 				*relFileNode,

	bool 						isLocalBuf, 

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	
	bool						isRedo,

	bool 						ignoreNonExistence);
extern void smgrschedulermfilespacedir(
	Oid 				filespaceOid,
	ItemPointer 		persistentTid,
	int64				persistentSerialNum,
	bool				sharedStorage);
extern void smgrschedulermtablespacedir(
	Oid 				tablespaceOid,
	ItemPointer 		persistentTid,
	int64				persistentSerialNum,
	bool				sharedStorage);
extern void smgrschedulermdbdir(
	DbDirNode			*dbDirNode,

	ItemPointer		 	persistentTid,

	int64				persistentSerialNum,
	bool				sharedStorage);
extern void smgrschedulermrelationdir(
	RelFileNode *relFileNode,
	ItemPointer persistentTid,
	int64 persistentSerialNum,
	bool sharedStorage);


extern void smgrdormfilespacedir(
	Oid 						filespaceOid,
	char						*filespaceLocation,
	bool 						ignoreNonExistence);
extern void smgrdormtablespacedir(
	Oid 						tablespaceOid,
	
	bool 						ignoreNonExistence);
extern void smgrdormdbdir(
	DbDirNode 					*dropDbDirNode,
	
	bool 						ignoreNonExistence);
extern void smgrdormrelationdir(
	RelFileNode *relFileNode,

	bool ignoreNonExistence);

extern void smgrextend(SMgrRelation reln, BlockNumber blocknum, char *buffer,
		   bool isTemp);
extern void smgrread(SMgrRelation reln, BlockNumber blocknum, char *buffer);
extern void smgrwrite(SMgrRelation reln, BlockNumber blocknum, char *buffer,
		  bool isTemp);
extern BlockNumber smgrnblocks(SMgrRelation reln);
extern BlockNumber smgrtruncate(SMgrRelation reln, BlockNumber nblocks,
			 bool isTemp, bool isLocalBuf, ItemPointer persistentTid, int64 persistentSerialNum);
extern bool smgrgetpersistentinfo(	
	XLogRecord		*record,

	RelFileNode	*relFileNode,

	ItemPointer	persistentTid,

	int64		*persistentSerialNum);
extern void smgrimmedsync(SMgrRelation reln);
extern int	smgrGetPendingFileSysWork(
	EndXactRecKind						endXactRecKind,

	PersistentEndXactFileSysActionInfo	**ptr);
extern bool	smgrIsPendingFileSysWork(
	EndXactRecKind						endXactRecKind);
extern void AtSubCommit_smgr(void);
extern void AtSubAbort_smgr(void);
extern void AtEOXact_smgr(bool forCommit);
extern void PostPrepare_smgr(void);
extern void smgrcommit(void);
extern void smgrabort(void);
extern void smgrsync(void);

extern void smgr_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *record);
extern void smgr_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record);


/* internals: move me elsewhere -- ay 7/94 */

/* in md.c */
extern bool mdinit(void);
extern bool mdclose(SMgrRelation reln);
extern void mdcreatefilespacedir(
	char						*filespaceLocation,
	int 						*ioError);
extern void mdcreatetablespacedir(
	Oid 						tablespaceOid,
	int 						*ioError);
extern void mdcreatedbdir(
	DbDirNode					*dbDirNode,
	bool						ignoreAlreadyExists,
	int 						*ioError);
extern void mdcreaterelationdir(
	RelFileNode *relFileNode,
	int *ioError);

extern void mdcreate(
	SMgrRelation 				reln,

	bool 						isLocalBuf, 

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */

	bool						ignoreAlreadyExists,

	int 						*primaryError);
extern bool mdunlink(
	RelFileNode 				rnode, 

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */

	bool						isRedo,

	bool 						ignoreNonExistence);
extern bool mdrmfilespacedir(
	Oid 						filespaceOid,
	char						*filespaceLocation,
	bool 						ignoreNonExistence);
extern bool mdrmtablespacedir(
	Oid 						tablespaceOid,
	bool 						ignoreNonExistence);
extern bool mdrmdbdir(
	DbDirNode					*dbDirNode,
	bool 						ignoreNonExistence);
extern bool mdrmrelationdir(
	RelFileNode *relFileNode,
	bool ignoreNonExistence);

extern bool mdextend(SMgrRelation reln, BlockNumber blocknum, char *buffer,
		 bool isTemp);
extern bool mdread(SMgrRelation reln, BlockNumber blocknum, char *buffer);
extern bool mdwrite(SMgrRelation reln, BlockNumber blocknum, char *buffer,
		bool isTemp);
extern BlockNumber mdnblocks(SMgrRelation reln);
extern BlockNumber mdtruncate(SMgrRelation reln, BlockNumber nblocks,
		   bool isTemp);
extern bool mdimmedsync(SMgrRelation reln);
extern bool mdsync(void);

/*
 * MPP-18228 - to make addition to pending delete list atomic with adding
 * a 'Create Pending' entry in persistent tables.
 * Wrapper around the static PendingDelete_AddCreatePendingEntry() for relations
 */
extern void PendingDelete_AddCreatePendingRelationEntry(
		PersistentFileSysObjName *fsObjName,

		ItemPointer 			persistentTid,

		int64					*persistentSerialNum,

		PersistentFileSysRelStorageMgr relStorageMgr,

		char				*relationName,

		bool				isLocalBuf,

		bool				bufferPoolBulkLoad);

/*
 * MPP-18228 - Wrapper around PendingDelete_AddCreatePendingEntry() for
 * database, tablespace and filespace
 */
extern void PendingDelete_AddCreatePendingEntryWrapper(
	PersistentFileSysObjName *fsObjName,

	ItemPointer 			persistentTid,

	int64					persistentSerialNum);

extern void RememberFsyncRequest(RelFileNode rnode, BlockNumber segno);
extern void ForgetRelationFsyncRequests(RelFileNode rnode);
extern void ForgetDatabaseFsyncRequests(Oid tblspc, Oid dbid);

/* smgrtype.c */
extern Datum smgrout(PG_FUNCTION_ARGS);
extern Datum smgrin(PG_FUNCTION_ARGS);
extern Datum smgreq(PG_FUNCTION_ARGS);
extern Datum smgrne(PG_FUNCTION_ARGS);

#endif   /* SMGR_H */
