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
 * cdbpersistentfilesysobj.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBPERSISTENTFILESYSOBJ_H
#define CDBPERSISTENTFILESYSOBJ_H

#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/palloc.h"
#include "storage/fd.h"
#include "access/persistentfilesysobjname.h"
#include "access/persistentendxactrec.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbdoublylinked.h"
#include "storage/smgr.h"

typedef struct PersistentFileSysObjSharedData
{

	PersistentStoreSharedData	storeSharedData;

} PersistentFileSysObjSharedData;

#define PersistentFileSysObjData_StaticInit {0,PersistentStoreData_StaticInit,0,0}

typedef struct PersistentFileSysObjData
{
	PersistentFsObjType		fsObjType;

	PersistentStoreData		storeData;

	int 					attNumPersistentState;

	int 					attNumParentXid;

} PersistentFileSysObjData;

inline static int Persistent_DebugPrintLevel(void)
{
	if (Debug_persistent_bootstrap_print && IsBootstrapProcessingMode())
		return WARNING;
	else
		return Debug_persistent_print_level;
}

extern void PersistentFileSysObj_InitShared(
	PersistentFileSysObjSharedData 	*fileSysObjSharedData);

extern void PersistentFileSysObj_Init(
	PersistentFileSysObjData 			*fileSysObjData,

	PersistentFileSysObjSharedData 		*fileSysObjSharedData,

	PersistentFsObjType					fsObjType,

	PersistentStoreScanTupleCallback	scanTupleCallback);

extern void PersistentFileSysObj_Reset(void);

extern void PersistentFileSysObj_GetDataPtrs(
	PersistentFsObjType				fsObjType,
	
	PersistentFileSysObjData		**fileSysObjData,
	
	PersistentFileSysObjSharedData	**fileSysObjSharedData);

extern void PersistentFileSysObj_BuildInitScan(void);

extern void PersistentFileSysObj_StartupInitScan(void);

extern void PersistentFileSysObj_VerifyInitScan(void);

extern void PersistentFileSysObj_Scan(
	PersistentFsObjType			fsObjType,

	PersistentStoreScanTupleCallback	scanTupleCallback);

extern void PersistentFileSysObj_FlushXLog(void);

extern int64 PersistentFileSysObj_MyHighestSerialNum(
	PersistentFsObjType 	fsObjType);
extern int64 PersistentFileSysObj_CurrentMaxSerialNum(
	PersistentFsObjType 	fsObjType);

extern PersistentTidIsKnownResult PersistentFileSysObj_TidIsKnown(
	PersistentFsObjType 	fsObjType,

	ItemPointer 			persistentTid,
	
	ItemPointer 			maxTid);

extern void PersistentFileSysObj_UpdateTuple(
	PersistentFsObjType		fsObjType,

	ItemPointer 			persistentTid,
				/* TID of the stored tuple. */

	Datum 					*values,

	bool					flushToXLog);
				/* When true, the XLOG record for this change will be flushed to disk. */

extern void PersistentFileSysObj_ReplaceTuple(
	PersistentFsObjType 	fsObjType,

	ItemPointer 			persistentTid,
				/* TID of the stored tuple. */

	HeapTuple				tuple,

	Datum					*newValues,

	bool					*replaces,

	bool					flushToXLog);
				/* When true, the XLOG record for this change will be flushed to disk. */

extern void PersistentFileSysObj_ReadTuple(
	PersistentFsObjType 		fsObjType,
					
	ItemPointer					readTid,

	Datum						*values,
	
	HeapTuple					*tupleCopy);

extern void PersistentFileSysObj_AddTuple(
	PersistentFsObjType			fsObjType,

	Datum						*values,

	bool						flushToXLog,
				/* When true, the XLOG record for this change will be flushed to disk. */

	ItemPointer 				persistentTid,
				/* TID of the stored tuple. */

	int64						*persistentSerialNum);

extern void PersistentFileSysObj_FreeTuple(
	PersistentFileSysObjData		*fileSysObjData,

	PersistentFileSysObjSharedData	*fileSysObjSharedData,

	PersistentFsObjType 		fsObjType,

	ItemPointer 				persistentTid,
				/* TID of the stored tuple. */

	bool						flushToXLog);
				/* When true, the XLOG record for this change will be flushed to disk. */


/*
 * The file kinds of a persistent file-system object verification results.
 */
typedef enum PersistentFileSysObjVerifyExpectedResult
{
	PersistentFileSysObjVerifyExpectedResult_None = 0,
	PersistentFileSysObjVerifyExpectedResult_DeleteUnnecessary = 1,
	PersistentFileSysObjVerifyExpectedResult_StateChangeAlreadyDone = 2,
	PersistentFileSysObjVerifyExpectedResult_StateChangeNeeded = 3,
	PersistentFileSysObjVerifyExpectedResult_ErrorSuppressed = 4,
	MaxPersistentFileSysObjVerifyExpectedResult /* must always be last */
} PersistentFileSysObjVerifyExpectedResult;

typedef void (*PersistentFileSysObjVerifiedActionCallback) (
											PersistentFileSysObjName 	*fsObjName,
											ItemPointer 				persistentTid,
											int64						persistentSerialNum,
											PersistentFileSysObjVerifyExpectedResult verifyExpectedResult);

/*
 * The file kinds of a persistent file-system object state-change results.
 */
typedef enum PersistentFileSysObjStateChangeResult
{
	PersistentFileSysObjStateChangeResult_None = 0,
	PersistentFileSysObjStateChangeResult_DeleteUnnecessary = 1,
	PersistentFileSysObjStateChangeResult_StateChangeOk = 2,
	PersistentFileSysObjStateChangeResult_ErrorSuppressed = 3,
	MaxPersistentFileSysObjStateChangeResult /* must always be last */
} PersistentFileSysObjStateChangeResult;

extern char *PersistentFileSysObjStateChangeResult_Name(
	PersistentFileSysObjStateChangeResult result);

extern PersistentFileSysObjStateChangeResult PersistentFileSysObj_StateChange(
	PersistentFileSysObjName	*fsObjName,

	ItemPointer 				persistentTid,

	int64						persistentSerialNum,
	
	PersistentFileSysState		nextState,

	bool						retryPossible,

	bool						flushToXLog,

	PersistentFileSysState		*oldState,

	PersistentFileSysObjVerifiedActionCallback verifiedActionCallback);

extern void PersistentFileSysObj_RepairDelete(
		PersistentFsObjType 		fsObjType,
	
		ItemPointer 				persistentTid);

extern void PersistentFileSysObj_Created(
	PersistentFileSysObjName	*fsObjName,

	ItemPointer 			persistentTid,

	int64					persistentSerialNum,

	bool					retryPossible);

extern PersistentFileSysObjStateChangeResult PersistentFileSysObj_MarkAbortingCreate(
	PersistentFileSysObjName	*fsObjName,

	ItemPointer 			persistentTid,

	int64					persistentSerialNum,

	bool					retryPossible);

extern PersistentFileSysObjStateChangeResult PersistentFileSysObj_MarkDropPending(
	PersistentFileSysObjName	*fsObjName,

	ItemPointer 			persistentTid,

	int64					persistentSerialNum,

	bool					retryPossible);

extern void PersistentFileSysObj_DropObject(
	PersistentFileSysObjName	*fsObjName,

	PersistentFileSysRelStorageMgr relStorageMgr,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */

	ItemPointer 				persistentTid,

	int64						persistentSerialNum,

	bool 						ignoreNonExistence,

	bool						debugPrint,

	int							debugPrintLevel);

extern void PersistentFileSysObj_EndXactDrop(
	PersistentFileSysObjName	*fsObjName,

	PersistentFileSysRelStorageMgr relStorageMgr,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */

	ItemPointer 				persistentTid,

	int64						persistentSerialNum,

	bool						ignoreNonExistence);

extern void PersistentFileSysObj_PreparedEndXactAction(
	TransactionId 					preparedXid,

	const char 						*gid,

	PersistentEndXactRecObjects 	*persistentObjects,
	
	bool							isCommit,

	int								prepareAppendOnlyIntentCount);

extern void PersistentFileSysObj_UpdateRelationBufpoolKind(
	RelFileNode							*relFileNode,

	int32								segmentFileNum,

	ItemPointer 						persistentTid,

	int64								persistentSerialNum,

	PersistentFileSysRelBufpoolKind 	relBufpoolKind);

extern bool PersistentFileSysObj_ScanForRelation(
	RelFileNode 		*relFileNode,
				/* The tablespace, database, and relation OIDs for the create. */

	int32				segmentFileNum,

	ItemPointer 		persistentTid,
				/* Resulting TID of the gp_persistent_rel_files tuple for the relation. */

	int64				*persistentSerialNum);
				/* Resulting serial number for the relation.  Distinquishes the uses of the tuple. */

extern void PersistentFileSysObj_StartupIntegrityCheck(void);

extern Size PersistentFileSysObj_ShmemSize(void);

extern void PersistentFileSysObj_ShmemInit(void);

extern void PersistentFileSysObj_DoGlobalSequenceScan(void);
#endif   /* CDBPERSISTENTFILESYSOBJ_H */
