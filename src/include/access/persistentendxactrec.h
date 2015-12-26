/*-------------------------------------------------------------------------
 *
 * persistentendxactrec.h
 *	  Make and read persistent information in commit, abort, and prepared records.
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
#ifndef PERSISTENTENDXACTREC_H
#define PERSISTENTENDXACTREC_H

#include "postgres.h"
#include "storage/dbdirnode.h"
#include "storage/relfilenode.h"
#include "storage/itemptr.h"
#include "access/persistentfilesysobjname.h"

/*
 * The end of transaction possibilities.
 */
typedef enum EndXactRecKind
{
	EndXactRecKind_None = 0,
	EndXactRecKind_Commit = 1,
	EndXactRecKind_Abort = 2,
	EndXactRecKind_Prepare = 3,
	MaxEndXactRecKind /* must always be last */
} EndXactRecKind;

/*
 * The persistent transaction object possibilities.
 */
typedef enum PersistentEndXactObjKind
{
	PersistentEndXactObjKind_None = 0,
	PersistentEndXactObjKind_FileSysAction = 1,
	PersistentEndXactObjKind_AppendOnlyMirrorResyncEofs = 2,
	MaxPersistentEndXactObjKind /* must always be last */
} PersistentEndXactObjKind;

/*
 * The persistent transaction object possibilities.
 */
typedef enum PersistentEndXactFileSysAction
{
	PersistentEndXactFileSysAction_None = 0,
	PersistentEndXactFileSysAction_Create = 1,
	PersistentEndXactFileSysAction_Drop = 2,
	PersistentEndXactFileSysAction_AbortingCreateNeeded = 3,
	MaxPersistentEndXactFileSysAction /* must always be last */
} PersistentEndXactFileSysAction;

static inline bool PersistentEndXactFileSysAction_IsValid(
	PersistentEndXactFileSysAction action)
{
	return (action >= PersistentEndXactFileSysAction_Create &&
		    action <= PersistentEndXactFileSysAction_AbortingCreateNeeded);
}

/*
 * Structure containing the end transaction information for a relation file.
 */
typedef struct PersistentEndXactFileSysActionInfo
{
	PersistentEndXactFileSysAction	action;

	PersistentFileSysObjName 	fsObjName;

	PersistentFileSysRelStorageMgr relStorageMgr;

	ItemPointerData			persistentTid;

	int64					persistentSerialNum;

} PersistentEndXactFileSysActionInfo;

typedef struct PersistentEndXactAppendOnlyMirrorResyncEofs
{
	RelFileNode				relFileNode;

	int32					segmentFileNum;

	ItemPointerData			persistentTid;

	int64					persistentSerialNum;

	int64					mirrorLossEof;

	int64					mirrorNewEof;
} PersistentEndXactAppendOnlyMirrorResyncEofs;


typedef struct PeristentEndXaxtUntyped
{
	void 		*data;

	int32		len;

	int32		count;
} PeristentEndXaxtUntyped;

/*
 * Structure containing the objects read.
 */
typedef union PersistentEndXactRecObjects
{
	PeristentEndXaxtUntyped		untyped[MaxPersistentEndXactObjKind];

	struct typed
	{
		PersistentEndXactFileSysActionInfo 		*fileSysActionInfos;
		int32 									fileSysActionInfosLen;
		int32 									fileSysActionInfosCount;
		
		PersistentEndXactAppendOnlyMirrorResyncEofs 	*appendOnlyMirrorResyncEofs;
		int32 											appendOnlyMirrorResyncEofsLen;
		int32 											appendOnlyMirrorResyncEofsCount;
	}typed; 
	
} PersistentEndXactRecObjects;

char *PersistentXactObjKind_Name(
	PersistentEndXactObjKind objKind);

extern char *EndXactRecKind_Name(
	EndXactRecKind endXactRecKind);

extern bool EndXactRecKind_NeedsAction(
		EndXactRecKind					endXactRecKind,
	
		PersistentEndXactFileSysAction	action);

extern char *PersistentEndXactFileSysAction_Name(
	PersistentEndXactFileSysAction action);

extern void PersistentEndXactRec_Init(
	PersistentEndXactRecObjects	*objects);

extern void PersistentEndXactRec_AddFileSysActionInfos(
	PersistentEndXactRecObjects			*objects,

	EndXactRecKind						endXactRecKind,

	PersistentEndXactFileSysActionInfo	*fileSysActionInfos,

	int									count);

extern int32 PersistentEndXactRec_FetchObjectsFromSmgr(
	PersistentEndXactRecObjects	*objects,

	EndXactRecKind				endXactRecKind,

	int16						*objectCount);

extern bool PersistentEndXactRec_WillHaveObjectsFromSmgr(
		EndXactRecKind				endXactRecKind);

extern void PersistentEndXactRec_Print(
		char							*procName,
	
		PersistentEndXactRecObjects 	*objects);

extern int16 PersistentEndXactRec_ObjectCount(
	PersistentEndXactRecObjects	*objects,
	
	EndXactRecKind				endXactRecKind);

extern int32 PersistentEndXactRec_SerializeLen(
	PersistentEndXactRecObjects	*objects,
	
	EndXactRecKind				endXactRecKind);

extern void PersistentEndXactRec_Serialize(
		PersistentEndXactRecObjects *objects,
	
		EndXactRecKind				endXactRecKind,
	
		int16						*numOfObjects,
	
		uint8						*buffer,
	
		int32						bufferLen);

extern int32 PersistentEndXactRec_DeserializeLen(
		uint8						*buffer,
	
		int16						numOfObjects);

extern void PersistentEndXactRec_Deserialize(
	uint8						*buffer,

	int16						numOfObjects,

	PersistentEndXactRecObjects	*objects,

	uint8						**nextData);

#endif   /* PERSISTENTENDXACTREC_H */

