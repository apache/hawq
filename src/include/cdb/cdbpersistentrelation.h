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
 * cdbpersistentrelation.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBPERSISTENTRELATION_H
#define CDBPERSISTENTRELATION_H

#include "access/persistentfilesysobjname.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "storage/relfilenode.h"
#include "storage/itemptr.h"
#include "utils/palloc.h"

extern void PersistentRelation_CheckTablespace(
    Oid tablespace,
    int32 *useCount,
    RelFileNode *exampleRelationNode);

extern void PersistentRelation_Reset(void);

extern PersistentFileSysState PersistentRelation_GetState(RelFileNode *relFileNode);

extern void PersistentRelation_LookupTidAndSerialNum(
		RelFileNode *relFileNode,
		ItemPointer persistentTid,
		int64 *persistentSerialNum);

/*
 * Indicate we intend to create a relation file as part of the current transaction.
 *
 * An XLOG IntentToCreate record is generated that will guard the subsequent file-system
 * create in case the transaction aborts.
 *
 * After 1 or more calls to this routine to mark intention about relation files that are going
 * to be created, call ~_DoPendingCreates to do the actual file-system creates.  (See its
 * note on XLOG flushing).
 */
extern void PersistentRelation_MarkCreatePending(
		RelFileNode *relFileNode,
					/* The tablespace, database, and relation OIDs for the create. */
		ItemPointer persistentTid,
		int64 *persistentSerialNum,
		bool flushToXLog);

extern void PersistentRelation_AddCreated(
		RelFileNode *relFileNode,
					/* The tablespace, database, and relation OIDs for the create. */
		ItemPointer persistentTid,
		int64 *persistentSerialNum,
		bool flushToXLog);

// -----------------------------------------------------------------------------
// Transaction End
// -----------------------------------------------------------------------------

/*
 * Indicate the transaction commited and the relation is officially created.
 */
extern void PersistentRelation_Created(
		PersistentFileSysObjName *fsObjName,
					/* The tablespace, database, and relation OIDs for the create. */
		ItemPointer persistentTid,
		int64 persistentSerialNum,
		bool retryPossible);

/*
 * Indicate we intend to drop a relation file as part of the current transaction.
 *
 * This relation file to drop will be listed inside a commit, distributed commit, a distributed
 * prepared, and distributed commit prepared XOG records.
 *
 * For any of the commit type records, once that XLOG record is flushed then the actual
 * file-system delete will occur.  The flush guarantees the action will be retried after system
 * crash.
 */
extern PersistentFileSysObjStateChangeResult PersistentRelation_MarkDropPending(
		PersistentFileSysObjName *fsObjName,
		ItemPointer persistentTid,
		int64 persistentSerialNum,
		bool retryPossible);

/*
 * Indicate we are aborting the create of a relation file.
 *
 * This state will make sure the relation gets dropped after a system crash.
 */
extern PersistentFileSysObjStateChangeResult PersistentRelation_MarkAbortingCreate(
		PersistentFileSysObjName *fsObjName,
		ItemPointer persistentTid,
		int64 persistentSerialNum,
		bool retryPossible);

/*
 * Indicate we phsyicalled removed the relation file.
 */
extern void PersistentRelation_Dropped(
		PersistentFileSysObjName *fsObjName,
		ItemPointer persistentTid,
		int64 persistentSerialNum);

// -----------------------------------------------------------------------------
// Shmem and Startup/Shutdown
// -----------------------------------------------------------------------------

/*
 * Return the required shared-memory size for this module.
 */
extern Size PersistentRelation_ShmemSize(void);

/*
 * Initialize the shared-memory for this module.
 */
extern void PersistentRelation_ShmemInit(void);

#endif		/* CDBPERSISTENTRELATION_H */
