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
 * cdbpersistentfilespace.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBPERSISTENTFILEESPACE_H
#define CDBPERSISTENTFILEESPACE_H

#include "access/persistentfilesysobjname.h"
#include "access/xlogmm.h"
#include "cdb/cdbsharedoidsearch.h"
#include "cdb/cdbdoublylinked.h"
#include "storage/fd.h"
#include "storage/itemptr.h"
#include "utils/palloc.h"
#include "cdb/cdbpersistentfilesysobj.h"

extern void PersistentFilespace_Reset(void);

extern void PersistentFilespace_ConvertBlankPaddedLocation(
	char 		**filespaceLocation,

	char 		*locationBlankPadded,

	bool		isPrimary);

extern void PersistentFilespace_LookupTidAndSerialNum(
	Oid 		filespaceOid,
	ItemPointer		persistentTid,
	int64			*persistentSerialNum);

extern bool PersistentFilespace_TryGetFilespacePathUnderLock(
	Oid 		filespaceOid,
	char **filespaceLocation);
				/* The primary filespace directory path.  Return NULL for global and base. 
				 * Or, returns NULL when mirror not configured. */

extern void PersistentFilespace_GetLocation(
	Oid 		filespaceOid,
	char **filespaceLocation);

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
extern void PersistentFilespace_MarkCreatePending(
	Oid 		filespaceOid,
	char		*filespaceLocation,
	ItemPointer		persistentTid,
	int64			*persistentSerialNum,			
	bool			flushToXLog);

// -----------------------------------------------------------------------------
// Rebuild filespace persistent table 'gp_persistent_filespace_node'
// -----------------------------------------------------------------------------
extern void PersistentFilespace_AddCreated(
        Oid 		filespaceOid,
        /* The filespace OID to be added. */

        bool			flushToXLog);
/* When true, the XLOG record for this change will be flushed to disk. */
// -----------------------------------------------------------------------------
// Transaction End	
// -----------------------------------------------------------------------------
				
/*
 * Indicate the transaction commited and the relation is officially created.
 */
extern void PersistentFilespace_Created(							
	PersistentFileSysObjName *fsObjName,
				/* The filespace OID for the create. */
	
	ItemPointer 	persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */
				
	int64			persistentSerialNum,
				/* Serial number for the relation.	Distinquishes the uses of the tuple. */

	bool			retryPossible);
					
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
extern PersistentFileSysObjStateChangeResult PersistentFilespace_MarkDropPending(
	PersistentFileSysObjName *fsObjName,
				/* The filespace OID for the drop. */
	
	ItemPointer 	persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */
							
	int64			persistentSerialNum,
				/* Serial number for the relation.	Distinquishes the uses of the tuple. */

	bool			retryPossible);

/*
 * Indicate we are aborting the create of a relation file.
 *
 * This state will make sure the relation gets dropped after a system crash.
 */
extern PersistentFileSysObjStateChangeResult PersistentFilespace_MarkAbortingCreate(
	PersistentFileSysObjName *fsObjName,
				/* The filespace OID for the aborting create. */
							
	ItemPointer 	persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */
							
	int64			persistentSerialNum,
				/* Serial number for the relation.	Distinquishes the uses of the tuple. */

	bool			retryPossible);
					
/*
 * Indicate we phsyicalled removed the relation file.
 */
extern void PersistentFilespace_Dropped(
	PersistentFileSysObjName *fsObjName,
				/* The filespace OID for the dropped filespace. */
										
	ItemPointer 	persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */
							
	int64			persistentSerialNum);
				/* Serial number for the relation.	Distinquishes the uses of the tuple. */
	

/* 
 * Identify the mirror dbid referenced in the gp_persistet_filespae_node table 
 */
extern int16 PersistentFilespace_LookupMirrorDbid(int16 primaryDbid);

// -----------------------------------------------------------------------------
// Shmem and Startup/Shutdown
// -----------------------------------------------------------------------------
				
/*
 * Return the required shared-memory size for this module.
 */
extern Size PersistentFilespace_ShmemSize(void);
								
/*
 * Initialize the shared-memory for this module.
 */
extern void PersistentFilespace_ShmemInit(void);

#ifdef MASTER_MIRROR_SYNC
extern void get_filespace_data(fspc_agg_state **fas, char *caller);
#endif

#endif   /* CDBPERSISTENTFILEESPACE_H */
