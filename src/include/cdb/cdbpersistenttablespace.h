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
 * cdbpersistenttablespace.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBPERSISTENTTABLESPACE_H
#define CDBPERSISTENTTABLESPACE_H

#include "access/persistentfilesysobjname.h"
#include "access/xlogmm.h"
#include "cdb/cdbsharedoidsearch.h"
#include "cdb/cdbdoublylinked.h"
#include "storage/fd.h"
#include "storage/itemptr.h"
#include "utils/palloc.h"
#include "cdb/cdbpersistentfilesysobj.h"

extern void PersistentTablespace_Reset(void);

extern PersistentFileSysState PersistentTablespace_GetState(
	Oid 	tablespaceOid);

extern void PersistentTablespace_LookupTidAndSerialNum(
	Oid 		tablespaceOid,
	ItemPointer		persistentTid,
	int64			*persistentSerialNum);


/*
 * The states(returned code) of a persistent database directory object usablility.
 */
typedef enum RC4PersistentTablespaceGetFilespaces
{
	RC4PersistentTablespaceGetFilespaces_None= 0,
	RC4PersistentTablespaceGetFilespaces_Ok,
	RC4PersistentTablespaceGetFilespaces_TablespaceNotFound,
	RC4PersistentTablespaceGetFilespaces_FilespaceNotFound,
	RC4PersistentTablespaceGetFilespaces_MaxVal /* must always be last */
} RC4PersistentTablespaceGetFilespaces;
				
extern RC4PersistentTablespaceGetFilespaces PersistentTablespace_TryGetFilespacePath(
	Oid 		tablespaceOid,
	char **filespaceLocation,
	Oid *filespaceOid);

extern void PersistentTablespace_GetFilespacePath(
	Oid 		tablespaceOid,
	bool 		needDispatchedTablespaceInfo,
	char		**filespaceLocation);

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
extern void PersistentTablespace_MarkCreatePending(
	Oid 		filespaceOid,
				/* The filespace where the tablespace lives. */

	Oid 		tablespaceOid,
				/* The tablespace OID for the create. */
				
	ItemPointer		persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */

	int64			*persistentSerialNum,

	bool			flushToXLog);
				/* When true, the XLOG record for this change will be flushed to disk. */

// -----------------------------------------------------------------------------
// Transaction End	
// -----------------------------------------------------------------------------
				
/*
 * Indicate the transaction commited and the relation is officially created.
 */
extern void PersistentTablespace_Created(							
	PersistentFileSysObjName *fsObjName,
				/* The tablespace OID for the create. */
	
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
extern PersistentFileSysObjStateChangeResult PersistentTablespace_MarkDropPending(
	PersistentFileSysObjName *fsObjName,
				/* The tablespace OID for the drop. */
	
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
extern PersistentFileSysObjStateChangeResult PersistentTablespace_MarkAbortingCreate(
	PersistentFileSysObjName *fsObjName,
				/* The tablespace OID for the aborting create. */
							
	ItemPointer 	persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */
							
	int64			persistentSerialNum,
				/* Serial number for the relation.	Distinquishes the uses of the tuple. */

	bool			retryPossible);
					
/*
 * Indicate we phsyicalled removed the relation file.
 */
extern void PersistentTablespace_Dropped(
	PersistentFileSysObjName *fsObjName,
				/* The tablespace OID for the dropped tablespace. */
										
	ItemPointer 	persistentTid,
				/* TID of the gp_persistent_rel_files tuple for the rel file */
							
	int64			persistentSerialNum);
				/* Serial number for the relation.	Distinquishes the uses of the tuple. */
	
// -----------------------------------------------------------------------------
// Re-build tablespace persistent table 'gp_persistent_tablespace_node'
// -----------------------------------------------------------------------------
extern void PersistentTablespace_AddCreated(
											 Oid 		filespaceOid,
											 /* The filespace where the tablespace lives. */
											 
											 Oid 		tablespaceOid,
											 /* The tablespace OID for the create. */
											 											 
											 bool			flushToXLog);
											 /* When true, the XLOG record for this change will be flushed to disk. */

// -----------------------------------------------------------------------------
// Shmem and Startup/Shutdown
// -----------------------------------------------------------------------------
				
/*
 * Return the required shared-memory size for this module.
 */
extern Size PersistentTablespace_ShmemSize(void);
								
/*
 * Initialize the shared-memory for this module.
 */
extern void PersistentTablespace_ShmemInit(void);
extern Oid PersistentTablespace_GetFileSpaceOid(Oid tablespaceOid);

#ifdef MASTER_MIRROR_SYNC 
void get_tablespace_data(tspc_agg_state **tas, char *caller);
#endif

#endif   /* CDBPERSISTENTTABLESPACE_H */
