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
 * persistentrecovery.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef PERSISTENTRECOVERY_H
#define PERSISTENTRECOVERY_H

#include "access/xlogdefs.h"
#include "access/xlog.h"
#include "cdb/cdbpersistentstore.h"

typedef struct Pass2RecoveryHashShmem_s {

	HTAB    *hash;

} Pass2RecoveryHashShmem_s; 

typedef struct Pass2RecoveryHashEntry_s {
	
	Oid objid;

	int32 segmentFileNum;

} Pass2RecoveryHashEntry_s;

extern Pass2RecoveryHashShmem_s *pass2RecoveryHashShmem;

extern void Pass2Recovery_ShmemInit(void);

extern Size Pass2Recovery_ShmemSize(void);

/* max number of AbortingCreate entry tracked in shared memory hash table */
#define GP_MAX_PASS2RECOVERY_ABORTINGCREATE 128

inline static int PersistentRecovery_DebugPrintLevel(void)
{
	if (Debug_persistent_bootstrap_print && IsBootstrapProcessingMode())
		return WARNING;
	else
		return Debug_persistent_recovery_print_level;
}


extern bool
PersistentRecovery_ShouldHandlePass1XLogRec(
	XLogRecPtr		*beginLoc,

	XLogRecPtr 		*lsn,
	
	XLogRecord 		*record);

extern void
PersistentRecovery_HandlePass2XLogRec(
	XLogRecPtr		*beginLoc,

	XLogRecPtr 		*lsn,
	
	XLogRecord 		*record);

extern bool
PersistentRecovery_ShouldHandlePass3XLogRec(
	XLogRecPtr		*beginLoc,

	XLogRecPtr 		*lsn,
	
	XLogRecord 		*record);

extern void
PersistentRecovery_CompletePass(void);

extern void
PersistentRecovery_Scan(void);

extern void
PersistentRecovery_CrashAbort(void);

extern void
PersistentRecovery_Update(void);

extern void
PersistentRecovery_Drop(void);

extern void
PersistentRecovery_SerializeRedoRelationFile(
	int redoRelationFile);

extern void
PersistentRecovery_DeserializeRedoRelationFile(
	int redoRelationFile);

#endif   /* PERSISTENTRECOVERY_H */
