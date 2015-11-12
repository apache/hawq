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
 * cdbmirroredappendonly.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBMIRROREDAPPENDONLY_H
#define CDBMIRROREDAPPENDONLY_H

#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "cdb/cdbpersistentstore.h"

/*
 * This module is for doing mirrored writes for Append-Only relation files privately being written
 * by a backend process.
 *
 * It is intended for Append-Only relation files not under the management of the Buffer Pool.
 */

/*
 * This structure contains write open information.  Consider the fields
 * inside to be private.
 */
typedef struct MirroredAppendOnlyOpen
{
	bool	isActive;

	RelFileNode	relFileNode;
	
	uint32		segmentFileNum;
	
	File		primaryFile;

	bool						create;
} MirroredAppendOnlyOpen;

// -----------------------------------------------------------------------------
// Open, Flush, and Close 
// -----------------------------------------------------------------------------

extern void AppendOnly_Overwrite(RelFileNode *relFileNode, int32 segmentFileNum, int *primaryError);

/*
 * We call MirroredAppendOnly_Create with the MirroredLock already held.
 */
extern void MirroredAppendOnly_Create(
	RelFileNode 				*relFileNode,
				/* The tablespace, database, and relation OIDs for the open. */

	int32						segmentFileNum,
				/* Which segment file. */

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	
	int 						*primaryError);


/*
 * MirroredAppendOnly_OpenReadWrite will acquire and release the MirroredLock.
 */
extern void MirroredAppendOnly_OpenReadWrite(
	MirroredAppendOnlyOpen		*open,
				/* The resulting open struct. */

	RelFileNode 				*relFileNode,
				/* The tablespace, database, and relation OIDs for the open. */

	int32						segmentFileNum,
				/* Which segment file. */

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	
	int64						logicalEof,
				/* The logical EOF to begin appending the new data. */

	bool						readOnly,

	int 						*primaryError);

extern bool MirroredAppendOnly_IsActive(
					MirroredAppendOnlyOpen *open);


/*
 * Flush and Close an Append-Only relation file.
 *
 * If the flush is unable to complete on the mirror, then information (segment file, old EOF, 
 * new EOF) on the new Append-Only data will be added to the commit, distributed commit,
 * distributed prepared and commit prepared XLOG records so that data can be resynchronized
 * later.
 */
extern void MirroredAppendOnly_FlushAndClose(
	MirroredAppendOnlyOpen 		*open,
				/* The open struct. */				

	int 						*primaryError);

/*
 * Flush an Append-Only relation file.
 */
extern void MirroredAppendOnly_Flush(
	MirroredAppendOnlyOpen 		*open,
				/* The open struct. */				

	int 						*primaryError);

/*
 * Close an Append-Only relation file.
 */
extern void MirroredAppendOnly_Close(
	MirroredAppendOnlyOpen 	*open);


extern void MirroredAppendOnly_Drop(
	RelFileNode					*relFileNode,
	 
	int32						segmentFileNum,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	
	int							*primaryError);


// -----------------------------------------------------------------------------
// Append 
// -----------------------------------------------------------------------------
				
/*
 * Append mirrored.
 */
extern void MirroredAppendOnly_Append(
	MirroredAppendOnlyOpen *open,
				/* The open struct. */

	void		*appendData,
				/* Pointer to the Append-Only data. */

	int32		appendDataLen,
	
				/* The byte length of the Append-Only data. */
	int 		*primaryError);

// -----------------------------------------------------------------------------
// Truncate
// ----------------------------------------------------------------------------
extern void MirroredAppendOnly_Truncate(
	MirroredAppendOnlyOpen *open,
				/* The open struct. */
	
	int64		position,
				/* The position to cutoff the data. */

	int 		*primaryError);


// -----------------------------------------------------------------------------
// Read local side (primary segment)
// ----------------------------------------------------------------------------
extern int MirroredAppendOnly_Read(
	MirroredAppendOnlyOpen *open,
	/* The open struct. */
	
	void					*buffer,
	/* Pointer to the buffer. */
	
	int32					bufferLen);

extern void MirroredFileSysObj_JustInTimeDbDirCreate(
		DbDirNode			*justInTimeDbDirNode);

#endif   /* CDBMIRROREDAPPENDONLY_H */


