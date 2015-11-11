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
 * cdbmirroredbufferpool.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBMIRROREDBUFFERPOOL_H
#define CDBMIRROREDBUFFERPOOL_H

#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"
#include "catalog/catalog.h"
#include "storage/smgr.h"

/*
 * This module is for doing mirrored writes for relation files currently managed by the
 * Buffer Pool.
 *
 * It is intended for use by the Background Writer process through the BufMgr module.
 */

/*
 * This structure contains write open information.  Consider the fields
 * inside to be private.
 */
typedef struct MirroredBufferPoolOpen
{
	bool		isActive;
	RelFileNode relFileNode;
	uint32		segmentFileNum;
	File		primaryFile;
	bool						create;
} MirroredBufferPoolOpen;

// -----------------------------------------------------------------------------
// Open, Flush, Close 
// -----------------------------------------------------------------------------

/*
 * Open a relation for mirrored write.
 */
extern void MirroredBufferPool_Open(
	MirroredBufferPoolOpen *open,
				/* The resulting open struct. */

	RelFileNode 	*relFileNode,
				/* The tablespace, database, and relation OIDs for the open. */

	uint32			segmentFileNum,
				/* Which segment file. */

	char			*relationName,
				/* For tracing only.  Can be NULL in some execution paths. */
	
	int 			*primaryError);

extern void MirroredBufferPool_Create(
	MirroredBufferPoolOpen		*open,
				/* The resulting open struct. */

	RelFileNode 				*relFileNode,
				/* The tablespace, database, and relation OIDs for the open. */

	uint32						segmentFileNum,
				/* Which segment file. */

	char						*relationName,
				/* For tracing only.  Can be NULL in some execution paths. */

	int 						*primaryError);

extern bool MirroredBufferPool_IsActive(
	MirroredBufferPoolOpen *open);
				/* The open struct. */
				
/*
 * Flush a Buffer Pool relation file.
 */
extern bool MirroredBufferPool_Flush(
	MirroredBufferPoolOpen *open);
				/* The open struct. */
				
/*
 * Close a Buffer Pool relation file.
 */
extern void MirroredBufferPool_Close(
	MirroredBufferPoolOpen *open);
				/* The open struct. */


extern void MirroredBufferPool_Drop(
	RelFileNode 				*relFileNode,
	 
	int32						segmentFileNum,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	bool						isRedo,
	
	int 						*primaryError);

/*
 * Write a Buffer Pool page mirrored.
 */
extern bool MirroredBufferPool_Write(
	MirroredBufferPoolOpen *open,
				/* The open struct. */

	int32		position,

	void		*buffer,
				/* Pointer to the Buffer Pool page, properly protected by locks. */

	int32		bufferLen);

extern int MirroredBufferPool_Read(
	MirroredBufferPoolOpen *open,
				/* The open struct. */

	int32		position,

	void		*buffer,
				/* Pointer to the Buffer Pool page, properly protected by locks. */

	int32		bufferLen);

extern int64 MirroredBufferPool_SeekSet(
	MirroredBufferPoolOpen *open,
				/* The open struct. */

	int32		position);

extern int64 MirroredBufferPool_SeekEnd(
	MirroredBufferPoolOpen *open);
				/* The open struct. */

extern bool MirroredBufferPool_Truncate(
	MirroredBufferPoolOpen *open,
				/* The open struct. */

	int64		position);

#endif   /* CDBMIRROREDBUFFERPOOL_H */
