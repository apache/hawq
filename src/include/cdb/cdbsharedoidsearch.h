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
 * cdbsharedoidsearch.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBSHAREDOIDSEARCH_H
#define CDBSHAREDOIDSEARCH_H

#include "utils/palloc.h"
#include "storage/fd.h"
#include "cdb/cdbshareddoublylinked.h"
#include "cdb/cdbsharedoidsearch.h"


typedef struct SharedOidSearchFreeObjPool
{
	SharedListBase		listBase;

	SharedDoublyLinkedHead		freeList;
} SharedOidSearchFreeObjPool;

typedef struct SharedOidSearchHashBucket
{
	SharedDoublyLinkedHead	bucketListHead;
} SharedOidSearchHashBucket;

typedef struct SharedOidSearchObjHeader
{
	struct
	{
		SharedDoubleLinks	links;

		int16	pinCount;

		bool	isDeleted;
	} private;

	Oid	oid1;		/* tablespace oid */
	Oid oid2;		/* database oid */

	uint8	clientData[];
} SharedOidSearchObjHeader;

typedef struct SharedOidSearchTable
{
	struct
	{
		int32		hashSize;

		SharedOidSearchFreeObjPool		freePool;

		SharedOidSearchHashBucket	buckets[1];
	} private;
} SharedOidSearchTable;

typedef enum SharedOidSearchAddResult
{
	SharedOidSearchAddResult_None = 0,
	SharedOidSearchAddResult_Ok = 1,
	SharedOidSearchAddResult_Exists = 2,
	SharedOidSearchAddResult_NoMemory = 3,
	MaxSharedOidSearchAddResult /* must always be last */
} SharedOidSearchAddResult;


// -----------------------------------------------------------------------------
// Initialize
// -----------------------------------------------------------------------------

extern int32 SharedOidSearch_TableLen(
	int32 hashSize,
				/* The hash array size */

	int32 freeObjectCount,

	int32 objectLen);
				/* 
				 * The total length of the objects that includes the embedded header
				 * SharedOidSearchObjHeader.
				 */
extern void SharedOidSearch_InitTable(
	SharedOidSearchTable *table,
				/* The shared search tables to initialize. */
	
	int32 hashSize,
				/* The hash array size */
	
	int32 freeObjectCount,
	
	int32 objectLen);
				/* 
				 * The total length of the objects that includes the embedded header
				 * SharedOidSearchObjHeader.
				 */

// -----------------------------------------------------------------------------
// Add, Find, Probe, Iterate, and Delete
// -----------------------------------------------------------------------------

extern SharedOidSearchAddResult SharedOidSearch_Add(
	SharedOidSearchTable 		*table,
	Oid 						oid1,
	Oid 						oid2,
	SharedOidSearchObjHeader	**header);

extern SharedOidSearchObjHeader *SharedOidSearch_Find(
	SharedOidSearchTable 	*table,
	Oid 					oid1,
	Oid 					oid2);

extern SharedOidSearchObjHeader *SharedOidSearch_Probe(
	SharedOidSearchTable 	*table,
	Oid 					oid1,
	Oid						oid2);

extern void SharedOidSearch_Iterate(
	SharedOidSearchTable 		*table,
	SharedOidSearchObjHeader	**header);

extern void SharedOidSearch_ReleaseIterator(
		SharedOidSearchTable		*table,
		SharedOidSearchObjHeader	**header);

extern void SharedOidSearch_Delete(
	SharedOidSearchTable 		*table,
	SharedOidSearchObjHeader	*header);
																																																																
#endif   /* CDBSHAREDOIDSEARCH_H */
