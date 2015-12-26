/*-------------------------------------------------------------------------
 *
 * syncrefhashtable.h
 *	  Interface for a synchronized, refcounted hashtable.
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

#ifndef SYNCREFHASHTABLE_H_
#define SYNCREFHASHTABLE_H_

#include "postgres.h"
#include "utils/hsearch.h"
#include "storage/lwlock.h"

#define GPDB_OFFSET(T,M) (((ptrdiff_t) & (((T*)0x1)->M)) - 1)

/*
 * Signature for function to test if a hash entry is empty and safe to be
 * removed
 */
typedef bool (*SyncHTEntryIsEmptyFunc) (const void *entry);

/*
 * Signature for function to clear an empty entry after inserting
 * in the hashtable
 */
typedef void (*SyncHTEntryInitFunc) (void *entry);

typedef struct SyncHTCtl
{

	Size keySize;		/* hash key length in bytes */
	Size entrySize;		/* total user element size in bytes */
	HashValueFunc hash;			/* hash function */
	HashCompareFunc match;		/* key comparison function */
	HashCopyFunc keyCopy;		/* key copying function */

	char *tabName; 	/* Name of the hashtable (used to attach to shared memory) */
	long numElements;

	LWLockId baseLWLockId; /* LockId of the first LW Lock to be used for locking partitions */
	long numPartitions; /* no. partitions (must be power of 2). Must match the number of LW locks in the array above */

	ptrdiff_t keyOffset; /* offset in the payload where the key is located */
	ptrdiff_t pinCountOffset; /* offset in the payload where the pincount is located */
	SyncHTEntryIsEmptyFunc isEmptyEntry; /* function to determine if an entry can be deleted */
	SyncHTEntryInitFunc initEntry; /* function to clear an empty entry before returning it to caller */

} SyncHTCtl;

/* Opaque type defined in synchrefhashtable.c */
typedef struct SyncHT SyncHT;

extern SyncHT *SyncHTCreate(SyncHTCtl *syncHTCtl);
extern void *SyncHTLookup(SyncHT *syncHT, void *key);
extern void *SyncHTInsert(SyncHT *syncHT, void *key, bool *existing);
extern bool SyncHTRelease(SyncHT *syncHT, void *entry);
extern void SyncHTDestroy(SyncHT *syncHT);
extern long SyncHTNumEntries(SyncHT *syncHT);

#endif /* SYNCREFHASHTABLE_H_ */
