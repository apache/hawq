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
 * mdver_global_mdvsn.c
 *	 Implementation of Global MDVSN for metadata versioning
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/mdver.h"
#include "miscadmin.h"
#include "cdb/cdbvars.h"

/* Name to identify the MD Versioning Global MDVSN shared memory */
#define MDVER_GLOB_MDVN_SHMEM_NAME "MDVer Global MDVSN"

/* Name to identify the MD Versioning global version counter (GVC) shared memory area */
#define MDVER_GLOBAL_VER_SHMEM_NAME "MDVer Global Version Counter"

/* Pointer to the shared memory global version counter (GVC) */
uint64 *mdver_global_version_counter = NULL;

/* MDVer Global MDVSN is stored here, once attached to */
Cache *mdver_glob_mdvsn = NULL;

/* Forward declarations */
static bool mdver_entry_equivalent(const void *new_entry, const void *cached_entry);
static void mdver_entry_populate(const void *resource, const void *param);

/*
 * Initialize the shared memory data structures needed for MD Versioning
 *
 */
void
mdver_shmem_init(void)
{
	CacheCtl cacheCtl;
	MemSet(&cacheCtl, 0, sizeof(CacheCtl));

	cacheCtl.maxSize = gp_mdver_max_entries;
	cacheCtl.cacheName = MDVER_GLOB_MDVN_SHMEM_NAME;
	cacheCtl.entrySize = sizeof(mdver_entry);
	cacheCtl.keySize = sizeof(((mdver_entry *)0)->key);
	cacheCtl.keyOffset = GPDB_OFFSET(mdver_entry, key);

	cacheCtl.hash = int32_hash;
	cacheCtl.keyCopy = (HashCopyFunc) memcpy;
	cacheCtl.match = (HashCompareFunc) memcmp;
	cacheCtl.equivalentEntries = mdver_entry_equivalent;
	cacheCtl.cleanupEntry = NULL; /* No cleanup necessary */
	cacheCtl.populateEntry = mdver_entry_populate;

	cacheCtl.baseLWLockId = FirstMDVersioningLock;
	cacheCtl.numPartitions = NUM_MDVERSIONING_PARTITIONS;

	mdver_glob_mdvsn = Cache_Create(&cacheCtl);
	Assert(NULL != mdver_glob_mdvsn);

	bool attach = false;
	/* Allocate or attach to shared memory area */
	void *shmem_base = ShmemInitStruct(MDVER_GLOBAL_VER_SHMEM_NAME,
			sizeof(*mdver_global_version_counter),
			&attach);

	mdver_global_version_counter = (uint64 *)shmem_base;
	Assert(0 == *mdver_global_version_counter);

}


/*
 * SharedCache callback. Tests if two mdver entries are equivalent
 *  Two entries are considered equivalent if the keys are equal, regardless
 *  of the versions.
 *
 * 	new_entry: A new entry that is considered for insertion/look-up
 * 	cached_entry: The entry in the cache we're comparing to
 */
static bool
mdver_entry_equivalent(const void *new_entry, const void *cached_entry)
{
	Assert(NULL != new_entry);
	Assert(NULL != cached_entry);

#if USE_ASSERT_CHECKING
	mdver_entry *res1 = (mdver_entry *) new_entry;
	mdver_entry *res2 = (mdver_entry *) cached_entry;
#endif

	Assert(res1->key == res2->key);

	/* As long as keys are equal, we consider the entries equal */
	return true;
}

/*
 * SharedCache callback. Populates a newly acquired mdver entry
 * returning it to the caller.
 * 	resource: Pointer to the acquired mdver entry
 * 	param: Pointer to the entry with the source data
 *
 */
static void
mdver_entry_populate(const void *resource, const void *param)
{
	Assert(NULL != resource);
	Assert(NULL != param);

	mdver_entry *mdver_new = (mdver_entry *) resource;
	mdver_entry *mdver_info = (mdver_entry *) param;

	memcpy(mdver_new, mdver_info, sizeof(mdver_entry));
}

/*
 * Compute the size of shared memory required for the MD Versioning component
 */
Size
mdver_shmem_size(void)
{
	return Cache_SharedMemSize(gp_mdver_max_entries, sizeof(mdver_entry))
			+ sizeof(*mdver_global_version_counter);
}

/*
 * Returns pointer to the MDVer Global MDVSN component
 */
Cache *
mdver_get_glob_mdvsn(void)
{
	Assert(NULL != mdver_glob_mdvsn);
	return mdver_glob_mdvsn;
}

/*
 * Look up an entry in the Global MDVSN component.
 * To avoid any concurrency issues, this returns a copy of the entry,
 * palloc'ed in the current memory context. The caller is responsible
 * for freeing this copy.
 *
 * 	 Returns a copy of the entry if found, NULL otherwise.
 *
 */
mdver_entry *
mdver_glob_mdvsn_find(Oid oid)
{

	Assert(NULL != mdver_glob_mdvsn);

	mdver_entry mdver_info;
	mdver_info.key = oid;

	/* FIXME gcaragea 03/18/2014: Trigger evictions if cache is full (MPP-22923) */
	CacheEntry *localEntry = Cache_AcquireEntry(mdver_glob_mdvsn, &mdver_info);
	Assert(NULL != localEntry);

	CacheEntry *cachedEntry = Cache_Lookup(mdver_glob_mdvsn, localEntry);

	/* Release local entry. We don't need it anymore */
	Cache_Release(mdver_glob_mdvsn, localEntry);

	mdver_entry *mdver_copy = NULL;
	if (NULL != cachedEntry)
	{
		/* Found a match. Make a local copy */
		mdver_entry *shared_mdver = (mdver_entry *) CACHE_ENTRY_PAYLOAD(cachedEntry);
		mdver_copy = (mdver_entry *) palloc0(sizeof(mdver_entry));

		/* Lock entry to ensure atomicity of copy */
		Cache_LockEntry(mdver_glob_mdvsn, cachedEntry);

		memcpy(mdver_copy, shared_mdver, sizeof(mdver_entry));

		/* Got the copy, unlock entry */
		Cache_UnlockEntry(mdver_glob_mdvsn, cachedEntry);

		/*
		 * We're also done with the entry, release our pincount on it
		 *
		 * TODO gcaragea 05/02/2014: Are there cases where we need to hold the
		 * entry past this point? (MPP-22923)
		 */
		Cache_Release(mdver_glob_mdvsn, cachedEntry);
	}

	return mdver_copy;
}

/*
 * Clears the contents of the entire Global MDVSN specified.
 *
 * Pinned entries are not deleted, but they are marked for removal as soon
 * as the last user releases them. Entries marked for deletion are not returned
 * as valid results during look-ups either.
 */
void
mdver_glob_mdvsn_nuke(void)
{
	Assert(NULL != mdver_glob_mdvsn);

	LWLockAcquire(MDVerWriteLock, LW_EXCLUSIVE);

	PG_TRY();
	{
		int32 num_deleted = Cache_Clear(mdver_glob_mdvsn);
		elog(gp_mdversioning_loglevel, "Nuke at Global MDVSN deleted %d entries", num_deleted);
	}
	PG_CATCH();
	{
		LWLockRelease(MDVerWriteLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	LWLockRelease(MDVerWriteLock);
}
