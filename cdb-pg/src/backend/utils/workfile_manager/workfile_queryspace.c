/*-------------------------------------------------------------------------
 *
 * workfile_queryspace.c
 *	 Implementation of workfile manager per query disk space accounting
 *
 * Copyright (c) 2013, EMC Corp.
 *
 *-------------------------------------------------------------------------
 */

#include <postgres.h>
#include "storage/shmem.h"
#include "utils/atomic.h"
#include "utils/workfile_mgr.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "cdb/cdbvars.h"
#include "utils/syncrefhashtable.h"

/* Name to identify the WorkfileQueryspace shared memory area by */
#define WORKFILE_QUERYSPACE_SHMEM_NAME "WorkfileQueryspace Hashtable"

static bool isQueryspaceEltEmpty(const void *entry);
static void initQueryspaceElt(void *entry);

/* Workfile queryspace hashtable is stored here, once attached to */
SyncHT *queryspace_Hashtable = NULL;

/* Per-query entry is stored here once initialized */
QueryspaceDesc *queryEntry = NULL;

/*
 * Nesting level for executor invocations. Used to ensure we only
 * initialize/release the queryEntry at the outer most nesting level
 */
int32 querySpaceNestingLevel = 0;

/*
 * Initialize shared memory area for the WorkfileDiskspace module
 */
void
WorkfileQueryspace_Init(void)
{
	SyncHTCtl syncHTCtl;
	MemSet(&syncHTCtl, 0, sizeof(SyncHTCtl));

	syncHTCtl.entrySize = sizeof(QueryspaceDesc);
	syncHTCtl.keySize = sizeof(Queryspace_HashKey);

	syncHTCtl.hash = tag_hash;
	syncHTCtl.keyCopy = (HashCopyFunc) memcpy;
	syncHTCtl.match = (HashCompareFunc) memcmp;

	/*
	 * The maximum number of queries active at any time is <= MaxBackends.
	 * Using MaxBackends as the size of our hashtable
	 */
	syncHTCtl.numElements = MaxBackends;

	syncHTCtl.baseLWLockId = FirstWorkfileQuerySpaceLock;
	syncHTCtl.numPartitions = NUM_WORKFILE_QUERYSPACE_PARTITIONS;

	syncHTCtl.tabName = WORKFILE_QUERYSPACE_SHMEM_NAME;
	syncHTCtl.isEmptyEntry = isQueryspaceEltEmpty;
	syncHTCtl.initEntry = initQueryspaceElt;

	syncHTCtl.keyOffset = GPDB_OFFSET(QueryspaceDesc, key);
	syncHTCtl.pinCountOffset = GPDB_OFFSET(QueryspaceDesc, pinCount);

	/* Create hashtable in the top memory context so we can clean up if transaction aborts */
	MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	queryspace_Hashtable = SyncHTCreate(&syncHTCtl);
	Assert(NULL != queryspace_Hashtable);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Returns the amount of shared memory needed for the WorkfileQueryspace module
 */
Size
WorkfileQueryspace_ShMemSize(void)
{
	return hash_estimate_size(MaxBackends, sizeof(QueryspaceDesc));
}


/*
 * Returns the amount of disk space used for workfiles for the query
 * uniquely identified by (session_id, command_count)
 *
 * Returns -1 if no corresponding entry found.
 */
int64
WorkfileQueryspace_GetSize(int session_id, int command_count)
{
	Queryspace_HashKey queryKey;
	queryKey.session_id = session_id;
	queryKey.command_count = command_count;

	QueryspaceDesc *queryEntry = (QueryspaceDesc *) SyncHTLookup(queryspace_Hashtable, &queryKey);

	int64 size = -1;
	if (NULL != queryEntry)
	{
		size = queryEntry->queryDiskspace;
		SyncHTRelease(queryspace_Hashtable, queryEntry);
	}


	return size;
}

/*
 * Reserve 'bytes' bytes to write to disk
 *   This should be called before actually writing to disk
 *
 *   If enough disk space is available, increments the per-query counter and returns true
 *   Otherwise, returns false
 */
bool
WorkfileQueryspace_Reserve(int64 bytes_to_reserve)
{
	bool success = false;

	if (NULL == queryEntry)
	{
		/*
		 * Did not find entry for this query, it must be a utility query
		 * that is spilling. No enforcing of per-query disk limit for these.
		 */
		return true;
	}

	int64 total = gp_atomic_add_64(&queryEntry->queryDiskspace, bytes_to_reserve);
	Assert(total >= (int64) 0);

	if (gp_workfile_limit_per_query == 0)
	{
		/* not enforced */
		success = true;
	}
	else
	{
		int64 max_allowed_diskspace = (int64) (gp_workfile_limit_per_query * 1024);
		success = (total <= max_allowed_diskspace);

		if (!success)
		{
			workfileError = WORKFILE_ERROR_LIMIT_PER_QUERY;

			/* Revert the reserved space */
			gp_atomic_add_64(&queryEntry->queryDiskspace, - bytes_to_reserve);
			/* Set diskfull to true to stop any further attempts to write more data */
			WorkfileDiskspace_SetFull(true /* isFull */);
		}
	}

	return success;
}

/*
 * Reserve 'num_chunks_to_reserve' number of chunks for current QE from the per-query memory quota
 *   This should be called every time we reserve vmem. Note: vmem is per-segment, but this is per-query.
 *
 *   If enough per-query memory is available, increments the per-query counter and returns true
 *   Otherwise, returns false.
 */
bool
PerQueryMemory_ReserveChunks(int32 num_chunks_to_reserve)
{
	bool success = false;

	if (NULL == queryEntry || max_chunks_per_query == 0)
	{
		/*
		 * Utility queries may not have queryEntry. Alternatively, if
		 * max_chunks_per_query is 0 then no enforcement. Note,
		 * max_chunks_per_query is derived from gp_vmem_limit_per_query
		 * inside memprot.c. Also note, for QD we will derive
		 * max_chunks_per_query from gp_vmem_limit_per_query, but
		 * we will not enforce, as in QD we do not use vmem protect system
		 * and instead call malloc directly.
		 */
		return true;
	}

	int32 total = gp_atomic_add_32(&queryEntry->chunksReserved, num_chunks_to_reserve);
	Assert(total >= (int32) 0);

	success = (total <= max_chunks_per_query);

	if (!success)
	{
		/* Revert the reserved space */
		gp_atomic_add_32(&queryEntry->chunksReserved, - num_chunks_to_reserve);
	}

	return success;
}

/*
 * Reports the number of chunks that the current query has reserved across all the QEs
 * 	in the current segment.
 */
int32
PerQueryMemory_TotalChunksReserved()
{
	if (NULL == queryEntry || max_chunks_per_query == 0)
	{
		/*
		 * Did not find entry for this query, it could be an utility query
		 * and max_chunks_per_query is set to 0. Alternatively, we might have run out of
		 * hash table slot. No enforcing of per-query memory limit for these cases.
		 */
		return 0;
	}

	return gp_atomic_add_32(&queryEntry->chunksReserved, 0);
}

/*
 * Notify of how many bytes were actually written to disk
 *
 * This should be called after writing to disk, with the actual number
 * of bytes written. This must be less or equal than the amount we reserved
 *
 */
void
WorkfileQueryspace_Commit(int64 commit_bytes, int64 reserved_bytes)
{
	Assert(reserved_bytes >= commit_bytes);

	if (reserved_bytes == commit_bytes)
	{
		/* Nothing to do, save a hashtable look-up and just return */
		return;
	}

	/*
	 * Query might not have an entry in the queryspace hashtable if it is a
	 * utility query that is spilling.
	 * No enforcing of per-query disk limit for these.
	 */
	if (NULL != queryEntry)
	{
#if USE_ASSERT_CHECKING
		int64 total =
#endif
		gp_atomic_add_64(&queryEntry->queryDiskspace, (commit_bytes - reserved_bytes));
		Assert(total >= (int64) 0);
	}
}

/*
 * Initializes and returns the Queryspace entry for this query.
 *
 * If the entry already exists, it returns the existing one, otherwise
 * it inserts a new one in the hashtable.
 */
QueryspaceDesc *
WorkfileQueryspace_InitEntry(int session_id, int command_count)
{
	Assert(querySpaceNestingLevel >= 0);

	/*
	 * Note, we assume that if we are in a segment (for master, we don't
	 * enforce any vmem limit) we already executed GPMemoryProtectInit(),
	 * where max_chunks_per_query is derived from gp_vmem_limit_per_query.
	 */
	if (gp_workfile_limit_per_query == 0 && max_chunks_per_query == 0)
	{
		/* Per-query limit not enforced, don't allocate hashtable entry */
		return NULL;
	}

	querySpaceNestingLevel++;

	if (querySpaceNestingLevel > 1)
	{
		/*
		 * We are in a nested Executor invocation. Don't allocate a new
		 * entry, used the stored one that was created by the top-level
		 * invocation.
		 */
		return queryEntry;
	}

	Queryspace_HashKey queryKey;
	queryKey.session_id = session_id;
	queryKey.command_count = command_count;
	bool existing = false;


	Assert(NULL == queryEntry);
	queryEntry = SyncHTInsert(queryspace_Hashtable, &queryKey, &existing);

	/*
	 * If HT runs out of entry, we may have a NULL here, in which case
	 * we cannot enforce any quota
	 */
	if (queryEntry != NULL)
	{
		/*
		 * We are accounting for all the allocations that we have reserved before
		 * we initialized our per-statement hash entry. Problem is: between
		 * setting queryEntry and updating prior allocations, we might have
		 * some allocations that are counted twice.
		 * Hopefully this amount will be very small, as the time between this
		 * assignment and queryEntry assignment is very small. We are loosing
		 * precision for performance. We also DO NOT use PerQueryMemory_ReserveChunks()
		 * to update chunksReserved, as that method automatically rolls back
		 * reservation if the quota is exceeded. Instead we want to go over
		 * the quota, if we have already exceeded it, and handle the error on
		 * next allocation request. Note, due to QE being recycled, getMOPChunksReserved()
		 * may return a higher than our limit. Although, the guc gp_vmem_limit_per_query
		 * is system level, if the prior query hits a limit, and ERRORs out, we
		 * end up extending our quota by 1 additional MB for error handling. So,
		 * if such QE is reused, we might have mop limit set to higher than our per-query
		 * limit. We therefore only want to enforce this limit if we try to
		 * actually *allocate* beyond our limit.
		 */
		gp_atomic_add_32(&queryEntry->chunksReserved, getMOPChunksReserved());
	}
	else
	{
		elog(LOG, "Could not enforce per-query vmem limit: No resource left for storing per-query vmem details.");
	}

	if (!existing)
	{
		elog(gp_workfile_caching_loglevel, "Inserted entry for query (sessionid=%d, commandcnt=%d)",
				queryEntry->key.session_id, queryEntry->key.command_count);
	}

	return queryEntry;
}

/*
 * Releases the corresponding Query space entry for this query
 *
 */
void
WorkfileQueryspace_ReleaseEntry(void)
{

	if (NULL == queryEntry)
	{
		/* Already released, nothing to do */
		return;
	}

	querySpaceNestingLevel--;
	Assert(querySpaceNestingLevel >= 0);

	if (querySpaceNestingLevel > 0)
	{
		/*
		 * We are in a nested Executor invocation. Don't release
		 * the entry, the top-level invocation will take care of that.
		 */
		return;
	}

	bool deleted = SyncHTRelease(queryspace_Hashtable, queryEntry);

	if (deleted)
	{
		elog(gp_workfile_caching_loglevel, "Deleted entry for query (sessionid=%d, commandcnt=%d)",
				queryEntry->key.session_id, queryEntry->key.command_count);
	}

	queryEntry = NULL;
}

/*
 * Callback function to test if an entry in the hashtable is "empty"
 */
static bool
isQueryspaceEltEmpty(const void *entry)
{
	/*
	 * Queryspace hashtable entries can always be removed when
	 * the last backend releases it
	 */
	return true;
}

/*
 * Callback function to initialize an entry before returning to the caller
 */
static void
initQueryspaceElt(void *entry)
{
	QueryspaceDesc *hashElt = (QueryspaceDesc *) entry;
	hashElt->queryDiskspace = 0L;
	hashElt->chunksReserved = 0;
	hashElt->pinCount = 0;
}
