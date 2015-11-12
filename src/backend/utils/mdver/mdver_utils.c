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
 * mdver_utils.c
 *	 Utility functions for metadata versioning
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "utils/atomic.h"
#include "utils/inval.h"
#include "utils/mdver.h"


static void mdver_request_from_global(Oid key, uint64 *ddl_version, uint64 *dml_version);
static void mdver_request_after_nuke(Oid key, uint64 *ddl_version, uint64 *dml_version);

/*
 * Returns a string representation of the MD Versioning event passed in.
 * Result string is palloc-ed in the current memory context.
 */
char *
mdver_event_str(mdver_event *ev)
{
	char *result = (char *) palloc(MDVER_EVENT_STR_LEN);
#ifdef MD_VERSIONING_INSTRUMENTATION
	snprintf(result, MDVER_EVENT_STR_LEN, "[MDVER_EVENT PID=%d OID=%d DDV= " UINT64_FORMAT " --> " UINT64_FORMAT " DMV= " UINT64_FORMAT " --> " UINT64_FORMAT "]",
			ev->backend_pid,
			ev->key,
			ev->old_ddl_version, ev->new_ddl_version,
			ev->old_dml_version, ev->new_dml_version);
#else
	snprintf(result, MDVER_EVENT_STR_LEN, "[MDVER_EVENT OID=%d DDV= " UINT64_FORMAT " --> " UINT64_FORMAT " DMV= " UINT64_FORMAT " --> " UINT64_FORMAT "]",
			ev->key,
			ev->old_ddl_version, ev->new_ddl_version,
			ev->old_dml_version, ev->new_dml_version);
#endif

	return result;
}

/*
 * Main entry point for clients that request versions for objects. Returns
 * the most recent visible version of an object when requested by a
 * backend/client.
 *
 * If no version is found, a new version for the object is generated and
 * recorded in the caches. New versioning events are potentially generated
 * as well if needed.
 *
 * This function never returns INVALID_MD_VERSION as a result.
 *
 *   key: The key of the looked-up object
 *   ddl_version: used to return the ddl version for the object
 *   dml_version: used to return the dml version for the object
 *
 */
void
mdver_request_version(Oid key, uint64 *ddl_version, uint64 *dml_version)
{

	Assert(NULL != ddl_version);
	Assert(NULL != dml_version);

	*dml_version = INVALID_MD_VERSION;
	*ddl_version = INVALID_MD_VERSION;

	if (!mdver_enabled())
	{
		/*
		 * MD Versioning feature is turned off. Return (1,0) as a fixed
		 * version so that ORCA can check for equality etc.
		 */
		*ddl_version = 1;
		*dml_version = 0;
		return;
	}

	/*
	 * Basic flow is described below. More details can be found in
	 * the Metadata Versioning design document.
	 *
	 * - Do a look-up in the Local MDVSN. If the object is found there,
	 *   return its version.
	 * - If not found in Local MDVSN, and nuke_happened is not set,
	 *   do a look-up in the Global MDVSN.
	 *    - If the object is found there,
	 *       - Add it to the Local MDVSN and
	 *       - Return its version.
	 *    - If the object is not in Global MDVSN
	 *        - Generate a new version for the object
	 *        - Record the version in Global MDVSN
	 *        - Add new version to Local MDVSN
	 *        - Return new version
	 * - If not found in Local MDVSN and nuke_happened is set, the object
	 *  needs a new version.
	 *    - Generate a new version for the object
	 *    - Read current version from Global MDVSN (if exists)
	 *    - Add a versioning event to the CVQ with the new version
	 *    - Record the new version in the Local MDVSN
	 *    - Return the new version.
	 */

	/* Try Local MDVSN first */
	mdver_local_mdvsn *local_mdvsn = GetCurrentLocalMDVSN();
	Assert(NULL != local_mdvsn);

	mdver_entry *crt_entry = mdver_local_mdvsn_find(local_mdvsn, key);
	if (NULL != crt_entry)
	{

#ifdef MD_VERSIONING_INSTRUMENTATION
		elog(gp_mdversioning_loglevel, "Found version in Local MDVSN: (%d, " UINT64_FORMAT ", " UINT64_FORMAT ")",
				key, crt_entry->ddl_version, crt_entry->dml_version);
#endif

		*ddl_version = crt_entry->ddl_version;
		*dml_version = crt_entry->dml_version;
		return;
	}

	if (!local_mdvsn->nuke_happened)
	{
		/* TODO gcaragea 6/3/2014: Traverse subtransaction contexts during look-up (MPP-22935) */
		mdver_request_from_global(key, ddl_version, dml_version);

	}
	else
	{
		mdver_request_after_nuke(key, ddl_version, dml_version);
	}

	Assert(INVALID_MD_VERSION != *ddl_version ||
			INVALID_MD_VERSION != *dml_version);

	mdver_entry new_entry = { key, *ddl_version, *dml_version};
	mdver_local_mdvsn_add(local_mdvsn, &new_entry, true /* local */);

}

/*
 * When a backend is requesting the more recent version of an object,
 * if the Local MDVSN cache doesn't have the version, and if a NUKE event
 * hasn't been encountered in the current transaction, it is looked up
 * in the Global MDVSN shared cache.
 *
 * If the object is found in Global MDVSN, return the global version.
 * If the object is not found, generate a new version, record it in Global MDVSN
 * and then return it.
 *
 *   key: The key of the looked-up object
 *   ddl_version: used to return the ddl version for the object
 *   dml_version: used to return the dml version for the object
 *
 */
static void
mdver_request_from_global(Oid key, uint64 *ddl_version, uint64 *dml_version)
{

	Assert(NULL != ddl_version);
	Assert(NULL != dml_version);

	Cache *mdver_glob_mdvsn = mdver_get_glob_mdvsn();
	Assert(NULL != mdver_glob_mdvsn);

	mdver_entry entry = {key, INVALID_MD_VERSION, INVALID_MD_VERSION};

	/* FIXME gcaragea 06/03/2014: Trigger evictions if cache is full (MPP-22923) */
	CacheEntry *localEntry = Cache_AcquireEntry(mdver_glob_mdvsn, &entry);

	Assert(NULL != localEntry);

	/*
	 * We're about to look-up and insert a shared cache entry.
	 * Grab writer lock in exclusive mode, so that no other backend
	 * can insert or update the same entry at the same time.
	 */
	LWLockAcquire(MDVerWriteLock, LW_EXCLUSIVE);

	CacheEntry *cachedEntry = Cache_Lookup(mdver_glob_mdvsn, localEntry);

	if (NULL != cachedEntry)
	{
		/* Not found in LVSN, not nuke happened, eventually found in GVSN */
		mdver_entry *crt_entry = CACHE_ENTRY_PAYLOAD(cachedEntry);

		*ddl_version = crt_entry->ddl_version;
		*dml_version = crt_entry->dml_version;

#ifdef MD_VERSIONING_INSTRUMENTATION
		elog(gp_mdversioning_loglevel, "Found version in Global MDVSN: (%d, " UINT64_FORMAT ", " UINT64_FORMAT "). Adding it to Local MDVSN",
				key, crt_entry->ddl_version, crt_entry->dml_version);
#endif

		/*
		 * We're also done with the entry, release our pincount on it
		 *
		 * TODO gcaragea 05/02/2014: Are there cases where we need to hold the
		 * entry past this point? (MPP-22923)
		 */

		Cache_Release(mdver_glob_mdvsn, cachedEntry);
	}
	else
	{
		/* Not found in LVSN, not nuke happened, not found in GVSN either */

		/* Generate new version */
		*ddl_version = mdver_next_global_version();
		*dml_version = mdver_next_global_version();

		/* Add to GVSN */
		mdver_entry *new_entry = CACHE_ENTRY_PAYLOAD(localEntry);
		new_entry->ddl_version = *ddl_version;
		new_entry->dml_version = *dml_version;

#ifdef MD_VERSIONING_INSTRUMENTATION
		elog(gp_mdversioning_loglevel, "Inserting new version in Global MDVSN: (%d, " UINT64_FORMAT ", " UINT64_FORMAT "). Adding it to Local MDVSN",
				key, new_entry->ddl_version, new_entry->dml_version);
#endif

		Cache_Insert(mdver_glob_mdvsn, localEntry);

	}

	LWLockRelease(MDVerWriteLock);

	/* Release local entry. We don't need it anymore */
	Cache_Release(mdver_glob_mdvsn, localEntry);
}

/*
 * When a backend is requesting the more recent version of an object,
 * if the Local MDVSN cache doesn't have the version, and if a NUKE event
 * has been encountered in the current transaction, a new version is
 * generated and returned for the object. A new versioning event is also
 * produced.
 *
 *   key: The key of the looked-up object
 *   ddl_version: used to return the ddl version for the object
 *   dml_version: used to return the dml version for the object
 *
 */
static void
mdver_request_after_nuke(Oid key, uint64 *ddl_version, uint64 *dml_version)
{
	Assert(NULL != ddl_version);
	Assert(NULL != dml_version);

	/* Generate new version */
	*ddl_version = mdver_next_global_version();
	*dml_version = mdver_next_global_version();

	mdver_event *new_event = (mdver_event *) palloc0(sizeof(mdver_event));
	new_event->key = key;
	new_event->new_ddl_version = *ddl_version;
	new_event->new_dml_version = *dml_version;
	new_event->old_ddl_version = INVALID_MD_VERSION;
	new_event->old_dml_version = INVALID_MD_VERSION;

#ifdef MD_VERSIONING_INSTRUMENTATION
	/* Add my current process id as the originating backend pid */
	new_event->backend_pid = MyProcPid;
#endif

	/* Annotate Versioning Event with the current version from Global MDVSN if exists */
	mdver_entry *crt_entry = mdver_glob_mdvsn_find(key);
	if (NULL != crt_entry)
	{
		new_event->old_ddl_version = crt_entry->ddl_version;
		new_event->old_dml_version = crt_entry->dml_version;
	}

	CacheAddVersioningEvent(new_event);

#ifdef MD_VERSIONING_INSTRUMENTATION
	char *mdev_str = mdver_event_str(new_event);
	ereport(gp_mdversioning_loglevel,
			(errmsg("mdver_consume_after_nuke: generated new VE %s",
					mdev_str),
					errprintstack(false)));
	pfree(mdev_str);
#endif

	/* A copy of the event is added to the queue above. We can pfree our local copy */
	pfree(new_event);
}


/*
 * Retrieves the next unique version using the Global Version Counter (GVC)
 */
uint64
mdver_next_global_version()
{
	return gp_atomic_add_uint64(mdver_global_version_counter, 1);
}

/*
 * Returns true if Metadata Versioning is enabled in the current
 * context
 */
bool
mdver_enabled(void)
{
	/*
	 * We only initialized Metadata Versioning on the master,
	 * and only for QD or utility mode process.
	 * MD Versioning can also be disabled by the guc gp_metadata_versioning.
	 */

	/* TODO gcaragea 05/06/2014: Do we need to disable MD Versioning during (auto)vacuum? (MPP-23504) */

	return gp_metadata_versioning &&
			AmIMaster() &&
			((GP_ROLE_DISPATCH == Gp_role) || (GP_ROLE_UTILITY == Gp_role));
}
