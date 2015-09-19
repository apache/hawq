/*-------------------------------------------------------------------------
 *
 * mdver_global_handler.c
 *	 Implementation of Global VE Handler for metadata versioning
 *
 * Copyright (c) 2014, Pivotal, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/mdver.h"
#include "utils/guc.h"
#include "storage/sinval.h"
#include "cdb/cdbvars.h"

/* Forward declarations */
static void mdver_globalhandler_add_version(mdver_event *event);
static void mdver_globalhandler_reconcile(mdver_event *event, CacheEntry *cached_entry);

/*
 * Entry point to the Global VE Handler. Iterate through a list of messages,
 * and add versions to the cache if there are any versioning events in the list.
 *  messages: Pointer to the list of messages
 *  n: Number of messages to process
 */
void
mdver_globalhandler_new_event(SharedInvalidationMessage *messages, int n)
{
	Assert(NULL != messages);

	/* There is no metadata versioning on segments, ignore */
	if (AmISegment())
	{
		return;
	}

	for (int i=0; i < n; i++)
	{
		SharedInvalidationMessage *msg = &messages[i];
		if (SHAREDVERSIONINGMSG_ID == msg->id)
		{
			mdver_globalhandler_add_version(&msg->ve.verEvent);
			/*
			 * We change the flag of the message to non-local once added
			 * to the SVQ
			 */
			msg->ve.local = false;
		}
	}
}

/*
 * Add or update an entry in the Global MDVSN cache for a versioning event
 * found in the event list. Reconcile with current contents of the cache
 * if needed.
 *  event: The event containing the versioning information for an update
 */
static void
mdver_globalhandler_add_version(mdver_event *event)
{
	Assert(NULL != event);

	Cache *glob_mdvsn = mdver_get_glob_mdvsn();

	if (mdver_is_nuke_event(event))
	{
		mdver_glob_mdvsn_nuke();
		return;
	}

	mdver_entry mdver = { InvalidOid, INVALID_MD_VERSION, INVALID_MD_VERSION };
	mdver.key = event->key;
	mdver.ddl_version = INVALID_MD_VERSION;
	mdver.dml_version = INVALID_MD_VERSION;

	/* FIXME gcaragea 04/14/2014: Trigger evictions if cache is full (MPP-22923) */
	CacheEntry *acquired_entry = Cache_AcquireEntry(glob_mdvsn, &mdver);
	Assert(NULL != acquired_entry);

	/*
	 * We're about to look-up and insert/update a shared cache entry.
	 * Grab writer lock in exclusive mode, so that no other backend
	 * tries to insert or update the same entry at the same time.
	 */
	LWLockAcquire(MDVerWriteLock, LW_EXCLUSIVE);

	CacheEntry *cached_entry = Cache_Lookup(glob_mdvsn, acquired_entry);

	if (NULL != cached_entry)
	{
		mdver_globalhandler_reconcile(event, cached_entry);

		/* Done with the looked-up entry. Release it */
		Cache_Release(glob_mdvsn, cached_entry);
	}
	else
	{
		/* Entry not found, insert new entry */
		mdver_entry *new_mdver_entry = CACHE_ENTRY_PAYLOAD(acquired_entry);

#ifdef MD_VERSIONING_INSTRUMENTATION
		elog(gp_mdversioning_loglevel, "Inserting into GlobalMDVSN entry %d: (%d,%d)",
				event->key,
				(int) event->new_ddl_version, (int) event->new_dml_version);
#endif

		new_mdver_entry->ddl_version = event->new_ddl_version;
		new_mdver_entry->dml_version = event->new_dml_version;

		Cache_Insert(glob_mdvsn, acquired_entry);
	}

	Cache_Release(glob_mdvsn, acquired_entry);
	LWLockRelease(MDVerWriteLock);
}

/*
 * Reconcile an incoming versioning event with an existing Global MDVSN entry
 * for the same versioned object.
 *
 * Each versioning event contains the old version and the new version as known
 * by the originating backend:
 *   VE = (key, oldV, newV)
 * Cached entry contains the current version globally visible:
 *   entry = (key, crtV)
 *
 * We have the following scenarios:
 *  - If oldV == crtV, (i.e. VE old version is the same as the current version)
 *     then nobody else has modified the object since the backend read it.
 *     We simply update the entry with the new version in that case:
 *       entry = (key, crtV) --> entry = (key, newV)
 *
 *  - If oldV < crtV, (i.e. VE old version is different than the current version)
 *     some other backend must have modified the object in the meantime.
 *    We generate an entirely new version new_newV for the object to reflect
 *     the new "combined" object.
 *
 *    The cached entry is updated directly with the new version:
 *        entry = (key, crtV) --> entry = (key, new_newV)
 *
 *    The versioning event in the queue is updated directly:
         VE = (key, oldV, newV)  --> VE = (key, crtV, new_newV)
 *
 *  event: The event containing the versioning information for an update
 *  cached_entry: The existing entry for this object in the Global MDVSN
 *
 * This function is called while the MDVerWriteLock is held in exclusive
 * mode. Don't do anything that is not allowed while holding a LWLock
 * (e.g. allocate memory, or call unsafe functions).
 *
 */
static void
mdver_globalhandler_reconcile(mdver_event *event, CacheEntry *cached_entry)
{

	/* Found existing entry, reconcile and update the version */
	mdver_entry *cached_mdver_entry = CACHE_ENTRY_PAYLOAD(cached_entry);

#ifdef MD_VERSIONING_INSTRUMENTATION
	elog(gp_mdversioning_loglevel, "Updating GlobalMDVSN entry %d: Current (%d,%d). Event: [(%d,%d)->(%d,%d)]",
			event->key,
			(int) cached_mdver_entry->ddl_version, (int) cached_mdver_entry->dml_version,
			(int) event->old_ddl_version, (int) event->old_dml_version,
			(int) event->new_ddl_version, (int) event->new_dml_version);
#endif

	/*
	 * Reconcile and resolve conflicts for incoming versioning events.
	 *  When a new versioning event is received at the Global MDVSN,
	 *  look up if the same object has a conflicting version.
	 * If so, resolve conflict by generating a new version.
	 */

	uint64 new_ddl_version = event->new_ddl_version;
	uint64 new_dml_version = event->new_dml_version;
	bool conflict = false;

	/*
	 * It is safe to read the cached_mdver_entry contents, since
	 * we're holding the write lock on the Global MDVSN cache.
	 */
	if (cached_mdver_entry->ddl_version != event->old_ddl_version)
	{
		new_ddl_version = mdver_next_global_version();
		conflict = true;
	}

	if (cached_mdver_entry->dml_version != event->old_dml_version)
	{
		new_dml_version = mdver_next_global_version();
		conflict = true;
	}

	if (conflict)
	{

#ifdef MD_VERSIONING_INSTRUMENTATION
		elog(gp_mdversioning_loglevel, "Updating event in the queue (pid=%d, oid=%d): Old event: [(%d,%d)->(%d,%d)]. Modified event: [(%d,%d)->(%d,%d)]",
				event->backend_pid,
				event->key,
				/* Old event */
				(int) event->old_ddl_version, (int) event->old_dml_version,
				(int) event->new_ddl_version, (int) event->new_dml_version,
				/* New event */
				(int) cached_mdver_entry->ddl_version, (int) cached_mdver_entry->dml_version,
				(int) new_ddl_version, (int) new_dml_version);
#endif

		/*
		 * A new version for this object is being generated here.
		 * We're going to directly update the event in the queue with the new
		 * version.
		 */

		event->new_ddl_version = new_ddl_version;
		event->new_dml_version = new_dml_version;

		/*
		 * We're also updating the VE old version to reflect the current
		 * visible global version
		 */
		event->old_ddl_version = cached_mdver_entry->ddl_version;
		event->old_dml_version = cached_mdver_entry->dml_version;
	}

	/* About to update the cached entry. Lock entry to make update atomic */
	Cache *glob_mdvsn = mdver_get_glob_mdvsn();
	Cache_LockEntry(glob_mdvsn, cached_entry);

	cached_mdver_entry->ddl_version = new_ddl_version;
	cached_mdver_entry->dml_version = new_dml_version;

	Cache_UnlockEntry(glob_mdvsn, cached_entry);

}

