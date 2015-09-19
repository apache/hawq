/*-------------------------------------------------------------------------
 *
 * mdver_local_handler.c
 *	 Implementation of Local VE Handler for metadata versioning
 *
 * Copyright (c) 2014, Pivotal, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/mdver.h"
#include "utils/guc.h"
#include "storage/sinval.h"

static void mdver_localhandler_reconcile(mdver_event *event, mdver_entry *local_entry);

/*
 * Entry point for the Local Versioning Event Handler. This gets called
 * for every message that is executed locally at a backend.
 */
extern void
mdver_localhandler_new_event(SharedInvalidationMessage *msg)
{
	Assert(NULL != msg);
	Assert(SHAREDVERSIONINGMSG_ID == msg->id);

#ifdef MD_VERSIONING_INSTRUMENTATION
	char *mdev_str = mdver_event_str(&msg->ve.verEvent);
	ereport(gp_mdversioning_loglevel,
			(errmsg("LocalExecuteVE: got %s event %s",
					msg->ve.local ? "LOCAL" : "REMOTE",
					mdev_str),
					errprintstack(false)));
	pfree(mdev_str);
#endif

	/*
	 * There are some cases where we don't have a transInvalInfo structure,
	 * and thus we don't have a Local MDVSN. For example:
	 *  - an auxiliary process (fts prober comes to mind) that queries
	 *    catalog tables directly using heap functions (no transaction)
	 *  - updating persistent tables during transaction commit
	 *    (transInvalInfo has already been reset).
	 *  - bootstrap
	 *
	 * In other cases, we simply don't have a Local MDVSN since we don't
	 * cache versions:
	 *  - a QE process running on the master or segments will have a
	 *    syscache, but not a Metadata Version cache
	 *
	 * In those cases we don't care about versioning, so skip adding
	 * to local MDVSN.
	 */

	mdver_local_mdvsn *local_mdvsn = GetCurrentLocalMDVSN();
	if (NULL != local_mdvsn)
	{

		mdver_event *event = &msg->ve.verEvent;

		if (mdver_is_nuke_event(event))
		{
			elog(gp_mdversioning_loglevel, "Local VE Handler: Received NUKE event");
			mdver_local_mdvsn_nuke(local_mdvsn);
			return;
		}

		if (msg->ve.local)
		{
			/*
			 * Locally generated event, we must add or update the version
			 * in the Local MDVSN.
			 */
			mdver_entry entry;
			entry.key = event->key;

			/* FIXME gcaragea 7/4/2014: Can we assert anything here? */
			entry.ddl_version = event->new_ddl_version;
			entry.dml_version = event->new_dml_version;

			mdver_local_mdvsn_add(local_mdvsn, &entry, msg->ve.local);
		}
		else
		{
			/*
			 * An event coming from the global queue (GVQ)
			 * If we are interested in this object, add / update
			 * version in Local MDVSN.
			 *
			 */

			mdver_entry *local_entry = mdver_local_mdvsn_find(local_mdvsn, event->key);
			if (NULL != local_entry)
			{

				/*
				 * A VE came from SVQ for a key that we already have locally.
				 * Need to reconcile and record.
				 */
				mdver_localhandler_reconcile(event, local_entry);
			}
			else
			{
				elog(gp_mdversioning_loglevel, "Local VE Handler: Ignoring remote event for object not of interest key=%d", event->key);
			}

			/* TODO gcaragea 5/27/2014: For subtransactions, keep all messages (MPP-22935) */
		}

	}
}

/*
 * Reconcile and resolve conflicts for incoming versioning events.
 *
 * This function handles both events generated locally and remotely by other
 * backends.
 *
 * When a new versioning event is received at the Local MDVSN,
 * look up if the same object has a conflicting version locally.
 *
 * If a conflict is detected, resolve by generating a new version for
 * the "combined" object. We don't generate a new VE for the conflict.
 * This means the combined object version is only visible to the
 * current backend.
 *
 *   event: The new event to be considered
 *   local_entry: The conflicting entry in the Local MDVSN cache
 */
static void
mdver_localhandler_reconcile(mdver_event *event, mdver_entry *local_entry)
{

#ifdef MD_VERSIONING_INSTRUMENTATION
	char *mdev_str = mdver_event_str(event);
	elog(gp_mdversioning_loglevel, "Local VE Handler: Reconcile: Local entry = %d: (%d, %d). Incoming event %s",
			local_entry->key,
			(int) local_entry->ddl_version,
			(int) local_entry->dml_version,
			mdev_str);
	pfree(mdev_str);
#endif

	uint64 new_ddl_version = event->new_ddl_version;
	uint64 new_dml_version = event->new_dml_version;
	bool conflict = false;

	if (local_entry->ddl_version != event->old_ddl_version)
	{
		new_ddl_version = mdver_next_global_version();
		conflict = true;
	}

	if (local_entry->dml_version != event->old_dml_version)
	{
		new_dml_version = mdver_next_global_version();
		conflict = true;
	}

#if MD_VERSIONING_INSTRUMENTATION
	if (conflict)
	{
		elog(gp_mdversioning_loglevel, "Local VE Handler: Conflict resolved. New"
				"version generated, updated local entry to %d : (%d,%d) -> (%d, %d)",
				local_entry->key,
				(int) local_entry->ddl_version, (int) local_entry->dml_version,
				(int) new_ddl_version, (int) new_dml_version);

	}
	else
	{
		elog(gp_mdversioning_loglevel, "Local VE Handler: No conflict. Update local entry to %d : (%d,%d) -> (%d, %d)",
				local_entry->key,
				(int) event->old_ddl_version, (int) event->old_dml_version,
				(int) event->new_ddl_version, (int) event->new_dml_version);
	}
#endif

	/* Update local entry with the resolved versions */
	local_entry->ddl_version = new_ddl_version;
	local_entry->dml_version = new_dml_version;
}
