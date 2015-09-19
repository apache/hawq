/*-------------------------------------------------------------------------
 *
 * mdver_dep_translator.c
 *	 Implementation of Dependency Translator (DT) for metadata versioning
 *
 * Copyright (c) 2014, Pivotal, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/catalog.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "storage/sinval.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/mdver.h"

/* Catalog table definitions */
#include "catalog/pg_constraint.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_aggregate.h"

/* We use Oid = InvalidOid to signal that it's a NUKE event */
#define MDVER_NUKE_KEY InvalidOid

static void mdver_dt_add_cache_events(List **events);
static mdver_event *mdver_new_nuke_event(void);

/*
 * Generate all the invalidation messages caused by updating a tuple in a catalog table
 * 	relation: The catalog table being touched
 * 	tuple: The affected tuple
 * 	action: The action performed on the tuple
 */
void
mdver_dt_catcache_inval(Relation relation, HeapTuple tuple, SysCacheInvalidateAction action)
{
	if (!mdver_enabled())
	{
		return;
	}

	List *events = NIL;

	/* For milestone 0, we generate NUKE events for any DDL.
	 * Well, not really, there's no need to generate NUKE events for INSERTS and DELETE
	 * in catalog tables, since those do not have dependencies that need to be tracked.
	 *
	 * TODO gcaragea 01/14/2015: Remove this code for milestone 1 (simple dependecies)
	 */

	if (IsAoSegmentRelation(relation))
	{
		/*
		 * We don't create versioning messages for catalog tables in the AOSEG
		 * namespace. These are modified for DML only (not DDL)
		 *
		 * TODO gcaragea 01/14/2015: Remove this once we have DML versioning
		 */
		return;
	}

	switch (action)
	{

	case SysCacheInvalidate_Update_OldTup:
		/* We ignore the first event from the update. We only process the second one below */
		break;
	case SysCacheInvalidate_Insert:
		/* fall through */
	case SysCacheInvalidate_Delete:
		/* fall through */
	case SysCacheInvalidate_Update_NewTup:
		/* fall through */
	case SysCacheInvalidate_Update_InPlace:
		/* fall through */
	case SysCacheInvalidate_VacuumMove:
		events = lappend(events, mdver_new_nuke_event());
		break;
	default:
		insist_log(false, "Unkown syscache invalidation operation");

	}

	/* If we generated any events, add them to the event message list */
	if (NIL != events)
	{
		mdver_dt_add_cache_events(&events);
	}
}

/*
 * Iterate through a list of versioning events and add them all to the
 * message queue. The list of events is deep-freed after events are added,
 * since we shouldn't need the actual events anymore.
 *
 * 	events: Pointer to list of events to be added to the CVQ
 */
static void
mdver_dt_add_cache_events(List **events)
{
	Assert(NIL != *events);

	ListCell *lc = NULL;
	foreach (lc, *events)
	{
		mdver_event *new_event = (mdver_event *) lfirst(lc);
		CacheAddVersioningEvent(new_event);
	}

	/* Deep free the list of events. This also calls pfree() on all the events */
	list_free_deep(*events);
	*events = NIL;
}

/*
 * Returns true if the event given is a NUKE event
 */
bool
mdver_is_nuke_event(const mdver_event *event)
{
	Assert(NULL != event);

	return (event->key == MDVER_NUKE_KEY);
}

/*
 * Creates and returns a new NUKE event.
 *
 * The event is palloc-ed in the current memory context, and the
 * caller is responsible for pfree-ing it.
 */
static mdver_event *
mdver_new_nuke_event(void)
{
	mdver_event *event = (mdver_event *) palloc0(sizeof(mdver_event));
	event->key = MDVER_NUKE_KEY;

#ifdef MD_VERSIONING_INSTRUMENTATION
	ereport(gp_mdversioning_loglevel,
					(errmsg("Generating NUKE event"),
							errprintstack(true)));
#endif

	return event;
}
