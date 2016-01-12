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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * portalmem.c
 *	  backend portal memory management
 *
 * Portals are objects representing the execution state of a query.
 * This module provides memory management services for portals, but it
 * doesn't actually run the executor for them.
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/utils/mmgr/portalmem.c,v 1.97.2.1 2007/04/26 23:24:57 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/variable.h"			/* show_session_authorization */
#include "commands/dbcommands.h" 		/* get_database_name */
#include "commands/portalcmds.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"            /* EState */
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/resscheduler.h"
#include "utils/tuplestore.h"           /* tuplestore_begin_heap, etc */

#include "cdb/cdbdisp.h"                /* CdbShutdownPortals, CdbCheckDispatchResult */
#include "cdb/cdbvars.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbfilesystemcredential.h"
#include "cdb/ml_ipc.h"
#include "cdb/dispatcher.h"


/*
 * Estimate of the maximum number of open portals a user would have,
 * used in initially sizing the PortalHashTable in EnablePortalManager().
 * Since the hash table can expand, there's no need to make this overly
 * generous, and keeping it small avoids unnecessary overhead in the
 * hash_seq_search() calls executed during transaction end.
 */
#define PORTALS_PER_USER	   16


/* ----------------
 *		Global state
 * ----------------
 */

#define MAX_PORTALNAME_LEN		NAMEDATALEN

typedef struct portalhashent
{
	char		portalname[MAX_PORTALNAME_LEN];
	Portal		portal;
} PortalHashEnt;

static HTAB *PortalHashTable = NULL;

#define PortalHashTableLookup(NAME, PORTAL) \
do { \
	PortalHashEnt *hentry; \
	\
	hentry = (PortalHashEnt *) hash_search(PortalHashTable, \
										   (NAME), HASH_FIND, NULL); \
	if (hentry) \
		PORTAL = hentry->portal; \
	else \
		PORTAL = NULL; \
} while(0)

#define PortalHashTableInsert(PORTAL, NAME) \
do { \
	PortalHashEnt *hentry; bool found; \
	\
	hentry = (PortalHashEnt *) hash_search(PortalHashTable, \
										   (NAME), HASH_ENTER, &found); \
	if (found) \
		elog(ERROR, "duplicate portal name"); \
	hentry->portal = PORTAL; \
	/* To avoid duplicate storage, make PORTAL->name point to htab entry */ \
	PORTAL->name = hentry->portalname; \
} while(0)

#define PortalHashTableDelete(PORTAL) \
do { \
	PortalHashEnt *hentry; \
	\
	hentry = (PortalHashEnt *) hash_search(PortalHashTable, \
										   PORTAL->name, HASH_REMOVE, NULL); \
	if (hentry == NULL) \
		elog(WARNING, "trying to delete portal name that does not exist"); \
} while(0)

static MemoryContext PortalMemory = NULL;


/* ----------------------------------------------------------------
 *				   public portal interface functions
 * ----------------------------------------------------------------
 */

/*
 * EnablePortalManager
 *		Enables the portal management module at backend startup.
 */
void
EnablePortalManager(void)
{
	HASHCTL		ctl;

	Assert(PortalMemory == NULL);

	PortalMemory = AllocSetContextCreate(TopMemoryContext,
										 "PortalMemory",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	ctl.keysize = MAX_PORTALNAME_LEN;
	ctl.entrysize = sizeof(PortalHashEnt);

	/*
	 * use PORTALS_PER_USER as a guess of how many hash table entries to
	 * create, initially
	 */
	PortalHashTable = hash_create("Portal hash", PORTALS_PER_USER,
								  &ctl, HASH_ELEM);
}

/*
 * GetPortalByName
 *		Returns a portal given a portal name, or NULL if name not found.
 */
Portal
GetPortalByName(const char *name)
{
	Portal		portal;

	if (PointerIsValid(name))
		PortalHashTableLookup(name, portal);
	else
		portal = NULL;

	return portal;
}

/*
 * PortalListGetPrimaryStmt
 *		Get the "primary" stmt within a portal, ie, the one marked canSetTag.
 *
 * Returns NULL if no such stmt.  If multiple PlannedStmt structs within the
 * portal are marked canSetTag, returns the first one.	Neither of these
 * cases should occur in present usages of this function.
 *
 * Copes if given a list of Querys --- can't happen in a portal, but may
 * occur elsewhere, e.g., PreparedStatement handling or, eventually,
 * plan caching.
 *
 * For use with a portal, use PortalGetPrimaryStmt rather than calling this 
 * directly.
 */
Node *
PortalListGetPrimaryStmt(List *stmts)
{
	ListCell   *lc;

	foreach(lc, stmts)
	{
		Node	   *stmt = (Node *) lfirst(lc);

		if (IsA(stmt, PlannedStmt))
		{
			if (((PlannedStmt *)stmt)->canSetTag)
				return stmt;
		}
		else if (IsA(stmt, Query))
		{
			if (((Query *) stmt)->canSetTag)
				return stmt;
		}
		else
		{
			/* Utility stmts are assumed canSetTag if they're the only stmt */
			if (list_length(stmts) == 1)
				return stmt;
		}
	}
	return NULL;
}

/*
 * CreatePortal
 *		Returns a new portal given a name.
 *
 * allowDup: if true, automatically drop any pre-existing portal of the
 * same name (if false, an error is raised).
 *
 * dupSilent: if true, don't even emit a WARNING.
 */
Portal
CreatePortal(const char *name, bool allowDup, bool dupSilent)
{
	Portal		portal;

	AssertArg(PointerIsValid(name));

	portal = GetPortalByName(name);
	if (PortalIsValid(portal))
	{
		if (!allowDup)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_CURSOR),
					 errmsg("cursor \"%s\" already exists", name)));
		if (!dupSilent)
			if (Gp_role != GP_ROLE_EXECUTE)
			ereport(WARNING,
					(errcode(ERRCODE_DUPLICATE_CURSOR),
					 errmsg("closing existing cursor \"%s\"",
							name)));
		PortalDrop(portal, false);
	}

	/* make new portal structure */
	portal = (Portal) MemoryContextAllocZero(PortalMemory, sizeof *portal);

	/* initialize portal heap context; typically it won't store much */
	portal->heap = AllocSetContextCreate(PortalMemory,
										 "PortalHeapMemory",
										 ALLOCSET_SMALL_MINSIZE,
										 ALLOCSET_SMALL_INITSIZE,
										 ALLOCSET_SMALL_MAXSIZE);

	/* create a resource owner for the portal */
	portal->resowner = ResourceOwnerCreate(CurTransactionResourceOwner,
										   "Portal");

	/* initialize portal fields that don't start off zero */
	PortalSetStatus(portal, PORTAL_NEW); 
	portal->cleanup = PortalCleanup;
	portal->createSubid = GetCurrentSubTransactionId();
	portal->strategy = PORTAL_MULTI_QUERY;
	portal->cursorOptions = CURSOR_OPT_NO_SCROLL;
	portal->atStart = true;
	portal->atEnd = true;		/* disallow fetches until query is set */
	portal->visible = true;
	portal->creation_time = GetCurrentStatementStartTimestamp();

	portal->is_extended_query = false; /* default value */	

	portal->filesystem_credentials = NULL;
	portal->filesystem_credentials_memory = NULL;

	/* put portal in table (sets portal->name) */
	PortalHashTableInsert(portal, name);

	/* Setup gpmon. Siva - should this be moved elsewhere? */
	gpmon_init();

	/* End Gpmon */

	return portal;
}

/*
 * CreateNewPortal
 *		Create a new portal, assigning it a random nonconflicting name.
 */
Portal
CreateNewPortal(void)
{
	static unsigned int unnamed_portal_count = 0;

	char		portalname[MAX_PORTALNAME_LEN];

	/* Select a nonconflicting name */
	for (;;)
	{
		unnamed_portal_count++;
		sprintf(portalname, "<unnamed portal %u>", unnamed_portal_count);
		if (GetPortalByName(portalname) == NULL)
			break;
	}

	return CreatePortal(portalname, false, false);
}

/*
 * PortalDefineQuery
 *		A simple subroutine to establish a portal's query.
 *
 * Notes: Caller MUST supply a sourceText string; it is no longer okay
 * to pass NULL.  (If you really don't have source text, pass a constant 
 * string, e.g., "(query not available)".) 
 *
 * commandTag shall be NULL if and only if the original query string
 * (before rewriting) was an empty string.	Also, commandTag must be a
 * pointer to a constant string, since it is not copied.  
 *
 * The caller is responsible for ensuring that the passed prepStmtName
 * (if not NULL) and sourceText have adequate lifetime. Also, queryContext 
 * must accurately  describe the location of the stmt list contents.
 */
void
PortalDefineQuery(Portal portal,
				  const char *prepStmtName,
				  const char *sourceText,
				  NodeTag	  sourceTag,
				  const char *commandTag,
				  List *stmts,
				  MemoryContext queryContext)
{
	AssertArg(PortalIsValid(portal));
	AssertState(portal->queryContext == NULL);	/* else defined already */
	
	AssertArg(sourceText != NULL);
	AssertArg(commandTag != NULL || stmts == NIL);
	
	portal->prepStmtName = prepStmtName;
	portal->sourceText = sourceText;
	portal->sourceTag = sourceTag;
	portal->commandTag = commandTag;
	portal->stmts = stmts;
	portal->queryContext = queryContext;
}

/*
 * PortalCreateHoldStore
 *		Create the tuplestore for a portal.
 */
void
PortalCreateHoldStore(Portal portal)
{
	MemoryContext oldcxt;

	Assert(portal->holdContext == NULL);
	Assert(portal->holdStore == NULL);

	/*
	 * Create the memory context that is used for storage of the tuple set.
	 * Note this is NOT a child of the portal's heap memory.
	 */
	portal->holdContext =
		AllocSetContextCreate(PortalMemory,
							  "PortalHoldContext",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);

	/* Create the tuple store, selecting cross-transaction temp files. */
	oldcxt = MemoryContextSwitchTo(portal->holdContext);

	/* XXX: Should maintenance_work_mem be used for the portal size? */
	portal->holdStore = tuplestore_begin_heap(true, true, work_mem); 

	MemoryContextSwitchTo(oldcxt);
}

/*
 * PortalDrop
 *		Destroy the portal.
 */
void
PortalDrop(Portal portal, bool isTopCommit)
{
	AssertArg(PortalIsValid(portal));

	/* Not sure if this case can validly happen or not... */
	if (PortalGetStatus(portal) == PORTAL_ACTIVE) 
		elog(ERROR, "cannot drop active or queued portal");

	TeardownSequenceServer();

	/*
	 * Remove portal from hash table.  Because we do this first, we will not
	 * come back to try to remove the portal again if there's any error in the
	 * subsequent steps.  Better to leak a little memory than to get into an
	 * infinite error-recovery loop.
	 */
	PortalHashTableDelete(portal);

	/* let portalcmds.c clean up the state it knows about */
	if (portal->cleanup)
		(*portal->cleanup) (portal);

	/*
	 * Cancel all file system credentials, and close files and file system
	 * handlers if necessary.  This has to be *after* the portal cleanup,
	 * which guarantees QE has sent ReadyForQuery.  Otherwise, QE might still
	 * be attempting to connect NameNode, which would fail to find
	 * credential token.
	 */
	cleanup_filesystem_credentials(portal);

	/*
	 * Release any resources still attached to the portal.	There are several
	 * cases being covered here:
	 *
	 * Top transaction commit (indicated by isTopCommit): normally we should
	 * do nothing here and let the regular end-of-transaction resource
	 * releasing mechanism handle these resources too.	However, if we have a
	 * FAILED portal (eg, a cursor that got an error), we'd better clean up
	 * its resources to avoid resource-leakage warning messages.
	 *
	 * Sub transaction commit: never comes here at all, since we don't kill
	 * any portals in AtSubCommit_Portals().
	 *
	 * Main or sub transaction abort: we will do nothing here because
	 * portal->resowner was already set NULL; the resources were already
	 * cleaned up in transaction abort.
	 *
	 * Ordinary portal drop: must release resources.  However, if the portal
	 * is not FAILED then we do not release its locks.	The locks become the
	 * responsibility of the transaction's ResourceOwner (since it is the
	 * parent of the portal's owner) and will be released when the transaction
	 * eventually ends.
	 */
	if (portal->resowner &&
		(!isTopCommit || PortalGetStatus(portal) == PORTAL_FAILED))
	{
		bool		isCommit = (PortalGetStatus(portal) != PORTAL_FAILED);

		ResourceOwnerRelease(portal->resowner,
							 RESOURCE_RELEASE_BEFORE_LOCKS,
							 isCommit, false);
		ResourceOwnerRelease(portal->resowner,
							 RESOURCE_RELEASE_LOCKS,
							 isCommit, false);
		ResourceOwnerRelease(portal->resowner,
							 RESOURCE_RELEASE_AFTER_LOCKS,
							 isCommit, false);
		ResourceOwnerDelete(portal->resowner);
	}
	portal->resowner = NULL;

	/*
	 * Delete tuplestore if present.  We should do this even under error
	 * conditions; since the tuplestore would have been using cross-
	 * transaction storage, its temp files need to be explicitly deleted.
	 */
	if (portal->holdStore)
	{
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(portal->holdContext);
		tuplestore_end(portal->holdStore); 
		MemoryContextSwitchTo(oldcontext);
		portal->holdStore = NULL;
	}
	
	/* delete tuplestore storage, if any */
	if (portal->holdContext)
		MemoryContextDelete(portal->holdContext);

	/* release subsidiary storage */
	MemoryContextDelete(PortalGetHeapMemory(portal));

	/* release portal struct (it's in PortalMemory) */
	pfree(portal);
}

/*
 * DropDependentPortals
 *		Drop any portals using the specified context as queryContext.
 *
 * This is normally used to make sure we can safely drop a prepared statement.
 */
void
DropDependentPortals(MemoryContext queryContext)
{
	HASH_SEQ_STATUS status;
	PortalHashEnt *hentry;

	hash_seq_init(&status, PortalHashTable);

	while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL)
	{
		Portal		portal = hentry->portal;

		if (portal->queryContext == queryContext)
			PortalDrop(portal, false);
	}
}


/*
 * Pre-commit processing for portals.
 *
 * Any holdable cursors created in this transaction need to be converted to
 * materialized form, since we are going to close down the executor and
 * release locks.  Other portals are not touched yet.
 *
 * Returns TRUE if any holdable cursors were processed, FALSE if not.
 */
bool
CommitHoldablePortals(void)
{
	bool		result = false;
	HASH_SEQ_STATUS status;
	PortalHashEnt *hentry;

	hash_seq_init(&status, PortalHashTable);

	while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL)
	{
		Portal		portal = hentry->portal;

		/* Is it a holdable portal created in the current xact? */
		if ((portal->cursorOptions & CURSOR_OPT_HOLD) &&
			portal->createSubid != InvalidSubTransactionId &&
			PortalGetStatus(portal) == PORTAL_READY)
		{
			/*
			 * We are exiting the transaction that created a holdable cursor.
			 * Instead of dropping the portal, prepare it for access by later
			 * transactions.
			 *
			 * Note that PersistHoldablePortal() must release all resources
			 * used by the portal that are local to the creating transaction.
			 */
			PortalCreateHoldStore(portal);
			PersistHoldablePortal(portal);

			/*
			 * Any resources belonging to the portal will be released in the
			 * upcoming transaction-wide cleanup; the portal will no longer
			 * have its own resources.
			 */
			portal->resowner = NULL;

			/*
			 * Having successfully exported the holdable cursor, mark it as
			 * not belonging to this transaction.
			 */
			portal->createSubid = InvalidSubTransactionId;

			result = true;
		}
	}

	return result;
}

/*
 * Pre-prepare processing for portals.
 *
 * Currently we refuse PREPARE if the transaction created any holdable
 * cursors, since it's quite unclear what to do with one.  However, this
 * has the same API as CommitHoldablePortals and is invoked in the same
 * way by xact.c, so that we can easily do something reasonable if anyone
 * comes up with something reasonable to do.
 *
 * Returns TRUE if any holdable cursors were processed, FALSE if not.
 */
bool
PrepareHoldablePortals(void)
{
	bool		result = false;
	HASH_SEQ_STATUS status;
	PortalHashEnt *hentry;

	hash_seq_init(&status, PortalHashTable);

	while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL)
	{
		Portal		portal = hentry->portal;

		/* Is it a holdable portal created in the current xact? */
		if ((portal->cursorOptions & CURSOR_OPT_HOLD) &&
			portal->createSubid != InvalidSubTransactionId &&
			PortalGetStatus(portal) == PORTAL_READY)
		{
			/*
			 * We are exiting the transaction that created a holdable cursor.
			 * Can't do PREPARE.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot PREPARE a transaction that has created a cursor WITH HOLD")));
		}
	}

	return result;
}

/*
 * Pre-commit processing for portals.
 *
 * Remove all non-holdable portals created in this transaction.
 * Portals remaining from prior transactions should be left untouched.
 */
void
AtCommit_Portals(void)
{
	HASH_SEQ_STATUS status;
	PortalHashEnt *hentry;

	hash_seq_init(&status, PortalHashTable);

	while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL)
	{
		Portal		portal = hentry->portal;

		/*
		 * Do not touch active portals --- this can only happen in the case of
		 * a multi-transaction utility command, such as VACUUM.
		 *
		 * Note however that any resource owner attached to such a portal is
		 * still going to go away, so don't leave a dangling pointer.
		 */
		if (PortalGetStatus(portal) == PORTAL_ACTIVE)
		{
			portal->resowner = NULL;
			continue;
		}

		/*
		 * Do nothing to cursors held over from a previous transaction
		 * (including holdable ones just frozen by CommitHoldablePortals).
		 */
		if (portal->createSubid == InvalidSubTransactionId)
			continue;

		/* Zap all non-holdable portals */
		PortalDrop(portal, true);

		/* Restart the iteration in case that led to other drops */
		/* XXX is this really necessary? */
		hash_seq_term(&status);
		hash_seq_init(&status, PortalHashTable);
	}
}

/*
 * Abort processing for portals.
 *
 * At this point we reset "active" status and run the cleanup hook if
 * present, but we can't release the portal's memory until the cleanup call.
 *
 * The reason we need to reset active is so that we can replace the unnamed
 * portal, else we'll fail to execute ROLLBACK when it arrives.
 */
void
AtAbort_Portals(void)
{
	HASH_SEQ_STATUS status;
	PortalHashEnt *hentry;

	hash_seq_init(&status, PortalHashTable);

	while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL)
	{
		Portal		portal = hentry->portal;

		if (PortalGetStatus(portal) == PORTAL_ACTIVE)
			PortalSetStatus(portal, PORTAL_FAILED);

		/*
		 * Do nothing else to cursors held over from a previous transaction.
		 */
		if (portal->createSubid == InvalidSubTransactionId)
			continue;

		/* let portalcmds.c clean up the state it knows about */
		if (portal->cleanup)
		{
			(*portal->cleanup) (portal);
			portal->cleanup = NULL;
		}

		/*
		 * Any resources belonging to the portal will be released in the
		 * upcoming transaction-wide cleanup; they will be gone before we run
		 * PortalDrop.
		 */
		portal->resowner = NULL;

		/*
		 * Although we can't delete the portal data structure proper, we can
		 * release any memory in subsidiary contexts, such as executor state.
		 * The cleanup hook was the last thing that might have needed data
		 * there.
		 */
		MemoryContextDeleteChildren(PortalGetHeapMemory(portal));
	}
}

/*
 * Post-abort cleanup for portals.
 *
 * Delete all portals not held over from prior transactions.  */
void
AtCleanup_Portals(void)
{
	HASH_SEQ_STATUS status;
	PortalHashEnt *hentry;

	hash_seq_init(&status, PortalHashTable);

	while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL)
	{
		Portal		portal = hentry->portal;

		/* Do nothing to cursors held over from a previous transaction */
		if (portal->createSubid == InvalidSubTransactionId)
		{
			Assert(PortalGetStatus(portal) != PORTAL_ACTIVE);
			Assert(portal->resowner == NULL);
			continue;
		}

		/* Else zap it. */
		PortalDrop(portal, false);
	}
}

/*
 * Pre-subcommit processing for portals.
 *
 * Reassign the portals created in the current subtransaction to the parent
 * subtransaction.
 */
void
AtSubCommit_Portals(SubTransactionId mySubid,
					SubTransactionId parentSubid,
					ResourceOwner parentXactOwner)
{
	HASH_SEQ_STATUS status;
	PortalHashEnt *hentry;

	hash_seq_init(&status, PortalHashTable);

	while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL)
	{
		Portal		portal = hentry->portal;

		if (portal->createSubid == mySubid)
		{
			portal->createSubid = parentSubid;
			if (portal->resowner)
				ResourceOwnerNewParent(portal->resowner, parentXactOwner);
		}
	}
}

/*
 * Subtransaction abort handling for portals.
 *
 * Deactivate portals created during the failed subtransaction.
 * Note that per AtSubCommit_Portals, this will catch portals created
 * in descendants of the subtransaction too.
 *
 * We don't destroy any portals here; that's done in AtSubCleanup_Portals.
 */
void
AtSubAbort_Portals(SubTransactionId mySubid,
				   SubTransactionId parentSubid,
				   ResourceOwner parentXactOwner)
{
	HASH_SEQ_STATUS status;
	PortalHashEnt *hentry;

    UnusedArg(parentSubid);
    UnusedArg(parentXactOwner);

	hash_seq_init(&status, PortalHashTable);

	while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL)
	{
		Portal		portal = hentry->portal;

		if (portal->createSubid != mySubid)
			continue;

		/*
		 * Force any active portals of my own transaction into FAILED state.
		 * This is mostly to ensure that a portal running a FETCH will go
		 * FAILED if the underlying cursor fails.  (Note we do NOT want to do
		 * this to upper-level portals, since they may be able to continue.)
		 *
		 * This is only needed to dodge the sanity check in PortalDrop.
		 */
		if (PortalGetStatus(portal) == PORTAL_ACTIVE)
			PortalSetStatus(portal, PORTAL_FAILED);

		/*
		 * If the portal is READY then allow it to survive into the parent
		 * transaction; otherwise shut it down.
		 *
		 * Currently, we can't actually support that because the portal's
		 * query might refer to objects created or changed in the failed
		 * subtransaction, leading to crashes if execution is resumed. So,
		 * even READY portals are deleted.	It would be nice to detect whether
		 * the query actually depends on any such object, instead.
		 */
#ifdef NOT_USED
		if (portal->status == PORTAL_READY)
		{
			portal->createSubid = parentSubid;
			if (portal->resowner)
				ResourceOwnerNewParent(portal->resowner, parentXactOwner);
		}
		else
#endif

			/* let portalcmds.c clean up the state it knows about */
			if (portal->cleanup)
			{
				(*portal->cleanup) (portal);
				portal->cleanup = NULL;
			}

		/*
		 * Any resources belonging to the portal will be released in the
		 * upcoming transaction-wide cleanup; they will be gone before we
		 * run PortalDrop.
		 */
		portal->resowner = NULL;

		/*
		 * Although we can't delete the portal data structure proper, we
		 * can release any memory in subsidiary contexts, such as executor
		 * state.  The cleanup hook was the last thing that might have
		 * needed data there.
		 */
		MemoryContextDeleteChildren(PortalGetHeapMemory(portal));
	}
}

/*
 * Post-subabort cleanup for portals.
 *
 * Drop all portals created in the failed subtransaction (but note that
 * we will not drop any that were reassigned to the parent above).
 */
void
AtSubCleanup_Portals(SubTransactionId mySubid)
{
	HASH_SEQ_STATUS status;
	PortalHashEnt *hentry;

	hash_seq_init(&status, PortalHashTable);

	while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL)
	{
		Portal		portal = hentry->portal;

		if (portal->createSubid != mySubid)
			continue;

		/* Zap it. */
		PortalDrop(portal, false);
	}
}

/* Find all available cursors */
Datum
pg_cursor(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS hash_seq;
	PortalHashEnt *hentry;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* need to build tuplestore in query context */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/*
	 * build tupdesc for result tuples. This must match the definition of
	 * the pg_cursors view in system_views.sql
	 */
	tupdesc = CreateTemplateTupleDesc(6, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "statement",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "is_holdable",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "is_binary",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "is_scrollable",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "creation_time",
					   TIMESTAMPTZOID, -1, 0);

	/*
	 * We put all the tuples into a tuplestore in one scan of the hashtable.
	 * This avoids any issue of the hashtable possibly changing between calls.
	 */
	tupstore = tuplestore_begin_heap(true, false, work_mem);

	hash_seq_init(&hash_seq, PortalHashTable);
	while ((hentry = hash_seq_search(&hash_seq)) != NULL)
	{
		Portal		portal = hentry->portal;
		HeapTuple	tuple;
		Datum		values[6];
		bool		nulls[6];

		/* report only "visible" entries */
		if (!portal->visible)
			continue;

		/* generate junk in short-term context */
		MemoryContextSwitchTo(oldcontext);

		MemSet(nulls, 0, sizeof(nulls));

		values[0] = DirectFunctionCall1(textin, CStringGetDatum((char *) portal->name));
		if (!portal->sourceText)
			nulls[1] = true;
		else
			values[1] = DirectFunctionCall1(textin,
										CStringGetDatum((char *) portal->sourceText));
		values[2] = BoolGetDatum(portal->cursorOptions & CURSOR_OPT_HOLD);
		values[3] = BoolGetDatum(portal->cursorOptions & CURSOR_OPT_BINARY);
		values[4] = BoolGetDatum(portal->cursorOptions & CURSOR_OPT_SCROLL);
		values[5] = TimestampTzGetDatum(portal->creation_time);

		tuple = heap_form_tuple(tupdesc, values, nulls);

		/* switch to appropriate context while storing the tuple */
		MemoryContextSwitchTo(per_query_ctx);
		tuplestore_puttuple(tupstore, tuple);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	MemoryContextSwitchTo(oldcontext);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	return (Datum) 0;
}

/*
 * Scan the hash table of active portals; shutdown all of their
 * threads and prepare for immediate exit. (called from die()).
 */
void
CdbShutdownPortals(void)
{
	HASH_SEQ_STATUS status;
	PortalHashEnt *hentry;

	hash_seq_init(&status, PortalHashTable);

	while ((hentry = (PortalHashEnt *) hash_seq_search(&status)) != NULL)
	{
		Portal 	portal = hentry->portal;
		EState	*es;

		if (portal->queryDesc == NULL ||
			portal->queryDesc->estate == NULL ||
			portal->queryDesc->estate->dispatch_data)
			continue;

		es = portal->queryDesc->estate;

		/* active portal, shut it down */
		if (es->dispatch_data)
		{
			dispatch_wait(es->dispatch_data);   /* cancel any QEs still running */
		}
	}
}

void PortalSetStatus(Portal p, PortalStatus s)
{
	p->portal_status = s;
}

PortalStatus
PortalGetStatus(Portal p)
{
	return p->portal_status;
}

const char *
PortalGetStatusString(Portal p)
{
	switch (p->portal_status)
	{
		case PORTAL_NEW:
			return "PORTAL_NEW";
		case PORTAL_READY:
			return "PORTAL_READY";
		case PORTAL_QUEUE:
			return "PORTAL_QUEUE";
		case PORTAL_ACTIVE:
			return "PORTAL_ACTIVE";
		case PORTAL_DONE:
			return "PORTAL_DONE";
		case PORTAL_FAILED:
			return "PORTAL_FAILED";
		case PORTAL_STATUSMAX:
		default:
			return "Unknown portal status";
	}
}
