/*-------------------------------------------------------------------------
 *
 * portal.h
 *	  POSTGRES portal definitions.
 *
 * A portal is an abstraction which represents the execution state of
 * a running or runnable query.  Portals support both SQL-level CURSORs
 * and protocol-level portals.
 *
 * Scrolling (nonsequential access) and suspension of execution are allowed
 * only for portals that contain a single SELECT-type query.  We do not want
 * to let the client suspend an update-type query partway through!	Because
 * the query rewriter does not allow arbitrary ON SELECT rewrite rules,
 * only queries that were originally update-type could produce multiple
 * plan trees; so the restriction to a single query is not a problem
 * in practice.
 *
 * For SQL cursors, we support three kinds of scroll behavior:
 *
 * (1) Neither NO SCROLL nor SCROLL was specified: to remain backward
 *	   compatible, we allow backward fetches here, unless it would
 *	   impose additional runtime overhead to do so.
 *
 * (2) NO SCROLL was specified: don't allow any backward fetches.
 *
 * (3) SCROLL was specified: allow all kinds of backward fetches, even
 *	   if we need to take a performance hit to do so.  (The planner sticks
 *	   a Materialize node atop the query plan if needed.)
 *
 * Case #1 is converted to #2 or #3 by looking at the query itself and
 * determining if scrollability can be supported without additional
 * overhead.
 *
 * Protocol-level portals have no nonsequential-fetch API and so the
 * distinction doesn't matter for them.  They are always initialized
 * to look like NO SCROLL cursors.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/utils/portal.h,v 1.71 2006/10/04 00:30:10 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PORTAL_H
#define PORTAL_H

#include "executor/execdesc.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"



struct Tuplestorestate;                 /* #include "utils/tuplestore.h" */

/*
 * We have several execution strategies for Portals, depending on what
 * query or queries are to be executed.  (Note: in all cases, a Portal
 * executes just a single source-SQL query, and thus produces just a
 * single result from the user's viewpoint.  However, the rule rewriter
 * may expand the single source query to zero or many actual queries.)
 *
 * PORTAL_ONE_SELECT: the portal contains one single SELECT query.	We run
 * the Executor incrementally as results are demanded.	This strategy also
 * supports holdable cursors (the Executor results can be dumped into a
 * tuplestore for access after transaction completion).
 *
 * PORTAL_ONE_RETURNING: the portal contains a single INSERT/UPDATE/DELETE
 * query with a RETURNING clause (plus possibly auxiliary queries added by
 * rule rewriting).  On first execution, we run the portal to completion
 * and dump the primary query's results into the portal tuplestore; the
 * results are then returned to the client as demanded.  (We can't support
 * suspension of the query partway through, because the AFTER TRIGGER code
 * can't cope, and also because we don't want to risk failing to execute
 * all the auxiliary queries.)
 *
 * PORTAL_UTIL_SELECT: the portal contains a utility statement that returns
 * a SELECT-like result (for example, EXPLAIN or SHOW).  On first execution,
 * we run the statement and dump its results into the portal tuplestore;
 * the results are then returned to the client as demanded.
 *
 * PORTAL_MULTI_QUERY: all other cases.  Here, we do not support partial
 * execution: the portal's queries will be run to completion on first call.
 */
typedef enum PortalStrategy
{
	PORTAL_ONE_SELECT,
	PORTAL_ONE_RETURNING,
	PORTAL_UTIL_SELECT,
	PORTAL_MULTI_QUERY
} PortalStrategy;

/*
 * A portal is always in one of these states.  It is possible to transit
 * from ACTIVE back to READY if the query is not run to completion;
 * otherwise we never back up in status.
 */
typedef enum PortalStatus
{
	PORTAL_NEW = 0,				/* in process of creation */
	PORTAL_READY,				/* PortalStart complete, can run it */
	PORTAL_QUEUE, 				/* portal is queued (cannot delete it) */
	PORTAL_ACTIVE,				/* portal is running (can't delete it) */
	PORTAL_DONE,				/* portal is finished (don't re-run it) */
	PORTAL_FAILED,				/* portal got error (can't re-run it) */
	PORTAL_STATUSMAX
} PortalStatus;

/*
 * Note: typedef Portal is declared in tcop/dest.h as
 *		typedef struct PortalData *Portal;
 */

typedef struct PortalData
{
	/* Bookkeeping data */
	const char *name;			/* portal's name */
	const char *prepStmtName;	/* source prepared statement (NULL if none) */
	MemoryContext heap;			/* subsidiary memory for portal */
	ResourceOwner resowner;		/* resources owned by portal */
	void		(*cleanup) (Portal portal);		/* cleanup hook */
	SubTransactionId createSubid;		/* the ID of the creating subxact */

	/*
	 * if createSubid is InvalidSubTransactionId, the portal is held over from
	 * a previous transaction
	 */

	/*
	 * if Resource Scheduling is enabled, we need to save the original
	 * statement type, keep a unique id for name portals (i.e CURSORS) and
	 * remember which queue wanted a lock on this portal.
	 */
	uint32		portalId;		/* id of this portal 0 for unnamed */
	NodeTag	    sourceTag;		/* nodetag for the original query */

	/* The query or queries the portal will execute */
	const char *sourceText;		/* text of query (as of PlannedStmt, never NULL) */
	const char *commandTag;		/* command tag for original query */
	List	   *stmts;			/* PlannedStmts and/or utility Querys */
	MemoryContext queryContext; /* where the parse trees live */

	/*
	 * Note: queryContext effectively identifies which prepared statement the
	 * portal depends on, if any.  The queryContext is *not* owned by the
	 * portal and is not to be deleted by portal destruction.  (But for a
	 * cursor it is the same as "heap", and that context is deleted by portal
	 * destruction.)  The plan trees may be in either queryContext or heap.
	 */

	ParamListInfo portalParams; /* params to pass to query */

	/* Features/options */
	PortalStrategy strategy;	/* see above */
	int			cursorOptions;	/* DECLARE CURSOR option bits */

	/* Status data */
	PortalStatus portal_status;		/* see above */

	/* If not NULL, Executor is active; call ExecutorEnd eventually: */
	QueryDesc  *queryDesc;		/* info needed for executor invocation */

	/* If portal returns tuples, this is their tupdesc: */
	TupleDesc	tupDesc;		/* descriptor for result tuples */
	/* and these are the format codes to use for the columns: */
	int16	   *formats;		/* a format code for each column */

	/*
	 * Where we store tuples for a held cursor or a PORTAL_ONE_RETURNING or
	 * PORTAL_UTIL_SELECT query.  (A cursor held past the end of its
	 * transaction no longer has any active executor state.)
	 */
	struct Tuplestorestate *holdStore; /* store for holdable cursors */
	MemoryContext holdContext;	/* memory containing holdStore */

	/*
	 * atStart, atEnd and portalPos indicate the current cursor position.
	 * portalPos is zero before the first row, N after fetching N'th row of
	 * query.  After we run off the end, portalPos = # of rows in query, and
	 * atEnd is true.  If portalPos overflows, set posOverflow (this causes us
	 * to stop relying on its value for navigation).  Note that atStart
	 * implies portalPos == 0, but not the reverse (portalPos could have
	 * overflowed).
	 */
	bool		atStart;
	bool		atEnd;
	bool		posOverflow;
	int64		portalPos;

	/* Presentation data, primarily used by the pg_cursors system view */
	TimestampTz creation_time;	/* time at which this portal was defined */
	bool		visible;		/* include this portal in pg_cursors? */
	
	/* MPP: is this portal a CURSOR, or protocol level portal? */	
	bool		is_extended_query; /* simple or extended query protocol? */
	bool		is_simply_updatable;

	/*
	 * The file system credentials information bind to this portal.
	 * It is used to access the file system which needs credentials.
	 * File system credentials are created by master, and then dispatched to segments.
	 * Master is responsible for canceling these credentials when drop the portal.
	 */
	MemoryContext filesystem_credentials_memory;
	HTAB		 *filesystem_credentials;
} PortalData;

extern PortalStatus PortalGetStatus(PortalData *p);
extern const char *PortalGetStatusString(PortalData *p);
extern void PortalSetStatus(PortalData *p, PortalStatus s);

/*
 * PortalIsValid
 *		True iff portal is valid.
 */
#define PortalIsValid(p) PointerIsValid(p)

/*
 * Access macros for Portal ... use these in preference to field access.
 */
#define PortalGetQueryDesc(portal)	((portal)->queryDesc)
#define PortalGetHeapMemory(portal) ((portal)->heap)
#define PortalGetPrimaryStmt(portal) PortalListGetPrimaryStmt((portal)->stmts)


/* Prototypes for functions in utils/mmgr/portalmem.c */
extern void EnablePortalManager(void);
extern bool CommitHoldablePortals(void);
extern bool PrepareHoldablePortals(void);
extern void AtCommit_Portals(void);
extern void AtAbort_Portals(void);
extern void AtCleanup_Portals(void);
extern void AtSubCommit_Portals(SubTransactionId mySubid,
					SubTransactionId parentSubid,
					ResourceOwner parentXactOwner);
extern void AtSubAbort_Portals(SubTransactionId mySubid,
				   SubTransactionId parentSubid,
				   ResourceOwner parentXactOwner);
extern void AtSubCleanup_Portals(SubTransactionId mySubid);
extern Portal CreatePortal(const char *name, bool allowDup, bool dupSilent);
extern Portal CreateNewPortal(void);
extern void PortalDrop(Portal portal, bool isTopCommit);
extern void DropDependentPortals(MemoryContext queryContext);
extern Portal GetPortalByName(const char *name);
extern void PortalDefineQuery(Portal portal,
				  const char *prepStmtName,
				  const char *sourceText,
				  NodeTag	  sourceTag, /* GPDB */
				  const char *commandTag,
				  List *stmts,
//				  List *parseTrees,
//				  List *planTrees,
				  MemoryContext queryContext);
extern Node *PortalListGetPrimaryStmt(List *stmts);
extern void PortalCreateHoldStore(Portal portal);
extern void TotalResPortalIncrements(int pid, Oid queueid,
									 Cost *totalIncrements, int *num);

#endif   /* PORTAL_H */
