/*-------------------------------------------------------------------------
 *
 * resscheduler.c
 *	  POSTGRES resource scheduling management code.
 *
 *
 * Copyright (c) 2006-2010, Greenplum inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/catquery.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_resqueue.h"
#include "catalog/pg_tablespace.h"
#include "cdb/cdbllize.h"
#include "cdb/cdbvars.h"
#include "cdb/memquota.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/resscheduler.h"
#include "utils/syscache.h"

/*
 * GetResQueueForRole -- determine what resource queue a role is going to use.
 *
 * Notes
 *	This could be called for each of ResLockPortal and ResUnLockPortal, but we 
 *	can eliminate a relation open and lock if it is cached.
 */
Oid	
GetResQueueForRole(Oid roleid)
{
	bool			isnull;
	int				fetchCount;
	Oid				queueid = InvalidOid;

	queueid = caql_getoid_plus(
			NULL,
			&fetchCount,
			&isnull,
			cql("SELECT rolresqueue FROM pg_authid "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(roleid)));

	/* MPP-6926: use default queue if none specified */
	if (!OidIsValid(queueid) || !fetchCount || isnull)
		queueid = DEFAULTRESQUEUE_OID;

	return queueid;
	
}

/*
 * ResQueueIdForName -- Return the Oid for a resource queue name
 *
 * Notes:
 *	Used by the various admin commands to convert a user supplied queue name
 *	to Oid.
 */
Oid
GetResQueueIdForName(char	*name)
{
	Oid					queueid = InvalidOid;

	queueid = caql_getoid(
			NULL,
			cql("SELECT oid FROM pg_resqueue "
				" WHERE rsqname = :1 ",
				CStringGetDatum(name)));

	return queueid;
}
