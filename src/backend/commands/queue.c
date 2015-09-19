/*-------------------------------------------------------------------------
 *
 * queue.c
 *	  Commands for manipulating resource queues.
 *
 * Copyright (c) 2006-2010, Greenplum inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_resqueue.h"
#include "nodes/makefuncs.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/queue.h"
#include "libpq/crypt.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/flatfiles.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "executor/execdesc.h"
#include "utils/resscheduler.h"
#include "utils/syscache.h"
#include "cdb/memquota.h"
#include "utils/guc_tables.h"

#define INVALID_RES_LIMIT_STRING		"-1"

/**
 * Establish a lower bound on what memory limit may be set on a queue.
 */
#define MIN_RESOURCEQUEUE_MEMORY_LIMIT_KB (10 * 1024L)
				
/* MPP-6923: */				  
List * 
GetResqueueCapabilityEntry(Oid  queueid)
{
	List		*elems = NIL;
	HeapTuple	 tuple;
	cqContext	*pcqCtx;
	cqContext	 cqc;
	Relation	 rel;
	TupleDesc	 tupdesc;

	rel = heap_open(ResQueueCapabilityRelationId, AccessShareLock);

	tupdesc = RelationGetDescr(rel);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), rel),
			cql("SELECT * FROM pg_resqueuecapability"
				" WHERE resqueueid = :1  ",
				ObjectIdGetDatum(queueid)));

	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		if (HeapTupleIsValid(tuple))
		{
			List		*pentry		 = NIL;
			int			 resTypeInt	 = 0;
			text		*resSet_text = NULL;
			Datum		 resSet_datum;
			char		*resSetting	 = NULL;
			bool		 isnull		 = false;

			resTypeInt =
					((Form_pg_resqueuecapability) GETSTRUCT(tuple))->restypid;

			resSet_datum = heap_getattr(tuple,
										Anum_pg_resqueuecapability_ressetting,
										tupdesc,
										&isnull);
			Assert(!isnull);
			resSet_text = DatumGetTextP(resSet_datum);
			resSetting = DatumGetCString(DirectFunctionCall1(textout,
					PointerGetDatum(resSet_text)));

			pentry = list_make2(
					makeInteger(resTypeInt),
					makeString(resSetting));

			/* list of lists */
			elems = lappend(elems, pentry);
		}
	}
	caql_endscan(pcqCtx);

	heap_close(rel, AccessShareLock);
	
	return (elems);
} /* end GetResqueueCapabilityEntry */

/**
 * Given a queue id, return its name
 */
char *GetResqueueName(Oid resqueueOid)
{
	int			 fetchCount;
	char		*result = NULL;

	result = caql_getcstring_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT rsqname FROM pg_resqueue"
				" WHERE oid = :1 ",
				ObjectIdGetDatum(resqueueOid)));

	/* If we cannot find a resource queue id for any reason */
	if (!fetchCount)
		result = pstrdup("Unknown");	

	return result;
}
