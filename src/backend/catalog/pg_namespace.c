/*-------------------------------------------------------------------------
 *
 * pg_namespace.c
 *	  routines to support manipulation of the pg_namespace relation
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/catalog/pg_namespace.c,v 1.16 2006/03/05 15:58:23 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_database.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"



/* ----------------
 * NamespaceCreate
 * ---------------
 */
Oid
NamespaceCreate(const char *nspName, Oid ownerId, Oid forceOid)
{
	Relation	nspdesc;
	HeapTuple	tup;
	Oid			nspoid;
	bool		nulls[Natts_pg_namespace];
	Datum		values[Natts_pg_namespace];
	NameData	nname;
	int			i;
	cqContext	cqc;
	cqContext	cqc2;
	cqContext  *pcqCtx;

	/* sanity checks */
	if (!nspName)
		elog(ERROR, "no namespace name supplied");

	nspdesc = heap_open(NamespaceRelationId, RowExclusiveLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), nspdesc),
			cql("INSERT INTO pg_namespace",
				NULL));

	/* make sure there is no existing namespace of same name in the current database */
	if (caql_getcount(
				caql_addrel(cqclr(&cqc2), nspdesc),
				cql("SELECT COUNT(*) FROM pg_namespace "
					" WHERE nspname = :1 and nspdboid = :2",
					PointerGetDatum((char *) nspName), ObjectIdGetDatum((Oid) NSPDBOID_CURRENT))))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_SCHEMA),
				 errmsg("schema \"%s\" already exists", nspName)));
	}

	/* initialize nulls and values */
	for (i = 0; i < Natts_pg_namespace; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
	}
	namestrcpy(&nname, nspName);
	values[Anum_pg_namespace_nspname - 1] = NameGetDatum(&nname);
	values[Anum_pg_namespace_nspowner - 1] = ObjectIdGetDatum(ownerId);
	nulls[Anum_pg_namespace_nspacl - 1] = true;
	values[Anum_pg_namespace_nspdboid - 1] = ObjectIdGetDatum((Oid) NSPDBOID_CURRENT);

	tup = caql_form_tuple(pcqCtx, values, nulls);
	
	if (forceOid != InvalidOid)
		HeapTupleSetOid(tup, forceOid);		/* override heap_insert's OID
											 * selection */

	/* insert a new tuple */
	nspoid = caql_insert(pcqCtx, tup); /* implicit update of index as well */
	Assert(OidIsValid(nspoid));

	caql_endscan(pcqCtx);
	heap_close(nspdesc, RowExclusiveLock);

	/* Record dependency on owner */
	recordDependencyOnOwner(NamespaceRelationId, nspoid, ownerId);

	return nspoid;
}

/*
 * Get the catalog entry for an namespace relation tuple.
 */
NameSpaceEntry *
GetNameSpaceEntryFromTuple(
	Relation		pg_namespace_rel,
	TupleDesc	pg_namespace_dsc,
	HeapTuple	tuple,
	Oid			*namespaceId)
{
	Datum		nspid,
				nspname,
				nspowner,
				nspacl,
				nspdboid;

	bool		isNull;

	NameSpaceEntry *nspentry;

	nspentry = (NameSpaceEntry *) palloc0(sizeof(NameSpaceEntry));

	nspid = heap_getattr(tuple,
							 Anum_pg_namespace_oid,
							 pg_namespace_dsc,
							 &isNull);

	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid relid value: NULL")));

	/* get the nspname */
	nspname = heap_getattr(tuple,
							 Anum_pg_namespace_nspname,
							 pg_namespace_dsc,
							 &isNull);

	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid relid value: NULL")));
	nspentry->nspname = DatumGetCString(nspname);

	/* get the nspowner */
	nspowner = heap_getattr(tuple,
							 Anum_pg_namespace_nspowner,
							 pg_namespace_dsc,
							 &isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid relid value: NULL")));
	nspentry->nspowner = DatumGetObjectId(nspowner);

	/* get the nspacl */
	nspacl = heap_getattr(tuple,
						   Anum_pg_namespace_nspacl,
						   pg_namespace_dsc,
						   &isNull);

	if (!isNull)
	{
		Datum	   *elems;
		int			nelems;
		int			i;
		char*		acl_str = NULL;

		deconstruct_array(DatumGetArrayTypeP(nspacl),
						  ACLITEMOID, sizeof(AclItem), false, 'i',
						  &elems, NULL, &nelems);

		for (i = 0; i < nelems; i++)
		{
			acl_str = DatumGetCString(DirectFunctionCall1(aclitemout, elems[i]));

			/* append to a list of Value nodes, size nelems */
			nspentry->nspacl = lappend(nspentry->nspacl, makeString(pstrdup(acl_str)));
		}
	} else {
		nspentry->nspacl = NULL;
	}


	/* get the nspdboid */
	nspdboid = heap_getattr(tuple,
						   Anum_pg_namespace_nspdboid,
						   pg_namespace_dsc,
						   &isNull);
	nspentry->nspdboid = DatumGetObjectId(nspdboid);


    *namespaceId = DatumGetObjectId(nspid);

	return nspentry;
}
