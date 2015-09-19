/*-------------------------------------------------------------------------
 *
 * aoblkdir.c
 *   This file contains routines to support creation of append-only block
 *   directory tables. This file is identical in functionality to aoseg.c
 *   that exists in the same directory.
 *
 * Copyright (c) 2009, Greenplum Inc.
 *
 * $Id: $
 * $Change: $
 * $DateTime: $
 * $Author: $
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "catalog/aoblkdir.h"


static bool
create_aoblkdir_table(Relation rel, Oid aoblkdirOid,
					  Oid aoblkdirIndexOid, Oid *comptypeOid);

/*
 * AlterTableCreateAoBlkdirTable
 *    If the table needs an AO block directory table, and doesn't already
 *    have one, then create an aoblkdir table.
 * 
 * We expect the caller to have verified that the relation is AO table and have
 * already done any necessary permission checks.  Callers expect this function
 * to end with CommandCounterIncrement if it makes any changes.
 */
void
AlterTableCreateAoBlkdirTable(Oid relOid)
{
	Relation rel;
	
	/*
	 * We've well and truly locked the table if we need to, so don't now.
	 * This is useful for avoiding lock table overflows with partitioning.
	 */
	rel = heap_open(relOid, NoLock);

	/* create_aoblkdir_table does all the work */
	(void) create_aoblkdir_table(rel, InvalidOid, InvalidOid, NULL);

	heap_close(rel, NoLock);
}

void
AlterTableCreateAoBlkdirTableWithOid(Oid relOid, Oid newOid, Oid newIndexOid,
									 Oid * comptypeOid, bool is_part_child)
{
	Relation	rel;

	/*
	 * Grab an exclusive lock on the target table, which we will NOT release
	 * until end of transaction.  (This is probably redundant in all present
	 * uses...)
	 */
	if (is_part_child)
		rel = heap_open(relOid, NoLock);
	else
		rel = heap_open(relOid, AccessExclusiveLock);

	/* create_aoblkdir_table does all the work */
	(void) create_aoblkdir_table(rel, newOid, newIndexOid, comptypeOid);

	heap_close(rel, NoLock);
}

/*
 * create_aoblkdir_table
 *
 * rel is already opened and exclusive-locked.
 * comptypeOid is InvalidOid.
 */
static bool
create_aoblkdir_table(Relation rel, Oid aoblkdirOid,
					  Oid aoblkdirIndexOid, Oid *comptypeOid)
{
	Oid relOid = RelationGetRelid(rel);
	Oid	aoblkdir_relid;
	Oid	aoblkdir_idxid;
	bool shared_relation = rel->rd_rel->relisshared;
	char aoblkdir_relname[NAMEDATALEN];
	char aoblkdir_idxname[NAMEDATALEN];
	TupleDesc	tupdesc;
	IndexInfo  *indexInfo;
	Oid			classObjectId[3];
	ObjectAddress baseobject;
	ObjectAddress aoblkdirobject;
	Oid			tablespaceOid = ChooseTablespaceForLimitedObject(rel->rd_rel->reltablespace);

	if (!RelationIsAoRows(rel))
		return false;
	
	/*
	 * We cannot allow creating a block directory for a shared relation
	 * after initdb (because there's no way to let other databases know
	 * this block directory.
	 */
	if (shared_relation && !IsBootstrapProcessingMode())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("shared tables cannot have block directory after initdb")));

	GetAppendOnlyEntryAuxOids(relOid, SnapshotNow, NULL,NULL, &aoblkdir_relid, &aoblkdir_idxid);

	/*
	 * Does it have a block directory?
	 */
	if (aoblkdir_relid != InvalidOid)
	{
		return false;
	}

	snprintf(aoblkdir_relname, sizeof(aoblkdir_relname),
			 "pg_aoblkdir_%u", relOid);
	snprintf(aoblkdir_idxname, sizeof(aoblkdir_idxname),
			 "pg_aoblkdir_%u_index", relOid);
	
	/* Create a tuple descriptor */
	tupdesc = CreateTemplateTupleDesc(4, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1,
					   "segno",
					   INT4OID,
					   -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2,
					   "columngroup_no",
					   INT4OID,
					   -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3,
					   "first_row_no",
					   INT8OID,
					   -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4,
					   "minipage",
					   VARBITOID,
					   -1, 0);
	/*
	 * We don't want any toast columns here.
	 */
	tupdesc->attrs[0]->attstorage = 'p';
	tupdesc->attrs[1]->attstorage = 'p';
	tupdesc->attrs[2]->attstorage = 'p';
	tupdesc->attrs[2]->attstorage = 'p';

	/*
	 * We place aoblkdir relation in the pg_aoseg namespace
	 * even if its master relation is a temp table. There cannot be
	 * any naming collision, and the aoblkdir relation will be
	 * destroyed when its master is, so there is no need to handle
	 * the aoblkdir relation as temp.
	 */
	aoblkdir_relid = heap_create_with_catalog(aoblkdir_relname,
											  PG_AOSEGMENT_NAMESPACE,
											  tablespaceOid,
											  aoblkdirOid,
											  rel->rd_rel->relowner,
											  tupdesc,
											  /* relam */ InvalidOid,
											  RELKIND_AOBLOCKDIR,
											  RELSTORAGE_HEAP,
											  shared_relation,
											  true,
											  /* bufferPoolBulkLoad */ false,
											  0,
											  ONCOMMIT_NOOP,
											  NULL, /* GP Policy */
											  (Datum) 0,
											  true,
											  comptypeOid,
						 					  /* persistentTid */ NULL,
						 					  /* persistentSerialNum */ NULL);
	
	/* Make this table visible, else index creation will fail */
	CommandCounterIncrement();
	
	/*
	 * Create index on segno, first_row_no.
	 */
	indexInfo = makeNode(IndexInfo);
	indexInfo->ii_NumIndexAttrs = 3;
	indexInfo->ii_KeyAttrNumbers[0] = 1;
	indexInfo->ii_KeyAttrNumbers[1] = 2;
	indexInfo->ii_KeyAttrNumbers[2] = 3;
	indexInfo->ii_Expressions = NIL;
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_Predicate = NIL;
	indexInfo->ii_PredicateState = NIL;
	indexInfo->ii_Unique = false;
	indexInfo->ii_Concurrent = false;
	
	classObjectId[0] = INT4_BTREE_OPS_OID;
	classObjectId[1] = INT4_BTREE_OPS_OID;
	classObjectId[2] = INT8_BTREE_OPS_OID;

	aoblkdir_idxid = index_create(aoblkdirOid, aoblkdir_idxname, aoblkdirIndexOid,
								  indexInfo,
								  BTREE_AM_OID,
								  tablespaceOid,
								  classObjectId, (Datum) 0,
								  true, false, (Oid *) NULL, true, false, false, NULL);
	
	/* Unlock target table -- no one can see it */
	UnlockRelationOid(aoblkdirOid, ShareLock);
	/* Unlock the index -- no one can see it anyway */
	UnlockRelationOid(aoblkdirIndexOid, AccessExclusiveLock);

	/*
	 * Store the aoblkdir table's OID in the parent relation's pg_appendonly row.
	 */
	UpdateAppendOnlyEntryAuxOids(relOid, InvalidOid, InvalidOid,
								 aoblkdir_relid, aoblkdir_idxid);

	/*
	 * Register dependency from the aoseg table to the master, so that the
	 * aoseg table will be deleted if the master is.
	 */
	baseobject.classId = RelationRelationId;
	baseobject.objectId = relOid;
	baseobject.objectSubId = 0;
	aoblkdirobject.classId = RelationRelationId;
	aoblkdirobject.objectId = aoblkdirOid;
	aoblkdirobject.objectSubId = 0;

	recordDependencyOn(&aoblkdirobject, &baseobject, DEPENDENCY_INTERNAL);

	/*
	 * Make changes visible
	 */
	CommandCounterIncrement();

	return true;
}

