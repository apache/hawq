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
 * aoseg.c
 *	  This file contains routines to support creation of append-only segment
 *    entry tables. This file is identical in functionality to toasting.c that
 *    exists in the same directory. One is in charge of creating toast tables
 *    (pg_toast_<reloid>) and the other append only segment position tables
 *    (pg_aoseg_<reloid>).
 *
 * Portions Copyright (c) 2008-2010, Greenplum Inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "access/aosegfiles.h"
#include "access/parquetsegfiles.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/aoseg.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "catalog/pg_appendonly.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#include "cdb/cdbmirroredappendonly.h"
#include "cdb/cdbvars.h"

static bool create_aoseg_table(Relation rel, Oid aosegOid, Oid aosegIndexOid, Oid * comptypeOid);
static bool needs_aoseg_table(Relation rel);

/*
 * AlterTableCreateAoSegTable
 *		If the table needs an AO segment table, and doesn't already have one,
 *		then create a aoseg table for it.
 *
 * We expect the caller to have verified that the relation is AO table and have
 * already done any necessary permission checks.  Callers expect this function
 * to end with CommandCounterIncrement if it makes any changes.
 */
void
AlterTableCreateAoSegTable(Oid relOid)
{
	Relation	rel;

	/*
	 * We've well and truly locked the table if we need to, so don't now.
	 * This is useful for avoiding lock table overflows with partitioning.
	 */
	rel = heap_open(relOid, NoLock);

	/* create_aoseg_table does all the work */
	(void) create_aoseg_table(rel, InvalidOid, InvalidOid, NULL);

	heap_close(rel, NoLock);
}

void
AlterTableCreateAoSegTableWithOid(Oid relOid, Oid newOid, Oid newIndexOid,
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

	/* create_aoseg_table does all the work */
	(void) create_aoseg_table(rel, newOid, newIndexOid, comptypeOid);

	heap_close(rel, NoLock);
}

/*
 * create_aoseg_table --- internal workhorse
 *
 * rel is already opened and exclusive-locked
 * aosegOid and aosegIndexOid are normally InvalidOid
 */
static bool
create_aoseg_table(Relation rel, Oid aosegOid, Oid aosegIndexOid, Oid * comptypeOid)
{
	Oid			relOid = RelationGetRelid(rel);
	TupleDesc	tupdesc;
	bool		shared_relation;
	Oid			aoseg_relid;
	Oid			aoseg_idxid;
	char		aoseg_relname[NAMEDATALEN];
	char		aoseg_idxname[NAMEDATALEN];
	IndexInfo  *indexInfo;
	Oid			classObjectId[2];
	ObjectAddress baseobject,
				aosegobject;
	Oid			tablespaceOid = ChooseTablespaceForLimitedObject(rel->rd_rel->reltablespace);

	/*
	 * Check to see whether the table actually needs an aoseg table.
	 */
	if (!needs_aoseg_table(rel))
		return false;

	shared_relation = rel->rd_rel->relisshared;

	/* can't have shared AO tables after initdb */
	/* TODO: disallow it at CREATE TABLE time */
	Assert(!(shared_relation && !IsBootstrapProcessingMode()) );

	GetAppendOnlyEntryAuxOids(relOid, SnapshotNow, &aoseg_relid, &aoseg_idxid, NULL, NULL);

	/*
	 * Was a aoseg table already created?
	 */
	if (aoseg_relid != InvalidOid)
	{
		return false;
	}

	if(RelationIsAoRows(rel))
	{

		/*
		 * Create the aoseg table and its index
		 */
		snprintf(aoseg_relname, sizeof(aoseg_relname),
				"pg_aoseg_%u", relOid);
		snprintf(aoseg_idxname, sizeof(aoseg_idxname),
				"pg_aoseg_%u_index", relOid);

		/* this is pretty painful...  need a tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(Natts_pg_aoseg, false );

		TupleDescInitEntry(tupdesc, (AttrNumber) Anum_pg_aoseg_segno,
				"segno", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) Anum_pg_aoseg_eof, "eof",
				FLOAT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) Anum_pg_aoseg_tupcount,
				"tupcount", FLOAT8OID, -1, 0);
		TupleDescInitEntry(tupdesc,
				(AttrNumber) Anum_pg_aoseg_varblockcount, "varblockcount",
				FLOAT8OID, -1, 0);
		TupleDescInitEntry(tupdesc,
				(AttrNumber) Anum_pg_aoseg_eofuncompressed,
				"eofuncompressed", FLOAT8OID, -1, 0);
	}
	else
	{

		/*
		 * Create the parquetseg table and its index
		 */
		snprintf(aoseg_relname, sizeof(aoseg_relname),
				"pg_paqseg_%u", relOid);
		snprintf(aoseg_idxname, sizeof(aoseg_idxname),
				"pg_paqseg_%u_index", relOid);

		/* this is pretty painful...  need a tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(Natts_pg_parquetseg, false );

		TupleDescInitEntry(tupdesc, (AttrNumber) Anum_pg_parquetseg_segno,
				"segno", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) Anum_pg_parquetseg_eof, "eof",
				FLOAT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) Anum_pg_parquetseg_tupcount,
				"tupcount", FLOAT8OID, -1, 0);
		TupleDescInitEntry(tupdesc,
				(AttrNumber) Anum_pg_parquetseg_eofuncompressed,
				"eofuncompressed", FLOAT8OID, -1, 0);
	}

	/*
	 * Note: the aoseg relation is placed in the regular pg_aoseg namespace
	 * even if its master relation is a temp table.  There cannot be any
	 * naming collision, and the aoseg rel will be destroyed when its master
	 * is, so there's no need to handle the aoseg rel as temp.
	 *
	 * XXX would it make sense to apply the master's reloptions to the aoseg
	 * table?
	 */
	aoseg_relid = heap_create_with_catalog(aoseg_relname,
										   PG_AOSEGMENT_NAMESPACE,
										   tablespaceOid,
										   aosegOid,
										   rel->rd_rel->relowner,
										   tupdesc,
										   /* relam */ InvalidOid,
										   RELKIND_AOSEGMENTS,
										   RELSTORAGE_HEAP,
										   shared_relation,
										   true,
										   /* bufferPoolBulkLoad */ false,
										   0,
										   ONCOMMIT_NOOP,
										   NULL, /* CDB POLICY */
										   (Datum) 0,
										   true,
										   comptypeOid,
						 				   /* persistentTid */ NULL,
						 				   /* persistentSerialNum */ NULL);

	/* make the toast relation visible, else index creation will fail */
	CommandCounterIncrement();

	/*
	 * Create unique index on segno.
	 */
	indexInfo = makeNode(IndexInfo);
	indexInfo->ii_NumIndexAttrs = 1;
	indexInfo->ii_KeyAttrNumbers[0] = 1;
	indexInfo->ii_Expressions = NIL;
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_Predicate = NIL;
	indexInfo->ii_PredicateState = NIL;
	indexInfo->ii_Unique = true;
	indexInfo->ii_Concurrent = false;

	classObjectId[0] = INT4_BTREE_OPS_OID;
	classObjectId[1] = INT4_BTREE_OPS_OID;

	aoseg_idxid = index_create(aoseg_relid, aoseg_idxname, aosegIndexOid,
							   indexInfo,
							   BTREE_AM_OID,
							   tablespaceOid,
							   classObjectId, (Datum) 0,
							   true, false, (Oid *) NULL, true, false, false, NULL);

	/* Unlock target table -- no one can see it */
	UnlockRelationOid(aoseg_relid, ShareLock);
	/* Unlock the index -- no one can see it anyway */
	UnlockRelationOid(aoseg_idxid, AccessExclusiveLock);

	/*
	 * Store the aoseg table's OID in the parent relation's pg_appendonly row
	 */
	UpdateAppendOnlyEntryAuxOids(relOid, aoseg_relid, aoseg_idxid,
							 InvalidOid, InvalidOid);

	/*
	 * Register dependency from the aoseg table to the master, so that the
	 * aoseg table will be deleted if the master is.
	 */
	baseobject.classId = RelationRelationId;
	baseobject.objectId = relOid;
	baseobject.objectSubId = 0;
	aosegobject.classId = RelationRelationId;
	aosegobject.objectId = aoseg_relid;
	aosegobject.objectSubId = 0;

	recordDependencyOn(&aosegobject, &baseobject, DEPENDENCY_INTERNAL);

	/*
	 * Make changes visible
	 */
	CommandCounterIncrement();

	return true;
}

/*
 * Check to see whether the table needs an aoseg table.	It does only if it is
 * an append-only relation.
 */
static bool
needs_aoseg_table(Relation rel)
{
	return (RelationIsAoRows(rel) || RelationIsParquet(rel));
}


