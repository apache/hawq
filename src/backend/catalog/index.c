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
 * index.c
 *	  code to create and destroy POSTGRES index relations
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/catalog/index.c,v 1.274 2006/10/04 00:29:50 momjian Exp $
 *
 *
 * INTERFACE ROUTINES
 *		index_create()			- Create a cataloged index relation
 *		index_drop()			- Removes index relation from catalogs
 *		BuildIndexInfo()		- Prepare to insert index tuples
 *		FormIndexDatum()		- Construct datum vector for one index tuple
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "bootstrap/bootstrap.h"
#include "catalog/catalog.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/aoblkdir.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/parse_expr.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbanalyze.h"
#include "cdb/cdboidsync.h"
#include "cdb/cdbappendonlyam.h"

#include "cdb/cdbmirroredfilesysobj.h"

/* state info for validate_index bulkdelete callback */
typedef struct
{
	void *tuplesort;	/* for sorting the index TIDs */
	/* statistics (for debug purposes only): */
	double		htups,
			itups,
			tups_inserted;
} v_i_state;

/* non-export function prototypes */
static TupleDesc ConstructTupleDescriptor(Relation heapRelation,
						 IndexInfo *indexInfo,
						 Oid *classObjectId);
static void InitializeAttributeOids(Relation indexRelation,
						int numatts, Oid indexoid);
static void AppendAttributeTuples(Relation indexRelation, int numatts);
static void UpdateIndexRelation(Oid indexoid, Oid heapoid,
					IndexInfo *indexInfo,
					Oid *classOids,
					bool primary,
					bool isvalid);
static void index_update_stats(Relation rel, bool hasindex, bool isprimary,
							   Oid reltoastidxid, double reltuples);
static bool validate_index_callback(ItemPointer itemptr, void *opaque);
static void validate_index_heapscan(Relation heapRelation,
						Relation indexRelation,
						IndexInfo *indexInfo,
						Snapshot snapshot,
						v_i_state *state);
static double IndexBuildHeapScan(Relation heapRelation,
								 Relation indexRelation,
								 struct IndexInfo *indexInfo,
								 EState *estate,
								 Snapshot snapshot,
								 TransactionId OldestXmin,
								 IndexBuildCallback callback,
								 void *callback_state);
static double IndexBuildAppendOnlyRowScan(Relation parentRelation,
										  Relation indexRelation,
										  struct IndexInfo *indexInfo,
										  EState *estate,
										  Snapshot snapshot,
										  IndexBuildCallback callback,
										  void *callback_state);


/*
 *		ConstructTupleDescriptor
 *
 * Build an index tuple descriptor for a new index
 */
static TupleDesc
ConstructTupleDescriptor(Relation heapRelation,
						 IndexInfo *indexInfo,
						 Oid *classObjectId)
{
	int			numatts = indexInfo->ii_NumIndexAttrs;
	ListCell   *indexpr_item = list_head(indexInfo->ii_Expressions);
	TupleDesc	heapTupDesc;
	TupleDesc	indexTupDesc;
	int			natts;			/* #atts in heap rel --- for error checks */
	int			i;
	int			fetchCount;
	cqContext  *pcqCtx;

	heapTupDesc = RelationGetDescr(heapRelation);
	natts = RelationGetForm(heapRelation)->relnatts;

	/*
	 * allocate the new tuple descriptor
	 */
	indexTupDesc = CreateTemplateTupleDesc(numatts, false);

	/*
	 * For simple index columns, we copy the pg_attribute row from the parent
	 * relation and modify it as necessary.  For expressions we have to cons
	 * up a pg_attribute row the hard way.
	 */
	for (i = 0; i < numatts; i++)
	{
		AttrNumber	atnum = indexInfo->ii_KeyAttrNumbers[i];
		Form_pg_attribute to = indexTupDesc->attrs[i];
		HeapTuple	tuple;
		Form_pg_type typeTup;
		Oid			keyType;

		if (atnum != 0)
		{
			/* Simple index column */
			Form_pg_attribute from;

			if (atnum < 0)
			{
				/*
				 * here we are indexing on a system attribute (-1...-n)
				 */
				from = SystemAttributeDefinition(atnum,
										   heapRelation->rd_rel->relhasoids);
			}
			else
			{
				/*
				 * here we are indexing on a normal attribute (1...n)
				 */
				if (atnum > natts)		/* safety check */
					elog(ERROR, "invalid column number %d", atnum);
				from = heapTupDesc->attrs[AttrNumberGetAttrOffset(atnum)];
			}

			/*
			 * now that we've determined the "from", let's copy the tuple desc
			 * data...
			 */
			memcpy(to, from, ATTRIBUTE_TUPLE_SIZE);

			/*
			 * Fix the stuff that should not be the same as the underlying
			 * attr
			 */
			to->attnum = i + 1;

			to->attstattarget = -1;
			to->attcacheoff = -1;
			to->attnotnull = false;
			to->atthasdef = false;
			to->attislocal = true;
			to->attinhcount = 0;
		}
		else
		{
			/* Expressional index */
			Node	   *indexkey;

			MemSet(to, 0, ATTRIBUTE_TUPLE_SIZE);

			if (indexpr_item == NULL)	/* shouldn't happen */
				elog(ERROR, "too few entries in indexprs list");
			indexkey = (Node *) lfirst(indexpr_item);
			indexpr_item = lnext(indexpr_item);

			/*
			 * Make the attribute's name "pg_expresssion_nnn" (maybe think of
			 * something better later)
			 */
			sprintf(NameStr(to->attname), "pg_expression_%d", i + 1);

			/*
			 * Lookup the expression type in pg_type for the type length etc.
			 */
			keyType = exprType(indexkey);

			pcqCtx = caql_beginscan(
					NULL,
					cql("SELECT * FROM pg_type "
						" WHERE oid = :1 ",
						ObjectIdGetDatum(keyType)));

			tuple = caql_getnext(pcqCtx);

			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for type %u", keyType);
			typeTup = (Form_pg_type) GETSTRUCT(tuple);

			/*
			 * Assign some of the attributes values. Leave the rest as 0.
			 */
			to->attnum = i + 1;
			to->atttypid = keyType;
			to->attlen = typeTup->typlen;
			to->attbyval = typeTup->typbyval;
			to->attstorage = typeTup->typstorage;
			to->attalign = typeTup->typalign;
			to->attstattarget = -1;
			to->attcacheoff = -1;
			to->atttypmod = -1;
			to->attislocal = true;

			caql_endscan(pcqCtx);
		}

		/*
		 * We do not yet have the correct relation OID for the index, so just
		 * set it invalid for now.	InitializeAttributeOids() will fix it
		 * later.
		 */
		to->attrelid = InvalidOid;

		/*
		 * Check the opclass to see if it provides a keytype (overriding the
		 * attribute type).
		 */
		keyType = caql_getoid_plus(
				NULL,
				&fetchCount,
				NULL,
				cql("SELECT opckeytype FROM pg_opclass "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(classObjectId[i])));

		if (!fetchCount)
			elog(ERROR, "cache lookup failed for opclass %u",
				 classObjectId[i]);

		if (OidIsValid(keyType) && keyType != to->atttypid)
		{
			/* index value and heap value have different types */
			pcqCtx = caql_beginscan(
					NULL,
					cql("SELECT * FROM pg_type "
						" WHERE oid = :1 ",
						ObjectIdGetDatum(keyType)));
			
			tuple = caql_getnext(pcqCtx);
			
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for type %u", keyType);
			typeTup = (Form_pg_type) GETSTRUCT(tuple);

			to->atttypid = keyType;
			to->atttypmod = -1;
			to->attlen = typeTup->typlen;
			to->attbyval = typeTup->typbyval;
			to->attalign = typeTup->typalign;
			to->attstorage = typeTup->typstorage;

			caql_endscan(pcqCtx);
		}
	}

	return indexTupDesc;
}

/* ----------------------------------------------------------------
 *		InitializeAttributeOids
 * ----------------------------------------------------------------
 */
static void
InitializeAttributeOids(Relation indexRelation,
						int numatts,
						Oid indexoid)
{
	TupleDesc	tupleDescriptor;
	int			i;

	tupleDescriptor = RelationGetDescr(indexRelation);

	for (i = 0; i < numatts; i += 1)
		tupleDescriptor->attrs[i]->attrelid = indexoid;
}

/* ----------------------------------------------------------------
 *		AppendAttributeTuples
 * ----------------------------------------------------------------
 */
static void
AppendAttributeTuples(Relation indexRelation, int numatts)
{
	TupleDesc	indexTupDesc;
	HeapTuple	new_tuple;
	int			i;
	cqContext  *pcqCtx;

	/*
	 * open the attribute relation and its indexes
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_attribute ",
				NULL));

	/*
	 * insert data from new index's tupdesc into pg_attribute
	 */
	indexTupDesc = RelationGetDescr(indexRelation);

	for (i = 0; i < numatts; i++)
	{
		/*
		 * There used to be very grotty code here to set these fields, but I
		 * think it's unnecessary.  They should be set already.
		 */
		Assert(indexTupDesc->attrs[i]->attnum == i + 1);
		Assert(indexTupDesc->attrs[i]->attcacheoff == -1);

		new_tuple = heap_addheader(Natts_pg_attribute,
								   false,
								   ATTRIBUTE_TUPLE_SIZE,
								   (void *) indexTupDesc->attrs[i]);

		caql_insert(pcqCtx, new_tuple);
		/* and Update indexes (implicit) */

		heap_freetuple(new_tuple);
	}

	caql_endscan(pcqCtx); /* close rel, indexes */
}

/* ----------------------------------------------------------------
 *		UpdateIndexRelation
 *
 * Construct and insert a new entry in the pg_index catalog
 * ----------------------------------------------------------------
 */
static void
UpdateIndexRelation(Oid indexoid,
					Oid heapoid,
					IndexInfo *indexInfo,
					Oid *classOids,
					bool primary,
					bool isvalid)
{
	int2vector *indkey;
	oidvector  *indclass;
	Datum		exprsDatum;
	Datum		predDatum;
	Datum		values[Natts_pg_index];
	bool		nulls[Natts_pg_index];
	HeapTuple	tuple;
	int			i;
	cqContext  *pcqCtx;
	/*
	 * Copy the index key and opclass info into arrays (should we make the
	 * caller pass them like this to start with?)
	 */
	indkey = buildint2vector(NULL, indexInfo->ii_NumIndexAttrs);
	indclass = buildoidvector(classOids, indexInfo->ii_NumIndexAttrs);
	for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
		indkey->values[i] = indexInfo->ii_KeyAttrNumbers[i];

	/*
	 * Convert the index expressions (if any) to a text datum
	 */
	if (indexInfo->ii_Expressions != NIL)
	{
		char	   *exprsString;

		exprsString = nodeToString(indexInfo->ii_Expressions);
		exprsDatum = CStringGetTextDatum(exprsString);
		pfree(exprsString);
	}
	else
		exprsDatum = (Datum) 0;

	/*
	 * Convert the index predicate (if any) to a text datum.  Note we convert
	 * implicit-AND format to normal explicit-AND for storage.
	 */
	if (indexInfo->ii_Predicate != NIL)
	{
		char	   *predString;

		predString = nodeToString(make_ands_explicit(indexInfo->ii_Predicate));
		predDatum = CStringGetTextDatum(predString);
		pfree(predString);
	}
	else
		predDatum = (Datum) 0;

	/*
	 * open the system catalog index relation
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_index ",
				NULL));

	/*
	 * Build a pg_index tuple
	 */
	MemSet(nulls, false, sizeof(nulls));

	values[Anum_pg_index_indexrelid - 1] = ObjectIdGetDatum(indexoid);
	values[Anum_pg_index_indrelid - 1] = ObjectIdGetDatum(heapoid);
	values[Anum_pg_index_indnatts - 1] = Int16GetDatum(indexInfo->ii_NumIndexAttrs);
	values[Anum_pg_index_indisunique - 1] = BoolGetDatum(indexInfo->ii_Unique);
	values[Anum_pg_index_indisprimary - 1] = BoolGetDatum(primary);
	values[Anum_pg_index_indisclustered - 1] = BoolGetDatum(false);
	values[Anum_pg_index_indisvalid - 1] = BoolGetDatum(isvalid);
	values[Anum_pg_index_indkey - 1] = PointerGetDatum(indkey);
	values[Anum_pg_index_indclass - 1] = PointerGetDatum(indclass);
	values[Anum_pg_index_indexprs - 1] = exprsDatum;
	if (exprsDatum == (Datum) 0)
		nulls[Anum_pg_index_indexprs - 1] = true;
	values[Anum_pg_index_indpred - 1] = predDatum;
	if (predDatum == (Datum) 0)
		nulls[Anum_pg_index_indpred - 1] = true;

	tuple = caql_form_tuple(pcqCtx, values, nulls);

	/*
	 * insert the tuple into the pg_index catalog
	 */
	caql_insert(pcqCtx, tuple);
	/* and Update indexes (implicit) */

	/*
	 * close the relation and free the tuple
	 */
	caql_endscan(pcqCtx);
	heap_freetuple(tuple);
}


/*
 * index_create
 *
 * heapRelationId: OID of table to build index on
 * indexRelationName: what it say
 * indexRelationId: normally, pass InvalidOid to let this routine
 *		generate an OID for the index.	During bootstrap this may be
 *		nonzero to specify a preselected OID.
 * indexInfo: same info executor uses to insert into the index
 * accessMethodObjectId: OID of index AM to use
 * tableSpaceId: OID of tablespace to use
 * classObjectId: array of index opclass OIDs, one per index column
 * reloptions: AM-specific options
 * isprimary: index is a PRIMARY KEY
 * isconstraint: index is owned by a PRIMARY KEY or UNIQUE constraint
 * constrOid: constraint OID to use if isconstraint is true
 * allow_system_table_mods: allow table to be a system catalog
 * skip_build: true to skip the index_build() step for the moment; caller
 *		must do it later (typically via reindex_index())
 * concurrent: if true, do not lock the table against writers.	The index
 *		will be marked "invalid" and the caller must take additional steps
 *		to fix it up.
 *
 * Returns OID of the created index.
 */
Oid
index_create(Oid heapRelationId,
			 const char *indexRelationName,
			 Oid indexRelationId,
			 struct IndexInfo *indexInfo,
			 Oid accessMethodObjectId,
			 Oid tableSpaceId,
			 Oid *classObjectId,
			 Datum reloptions,
			 bool isprimary,
			 bool isconstraint,
			 Oid *constrOid,
			 bool allow_system_table_mods,
			 bool skip_build,
			 bool concurrent,
			 const char *altConName)
{
	Relation	pg_class;
	Relation	gp_relfile_node;
	Relation	heapRelation;
	Relation	indexRelation;
	TupleDesc	indexTupDesc;
	bool		shared_relation;
	Oid			namespaceId;
	int			i;
	LOCKMODE	heap_lockmode;

	pg_class = heap_open(RelationRelationId, RowExclusiveLock);

	if (!IsBootstrapProcessingMode())
		gp_relfile_node = heap_open(GpRelfileNodeRelationId, RowExclusiveLock);
	else
		gp_relfile_node = NULL;

	/*
	 * Only SELECT ... FOR UPDATE/SHARE are allowed while doing a standard
	 * index build; but for concurrent builds we allow INSERT/UPDATE/DELETE
	 * (but not VACUUM).
	 */
	heap_lockmode = (concurrent ? ShareUpdateExclusiveLock : ShareLock);
	heapRelation = heap_open(heapRelationId, heap_lockmode);


	/*
	 * The index will be in the same namespace as its parent table, and is
	 * shared across databases if and only if the parent is.
	 */
	namespaceId = RelationGetNamespace(heapRelation);
	shared_relation = heapRelation->rd_rel->relisshared;

	/*
	 * check parameters
	 */
	if (indexInfo->ii_NumIndexAttrs < 1)
		elog(ERROR, "must index at least one column");

	if (!allow_system_table_mods &&
		IsSystemRelation(heapRelation) &&
		IsNormalProcessingMode())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("user-defined indexes on system catalog tables are not supported")));

	/*
	 * concurrent index build on a system catalog is unsafe because we tend to
	 * release locks before committing in catalogs
	 */
	if (concurrent &&
		IsSystemRelation(heapRelation))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("concurrent index creation on system catalog tables is not supported")));

	/*
	 * We cannot allow indexing a shared relation after initdb (because
	 * there's no way to make the entry in other databases' pg_class),
	 * except during upgrade.
	 */
	if (shared_relation &&  !(IsBootstrapProcessingMode() || gp_upgrade_mode))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("shared indexes cannot be created after initdb")));

	if (get_relname_relid(indexRelationName, namespaceId))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists",
						indexRelationName)));

	/*
	 * construct tuple descriptor for index tuples
	 */
	indexTupDesc = ConstructTupleDescriptor(heapRelation,
											indexInfo,
											classObjectId);

	/*
	 * Allocate an OID for the index, unless we were told what to use.
	 *
	 * The OID will be the relfilenode as well, so make sure it doesn't
	 * collide with either pg_class OIDs or existing physical files.
	 */
	if (!OidIsValid(indexRelationId))
		indexRelationId = GetNewRelFileNode(tableSpaceId, shared_relation,
											pg_class, false);
	else
		if (IsUnderPostmaster)
		{
			CheckNewRelFileNodeIsOk(indexRelationId, tableSpaceId, shared_relation, pg_class, false);
		}

	/*
	 * create the index relation's relcache entry and physical disk file. (If
	 * we fail further down, it's the smgr's responsibility to remove the disk
	 * file again.)
	 */
	indexRelation = heap_create(indexRelationName,
								namespaceId,
								tableSpaceId,
								indexRelationId,
								indexTupDesc,
								accessMethodObjectId,
								RELKIND_INDEX,
								RELSTORAGE_HEAP,
								shared_relation,
								allow_system_table_mods,
								/* bufferPoolBulkLoad */ false);

	Assert(indexRelationId == RelationGetRelid(indexRelation));

	/*
	 * Obtain exclusive lock on it.  Although no other backends can see it
	 * until we commit, this prevents deadlock-risk complaints from lock
	 * manager in cases such as CLUSTER.
	 */
	LockRelation(indexRelation, AccessExclusiveLock);

	/*
	 * Fill in fields of the index's pg_class entry that are not set correctly
	 * by heap_create.
	 *
	 * XXX should have a cleaner way to create cataloged indexes
	 */
	indexRelation->rd_rel->relowner = heapRelation->rd_rel->relowner;
	indexRelation->rd_rel->relam = accessMethodObjectId;
	indexRelation->rd_rel->relkind = RELKIND_INDEX;
	indexRelation->rd_rel->relhasoids = false;

	/*
	 * store index's pg_class entry
	 */
	InsertPgClassTuple(pg_class, indexRelation,
					   RelationGetRelid(indexRelation),
					   reloptions);

	/* done with pg_class */
	heap_close(pg_class, RowExclusiveLock);

	{							/* MPP-7575: track index creation */
		bool	 doIt	= true;
		char	*subtyp = "INDEX";

		/* MPP-7576: don't track internal namespace tables */
		switch (namespaceId) 
		{
			case PG_CATALOG_NAMESPACE:
				/* MPP-7773: don't track objects in system namespace
				 * if modifying system tables (eg during upgrade)  
				 */
				if (allowSystemTableModsDDL)
					doIt = false;
				break;

			case PG_TOAST_NAMESPACE:
			case PG_BITMAPINDEX_NAMESPACE:
			case PG_AOSEGMENT_NAMESPACE:
				doIt = false;
				break;
			default:
				break;
		}

		if (doIt)
			doIt = (!(isAnyTempNamespace(namespaceId)));

		/* MPP-6929: metadata tracking */
		if (doIt)
			MetaTrackAddObject(RelationRelationId,
							   RelationGetRelid(indexRelation),
							   GetUserId(), /* not ownerid */
							   "CREATE", subtyp
					);
	}

	if (gp_relfile_node != NULL)
	{
		InsertGpRelfileNodeTuple(
							gp_relfile_node,
							indexRelation->rd_id,
							indexRelation->rd_rel->relname.data,
							indexRelation->rd_rel->relfilenode,
							/* segmentFileNum */ 0,
							/* updateIndex */ true,
							&indexRelation->rd_relationnodeinfo.persistentTid,
							indexRelation->rd_relationnodeinfo.persistentSerialNum);
	
		heap_close(gp_relfile_node, RowExclusiveLock);
	}

	/*
	 * now update the object id's of all the attribute tuple forms in the
	 * index relation's tuple descriptor
	 */
	InitializeAttributeOids(indexRelation,
							indexInfo->ii_NumIndexAttrs,
							indexRelationId);

	/*
	 * append ATTRIBUTE tuples for the index
	 */
	AppendAttributeTuples(indexRelation, indexInfo->ii_NumIndexAttrs);

	/* ----------------
	 *	  update pg_index
	 *	  (append INDEX tuple)
	 *
	 *	  Note that this stows away a representation of "predicate".
	 *	  (Or, could define a rule to maintain the predicate) --Nels, Feb '92
	 * ----------------
	 */
	UpdateIndexRelation(indexRelationId, heapRelationId, indexInfo,
						classObjectId, isprimary, !concurrent);

	/*
	 * Register constraint and dependencies for the index.
	 *
	 * If the index is from a CONSTRAINT clause, construct a pg_constraint
	 * entry. The index is then linked to the constraint, which in turn is
	 * linked to the table.  If it's not a CONSTRAINT, make the dependency
	 * directly on the table.
	 *
	 * We don't need a dependency on the namespace, because there'll be an
	 * indirect dependency via our parent table.
	 *
	 * During bootstrap we can't register any dependencies, and we don't try
	 * to make a constraint either.
	 */
	if (!IsBootstrapProcessingMode())
	{
		ObjectAddress myself,
					referenced;

		myself.classId = RelationRelationId;
		myself.objectId = indexRelationId;
		myself.objectSubId = 0;

		if (isconstraint)
		{
			char		constraintType;
			const char *constraintName = indexRelationName;
			
			if ( altConName )
			{
				constraintName = altConName;
			}

			if (isprimary)
				constraintType = CONSTRAINT_PRIMARY;
			else if (indexInfo->ii_Unique)
				constraintType = CONSTRAINT_UNIQUE;
			else
			{
				elog(ERROR, "constraint must be PRIMARY or UNIQUE");
				constraintType = 0;		/* keep compiler quiet */
			}

			/* Shouldn't have any expressions */
			if (indexInfo->ii_Expressions)
				elog(ERROR, "constraints can't have index expressions");

			Insist(constrOid != NULL);
			*constrOid = CreateConstraintEntry(constraintName,
											   *constrOid,
											   namespaceId,
											   constraintType,
											   false,		/* isDeferrable */
											   false,		/* isDeferred */
											   heapRelationId,
											   indexInfo->ii_KeyAttrNumbers,
											   indexInfo->ii_NumIndexAttrs,
											   InvalidOid,	/* no domain */
											   InvalidOid,	/* no foreign key */
											   NULL,
											   0,
											   ' ',
											   ' ',
											   ' ',
											   InvalidOid,	/* no associated index */
											   NULL,		/* no check constraint */
											   NULL,
											   NULL);

			referenced.classId = ConstraintRelationId;
			referenced.objectId = *constrOid;
			referenced.objectSubId = 0;

			recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
		}
		else
		{
			bool		have_simple_col = false;

			/* Create auto dependencies on simply-referenced columns */
			for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
			{
				if (indexInfo->ii_KeyAttrNumbers[i] != 0)
				{
					referenced.classId = RelationRelationId;
					referenced.objectId = heapRelationId;
					referenced.objectSubId = indexInfo->ii_KeyAttrNumbers[i];

					recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

					have_simple_col = true;
				}
			}

			/*
			 * It's possible for an index to not depend on any columns of the
			 * table at all, in which case we need to give it a dependency on
			 * the table as a whole; else it won't get dropped when the table
			 * is dropped.	This edge case is not totally useless; for
			 * example, a unique index on a constant expression can serve to
			 * prevent a table from containing more than one row.
			 */
			if (!have_simple_col &&
				!contain_vars_of_level((Node *) indexInfo->ii_Expressions, 0) &&
				!contain_vars_of_level((Node *) indexInfo->ii_Predicate, 0))
			{
				referenced.classId = RelationRelationId;
				referenced.objectId = heapRelationId;
				referenced.objectSubId = 0;

				recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
			}
		}

		/* Store dependency on operator classes */
		for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
		{
			referenced.classId = OperatorClassRelationId;
			referenced.objectId = classObjectId[i];
			referenced.objectSubId = 0;

			recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
		}

		/* Store dependencies on anything mentioned in index expressions */
		if (indexInfo->ii_Expressions)
		{
			recordDependencyOnSingleRelExpr(&myself,
										  (Node *) indexInfo->ii_Expressions,
											heapRelationId,
											DEPENDENCY_NORMAL,
											DEPENDENCY_AUTO);
		}

		/* Store dependencies on anything mentioned in predicate */
		if (indexInfo->ii_Predicate)
		{
			recordDependencyOnSingleRelExpr(&myself,
											(Node *) indexInfo->ii_Predicate,
											heapRelationId,
											DEPENDENCY_NORMAL,
											DEPENDENCY_AUTO);
		}
	}

	/*
	 * Advance the command counter so that we can see the newly-entered
	 * catalog tuples for the index.
	 */
	CommandCounterIncrement();

	/*
	 * In bootstrap mode, we have to fill in the index strategy structure with
	 * information from the catalogs.  If we aren't bootstrapping, then the
	 * relcache entry has already been rebuilt thanks to sinval update during
	 * CommandCounterIncrement.
	 */
	if (IsBootstrapProcessingMode())
		RelationInitIndexAccessInfo(indexRelation);
	else
		Assert(indexRelation->rd_indexcxt != NULL);

	/*
	 * For upgrade, if we've already created the index in another database,
	 * we don't need or want to recreate it.
	 */
	 if (gp_upgrade_mode && (RelationGetNumberOfBlocks(indexRelation) > 0))
	 	skip_build = true;

	/*
	 * If this is bootstrap (initdb) time, then we don't actually fill in the
	 * index yet.  We'll be creating more indexes and classes later, so we
	 * delay filling them in until just before we're done with bootstrapping.
	 * Similarly, if the caller specified skip_build then filling the index is
	 * delayed till later (ALTER TABLE can save work in some cases with this).
	 * Otherwise, we call the AM routine that constructs the index.
	 */
	if (IsBootstrapProcessingMode())
	{
		index_register(heapRelationId, indexRelationId, indexInfo);
	}
	else if (skip_build)
	{
		/*
		 * Caller is responsible for filling the index later on.  However,
		 * we'd better make sure that the heap relation is correctly marked as
		 * having an index.
		 */
		index_update_stats(heapRelation,
						   true,
						   isprimary,
						   InvalidOid,
						   heapRelation->rd_rel->reltuples);
		/* Make the above update visible */
		CommandCounterIncrement();
	}
	else
	{
		index_build(heapRelation, indexRelation, indexInfo, isprimary);
	}

	/*
	 * Close the heap and index; but we keep the locks that we acquired above
	 * until end of transaction unless we're dealing with a child of a partition
	 * table, in which case the lock on the master is sufficient.
	 */
	if (rel_needs_long_lock(RelationGetRelid(heapRelation)))
	{
		index_close(indexRelation, NoLock);
		heap_close(heapRelation, NoLock);
	}
	else
	{
		index_close(indexRelation, AccessExclusiveLock);
		heap_close(heapRelation, heap_lockmode);
	}

	return indexRelationId;
}

/*
 *		index_drop
 *
 * NOTE: this routine should now only be called through performDeletion(),
 * else associated dependencies won't be cleaned up.
 */
void
index_drop(Oid indexId)
{
	Oid			heapId;
	Relation	userHeapRelation;
	Relation	userIndexRelation;
	HeapTuple	tuple;
	bool		hasexprs;
	bool		need_long_lock;
	cqContext  *pcqCtx;

	/*
	 * To drop an index safely, we must grab exclusive lock on its parent
	 * table; otherwise there could be other backends using the index!
	 * Exclusive lock on the index alone is insufficient because another
	 * backend might be in the midst of devising a query plan that will use
	 * the index.  The parser and planner take care to hold an appropriate
	 * lock on the parent table while working, but having them hold locks on
	 * all the indexes too seems overly expensive.	We do grab exclusive lock
	 * on the index too, just to be safe. Both locks must be held till end of
	 * transaction, else other backends will still see this index in pg_index.
	 */
	heapId = IndexGetRelation(indexId);
	userHeapRelation = heap_open(heapId, AccessExclusiveLock);

	userIndexRelation = index_open(indexId, AccessExclusiveLock);


	/*
	 * TODO, in hawq, only MASTER_CONTENT_ID is used here,
	 * will changed later depends on the design of index.
	 */
	if (!userIndexRelation->rd_relationnodeinfo.isPresent)
		RelationFetchGpRelationNode(userIndexRelation);

	/*
	 * Schedule physical removal of the files
	 */
	MirroredFileSysObj_ScheduleDropBufferPoolRel(userIndexRelation);

	DeleteGpRelfileNodeTuple(
					userIndexRelation,
					/* segmentFileNum */ 0);
	

	/*
	 * Close and flush the index's relcache entry, to ensure relcache doesn't
	 * try to rebuild it while we're deleting catalog entries. We keep the
	 * lock though.
	 */
	need_long_lock = rel_needs_long_lock(RelationGetRelid(userIndexRelation));
	if (need_long_lock)
		index_close(userIndexRelation, NoLock);
	else
		index_close(userIndexRelation, AccessExclusiveLock);

	RelationForgetRelation(indexId);

	/*
	 * fix INDEX relation, and check for expressional index
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_index "
				" WHERE indexrelid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(indexId)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for index %u", indexId);

	hasexprs = !heap_attisnull(tuple, Anum_pg_index_indexprs);

	caql_delete_current(pcqCtx);

	caql_endscan(pcqCtx);

	/*
	 * if it has any expression columns, we might have stored statistics about
	 * them.
	 */
	if (hasexprs)
		RemoveStatistics(indexId, 0);

	/*
	 * fix ATTRIBUTE relation
	 */
	DeleteAttributeTuples(indexId);

	/*
	 * fix RELATION relation
	 */
	DeleteRelationTuple(indexId);

	/* MPP-6929: metadata tracking */
	MetaTrackDropObject(RelationRelationId, 
						indexId);

	/*
	 * We are presently too lazy to attempt to compute the new correct value
	 * of relhasindex (the next VACUUM will fix it if necessary). So there is
	 * no need to update the pg_class tuple for the owning relation. But we
	 * must send out a shared-cache-inval notice on the owning relation to
	 * ensure other backends update their relcache lists of indexes.
	 */
	CacheInvalidateRelcache(userHeapRelation);

	/*
	 * Close owning rel, but keep lock
	 */
	heap_close(userHeapRelation, need_long_lock ? NoLock : AccessExclusiveLock);
}

/* ----------------------------------------------------------------
 *						index_build support
 * ----------------------------------------------------------------
 */

/* ----------------
 *		BuildIndexInfo
 *			Construct an IndexInfo record for an open index
 *
 * IndexInfo stores the information about the index that's needed by
 * FormIndexDatum, which is used for both index_build() and later insertion
 * of individual index tuples.	Normally we build an IndexInfo for an index
 * just once per command, and then use it for (potentially) many tuples.
 * ----------------
 */
struct IndexInfo *
BuildIndexInfo(Relation index)
{
	IndexInfo  *ii = makeNode(IndexInfo);
	Form_pg_index indexStruct = index->rd_index;
	int			i;
	int			numKeys;

	/* check the number of keys, and copy attr numbers into the IndexInfo */
	numKeys = indexStruct->indnatts;
	if (numKeys < 1 || numKeys > INDEX_MAX_KEYS)
		elog(ERROR, "invalid indnatts %d for index %u",
			 numKeys, RelationGetRelid(index));
	ii->ii_NumIndexAttrs = numKeys;
	for (i = 0; i < numKeys; i++)
		ii->ii_KeyAttrNumbers[i] = indexStruct->indkey.values[i];

	/* fetch any expressions needed for expressional indexes */
	ii->ii_Expressions = RelationGetIndexExpressions(index);
	ii->ii_ExpressionsState = NIL;

	/* fetch index predicate if any */
	ii->ii_Predicate = RelationGetIndexPredicate(index);
	ii->ii_PredicateState = NIL;

	/* other info */
	ii->ii_Unique = indexStruct->indisunique;
	ii->ii_Concurrent = false;	/* assume normal case */

	ii->opaque = NULL;

	return ii;
}

/* ----------------
 *		FormIndexDatum
 *			Construct values[] and isnull[] arrays for a new index tuple.
 *
 *	indexInfo		Info about the index
 *	slot			Heap tuple for which we must prepare an index entry
 *	estate			executor state for evaluating any index expressions
 *	values			Array of index Datums (output area)
 *	isnull			Array of is-null indicators (output area)
 *
 * When there are no index expressions, estate may be NULL.  Otherwise it
 * must be supplied, *and* the ecxt_scantuple slot of its per-tuple expr
 * context must point to the heap tuple passed in.
 *
 * Notice we don't actually call index_form_tuple() here; we just prepare
 * its input arrays values[] and isnull[].	This is because the index AM
 * may wish to alter the data before storage.
 * ----------------
 */
void
FormIndexDatum(struct IndexInfo *indexInfo,
			   TupleTableSlot *slot,
			   struct EState *estate,
			   Datum *values,
			   bool *isnull)
{
	ListCell   *indexpr_item;
	int			i;

	if (indexInfo->ii_Expressions != NIL &&
		indexInfo->ii_ExpressionsState == NIL)
	{
		/* First time through, set up expression evaluation state */
		indexInfo->ii_ExpressionsState = (List *)
			ExecPrepareExpr((Expr *) indexInfo->ii_Expressions,
							estate);
		/* Check caller has set up context correctly */
		Assert(GetPerTupleExprContext(estate)->ecxt_scantuple == slot);
	}
	indexpr_item = list_head(indexInfo->ii_ExpressionsState);

	for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
	{
		int			keycol = indexInfo->ii_KeyAttrNumbers[i];
		Datum		iDatum;
		bool		isNull;

		if (keycol != 0)
		{
			/*
			 * Plain index column; get the value we need directly from the
			 * heap tuple.
			 */
			iDatum = slot_getattr(slot, keycol, &isNull);
		}
		else
		{
			/*
			 * Index expression --- need to evaluate it.
			 */
			if (indexpr_item == NULL)
				elog(ERROR, "wrong number of index expressions");
			iDatum = ExecEvalExprSwitchContext((ExprState *) lfirst(indexpr_item),
											   GetPerTupleExprContext(estate),
											   &isNull,
											   NULL);
			indexpr_item = lnext(indexpr_item);
		}
		values[i] = iDatum;
		isnull[i] = isNull;
	}

	if (indexpr_item != NULL)
		elog(ERROR, "wrong number of index expressions");
}


/*
 * index_update_stats --- update pg_class entry after CREATE INDEX or REINDEX
 *
 * This routine updates the pg_class row of either an index or its parent
 * relation after CREATE INDEX or REINDEX.	Its rather bizarre API is designed
 * to ensure we can do all the necessary work in just one update.
 *
 * hasindex: set relhasindex to this value
 * isprimary: if true, set relhaspkey true; else no change
 * reltoastidxid: if not InvalidOid, set reltoastidxid to this value;
 *		else no change
 * reltuples: set reltuples to this value
 *
 * relpages is also updated (using RelationGetNumberOfBlocks()).
 *
 * NOTE: an important side-effect of this operation is that an SI invalidation
 * message is sent out to all backends --- including me --- causing relcache
 * entries to be flushed or updated with the new data.	This must happen even
 * if we find that no change is needed in the pg_class row.  When updating
 * a heap entry, this ensures that other backends find out about the new
 * index.  When updating an index, it's important because some index AMs
 * expect a relcache flush to occur after REINDEX.
 */
static void
index_update_stats(Relation rel, bool hasindex, bool isprimary,
				   Oid reltoastidxid, double reltuples)
{
	BlockNumber relpages = RelationGetNumberOfBlocks(rel);
	Oid			relid = RelationGetRelid(rel);
	Relation	pg_class;
	HeapTuple	tuple;
	Form_pg_class rd_rel;
	bool		dirty;

	/*
	 * We always update the pg_class row using a non-transactional,
	 * overwrite-in-place update.  There are several reasons for this:
	 *
	 * 1. In bootstrap mode, we have no choice --- UPDATE wouldn't work.
	 *
	 * 2. We could be reindexing pg_class itself, in which case we can't move
	 * its pg_class row because CatalogUpdateIndexes might not know about all
	 * the indexes yet (see reindex_relation).
	 *
	 * 3. Because we execute CREATE INDEX with just share lock on the parent
	 * rel (to allow concurrent index creations), an ordinary update could
	 * suffer a tuple-concurrently-updated failure against another CREATE
	 * INDEX committing at about the same time.  We can avoid that by having
	 * them both do nontransactional updates (we assume they will both be
	 * trying to change the pg_class row to the same thing, so it doesn't
	 * matter which goes first).
	 *
	 * 4. Even with just a single CREATE INDEX, there's a risk factor because
	 * someone else might be trying to open the rel while we commit, and this
	 * creates a race condition as to whether he will see both or neither of
	 * the pg_class row versions as valid.	Again, a non-transactional update
	 * avoids the risk.  It is indeterminate which state of the row the other
	 * process will see, but it doesn't matter (if he's only taking
	 * AccessShareLock, then it's not critical that he see relhasindex true).
	 *
	 * It is safe to use a non-transactional update even though our
	 * transaction could still fail before committing.	Setting relhasindex
	 * true is safe even if there are no indexes (VACUUM will eventually fix
	 * it), and of course the relpages and reltuples counts are correct (or at
	 * least more so than the old values) regardless.
	 */

	pg_class = heap_open(RelationRelationId, RowExclusiveLock);

	/*
	 * Make a copy of the tuple to update.	Normally we use the syscache, but
	 * we can't rely on that during bootstrap or while reindexing pg_class
	 * itself.
	 */
	if (IsBootstrapProcessingMode() ||
		ReindexIsProcessingHeap(RelationRelationId))
	{
		/* don't assume syscache will work */
		cqContext	cqc;

		/* heapscan, noindex */
		tuple = caql_getfirst(
			caql_syscache(
					caql_indexOK(caql_addrel(cqclr(&cqc), pg_class),
								 false),
					false),
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid)));
	}
	else
	{
		cqContext	cqc;

		/* normal case, use syscache */
		tuple = caql_getfirst(
				caql_addrel(cqclr(&cqc), pg_class),
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid)));
	}

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for relation %u", relid);
	rd_rel = (Form_pg_class) GETSTRUCT(tuple);

	/* Apply required updates, if any, to copied tuple */

	dirty = false;
	if (rd_rel->relhasindex != hasindex)
	{
		rd_rel->relhasindex = hasindex;
		dirty = true;
	}
	if (isprimary)
	{
		if (!rd_rel->relhaspkey)
		{
			rd_rel->relhaspkey = true;
			dirty = true;
		}
	}
	if (OidIsValid(reltoastidxid))
	{
		Assert(rd_rel->relkind == RELKIND_TOASTVALUE);
		if (rd_rel->reltoastidxid != reltoastidxid)
		{
			rd_rel->reltoastidxid = reltoastidxid;
			dirty = true;
		}
	}

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		/**
		 * Do not overwrite relpages, reltuples in QD.
		 */
		if (rd_rel->reltuples != (float4) reltuples)
		{
			rd_rel->reltuples = (float4) reltuples;
			dirty = true;
		}
		if (rd_rel->relpages != (int32) relpages)
		{
			rd_rel->relpages = (int32) relpages;
			dirty = true;
		}
	}
	/*
	 * If anything changed, write out the tuple
	 */
	if (dirty)
	{
		heap_inplace_update(pg_class, tuple);
		/* the above sends a cache inval message */
	}
	else
	{
		/* no need to change tuple, but force relcache inval anyway */
		CacheInvalidateRelcacheByTuple(tuple);
	}

	heap_freetuple(tuple);

	heap_close(pg_class, RowExclusiveLock);
}

/*
 * index_build - invoke access-method-specific index build procedure
 *
 * On entry, the index's catalog entries are valid, and its physical disk
 * file has been created but is empty.	We call the AM-specific build
 * procedure to fill in the index contents.  We then update the pg_class
 * entries of the index and heap relation as needed, using statistics
 * returned by ambuild as well as data passed by the caller.
 *
 * Note: when reindexing an existing index, isprimary can be false;
 * the index is already properly marked and need not be re-marked.
 *
 * Note: before Postgres 8.2, the passed-in heap and index Relations
 * were automatically closed by this routine.  This is no longer the case.
 * The caller opened 'em, and the caller should close 'em.
 */
void
index_build(Relation heapRelation,
			Relation indexRelation,
			IndexInfo *indexInfo,
			bool isprimary)
{
	RegProcedure procedure;
	IndexBuildResult *stats;
	Oid			save_userid;
	bool		save_secdefcxt;

	/*
	 * sanity checks
	 */
	Assert(RelationIsValid(indexRelation));
	Assert(PointerIsValid(indexRelation->rd_am));

	procedure = indexRelation->rd_am->ambuild;
	Assert(RegProcedureIsValid(procedure));

	/*
	 * Switch to the table owner's userid, so that any index functions are
	 * run as that user.
	 */
	GetUserIdAndContext(&save_userid, &save_secdefcxt);
	SetUserIdAndContext(heapRelation->rd_rel->relowner, true);

	/*
	 * Call the access method's build procedure
	 */
	stats = (IndexBuildResult *)
		DatumGetPointer(OidFunctionCall3(procedure,
										 PointerGetDatum(heapRelation),
										 PointerGetDatum(indexRelation),
										 PointerGetDatum(indexInfo)));
	Assert(PointerIsValid(stats));

	/* Restore userid */
	SetUserIdAndContext(save_userid, save_secdefcxt);

	/*
	 * Update heap and index pg_class rows
	 */
	index_update_stats(heapRelation,
					   true,
					   isprimary,
					   (heapRelation->rd_rel->relkind == RELKIND_TOASTVALUE) ?
					   RelationGetRelid(indexRelation) : InvalidOid,
					   stats->heap_tuples);

#if 0
	/*
	 * Update an AO segment or block directory index oid
	 */
	switch(heapRelation->rd_rel->relkind)
	{
		case RELKIND_AOSEGMENTS:
			UpdateAppendOnlyEntryIdxid(RelationGetRelid(heapRelation),
									   Anum_pg_appendonly_segidxid,
									   RelationGetRelid(indexRelation));
			break;
		case RELKIND_AOBLOCKDIR:
			UpdateAppendOnlyEntryIdxid(RelationGetRelid(heapRelation),
									   Anum_pg_appendonly_blkdiridxid,
									   RelationGetRelid(indexRelation));
			break;
		default:
			/* do nothing */
	}
#endif

	index_update_stats(indexRelation,
					   false,
					   false,
					   InvalidOid,
					   stats->index_tuples);

	/* Make the updated versions visible */
	CommandCounterIncrement();
}

/*
 * IndexBuildScan - scan the heap, or the append-only row, or the append-only
 * column relation to find tuples to be indexed.
 *
 * This is called back from an access-method-specific index build procedure
 * after the AM has done whatever setup it needs.  The parent relation
 * is scanned to find tuples that should be entered into the index.  Each
 * such tuple is passed to the AM's callback routine, which does the right
 * things to add it to the new index.  After we return, the AM's index
 * build procedure does whatever cleanup is needed; in particular, it should
 * close the heap and index relations.
 *
 * The total count of heap tuples is returned.	This is for updating pg_class
 * statistics.	(It's annoying not to be able to do that here, but we can't
 * do it until after the relation is closed.)  Note that the index AM itself
 * must keep track of the number of index tuples; we don't do so here because
 * the AM might reject some of the tuples for its own reasons, such as being
 * unable to store NULLs.
 */
double
IndexBuildScan(Relation parentRelation,
			   Relation indexRelation,
			   struct IndexInfo *indexInfo,
			   IndexBuildCallback callback,
			   void *callback_state)
{
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	Snapshot	snapshot;
	TransactionId OldestXmin;
	double reltuples = 0;

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.	Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(parentRelation));

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/*
	 * Prepare for scan of the base relation.  In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples). In a
	 * concurrent build, we take a regular MVCC snapshot and index whatever's
	 * live according to that.	During bootstrap we just use SnapshotNow.
	 *
	 * If the relation is an append-only table, we use a regular MVCC snapshot
	 * and index what is actually in the table.
	 */
	if (IsBootstrapProcessingMode())
	{
		snapshot = SnapshotNow;
		OldestXmin = InvalidTransactionId;		/* not used */
	}
	else if (indexInfo->ii_Concurrent ||
			 RelationIsAoRows(parentRelation))
	{
		snapshot = CopySnapshot(GetTransactionSnapshot());
		OldestXmin = InvalidTransactionId;		/* not used */
	}
	else
	{
		snapshot = SnapshotAny;
		/* okay to ignore lazy VACUUMs here */
		OldestXmin = GetOldestXmin(parentRelation->rd_rel->relisshared);
	}

	if (RelationIsHeap(parentRelation))
		reltuples = IndexBuildHeapScan(parentRelation,
									   indexRelation,
									   indexInfo,
									   estate,
									   snapshot,
									   OldestXmin,
									   callback,
									   callback_state);
	
	else if (RelationIsAoRows(parentRelation))
		reltuples = IndexBuildAppendOnlyRowScan(parentRelation,
												indexRelation,
												indexInfo,
												estate,
												snapshot,
												callback,
												callback_state);

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NIL;

	return reltuples;
}


/*
 * IndexBuildHeapScan - scan the heap relation to find tuples to be indexed
 *
 * This is called back from an access-method-specific index build procedure
 * after the AM has done whatever setup it needs.  The parent heap relation
 * is scanned to find tuples that should be entered into the index.  Each
 * such tuple is passed to the AM's callback routine, which does the right
 * things to add it to the new index.  After we return, the AM's index
 * build procedure does whatever cleanup is needed; in particular, it should
 * close the heap and index relations.
 *
 * The total count of heap tuples is returned.	This is for updating pg_class
 * statistics.	(It's annoying not to be able to do that here, but we can't
 * do it until after the relation is closed.)  Note that the index AM itself
 * must keep track of the number of index tuples; we don't do so here because
 * the AM might reject some of the tuples for its own reasons, such as being
 * unable to store NULLs.
 */
static double
IndexBuildHeapScan(Relation heapRelation,
				   Relation indexRelation,
				   struct IndexInfo *indexInfo,
				   EState *estate,
				   Snapshot snapshot,
				   TransactionId OldestXmin,
				   IndexBuildCallback callback,
				   void *callback_state)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	HeapScanDesc scan;
	HeapTuple	heapTuple;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	double		reltuples;
	List	   *predicate = NIL;
	ExprContext *econtext;
	TupleTableSlot *slot;

	Assert(estate->es_per_tuple_exprcontext != NULL);
	econtext = estate->es_per_tuple_exprcontext;
	slot = econtext->ecxt_scantuple;

	/* Set up execution state for predicate, if any. */
	predicate = (List *)
		ExecPrepareExpr((Expr *) indexInfo->ii_Predicate, estate);

	scan = heap_beginscan(heapRelation, /* relation */
						  snapshot,		/* seeself */
						  0,	/* number of keys */
						  NULL);	/* scan key */

	reltuples = 0;

	/*
	 * Scan all tuples in the base relation.
	 */
	while ((heapTuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool		tupleIsAlive;

		CHECK_FOR_INTERRUPTS();

		if (snapshot == SnapshotAny)
		{
			/* do our own time qual check */
			bool		indexIt;

			/*
			 * We could possibly get away with not locking the buffer here,
			 * since caller should hold ShareLock on the relation, but let's
			 * be conservative about it.
			 */
			
			// -------- MirroredLock ----------
			MIRROREDLOCK_BUFMGR_LOCK;
			
			LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

			switch (HeapTupleSatisfiesVacuum(heapTuple->t_data, OldestXmin,
											 scan->rs_cbuf, true))
			{
				case HEAPTUPLE_DEAD:
					/* Definitely dead, we can ignore it */
					indexIt = false;
					tupleIsAlive = false;
					break;
				case HEAPTUPLE_LIVE:
					/* Normal case, index and unique-check it */
					indexIt = true;
					tupleIsAlive = true;
					break;
				case HEAPTUPLE_RECENTLY_DEAD:

					/*
					 * If tuple is recently deleted then we must index it
					 * anyway to preserve MVCC semantics.  (Pre-existing
					 * transactions could try to use the index after we finish
					 * building it, and may need to see such tuples.) Exclude
					 * it from unique-checking, however.
					 */
					indexIt = true;
					tupleIsAlive = false;
					break;
				case HEAPTUPLE_INSERT_IN_PROGRESS:

					/*
					 * Since caller should hold ShareLock or better, we should
					 * not see any tuples inserted by open transactions ---
					 * unless it's our own transaction. (Consider INSERT
					 * followed by CREATE INDEX within a transaction.)	An
					 * exception occurs when reindexing a system catalog,
					 * because we often release lock on system catalogs before
					 * committing.
					 */
					if (!TransactionIdIsCurrentTransactionId(
								   HeapTupleHeaderGetXmin(heapTuple->t_data))
						&& !IsSystemRelation(heapRelation))
						elog(ERROR, "concurrent insert in progress");
					indexIt = true;
					tupleIsAlive = true;
					break;
				case HEAPTUPLE_DELETE_IN_PROGRESS:

					/*
					 * Since caller should hold ShareLock or better, we should
					 * not see any tuples deleted by open transactions ---
					 * unless it's our own transaction. (Consider DELETE
					 * followed by CREATE INDEX within a transaction.)	An
					 * exception occurs when reindexing a system catalog,
					 * because we often release lock on system catalogs before
					 * committing.
					 *
					 * XXX we also skip the check for any bitmap indexes.
					 */
					Assert(!(heapTuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI));
					if (!TransactionIdIsCurrentTransactionId(
								   HeapTupleHeaderGetXmax(heapTuple->t_data))
						&& !IsSystemRelation(heapRelation)
						&& (!RelationIsBitmapIndex(indexRelation)))
						elog(ERROR, "concurrent delete in progress");
					indexIt = true;
					tupleIsAlive = false;
					break;
				default:
					elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
					indexIt = tupleIsAlive = false;		/* keep compiler quiet */
					break;
			}

			LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			if (!indexIt)
				continue;
		}
		else
		{
			/* heap_getnext did the time qual check */
			tupleIsAlive = true;
		}

		reltuples += 1;

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/* Set up for predicate or expression evaluation */
		ExecStoreGenericTuple(heapTuple, slot, false);

		/*
		 * In a partial index, discard tuples that don't satisfy the
		 * predicate.
		 */
		if (predicate != NIL)
		{
			if (!ExecQual(predicate, econtext, false))
				continue;
		}

		/*
		 * For the current heap tuple, extract all the attributes we use in
		 * this index, and note which are null.  This also performs evaluation
		 * of any expressions needed.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/*
		 * You'd think we should go ahead and build the index tuple here, but
		 * some index AMs want to do further processing on the data first.	So
		 * pass the values[] and isnull[] arrays, instead.
		 */

		/* Call the AM's callback routine to process the tuple */
		callback(indexRelation, slot_get_ctid(slot),
				 values, isnull, tupleIsAlive, callback_state);
	}

	heap_endscan(scan);

	return reltuples;
}

/*
 * IndexBuildAppendOnlyRowScan - scan the Append-Only Row relation to find
 * tuples to be indexed.
 *
 * If the block directory of the append-only relation does not exist, it is
 * created here. This occurs when the append-only relation is upgraded from
 * pre-3.4 release.
 */
static double
IndexBuildAppendOnlyRowScan(Relation parentRelation,
							Relation indexRelation,
							struct IndexInfo *indexInfo,
							EState *estate,
							Snapshot snapshot,
							IndexBuildCallback callback,
							void *callback_state)
{
	List *predicate = NIL;
	ExprContext *econtext;
	struct AppendOnlyScanDescData *aoscan;
	TupleTableSlot *slot;
	double reltuples = 0;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	AppendOnlyBlockDirectory *blockDirectory = NULL;
	
	Assert(estate->es_per_tuple_exprcontext != NULL);
	econtext = estate->es_per_tuple_exprcontext;
	slot = econtext->ecxt_scantuple;

	/* Set up execution state for predicate, if any */
	predicate = (List *)
		ExecPrepareExpr((Expr *)indexInfo->ii_Predicate, estate);
	
	aoscan = appendonly_beginscan(parentRelation,
								  snapshot,
								  0,
								  NULL);

	if (!OidIsValid(aoscan->aoEntry->blkdirrelid) ||
		!OidIsValid(aoscan->aoEntry->blkdiridxid))
	{
		IndexInfoOpaque *opaque;

		if (indexInfo->ii_Concurrent)
			ereport(ERROR,
					(errcode(ERRCODE_GP_COMMAND_ERROR),
					 errmsg("Cannot create index concurrently. Create an index non-concurrently "
					        "before creating an index concurrently in an appendonly table.")));
		
		/* Obtain the oids from IndexInfo. */
		Assert(indexInfo->opaque != NULL);

		opaque = (IndexInfoOpaque *)indexInfo->opaque;
		
		Assert(OidIsValid(opaque->blkdirRelOid) && OidIsValid(opaque->blkdirIdxOid));
		AlterTableCreateAoBlkdirTableWithOid(RelationGetRelid(parentRelation),
											 opaque->blkdirRelOid,
											 opaque->blkdirIdxOid,
											 &opaque->blkdirComptypeOid,
											 false);

		/* Update blkdirrelid, blkdiridxid in aoEntry with new values */
		aoscan->aoEntry->blkdirrelid = opaque->blkdirRelOid;
		aoscan->aoEntry->blkdiridxid = opaque->blkdirIdxOid;
		
		aoscan->buildBlockDirectory = true;
		aoscan->blockDirectory =
			(AppendOnlyBlockDirectory *)palloc0(sizeof(AppendOnlyBlockDirectory));
		blockDirectory = aoscan->blockDirectory;
	}
	
	while (appendonly_getnext(aoscan, ForwardScanDirection, slot) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		reltuples++;

		MemoryContextReset(econtext->ecxt_per_tuple_memory);
		
		if (predicate != NIL)
		{
			if (!ExecQual(predicate, econtext, false))
				continue;
		}
		
		/*
		 * For the current heap tuple, extract all the attributes we use in
		 * this index, and note which are null.  This also performs evaluation
		 * of any expressions needed.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/*
		 * You'd think we should go ahead and build the index tuple here, but
		 * some index AMs want to do further processing on the data first.	So
		 * pass the values[] and isnull[] arrays, instead.
		 */
		Assert(ItemPointerIsValid(slot_get_ctid(slot)));
		
		/* Call the AM's callback routine to process the tuple */
		callback(indexRelation, slot_get_ctid(slot),
				 values, isnull, true, callback_state);
	}
	
	appendonly_endscan(aoscan);
	
	if (blockDirectory != NULL)
		pfree(blockDirectory);
	
	return reltuples;
}


/*
 * validate_index - support code for concurrent index builds
 *
 * We do a concurrent index build by first building the index normally via
 * index_create(), while holding a weak lock that allows concurrent
 * insert/update/delete.  Also, we index only tuples that are valid
 * as of the start of the scan (see IndexBuildHeapScan), whereas a normal
 * build takes care to include recently-dead tuples.  This is OK because
 * we won't mark the index valid until all transactions that might be able
 * to see those tuples are gone.  The reason for doing that is to avoid
 * bogus unique-index failures due to concurrent UPDATEs (we might see
 * different versions of the same row as being valid when we pass over them,
 * if we used HeapTupleSatisfiesVacuum).  This leaves us with an index that
 * does not contain any tuples added to the table while we built the index.
 *
 * Next, we commit the transaction so that the index becomes visible to other
 * backends, but it is marked not "indisvalid" to prevent the planner from
 * relying on it for indexscans.  Then we wait for all transactions that
 * could have been modifying the table to terminate.  At this point we
 * know that any subsequently-started transactions will see the index and
 * insert their new tuples into it.  We then take a new reference snapshot
 * which is passed to validate_index().  Any tuples that are valid according
 * to this snap, but are not in the index, must be added to the index.
 * (Any tuples committed live after the snap will be inserted into the
 * index by their originating transaction.	Any tuples committed dead before
 * the snap need not be indexed, because we will wait out all transactions
 * that might care about them before we mark the index valid.)
 *
 * validate_index() works by first gathering all the TIDs currently in the
 * index, using a bulkdelete callback that just stores the TIDs and doesn't
 * ever say "delete it".  (This should be faster than a plain indexscan;
 * also, not all index AMs support full-index indexscan.)  Then we sort the
 * TIDs, and finally scan the table doing a "merge join" against the TID list
 * to see which tuples are missing from the index.	Thus we will ensure that
 * all tuples valid according to the reference snapshot are in the index.
 *
 * Building a unique index this way is tricky: we might try to insert a
 * tuple that is already dead or is in process of being deleted, and we
 * mustn't have a uniqueness failure against an updated version of the same
 * row.  We can check the tuple to see if it's already dead and tell
 * index_insert() not to do the uniqueness check, but that still leaves us
 * with a race condition against an in-progress update.  To handle that,
 * we expect the index AM to recheck liveness of the to-be-inserted tuple
 * before it declares a uniqueness error.
 *
 * After completing validate_index(), we wait until all transactions that
 * were alive at the time of the reference snapshot are gone; this is
 * necessary to be sure there are none left with a serializable snapshot
 * older than the reference (and hence possibly able to see tuples we did
 * not index).	Then we mark the index valid and commit.
 *
 * Doing two full table scans is a brute-force strategy.  We could try to be
 * cleverer, eg storing new tuples in a special area of the table (perhaps
 * making the table append-only by setting use_fsm).  However that would
 * add yet more locking issues.
 */
void
validate_index(Oid heapId, Oid indexId, Snapshot snapshot)
{
	Relation	heapRelation,
				indexRelation;
	IndexInfo  *indexInfo;
	IndexVacuumInfo ivinfo;
	v_i_state	state;
	Oid			save_userid;
	bool		save_secdefcxt;

	/* Open and lock the parent heap relation */
	heapRelation = heap_open(heapId, ShareUpdateExclusiveLock);
	/* And the target index relation */
	indexRelation = index_open(indexId, RowExclusiveLock);

	/*
	 * Fetch info needed for index_insert.	(You might think this should be
	 * passed in from DefineIndex, but its copy is long gone due to having
	 * been built in a previous transaction.)
	 */
	indexInfo = BuildIndexInfo(indexRelation);

	/* mark build is concurrent just for consistency */
	indexInfo->ii_Concurrent = true;

	/*
	 * Switch to the table owner's userid, so that any index functions are
	 * run as that user.
	 */
	GetUserIdAndContext(&save_userid, &save_secdefcxt);
	SetUserIdAndContext(heapRelation->rd_rel->relowner, true);

	/*
	 * Scan the index and gather up all the TIDs into a tuplesort object.
	 */
	ivinfo.index = indexRelation;
	ivinfo.vacuum_full = false;
	ivinfo.message_level = DEBUG2;
	ivinfo.num_heap_tuples = -1;
	ivinfo.extra_oids = NIL;
	state.tuplesort = NULL;

	PG_TRY();
	{
		if(gp_enable_mk_sort)
			state.tuplesort = tuplesort_begin_datum_mk(NULL,
					TIDOID,
					TIDLessOperator,
					maintenance_work_mem,
					false);
		else
			state.tuplesort = tuplesort_begin_datum(TIDOID,
					TIDLessOperator,
					maintenance_work_mem,
					false);

		state.htups = state.itups = state.tups_inserted = 0;

		(void) index_bulk_delete(&ivinfo, NULL,
				validate_index_callback, (void *) &state);

		/* Execute the sort */
		if(gp_enable_mk_sort)
		{
			tuplesort_performsort_mk((Tuplesortstate_mk *)state.tuplesort);
		}
		else
		{
			tuplesort_performsort((Tuplesortstate *) state.tuplesort);
		}

		/*
		 * Now scan the heap and "merge" it with the index
		 */
		validate_index_heapscan(heapRelation,
				indexRelation,
				indexInfo,
				snapshot,
				&state);

		/* Done with tuplesort object */
		if(gp_enable_mk_sort)
		{
			tuplesort_end_mk((Tuplesortstate_mk *)state.tuplesort);
		}
		else
		{
			tuplesort_end((Tuplesortstate *) state.tuplesort);
		}

		state.tuplesort = NULL;

	}
	PG_CATCH();
	{
		/* Clean up the sort state on error */
		if (state.tuplesort)
		{
			if(gp_enable_mk_sort)
			{
				tuplesort_end_mk((Tuplesortstate_mk *)state.tuplesort);
			}
			else
			{
				tuplesort_end((Tuplesortstate *) state.tuplesort);
			}
			state.tuplesort = NULL;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	elog(DEBUG2,
		 "validate_index found %.0f heap tuples, %.0f index tuples; inserted %.0f missing tuples",
		 state.htups, state.itups, state.tups_inserted);

	/* Restore userid */
	SetUserIdAndContext(save_userid, save_secdefcxt);

	/* Close rels, but keep locks */
	index_close(indexRelation, NoLock);
	heap_close(heapRelation, NoLock);
}

/*
 * validate_index_callback - bulkdelete callback to collect the index TIDs
 */
static bool
validate_index_callback(ItemPointer itemptr, void *opaque)
{
	v_i_state  *state = (v_i_state *) opaque;

	if(gp_enable_mk_sort)
		tuplesort_putdatum_mk((Tuplesortstate_mk *) state->tuplesort, PointerGetDatum(itemptr), false);
	else
		tuplesort_putdatum((Tuplesortstate *) state->tuplesort, PointerGetDatum(itemptr), false);

	state->itups += 1;
	return false;				/* never actually delete anything */
}

/*
 * validate_index_heapscan - second table scan for concurrent index build
 *
 * This has much code in common with IndexBuildHeapScan, but it's enough
 * different that it seems cleaner to have two routines not one.
 */
static void
validate_index_heapscan(Relation heapRelation,
						Relation indexRelation,
						IndexInfo *indexInfo,
						Snapshot snapshot,
						v_i_state *state)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	HeapScanDesc scan;
	HeapTuple	heapTuple;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	List	   *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;

	/* state variables for the merge */
	ItemPointer indexcursor = NULL;
	bool		tuplesort_empty = false;

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.	Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation));

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = (List *)
		ExecPrepareExpr((Expr *) indexInfo->ii_Predicate,
						estate);

	/*
	 * Prepare for scan of the base relation.  We need just those tuples
	 * satisfying the passed-in reference snapshot.
	 */
	scan = heap_beginscan(heapRelation, /* relation */
						  snapshot,		/* seeself */
						  0,	/* number of keys */
						  NULL);	/* scan key */

	/*
	 * Scan all tuples matching the snapshot.
	 */
	while ((heapTuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		ItemPointer heapcursor = &heapTuple->t_self;

		CHECK_FOR_INTERRUPTS();

		state->htups += 1;

		/*
		 * "merge" by skipping through the index tuples until we find or pass
		 * the current heap tuple.
		 */
		while (!tuplesort_empty &&
			   (!indexcursor ||
				ItemPointerCompare(indexcursor, heapcursor) < 0))
		{
			Datum		ts_val;
			bool		ts_isnull;

			if (indexcursor)
				pfree(indexcursor);

			if(gp_enable_mk_sort)
				tuplesort_empty = !tuplesort_getdatum_mk((Tuplesortstate_mk *) state->tuplesort,
						true, &ts_val, &ts_isnull);
			else
				tuplesort_empty = !tuplesort_getdatum((Tuplesortstate *) state->tuplesort,
						true, &ts_val, &ts_isnull);

			Assert(tuplesort_empty || !ts_isnull);
			indexcursor = (ItemPointer) DatumGetPointer(ts_val);
		}

		if (tuplesort_empty ||
			ItemPointerCompare(indexcursor, heapcursor) > 0)
		{
			/*
			 * We've overshot which means this heap tuple is missing from the
			 * index, so insert it.
			 */
			bool		check_unique;

			MemoryContextReset(econtext->ecxt_per_tuple_memory);

			/* Set up for predicate or expression evaluation */
			ExecStoreGenericTuple(heapTuple, slot, false);

			/*
			 * In a partial index, discard tuples that don't satisfy the
			 * predicate.
			 */
			if (predicate != NIL)
			{
				if (!ExecQual(predicate, econtext, false))
					continue;
			}

			/*
			 * For the current heap tuple, extract all the attributes we use
			 * in this index, and note which are null.	This also performs
			 * evaluation of any expressions needed.
			 */
			FormIndexDatum(indexInfo,
						   slot,
						   estate,
						   values,
						   isnull);

			/*
			 * If the tuple is already committed dead, we still have to put it
			 * in the index (because some xacts might be able to see it), but
			 * we might as well suppress uniqueness checking. This is just an
			 * optimization because the index AM is not supposed to raise a
			 * uniqueness failure anyway.
			 */
			if (indexInfo->ii_Unique)
			{
				
				// -------- MirroredLock ----------
				MIRROREDLOCK_BUFMGR_LOCK;
				
				/* must hold a buffer lock to call HeapTupleSatisfiesNow */
				LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

				if (HeapTupleSatisfiesNow(scan->rs_rd, heapTuple->t_data, scan->rs_cbuf))
					check_unique = true;
				else
					check_unique = false;

				LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
				
				MIRROREDLOCK_BUFMGR_UNLOCK;
				// -------- MirroredLock ----------
				
			}
			else
				check_unique = false;

			/*
			 * You'd think we should go ahead and build the index tuple here,
			 * but some index AMs want to do further processing on the data
			 * first. So pass the values[] and isnull[] arrays, instead.
			 */
			index_insert(indexRelation,
						 values,
						 isnull,
						 heapcursor,
						 heapRelation,
						 check_unique);

			state->tups_inserted += 1;
		}
	}

	heap_endscan(scan);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NIL;
}


/*
 * IndexGetRelation: given an index's relation OID, get the OID of the
 * relation it is an index on.	Uses the system cache.
 */
Oid
IndexGetRelation(Oid indexId)
{
	HeapTuple	tuple;
	Form_pg_index index;
	Oid			result;
	cqContext  *pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_index "
				" WHERE indexrelid = :1 ",
				ObjectIdGetDatum(indexId)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for index %u", indexId);
	index = (Form_pg_index) GETSTRUCT(tuple);
	Assert(index->indexrelid == indexId);

	result = index->indrelid;

	caql_endscan(pcqCtx);
	return result;
}

/*
 * createIndexInfoOpaque: create the opaque value in indexInfo
 * based on the given list of OIDs passed from reindex_index().
 *
 * The extra_oids contains 2 OID values. They are used by
 * the bitmap indexes to create their internal heap and btree.
 * See reindex_index() for more info.
 */
static void
createIndexInfoOpaque(List *extra_oids,
					  bool isBitmapIndex,
					  IndexInfo *indexInfo)
{
	Assert(extra_oids != NULL &&
		   list_length(extra_oids) == 2);
	Assert(indexInfo != NULL);
	Assert(indexInfo->opaque == NULL);

	indexInfo->opaque = (void*)palloc0(sizeof(IndexInfoOpaque));
	
	ListCell *lc = list_head(extra_oids);

	((IndexInfoOpaque *)indexInfo->opaque)->heapRelfilenode =
		lfirst_oid(lc);
	lc = lnext(lc);
	((IndexInfoOpaque *)indexInfo->opaque)->indexRelfilenode =
		lfirst_oid(lc);
	lc = lnext(lc);

#ifdef USE_ASSERT_CHECKING
	if (isBitmapIndex)
	{
		Assert(OidIsValid(((IndexInfoOpaque *)indexInfo->opaque)->heapRelfilenode));
		Assert(OidIsValid(((IndexInfoOpaque *)indexInfo->opaque)->indexRelfilenode));
	}
	
	else
	{
		Assert(!OidIsValid(((IndexInfoOpaque *)indexInfo->opaque)->heapRelfilenode));
		Assert(!OidIsValid(((IndexInfoOpaque *)indexInfo->opaque)->indexRelfilenode));
	}
#endif
}

/*
 * generateExtraOids: generate the given number of extra Oids.
 *
 * If genNewOid is true, all generated OIDs will be valid. Otherwise,
 * all OIDs will be InvalidOid.
 */
static List *
generateExtraOids(int num_extra_oids,
				  Oid reltablespace,
				  bool relisshared,
				  bool genNewOid)
{
	List *extra_oids = NIL;
	
	Assert(num_extra_oids > 0);
	
	for (int no = 0; no < num_extra_oids; no++)
	{
		Oid newOid = InvalidOid;
		if (genNewOid)
			newOid = GetNewRelFileNode(reltablespace,
									   relisshared,
									   NULL,
									   false);
		
		extra_oids = lappend_oid(extra_oids, newOid);
	}

	return extra_oids;
}

/*
 * reindex_index - This routine is used to recreate a single index.
 *
 * GPDB: we return the new relfilenode for transmission to QEs. If
 * newrelfilenode is valid, we use that Oid instead.
 *
 * XXX The bitmap index requires two additional oids for its internal
 * heap and index. We pass those in as extra_oids. If there are no
 * such oids, this function generates them and pass them out to
 * the caller.
 *
 * The extra_oids list always contain 2 values. If the index is
 * a bitmap index, those two values are valid OIDs. Otherwise,
 * they are InvalidOids.
 */
Oid
reindex_index(Oid indexId, Oid newrelfilenode, List **extra_oids)
{
	Relation		iRel,
					heapRelation,
					pg_index;
	Oid				heapId;
	bool			inplace;
	HeapTuple		indexTuple;
	Form_pg_index	indexForm;
	Oid				retrelfilenode;
	Oid				namespaceId;
	cqContext		cqc;
	cqContext	   *pcqCtx;

	Assert(OidIsValid(indexId));
	Assert(extra_oids != NULL);

	/*
	 * Open and lock the parent heap relation.	ShareLock is sufficient since
	 * we only need to be sure no schema or data changes are going on.
	 */
	heapId = IndexGetRelation(indexId);
	heapRelation = heap_open(heapId, ShareLock);

	namespaceId = RelationGetNamespace(heapRelation);

	/*
	 * Open the target index relation and get an exclusive lock on it, to
	 * ensure that no one else is touching this particular index.
	 */
	iRel = index_open(indexId, AccessExclusiveLock);

	/*
	 * Don't allow reindex on temp tables of other backends ... their local
	 * buffer manager is not going to cope.
	 */
	if (isOtherTempNamespace(RelationGetNamespace(iRel)))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot reindex temporary tables of other sessions")));

	/*
	 * Also check for active uses of the index in the current transaction;
	 * we don't want to reindex underneath an open indexscan.
	 */
	CheckTableNotInUse(iRel, "REINDEX INDEX");

	/*
	 * If it's a shared index, we must do inplace processing (because we have
	 * no way to update relfilenode in other databases).  Otherwise we can do
	 * it the normal transaction-safe way.
	 *
	 * Since inplace processing isn't crash-safe, we only allow it in a
	 * standalone backend.	(In the REINDEX TABLE and REINDEX DATABASE cases,
	 * the caller should have detected this.)
	 *
	 * MPP: If we are in a standalone backend always perform reindex operations
	 * in place.  In postgres this only applies to shared relations, for 
	 * Greenplum we apply it to all tables as a means of enabling upgrade to
	 * filerep: it is required to reindex gp_relation_node in place before it
	 * is possible to populate the gp_persistent tables.
	 */
	inplace = iRel->rd_rel->relisshared || !IsUnderPostmaster;

	if (inplace && IsUnderPostmaster)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("shared index \"%s\" can only be reindexed in stand-alone mode",
						RelationGetRelationName(iRel))));

	PG_TRY();
	{
		IndexInfo  *indexInfo;

		/* Suppress use of the target index while rebuilding it */
		SetReindexProcessing(heapId, indexId);

		/* Fetch info needed for index_build */
		indexInfo = BuildIndexInfo(iRel);

		if (inplace)
		{
			/* Truncate the actual file (and discard buffers) */

			RelationTruncate(
						iRel, 
						0,
						/* markPersistentAsPhysicallyTruncated */ true);

			retrelfilenode = iRel->rd_rel->relfilenode;
			Assert(retrelfilenode == newrelfilenode ||
				   !OidIsValid(newrelfilenode));
		}
		else
		{
			/*
			 * We'll build a new physical relation for the index.
			 */
			if (OidIsValid(newrelfilenode))
			{
				setNewRelfilenodeToOid(iRel, newrelfilenode);
				retrelfilenode = newrelfilenode;
			}
			else
			{
				retrelfilenode = setNewRelfilenode(iRel);

				Assert(*extra_oids == NULL);

				/*
				 * If this is a bitmap index, we generate two more relfilenodes
				 * for its internal heap and index.
				 */
				*extra_oids = generateExtraOids(2,
												iRel->rd_rel->reltablespace,
												iRel->rd_rel->relisshared,
												RelationIsBitmapIndex(iRel));

			}
			

			/* Store extra_oids into indexInfo->opaque */
			createIndexInfoOpaque(*extra_oids,
								  RelationIsBitmapIndex(iRel),
								  indexInfo);
		}

		/* Initialize the index and rebuild */
		/* Note: we do not need to re-establish pkey setting */
		index_build(heapRelation, iRel, indexInfo, false);
	}
	PG_CATCH();
	{
		/* Make sure flag gets cleared on error exit */
		ResetReindexProcessing();
		PG_RE_THROW();
	}
	PG_END_TRY();
	ResetReindexProcessing();

	/*
	 * If the index is marked invalid (ie, it's from a failed CREATE INDEX
	 * CONCURRENTLY), we can now mark it valid.  This allows REINDEX to be
	 * used to clean up in such cases.
	 */
	pg_index = heap_open(IndexRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), pg_index);

	indexTuple = caql_getfirst(pcqCtx,
							   cql("SELECT * FROM pg_index "
								   " WHERE indexrelid = :1 "
								   " FOR UPDATE ",
								   ObjectIdGetDatum(indexId)));

	if (!HeapTupleIsValid(indexTuple))
		elog(ERROR, "cache lookup failed for index %u", indexId);
	indexForm = (Form_pg_index) GETSTRUCT(indexTuple);

	if (!indexForm->indisvalid)
	{
		indexForm->indisvalid = true;
		caql_update_current(pcqCtx, indexTuple);
		/* and Update indexes (implicit) */
	}
	heap_close(pg_index, RowExclusiveLock);

	{
		bool	 doIt	= true;
		char	*subtyp = "REINDEX";

		/* MPP-7576: don't track internal namespace tables */
		switch (namespaceId) 
		{
			case PG_CATALOG_NAMESPACE:
				/* MPP-7773: don't track objects in system namespace
				 * if modifying system tables (eg during upgrade)  
				 */
				if (allowSystemTableModsDDL)
					doIt = false;
				break;

			case PG_TOAST_NAMESPACE:
			case PG_BITMAPINDEX_NAMESPACE:
			case PG_AOSEGMENT_NAMESPACE:
				doIt = false;
				break;
			default:
				break;
		}

		if (doIt)
			doIt = (!(isAnyTempNamespace(namespaceId)));

		/* MPP-6929: metadata tracking */
		/* MPP-7587: treat as a VACUUM operation, since the index is
		 * rebuilt */
		if (doIt)
			MetaTrackUpdObject(RelationRelationId,
							   indexId,
							   GetUserId(), /* not ownerid */
							   "VACUUM", subtyp
					);
	}


	/* Close rels, but keep locks */
	index_close(iRel, NoLock);
	heap_close(heapRelation, NoLock);

	return retrelfilenode;
}

/*
 * reindex_relation - This routine is used to recreate all indexes
 * of a relation (and optionally its toast relation too, if any).
 *
 * Returns true if any indexes were rebuilt.  Note that a
 * CommandCounterIncrement will occur after each index rebuild.
 *
 * If build_map is true, build a map of index relation OID -> new relfilenode.
 * If it is false but *oidmap is valid and we're on a QE, use the
 * new relfilenode specified in the map.
 */
bool
reindex_relation(Oid relid, bool toast_too, bool aoseg_too, bool aoblkdir_too,
				 List **oidmap, bool build_map)
{
	Relation	rel;
	Oid			toast_relid;
	Oid			aoseg_relid = InvalidOid;
	Oid         aoblkdir_relid = InvalidOid;
	bool		is_pg_class;
	bool		result;
	List	   *indexIds,
			   *doneIndexes;
	ListCell   *indexId;
	bool relIsAO = false;

	/*
	 * Open and lock the relation.	ShareLock is sufficient since we only need
	 * to prevent schema and data changes in it.
	 */
	rel = heap_open(relid, ShareLock);

	relIsAO = (RelationIsAoRows(rel) || RelationIsParquet(rel));

	toast_relid = rel->rd_rel->reltoastrelid;

	/*
	 * Get the list of index OIDs for this relation.  (We trust to the
	 * relcache to get this with a sequential scan if ignoring system
	 * indexes.)
	 */
	indexIds = RelationGetIndexList(rel);

	/*
	 * reindex_index will attempt to update the pg_class rows for the relation
	 * and index.  If we are processing pg_class itself, we want to make sure
	 * that the updates do not try to insert index entries into indexes we
	 * have not processed yet.	(When we are trying to recover from corrupted
	 * indexes, that could easily cause a crash.) We can accomplish this
	 * because CatalogUpdateIndexes will use the relcache's index list to know
	 * which indexes to update. We just force the index list to be only the
	 * stuff we've processed.
	 *
	 * It is okay to not insert entries into the indexes we have not processed
	 * yet because all of this is transaction-safe.  If we fail partway
	 * through, the updated rows are dead and it doesn't matter whether they
	 * have index entries.	Also, a new pg_class index will be created with an
	 * entry for its own pg_class row because we do setNewRelfilenode() before
	 * we do index_build().
	 *
	 * Note that we also clear pg_class's rd_oidindex until the loop is done,
	 * so that that index can't be accessed either.  This means we cannot
	 * safely generate new relation OIDs while in the loop; shouldn't be a
	 * problem.
	 */
	is_pg_class = (RelationGetRelid(rel) == RelationRelationId);
	doneIndexes = NIL;

	/* Reindex all the indexes. */
	foreach(indexId, indexIds)
	{
		Oid			indexOid = lfirst_oid(indexId);
		Oid			newrelfilenode;
		Oid			mapoid = InvalidOid;
		List        *extra_oids = NIL;

		if (is_pg_class)
			RelationSetIndexList(rel, doneIndexes, InvalidOid);

		if (Gp_role == GP_ROLE_EXECUTE && !build_map && oidmap &&
			*oidmap)
		{
			ListCell *c;

			/* Yes, this is O(N^2) but N is small */
			foreach(c, *oidmap)
			{
				List *map = lfirst(c);
				Oid ind = linitial_oid(map);

				if (ind == indexOid)
				{
					mapoid = lsecond_oid(map);
					
					/*
					 * The map should contain more than 2 OIDs (the OID of the
					 * index and its new relfilenode), to support the bitmap
					 * index, see reindex_index() for more info. Construct
					 * the extra_oids list by skipping the first two OIDs.
					 */
					Assert(list_length(map) > 2);
					extra_oids = list_copy_tail(map, 2);

					break;
				}
			}
			Assert(OidIsValid(mapoid));
		}

		elog(DEBUG5, "reindexing index with OID %u (supplied %u as new OID)",
			 indexOid, mapoid);

		newrelfilenode = reindex_index(indexOid, mapoid, &extra_oids);

		Assert(!OidIsValid(mapoid) || newrelfilenode == mapoid);

		CommandCounterIncrement();

		if (oidmap && build_map)
		{
			List *map = list_make2_oid(indexOid, newrelfilenode);

			Assert(extra_oids != NULL);
			map = list_concat(map, extra_oids);

			*oidmap = lappend(*oidmap, map);
		}

		if (is_pg_class)
			doneIndexes = lappend_oid(doneIndexes, indexOid);
	}

	if (is_pg_class)
		RelationSetIndexList(rel, indexIds, ClassOidIndexId);

	/*
	 * Close rel, but continue to hold the lock.
	 */
	heap_close(rel, NoLock);

	result = (indexIds != NIL);

	/*
	 * If the relation has a secondary toast rel, reindex that too while we
	 * still hold the lock on the master table.
	 */
	if (toast_too && OidIsValid(toast_relid))
		result |= reindex_relation(toast_relid, false, false, false, oidmap, build_map);

	/* Obtain the aoseg_relid and aoblkdir_relid if the relation is an AO table. */
	if ((aoseg_too || aoblkdir_too) && relIsAO)
		GetAppendOnlyEntryAuxOids(relid, SnapshotNow,
								  &aoseg_relid, NULL, &aoblkdir_relid, NULL);

	/*
	 * If an AO rel has a secondary segment list rel, reindex that too while we
	 * still hold the lock on the master table.
	 */
	if (aoseg_too && OidIsValid(aoseg_relid))
		result |= reindex_relation(aoseg_relid, false, false, false, oidmap, build_map);

	/*
	 * If an AO rel has a secondary block directory rel, reindex that too while we
	 * still hold the lock on the master table.
	 */
	if (aoblkdir_too && OidIsValid(aoblkdir_relid))
		result |= reindex_relation(aoblkdir_relid, false, false, false, oidmap, build_map);

	return result;
}
