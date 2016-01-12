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
 * indexcmds.c
 *	  POSTGRES define and remove index code.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/commands/indexcmds.c,v 1.149.2.2 2007/09/10 22:02:05 alvherre Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_resqueue.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "cdb/cdbpartition.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parsetree.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbcat.h"
#include "cdb/cdbrelsize.h"
#include "cdb/cdboidsync.h"
#include "cdb/dispatcher.h"


/* non-export function prototypes */
static void CheckPredicate(Expr *predicate);
static void ComputeIndexAttrs(IndexInfo *indexInfo, Oid *classOidP,
				  List *attList,
				  Oid relId,
				  char *accessMethodName, Oid accessMethodId,
				  bool isconstraint);
static Oid GetIndexOpClass(List *opclass, Oid attrType,
				char *accessMethodName, Oid accessMethodId);
static bool relationHasPrimaryKey(Relation rel);
static bool relationHasUniqueIndex(Relation rel);

bool gp_hash_index = false; /* hash index phase out. */

/*
 * DefineIndex
 *		Creates a new index.
 *
 * 'relationId': the OID of the heap relation on which the index is to be
 *     created
 * 'indexRelationName': the name for the new index, or NULL to indicate
 *		that a nonconflicting default name should be picked.
 * 'indexRelationId': normally InvalidOid, but during bootstrap can be
 *		nonzero to specify a preselected OID for the index.
 * 'accessMethodName': name of the AM to use.
 * 'tableSpaceName': name of the tablespace to create the index in.
 *		NULL specifies using the appropriate default.
 * 'attributeList': a list of IndexElem specifying columns and expressions
 *		to index on.
 * 'predicate': the partial-index condition, or NULL if none.
 * 'rangetable': needed to interpret the predicate.
 * 'options': reloptions from WITH (in list-of-DefElem form).
 * 'unique': make the index enforce uniqueness.
 * 'primary': mark the index as a primary key in the catalogs.
 * 'isconstraint': index is for a PRIMARY KEY or UNIQUE constraint,
 *		so build a pg_constraint entry for it.
 * 'is_alter_table': this is due to an ALTER rather than a CREATE operation.
 * 'check_rights': check for CREATE rights in the namespace.  (This should
 *		be true except when ALTER is deleting/recreating an index.)
 * 'skip_build': make the catalog entries but leave the index file empty;
 *		it will be filled later.
 * 'quiet': suppress the NOTICE chatter ordinarily provided for constraints.
 * 'concurrent': avoid blocking writers to the table while building.
 * 'part_expanded': is this a lower-level call to index a branch or part of
 *		a partitioned table?
 * 'stmt': the IndexStmt for this index.  Many other arguments are just values
 *		of fields in here.  
 *		XXX One day it might pay to eliminate the redundancy.
 */
void
DefineIndex(Oid relationId,
			char *indexRelationName,
			Oid indexRelationId,
			char *accessMethodName,
			char *tableSpaceName,
			List *attributeList,
			Expr *predicate,
			List *rangetable,
			List *options,
			bool unique,
			bool primary,
			bool isconstraint,
			bool is_alter_table,
			bool check_rights,
			bool skip_build,
			bool quiet,
			bool concurrent,
			bool part_expanded,
			IndexStmt *stmt)
{
	Oid		   *classObjectId;
	Oid			accessMethodId;
	Oid			namespaceId;
	Oid			tablespaceId;
	Relation	rel;
	HeapTuple	tuple;
	Form_pg_am	accessMethodForm;
	RegProcedure amoptions;
	Datum		reloptions;
	IndexInfo  *indexInfo;
	int			numberOfAttributes;
	List	   *old_xact_list;
	ListCell   *lc;
	uint32		ixcnt;
	LockRelId	heaprelid;
	LOCKTAG		heaplocktag;
	Snapshot	snapshot;
	Relation	pg_index;
	HeapTuple	indexTuple;
	Form_pg_index indexForm;
	LOCKMODE	heap_lockmode;
	bool		need_longlock = true;
	bool		shouldDispatch = Gp_role == GP_ROLE_DISPATCH && !IsBootstrapProcessingMode();
	char	   *altconname = stmt ? stmt->altconname : NULL;
	cqContext	cqc;
	cqContext  *pcqCtx;
	cqContext  *amcqCtx;
	cqContext  *attcqCtx;

	/*
	 * count attributes in index
	 */
	numberOfAttributes = list_length(attributeList);
	if (numberOfAttributes <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("must specify at least one column")));
	if (numberOfAttributes > INDEX_MAX_KEYS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("cannot use more than %d columns in an index",
						INDEX_MAX_KEYS)));

	/*
	 * Only SELECT ... FOR UPDATE/SHARE are allowed while doing a standard
	 * index build; but for concurrent builds we allow INSERT/UPDATE/DELETE
	 * (but not VACUUM).
	 *
	 * NB: Caller is responsible for making sure that relationId refers
	 * to the relation on which the index should be built; except in bootstrap
	 * mode, this will typically require the caller to have already locked
	 * the relation.  To avoid lock upgrade hazards, that lock should be at
	 * least as strong as the one we take here.
	 */
	heap_lockmode = concurrent ? ShareUpdateExclusiveLock : ShareLock;
	rel = heap_open(relationId, heap_lockmode);

	relationId = RelationGetRelid(rel);
	namespaceId = RelationGetNamespace(rel);

	if(RelationIsExternal(rel))
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot create indexes on external tables.")));
		
	/* Note: during bootstrap may see uncataloged relation */
	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_UNCATALOGED)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table",
						RelationGetRelationName(rel)),
				 errOmitLocation(true)));

	/*
	 * Don't try to CREATE INDEX on temp tables of other backends.
	 */
	if (isOtherTempNamespace(namespaceId))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot create indexes on temporary tables of other sessions"),
				 errOmitLocation(true)));

	/*
	 * Verify we (still) have CREATE rights in the rel's namespace.
	 * (Presumably we did when the rel was created, but maybe not anymore.)
	 * Skip check if caller doesn't want it.  Also skip check if
	 * bootstrapping, since permissions machinery may not be working yet.
	 */
	if (check_rights && !IsBootstrapProcessingMode())
	{
		AclResult	aclresult;

		aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(),
										  ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
						   get_namespace_name(namespaceId));
	}

	/*
	 * Select tablespace to use.  If not specified, use default_tablespace
	 * (which may in turn default to database's default).
	 *
	 * Note: This code duplicates code in tablecmds.c
	 */
	if (tableSpaceName)
	{
		tablespaceId = get_tablespace_oid(tableSpaceName);
		if (!OidIsValid(tablespaceId))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("tablespace \"%s\" does not exist",
							tableSpaceName),
					 errOmitLocation(true)));
		CheckCrossAccessTablespace(tablespaceId);
	}
	else
	{
		tablespaceId = GetDefaultTablespace();

		/* Need the real tablespace id for dispatch */
		if (!OidIsValid(tablespaceId)) 
			tablespaceId = MyDatabaseTableSpace;
		else
			CheckCrossAccessTablespace(tablespaceId);

		/* 
		 * MPP-8238 : inconsistent tablespaces between segments and master 
		 */
		if (shouldDispatch)
			stmt->tableSpace = get_tablespace_name(tablespaceId);
	}

	/* Goh tablespace check. */
	RejectAccessTablespace(tablespaceId, "cannot create index on tablespace %s");

	/* Check permissions except when using database's default */
	if (OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace &&
		tablespaceId != get_database_dts(MyDatabaseId))
	{
		AclResult	aclresult;

		aclresult = pg_tablespace_aclcheck(tablespaceId, GetUserId(),
										   ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
						   get_tablespace_name(tablespaceId));
	}

	/*
	 * Force shared indexes into the pg_global tablespace.	This is a bit of a
	 * hack but seems simpler than marking them in the BKI commands.
	 */
	if (rel->rd_rel->relisshared)
		tablespaceId = GLOBALTABLESPACE_OID;

	/*
	 * Select name for index if caller didn't specify
	 */
	if (indexRelationName == NULL)
	{
		if (primary)
			indexRelationName = ChooseRelationName(RelationGetRelationName(rel),
												   NULL,
												   "pkey",
												   namespaceId,
												   NULL);
		else
		{
			IndexElem  *iparam = (IndexElem *) linitial(attributeList);

			indexRelationName = ChooseRelationName(RelationGetRelationName(rel),
												   iparam->name,
												   "key",
												   namespaceId,
												   NULL);
		}
		stmt->idxname = indexRelationName;
	}

	/*
	 * look up the access method, verify it can handle the requested features
	 */
	amcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_am "
				" WHERE amname = :1 ",
				PointerGetDatum(accessMethodName)));
	
	tuple = caql_getnext(amcqCtx);

	if (!HeapTupleIsValid(tuple))
	{
		caql_endscan(amcqCtx);

		/*
		 * Hack to provide more-or-less-transparent updating of old RTREE
		 * indexes to GIST: if RTREE is requested and not found, use GIST.
		 */
		if (strcmp(accessMethodName, "rtree") == 0)
		{
			ereport(NOTICE,
					(errmsg("substituting access method \"gist\" for obsolete method \"rtree\"")));
			accessMethodName = "gist";

			amcqCtx = caql_beginscan(
					NULL,
					cql("SELECT * FROM pg_am "
						" WHERE amname = :1 ",
						PointerGetDatum(accessMethodName)));
	
			tuple = caql_getnext(amcqCtx);
		}

		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("access method \"%s\" does not exist",
							accessMethodName)));
	}
	accessMethodId = HeapTupleGetOid(tuple);
	accessMethodForm = (Form_pg_am) GETSTRUCT(tuple);

	if (accessMethodId == HASH_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hash indexes are not supported")));

	/* MPP-9329: disable creation of GIN indexes */
	if (accessMethodId == GIN_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("GIN indexes are not supported")));

	if (unique && !accessMethodForm->amcanunique)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("access method \"%s\" does not support unique indexes",
						accessMethodName)));
	if (numberOfAttributes > 1 && !accessMethodForm->amcanmulticol)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("access method \"%s\" does not support multicolumn indexes",
						accessMethodName)));

    if  (unique && (RelationIsAoRows(rel) || RelationIsParquet(rel)))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("append-only tables do not support unique indexes")));

	amoptions = accessMethodForm->amoptions;

	caql_endscan(amcqCtx);

	/*
	 * If a range table was created then check that only the base rel is
	 * mentioned.
	 */
	if (rangetable != NIL)
	{
		if (list_length(rangetable) != 1 || getrelid(1, rangetable) != relationId)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("index expressions and predicates may refer only to the table being indexed")));
	}

	/*
	 * Validate predicate, if given
	 */
	if (predicate)
		CheckPredicate(predicate);

	/*
	 * Extra checks when creating a PRIMARY KEY index.
	 */
	if (primary)
	{
		List	   *cmds;
		ListCell   *keys;

		/*
		 * If ALTER TABLE, check that there isn't already a PRIMARY KEY. In
		 * CREATE TABLE, we have faith that the parser rejected multiple pkey
		 * clauses; and CREATE INDEX doesn't have a way to say PRIMARY KEY, so
		 * it's no problem either.
		 */
		if (is_alter_table &&
			relationHasPrimaryKey(rel))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("multiple primary keys for table \"%s\" are not allowed",
							RelationGetRelationName(rel))));
		}

		/*
		 * Check that all of the attributes in a primary key are marked as not
		 * null, otherwise attempt to ALTER TABLE .. SET NOT NULL
		 */
		cmds = NIL;
		foreach(keys, attributeList)
		{
			IndexElem  *key = (IndexElem *) lfirst(keys);
			HeapTuple	atttuple;

			if (!key->name)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("primary keys cannot be expressions")));

			/* System attributes are never null, so no problem */
			if (SystemAttributeByName(key->name, rel->rd_rel->relhasoids))
				continue;

			attcqCtx = caql_getattname_scan(NULL, relationId, key->name);
			atttuple = caql_get_current(attcqCtx);

			if (HeapTupleIsValid(atttuple))
			{
				if (!((Form_pg_attribute) GETSTRUCT(atttuple))->attnotnull)
				{
					/* Add a subcommand to make this one NOT NULL */
					AlterTableCmd *cmd = makeNode(AlterTableCmd);

					cmd->subtype = AT_SetNotNull;
					cmd->name = key->name;
					cmd->part_expanded = true;

					cmds = lappend(cmds, cmd);
				}
			}
			else
			{
				/*
				 * This shouldn't happen during CREATE TABLE, but can happen
				 * during ALTER TABLE.	Keep message in sync with
				 * transformIndexConstraints() in parser/analyze.c.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" named in key does not exist",
								key->name)));
			}
			caql_endscan(attcqCtx);
		}

		/*
		 * XXX: Shouldn't the ALTER TABLE .. SET NOT NULL cascade to child
		 * tables?	Currently, since the PRIMARY KEY itself doesn't cascade,
		 * we don't cascade the notnull constraint(s) either; but this is
		 * pretty debatable.
		 *
		 * XXX: possible future improvement: when being called from ALTER
		 * TABLE, it would be more efficient to merge this with the outer
		 * ALTER TABLE, so as to avoid two scans.  But that seems to
		 * complicate DefineIndex's API unduly.
		 */
		if (cmds)
			AlterTableInternal(relationId, cmds, false);
	}

  	/*
 	 * Parse AM-specific options, convert to text array form, validate
 	 *
	 * However, accept and only accept tidycat option during upgrade.
	 * During bootstrap, we don't have any storage option. So, during
	 * upgrade, we don't need it as well because we're just creating
	 * catalog objects. Further, we overload the WITH clause to pass-in
	 * the index oid. So, if we don't strip it out, it'll appear in
	 * the pg_class.reloptions, and we don't want that.
	 */
 	reloptions = transformRelOptions((Datum) 0, options, false, false);
	if (gp_upgrade_mode)
 	{
 		TidycatOptions *tidycatoptions = (TidycatOptions*) tidycat_reloptions(reloptions);
 		indexRelationId = tidycatoptions->indexid;
 		reloptions = 0;
 	}
 	else
 		(void) index_reloptions(amoptions, reloptions, true);

	/*
	 * Prepare arguments for index_create, primarily an IndexInfo structure.
	 * Note that ii_Predicate must be in implicit-AND format.
	 */
	indexInfo = makeNode(IndexInfo);
	indexInfo->ii_NumIndexAttrs = numberOfAttributes;
	indexInfo->ii_Expressions = NIL;	/* for now */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_Predicate = make_ands_implicit(predicate);
	indexInfo->ii_PredicateState = NIL;
	indexInfo->ii_Unique = unique;
	indexInfo->ii_Concurrent = concurrent;
	indexInfo->opaque = (void*)palloc0(sizeof(IndexInfoOpaque));

	classObjectId = (Oid *) palloc(numberOfAttributes * sizeof(Oid));
	ComputeIndexAttrs(indexInfo, classObjectId, attributeList,
					  relationId, accessMethodName, accessMethodId,
					  isconstraint);

	if (shouldDispatch)
	{
		if (stmt)
		{
			Assert(stmt->idxOids == 0);
			stmt->idxOids = NIL;
		}

		if ((primary || unique) && rel->rd_cdbpolicy)
			checkPolicyForUniqueIndex(rel,
									  indexInfo->ii_KeyAttrNumbers,
									  indexInfo->ii_NumIndexAttrs,
									  primary,
									  list_length(indexInfo->ii_Expressions),
									  relationHasPrimaryKey(rel),
									  relationHasUniqueIndex(rel));
		
		/* We don't have to worry about constraints on parts.  Already checked. */
		if ( isconstraint && rel_is_partitioned(relationId) )
			checkUniqueConstraintVsPartitioning(rel,
												indexInfo->ii_KeyAttrNumbers,
												indexInfo->ii_NumIndexAttrs,
												primary);

	}
	else if (Gp_role == GP_ROLE_EXECUTE)
	{
		if (stmt)
		{
			IndexInfoOpaque *iio = (IndexInfoOpaque *)indexInfo->opaque;

			/* stmt->idxOids can have 7 oids currently. */
			Assert(list_length(stmt->idxOids) == 7);

 			indexRelationId = linitial_oid(stmt->idxOids);
 			iio->comptypeOid = lsecond_oid(stmt->idxOids);
 			iio->heapOid = lthird_oid(stmt->idxOids);
 			iio->indexOid = lfourth_oid(stmt->idxOids);
			iio->blkdirRelOid = lfifth_oid(stmt->idxOids);
			iio->blkdirIdxOid = lfirst_oid(lnext(lcfifth(stmt->idxOids)));
			iio->blkdirComptypeOid = lfirst_oid(lnext(lnext(lcfifth(stmt->idxOids))));


			/* Not all Oids are used (and therefore unset) during upgrade index
			 * creation. So, skip the Oid assert during upgrade.
			 *
			 * In normal operations we proactively allocate a bunch of oids to support
			 * bitmap indexes and ao indexes, however in bootstrap/upgrade mode when we
			 * create an index using a supplied oid we do not allocate all these
			 * additional oids. (See the "ShouldDispatch" block below). This implies that
			 * we cannot currently support bitmap indexes or ao indexes as part of the catalog.
			 */
			Insist(OidIsValid(indexRelationId));
			if (!gp_upgrade_mode)
			{
	 			Insist(OidIsValid(iio->comptypeOid));
	 			Insist(OidIsValid(iio->heapOid));
	 			Insist(OidIsValid(iio->indexOid));
	 			Insist(OidIsValid(iio->blkdirRelOid));
	 			Insist(OidIsValid(iio->blkdirIdxOid));
	 			Insist(OidIsValid(iio->blkdirComptypeOid));
			}

			quiet = true;
		}
	}

	/*
	 * Report index creation if appropriate (delay this till after most of the
	 * error checks)
	 */
	if (isconstraint && !quiet)
		if (Gp_role != GP_ROLE_EXECUTE)
			ereport(NOTICE,
					(errmsg("%s %s will create implicit index \"%s\" for table \"%s\"",
							is_alter_table ? "ALTER TABLE / ADD" : "CREATE TABLE /",
							primary ? "PRIMARY KEY" : "UNIQUE",
							indexRelationName, RelationGetRelationName(rel))));

	if (rel_needs_long_lock(RelationGetRelid(rel)))
		need_longlock = true;
	else
		/* if this is a concurrent build, we must lock you long time */
		need_longlock = (false || concurrent);

   	if (shouldDispatch)
	{
		IndexInfoOpaque *iiopaque = (IndexInfoOpaque*)(indexInfo->opaque);

		if (!OidIsValid(indexRelationId))
		{
			Relation pg_class;
			Relation pg_type;

			/**
			 * In HAWQ, only the master have the catalog information.
			 * So, no need to sync oid to segments.
			 */
			/* cdb_sync_oid_to_segments(); */

			pg_class = heap_open(RelationRelationId, RowExclusiveLock);

			indexRelationId = GetNewRelFileNode(tablespaceId, false, pg_class, false);
		    iiopaque->heapOid = GetNewRelFileNode(tablespaceId, false, pg_class, false);
			iiopaque->indexOid = GetNewRelFileNode(tablespaceId, false, pg_class, false);
		    iiopaque->blkdirRelOid = GetNewRelFileNode(tablespaceId, false, pg_class, false);
			iiopaque->blkdirIdxOid = GetNewRelFileNode(tablespaceId, false, pg_class, false);

			/* done with pg_class */
			heap_close(pg_class, NoLock);

			pg_type = heap_open(TypeRelationId, RowExclusiveLock);
			iiopaque->comptypeOid = GetNewOid(pg_type);
			iiopaque->blkdirComptypeOid = GetNewOid(pg_type);
			heap_close(pg_type, NoLock);

		}

		/* create the index on the QEs first, so we can get their stats when we create on the QD */
		if (stmt)
		{
			Assert(stmt->idxOids == 0 ||
				   stmt->idxOids == (List*)NULL);
			stmt->idxOids = NIL;
			stmt->idxOids = lappend_oid(stmt->idxOids, indexRelationId);
			stmt->idxOids = lappend_oid(stmt->idxOids, iiopaque->comptypeOid);
			stmt->idxOids = lappend_oid(stmt->idxOids, iiopaque->heapOid);
			stmt->idxOids = lappend_oid(stmt->idxOids, iiopaque->indexOid);
			stmt->idxOids = lappend_oid(stmt->idxOids, iiopaque->blkdirRelOid);
			stmt->idxOids = lappend_oid(stmt->idxOids, iiopaque->blkdirIdxOid);
			stmt->idxOids = lappend_oid(stmt->idxOids, iiopaque->blkdirComptypeOid);
		}

		/*
		 * Lock the index relation exclusively on the QD, before dispatching,
		 * otherwise we could get a deadlock between QEs trying to do this same work.
		 *
		 * MPP-4889
		 * NOTE: we also have to do the local create before dispatching, otherwise
		 * competing attempts to create the same index deadlock.
		 *
		 * Don't do this for partition children.
		 */
		if (need_longlock)
			LockRelationOid(indexRelationId, AccessExclusiveLock);

		/*
		 * We defer the dispatch of the utility command until after
		 * index_create(), because that call will *wait*
		 * for any other transactions touching this new relation,
		 * which can cause a non-local deadlock if we've already
		 * dispatched
		 */
		indexRelationId =
			index_create(relationId, indexRelationName, indexRelationId,
						 indexInfo, accessMethodId, tablespaceId, classObjectId,
						 reloptions, primary, isconstraint, &(stmt->constrOid),
						 allowSystemTableModsDDL, skip_build, concurrent, altconname);

        /*
         * Dispatch the command to all primary and mirror segment dbs.
         * Start a global transaction and reconfigure cluster if needed.
         * Wait for QEs to finish.  Exit via ereport(ERROR,...) if error.
         */
        if (stmt->concurrent)
        {
			/* In hawq, there is no local index, so no need to support this feature. */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("create index concurrent is not support"),
							   errOmitLocation(true)));
        }
        else
		{
        		dispatch_statement_node((Node *)stmt, NULL, NULL, NULL);
		}
	}

	/* save lockrelid for below, then close rel */
	heaprelid = rel->rd_lockInfo.lockRelId;
	if (need_longlock)
		heap_close(rel, NoLock);
	else
		heap_close(rel, heap_lockmode);

	if (!shouldDispatch)
	{
		indexRelationId =
			index_create(relationId, indexRelationName, indexRelationId,
						 indexInfo, accessMethodId, tablespaceId, classObjectId,
						 reloptions, primary, isconstraint, &(stmt->constrOid),
						 allowSystemTableModsDDL, skip_build, concurrent, altconname);
	}

	if (!concurrent)
		return;					/* We're done, in the standard case */

	/*
	 * Phase 2 of concurrent index build (see comments for validate_index()
	 * for an overview of how this works)
	 *
	 * We must commit our current transaction so that the index becomes
	 * visible; then start another.  Note that all the data structures we just
	 * built are lost in the commit.  The only data we keep past here are the
	 * relation IDs.
	 *
	 * Before committing, get a session-level lock on the table, to ensure
	 * that neither it nor the index can be dropped before we finish. This
	 * cannot block, even if someone else is waiting for access, because we
	 * already have the same lock within our transaction.
	 *
	 * Note: we don't currently bother with a session lock on the index,
	 * because there are no operations that could change its state while we
	 * hold lock on the parent table.  This might need to change later.
	 */
	LockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);

	CommitTransactionCommand();
	StartTransactionCommand();

	/*
	 * Now we must wait until no running transaction could have the table open
	 * with the old list of indexes.  To do this, inquire which xacts
	 * currently would conflict with ShareLock on the table -- ie, which ones
	 * have a lock that permits writing the table.	Then wait for each of
	 * these xacts to commit or abort.	Note we do not need to worry about
	 * xacts that open the table for writing after this point; they will see
	 * the new index when they open it.
	 *
	 * Note: GetLockConflicts() never reports our own xid, hence we need not
	 * check for that.
	 */
	SET_LOCKTAG_RELATION(heaplocktag, heaprelid.dbId, heaprelid.relId);
	old_xact_list = GetLockConflicts(&heaplocktag, ShareLock);

	foreach(lc, old_xact_list)
	{
		TransactionId xid = lfirst_xid(lc);

		XactLockTableWait(xid);
	}

	/*
	 * Now take the "reference snapshot" that will be used by validate_index()
	 * to filter candidate tuples.	All other transactions running at this
	 * time will have to be out-waited before we can commit, because we can't
	 * guarantee that tuples deleted just before this will be in the index.
	 *
	 * We also set ActiveSnapshot to this snap, since functions in indexes may
	 * need a snapshot.
	 */
	snapshot = CopySnapshot(GetTransactionSnapshot());
	ActiveSnapshot = snapshot;

	/*
	 * Scan the index and the heap, insert any missing index entries.
	 */
	validate_index(relationId, indexRelationId, snapshot);

	/*
	 * The index is now valid in the sense that it contains all currently
	 * interesting tuples.	But since it might not contain tuples deleted just
	 * before the reference snap was taken, we have to wait out any
	 * transactions older than the reference snap.	We can do this by waiting
	 * for each xact explicitly listed in the snap.
	 *
	 * Note: GetSnapshotData() never stores our own xid into a snap, hence we
	 * need not check for that.
	 */
	for (ixcnt = 0; ixcnt < snapshot->xcnt; ixcnt++)
		XactLockTableWait(snapshot->xip[ixcnt]);

	/* Index can now be marked valid -- update its pg_index entry */
	pg_index = heap_open(IndexRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), pg_index);

	indexTuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_index "
				" WHERE indexrelid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(indexRelationId)));

	if (!HeapTupleIsValid(indexTuple))
		elog(ERROR, "cache lookup failed for index %u", indexRelationId);
	indexForm = (Form_pg_index) GETSTRUCT(indexTuple);

	Assert(indexForm->indexrelid = indexRelationId);
	Assert(!indexForm->indisvalid);

	indexForm->indisvalid = true;

	caql_update_current(pcqCtx, indexTuple); 
	/* and Update indexes (implicit) */

	heap_close(pg_index, RowExclusiveLock);

	/*
	 * Last thing to do is release the session-level lock on the parent table.
	 */
	UnlockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);
}


/*
 * CheckPredicate
 *		Checks that the given partial-index predicate is valid.
 *
 * This used to also constrain the form of the predicate to forms that
 * indxpath.c could do something with.	However, that seems overly
 * restrictive.  One useful application of partial indexes is to apply
 * a UNIQUE constraint across a subset of a table, and in that scenario
 * any evaluatable predicate will work.  So accept any predicate here
 * (except ones requiring a plan), and let indxpath.c fend for itself.
 */
static void
CheckPredicate(Expr *predicate)
{
	/*
	 * We don't currently support generation of an actual query plan for a
	 * predicate, only simple scalar expressions; hence these restrictions.
	 */
	if (contain_subplans((Node *) predicate))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use subquery in index predicate"),
						   errOmitLocation(true)));
	if (contain_agg_clause((Node *) predicate))
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg("cannot use aggregate in index predicate"),
						   errOmitLocation(true)));
	if (checkExprHasWindFuncs((Node *)predicate))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot use window function in index predicate"),
						   errOmitLocation(true)));
	/*
	 * A predicate using mutable functions is probably wrong, for the same
	 * reasons that we don't allow an index expression to use one.
	 */
	if (contain_mutable_functions((Node *) predicate))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
		   errmsg("functions in index predicate must be marked IMMUTABLE"),
				   errOmitLocation(true)));
}

static void
ComputeIndexAttrs(IndexInfo *indexInfo,
				  Oid *classOidP,
				  List *attList,	/* list of IndexElem's */
				  Oid relId,
				  char *accessMethodName,
				  Oid accessMethodId,
				  bool isconstraint)
{
	ListCell   *rest;
	int			attn = 0;

	/*
	 * process attributeList
	 */
	foreach(rest, attList)
	{
		IndexElem  *attribute = (IndexElem *) lfirst(rest);
		Oid			atttype;

		if (attribute->name != NULL)
		{
			/* Simple index attribute */
			HeapTuple	atttuple;
			Form_pg_attribute attform;
			cqContext	*pcqCtx;

			Assert(attribute->expr == NULL);

			pcqCtx = caql_getattname_scan(NULL, relId, attribute->name);
			atttuple = caql_get_current(pcqCtx);

			if (!HeapTupleIsValid(atttuple))
			{
				/* difference in error message spellings is historical */
				if (isconstraint)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
						  errmsg("column \"%s\" named in key does not exist",
								 attribute->name)));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									attribute->name)));
			}
			attform = (Form_pg_attribute) GETSTRUCT(atttuple);
			indexInfo->ii_KeyAttrNumbers[attn] = attform->attnum;
			atttype = attform->atttypid;

			caql_endscan(pcqCtx);
		}
		else if (attribute->expr && IsA(attribute->expr, Var))
		{
			/* Tricky tricky, he wrote (column) ... treat as simple attr */
			Var		   *var = (Var *) attribute->expr;

			indexInfo->ii_KeyAttrNumbers[attn] = var->varattno;
			atttype = get_atttype(relId, var->varattno);
		}
		else
		{
			/* Index expression */
			Assert(attribute->expr != NULL);
			indexInfo->ii_KeyAttrNumbers[attn] = 0;		/* marks expression */
			indexInfo->ii_Expressions = lappend(indexInfo->ii_Expressions,
												attribute->expr);
			atttype = exprType(attribute->expr);

			/*
			 * We don't currently support generation of an actual query plan
			 * for an index expression, only simple scalar expressions; hence
			 * these restrictions.
			 */
			if (contain_subplans(attribute->expr))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot use subquery in index expression")));
			if (contain_agg_clause(attribute->expr))
				ereport(ERROR,
						(errcode(ERRCODE_GROUPING_ERROR),
				errmsg("cannot use aggregate function in index expression")));
			if (checkExprHasWindFuncs(attribute->expr))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use window function in index expression")));


			/*
			 * A expression using mutable functions is probably wrong, since
			 * if you aren't going to get the same result for the same data
			 * every time, it's not clear what the index entries mean at all.
			 */
			if (contain_mutable_functions(attribute->expr))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("functions in index expression must be marked IMMUTABLE")));
		}

		classOidP[attn] = GetIndexOpClass(attribute->opclass,
										  atttype,
										  accessMethodName,
										  accessMethodId);
		attn++;
	}
}

/*
 * Resolve possibly-defaulted operator class specification
 */
static Oid
GetIndexOpClass(List *opclass, Oid attrType,
				char *accessMethodName, Oid accessMethodId)
{
	char	   *schemaname;
	char	   *opcname;
	HeapTuple	tuple;
	Oid			opClassId,
				opInputType;
	cqContext	*pcqCtx;

	/*
	 * Release 7.0 removed network_ops, timespan_ops, and datetime_ops, so we
	 * ignore those opclass names so the default *_ops is used.  This can be
	 * removed in some later release.  bjm 2000/02/07
	 *
	 * Release 7.1 removes lztext_ops, so suppress that too for a while.  tgl
	 * 2000/07/30
	 *
	 * Release 7.2 renames timestamp_ops to timestamptz_ops, so suppress that
	 * too for awhile.	I'm starting to think we need a better approach. tgl
	 * 2000/10/01
	 *
	 * Release 8.0 removes bigbox_ops (which was dead code for a long while
	 * anyway).  tgl 2003/11/11
	 */
	if (list_length(opclass) == 1)
	{
		char	   *claname = strVal(linitial(opclass));

		if (strcmp(claname, "network_ops") == 0 ||
			strcmp(claname, "timespan_ops") == 0 ||
			strcmp(claname, "datetime_ops") == 0 ||
			strcmp(claname, "lztext_ops") == 0 ||
			strcmp(claname, "timestamp_ops") == 0 ||
			strcmp(claname, "bigbox_ops") == 0)
			opclass = NIL;
	}

	if (opclass == NIL)
	{
		/* no operator class specified, so find the default */
		opClassId = GetDefaultOpClass(attrType, accessMethodId);
		if (!OidIsValid(opClassId))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("data type %s has no default operator class for access method \"%s\"",
							format_type_be(attrType), accessMethodName),
					 errhint("You must specify an operator class for the index or define a default operator class for the data type.")));
		return opClassId;
	}

	/*
	 * Specific opclass name given, so look up the opclass.
	 */

	/* deconstruct the name list */
	DeconstructQualifiedName(opclass, &schemaname, &opcname);

	if (schemaname)
	{
		/* Look in specific schema only */
		Oid			namespaceId;

		namespaceId = LookupExplicitNamespace(schemaname, NSPDBOID_CURRENT);

		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_opclass "
					" WHERE opcamid = :1 "
					" AND opcname = :2 "
					" AND opcnamespace = :3 ",
					ObjectIdGetDatum(accessMethodId),
					PointerGetDatum(opcname),
					ObjectIdGetDatum(namespaceId)));
	}
	else
	{
		/* Unqualified opclass name, so search the search path */
		opClassId = OpclassnameGetOpcid(accessMethodId, opcname);
		if (!OidIsValid(opClassId))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("operator class \"%s\" does not exist for access method \"%s\"",
							opcname, accessMethodName)));

		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_opclass "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(opClassId)));
	}

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("operator class \"%s\" does not exist for access method \"%s\"",
						NameListToString(opclass), accessMethodName)));

	/*
	 * Verify that the index operator class accepts this datatype.	Note we
	 * will accept binary compatibility.
	 */
	opClassId = HeapTupleGetOid(tuple);
	opInputType = ((Form_pg_opclass) GETSTRUCT(tuple))->opcintype;

	if (!IsBinaryCoercible(attrType, opInputType))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("operator class \"%s\" does not accept data type %s",
					  NameListToString(opclass), format_type_be(attrType))));

	caql_endscan(pcqCtx);

	return opClassId;
}

/*
 * GetDefaultOpClass
 *
 * Given the OIDs of a datatype and an access method, find the default
 * operator class, if any.	Returns InvalidOid if there is none.
 */
Oid
GetDefaultOpClass(Oid type_id, Oid am_id)
{
	int			nexact = 0;
	int			ncompatible = 0;
	Oid			exactOid = InvalidOid;
	Oid			compatibleOid = InvalidOid;
	cqContext  *pcqCtx;
	HeapTuple	tup;

	/* If it's a domain, look at the base type instead */
	type_id = getBaseType(type_id);

	/*
	 * We scan through all the opclasses available for the access method,
	 * looking for one that is marked default and matches the target type
	 * (either exactly or binary-compatibly, but prefer an exact match).
	 *
	 * We could find more than one binary-compatible match, in which case we
	 * require the user to specify which one he wants.	If we find more than
	 * one exact match, then someone put bogus entries in pg_opclass.
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_opclass "
				" WHERE opcamid = :1 ",
				ObjectIdGetDatum(am_id)));

	while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
	{
		Form_pg_opclass opclass = (Form_pg_opclass) GETSTRUCT(tup);

		if (opclass->opcdefault)
		{
			if (opclass->opcintype == type_id)
			{
				nexact++;
				exactOid = HeapTupleGetOid(tup);
			}
			else if (IsBinaryCoercible(type_id, opclass->opcintype))
			{
				ncompatible++;
				compatibleOid = HeapTupleGetOid(tup);
			}
		}
	}

	caql_endscan(pcqCtx);

	if (nexact == 1)
		return exactOid;
	if (nexact != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
		errmsg("there are multiple default operator classes for data type %s",
			   format_type_be(type_id))));
	if (ncompatible == 1)
		return compatibleOid;

	return InvalidOid;
}

/*
 *	makeObjectName()
 *
 *	Create a name for an implicitly created index, sequence, constraint, etc.
 *
 *	The parameters are typically: the original table name, the original field
 *	name, and a "type" string (such as "seq" or "pkey").	The field name
 *	and/or type can be NULL if not relevant.
 *
 *	The result is a palloc'd string.
 *
 *	The basic result we want is "name1_name2_label", omitting "_name2" or
 *	"_label" when those parameters are NULL.  However, we must generate
 *	a name with less than NAMEDATALEN characters!  So, we truncate one or
 *	both names if necessary to make a short-enough string.	The label part
 *	is never truncated (so it had better be reasonably short).
 *
 *	The caller is responsible for checking uniqueness of the generated
 *	name and retrying as needed; retrying will be done by altering the
 *	"label" string (which is why we never truncate that part).
 */
char *
makeObjectName(const char *name1, const char *name2, const char *label)
{
	char	   *name;
	int			overhead = 0;	/* chars needed for label and underscores */
	int			availchars;		/* chars available for name(s) */
	int			name1chars;		/* chars allocated to name1 */
	int			name2chars;		/* chars allocated to name2 */
	int			ndx;

	name1chars = strlen(name1);
	if (name2)
	{
		name2chars = strlen(name2);
		overhead++;				/* allow for separating underscore */
	}
	else
		name2chars = 0;
	if (label)
		overhead += strlen(label) + 1;

	availchars = NAMEDATALEN - 1 - overhead;
	Assert(availchars > 0);		/* else caller chose a bad label */

	/*
	 * If we must truncate,  preferentially truncate the longer name. This
	 * logic could be expressed without a loop, but it's simple and obvious as
	 * a loop.
	 */
	while (name1chars + name2chars > availchars)
	{
		if (name1chars > name2chars)
			name1chars--;
		else
			name2chars--;
	}

	name1chars = pg_mbcliplen(name1, name1chars, name1chars);
	if (name2)
		name2chars = pg_mbcliplen(name2, name2chars, name2chars);

	/* Now construct the string using the chosen lengths */
	name = palloc(name1chars + name2chars + overhead + 1);
	memcpy(name, name1, name1chars);
	ndx = name1chars;
	if (name2)
	{
		name[ndx++] = '_';
		memcpy(name + ndx, name2, name2chars);
		ndx += name2chars;
	}
	if (label)
	{
		name[ndx++] = '_';
		strcpy(name + ndx, label);
	}
	else
		name[ndx] = '\0';

	return name;
}

/*
 * Select a nonconflicting name for a new relation.  This is ordinarily
 * used to choose index names (which is why it's here) but it can also
 * be used for sequences, or any autogenerated relation kind.
 *
 * name1, name2, and label are used the same way as for makeObjectName(),
 * except that the label can't be NULL; digits will be appended to the label
 * if needed to create a name that is unique within the specified namespace.
 *
 * Note: it is theoretically possible to get a collision anyway, if someone
 * else chooses the same name concurrently.  This is fairly unlikely to be
 * a problem in practice, especially if one is holding an exclusive lock on
 * the relation identified by name1.  
 *
 * If choosing multiple names within a single command, there are two options:
 *   1) Create the new object and do CommandCounterIncrement
 *   2) Pass a hash-table to this function to use as a cache of objects 
 *      created in this statement.
 *
 * Returns a palloc'd string.
 */
char *
ChooseRelationName(const char *name1, const char *name2,
				   const char *label, Oid namespace,
				   HTAB *cache)
{
	int			 pass	 = 0;
	char		*relname = NULL;
	char		 modlabel[NAMEDATALEN];
	bool		 found	 = false;

	/* try the unmodified label first */
	StrNCpy(modlabel, label, sizeof(modlabel));

	for (;;)
	{
		relname = makeObjectName(name1, name2, modlabel);

		if (cache)
			hash_search(cache, (void *) relname, HASH_FIND, &found);

		if (!found && !OidIsValid(get_relname_relid(relname, namespace)))
			break;

		/* found a conflict, so try a new name component */
		pfree(relname);
		snprintf(modlabel, sizeof(modlabel), "%s%d", label, ++pass);
	}

	/* If we are caching found values add the value to our hash */
	if (cache)
	{
		hash_search(cache, (void *) relname, HASH_ENTER, &found);
		Assert(!found);
	}

	return relname;
}

/*
 * relationHasPrimaryKey -
 *
 *	See whether an existing relation has a primary key.
 */
static bool
relationHasPrimaryKey(Relation rel)
{
	bool		result = false;
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	cqContext  *pcqCtx;
	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache until we find one marked primary key
	 * (hopefully there isn't more than one such).
	 */
	indexoidlist = RelationGetIndexList(rel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		HeapTuple	indexTuple;

		/* XXX: select * from pg_index where indexrelid = :1 
		   and indisprimary */
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_index "
					" WHERE indexrelid = :1 ",
					ObjectIdGetDatum(indexoid)));
		
		indexTuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(indexTuple))		/* should not happen */
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		result = ((Form_pg_index) GETSTRUCT(indexTuple))->indisprimary;

		caql_endscan(pcqCtx);

		if (result)
			break;
	}

	list_free(indexoidlist);

	return result;
}


/*
 * relationHasPrimaryKey -
 *
 *	See whether an existing relation has a primary key.
 */
static bool
relationHasUniqueIndex(Relation rel)
{
	bool		result = false;
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	cqContext  *pcqCtx;
	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache until we find one marked unique
	 */
	indexoidlist = RelationGetIndexList(rel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		HeapTuple	indexTuple;

		/* XXX: select * from pg_index where indexrelid = :1 
		   and indisunique */
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_index "
					" WHERE indexrelid = :1 ",
					ObjectIdGetDatum(indexoid)));

		indexTuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(indexTuple))		/* should not happen */
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		result = ((Form_pg_index) GETSTRUCT(indexTuple))->indisunique;

		caql_endscan(pcqCtx);
		if (result)
			break;
	}

	list_free(indexoidlist);

	return result;
}

/*
 * RemoveIndex
 *		Deletes an index.
 */
void
RemoveIndex(RangeVar *relation, DropBehavior behavior)
{
	Oid			indOid;
	char		relkind;
	ObjectAddress object;
	HeapTuple tuple;
	PartStatus pstat;
	cqContext	*pcqCtx;

	indOid = RangeVarGetRelid(relation, false, false /*allowHcatalog*/);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		LockRelationOid(RelationRelationId, RowExclusiveLock);
	}

	/* Lock the relation to be dropped */
	LockRelationOid(indOid, AccessExclusiveLock);

	/* XXX: just an existence (count(*)) check? */

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(indOid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "index \"%s\" does not exist", relation->relname);

	relkind = get_rel_relkind(indOid);
	if (relkind != RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index",
						relation->relname)));

	object.classId = RelationRelationId;
	object.objectId = indOid;
	object.objectSubId = 0;
	
	pstat = rel_part_status(IndexGetRelation(indOid));

	caql_endscan(pcqCtx);

	performDeletion(&object, behavior);
	
	if ( pstat == PART_STATUS_ROOT || pstat == PART_STATUS_INTERIOR )
	{
		ereport(WARNING,
				(errmsg("Only dropped the index \"%s\"", relation->relname),
				 errhint("To drop other indexes on child partitions, drop each one explicitly.")));
	}
}

/*
 * ReindexIndex
 *		Recreate a specific index.
 */
void
ReindexIndex(ReindexStmt *stmt)
{
	Oid			indOid;
	HeapTuple	tuple;
	Oid			newOid;
	Oid			mapoid = InvalidOid;
	List        *extra_oids = NIL;
	cqContext	*pcqCtx;

	indOid = RangeVarGetRelid(stmt->relation, false, false /*allowHcatalog*/);

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(indOid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))		/* shouldn't happen */
		elog(ERROR, "cache lookup failed for relation %u", indOid);

	if (((Form_pg_class) GETSTRUCT(tuple))->relkind != RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index",
						stmt->relation->relname)));

	/* Check permissions */
	if (!pg_class_ownercheck(indOid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   stmt->relation->relname);

	caql_endscan(pcqCtx);

	if (Gp_role == GP_ROLE_EXECUTE)
	{
		if (PointerIsValid(stmt->new_ind_oids))
		{
			ListCell *lc;
			foreach(lc, stmt->new_ind_oids)
			{
				List *map = lfirst(lc);
				Oid ind = linitial_oid(map);

				if (ind == indOid)
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
	}
	newOid = reindex_index(indOid, mapoid, &extra_oids);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		List *map = list_make2_oid(indOid, newOid);

		Assert(extra_oids != NULL);
		map = list_concat(map, extra_oids);

		stmt->new_ind_oids = lappend(stmt->new_ind_oids, map);
	}
}

/*
 * ReindexTable
 *		Recreate all indexes of a table (and of its toast table, if any)
 */
void
ReindexTable(ReindexStmt *stmt)
{
	Oid			heapOid;
	HeapTuple	tuple;
	cqContext  *pcqCtx;

	heapOid = RangeVarGetRelid(stmt->relation, false, false /*allowHcatalog*/);

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(heapOid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))		/* shouldn't happen */
		elog(ERROR, "cache lookup failed for relation %u", heapOid);

	if (((Form_pg_class) GETSTRUCT(tuple))->relkind != RELKIND_RELATION &&
		((Form_pg_class) GETSTRUCT(tuple))->relkind != RELKIND_TOASTVALUE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table",
						stmt->relation->relname)));

	/* Check permissions */
	if (!pg_class_ownercheck(heapOid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   stmt->relation->relname);

	/* Can't reindex shared tables except in standalone mode */
	if (((Form_pg_class) GETSTRUCT(tuple))->relisshared && IsUnderPostmaster)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("shared table \"%s\" can only be reindexed in stand-alone mode",
						stmt->relation->relname)));

	caql_endscan(pcqCtx);

	if (!reindex_relation(heapOid, true, true, true, &stmt->new_ind_oids,
						  Gp_role == GP_ROLE_DISPATCH))
	{
		if (Gp_role != GP_ROLE_EXECUTE)
			ereport(NOTICE,
				(errmsg("table \"%s\" has no indexes",
						stmt->relation->relname)));
	}
}

/*
 * ReindexDatabase
 *		Recreate indexes of a database.
 *
 * To reduce the probability of deadlocks, each table is reindexed in a
 * separate transaction, so we can release the lock on it right away.
 */
void
ReindexDatabase(ReindexStmt *stmt)
{
	cqContext  *pcqCtx;
	HeapTuple	tuple;
	MemoryContext private_context;
	MemoryContext old;
	List	   *relids = NIL;
	ListCell   *l;
	bool do_system = stmt->do_system;
	bool do_user = stmt->do_user;
	const char *databaseName = stmt->name;

	AssertArg(databaseName);

	if (strcmp(databaseName, get_database_name(MyDatabaseId)) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("can only reindex the currently open database")));

	if (!pg_database_ownercheck(MyDatabaseId, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
					   databaseName);

	/*
	 * We cannot run inside a user transaction block; if we were inside a
	 * transaction, then our commit- and start-transaction-command calls would
	 * not have the intended effect!
	 */
	PreventTransactionChain((void *) databaseName, "REINDEX DATABASE");

	/*
	 * Create a memory context that will survive forced transaction commits we
	 * do below.  Since it is a child of PortalContext, it will go away
	 * eventually even if we suffer an error; there's no need for special
	 * abort cleanup logic.
	 */
	private_context = AllocSetContextCreate(PortalContext,
											"ReindexDatabase",
											ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * We always want to reindex pg_class first.  This ensures that if there
	 * is any corruption in pg_class' indexes, they will be fixed before we
	 * process any other tables.  This is critical because reindexing itself
	 * will try to update pg_class.
	 */
	if (do_system)
	{
		old = MemoryContextSwitchTo(private_context);
		relids = lappend_oid(relids, RelationRelationId);
		MemoryContextSwitchTo(old);
	}

	/*
	 * Scan pg_class to build a list of the relations we need to reindex.
	 *
	 * We only consider plain relations here (toast rels will be processed
	 * indirectly by reindex_relation).
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class ", NULL));

	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		Form_pg_class classtuple = (Form_pg_class) GETSTRUCT(tuple);

		if (classtuple->relkind != RELKIND_RELATION)
			continue;

		/* Skip temp tables of other backends; we can't reindex them at all */
		if (isOtherTempNamespace(classtuple->relnamespace))
			continue;

		/* Check user/system classification, and optionally skip */
		if (IsSystemClass(classtuple))
		{
			if (!do_system)
				continue;
		}
		else
		{
			if (!do_user)
				continue;
		}

		if (IsUnderPostmaster)	/* silently ignore shared tables */
		{
			if (classtuple->relisshared)
				continue;
		}

		if (HeapTupleGetOid(tuple) == RelationRelationId)
			continue;			/* got it already */

		old = MemoryContextSwitchTo(private_context);
		relids = lappend_oid(relids, HeapTupleGetOid(tuple));
		MemoryContextSwitchTo(old);
	}
	caql_endscan(pcqCtx);

	/* Now reindex each rel in a separate transaction */
	CommitTransactionCommand();
	foreach(l, relids)
	{
		Oid			relid = lfirst_oid(l);

		StartTransactionCommand();
		/* functions in indexes may want a snapshot set */
		ActiveSnapshot = CopySnapshot(GetTransactionSnapshot());
		if (reindex_relation(relid, true, true, true, NULL, false))
			ereport(NOTICE,
					(errmsg("table \"%s\" was reindexed",
							get_rel_name(relid))));
		CommitTransactionCommand();
	}
	StartTransactionCommand();

	MemoryContextDelete(private_context);
}
