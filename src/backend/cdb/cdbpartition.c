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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*--------------------------------------------------------------------------
 *
 * cdbpartition.c
 *	  Provides utility routines to support sharding via partitioning
 *	  within Greenplum Database. 
 * 
 *    Many items are just extensions of tablecmds.c.
 *
 *--------------------------------------------------------------------------
 */
#include "postgres.h"
#include "funcapi.h"
#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_partition_encoding.h"
#include "catalog/namespace.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "nodes/makefuncs.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#define DEFAULT_CONSTRAINT_ESTIMATE 16
#define MIN_XCHG_CONTEXT_SIZE 4096
#define INIT_XCHG_BLOCK_SIZE 4096
#define MAX_XCHG_BLOCK_SIZE 4096

typedef struct
	{
		char *key;
		List *table_cons;
		List *part_cons;
		List *cand_cons;
	} ConstraintEntry;

typedef struct
{
	Node *entry;
} ConNodeEntry;

static void
record_constraints(Relation pgcon, MemoryContext context, 
				   HTAB *hash_tbl, Relation rel, 
				   PartExchangeRole xrole);

static char *
constraint_names(List *cons);

static void 
constraint_diffs(List *cons_a, List *cons_b, bool match_names, List **missing, List **extra);

static void add_template_encoding_clauses(Oid relid, Oid paroid, List *stenc);

static PartitionNode *
findPartitionNodeEntry(PartitionNode *partitionNode, Oid partOid);

static uint32
constrNodeHash(const void *keyPtr, Size keysize);

static int
constrNodeMatch(const void *keyPtr1, const void *keyPtr2, Size keysize);
/*
 * Hash keys are null-terminated C strings assumed to be stably 
 * allocated. We accomplish this by allocating them in a context 
 * that lives as long as the hash table that contains them.
 */

/* Hash entire string. */
static uint32 key_string_hash(const void *key, Size keysize)
{
	Size		s_len = strlen((const char *) key);
	
	Assert(keysize == sizeof(char*));
	return DatumGetUInt32(hash_any((const unsigned char *) key, (int) s_len));
}

/* Compare entire string. */
static int key_string_compare(const void *key1, const void *key2, Size keysize)
{
	Assert(keysize == sizeof(char*));
	return strcmp(((ConstraintEntry*)key1)->key, key2);
}

/* Copy string by copying pointer. */
static void *key_string_copy(void *dest, const void *src, Size keysize)
{
	Assert(keysize == sizeof(char*));
	
	*((char**)dest) = (char*)src; /* trust caller re allocation */
	return NULL; /* not used */
}

static char parttype_to_char(PartitionByType type);
static void add_partition(Partition *part);
static void add_partition_rule(PartitionRule *rule);
static Oid get_part_oid(Oid rootrelid, int16 parlevel, bool istemplate);
static Datum *magic_expr_to_datum(Relation rel, PartitionNode *partnode,
								  Node *expr, bool **ppisnull);
static Oid selectPartitionByRank(PartitionNode *partnode, int rnk);
static bool compare_partn_opfuncid(PartitionNode *partnode,
								   char *pub, char *compare_op,
								   List *colvals,
								   Datum *values, bool *isnull,
								   TupleDesc tupdesc);
static PartitionNode *
selectListPartition(PartitionNode *partnode, Datum *values, bool *isnull,
					TupleDesc tupdesc, PartitionAccessMethods *accessMethods,
					Oid *foundOid, PartitionRule **prule, Oid exprTypid);
static Oid get_less_than_oper(Oid lhstypid, Oid rhstypid, bool strictlyless);
static FmgrInfo *get_less_than_comparator(int keyno, PartitionRangeState *rs, Oid ruleTypeOid, Oid exprTypeOid, bool strictlyless, bool is_direct);
static int range_test(Datum tupval, Oid ruleTypeOid, Oid exprTypeOid, PartitionRangeState *rs, int keyno,
		   PartitionRule *rule);
static PartitionNode *
selectRangePartition(PartitionNode *partnode, Datum *values, bool *isnull,
					 TupleDesc tupdesc, PartitionAccessMethods *accessMethods,
					 Oid *foundOid, int *pSearch, PartitionRule **prule, Oid exprTypid);
static PartitionNode *
selectHashPartition(PartitionNode *partnode, Datum *values, bool *isnull,
					TupleDesc tupdesc, PartitionAccessMethods *accessMethods,
					Oid *found, PartitionRule **prule);
static Oid
selectPartition1(PartitionNode *partnode, Datum *values, bool *isnull,
				 TupleDesc tupdesc, PartitionAccessMethods *accessMethods,
				 int *pSearch,
				 PartitionNode **ppn_out);
static int
atpxPart_validate_spec(
					   PartitionBy 			*pBy,
					   CreateStmtContext   	*pcxt,
					   Relation 		 		 rel,
					   CreateStmt 				*ct,
					   PartitionElem 			*pelem,
					   PartitionNode 			*pNode,
					   Node 					*partName,
					   bool 			 		 isDefault,
					   PartitionByType  		 part_type,
					   char 					*partDesc);

static void atpxSkipper(PartitionNode *pNode, int *skipped);

static List *
build_rename_part_recurse(PartitionRule *rule, const char *old_parentname,
						  const char *new_parentname,
						  int *skipped);
static Oid
get_opfuncid_by_opname(List *opname, Oid lhsid, Oid rhsid);

static PgPartRule *
get_pprule_from_ATC(Relation rel, AlterTableCmd *cmd);

static List*
get_partition_rules(PartitionNode *pn);

static bool
relation_has_supers(Oid relid);

static NewConstraint * 
constraint_apply_mapped(HeapTuple tuple, AttrMap *map, Relation cand, 
						bool validate, bool is_split, Relation pgcon);

static char *
ChooseConstraintNameForPartitionCreate(const char *rname, 
									   const char *cname, 
									   const char *label, 
									   List *used_names);

static Bitmapset *
get_partition_key_bitmapset(Oid relid);

static List *get_deparsed_partition_encodings(Oid relid, Oid paroid);
static List *rel_get_leaf_relids_from_rule(Oid ruleOid);

/* Is the given relation the top relation of a partitioned table?
 *
 *   exists (select *
 *           from pg_partition
 *           where parrelid = relid)
 *
 * False for interior branches and leaves or when called other
 * then on the entry database, i.e., only meaningful on the
 * entry database.
 */
bool
rel_is_partitioned(Oid relid)
{
	return (
			(caql_getcount(
					NULL,
					cql("SELECT COUNT(*) FROM pg_partition "
						" WHERE parrelid = :1 ",
						ObjectIdGetDatum(relid))) > 0));
}

/* 
 * Return an integer list of the attribute numbers of the partitioning
 * key of the partitioned table identified by the argument or NIL.
 *
 * This is similar to get_partition_attrs but is driven by OID and 
 * the partition catalog, not by a PartitionNode.
 *
 * Note: Only returns a non-empty list of keys for partitioned table
 *       as a whole.  Returns empty for non-partitioned tables or for
 *       parts of partitioned tables.  Key attributes are attribute
 *       numbers in the partitioned table.
 */
List *
rel_partition_key_attrs(Oid relid)
{
	Relation rel;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple tuple;
	List *pkeys = NIL;
	
	/* Table pg_partition is only populated on the entry database, 
	 * however, we disable calls from outside dispatch to foil use
	 * of utility mode.  (Full UCS may may this test obsolete.)
	 */
	if (Gp_session_role != GP_ROLE_DISPATCH )
		elog(ERROR, "mode not dispatch");
	
	rel = heap_open(PartitionRelationId, AccessShareLock);
	
	ScanKeyInit(&key,
				Anum_pg_partition_parrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	
	
	scan = systable_beginscan(rel, PartitionParrelidIndexId, true,
							  SnapshotNow, 1, &key);
	
	tuple = systable_getnext(scan);

	while ( HeapTupleIsValid(tuple) ) 
	{
		Index i;
		Form_pg_partition p = (Form_pg_partition) GETSTRUCT(tuple);
		
		if (p->paristemplate)
		{
			tuple = systable_getnext(scan);
			continue;
		}
		
		for ( i = 0; i < p->parnatts; i++ )
		{
			pkeys = lappend_int(pkeys, (Oid)p->paratts.values[i]);
		}
		
		tuple = systable_getnext(scan);
	}
		
	systable_endscan(scan);
	heap_close(rel, AccessShareLock);
	
	return pkeys;
}

/*
 * Return a list of lists representing the partitioning keys of the partitioned
 * table identified by the argument or NIL. The keys are in the order of
 * partitioning levels. Each of the lists inside the main list correspond to one
 * level, and may have one or more attribute numbers depending on whether the
 * part key for that level is composite or not.
 *
 * Note: Only returns a non-empty list of keys for partitioned table
 *       as a whole.  Returns empty for non-partitioned tables or for
 *       parts of partitioned tables.  Key attributes are attribute
 *       numbers in the partitioned table.
 */
List *
rel_partition_keys_ordered(Oid relid)
{
	cqContext *pcqCtx = caql_beginscan(
							NULL,
							cql("SELECT * FROM pg_partition "
								" WHERE parrelid = :1 ",
								ObjectIdGetDatum(relid)));

	List *levels = NIL;
	List *keysUnordered = NIL;
	int nlevels = 0;
	HeapTuple tuple = NULL;
	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		Form_pg_partition p = (Form_pg_partition) GETSTRUCT(tuple);

		if (p->paristemplate)
		{
			continue;
		}

		List *levelkeys = NIL;
		for (int i = 0; i < p->parnatts; i++ )
		{
			levelkeys = lappend_int(levelkeys, (Oid)p->paratts.values[i]);
		}

		nlevels++;
		levels = lappend_int(levels, p->parlevel);
		keysUnordered = lappend(keysUnordered, levelkeys);
	}
	caql_endscan(pcqCtx);

	if (1 == nlevels)
	{
		list_free(levels);
		return keysUnordered;
	}

	// now order the keys by level
	List *pkeys = NIL;
	for (int i = 0; i< nlevels; i++)
	{
		int pos = list_find_int(levels, i);
		Assert (0 <= pos);

		pkeys = lappend(pkeys, list_nth(keysUnordered, pos));
	}
	list_free(levels);
	list_free(keysUnordered);

	return pkeys;
}

/*
 * Is relid a child in a partitioning hierarchy? 
 *
 *    exists (select *
 *            from pg_partition_rule
 *            where parchildrelid = relid)
 *
 * False for the partitioned table as a whole or when called 
 * other then on the entry database, i.e., only meaningful on 
 * the entry database.
 */
bool
rel_is_child_partition(Oid relid)
{
	return (
			(caql_getcount(
					NULL,
					cql("SELECT COUNT(*) FROM pg_partition_rule "
						" WHERE parchildrelid = :1 ",
						ObjectIdGetDatum(relid))) > 0));
}

/*
 * Is relid a leaf node of a partitioning hierarchy? If no, it does not follow
 * that it is the root.
 *
 * To determine if it is a leaf or not, we need to find the depth of our
 * partition and compare this to the maximum depth of the partition set itself.
 * If they're equal, we have a leaf, otherwise, something else.
 *
 * Call only on entry database.
 */
bool
rel_is_leaf_partition(Oid relid)
{
	HeapTuple tuple;
	Oid paroid = InvalidOid;
	int maxdepth = 0;
	int mylevel = 0;
	int fetchCount = 0;
	cqContext	 cqc;
	cqContext	*pcqCtx;
	Oid partitioned_rel = InvalidOid; /* OID of the root table of the
									   * partition set
									   */

	/*
	 * Find the pg_partition_rule entry to see if this is a child at
	 * all and, if so, to locate the OID for the pg_partition entry.
	 */
	paroid = 
			caql_getoid_plus(NULL,
							 &fetchCount,
							 NULL,
							 cql("SELECT paroid FROM pg_partition_rule "
								 " WHERE parchildrelid = :1 ",
								 ObjectIdGetDatum(relid)));

	if (!OidIsValid(paroid) || !fetchCount)
		return false;
	
	tuple = caql_getfirst(NULL,
						  cql("SELECT * FROM pg_partition "
							   " WHERE oid = :1 ",
							   ObjectIdGetDatum(paroid)));
	
	Insist(HeapTupleIsValid(tuple));
	
	mylevel = ((Form_pg_partition)GETSTRUCT(tuple))->parlevel;
	partitioned_rel = ((Form_pg_partition)GETSTRUCT(tuple))->parrelid;

	pcqCtx = caql_beginscan(
			cqclr(&cqc),
			cql("SELECT * FROM pg_partition "
				" WHERE parrelid = :1 ",
				ObjectIdGetDatum(partitioned_rel)));
	
	/*
	 * Of course, we could just maxdepth++ but this seems safer -- we
	 * don't have to worry about the starting depth being 0, 1 or
	 * something else.
	 */
	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		/* not interested in templates */
		if (((Form_pg_partition)GETSTRUCT(tuple))->paristemplate == false)
		{
			int depth = ((Form_pg_partition)GETSTRUCT(tuple))->parlevel;
			maxdepth = Max(maxdepth, depth);
		}
	}
	
	caql_endscan(pcqCtx);

	return maxdepth == mylevel;
}

/* Determine the status of the given table with respect to partitioning.
 *
 * Uses lower level routines. Returns PART_STATUS_NONE for a non-partitioned 
 * table or when called other then on the entry database, i.e., only meaningful 
 * on  the entry database.
 */
PartStatus rel_part_status(Oid relid)
{
	if (Gp_role != GP_ROLE_DISPATCH)
	{
		ereport(DEBUG1,
				(errmsg("requesting part status outside dispatch - returning none")));
		return PART_STATUS_NONE;
	}
	
	if ( rel_is_partitioned(relid) )
	{
		Assert( !rel_is_child_partition(relid) && !rel_is_leaf_partition(relid) );
		return PART_STATUS_ROOT;
	}
	else /* not an actual partitioned table root */
	{
		if ( rel_is_child_partition(relid) )
			return rel_is_leaf_partition(relid) ? PART_STATUS_LEAF : PART_STATUS_INTERIOR;
		else /* not a part of a partitioned table */
			Assert( !rel_is_child_partition(relid) );
	}
	return PART_STATUS_NONE;
}


/* Locate all the constraints on the given open relation (rel) and 
 * record them in the hash table (hash_tbl) of ConstraintEntry 
 * structs.  
 *
 * Depending on the value of xrole, the given relation must be either
 * the root, an existing part, or an exchange candidate for the same 
 * partitioned table.  A copy of each constraint tuple found is
 * appended to the corresponding field of the hash entry.
 *
 * The key  of the hash table is a string representing the constraint 
 * in SQL. This should be comparable across parts of a partitioning 
 * hierarchy regardless of the history (hole pattern) or storage type
 * of the table.
 *
 * Note that pgcon (the ConstraintRelationId appropriately locked) 
 * is supplied externally for efficiency.  No other relation should
 * be supplied via this argument.
 *
 * Memory allocated in here (strings, tuples, lists, list cells, etc)
 * is all associated with the hash table and is allocated in the given 
 * memory context, so it will be easy to free in bulk.
 */
void
record_constraints(Relation pgcon, 
				   MemoryContext context, 
				   HTAB *hash_tbl, 
				   Relation rel, 
				   PartExchangeRole xrole)
{
	HeapTuple	tuple;
	Oid conid;
	char *condef;
	ConstraintEntry *entry;
	bool found;
	MemoryContext oldcontext;
	cqContext	*pcqCtx;
	cqContext	 cqc;

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), pgcon),
			cql("SELECT * FROM pg_constraint "
				" WHERE conrelid = :1 ",
				ObjectIdGetDatum(RelationGetRelid(rel))));
	
	/* For each constraint on rel: */
	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		oldcontext = MemoryContextSwitchTo(context);
		
		conid = HeapTupleGetOid(tuple);

		condef = pg_get_constraintexpr_string(conid);
		entry = (ConstraintEntry*)hash_search(hash_tbl, 
											  (void*) condef, 
											  HASH_ENTER, 
											  &found);
		
		/* A tuple isn't a Node, but we'll stick it in a List
		 * anyway, and just be careful.
		 */
		if ( !found )
		{
			entry->key = condef;
			entry->table_cons = NIL;
			entry->part_cons = NIL;
			entry->cand_cons = NIL;
		}
		tuple = heap_copytuple(tuple);
		
		switch(xrole)
		{
			case PART_TABLE:
				entry->table_cons = lappend(entry->table_cons, tuple);
				break;
			case PART_PART:
				entry->part_cons = lappend(entry->part_cons, tuple);
				break;
			case PART_CAND:
				entry->cand_cons = lappend(entry->cand_cons, tuple);
				break;
			default:
				Assert(FALSE);
		}
		
		MemoryContextSwitchTo(oldcontext);
	}
	caql_endscan(pcqCtx);
}

/* Subroutine of ATPExecPartExchange used to swap constraints on existing
 * part and candidate part.  Note that this runs on both the QD and QEs
 * so must not assume availability of partition catalogs.
 *
 * table -- open relation of the parent partitioned table
 * part -- open relation of existing part to exchange
 * cand -- open relation of the candidate part
 * validate -- whether to collect constraints into a result list for
 *             enforcement during phase 3 (WITH/WITHOUT VALIDATION).
 */
List *
cdb_exchange_part_constraints(Relation table, 
						  Relation part, 
						  Relation cand, 
						  bool validate,
						  bool is_split)
{
	HTAB *hash_tbl;
	HASHCTL hash_ctl;
	HASH_SEQ_STATUS hash_seq;
	Relation pgcon;
	MemoryContext context;
	MemoryContext oldcontext;
	ConstraintEntry *entry;
	AttrMap *p2t = NULL;
	AttrMap *c2t = NULL;

	HeapTuple tuple;
	Form_pg_constraint con;
	
	List *excess_constraints = NIL;
	List *missing_constraints = NIL;
	List *missing_part_constraints = NIL;
	List *validation_list = NIL;
	int delta_checks = 0;
	
	
	/* Setup an empty hash table mapping constraint definition
	 * strings to ConstraintEntry structures.
	 */
	context = AllocSetContextCreate(CurrentMemoryContext,
									"Constraint Exchange Context",
									MIN_XCHG_CONTEXT_SIZE,
									INIT_XCHG_BLOCK_SIZE,
									MAX_XCHG_BLOCK_SIZE);
	
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(char*);
	hash_ctl.entrysize = sizeof(ConstraintEntry);
	hash_ctl.hash = key_string_hash;
	hash_ctl.match = key_string_compare;
	hash_ctl.keycopy = key_string_copy;
	hash_ctl.hcxt = context;
	hash_tbl = hash_create("Constraint Exchange Map",
						   DEFAULT_CONSTRAINT_ESTIMATE,
						   &hash_ctl,
						   HASH_ELEM | HASH_FUNCTION | 
						   HASH_COMPARE | HASH_KEYCOPY | 
						   HASH_CONTEXT);
	
	/* Open pg_constraint here for use in the subroutine and below. */
	pgcon = heap_open(ConstraintRelationId, AccessShareLock);
	
	/* We need attribute numbers normalized to the partitioned table. 
	 * Note that these maps are inverse to the usual table-to-part maps.
	 */
	oldcontext =  MemoryContextSwitchTo(context);
	map_part_attrs(part, table, &p2t, TRUE);
	map_part_attrs(cand, table, &c2t, TRUE);
	MemoryContextSwitchTo(oldcontext);
	
	/* Find and record constraints on the players. */
	record_constraints(pgcon, context, hash_tbl, table, PART_TABLE);
	record_constraints(pgcon, context, hash_tbl, part, PART_PART);
	record_constraints(pgcon, context, hash_tbl, cand, PART_CAND);
	hash_freeze(hash_tbl);
	
	/* Each entry in the hash table represents a single logically equivalent
	 * constraint which may appear zero or more times (under different names) 
	 * on each of the three involved relations.  By construction, it will 
	 * appear on at least one list.
	 *
	 * For each hash table entry:
	 */
	hash_seq_init(&hash_seq, hash_tbl);
	while ((entry = hash_seq_search(&hash_seq)))
	{
		if ( list_length(entry->table_cons) > 0 )
		{
			/* REGULAR CONSTRAINT
			 * 
			 * Constraints on the whole partitioned table are regular (in
			 * the sense that they do not enforce partitioning rules and
			 * corresponding constraints must occur on every part).
			 */
			
			List *missing = NIL;
			List *extra = NIL;
			
			if ( list_length(entry->part_cons) == 0 )
			{
				/* The regular constraint is missing from the existing part, 
				 * so there is a database anomaly.  Warn rather than issuing
				 * an error, because this may be an attempt to use EXCHANGE
				 * to correct the problem.  There may be multiple constraints
				 * with different names, but report only the first name since
				 * the constraint expression itself is all that matters.
				 */
				tuple = linitial(entry->table_cons);
				con = (Form_pg_constraint) GETSTRUCT(tuple);
				
				ereport(WARNING, 
						(errcode(ERRCODE_WARNING),
						 errmsg("ignoring inconsistency: \"%s\" "
								"has no constraint corresponding to \"%s\" "
								"on \"%s\"",
								RelationGetRelationName(part),
								NameStr(con->conname),
								RelationGetRelationName(table)),
						 errOmitLocation(true)));
			}
			
			/* The regular constraint should ultimately appear on the candidate
			 * part the same number of times and with the same name as it appears 
			 * on the partitioned table. The call to constraint_diff will find 
			 * matching names and we'll be left with occurances of the constraint 
			 * that must be added to the candidate (missing) and occurances that 
			 * must be dropped from the candidate (extra).
			 */
			constraint_diffs(entry->table_cons, entry->cand_cons, true, &missing, &extra);
			missing_constraints = list_concat(missing_constraints, missing);
			excess_constraints = list_concat(excess_constraints, extra);
		}
		else if ( list_length(entry->part_cons) > 0 ) /* and none on whole */
		{
			/* PARTITION CONSTRAINT
			 *
			 * Constraints on the part and not the whole must guard a partition 
			 * rule, so they must be CHECK constraints on partitioning columns.  
			 * They are managed internally, so there must be only one of them.
			 * (Though a part will have a partition constraint for each partition
			 * level, a given constraint should appear only once per part.)
			 *
			 * They should either already occur on the candidate or be added.
			 * Partition constraint names are not carefully managed so they
			 * shouldn't be regarded as meaningful.
			 *
			 * Since we use the partition constraint of the part to check or
			 * construct the partition constraint of the candidate, we insist it 
			 * is in good working order, and issue an error, if not.
			 */
			int n = list_length(entry->part_cons);
			
			if ( n > 1 )
			{
				elog(ERROR, 
					 "multiple partition constraints (same key) on \"%s\"", 
					 RelationGetRelationName(part));
			}
			
			/* Get the model partition constraint. 
			 */
			tuple = linitial(entry->part_cons);
			con = (Form_pg_constraint) GETSTRUCT(tuple);
			
			/* Check it, though this is cursory in that we don't check that
			 * the right attributes are involved and that the semantics are
			 * right. 
			 */
			if (con->contype != CONSTRAINT_CHECK)
			{
				elog(ERROR, 
					 "invalid partition constraint on \"%s\"", 
					 RelationGetRelationName(part));
			}
			
			n = list_length(entry->cand_cons);
			
			if ( n == 0 )
			{
				/* The partition constraint is missing from the candidate and
				 * must be added.
				 */
				missing_part_constraints = lappend(missing_part_constraints,
												   (HeapTuple)linitial(entry->part_cons));
			}
			else if ( n == 1 )
			{
				/* One instance of the partition constraint exists on the
				 * candidate, so let's not worry about name drift.  All is
				 * well. */
			}
			else 
			{
				/* Several instances of the partition constraint exist on
				 * the candidate. If one has a matching name, prefer it.
				 * Else, just chose the first (arbitrary).
				 */
				List *missing = NIL;
				List *extra = NIL;
				
				constraint_diffs(entry->part_cons, entry->cand_cons, false, &missing, &extra);
				
				if ( list_length(missing) == 0 )
				{
					excess_constraints = list_concat(excess_constraints, extra);
				}
				else /* missing */
				{
					ListCell *lc;
					bool skip = TRUE;
					
					foreach(lc, entry->cand_cons)
					{
						HeapTuple tuple = (HeapTuple)lfirst(lc);
						if ( skip )
						{
							skip = FALSE;
						}
						else 
						{
							excess_constraints = lappend(excess_constraints, tuple);
						}
					}
				}
			}
		}
		else if ( list_length(entry->cand_cons) > 0 ) /* and none on whole or part */
		{
			/* MAVERICK CONSTRAINT
			 *
			 * Constraints on only the candidate are extra and must be
			 * dropped before the candidate can replace the part.
			 */
			excess_constraints = list_concat(excess_constraints, 
											 entry->cand_cons);
		}
		else /* Defensive: Can't happen that no constraints are set. */
		{
			elog(ERROR, "constraint hash table inconsistent");
		}
	}

	
	if ( excess_constraints )
	{
		/* Disallow excess constraints.  We could drop them automatically, but they 
		 * may carry semantic information about the candidate that is important to 
		 * the user, so make the user decide whether to drop them.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION), 
				 errmsg("invalid constraint(s) found on \"%s\": %s",
				 		RelationGetRelationName(cand),
						constraint_names(excess_constraints)),
				 errhint("drop the invalid constraints and retry"),
				errOmitLocation(true)));
	}
	

	if ( missing_part_constraints )
	{
		ListCell *lc;
		
		foreach(lc, missing_part_constraints)
		{
			HeapTuple missing_part_constraint = (HeapTuple)lfirst(lc);
			/* We need a constraint like the missing one for the part, but translated 
			 * for the candidate.
			 */
			AttrMap *map;
			struct NewConstraint *nc;
			Form_pg_constraint mcon = (Form_pg_constraint)GETSTRUCT(missing_part_constraint);
			
			if ( mcon->contype != CONSTRAINT_CHECK )
				elog(ERROR,"Invalid partition constration, not CHECK type");
			
			map_part_attrs(part, cand, &map, TRUE);
			nc = constraint_apply_mapped(missing_part_constraint, map, cand,
										 validate, is_split, pgcon);
			if ( nc ) 
				validation_list = lappend(validation_list, nc);
			
			delta_checks++;
		}
	}
	
	if ( missing_constraints )
	{
		/* We need constraints like the missing ones for the whole, but
		 * translated for the candidate.
		 */
		AttrMap *map;
		struct NewConstraint *nc;
		ListCell *lc;
		
		map_part_attrs(table, cand, &map, TRUE);
		foreach(lc, missing_constraints)
		{
			HeapTuple tuple = (HeapTuple)lfirst(lc);
			Form_pg_constraint mcon = (Form_pg_constraint)GETSTRUCT(tuple);
			nc = constraint_apply_mapped(tuple, map, cand, 
										 validate, is_split, pgcon);
			if ( nc )
				validation_list = lappend(validation_list, nc);
			
			if ( mcon->contype == CONSTRAINT_CHECK )
				delta_checks++;
		}
	}
	
	if ( delta_checks )
	{
		SetRelationNumChecks(cand, cand->rd_rel->relchecks + delta_checks);
	}
	
	
	hash_destroy(hash_tbl);
	MemoryContextDelete(context);
	heap_close(pgcon, AccessShareLock);
	
	return validation_list;
}

/*
 * Return a string of comma-delimited names of the constraints in the 
 * argument list of pg_constraint tuples.  This is primarily for use
 * in messages.
 */
static char *
constraint_names(List *cons)
{
	HeapTuple tuple;
	Form_pg_constraint con;
	ListCell *lc;
	StringInfoData str;
		
	initStringInfo(&str);
	
	char *p = "";
	foreach (lc, cons)
	{
		tuple = linitial(cons);
		con = (Form_pg_constraint) GETSTRUCT(tuple);
		appendStringInfo(&str, "%s\"%s\"", p, NameStr(con->conname));
		p = ", ";
	}

	return str.data;
}


/*
 * Identify constraints in the first list that don't correspond to 
 * constraints in the second (missing) and vice versa (extra). Assume
 * that the constraints all match semantically, i.e, their expressions 
 * are equivalent. (There are no checks for this.) Also assume there 
 * are no duplicate constraint names in either argument list. (This
 * isn't checked, but is asserted.
 *
 * There are two checking modes.  One matches by name, one prefers
 * matching names, but accepts mismatches based on the assumed 
 * semantic equivlence.
 *
 * Matching by name (which applies to regular constraints on the whole
 * table) may have missing and/or extra constraints.
 *
 * Matching by expression (which applies to partition constraints)
 * can yield only one or the other of extra or missing constraints.
 * The cleverness is that we match by name to avoid choosing as the
 * missing or extra constraints ones that match by name with items in
 * the other list. 
 */
static void 
constraint_diffs(List *cons_a, List *cons_b, bool match_names, List **missing, List **extra)
{
	ListCell *cell_a, *cell_b;
	Index pos_a, pos_b;
	int *match_a, *match_b;
	int n;
	
	int len_a = list_length(cons_a);
	int len_b = list_length(cons_b);
	
	Assert(missing != NULL);
	Assert(extra != NULL);
	
	if ( len_a == 0 )
	{
		*extra = list_copy(cons_b);
		*missing = NIL;
		return;
	}
	
	if ( len_b == 0 )
	{
		*extra = NIL;
		*missing = list_copy(cons_a);
		return;
	}
	
	match_a = (int*)palloc(len_a * sizeof(int));
	for ( pos_a = 0; pos_a < len_a; pos_a++ )
		match_a[pos_a] = -1;
	
	match_b = (int*)palloc(len_b * sizeof(int));
	for ( pos_b = 0; pos_b < len_b; pos_b++ )
		match_b[pos_b] = -1;
	
	pos_b = 0;
	foreach (cell_b, cons_b)
	{
		HeapTuple tuple_b = (HeapTuple)lfirst(cell_b);
		Form_pg_constraint b = (Form_pg_constraint) GETSTRUCT(tuple_b);

		
		pos_a = 0;
		foreach (cell_a, cons_a)
		{
			HeapTuple tuple_a = lfirst(cell_a);
			Form_pg_constraint a = (Form_pg_constraint) GETSTRUCT(tuple_a);
			
			if ( strncmp(NameStr(a->conname), NameStr(b->conname), NAMEDATALEN) == 0 )
			{
				/* No duplicate names on either list. */
				Assert(match_a[pos_a] == -1 && match_b[pos_b] == -1);
				
				match_b[pos_b] = pos_a;
				match_a[pos_a] = pos_b;
				break;
			}
			pos_a++;
		}
		pos_b++;
	}
	
	*missing = NIL;
	*extra = NIL;
	
	n = len_a - len_b;
	if ( n > 0 || match_names )
	{
		pos_a = 0;
		foreach (cell_a, cons_a)
		{
			if ( match_a[pos_a] == -1 )
				*missing = lappend(*missing, lfirst(cell_a));
			pos_a++;
			n--;
			if ( n <= 0 && !match_names)
				break;
		}
	}
	
	n = len_b - len_a;
	if ( n > 0 || match_names )
	{
		pos_b = 0;
		foreach (cell_b, cons_b)
		{
			if ( match_b[pos_b] == -1 )
				*extra = lappend(*extra, lfirst(cell_b));
			pos_b++;
			n--;
			if ( n <= 0 && !match_names )
				break;
		}
	}
	
	pfree(match_a);
	pfree(match_b);
}



/*
 * Install a dependency of a regular constraint on a part of a
 * partitioned to on the corresponding regular constaint on the
 * partitioned table as a whole.
 *
 * NOTE This doesn't apply to partition constraints that guard
 * the partitioning policy of the table.  These aren't represented
 * on the partitioned table as a whole.
 */

void
add_reg_part_dependency(Oid tableconid, Oid partconid)
{
	ObjectAddress tablecon;
	ObjectAddress partcon;
	
	tablecon.classId = ConstraintRelationId;
	tablecon.objectId = tableconid;
	tablecon.objectSubId = 0;
	partcon.classId = ConstraintRelationId;
	partcon.objectId = partconid;
	partcon.objectSubId = 0;
	recordDependencyOn(&partcon, &tablecon, DEPENDENCY_INTERNAL);
}



/*
 * Translate internal representation to catalog partition type indication 
 * ('r', 'h' or 'l').
 */
static char
parttype_to_char(PartitionByType type)
{
	char c;
	
	switch (type)
	{
		case PARTTYP_HASH: c = 'h'; break;
		case PARTTYP_RANGE: c = 'r'; break;
		case PARTTYP_LIST: c = 'l'; break;
		default:
			c = 0; /* quieten compiler */
			elog(ERROR, "unknown partitioning type %i", type);
			
	}
	return c;
}

/*
 * Translate a catalog partition type indication ('r', 'h' or 'l') to the
 * internal representation.
 */
PartitionByType
char_to_parttype(char c)
{
	PartitionByType pt = PARTTYP_RANGE; /* just to shut GCC up */
	
	switch(c)
	{
		case 'h': /* hash */
			pt = PARTTYP_HASH;
			break;
		case 'r': /* range */
			pt = PARTTYP_RANGE;
			break;
		case 'l': /* list */
			pt = PARTTYP_LIST;
			break;
		default:
			elog(ERROR, "unrecognized partitioning kind '%c'",
				 c);
			Assert(false);
			break;
	} /* end switch */
	
	return pt;
}

/*
 * Add metadata for a partition level.
 */
static void
add_partition(Partition *part)
{
	Datum		 values[Natts_pg_partition];
	bool		 isnull[Natts_pg_partition];
	Relation	 partrel;
	HeapTuple	 tup;
	oidvector	*opclass;
	int2vector	*attnums;
	cqContext	 cqc;
	cqContext	*pcqCtx;
	
	MemSet(isnull, 0, sizeof(bool) * Natts_pg_partition);
	
	values[Anum_pg_partition_parrelid - 1] = ObjectIdGetDatum(part->parrelid);
	values[Anum_pg_partition_parkind - 1] = CharGetDatum(part->parkind);
	values[Anum_pg_partition_parlevel - 1] = Int16GetDatum(part->parlevel);
	values[Anum_pg_partition_paristemplate - 1] =
									BoolGetDatum(part->paristemplate);
	values[Anum_pg_partition_parnatts - 1] = Int16GetDatum(part->parnatts);
	
	attnums = buildint2vector(part->paratts, part->parnatts);
	values[Anum_pg_partition_paratts - 1] = PointerGetDatum(attnums);
	
	opclass = buildoidvector(part->parclass, part->parnatts);
	values[Anum_pg_partition_parclass - 1] = PointerGetDatum(opclass);
	
	partrel = heap_open(PartitionRelationId, RowExclusiveLock);

	pcqCtx = 
			caql_beginscan(
					caql_addrel(cqclr(&cqc), partrel),
					cql("INSERT INTO pg_partition ",
						NULL));
	
	tup = caql_form_tuple(pcqCtx, values, isnull);

	/* Insert tuple into the relation */
	part->partid = caql_insert(pcqCtx, tup);
	/* and Update indexes (implicit) */	

	caql_endscan(pcqCtx);
	heap_close(partrel, NoLock);
}

/*
 * Add a partition rule. A partition rule represents a discrete partition
 * child.
 */
static void
add_partition_rule(PartitionRule *rule)
{
	Datum		 values[Natts_pg_partition_rule];
	bool		 isnull[Natts_pg_partition_rule];
	Relation	 rulerel;
	HeapTuple	 tup;
	NameData	 name;
	cqContext	 cqc;
	cqContext	*pcqCtx;
	
	MemSet(isnull, 0, sizeof(bool) * Natts_pg_partition_rule);
	
	values[Anum_pg_partition_rule_paroid - 1] = ObjectIdGetDatum(rule->paroid);
	values[Anum_pg_partition_rule_parchildrelid - 1] =
							ObjectIdGetDatum(rule->parchildrelid);
	values[Anum_pg_partition_rule_parparentrule - 1] =
							ObjectIdGetDatum(rule->parparentoid);
	
	name.data[0] = '\0';
	namestrcpy(&name, rule->parname);
	values[Anum_pg_partition_rule_parname - 1] = NameGetDatum(&name);
	
	values[Anum_pg_partition_rule_parisdefault - 1] =
							BoolGetDatum(rule->parisdefault);
	values[Anum_pg_partition_rule_parruleord - 1] =
							Int16GetDatum(rule->parruleord);
	values[Anum_pg_partition_rule_parrangestartincl - 1] =
							BoolGetDatum(rule->parrangestartincl);
	values[Anum_pg_partition_rule_parrangeendincl - 1] =
							BoolGetDatum(rule->parrangeendincl);
	
	values[Anum_pg_partition_rule_parrangestart - 1] =
			DirectFunctionCall1(textin,
						CStringGetDatum(nodeToString(rule->parrangestart)));
	values[Anum_pg_partition_rule_parrangeend - 1] =
			DirectFunctionCall1(textin,
						CStringGetDatum(nodeToString(rule->parrangeend)));
	values[Anum_pg_partition_rule_parrangeevery - 1] =
			DirectFunctionCall1(textin,
						CStringGetDatum(nodeToString(rule->parrangeevery)));
	values[Anum_pg_partition_rule_parlistvalues - 1] =
			DirectFunctionCall1(textin,
						CStringGetDatum(nodeToString(rule->parlistvalues)));
	if (rule->parreloptions)
		values[Anum_pg_partition_rule_parreloptions - 1] =
				transformRelOptions((Datum) 0, rule->parreloptions, true, false);
	else
		isnull[Anum_pg_partition_rule_parreloptions - 1] = true;
	
	values[Anum_pg_partition_rule_partemplatespace -1] = 
							ObjectIdGetDatum(rule->partemplatespaceId);
	
	rulerel = heap_open(PartitionRuleRelationId, RowExclusiveLock);

	pcqCtx = 
			caql_beginscan(
					caql_addrel(cqclr(&cqc), rulerel),
					cql("INSERT INTO pg_partition_rule ",
						NULL));
	
	tup = caql_form_tuple(pcqCtx, values, isnull);

	/* Insert tuple into the relation */	
	rule->parruleid = caql_insert(pcqCtx, tup);
	/* and Update indexes (implicit) */	

	caql_endscan(pcqCtx);
	heap_close(rulerel, NoLock);
}

/*
 * Oid of the row of pg_partition corresponding to the given relation and level.
 */
static Oid
get_part_oid(Oid rootrelid, int16 parlevel, bool istemplate)
{
	Oid paroid = InvalidOid;
	cqContext	 cqc;

	/* select oid
	 * from pg_partition
	 * where
	 *     parrelid = :rootrelid and
	 *     parlevel = :parlevel and
	 *     paristemplate = :istemplate;
	 */
	Relation rel = heap_open(PartitionRelationId, AccessShareLock);

	paroid = caql_getoid(
			caql_snapshot(caql_addrel(cqclr(&cqc), rel), 
						  SnapshotSelf), /* XXX XXX: SnapshotSelf */
			cql("SELECT oid FROM pg_partition "
				" WHERE parrelid = :1 "
				" AND parlevel = :2 "
				" AND paristemplate = :3",
				ObjectIdGetDatum(rootrelid),
				Int16GetDatum(parlevel),
				Int16GetDatum(istemplate)));
	
	heap_close(rel, NoLock);
	
	return paroid;
}

/* 
 * delete the template for a partition 
 */
int
del_part_template(Oid rootrelid, int16 parlevel, Oid parent)
{
	bool istemplate = true;
	Oid paroid = InvalidOid;
	int ii = 0;
	int			 fetchCount;

	paroid = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT oid FROM pg_partition "
				" WHERE parrelid = :1 "
				" AND parlevel = :2 "
				" AND paristemplate = :3 "
				" FOR UPDATE ",
				ObjectIdGetDatum(rootrelid),
				Int16GetDatum(parlevel),
				Int16GetDatum(istemplate))
			);

	if (0 == fetchCount)
		return 0;

	/* should only be one matching template per level */
	if (fetchCount > 1)
		return 2;
	
	ii = caql_getcount(
			NULL,
			cql("DELETE FROM pg_partition_rule "
				" WHERE paroid = :1 "
				" AND parparentrule = :2 ",
				ObjectIdGetDatum(paroid),
				ObjectIdGetDatum(parent))
			);

	/* now delete the pg_partition entry */

	ii = caql_getcount(
			NULL,
			cql("DELETE FROM pg_partition "
				" WHERE parrelid = :1 "
				" AND parlevel = :2 "
				" AND paristemplate = :3 ",
				ObjectIdGetDatum(rootrelid),
				Int16GetDatum(parlevel),
				Int16GetDatum(istemplate))
			);
			
	/* make visible */
	CommandCounterIncrement();
	
	return 1;
} /* end del_part_template */


/*
 * add_part_to_catalog() - add a partition to the catalog
 *
 *
 
 * NOTE: If bTemplate_Only = false, add both actual partitions and the
 * template definitions (if specified).  However, if bTemplate_Only =
 * true, then only treat the partition spec as a template.
 */
void
add_part_to_catalog(Oid relid, PartitionBy *pby,
					bool bTemplate_Only /* = false */)
{
	char pt = parttype_to_char(pby->partType);
	ListCell *lc;
	PartitionSpec *spec;
	Oid paroid = InvalidOid;
	Oid rootrelid = InvalidOid;
	Relation rel;
	Oid parttemplid = InvalidOid;
	bool add_temp = bTemplate_Only; /* normally false */
	spec = (PartitionSpec *)pby->partSpec;
	
	/* only create partition catalog entries on the master */
	if (Gp_role == GP_ROLE_EXECUTE)
		return;
	
	/*
	 * Get the partitioned table relid.
	 */
	rootrelid = RangeVarGetRelid(pby->parentRel, false, false /*allowHcatalog*/);
	paroid = get_part_oid(rootrelid, pby->partDepth,
						  bTemplate_Only /* = false */);
	
	/* create a partition for this level, if one doesn't exist */
	if (!OidIsValid(paroid))
	{
		AttrNumber *attnums;
		Oid *parclass;
		Partition *part = makeNode(Partition);
		int i = 0;
		
		part->parrelid = rootrelid;
		part->parkind = pt;
		part->parlevel = pby->partDepth;
		
		if (pby->partSpec)
			part->paristemplate = ((PartitionSpec *)pby->partSpec)->istemplate;
		else
			part->paristemplate = false;
		
		part->parnatts = list_length(pby->keys);
		
		attnums = palloc(list_length(pby->keys) * sizeof(AttrNumber));
		
		foreach(lc, pby->keys)
		{
			int colnum = lfirst_int(lc);
			
			attnums[i++] = colnum;
		}
		
		part->paratts = attnums;
		
		parclass = palloc(list_length(pby->keys) * sizeof(Oid));
		
		i = 0;
		foreach(lc, pby->keyopclass)
		{
			Oid opclass = lfirst_oid(lc);
			
			parclass[i++] = opclass;
		}
		part->parclass = parclass;
		
		add_partition(part);
		
		/*
		 * If we added a template, we treat that as a 'virtual' entry and then
		 * add a modifiable entry, which is not a template.
		 */
		if (part->paristemplate)
		{
			add_temp = true;
			parttemplid = part->partid;

			if (spec && spec->enc_clauses)
			{
				add_template_encoding_clauses(relid, parttemplid,
											  spec->enc_clauses);
			}
		
			/* if only building a template, don't add "real" entries */
			if (!bTemplate_Only)
			{
				part->paristemplate = false;
				add_partition(part);

			}
		}
		paroid = part->partid;
	}
	else
	/* oid of the template accompanying the real partition */
		parttemplid = get_part_oid(rootrelid, pby->partDepth, true);
	
	/* create partition rule */
	if (spec)
	{
		Node *listvalues = NULL;
		Node *rangestart = NULL;
		Node *rangeend = NULL;
		Node *rangeevery = NULL;
		bool rangestartinc = false;
		bool rangeendinc = false;
		int2 parruleord = 0;
		PartitionRule *rule = makeNode(PartitionRule);
		PartitionElem *el;
		char *parname = NULL;
		Oid parentoid = InvalidOid;
		
		Assert(list_length(spec->partElem) == 1);
		
		el = linitial(spec->partElem);
		
		parruleord = el->partno;
		
		if (el->partName)
			parname = strVal(el->partName);
		
		switch (pby->partType)
		{
			case PARTTYP_HASH: break;
			case PARTTYP_LIST:
			{
				PartitionValuesSpec *vspec =
				(PartitionValuesSpec *)el->boundSpec;
				
				/* might be NULL if this is a default spec */
				if (vspec)
					listvalues = (Node *)vspec->partValues;
			}
				break;
			case PARTTYP_RANGE:
			{
				PartitionBoundSpec *bspec =
				(PartitionBoundSpec *)el->boundSpec;
				PartitionRangeItem *ri;
				
				/* remember, could be a default clause */
				if (bspec)
				{
					Assert(IsA(bspec, PartitionBoundSpec));
					ri = (PartitionRangeItem *)bspec->partStart;
					if (ri)
					{
						Assert(ri->partedge == PART_EDGE_INCLUSIVE ||
							   ri->partedge == PART_EDGE_EXCLUSIVE);
						
						rangestartinc = ri->partedge == PART_EDGE_INCLUSIVE;
						rangestart = (Node *)ri->partRangeVal;
					}
					
					ri = (PartitionRangeItem *)bspec->partEnd;
					if (ri)
					{
						Assert(ri->partedge == PART_EDGE_INCLUSIVE ||
							   ri->partedge == PART_EDGE_EXCLUSIVE);
						
						rangeendinc = ri->partedge == PART_EDGE_INCLUSIVE;
						rangeend = (Node *)ri->partRangeVal;
					}
					
					if (bspec->partEvery)
					{
						ri = (PartitionRangeItem *)bspec->partEvery;
						rangeevery = (Node *)ri->partRangeVal;
					}
					else
						rangeevery = NULL;
				}
			}
				break;
			default:
				elog(ERROR, "unknown partitioning type %i", pby->partType);
				break;
		}
		
		/* Find our parent */
		if (!bTemplate_Only && (pby->partDepth > 0))
		{
			Oid			inhoid;
			cqContext	cqc;
			int			fetchCount = 0;
			
			rel = heap_open(InheritsRelationId, AccessShareLock);

			inhoid = caql_getoid_plus(
					caql_snapshot(caql_addrel(cqclr(&cqc), rel), 
						  SnapshotAny), /* XXX XXX: SnapshotAny */
					&fetchCount,
					NULL,
					cql("SELECT inhparent FROM pg_inherits "
						" WHERE inhrelid = :1 ",
						ObjectIdGetDatum(relid)));
			
			Assert(fetchCount > 0);

			heap_close(rel, NoLock);
			
			rel = heap_open(PartitionRuleRelationId, AccessShareLock);

			parentoid = caql_getoid_plus(
					caql_snapshot(caql_addrel(cqclr(&cqc), rel), 
								  SnapshotAny), /* XXX XXX: SnapshotAny */
					&fetchCount,
					NULL,
					cql("SELECT oid FROM pg_partition_rule "
						" WHERE parchildrelid = :1 ",
						ObjectIdGetDatum(inhoid)));

			Assert(fetchCount > 0);

			heap_close(rel, NoLock);
		}
		else
			add_temp = true;
		
		/* we still might have to add template rules */
		if (!add_temp && OidIsValid(parttemplid))
		{
			Oid			pcr;
			cqContext	cqc;
			int			fetchCount = 0;

			pcr = caql_getoid_plus(
					caql_snapshot(cqclr(&cqc), 
								  SnapshotAny), /* XXX XXX: SnapshotAny */
					&fetchCount,
					NULL,
					cql("SELECT parchildrelid FROM pg_partition_rule "
						" WHERE paroid = :1 "
						" AND parparentrule = :2 "
						" AND parruleord = :3 ",
						ObjectIdGetDatum(parttemplid),
						ObjectIdGetDatum(InvalidOid),
						Int16GetDatum(parruleord)));

			if (fetchCount)
				Assert(pcr == InvalidOid);
			else
				add_temp = true;
			
		}
		
		rule->paroid = paroid;
		rule->parchildrelid = relid;
		rule->parparentoid = parentoid;
		rule->parisdefault = el->isDefault;
		rule->parname = parname;
		rule->parruleord = parruleord;
		rule->parrangestartincl = rangestartinc;
		rule->parrangestart = rangestart;
		rule->parrangeendincl = rangeendinc;
		rule->parrangeend = rangeend;
		rule->parrangeevery = rangeevery;
		rule->parlistvalues = (List *)listvalues;
		rule->partemplatespaceId = InvalidOid; /* only valid for template */
		
		if (!bTemplate_Only)
			add_partition_rule(rule);
		
		if (OidIsValid(parttemplid) && add_temp)
		{
			rule->paroid = parttemplid;
			rule->parparentoid = InvalidOid;
			rule->parchildrelid = InvalidOid;
			
			if (el->storeAttr)
			{
				if (((AlterPartitionCmd *)el->storeAttr)->arg1)
					rule->parreloptions =
					(List *)((AlterPartitionCmd *)el->storeAttr)->arg1;
				if (((AlterPartitionCmd *)el->storeAttr)->arg2)
				{
					Oid			tablespaceId;
					
					tablespaceId = 
					get_settable_tablespace_oid(
												strVal(((AlterPartitionCmd *)el->storeAttr)->arg2));
					
					/* get_settable_tablespace_oid will error out for us */
					Assert(OidIsValid(tablespaceId)); 
					
					/* only valid for template definitions */
					rule->partemplatespaceId = tablespaceId;
				}
			}
			add_partition_rule(rule);
		}
	}
	
	/* allow subsequent callers to see our work */
	CommandCounterIncrement();
} /* end add_part_to_catalog */


/*
 * parruleord_reset_rank
 *
 * iterate over the specified set of range partitions (in ascending
 * order) in pg_partition_rule and reset the parruleord to start at 1
 * and continue in an ascending sequence.
 */
void
parruleord_reset_rank(Oid partid, int2 level, Oid parent, int2 ruleord,
					  MemoryContext mcxt)
{
	ScanKeyData key[3];
	HeapTuple tuple;
	Relation rel;
	SysScanDesc scan;
	int ii = 1;
	
	rel = heap_open(PartitionRuleRelationId, AccessShareLock);
	
	/* CaQL UNDONE: no test coverage; this function is not called at all */
	ScanKeyInit(&key[0],
				Anum_pg_partition_rule_paroid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(partid));
	ScanKeyInit(&key[1],
				Anum_pg_partition_rule_parparentrule,
				BTEqualStrategyNumber, F_INT2EQ,
				ObjectIdGetDatum(parent));
	ScanKeyInit(&key[2],
                Anum_pg_partition_rule_parruleord,
				BTGreaterEqualStrategyNumber, F_INT2GE,
				Int16GetDatum(ruleord));
	
	scan = systable_beginscan(rel, 
							  PartitionRuleParoidParparentruleParruleordIndexId,
							  true, SnapshotNow, 3, key);
	
	while ((tuple = systable_getnext(scan)))
	{
		Form_pg_partition_rule rule_desc;
		
		Insist(HeapTupleIsValid(tuple));
		
		tuple = heap_copytuple(tuple);
		
		rule_desc =
		(Form_pg_partition_rule)GETSTRUCT(tuple);
		
		rule_desc->parruleord = ii;
		ii++;
		
		simple_heap_update(rel, &tuple->t_self, tuple);
	    CatalogUpdateIndexes(rel, tuple);
		
	}
	systable_endscan(scan);
	heap_close(rel, AccessShareLock);
} /* end parruleord_reset_rank */

/*
 * parruleord_open_gap
 *
 * iterate over the specified set of range partitions (in *DESCENDING*
 * order) in pg_partition_rule and increment the parruleord in order
 * to open a "gap" for a new partition.  The stopkey is inclusive: to
 * insert a new partition at parruleord=5, set the stopkey to 5.  The
 * current partition at parruleord=5 (and all subsequent partitions)
 * are incremented by 1 to allow the insertion of the new partition.
 *
 */
void
parruleord_open_gap(Oid partid, int2 level, Oid parent, int2 ruleord,
					int stopkey,
					MemoryContext mcxt)
{
	HeapTuple tuple;
	cqContext	*pcqCtx;
	
	/* XXX XXX: should be an ORDER BY DESC */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_partition_rule "
				" WHERE paroid = :1 "
				" AND parparentrule = :2 "
				" AND parruleord <= :3 "
				" ORDER BY paroid, parparentrule, parruleord "
				" FOR UPDATE ",
				ObjectIdGetDatum(partid),
				ObjectIdGetDatum(parent),
				Int16GetDatum(ruleord)));
	
	while (HeapTupleIsValid(tuple = caql_getprev(pcqCtx)))
	{
		int old_ruleord;
		Form_pg_partition_rule rule_desc;
		
		Insist(HeapTupleIsValid(tuple));
		
		tuple = heap_copytuple(tuple);
		
		rule_desc =
		(Form_pg_partition_rule)GETSTRUCT(tuple);
		
		old_ruleord = rule_desc->parruleord;
		rule_desc->parruleord++;
		
		caql_update_current(pcqCtx, tuple);
		
		if (old_ruleord <= stopkey)
			break;
	}
	caql_endscan(pcqCtx);

} /* end parruleord_open_gap */

/*
 * Build up a PartitionRule based on a tuple from pg_partition_rule
 * Exported for ruleutils.c
 */
PartitionRule *
ruleMakePartitionRule(HeapTuple tuple, TupleDesc tupdesc, MemoryContext mcxt)
{
	Form_pg_partition_rule rule_desc =
	(Form_pg_partition_rule)GETSTRUCT(tuple);
	text *rule_text;
	char *rule_str;
	Datum rule_datum;
	bool isnull;
	MemoryContext oldcxt;
	PartitionRule *rule;
	
	oldcxt = MemoryContextSwitchTo(mcxt);
	rule = makeNode(PartitionRule);
	MemoryContextSwitchTo(oldcxt);
	
	rule->parruleid = HeapTupleGetOid(tuple);
	rule->paroid = rule_desc->paroid;
	rule->parchildrelid = rule_desc->parchildrelid;
	rule->parparentoid = rule_desc->parparentrule;
	rule->parisdefault = rule_desc->parisdefault;
	
	rule->parname = MemoryContextStrdup(mcxt, NameStr(rule_desc->parname));
	
	rule->parruleord = rule_desc->parruleord;
	rule->parrangestartincl = rule_desc->parrangestartincl;
	rule->parrangeendincl = rule_desc->parrangeendincl;
	
	/* start range */
	rule_datum = heap_getattr(tuple,
							  Anum_pg_partition_rule_parrangestart,
							  tupdesc,
							  &isnull);
	Assert(!isnull);
	rule_text = DatumGetTextP(rule_datum);
	rule_str = DatumGetCString(DirectFunctionCall1(textout,
												   PointerGetDatum(rule_text)));
	
	oldcxt = MemoryContextSwitchTo(mcxt);
	rule->parrangestart = stringToNode(rule_str);
	MemoryContextSwitchTo(oldcxt);
	
	pfree(rule_str);
	
	/* end range */
	rule_datum = heap_getattr(tuple,
							  Anum_pg_partition_rule_parrangeend,
							  tupdesc,
							  &isnull);
	Assert(!isnull);
	rule_text = DatumGetTextP(rule_datum);
	rule_str = DatumGetCString(DirectFunctionCall1(textout,
												   PointerGetDatum(rule_text)));
	
	oldcxt = MemoryContextSwitchTo(mcxt);
	rule->parrangeend = stringToNode(rule_str);
	MemoryContextSwitchTo(oldcxt);
	
	pfree(rule_str);
	
	/* every */
	rule_datum = heap_getattr(tuple,
							  Anum_pg_partition_rule_parrangeevery,
							  tupdesc,
							  &isnull);
	Assert(!isnull);
	rule_text = DatumGetTextP(rule_datum);
	rule_str = DatumGetCString(DirectFunctionCall1(textout,
												   PointerGetDatum(rule_text)));
	
	oldcxt = MemoryContextSwitchTo(mcxt);
	rule->parrangeevery = stringToNode(rule_str);
	MemoryContextSwitchTo(oldcxt);
	
	/* list values */
	rule_datum = heap_getattr(tuple,
							  Anum_pg_partition_rule_parlistvalues,
							  tupdesc,
							  &isnull);
	Assert(!isnull);
	rule_text = DatumGetTextP(rule_datum);
	rule_str = DatumGetCString(DirectFunctionCall1(textout,
												   PointerGetDatum(rule_text)));
	
	oldcxt = MemoryContextSwitchTo(mcxt);
	rule->parlistvalues = stringToNode(rule_str);
	MemoryContextSwitchTo(oldcxt);
	
	pfree(rule_str);
	
	if (rule->parlistvalues)
		Assert(IsA(rule->parlistvalues, List));
	
	rule_datum = heap_getattr(tuple,
							  Anum_pg_partition_rule_parreloptions,
							  tupdesc,
							  &isnull);
	
	if (isnull)
		rule->parreloptions = NIL;
	else
	{
		ArrayType  *array = DatumGetArrayTypeP(rule_datum);
		Datum	   *options;
		int			noptions;
		List	   *opts = NIL;
		int i;
		
		/* XXX XXX: why not use untransformRelOptions ? */
		
		Assert(ARR_ELEMTYPE(array) == TEXTOID);
		
		deconstruct_array(array, TEXTOID, -1, false, 'i',
						  &options, NULL, &noptions);
		
		/* key/value pairs for storage clause */
		for (i = 0; i < noptions; i++)
		{
			char *n = DatumGetCString(DirectFunctionCall1(textout, options[i]));
			Value *v = NULL;
			char *s;
			
			s = strchr(n, '=');
			
			if (s)
			{
				*s = '\0';
				s++;
				if (*s)
					v = makeString(s);
			}
			
			opts = lappend(opts, makeDefElem(n, (Node *)v));
		}
		rule->parreloptions = opts;
		
	}
	
	rule_datum = heap_getattr(tuple,
							  Anum_pg_partition_rule_partemplatespace,
							  tupdesc,
							  &isnull);
	if (isnull)
		rule->partemplatespaceId = InvalidOid;
	else
		rule->partemplatespaceId = DatumGetObjectId(rule_datum);
	
	return rule;
}

/*
 * Construct a Partition node from a pg_partition tuple and its description.
 * Result is in the given memory context.
 */
Partition *
partMakePartition(HeapTuple tuple, TupleDesc tupdesc, MemoryContext mcxt)
{
	oidvector *oids;
	int2vector *atts;
	bool isnull;
	Form_pg_partition partrow = (Form_pg_partition)GETSTRUCT(tuple);
	MemoryContext oldcxt;
	Partition *p;
	
	oldcxt = MemoryContextSwitchTo(mcxt);
	p = makeNode(Partition);
	MemoryContextSwitchTo(oldcxt);
	
	p->partid = HeapTupleGetOid(tuple);
	p->parrelid = partrow->parrelid;
	p->parkind = partrow->parkind;
	p->parlevel = partrow->parlevel;
	p->paristemplate = partrow->paristemplate;
	p->parnatts = partrow->parnatts;
	
	atts = DatumGetPointer(heap_getattr(tuple, Anum_pg_partition_paratts,
										tupdesc, &isnull));
	Assert(!isnull);
	oids = DatumGetPointer(heap_getattr(tuple, Anum_pg_partition_parclass,
										tupdesc, &isnull));
	Assert(!isnull);
	
	oldcxt = MemoryContextSwitchTo(mcxt);
	p->paratts = palloc(sizeof(int2) * p->parnatts);
	p->parclass = palloc(sizeof(Oid) * p->parnatts);
	MemoryContextSwitchTo(oldcxt);
	
	memcpy(p->paratts, atts->values, sizeof(int2) * p->parnatts);
	memcpy(p->parclass, oids->values, sizeof(Oid) * p->parnatts);
	
	return p;
}

/*
 * Construct a PartitionNode-PartitionRule tree for the given part. 
 * Recurs to contruct branches.  Note that the ParitionRule (and,
 * hence, the Oid) of the given part itself is not included in the
 * result.
 *
 *    relid				--	pg_class.oid of the partitioned table
 *    level				--	partitioning level
 *    parent			--	pg_partition_rule.oid of the parent of
 *    inctemplate		--	should the tree include template rules?
 *    mcxt				--	memory context
 *    includesubparts	--	whether or not to include sub partitions
 */
PartitionNode *
get_parts(Oid relid, int2 level, Oid parent, bool inctemplate,
		  MemoryContext mcxt, bool includesubparts)
{
	PartitionNode *pnode = NULL;
	MemoryContext oldcxt;
	HeapTuple tuple;
	Relation rel;
	List *rules = NIL;
	cqContext	*pcqCtx;
	cqContext	 cqc;

	/* select oid as partid, * 
	 * from pg_partition
	 * where 
	 *     parrelid = :relid and 
	 *     parlevel = :level and 
	 *     paristemplate = :inctemplate;
	 */
	rel = heap_open(PartitionRelationId, AccessShareLock);

	tuple = caql_getfirst(
			caql_addrel(cqclr(&cqc), rel), 
			cql("SELECT * FROM pg_partition "
				" WHERE parrelid = :1 "
				" AND parlevel = :2 "
				" AND paristemplate = :3 ",
				ObjectIdGetDatum(relid),
				Int16GetDatum(level),
				BoolGetDatum(inctemplate)));

	if (HeapTupleIsValid(tuple))
	{
		pnode = makeNode(PartitionNode);
		pnode->part = partMakePartition(tuple, RelationGetDescr(rel), mcxt);
	}
	
	heap_close(rel, AccessShareLock);
	
	if ( ! pnode )
		return pnode;
	
	/* select * 
	 * from pg_partition_rule 
	 * where 
	 *     paroid = :pnode->part->partid and -- pg_partition.oid
	 *     parparentrule = :parent;
	 */
	rel = heap_open(PartitionRuleRelationId, AccessShareLock);

	if (includesubparts)
	{
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), rel),
				cql("SELECT * FROM pg_partition_rule "
					" WHERE paroid = :1 "
					" AND parparentrule = :2 ",
					ObjectIdGetDatum(pnode->part->partid),
					ObjectIdGetDatum(parent)));
	}
	else
	{
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), rel),
				cql("SELECT * FROM pg_partition_rule "
					" WHERE paroid = :1 ",
					ObjectIdGetDatum(pnode->part->partid)));
	}
	
	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		PartitionRule *rule;
		
		rule = ruleMakePartitionRule(tuple, RelationGetDescr(rel), mcxt);
		if (includesubparts)
		{
			rule->children = get_parts(relid, level + 1, rule->parruleid,
								   	   inctemplate, mcxt, true /*includesubparts*/);
		}
		
		if (rule->parisdefault)
			pnode->default_part = rule;
		else
		{
			oldcxt = MemoryContextSwitchTo(mcxt);
			rules = lappend(rules, rule);
			MemoryContextSwitchTo(oldcxt);
		}
	}
	/* NOTE: this assert is valid, except for the case of splitting
	 * the very last partition of a table.  For that case, we must
	 * drop the last partition before re-adding the new pieces, which
	 * violates this invariant
	 */
	/*	Assert(inctemplate || list_length(rules) || pnode->default_part); */
	pnode->rules = rules;
	
	caql_endscan(pcqCtx);
	heap_close(rel, AccessShareLock);
	return pnode;
}

PartitionNode *
RelationBuildPartitionDesc(Relation rel, bool inctemplate)
{
	return RelationBuildPartitionDescByOid(RelationGetRelid(rel), inctemplate);
}

PartitionNode *
RelationBuildPartitionDescByOid(Oid relid, bool inctemplate)
{
	PartitionNode *n;
	MemoryContext mcxt = CurrentMemoryContext;

	n = get_parts(relid, 0, 0, inctemplate, mcxt, true /*includesubparts*/);

	return n;
}

/*
 * Return a Bitmapset of the attribute numbers of a partitioned table
 * (not a part).  The attribute numbers refer to the root partition table.
 * Call only on the entry database.  Returns an empty set, if called on a 
 * regular table or a part.
 *
 * Note: Reads the pg_partition catalog.  If you have a PartitionNode, 
 * in hand, use get_partition_attrs and construct a Bitmapset from its
 * result instead.
 */
Bitmapset *
get_partition_key_bitmapset(Oid relid)
{
	Relation rel;
	HeapTuple tuple;
	TupleDesc tupledesc;
	cqContext	*pcqCtx;
	cqContext	 cqc;
	
	Bitmapset *partition_key = NULL;
	
	/* select paratts
	 * from pg_partition
	 * where 
	 *     parrelid = :relid and 
	 *     not paristemplate;
	 */
	rel = heap_open(PartitionRelationId, AccessShareLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), rel), 
			cql("SELECT * FROM pg_partition "
				" WHERE parrelid = :1 ",
				ObjectIdGetDatum(relid)));
	
	/* XXX XXX: hmm, could we add 				
	   " AND paristemplate = :2 ",
	   ...
	   BoolGetDatum(false)));

	   Could caql use an index on parrelid even though paristemplate
	   not in index?
	*/

	tupledesc = RelationGetDescr(rel);
	
	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
	    int i;
		int2 natts;
		int2vector *atts;
		bool isnull;
	    Form_pg_partition partrow = (Form_pg_partition)GETSTRUCT(tuple);
		
		if (partrow->paristemplate)
			continue;  /* no interest in template parts */
		
		natts = partrow->parnatts;
	    atts = DatumGetPointer(heap_getattr(tuple, Anum_pg_partition_paratts,
											tupledesc, &isnull));
		Insist(!isnull);
		
		for ( i = 0; i < natts; i++ )
			partition_key = bms_add_member(partition_key, atts->values[i]);
	}
	
	caql_endscan(pcqCtx);
	heap_close(rel, AccessShareLock);
	
	return partition_key;
}


/*
 * Return a list of partition attributes. Order is not guaranteed.
 * Caller must free the result.
 */
List *
get_partition_attrs(PartitionNode *pn)
{
	List *attrs = NIL;
	int i;
	
	if (!pn)
		return NIL;
	
	for (i = 0; i < pn->part->parnatts; i++)
		attrs = lappend_int(attrs, pn->part->paratts[i]);
	
	/* We don't want duplicates, do just go down a single branch */
	if (list_length(pn->rules))
	{
		PartitionRule *rule = linitial(pn->rules);
		
		return list_concat_unique_int(attrs, get_partition_attrs(rule->children));
	}
	else
		return attrs;
}

void
partition_get_policies_attrs(PartitionNode *pn, GpPolicy *master_policy,
							 List **cols)
{
	if (!pn)
		return;
	else
	{
		ListCell *lc;
		
		/*
		 * We use master_policy as a fast path. The assumption is that most
		 * child partitions look like the master so we don't want to enter
		 * the O(N^2) loop below if we can avoid it. Firstly, though, we must
		 * copy the master policy into the list.
		 */
		if (*cols == NIL && master_policy->nattrs)
		{
			int attno;
			
			for (attno = 0; attno < master_policy->nattrs; attno++)
				*cols = lappend_int(*cols, master_policy->attrs[attno]);
		}
		
		foreach(lc, pn->rules)
		{
			PartitionRule *rule = lfirst(lc);
			Relation rel = heap_open(rule->parchildrelid, NoLock);
			
			if (master_policy->nattrs != rel->rd_cdbpolicy->nattrs ||
				memcmp(master_policy->attrs, rel->rd_cdbpolicy->attrs,
					   (master_policy->nattrs * sizeof(AttrNumber))))
			{
				int attno;
				
				for (attno = 0; attno < rel->rd_cdbpolicy->nattrs; attno++)
				{
					if (!list_member_int(*cols,
										 rel->rd_cdbpolicy->attrs[attno]))
						*cols = lappend_int(*cols,
											rel->rd_cdbpolicy->attrs[attno]);
				}
			}
			heap_close(rel, NoLock);
			
			partition_get_policies_attrs(rule->children, master_policy,
										 cols);
		}
	}
}

bool
partition_policies_equal(GpPolicy *p, PartitionNode *pn)
{
	if (!pn)
		return true;
	
	if (pn->rules)
	{
		ListCell *lc;
		
		foreach(lc, pn->rules)
		{
			PartitionRule *rule = lfirst(lc);
			Relation rel = heap_open(rule->parchildrelid, NoLock);
			
			if (p->nattrs != rel->rd_cdbpolicy->nattrs)
			{
				heap_close(rel, NoLock);
				return false;
			}
			else
			{
				if (p->attrs == 0)
				/* random policy, skip */
					;
				if (memcmp(p->attrs, rel->rd_cdbpolicy->attrs,
						   (sizeof(AttrNumber) * p->nattrs)))
				{
					heap_close(rel, NoLock);
					return false;
				}
			}
			if (!partition_policies_equal(p, rule->children))
			{
				heap_close(rel, NoLock);
				return false;
			}
			heap_close(rel, NoLock);
		}
	}
	return true;
}

AttrNumber
max_partition_attr(PartitionNode *pn)
{
	AttrNumber n = 0;
	List *l = get_partition_attrs(pn);
	
	if (l)
	{
		ListCell *lc;
		
		foreach(lc, l)
		{
			AttrNumber att = lfirst_int(lc);
			
			n = Max(att, n);
		}
		pfree(l);
	}
	return n;
}

int
num_partition_levels(PartitionNode *pn)
{
	PartitionNode *tmp;
	int level = 0;
	
	tmp = pn;
	
	/* just descend one branch of the tree until we hit a leaf */
	while (tmp)
	{
		level++;
		if (tmp->rules)
		{
			PartitionRule *rule = linitial(tmp->rules);
			tmp = rule->children;
		}
		else if (tmp->default_part)
		{
			PartitionRule *rule = tmp->default_part;
			tmp = rule->children;
		}
		else
			tmp = NULL;
	}
	
	return level;
}

/* Return the pg_class Oids of the relations representing parts of the
 * PartitionNode tree headed by the argument PartitionNode.
 */
List *
all_partition_relids(PartitionNode *pn)
{
	if (!pn)
		return NIL;
	else
	{
		ListCell *lc;
		List *out = NIL;
		
		foreach(lc, pn->rules)
		{
			PartitionRule *rule = lfirst(lc);
			
			Assert(OidIsValid(rule->parchildrelid));
			out = lappend_oid(out, rule->parchildrelid);
			
			out = list_concat(out, all_partition_relids(rule->children));
		}
		if (pn->default_part)
		{
			out = lappend_oid(out, pn->default_part->parchildrelid);
			out = list_concat(out,
							  all_partition_relids(pn->default_part->children));
		}
		return out;
	}
}

/*
 * getPartConstraintsContainsKeys
 *   Given an OID, returns a Node that represents the check constraints
 *   on the table having constraint keys from the given key list.
 *   If there are multiple constraints they are AND'd together.
 */
static Node *
getPartConstraintsContainsKeys(Oid partOid, Oid rootOid, List *partKey)
{
	cqContext       *pcqCtx;
	cqContext       cqc;
	Relation        conRel;
	HeapTuple       conTup;
	Node            *conExpr;
	Node            *result = NULL;
	Datum           conBinDatum;
	Datum			conKeyDatum;
	char            *conBin;
	bool            conbinIsNull = false;
	bool			conKeyIsNull = false;
	AttrMap		*map;

	/* create the map needed for mapping attnums */
	Relation rootRel = heap_open(rootOid, AccessShareLock);
	Relation partRel = heap_open(partOid, AccessShareLock);

	map_part_attrs(partRel, rootRel, &map, false);

	heap_close(rootRel, AccessShareLock);
	heap_close(partRel, AccessShareLock);

	/* Fetch the pg_constraint row. */
	conRel = heap_open(ConstraintRelationId, AccessShareLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), conRel),
			cql("SELECT * FROM pg_constraint "
				" WHERE conrelid = :1",
				ObjectIdGetDatum(partOid)));

	while (HeapTupleIsValid(conTup = caql_getnext(pcqCtx)))
	{
		/* we defer the filter on contype to here in order to take advantage of
		 * the index on conrelid in the above CAQL query */
		Form_pg_constraint conEntry = (Form_pg_constraint) GETSTRUCT(conTup);
		if (conEntry->contype != 'c')
		{
			continue;
		}
		/* Fetch the constraint expression in parsetree form */
		conBinDatum = heap_getattr(conTup, Anum_pg_constraint_conbin,
				RelationGetDescr(conRel), &conbinIsNull);

		Assert (!conbinIsNull);
		/* map the attnums in constraint expression to root attnums */
		conBin = TextDatumGetCString(conBinDatum);
		conExpr = stringToNode(conBin);
		conExpr = attrMapExpr(map, conExpr);

		// fetch the key associated with this constraint
		conKeyDatum = heap_getattr(conTup, Anum_pg_constraint_conkey,
				RelationGetDescr(conRel), &conKeyIsNull);
		Datum *dats = NULL;
		int numKeys = 0;

		bool found = false;
		// extract key elements
		deconstruct_array(DatumGetArrayTypeP(conKeyDatum), INT2OID, 2, true, 's', &dats, NULL, &numKeys);
		for (int i = 0; i < numKeys; i++)
		{
			int16 key_elem =  DatumGetInt16(dats[i]);
			if (list_find_int(partKey, key_elem) >= 0)
			{
				found = true;
				break;
			}
		}

		if (found)
		{
			if (result)
				result = (Node *)make_andclause(list_make2(result, conExpr));
			else
				result = conExpr;
		}
	}

	caql_endscan(pcqCtx);
	heap_close(conRel, AccessShareLock);

	return result;
}

/*
 * Create a hash table with both key and hash entry as a constraint Node*
 * Input:
 * 	nEntries - estimated number of elements in the hash table
 * Outout:
 * 	a pointer to the created hash table
 */
static HTAB*
createConstraintHashTable(unsigned int nEntries)
{
	HASHCTL	hash_ctl;
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));

	hash_ctl.keysize = sizeof(Node**);
	hash_ctl.entrysize = sizeof(ConNodeEntry);
	hash_ctl.hash = constrNodeHash;
	hash_ctl.match = constrNodeMatch;

	return hash_create("ConstraintHashTable", nEntries, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
}

/*
 * Hash function for a constraint node
 * Input:
 * 	keyPtr - pointer to hash key
 * 	keysize - not used, hash function must have this signature
 * Output:
 * 	result - hash value as an unsigned integer
 */
static uint32
constrNodeHash(const void *keyPtr, Size keysize)
{
	uint32 result = 0;
	Node *constr = *((Node **) keyPtr);
	int con_len = 0;
	if (constr)
	{
		char* constr_bin = nodeToBinaryStringFast(constr, &con_len);
		Assert(con_len > 0);
		result = tag_hash(constr_bin, con_len);
		pfree(constr_bin);
	}

	return result;
}

/**
 * Match function for two constraint nodes.
 * Input:
 * 	keyPtr1, keyPtr2 - pointers to two hash keys
 * 	keysize - not used, hash function must have this signature
 * Output:
 * 	0 if two hash keys match, 1 otherwise
 */
static int
constrNodeMatch(const void *keyPtr1, const void *keyPtr2, Size keysize)
{
	Node *left = *((Node **) keyPtr1);
	Node *right = *((Node **) keyPtr2);
	return equal(left, right) ? 0 : 1;
}

/*
 * Check if a partitioning hierarchy is uniform, i.e. for each partitioning level,
 * all the partition nodes should have the same number of children, AND the child nodes
 * at the same position w.r.t the subtree should have the same constraint on the partition
 * key of that level.
 * The time complexity of this check is linear to the number of nodes in the partitioning
 * hierarchy.
 */
bool
rel_partitioning_is_uniform(Oid rootOid)
{
	Assert(OidIsValid(rootOid));
	Assert(rel_is_partitioned(rootOid));

	bool result = true;

	MemoryContext uniformityMemoryContext = AllocSetContextCreate(CurrentMemoryContext,
			"PartitioningIsUniform",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext callerMemoryContext = MemoryContextSwitchTo(uniformityMemoryContext);

	PartitionNode *pnRoot = RelationBuildPartitionDescByOid(rootOid, false /*inctemplate*/);
	List *queue = list_make1(pnRoot);

	while (result)
	{
		/* we process the partitioning tree level by level, each outer loop corresponds to one level */
		int size = list_length(queue);
		if (0 == size)
		{
			break;
		}

		/* Look ahead to get the number of children of the first partition node in this level.
		 * This allows us to initialize a hash table on the constraints which each partition node
		 * in this level will be compared to.
		 */
		PartitionNode *pn_ahead = (PartitionNode*) linitial(queue);
		int nChildren = list_length(pn_ahead->rules) + (pn_ahead->default_part ? 1 : 0);
		HTAB* conHash = createConstraintHashTable(nChildren);

		/* get the list of part keys for this level */
		List *lpartkey = NIL;
		for (int i = 0; i < pn_ahead->part->parnatts; i++)
		{
			lpartkey = lappend_int(lpartkey, pn_ahead->part->paratts[i]);
		}

		/* now iterate over all partition nodes on this level */
		bool fFirstNode = true;
		while (size > 0 && result)
		{
			PartitionNode *pn = (PartitionNode*) linitial(queue);
			List *lrules = get_partition_rules(pn);
			int curr_nChildren = list_length(lrules);

			if (curr_nChildren != nChildren)
			{
				result = false;
				break;
			}

			/* loop over the children's constraints of this node */
			ListCell *lc = NULL;
			foreach(lc, lrules)
			{
				PartitionRule *pr = (PartitionRule*) lfirst(lc);
				Node *curr_con = getPartConstraintsContainsKeys(pr->parchildrelid, rootOid, lpartkey);
				bool found = false;

				/* we populate the hash table with the constraints of the children of the
				 * first node in this level */
				if (fFirstNode)
				{
					/* add current constraint to hash table */
					void *con_entry = hash_search(conHash, &curr_con, HASH_ENTER, &found);
					if (con_entry == NULL)
					{
						ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
					}
					((ConNodeEntry*) con_entry)->entry = curr_con;
				}

				/* starting from the second node in this level, we probe the children's constraints */
				else
				{
					hash_search(conHash, &curr_con, HASH_FIND, &found);
					if (!found)
					{
						result = false;
						break;
					}
				}

				if (pr->children)
				{
					queue = lappend(queue, pr->children);
				}
			}
			size--;
			fFirstNode = false;
			queue = list_delete_first(queue);
			pfree(lrules);
		}

		hash_destroy(conHash);
		pfree(lpartkey);

	}

	MemoryContextSwitchTo(callerMemoryContext);
	MemoryContextDelete(uniformityMemoryContext);

	return result;
}

/* Return the pg_class Oids of the relations representing leaf parts of the
 * PartitionNode tree headed by the argument PartitionNode.
 *
 * The caller should be responsible for freeing the list after
 * using it.
 */
List *
all_leaf_partition_relids(PartitionNode *pn)
{
	if (NULL == pn)
	{
		return NIL;
	}

	ListCell *lc;
	List *leaf_relids = NIL;

	foreach(lc, pn->rules)
	{
		PartitionRule *rule = lfirst(lc);
		if (NULL != rule->children)
		{
			leaf_relids = list_concat(leaf_relids, all_leaf_partition_relids(rule->children));
		}
		else
		{
			leaf_relids = lappend_oid(leaf_relids, rule->parchildrelid);
		}

	}
	if (NULL != pn->default_part)
	{
		if (NULL != pn->default_part->children)
		{
			leaf_relids = list_concat(leaf_relids,
							  all_leaf_partition_relids(pn->default_part->children));
		}
		else
		{
			leaf_relids = lappend_oid(leaf_relids, pn->default_part->parchildrelid);
		}
	}
	return leaf_relids;
}

/*
 * Given an Oid of a partition rule, return all leaf-level table Oids that are
 * descendants of the given rule.
 * Input:
 * 	ruleOid - the oid of an entry in pg_partition_rule
 * Output:
 * 	a list of Oids of all leaf-level partition tables under the given rule in
 * 	the partitioning hierarchy.
 */
static List *
rel_get_leaf_relids_from_rule(Oid ruleOid)
{
	if (!OidIsValid(ruleOid))
	{
		return NIL;
	}

	List* lChildrenOid = NIL;
	int nChildren = caql_getcount(NULL, cql("SELECT * FROM pg_partition_rule "
											" WHERE parparentrule = :1 ",
											ObjectIdGetDatum(ruleOid)));
	/* if ruleOid is not parent of any rule, we have reached the leaf level and
	 * we need to append parchildrelid of this entry to the output
	 */
	if (nChildren == 0)
	{
		HeapTuple tuple = caql_getfirst(NULL,
									  cql("SELECT * FROM pg_partition_rule "
										  " WHERE oid = :1 ",
										  ObjectIdGetDatum(ruleOid)));
		Form_pg_partition_rule rule_desc = (Form_pg_partition_rule)GETSTRUCT(tuple);

		lChildrenOid = lcons_oid(rule_desc->parchildrelid, lChildrenOid);
	}

	/* Otherwise we are still in mid-level, hence recursively call this function
	 * on children rules of the given rule.
	 */
	else
	{
		cqContext *pcqCtx = caql_beginscan(
									NULL,
									cql("SELECT * FROM pg_partition_rule "
										" WHERE parparentrule = :1 ",
										ObjectIdGetDatum(ruleOid)));
		HeapTuple tup;
		while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
		{
			lChildrenOid = list_concat(lChildrenOid, rel_get_leaf_relids_from_rule(HeapTupleGetOid(tup)));
		}
		caql_endscan(pcqCtx);
	}

	return lChildrenOid;
}

/* Given a partition table Oid (root or interior), return the Oids of all leaf-level
 * children below it. Similar to all_leaf_partition_relids() but takes Oid as input.
 */
List *
rel_get_leaf_children_relids(Oid relid)
{
	PartStatus ps = rel_part_status(relid);
	List *leaf_relids = NIL;
	Assert(PART_STATUS_INTERIOR == ps || PART_STATUS_ROOT == ps);

	if (PART_STATUS_ROOT == ps)
	{
		PartitionNode *pn = get_parts(relid, 0 /*level*/, 0 /*parent*/, false /*inctemplate*/,
				CurrentMemoryContext, true /*includesubparts*/);
		leaf_relids = all_leaf_partition_relids(pn);
		pfree(pn);
	}
	else if (PART_STATUS_INTERIOR == ps)
	{
		HeapTuple tuple = caql_getfirst(NULL,
								  cql("SELECT * FROM pg_partition_rule "
									  " WHERE parchildrelid = :1 ",
									  ObjectIdGetDatum(relid)));
		if (HeapTupleIsValid(tuple))
		{
			leaf_relids = rel_get_leaf_relids_from_rule(HeapTupleGetOid(tuple));
			heap_freetuple(tuple);
		}
	}
	else if (PART_STATUS_LEAF == ps)
	{
		leaf_relids = list_make1_oid(relid);
	}

	return leaf_relids;
}

/* Return the pg_class Oids of the relations representing interior parts of the
 * PartitionNode tree headed by the argument PartitionNode.
 *
 * The caller is responsible for freeing the list after using it.
 */
List *
all_interior_partition_relids(PartitionNode *pn)
{
	if (NULL == pn)
	{
		return NIL;
	}

	ListCell *lc;
	List *interior_relids = NIL;

	foreach(lc, pn->rules)
	{
		PartitionRule *rule = lfirst(lc);
		if (rule->children)
		{
			interior_relids = lappend_oid(interior_relids, rule->parchildrelid);
			interior_relids = list_concat(interior_relids, all_interior_partition_relids(rule->children));
		}
	}

	if (pn->default_part)
	{
		if (pn->default_part->children)
		{
			interior_relids = lappend_oid(interior_relids, pn->default_part->parchildrelid);
			interior_relids = list_concat(interior_relids,
							  all_interior_partition_relids(pn->default_part->children));
		}
	}

	return interior_relids;
}

/*
 * Return the number of leaf parts of the partitioned table with the given oid
 */
int
countLeafPartTables(Oid rootOid)
{
	Assert (rel_is_partitioned(rootOid));

	PartitionNode *pn = get_parts(rootOid, 0 /* level */, 0 /* parent */, false /* inctemplate */,
	                              CurrentMemoryContext, true /* include subparts */);

	List *lRelOids = all_leaf_partition_relids(pn);
	Assert (list_length(lRelOids) > 0);
	int count = list_length(lRelOids);
	list_free(lRelOids);
	pfree(pn);
	return count;
}

/* Return the pg_class Oids of the relations representing the parts
 * of the PartitionRule tree headed by the argument PartitionRule.
 *
 * This local function is similar to all_partition_relids but enters
 * at a PartitionRule which is more convenient in some cases, e.g.,
 * on the topRule of a PgPartRule.
 */
List *
all_prule_relids(PartitionRule *prule)
{
	ListCell *lcr;
	PartitionNode *pnode = NULL;
	
	List *oids = NIL; /* of pg_class Oid */
	
	if ( prule )
	{
		oids = lappend_oid(oids, prule->parchildrelid);
		
		pnode = prule->children;
		if ( pnode )
		{
			oids = list_concat(oids, all_prule_relids(pnode->default_part));
			foreach (lcr, pnode->rules)
			{
				PartitionRule *child = (PartitionRule*)lfirst(lcr);
				oids = list_concat(oids, all_prule_relids(child));
			}
		}
	}
	return oids;
}

/*
 * Returns the parent Oid from the given part Oid.
 */
Oid
rel_partition_get_root(Oid relid)
{
	int fetchCount = 0;

	Oid masteroid = caql_getoid_plus(NULL,
						&fetchCount,
						NULL,
						cql("SELECT inhparent FROM pg_inherits "
							" WHERE inhrelid = :1 ", 
							ObjectIdGetDatum(relid)));

	if (!OidIsValid(masteroid))
	{
		return InvalidOid;
	}

	return masteroid;
}

/* Get the top relation of the partitioned table of which the given
 * relation is a part, or error.
 *
 *   select parrelid
 *   from pg_partition
 *   where oid = (
 *     select paroid
 *     from pg_partition_rule
 *     where parchildrelid = relid);
 */
Oid
rel_partition_get_master(Oid relid)
{
	Oid paroid = InvalidOid;
	Oid masteroid = InvalidOid;
	int	fetchCount = 0;

	paroid = caql_getoid(NULL,
						 cql("SELECT paroid FROM pg_partition_rule "
							 " WHERE parchildrelid = :1 ",
							 ObjectIdGetDatum(relid)));
	
	if (!OidIsValid(paroid))
		return InvalidOid;
	
	masteroid = caql_getoid_plus(NULL,
								 &fetchCount,
								 NULL,
								 cql("SELECT parrelid FROM pg_partition "
									 " WHERE oid = :1 ",
									 ObjectIdGetDatum(paroid)));
	
	if (!fetchCount)
		elog(ERROR, "could not find pg_partition entry with oid %d for "
			 "pg_partition_rule with child table %d", paroid, relid);
	
	
	return masteroid;
	
} /* end rel_partition_get_master */

/* given a relid, build a path list from the master tablename down to
 * the partition for that relation, using partition names if possible,
 * else rank or value expressions.
 */
List *rel_get_part_path1(Oid relid)
{
	HeapTuple	 tuple;
	Oid			 paroid		   = InvalidOid;
	Oid			 parparentrule = InvalidOid;
	List		*lrelid		   = NIL;

	/* use the relid of the table to find the first rule */

	tuple = caql_getfirst(NULL,
						  cql("SELECT * FROM pg_partition_rule "
							  " WHERE parchildrelid = :1 ",
							  ObjectIdGetDatum(relid)));
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_partition_rule rule_desc =
		(Form_pg_partition_rule)GETSTRUCT(tuple);
		
		paroid = rule_desc->paroid;
		parparentrule = rule_desc->parparentrule;
		
		/* prepend relid of child table to list */
		lrelid = lcons_oid(rule_desc->parchildrelid, lrelid);
		
	}
	
	if (!OidIsValid(paroid))
		return NIL;
	
	/* walk up the tree using the parent rule oid */
	
	while (OidIsValid(parparentrule))
	{
		tuple = caql_getfirst(NULL,
							  cql("SELECT * FROM pg_partition_rule "
								  " WHERE oid = :1 ",
								  ObjectIdGetDatum(parparentrule)));
		if (HeapTupleIsValid(tuple))
		{
			Form_pg_partition_rule rule_desc =
			(Form_pg_partition_rule)GETSTRUCT(tuple);
			
			paroid = rule_desc->paroid;
			parparentrule = rule_desc->parparentrule;
			
			/* prepend relid of child table to list */
			lrelid = lcons_oid(rule_desc->parchildrelid, lrelid);
		}
		else
			parparentrule = InvalidOid; /* we are done */
	}
	
	return lrelid;
	
} /* end rel_get_part_path1 */

static List *rel_get_part_path(Oid relid)
{
	PartitionNode 		*pNode	   = NULL;
	Partition			*part	   = NULL;
	MemoryContext		 mcxt	   = CurrentMemoryContext;
	List				*lrelid	   = NIL;
	List				*lnamerank = NIL;
	List				*lnrv	   = NIL;
	ListCell			*lc, *lc2;
	Oid					 masteroid = InvalidOid;
	
	masteroid = rel_partition_get_master(relid);
	
	if (!OidIsValid(masteroid))
		return NIL;
	
	/* call the guts of RelationBuildPartitionDesc */
	pNode = get_parts(masteroid, 0, 0, false, mcxt, true /*includesubparts*/);
	
	if (!pNode)
	{
		return NIL;
	}
	
	part = pNode->part;
	
	/* get the relids for each table that corresponds to the partition
	 * heirarchy from the master to the specified partition
	 */
	lrelid = rel_get_part_path1(relid);
	
	/* walk the partition tree, finding the partition for each relid,
	 * and extract useful information (name, rank, value)
	 */
	foreach(lc, lrelid)
	{
		Oid parrelid = lfirst_oid(lc);
		PartitionRule *prule;
		int rulerank = 1;
		
		Assert(pNode);
		
		part = pNode->part;
		
		rulerank = 1;
		
		foreach (lc2, pNode->rules)
		{
			prule = (PartitionRule *)lfirst(lc2);
			
			if (parrelid == prule->parchildrelid)
			{
				pNode = prule->children;
				goto L_rel_get_part_path_match;
			}
			rulerank++;
		}
		
		/* if we get here, then must match the default partition */
		prule = pNode->default_part;
		
		Assert(parrelid == prule->parchildrelid);
		
		/* default partition must have a name (and no rank) */
		Assert (prule->parname && strlen(prule->parname));
		
		pNode = prule->children;
		rulerank = 0;
		
	L_rel_get_part_path_match:
		
		if (!rulerank) /* must be default, so it has a name, but no
						* rank or value */
		{
			lnrv = list_make3(prule->parname, NULL, NULL);
		}
		else if (part->parkind == 'l') /* list partition by value */
		{
			char				*idval	= NULL;
			ListCell			*lc3;
			List				*l1		= (List *)prule->parlistvalues;
			StringInfoData       sid1;
			int2				 nkeys  = part->parnatts;
			int2				 parcol = 0;
			
			initStringInfo(&sid1);
			
			/*			foreach(lc3, l1) */
			/* don't loop -- just need first set of values */
			
			lc3 = list_head(l1);
			
			if (lc3)
			{
				List		*vals = lfirst(lc3);
				ListCell	*lcv  = list_head(vals);
				
				/* Note: similar code in
				 * ruleutils.c:partition_rule_def_worker
				 */
				
				for (parcol = 0; parcol < nkeys; parcol++)
				{
					Const *con = lfirst(lcv);
					
					if (lcv != list_head(vals))
						appendStringInfoString(&sid1, ", ");
					
					idval =
					deparse_expression((Node*)con,
									   deparse_context_for(get_rel_name(relid),
														   relid),
									   false, false);
					
					appendStringInfo(&sid1, "%s", idval);
					
					lcv = lnext(lcv);
				} /* end for parcol */
			}
			/* list - no rank */
			lnrv = list_make3(prule->parname, NULL, sid1.data);
		}
		else /* range (or hash) - use rank (though rank is not really
			  * appropriate for hash)
			  */
		{
			char *rtxt = palloc(NAMEDATALEN);
			
			sprintf(rtxt, "%d", rulerank);
			
			/* not list - rank, but no value */
			lnrv = list_make3(prule->parname, rtxt, NULL);
		}
		
		/* build the list of (lists of name, rank, value) for each level */
		lnamerank = lappend(lnamerank, lnrv);
		
	} /* end foreach lc (walking list of relids) */
	
	return lnamerank;
} /* end rel_get_part_path */

char *
rel_get_part_path_pretty(Oid relid,
									  char *separator,
									  char *lastsep)
{
	List				*lnamerank = NIL;
	List				*lnrv	   = NIL;
	ListCell			*lc, *lc2;
	int maxlen;
	StringInfoData      	 sid1, sid2;
	
	lnamerank = rel_get_part_path(relid);
	
	maxlen = list_length(lnamerank);
	
	if (!maxlen)
		return NULL;
	
	Assert(separator);
	Assert(lastsep);
	
	initStringInfo(&sid1);
	initStringInfo(&sid2);
	
	foreach(lc, lnamerank)
	{
		int lcnt = 0;
		
		lnrv = (List *)lfirst(lc);
		
		maxlen--;
		
		appendStringInfo(&sid1, "%s", maxlen ? separator : lastsep);
		
		lcnt = 0;
		
		foreach (lc2, lnrv)
		{
			char *str = (char *)lfirst(lc2);
			
			truncateStringInfo(&sid2, 0);
			
			switch(lcnt)
			{
				case 0:
					if (str && strlen(str))
					{
						appendStringInfo(&sid2, "\"%s\"", str);
						goto l_pretty;
					}
					break;
					
				case 1:
					if (str && strlen(str))
					{
						appendStringInfo(&sid2, "FOR(RANK(%s))", str);
						goto l_pretty;
					}
					break;
					
				case 2:
					if (str && strlen(str))
					{
						appendStringInfo(&sid2, "FOR(%s)", str);
						goto l_pretty;
					}
					break;
				default:
					break;
			}
			lcnt++;
		}
		
	l_pretty:
		
		appendStringInfo(&sid1, "%s", sid2.data);
	}
	
	return sid1.data;
} /* end rel_get_part_path_pretty */


/*
 * ChoosePartitionName: given a table name, partition "depth", and a
 * partition name, generate a unique name using ChooseRelationName.
 * The partition depth is the raw parlevel (zero-based), which is
 * incremented by 1 to be one-based.
 *
 * Note: calls CommandCounterIncrement.
 *
 * Returns a palloc'd string (from ChooseRelationName)
 */
char *
ChoosePartitionName(const char *tablename, int partDepth,
					const char *partname, Oid namespaceId)
{
	char  	*relname;
	char	 depthstr[NAMEDATALEN];
	char     prtstr[NAMEDATALEN];
	
	/* build a relation name (see transformPartitionBy */
	snprintf(depthstr, sizeof(depthstr), "%d", partDepth+1);
	snprintf(prtstr, sizeof(prtstr), "prt_%s", partname);
	
	relname = ChooseRelationName(tablename,
								 depthstr, /* depth */
								 prtstr, 	/* part spec */
								 namespaceId,
								 NULL);
	CommandCounterIncrement();
	
	return relname;
}

/*
 * Given a constant expression, build a datum according to
 * part->paratts and the relation tupledesc.  Needs work for
 * type_coercion, multicol, etc.  The returned Datum * is suitable for
 * use in SelectPartition
 */
static Datum *
magic_expr_to_datum(Relation rel, PartitionNode *partnode,
					Node *expr, bool **ppisnull)
{
	Partition  *part = partnode->part;
	TupleDesc	tupleDesc;
	Datum	   *values;
	bool	   *isnull;
	int ii, jj;
	
	Assert(rel);
	
	tupleDesc = RelationGetDescr(rel);
	
	/* Preallocate values/isnull arrays */
	ii = tupleDesc->natts;
	values = (Datum *) palloc(ii * sizeof(Datum));
	isnull = (bool *) palloc(ii * sizeof(bool));
	memset(values, 0, ii * sizeof(Datum));
	memset(isnull, true, ii * sizeof(bool));
	
	*ppisnull = isnull;
	
	Assert (IsA(expr, List));
	
	jj = list_length((List *)expr);
	
	if (jj > ii)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("too many columns in boundary specification (%d > %d)",
						jj, ii)));
	
	if (jj > part->parnatts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("too many columns in boundary specification (%d > %d)",
						jj, part->parnatts)));
	
	{
		ListCell   *lc;
		int i = 0;
		
		foreach(lc, (List *)expr)
		{
			Node *n1 = (Node *) lfirst(lc);
			Const *c1;
			AttrNumber attno = part->paratts[i++];
			Form_pg_attribute attribute = tupleDesc->attrs[attno - 1];
			Oid lhsid = attribute->atttypid;
			
			if (!IsA(n1, Const))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("Not a constant expression")));
			
			c1 = (Const *)n1;
			
			if (lhsid != c1->consttype)
			{
				/* see coerce_partition_value */
				Node *out;
				
				out = coerce_partition_value(n1, lhsid, attribute->atttypmod,
											 char_to_parttype(partnode->part->parkind));
				if (!out)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("cannot coerce column type (%s versus %s)",
									format_type_be(c1->consttype),
									format_type_be(lhsid))));
				
				Assert(IsA(out, Const));
				
				c1 = (Const *)out;
			}
			
			/* XXX: cache */
			values[attno - 1] = c1->constvalue;
			isnull[attno - 1] = c1->constisnull;
		}
	}
	
	return values;
} /* end magic_expr_to_datum */

/*
 * Assume the partition rules are in the "correct" order and return
 * the nth rule (1-based).  If rnk is negative start from the end, ie
 * -1 is the last rule.
 */
static Oid
selectPartitionByRank(PartitionNode *partnode, int rnk)
{
	Oid   relid = InvalidOid;
	List *rules = partnode->rules;
	PartitionRule *rule;
	
	Assert(partnode->part->parkind == 'r');
	
	if (rnk > list_length(rules))
		return relid;
	
	if (rnk == 0)
		return relid;
	
	if (rnk > 0)
		rnk--; /* list_nth is zero-based, not one-based */
	else if (rnk < 0)
	{
		rnk = list_length(rules) + rnk; /* if negative go from end */
		
		/* mpp-3265 */
		if (rnk < 0) /* oops -- too negative */
			return relid;
	}
	
	rule = (PartitionRule *)list_nth(rules, rnk);
	
	return rule->parchildrelid;
} /* end selectPartitionByRank */

static bool compare_partn_opfuncid(PartitionNode *partnode,
								   char *pub, char *compare_op,
								   List *colvals,
								   Datum *values, bool *isnull,
								   TupleDesc tupdesc)

{
	Partition 	*part 		 = partnode->part;
	List 		*last_opname = list_make2(makeString(pub),
										  makeString(compare_op));
	List 		*opname 	 = NIL;
	ListCell 	*lc;
	int			 numCols  	 = 0;
	int			 colCnt   	 = 0;
	int  		 ii 		 = 0;
	
	if (1 == strlen(compare_op))
	{
		/* handle case of less than or greater than */
		if (0 == strcmp("<", compare_op))
			compare_op = "<=";
		if (0 == strcmp(">", compare_op))
			compare_op = ">=";
		
		/* for a list of values, when performing less than or greater
		 * than comparison, only the final value is compared using
		 * less than or greater.  All prior values must be compared
		 * with LTE/GTE.  For example, comparing the list (1,2,3) to
		 * see if it is less than (1,2,4), we see that 1 <= 1, 2 <= 2,
		 * and 3 < 4.  So the last_opname is the specified compare_op,
		 * and the prior opnames are LTE or GTE.
		 */
		
	}
	
	opname = list_make2(makeString(pub), makeString(compare_op));
	
	colCnt = numCols = list_length(colvals);
	
	foreach(lc, colvals)
	{
		Const *c = lfirst(lc);
		AttrNumber attno = part->paratts[ii];
		
		if (isnull && isnull[attno - 1])
		{
			if (!c->constisnull)
				return false;
		}
		else
		{
			Oid lhsid = tupdesc->attrs[attno - 1]->atttypid;
			Oid rhsid = lhsid;
			Oid opfuncid;
			Datum res;
			Datum d = values[attno - 1];
			
			if (1 == colCnt)
			{
				opname = last_opname;
			}
			
			opfuncid = get_opfuncid_by_opname(opname, lhsid, rhsid);
			res = OidFunctionCall2(opfuncid, c->constvalue, d);
			
			if (!DatumGetBool(res))
				return false;
		}
		
		ii++;
		colCnt--;
	} /* end foreach */
	
	return true;
} /* end compare_partn_opfuncid */

/*
 *	Given a partition-by-list PartitionNode, search for
 *	a part that matches the given datum value.
 *
 *	Input parameters:
 *	partnode: the PartitionNode that we search for matched parts
 *	values, isnull: datum values to search for parts
 *	tupdesc: TupleDesc for retrieving values
 *	accessMethods: PartitionAccessMethods
 *	foundOid: output parameter for matched part Oid
 *	prule: output parameter for matched part PartitionRule
 *	exprTypeOid: type of the expression used to select partition
 */
static PartitionNode *
selectListPartition(PartitionNode *partnode, Datum *values, bool *isnull,
					TupleDesc tupdesc, PartitionAccessMethods *accessMethods, Oid *foundOid, PartitionRule **prule,
					Oid exprTypeOid)
{
	ListCell *lc;
	Partition *part = partnode->part;
	MemoryContext oldcxt = NULL;
	PartitionListState *ls;
	
	if (accessMethods && accessMethods->amstate[partnode->part->parlevel])
		ls = (PartitionListState *)accessMethods->amstate[partnode->part->parlevel];
	else
	{
		int natts = partnode->part->parnatts;
		
		ls = palloc(sizeof(PartitionListState));
		
		ls->eqfuncs = palloc(sizeof(FmgrInfo) * natts);
		ls->eqinit = palloc0(sizeof(bool) * natts);
		
		if (accessMethods)
			accessMethods->amstate[partnode->part->parlevel] = (void *)ls;
	}
	
	if (accessMethods && accessMethods->part_cxt)
		oldcxt = MemoryContextSwitchTo(accessMethods->part_cxt);
	
	*foundOid = InvalidOid;
	
	/* With LIST, we have no choice at the moment except to be exhaustive */
	foreach(lc, partnode->rules)
	{
		PartitionRule *rule = lfirst(lc);
		List *vals = rule->parlistvalues;
		ListCell *lc2;
		bool matched = false;
		
		/*
		 * list values are stored in a list of lists to support multi column
		 * partitions.
		 *
		 * At this level, we're processing the list of possible values for the
		 * given rule, for example:
		 *     values(1, 2, 3)
		 *     values((1, '2005-01-01'), (2, '2006-01-01'))
		 *
		 * Each iteraction is one element of the values list. In the first
		 * example, we iterate '1', '2' then '3'. For the second, we iterate
		 * through '(1, '2005-01-01')' then '(2, '2006-01-01')'. It's for that
		 * reason that we need the inner loop.
		 */
		foreach(lc2, vals)
		{
			ListCell *lc3;
			List *colvals = (List *)lfirst(lc2);
			int i = 0;
			
		   	matched = true; /* prove untrue */
			
			foreach(lc3, colvals)
			{
				Const *c = lfirst(lc3);
				AttrNumber attno = part->paratts[i];
				
				if (isnull[attno - 1])
				{
					if (!c->constisnull)
					{
						matched = false;
						break;
					}
				}
				else if (c->constisnull)
				{
					/* constant is null but datum isn't so break */
					matched = false;
					break;
				}
				else
				{
					Datum res;
					Datum d = values[attno - 1];
					FmgrInfo *finfo;
					
					if (!ls->eqinit[i])
					{

						/*
						 * Compute the type of the LHS and RHS for the equality comparator.
						 * The way we call the comparator is comp(expr, rule)
						 * So lhstypid = type(expr) and rhstypeid = type(rule)
						 */

						/* The tupdesc tuple descriptor matches the table schema, so it has the rule type */
						Oid rhstypid = tupdesc->attrs[attno - 1]->atttypid;

						/*
						 * exprTypeOid is passed to us from our caller which evaluated the expression.
						 * In some cases (e.g legacy optimizer doing explicit casting), we don't compute
						 * specify exprTypeOid.
						 * Assume lhstypid = rhstypid in those cases
						 */
						Oid lhstypid = exprTypeOid;
						if (!OidIsValid(lhstypid))
						{
							lhstypid = rhstypid;
						}

						List *opname = list_make2(makeString("pg_catalog"),
												  makeString("="));
						
						Oid opfuncid = get_opfuncid_by_opname(opname, lhstypid, rhstypid);
						fmgr_info(opfuncid, &(ls->eqfuncs[i]));
						ls->eqinit[i] = true;
					}
					
					finfo = &(ls->eqfuncs[i]);
					res = FunctionCall2(finfo, d, c->constvalue);
					
					if (!DatumGetBool(res))
					{
						/* found it */
						matched = false;
						break;
					}
				}
				i++;
			}
			if (matched)
				break;
		}
		
		if (matched)
		{
			*foundOid = rule->parchildrelid;
			*prule = rule;
			
			/* go to the next level */
			if (oldcxt)
				MemoryContextSwitchTo(oldcxt);
			return rule->children;
		}
	}
	
	if (oldcxt)
		MemoryContextSwitchTo(oldcxt);
	
	return NULL;
	
}

/*
 * get_less_than_oper()
 *
 * Retrieves the appropriate comparator that knows how to handle
 * the two types lhsid, rhsid.
 *
 * Input parameters:
 * lhstypid: Type oid of the LHS of the comparator
 * rhstypid: Type oid of the RHS of the comparator
 * strictlyless: If true, requests the 'strictly less than' operator instead of 'less or equal than'
 *
 * Returns: The Oid of the appropriate comparator; throws an ERROR if no such comparator exists
 *
 */
static Oid
get_less_than_oper(Oid lhstypid, Oid rhstypid, bool strictlyless)
{
	Value *str = strictlyless ? makeString("<") : makeString("<=");
	Value *pub = makeString("pg_catalog");
	List *opname = list_make2(pub, str);

	Oid funcid = get_opfuncid_by_opname(opname, lhstypid, rhstypid);
	list_free_deep(opname);

	return funcid;
}

/*
 * get_comparator
 *   Retrieves the comparator function between the expression and the partition rules
 *
 *  Input:
 *   keyno: Index in the key array of the partitioning key considered
 *   rs: the current PartitionRangeState
 *   ruleTypeOid: Oid for the type of the partition rules
 *   exprTypeOid: Oid for the type of the expressions
 *   strictlyless: If true, the operator for strictly less than (LT) is retrieved. Otherwise,
 *     it's less or equal than (LE)
 *   is_direct: If true, then the "direct" comparison (expression OP rule) is retrieved.
 *     Otherwise, it's the "inverse" comparison (rule OP expression)
 *
 */
static FmgrInfo *
get_less_than_comparator(int keyno, PartitionRangeState *rs, Oid ruleTypeOid, Oid exprTypeOid, bool strictlyless, bool is_direct)
{

	Assert(NULL != rs);

	Oid lhsOid = InvalidOid;
	Oid rhsOid = InvalidOid;
	FmgrInfo *funcInfo = NULL;

	if (is_direct && strictlyless) {
		/* Looking for expr < partRule comparator */
		funcInfo = &rs->ltfuncs_direct[keyno];
	} else if (is_direct && !strictlyless) {
		/* Looking for expr <= partRule comparator */
		funcInfo = &rs->lefuncs_direct[keyno];
	} else if (!is_direct && strictlyless) {
		/* Looking for partRule < expr comparator */
		funcInfo = &rs->ltfuncs_inverse[keyno];
	} else if (!is_direct && !strictlyless) {
		/* Looking for partRule <= expr comparator */
		funcInfo = &rs->lefuncs_inverse[keyno];
	}

	Assert(NULL != funcInfo);

	if (!OidIsValid(funcInfo->fn_oid)) {
		/* We haven't looked up this comparator before, let's do it now */

		if (is_direct) {
			/* Looking for "direct" comparators (expr OP partRule ) */
			lhsOid = exprTypeOid;
			rhsOid = ruleTypeOid;
		}
		else {
			/* Looking for "inverse" comparators (partRule OP expr ) */
			lhsOid = ruleTypeOid;
			rhsOid = exprTypeOid;
		}

		Oid funcid = get_less_than_oper(lhsOid, rhsOid, strictlyless);
		fmgr_info(funcid, funcInfo);
	}

	Assert(OidIsValid(funcInfo->fn_oid));
	return funcInfo;
}

/*
 * range_test
 *   Test if an expression value falls in the range of a given partition rule
 *
 *  Input parameters:
 *    tupval: The value of the expression
 *    ruleTypeOid: The type of the partition rule boundaries
 *    exprTypeOid: The type of the expression (can be different from ruleTypeOid
 *      if types can be directly compared with each other)
 *    rs: The partition range state
 *    keyno: The index of the partitioning key considered (for composite partitioning keys)
 *    rule: The rule whose boundaries we're testing
 *
 */
static int
range_test(Datum tupval, Oid ruleTypeOid, Oid exprTypeOid, PartitionRangeState *rs, int keyno,
		   PartitionRule *rule)
{
	Const *c = NULL;
	FmgrInfo *finfo;
	Datum res;
	
	Assert(PointerIsValid(rule->parrangestart) ||
		   PointerIsValid(rule->parrangeend));
	
	/* might be a rule with no START value */
	if (PointerIsValid(rule->parrangestart))
	{
		Assert(IsA(rule->parrangestart, List));
#if NOT_YET
		c = (Const *)list_nth((List *)rule->parrangestart, keyno);
#else
		c = (Const *)linitial((List *)rule->parrangestart);
#endif
		
		/*
		 * Is the value in the range?
		 *   If rule->parrangestartincl, we request for comparator ruleVal <= exprVal ( ==> strictly_less = false)
		 *   Otherwise, we request comparator ruleVal < exprVal ( ==> strictly_less = true)
		 */
		finfo = get_less_than_comparator(keyno, rs, ruleTypeOid, exprTypeOid, !rule->parrangestartincl /* strictly_less */, false /* is_direct */);
		res = FunctionCall2(finfo, c->constvalue, tupval);
		
		if (!DatumGetBool(res))
			return -1;
	}
	
	/* There might be no END value */
	if (PointerIsValid(rule->parrangeend))
	{
#if NOT_YET
		c = (Const *)list_nth((List *)rule->parrangeend, keyno);
#else
		c = (Const *)linitial((List *)rule->parrangeend);
#endif

		/*
		 * Is the value in the range?
		 *   If rule->parrangeendincl, we request for comparator exprVal <= ruleVal ( ==> strictly_less = false)
		 *   Otherwise, we request comparator exprVal < ruleVal ( ==> strictly_less = true)
		 */
		finfo = get_less_than_comparator(keyno, rs, ruleTypeOid, exprTypeOid, !rule->parrangeendincl /* strictly_less */, true /* is_direct */);
		res = FunctionCall2(finfo, tupval, c->constvalue);
		
		if (!DatumGetBool(res))
		{
			return 1;
		}
	}
	return 0;
}

/*
 * Given a partition specific part, a tuple as represented by values and isnull and
 * a list of rules, return an Oid in *foundOid or the next set of rules.
 */
static PartitionNode *
selectRangePartition(PartitionNode *partnode, Datum *values, bool *isnull,
					 TupleDesc tupdesc, PartitionAccessMethods *accessMethods,
					 Oid *foundOid, int *pSearch, PartitionRule **prule, Oid exprTypeOid)
{
	List *rules = partnode->rules;
	int high = list_length(rules) - 1;
	int low = 0;
    int searchpoint = 0;
	int mid = 0;
	bool matched = false;
	PartitionRule *rule = NULL;
	PartitionNode *pNode = NULL;
	PartitionRangeState *rs = NULL;
	MemoryContext oldcxt = NULL;
	
	Assert(partnode->part->parkind == 'r');
	/* For composite partitioning keys, exprTypeOid should always be InvalidOid */
	AssertImply(partnode->part->parnatts > 1, !OidIsValid(exprTypeOid));
	
	if (accessMethods && accessMethods->amstate[partnode->part->parlevel])
		rs = (PartitionRangeState *)accessMethods->amstate[partnode->part->parlevel];
	else
	{
		int natts = partnode->part->parnatts;
		
		/*
		 * We're still in our caller's memory context so
		 * the memory will persist long enough for us.
		 */
		rs = palloc(sizeof(PartitionRangeState));
		rs->lefuncs_direct = palloc0(sizeof(FmgrInfo) * natts);
		rs->ltfuncs_direct = palloc0(sizeof(FmgrInfo) * natts);
		rs->lefuncs_inverse = palloc0(sizeof(FmgrInfo) * natts);
		rs->ltfuncs_inverse = palloc0(sizeof(FmgrInfo) * natts);
		
		/*
		 * Set the function Oid to InvalidOid to signal that we
		 * haven't looked up this function yet
		 */
		for (int keyno = 0; keyno < natts; keyno++)
		{
			rs->lefuncs_direct[keyno].fn_oid = InvalidOid;
			rs->ltfuncs_direct[keyno].fn_oid = InvalidOid;
			rs->lefuncs_inverse[keyno].fn_oid = InvalidOid;
			rs->lefuncs_inverse[keyno].fn_oid = InvalidOid;
		}

		/*
		 * Unrolling the rules into an array currently works for the
		 * top level partition only
		 */
		if (partnode->part->parlevel == 0)
		{
			int i = 0;
			ListCell *lc;
			
			rs->rules = palloc(sizeof(PartitionRule *) * list_length(rules));
			
			foreach(lc, rules)
			rs->rules[i++] = (PartitionRule *)lfirst(lc);
		}
		else
			rs->rules = NULL;
	}
	
	if (accessMethods && accessMethods->part_cxt)
		oldcxt = MemoryContextSwitchTo(accessMethods->part_cxt);
	
	*foundOid = InvalidOid;

	/*
	 * Use a binary search to try and pin point the region within the set of
	 * rules where the rule is. If the partition is across a single column,
	 * the rule located by the binary search is the only possible candidate.
	 * If the partition is across more than one column, we need to search
	 * sequentially to either side of the rule to see if the match is there.
	 * The reason for this complexity is the nature of find a point within an
	 * interval.
	 *
	 * Consider the following intervals:
	 *
	 * 1. start( 1, 8)   end( 10,  9)
	 * 2. start( 1, 9)   end( 15, 10)
	 * 3. start( 1, 11)  end(100, 12)
	 * 4. start(15, 10)  end( 30, 11)
	 *
	 * If we were to try and find the partition for a tuple (25, 10), using the
	 * binary search for the first element, we'd select partition 3 but
	 * partition 4 is also a candidate. It is only when we look at the second
	 * element that we find the single definitive rule.
	 */
	while (low <= high)
	{
		AttrNumber attno = partnode->part->paratts[0];
		Datum exprValue = values[attno - 1];
		int ret;
		
		mid = low + (high - low)/2;
		
		if (rs->rules)
			rule = rs->rules[mid];
		else
			rule = (PartitionRule *)list_nth(rules, mid);
		
		if (isnull[attno - 1])
		{
			pNode = NULL;
			goto l_fin_range;
		}

		Oid ruleTypeOid = tupdesc->attrs[attno - 1]->atttypid;
		if (OidIsValid(exprTypeOid))
		{
			ret = range_test(exprValue, ruleTypeOid, exprTypeOid, rs, 0, rule);
		}
		else
		{
			/*
			 * In some cases, we don't have an expression type oid. In those cases, the expression and
			 * partition rules have the same type.
			 */
			ret = range_test(exprValue, ruleTypeOid, ruleTypeOid, rs, 0, rule);
		}
		
		if (ret > 0)
		{
			searchpoint = mid;
			low = mid + 1;
			continue;
		}
		else if (ret < 0)
		{
			high = mid - 1;
			continue;
		}
		else
		{
			matched = true;
			break;
		}
	}
	
	if (matched)
	{
		int j;
		
		/* Non-composite partition key, we matched so we're done */
		if (partnode->part->parnatts == 1)
		{
			*foundOid = rule->parchildrelid;
			*prule = rule;
	
			pNode = rule->children;
			goto l_fin_range;
		}
		
		/* We have more than one partition key.. Must match on the other keys as well */
		j = mid;
		do
		{
			int i;
			bool matched = true;
			bool first_fail = false;
			
			for (i = 0; i < partnode->part->parnatts; i++)
			{
				AttrNumber attno = partnode->part->paratts[i];
				Datum d = values[attno - 1];
				int ret;
				
				if (j != mid)
					rule = (PartitionRule *)list_nth(rules, j);
				
				if (isnull[attno - 1])
				{
					pNode = NULL;
					goto l_fin_range;
				}
				
				Oid ruleTypeOid = tupdesc->attrs[attno - 1]->atttypid;
				/* For composite partition keys, we don't support casting comparators, so both sides must be of identical types */
				Assert(!OidIsValid(exprTypeOid));
				ret = range_test(d, ruleTypeOid, ruleTypeOid,
								 rs, i, rule);
				if (ret != 0)
				{
					matched = false;
					
					/*
					 * If we've gone beyond the boundary of rules which match
					 * the first tuple, no use looking further
					 */
					if (i == 0)
						first_fail = true;
					
					break;
				}
			}
			
			if (first_fail)
				break;
			
			if (matched)
			{
				*foundOid = rule->parchildrelid;
				*prule = rule;
	
				pNode = rule->children;
				goto l_fin_range;
			}
			
		}
		while (--j >= 0);
		
		j = mid;
		do
		{
			int i;
			bool matched = true;
			bool first_fail = false;
			
			for (i = 0; i < partnode->part->parnatts; i++)
			{
				AttrNumber attno = partnode->part->paratts[i];
				Datum d = values[attno - 1];
				int ret;
				
				rule = (PartitionRule *)list_nth(rules, j);
				
				if (isnull[attno - 1])
				{
					pNode = NULL;
					goto l_fin_range;
				}
				
				Oid ruleTypeOid = tupdesc->attrs[attno - 1]->atttypid;
				/* For composite partition keys, we don't support casting comparators, so both sides must be of identical types */
				Assert(!OidIsValid(exprTypeOid));
				ret = range_test(d, ruleTypeOid, ruleTypeOid, rs, i, rule);
				if (ret != 0)
				{
					matched = false;
					
					/*
					 * If we've gone beyond the boundary of rules which match
					 * the first tuple, no use looking further
					 */
					if (i == 0)
						first_fail = true;
					
					break;
				}
			}
			
			if (first_fail)
				break;
			if (matched)
			{
				*foundOid = rule->parchildrelid;
				*prule = rule;
				
				pNode = rule->children;
				goto l_fin_range;
			}
			
		}
		while (++j < list_length(rules));
	} /* end if matched */
	
	pNode = NULL;
	
l_fin_range:
	if (pSearch)
		*pSearch = searchpoint;
	
	if (oldcxt)
		MemoryContextSwitchTo(oldcxt);
	
	if (accessMethods)
		accessMethods->amstate[partnode->part->parlevel] = (void *)rs;
	
	return pNode;
} /* end selectrangepartition */


/* select partition via hash */
static PartitionNode *
selectHashPartition(PartitionNode *partnode, Datum *values, bool *isnull,
					TupleDesc tupdesc, PartitionAccessMethods *accessMethods, Oid *found, PartitionRule **prule)
{
	uint32 hash = 0;
	int i;
	int part;
	PartitionRule *rule;
	MemoryContext oldcxt = NULL;
	
	if (accessMethods && accessMethods->part_cxt)
		oldcxt = MemoryContextSwitchTo(accessMethods->part_cxt);
	
	for (i = 0; i < partnode->part->parnatts; i++)
	{
		AttrNumber attnum = partnode->part->paratts[i];
		
		/* rotate hash left 1 bit at each step */
		hash = (hash << 1) | ((hash & 0x80000000) ? 1 : 0);
		
		/*
		 * If we found a NULL, just pretend it has a hashcode of 0 (like the
		 * hash join code does.
		 */
		if (isnull[attnum - 1])
			continue;
		else
		{
			Oid opclass = partnode->part->parclass[i];
			Oid hashfunc = get_opclass_proc(opclass, 0, HASHPROC);
			Datum d = values[attnum - 1];
			hash ^= DatumGetUInt32(OidFunctionCall1(hashfunc, d));
		}
	}
	
	part = hash % list_length(partnode->rules);
	
	rule = (PartitionRule *)list_nth(partnode->rules, part);
	
	*found = rule->parchildrelid;
	*prule = rule;
	
	if (oldcxt)
		MemoryContextSwitchTo(oldcxt);
	
	return rule->children;
}

/*
 * selectPartition1()
 *
 * Given pdata and prules, try and find a suitable partition for the input key.
 * values is an array of datums representing the partitioning key, isnull
 * tells us which of those is NULL. pSearch allows the caller to get the
 * position in the partition range where the key falls (might be hypothetical).
 */
static Oid
selectPartition1(PartitionNode *partnode, Datum *values, bool *isnull,
				 TupleDesc tupdesc, PartitionAccessMethods *accessMethods,
				 int *pSearch,
				 PartitionNode **ppn_out)
{
	Oid relid = InvalidOid;
	Partition *part = partnode->part;
	PartitionNode *pn = NULL;
	PartitionRule *prule = NULL;
	
	if (ppn_out)
		*ppn_out = NULL;
	
	/* what kind of partition? */
	switch (part->parkind)
	{
		case 'r': /* range */
			pn = selectRangePartition(partnode, values, isnull, tupdesc,
									  accessMethods, &relid, pSearch, &prule, InvalidOid);
			break;
		case 'h': /* hash */
			pn = selectHashPartition(partnode, values, isnull, tupdesc,
									 accessMethods, &relid, &prule);
			break;
		case 'l': /* list */
			pn = selectListPartition(partnode, values, isnull, tupdesc,
									 accessMethods, &relid, &prule, InvalidOid);
			break;
		default:
			elog(ERROR, "unrecognized partitioning kind '%c'",
				 part->parkind);
			break;
	}
	
	if (pn)
	{
		if (ppn_out)
		{
			*ppn_out = pn;
			return relid;
		}
		return selectPartition1(pn, values, isnull, tupdesc, accessMethods,
								pSearch, ppn_out);
	}
	else
	{
		/* retry a default */
		if (partnode->default_part && !OidIsValid(relid))
		{
			if (partnode->default_part->children)
			{
				if (ppn_out)
				{
					*ppn_out = partnode->default_part->children;
					
					/* don't return the relid, it is invalid -- return
					 * the relid of the default partition instead
					 */
					return partnode->default_part->parchildrelid;
				}
				return selectPartition1(partnode->default_part->children,
										values, isnull, tupdesc, accessMethods,
										pSearch, ppn_out);
			}
			else
				return partnode->default_part->parchildrelid;
		}
		else
			return relid;
	}
}

Oid
selectPartition(PartitionNode *partnode, Datum *values, bool *isnull,
				TupleDesc tupdesc, PartitionAccessMethods *accessMethods)
{
	return selectPartition1(partnode, values, isnull, tupdesc, accessMethods,
							NULL, NULL);
}

/*
 * get_next_level_matched_partition()
 *
 * Given pdata and prules, try to find a suitable partition for the input key.
 * values is an array of datums representing the partitioning key, isnull
 * tells us which of those is NULL. It will return NULL if no part matches.
 *
 * Input parameters:
 * partnode: the PartitionNode that we search for matched parts
 * values, isnull: datum values to search for parts
 * tupdesc: TupleDesc for retrieving values
 * accessMethods: PartitionAccessMethods
 * exprTypid: the type of the datum
 *
 * return: PartitionRule of which constraints match the input key
 */
PartitionRule*
get_next_level_matched_partition(PartitionNode *partnode, Datum *values, bool *isnull,
								TupleDesc tupdesc, PartitionAccessMethods *accessMethods,
								Oid exprTypid)
{
	Oid relid = InvalidOid;
	Partition *part = partnode->part;
	PartitionRule *prule = NULL;

	/* what kind of partition? */
	switch (part->parkind)
	{
		case 'r': /* range */
			selectRangePartition(partnode, values, isnull, tupdesc,
								accessMethods, &relid, NULL, &prule, exprTypid);
			break;
		case 'l': /* list */
			selectListPartition(partnode, values, isnull, tupdesc,
								accessMethods, &relid, &prule, exprTypid);
			break;
		default:
			elog(ERROR, "unrecognized partitioning kind '%c'",
				 part->parkind);
			break;
	}

	if (NULL != prule)
	{
		return prule;
	}
	/* retry a default */
	return partnode->default_part;
}

/*
 * get_part_rule
 *
 * find PartitionNode and the PartitionRule for a partition if it
 * exists.
 *
 * If bExistError is not set, just return the PgPartRule.
 * If bExistError is set, return an error message based upon
 * bMustExist.  That is, if the table does not exist and
 * bMustExist=true then return an error.  Conversely, if the table
 * *does* exist and bMustExist=false then return an error.
 *
 * If pSearch is set, return a ptr to the position where the pid
 * *might* be.  For get_part_rule1, pNode is the position to start at
 * (ie not necessarily at the top).  relname is a char string to
 * describe the current relation/partition for error messages, eg
 * 'relation "foo"' or 'partition "baz" of relation "foo"'.
 *
 */
PgPartRule*
get_part_rule1(Relation rel, 
			   AlterPartitionId *pid,
			   bool bExistError, 
			   bool bMustExist,
			   MemoryContext mcxt, 
			   int *pSearch,
			   PartitionNode *pNode,
			   char *relname,
			   PartitionNode **ppNode
			   )
{
	char		   namBuf[NAMEDATALEN]; /* the real partition name */
	
	/* a textual representation of the partition id (for error msgs) */
	char		   partIdStr[(NAMEDATALEN * 2)];
	
	PgPartRule  		*prule 	   = NULL;
	
	Oid 			 	 partrelid = InvalidOid;
	int 			 	 idrank    = 0;	/* only set for range partns by rank */
	
	if (!pid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("No partition id specified for %s",
						relname),
				 errOmitLocation(true)));
	
	namBuf[0] = 0;
	
	/* build the partition "id string" for error messages as a
	 * partition name, value, or rank.
	 *
	 * Later on, if we discover
	 *   (the partition exists) and
	 *   (it has a name)
	 * then we update the partIdStr to the name
	 */
	switch (pid->idtype)
	{
		case AT_AP_IDNone:				/* no ID */
			/* should never happen */
			partIdStr[0] = 0;
			
			break;
		case AT_AP_IDName:				/* IDentify by Name */
			snprintf(partIdStr, sizeof(partIdStr), " \"%s\"",
					 strVal(pid->partiddef));
			snprintf(namBuf, sizeof(namBuf), "%s",
					 strVal(pid->partiddef));
			break;
		case AT_AP_IDValue:				/* IDentifier FOR Value */
			snprintf(partIdStr, sizeof(partIdStr), " for specified value");
			break;
		case AT_AP_IDRank:				/* IDentifier FOR Rank */
		{
			snprintf(partIdStr, sizeof(partIdStr), " for specified rank");
			
#ifdef WIN32
#define round(x) (x+0.5)
#endif
			if (IsA(pid->partiddef, Integer))
				idrank = intVal(pid->partiddef);
			else if (IsA(pid->partiddef, Float))
				idrank = floor(floatVal(pid->partiddef));
			else
				Assert(false);
			
			snprintf(partIdStr, sizeof(partIdStr),
					 " for rank %d",
					 idrank);
		}
			break;
		case AT_AP_ID_oid:				/* IDentifier by oid */
			snprintf(partIdStr, sizeof(partIdStr), " for oid %u",
					 *((Oid *)(pid->partiddef)));
			break;
		case AT_AP_IDDefault:			/* IDentify DEFAULT partition */
			snprintf(partIdStr, sizeof(partIdStr), " for DEFAULT");
			break;
		case AT_AP_IDRule:
		{
			PgPartRule *p = linitial((List *)pid->partiddef);
			snprintf(partIdStr, sizeof(partIdStr), "%s",
					 p->partIdStr);
			return p;
			break;
		}
		default: /* XXX XXX */
			Assert(false);
			
	}
	
	if (bExistError && !pNode)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("%s is not partitioned",
						relname),
				 errOmitLocation(true)));
	
	/* if id is a value or rank, get the relid of the partition if
	 * it exists */
	if (pNode)
	{
		if (pid->idtype == AT_AP_IDValue)
		{
			TupleDesc			 tupledesc = RelationGetDescr(rel);
			bool				*isnull;
			PartitionNode 		*pNode2	   = NULL;
			Datum	*d	= magic_expr_to_datum(rel, pNode,
											  pid->partiddef, &isnull);
			
			/* MPP-4011: get right pid for FOR(value).  pass a pNode
			 * ptr down to prevent recursion in selectPartition -- we
			 * only want the top-most partition for the value in this
			 * case
			 */
			if (ppNode)
				partrelid = selectPartition1(pNode, d, isnull, tupledesc, NULL,
											 pSearch, ppNode);
			else
				partrelid = selectPartition1(pNode, d, isnull, tupledesc, NULL,
											 pSearch, &pNode2);
			
			/* build a string rep for the value */
			{
				ParseState 		*pstate = make_parsestate(NULL);
				Node 			*pval 	= (Node *)pid->partiddef;
                char 			*idval 	= NULL;
				
				pval = (Node *)transformExpressionList(pstate,
													   (List *)pval);
				
				free_parsestate(&pstate);
				
				idval =
				deparse_expression(pval,
								   deparse_context_for(RelationGetRelationName(rel),
													   RelationGetRelid(rel)),
								   false, false);
				
				if (idval)
					snprintf(partIdStr, sizeof(partIdStr),
							 " for value (%s)",
							 idval);
			}
			
		}
		else if (pid->idtype == AT_AP_IDRank)
		{
			char *parTypName = "UNKNOWN";
			
			if (pNode->part->parkind != 'r')
			{
				switch (pNode->part->parkind)
				{
					case 'h': /* hash */
						parTypName = "HASH";
						break;
					case 'l': /* list */
						parTypName = "LIST";
						break;
				} /* end switch */
				
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot find partition by RANK -- "
								"%s is %s partitioned",
								relname,
								parTypName),
						 errOmitLocation(true)));
				
			}
			
			partrelid =	selectPartitionByRank(pNode, idrank);
		}
	}
	
	/* check thru the list of partition rules to match by relid or name */
	if (pNode)
	{
		ListCell *lc;
		int rulerank = 1;
		
		/* set up the relid for the default partition if necessary */
		if ((pid->idtype == AT_AP_IDDefault)
			&& pNode->default_part)
			partrelid = pNode->default_part->parchildrelid;
		
		foreach(lc, pNode->rules)
		{
			PartitionRule *rule = lfirst(lc);
			bool foundit = false;
			
			if ((pid->idtype == AT_AP_IDValue)
				|| (pid->idtype == AT_AP_IDRank))
			{
				if ((partrelid != InvalidOid)
					&& (partrelid == rule->parchildrelid))
				{
					foundit = true;
					
					if (strlen(rule->parname))
					{
						snprintf(partIdStr, sizeof(partIdStr), " \"%s\"",
								 rule->parname);
						snprintf(namBuf, sizeof(namBuf), "%s",
								 rule->parname);
					}
				}
			}
			else if (pid->idtype == AT_AP_IDName)
			{
				if (0 == strcmp(rule->parname, namBuf))
					foundit = true;
			}
			
			if (foundit)
			{
				prule = makeNode(PgPartRule);
				
				prule->pNode = pNode;
				prule->topRule = rule;
				prule->topRuleRank = rulerank; /* 1-based */
				prule->relname = relname;
				break;
			}
			rulerank++;
		} /* end foreach */
		
		/* if cannot find, check default partition */
		if (!prule && pNode->default_part)
		{
			PartitionRule *rule = pNode->default_part;
			bool foundit = false;
			
			if ((pid->idtype == AT_AP_IDValue)
				|| (pid->idtype == AT_AP_IDRank)
				|| (pid->idtype == AT_AP_IDDefault))
			{
				if ((partrelid != InvalidOid)
					&& (partrelid == rule->parchildrelid))
				{
					foundit = true;
					
					if (strlen(rule->parname))
					{
						snprintf(partIdStr, sizeof(partIdStr), " \"%s\"",
								 rule->parname);
						snprintf(namBuf, sizeof(namBuf), "%s",
								 rule->parname);
					}
				}
			}
			else if (pid->idtype == AT_AP_IDName)
			{
				if (0 == strcmp(rule->parname, namBuf))
					foundit = true;
			}
			
			if (foundit)
			{
				prule = makeNode(PgPartRule);
				
				prule->pNode = pNode;
				prule->topRule = rule;
				prule->topRuleRank = 0; /* 1-based -- 0 means no rank */
				prule->relname = relname;
			}
		}
	} /* end if pnode */
	
	/* if the partition exists, set the "id string" in prule and
	 * indicate whether it is the partition name.  The ATPExec
	 * commands will notify users of the "real" name if the original
	 * specification was by value or rank
	 */
	if (prule)
	{
		prule->partIdStr = pstrdup(partIdStr);
		prule->isName = (strlen(namBuf) > 0);
	}
	
	if (!bExistError)
		goto L_fin_partrule;
	
	/* MPP-3722: complain if for(value) matches the default partition */
	if ((pid->idtype == AT_AP_IDValue)
		&& prule &&
		(prule->topRule == prule->pNode->default_part))
	{
		if (bMustExist)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("FOR expression matches "
							"DEFAULT partition%s of %s",
							prule->isName ?
							partIdStr : "",
							relname),
					 errhint("FOR expression may only specify "
							 "a non-default partition in this context."),
					 errOmitLocation(true)));
		
	}
	
	if (bMustExist && !prule)
	{
		switch (pid->idtype)
		{
			case AT_AP_IDNone:				/* no ID */
				/* should never happen */
				Assert(false);
				break;
			case AT_AP_IDName:				/* IDentify by Name */
			case AT_AP_IDValue:				/* IDentifier FOR Value */
			case AT_AP_IDRank:				/* IDentifier FOR Rank */
			case AT_AP_ID_oid:				/* IDentifier by oid */
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("partition%s of %s does not exist",
								partIdStr,
								relname),
						 errOmitLocation(true)));
				break;
			case AT_AP_IDDefault:			/* IDentify DEFAULT partition */
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("DEFAULT partition of %s does not exist",
								relname),
						 errOmitLocation(true)));
				break;
			default: /* XXX XXX */
				Assert(false);
				
		}
	}
	else if (!bMustExist && prule)
	{
		switch (pid->idtype)
		{
			case AT_AP_IDNone:				/* no ID */
				/* should never happen */
				Assert(false);
				break;
			case AT_AP_IDName:				/* IDentify by Name */
			case AT_AP_IDValue:				/* IDentifier FOR Value */
			case AT_AP_IDRank:				/* IDentifier FOR Rank */
			case AT_AP_ID_oid:				/* IDentifier by oid */
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("partition%s of %s already exists",
								partIdStr,
								relname),
						 errOmitLocation(true)));
				break;
			case AT_AP_IDDefault:			/* IDentify DEFAULT partition */
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("DEFAULT partition%s of %s already exists",
								prule->isName ?
								partIdStr : "",
								relname),
						 errOmitLocation(true)));
				break;
			default: /* XXX XXX */
				Assert(false);
				
		}
		
	}
	
L_fin_partrule:
	
	return prule;
} /* end get_part_rule1 */

PgPartRule *
get_part_rule(Relation rel, 
			  AlterPartitionId *pid,
			  bool bExistError, 
			  bool bMustExist,
			  MemoryContext mcxt, 
			  int *pSearch, 
			  bool inctemplate)
{
	PartitionNode 		*pNode 	= NULL;
	PartitionNode 		*pNode2 = NULL;
	char 				relnamBuf[(NAMEDATALEN * 2)];
	char *relname;
	
	snprintf(relnamBuf, sizeof(relnamBuf), "relation \"%s\"",
			 RelationGetRelationName(rel));
	
	relname = pstrdup(relnamBuf);
	
	pNode = RelationBuildPartitionDesc(rel, inctemplate);
	
	if (pid && ((pid->idtype != AT_AP_IDList)
				&& (pid->idtype != AT_AP_IDRule)))
		return get_part_rule1(rel,
							  pid,
							  bExistError, bMustExist,
							  mcxt, pSearch, pNode, relname, NULL);
	
	if (!pid)
		return NULL;
	
	if (pid->idtype == AT_AP_IDRule)
	{
		List 					*l1 	= (List *)pid->partiddef;
		ListCell 				*lc;
		AlterPartitionId 		*pid2 	= NULL;
		PgPartRule* 			 prule2 = NULL;
		StringInfoData      	 sid1, sid2;
		
		initStringInfo(&sid1);
		initStringInfo(&sid2);
		
		lc = list_head(l1);
		prule2 = (PgPartRule*) lfirst(lc);
		if (prule2 && prule2->topRule && prule2->topRule->children)
			pNode = prule2->topRule->children;
		
		truncateStringInfo(&sid1, 0);
		appendStringInfo(&sid1, "%s", prule2->relname);
		
		lc = lnext(lc);
		
		pid2 = (AlterPartitionId *)lfirst(lc);
		
		prule2 = get_part_rule1(rel,
								pid2,
								bExistError, bMustExist,
								mcxt, pSearch, pNode, sid1.data, &pNode2);
		
		pNode = pNode2;
		
		if (!pNode)
		{
			if (prule2 && prule2->topRule && prule2->topRule->children)
				pNode = prule2->topRule->children;
		}
		
		return prule2;
	}
	
	if (pid->idtype == AT_AP_IDList)
	{
		List 					*l1 	= (List *)pid->partiddef;
		ListCell 				*lc;
		AlterPartitionId 		*pid2 	= NULL;
		PgPartRule* 			 prule2 = NULL;
		StringInfoData      	 sid1, sid2;
		
		initStringInfo(&sid1);
		initStringInfo(&sid2);
		appendStringInfoString(&sid1, relnamBuf);
		
		foreach(lc, l1)
		{
			
			pid2 = (AlterPartitionId *)lfirst(lc);
			
			prule2 = get_part_rule1(rel,
									pid2,
									bExistError, bMustExist,
									mcxt, pSearch, pNode, sid1.data, &pNode2);
			
			pNode = pNode2;
			
			if (!pNode)
			{
				if (prule2 && prule2->topRule && prule2->topRule->children)
					pNode = prule2->topRule->children;
			}
			
			appendStringInfo(&sid2, "partition%s of %s",
							 prule2->partIdStr, sid1.data);
			truncateStringInfo(&sid1, 0);
			appendStringInfo(&sid1, "%s", sid2.data);
			truncateStringInfo(&sid2, 0);
		} /* end foreach */
		
		return prule2;
	}
	return NULL;
} /* end get_part_rule */

static void
fixup_table_storage_options(CreateStmt *ct)
{
	if (!ct->options)
	{
    	ct->options = list_make2(makeDefElem("appendonly",
											 (Node *)makeString("true")),
								 makeDefElem("orientation",
											 (Node *)makeString("column")));
	}
}

/*
 * The user is adding a partition and we have a subpartition template that has
 * been brought into the mix. At partition create time, if the user added any
 * storage encodings to the subpartition template, we would have cached them in
 * pg_partition_encoding. Retrieve them now, deparse and apply to the
 * PartitionSpec as if this was CREATE TABLE.
 */
static void
apply_template_storage_encodings(CreateStmt *ct, Oid relid, Oid paroid,
								 PartitionSpec *tmpl)
{
	List *encs = get_deparsed_partition_encodings(relid, paroid);

	if (encs)
	{
		/* 
		 * If the user didn't specify WITH (...) at create time,
		 * we need to force the new partitions to be AO/CO.
		 */
		fixup_table_storage_options(ct);
		tmpl->partElem = list_concat(tmpl->partElem,
									 encs);
	}
}

/*
 * atpxPart_validate_spec: kludge up a PartitionBy statement and use
 * validate_partition_spec to transform and validate a partition
 * boundary spec.
 */

static int
atpxPart_validate_spec(
					   PartitionBy 			*pBy,
					   CreateStmtContext   	*pcxt,
					   Relation 		 		 rel,
					   CreateStmt 				*ct,
					   PartitionElem 			*pelem,
					   PartitionNode 			*pNode,
					   Node 					*partName,
					   bool 			 		 isDefault,
					   PartitionByType  		 part_type,
					   char 					*partDesc)
{
	PartitionSpec 		*spec  	= makeNode(PartitionSpec);
	ParseState 			*pstate = NULL;
	List	   			*schema = NIL;
	List	   			*inheritOids;
	List	   			*old_constraints;
	int					 parentOidCount;
	int                  result;
	PartitionNode 		*pNode_tmpl = NULL;
	
	/* get the table column defs */
	schema =
	MergeAttributes(schema,
					list_make1(
							   makeRangeVar(
                                                                                        NULL /*catalogname*/, 
											get_namespace_name(
															   RelationGetNamespace(rel)),
											pstrdup(RelationGetRelationName(rel)), -1)),
					false, true /* isPartitioned */,
					&inheritOids, &old_constraints, &parentOidCount, NULL);
	
	pcxt->columns = schema;
	
	spec->partElem = list_make1(pelem);
	
	pelem->partName = partName ? copyObject(partName) : NULL;
	pelem->isDefault = isDefault;
	pelem->AddPartDesc = pstrdup(partDesc);
	
	/* generate a random name for the partition relation if necessary */
	if (partName)
		pelem->rrand = 0;
	else
		pelem->rrand = random();
	
 	pBy->partType = part_type;
	pBy->keys     = NULL;
	pBy->partNum  = 0;
	pBy->subPart  = NULL;
	pBy->partSpec = (Node *)spec;
	pBy->partDepth = pNode->part->parlevel;
	/* Note: pBy->partQuiet already set by caller */
	pBy->parentRel =
	makeRangeVar(NULL /*catalogname*/, get_namespace_name(RelationGetNamespace(rel)),
				 pstrdup(RelationGetRelationName(rel)), -1);
	pBy->location  = -1;
	pBy->partDefault = NULL;
	pBy->bKeepMe = true; /* nefarious: we need to keep the "top"
						  * partition by statement because
						  * analyze.c:do_parse_analyze needs to find
						  * it to re-order the ALTER statements
						  */
	
	/* fixup the pnode_tmpl to get the right parlevel */
	if (pNode && (pNode->rules || pNode->default_part))
	{
		PartitionRule *prule;
		
		if (pNode->default_part)
			prule = pNode->default_part;
		else
			prule = linitial(pNode->rules);
		
		pNode_tmpl = get_parts(
							   pNode->part->parrelid,
							   pNode->part->parlevel + 1,
							   InvalidOid, /* no parent for template */
							   true,
							   CurrentMemoryContext,
							   true /*includesubparts*/
							   );
	}
	
	{ /* find the partitioning keys (recursively) */
		
		PartitionBy 			*pBy2 = pBy;
		PartitionBy 			*parent_pBy2 = NULL;
		PartitionNode 			*pNode2 = pNode;
		
		int 			 ii;
		TupleDesc		 tupleDesc 		= RelationGetDescr(rel);
		List 			*pbykeys   		= NIL;
		List 			*pbyopclass 	= NIL;
		Oid 			 accessMethodId = BTREE_AM_OID;
		
		while (pNode2)
		{
			pbykeys    = NIL;
			pbyopclass = NIL;
			
			for (ii = 0; ii < pNode2->part->parnatts; ii++)
			{
				AttrNumber 		 	 attno 		   =
				pNode2->part->paratts[ii];
				Form_pg_attribute 	 attribute 	   =
				tupleDesc->attrs[attno - 1];
				char	   			*attributeName =
				NameStr(attribute->attname);
				Oid 				 opclass 	   =
				InvalidOid;
				
				opclass =
				GetDefaultOpClass(attribute->atttypid, accessMethodId);
				
				if (pbykeys)
				{
					pbykeys = lappend(pbykeys, makeString(attributeName));
					pbyopclass = lappend_oid(pbyopclass, opclass);
				}
				else
				{
					pbykeys = list_make1(makeString(attributeName));
					pbyopclass = list_make1_oid(opclass);
				}
			} /* end for */
			
			pBy2->keys = pbykeys;
			pBy2->keyopclass = pbyopclass;
			
			if (parent_pBy2)
				parent_pBy2->subPart = (Node *)pBy2;
			
			parent_pBy2 = pBy2;
			
			if (pNode2 && (pNode2->rules || pNode2->default_part))
			{
				PartitionRule *prule;
				PartitionElem *el = NULL; /* for the subpartn template */
				
				if (pNode2->default_part)
					prule = pNode2->default_part;
				else
					prule = linitial(pNode2->rules);
				
				if (prule && prule->children)
				{
					pNode2 = prule->children;
					
					/* XXX XXX: make this work for HASH at some point */
					
					Assert(
						   ('l' == pNode2->part->parkind) ||
						   ('r' == pNode2->part->parkind));
					
					pBy2 = makeNode(PartitionBy);
					pBy2->partType 	=
					('r' == pNode2->part->parkind) ?
					PARTTYP_RANGE :
					PARTTYP_LIST;
					pBy2->keys     	= NULL;
					pBy2->partNum  	= 0;
					pBy2->subPart  	= NULL;
					pBy2->partSpec 	= NULL;
					pBy2->partDepth = pNode2->part->parlevel;
					pBy2->partQuiet = pBy->partQuiet;
					pBy2->parentRel =
					makeRangeVar(
                                                                NULL /*catalogname*/,  
								get_namespace_name(
													RelationGetNamespace(rel)),
								 pstrdup(RelationGetRelationName(rel)), -1);
					pBy2->location  = -1;
					pBy2->partDefault = NULL;
					
					el = NULL;
					
					/* build the template (if it exists) */
					if (pNode_tmpl)
					{
						PartitionSpec *spec_tmpl = makeNode(PartitionSpec);
						ListCell *lc;
						
						spec_tmpl->istemplate = true;
						
						/* add entries for rules at current level */
						foreach(lc, pNode_tmpl->rules)
						{
							PartitionRule *rule_tmpl = lfirst(lc);
							
							el = makeNode(PartitionElem);
							
							if (rule_tmpl->parname &&
								strlen(rule_tmpl->parname))
								el->partName =
								(Node*)makeString(rule_tmpl->parname);
							
							el->isDefault = rule_tmpl->parisdefault;
							
							/* MPP-6904: use storage options from template */
							if (rule_tmpl->parreloptions ||
								rule_tmpl->partemplatespaceId)
							{
								Node *tspaceName = NULL;
								AlterPartitionCmd *apc = 
								makeNode(AlterPartitionCmd);
								
								el->storeAttr = (Node *)apc;
								
								if (rule_tmpl->partemplatespaceId)
									tspaceName = 
									(Node*)makeString(
													  get_tablespace_name(
																		  rule_tmpl->partemplatespaceId
																		  ));
								
								apc->partid = NULL;
								apc->arg2	= tspaceName;
								apc->arg1	= (Node *)rule_tmpl->parreloptions;
								
							}
							
							/* LIST */
							if (rule_tmpl->parlistvalues)
							{
								PartitionValuesSpec *vspec =
								makeNode(PartitionValuesSpec);
								
								el->boundSpec = (Node*)vspec;
								
								vspec->partValues = rule_tmpl->parlistvalues;
							}
							
							/* RANGE */
							if (rule_tmpl->parrangestart ||
								rule_tmpl->parrangeend)
							{
								PartitionBoundSpec *bspec =
								makeNode(PartitionBoundSpec);
								PartitionRangeItem *ri;
								
								if (rule_tmpl->parrangestart)
								{
									ri =
									makeNode(PartitionRangeItem);
									
									ri->partedge =
									rule_tmpl->parrangestartincl ?
									PART_EDGE_INCLUSIVE :
									PART_EDGE_EXCLUSIVE ;
									
									ri->partRangeVal =
									(List *)rule_tmpl->parrangestart;
									
									bspec->partStart = (Node*)ri;
									
								}
								if (rule_tmpl->parrangeend)
								{
									ri =
									makeNode(PartitionRangeItem);
									
									ri->partedge =
									rule_tmpl->parrangeendincl ?
									PART_EDGE_INCLUSIVE :
									PART_EDGE_EXCLUSIVE ;
									
									ri->partRangeVal =
									(List *)rule_tmpl->parrangeend;
									
									bspec->partEnd = (Node*)ri;
									
								}
								if (rule_tmpl->parrangeevery)
								{
									ri =
									makeNode(PartitionRangeItem);
									
									ri->partRangeVal =
									(List *)rule_tmpl->parrangeevery;
									
									bspec->partEvery = (Node*)ri;
									
								}
								
								el->boundSpec = (Node*)bspec;
								
							} /* end if RANGE */
							
							spec_tmpl->partElem = lappend(spec_tmpl->partElem,
														  el);
						} /* end foreach */
						
						/* MPP-4725 */
						/* and the default partition */
						if (pNode_tmpl->default_part)
						{
							PartitionRule *rule_tmpl =
							pNode_tmpl->default_part;
							
							el = makeNode(PartitionElem);
							
							if (rule_tmpl->parname &&
								strlen(rule_tmpl->parname))
								el->partName =
								(Node*)makeString(rule_tmpl->parname);
							
							el->isDefault = rule_tmpl->parisdefault;
							
							spec_tmpl->partElem = lappend(spec_tmpl->partElem,
														  el);
							
						}

						/* apply storage encoding for this template */
						apply_template_storage_encodings(ct,
												 RelationGetRelid(rel),
												 pNode_tmpl->part->partid,
												 spec_tmpl);
						
						/* the PartitionElem should hang off the pby
						 * partspec, and subsequent templates should
						 * hang off the subspec for the prior
						 * PartitionElem.
						 */
						
						pBy2->partSpec 	= (Node *)spec_tmpl;
						
					} /* end if pNode_tmpl */
					
					/* fixup the pnode_tmpl to get the right parlevel */
					if (pNode2 && (pNode2->rules || pNode2->default_part))
					{
						PartitionRule *prule66;
						
						if (pNode2->default_part)
							prule66 = pNode2->default_part;
						else
							prule66 = linitial(pNode2->rules);
						
						pNode_tmpl = get_parts(
											   pNode2->part->parrelid,
											   pNode2->part->parlevel + 1,
											   InvalidOid, /* no parent for template */
											   true,
											   CurrentMemoryContext,
											   true /*includesubparts*/
											   );
					}
					
				}
				else
					pNode2 = NULL;
			}
			else
				pNode2 = NULL;
			
		} /* end while */
	}
	
	pstate = make_parsestate(NULL);
	result = validate_partition_spec(pstate, pcxt, ct, pBy, "", -1);
	free_parsestate(&pstate);
	
	return result;
} /* end atpxPart_validate_spec */

Node *
atpxPartAddList(Relation rel,
				AlterPartitionCmd *pc,
				PartitionNode  *pNode,
				Node *pUtl,     /* pc2->arg2 */
				Node *partName, /* pid->partiddef (or NULL) */
				bool isDefault,
				PartitionElem *pelem,
				PartitionByType part_type,
				PgPartRule* par_prule,
				char *lrelname,
				bool bSetTemplate,
				Oid ownerid)
{
	CreateStmt 			*ct    	   = NULL;
	DestReceiver 		*dest  	   = None_Receiver;
	int 				 partno    = 0;
	int 				 maxpartno = 0;
	bool 				 bOpenGap  = false;
	PartitionBy 		*pBy   	   = makeNode(PartitionBy);
	CreateStmtContext    cxt;
	Node 				*pSubSpec  = NULL;	/* return the subpartition spec */
	Relation 			 par_rel   = rel;
	PartitionNode 		 pNodebuf;
	PartitionNode 		*pNode2 = &pNodebuf;
	AlterPartitionCmd   *pc2 = (AlterPartitionCmd *)pc->arg2;
	bool is_split = PointerIsValid(pc2->partid);
	
	/* get the relation for the parent of the new partition */
	if (par_prule && par_prule->topRule)
		par_rel =
		heap_open(par_prule->topRule->parchildrelid, AccessShareLock);
	
	MemSet(&cxt, 0, sizeof(cxt));
	
	Assert( (PARTTYP_LIST == part_type) || (PARTTYP_RANGE == part_type) );
	
	/* ct - the CreateStmt from pUtl ammended to show that it is for an
	 * added part, that it is owned by the argument ownerid, and that it
	 * is distributed like the parent rel.  Note that, at this time,
	 * the name is "fake_partition_name". */
	
	Assert(IsA(pUtl, List));
	
	ct = (CreateStmt *)linitial((List *)pUtl);
	Assert(IsA(ct, CreateStmt));
	
	ct->is_add_part = true; /* subroutines need to know this */
	ct->ownerid = ownerid;
	
	if (!ct->distributedBy)
		ct->distributedBy = make_dist_clause(rel);
	
	if (bSetTemplate)
	/* if creating a template, silence partition name messages */
		pBy->partQuiet = PART_VERBO_NOPARTNAME;
	else
	/* just silence distribution policy messages */
		pBy->partQuiet = PART_VERBO_NODISTRO;
	
	/* XXX XXX: handle case of missing boundary spec for range with EVERY */
	
	if (pelem && pelem->boundSpec)
	{
		if (PARTTYP_RANGE == part_type)
		{
			PartitionBoundSpec 	*pbs   	   = NULL;
			PgPartRule   		*prule 	   = NULL;
			AlterPartitionId 	 pid;
			ParseState 			*pstate    = make_parsestate(NULL);
			TupleDesc 		 	 tupledesc = RelationGetDescr(rel);
			
			MemSet(&pid, 0, sizeof(AlterPartitionId));
			
			pid.idtype = AT_AP_IDRank;
			pid.location  = -1;
			
			Assert (IsA(pelem->boundSpec, PartitionBoundSpec));
			
			pbs = (PartitionBoundSpec *)pelem->boundSpec;
			pSubSpec = pelem->subSpec; /* look for subpartition spec */
			
			/* no EVERY */
			if (pbs->partEvery)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("cannot specify EVERY when adding "
								"RANGE partition to %s",
								lrelname),
						 errOmitLocation(true)));
			
			if (!(pbs->partStart || pbs->partEnd ))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("Need START or END when adding "
								"RANGE partition to %s",
								lrelname),
						 errOmitLocation(true)));
			
			/* if no START, then START after last partition */
			
			if (!(pbs->partStart))
			{
				Datum 					*d_end 	   = NULL;
				bool 					*isnull;
				bool 					 bstat;
				
				pid.partiddef = (Node *)makeInteger(-1);
				
				prule = get_part_rule1(rel, &pid, false, false,
									   CurrentMemoryContext, NULL,
									   pNode,
									   lrelname,
									   &pNode2);
				
				/* ok if no prior -- just means this is first
				 * partition (XXX XXX though should always have 1
				 * partition in the table...)
				 */
				
				if (!(prule && prule->topRule))
				{
					maxpartno = 1;
					bOpenGap = true;
					goto L_fin_no_start;
				}
				
				{
					Node *n1;
					
					if ( !IsA(pbs->partEnd, PartitionRangeItem) )
					{
						/* pbs->partEnd isn't a PartitionRangeItem!  This probably means
						 * an invalid split of a default part, but we aren't really sure. 
						 * See MPP-14613.
						 */
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								 errmsg("invalid partition range specification."),
								 errOmitLocation(true)));
					}

					PartitionRangeItem *ri =
					(PartitionRangeItem *)pbs->partEnd;
					PartitionRangeItemIsValid(NULL, ri);
					
					n1 = (Node *)copyObject(ri->partRangeVal);
					n1 = (Node *)transformExpressionList(pstate,
														 (List *)n1);
					
					d_end =
					magic_expr_to_datum(rel, pNode,
										n1, &isnull);
				}
				
				if (prule && prule->topRule && prule->topRule->parrangeend
					&& list_length((List *)prule->topRule->parrangeend))
				{
					bstat =
					compare_partn_opfuncid(pNode,
										   "pg_catalog",
										   "<",
										   (List *)prule->topRule->parrangeend,
										   d_end, isnull, tupledesc);
					
					/* if the current end is less than the new end
					 * then use it as the start of the new
					 * partition
					 */
					
					if (bstat)
					{
						PartitionRangeItem *ri = makeNode(PartitionRangeItem);
						
						ri->location = -1;
						
						ri->partRangeVal =
						copyObject(prule->topRule->parrangeend);
						
						/* invert the inclusive/exclusive */
						ri->partedge = prule->topRule->parrangeendincl ?
						PART_EDGE_EXCLUSIVE :
						PART_EDGE_INCLUSIVE;
						
						/* should be final partition */
						maxpartno = prule->topRule->parruleord + 1;
						
						pbs->partStart = (Node *)ri;
						goto L_fin_no_start;
					}
				}
				
				/* if the last partition doesn't have an end, or the
				 * end isn't less than the new end, check if new end
				 * is less than current start
				 */
				
				pid.partiddef = (Node *)makeInteger(1);
				
				prule = get_part_rule1(rel, &pid, false, false,
									   CurrentMemoryContext, NULL,
									   pNode,
									   lrelname,
									   &pNode2);
				
				if (!(prule && prule->topRule && prule->topRule->parrangestart
					  && list_length((List *)prule->topRule->parrangestart)))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("new partition overlaps existing "
									"partition"),
							 errOmitLocation(true)));
				
				bstat =
				compare_partn_opfuncid(pNode,
									   "pg_catalog",
									   ">",
									   (List *)prule->topRule->parrangestart,
									   d_end, isnull, tupledesc);
				
				
				if (!bstat)
				{
					/* 
					 * MPP-13806 fix by Gavin Sherry
					 *
					 * As we support explicit inclusive and exclusive ranges
					 * we need to be even more careful.
					 *
					 * We can proceed if we have the following:
					 *
					 * END (R) EXCLUSIVE ; START (R) INCLUSIVE
					 * END (R) INCLUSIVE ; START (R) EXCLUSIVE
					 *
					 * XXX: this should be refactored into a single generic
					 * function that can be used here and in the unbounded end
					 * case, checked further down. That said, a lot of this code
					 * should be refactored.
					 */
					PartitionRangeItem *ri = (PartitionRangeItem *)pbs->partEnd;
					
					if ((ri->partedge == PART_EDGE_EXCLUSIVE &&
						 prule->topRule->parrangestartincl) ||
						(ri->partedge == PART_EDGE_INCLUSIVE &&
						 !prule->topRule->parrangestartincl))
					{
						bstat = compare_partn_opfuncid(pNode, "pg_catalog", "=",
													   (List *)prule->topRule->parrangestart,
													   d_end, isnull, tupledesc);
					}
					
				}
				
				if (bstat)
				{
					/* should be first partition */
					maxpartno = prule->topRule->parruleord - 1;
					if (0 == maxpartno)
					{
						maxpartno = 1;
						bOpenGap = true;
					}
					
				}
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("new partition overlaps existing "
									"partition"),
							 errOmitLocation(true)));
				
			L_fin_no_start:
				bstat = false; /* fix warning */
				
			}
			else if (!(pbs->partEnd))
			{ /* if no END, then END before first partition
			   **ONLY IF**
			   * START of this partition is before first partition ... */
				
				Datum 					*d_start   = NULL;
				bool 					*isnull;
				bool 					 bstat;
				
				pid.partiddef = (Node *)makeInteger(1);
				
				prule = get_part_rule1(rel, &pid, false, false,
									   CurrentMemoryContext, NULL,
									   pNode,
									   lrelname,
									   &pNode2);
				
				/* NOTE: invert all the logic of case of missing partStart */
				
				/* ok if no successor [?] -- just means this is first
				 * partition (XXX XXX though should always have 1
				 * partition in the table... [XXX XXX unless did a
				 * SPLIT of a single partition !! ])
				 */
				
				if (!(prule && prule->topRule))
				{
					maxpartno = 1;
					bOpenGap = true;
					goto L_fin_no_end;
				}
				
				{
					Node *n1;
					PartitionRangeItem *ri =
					(PartitionRangeItem *)pbs->partStart;
					
					PartitionRangeItemIsValid(NULL, ri);
					n1 = (Node *)copyObject(ri->partRangeVal);
					n1 = (Node *)transformExpressionList(pstate,
														 (List *)n1);
					
					d_start =
					magic_expr_to_datum(rel, pNode,
										n1, &isnull);
				}
				
				
				if (prule && prule->topRule && prule->topRule->parrangestart
					&& list_length((List *)prule->topRule->parrangestart))
				{
					bstat =
					compare_partn_opfuncid(pNode,
										   "pg_catalog",
										   ">",
										   (List *)prule->topRule->parrangestart,
										   d_start, isnull, tupledesc);
					
					/* if the current start is greater than the new start
					 * then use the current start as the end of the new
					 * partition
					 */
					
					if (bstat)
					{
						PartitionRangeItem *ri = makeNode(PartitionRangeItem);
						
						ri->location = -1;
						
						ri->partRangeVal =
						copyObject(prule->topRule->parrangestart);
						
						/* invert the inclusive/exclusive */
						ri->partedge = prule->topRule->parrangestartincl ?
						PART_EDGE_EXCLUSIVE :
						PART_EDGE_INCLUSIVE;
						
						/* should be first partition */
						maxpartno = prule->topRule->parruleord - 1;
						if (0 == maxpartno)
						{
							maxpartno = 1;
							bOpenGap = true;
						}
						
						pbs->partEnd = (Node *)ri;
						goto L_fin_no_end;
					}
				}
				
				/* if the first partition doesn't have an start, or the
				 * start isn't greater than the new start, check if new start
				 * is greater than current end
				 */
				
				pid.partiddef = (Node *)makeInteger(-1);
				
				prule = get_part_rule1(rel, &pid, false, false,
									   CurrentMemoryContext, NULL,
									   pNode,
									   lrelname,
									   &pNode2);
				
				if (!(prule && prule->topRule && prule->topRule->parrangeend
					  && list_length((List *)prule->topRule->parrangeend)))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("new partition overlaps existing "
									"partition"),
							 errOmitLocation(true)));
				
				bstat =
				compare_partn_opfuncid(pNode,
									   "pg_catalog",
									   "<",
									   (List *)prule->topRule->parrangeend,
									   d_start, isnull, tupledesc);
				if (bstat)
				{
					/* should be final partition */
					maxpartno = prule->topRule->parruleord + 1;
				}
				else
				{
					PartitionRangeItem *ri =
					(PartitionRangeItem *)pbs->partStart;
					
					/* check for equality */
					bstat =
					compare_partn_opfuncid(pNode,
										   "pg_catalog",
										   "=",
										   (List *)prule->topRule->parrangeend,
										   d_start, isnull, tupledesc);
					
					/* if new start not >= to current end, then 
					 * new start < current end, so it overlaps. Or if
					 * new start == current end, but the
					 * inclusivity is not opposite for the boundaries
					 * (eg inclusive end abuts inclusive start for
					 * same start/end value) then it overlaps
					 */
					if (!bstat || 
						(bstat && 
						 (prule->topRule->parrangeendincl == 
						  (ri->partedge == PART_EDGE_INCLUSIVE)))
						)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								 errmsg("new partition overlaps existing "
										"partition"),
								 errOmitLocation(true)));
					
					/* doesn't overlap, should be final partition */
					maxpartno = prule->topRule->parruleord + 1;
					
				}
			L_fin_no_end:
				bstat = false; /* fix warning */
				
			}
			else
			{ 					/* both start and end are specified */
				PartitionRangeItem 		*ri;
				bool 					 bOverlap  = false;
				int isMiddle  = 0;	/* -1 for new first, 1 for new last */
				bool 					*isnull;
				int 			 		 startSearchpoint;
				int 			 		 endSearchpoint;
				Datum 					*d_start   = NULL;
				Datum 					*d_end 	   = NULL;
				
				/* see if start or end overlaps */
				pid.idtype = AT_AP_IDValue;
				
				/* check the start */
				
				ri = (PartitionRangeItem *)pbs->partStart;
				PartitionRangeItemIsValid(NULL, ri);
				
				pid.partiddef = (Node *)copyObject(ri->partRangeVal);
				pid.partiddef =
				(Node *)transformExpressionList(pstate,
												(List *)pid.partiddef);
				
				prule = get_part_rule1(rel, &pid, false, false,
									   CurrentMemoryContext, &startSearchpoint,
									   pNode,
									   lrelname,
									   &pNode2);
				
				/* found match for start value in rules */
				if (prule && !(prule->topRule->parisdefault && is_split))
				{
					bool bstat;
					PartitionRule *a_rule = prule->topRule;
					d_start =
					magic_expr_to_datum(rel, pNode,
										pid.partiddef, &isnull);
					
					/* if start value was inclusive then it definitely
					 * overlaps
					 */
					if (ri->partedge == PART_EDGE_INCLUSIVE)
					{
						bOverlap = true;
						goto L_end_overlap;
					}
					
					/* not inclusive -- check harder if START really
					 * overlaps
					 */
					
					if (0 ==
						list_length((List *)a_rule->parrangeend))
					{
						/* infinite end > new start - overlap */
						bOverlap = true;
						goto L_end_overlap;
					}
					
					bstat =
					compare_partn_opfuncid(pNode,
										   "pg_catalog",
										   ">",
										   (List *)a_rule->parrangeend,
										   d_start, isnull, tupledesc);
					if (bstat)
					{
						/* end > new start - overlap */
						bOverlap = true;
						goto L_end_overlap;
					}
					
					/* Must be the case that new start == end of
					 * a_rule (because if the end < new start then how
					 * could we find it in the interval for prule ?)
					 * This is ok if they have opposite
					 * INCLUSIVE/EXCLUSIVE ->  New partition does not
					 * overlap.
					 */
					
					Assert (compare_partn_opfuncid(pNode,
												   "pg_catalog",
												   "=",
												   (List *)a_rule->parrangeend,
												   d_start, isnull, tupledesc));
					
					if (a_rule->parrangeendincl == 
						(ri->partedge == PART_EDGE_INCLUSIVE))
					{
						/* start and end must be of opposite
						 * types, else they overlap
						 */
						bOverlap = true;
						goto L_end_overlap;
					}
					
					/* opposite inclusive/exclusive, so in middle of
					 * range of existing partitions
					 */
					isMiddle = 0;
					goto L_check_end;
				} /* end if prule */
				
				/* check for basic case of START > last partition */
				if (pNode && pNode->rules && list_length(pNode->rules))
				{
					bool bstat;
					PartitionRule *a_rule = /* get last rule */
					(PartitionRule *)list_nth(pNode->rules,
											  list_length(pNode->rules) - 1);
					d_start =
					magic_expr_to_datum(rel, pNode,
										pid.partiddef, &isnull);
					
					if (0 ==
						list_length((List *)a_rule->parrangeend))
					{
						/* infinite end > new start */
						bstat = false;
					}
					else
						bstat =
						compare_partn_opfuncid(pNode,
											   "pg_catalog",
											   "<",
											   (List *)a_rule->parrangeend,
											   d_start, isnull, tupledesc);
					
					/* if the new partition start > end of the last
					 * partition then it is the new final partition.
					 * Don't bother checking the new end for overlap
					 * (just check if end > start in validation
					 * phase
					 */
					if (bstat)
					{
						isMiddle = 1;
						
						/* should be final partition */
						maxpartno = a_rule->parruleord + 1;
						
						goto L_end_overlap;
					}
					
					/* could be the case that new start == end of
					 * last.  This is ok if they have opposite
					 * INCLUSIVE/EXCLUSIVE.  New partition is still
					 * final partition for this case
					 */
					
					if (0 ==
						list_length((List *)a_rule->parrangeend))
					{
						/* infinite end > new start */
						bstat = false;
					}
					else
						bstat =
						compare_partn_opfuncid(pNode,
											   "pg_catalog",
											   "=",
											   (List *)a_rule->parrangeend,
											   d_start, isnull, tupledesc);
					if (bstat)
					{
						if (a_rule->parrangeendincl == 
							(ri->partedge == PART_EDGE_INCLUSIVE))
						{
							/* start and end must be of opposite
							 * types, else they overlap */
							bOverlap = true;
							goto L_end_overlap;
						}
						
						isMiddle = 1;
						
						/* should be final partition */
						maxpartno = a_rule->parruleord + 1;
						
						goto L_end_overlap;
					}
					else
					{
						/* tricky case: the new start is less than the
						 * end of the final partition, but it does not
						 * intersect any existing partitions.  So we
						 * are trying to add a partition in the middle
						 * of the existing partitions or before the
						 * first partition.
						 */
						a_rule = /* get first rule */
						(PartitionRule *)list_nth(pNode->rules, 0);
						
						if (0 ==
							list_length((List *)a_rule->parrangestart))
						{
							/* new start > negative infinite start */
							bstat = false;
						}
						else
							bstat =
							compare_partn_opfuncid(pNode,
												   "pg_catalog",
												   ">",
												   (List *)a_rule->parrangestart,
												   d_start, isnull, tupledesc);
						
						/* if the new partition start < start of the
						 * first partition then it is the new first
						 * partition.  Check the new end for overlap.
						 *
						 * NOTE: ignore the case where
						 * new start == 1st start and
						 * inclusive vs exclusive because that is just
						 * stupid.
						 *
						 */
						if (bstat)
						{
							isMiddle = -1;
							
							/* should be first partition */
							maxpartno = a_rule->parruleord - 1;
							if (0 == maxpartno)
							{
								maxpartno = 1;
								bOpenGap = true;
							}
							
						}
						else
						{
							isMiddle = 0;
						}
					}
				}
				else
				{
					/* if no "rules", then this is the first partition */
					
					isMiddle = 1;
					
					/* should be final partition */
					maxpartno = 1;
					
					goto L_end_overlap;
				}
				
			L_check_end:
				/* check the end */
				/* check for basic case of END < first partition (the
				 opposite of START > last partition) */
				
				ri = (PartitionRangeItem *)pbs->partEnd;
				PartitionRangeItemIsValid(NULL, ri);
				
				pid.partiddef = (Node *)copyObject(ri->partRangeVal);
				pid.partiddef =
				(Node *)transformExpressionList(pstate,
												(List *)pid.partiddef);
				
				prule = get_part_rule1(rel, &pid, false, false,
									   CurrentMemoryContext, &endSearchpoint,
									   pNode,
									   lrelname,
									   &pNode2);
				
				/* found match for end value in rules */
				if (prule && !(prule->topRule->parisdefault &&
							   is_split))
				{
					bool bstat;
					PartitionRule *a_rule = prule->topRule;
					d_end =
					magic_expr_to_datum(rel, pNode,
										pid.partiddef, &isnull);
					
					/* if end value was inclusive then it definitely
					 * overlaps
					 */
					if (ri->partedge == PART_EDGE_INCLUSIVE)
					{
						bOverlap = true;
						goto L_end_overlap;
					}
					
					/* not inclusive -- check harder if END really
					 * overlaps
					 */
					if (0 ==
						list_length((List *)a_rule->parrangestart))
					{
						/* -infinite start < new end - overlap */
						bOverlap = true;
						goto L_end_overlap;
					}
					
					bstat =
					compare_partn_opfuncid(pNode,
										   "pg_catalog",
										   "<",
										   (List *)a_rule->parrangestart,
										   d_end, isnull, tupledesc);
					if (bstat)
					{
						/* start < new end - overlap */
						bOverlap = true;
						goto L_end_overlap;
					}
					
					/* Must be the case that new end = start of
					 * a_rule (because if the start > new end then how
					 * could we find it in the interval for prule ?)
					 * This is ok if they have opposite
					 * INCLUSIVE/EXCLUSIVE ->  New partition does not
					 * overlap.
					 */
					
					Assert (compare_partn_opfuncid(pNode,
												   "pg_catalog",
												   "=",
												   (List *)a_rule->parrangestart,
												   d_end, isnull, tupledesc));
					
					if (a_rule->parrangestartincl == 
						(ri->partedge == PART_EDGE_INCLUSIVE))
					{
						/* start and end must be of opposite
						 * types, else they overlap
						 */
						bOverlap = true;
						goto L_end_overlap;
					}
					
					/* opposite inclusive/exclusive, so in middle of
					 * range of existing partitions
					 */
					isMiddle = 0;
				} /* end if prule */
				
				
				/* check for case of END < first partition */
				if (pNode && pNode->rules && list_length(pNode->rules))
				{
					bool bstat;
					PartitionRule *a_rule = /* get first rule */
					(PartitionRule *)list_nth(pNode->rules, 0);
					d_end =
					magic_expr_to_datum(rel, pNode,
										pid.partiddef, &isnull);
					
					if (0 ==
						list_length((List *)a_rule->parrangestart))
					{
						/* new end > negative infinite start */
						bstat = false;
					}
					else
						bstat =
						compare_partn_opfuncid(pNode,
											   "pg_catalog",
											   ">",
											   (List *)a_rule->parrangestart,
											   d_end, isnull, tupledesc);
					
					/* if the new partition end < start of the first
					 * partition then it is the new first partition.
					 */
					if (bstat)
					{
						/* check if start was also ok for first partition */
						switch (isMiddle)
						{
							case -1:
								/* since new start < first start and
								 * new end < first start should be
								 * first.
								 */
								
								/* should be first partition */
								maxpartno = a_rule->parruleord - 1;
								if (0 == maxpartno)
								{
									maxpartno = 1;
									bOpenGap = true;
								}
								
								break;
							case 0:
							case 1:
							default:
								/* new end is less than first
								 * partition start but new start isn't
								 * -- must be end < start
								 */
								break;
						}
						goto L_end_overlap;
					}
					
					/* could be the case that new end == start of
					 * first.  This is ok if they have opposite
					 * INCLUSIVE/EXCLUSIVE.  New partition is still
					 * first partition for this case
					 */
					
					if (0 ==
						list_length((List *)a_rule->parrangestart))
					{
						/* new end > negative infinite start */
						bstat = false;
					}
					else
						bstat =
						compare_partn_opfuncid(pNode,
											   "pg_catalog",
											   "=",
											   (List *)a_rule->parrangestart,
											   d_end, isnull, tupledesc);
					if (bstat)
					{
						if (a_rule->parrangestartincl == 
							(ri->partedge == PART_EDGE_INCLUSIVE))
						{
							/* start and end must be of opposite
							 * types, else they overlap */
							bOverlap = true;
							goto L_end_overlap;
						}
						/* check if start was also ok for first partition */
						switch (isMiddle)
						{
							case -1:
								/* since new start < first start and
								 * new end < first start should be
								 * first.
								 */
								
								/* should be first partition */
								maxpartno = a_rule->parruleord - 1;
								if (0 == maxpartno)
								{
									maxpartno = 1;
									bOpenGap = true;
								}
								
								break;
							case 0:
							case 1:
							default:
								/* new end is less than first
								 * partition start but new start isn't
								 * -- must be end < start
								 */
								break;
						}
						goto L_end_overlap;
					}
					else
					{
						/* tricky case: the new end is greater than the
						 * start of the first partition, but it does not
						 * intersect any existing partitions.  So we
						 * are trying to add a partition in the middle
						 * of the existing partitions or after the
						 * last partition.
						 */
						a_rule = /* get last rule */
						(PartitionRule *)list_nth(pNode->rules,
												  list_length(pNode->rules) - 1);
						if (0 ==
							list_length((List *)a_rule->parrangeend))
						{
							/* new end < infinite end */
							bstat = false;
						}
						else
							
							bstat =
							compare_partn_opfuncid(pNode,
												   "pg_catalog",
												   "<",
												   (List *)a_rule->parrangeend,
												   d_end, isnull, tupledesc);
						
						/* if the new partition end > end of the
						 * last partition then it is the new last
						 * partition (maybe)
						 *
						 * NOTE: ignore the case where
						 * new end == last end and
						 * inclusive vs exclusive because that is just
						 * stupid.
						 *
						 */
						if (bstat)
						{
							switch (isMiddle)
							{
								case 1:
									/* since new start > last end and
									 * new end > last end should be
									 * last.
									 */
									
									/* should be last partition */
									maxpartno = a_rule->parruleord + 1;
									
									break;
								case -1:
									/* since new start < first start
									 * and new end > last end we would
									 * overlap all partitions!!!
									 */
								case 0:
									/* since new start < last end
									 * and new end > last end we would
									 * overlap last partition
									 */
									bOverlap = true;
									goto L_end_overlap;
									break;
								default:
									/* new end is less than last
									 * partition end but new start isn't
									 * -- must be end < start
									 */
									break;
							}
						}
						else
						{
							switch (isMiddle)
							{
								case -1:
									/* since new start < first start
									 * and new end in middle we overlap
									 */
									bOverlap = true;
									goto L_end_overlap;
									
									break;
								case 0:
									/* both start and end in middle */
									break;
								case 1:
								default:
									/* since new start > last end and
									 * new end in middle
									 * -- must be end < start
									 */
									break;
							}
						}
					}
				}
				else
				{
					/* if no "rules", then this is the first partition */
					
					isMiddle = 1;
					
					/* should be final partition */
					maxpartno = 1;
					
					goto L_end_overlap;
				}
				
				
				/* if the individual start and end values don't
				 * intersect an existing partition, make sure they
				 * don't define a range which contains an existing
				 * partition, ie new start < existing start and new
				 * end > existing end
				 */
				
				if (!bOverlap && (0 == isMiddle))
				{
					bOpenGap = true;
					
					/*
					 hmm, not always true.  see MPP-3667, MPP-3636, MPP-3593
					 
					 if (startSearchpoint != endSearchpoint)
					 {
					 bOverlap = true;
					 goto L_end_overlap;
					 }
					 
					 */
					while (1)
					{
						bool bstat;
						PartitionRule *a_rule = /* get the rule */
						(PartitionRule *)list_nth(pNode->rules,
												  startSearchpoint);
						
						/* MPP-3621: fix ADD for open intervals */
						
						if (0 ==
							list_length((List *)a_rule->parrangeend))
						{
							/* new end < infinite end */
							bstat = false;
						}
						else
							bstat =
							compare_partn_opfuncid(pNode,
												   "pg_catalog",
												   "<=",
												   (List *)a_rule->parrangeend,
												   d_start, isnull, tupledesc);
						
						if (bstat)
						{
							startSearchpoint++;
							Assert(startSearchpoint
								   <= list_length(pNode->rules));
							continue;
						}
						
						/* if previous partition was less than
						 current, then this one should be larger.
						 if not, then it overlaps...
						 */
						if (
							(0 ==
							 list_length((List *)a_rule->parrangestart))
							||
							!compare_partn_opfuncid(pNode,
													"pg_catalog",
													">=",
													(List *)a_rule->parrangestart,
													d_end, isnull, tupledesc))
						{
							prule = NULL; /* could get the right prule... */
							bOverlap = true;
							goto L_end_overlap;
						}
						
						/* shift a_rule up so new rule has a place to fit */
						maxpartno = a_rule->parruleord;
						
						break;
					} /* end while */
					
				} /* end 0 == middle */
				
			L_end_overlap:
				if (bOverlap)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("new partition overlaps existing "
									"partition%s",
									(prule && prule->isName) ?
									prule->partIdStr : ""),
							 errOmitLocation(true)));
				
			}
			
			free_parsestate(&pstate);
		} /* end if parttype_range */
	} /* end if pelem && pelem->boundspec */
	
	/* this function does transformExpr on the boundary specs */
	partno = atpxPart_validate_spec(pBy,
									&cxt,
									rel,
									ct,
									pelem,
									pNode,
									partName,
									isDefault,
									part_type,
									"");
	
	if (pelem && pelem->boundSpec)
	{
		if (PARTTYP_LIST == part_type)
		{
			ListCell 			*lc;
			PartitionValuesSpec *spec;
			AlterPartitionId 	 pid;
			
			Assert (IsA(pelem->boundSpec, PartitionValuesSpec));
			
			MemSet(&pid, 0, sizeof(AlterPartitionId));
			
			spec = (PartitionValuesSpec *)pelem->boundSpec;
			
			/* only check this if we aren't doing split */
			if (1)
			{
				foreach(lc, spec->partValues)
				{
					List			*vals  = lfirst(lc);
					PgPartRule   	*prule = NULL;
					
					pid.idtype = AT_AP_IDValue;
					pid.partiddef = (Node *)vals;
					pid.location  = -1;
					
					prule = get_part_rule1(rel, &pid, false, false,
										   CurrentMemoryContext, NULL,
										   pNode,
										   lrelname,
										   &pNode2);
					
					if (prule && !(prule->topRule->parisdefault && is_split))
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
								 errmsg("new partition definition overlaps "
										"existing partition%s of %s",
										prule->partIdStr,
										lrelname),
								 errOmitLocation(true)));
					
				} /* end foreach */
			}
			
			/* give a new maxpartno for the list partition */
			if (pNode && pNode->rules && list_length(pNode->rules))
			{
				ListCell *lc;
				
				foreach(lc, pNode->rules)
				{
					PartitionRule *rule = lfirst(lc);
					
					/* find the highest parruleord */
					if (rule->parruleord > maxpartno)
						maxpartno = rule->parruleord;
				}
				maxpartno++;
			}
			
		}
		
	}
	
	/* create the partition - change the table name from "fake_partition_name" to 
	 * name of the parent relation
	 */
	
	ct->relation = makeRangeVar(
								NULL /*catalogname*/, 
								get_namespace_name(RelationGetNamespace(par_rel)),
								RelationGetRelationName(par_rel), -1);
	
	if (bOpenGap) /* might need a gap to insert the new partition */
	{
		AlterPartitionId 	 	 pid;
		PgPartRule   			*prule = NULL;
		
		pid.idtype = AT_AP_IDRank;
		pid.partiddef = (Node *)makeInteger(-1);
		pid.location  = -1;
		
		/* get the last rule, and increment each parruleord until
		 * reach parruleord == maxpartno
		 */
		prule = get_part_rule1(rel, &pid, false, false,
							   CurrentMemoryContext, NULL,
							   pNode,
							   lrelname,
							   &pNode2);
		
		/* NOTE: this assert is valid, except for the case of splitting
		 * the very last partition of a table.  For that case, we must
		 * drop the last partition before re-adding the new pieces, which
		 * violates this invariant
		 */
		/*	Assert(prule && prule->topRule && prule->pNode && prule->pNode->part); */
		
		if (prule && prule->topRule && prule->pNode && prule->pNode->part)
			parruleord_open_gap(
								prule->pNode->part->partid,
								prule->pNode->part->parlevel,
								prule->topRule->parparentoid,
								prule->topRule->parruleord,
								maxpartno,
								CurrentMemoryContext);
	}
	
	{
		List			*l1;
		ListCell		*lc;
		int 			 ii					 = 0;
		bool			 bFixFirstATS		 = true;
		bool			 bFirst_TemplateOnly = true;   /* ignore dummy entry */
		int 			 pby_templ_depth	 = 0;	   /* template partdepth */
		Oid 			 skipTableRelid	 	 = InvalidOid; 
		List			*attr_encodings		 = NIL;
		
		ct->partitionBy = (Node *)pBy;

		/* this parse_analyze expands the phony create of a partitioned table
		 * that we just build into the constituent commands we need to create
		 * the new part.  (This will include some commands for the parent that
		 * we don't need, since the parent already exists.)
		 */
		
		l1 = parse_analyze((Node *)ct, NULL, NULL, 0);
		
		/* 
		 * Must reference ct->attr_encodings after parse_analyze() since that
		 * stage by change the attribute encodings via expansion.
		 */
		attr_encodings = ct->attr_encodings;

		/*
		 * Look for the first CreateStmt and generate a GrantStmt
		 * based on the RangeVar in it.
		 */
		foreach(lc, l1)
		{
			Node *s = lfirst(lc);
			
			/* skip the first one, it's the fake create table for the parent */
			if (lc == list_head(l1))
				continue;
			
			if (IsA(s, Query) && IsA(((Query *)s)->utilityStmt, CreateStmt))
			{
				HeapTuple tuple;
				Datum aclDatum;
				bool isNull;
				CreateStmt *t = (CreateStmt *)((Query *)s)->utilityStmt;
				cqContext	*classcqCtx;
				
				t->attr_encodings = copyObject(attr_encodings);

				classcqCtx = caql_beginscan(
						NULL,
						cql("SELECT * FROM pg_class "
							" WHERE oid = :1 ",
							ObjectIdGetDatum(RelationGetRelid(rel))));

				tuple = caql_getnext(classcqCtx);

				if (!HeapTupleIsValid(tuple))
					elog(ERROR, "cache lookup failed for relation %u",
						 RelationGetRelid(rel));
				
				aclDatum = caql_getattr(classcqCtx,
										Anum_pg_class_relacl,
										&isNull);
				if (!isNull)
				{
					List *cp = NIL;
					int i, num;
					Acl *acl;
					AclItem *aidat;
					
					acl = DatumGetAclP(aclDatum);
					
					num = ACL_NUM(acl);
					aidat = ACL_DAT(acl);
					
					for (i = 0; i < num; i++)
					{
						AclItem	*aidata = &aidat[i];
						Datum d;
						char *str;
						
						d = DirectFunctionCall1(aclitemout,
												PointerGetDatum(aidata));
						str = DatumGetCString(d);
						
						cp = lappend(cp, makeString(str));
						
					}
					
					if (list_length(cp))
					{
						GrantStmt *gs = makeNode(GrantStmt);
						List *pt;
						
						gs->is_grant = true;
						gs->objtype = ACL_OBJECT_RELATION;
						gs->cooked_privs = cp;
						
						gs->objects = list_make1(copyObject(t->relation));
						
						pt = parse_analyze((Node *)gs, NULL, NULL, 0);
						l1 = list_concat(l1, pt);
					}
				}

				caql_endscan(classcqCtx);
				break;
			}
		}
		
		/* skip the first cell because the table already exists --
		 * don't recreate it
		 */
		lc = list_head(l1);
		
		if (lc)
		{
			Node *s = lfirst(lc);
			
			/* MPP-10421: but save the relid of the skipped table,
			 * because we skip indexes associated with it... 
			 */
			if (IsA(s, Query) && IsA(((Query *)s)->utilityStmt, CreateStmt))
			{
				CreateStmt *t = (CreateStmt *)((Query *)s)->utilityStmt;
				
				skipTableRelid = RangeVarGetRelid(t->relation, true, false /*allowHcatalog*/);
			}
		}
		
		
		for_each_cell(lc, lnext(lc))
		{
			Query *q = lfirst(lc);
			
			/*
			 * MPP-6379, MPP-10421: If the statement is an expanded
			 * index creation statement on the parent (or the "skipped
			 * table"), ignore it. We get into this situation when the
			 * parent has one or more indexes on it that our new
			 * partition is inheriting.
			 */
			if (IsA(q->utilityStmt, IndexStmt))
			{
				IndexStmt *istmt	 = (IndexStmt *)q->utilityStmt;
				Oid		   idxRelid	 = RangeVarGetRelid(istmt->relation, true, false /*allowHcatalog*/);
				
				if (idxRelid == RelationGetRelid(rel))
					continue;
				
				if (OidIsValid(skipTableRelid) &&
					(idxRelid == skipTableRelid))
					continue;
			}
			
			/* XXX XXX: fix the first Alter Table Statement to have
			 * the correct maxpartno.  Whoohoo!!
			 */
			if (bFixFirstATS && q && q->utilityStmt
				&& IsA(q->utilityStmt, AlterTableStmt))
			{
				PartitionSpec 	*spec = NULL;
				AlterTableStmt 	*ats;
				AlterTableCmd  	*atc;
				List 			*cmds;
				
				bFixFirstATS = false;
				
				ats = (AlterTableStmt *)q->utilityStmt;
				Assert(IsA(ats, AlterTableStmt));
				
				cmds = ats->cmds;
				
				Assert(cmds && (list_length(cmds) > 1));
				
				atc = (AlterTableCmd *)lsecond(cmds);
				
				Assert(atc->def);
				
				pBy = (PartitionBy *)atc->def;
				
				Assert(IsA(pBy, PartitionBy));
				
				spec = (PartitionSpec *)pBy->partSpec;
				
				if (spec)
				{
					List 				*l2 = spec->partElem;
					PartitionElem 		*pel;
					
					if (l2 && list_length(l2))
					{
						pel = (PartitionElem *)linitial(l2);
						
						pel->partno = maxpartno;
					}
					
				}
				
			} /* end first alter table fixup */
			else if (IsA(q->utilityStmt, CreateStmt))
			{
				/* propagate owner */
				((CreateStmt *)q->utilityStmt)->ownerid = ownerid;
			}
			
			/* normal case - add partitions using CREATE statements
			 * that get dispatched to the segments
			 */
			if (!bSetTemplate)
				ProcessUtility(q->utilityStmt, 
							   synthetic_sql,
							   NULL, 
							   false, /* not top level */
							   dest, 
							   NULL);
			else
			{ /* setting subpartition template only */
				
				/* find all the alter table statements that contain
				 * partaddinternal, and extract the definitions.  Only
				 * build the catalog entries for subpartition
				 * templates, not "real" table entries.
				 */
				if (q && q->utilityStmt
					&& IsA(q->utilityStmt, AlterTableStmt))
				{
					AlterTableStmt *at2 = (AlterTableStmt *)q->utilityStmt;
					List *l2 = at2->cmds;
					ListCell *lc2;
					
					foreach(lc2, l2)
					{
						AlterTableCmd *ac2 = (AlterTableCmd *) lfirst(lc2);
						
						if (ac2->subtype == AT_PartAddInternal)
						{
							PartitionBy *templ_pby =
							(PartitionBy *)ac2->def;
							
							Assert(IsA(templ_pby, PartitionBy));
							
							/* skip the first one because it's the
							 * fake parent partition definition for
							 * the subpartition template entries
							 */
							
							if (bFirst_TemplateOnly)
							{
								bFirst_TemplateOnly = false;
								
								/* MPP-5992: only set one level of
								 * templates -- we might have
								 * templates for subpartitions of the
								 * subpartitions, which would add
								 * duplicate templates into the table.
								 * Only add templates of the specified
								 * depth and skip deeper template
								 * definitions.
								 */
								pby_templ_depth = templ_pby->partDepth + 1;
								
							}
							else
							{
								if (templ_pby->partDepth == pby_templ_depth)
									add_part_to_catalog(
														RelationGetRelid(rel),
														(PartitionBy *)ac2->def,
														true);
							}
							
						}
					} /* end foreach lc2 l2 */
				}
			} /* end else setting subpartition templates only */
			
			ii++;
		} /* end for each cell */
		
	}
	
	if (par_prule && par_prule->topRule)
		heap_close(par_rel, NoLock);
	
	return pSubSpec;
} /* end atpxPartAddList */


List *
atpxDropList(Relation rel, PartitionNode *pNode)
{
	List *l1 = NIL;
	ListCell *lc;
	
	if (!pNode)
		return l1;
	
	/* add the child lists first */
	foreach(lc, pNode->rules)
	{
		PartitionRule *rule = lfirst(lc);
		List *l2 = NIL;
		
		if (rule->children)
			l2 = atpxDropList(rel, rule->children);
		else
			l2 = NIL;
		
		if (l2)
		{
			if (l1)
				l1 = list_concat(l1, l2);
			else
				l1 = l2;
		}
	}
	
	/* and the default partition */
	if (pNode->default_part)
	{
		PartitionRule *rule = pNode->default_part;
		List *l2 = NIL;
		
		if (rule->children)
			l2 = atpxDropList(rel, rule->children);
		else
			l2 = NIL;
		
		if (l2)
		{
			if (l1)
				l1 = list_concat(l1, l2);
			else
				l1 = l2;
		}
	}
	
	/* add entries for rules at current level */
	foreach(lc, pNode->rules)
	{
		PartitionRule *rule = lfirst(lc);
		char	*prelname;
		char 	*nspname;
		Relation	rel;
		
		rel = heap_open(rule->parchildrelid, AccessShareLock);
		prelname = pstrdup(RelationGetRelationName(rel));
		nspname = pstrdup(get_namespace_name(RelationGetNamespace(rel)));
		heap_close(rel, NoLock);
		
		if (l1)
			l1 = lappend(l1, list_make2(makeString(nspname),
										makeString(prelname)));
		else
			l1 = list_make1(list_make2(makeString(nspname),
									   makeString(prelname)));
	}
	
	/* and the default partition */
	if (pNode->default_part)
	{
		PartitionRule *rule = pNode->default_part;
		char *prelname;
		char *nspname;
		Relation	rel;
		
		rel = heap_open(rule->parchildrelid, AccessShareLock);
		prelname = pstrdup(RelationGetRelationName(rel));
		nspname = pstrdup(get_namespace_name(RelationGetNamespace(rel)));
		heap_close(rel, NoLock);
		
		if (l1)
			l1 = lappend(l1, list_make2(makeString(nspname),
										makeString(prelname)));
		else
			l1 = list_make1(list_make2(makeString(nspname),
									   makeString(prelname)));
	}
	
	return l1;
} /* end atpxDropList */


void
exchange_part_rule(Oid oldrelid, Oid newrelid)
{
	HeapTuple	 tuple;
	Relation	 catalogRelation;
	cqContext	 cqc;
	cqContext	*pcqCtx;

	catalogRelation = heap_open(PartitionRuleRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), catalogRelation);

	tuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_partition_rule "
				" WHERE parchildrelid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(oldrelid)));

	if (HeapTupleIsValid(tuple))
	{
		((Form_pg_partition_rule)GETSTRUCT(tuple))->parchildrelid = newrelid;
		
		caql_update_current(pcqCtx, tuple);
		/* and Update indexes (implicit) */	

		heap_freetuple(tuple);
	}
	
	heap_close(catalogRelation, NoLock);
}

void
exchange_permissions(Oid oldrelid, Oid newrelid)
{
	HeapTuple oldtuple;
	HeapTuple newtuple;
	Datum save;
	bool saveisnull;
	Datum values[Natts_pg_class];
	bool nulls[Natts_pg_class];
	bool replaces[Natts_pg_class];
	HeapTuple replace_tuple;
	bool isnull;
	Relation rel = heap_open(RelationRelationId, RowExclusiveLock);
	cqContext	oldcqc;
	cqContext	newcqc;
	cqContext	*oldpcqCtx;
	cqContext	*newpcqCtx;

	oldpcqCtx = caql_beginscan(
			caql_addrel(cqclr(&oldcqc), rel),
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(oldrelid)));

	oldtuple = caql_getnext(oldpcqCtx);
	
	if (!HeapTupleIsValid(oldtuple))
		elog(ERROR, "cache lookup failed for relation %u", oldrelid);
	
	save = caql_getattr(oldpcqCtx,
						Anum_pg_class_relacl,
						&saveisnull);
	
	newpcqCtx = caql_beginscan(
			caql_addrel(cqclr(&newcqc), rel),
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(newrelid)));

	newtuple = caql_getnext(newpcqCtx);

	if (!HeapTupleIsValid(newtuple))
		elog(ERROR, "cache lookup failed for relation %u", newrelid);
	
	/* finished building new ACL value, now insert it */
	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));
	
	replaces[Anum_pg_class_relacl - 1] = true;
	values[Anum_pg_class_relacl - 1] = caql_getattr(newpcqCtx,
													Anum_pg_class_relacl,
													&isnull);
	if (isnull)
		nulls[Anum_pg_class_relacl - 1] = true;
	
	replace_tuple = caql_modify_current(oldpcqCtx,
										values, nulls, replaces);
	
	caql_update_current(oldpcqCtx, replace_tuple);
	/* and Update indexes (implicit) */
	
	/* XXX: Update the shared dependency ACL info */
	
	/* finished building new ACL value, now insert it */
	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));
	
	replaces[Anum_pg_class_relacl - 1] = true;
	values[Anum_pg_class_relacl - 1] = save;
	
	if (saveisnull)
		nulls[Anum_pg_class_relacl - 1] = true;
	
	replace_tuple = caql_modify_current(newpcqCtx,
										values, nulls, replaces);
	
	caql_update_current(newpcqCtx, replace_tuple);
	/* and Update indexes (implicit) */
	
	/* update shared dependency */
	
	caql_endscan(oldpcqCtx);
	caql_endscan(newpcqCtx);
	heap_close(rel, NoLock);
}


bool
atpxModifyListOverlap (Relation rel,
					   AlterPartitionId *pid,
					   PgPartRule   	*prule,
					   PartitionElem 	*pelem,
					   bool bAdd)
{
	if (prule->pNode->default_part && bAdd)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("cannot MODIFY %s partition%s for "
						"relation \"%s\" to ADD values -- would "
						"overlap DEFAULT partition \"%s\"",
						"LIST",
						prule->partIdStr,
						RelationGetRelationName(rel),
						prule->pNode->default_part->parname),
				 errhint("need to SPLIT partition \"%s\"",
						 prule->pNode->default_part->parname),
				 errOmitLocation(true)));
	
	{
		ListCell 				*lc;
		PartitionValuesSpec 	*pVSpec;
		AlterPartitionId 	 	 pid2;
		PartitionNode 			*pNode 	= prule->pNode;
		int 				 	 partno = 0;
		CreateStmtContext    	 cxt;
		
		MemSet(&cxt, 0, sizeof(cxt));
		
		Assert (IsA(pelem->boundSpec, PartitionValuesSpec));
		
		MemSet(&pid2, 0, sizeof(AlterPartitionId));
		
		/* this function does transformExpr on the boundary specs */
		partno = atpxPart_validate_spec(makeNode(PartitionBy),
										&cxt,
										rel,
										NULL,		 /* CreateStmt */
										pelem,
										pNode,
										(pid->idtype == AT_AP_IDName) ?
										pid->partiddef : NULL,
										false,		 /* isDefault */
										PARTTYP_LIST, /* part_type */
										prule->partIdStr);
		
		pVSpec = (PartitionValuesSpec *)pelem->boundSpec;
		
		foreach(lc, pVSpec->partValues)
		{
			List			*vals  = lfirst(lc);
			PgPartRule   	*prule2 = NULL;
			
			pid2.idtype = AT_AP_IDValue;
			pid2.partiddef = (Node *)vals;
			pid2.location  = -1;
			
			prule2 = get_part_rule(rel, &pid2, false, false,
								   CurrentMemoryContext, NULL, false);
			
			if (bAdd)
			{
				/* if ADDing a value, should not match */
				
				if (prule2)
				{
					if (prule2->topRuleRank != prule->topRuleRank)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
								 errmsg("cannot MODIFY LIST partition%s for "
										"relation \"%s\" -- "
										"would overlap "
										"existing partition%s",
										prule->partIdStr,
										RelationGetRelationName(rel),
										prule2->isName ?
										prule2->partIdStr : ""),
								 errOmitLocation(true)));
					else
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
								 errmsg("cannot MODIFY LIST partition%s for "
										"relation \"%s\" -- "
										"ADD value has duplicate in "
										"existing partition",
										prule->partIdStr,
										RelationGetRelationName(rel)),
								 errOmitLocation(true)));
				}
			}
			else /* DROP values */
			{
				/* if DROPping a value, it should only be in the
				 * specified partition
				 */
				
				if (!prule2)
				{
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("cannot MODIFY LIST partition%s for "
									"relation \"%s\" -- DROP value not found",
									prule->partIdStr,
									RelationGetRelationName(rel)),
							 errOmitLocation(true)));
				}
				
				if (prule2 && (prule2->topRuleRank != prule->topRuleRank))
				{
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("cannot MODIFY LIST partition%s for "
									"relation \"%s\" -- "
									"found DROP value in%s partition%s",
									prule->partIdStr,
									RelationGetRelationName(rel),
									prule2->isName ? "" : "other",
									prule2->isName ? prule2->partIdStr : ""),
							 errOmitLocation(true)));
				}
			}
			
		} /* end foreach */
	}
	
	return false;
} /* end atpxModifyListOverlap */

bool
atpxModifyRangeOverlap (Relation 		 		 rel,
						AlterPartitionId 		*pid,
						PgPartRule   			*prule,
						PartitionElem 			*pelem)
{
	PgPartRule   		*prule2 	 = NULL;
	AlterPartitionId 	 pid2;
	PartitionNode 		*pNode 		 = prule->pNode;
	bool 				 bCheckStart = true;
	PartitionBoundSpec 	*pbs   		 = NULL;
	ParseState 			*pstate    	 = NULL;
	bool 				 bOverlap 	 = false;
	bool 				*isnull;
	TupleDesc 		 	 tupledesc 	 = RelationGetDescr(rel);
	Datum 				*d_start   	 = NULL;
	Datum 				*d_end 	   	 = NULL;
	Node *pRangeValList = NULL;
	int ii;
	
	Assert (IsA(pelem->boundSpec, PartitionBoundSpec));
	
	pbs = (PartitionBoundSpec *)pelem->boundSpec;
	
	for (ii = 0; ii < 2 ; ii++)
	{
		PartitionRangeItem 		*ri;
		
		if (bCheckStart) /* check START first, then END */
		{
			if (!(pbs->partStart))
			{
				bCheckStart = false;
				
				if (prule->topRule->parrangestart)
				{
					PartitionRangeItem *ri = makeNode(PartitionRangeItem);
					
					ri->location = -1;
					
					ri->partRangeVal =
					copyObject(prule->topRule->parrangestart);
					
					ri->partedge = prule->topRule->parrangestartincl ?
					PART_EDGE_INCLUSIVE :
					PART_EDGE_EXCLUSIVE;
					
					/* no start, so use current start */
					pbs->partStart = (Node *)ri;
				}
				
				continue;
			}
			ri = (PartitionRangeItem *)pbs->partStart;
		}
		else
		{
			/* no END, so we are done */
			if (!(pbs->partEnd))
			{
				
				if (prule->topRule->parrangeend)
				{
					PartitionRangeItem *ri = makeNode(PartitionRangeItem);
					
					ri->location = -1;
					
					ri->partRangeVal = copyObject(prule->topRule->parrangeend);
					
					ri->partedge = prule->topRule->parrangeendincl ?
					PART_EDGE_INCLUSIVE :
					PART_EDGE_EXCLUSIVE;
					
					/* no end, so use current end */
					pbs->partEnd = (Node *)ri;
				}
				
				break;
			}
			ri = (PartitionRangeItem *)pbs->partEnd;
		}
		
		MemSet(&pid2, 0, sizeof(AlterPartitionId));
		
		pid2.idtype = AT_AP_IDValue;
		pstate = make_parsestate(NULL);
		pRangeValList = (Node *) copyObject(ri->partRangeVal);
		pRangeValList = (Node *)
		transformExpressionList(pstate, (List *)pRangeValList);
		free_parsestate(&pstate);
		pid2.partiddef = pRangeValList;
		pid2.location  = -1;
		
		prule2 = get_part_rule(rel, &pid2, false, false,
							   CurrentMemoryContext, NULL, false);
		
		if (!prule2)
		{
			/* no rules matched -- this is ok as long as no
			 * default partition
			 */
			if (prule->pNode->default_part)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("cannot MODIFY %s partition%s for "
								"relation \"%s\" to extend range -- would "
								"overlap DEFAULT partition \"%s\"",
								"RANGE",
								prule->partIdStr,
								RelationGetRelationName(rel),
								prule->pNode->default_part->parname),
						 errhint("need to SPLIT partition \"%s\"",
								 prule->pNode->default_part->parname),
						 errOmitLocation(true)));
			
			/* if only 1 partition no further check needed */
			if (1 == list_length(pNode->rules))
				continue;
			
			if (bCheckStart)
			{
				bool bstat;
				PartitionRule *a_rule;
				
				/* check for adjacent partition */
				if (1 == prule->topRuleRank)
					continue; /* no previous, so changing start is ok */
				
				MemSet(&pid2, 0, sizeof(AlterPartitionId));
				
				pid2.idtype = AT_AP_IDRank;
				pid2.partiddef = (Node *)makeInteger(prule->topRuleRank - 1);
				pid2.location  = -1;
				
				prule2 = get_part_rule(rel, &pid2, false, false,
									   CurrentMemoryContext, NULL, false);
				
				Assert(prule2);
				
				a_rule = prule2->topRule;
				
				/* just check against end of adjacent partition */
				d_start =
				magic_expr_to_datum(rel, pNode,
									pRangeValList, &isnull);
				
				bstat =
				compare_partn_opfuncid(pNode,
									   "pg_catalog",
									   ">",
									   (List *)a_rule->parrangeend,
									   d_start, isnull, tupledesc);
				if (bstat)
				{
					/* end > new start - overlap */
					bOverlap = true;
					break;
				}
				
				/* could be the case that new start == end of
				 * previous.  This is ok if they have opposite
				 * INCLUSIVE/EXCLUSIVE.
				 */
				
				bstat =
				compare_partn_opfuncid(pNode,
									   "pg_catalog",
									   "=",
									   (List *)a_rule->parrangeend,
									   d_start, isnull, tupledesc);
				
				if (bstat)
				{
					if (a_rule->parrangeendincl == 
						(ri->partedge == PART_EDGE_INCLUSIVE))
					{
						/* start and end must be of opposite
						 * types, else they overlap */
						bOverlap = true;
						break;
					}
				}
			}
			else /* check the end */
			{
				bool bstat;
				PartitionRule *a_rule;
				
				/* check for adjacent partition */
				if (list_length(pNode->rules) == prule->topRuleRank)
					continue; /* no next, so changing end is ok */
				
				MemSet(&pid2, 0, sizeof(AlterPartitionId));
				
				pid2.idtype = AT_AP_IDRank;
				pid2.partiddef = (Node *)makeInteger(prule->topRuleRank + 1);
				pid2.location  = -1;
				
				prule2 = get_part_rule(rel, &pid2, false, false,
									   CurrentMemoryContext, NULL, false);
				
				Assert(prule2);
				
				a_rule = prule2->topRule;
				
				/* just check against start of adjacent partition */
				d_end =
				magic_expr_to_datum(rel, pNode,
									pRangeValList, &isnull);
				
				bstat =
				compare_partn_opfuncid(pNode,
									   "pg_catalog",
									   "<",
									   (List *)a_rule->parrangestart,
									   d_end, isnull, tupledesc);
				if (bstat)
				{
					/* start < new end - overlap */
					bOverlap = true;
					break;
				}
				
				/* could be the case that new end == start of
				 * next.  This is ok if they have opposite
				 * INCLUSIVE/EXCLUSIVE.
				 */
				
				bstat =
				compare_partn_opfuncid(pNode,
									   "pg_catalog",
									   "=",
									   (List *)a_rule->parrangestart,
									   d_end, isnull, tupledesc);
				
				if (bstat)
				{
					if (a_rule->parrangeendincl == 
						(ri->partedge == PART_EDGE_INCLUSIVE))
					{
						/* start and end must be of opposite
						 * types, else they overlap */
						bOverlap = true;
						break;
					}
				}
			} /* end else check the end */
			
		}
		else
		{
			/* matched a rule - definitely a problem if the range was
			 * inclusive
			 */
			if (prule2->topRuleRank != prule->topRuleRank)
			{
				if (0 == prule2->topRuleRank)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("cannot MODIFY %s partition%s for "
									"relation \"%s\" to extend range -- would "
									"overlap DEFAULT partition \"%s\"",
									"RANGE",
									prule->partIdStr,
									RelationGetRelationName(rel),
									prule->pNode->default_part->parname),
							 errhint("need to SPLIT partition \"%s\"",
									 prule->pNode->default_part->parname),
							 errOmitLocation(true)));
				
				
				if (ri->partedge == PART_EDGE_INCLUSIVE)
				{
					bOverlap = true;
					break;
				}
				
				/* range was exclusive -- need to do some checking */
				if (bCheckStart)
				{
					bool bstat;
					PartitionRule *a_rule = prule2->topRule;
					
					/* check for adjacent partition */
					if ((prule->topRuleRank - 1) != prule2->topRuleRank)
					{
						bOverlap = true;
						break;
					}
					
					/* just check against end of adjacent partition */
					d_start =
					magic_expr_to_datum(rel, pNode,
										pid2.partiddef, &isnull);
					
					bstat =
					compare_partn_opfuncid(pNode,
										   "pg_catalog",
										   ">",
										   (List *)a_rule->parrangeend,
										   d_start, isnull, tupledesc);
					if (bstat)
					{
						/* end > new start - overlap */
						bOverlap = true;
						break;
					}
					
					/* Must be the case that new start == end of
					 * a_rule (because if the end < new start then how
					 * could we find it in the interval for prule ?)
					 * This is ok if they have opposite
					 * INCLUSIVE/EXCLUSIVE ->  New partition does not
					 * overlap.
					 */
					
					Assert (compare_partn_opfuncid(pNode,
												   "pg_catalog",
												   "=",
												   (List *)a_rule->parrangeend,
												   d_start, isnull, tupledesc));
					
					if (a_rule->parrangeendincl == 
						(ri->partedge == PART_EDGE_INCLUSIVE))
					{
						/* start and end must be of opposite
						 * types, else they overlap
						 */
						bOverlap = true;
						break;
					}
				}
				else /* check the end */
				{
					bool bstat;
					PartitionRule *a_rule = prule2->topRule;
					
					/* check for adjacent partition */
					if ((prule->topRuleRank + 1) != prule2->topRuleRank)
					{
						bOverlap = true;
						break;
					}
					
					/* just check against start of adjacent partition */
					d_end =
					magic_expr_to_datum(rel, pNode,
										pid2.partiddef, &isnull);
					
					bstat =
					compare_partn_opfuncid(pNode,
										   "pg_catalog",
										   "<",
										   (List *)a_rule->parrangestart,
										   d_end, isnull, tupledesc);
					if (bstat)
					{
						/* start < new end - overlap */
						bOverlap = true;
						break;
					}
					
					/* Must be the case that new end = start of
					 * a_rule (because if the start > new end then how
					 * could we find it in the interval for prule ?)
					 * This is ok if they have opposite
					 * INCLUSIVE/EXCLUSIVE ->  New partition does not
					 * overlap.
					 */
					
					Assert (compare_partn_opfuncid(pNode,
												   "pg_catalog",
												   "=",
												   (List *)a_rule->parrangestart,
												   d_end, isnull, tupledesc));
					
					if (a_rule->parrangestartincl == 
						(ri->partedge == PART_EDGE_INCLUSIVE))
					{
						/* start and end must be of opposite
						 * types, else they overlap
						 */
						bOverlap = true;
						break;
					}
				} /* end else check the end */
			} /* end if (prule2->topRuleRank != prule->topRuleRank) */
		}
		
		/* if checked START, then check END.  If checked END, then done */
		if (!bCheckStart)
			break;
		if (bCheckStart)
			bCheckStart = false;
	} /* end for */
	
	if (bOverlap)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("cannot MODIFY RANGE partition%s for "
						"relation \"%s\" -- "
						"would overlap "
						"existing partition%s",
						prule->partIdStr,
						RelationGetRelationName(rel),
						(prule2 && prule2->isName) ?
						prule2->partIdStr : ""),
				 errOmitLocation(true)));
	
	{
		int 				 	 partno = 0;
		CreateStmtContext    	 cxt;
		
		MemSet(&cxt, 0, sizeof(cxt));
		
		/* this function does transformExpr on the boundary specs */
		partno = atpxPart_validate_spec(makeNode(PartitionBy),
										&cxt,
										rel,
										NULL,		 /* CreateStmt */
										pelem,
										pNode,
										(pid->idtype == AT_AP_IDName) ?
										pid->partiddef : NULL,
										false,		 /* isDefault */
										PARTTYP_RANGE, /* part_type */
										prule->partIdStr);
	}
	
	
	return false;
} /* end atpxModifyRangeOverlap */

static void atpxSkipper(PartitionNode *pNode, int *skipped)
{
	ListCell *lc;
	
	if (!pNode) return;
	
	/* add entries for rules at current level */
	foreach(lc, pNode->rules)
	{
		PartitionRule 	*rule = lfirst(lc);
		
		if (skipped) *skipped += 1;
		
		if (rule->children)
			atpxSkipper(rule->children, skipped);
	} /* end foreach */
	
	/* and the default partition */
	if (pNode->default_part)
	{
		PartitionRule 	*rule = pNode->default_part;
		
		if (skipped) *skipped += 1;
		
		if (rule->children)
			atpxSkipper(rule->children, skipped);
	}
} /* end atpxSkipper */

static List *
build_rename_part_recurse(PartitionRule *rule, const char *old_parentname,
						  const char *new_parentname,
						  int *skipped)
{
	
	RangeVar 		*rv;
	Relation		 rel;
	char *relname = NULL;
	char newRelNameBuf[(NAMEDATALEN*2)];
	List *l1 = NIL;
	
	rel = heap_open(rule->parchildrelid, AccessShareLock);
	
	relname = pstrdup(RelationGetRelationName(rel));
	
	rv = makeRangeVar(NULL /*catalogname*/, get_namespace_name(RelationGetNamespace(rel)),
					  relname, -1);
	
	/* unlock, because we have a lock on the master */
	heap_close(rel, AccessShareLock);
	
	/* 
	 * The child name should contain the old parent name as a
	 * prefix - check the length and compare to make sure.
	 *
	 * To build the new child name, just use the new name as a
	 * prefix, and use the remainder of the child name (the part
	 * after the old parent name prefix) as the suffix.
	 */
	if (strlen(old_parentname) > strlen(relname))
	{
		if (skipped)
			*skipped += 1;
		
		atpxSkipper(rule->children, skipped);
	}
	else
	{
		if (0 != (strncmp(old_parentname, relname, strlen(old_parentname))))
		{
			if (skipped)
				*skipped += 1;
			atpxSkipper(rule->children, skipped);
		}
		else
		{
			snprintf(newRelNameBuf, sizeof(newRelNameBuf), "%s%s",
					 new_parentname, relname + strlen(old_parentname));
			
			if (strlen(newRelNameBuf) > NAMEDATALEN)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("relation name \"%s\" for child partition "
								"is too long",
								newRelNameBuf)));
			
			l1 = lappend(l1, list_make2(rv, pstrdup(newRelNameBuf)));
			
			/* add the child lists next (not first) */
			{
				List *l2 = NIL;
				
				if (rule->children)
					l2 = atpxRenameList(rule->children,
										relname, newRelNameBuf, skipped);

				if (l2)
					l1 = list_concat(l1, l2);
			}
		}
	}
	return l1;
}

List *
atpxRenameList(PartitionNode *pNode,
			   char *old_parentname, const char *new_parentname, int *skipped)
{
	List *l1 = NIL;
	ListCell *lc;
	
	if (!pNode)
		return l1;
	
	/* add entries for rules at current level */
	foreach(lc, pNode->rules)
	{
		PartitionRule 	*rule = lfirst(lc);
		
		l1 = list_concat(l1,
						 build_rename_part_recurse(rule,
												   old_parentname,
												   new_parentname,
												   skipped));
	} /* end foreach */
	
	/* and the default partition */
	if (pNode->default_part)
	{
		PartitionRule 	*rule = pNode->default_part;
		
		l1 = list_concat(l1,
						 build_rename_part_recurse(rule,
												   old_parentname,
												   new_parentname,
												   skipped));
	}
	
	return l1;
} /* end atpxRenameList */


static Oid
get_opfuncid_by_opname(List *opname, Oid lhsid, Oid rhsid)
{
	Oid opfuncid;
	Operator op;
	
	op = oper(NULL, opname, lhsid, rhsid, false, -1);
	
	if (op == NULL)	  /* should not fail */
		elog(ERROR, "could not find operator");
	
	opfuncid = ((Form_pg_operator)GETSTRUCT(op))->oprcode;
	
	ReleaseOperator(op);
	return opfuncid;
}


/* Construct the PgPartRule for a branch of a partitioning hierarchy.
 *
 * 		rel - the partitioned relation (top-level)
 *		cmd - an AlterTableCmd, possibly nested in type AT_PartAlter AlterTableCmds
 *            identifying a subset of the parts of the partitioned relation.
 */
static PgPartRule *
get_pprule_from_ATC(Relation rel, AlterTableCmd *cmd)
{
	List *pids = NIL; /* of AlterPartitionId */
	AlterPartitionId *pid = NULL;
	PgPartRule *pprule = NULL;
	AlterPartitionId *work_partid = NULL;
	
	AlterTableCmd *atc = cmd;
	
	
	/* Get list of enclosing ALTER PARTITION ids. */
	while ( atc->subtype == AT_PartAlter )
	{
		AlterPartitionCmd *apc = (AlterPartitionCmd*)atc->def;
		
		pid = (AlterPartitionId*)apc->partid;
		Insist(IsA(pid, AlterPartitionId));
		
		atc = (AlterTableCmd*)apc->arg1;
		Insist(IsA(atc, AlterTableCmd));
		
		pids = lappend(pids, pid);
	}
	
	/* The effective ALTER TABLE command is in atc.
	 * The pids list (of AlterPartitionId nodes) represents the path to 
	 * top partitioning branch of rel.  Since we are only called for
	 * branches and leaves (never the root) of the partition, the pid
	 * list should not empty.
	 *
	 * Use the AlterPartitionId interpretter, get_part_rule, to do
	 * the interpretation.
	 */
	Insist( list_length(pids) > 0 );
	
	work_partid = makeNode(AlterPartitionId);
	
	work_partid->idtype = AT_AP_IDList;
	work_partid->partiddef = (Node*)pids;
	work_partid->location = -1;
	
	pprule = get_part_rule(rel, 
						   work_partid, 
						   true, true, /* parts must exist */
						   CurrentMemoryContext,
						   NULL, /* no implicit results */
						   false /* no template rules */
						   );
	
	return pprule;
}

/* Return the pg_class OIDs of the relations representing the parts of
 * a partitioned table designated by the given AlterTable command.  
 *
 * 		rel - the partitioned relation (top-level)
 *		cmd - an AlterTableCmd, possibly nested in type AT_PartAlter AlterTableCmds
 *            identifying a subset of the parts of the partitioned relation.
 */
List *
basic_AT_oids(Relation rel, AlterTableCmd *cmd)
{
	PgPartRule *pprule = get_pprule_from_ATC(rel, cmd);
	
	if ( ! pprule )
		return NIL;
	
	return all_prule_relids(pprule->topRule);	
}

/*
 * Return the basic AlterTableCmd found by peeling off intervening layers of
 * ALTER PARTITION from the given AlterTableCmd.
 */
AlterTableCmd *basic_AT_cmd(AlterTableCmd *cmd)
{
	while ( cmd->subtype == AT_PartAlter )
	{
		AlterPartitionCmd *apc = (AlterPartitionCmd*)cmd->def;
		Insist(IsA(apc, AlterPartitionCmd));
		cmd = (AlterTableCmd*)apc->arg1;
		Insist(IsA(cmd, AlterTableCmd));
	}
	return cmd;
}


/* Determine whether we can implement a requested distribution on a part of
 * the specified partitioned table.
 *
 * In 3.3
 *   DISTRIBUTED RANDOMLY or distributed just like the whole partitioned
 *   table is implementable.  Anything else is not.
 *
 * rel              Pointer to cache entry for the whole partitioned table
 * dist_cnames      List of column names proposed for distribution some part
 */
bool can_implement_dist_on_part(Relation rel, List *dist_cnames)
{
	ListCell *lc;
	int i;
	
	if (Gp_role != GP_ROLE_DISPATCH)
	{
		ereport(DEBUG1,
				(errmsg("requesting redistribution outside dispatch - returning no")));
		return false;
	}
	
	/* Random is okay.  It is represented by a list of one empty list. */
	if ( list_length(dist_cnames) == 1 && linitial(dist_cnames) == NIL )
		return true;
	
	/* Require an exact match to the policy of the parent. */
	if ( list_length(dist_cnames) != rel->rd_cdbpolicy->nattrs )
		return false;
	
	i = 0;
	foreach(lc, dist_cnames)
	{
		AttrNumber     attnum;
		char *cname;
		HeapTuple tuple;
		Node *item = lfirst(lc);
		bool ok = false;
		cqContext	*pcqCtx;
		
		if ( !(item && IsA(item, String)) )
			return false;
		
		cname = strVal((Value *)item);
		pcqCtx = caql_getattname_scan(NULL, RelationGetRelid(rel), cname);
		tuple = caql_get_current(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							cname,
							RelationGetRelationName(rel)),
					 errOmitLocation(true)));
		
		attnum = ((Form_pg_attribute) GETSTRUCT(tuple))->attnum;
		ok = attnum == rel->rd_cdbpolicy->attrs[i++];
		caql_endscan(pcqCtx);
		
		if ( ! ok )
			return false;
	}
	return true;
}



/* Test whether we can exchange newrel into oldrel's space within the 
 * partitioning hierarchy of rel as far as the table schema is concerned.
 * (Does not, e.g., look into constraint agreement, etc.)
 *
 * If throw is true, throw an appropriate error in case the answer is
 * "no, can't exchange".  If throw is false, just return the answer
 * quietly.
 */
bool
is_exchangeable(Relation rel, Relation oldrel, Relation newrel, bool throw)
{	
	AttrMap *map_new = NULL;
	AttrMap *map_old = NULL;
	bool congruent = TRUE;
	
	/* Both parts must be relations. */
	if (!(oldrel->rd_rel->relkind == RELKIND_RELATION ||
		  newrel->rd_rel->relkind == RELKIND_RELATION))
	{
		congruent = FALSE;
		if ( throw )
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cannot exchange relation "
							"which is not a table")));
	}
	
	/* Attributes of the existing part (oldrel) must be compatible with the 
	 * partitioned table as a whole.  This might be an assertion, but we don't
	 * want this case to pass in a production build, so we use an internal
	 * error.
	 */
	if (congruent && ! map_part_attrs(rel, oldrel, &map_old, FALSE) )
	{
		congruent = FALSE;
		if ( throw )
			elog(ERROR, "existing part \"%s\" not congruent with"
				 "partitioned table \"%s\"",
				 RelationGetRelationName(oldrel),
				 RelationGetRelationName(rel));
	}
	
	/* From here on we need to be careful to free the maps. */
	
	/* Attributes of new part must be compatible with the partitioned table.
	 * (We assume that the attributes of the old part are compatible.)
	 */
	if ( congruent && ! map_part_attrs(rel, newrel, &map_new, throw) )
		congruent = FALSE;
	
	/* Both parts must have the same owner. */
	if ( congruent && oldrel->rd_rel->relowner != newrel->rd_rel->relowner)
	{
		congruent = FALSE;
		if ( throw )
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("owner of \"%s\" must be the same as that "
							"of \"%s\"",
							RelationGetRelationName(newrel),
							RelationGetRelationName(rel)),
					 errOmitLocation(true)));
	}
	
	/* Both part tables must have the same "WITH OID"s setting */
	if (congruent && oldrel->rd_rel->relhasoids != newrel->rd_rel->relhasoids)
	{
		congruent = FALSE;
		if ( throw )
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("\"%s\" and \"%s\" must have same OIDs setting",
							RelationGetRelationName(rel),
							RelationGetRelationName(newrel)),
					 errOmitLocation(true)));
	}
	
	/* The new part table must not be involved in inheritance. */
	if ( congruent && has_subclass_fast(RelationGetRelid(newrel)))
	{
		congruent = FALSE;
		if ( throw )
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cannot EXCHANGE table \"%s\" as it has "
							"child table(s)",
							RelationGetRelationName(newrel)),
					 errOmitLocation(true)));
	}
	
	if (congruent && relation_has_supers(RelationGetRelid(newrel)))
	{
		congruent = FALSE;
		if ( throw )
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cannot exchange table \"%s\" as it "
							"inherits other table(s)",
							RelationGetRelationName(newrel)),
					 errOmitLocation(true)));
	}
	
	/* The new part table must not have rules on it. */
	if ( congruent && ( newrel->rd_rules || oldrel->rd_rules ) )
	{
		congruent = FALSE;
		if ( throw )
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cannot exchange table which has rules "
							"defined on it"),
					 errOmitLocation(true)));
	}
	
	/* The distribution policies of the existing part (oldpart) and the
	 * candidate part (newpart) must match that of the whole partitioned
	 * table.  However, we can only check this where the policy table is 
	 * populated, i.e., on the entry database.  Note checking the policy
	 * of the existing part is defensive.  It SHOULD match.
	 */
	if (congruent && Gp_role == GP_ROLE_DISPATCH)
	{
		GpPolicy *parpol = rel->rd_cdbpolicy;
		GpPolicy *oldpol = oldrel->rd_cdbpolicy;
		GpPolicy *newpol = newrel->rd_cdbpolicy;
		GpPolicy *adjpol = NULL;
		
		if ( map_old != NULL )
		{
			int i;
			AttrNumber remapped_parent_attr = 0;
			
			for ( i = 0; i < parpol->nattrs; i++ )
			{
				remapped_parent_attr = attrMap(map_old, parpol->attrs[i]);
				
				if ( ! (parpol->attrs[i] > 0  /* assert parent live */
						&& oldpol->attrs[i] > 0   /* assert old part live */
						&& remapped_parent_attr == oldpol->attrs[i] /* assert match */
						))
					elog(ERROR, 
						 "discrepancy in partitioning policy of \"%s\"",
						 RelationGetRelationName(rel));
			}
		}
		else
		{
			if (! GpPolicyEqual(parpol, oldpol) )
				elog(ERROR, 
					 "discrepancy in partitioning policy of \"%s\"",
					 RelationGetRelationName(rel));
		}
		
		if ( map_new != NULL )
		{
			int i;
			adjpol = GpPolicyCopy(CurrentMemoryContext, parpol);
			
			for ( i = 0; i < adjpol->nattrs; i++ )
			{
				adjpol->attrs[i] = attrMap(map_new, parpol->attrs[i]);
				Assert(newpol->attrs[i] > 0); /* check new part */
			}
		}
		else
		{
			adjpol = parpol;
		}
		
		if (! GpPolicyEqual(adjpol, newpol) )
		{
			congruent = FALSE;
			if ( throw )
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("distribution policy for \"%s\" "
								"must be the same as that for \"%s\"",
								RelationGetRelationName(newrel),
								RelationGetRelationName(rel)),
						 errOmitLocation(true)));
		}
		else if (false && memcmp(oldpol->attrs, newpol->attrs,
								 sizeof(AttrNumber) * adjpol->nattrs))
		{
			congruent = FALSE;
			if ( throw )
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("distribution policy matches but implementation lags"),
						 errOmitLocation(true)));
		}
	}
	
	if ( map_old != NULL ) pfree(map_old);
	if ( map_new != NULL ) pfree(map_new);
	
	return congruent;
}


/*
 * Apply the constraint represented by the argument pg_constraint tuple
 * remapped through the argument attribute map to the candidate relation.
 *
 * In addition, if validate is true and the constraint is one we enforce
 * on partitioned tables, allocate and return a NewConstraint structure 
 * for use in phase 3 to validate the relation (i.e., to make sure it 
 * conforms to its constraints).
 *
 * Note that pgcon (the ConstraintRelationId appropriately locked) 
 * is supplied externally for efficiency.  No other relation should
 * be supplied via this argument.
 
 */
static NewConstraint * 
constraint_apply_mapped(HeapTuple tuple, AttrMap *map, Relation cand, 
						bool validate, bool is_split, Relation pgcon)
{
	Datum val;
	bool isnull;
	Datum *dats;
	int16 *keys;
	int nkeys;
	int i;
	Node *conexpr;
	char *consrc;
	char *conbin;
	Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tuple);
	NewConstraint *newcon = NULL;
	
	/* Translate pg_constraint.conkey */
	val = heap_getattr(tuple, Anum_pg_constraint_conkey,
					   RelationGetDescr(pgcon), &isnull);
	Assert(!isnull);
	
	deconstruct_array(DatumGetArrayTypeP(val),
					  INT2OID, 2, true, 's',
					  &dats, NULL, &nkeys);
	
	keys = palloc(sizeof(int16) * nkeys);
	for (i = 0; i < nkeys; i++)
	{
		int16 key =  DatumGetInt16(dats[i]);
		keys[i] = (int16)attrMap(map, key);
	}
	
	/* Translate pg_constraint.conbin */
	val = heap_getattr(tuple, Anum_pg_constraint_conbin,
					   RelationGetDescr(pgcon), &isnull);
	if ( !isnull )
	{
		conbin = DatumGetCString(DirectFunctionCall1(textout, val));
		conexpr = stringToNode(conbin);
		conexpr = attrMapExpr(map, conexpr);
		conbin = nodeToString(conexpr);
	}
	else 
	{
		conbin = NULL;
		conexpr = NULL;
	}
	
	
	/* Don't translate pg_constraint.consrc -- per doc'n, use original */
	val = heap_getattr(tuple, Anum_pg_constraint_consrc,
					   RelationGetDescr(pgcon), &isnull);
	if (!isnull)
	{
		consrc = DatumGetCString(DirectFunctionCall1(textout, val));
	}
	else
	{
		consrc = NULL;
	}
	
	/* Apply translated constraint to candidate. */
	switch ( con->contype )
	{
		case CONSTRAINT_CHECK:
		{
			Assert( conexpr && conbin && consrc );
			
			CreateConstraintEntry(NameStr(con->conname),
								  InvalidOid,
								  con->connamespace, // XXX should this be RelationGetNamespace(cand)?
								  con->contype,
								  con->condeferrable,
								  con->condeferred,
								  RelationGetRelid(cand),
								  keys,
								  nkeys,
								  InvalidOid,
								  InvalidOid,
								  NULL,
								  0,
								  ' ',
								  ' ',
								  ' ',
								  InvalidOid,
								  conexpr,
								  conbin,
								  consrc);
			break;
		}
			
		case CONSTRAINT_FOREIGN:
		{
			int16 *fkeys;
			int nfkeys;
			Oid indexoid = InvalidOid;
			Oid *opclasses = NULL;
			Relation frel;
			
			val = heap_getattr(tuple, Anum_pg_constraint_confkey,
							   RelationGetDescr(pgcon), &isnull);
			Assert(!isnull);
			
			deconstruct_array(DatumGetArrayTypeP(val),
							  INT2OID, 2, true, 's',
							  &dats, NULL, &nfkeys);
			fkeys = palloc(sizeof(int16) * nfkeys);
			for (i = 0; i < nfkeys; i++)
			{
				fkeys[i] =  DatumGetInt16(dats[i]);
			}
			
			frel = heap_open(con->confrelid, AccessExclusiveLock);
			indexoid = transformFkeyCheckAttrs(frel, nfkeys, fkeys, opclasses);
			
			CreateConstraintEntry(NameStr(con->conname),
								  InvalidOid,
								  RelationGetNamespace(cand),
								  con->contype,
								  con->condeferrable,
								  con->condeferred,
								  RelationGetRelid(cand),
								  keys,
								  nkeys,
								  InvalidOid,
								  con->confrelid,
								  fkeys,
								  nfkeys,
								  con->confupdtype,
								  con->confdeltype,
								  con->confmatchtype,
								  indexoid,
								  NULL,		/* no check constraint */
								  NULL,
								  NULL);
			
			heap_close(frel, AccessExclusiveLock);
			break;
		}
		case CONSTRAINT_PRIMARY:
		case CONSTRAINT_UNIQUE:
		{
			/* Index-backed constraints are handled as indexes.  No action here. */
			char *what = (con->contype == CONSTRAINT_PRIMARY)? "PRIMARY KEY" :"UNIQUE";
			char *who = NameStr(con->conname);
			
			if (is_split)
			{
				; /* nothing */
			}
			else if (validate)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("%s constraint \"%s\" missing", what, who),
						 errhint("Add %s constraint \"%s\" to the candidate table"
								 " or drop it from the partitioned table."
								 , what, who),
						 errOmitLocation(true)));
			}
			else 
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("WITHOUT VALIDATION incompatible with missing %s constraint \"%s\"", 
								what, who),
						 errhint("Add %s constraint %s to the candidate table"
								 " or drop it from the partitioned table."
								 , what, who),
						 errOmitLocation(true)));
					
			}
			break;
		}
		default:
			/* Defensive, can't occur. */
			elog(ERROR,"invalid constraint type: %c", con->contype);
			break;
	}
	
	newcon = NULL;
	
	if ( validate )
	{
		switch ( con->contype )
		{
			case CONSTRAINT_CHECK:
			{
				newcon = (NewConstraint*) palloc0(sizeof(NewConstraint));
				newcon->name = pstrdup(NameStr(con->conname));
				/* ExecQual wants implicit-AND format */
				newcon->qual = (Node *)make_ands_implicit((Expr *)conexpr);
				newcon->contype = CONSTR_CHECK;
				break;
			}
			case CONSTRAINT_FOREIGN:
			{
				elog(WARNING, "Won't enforce FK constraint.");
				break;
			}
			case CONSTRAINT_PRIMARY:
			{
				elog(WARNING, "Won't enforce PK constraint.");
				break;
			}
			case CONSTRAINT_UNIQUE:
			{
				elog(WARNING, "Won't enforce ND constraint.");
				break;
			}
			default:
			{
				elog(WARNING, "!! NOT READY FOR TYPE %c CONSTRAINT !!", con->contype);
				break;
			}
		}
	}
	return newcon;
}


static bool
relation_has_supers(Oid relid)
{
	return (
			(caql_getcount(
					NULL,
					cql("SELECT count(*) FROM pg_inherits "
						" WHERE inhrelid = :1 ",
						ObjectIdGetDatum(relid))) > 0));
}

/*
 * Choose a default name for a constraint on an existing partitioned table.
 * For sanity (and visually matching constraints created by the same ALTER 
 * command) we want to avoid letting a lower-level routine pick the default 
 * name for a constraint on a partitioned table, because this would result 
 * in different names for the "same" constraint on each part.  So we use
 * this routine to forge a non-default name earlier than is done for ordinary 
 * tables.
 *
 * This code is modelled on code in AddRelationRawConstraints from heap.c, 
 * however, there is no actual requirement that they stay in sync.  It's 
 * just nice, if they do.  Here, the expression hasn't been cooked yet, so
 * we can't use an expression tree walker to get the column names.
 *
 * For CHECK constraints, expr is an uncooked constraint expression.
 * For index-backed constraints (PRIMARY KEY, UNIQUE), expr is a list of IndexElem.
 *
 * A small possibility of name collision still exists, however, it seems 
 * remote and will be detected and rejected later anyway.
 */
char *
ChooseConstraintNameForPartitionEarly(Relation rel, ConstrType contype, Node *expr)
{
	char *colname = NULL;
	char *ccname = NULL;
	char *label = NULL;
	
	switch (contype)
	{
		case CONSTR_CHECK:
		{
			ParseState *dummy;
			Node *txpr;
			List *vars = NIL;
			
			dummy = make_parsestate(NULL);
			addRTEtoQuery(dummy, 
						  addRangeTableEntryForRelation(dummy, rel, NULL, false, true), 
						  true, true, true);
			txpr = transformExpr(dummy, expr);
			vars = pull_var_clause(txpr, false);
			
			/* eliminate duplicates */
			vars = list_union(NIL, vars);
			
			if (list_length(vars) == 1)
				colname = get_attname(RelationGetRelid(rel),
									  ((Var *) linitial(vars))->varattno);

			label = "check";
		}
			break;
		case CONSTR_PRIMARY:
			/* Conventionally, we ignore column name for the PK. */
			label = "pkey";
			break;
		case CONSTR_UNIQUE:
		{
			ListCell *lc;
			IndexElem *elem;
			
			foreach(lc, (List*)expr)
			{
				elem = (IndexElem*)lfirst(lc);
				if ( elem->name )
				{
					colname = elem->name;
					break;
				}
			}
			
			label = "key";
		}
			break;
		default:
			elog(ERROR, "name request for constraint of inappropriate type");
			break;
	}
	
	ccname = ChooseConstraintName(RelationGetRelationName(rel),
								  colname,
								  label,
								  RelationGetNamespace(rel),
								  NIL);
	
	return ccname;
}

/*
 * Preprocess a CreateStmt for a partitioned table before letting it
 * fall into the regular tranformation code.  This is an analyze-time
 * function.
 *
 * At the moment, the only fixing needed is to change default constraint
 * names to explicit ones so that they will propagate correctly through
 * to the parts of a partitioned table.
 */
void
fixCreateStmtForPartitionedTable(CreateStmt *stmt)
{
	ListCell *lc_elt;
	Constraint *con;
	List *unnamed_cons = NIL;
	List *unnamed_cons_col = NIL;
	List *unnamed_cons_lbl = NIL;
	List *used_names = NIL;
	char *no_name = "";
	int i;
	
	/* Caller should check this! */
	Assert(stmt->partitionBy && !stmt->is_part_child);
	
	foreach( lc_elt, stmt->tableElts )
	{
		Node * elt = lfirst(lc_elt);
		
		switch (nodeTag(elt))
		{
			case T_ColumnDef:
			{
				ListCell *lc_con;
				
				ColumnDef *cdef = (ColumnDef*)elt;
				
				foreach( lc_con, cdef->constraints )
				{
					Node *conelt = lfirst(lc_con);
					
					if ( IsA(conelt, Constraint) )
					{
						con = (Constraint*)conelt;
						
						if ( con->name )
						{
							used_names = lappend(used_names, con->name);
							continue;
						}
						switch (con->contype)
						{
							case CONSTR_CHECK:
								unnamed_cons = lappend(unnamed_cons, con);
								unnamed_cons_col = lappend(unnamed_cons_col, cdef->colname);
								unnamed_cons_lbl = lappend(unnamed_cons_lbl, "check");
								break;
							case CONSTR_PRIMARY:
								unnamed_cons = lappend(unnamed_cons, con);
								unnamed_cons_col = lappend(unnamed_cons_col, cdef->colname);
								unnamed_cons_lbl = lappend(unnamed_cons_lbl, "pkey");
								break;
							case CONSTR_UNIQUE:
								unnamed_cons = lappend(unnamed_cons, con);
								unnamed_cons_col = lappend(unnamed_cons_col, cdef->colname);
								unnamed_cons_lbl = lappend(unnamed_cons_lbl, "key");
								break;
							default:
								break;
						}
					}
					else
					{
						FkConstraint *fkcon = (FkConstraint*)conelt;
						
						Insist( IsA(fkcon, FkConstraint) );
						
						if ( fkcon->constr_name )
						{
							used_names = lappend(used_names, fkcon->constr_name);
							continue;
						}
						
						unnamed_cons = lappend(unnamed_cons, fkcon);
						unnamed_cons_col = lappend(unnamed_cons_col, cdef->colname);
						unnamed_cons_lbl = lappend(unnamed_cons_lbl, "fkey");
					}
				}
				break;
			}
			case T_Constraint:
			{
				con = (Constraint*)elt;
				
				if ( con->name )
				{
					used_names = lappend(used_names, con->name);
				}
				else
				{
					switch (con->contype)
					{
						case CONSTR_CHECK:
							unnamed_cons = lappend(unnamed_cons, con);
							unnamed_cons_col = lappend(unnamed_cons_col, no_name);
							unnamed_cons_lbl = lappend(unnamed_cons_lbl, "check");
							break;
						case CONSTR_PRIMARY:
							unnamed_cons = lappend(unnamed_cons, con);
							unnamed_cons_col = lappend(unnamed_cons_col, no_name);
							unnamed_cons_lbl = lappend(unnamed_cons_lbl, "pkey");
							break;
						case CONSTR_UNIQUE:
							unnamed_cons = lappend(unnamed_cons, con);
							unnamed_cons_col = lappend(unnamed_cons_col, no_name);
							unnamed_cons_lbl = lappend(unnamed_cons_lbl, "key");
							break;
						default:
							break;
					}
				}
				break;
			}
			case T_FkConstraint:
			{
				FkConstraint *fkcon = (FkConstraint*)elt;
				
				unnamed_cons = lappend(unnamed_cons, fkcon);
				unnamed_cons_col = lappend(unnamed_cons_col, no_name);
				unnamed_cons_lbl = lappend(unnamed_cons_lbl, "fkey");
				
				if ( fkcon->constr_name )
				{
					used_names = lappend(used_names, fkcon->constr_name);
				}
				break;
			}
			case T_InhRelation:
			{
				break;
			}
			default:
				break;
		}
	}
	
	used_names = list_union(used_names, NIL); /* eliminate dups */
	
	for ( i = 0; i < list_length(unnamed_cons); i++ )
	{
		char *label = list_nth(unnamed_cons_lbl, i);
		char *colname = NULL;
		Node *elt = list_nth(unnamed_cons, i);
		
		switch ( nodeTag(elt) )
		{
			case T_FkConstraint:
			{
				FkConstraint *fcon = list_nth(unnamed_cons, i);
			
				fcon->constr_name = 
					ChooseConstraintNameForPartitionCreate(stmt->relation->relname,
														   colname,
														   label,
														   used_names);
				used_names = lappend(used_names, fcon->constr_name);
				break;
			}
			
			case T_Constraint:
			{
				Constraint *con = list_nth(unnamed_cons, i);
				
				/* Conventionally, no column name for PK. */
				if ( 0 != strcmp(label, "pkey") )
					colname = list_nth(unnamed_cons_col, i);
				
				con->name = ChooseConstraintNameForPartitionCreate(stmt->relation->relname,
																   colname,
																   label,
																   used_names);
				used_names = lappend(used_names,con->name);
				
				break;
			}
			default:
				break;
		}
	}
}


/*
 * Subroutine for fixCreateStmtForPartitionedTable.
 *
 * Similar to ChooseConstraintNameForPartitionEarly but doesn't use the
 * catalogs since we're dealing with a currently non-existent namespace
 * (the space of constraint names on the table to be created).  
 *
 * Modelled on ChooseConstraintName, though synchronization isn't a
 * requirement, just a nice idea.
 *
 * Caller is responsible for supplying the (unqualified) relation name,
 * optional column name (NULL or "" is okay for a table constraint),
 * label (e.g. "check"), and list of names to avoid.
 *
 * Result is palloc'd and caller's responsibility.
 */
char *
ChooseConstraintNameForPartitionCreate(const char *rname, 
									   const char *cname, 
									   const char *label, 
									   List *used_names)
{
	int pass = 0;
	char *conname = NULL;
	char modlabel[NAMEDATALEN];
	bool found = false;
	ListCell *lc;
	
	Assert(rname && *rname);
	
	/* Allow caller to pass "" instead of NULL for non-singular cname */
	if ( cname && *cname == '\0' )
		cname = NULL;
	
	/* try the unmodified label first */
	StrNCpy(modlabel, label, sizeof(modlabel));
	
	for (;;)
	{
		conname = makeObjectName(rname, cname, modlabel);
		found = false;
		
		foreach(lc, used_names)
		{
			if (strcmp((char*)lfirst(lc), conname) == 0)
			{
				found = true;
				break;
			}
		}
		if ( ! found )
			break;  /* we have a winner */
		
		pfree(conname);
		snprintf(modlabel, sizeof(modlabel), "%s%d", label, ++pass);
	}
	return conname;
}

/*
 * Determine whether the given attributes can be enforced unique within
 * the partitioning policy of the given partitioned table.  If not, issue
 * an error.  The argument primary just conditions the message text.
 */
void
checkUniqueConstraintVsPartitioning(Relation rel, AttrNumber *indattr, int nidxatts, bool primary)
{
	int i;
	bool contains;
	Bitmapset *ikey = NULL;
	Bitmapset *pkey = get_partition_key_bitmapset(RelationGetRelid(rel));
	
	for (i = 0; i < nidxatts; i++)
		ikey = bms_add_member(ikey, indattr[i]);
	
	contains = bms_is_subset(pkey, ikey);
	
	if (pkey)
		bms_free(pkey);
	if (ikey)
		bms_free(ikey);
	
	if (! contains )
	{
		char *what = "UNIQUE";
		
		if ( primary )
			what = "PRIMARY KEY";
		
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s constraint must contain all columns in the "
						"partition key of relation \"%s\".", 
						what, RelationGetRelationName(rel)),
				 errhint("Include the partition key or create a part-wise UNIQUE index instead.")));
	}
}

/**
 * Does a partition node correspond to a leaf partition?
 */
static bool IsLeafPartitionNode(PartitionNode *p)
{
	Assert(p);

	/**
	 * If all of the rules have no children, this is a leaf partition.
	 */
	ListCell *lc = NULL;
	foreach (lc, p->rules)
	{
		PartitionRule *rule = (PartitionRule *) lfirst(lc);
		if (rule->children)
		{
			return false;
		}
	}

	/**
	 * If default partition has children then, this is not a leaf
	 */
	if (p->default_part
				&& p->default_part->children)
	{
		return false;
	}

	return true;
}

/*
 * Given a partition node, return all the associated rules, including the default partition rule if present
 */
static List*
get_partition_rules(PartitionNode *pn)
{
	Assert(pn);

	List *result = NIL;
	if (pn->default_part)
	{
		result = lappend(result, pn->default_part);
	}

	result = list_concat(result, pn->rules);

	return result;
}

/**
 * Given a partition node, return list of children. Should not be called on a leaf partition node.
 *
 * Input:
 * 	p - input partition node
 * Output:
 * 	List of partition nodes corresponding to its children across all rules.
 */
static List *PartitionChildren(PartitionNode *p)
{
	Assert(p);
	Assert(!IsLeafPartitionNode(p));

	List *result = NIL;

	ListCell *lc = NULL;
	foreach (lc, p->rules)
	{
		PartitionRule *rule = (PartitionRule *) lfirst(lc);

		if (rule->children)
		{
			result = lappend(result, rule->children);
		}
	}

	/**
	 * Also add default child
	 */
	if (p->default_part
			&& p->default_part->children)
	{
		result = lappend(result, p->default_part->children);
	}

	return result;
}

/*
 * selectPartitionMulti()
 *
 * Given an input partition node, values and nullness, and partition state,
 * find matching leaf partitions. This is similar to selectPartition() with one
 * big difference around nulls. If there is a null value corresponding to a partitioning attribute,
 * then all children are considered matches.
 *
 * Output:
 * 	leafPartitionOids - list of leaf partition oids, null if there are no matches
 */
List *
selectPartitionMulti(PartitionNode *partnode, Datum *values, bool *isnull,
				 TupleDesc tupdesc, PartitionAccessMethods *accessMethods)
{
	Assert(partnode);

	List *leafPartitionOids = NIL;

	List *inputList = list_make1(partnode);

	while (list_length(inputList) > 0)
	{
		List *levelOutput = NIL;

		ListCell *lc = NULL;
		foreach (lc, inputList)
		{
			PartitionNode *candidatePartNode = (PartitionNode *) lfirst(lc);
			bool foundNull = false;

			for (int i = 0; i < candidatePartNode->part->parnatts; i++)
			{
				AttrNumber attno = candidatePartNode->part->paratts[i];

				/**
				 * If corresponding value is null, then we should pick all of its
				 * children (or itself if it is a leaf partition)
				 */
				if (isnull[attno - 1])
				{
					foundNull = true;
					if (IsLeafPartitionNode(candidatePartNode))
					{
						/**
						 * Extract out Oids of all children
						 */
						leafPartitionOids = list_concat(leafPartitionOids, all_partition_relids(candidatePartNode));
					}
					else
					{
						levelOutput = list_concat(levelOutput, PartitionChildren(candidatePartNode));
					}
				}
			}

			/**
			 * If null was not found on the attribute, and if this is a leaf partition,
			 * then there will be an exact match. If it is not a leaf partition, then
			 * we have to find the right child to investigate.
			 */
			if (!foundNull)
			{
				if (IsLeafPartitionNode(candidatePartNode))
				{
					Oid matchOid = selectPartition1(candidatePartNode, values, isnull, tupdesc, accessMethods, NULL, NULL);
					if (matchOid != InvalidOid)
					{
						leafPartitionOids = lappend_oid(leafPartitionOids, matchOid);
					}
				}
				else
				{
					PartitionNode *childPartitionNode = NULL;
					selectPartition1(candidatePartNode, values, isnull, tupdesc, accessMethods, NULL, &childPartitionNode);
					if (childPartitionNode)
					{
						levelOutput = lappend(levelOutput, childPartitionNode);
					}
				}
			}

		}

		/**
		 * Start new level
		 */
		list_free(inputList);
		inputList = levelOutput;
	}

	return leafPartitionOids;
}

/*
 * Add a partition encoding clause for a subpartition template. We need to be
 * able to recall these for when we later add partitions which inherit the
 * subpartition template definition.
 */
static void
add_partition_encoding(Oid relid, Oid paroid, AttrNumber attnum, List *encoding)
{
	Datum		 partoptions;
	Datum		 values[Natts_pg_partition_encoding];
	bool		 nulls[Natts_pg_partition_encoding];
	HeapTuple	 tuple;
	cqContext	*pcqCtx;

	pcqCtx = 
			caql_beginscan(
					NULL,
					cql("INSERT INTO pg_partition_encoding ",
						NULL));

	Insist(attnum > 0);

	partoptions = transformRelOptions(PointerGetDatum(NULL),
									  encoding,
									  true,
									  false);

	MemSet(nulls, 0, sizeof(nulls));

	values[Anum_pg_partition_encoding_parencoid - 1] = ObjectIdGetDatum(paroid);
	values[Anum_pg_partition_encoding_parencattnum - 1] = Int16GetDatum(attnum);
	values[Anum_pg_partition_encoding_parencattoptions - 1] = partoptions;

	tuple = caql_form_tuple(pcqCtx, values, nulls);

	/* Insert tuple into the relation */	
	caql_insert(pcqCtx, tuple); /* implicit update of index as well */

	heap_freetuple(tuple);

	caql_endscan(pcqCtx);
}

static void
remove_partition_encoding_entry(Oid paroid, AttrNumber attnum)
{
	HeapTuple		tup;
	cqContext	   *pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_partition_encoding "
				" WHERE parencoid = :1 "
				" FOR UPDATE",
				ObjectIdGetDatum(paroid)));

	while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
	{
		if (attnum != InvalidAttrNumber)
		{
			Form_pg_partition_encoding ppe =
				(Form_pg_partition_encoding)GETSTRUCT(tup);

			if (ppe->parencattnum != attnum)
				continue;
		}
		caql_delete_current(pcqCtx);
	}

	caql_endscan(pcqCtx);
}

/*
 * Remove all trace of partition encoding for a relation. This is the DROP TABLE
 * case. May be called even if there's no entry for the partition.
 */
static void
remove_partition_encoding_by_key(Oid relid, AttrNumber attnum)
{
	HeapTuple		tup;
	cqContext	   *pcqCtx;
	cqContext	 	cqc;

	/* XXX XXX: not FOR UPDATE because update a child table... */

	pcqCtx = caql_beginscan(
			cqclr(&cqc), 
			cql("SELECT * FROM pg_partition "
				" WHERE parrelid = :1 ",
				ObjectIdGetDatum(relid)));

	/* XXX XXX: hmm, could we add 				
	   " AND paristemplate = :2 ",
	   ...
	   BoolGetDatum(true)));

	   Could caql use an index on parrelid even though paristemplate
	   not in index?
	*/

	while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
	{
		Form_pg_partition part = (Form_pg_partition)GETSTRUCT(tup);

		if (part->paristemplate)
			remove_partition_encoding_entry(HeapTupleGetOid(tup), attnum);
	}

	caql_endscan(pcqCtx);
}

void
RemovePartitionEncodingByRelid(Oid relid)
{
	remove_partition_encoding_by_key(relid, InvalidAttrNumber);
}

/*
 * Remove a partition encoding entry for a specific attribute number.
 * May be called when no such entry actually exists.
 */
void
RemovePartitionEncodingByRelidAttribute(Oid relid, AttrNumber attnum)
{
	remove_partition_encoding_by_key(relid, attnum);
}

/*
 * For all encoding clauses, create a pg_partition_encoding entry
 */
static void
add_template_encoding_clauses(Oid relid, Oid paroid, List *stenc)
{
	ListCell *lc;

	foreach(lc, stenc)
	{
		ColumnReferenceStorageDirective *c = lfirst(lc);
		AttrNumber attnum;

		/* 
		 * Don't store default clauses since we have no need of them
		 * when we add partitions later.
		 */
		if (c->deflt)
			continue;

		attnum = get_attnum(relid, strVal(c->column));

		Insist(attnum > 0);

		add_partition_encoding(relid, paroid, attnum,  c->encoding);
	}
}

Datum *
get_partition_encoding_attoptions(Relation rel, Oid paroid)
{
	HeapTuple		tup;
	cqContext	   *pcqCtx;
	cqContext	 	cqc;
	Relation		pgpeenc = heap_open(PartitionEncodingRelationId,
										RowExclusiveLock);
	Datum		   *opts;


	opts = palloc0(sizeof(Datum) * RelationGetNumberOfAttributes(rel));

	/* XXX XXX: should be FOR UPDATE ? why ? probably should be an
	 * AccessShare
	 */
	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), pgpeenc), 
			cql("SELECT * FROM pg_partition_encoding "
				" WHERE parencoid = :1 ",
				ObjectIdGetDatum(paroid)));

	while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
	{
		Datum paroptions;
		AttrNumber attnum;
		bool isnull;
		
		attnum = ((Form_pg_partition_encoding)GETSTRUCT(tup))->parencattnum;
		paroptions = heap_getattr(tup,
								  Anum_pg_partition_encoding_parencattoptions,
								  RelationGetDescr(pgpeenc),
								  &isnull);

		Insist(!isnull);
		Insist((attnum - 1) >= 0);

		opts[attnum - 1] = datumCopy(paroptions, false, -1);
	}

	caql_endscan(pcqCtx);
	heap_close(pgpeenc, RowExclusiveLock);

	return opts;
}

static List *
get_deparsed_partition_encodings(Oid relid, Oid paroid)
{
	int i;
	List *out = NIL;
	Relation rel = heap_open(relid, AccessShareLock);
	Datum *opts = get_partition_encoding_attoptions(rel, paroid);

	for (i = 0; i < RelationGetNumberOfAttributes(rel); i++)
	{
		if (opts[i] && !rel->rd_att->attrs[i]->attisdropped)
		{
			char *column;
			ColumnReferenceStorageDirective *c =
				makeNode(ColumnReferenceStorageDirective);

			c->encoding = untransformRelOptions(opts[i]);
			column = get_attname(relid, i + 1);
			c->column = makeString(column);
			out = lappend(out, c);
		}
	}

	heap_close(rel, AccessShareLock);

	return out;
}

/**
 * Function that returns a string representation of partition oids.
 *
 * elements: an array of datums, containing oids of partitions.
 * n: length of the elements array.
 *
 * Result is allocated in the current memory context.
 */
char*
DebugPartitionOid(Datum *elements, int n)
{

	StringInfoData str;
	initStringInfo(&str);
	appendStringInfo(&str, "{");
	for (int i=0; i<n; i++)
	{
		Oid o = DatumGetObjectId(elements[i]);
		appendStringInfo(&str, "%s, ", get_rel_name(o));
	}
	appendStringInfo(&str, "}");
	return str.data;
}

/**
 * Function that returns partition oids and number of partitions
 * from the partOidHash hash table.
 *
 * partOidHash: a hash table with partition oids
 * partOids: out parameter; returns a palloc'ed array of oid datums
 * partCount: out parameter; returns the length of palloc'ed oid array
 *
 * The partOids are palloc'ed in current memory context. Caller should
 * ensure timely pfree.
 */
void
GetSelectedPartitionOids(HTAB *partOidHash, Datum **partOids, long *partCount)
{
	if (NULL != partOidHash)
	{
		int i = 0;
		*partCount = hash_get_num_entries(partOidHash);
		*partOids = (Datum *) palloc(*partCount * sizeof(Datum));

		HASH_SEQ_STATUS pidStatus;
		hash_seq_init(&pidStatus, partOidHash);

		Oid *pid = NULL;
		while ((pid = (Oid *) hash_seq_search(&pidStatus)) != NULL)
		{
			Assert(i < *partCount);
			(*partOids)[i++] = ObjectIdGetDatum(*pid);
		}

		Assert(i == *partCount);
	}
	else
	{
		*partCount = 0;
		*partOids = (Datum *) palloc(*partCount * sizeof(Datum));
	}
}


/**
 * Prints the names of DPE selected partitions from a
 * HTAB (pidIndex) of partition oids.
 */
void
LogSelectedPartitionOids(HTAB *pidIndex)
{
	Datum *elements = NULL;
	long numPartitions = 0;
	GetSelectedPartitionOids(pidIndex, &elements, &numPartitions);
	Assert(NULL != elements);

	elog(LOG, "DPE matched partitions: %s", DebugPartitionOid(elements, numPartitions));
	pfree(elements);
}

/*
 * findPartitionMetadataEntry
 *   Find PartitionMetadata object for a given partition oid from a list.
 *
 * Input arguments:
 * partsMetadata: list of PartitionMetadata
 * partOid: Part Oid
 * partsAndRules: output parameter for matched PartitionNode
 * accessMethods: output parameter for PartitionAccessMethods
 *
 */
void
findPartitionMetadataEntry(List *partsMetadata, Oid partOid, PartitionNode **partsAndRules, PartitionAccessMethods **accessMethods)
{
	ListCell *lc = NULL;
	foreach (lc, partsMetadata)
	{
		PartitionMetadata *metadata = (PartitionMetadata *)lfirst(lc);
		*partsAndRules = findPartitionNodeEntry(metadata->partsAndRules, partOid);

		if (NULL != *partsAndRules)
		{
			// accessMethods define the lookup access methods for partitions, one for each level
			*accessMethods = metadata->accessMethods;
			return;
		}
	}
}

/*
 * findPartitionNodeEntry
 *   Find PartitionNode object for a given partition oid
 *
 * Input arguments:
 * partsMetadata: list of PartitionMetadata
 * partOid: Part Oid
 * return: matched PartitionNode
 */
static PartitionNode *
findPartitionNodeEntry(PartitionNode *partitionNode, Oid partOid)
{
	if (NULL == partitionNode)
	{
		return NULL;
	}

	Assert(NULL != partitionNode->part);
	if (partitionNode->part->parrelid == partOid)
	{
		return partitionNode;
	}

	/*
	 * check recursively in child parts in case we have the oid of an
	 * intermediate node
	 */
	PartitionNode *childNode = NULL;
	ListCell *lcChild = NULL;
	foreach (lcChild, partitionNode->rules)
	{
		PartitionRule *childRule = (PartitionRule *) lfirst(lcChild);
		childNode = findPartitionNodeEntry(childRule->children, partOid);
		if (NULL != childNode)
		{
			return childNode;
		}
	}

	/*
	 * check recursively in the default part, if any
	 */
	if (NULL != partitionNode->default_part)
	{
		childNode = findPartitionNodeEntry(partitionNode->default_part->children, partOid);
	}

	return childNode;
}

/*
 * createValueArrays
 *   Create an Datum/bool array that will be used to populate partition key value.
 *
 * The size of this array is based on the attribute number of the partition key.
 */
void
createValueArrays(int keyAttno, Datum **values, bool **isnull)
{
	*values = palloc0(keyAttno * sizeof(Datum));
	*isnull = palloc(keyAttno * sizeof(bool));
	Assert (NULL != values);
	Assert (NULL != isnull);

	MemSet(*isnull, true, keyAttno * sizeof(bool));
}

/*
 * freeValueArrays
 *    Free Datum/bool array.
 */
void
freeValueArrays(Datum *values, bool *isnull)
{
	Assert (NULL != values);
	Assert (NULL != isnull);
	pfree(values);
	pfree(isnull);
}
