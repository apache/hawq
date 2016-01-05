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
 * relation.h
 *	  Definitions for planner's internal data structures.
 *
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/nodes/relation.h,v 1.128.2.4 2007/08/31 01:44:14 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELATION_H
#define RELATION_H

#include "access/sdir.h"
#include "nodes/bitmapset.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "executor/execdesc.h"
#include "storage/block.h"
#include "nodes/plannerconfig.h"
#include "cdb/cdbpathlocus.h"

/*
 * Relids
 *		Set of relation identifiers (indexes into the rangetable).
 */
typedef Bitmapset *Relids;

/*
 * Estimated costs
 */
typedef double EstimatedBytes;  /* an estimated number of bytes */

/*
 * When looking for a "cheapest path", this enum specifies whether we want
 * cheapest startup cost or cheapest total cost.
 */
typedef enum CostSelector
{
	STARTUP_COST, TOTAL_COST
} CostSelector;

/*
 * The cost estimate produced by cost_qual_eval() includes both a one-time
 * (startup) cost, and a per-tuple cost.
 */
typedef struct QualCost
{
	Cost		startup;		/* one-time cost */
	Cost		per_tuple;		/* per-evaluation cost */
} QualCost;


/*
 * Context for apply shareinput processing during planning.  We could fold
 * this into PlannerGlobal, but this encapsulates it nicely.
 */
typedef struct ApplyShareInputContext
{
	List *sharedNodes;
	List *sliceMarks;
	List *motStack;
	List *qdShares;
	List *qdSlices;
	List *planNodes;
	int nextPlanId;
} ApplyShareInputContext;


/*----------
 * PlannerGlobal
 *		Global information for planning/optimization
 *
 * PlannerGlobal holds state for an entire planner invocation; this state
 * is shared across all levels of sub-Queries that exist in the command being
 * planned.
 *----------
 */
typedef struct PlannerGlobal
{
	NodeTag		type;

	ParamListInfo boundParams;	/* Param values provided to planner() */

	List	   *paramlist;		/* to keep track of cross-level Params */

	List	   *subplans;		/* Plans for SubPlan nodes */

	List	   *subrtables;		/* Rangetables for SubPlan nodes */

	Bitmapset  *rewindPlanIDs;	/* indices of subplans that require REWIND */

	List	   *finalrtable;	/* "flat" rangetable for executor */

	List	   *relationOids;	/* OIDs of relations the plan depends on */

	List	   *invalItems;		/* other dependencies, as PlanInvalItems */

	bool		transientPlan;	/* redo plan when TransactionXmin changes? */
	
	ApplyShareInputContext share;	/* workspace for GPDB plan sharing */
	
	struct QueryResource *resource;	/* the resource for the plan to be optimized and executed */

	List* relsType; /* relation and relation runtime type list. for hash table may convert to random table in runtime*/
} PlannerGlobal;

/*
 * CtePlanInfo
 *    Information for subplans that are associated with a CTE.
 */
typedef struct CtePlanInfo
{
	/*
	 * List of subplans that are associated with a CTE.
	 * If a CTE is referenced once, this list contains one element.
	 * If a CTE is referenced multiple times, this list contains multiple plans,
	 * each of which has ShareNode on top.
	 */
	List *subplans;

	/*
	 * The rtable corresponding to the subplan.
	 */
	List *subrtable;

	/*
	 * The pathkeys corresponding to the subplan.
	 */
	List *pathkeys;

	/*
	 * The next plan id in subplans that should be used (starting with 0).
	 */
	int nextPlanId;

	/*
	 * Number of non-shared plans generated for this cte.
	 */
	int numNonSharedPlans;
} CtePlanInfo;

/*----------
 * PlannerInfo
 *		Per-query information for planning/optimization
 *
 * This struct is conventionally called "root" in all the planner routines.
 * It holds links to all of the planner's working state, in addition to the
 * original Query.	Note that at present the planner extensively modifies
 * the passed-in Query data structure; someday that should stop.
 *----------
 */
typedef struct PlannerInfo
{
	NodeTag		type;

	Query	   *parse;			/* the Query being planned */

	PlannerGlobal *glob;		/* global info for current planner run */

	Index		query_level;	/* 1 at the outermost Query */

	struct PlannerInfo *parent_root; /* NULL at outermost Query */

	/*
	 * simple_rel_array holds pointers to "base rels" and "other rels" (see
	 * comments for RelOptInfo for more info).	It is indexed by rangetable
	 * index (so entry 0 is always wasted).  Entries can be NULL when an RTE
	 * does not correspond to a base relation, such as a join RTE or an
	 * unreferenced view RTE; or if the RelOptInfo hasn't been made yet.
	 */
	struct RelOptInfo **simple_rel_array;		/* All 1-relation RelOptInfos */
	int			simple_rel_array_size;	/* allocated size of array */

	/*
	 * simple_rte_array is the same length as simple_rel_array and holds
	 * pointers to the associated rangetable entries.  This lets us avoid
	 * rt_fetch(), which can be a bit slow once large inheritance sets have
	 * been expanded.
	 */
	RangeTblEntry **simple_rte_array;	/* rangetable as an array */

	/*
	 * join_rel_list is a list of all join-relation RelOptInfos we have
	 * considered in this planning run.  For small problems we just scan the
	 * list to do lookups, but when there are many join relations we build a
	 * hash table for faster lookups.  The hash table is present and valid
	 * when join_rel_hash is not NULL.	Note that we still maintain the list
	 * even when using the hash table for lookups; this simplifies life for
	 * GEQO.
	 */
	List	   *join_rel_list;	/* list of join-relation RelOptInfos */
	struct HTAB *join_rel_hash; /* optional hashtable for join relations */
	
	/* Note:  Prior to 3.4, these fields were in the Query node.  Now they
	 *        are managed here for later installation in PlannedStmt.
	 */
	List	   *resultRelations;	/* integer list of RT indexes, or NIL */
	PartitionNode *result_partitions;	
	List	   *returningLists; /* list of lists of TargetEntry, or NIL */
	List	   *result_aosegnos;
	
	List	   *init_plans;		/* init SubPlans for query */

	List       *list_cteplaninfo; /* list of CtePlannerInfo, one for each CTE */

	List	   *equi_key_list;	/* list of lists of equijoined PathKeyItems */

    /* Jointree result is a subset of the cross product of these relids... */
    Relids      currlevel_relids;   /* CDB: all relids of current query level,
                                     * omitting any pulled-up subquery relids */

    /*
     * Outer join info
     */
    List	   *left_join_clauses;		/* list of RestrictInfos for outer
										 * join clauses w/nonnullable var on
										 * left */

	List	   *right_join_clauses;		/* list of RestrictInfos for outer
										 * join clauses w/nonnullable var on
										 * right */

	List	   *full_join_clauses;		/* list of RestrictInfos for full
										 * outer join clauses */

	List	   *oj_info_list;	/* list of OuterJoinInfos */

	List	   *in_info_list;	/* list of InClauseInfos */

	List	   *append_rel_list;	/* list of AppendRelInfos */

	List	   *query_pathkeys; /* desired pathkeys for query_planner(), and
								 * actual pathkeys afterwards */

	List	   *group_pathkeys; /* groupClause pathkeys, if any */
	List	   *sort_pathkeys;	/* sortClause pathkeys, if any */

	MemoryContext planner_cxt;	/* context holding PlannerInfo */

	double		total_table_pages;		/* # of pages in all tables of query */

	double		tuple_fraction; /* tuple_fraction passed to query_planner */

	bool		hasJoinRTEs;	/* true if any RTEs are RTE_JOIN kind */
	bool		hasOuterJoins;	/* true if any RTEs are outer joins */
	bool		hasHavingQual;	/* true if havingQual was non-null */
	bool		hasPseudoConstantQuals; /* true if any RestrictInfo has
										 * pseudoconstant = true */

	/* At the end to avoid breaking existing 8.2 add-ons */
	List	   *initial_rels;	/* RelOptInfos we are now trying to join */
	
	PlannerConfig *config;		/* Planner configuration */

} PlannerInfo;


/*
 * In places where it's known that simple_rte_array[] must have been prepared
 * already, we just index into it to fetch RTEs.  In code that might be
 * executed before or after entering query_planner(), use this macro.
 */
#define planner_rt_fetch(rti, root) \
	((root)->simple_rte_array ? (root)->simple_rte_array[rti] : \
	 rt_fetch(rti, (root)->parse->rtable))


/* 
 * Fetch the Plan associated with a SubPlan node during planning. 
 */
static inline struct Plan *planner_subplan_get_plan(struct PlannerInfo *root, SubPlan *subplan) 
{
	return (Plan *) list_nth(root->glob->subplans, subplan->plan_id - 1);
}

/**
 * Fetch the rtable list for a subplan
 */
static inline struct List *planner_subplan_get_rtable(struct PlannerInfo *root, SubPlan *subplan)
{
	return (List *) list_nth(root->glob->subrtables, subplan->plan_id - 1);
}

/* 
 * Rewrite the Plan associated with a SubPlan node during planning. 
 */
static inline void planner_subplan_put_plan(struct PlannerInfo *root, SubPlan *subplan, Plan *plan) 
{
	ListCell *cell = list_nth_cell(root->glob->subplans, subplan->plan_id-1);
	cell->data.ptr_value = plan;
}


/*----------
 * RelOptInfo
 *		Per-relation information for planning/optimization
 *
 * For planning purposes, a "base rel" is either a plain relation (a table)
 * or the output of a sub-SELECT or function that appears in the range table.
 * In either case it is uniquely identified by an RT index.  A "joinrel"
 * is the joining of two or more base rels.  A joinrel is identified by
 * the set of RT indexes for its component baserels.  We create RelOptInfo
 * nodes for each baserel and joinrel, and store them in the PlannerInfo's
 * simple_rel_array and join_rel_list respectively.
 *
 * Note that there is only one joinrel for any given set of component
 * baserels, no matter what order we assemble them in; so an unordered
 * set is the right datatype to identify it with.
 *
 * We also have "other rels", which are like base rels in that they refer to
 * single RT indexes; but they are not part of the join tree, and are given
 * a different RelOptKind to identify them.
 *
 * Currently the only kind of otherrels are those made for member relations
 * of an "append relation", that is an inheritance set or UNION ALL subquery.
 * An append relation has a parent RTE that is a base rel, which represents
 * the entire append relation.	The member RTEs are otherrels.	The parent
 * is present in the query join tree but the members are not.  The member
 * RTEs and otherrels are used to plan the scans of the individual tables or
 * subqueries of the append set; then the parent baserel is given an Append
 * plan comprising the best plans for the individual member rels.  (See
 * comments for AppendRelInfo for more information.)
 *
 * At one time we also made otherrels to represent join RTEs, for use in
 * handling join alias Vars.  Currently this is not needed because all join
 * alias Vars are expanded to non-aliased form during preprocess_expression.
 *
 * Parts of this data structure are specific to various scan and join
 * mechanisms.	It didn't seem worth creating new node types for them.
 *
 *		relids - Set of base-relation identifiers; it is a base relation
 *				if there is just one, a join relation if more than one
 *		rows - estimated number of tuples in the relation after restriction
 *			   clauses have been applied (ie, output rows of a plan for it)
 *		width - avg. number of bytes per tuple in the relation after the
 *				appropriate projections have been done (ie, output width)
 *		reltargetlist - List of Var nodes for the attributes we need to
 *						output from this relation (in no particular order)
 *						NOTE: in a child relation, may contain RowExprs
 *		pathlist - List of Path nodes, one for each potentially useful
 *				   method of generating the relation
 *		cheapest_startup_path - the pathlist member with lowest startup cost
 *								(regardless of its ordering)
 *		cheapest_total_path - the pathlist member with lowest total cost
 *							  (regardless of its ordering)
 *		cheapest_unique_path - for caching cheapest path to produce unique
 *							   (no duplicates) output from relation
 *
 * If the relation is a base relation it will have these fields set:
 *
 *		relid - RTE index (this is redundant with the relids field, but
 *				is provided for convenience of access)
 *		rtekind - distinguishes plain relation, subquery, or function RTE
 *		min_attr, max_attr - range of valid AttrNumbers for rel
 *		attr_needed - array of bitmapsets indicating the highest joinrel
 *				in which each attribute is needed; if bit 0 is set then
 *				the attribute is needed as part of final targetlist
 *		attr_widths - cache space for per-attribute width estimates;
 *					  zero means not computed yet
 *		indexlist - list of IndexOptInfo nodes for relation's indexes
 *					(always NIL if it's not a table)
 *		pages - number of disk pages in relation (zero if not a table)
 *		tuples - number of tuples in relation (not considering restrictions)
 *		subplan - plan for subquery (NULL if it's not a subquery)
 *		subrtable - rangetable for subquery (NIL if it's not a subquery)
 *
 *		Note: for a subquery, tuples and subplan are not set immediately
 *		upon creation of the RelOptInfo object; they are filled in when
 *		set_base_rel_pathlist processes the object.
 *
 *		For otherrels that are appendrel members, these fields are filled
 *		in just as for a baserel.
 *
 * The presence of the remaining fields depends on the restrictions
 * and joins that the relation participates in:
 *
 *		baserestrictinfo - List of RestrictInfo nodes, containing info about
 *					each non-join qualification clause in which this relation
 *					participates (only used for base rels)
 *		baserestrictcost - Estimated cost of evaluating the baserestrictinfo
 *					clauses at a single tuple (only used for base rels)
 *		joininfo  - List of RestrictInfo nodes, containing info about each
 *					join clause in which this relation participates
 *		index_outer_relids - only used for base rels; set of outer relids
 *					that participate in indexable joinclauses for this rel
 *		index_inner_paths - only used for base rels; list of InnerIndexscanInfo
 *					nodes showing best indexpaths for various subsets of
 *					index_outer_relids.
 *
 * Note: Keeping a restrictinfo list in the RelOptInfo is useful only for
 * base rels, because for a join rel the set of clauses that are treated as
 * restrict clauses varies depending on which sub-relations we choose to join.
 * (For example, in a 3-base-rel join, a clause relating rels 1 and 2 must be
 * treated as a restrictclause if we join {1} and {2 3} to make {1 2 3}; but
 * if we join {1 2} and {3} then that clause will be a restrictclause in {1 2}
 * and should not be processed again at the level of {1 2 3}.)	Therefore,
 * the restrictinfo list in the join case appears in individual JoinPaths
 * (field joinrestrictinfo), not in the parent relation.  But it's OK for
 * the RelOptInfo to store the joininfo list, because that is the same
 * for a given rel no matter how we form it.
 *
 * We store baserestrictcost in the RelOptInfo (for base relations) because
 * we know we will need it at least once (to price the sequential scan)
 * and may need it multiple times to price index scans.
 *----------
 */
typedef enum RelOptKind
{
	RELOPT_BASEREL,
	RELOPT_JOINREL,
	RELOPT_OTHER_MEMBER_REL
} RelOptKind;

typedef struct RelOptInfo
{
	NodeTag		type;

	RelOptKind	reloptkind;

	/* all relations included in this RelOptInfo */
	Relids		relids;			/* set of base relids (rangetable indexes) */

	/* size estimates generated by planner */
	double		rows;			/* estimated number of result tuples */
	int			width;			/* estimated avg width of result tuples */
    bool        onerow;         /* true => rel is inherently 1 row or empty */

	/* materialization information */
	List	   *reltargetlist;	/* needed Vars */
	List	   *pathlist;		/* Path structures */
	struct Path *cheapest_startup_path;
	struct Path *cheapest_total_path;
	struct Path *cheapest_unique_path;
    struct CdbRelDedupInfo *dedup_info; /* subquery dup removal info, or NULL */

	/* information about a base rel (not set for join rels!) */
	Index		relid;
	RTEKind		rtekind;		/* RELATION, SUBQUERY, or FUNCTION */
	AttrNumber	min_attr;		/* smallest attrno of rel (often <0) */
	AttrNumber	max_attr;		/* largest attrno of rel */
	Relids	   *attr_needed;	/* array indexed [min_attr .. max_attr] */
	int32	   *attr_widths;	/* array indexed [min_attr .. max_attr] */
	List	   *indexlist;
	BlockNumber pages;
	double		tuples;
    struct GpPolicy   *cdbpolicy;      /* distribution of stored tuples */
    bool        cdb_default_stats_used; /* true if ANALYZE needed */
	struct Plan *subplan;		/* if subquery */
	List	   *subrtable;		/* if subquery */

	/* used by external scan */
	List		*locationlist;
	char		*execcommand;
	char		fmttype;
	char		*fmtopts;
	int32		rejectlimit;
	char		rejectlimittype;
	Oid			fmterrtbl;
	int32		ext_encoding;
	bool		isrescannable; /* true for ext tables false for ext web tables */
	bool		writable;	   /* true for writable, false for readable ext tables*/
	
	/* used by various scans and joins: */
	List	   *baserestrictinfo;		/* RestrictInfo structures (if base
										 * rel) */
	QualCost	baserestrictcost;		/* cost of evaluating the above */
	List	   *joininfo;		/* RestrictInfo structures for join clauses
								 * involving this rel */

	/* cached info about inner indexscan paths for relation: */
	Relids		index_outer_relids;		/* other relids in indexable join
										 * clauses */
	List	   *index_inner_paths;		/* InnerIndexscanInfo nodes */

	/*
	 * Inner indexscans are not in the main pathlist because they are not
	 * usable except in specific join contexts.  We use the index_inner_paths
	 * list just to avoid recomputing the best inner indexscan repeatedly for
	 * similar outer relations.  See comments for InnerIndexscanInfo.
	 */
} RelOptInfo;

/*
 * IndexOptInfo
 *		Per-index information for planning/optimization
 *
 *		Prior to Postgres 7.0, RelOptInfo was used to describe both relations
 *		and indexes, but that created confusion without actually doing anything
 *		useful.  So now we have a separate IndexOptInfo struct for indexes.
 *
 *		classlist[], indexkeys[], and ordering[] have ncolumns entries.
 *		Zeroes in the indexkeys[] array indicate index columns that are
 *		expressions; there is one element in indexprs for each such column.
 *
 *		Note: for historical reasons, the classlist and ordering arrays have
 *		an extra entry that is always zero.  Some code scans until it sees a
 *		zero entry, rather than looking at ncolumns.
 *
 *		The indexprs and indpred expressions have been run through
 *		prepqual.c and eval_const_expressions() for ease of matching to
 *		WHERE clauses.	indpred is in implicit-AND form.
 */

typedef struct IndexOptInfo
{
	NodeTag		type;

	Oid			indexoid;		/* OID of the index relation */
	RelOptInfo *rel;			/* back-link to index's table */

	/* statistics from pg_class */
	BlockNumber pages;			/* number of disk pages in index */
	double		tuples;			/* number of index tuples in index */

	/* index descriptor information */
	int			ncolumns;		/* number of columns in index */
	Oid		   *classlist;		/* OIDs of operator classes for columns */
	int		   *indexkeys;		/* column numbers of index's keys, or 0 */
	Oid		   *ordering;		/* OIDs of sort operators for each column */
	Oid			relam;			/* OID of the access method (in pg_am) */

	RegProcedure amcostestimate;	/* OID of the access method's cost fcn */

	List	   *indexprs;		/* expressions for non-simple index columns */
	List	   *indpred;		/* predicate if a partial index, else NIL */

	bool		predOK;			/* true if predicate matches query */
	bool		unique;			/* true if a unique index */
	bool		amoptionalkey;	/* can query omit key for the first column? */
    bool        cdb_default_stats_used; /* true if ANALYZE needed */
    int         num_leading_eq; /* CDB: always 0, except amcostestimate proc may
                                 * set it briefly; it is transferred forthwith
                                 * to the IndexPath (q.v.), then reset. Kludge.
                                 */
} IndexOptInfo;


/*
 * CdbRelColumnInfo
 *
 * Describes a synthetic column to be added to a baserel's targetlist.
 * The pseudocols field of the RTE points to a List of CdbRelColumnInfo.
 */
typedef struct CdbRelColumnInfo
{
	NodeTag		type;                   /* T_CdbRelColumnInfo */

    AttrNumber  pseudoattno;            /* FirstLowInvalidHeapAttributeNumber
                                         *  minus the 0-based position of the
                                         *  CdbRelColumnInfo node in the
                                         *  rte->pseudocols list
                                         */
    AttrNumber  targetresno;            /* 1-based position of the pseudo
                                         *  column in the rel's targetlist
                                         */
    Expr       *defexpr;                /* expr to be evaluated in targetlist */
	Relids	    where_needed;           /* set of relids whose quals use col */
	int32	    attr_width;             /* expected #bytes for column value */
    char        colname[NAMEDATALEN+1]; /* name for EXPLAIN */
} CdbRelColumnInfo;


/*
 * CdbRelDedupInfo
 *
 * One of these hangs off each RelOptInfo entry whose paths might need
 * special treatment for duplicate suppression for flattened subqueries.
 */
typedef struct CdbRelDedupInfo
{
	NodeTag		type;                   /* T_CdbRelDedupInfo */

    Relids      prejoin_dedup_subqrelids;
                                        /* relids of subqueries' own (righthand)
                                         * tables for those subqueries that have
                                         * all of their own tables present in
                                         * this rel.
                                         */
    Relids      spent_subqrelids;       /* set of subquery relids that are
                                         * inputs to this rel but won't be
                                         * referenced again downstream (i.e.,
                                         * are not mentioned in reltargetlist).
                                         * Can use JOIN_IN when inner relids
                                         * are a subset of spent_subq_relids.
                                         */
    bool        try_postjoin_dedup;     /* true => this rel includes all inputs
                                         * required (including lefthand and
                                         * correlating inputs as well as the
                                         * subqueries' own tables) to fully
                                         * evaluate the subqueries indicated by
                                         * prejoin_dedup_subqrelids.
                                         */
    bool        no_more_subqueries;     /* true => this rel includes all inputs
                                         * required for all flattened subqueries
                                         * of the current query level.
                                         */
    struct InClauseInfo   *join_unique_ininfo;
                                        /* uncorrelated "= ANY" subquery with
                                         * exactly the same relids as this rel.
                                         */
    List       *later_dedup_pathlist;   /* List of Path.  Contains paths which
                                         * yield this rel but lack duplicate
                                         * suppression which is to occur later.
                                         * Their subq_complete flags are false.
                                         */
	struct Path *cheapest_startup_path; /* cheapest of later_dedup_pathlist */
	struct Path *cheapest_total_path;   /* cheapest of later_dedup_pathlist */
} CdbRelDedupInfo;


/*
 * PathKeys
 *
 *	The sort ordering of a path is represented by a list of sublists of
 *	PathKeyItem nodes.	An empty list implies no known ordering.  Otherwise
 *	the first sublist represents the primary sort key, the second the
 *	first secondary sort key, etc.	Each sublist contains one or more
 *	PathKeyItem nodes, each of which can be taken as the attribute that
 *	appears at that sort position.	(See optimizer/README for more
 *	information.)
 */

typedef struct PathKeyItem
{
	NodeTag		type;

	Node	   *key;			/* the item that is ordered */
	Oid			sortop;			/* the ordering operator ('<' op) */
    Relids      cdb_key_relids; /* set of relids referenced by key expr */
    int         cdb_num_relids; /* num of relids referenced by key expr */

	/*
	 * key typically points to a Var node, ie a relation attribute, but it can
	 * also point to an arbitrary expression representing the value indexed by
	 * an index expression.
	 */
} PathKeyItem;

/*
 * CdbPathKeyItemIsConstant
 *      is true if there is no Var of the current level in the expr
 *      referenced by a given PathKeyItem.
 */
#define CdbPathKeyItemIsConstant(_pathkeyitem)  \
    ((_pathkeyitem)->cdb_num_relids == 0)

/*
 * CdbPathkeyEqualsConstant
 *      is true if there is a constant expr in a given set of
 *      equijoin-equivalent exprs represented by a pathkey
 *      (i.e. a List of PathKeyItem).  If there is a constant
 *      expr, it will be at the head of the list.
 */
#define CdbPathkeyEqualsConstant(_pathkey)  \
    ( (_pathkey) != NIL &&                  \
      CdbPathKeyItemIsConstant((PathKeyItem *)linitial(_pathkey)) )


/*
 * Type "Path" is used as-is for sequential-scan paths.  For other
 * path types it is the first component of a larger struct.
 *
 * Note: "pathtype" is the NodeTag of the Plan node we could build from this
 * Path.  It is partially redundant with the Path's NodeTag, but allows us
 * to use the same Path type for multiple Plan types where there is no need
 * to distinguish the Plan type during path processing.
 */

typedef struct Path
{
	NodeTag		type;

	NodeTag		pathtype;		/* tag identifying scan/join method */

	RelOptInfo *parent;			/* the relation this path can build */

	/* estimated execution costs for path (see costsize.c for more info) */
	Cost		startup_cost;	/* cost expended before fetching any tuples */
	Cost		total_cost;		/* total cost (assuming all tuples fetched) */

    EstimatedBytes  memory;     /* executor RAM needed for Path + kids */

    CdbPathLocus    locus;      /* distribution of the result tuples */

    bool        motionHazard;   /* true => path contains a CdbMotion operator
                                 *    without a slackening operator above it */
	
	bool		rescannable;    /* CDB: true => path can accept ExecRescan call
                                 */
    bool        subq_complete;  /* CDB: true => there is no flattened subquery
                                 *  having all of its tables present in this rel
                                 *  but still needing duplicate suppression.
                                 *  Set by add_path().
                                 */
	List	   *pathkeys;		/* sort ordering of path's output */
	/* pathkeys is a List of Lists of PathKeyItem nodes; see above */
} Path;

/* 
 * AppendOnlyPath is used for append-only table scans. 
 */
typedef struct AppendOnlyPath
{
	Path		path;
	
	/* for now it's pretty plain.. */
} AppendOnlyPath;

/*
 * ParquetPath is used for parquet table scans.
 */
typedef struct ParquetPath
{
	Path		path;

	/* for now it's pretty plain.. */
} ParquetPath;

/* 
 * ExternalPath is used for external table scans. 
 */
typedef struct ExternalPath
{
	Path		path;
	
	/* for now it's pretty plain.. */
} ExternalPath;


/*----------
 * IndexPath represents an index scan over a single index.
 *
 * 'indexinfo' is the index to be scanned.
 *
 * 'indexclauses' is a list of index qualification clauses, with implicit
 * AND semantics across the list.  Each clause is a RestrictInfo node from
 * the query's WHERE or JOIN conditions.
 *
 * 'indexquals' has the same structure as 'indexclauses', but it contains
 * the actual indexqual conditions that can be used with the index.
 * In simple cases this is identical to 'indexclauses', but when special
 * indexable operators appear in 'indexclauses', they are replaced by the
 * derived indexscannable conditions in 'indexquals'.
 *
 * 'isjoininner' is TRUE if the path is a nestloop inner scan (that is,
 * some of the index conditions are join rather than restriction clauses).
 * Note that the path costs will be calculated differently from a plain
 * indexscan in this case, and in addition there's a special 'rows' value
 * different from the parent RelOptInfo's (see below).
 *
 * 'indexscandir' is one of:
 *		ForwardScanDirection: forward scan of an ordered index
 *		BackwardScanDirection: backward scan of an ordered index
 *		NoMovementScanDirection: scan of an unordered index, or don't care
 * (The executor doesn't care whether it gets ForwardScanDirection or
 * NoMovementScanDirection for an indexscan, but the planner wants to
 * distinguish ordered from unordered indexes for building pathkeys.)
 *
 * 'indextotalcost' and 'indexselectivity' are saved in the IndexPath so that
 * we need not recompute them when considering using the same index in a
 * bitmap index/heap scan (see BitmapHeapPath).  The costs of the IndexPath
 * itself represent the costs of an IndexScan plan type.
 *
 * 'rows' is the estimated result tuple count for the indexscan.  This
 * is the same as path.parent->rows for a simple indexscan, but it is
 * different for a nestloop inner scan, because the additional indexquals
 * coming from join clauses make the scan more selective than the parent
 * rel's restrict clauses alone would do.
 *----------
 */
typedef struct IndexPath
{
	Path		path;
	IndexOptInfo *indexinfo;
	List	   *indexclauses;
	List	   *indexquals;
	bool		isjoininner;
	ScanDirection indexscandir;
	Cost		indextotalcost;
	Selectivity indexselectivity;
	double		rows;			/* estimated number of result tuples */
    int         num_leading_eq; /* CDB: number of leading key columns matched by
                                 * equality predicates in indexquals.  If equal
                                 * to indexinfo->ncolumns, at most one distinct
                                 * value of the index key can satisfy the quals.
                                 * Further if the index is unique, we can assume
                                 * at most one visible row satisfies the quals.
                                 */
} IndexPath;

/*
 * BitmapHeapPath represents one or more indexscans that generate TID bitmaps
 * instead of directly accessing the heap, followed by AND/OR combinations
 * to produce a single bitmap, followed by a heap scan that uses the bitmap.
 * Note that the output is always considered unordered, since it will come
 * out in physical heap order no matter what the underlying indexes did.
 *
 * The individual indexscans are represented by IndexPath nodes, and any
 * logic on top of them is represented by a tree of BitmapAndPath and
 * BitmapOrPath nodes.	Notice that we can use the same IndexPath node both
 * to represent a regular IndexScan plan, and as the child of a BitmapHeapPath
 * that represents scanning the same index using a BitmapIndexScan.  The
 * startup_cost and total_cost figures of an IndexPath always represent the
 * costs to use it as a regular IndexScan.	The costs of a BitmapIndexScan
 * can be computed using the IndexPath's indextotalcost and indexselectivity.
 *
 * BitmapHeapPaths can be nestloop inner indexscans.  The isjoininner and
 * rows fields serve the same purpose as for plain IndexPaths.
 */
typedef struct BitmapHeapPath
{
	Path		path;
	Path	   *bitmapqual;		/* IndexPath, BitmapAndPath, BitmapOrPath */
	bool		isjoininner;	/* T if it's a nestloop inner scan */
	double		rows;			/* estimated number of result tuples */
} BitmapHeapPath;

/*
 * NOTE: This is a copy of the BitmapHeapPath structure.
 */
typedef struct BitmapAppendOnlyPath
{
	Path		path;
	Path	   *bitmapqual;		/* IndexPath, BitmapAndPath, BitmapOrPath */
	bool		isjoininner;	/* T if it's a nestloop inner scan */
	double		rows;			/* estimated number of result tuples */
	bool        isAORow;        /* If this is for AO Row tables */
} BitmapAppendOnlyPath;

typedef struct BitmapTableScanPath
{
	Path		path;
	Path	   *bitmapqual;		/* IndexPath, BitmapAndPath, BitmapOrPath */
	bool		isjoininner;	/* T if it's a nestloop inner scan */
	double		rows;			/* estimated number of result tuples */
} BitmapTableScanPath;

/*
 * BitmapAndPath represents a BitmapAnd plan node; it can only appear as
 * part of the substructure of a BitmapHeapPath.  The Path structure is
 * a bit more heavyweight than we really need for this, but for simplicity
 * we make it a derivative of Path anyway.
 */
typedef struct BitmapAndPath
{
	Path		path;
	List	   *bitmapquals;	/* IndexPaths and BitmapOrPaths */
	Selectivity bitmapselectivity;
} BitmapAndPath;

/*
 * BitmapOrPath represents a BitmapOr plan node; it can only appear as
 * part of the substructure of a BitmapHeapPath.  The Path structure is
 * a bit more heavyweight than we really need for this, but for simplicity
 * we make it a derivative of Path anyway.
 */
typedef struct BitmapOrPath
{
	Path		path;
	List	   *bitmapquals;	/* IndexPaths and BitmapAndPaths */
	Selectivity bitmapselectivity;
} BitmapOrPath;

/*
 * TidPath represents a scan by TID
 *
 * tidquals is an implicitly OR'ed list of qual expressions of the form
 * "CTID = pseudoconstant" or "CTID = ANY(pseudoconstant_array)".
 * Note they are bare expressions, not RestrictInfos.
 */
typedef struct TidPath
{
	Path		path;
	List	   *tidquals;		/* qual(s) involving CTID = something */
} TidPath;

/*
 * CdbMotionPath represents transmission of the child Path results
 * from a set of sending processes to a set of receiving processes.
 */
typedef struct CdbMotionPath
{
	Path		path;
    Path	   *subpath;
} CdbMotionPath;

/*
 * AppendPath represents an Append plan, ie, successive execution of
 * several member plans.
 *
 * Note: it is possible for "subpaths" to contain only one, or even no,
 * elements.  These cases are optimized during create_append_plan.
 */
typedef struct AppendPath
{
	Path		path;
	List	   *subpaths;		/* list of component Paths */
} AppendPath;

/*
 * ResultPath represents use of a Result plan node to compute a variable-free
 * targetlist with no underlying tables (a "SELECT expressions" query).
 * The query could have a WHERE clause, too, represented by "quals".
 *
 * Note that quals is a list of bare clauses, not RestrictInfos.
 */
typedef struct ResultPath
{
	Path		path;
	Path	   *subpath;
	List	   *quals;
} ResultPath;

/*
 * MaterialPath represents use of a Material plan node, i.e., caching of
 * the output of its subpath.  This is used when the subpath is expensive
 * and needs to be scanned repeatedly, or when we need mark/restore ability
 * and the subpath doesn't have it.
 */
typedef struct MaterialPath
{
	Path		path;
	Path	   *subpath;
    bool        cdb_strict;     /* true  => consume and store all input tuples
                                 *            before yielding output tuples
                                 * false => memoize tuples as they stream thru
                                 */
} MaterialPath;

/*
 * UniquePath represents elimination of distinct rows from the output of
 * its subpath.
 *
 * This is unlike the other Path nodes in that it can actually generate
 * different plans: either hash-based or sort-based implementation, or a
 * no-op if the input path can be proven distinct already.	The decision
 * is sufficiently localized that it's not worth having separate Path node
 * types.  (Note: in the no-op case, we could eliminate the UniquePath node
 * entirely and just return the subpath; but it's convenient to have a
 * UniquePath in the path tree to signal upper-level routines that the input
 * is known distinct.)
 */
typedef enum
{
	UNIQUE_PATH_NOOP,			/* input is known unique already */
	UNIQUE_PATH_HASH,			/* use hashing */
	UNIQUE_PATH_SORT,			/* use sorting */
    UNIQUE_PATH_LIMIT1          /* CDB: take at most one row from the subpath */
} UniquePathMethod;

typedef struct UniquePath
{
	Path		path;
	Path	   *subpath;
	UniquePathMethod umethod;
	double		rows;			/* estimated number of result tuples */
    List       *distinct_on_exprs;
                                /* CDB: list of exprs to be uniqueified */
    Relids      distinct_on_rowid_relids;
                                /* CDB: set of relids whose row ids are to be
                                 * uniqueified.
                                 */
    bool        must_repartition;
                                /* CDB: true => add Motion atop subpath  */
} UniquePath;

/*
 * All join-type paths share these fields.
 */

typedef struct JoinPath
{
	Path		path;

	JoinType	jointype;

	Path	   *outerjoinpath;	/* path for the outer side of the join */
	Path	   *innerjoinpath;	/* path for the inner side of the join */

	List	   *joinrestrictinfo;		/* RestrictInfos to apply to join */

	/*
	 * See the notes for RelOptInfo to understand why joinrestrictinfo is
	 * needed in JoinPath, and can't be merged into the parent RelOptInfo.
	 */
} JoinPath;

/*
 * IsJoinPath
 *      Returns true if the node type is one that derives from JoinPath.
 */
#define IsJoinPath(node)        \
    (IsA((node), NestPath) ||   \
     IsA((node), HashPath) ||   \
     IsA((node), MergePath))

/*
 * A nested-loop path has special fields which may be used if it falls back to
 * plan-B during execution.
 */

typedef struct NestPath
{
	JoinPath            jpath;
} NestPath;

/*
 * A mergejoin path has these fields.
 *
 * path_mergeclauses lists the clauses (in the form of RestrictInfos)
 * that will be used in the merge.
 *
 * Note that the mergeclauses are a subset of the parent relation's
 * restriction-clause list.  Any join clauses that are not mergejoinable
 * appear only in the parent's restrict list, and must be checked by a
 * qpqual at execution time.
 *
 * outersortkeys (resp. innersortkeys) is NIL if the outer path
 * (resp. inner path) is already ordered appropriately for the
 * mergejoin.  If it is not NIL then it is a PathKeys list describing
 * the ordering that must be created by an explicit sort step.
 */

typedef struct MergePath
{
	JoinPath	jpath;
	List	   *path_mergeclauses;		/* join clauses to be used for merge */
	List	   *outersortkeys;	/* keys for explicit sort, if any */
	List	   *innersortkeys;	/* keys for explicit sort, if any */
} MergePath;

/*
 * A hashjoin path has these fields.
 *
 * The remarks above for mergeclauses apply for hashclauses as well.
 *
 * Hashjoin does not care what order its inputs appear in, so we have
 * no need for sortkeys.
 */

typedef struct HashPath
{
	JoinPath	jpath;
	List	   *path_hashclauses;		/* join clauses used for hashing */
} HashPath;

/*
 * Restriction clause info.
 *
 * We create one of these for each AND sub-clause of a restriction condition
 * (WHERE or JOIN/ON clause).  Since the restriction clauses are logically
 * ANDed, we can use any one of them or any subset of them to filter out
 * tuples, without having to evaluate the rest.  The RestrictInfo node itself
 * stores data used by the optimizer while choosing the best query plan.
 *
 * If a restriction clause references a single base relation, it will appear
 * in the baserestrictinfo list of the RelOptInfo for that base rel.
 *
 * If a restriction clause references more than one base rel, it will
 * appear in the joininfo list of every RelOptInfo that describes a strict
 * subset of the base rels mentioned in the clause.  The joininfo lists are
 * used to drive join tree building by selecting plausible join candidates.
 * The clause cannot actually be applied until we have built a join rel
 * containing all the base rels it references, however.
 *
 * When we construct a join rel that includes all the base rels referenced
 * in a multi-relation restriction clause, we place that clause into the
 * joinrestrictinfo lists of paths for the join rel, if neither left nor
 * right sub-path includes all base rels referenced in the clause.	The clause
 * will be applied at that join level, and will not propagate any further up
 * the join tree.  (Note: the "predicate migration" code was once intended to
 * push restriction clauses up and down the plan tree based on evaluation
 * costs, but it's dead code and is unlikely to be resurrected in the
 * foreseeable future.)
 *
 * Note that in the presence of more than two rels, a multi-rel restriction
 * might reach different heights in the join tree depending on the join
 * sequence we use.  So, these clauses cannot be associated directly with
 * the join RelOptInfo, but must be kept track of on a per-join-path basis.
 *
 * When dealing with outer joins we have to be very careful about pushing qual
 * clauses up and down the tree.  An outer join's own JOIN/ON conditions must
 * be evaluated exactly at that join node, and any quals appearing in WHERE or
 * in a JOIN above the outer join cannot be pushed down below the outer join.
 * Otherwise the outer join will produce wrong results because it will see the
 * wrong sets of input rows.  All quals are stored as RestrictInfo nodes
 * during planning, but there's a flag to indicate whether a qual has been
 * pushed down to a lower level than its original syntactic placement in the
 * join tree would suggest.  If an outer join prevents us from pushing a qual
 * down to its "natural" semantic level (the level associated with just the
 * base rels used in the qual) then we mark the qual with a "required_relids"
 * value including more than just the base rels it actually uses.  By
 * pretending that the qual references all the rels appearing in the outer
 * join, we prevent it from being evaluated below the outer join's joinrel.
 * When we do form the outer join's joinrel, we still need to distinguish
 * those quals that are actually in that join's JOIN/ON condition from those
 * that appeared elsewhere in the tree and were pushed down to the join rel
 * because they used no other rels.  That's what the is_pushed_down flag is
 * for; it tells us that a qual is not an OUTER JOIN qual for the set of base
 * rels listed in required_relids.  A clause that originally came from WHERE
 * or an INNER JOIN condition will *always* have its is_pushed_down flag set.
 * It's possible for an OUTER JOIN clause to be marked is_pushed_down too,
 * if we decide that it can be pushed down into the nullable side of the join.
 * In that case it acts as a plain filter qual for wherever it gets evaluated.
 *
 * When application of a qual must be delayed by outer join, we also mark it
 * with outerjoin_delayed = true.  This isn't redundant with required_relids
 * because that might equal clause_relids whether or not it's an outer-join
 * clause.
 *
 * In general, the referenced clause might be arbitrarily complex.	The
 * kinds of clauses we can handle as indexscan quals, mergejoin clauses,
 * or hashjoin clauses are fairly limited --- the code for each kind of
 * path is responsible for identifying the restrict clauses it can use
 * and ignoring the rest.  Clauses not implemented by an indexscan,
 * mergejoin, or hashjoin will be placed in the plan qual or joinqual field
 * of the finished Plan node, where they will be enforced by general-purpose
 * qual-expression-evaluation code.  (But we are still entitled to count
 * their selectivity when estimating the result tuple count, if we
 * can guess what it is...)
 *
 * When the referenced clause is an OR clause, we generate a modified copy
 * in which additional RestrictInfo nodes are inserted below the top-level
 * OR/AND structure.  This is a convenience for OR indexscan processing:
 * indexquals taken from either the top level or an OR subclause will have
 * associated RestrictInfo nodes.
 *
 * The can_join flag is set true if the clause looks potentially useful as
 * a merge or hash join clause, that is if it is a binary opclause with
 * nonoverlapping sets of relids referenced in the left and right sides.
 * (Whether the operator is actually merge or hash joinable isn't checked,
 * however.)
 *
 * The pseudoconstant flag is set true if the clause contains no Vars of
 * the current query level and no volatile functions.  Such a clause can be
 * pulled out and used as a one-time qual in a gating Result node.	We keep
 * pseudoconstant clauses in the same lists as other RestrictInfos so that
 * the regular clause-pushing machinery can assign them to the correct join
 * level, but they need to be treated specially for cost and selectivity
 * estimates.  Note that a pseudoconstant clause can never be an indexqual
 * or merge or hash join clause, so it's of no interest to large parts of
 * the planner.
 */

typedef struct RestrictInfo
{
	NodeTag		type;

	Expr	   *clause;			/* the represented clause of WHERE or JOIN */

	bool		is_pushed_down; /* TRUE if clause was pushed down in level */

	bool		outerjoin_delayed;		/* TRUE if delayed by outer join */

	bool		can_join;		/* see comment above */

	bool		pseudoconstant; /* see comment above */

	/* The set of relids (varnos) actually referenced in the clause: */
	Relids		clause_relids;

	/* The set of relids required to evaluate the clause: */
	Relids		required_relids;

	/* These fields are set for any binary opclause: */
	Relids		left_relids;	/* relids in left side of clause */
	Relids		right_relids;	/* relids in right side of clause */

	/* This field is NULL unless clause is an OR clause: */
	Expr	   *orclause;		/* modified clause with RestrictInfos */

	/* cache space for cost and selectivity */
	QualCost	eval_cost;		/* eval cost of clause; -1 if not yet set */
	Selectivity this_selec;		/* selectivity; -1 if not yet set */

	/* valid if clause is mergejoinable, else InvalidOid: */
	Oid			mergejoinoperator;		/* copy of clause operator */
	Oid			left_sortop;	/* leftside sortop needed for mergejoin */
	Oid			right_sortop;	/* rightside sortop needed for mergejoin */

	/* cache space for mergeclause processing; NIL if not yet set */
	List	   *left_pathkey;	/* canonical pathkey for left side */
	List	   *right_pathkey;	/* canonical pathkey for right side */

	/* cache space for mergeclause processing; -1 if not yet set */
	Selectivity left_mergescansel;		/* fraction of left side to scan */
	Selectivity right_mergescansel;		/* fraction of right side to scan */

	/* valid if clause is hashjoinable, else InvalidOid: */
	Oid			hashjoinoperator;		/* copy of clause operator */

	/* cache space for hashclause processing; -1 if not yet set */
	Selectivity left_bucketsize;	/* avg bucketsize of left side */
	Selectivity right_bucketsize;		/* avg bucketsize of right side */
} RestrictInfo;

/*
 * Inner indexscan info.
 *
 * An inner indexscan is one that uses one or more joinclauses as index
 * conditions (perhaps in addition to plain restriction clauses).  So it
 * can only be used as the inner path of a nestloop join where the outer
 * relation includes all other relids appearing in those joinclauses.
 * The set of usable joinclauses, and thus the best inner indexscan,
 * thus varies depending on which outer relation we consider; so we have
 * to recompute the best such paths for every join.  To avoid lots of
 * redundant computation, we cache the results of such searches.  For
 * each relation we compute the set of possible otherrelids (all relids
 * appearing in joinquals that could become indexquals for this table).
 * Two outer relations whose relids have the same intersection with this
 * set will have the same set of available joinclauses and thus the same
 * best inner indexscans for the inner relation.  By taking the intersection
 * before scanning the cache, we avoid recomputing when considering
 * join rels that differ only by the inclusion of irrelevant other rels.
 *
 * The search key also includes a bool showing whether the join being
 * considered is an outer join.  Since we constrain the join order for
 * outer joins, I believe that this bool can only have one possible value
 * for any particular lookup key; but store it anyway to avoid confusion.
 */

typedef struct InnerIndexscanInfo
{
	NodeTag		type;
	/* The lookup key: */
	Relids		other_relids;	/* a set of relevant other relids */
	bool		isouterjoin;	/* true if join is outer */
	/* Best paths for this lookup key (NULL if no available indexscans): */
	Path	   *cheapest_startup_innerpath;	/* cheapest startup cost */
	Path	   *cheapest_total_innerpath;	/* cheapest total cost */
} InnerIndexscanInfo;

/*
 * Outer join info.
 *
 * One-sided outer joins constrain the order of joining partially but not
 * completely.	We flatten such joins into the planner's top-level list of
 * relations to join, but record information about each outer join in an
 * OuterJoinInfo struct.  These structs are kept in the PlannerInfo node's
 * oj_info_list.
 *
 * min_lefthand and min_righthand are the sets of base relids that must be
 * available on each side when performing the outer join.  lhs_strict is
 * true if the outer join's condition cannot succeed when the LHS variables
 * are all NULL (this means that the outer join can commute with upper-level
 * outer joins even if it appears in their RHS).  We don't bother to set
 * lhs_strict for FULL JOINs, however.
 *
 * It is not valid for either min_lefthand or min_righthand to be empty sets;
 * if they were, this would break the logic that enforces join order.
 *
 * syn_lefthand and syn_righthand are the sets of base relids that are
 * syntactically below this outer join.  (These are needed to help compute
 * min_lefthand and min_righthand for higher joins, but are not used
 * thereafter.)
 *
 * delay_upper_joins is set TRUE if we detect a pushed-down clause that has
 * to be evaluated after this join is formed (because it references the RHS).
 * Any outer joins that have such a clause and this join in their RHS cannot
 * commute with this join, because that would leave noplace to check the
 * pushed-down clause.  (We don't track this for FULL JOINs, either.)
 *
 * Note: OuterJoinInfo directly represents only LEFT JOIN and FULL JOIN;
 * RIGHT JOIN is handled by switching the inputs to make it a LEFT JOIN.
 * We make an OuterJoinInfo for FULL JOINs even though there is no flexibility
 * of planning for them, because this simplifies make_join_rel()'s API.
 */

typedef struct OuterJoinInfo
{
	NodeTag		type;
	Relids		min_lefthand;	/* base relids in minimum LHS for join */
	Relids		min_righthand;	/* base relids in minimum RHS for join */
	Relids		syn_lefthand;	/* base relids syntactically within LHS */
	Relids		syn_righthand;	/* base relids syntactically within RHS */
	JoinType	join_type;		/* LEFT, FULL, or ANTI */
	bool		lhs_strict;		/* joinclause is strict for some LHS rel */
	bool		delay_upper_joins;	/* can't commute with upper RHS */

	/**
	 * list of lists of equijoined PathKeyItems
	 * only valid for FULL joins.  Will contain equi_key sets but ONLY
	 * for tables that are below the LEFT nullable side of the outer join.
	 */
	List	   *left_equi_key_list;

	/**
	 * list of lists of equijoined PathKeyItems
	 * Will contain equi_key sets but ONLY
	 * for tables that are below the RIGHT nullable side of the outer join.
	 */
	List	   *right_equi_key_list;

} OuterJoinInfo;

/*
 * IN clause info.
 *
 * When we convert top-level IN quals into join operations, we must restrict
 * the order of joining and use special join methods at some join points.
 * We record information about each such IN clause in an InClauseInfo struct.
 * These structs are kept in the PlannerInfo node's in_info_list.
 */

typedef struct InClauseInfo
{
	NodeTag		type;
	Relids		lefthand; 		/* base relids in lefthand expressions */
	Relids		righthand;		/* base relids coming from the subselect */
	List	   *sub_targetlist; /* targetlist of original RHS subquery */

	/*
	 * Note: sub_targetlist is just a list of Vars or expressions; it does not
	 * contain TargetEntry nodes.
	 */

    bool        try_join_unique;
                                /* CDB: true => comparison is equality op and
                                 *  subquery is not correlated.  Ok to consider
                                 *  JOIN_UNIQUE method of duplicate suppression.
                                 */

} InClauseInfo;

/*
 * Append-relation info.
 *
 * When we expand an inheritable table or a UNION-ALL subselect into an
 * "append relation" (essentially, a list of child RTEs), we build an
 * AppendRelInfo for each child RTE.  The list of AppendRelInfos indicates
 * which child RTEs must be included when expanding the parent, and each
 * node carries information needed to translate Vars referencing the parent
 * into Vars referencing that child.
 *
 * These structs are kept in the PlannerInfo node's append_rel_list.
 * Note that we just throw all the structs into one list, and scan the
 * whole list when desiring to expand any one parent.  We could have used
 * a more complex data structure (eg, one list per parent), but this would
 * be harder to update during operations such as pulling up subqueries,
 * and not really any easier to scan.  Considering that typical queries
 * will not have many different append parents, it doesn't seem worthwhile
 * to complicate things.
 *
 * Note: after completion of the planner prep phase, any given RTE is an
 * append parent having entries in append_rel_list if and only if its
 * "inh" flag is set.  We clear "inh" for plain tables that turn out not
 * to have inheritance children, and (in an abuse of the original meaning
 * of the flag) we set "inh" for subquery RTEs that turn out to be
 * flattenable UNION ALL queries.  This lets us avoid useless searches
 * of append_rel_list.
 *
 * Note: the data structure assumes that append-rel members are single
 * baserels.  This is OK for inheritance, but it prevents us from pulling
 * up a UNION ALL member subquery if it contains a join.  While that could
 * be fixed with a more complex data structure, at present there's not much
 * point because no improvement in the plan could result.
 */

typedef struct AppendRelInfo
{
	NodeTag		type;

	/*
	 * These fields uniquely identify this append relationship.  There can be
	 * (in fact, always should be) multiple AppendRelInfos for the same
	 * parent_relid, but never more than one per child_relid, since a given
	 * RTE cannot be a child of more than one append parent.
	 */
	Index		parent_relid;	/* RT index of append parent rel */
	Index		child_relid;	/* RT index of append child rel */

	/*
	 * For an inheritance appendrel, the parent and child are both regular
	 * relations, and we store their rowtype OIDs here for use in translating
	 * whole-row Vars.	For a UNION-ALL appendrel, the parent and child are
	 * both subqueries with no named rowtype, and we store InvalidOid here.
	 */
	Oid			parent_reltype; /* OID of parent's composite type */
	Oid			child_reltype;	/* OID of child's composite type */

	/*
	 * The N'th element of this list is the integer column number of the child
	 * column corresponding to the N'th column of the parent. A list element
	 * is zero if it corresponds to a dropped column of the parent (this is
	 * only possible for inheritance cases, not UNION ALL).
	 */
	List	   *col_mappings;	/* list of child attribute numbers */

	/*
	 * The N'th element of this list is a Var or expression representing the
	 * child column corresponding to the N'th column of the parent. This is
	 * used to translate Vars referencing the parent rel into references to
	 * the child.  A list element is NULL if it corresponds to a dropped
	 * column of the parent (this is only possible for inheritance cases, not
	 * UNION ALL).
	 *
	 * This might seem redundant with the col_mappings data, but it is handy
	 * because flattening of sub-SELECTs that are members of a UNION ALL will
	 * cause changes in the expressions that need to be substituted for a
	 * parent Var.	Adjusting this data structure lets us track what really
	 * needs to be substituted.
	 *
	 * Notice we only store entries for user columns (attno > 0).  Whole-row
	 * Vars are special-cased, and system columns (attno < 0) need no special
	 * translation since their attnos are the same for all tables.
	 *
	 * Caution: the Vars have varlevelsup = 0.	Be careful to adjust as needed
	 * when copying into a subquery.
	 */
	List	   *translated_vars;	/* Expressions in the child's Vars */

	/*
	 * We store the parent table's OID here for inheritance, or InvalidOid for
	 * UNION ALL.  This is only needed to help in generating error messages if
	 * an attempt is made to reference a dropped parent column.
	 */
	Oid			parent_reloid;	/* OID of parent relation */
} AppendRelInfo;

/*
 * glob->paramlist keeps track of the PARAM_EXEC slots that we have decided
 * we need for the query.  At runtime these slots are used to pass values
 * either down into subqueries (for outer references in subqueries) or up out
 * of subqueries (for the results of a subplan).  The n'th entry in the list
 * (n counts from 0) corresponds to Param->paramid = n.
 *
 * Each paramlist item shows the absolute query level it is associated with,
 * where the outermost query is level 1 and nested subqueries have higher
 * numbers.  The item the parameter slot represents can be one of three kinds:
 *
 * A Var: the slot represents a variable of that level that must be passed
 * down because subqueries have outer references to it.  The varlevelsup
 * value in the Var will always be zero.
 *
 * An Aggref (with an expression tree representing its argument): the slot
 * represents an aggregate expression that is an outer reference for some
 * subquery.  The Aggref itself has agglevelsup = 0, and its argument tree
 * is adjusted to match in level.
 *
 * A Param: the slot holds the result of a subplan (it is a setParam item
 * for that subplan).  The absolute level shown for such items corresponds
 * to the parent query of the subplan.
 *
 * Note: we detect duplicate Var parameters and coalesce them into one slot,
 * but we do not do this for Aggref or Param slots.
 */
typedef struct PlannerParamItem
	{
		NodeTag		type;
		
		Node	   *item;			/* the Var, Aggref, or Param */
		Index		abslevel;		/* its absolute query level */
	} PlannerParamItem;

/* 
 * Partitioning meta data 
 */

/*
 * convenient representation of a row of pg_partition -- a partitioning level of
 * a partitioned table or a template for all the partitioning branches at a level.
 */
typedef struct Partition
{
	NodeTag type;
	Oid partid;			/* OID of row in pg_partition. */
	Oid parrelid;		/* OID in pg_class of top-level partitioned relation */
	char parkind;		/* 'r', 'l', or (unsupported) 'h' */
	int2 parlevel;		/* depth below parent partitioned table */
	bool paristemplate;	/* just a template, or really a part? */
	int2 parnatts;		/* number of partitioning attributes */
	AttrNumber *paratts;/* attribute number vector */ 
	Oid *parclass;		/* operator class vector */
} Partition;

struct PartitionNode
{
	NodeTag type;
	Partition *part;
	struct PartitionRule *default_part;
	List *rules; /* rules for this level */
};

/* Individual partitioning rule */
typedef struct PartitionRule
{
	NodeTag		 type;
	Oid			 parruleid;
	Oid			 paroid;
	Oid			 parchildrelid;
	Oid			 parparentoid;
	bool		 parisdefault;
	char		*parname;
	Node		*parrangestart;
	bool		 parrangestartincl;
	Node		*parrangeend;
	bool		 parrangeendincl;
	Node		*parrangeevery;
	List		*parlistvalues;
	int2		 parruleord;
	List		*parreloptions;
	Oid			 partemplatespaceId; 	/* the tablespace id for the
										 * template (or InvalidOid for 
										 * non-template rules */
	struct PartitionNode *children; /* sub partition */
} PartitionRule;

typedef struct PgPartRule
{
	NodeTag type;
	PartitionNode 		*pNode;	
	PartitionRule 		*topRule;	/* the rule for the specified partition */

	/* a textual representation of the partition id (for error msgs) */
	char		*partIdStr;
	bool         isName;		/* true if partid is name */
	int          topRuleRank;	/* rank of topRule */
	char        *relname; 		/* the error msg formatted "relname" */
} PgPartRule;

/*
 * A Mapping created by the QD during data loading that maps a
 * relation id to the segfile number that is should be inserting
 * into (in cases of inserting into a partitioned table the QD
 * assigns a segno for each possible partition child relation).
 * 
 * It is a node because it needs to get serialized as a part of 
 * CopyStmt.
 */
typedef struct SegfileMapNode
{
	NodeTag 	type;
	Oid			relid;
	List			*segnos;
} SegfileMapNode;

/*
 * Result relation segment file information
 *
 *
 */
typedef struct ResultRelSegFileInfo
{
	NodeTag type;

	int32 segno;
	int64 varblock;
	int64 tupcount;
	int32 numfiles;
	int64 *eof;
	int64 *uncompressed_eof;
} ResultRelSegFileInfo;

typedef struct ResultRelSegFileInfoMapNode
{
	NodeTag type;
	Oid relid;
	List *segfileinfos;
} ResultRelSegFileInfoMapNode;

#endif   /* RELATION_H */
