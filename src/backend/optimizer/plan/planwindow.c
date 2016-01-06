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
 * planwindow.c
 *	  Planning for window queries.
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $ID$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/catquery.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_window.h"
#include "nodes/makefuncs.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/planshare.h"
#include "optimizer/var.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "utils/debugbreak.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/selfuncs.h"

#include "cdb/cdbgroup.h"					/* cdbpathlocus_collocates() */
#include "cdb/cdbllize.h"                   /* pull_up_Flow() */
#include "cdb/cdbpath.h"
#include "cdb/cdbsetop.h"					/* make_motion... routines */
#include "cdb/cdbvars.h"

typedef enum CoplanType
{
	COPLAN_WINDOW,
	COPLAN_AGG
} CoplanType;

typedef struct Coplan
{
	CoplanType	type;
	Index		varno;
	AttrNumber	last_resno;
	int			num_aggs;
	List	   *targetnames;
	List	   *targetlist;
	int			rowkey_len;
	AttrNumber *rowkey_attrs;
	int			partkey_len;
	AttrNumber *partkey_attrs;
	int			trans_space;
} Coplan;

/* WindowInfo represents a set of coplans in the final plan for a window 
 * query.  The set contains at least one of a coplan for a Window node 
 * and a coplan for an Agg node.
 *
 * Each WindowInfo controls an array of SpecInfos all of which have the same
 * partitioning set and compatible ordering keys.  The SpecInfos are stored 
 * in WindowContext's array (nspecinfos, specinfos).  A WindowSpec controls
 * numspecindex of them beginning at firstspecindex.
 *
 * Stored in the WindowContext's array (nwindowinfos, windowinfos).
 */
typedef struct WindowInfo
{
	Index	firstspecindex;
	Index	numspecindex;
	List   *sortclause;
	int		partkey_len;
	int		rowkey_len;
	int     orderkeys_offset;
	bool	needpartkey;
	bool	needauxcount;
	
	AttrNumber *partkey_attrs;
	List	   *key_list;
	
	/* coplan assembly */
	
	struct Coplan  *window_coplan;
	struct Coplan  *agg_coplan;
} WindowInfo;


/* RefInfo holds a WindowRef and describes it in the context of the overall
 * query.  During planning, the contained WindowRef refers back to the
 * RefInfo via winindex - the position (counting from 0) of the RefInfo in
 * the WindowContext's list, refinfos.
 */
typedef struct RefInfo
{
	WindowRef  *ref;
	Bitmapset  *varset;
	Index		specindex; /* index of parent SpecInfo */
	bool		isagg;
	char		winkind;
	
	/* When isagg  */
	bool		hasinvtrans;
	bool		hasprelim;
	bool		hasinvprelim;
	
	/* When iswin = !isagg */
	bool		needcount;
	Oid			winpretype;
	Oid			winfinfunc;
	
	int			trans_space;
	
	Expr	   *resultexpr; /* Result expression for upper target list in parallel target list */
	AttrNumber	resno; /* Attribute number of result in sequential target list */
} RefInfo;

/* Macros to determine what kind of window function a RefInfo represents
 * should be used AFTER the context has been intialized by functions through
 * through assign_window_info().
 */
#define RefInfo_AggregateUnordered(rinfo, context) \
	( (rinfo)->isagg && !(rinfo)->needcount && (context)->specinfos[rinfo->specindex].order == NIL )

#define RefInfo_AggregateOrdered(rinfo, context) \
	( (rinfo)->isagg && !(rinfo)->needcount && (context)->specinfos[rinfo->specindex].order != NIL )

#define RefInfo_WindowDeferred(rinfo, context) \
	( !(rinfo)->isagg && (rinfo)->needcount )

#define RefInfo_WindowImmediate(rinfo, context) \
	( !(rinfo)->isagg && !(rinfo)->needcount )

/* SpecInfo represents a distinct, referenced WindowSpec from the query.
 * They are stored in the WindowContext's array (nspecinfos,specinfos).
 *
 * Later in planning, SpecInfo's with identical partitioning and compatible
 * ordering are grouped under a WindowInfo.
 */
typedef struct SpecInfo
{
	Index			specindex;		/* index of SpecInfo in context */
	Bitmapset	   *partset;		/* sortgrouprefs of partitioning keys */
	List		   *order;			/* ORDER BY clause */
	WindowFrame	   *frame;			/* framing clause */
	Bitmapset	   *refset;			/* indices of referencing RefInfo */
	Index			windowindex;	/* index of assigned Window node */
	int				keylevel;		/* key level of spec in Window node */
	List		   *partkey;		/* PARTITION BY clause - transient */
	/*
	 * Remaining SortClauses after removing keys that are
	 * duplicates of partitioning keys or other ORDER BY keys.
	 *
	 * This is used to sort SpecInfos and group SpecInfos into a
	 * WindowInfo.
	 */
	List           *unique_order; 
} SpecInfo;


/* WindowContext is the overall context for window planning.
 *
 */
typedef struct WindowContext
{
	List		   *upper_tlist;
	Bitmapset	   *upper_var_set;
	List		   *refinfos;
	
	int				nspecinfos;
	SpecInfo	   *specinfos;
	int				nwindowinfos;
	WindowInfo	   *windowinfos;

	int				rowkey_len;
	AttrNumber	   *rowkey_attrs;
	
	Index			coplan_count;
	
	bool			has_unordered_aggs;
	bool			has_deferred_window_fns;
	
	 /* Only for sequential plans */
	bool			original_range;
	Query		   *subquery;
	
	/* Map (varno,varattno) <--> (integer) for use in Bitmapsets recording
	 * use of vars from the window query's range table.
	 */
	int				max_varno;
	int			   *varattno_offsets;
	int			   *offset_varnos;
	int				offset_lim;
	
	/* Offset map for final targetlist translation for parallel plans. */
	AttrNumber	   *offset_upper_varattrnos;
	
	/* Target list for the "common subquery".
	 */
	List		   *lower_tlist;
	List		   *keyed_lower_tlist;
	Plan		   *subplan;
	CdbPathLocus	subplan_locus;
	List		   *subplan_pathkeys;

	/* List of PathKeys, one for each coplan. */
	List           *subplans_pathkeys;
	
		
	/* Map (ressortgrpref) --> (resno) in lower_tlist */
	int				max_sortref;
	AttrNumber	   *sortref_resno;
	
	/* Transient state for preprocess_window_tlist */
	RefInfo		   *cur_refinfo;
	List           *win_specs;
	List           *orig_tlist;
} WindowContext;

#define VarattnoAdjustment (-(FirstLowInvalidHeapAttributeNumber + 1))

/* Forward declarations (local) */

static WindowContext *newWindowContext(void);
static void deleteWindowContext(WindowContext *context);
static void build_sortref_index(List *tlist, int *max_sortref, AttrNumber **sortref_resno);
static void build_var_index(Query *parse, WindowContext *context);
static int index_of_var(Var * var, WindowContext *context);
static void preprocess_window_tlist(List *orig_tlist, WindowContext *context);
static Node * window_tlist_mutator(Node *node, WindowContext *context);
static bool window_tlist_vars_walker(Node *node, WindowContext *context);
static void inventory_window_specs(List *window_specs, WindowContext *context);
static int compare_order(List *a, List* b);
static int compare_edge(WindowFrameEdge *a, WindowFrameEdge *b);
static int compare_frame(WindowFrame *a, WindowFrame *b);
static int compare_spec_info_ptr(const void *arg1, const void *arg2);
static bool validateBound(Node *node, bool is_rows);
static WindowFrameEdge *adjustFrameBound(WindowFrameEdge *edge, bool is_rows);
static void make_lower_targetlist(Query *parse, WindowContext *context);
static void set_window_keys(WindowContext *context, int wind_index);
static void assign_window_info(WindowContext *context);
static bool is_order_prefix_of(List *sca, List *scb);
static void lookup_window_function(RefInfo *rinfo);
static Plan *plan_trivial_window_query(PlannerInfo *root, WindowContext *context, List **pathkeys_ptr);
static Plan *plan_sequential_window_query(PlannerInfo *root, WindowContext *context, List **pathkeys_ptr);
static Plan *plan_sequential_stage(PlannerInfo *root, WindowContext *context, 
		int hi_windex, int lo_windex,
		Plan *input_plan, CdbPathLocus *locus_ptr, List **pathkeys_ptr);
static Plan *assure_order(PlannerInfo *root, Plan *input_plan, List *sortclause, List **pathkeys_ptr);
static Expr * make_mergeclause(Index lftvarno, AttrNumber lftattrno, Node *rgtexpr);
static List *translate_upper_tlist_sequential(List *orig_tlist, List *window_tlist, WindowContext *context);
static Plan *plan_parallel_window_query(PlannerInfo *root, WindowContext *context, List **pathkeys_ptr);
static void plan_windowinfo_coplans(PlannerInfo *root, WindowContext *context, int window_index);
static List * plan_window_rtable(PlannerInfo* root, WindowContext *context);
static List *make_rowkey_targets(void);
static Coplan *makeCoplan(CoplanType type, WindowContext *context);
static Plan *assure_collocation_and_order(PlannerInfo *root, Plan *input_plan, 
		int partkey_len, AttrNumber *partkey_attrs, List *sortclause, 
		CdbPathLocus input_locus, CdbPathLocus *output_locus, List **pathkeys_ptr);
static AttrNumber addTargetToCoplan(Node *target, Coplan *coplan, WindowContext *context);
static Aggref* makeWindowAggref(WindowRef *winref);
static Aggref* makeAuxCountAggref(void);
static List *translate_upper_tlist_parallel(List *orig_tlist, WindowContext *context);
static Node *translate_upper_vars_sequential(Node *node, WindowContext *context);
static FromExpr *plan_window_jointree(PlannerInfo *root, WindowContext *context);
static Expr *make_window_join_term(Oid type, Index lftvarno, AttrNumber lftattrno, Index rgtvarno, AttrNumber rgtattrno, bool no_nulls);
static Oid *get_window_attr_types(List *targetlist, int nattrs, AttrNumber *attrs);
static Plan *add_join_to_wrapper(PlannerInfo *root, Plan *plan, Query *query, List *join_tlist,
								 unsigned partkey_len, 
								 const char *alias_name, List *col_names, 
								 Query *wrapper_query);
static Query *copy_common_subquery(Query *original, List *targetList);
static char *get_function_name(Oid proid, const char *dflt);


Plan *
window_planner(PlannerInfo *root, double tuple_fraction, List **pathkeys_ptr)
{
	Query		   *parse = root->parse;
	WindowContext  *context = newWindowContext();
	Plan		   *result_plan = NULL;

	/* Assert existence of windowing in query. */
	Assert(parse->targetList != NIL);
	Assert(parse->windowClause != NULL);
	Assert(parse->hasWindFuncs);	
	/* Assert no unsupported stuff */
	Assert(parse->setOperations == NULL);
	Assert(!parse->hasAggs);
	Assert(parse->groupClause == NIL);
	Assert(parse->havingQual == NULL);
	
#ifdef CDB_WINDOW_DISPLAY	
	elog_node_display(NOTICE, "Window  query target list", parse->targetList, true);
	elog_node_display(NOTICE, "Window  query window  clause", parse->windowClause, true);
#endif
	
	/* Create a map: (varno,varattno)  <--> (integer) for use in recording
	 * Var references in Bitmapsets.
	 */
	build_var_index(parse, context);

	/*
	 * Set context->win_specs and context->orig_tlist. These
	 * are needed in preprecess_window_tlist() to check NTILE function
	 * arguments.
	 */
	context->win_specs = parse->windowClause;
	context->orig_tlist = parse->targetList;
	
	/* Create a copy of the input target list for our use.  Save pointers
	 * to the contained WindowRef nodes on the side in a array of RefInfo
	 * structures.
	 */
	preprocess_window_tlist(parse->targetList, context);

	/* Create an array of SpecInfo structures corresponding to the distinct
	 * referenced WindowSpec nodes in the input. Refer back to this array
	 * from the array of RefInfo structures.
	 */
	inventory_window_specs(parse->windowClause, context);

	/* Generate the (lower) target list for the common subquery.  
	 * Importantly, the ressortgroupref values agree with those in
	 * the input (upper) target list (and all of them appear).
	 */
	make_lower_targetlist(root->parse, context);
	
	/* Divide the SpecInfos into (contiguous) groups to be handled by a
	 * single Window node and associated auxiliary stuff (in other words,
	 * by a single sort of the input) and assign a WindowInfo to represent
	 * each group.
	 */
	assign_window_info(context);
	
	Assert( context->upper_tlist != NULL );
	Assert( list_length(context->refinfos) > 0 );
	Assert( context->nspecinfos > 0 );
	
	
	if ( context->nwindowinfos == 1 && 
		!(context->has_unordered_aggs || context->has_deferred_window_fns) )
	{
		return plan_trivial_window_query(root, context, pathkeys_ptr);
	}
	else
	{
		if ( root->config->gp_enable_sequential_window_plans )
			return plan_sequential_window_query(root, context, pathkeys_ptr);
		else
			return plan_parallel_window_query(root, context, pathkeys_ptr);
	}

	/* TODO Check our API.
	 *
	 * Note: pathkeys may be an important implicit result. 
	 */
	
	deleteWindowContext(context);
	
	if ( result_plan == NULL )
	{
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("That windowing query is not supported yet.")));
	}
	
	return result_plan;
}

/* Create a palloc'd WindowContext structure for local use. */
static WindowContext *newWindowContext()
{
	WindowContext *context = palloc(sizeof(WindowContext));
	
	context->upper_tlist = NIL;
	context->upper_var_set = NULL;
	context->refinfos = NIL;
	context->nspecinfos = 0;
	context->specinfos = NULL;
	context->nwindowinfos = 0;
	context->windowinfos = NULL;
	context->rowkey_len = 0;
	context->rowkey_attrs = NULL;
	
	context->coplan_count = 0;
	context->has_unordered_aggs = false;
	context->has_deferred_window_fns = false;

	context->original_range = true;
	context->subquery = NULL;
	
	context->max_varno = 0;
	context->varattno_offsets = NULL;
	context->offset_varnos = NULL;
	context->offset_lim = 0;
	context->offset_upper_varattrnos = NULL;
	
	context->max_sortref = 0;
	context->sortref_resno = NULL;

	context->cur_refinfo = NULL;
	
	return context;
}

/* Delete palloc'd WindowContext and owned sub-structures. */
static void deleteWindowContext(WindowContext *context)
{
	ListCell *cell;
	
	if ( context == NULL )
		return;
	
	/* no need to free upper_tlist */
	
	bms_free(context->upper_var_set);
	
	foreach( cell, context->refinfos )
	{
		RefInfo *ref = (RefInfo *)lfirst(cell);
		/* no need to free actual ref */
		bms_free(ref->varset);
	}
	list_free_deep(context->refinfos);
	
	if ( context->specinfos != NULL )
		pfree(context->specinfos);
	
	if ( context->rowkey_attrs != NULL )
		pfree(context->rowkey_attrs);
	
	if ( context->windowinfos != NULL )
		pfree(context->windowinfos);
	
	if ( context->varattno_offsets != NULL )
		pfree(context->varattno_offsets);
	
	if ( context->offset_varnos != NULL )
		pfree(context->offset_varnos);
	
	if ( context->offset_upper_varattrnos != NULL )
		pfree(context->offset_upper_varattrnos);
	
	if ( context->sortref_resno != NULL )
		pfree(context->sortref_resno);
	
	/* TODO Clean up handling of 
	 *   lower_tlist, 
	 *   keyed_lower_tlist, 
	 *   subplan, 
	 *   subplan_pathkeys
	 * so we know whether to free them!
	 */
	
	/* don't free cur_refinfo */
	
	pfree(context);
}



/* Build an index structure to convert a (varno, varattno) to an integer
 * index and save it in the context.  The intended use of the index is
 * as follows:
 *
 *    varattno_offsets[varno] + varattno + VarattnoAdjustment  -->  index
 *
 *    offset_varnos[index]  -->  varno
 *    index - varattno_offsets[varno] - VarattnoAdjustment  --> varattno
 */
static void build_var_index(Query *parse, WindowContext *context)
{
	int i, j, n;
	int max_varno = list_length(parse->rtable);
	
	context->max_varno = max_varno;
	context->varattno_offsets = palloc0((max_varno+1)*sizeof(int));
	
	expression_tree_walker((Node*)parse->targetList,
							window_tlist_vars_walker, 
							(void*)context );
							
	for ( i = 0, n = 0; i <= max_varno; i++ )
	{
		int max_attno = context->varattno_offsets[i];
		context->varattno_offsets[i] = n;
		n += max_attno;
	}
	
	context->offset_lim = (n+1)*sizeof(int);
	context->offset_varnos = palloc0(context->offset_lim);
	
	for ( i = 1, j = 1; i <= max_varno; i++ )
	{
		for ( ; j <= context->varattno_offsets[i]; j++ )
			context->offset_varnos[j] = i;
	}
}

/* Subroutine for build_var_index() updates varattno_offsets vector to
 * record largest attribute number seen per varno (at the current level). 
 */
static bool window_tlist_vars_walker(Node *node, WindowContext *context)
{
	if ( node == NULL )
		return false;
	
	if ( IsA(node, Var) )
	{
		Var *var = (Var *) node;
		AttrNumber adjusted_attno;
		Assert( var->varno <= context->max_varno );
		if ( var->varlevelsup > 0 )
			return false;
		
		adjusted_attno = var->varattno + VarattnoAdjustment;
		if ( adjusted_attno > context->varattno_offsets[var->varno] )
			context->varattno_offsets[var->varno] = adjusted_attno;
		return false;
	}
	return expression_tree_walker(node, window_tlist_vars_walker, (void *)context);
}

/* Standard use of variable-to-offset index -- convert a Var on the lower
 * range to an integer, e.g., for use in a bit map. 
 */
static int index_of_var(Var * var, WindowContext *context)
{
	AttrNumber adjusted_attno;
	Assert( var->varno <= context->max_varno );
	
	adjusted_attno = var->varattno + VarattnoAdjustment;
	return context->varattno_offsets[var->varno] + adjusted_attno;
}


/* Build a map of sortgroupref to resno for the given target list.
 * Store its maximum sortref value and the index where specified.
 */
static void build_sortref_index(List *tlist, int *max_sortref, AttrNumber **sortref_resno)
{
	ListCell *lc;
	AttrNumber *index = NULL;
	int n = 0;
	
	foreach ( lc, tlist )
	{
		TargetEntry *tle = (TargetEntry *)lfirst(lc);
		if ( tle->ressortgroupref > n )
			n = tle->ressortgroupref;
	}

	index = (AttrNumber *)palloc0((n+1) * sizeof(AttrNumber));
	
	foreach ( lc, tlist )
	{
		TargetEntry *tle = (TargetEntry *)lfirst(lc);
		if ( tle->ressortgroupref != 0 )
			index[tle->ressortgroupref] = tle->resno;
	}
	
	*max_sortref = n;
	*sortref_resno = index;
}

/*
 * ntile_argument_walker
 *
 * Walk through the expressions in the NTILE argument. Return false
 * when the expression is a constant or the one that appears in
 * part_tlist.
 */
static bool
ntile_argument_walker(Node *node, List *part_tlist)
{
	ListCell *lc = NULL;
	
	if (node == NULL)
		return false;
	if (IsA(node, Const))
		return false;

	/*
	 * Check if node appears in part_tlist.
	 */
	foreach(lc, part_tlist)
	{
		TargetEntry *tle = (TargetEntry*)lfirst(lc);

		if (equal(tle->expr, node))
			return false;
	}
	
	if (IsA(node, Var))
		return true;
	
	return expression_tree_walker(node, ntile_argument_walker, part_tlist);
}

/*
 * check_ntile_argument
 *
 * Check expressions used in the NTILE function argument. These expressions
 * should be either a constant or expressions in PARTITION BY clauses.
 */
static void
check_ntile_argument(List *args, WindowSpec *win_spec, List *tlist)
{
	ListCell *lc;
	List *part_tlist = NIL;
	
	if (list_length(win_spec->partition) > 0)
	{
		/*
		 * Obtain the list of target entries for each expression in the
		 * PARTITION BY clause. The PARTITION BY expressions should appear
		 * in tlist.
		 */
		foreach (lc, win_spec->partition)
		{
			ListCell *tlist_lc = NULL;
			
			SortClause *sc = (SortClause *) lfirst(lc);
			Assert(IsA(sc, SortClause));

			foreach(tlist_lc, tlist)
			{
				TargetEntry *tle = (TargetEntry *)lfirst(tlist_lc);
				if (tle->ressortgroupref == sc->tleSortGroupRef)
				{
					part_tlist = lappend(part_tlist,
										 tle);
					break;
				}
			}

			if (tlist_lc == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PARTITION BY expression does not appear in the targetlist."),
								   errOmitLocation(true)));
		}
	}

	foreach (lc, args)
	{
		Node *node = lfirst(lc);

		if (ntile_argument_walker(node, part_tlist))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("NTILE function argument expression should be in PARTITION BY."),
							   errOmitLocation(true)));
	}
}

/*  preprocess_window_tlist
 *
 * Stores a deep copy of the given target list in the context and takes
 * inventory of its content as follows:
 *
 *	upper_tlist		the deep copy
 *
 *	upper_var_set	Vars used outside of WindowRef nodes
 *
 *	ref_infos		per WindowRef list of RefInfo containing
 *		ref		the WindowRef in upper_tlist
 *		varset	Vars used in the WindowRefs explicit arguments
 */
static void preprocess_window_tlist(List *orig_tlist, WindowContext *context)
{
	context->cur_refinfo = NULL;
	context->upper_var_set = NULL;
	
	context->upper_tlist =  (List*)  
		expression_tree_mutator((Node*)orig_tlist,
								 window_tlist_mutator, 
								 (void*)context );
}

/* Subroutine for preprocess_window_tlist.
 *
 * Deep copy the given expression (presumed to be a part of the parse tree
 * target list for a window query) while checking validity of and recording
 * information about contained WindowRef nodes.
 */
static Node * window_tlist_mutator(Node *node, WindowContext *context)
{
	if ( node == NULL )
		return NULL;
		
	if ( IsA(node, Var) )
	{
		Var *var = (Var *)node;
		int idx = index_of_var(var, context);
		
		if ( context->cur_refinfo == NULL ) /* above all WindowRef */
		{
			if ( var->varlevelsup == 0 )
			{
				context->upper_var_set = 
					bms_add_member(context->upper_var_set, idx);
			}
		}
		else /* below a WindowRef */
		{
			if ( var->varlevelsup == 0 )
			{
				context->cur_refinfo->varset = 
					bms_add_member(context->cur_refinfo->varset, idx);
			}
			else
			{
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("Call to window function may not reference outer queries."),
							   errOmitLocation(true)));
			}
		}
		return (Node *) copyObject(node); /* Exact copy of the Var */
	}
	else if ( IsA(node, FuncExpr) )
	{
		/*
		 * If this function is part of the argument for a NTILE function,
		 * it is not allowed to be a volatilve function.
		 */
		if (context->cur_refinfo != NULL)
		{
			FuncExpr *expr = (FuncExpr *)node;

			if (IS_NTILE(context->cur_refinfo->winkind) &&
				(func_volatile(expr->funcid) == PROVOLATILE_VOLATILE))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("NTILE function argument should not use volatile functions."),
								   errOmitLocation(true)));
		}
	}
	
	else if ( IsA(node, WindowRef) )
	{
		WindowRef *window_ref = (WindowRef *)node;
		WindowRef *new_ref = NULL;
		
		if ( context->cur_refinfo == NULL ) /* top-level WindowRef */
		{
			new_ref = copyObject(window_ref);

			context->cur_refinfo = palloc0(sizeof(RefInfo));			
			context->cur_refinfo->ref = new_ref;
			context->cur_refinfo->varset = NULL;
			lookup_window_function(context->cur_refinfo);
			
			new_ref->args = (List *)
				window_tlist_mutator((Node*)new_ref->args, context);

			/*
			 * If this function is a NTILE function, check if its argument
			 * contains expressions that are constant or those in the
			 * PARTITION BY clause.
			 */
			if (IS_NTILE(context->cur_refinfo->winkind))
			{
				WindowSpec *winspec = list_nth(context->win_specs, new_ref->winspec);
				
				check_ntile_argument(new_ref->args, winspec, context->orig_tlist);
			}
							
			/* Record the WindowRef and its Vars referenced set. */
			new_ref->winindex = list_length(context->refinfos);
			context->refinfos = lappend(context->refinfos, context->cur_refinfo);
			
			/* Clean up context. */
			context->cur_refinfo = NULL;
			
			return (Node*)new_ref;
		}
		else
		{
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Window function calls may not be nested."),
						   errOmitLocation(true)));
		}
	}
	
	return expression_tree_mutator(node, window_tlist_mutator, (void*) context);
}


/* inventory_window_specs
 * 
 * Take inventory of the supplied list of WindowSpecs in the given context. 
 * Add an array of distinct SpecInfo to the context.  Exclude SpecInfos that
 * are not referenced by any WindowRefs.  Afterward, the specindex field of
 * a RefInfo identifies the parent SpecInfo of its WindowRef.
 *
 * Note that preprocess_window_tlist() must already have run in order to
 * fill in the list refinfos list in context.
 */
static void inventory_window_specs(List *window_specs, WindowContext *context)
{
	ListCell *lcs, *lcp, *lcr;
	SpecInfo **index;
	int i, j, ndistinct;
	unsigned nspec, nextra;
	SpecInfo *specinfos;
	
	/* Make a preliminary pass over the refinfos to count how many
	 * extra specinfos we might need to add to those specified
	 * in the parse tree.
	 */
	nextra = 0;
	foreach ( lcr, context->refinfos )
	{
		RefInfo *rinfo = (RefInfo *)lfirst(lcr);
		if ( rinfo->winkind == WINKIND_LEAD || rinfo->winkind == WINKIND_LAG )
			nextra++;
	}
	
	/* Construct preliminary array of SpecInfo structs, one per WindowSpec
	 * plus possible extras as counted above.  Note that, at this point, 
	 * the order of the initial entries is the same as the entries in the
	 * Query's windowClause list.  Space for extras is allocated at the
	 * end and may be used as needed.
	 */
	nspec = list_length(window_specs);
	specinfos = palloc0((nspec+nextra) * sizeof(SpecInfo));
	i = 0;
	foreach ( lcs, window_specs )
	{
		WindowSpec *spec = (WindowSpec *) lfirst(lcs);
		Bitmapset *map = NULL;
		ListCell *lc;
		Bitmapset *orderset = NULL;
		
		foreach ( lcp, spec->partition )
		{
			SortClause *sc = (SortClause *) lfirst(lcp);
			map = bms_add_member(map, sc->tleSortGroupRef);
		}
		specinfos[i].specindex = i;
		specinfos[i].partset = map;
		specinfos[i].order = spec->order;
		specinfos[i].frame = spec->frame;
		specinfos[i].refset = NULL;
		specinfos[i].windowindex = 0;
		specinfos[i].keylevel = 0;
		specinfos[i].partkey = spec->partition;

		/* Construct unique_order for each SpecInfo by removing
		 * keys that are duplicates of partitioning keys and other
		 * ORDER BY keys.
		 */
		specinfos[i].unique_order = NIL;
		
		foreach (lc, specinfos[i].order)
		{
			SortClause *sc = (SortClause *)lfirst(lc);
			if (!bms_is_member(sc->tleSortGroupRef, specinfos[i].partset))
			{
				if (!bms_is_member(sc->tleSortGroupRef, orderset))
				{
					specinfos[i].unique_order =
						lappend(specinfos[i].unique_order, sc);
					orderset =
						bms_add_member(orderset, sc->tleSortGroupRef);
				}
			}
		}

		bms_free(orderset);

		i++;
	}
	
	/* Note which WindowRefs each WindowSpec covers by saving the indexes
	 * of their RefInfos.  Redirect WindowRefs with special framing needs
	 * to one of the extra SpecInfos allocated above.  In addition, note 
	 * in the context any occurence of a window function that requires an 
	 * auxiliary aggregate coplan. 
	 */
	i = 0;
	foreach ( lcr, context->refinfos )
	{
		RefInfo *rinfo = (RefInfo *)lfirst(lcr);
		WindowRef *ref = rinfo->ref;
		int sindex = ref->winspec;
		SpecInfo *sinfo = &specinfos[sindex];
		
		/* If Special Framing ... */
		if ( rinfo->winkind == WINKIND_LEAD || rinfo->winkind == WINKIND_LAG )
		{ 
			Node *offset = NULL;
			WindowFrame * frame = NULL;
			SpecInfo *xinfo = sinfo;
			
			/* Add a new "extra" SpecInfo for the specially framed window
			 * reference.  Note that we could be better by recognizing a
			 * previously added "extra" that meets our needs, but we
			 * don't do that yet.
			 */
			sindex = nspec++;
			sinfo = &specinfos[sindex];
			memcpy(sinfo, xinfo, sizeof(SpecInfo));
			sinfo->specindex = sindex;
			sinfo->refset = NULL; /* MPP-4836 */
			
			Assert( sinfo->frame == NULL );
			
			/* At the moment, the only special framing requirements are for
			 * LEAD and LAG.  These carry an offset as their second argument
			 * and, by now, the offset has be resolved to a Const node, if
			 * possible.
			 */	
			if ( list_length(ref->args) > 1 )
			{
				offset = (Node*)list_nth(ref->args,1);
			}
			else
			{
				Const *c = (Const*)makeNode(Const);
				c->consttype = INT8OID;
				c->constlen = 8;
				c->constvalue = 1;
				c->constisnull = false;
				c->constbyval = true;
				offset = (Node*)c;
			}
			
			frame = makeNode(WindowFrame);
			frame->system_generated = true;
			frame->is_between = true;
			frame->is_rows = true;
			frame->trail = makeNode(WindowFrameEdge);
			frame->lead = makeNode(WindowFrameEdge);

			switch ( rinfo->winkind )
			{
			case WINKIND_LAG:
				if ( IsA(offset,Const) )
					frame->trail->kind = frame->lead->kind = WINDOW_BOUND_PRECEDING;
				else
					frame->trail->kind = frame->lead->kind = WINDOW_DELAYED_BOUND_PRECEDING;
				break;
				
			case WINKIND_LEAD:
				if ( IsA(offset,Const) )
					frame->trail->kind = frame->lead->kind = WINDOW_BOUND_FOLLOWING;
				else
					frame->trail->kind = frame->lead->kind = WINDOW_DELAYED_BOUND_FOLLOWING;
				break;
				
			default:
				elog(ERROR, "internal window framing error");
			}
			
			frame->trail->val = frame->lead->val = offset;
			sinfo->frame = frame;
		}
		else if ( sinfo->frame != NULL )
		{
			if (sinfo->frame->lead &&
				sinfo->frame->lead->kind != WINDOW_DELAYED_BOUND_PRECEDING &&
				sinfo->frame->lead->kind != WINDOW_DELAYED_BOUND_FOLLOWING)
				sinfo->frame->lead = adjustFrameBound(sinfo->frame->lead, sinfo->frame->is_rows);
			if (sinfo->frame->trail &&
				sinfo->frame->trail->kind != WINDOW_DELAYED_BOUND_PRECEDING &&
				sinfo->frame->trail->kind != WINDOW_DELAYED_BOUND_FOLLOWING)
				sinfo->frame->trail = adjustFrameBound(sinfo->frame->trail, sinfo->frame->is_rows);
		}
		
		sinfo->refset = bms_add_member(sinfo->refset, i);
		
		rinfo->specindex = sinfo->specindex; /* in case this is an extra */
		
		if ( rinfo->isagg && sinfo->order == NIL )
			context->has_unordered_aggs = true;
		if ( rinfo->needcount && ! rinfo->isagg )
			context->has_deferred_window_fns = true;
		
		i++;
	}	
	
	/* Make an index to group by partition, order, and frame. */
	index = (SpecInfo**)palloc(nspec * sizeof(SpecInfo**));
	for ( i = 0; i < nspec; i++ )
		index[i] = &specinfos[i];
	qsort(index, nspec, sizeof(SpecInfo*), compare_spec_info_ptr);
	
	
	
	/* Identify distinct, referenced window specs and bubble up their
	 * refsets. Begin by finding the first referenced window spec and
	 * mark it to be the first  distinct representative window spec.  
	 * 
	 * Though not strictly necessary, we mark unreferenced window specs 
	 * with an index of nspec.
	 *
	 * In the second loop, j (via the index) indexes the current distinct 
	 * representative SpecInfo while i indexes the next candidate.
	 *
	 * Since processing moves forward through the index, we write the
	 * answer in its leading elements.
	 */
	for ( j = 0; j < nspec; j++ )
	{
		if ( ! bms_is_empty(index[j]->refset) )
			break;
		else
			index[j]->specindex = nspec; /* don't use */
	}
	Assert ( j < nspec ); /* there is a referenced specinfo */
	
	ndistinct = 0;
	index[j]->specindex = ndistinct;
	index[ndistinct++] = index[j];
	for ( i = j+1; i < nspec; i++ )
	{
		if ( compare_spec_info_ptr(&index[j],&index[i]) != 0 )
		{
			if ( bms_is_empty(index[i]->refset) )
			{
				index[i]->specindex = nspec; /* don't use */
				continue;
			}
			/* SpecInfo at index[i] is start of next run. */
			j = i;
			index[j]->specindex = ndistinct;
			index[ndistinct++] = index[j];
		}
		else
		{
			/* SpecInfo at index[i] is duplicate of last. */
			index[j]->refset = bms_union(index[j]->refset, index[i]->refset);
			index[i]->specindex = index[j]->specindex;
		}
	}
	
	/* Construct array of distinct windowspecs in the context. */
	context->nspecinfos = ndistinct;
	context->specinfos = palloc0(ndistinct * sizeof(SpecInfo));
	for ( j = 0; j < ndistinct; j++ )
	{
		Assert( index[j]->specindex == j );
		memcpy(&context->specinfos[j], index[j], sizeof(SpecInfo));
	}
	
	/* Adjust RefInfo specindex to index the parent SpecInfo in context. 
	 * Update summary info in the parent.
	 */
	foreach ( lcr, context->refinfos )
	{
		RefInfo *rinfo = (RefInfo *)lfirst(lcr);
		SpecInfo *sinfo = & specinfos[rinfo->specindex];
		
		Assert( sinfo->specindex < ndistinct );
		rinfo->specindex = sinfo->specindex;
	}		
	pfree(specinfos);
}


/* assign_window_info
 *
 * Assigns a WindowInfo to each group of compatible SpecInfo structures in
 * the context. The groups will be contiguous in array context->specinfos
 * due to the sort order established by inventory_window_specs.
 *
 * Ultimately each WindowInfo will correspond to a Window node and the
 * notion of "compatible" is based on the requirements of Window nodes.
 */
static void assign_window_info(WindowContext *context)
{
	int i, j, k, n;
	Index current;
	ListCell *lc;
	
	Assert( context->nspecinfos > 0 );
	
	/* First divide the previously produced, sorted list of distinct
	 * SpecInfo into groups with matching partitioning requirements 
	 * and so that each group member's ordering key is a prefix of 
	 * each following member's keys.
	 */
	context->specinfos[0].windowindex = current = 0;	
	for ( i = 1, j = 0; i < context->nspecinfos; j = i, i++ )
	{
		if ( ! bms_equal(context->specinfos[j].partset, context->specinfos[i].partset) 
			 || ! is_order_prefix_of(context->specinfos[j].unique_order, context->specinfos[i].unique_order) )
		{
			current++;
		}
		context->specinfos[i].windowindex = current;
	}
	n = current + 1;
	
	/* Next allocate a WindowInfo for each group and setup its requirements.
	 */
	context->nwindowinfos = n;
	context->windowinfos = (WindowInfo *)palloc0(n * sizeof(WindowInfo));
	for ( i = k = 0; i < n; i++ )
	{
		SpecInfo *final_sinfo = NULL;
		WindowInfo *winfo = &context->windowinfos[i];
		int sortgroupref = 0;
		
		j = k; /* j indexes first SpecInfo in this WindowInfo */
		
		for ( ; k < context->nspecinfos; k++ )
		{
			SpecInfo *sinfo = &context->specinfos[k];
			
			if ( sinfo->windowindex != i )
				break; /* k indexes first SpecInfo in next WindowInfo */
			final_sinfo = sinfo; /* has longest sort key so far */
		}
		
		Assert( final_sinfo != NULL );

		winfo->firstspecindex = j;
		winfo->numspecindex = k-j;

		winfo->sortclause = NIL;

		/* Append part keys into sortclause in the order defined by
		 * final_sinfo->partset. This guarantees that same part keys
		 * in all WindowInfos are in the same order.
		 */
		sortgroupref = bms_first_from(final_sinfo->partset, 0);
		while (sortgroupref >= 0)
		{
			foreach (lc, final_sinfo->partkey)
			{
				SortClause *sc = (SortClause *)lfirst(lc);
				
				if (sc->tleSortGroupRef == sortgroupref)
				{
					winfo->sortclause = lappend(winfo->sortclause, sc);
					break;
				}
			}

			Assert(lc != NULL);
			
			sortgroupref = bms_first_from(final_sinfo->partset, sortgroupref+1);
		}
		
		winfo->sortclause = list_concat(winfo->sortclause,
										final_sinfo->unique_order);
		winfo->partkey_len = list_length(final_sinfo->partkey);
		winfo->orderkeys_offset = list_length(final_sinfo->partkey);
	}
	
	/* Set up the partitioning and ordering key levels (WindowKeys)  */
	for ( i = 0; i < context->nwindowinfos; i++ )
	{	
		set_window_keys(context, i);
	}
	
	foreach ( lc, context->refinfos )
	{
		RefInfo *rinfo = (RefInfo*)lfirst(lc);
		SpecInfo *sinfo = &context->specinfos[rinfo->specindex];
		WindowInfo *winfo = &context->windowinfos[sinfo->windowindex];
		
		if ( sinfo->keylevel < 0 )
		{
			/* Unordered */
			Assert( sinfo->order == NIL );
			
			rinfo->ref->winlevel = list_length (winfo->key_list);
			if ( rinfo->isagg )
				winfo->needpartkey = true; /* unordered aggregation */
		}
		else
		{
			/* Ordered */
			Assert( sinfo->order != NIL );
			
			rinfo->ref->winlevel = sinfo->keylevel;
		}
		
		/* Check for window function in need of auxiliary count. */
		if ( !rinfo->isagg && rinfo->needcount )
		{
			winfo->needpartkey = true;
			winfo->needauxcount = true;
		}
	}
}


static bool is_order_prefix_of(List *sca, List *scb)
{
	ListCell *lca, *lcb;
	
	if ( list_length(sca) > list_length(scb) )
		return false;
	
	forboth(lca, sca, lcb, scb)
	{
		if ( !equal(lfirst(lca),lfirst(lcb)) )
			return false;
	}
	return true;
}



/* Comparision functions for local use */

static int compare_spec_info_ptr(const void *arg1, const void *arg2)
{
	int n;
	SpecInfo *a = *(SpecInfo **)arg1;
	SpecInfo *b = *(SpecInfo **)arg2;
	
	/* partition */
	n = bms_compare(a->partset, b->partset);
	if ( n != 0 ) 
		return n;
	
	/* order */
	n = compare_order(a->unique_order, b->unique_order);
	if ( n != 0 ) 
		return n;
	
	n = compare_order(a->order, b->order);
	if ( n != 0 ) 
		return n;

	/* frame */
	return compare_frame(a->frame, b->frame);
}

static int compare_order(List *a, List* b)
{
	ListCell *lca, *lcb;
	int na, nb;
	
	forboth ( lca, a, lcb, b )
	{
		SortClause *sca = (SortClause *)lfirst(lca);
		SortClause *scb = (SortClause *)lfirst(lcb);
		
		if ( sca->tleSortGroupRef < scb->tleSortGroupRef )
			return -1;
		else if ( sca->tleSortGroupRef > scb->tleSortGroupRef )
			return 1;
		else if ( sca->sortop < scb->sortop )
			return -1;
		else if ( sca->sortop > scb->sortop )
			return 1;
	}
	na = list_length(a);
	nb = list_length(b);
	if ( na < nb )
		return -1;
	else if ( na > nb )
		return 1;
	return 0;
}

static int compare_frame(WindowFrame *a, WindowFrame *b)
{
	int n;
	
	if ( a == b )
		return 0;
	else if ( a == NULL )
		return -1;
	else if ( b == NULL )
		return 1;
	
	if ( a->is_rows && ! b->is_rows )
		return -1;
	else if ( b->is_rows && ! a->is_rows )
		return 1;
	
	n = compare_edge(a->trail, b->trail);
	if ( n != 0 )
		return n;
		
	n = compare_edge(a->lead, b->lead);
	if ( n != 0 )
		return n;
	
	if ( a->exclude < b->exclude )
		return -1;
	else if ( a->exclude > b->exclude )
		return 1;
	
	return 0;
}

static int compare_edge(WindowFrameEdge *a, WindowFrameEdge *b)
{
	if ( a->kind < b->kind )
		return -1;
	else if ( a->kind > b->kind )
		return 1;
	else if ( equal(a->val, b->val) )
		return 0;
	/* When the bounds aren't equal (since they may be expressions) we
	 * just compare pointer values a->val and b->val.  The resulting order,
	 * though arbitrary, is consistent.
	 */
	else if ( ((void*)a->val) < ((void*)b->val) )
		return -1;

	return 1;
}


/* If the node is a Const with a value and we understand the value
 * well enough, make sure it is non-negative.
 *
 * The result is true, if the bound is constant and valid.
 * The result is false if we must delay checking until run time.
 * The function issues an error, if the bound is constant and invalid.
 */
static bool validateBound(Node *node, bool is_rows)
{
	Const *bound;
	Operator tup;
	Type typ;
	Datum zero;
	Oid funcoid;
	bool isNeg;
	
	if ( node == NULL || !IsA(node,Const) )
		return FALSE; /* Can't check here, wait until run time. */
	
	bound = (Const*)node;
	
	tup = ordering_oper(bound->consttype, TRUE);
	if ( !HeapTupleIsValid(tup) )
		return FALSE ; /* Can't check here, wait until run time. */
	
	typ = typeidType(bound->consttype);
	funcoid = oprfuncid(tup);
	zero = stringTypeDatum(typ, "0", exprTypmod(node));
	isNeg = OidFunctionCall2(funcoid, bound->constvalue, zero);
	
	if ( isNeg )						
		ereport(ERROR,
				(errcode(ERROR_INVALID_WINDOW_FRAME_PARAMETER),
				 errmsg("%s parameter cannot be negative", is_rows ? "ROWS" : "RANGE"),
						   errOmitLocation(true)));
	
	ReleaseOperator(tup);
	ReleaseType(typ);
	
	return TRUE;
}


/* Make any necessary adjustments to an ordinary window frame edge to
 * prepare it for later planning stages and for execution.
 *
 * Currently this is just resetting the window frame bound to delayed
 * if the value parameter can't be validated at planning time.  The
 * function issues an error if the value parameter is a negative 
 * constant.
 *
 * The function assumes the frame comes from the parser/rewriter so 
 * it will reject a delayed frame bound.  So don't use this to adjust 
 * edges of special frames such as those created for LAG/LEAD functions.
 */
static WindowFrameEdge *adjustFrameBound(WindowFrameEdge *edge, bool is_rows)
{
	WindowBoundingKind kind;
	
	if ( edge == NULL )
		return NULL;
		
	kind = edge->kind;
	
	if ( kind == WINDOW_BOUND_PRECEDING || kind == WINDOW_BOUND_FOLLOWING )
	{
		if ( validateBound(edge->val, is_rows) )
			;
		else
		{
			edge = copyObject(edge);
			if ( kind == WINDOW_BOUND_PRECEDING )
				edge->kind = WINDOW_DELAYED_BOUND_PRECEDING;
			else
				edge->kind = WINDOW_DELAYED_BOUND_FOLLOWING;
		}
	}
	else if ( edge->kind == WINDOW_BOUND_PRECEDING 
			|| edge->kind == WINDOW_BOUND_FOLLOWING
			|| edge->val != NULL )
	{
		elog(ERROR,"invalid window frame edge");
	}
	
	return edge;
}


/*---------------
 * make_lower_targetlist
 *
 * When window_planner inserts Window, Aggregate, or Result plan nodes
 * above the result of query_planner, we may want to pass a different
 * target list to query_planner than the outer plan nodes should have.
 * This routine generates the correct target list for the subplan.
 *
 * The initial target list passed from the parser already contains entries
 * for all PARTITION BY and ORDER BY expressions. We flatten all expressions
 * except these into their component variables; the other expressions
 * will be computed by the inserted nodes rather than by the subplan.
 * For example, given a query like
 *		SELECT a+b,SUM(c+d) OVER (ORDER BY a+b) FROM table;
 * we want to pass this targetlist to the subplan:
 *		a,b,c,d,a+b
 * where the a+b target will be used by the Sort/Window step, and the
 * other targets will be used for computing the final results.	(In the
 * above example we could theoretically suppress the a and b targets and
 * pass down only c,d,a+b, but it's not really worth the trouble to
 * eliminate simple var references from the subplan.  We will avoid doing
 * the extra computation to recompute a+b at the outer level; see
 * replace_vars_with_subplan_refs() in setrefs.c.)
 *
 * If we are grouping or aggregating, *and* there are no non-Var grouping
 * expressions, then the returned tlist is effectively dummy; we do not
 * need to force it to be evaluated, because all the Vars it contains
 * should be present in the output of query_planner anyway.
 *
 * The targetlist to be passed to the subplan goes in context->lower_tlist.
 *---------------
 */
static void
make_lower_targetlist(Query *parse,
					  WindowContext *context)
{
	List	   *tlist = parse->targetList;
	List	   *extravars = NIL;
	List	   *lower_tlist;
	bool		need_tlist_eval;
	int			i;
	SortClause *dummy;
	ListCell   *lc;

	Assert ( parse->hasWindFuncs );
	
	/* Start with a "flattened" tlist (having just the vars mentioned in 
	 * the targetlist or the window clause --- but no upper-level Vars; 
	 * they will be replaced  by Params later on).
	 */
	lower_tlist = flatten_tlist(tlist);
	
	/* Make sure Vars that may only occur in expression defining 
	 * delayed window frame edge bounds are represented.
	 */
	for ( i = 0; i < context->nspecinfos; i++ )
	{
		WindowFrame *f = context->specinfos[i].frame;
		if ( f == NULL )
			continue;
			
		if ( window_edge_is_delayed(f->trail) )
		{
			extravars = list_concat(extravars, 
							pull_var_clause(f->trail->val, false));
		}
		if ( window_edge_is_delayed(f->lead) )
		{
			extravars = list_concat(extravars, 
							pull_var_clause(f->lead->val, false));
		}
	}
	lower_tlist = add_to_flat_tlist(lower_tlist, extravars, false /* resjunk */);
	list_free(extravars);

	need_tlist_eval = false;
	
	/* Find or add target list entries for partitioning or ordering exprs
	 * mentioned in window specs to be fed by this targetlist. 
	 */
	dummy = makeNode(SortClause);
	for ( i = 0; i < context->nspecinfos; i++ )
	{
		TargetEntry *tle;
		int sortgroupref;
		List *extra_tles = NIL;
		
		bms_foreach ( sortgroupref, context->specinfos[i].partset )
		{
			dummy->tleSortGroupRef = sortgroupref;
			tle = get_sortgroupclause_tle(dummy, tlist);
			extra_tles = lappend(extra_tles, tle);
		}
		foreach ( lc, context->specinfos[i].order )
		{
			SortClause *sc = (SortClause *) lfirst(lc);
			tle = get_sortgroupclause_tle(sc, tlist);
			extra_tles = lappend(extra_tles, tle);
		}
			
		foreach ( lc, extra_tles )
		{
			ListCell *lct;
			TargetEntry *te;
			
			tle = (TargetEntry *)lfirst(lc);
			
			foreach (lct, lower_tlist)
			{
				te = (TargetEntry *)lfirst(lct);
				if (equal(tle->expr, te->expr) && 
					(te->ressortgroupref == tle->ressortgroupref ||
					 te->ressortgroupref == 0))
				{
					break;
				}
			}
			
			if ( lct != NULL )
			{
				/* Found one. */
				if ( te->ressortgroupref != tle->ressortgroupref )
				{
					Assert( te->ressortgroupref == 0 );
					te->ressortgroupref = tle->ressortgroupref;
				}
			}
			else
			{
				/* Need to add one. */
				te = copyObject(tle);
				te->resno = list_length(lower_tlist) + 1;
				lower_tlist = lappend(lower_tlist, te);
				/* NOTE that the addition must be an expr other than 
				 *  a simple Var.  Do we care? */
				need_tlist_eval = true;
			}
		}
		list_free(extra_tles);
	}
	pfree(dummy);
	dummy = NULL;
	
	build_sortref_index(lower_tlist, &context->max_sortref, &context->sortref_resno);

	context->lower_tlist = lower_tlist;
}


static void set_window_keys(WindowContext *context, int wind_index)
{
	WindowInfo *winfo;
	int i, j;
	int skoffset, nextsk;
	ListCell *lc;
	int nattrs;
	AttrNumber* sortattrs = NULL;
	Oid* sortops = NULL;
	
	/* results  */
	int partkey_len = 0;
	AttrNumber *partkey_attrs = NULL;
	List *window_keys = NIL;
	
	Assert( 0 <= wind_index && wind_index < context->nwindowinfos );
	
	winfo = &context->windowinfos[wind_index];

	/* Translate the sort clause to attribute numbers. */
	nattrs = list_length(winfo->sortclause);
	if ( nattrs > 0 )
	{
		sortattrs = (AttrNumber *)palloc(nattrs*sizeof(AttrNumber));
		sortops = (Oid *)palloc(nattrs*sizeof(Oid));
		i = 0;
		foreach ( lc, winfo->sortclause )
		{
			SortClause *sc = (SortClause *)lfirst(lc);
			sortattrs[i] = context->sortref_resno[sc->tleSortGroupRef];
			sortops[i] = sc->sortop;
			i++;
		}
	}
	
	/* Make a separate copy of just the partition key. */
	if ( winfo->partkey_len > 0 )
	{
		partkey_len = winfo->partkey_len;
		partkey_attrs = (AttrNumber *)palloc(partkey_len*sizeof(AttrNumber));
		for ( i = 0; i < partkey_len; i++ )
			partkey_attrs[i] = sortattrs[i];
	}
	
	/* Careful.  Within sort key, parition key may overlap order keys. */
	nextsk = skoffset = winfo->orderkeys_offset;
	
	/* Make a WindowKey per SpecInfo. */
	for ( i = 0; i < winfo->numspecindex; i++ )
	{
		WindowKey *wkey;
		SpecInfo *sinfo = &context->specinfos[winfo->firstspecindex + i];
		int keylen = list_length(sinfo->order);
		
		if ( keylen == 0 )
		{
			if ( sinfo->frame != NULL )
			{
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid window specification"),
					 errhint("Only ordered windows may specify ROWS or RANGE framing."),
							   errOmitLocation(true)));
			}

			sinfo->keylevel = -1;  /* No ordering key, just partition key. */
			continue;
		}

		keylen = list_length(sinfo->unique_order);

		wkey = makeNode(WindowKey);
		wkey->numSortCols = (keylen + skoffset) - nextsk;
		
		Assert( wkey->numSortCols >= 0 );
		
		if (  wkey->numSortCols > 0 )
		{
			wkey->sortColIdx = (AttrNumber*)palloc(wkey->numSortCols * sizeof(AttrNumber));
			wkey->sortOperators = (Oid*)palloc(wkey->numSortCols * sizeof(Oid));
			
			for ( j = 0; j < wkey->numSortCols; j++ )
			{
				wkey->sortColIdx[j] = sortattrs[nextsk];
				wkey->sortOperators[j] = sortops[nextsk]; /* TODO SET THIS CORRECTLY!!! */
				nextsk++;
			}
		}

		else
		{
			SortClause *sc;
			
			/* Copy the first ORDER BY key into SortCols. */
			wkey->numSortCols = 1;
			wkey->sortColIdx = (AttrNumber *)palloc(sizeof(AttrNumber));
			wkey->sortOperators = (Oid*)palloc(sizeof(Oid));
			sc = (SortClause *)linitial(sinfo->order);
			wkey->sortColIdx[0] = context->sortref_resno[sc->tleSortGroupRef];
			wkey->sortOperators[0] = sc->sortop;
		}
		
		wkey->frame = copyObject(sinfo->frame);
		sinfo->keylevel = list_length(window_keys); /* WindowKey position. */
		window_keys = lappend(window_keys,  wkey);
	}
	
	winfo->partkey_attrs = partkey_attrs;
	winfo->key_list = window_keys;
}

/* Fill in the fields of the given RefInfo that characterize the window 
 * function represented by its WindowRef. 
 */
static void
lookup_window_function(RefInfo *rinfo)
{
	HeapTuple	tuple;
	Form_pg_proc proform;
	bool isagg, iswin;
	cqContext	*procqCtx;
	Oid transtype = InvalidOid;
	
	Oid fnoid = rinfo->ref->winfnoid;
	
	/* pg_proc */
	procqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_proc "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(fnoid)));

	tuple = caql_getnext(procqCtx);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", fnoid);
	proform = (Form_pg_proc) GETSTRUCT(tuple);
	isagg = proform->proisagg;
	iswin = proform->proiswin;
	
	if ( (!isagg) && (!iswin) )
	{
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("can not call ordinary function, %s, as window function", NameStr(proform->proname)),
					   errOmitLocation(true)));
	}
	caql_endscan(procqCtx);
	
	Assert( isagg != iswin );
	
	if ( isagg )
	{
		Form_pg_aggregate		 aggform;
		cqContext				*aggcqCtx;

		aggcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_aggregate "
					" WHERE aggfnoid = :1 ",
					ObjectIdGetDatum(fnoid)));
		
		tuple = caql_getnext(aggcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for aggregate function %u", fnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(tuple);
		
		rinfo->isagg = true;
		rinfo->winkind = WINKIND_AGGREGATE;
		rinfo->hasinvtrans = (aggform->agginvtransfn != InvalidOid);
		rinfo->hasprelim = (aggform->aggprelimfn != InvalidOid);
		rinfo->hasinvprelim = (aggform->agginvprelimfn != InvalidOid);
		transtype = aggform->aggtranstype;

		caql_endscan(aggcqCtx);
		
		/*
		 * If the transition type is pass-by-reference, note the estimated 
		 * size of the value itself, plus palloc overhead.
		 */
		if (!get_typbyval(transtype))
		{
			int32		avgwidth;

			avgwidth = get_typavgwidth(transtype, -1);
			avgwidth = MAXALIGN(avgwidth);

			rinfo->trans_space += avgwidth + 2 * sizeof(void *);
		}
		
	}
	else /* iswin */
	{
		Form_pg_window	 winform;
		cqContext		*wincqCtx;

		wincqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_window "
					" WHERE winfnoid = :1 ",
					ObjectIdGetDatum(fnoid)));

		tuple = caql_getnext(wincqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for window function %u", fnoid);
		winform = (Form_pg_window) GETSTRUCT(tuple);

		rinfo->winkind = winform->winkind;
		rinfo->needcount = winform->wincount;
		rinfo->winpretype = winform->winpretype;
		rinfo->winfinfunc = winform->winfinfunc;		

		caql_endscan(wincqCtx);
	}
}

/*
 *
 */
static Plan *plan_common_subquery(PlannerInfo *root, List *lower_tlist,
								  List *order_hint,
								  CdbPathLocus *result_locus,
								  List **result_pathkeys
								  )
{
	Plan *result_plan = NULL;
	Path *cheapest_path = NULL;
	Path *sorted_path = NULL;
	Path *best_path = NULL;
	double num_groups = 0.0;
	QualCost tlist_cost;
	
	root->query_pathkeys = NIL;
	root->group_pathkeys = NIL;
	root->sort_pathkeys = NIL;
	
	/* If an order hint is specified, try for it.   */
	if ( order_hint != NIL )
	{
		root->query_pathkeys = make_pathkeys_for_sortclauses(order_hint, lower_tlist);
		root->sort_pathkeys = root->query_pathkeys;
	}


	/* Generate the best unsorted and presorted paths for this Query. */
	query_planner(root, lower_tlist, 0.0, 
				  &cheapest_path, &sorted_path, &num_groups);
					  
	if ( order_hint != NIL &&
		 sorted_path != NULL &&
		 (	CdbPathLocus_IsEntry(sorted_path->locus) ||
			CdbPathLocus_IsSingleQE(sorted_path->locus) ) )
	{
		best_path = sorted_path;
	}
	else
	{
		best_path = cheapest_path;
	}
	
	/* Make the plan. */
	result_plan = create_plan(root, best_path);
	result_plan = plan_pushdown_tlist(result_plan, lower_tlist);
	
	Assert(result_plan->flow);

	/* Account for the cost of evaluation of the lower tlist. */
	cost_qual_eval(&tlist_cost, lower_tlist, root);
	result_plan->startup_cost += tlist_cost.startup;
	result_plan->total_cost +=
		tlist_cost.startup +
		tlist_cost.per_tuple * result_plan->plan_rows;

	/* Set up results. */
	*result_locus = best_path->locus;
	*result_pathkeys = best_path->pathkeys;	
	return result_plan;
}


/* Assure that the given input plan meets the given distribution and sort order
 * requirements -- either by observing that it does or by adding Motion and/or 
 * Sort nodes to meet the requirements.
 *
 * Note that the function currently won't redistribute a plan that meets the
 * collocation requirement, e.g., since a singleton locus collocates on any 
 * partition key, the function won't "spread" such a plan's result across the 
 * segments.  Perhaps we should reconsider this.
 *
 * Returns the input plan, possibly with new nodes on top.  Input and output
 * must have flow declarations.
 *
 * The output locus for the result plan is also returned if output_locus is not NULL.
 */
Plan *assure_collocation_and_order(
		PlannerInfo *root,
		Plan *input_plan,
		int partkey_len,
		AttrNumber *partkey_attrs,
		List *sortclause,
		CdbPathLocus input_locus,
		CdbPathLocus *output_locus,  /*OUT*/
		List **pathkeys_ptr /*OUT*/
		)
{
	Plan *result_plan;
	List *sort_pathkeys = NULL;
	double motion_cost_per_row = (gp_motion_cost_per_row > 0.0) ?
					gp_motion_cost_per_row :
					2.0 * cpu_tuple_cost;

	Assert( input_plan && (input_plan->flow || IsA(input_plan, Motion)) );
	Assert( !CdbPathLocus_IsNull(input_locus));
	Assert( pathkeys_ptr && (*pathkeys_ptr == NIL || IsA(*pathkeys_ptr, List)) );
	
	result_plan = input_plan;
	if (output_locus != NULL)
		*output_locus = input_locus;
	
	if ( sortclause != NIL )
	{
		sort_pathkeys = make_pathkeys_for_sortclauses(sortclause, input_plan->targetlist);
		if ( root != NULL )
			sort_pathkeys = canonicalize_pathkeys(root, sort_pathkeys);
		Assert(sort_pathkeys != NULL);
	}
	
	if ( partkey_len == 0 ) /* Plan for single process locus. */
	{
		/* Assure sort order first */
		if(sort_pathkeys != NULL)
		{
			if(!pathkeys_contained_in(sort_pathkeys, *pathkeys_ptr))
			{
				result_plan = (Plan *) make_sort_from_sortclauses(root, sortclause, result_plan);
				*pathkeys_ptr = sort_pathkeys;
				mark_sort_locus(result_plan);
			}
		}

		/* bring to single locus */
		if( CdbPathLocus_IsPartitioned(input_locus))
		{
			result_plan = (Plan *) make_motion_gather_to_QE(result_plan, (*pathkeys_ptr != NULL));
			result_plan->total_cost += motion_cost_per_row * result_plan->plan_rows;
		}

		Assert(result_plan->flow);
	}
	else  /* Plan for hash distributed locus. */
	{
		List *dist_keys = NIL;
		List *dist_pathkeys = NIL;
		List *dist_exprs = NIL;
		ListCell *lc;
		int n;
		
		/* Get the required distribution path keys. */
		n = partkey_len;
		foreach (lc, sortclause)
		{
			if ( 0 >= n-- ) break;
			dist_keys = lappend(dist_keys, lfirst(lc));
		}
		dist_pathkeys = make_pathkeys_for_sortclauses(dist_keys, input_plan->targetlist);
		if ( root != NULL )
			dist_pathkeys = canonicalize_pathkeys(root, dist_pathkeys);
		
		/* Assure the required distribution. */
		if ( ! cdbpathlocus_collocates(input_locus, dist_pathkeys, false /*exact_match*/) )
		{
			foreach (lc, dist_keys)
			{
				SortClause *sc = (SortClause*)lfirst(lc);
				TargetEntry *tle =  get_sortgroupclause_tle(sc,input_plan->targetlist);
				dist_exprs = lappend(dist_exprs, tle->expr);
			}

			Insist(dist_exprs != NIL); /* since partkey is non-empty */
			
			result_plan = (Plan *) make_motion_hash(root, result_plan, dist_exprs);
			result_plan->total_cost += motion_cost_per_row * result_plan->plan_rows;
			*pathkeys_ptr = NIL; /* no longer sorted */
			Assert(result_plan->flow);

			/*
			 * Change output_locus based on the new distribution pathkeys.
			 */
			if (output_locus != NULL)
				CdbPathLocus_MakeHashed(output_locus, dist_pathkeys);
		}

		if(sortclause != NIL)
		{
			if(! pathkeys_contained_in(sort_pathkeys, *pathkeys_ptr))
			{
				result_plan = (Plan *) make_sort_from_sortclauses(root, sortclause, result_plan);
				*pathkeys_ptr = sort_pathkeys;
				mark_sort_locus(result_plan);
			}
		}
	}

	return result_plan;
}

/* Assure that the given input plan meets the sort order requirements 
 * by adding a Sort node, if necessary.
 *
 * Returns the input plan, possibly with a new Sort node on top.  
 * Input and output must have flow declarations.
 */
Plan *assure_order(
		PlannerInfo *root,
		Plan *input_plan,
		List *sortclause,
		List **pathkeys_ptr)
{
	Plan *result_plan;
	List *sort_pathkeys = NULL;

	Assert( input_plan && (input_plan->flow || IsA(input_plan, Motion)) );
	Assert( pathkeys_ptr && (*pathkeys_ptr == NIL || IsA(*pathkeys_ptr, List)) );
	
	result_plan = input_plan;

	if ( sortclause != NIL )
	{
		sort_pathkeys = make_pathkeys_for_sortclauses(sortclause, input_plan->targetlist);
		if ( root != NULL )
			sort_pathkeys = canonicalize_pathkeys(root, sort_pathkeys);
		Assert(sort_pathkeys != NULL);
	}

	if(sort_pathkeys != NULL)
	{
		if(!pathkeys_contained_in(sort_pathkeys, *pathkeys_ptr))
		{
			result_plan = (Plan *) make_sort_from_sortclauses(root, sortclause, result_plan);
			*pathkeys_ptr = sort_pathkeys;
			mark_sort_locus(result_plan);
		}
	}

	return result_plan;
}


Expr *make_mergeclause(Index varno, AttrNumber attrno, Node *expr)
{
	Oid type = exprType(expr);
	Node *lft = (Node*)makeVar(varno, attrno, type, -1, 0);
	Node *rgt = copyObject(expr);
	Expr *xpr = make_op(NULL, list_make1(makeString("=")), lft, rgt, -1);
	
	xpr->type = T_DistinctExpr;
	
	return make_notclause(xpr);
}


/* Plan a window query that can be implemented without any joins or 
 * input sharing, i.e., it involves one scan of the input plan result
 * and a single Window node.
 *
 * Returns plan and, implicitly, pathkeys.
 */
static Plan *plan_trivial_window_query(PlannerInfo *root, WindowContext *context, List **pathkeys_ptr)
{
	Plan *result_plan;
	WindowInfo *winfo;
	List *lower_tlist = context->lower_tlist;
	CdbPathLocus input_locus, output_locus;
	List *pathkeys = NIL;
	List *window_pathkeys = NIL;	
	
	Assert ( pathkeys_ptr );
	Assert (context->nwindowinfos == 1);
	winfo = context->windowinfos;


	result_plan = plan_common_subquery(root, lower_tlist,
								  winfo->sortclause, /* order hint */
								  &input_locus,
								  &pathkeys);
								  
	
	window_pathkeys = make_pathkeys_for_sortclauses(winfo->sortclause, lower_tlist);
	window_pathkeys = canonicalize_pathkeys(root, window_pathkeys);
	
	/* Assure needed colocation and order. */
	result_plan = assure_collocation_and_order(root,
											   result_plan,
											   winfo->partkey_len,
											   winfo->partkey_attrs,
											   winfo->sortclause,
											   input_locus,
											   NULL,
											   &pathkeys);
	
	/* Add the single Window node. */
	result_plan = (Plan*) make_window(root, context->upper_tlist, 
									  winfo->partkey_len, winfo->partkey_attrs,
									  winfo->key_list, /* XXX copy? */
									  result_plan);
	
	/* 
	 * Add a Flow node to the top node, if it doesn't have one yet.
	 * Retrieve distribution info from the next-to-top node which
	 * must have a Flow node.
	 */
	if (!result_plan->flow)
		result_plan->flow = pull_up_Flow(result_plan, 
										 result_plan->lefttree, 
										 (pathkeys != NIL));

	/* TODO Check our API.
	 *
	 * Note: locus and pathkeys may be an important implicit result. 
	 */
	CdbPathLocus_MakeSingleQE(&output_locus); /* We don't partition yet. */
	
	*pathkeys_ptr = pathkeys;
	return result_plan;
}


/* Construct a sequential plan for a non-trivial window query.  A non-trivial
 * window query is one that involves more than one partition/sort order or
 * that contains deferred window functions.
 *
 * A sequential plan implements the query as a sequence of stages, each of
 * which adds partial results corresponding to window functions to the 
 * relation under contruction.  At the top these partial results are
 * combined to produce the final result.
 * 
 * Aggregations (for deferred window functions or unordered aggregate window
 * functions) are immediately joined back into the relation under construction
 * via a locally generated MergeJoin, i.e., the join optimizer isn't involved.
 *
 * Returns plan and, implicitly, pathkeys.
 */
static Plan *plan_sequential_window_query(PlannerInfo *root, WindowContext *context, List **pathkeys_ptr)
{
	Plan *result_plan;
	CdbPathLocus locus;
	List *pathkeys = NIL;
	List *targetlist;
	List *sort_hint;
	int hi, lo;
	TargetEntry *tle;
	AttrNumber resno;
	ListCell *lc;
	QualCost tlist_cost;
		
	Assert ( pathkeys_ptr != NULL );
	Assert ( context->original_range );
	
	/* Plan common subquery. */
	sort_hint = context->windowinfos[context->nwindowinfos-1].sortclause;
	result_plan = plan_common_subquery(root, context->lower_tlist,
									   sort_hint,
									   &locus,
									   &pathkeys);
	
	/* Record common subquery information in context. 
	 *
	 * It's not clear that we need this for the sequential strategy.
	 * Set the input parse tree for plan_sequential_stage().
	 */
	context->subplan = result_plan;
	context->subplan_locus = locus;
	context->subplan_pathkeys = pathkeys;
	context->subquery = copy_common_subquery(root->parse, context->lower_tlist);
		
	/* Be tidy: no row key used in the sequential strategy. */
	context->rowkey_len = 0;
	context->rowkey_attrs = NULL;	
	context->keyed_lower_tlist = NULL;	
	
	/* Initialize an array to convert the index of a Var from the lower 
	 * range to its position (resno) in the target list we are producing.  
	 * This will be used later by translate_upper_tlist_sequential to mutate 
	 * Vars on the original range table (from the original target list) to 
	 * Vars on the final range table.
	 */	
	resno = 0;
	context->offset_upper_varattrnos = palloc0(context->offset_lim * sizeof(AttrNumber));
	
	foreach ( lc, context->lower_tlist )
	{
		tle = (TargetEntry *)lfirst(lc);
		resno++;
		
		/* This assumption later allows us to copy key attributes. */
		Assert( resno == tle->resno ); 
		
		if ( IsA(tle->expr, Var) )
		{
			Var *var = (Var*)tle->expr;
			context->offset_upper_varattrnos[index_of_var(var,context)] = resno;
		}
	}
	
	/* 
	 * A stage is a set of WindowInfos with the same partitioning.
	 *
	 * We construct plan stages from the last window info to the first.
	 * This is because the window info sort places non-partitioned windows 
	 * first and we prefer to process these last. 
	 */
	for ( hi = context->nwindowinfos - 1; hi >= 0; hi = lo - 1 )
	{
		int j;
		Bitmapset *partset = context->specinfos[context->windowinfos[hi].firstspecindex].partset;

		lo = hi;		
		for ( j = hi-1; j  >= 0; j-- )
		{
			WindowInfo *winfo = &context->windowinfos[j];
			
			if ( ! bms_equal( context->specinfos[winfo->firstspecindex].partset, partset ) )
				break;
				
			lo = j;
		}
		
		/* Plan a stage for WindowInfos from hi to lo. */
		result_plan = plan_sequential_stage(root, 
											context, 
											hi, lo, 
											result_plan, 
											&locus, 
											&pathkeys
											);
	}
			
	/* Mutate the upper target list to compute the final result. */
	targetlist = translate_upper_tlist_sequential(context->upper_tlist, 
												  result_plan->targetlist, 
												  context);
	
	/* Adjust root so the table expression portion of the parse tree looks 
	 * like the output of the final stage. */
	root->parse->rtable = context->subquery->rtable;
	root->parse->jointree = context->subquery->jointree;

	/*
	 * Since rtable may be changed, reconstruct RelOptInfo nodes for all base relations
	 * in the root query to match this change.
	 */
	if (root->simple_rel_array)
		pfree(root->simple_rel_array);
	root->simple_rel_array_size = list_length(root->parse->rtable) + 1;
	root->simple_rel_array = (RelOptInfo **)
		palloc0(root->simple_rel_array_size * sizeof(RelOptInfo *));
	add_base_rels_to_query(root, (Node *)root->parse->jointree);

	/* XXX I don't think the quals are used later so no need to translate. 
	 *     They would either be empty or represent a join on the last
	 *     partitioning key processed by plan_sequential_stage.
	 *
	 * XXX There won't be any grouping stuff because of the semantic
	 *     transformation that pushes grouping below windowing.  There
	 *     may, however, be expression to translate in the limit/offset
	 *     clauses.  Distinct and sort use sort/group refs which are
	 *     preserved in the targetlist.
	 */

	/* Set target list and adjust costs since it may have expressions.  
	 *
	 * XXX Could track this and avoid, but not yet.
	 */
	root->parse->targetList = targetlist;
	result_plan = plan_pushdown_tlist(result_plan, targetlist);
	cost_qual_eval(&tlist_cost, targetlist, root);
	result_plan->startup_cost += tlist_cost.startup;
	result_plan->total_cost += tlist_cost.startup + tlist_cost.per_tuple * result_plan->plan_rows;

	Assert( result_plan->flow != NULL );

	*pathkeys_ptr = pathkeys;
	return result_plan;
}


/* Plan a single stage of a sequential window query plan.
 *
 * By construction, all the window functions computed by a stage have the 
 * same PARTITION BY specification.  They are represented by contiguous
 * WindowInfo structures (from lo_windex to hi_windex) in the WindowContext.
 *
 * A stage comprises a chain of Window nodes (with an initial motion and 
 * intervening sorts as required) and, if needed, an Agg node which is 
 * merge-joined to the head of the Window node chain on the common partition 
 * key.  By construction, this key is the high-order term of any sorts.
 *
 * Thus a simple (no Agg) stage looks like
 *
 *   input->Motion->Sort->Window->...->Sort->Window->
 * 
 * With aggregation, this becomes
 *
 *   input->Motion->Sort->Share->Window->...->Sort->Window-+->Join->
 *                            |                            |
 *                            +------------>Agg------------+
 *
 * The input plan is the window plan to date.  Some invariants are
 * - the lower target list is a prefix of the input plan target list
 * - the lower range table is a prefix of the input plan range table.
 * The input plan may have additional targets and range table entries
 * added by previous planning stages. 
 */
static Plan *plan_sequential_stage(PlannerInfo *root, 
								   WindowContext *context, 
								   int hi_windex, int lo_windex, 
								   Plan *input_plan,
								   CdbPathLocus *locus_ptr,
								   List **pathkeys_ptr )
{
	Plan *window_plan;
	Plan *agg_plan = NULL;
	WindowInfo *winfo;
	int i;
	ListCell *lc;
	Index win_varno = 1;
	Index agg_varno = 2;
	AttrNumber aux_attrno = 0;
	AttrNumber agg_attrno = 0;
	List * win_names = NIL; /* for Window sub-query RTEs */
	List * agg_names = NIL; /* for Agg sub-query RTE */
	List * join_tlist = NIL;
	Query *agg_subquery;
	bool hasaux, hasagg;
	CdbPathLocus input_locus = *locus_ptr;

	/* In-Out parameters */
	List *pathkeys = *pathkeys_ptr;
	
	/* elog(NOTICE, "Start of plan_sequential_stage()."); */

	/* Determine whether this stage requires external aggregation and an
	 * auxilliary aggregate. 
	 */
	{
		hasaux = false;
		hasagg = false;

		for ( i = lo_windex; i <= hi_windex; i++ )
		{
			winfo = &context->windowinfos[i];
			hasaux = hasaux || winfo->needauxcount;
			hasagg = hasagg || hasaux || winfo->needpartkey;
		}
	}
	
	/* Derive names for the targets in the initial Window subquery and,
	 * if we'll use a join, set up the beginning of the join targetlist. 
	 */
	i = 1;
	foreach ( lc, input_plan->targetlist )
	{
		Value *tle_name;
		TargetEntry *tle = (TargetEntry*)lfirst(lc);
		AttrNumber resno = i++;
		
		StringInfo buf = makeStringInfo();
		appendStringInfo(buf, "unnamed_attr_%d", resno);
		
		tle_name = get_tle_name(tle, context->subquery->rtable, buf->data);
		win_names = lappend(win_names, tle_name);
		
		pfree(buf->data);
		pfree(buf);
		buf = NULL;
		
		if ( hasagg )
		{
			Var *join_var = makeVar(win_varno, resno, 
									exprType((Node*)tle->expr), 
									exprTypmod((Node*)tle->expr), 0);
			
			char *cname = pstrdup(tle_name->val.str);
									
			TargetEntry *join_tle = makeTargetEntry((Expr*)join_var,
													resno,
													cname,
													false);
			join_tle->ressortgroupref = tle->ressortgroupref;
			join_tlist = lappend(join_tlist, join_tle);
		}
	}


	/*
	 * Assure collocation on the partition key and ordering for the first
	 * WindowInfo in the stage.
	 */
	winfo = &context->windowinfos[lo_windex];
	window_plan = assure_collocation_and_order(root,
											   input_plan,
											   winfo->partkey_len,
											   winfo->partkey_attrs,
											   winfo->sortclause,
											   input_locus,
											   locus_ptr,
											   &pathkeys);
	
	if ( hasagg )
	{
		AggStrategy strategy = AGG_PLAIN;
		long num_groups = 1;
		AttrNumber *grpcolidx = NULL;
		List *share_partners = NIL;
		List *tlist = NIL;
			
		/* elog(NOTICE, "Fn plan_sequential_stage(): Preparing for Agg."); */

		/* Since we'll be encountering some Agg targets.  Prepare for that 
		 * by sharing the input window plan locally (within the slice) so
		 * we can develop an Agg and a Window chain within the stage. 
		 */
		share_partners = share_plan(root, window_plan, 2);
		window_plan = list_nth(share_partners, 0);
		agg_plan = list_nth(share_partners, 1);
		list_free(share_partners);
		
		/* An agg plan will have a base subquery like the first window plan. */
		agg_subquery = copyObject(context->subquery);
		
		/* If there's a partition key, we'll group on that, else default
		 * to a plain aggregation.
		 */
		if ( winfo->partkey_len > 0 )
		{
			size_t sz;
			
			/* Estimate the number of parts in the partition.  Note that
			 * we use the global root for this, but retain only the estimate.
			 */
			{
				int i;
				double d;
				List *part_exprs = NIL;
			
				for ( i = 0; i < winfo->partkey_len; i++ )
				{
					TargetEntry *tle; 
					tle = get_tle_by_resno(context->lower_tlist,
										   winfo->partkey_attrs[i]);

					part_exprs = lappend(part_exprs, tle->expr);
				}
				d = estimate_num_groups(root, part_exprs, window_plan->plan_rows);
				num_groups = (d<0)? 0: (d>LONG_MAX)? LONG_MAX: (long)d;
				list_free(part_exprs);
			}

			/* Set up for grouped aggregation */
			strategy = AGG_SORTED;
			sz = winfo->partkey_len * sizeof(AttrNumber);
			grpcolidx = (AttrNumber*)palloc(sz);
			for ( i = 0; i < winfo->partkey_len; i++ )
			{
				TargetEntry *tle; 			
				tle = get_tle_by_resno(window_plan->targetlist, 
									   winfo->partkey_attrs[i]);

				tle = copyObject(tle);
				grpcolidx[i] = tle->resno;
				tle->resno = i+1;
				tlist = lappend(tlist, tle);
				agg_names = lappend(agg_names,
								    makeString(pstrdup(tle->resname? 
											   tle->resname:
											   "part_key")));
			}			
		}
		
		/* If any WindowRefs need a partition count (auxiliary aggregate)
		 * add a target for it and remember where it is.
		 */
		if ( hasaux )
		{
			TargetEntry *tle;
			Aggref *aggref = makeAuxCountAggref();
			aux_attrno = 1 + list_length(tlist);
			tle = makeTargetEntry((Expr*)aggref, aux_attrno, pstrdup("partition_count"), false);
			
			tlist = lappend(tlist, tle);
			agg_names = lappend(agg_names, makeString(pstrdup(tle->resname)));
		}
		
		/* Put the Agg node atop the Agg plan. It's targetlist is, however,
		 * incomplete at this point.  We'll fill it in as we go.
		 */
		agg_plan = (Plan *)make_agg(
					root,
					tlist, /* just partkey (if any) at this point */
					NIL, /* qual */
					strategy, false,
					winfo->partkey_len, 
					grpcolidx,
				    num_groups,
				    0, /* num_nullcols */
				    0, /* input_grouping */
				    0, /* grouping_id */
				    0, /* rollup_gs_times */
					0, /* numAggs */
					0, /* transSpace */
					agg_plan /* now just the shared input */
					);
		
		agg_plan->flow = pull_up_Flow(agg_plan, agg_plan->lefttree, true);
		
		/* Later we'll package this Agg plan as the second sub-query RTE
		 * in a fake Query representing a two-way join of Window sub-query 
		 * to Agg sub-query. We keep track of the attribute number (resno) 
		 * of the next Aggref target we'll add to the Agg plan.
		 */
		 agg_attrno = 1 + list_length(agg_plan->targetlist);
	}
	else
	{
		agg_subquery = NULL; /* tidy */
	}
	
	/* Put Window node (and any required Sort) atop the Window plan for each
	 * WindowInfo in the stage.
	 */
	for ( i = lo_windex; i <= hi_windex; i++ )
	{
		int j;
		winfo = &context->windowinfos[i];

		if ( i > lo_windex )
		{
			/* [Re]condition input for this window info.  At most this may
			 * add a Sort node, but the partitioning and the partitioning
			 * sort order will be preserved so we don't need to reSort the
			 * agg_plan (if any).
			 *
			 * XXX If we had a partitioned Sort operator, we could use
			 *     it here on a partitioned window plan.
			 */
			window_plan = assure_order(root,
									   window_plan,
									   winfo->sortclause,
									   &pathkeys);
		}
		 
		/* Initialize a Window node for this WindowInfo. 
		 */
		{
			size_t sz = winfo->partkey_len * sizeof(AttrNumber);
			AttrNumber *partkey = NULL;
			
			/* elog(NOTICE, "Fn plan_sequential_stage(): Adding Window node."); */

			if ( winfo->partkey_len > 0 )
			{
				partkey = (AttrNumber*)palloc(sz);
				memcpy(partkey, winfo->partkey_attrs, sz);
			}
			
			window_plan = (Plan *)make_window(
							root,
							copyObject(window_plan->targetlist),
							winfo->partkey_len,
							partkey,
							(List*) translate_upper_vars_sequential((Node*)winfo->key_list, context), /* XXX mutate windowKeys to translate any Var nodes in frame vals. */
							window_plan);
							
			window_plan->flow = pull_up_Flow(window_plan, window_plan->lefttree, true);
		}

		
		/* Add a target to the window plan (and, if needed, the agg plan)
		 * for each RefInfo controlled by each SpecInfo of the current
		 * WindowInfo.
		 */
		for ( j = 0; j < winfo->numspecindex; j++ )
		{
			ListCell *lc;
			SpecInfo *sinfo = &context->specinfos[j+winfo->firstspecindex];
			
			/* Per RefInfo */
			foreach ( lc, context->refinfos )
			{
				WindowRef *ref;
				Aggref *aggref;
				AttrNumber win_resno;
				Var *var;
				TargetEntry *tle;
				RefInfo *rinfo = (RefInfo *)lfirst(lc);
				
				if ( rinfo->specindex != sinfo->specindex )
					continue;
					
				ref = (WindowRef*)translate_upper_vars_sequential((Node*)rinfo->ref, context); /* XXX mutate ref's args to translate any Vars */
				win_resno = 1 + list_length(window_plan->targetlist);
				rinfo->resno = win_resno;


				/* Finish target based on function type. */
				if ( RefInfo_AggregateUnordered(rinfo, context) )
				{
					/* Plan unordered aggregate */
					char *agg_resname;
					AttrNumber agg_resno = agg_attrno++;
					
					/* Add target to window plan. */
					tle = makeTargetEntry((Expr*)makeNullConst(ref->restype, -1),
										  win_resno, 
										  pstrdup("dummy"), 
										  false);
					window_plan->targetlist = lappend(window_plan->targetlist, tle); 
					win_names = lappend(win_names, get_tle_name(tle, context->subquery->rtable, NULL));
				
					
					/* Add target to the aggregation plan */
					aggref = makeWindowAggref(ref);
					agg_resname = get_function_name(aggref->aggfnoid, "partition_aggregate");					

					tle = makeTargetEntry((Expr*)aggref, agg_resno, agg_resname, false);
					agg_plan->targetlist = lappend(agg_plan->targetlist, tle);
					agg_names = lappend(agg_names, makeString(pstrdup(tle->resname)));
					
					Assert( list_length(agg_names) == list_length(agg_plan->targetlist) );
					Assert( agg_resno == list_length(agg_plan->targetlist) );
					
					/* Result is reference to joined aggregate value. */
					var = makeVar(agg_varno, agg_resno, ref->restype, -1, false);
					tle = makeTargetEntry((Expr *)var, win_resno, 
										   get_function_name(ref->winfnoid, "window_function"), 
										   false);
					join_tlist = lappend(join_tlist, tle);
				}
				else if ( RefInfo_WindowDeferred(rinfo, context) )
				{
					/* Plan deferred window function */
					FuncExpr *func;
					Var *win_var;
					Var *aux_var;
					Oid counttype = 20; /* TODO count(*) type in pg_proc */
					Oid restype = ref->restype;
					
					/* Adjust WindowRef */
					ref->winstage = WINSTAGE_PRELIMINARY;
					ref->restype = rinfo->winpretype;
					
					/* Add target to window plan. */
					tle = makeTargetEntry((Expr*)ref, 
										  win_resno, 
										  get_function_name(ref->winfnoid, "window_function"), 
										  false);
					window_plan->targetlist = lappend(window_plan->targetlist, tle); 
					win_names = lappend(win_names, get_tle_name(tle, context->subquery->rtable, NULL));
					
					/* Result is finalization of partial window with joined auxiliary count. */
					win_var = makeVar(win_varno, win_resno, ref->restype, -1, false);					
					aux_var = makeVar(agg_varno, aux_attrno, counttype , -1, false);
					func = makeFuncExpr(rinfo->winfinfunc, restype, list_make2(win_var, aux_var), COERCE_DONTCARE);
					tle = makeTargetEntry((Expr*)func, win_resno,
										   get_function_name(ref->winfnoid, "window_function"),
										   false);
					join_tlist = lappend(join_tlist, tle);
				}
				else if ( RefInfo_WindowImmediate(rinfo, context) || 
						  RefInfo_AggregateOrdered(rinfo, context) )
				{
					/* Plan immediate window function or ordered window agg) */
					Var *win_var;
					
					/* Adjust WindowRef */
					ref->winstage = WINSTAGE_IMMEDIATE;

					/* Add target to window plan. */
					tle = makeTargetEntry((Expr*)ref, 
										  win_resno, 
										  get_function_name(ref->winfnoid, "window_function"), 
										  false);
					window_plan->targetlist = lappend(window_plan->targetlist, tle); 
					win_names = lappend(win_names, get_tle_name(tle, context->subquery->rtable, NULL));
				
					/* If the stage has a join, add a join target. */
					if ( hasagg )
					{
						win_var = makeVar(win_varno, win_resno, ref->restype, -1, false);
						tle = makeTargetEntry((Expr*)win_var, win_resno,
											  get_function_name(ref->winfnoid, "window_function"),
											  false);
						join_tlist = lappend(join_tlist, tle);
					}
				}
			}
		}
		
		/* Now the next Window node in this stage's chain is ready. 
		 * Package it as a subquery RTE for use in the phony query we'll 
		 * produce as the next level in the stage.  This will be either 
		 * a simple select from one subquery (in the case of an intermediate
		 * level or a top-level with no aggregation), or a join of this RTE
		 * and the Agg RTE produced at the end of this stage. 
		 */
		{
			Query *wquery;
			
			window_plan = wrap_plan(root, window_plan, 
									context->subquery, &pathkeys, 
									"coplan", win_names, 
									&wquery);
			
			context->subquery = wquery; /* the "input query" for the next time through */
			context->original_range = false; /* range is now our introduced range */
		}
	}
	
	
	if ( hasagg )
	{
		Plan *join_plan = NULL;	
		
		agg_plan = add_join_to_wrapper(root, agg_plan, agg_subquery, join_tlist,
									   winfo->partkey_len,
									   "coplan", agg_names,
									   context->subquery);
		agg_subquery = NULL;
		
		/* Finally, join the aggregation query to the main query.  
		 * This will be either be cartesian product (NestLoop) or 
		 * a many-to-one join on the partitioning key (MergeJoin).
		 * In both cases, the aggregation query should be the outer
		 * plan since this is more efficient for joining a single
		 * tuple (outer) to a sequence of consecutive tuples (inner).
		 *
		 * Make any necessary adjustments to root->parse (e.g., push 
		 * a Query into  its rangetable).
		 *
		 * Return the result in window_plan
		 */		
		 if ( winfo->partkey_len > 0 )
		 {
			List *mergeclauses = NIL;
			
			for ( i = 0; i < winfo->partkey_len; i++ )
			{
				TargetEntry *tle;
				Expr *mc;
				Node *rgt;
				
				tle = get_tle_by_resno(window_plan->targetlist, winfo->partkey_attrs[i]);
				rgt = (Node*)tle->expr;

				mc = make_mergeclause(agg_varno, i+1, copyObject(rgt));
				mergeclauses = lappend(mergeclauses, mc);
			}
			
			join_plan = (Plan *)make_mergejoin(join_tlist,
											   NIL, NIL,
											   mergeclauses,
											   agg_plan, window_plan,
											   JOIN_INNER);
			((MergeJoin*)join_plan)->unique_outer = true;
		 }
		 else
		 {
			/* Cartesian product: 1 x MANY. */
			join_plan = (Plan *)make_nestloop(join_tlist,
											   NIL, NIL,
											   agg_plan, window_plan,
											   JOIN_INNER);
			((NestLoop*)join_plan)->singleton_outer = true;
		 }
		join_plan->startup_cost = agg_plan->startup_cost + window_plan->startup_cost;
		join_plan->plan_rows = window_plan->plan_rows;
		join_plan->plan_width = agg_plan->plan_width + window_plan->plan_width;
		
		join_plan->total_cost = agg_plan->total_cost + window_plan->total_cost;
		join_plan->total_cost += cpu_tuple_cost * join_plan->plan_rows;
		
		join_plan->flow = pull_up_Flow(join_plan, join_plan->righttree, true);
		window_plan = join_plan;
	}	

	*pathkeys_ptr = pathkeys;

	/* elog(NOTICE, "End of plan_sequential_stage()."); */
	return window_plan;
}


/* Construct a parallel plan for a non-trivial window query.  A non-trivial
 * window query is one that involves more than one partition/sort order or
 * that contains deferred window functions.
 *
 * A parallel plan implements the query as a top-level join of a set of
 * independent "coplans" that compute portions of the final target list.
 * The coplans usually take their input from the common subquery via input
 * sharing.
 *
 * Returns plan and, implicitly, pathkeys.
 */
static Plan *plan_parallel_window_query(PlannerInfo *root, WindowContext *context, List **pathkeys_ptr)
{
	Plan *subplan;
	Plan *result_plan;
	List *result_pathkeys;
	CdbPathLocus input_locus;
	List *pathkeys = NIL;
	List *targetlist;
	List *rtable;
	FromExpr *jointree;
	
	int i;
	
	Assert ( pathkeys_ptr != NULL );

	subplan = plan_common_subquery(root, context->lower_tlist,
								  NULL, /* order hint */
								  &input_locus,
								  &pathkeys);
	
	/* If necessary, add row key to the lower plan. */
	if ( context->nwindowinfos > 1 )
	{
		AttrNumber resno;
		ListCell *lc;
		List *lower_tlist = copyObject(context->lower_tlist);
		List *rowkey_tlist = make_rowkey_targets();
		
		context->rowkey_len = list_length(rowkey_tlist);
		context->rowkey_attrs = palloc(context->rowkey_len * sizeof(AttrNumber));
			
		resno = list_length(lower_tlist); /* XXX safe? */
		i = 0;
		foreach ( lc, rowkey_tlist )
		{
			TargetEntry *tle = (TargetEntry *)lfirst(lc);
			tle->resno = ++resno;
			context->rowkey_attrs[i++] = resno;
		}
		lower_tlist = list_concat(lower_tlist, rowkey_tlist);
		context->keyed_lower_tlist = lower_tlist;
		
		subplan = (Plan*) make_window(root, lower_tlist, 
										  0, NULL, /* No paritioning */
										  NIL, /* No ordering */
										  subplan);
		if (!subplan->flow)
			subplan->flow = pull_up_Flow(subplan, 
											 subplan->lefttree, 
											 (pathkeys != NIL));
	}
	else
	{
		context->rowkey_len = 0;
		context->rowkey_attrs = NULL;	
		context->keyed_lower_tlist = context->lower_tlist;	
	}
	
	/* Record common subquery information in context. */
	context->subplan = subplan;
	context->subplan_locus = input_locus;
	context->subplan_pathkeys = pathkeys;
	
	/* Generate coplans for each WindowInfo */								  								  
	context->coplan_count = 0;
	
	for ( i = 0; i < context->nwindowinfos; i++ )
	{
		plan_windowinfo_coplans(root, context, i);
	}
	
	/* Construct the range table for the join query to be constructed. */
	rtable = plan_window_rtable(root, context);
	
	/* Mutate the upper target list to refer to the new range table. */
	targetlist = translate_upper_tlist_parallel(context->upper_tlist, context);
	
	/* Arrange to join the coplans back together. */
	jointree = plan_window_jointree(root,  context);
	
	/* XXX This is the evil part!
	 *
	 * Modify the input query to look like a new upper level join
	 * query.  Retain what's above  the jointree (ordering, etc)
	 * from the original.  This works because the targetlist is
	 * "conformable" with the original w.r.t. sort/group refs,
	 * resnos, types, etc.
	 *
	 * We don't need to descend into the range table to fix varlevelsup
	 * due to the new query level we're adding, because the range table
	 * has already been planned. (Right?)
	 */
	root->parse->targetList = targetlist;
	root->parse->rtable = rtable;
	root->parse->jointree = jointree;
	root->parse->windowClause = NIL;
	/*
	 * since we modify the upper level query, the in_info_list is not valid
	 * anymore, and needs to be released (MPP-21017)
	 */
	list_free(root->in_info_list);
	root->in_info_list = NIL;
	
	/* Plan the join.
	 */
	{
		Path *cheapest_path;
		Path *best_path;
		Path *sorted_path;
		double ngroups;
		QualCost tlist_cost;
						
		root->group_pathkeys = NIL;
		root->sort_pathkeys = 
			make_pathkeys_for_sortclauses(root->parse->sortClause, targetlist);
		root->query_pathkeys = root->sort_pathkeys;
		
		query_planner(root, targetlist, 0.0, &cheapest_path, &sorted_path, &ngroups);
		
		if ( sorted_path != NULL )
			best_path = sorted_path;
		else
			best_path = cheapest_path;
		
		result_plan = create_plan(root, best_path);
		result_pathkeys = best_path->pathkeys;
		
		/* Adjust target list (create_plan returns a flat one) and costs
		 * since our target list may have expressions.  (Could track this
		 * and avoid, but not yet.)
		 */
		result_plan = plan_pushdown_tlist(result_plan, targetlist);
		cost_qual_eval(&tlist_cost, targetlist, root);
		result_plan->startup_cost += tlist_cost.startup;
		result_plan->total_cost += tlist_cost.startup + tlist_cost.per_tuple * result_plan->plan_rows;

		Assert( result_plan->flow != NULL );
	}

	*pathkeys_ptr = result_pathkeys;
	return result_plan;
}

/* Plan the coplans for a single WindowInfo. Results are stored back in the
 * indicated WindowInfo (fields window_coplan and agg_coplan).  The context
 * is updated appropriately (e.g., the Var translation map is kept current).
 */
static void plan_windowinfo_coplans(PlannerInfo *root, WindowContext *context, int window_index)
{
	Coplan *window_coplan = NULL;
	Coplan *agg_coplan = NULL;
	int i;
	ListCell *lc;
	WindowInfo *winfo = context->windowinfos + window_index;
	
	/* There's always a window coplan. If there is more than one WindowInfo, 
	 * we must make sure they all contain the row key.
	 *
	 * XXX Except for the first (required) window coplan, we don't really
	 *     need a window coplan when there's only a widow aggregate over
	 *     an unpartitioned, unordered window.  Currently, we ignore
	 *     this optimization and always include one.
	 */
	window_coplan = makeCoplan(COPLAN_WINDOW, context);
	
	/* There's an agg coplan only if there are unordered aggregates or
	 * deferred strategy window functions.  The agg coplan will join to
	 * its window coplan on the partition key, if one exists, or by 
	 * cartesian product.
	 */
	if ( winfo->needpartkey || winfo->needauxcount )
		agg_coplan = makeCoplan(COPLAN_AGG, context);
	
	if ( window_index == 0 )
	{
		/* First window coplan is special.  It contains the entire lower
		 * target list along with the row key, if any.  Subsequent window 
		 * coplans are sparse.  They contain only those targets needed 
		 * locally by their WindowInfo.
		 *
		 * Note that, at this point, the upper and lower target lists refer 
		 * to the same range -- that of the common subquery.  But soon the 
		 * upper list will refer to the middle range we are about to build.  
		 *
		 * Here, we initialize the Var translation map used later by
		 * translate_upper_tlist_parallel to adjust upper Vars to reference 
		 * the new middle range. During processing we add an entry to this 
		 * map for the entries corresponding to plain Vars.  By construction, 
		 * this will include an entry for each distinct Var in the range.
		 */		
		context->offset_upper_varattrnos = palloc0(context->offset_lim * sizeof(AttrNumber));
		
		foreach ( lc, context->keyed_lower_tlist )
		{
			TargetEntry *tle = (TargetEntry *)lfirst(lc);
			AttrNumber resno = addTargetToCoplan((Node*)tle, window_coplan, context);
			
			/* This assumption used below to copy key attributes. */
			Assert( resno == tle->resno ); 
			
			if ( IsA(tle->expr, Var) )
			{
				Var *var = (Var*)tle->expr;
				context->offset_upper_varattrnos[index_of_var(var,context)] = resno;
			}
		}

		/* The attribute numbers of keys are the same as in the lower target 
		 * list, so we can just copy them. 
		 */
		if ( context->rowkey_len > 0 )
		{
			size_t sz = context->rowkey_len * sizeof(AttrNumber);
			window_coplan->rowkey_len = context->rowkey_len;
			window_coplan->rowkey_attrs = palloc(sz);
			memcpy(window_coplan->rowkey_attrs, context->rowkey_attrs, sz);
		}

		if ( winfo->partkey_len > 0 )
		{
			size_t sz = winfo->partkey_len * sizeof(AttrNumber);
			window_coplan->partkey_attrs = palloc(sz);
			memcpy(window_coplan->partkey_attrs, winfo->partkey_attrs, sz);
		}
			
		if ( (agg_coplan != NULL) && winfo->partkey_len > 0 )
		{
			size_t sz = winfo->partkey_len * sizeof(AttrNumber*);
			agg_coplan->partkey_len = winfo->partkey_len;
			agg_coplan->partkey_attrs = palloc(sz);
			for ( i = 0; i < winfo->partkey_len; i ++ )
			{
				TargetEntry *tle; 
				tle = get_tle_by_resno(context->lower_tlist, winfo->partkey_attrs[i]);
				agg_coplan->partkey_attrs[i] = 
					addTargetToCoplan((Node*)tle, agg_coplan, context);
			}
		}
	}
	else
	{
		/* Later window coplans are all similar and need contain only 
		 * targets specifically used by this WindowInfo's WindowRefs. */			
		
		/* Include input key, if any. */
		if ( context->rowkey_len > 0 )
		{
			size_t sz = winfo->rowkey_len * sizeof(AttrNumber);
			window_coplan->rowkey_len = context->rowkey_len;
			window_coplan->rowkey_attrs = palloc(sz);
			for ( i = 0; i < context->rowkey_len; i ++ )
			{
				TargetEntry *tle;
				
				
				tle = get_tle_by_resno(context->keyed_lower_tlist, 
									   context->rowkey_attrs[i]);
									 
				window_coplan->rowkey_attrs[i] = 
					addTargetToCoplan((Node*)tle, window_coplan, context);
			}
		}
			
		/* Include partition key, if needed. */
		if ( (agg_coplan != NULL) && winfo->partkey_len > 0 )
		{
			size_t sz = winfo->partkey_len * sizeof(AttrNumber);
			window_coplan->partkey_len = winfo->partkey_len;
			window_coplan->partkey_attrs = (AttrNumber*)palloc(sz);
			agg_coplan->partkey_len = winfo->partkey_len;
			agg_coplan->partkey_attrs = (AttrNumber*)palloc(sz);
			for ( i = 0; i < winfo->partkey_len; i++ )
			{
				TargetEntry *tle; 
			
				tle = get_tle_by_resno(context->keyed_lower_tlist, 
									   winfo->partkey_attrs[i]);

				window_coplan->partkey_attrs[i] = 
					addTargetToCoplan((Node*)tle, window_coplan, context);
				agg_coplan->partkey_attrs[i] = 
					addTargetToCoplan((Node*)tle, agg_coplan, context);
			}
		}
	}
	
	
	/* Now add targets for each RefInfo covered by this WindowInfo.
	 *
	 * TODO Reorganize context->refinfos into an array.  Use varsets
	 *      to access the refinfos. 
	 */
	foreach ( lc, context->refinfos )
	{
		WindowRef *ref;
		AttrNumber resno;
		RefInfo *rinfo = (RefInfo *)lfirst(lc);
		SpecInfo *sinfo = &context->specinfos[rinfo->specindex];
		
		if ( sinfo->windowindex != window_index )
			continue;
		
		ref = rinfo->ref;
		
		if ( RefInfo_AggregateUnordered(rinfo, context) ) // ( rinfo->isagg && sinfo->order == NIL )
		{
			/* Plan unordered aggregate */
			Aggref *aggref;
			Var *var;
			
			aggref = makeWindowAggref(ref);
			resno = addTargetToCoplan((Node*)aggref, agg_coplan, context);
			
			/* Add result expr to ref */
			var = makeVar(agg_coplan->varno, resno, ref->restype, -1, false);
			rinfo->resultexpr = (Expr*)var;
		}
		else if ( RefInfo_WindowDeferred(rinfo, context) ) // ( !rinfo->isagg && rinfo->needcount )
		{
			/* Plan deferred window function */
			FuncExpr *func;
			Var *arg1, *arg2;
			Oid counttype = 20; /* TODO count(*) type in pg_proc */
			Oid restype = ref->restype;
			
			ref->winstage = WINSTAGE_PRELIMINARY;
			ref->restype = rinfo->winpretype;

			resno = addTargetToCoplan((Node*)ref, window_coplan, context);			
			arg1 = makeVar(window_coplan->varno, resno, ref->restype, -1, false);
			
			resno = addTargetToCoplan((Node*)makeAuxCountAggref(), agg_coplan, context);
			arg2 = makeVar(agg_coplan->varno, resno, counttype , -1, false);
			
			/* Add result expr to ref */
			func = makeFuncExpr(rinfo->winfinfunc, restype, list_make2(arg1, arg2), COERCE_DONTCARE);
			rinfo->resultexpr = (Expr*)func;
		}
		else if ( RefInfo_WindowImmediate(rinfo, context) || 
				  RefInfo_AggregateOrdered(rinfo, context) ) // ( rinfo->isagg || !rinfo->needcount )
		{
			/* Plan immediate window function or ordered window agg) */
			ref->winstage = WINSTAGE_IMMEDIATE;
			resno = addTargetToCoplan((Node*)ref, window_coplan, context);
			
			/* Add result expr to ref */
			rinfo->resultexpr = (Expr*)makeVar(window_coplan->varno, resno, ref->restype, -1, false);
		}
		else
		{
			elog(ERROR,"internal error planning window function call");
		}
		
		if ( agg_coplan != NULL )
			agg_coplan->trans_space += rinfo->trans_space; /* may over count, just an estimate */
	}
	
	/* Store results. */
	winfo->window_coplan = window_coplan;
	winfo->agg_coplan = agg_coplan;
}


/* Return a target list to use as the join key for the separate window
 * coplans generated by a non-trivial window query.  The key consists
 * of a segment number and row number and must be evaluated within a
 * Window node.  
 *
 * The caller is responsible for assigning appropriate resno values 
 * in the targets.  (They are initialized, here, to zero!).
 */
static List *make_rowkey_targets()
{
	FuncExpr *seg;
	WindowRef *row;
	
	seg = makeFuncExpr(MPP_EXECUTION_SEGMENT_OID, 
					   MPP_EXECUTION_SEGMENT_TYPE, 
					   NIL, 
					   COERCE_DONTCARE);
								  
	row = makeNode(WindowRef);
	row->winfnoid = ROW_NUMBER_OID;
	row->restype = ROW_NUMBER_TYPE;
	row->args = NIL;
	row->winlevelsup = row->winspec = row->winindex = 0;
	row->winstage = WINSTAGE_ROWKEY; /* so setrefs doesn't get confused  */
	row->winlevel = 0;
	
	return list_make2(
		makeTargetEntry((Expr*)seg, 1, pstrdup("segment_join_key"), false),
		makeTargetEntry((Expr*)row, 1, pstrdup("row_join_key"), false) );
}


static Coplan *makeCoplan(CoplanType type, WindowContext *context)
{
	Index varno =  ++context->coplan_count;
	Coplan *coplan = palloc0(sizeof(Coplan));
	
	Assert ( type==COPLAN_WINDOW || type==COPLAN_AGG );
	Assert ( varno > 0 );
	
	coplan->type = type;
	coplan->varno = varno;
	coplan->num_aggs = 0;
	
	return coplan;
}

/* Return the atttribute number of the given target in the given coplan.
 *
 * The target may be a TargetEntry (in which case we copy it) or an Expr
 * (in which case we construct a TargetEntry and copy the Expr into it).
 * The result is the attribute number (resno) of the new target in the
 * coplan's target list.
 *
 * TODO Don't make a new target if an identical one exists already.
 * TODO API? 
 */
static AttrNumber addTargetToCoplan(Node *target, Coplan *coplan, WindowContext *context)
{
	char *name;
	TargetEntry *tle;
	AttrNumber resno = ++coplan->last_resno;
	
	if ( IsA(target,TargetEntry) )
	{
		tle = copyObject(target);
	}
	else
	{
		tle = makeTargetEntry(copyObject(target), resno, 
							  pstrdup("window_column"), false);
	}
	
	if ( IsA(target, Aggref) )
		coplan->num_aggs++;
	
	/* TODO Really should do better.  Need names for rangetable erefs. */
	if ( tle->resname != NULL )
		name = pstrdup(tle->resname);
	else
		name = pstrdup("coplan_target");
	
		
	tle->resno = resno;
	coplan->targetlist = lappend(coplan->targetlist, tle);
	coplan->targetnames = lappend(coplan->targetnames, makeString(name));
	
	Assert( resno == list_length(coplan->targetlist) );
	return resno;
}


static Aggref* makeWindowAggref(WindowRef *winref)
{
	Aggref *aggref = makeNode(Aggref);
	
	aggref->aggfnoid = winref->winfnoid;
	aggref->aggtype = winref->restype;
	aggref->args = copyObject(winref->args);
	aggref->agglevelsup = 0;
	aggref->aggstar = false; /* at this point in processing, doesn't matter */
	aggref->aggdistinct = winref->windistinct;
	aggref->aggstage = AGGSTAGE_NORMAL;

	return aggref;
}

static Aggref* makeAuxCountAggref()
{
	Aggref *aggref = makeNode(Aggref);
	
	aggref->aggfnoid = 2803; /* TODO count(*) oid define in pg_proc.h */
	aggref->aggtype = 20; /* TODO count(*) result type oid in pg_proc.h */
	aggref->args = NIL;
	aggref->agglevelsup = 0;
	aggref->aggstar = true; 
	aggref->aggdistinct = false; 
	aggref->aggstage = AGGSTAGE_NORMAL;
	
	return aggref;
}

/* Constructs a plan for a single coplan of a window query and returns it 
 * as a variant form of subquery RTE.  
 * 
 * The input subplan (and associated locus and pathkeys) should have the 
 * same result as the "common subquery", but need not be a plan for that 
 * query.  For example, it might be a ShareInputScan. The subplan is copied, 
 * not used directly. 
 *
 * XXX Perhaps copy should be optional based an additional argument?
 *
 * The generated RTE differs from an ordinary subquery RTE in that it
 * it holds the produced plan and associated pathkeys.  These are used
 * later in the optimizer short-circuit the recursive planning of the
 * RTE's subquery.
 *
 * TODO The spare fields that hold the plan aren't in, e.g., copyfuncs.c.
 *      Decide whether or not we need to add them!!!
 */

static RangeTblEntry *rte_for_coplan(
		PlannerInfo *root,
		Plan *input_plan, 
		CdbPathLocus subplan_locus,
		List *subplan_pathkeys,
		WindowInfo *winfo,
		Coplan *coplan)
{
	RangeTblEntry *rte;
	Plan *new_plan = input_plan;
	Alias *new_eref;
	
	/* Now new_plan is suitable input for a coplan of this winfo. 
	 * Add the coplan processing.
	 */
	switch ( coplan->type )
	{
		case COPLAN_WINDOW:
			new_plan = (Plan*) 
				make_window(root, coplan->targetlist, 
						winfo->partkey_len, winfo->partkey_attrs,
						winfo->key_list, /* XXX copy? */
						new_plan);
		break;
	case COPLAN_AGG:
		if ( coplan->partkey_len > 0 )
		{
			/* Grouped aggregation. */
			
			/* TODO Fix this cheesy estimate. */
			double d = new_plan->plan_rows / 100.0;
			long num_groups = (d < 0)? 0: ( d > LONG_MAX )? LONG_MAX: (long)d;
			
			new_plan = (Plan*)
				make_agg(root, coplan->targetlist, NULL,
						 AGG_SORTED, false,
						 winfo->partkey_len, winfo->partkey_attrs,
						 num_groups,
						 0, /* num_nullcols */
						 0, /* input_grouping */
						 0, /* grouping_id */
						 0, /* rollup_gs_times */
						 coplan->num_aggs, coplan->trans_space,
						 new_plan);
		}
		else
		{
			/* Plain aggregation, no grouping. */
			new_plan = (Plan*)
				make_agg(root, coplan->targetlist, NULL,
						 AGG_PLAIN, false,
						 0, NULL,
						 1, /* number of groups */
						 0, /* num_nullcols */
						 0, /* input_grouping */
						 0, /* grouping_id */
						 0, /* rollup_gs_times */
						 coplan->num_aggs, coplan->trans_space,
						 new_plan);
		}
		break;
	default:
		elog(ERROR,"invalid window coplan");
	}

	if (!new_plan->flow)
		new_plan->flow = pull_up_Flow(new_plan, 
				new_plan->lefttree, 
				(subplan_pathkeys != NIL));

	/* Need to specify eref for messages. */
	new_eref = makeNode(Alias);
	new_eref->aliasname = pstrdup("coplan");
	new_eref->colnames = coplan->targetnames;
	
	rte = package_plan_as_rte(root->parse, new_plan, new_eref, subplan_pathkeys);

	return rte;
}

/* Package a plan as a pre-planned subquery RTE.
 *
 * Note that the input query is often root->parse (since that is the
 * query from which this invocation of the planner usually takes its 
 * context), but may be a derived query, e.g., in the case of sequential 
 * window plans or multiple-DQA pruning (in cdbgroup.c).
 * 
 * Note also that the supplied plan's target list must be congruent with
 * the supplied query: its Var nodes must refer to RTEs in the range 
 * table of the Query node, it should conserve sort/group reference
 * values, and its SubqueryScan nodes should match up with the query's
 * Subquery RTEs.
 *
 * The result is a pre-planned subquery RTE which incorporates the given
 * plan, alias, and pathkeys (if any) directly.  The input query is not
 * modified.
 *
 * The caller must install the RTE in the range table of an appropriate query 
 * and the corresponding plan should reference its results through a 
 * SubqueryScan node.
 */
RangeTblEntry *package_plan_as_rte(Query *query, Plan *plan, Alias *eref, List *pathkeys)
{
	Query *subquery;
	RangeTblEntry *rte;
	
	
	Assert( query != NULL );
	Assert( plan != NULL );
	Assert( eref != NULL );
	Assert( plan->flow != NULL ); /* essential in a pre-planned RTE */
	
	/* Make a plausible subquery for the RTE we'll produce. */
	subquery = makeNode(Query);
	memcpy(subquery, query, sizeof(Query));
	
	subquery->querySource = QSRC_PLANNER;
	subquery->canSetTag = false;
	subquery->resultRelation = 0;
	subquery->intoClause = NULL;
	
	subquery->rtable = copyObject(subquery->rtable);

	subquery->targetList = copyObject(plan->targetlist);
	subquery->windowClause = NIL;
	
	subquery->distinctClause = NIL;
	subquery->sortClause = NIL;
	subquery->limitOffset = NULL;
	subquery->limitCount = NULL;
	
	Assert( subquery->setOperations == NULL );
	
	/* Package up the RTE. */
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = subquery;
	rte->eref = eref;
	rte->subquery_plan = plan;
	rte->subquery_rtable = subquery->rtable;
	rte->subquery_pathkeys = pathkeys;

	return rte;
}

/*
 * construct_share_plans - construct share plans for both window
 * coplans and aggregate coplans. When one WindowInfo contains
 * both a window coplan and an aggregate coplan, we share
 * more than just the given subplan. This function takes care
 * of that.
 *
 * For two different WindowInfos, the given subplan is shared.
 */
static List *
construct_share_plans(PlannerInfo *root, WindowContext *context)
{
	List *lower_share_nodes = NIL;
	List *share_nodes = NIL;
	int i;
	ListCell *lc;
	
	if (context->nwindowinfos > 1)
	{
		lower_share_nodes = share_plan(root, context->subplan,
									   context->nwindowinfos);
	}
	else
	{
		Assert(context->nwindowinfos == 1);
		lower_share_nodes = list_make1(context->subplan);
	}

	lc = list_head(lower_share_nodes);

	context->subplans_pathkeys = NIL;
	for (i = 0; i < context->nwindowinfos; i++)
	{
		WindowInfo *winfo = context->windowinfos + i;
		Plan *share_node;
		
		Assert(lc);
		share_node = (Plan *)lfirst(lc);

		if (winfo->window_coplan != NULL &&
			winfo->agg_coplan != NULL)
		{
			List *new_share_nodes = NIL;
			List *pathKeys = (List *)copyObject(context->subplan_pathkeys);
			
			Plan *new_plan =
				assure_collocation_and_order(root,
											 share_node,
											 winfo->partkey_len, 
											 winfo->partkey_attrs,
											 winfo->sortclause,
											 context->subplan_locus,
											 NULL,
											 &pathKeys);
			new_share_nodes = share_plan(root, new_plan, 2);
			share_nodes = list_concat(share_nodes, new_share_nodes);

			/* Append two copies of new pathkeys */
			context->subplans_pathkeys = lappend(context->subplans_pathkeys,
												 pathKeys);
			context->subplans_pathkeys = lappend(context->subplans_pathkeys,
												 pathKeys);
		}

		else if (winfo->window_coplan != NULL ||
				 winfo->agg_coplan != NULL)
		{
			List *pathKeys = (List *)copyObject(context->subplan_pathkeys);
			Plan *new_plan =
				assure_collocation_and_order(root,
											 share_node,
											 winfo->partkey_len, 
											 winfo->partkey_attrs,
											 winfo->sortclause,
											 context->subplan_locus,
											 NULL,
											 &pathKeys);
			share_nodes = lappend(share_nodes, new_plan);
			
			context->subplans_pathkeys = lappend(context->subplans_pathkeys,
												 pathKeys);
		}

		lc = lnext(lc);
	}

	return share_nodes;
}

/* 
 * plan_window_rtable is the stub for plan_window_rtable_dup/share.
 *
 * XXX TODO: Right now we either always dup the subplan or always share
 * the sub plan, depending on a guc gp_window_shareinput.
 * Should make this cost based
 */
List *plan_window_rtable(PlannerInfo *root, WindowContext *context)
{
	int i;
	RangeTblEntry *rte;
	List *sharedNodes = NIL;
	List *rtable = NIL;
	ListCell *lc = NULL;
	ListCell *pk_lc = NULL;

	sharedNodes = construct_share_plans(root, context);
	Assert(list_length(sharedNodes) == context->coplan_count);

	lc = list_head(sharedNodes);
	pk_lc = list_head(context->subplans_pathkeys);

	for ( i = 0; i < context->nwindowinfos; i++ )
	{
		WindowInfo *winfo = context->windowinfos + i; 
		if ( winfo->window_coplan != NULL )
		{
			Assert(lc);
			Assert(pk_lc);
			
			rte = rte_for_coplan(root, 
					(Plan *) lfirst(lc),
					context->subplan_locus,
					(List *)lfirst(pk_lc),
					winfo,
					winfo->window_coplan);
			lc = lnext(lc);
			pk_lc = lnext(pk_lc);
			rtable = lappend(rtable, rte);

		}

		if ( winfo->agg_coplan != NULL )
		{
			Assert(lc);
			Assert(pk_lc);
			rte = rte_for_coplan(root, 
					(Plan *) lfirst(lc),
					context->subplan_locus,
					(List *)lfirst(pk_lc),
					winfo,
					winfo->agg_coplan);
			lc = lnext(lc);
			pk_lc = lnext(pk_lc);
			rtable = lappend(rtable, rte);
		}
	}

	Assert( list_length(rtable) == context->coplan_count );
	return rtable;
}

/* Plan the FROM-WHERE clauses to rejoin the coplans.
 */
static FromExpr *plan_window_jointree(PlannerInfo *root, WindowContext *context)
{
	FromExpr *jointree;
	List *from = NIL;
	List *quals = NIL;
	RangeTblRef *rtr;
	Coplan *coplan;
	int i,j;
	Oid *rowkey_types = NULL;
	Coplan *base_window_coplan = NULL;
	
	
	for ( i = 0; i < context->nwindowinfos; i++ )
	{
		WindowInfo *winfo = &context->windowinfos[i];
		
		for ( j = 0,	coplan = winfo->window_coplan; 
			  j < 2; 
			  j++,		coplan = winfo->agg_coplan ) 
		{
			if ( coplan != NULL ) 
			{
				rtr = makeNode(RangeTblRef);
				rtr->rtindex = coplan->varno;
				from = lappend(from, rtr);
			}
		}

		if ( context->rowkey_len > 0 )
		{
			/* Join window_coplans on the row key */
			if ( i == 0 )
			{
				Assert( winfo->window_coplan != NULL );
				base_window_coplan = winfo->window_coplan;
				rowkey_types = get_window_attr_types(((Plan *)(context->subplan))->targetlist, context->rowkey_len, context->rowkey_attrs);
			}
			else if ( winfo->window_coplan != NULL )
			{
				/* Append qual to join base_window_coplan to 
				 * winfo->window_coplan on the row key.
				 */
				for ( j = 0; j < context->rowkey_len; j++ )
				{
					Expr *term = make_window_join_term(rowkey_types[j], 
													   base_window_coplan->varno,
													   base_window_coplan->rowkey_attrs[j],
													   winfo->window_coplan->varno,
													   winfo->window_coplan->rowkey_attrs[j],
													   true); /* true - no nulls */
					quals = lappend(quals, term);
				}
			}
		}
		
		if ( winfo->partkey_len > 0 && winfo->window_coplan != NULL && winfo->agg_coplan != NULL )
		{
			Oid *partkey_types;
			partkey_types = get_window_attr_types(context->lower_tlist, winfo->partkey_len, winfo->partkey_attrs);
			
			/* Append quals to join winfo->window_coplan to winfo->agg_coplan
			 * on the  the partitioning key. 
			 */
			for ( j = 0; j < winfo->partkey_len; j++ )
			{
				Expr *term = make_window_join_term(partkey_types[j], 
												   winfo->window_coplan->varno,
												   winfo->window_coplan->partkey_attrs[j],
												   winfo->agg_coplan->varno,
												   winfo->agg_coplan->partkey_attrs[j],
												   false); /* false - may contain nulls */
				quals = lappend(quals, term);
			}
			
			pfree(partkey_types);
		}
	}
	if ( rowkey_types != NULL )
		pfree(rowkey_types);
		
	jointree = makeNode(FromExpr);
	jointree->fromlist = from;
	jointree->quals = (Node*)quals;
	
	return jointree;
}

static Expr *make_window_join_term(Oid type, Index lftvarno, AttrNumber lftattrno, Index rgtvarno, AttrNumber rgtattrno, bool no_nulls)
{
	Var *lft = makeVar(lftvarno, lftattrno, type, -1, 0);
	Var *rgt = makeVar(rgtvarno, rgtattrno, type, -1, 0);
	Expr *xpr = make_op(NULL, list_make1(makeString("=")), (Node*)lft, (Node*)rgt, -1);
	
	if ( ! no_nulls )
	{
		/* Change the equality OpExpr into <NOT IS DISTINCT> */
		xpr->type = T_DistinctExpr;
		xpr = make_notclause(xpr);
	}

	return xpr;
}

static Oid *get_window_attr_types(List *targetlist, int nattrs, AttrNumber *attrs)
{
	int i;
	Oid *types = palloc0(nattrs*sizeof(Oid));
	
	for ( i = 0; i < nattrs; i++ )
	{
		TargetEntry *tle = get_tle_by_resno(targetlist, attrs[i]);
		types[i] = exprType((Node*)tle->expr);
	}
	return types;
}


/* Translate a targetlist on the lower range (of the common subquery) to
 * a targetlist on the upper range.
 *
 * TODO Avoid reevaluating sort/group expressions that have already been
 *      evaluated in the common subquery. 
 *
 * This translator is specific to parallel window plans!
 */

static Node * translate_upper_tlist_parallel_mutator(Node *node, WindowContext *context)
{
	if ( node == NULL )
		return NULL;
		
	if ( IsA(node, Var) )
	{
		Var *new_var;
		Var *var = (Var *)node;
		
		/* We don't descend into WindowRefs, so Var isn't in one. */
		if ( var->varlevelsup == 0 )
		{
			int idx = index_of_var(var, context);
			AttrNumber attrno = context->offset_upper_varattrnos[idx];
			new_var = makeVar(
						1, /* first RTE has all entries */
						attrno,
						var->vartype,
						var->vartypmod,
						0 /* levels up known 0 */);
		}
		else
			new_var = copyObject(var);

		return (Node *) new_var;
	}
	else if ( IsA(node, WindowRef) )
	{
		WindowRef *ref = (WindowRef *)node;
		RefInfo *rinfo = (RefInfo *)list_nth(context->refinfos, ref->winindex);
		return copyObject(rinfo->resultexpr); /* XXX why copy? */
	}
	
	return expression_tree_mutator(node, translate_upper_tlist_parallel_mutator, (void*) context);
}

static List *translate_upper_tlist_parallel(List *orig_tlist, WindowContext *context)
{
	List *new_tlist;
	
	new_tlist =  (List*)  
		expression_tree_mutator((Node*)orig_tlist,
								 translate_upper_tlist_parallel_mutator, 
								 (void*)context );
	
	return new_tlist;
}


/* This translator is specific to sequential window plans!
 */
typedef struct xut_context
{
	WindowContext *context;
	List *window_tlist;
} xut_context;

static Node * translate_upper_tlist_sequential_mutator(Node *node, xut_context *ctxt)
{
	WindowContext *context = ctxt->context;
	
	if ( node == NULL )
		return NULL;
		
	if ( IsA(node, Var) )
	{
		Var *new_var;
		Var *var = (Var *)node;
		
		/* We don't descend into WindowRefs, so Var isn't in one. */
		if ( var->varlevelsup == 0 )
		{
			int idx = index_of_var(var, context);
			AttrNumber attrno = context->offset_upper_varattrnos[idx];
			new_var = makeVar(
						1, /* first RTE has all entries */
						attrno,
						var->vartype,
						var->vartypmod,
						0 /* levels up known 0 */);
		}
		else
			new_var = copyObject(var);

		return (Node *) new_var;
	}
	else if ( IsA(node, WindowRef) )
	{
		WindowRef *ref = (WindowRef *)node;
		RefInfo *rinfo = (RefInfo *)list_nth(context->refinfos, ref->winindex);
		TargetEntry *tle = get_tle_by_resno(ctxt->window_tlist, rinfo->resno);
		return copyObject(tle->expr);
	}
	
	return expression_tree_mutator(node, translate_upper_tlist_sequential_mutator, (void*) ctxt);
}


static List *translate_upper_tlist_sequential(List *orig_tlist, List *window_tlist, WindowContext *context)
{
	xut_context ctxt;
	List *new_tlist;
	
	ctxt.context = context;
	ctxt.window_tlist = window_tlist;
		
	new_tlist =  (List*)  
		expression_tree_mutator((Node*)orig_tlist,
								 translate_upper_tlist_sequential_mutator, 
								 &ctxt);
	
	return new_tlist;
}


/* This translator is specific to sequential window plans. It converts Vars
 * the given tree (including top-level WindowRef node args, and WindowFrameEdge 
 * vals from the input range to the intermediate range used in sequential
 * planning.
 */
typedef struct xuv_context
{
	WindowContext *context;
	bool is_top_level;
} xuv_context;

static Node * translate_upper_vars_sequential_mutator(Node *node, xuv_context *ctxt)
{
	WindowContext *context = ctxt->context;
	
	if ( node == NULL )
		return NULL;
		
	if ( IsA(node, Var) )
	{
		Var *new_var;
		Var *var = (Var *)node;
		
		/* We don't descend into WindowRefs, so Var isn't in one. */
		if ( var->varlevelsup == 0 )
		{
			int idx = index_of_var(var, context);
			AttrNumber attrno = context->offset_upper_varattrnos[idx];
			new_var = makeVar(
						1, /* first RTE has all entries */
						attrno,
						var->vartype,
						var->vartypmod,
						0 /* levels up known 0 */);
		}
		else
			new_var = copyObject(var);

		return (Node *) new_var;
	}
	else if ( IsA(node, WindowRef) )
	{
		/* XXX We violate the spirit of the thing here by actually modifying
		 *     and returning the WindowRef itself.  We can't copy it because
		 *     its RefInfo's link must still find it.
		 */
		WindowRef *ref = (WindowRef *)node;
		
		if ( ! ctxt->is_top_level )
			elog(ERROR, "nested window reference invalid");		
		ctxt->is_top_level = false;
		
		ref->args = (List*)expression_tree_mutator((Node*)ref->args, translate_upper_vars_sequential_mutator, (void*) ctxt);
		
		ctxt->is_top_level = true;
		return (Node*)ref;
	}
	else if( IsA(node, WindowKey) )
	{
		WindowKey *key = (WindowKey*)node;
		WindowKey *newkey;
		
		if ( ! ctxt->is_top_level )
			elog(ERROR, "nested window key invalid");		
		ctxt->is_top_level = false;
		
		newkey = makeNode(WindowKey);
		memcpy(newkey, key, sizeof(WindowKey));
		if ( key->numSortCols > 0 )
		{
			size_t sz;
			
			sz = key->numSortCols * sizeof(AttrNumber);
			newkey->sortColIdx = (AttrNumber*)palloc(sz);
			memcpy(newkey->sortColIdx, key->sortColIdx, sz);
			
			sz = key->numSortCols * sizeof(Oid);
			newkey->sortOperators = (Oid*)palloc(sz);
			memcpy(newkey->sortOperators, key->sortOperators, sz);
		}
		
		newkey->frame = (WindowFrame*)expression_tree_mutator(
										(Node*)key->frame, 
										translate_upper_vars_sequential_mutator, 
										(void*) ctxt);
		
		ctxt->is_top_level = true;
		return (Node*)newkey;
	}
	
	return expression_tree_mutator(node, translate_upper_vars_sequential_mutator, (void*) ctxt);
}


static Node *translate_upper_vars_sequential(Node *node, WindowContext *context)
{
	Node *result;
	xuv_context ctxt;
	
	ctxt.context = context;
	ctxt.is_top_level = true;
	
	if ( context->original_range )
	{
		result = node;
	}
	else
	{
		result =  
			expression_tree_mutator(node,
									translate_upper_vars_sequential_mutator, 
									&ctxt);
	}
	
	return result;
}



/* Utility to get a name for a function to use as an eref. */
char *get_function_name(Oid proid, const char *dflt)
{
	char	   *result;

	if (!OidIsValid(proid))
	{
		result = pstrdup(dflt);
	}
	else
	{
		int			fetchCount;

		result = caql_getcstring_plus(
				NULL,
				&fetchCount,
				NULL,
				cql("SELECT proname FROM pg_proc "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(proid)));
	
		if (!fetchCount)
			result = pstrdup(dflt);
	}

	return result;
}

/* Utility to get a name for a tle to use as an eref. */
Value *get_tle_name(TargetEntry *tle, List *rtable, const char *default_name)
{
	char *name = NULL;
	Node *expr = (Node*)tle->expr;
	
	if ( tle->resname != NULL )
	{
		name = pstrdup(tle->resname);
	}
	else if ( IsA(tle->expr, Var) && rtable != NULL )
	{
		Var *var = (Var*)tle->expr;
		RangeTblEntry *rte = rt_fetch(var->varno, rtable);
		name = pstrdup(get_rte_attribute_name(rte, var->varattno));
	}
	else if ( IsA(tle->expr, WindowRef) )
	{
		if ( default_name == NULL ) default_name = "window_func";
		name = get_function_name(((WindowRef*)expr)->winfnoid, default_name);
	}
	else if ( IsA(tle->expr, Aggref) )
	{
		if ( default_name == NULL ) default_name = "aggregate_func";
		name = get_function_name(((Aggref*)expr)->aggfnoid, default_name);
	}
	
	if ( name == NULL )
	{
		if (default_name == NULL ) default_name = "unnamed_attr";
		name = pstrdup(default_name);
	}
	
	return makeString(name);
}


/* Wrap a window plan under construction in a SubqueryScan thereby 
 * renumbering its attributes so that its targetlist becomes a list 
 * of Var nodes on a single entry range table (thus the only varno 
 * is 1).
 *
 * The input consists of
 * - the window Plan (with attached Flow) under development
 * - the pathkeys corresponding to the plan
 * - the Query tree corresponding to the plan
 * 
 * The output consists of
 * - the Plan modified by the addition of a SubqueryScan node
 *   (explicit result), and
 * - a Query tree representing simple select of all targets
 *   from the sub-query (in location query_p).
 *
 * Input query table expression gets pushed down into a sqry RTE.
 * 
 * The caller is responsible for translating pathkeys and locus, if needed.
 *
 * XXX Is it necessary to adjust varlevelsup in the plan and query trees?
 *     I don't think so.
 */
Plan *wrap_plan(PlannerInfo *root, Plan *plan, Query *query, 
				List **p_pathkeys, 
				const char *alias_name, List *col_names, 
				Query **query_p)	
{
	Query *subquery;
	Alias *eref;
	RangeTblEntry *rte;
	RangeTblRef *rtr;
	FromExpr *jointree;
	List *subq_tlist = NIL;
	Index varno = 1;
	int *resno_map;
	
	Assert( query_p != NULL );
	
	/*
	 * If NIL passed for col_names, generates it from subplan's targetlist.
	 */
	if (col_names == NIL)
	{
		ListCell	   *l;

		foreach (l, plan->targetlist)
		{
			TargetEntry	   *tle = lfirst(l);
			Value		   *col_name;

			col_name = get_tle_name(tle, query->rtable, NULL);
			col_names = lappend(col_names, col_name);
		}
	}
	/* Make the subquery RTE. Note that this will include a Query derived
	 * from the input Query.
	 */
	eref = makeNode(Alias);
	eref->aliasname = pstrdup(alias_name);
	eref->colnames = col_names;

	rte = package_plan_as_rte(query, plan, eref, p_pathkeys ? *p_pathkeys : NIL);
	
	/* Make the target list for the plan and for the wrapper Query
	 * that will correspond to it.
	 */
	subq_tlist = generate_subquery_tlist(varno, plan->targetlist, false, &resno_map);
	
	/* Make the plan. 
	 */
	plan = (Plan*)make_subqueryscan(root, subq_tlist, NIL, varno, plan, query->rtable);
	mark_passthru_locus(plan, true, true);
	
	/* Make the corresponding Query.
	 */
	subquery = makeNode(Query);
	
	subquery->commandType = CMD_SELECT;
	subquery->querySource = QSRC_PLANNER;
	subquery->canSetTag = false;
	subquery->intoClause = NULL;
	subquery->rtable = list_make1(rte);	
	rtr = makeNode(RangeTblRef);
	rtr->rtindex = varno;
	jointree = makeNode(FromExpr);
	jointree->fromlist = list_make1(rtr);
	subquery->jointree = jointree;
	subquery->targetList = subq_tlist;
	subquery->windowClause = copyObject(query->windowClause); /* XXX need translation for frame vals */

	/* The rest may default to zero ...
	
	subquery->utilityStmt = NULL;
	subquery->resultRelation = 0;
	subquery->intoClause = NULL;
	subquery->hasAggs = false;
	subquery->hasWindFuncs = false;
	subquery->hasSubLinks = false;
	subquery->returningList = NIL;
	subquery->groupClause = NIL;
	subquery->havingClause = NULL;
	subquery->distinctClause = NIL;
	subquery->sortClause = NIL;
	subquery->limitOffset = NULL;
	subquery->limitCount = NULL;
	subquery->rowMarks = NIL;
	subquery->setOperations = NULL;
	subquery->resultRelations = NIL;
	subquery->returningLists = NIL;
	
	... */

	/* Reconstruct equi_key_list since the rtable has changed.
	 * XXX we leak the old one.
	 */
	root->equi_key_list =
		construct_equivalencekey_list(root->equi_key_list,
									  resno_map,
									  ((SubqueryScan *)plan)->subplan->targetlist,
									  subq_tlist);

	/* Construct the new pathkeys */
	if (p_pathkeys != NULL)
		*p_pathkeys = reconstruct_pathkeys(root, *p_pathkeys, resno_map,
										   ((SubqueryScan *)plan)->subplan->targetlist,
										   subq_tlist);
	pfree(resno_map);
	
	*query_p = subquery;
	return plan;
}

/* Include the auxiliary aggregation plan into a wrapper query (a window 
 * query with a singleton range table and null quals) by adding a new 
 * subquery RTE representing the aggregation plan.  Add a corresponding
 * subquery scan node to the top of the aggregation plan.
 */
Plan *add_join_to_wrapper(
				PlannerInfo *root,
				Plan *plan, Query *query, /* the auxiliary aggregate to join */
				List *join_tlist,
				unsigned partkey_len, /* count of leading key columns of auxiliary */
				const char *alias_name, List *col_names, /* for auxiliary RTE */
				Query *wrapper_query /* the window query to modify */
				)
{
	Alias *eref;
	RangeTblEntry *rte;
	RangeTblRef *rtr;
	List *subq_tlist = NIL;
	Index varno = 2; /* Always */
	List *pathkeys = NIL;
	int *resno_map;
	
	Insist( list_length(wrapper_query->rtable) == 1 );
	Insist( wrapper_query->jointree->quals == NULL );
	
	/* Make the new subquery RTE. Note that this will include a Query derived
	 * from the input Query.
	 */
	eref = makeNode(Alias);
	eref->aliasname = pstrdup(alias_name);
	eref->colnames = col_names;

	rte = package_plan_as_rte(query, plan, eref, pathkeys);
	
	/* Make the target list for the plan and for the wrapper Query
	 * that will correspond to it.
	 */
	subq_tlist = generate_subquery_tlist(varno, plan->targetlist, false, &resno_map);
	
	/* Make the plan. 
	 */
	plan = (Plan*)make_subqueryscan(root, subq_tlist, NIL, varno, plan, query->rtable);
	mark_passthru_locus(plan, true, true);

	
	/* Add new RTE */
	wrapper_query->rtable = lappend(wrapper_query->rtable, rte);	
	
	rtr = makeNode(RangeTblRef);
	rtr->rtindex = varno;
	wrapper_query->jointree->fromlist = lappend(wrapper_query->jointree->fromlist, rtr);
	
	wrapper_query->targetList = copyObject(join_tlist);
	
	/* XXX Do we need to add a join qual? I don't think so. */	
	
	return plan;
}

/* Copy the given query but use the given target list instead of the one
 * in the query.  The given target list should be "flat" and conformable
 * to the range table of the query and any sort/group refs.  In particular,
 * it may not include aggregate or window functions.  Null out the "windowing 
 * and above" portions of the copy but retain the windowClause so that
 * EXPLAIN can use it.
 *
 * We expect the grouping specification to be null already (due to the
 * syntactic transformation that separates windowing from grouping)  so 
 * we assert that.
 * 
 * This produces the initial value of the windowing subquery
 * from root->parse for sequential window planning.
 */
Query *copy_common_subquery(Query *original, List *targetList)
{
	/* Flat-copy the input */
	Query *common = makeNode(Query);
	memcpy(common, original, sizeof(Query));

	/* Perhaps not strictly necessary, but its what we expect. */
	Assert (common->groupClause == NIL && common->havingQual == NULL);
	Assert (common->hasAggs == false);
	Assert (common->setOperations == NULL);
	
	/* Leave rtable and jointree alone. */
	
	/* New target list must assure no aggregation or windowing. */
	common->targetList = targetList;
	
	common->commandType = CMD_SELECT;
	common->querySource = QSRC_PLANNER;
	common->canSetTag = false;
	common->intoClause = NULL;
	common->windowClause = copyObject(original->windowClause);
	
	/* The rest is "blank". */
	common->resultRelation = 0;
	common->utilityStmt = NULL;
	common->intoClause = NULL;
	common->hasWindFuncs = false;
	common->hasSubLinks = false; /* XXX */
	common->returningList = NIL;
	common->distinctClause = NIL;
	common->sortClause = NIL;
	common->limitOffset = NULL;
	common->limitCount = NULL;
	common->rowMarks = NIL;
	common->resultRelations = NIL;
	common->returningLists = NIL;
	
	return common;
}

/*
 * Return true if a node contains WindowRefs.
 *
 * 'context' is not used in this function.
 */
bool
contain_windowref(Node *node, void *context)
{
	if (node == NULL)
		return false;
	
	if (IsA(node, WindowRef))
		return true;
	
	return expression_tree_walker(node, contain_windowref, NULL);
}

/*
 * Does the given window frame edge contains an expression that must be
 * evaluated at run time (i.e., may contain a Var)?
 */
bool
window_edge_is_delayed(WindowFrameEdge *edge)
{
	if (edge == NULL)
		return false;
	if ((edge->kind == WINDOW_DELAYED_BOUND_PRECEDING ||
		 edge->kind == WINDOW_DELAYED_BOUND_FOLLOWING) &&
			edge->val != NULL)
		return true;

	/* Non-delayed frame edge must not have Var */
	Assert(pull_var_clause((Node *) edge->val, false) == NIL);

	return false;
}
