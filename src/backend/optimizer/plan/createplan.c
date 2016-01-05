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
 * createplan.c
 *	  Routines to create the desired plan for processing a query.
 *	  Planning is complete, we just need to convert the selected
 *	  Path into a Plan.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/optimizer/plan/createplan.c,v 1.217.2.1 2007/07/31 19:53:49 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "miscadmin.h" /* work_mem */

#include "access/hd_work_mgr.h"
#include "catalog/pg_type.h"    /* INT8OID */
#include "nodes/makefuncs.h"
#include "executor/execHHashagg.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planshare.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "optimizer/subselect.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"     /* ordering_oper_opid */
#include "utils/lsyscache.h"

#include "cdb/cdbgroup.h"       /* adapt_flow_to_targetlist() */
#include "cdb/cdblink.h"        /* getgphostCount() */
#include "cdb/cdbllize.h"       /* pull_up_Flow() */
#include "cdb/cdbpath.h"        /* cdbpath_rows() */
#include "cdb/cdbpathtoplan.h"  /* cdbpathtoplan_create_flow() etc. */
#include "cdb/cdbpullup.h"      /* cdbpullup_targetlist() */
#include "cdb/cdbsetop.h" /* mark_passthru_locus() */
#include "cdb/cdbutil.h"        /* cdbComponentDatabase, makeRandomSegMap() */		
#include "cdb/cdbsreh.h"

typedef struct CreatePlanContext
{
    PlannerInfo    *root;
} CreatePlanContext;


static Plan *create_subplan(CreatePlanContext *ctx, Path *best_path);   /*CDB*/
static Plan *create_scan_plan(CreatePlanContext *ctx, Path *best_path);

List *build_relation_tlist(RelOptInfo *rel);
static bool use_physical_tlist(CreatePlanContext *ctx, RelOptInfo *rel);
static void disuse_physical_tlist(Plan *plan, Path *path);
static Plan *create_gating_plan(CreatePlanContext *ctx, Plan *plan, List *quals);
static Plan *create_join_plan(CreatePlanContext *ctx, JoinPath *best_path);
static Plan *create_append_plan(CreatePlanContext *ctx, AppendPath *best_path);
static Result *create_result_plan(CreatePlanContext *ctx, ResultPath *best_path);
static Material *create_material_plan(CreatePlanContext *ctx, MaterialPath *best_path);
static Plan *create_unique_plan(CreatePlanContext *ctx, UniquePath *best_path);
static Plan *create_motion_plan(CreatePlanContext *ctx, CdbMotionPath *path);
static SeqScan *create_seqscan_plan(CreatePlanContext *ctx, Path *best_path,
					List *tlist, List *scan_clauses);
static ExternalScan *create_externalscan_plan(CreatePlanContext *ctx, Path *best_path,
        List *tlist, List *scan_clauses);
static AppendOnlyScan *create_appendonlyscan_plan(CreatePlanContext *ctx, Path *best_path,
        List *tlist, List *scan_clauses);
static ParquetScan *create_parquetscan_plan(CreatePlanContext *ctx, Path *best_path,
						   List *tlist, List *scan_clauses);
static IndexScan *create_indexscan_plan(CreatePlanContext *ctx, IndexPath *best_path,
					  List *tlist, List *scan_clauses,
					  List **nonlossy_clauses);
static BitmapHeapScan *create_bitmap_scan_plan(CreatePlanContext *ctx,
						BitmapHeapPath *best_path,
						List *tlist, List *scan_clauses);
static Plan *create_bitmap_subplan(CreatePlanContext *ctx, Path *bitmapqual,
					  List **qual, List **indexqual);
static TidScan *create_tidscan_plan(CreatePlanContext *ctx, TidPath *best_path,
					List *tlist, List *scan_clauses);
static SubqueryScan *create_subqueryscan_plan(CreatePlanContext *ctx, Path *best_path,
						 List *tlist, List *scan_clauses);
static SubqueryScan *create_ctescan_plan(CreatePlanContext *ctx, Path *best_path,
										 List *tlist, List *scan_clauses);
static FunctionScan *create_functionscan_plan(CreatePlanContext *ctx, Path *best_path,
						 List *tlist, List *scan_clauses);
static TableFunctionScan *create_tablefunction_plan(CreatePlanContext *ctx, 
													Path *best_path,
													List *tlist, 
													List *scan_clauses);
static ValuesScan *create_valuesscan_plan(CreatePlanContext *ctx, Path *best_path,
					   List *tlist, List *scan_clauses);
static BitmapAppendOnlyScan *create_bitmap_appendonly_scan_plan(CreatePlanContext *ctx,
						BitmapAppendOnlyPath *best_path,
						List *tlist, List *scan_clauses);
static Plan *create_nestloop_plan(CreatePlanContext *ctx, NestPath *best_path,
					 Plan *outer_plan, Plan *inner_plan);
static MergeJoin *create_mergejoin_plan(CreatePlanContext *ctx, MergePath *best_path,
					  Plan *outer_plan, Plan *inner_plan);
static HashJoin *create_hashjoin_plan(CreatePlanContext *ctx, HashPath *best_path,
					 Plan *outer_plan, Plan *inner_plan);
static void fix_indexqual_references(List *indexquals, IndexPath *index_path,
						 List **fixed_indexquals,
						 List **nonlossy_indexquals,
						 List **indexstrategy,
						 List **indexsubtype);
static Node *fix_indexqual_operand(Node *node, IndexOptInfo *index,
					  Oid *opclass);
static List *get_switched_clauses(Relids innerrelids, List *clauses);
static List *order_qual_clauses(PlannerInfo *root, List *clauses);
static void copy_path_costsize(PlannerInfo *root, Plan *dest, Path *src);
static void copy_plan_costsize(Plan *dest, Plan *src);
static SeqScan *make_seqscan(List *qptlist, List *qpqual, Index scanrelid);
static AppendOnlyScan *make_appendonlyscan(List *qptlist, List *qpqual, Index scanrelid);
static ParquetScan *make_parquetscan(List *qptlist, List *qpqual, Index scanrelid);
static ExternalScan *make_externalscan(List *qptlist,
									   List *qpqual,
									   Index scanrelid,
									   List *filenames,
									   List *fmtopts,
									   bool istext,
									   bool ismasteronly,
									   int rejectlimit,
									   bool rejectlimitinrows,
									   Oid fmterrtableOid,
									   int encoding);
static IndexScan *make_indexscan(List *qptlist, List *qpqual, Index scanrelid,
			   Oid indexid, List *indexqual, List *indexqualorig,
			   List *indexstrategy, List *indexsubtype,
			   ScanDirection indexscandir);
static BitmapIndexScan *make_bitmap_indexscan(Index scanrelid, Oid indexid,
					  List *indexqual,
					  List *indexqualorig,
					  List *indexstrategy,
					  List *indexsubtype);
static BitmapHeapScan *make_bitmap_heapscan(List *qptlist,
					 List *qpqual,
					 Plan *lefttree,
					 List *bitmapqualorig,
					 Index scanrelid);
static BitmapAppendOnlyScan *make_bitmap_appendonlyscan(List *qptlist,
														List *qpqual,
														Plan *lefttree,
														List *bitmapqualorig,
														Index scanrelid,
														bool isAORow);
static TableFunctionScan* make_tablefunction(List *tlist,
											 List *scan_quals,
											 Plan *subplan,
											 List *subrtable,
											 Index scanrelid);
static TidScan *make_tidscan(List *qptlist, List *qpqual, Index scanrelid,
			 List *tidquals);
static FunctionScan *make_functionscan(List *qptlist, List *qpqual,
				  Index scanrelid);
static ValuesScan *make_valuesscan(List *qptlist, List *qpqual,
				Index scanrelid);
static BitmapAnd *make_bitmap_and(List *bitmapplans);
static BitmapOr *make_bitmap_or(List *bitmapplans);
static Sort *make_sort(PlannerInfo *root, Plan *lefttree, int numCols,
		  AttrNumber *sortColIdx, Oid *sortOperators);

static List *flatten_grouping_list(List *groupcls);
static char** create_pxf_plan(char **segdb_file_map, RelOptInfo *rel, int total_segs, 
							  CreatePlanContext *ctx, Index scan_relid);

/*
 * create_plan
 *	  Creates the access plan for a query by tracing backwards through the
 *	  desired chain of pathnodes, starting at the node 'best_path'.  For
 *	  every pathnode found, we create a corresponding plan node containing
 *	  appropriate id, target list, and qualification information.
 *
 *	  The tlists and quals in the plan tree are still in planner format,
 *	  ie, Vars still correspond to the parser's numbering.  This will be
 *	  fixed later by setrefs.c.
 *
 *	  best_path is the best access path
 *
 *	  Returns a Plan tree.
 */
Plan *
create_plan(PlannerInfo *root, Path *path)
{
    CreatePlanContext   ctx;
    Plan               *plan;

    ctx.root = root;

    /* Modify path to support unique rowid operation for subquery preds. */
    if (root->in_info_list)
        cdbpath_dedup_fixup(root, path);

    /* Generate the Plan tree. */
    plan = create_subplan(&ctx, path);

	/* Decorate the top node of the plan with a Flow node. */
	plan->flow = cdbpathtoplan_create_flow(root,
                                           path->locus,
                                           path->parent ? path->parent->relids
                                                        : NULL,
                                           path->pathkeys,
                                           plan);
    return plan;
}                               /* create_plan */


/*
 * create_subplan
 */
Plan *
create_subplan(CreatePlanContext *ctx, Path *best_path)
{
	Plan	   *plan;

	switch (best_path->pathtype)
	{
		case T_SeqScan:
		case T_IndexScan:
		case T_ExternalScan:
		case T_AppendOnlyScan:
		case T_ParquetScan:
		case T_BitmapHeapScan:
		case T_BitmapAppendOnlyScan:
		case T_BitmapTableScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFunctionScan:
        case T_ValuesScan:
		case T_CteScan:
			plan = create_scan_plan(ctx, best_path);
			break;
		case T_HashJoin:
		case T_MergeJoin:
		case T_NestLoop:
			plan = create_join_plan(ctx,
									(JoinPath *) best_path);
			break;
		case T_Append:
			plan = create_append_plan(ctx,
									  (AppendPath *) best_path);
			break;
		case T_Result:
			plan = (Plan *) create_result_plan(ctx,
											   (ResultPath *) best_path);
			break;
		case T_Material:
			plan = (Plan *) create_material_plan(ctx,
												 (MaterialPath *) best_path);
			break;
		case T_Unique:
			plan = create_unique_plan(ctx,
									  (UniquePath *) best_path);
			break;
        case T_Motion:
            plan = create_motion_plan(ctx, (CdbMotionPath *)best_path);
            break;
        default:
            Assert(false);
			elog(ERROR, "unrecognized node type: %d",
				 (int) best_path->pathtype);
			plan = NULL;		/* keep compiler quiet */
			break;
	}

    if (CdbPathLocus_IsPartitioned(best_path->locus) ||
        CdbPathLocus_IsReplicated(best_path->locus))
        plan->dispatch = DISPATCH_PARALLEL;

    return plan;
}                               /* create_subplan */


/*
 * create_scan_plan
 *	 Create a scan plan for the parent relation of 'best_path'.
 */
static Plan *
create_scan_plan(CreatePlanContext *ctx, Path *best_path)
{
	RelOptInfo *rel = best_path->parent;
	List	   *tlist;
	List	   *scan_clauses;
	Plan	   *plan;

	/*
	 * For table scans, rather than using the relation targetlist (which is
	 * only those Vars actually needed by the query), we prefer to generate a
	 * tlist containing all Vars in order.	This will allow the executor to
	 * optimize away projection of the table tuples, if possible.  (Note that
	 * planner.c may replace the tlist we generate here, forcing projection to
	 * occur.)
	 */
	if (use_physical_tlist(ctx, rel))
	{
		tlist = build_physical_tlist(ctx->root, rel);
		/* if fail because of dropped cols, use regular method */
		if (tlist == NIL)
			tlist = build_relation_tlist(rel);
	}
	else
		tlist = build_relation_tlist(rel);
	
	/*
	 * Extract the relevant restriction clauses from the parent relation. The
	 * executor must apply all these restrictions during the scan, except for
	 * pseudoconstants which we'll take care of below.
	 */
	scan_clauses = rel->baserestrictinfo;

	switch (best_path->pathtype)
	{
		case T_SeqScan:
			plan = (Plan *) create_seqscan_plan(ctx,
												best_path,
												tlist,
												scan_clauses);
			break;

		case T_AppendOnlyScan:
			plan = (Plan *) create_appendonlyscan_plan(ctx,
													   best_path,
													   tlist,
													   scan_clauses);
			break;

		case T_ParquetScan:
			plan = (Plan *) create_parquetscan_plan(ctx,
													   best_path,
													   tlist,
													   scan_clauses);
			break;

		case T_ExternalScan:
			plan = (Plan *) create_externalscan_plan(ctx,
													 best_path,
													 tlist,
													 scan_clauses);
			break;

		case T_IndexScan:
			plan = (Plan *) create_indexscan_plan(ctx,
												  (IndexPath *) best_path,
												  tlist,
												  scan_clauses,
												  NULL);
			break;

		case T_BitmapHeapScan:
			plan = (Plan *) create_bitmap_scan_plan(ctx,
												(BitmapHeapPath *) best_path,
													tlist,
													scan_clauses);
			break;

		case T_BitmapAppendOnlyScan:
			plan = (Plan *) create_bitmap_appendonly_scan_plan(ctx,
												    (BitmapAppendOnlyPath *) best_path,
													tlist,
													scan_clauses);
			break;

		case T_TidScan:
			plan = (Plan *) create_tidscan_plan(ctx,
												(TidPath *) best_path,
												tlist,
												scan_clauses);
			break;

		case T_SubqueryScan:
			plan = (Plan *) create_subqueryscan_plan(ctx,
													 best_path,
													 tlist,
													 scan_clauses);
			break;

		case T_FunctionScan:
			plan = (Plan *) create_functionscan_plan(ctx,
													 best_path,
													 tlist,
													 scan_clauses);
			break;

		case T_TableFunctionScan:
			plan = (Plan *) create_tablefunction_plan(ctx,
													   best_path,
													   tlist,
													   scan_clauses);
			break;

		case T_ValuesScan:
			plan = (Plan *) create_valuesscan_plan(ctx,
												   best_path,
												   tlist,
												   scan_clauses);
			break;

		case T_CteScan:
			plan = (Plan *) create_ctescan_plan(ctx,
												best_path,
												tlist,
												scan_clauses);
			
			break;

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) best_path->pathtype);
			plan = NULL;		/* keep compiler quiet */
			break;
	}

	/* Decorate the top node of the plan with a Flow node. */
	plan->flow = cdbpathtoplan_create_flow(ctx->root,
			best_path->locus,
			best_path->parent ? best_path->parent->relids
					: NULL,
					  best_path->pathkeys,
					  plan);

	/**
	 * If plan has a flow node, ensure all entries of hashExpr
	 * are in the targetlist.
	 */
	if (plan->flow
			&& plan->flow->hashExpr)
	{
		plan->targetlist = add_to_flat_tlist(plan->targetlist, plan->flow->hashExpr, true /* resjunk */);
	}

	/*
	 * If there are any pseudoconstant clauses attached to this node, insert a
	 * gating Result node that evaluates the pseudoconstants as one-time
	 * quals.
	 */
	if (ctx->root->hasPseudoConstantQuals)
		plan = create_gating_plan(ctx, plan, scan_clauses);

	return plan;
}

/*
 * Build a target list (ie, a list of TargetEntry) for a relation.
 */
List *
build_relation_tlist(RelOptInfo *rel)
{
	List	   *tlist = NIL;
	int			resno = 1;
	ListCell   *v;

	foreach(v, rel->reltargetlist)
	{
		/* Do we really need to copy here?	Not sure */
		Var		   *var = (Var *) copyObject(lfirst(v));

		tlist = lappend(tlist, makeTargetEntry((Expr *) var,
											   resno,
											   NULL,
											   false));
		resno++;
	}
	return tlist;
}

/*
 * use_physical_tlist
 *		Decide whether to use a tlist matching relation structure,
 *		rather than only those Vars actually referenced.
 */
static bool
use_physical_tlist(CreatePlanContext *ctx, RelOptInfo *rel)
{
    RangeTblEntry  *rte;
	int			    i;

	/*
	 * We can do this for real relation scans, subquery scans, function scans,
	 * and values scans (but not for, eg, joins).
	 */
	if (rel->rtekind != RTE_RELATION &&
		rel->rtekind != RTE_SUBQUERY &&
		rel->rtekind != RTE_FUNCTION &&
		rel->rtekind != RTE_VALUES &&
		rel->rtekind != RTE_TABLEFUNCTION &&
		rel->rtekind != RTE_CTE)
		return false;

	/*
	 * Can't do it with inheritance cases either (mainly because Append
	 * doesn't project).
	 */
	if (rel->reloptkind != RELOPT_BASEREL)
		return false;

	/*
	 * Can't do it if any system columns or whole-row Vars are requested,
	 * either.	(This could possibly be fixed but would take some fragile
	 * assumptions in setrefs.c, I think.)
	 */
	for (i = rel->min_attr; i <= 0; i++)
	{
		if (!bms_is_empty(rel->attr_needed[i - rel->min_attr]))
			return false;
	}

    /* CDB: Don't use physical tlist if rel has pseudo columns. */
    rte = rt_fetch(rel->relid, ctx->root->parse->rtable);
    if (rte->pseudocols)
        return false;

	return true;
}

/*
 * disuse_physical_tlist
 *		Switch a plan node back to emitting only Vars actually referenced.
 *
 * If the plan node immediately above a scan would prefer to get only
 * needed Vars and not a physical tlist, it must call this routine to
 * undo the decision made by use_physical_tlist().	Currently, Hash, Sort,
 * and Material nodes want this, so they don't have to store useless columns.
 * We need to ensure that all vars referenced in Flow node, if any, are added
 * to the targetlist as resjunk.
 */
static void
disuse_physical_tlist(Plan *plan, Path *path)
{
	/* Only need to undo it for path types handled by create_scan_plan() */
	switch (path->pathtype)
	{
		case T_SeqScan:
		case T_AppendOnlyScan:
		case T_ParquetScan:
		case T_ExternalScan:
		case T_IndexScan:
		case T_BitmapHeapScan:
		case T_BitmapAppendOnlyScan:
		case T_BitmapTableScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_ValuesScan:
			{
				plan->targetlist = build_relation_tlist(path->parent);
				/**
				 * If plan has a flow node, ensure all entries of hashExpr
				 * are in the targetlist.
				 */
				if (plan->flow
						&& plan->flow->hashExpr)
				{
					plan->targetlist = add_to_flat_tlist(plan->targetlist, plan->flow->hashExpr, true /* resjunk */);
				}
			}
			break;
		default:
			break;
	}
}

/*
 * create_gating_plan
 *	  Deal with pseudoconstant qual clauses
 *
 * If the node's quals list includes any pseudoconstant quals, put them
 * into a gating Result node atop the already-built plan.  Otherwise,
 * return the plan as-is.
 *
 * Note that we don't change cost or size estimates when doing gating.
 * The costs of qual eval were already folded into the plan's startup cost.
 * Leaving the size alone amounts to assuming that the gating qual will
 * succeed, which is the conservative estimate for planning upper queries.
 * We certainly don't want to assume the output size is zero (unless the
 * gating qual is actually constant FALSE, and that case is dealt with in
 * clausesel.c).  Interpolating between the two cases is silly, because
 * it doesn't reflect what will really happen at runtime, and besides which
 * in most cases we have only a very bad idea of the probability of the gating
 * qual being true.
 */
static Plan *
create_gating_plan(CreatePlanContext *ctx, Plan *plan, List *quals)
{
	List	   *pseudoconstants;

	/* Pull out any pseudoconstant quals from the RestrictInfo list */
	pseudoconstants = extract_actual_clauses(quals, true);

	if (!pseudoconstants)
		return plan;

	pseudoconstants = order_qual_clauses(ctx->root, pseudoconstants);

	return (Plan *) make_result((List *) copyObject(plan->targetlist),
								(Node *) pseudoconstants,
								plan);
}

/*
 * create_join_plan
 *	  Create a join plan for 'best_path' and (recursively) plans for its
 *	  inner and outer paths.
 */
static Plan *
create_join_plan(CreatePlanContext *ctx, JoinPath *best_path)
{
	Plan	   *outer_plan;
	Plan	   *inner_plan;
	Plan	   *plan;

    Assert(best_path->outerjoinpath);
    Assert(best_path->innerjoinpath);

    outer_plan = create_subplan(ctx, best_path->outerjoinpath);
	inner_plan = create_subplan(ctx, best_path->innerjoinpath);

	switch (best_path->path.pathtype)
	{
		case T_MergeJoin:
			plan = (Plan *) create_mergejoin_plan(ctx,
												  (MergePath *) best_path,
												  outer_plan,
												  inner_plan);
			break;
		case T_HashJoin:
			plan = (Plan *) create_hashjoin_plan(ctx,
												 (HashPath *) best_path,
												 outer_plan,
												 inner_plan);
			break;
		case T_NestLoop:
			plan = create_nestloop_plan(ctx,
										(NestPath *) best_path,
										outer_plan,
										inner_plan);
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) best_path->path.pathtype);
			plan = NULL;		/* keep compiler quiet */
			break;
	}

	plan->flow = cdbpathtoplan_create_flow(ctx->root,
			best_path->path.locus,
			best_path->path.parent ? best_path->path.parent->relids
					: NULL,
					  best_path->path.pathkeys,
					  plan);

	/**
	 * If plan has a flow node, ensure all entries of hashExpr
	 * are in the targetlist.
	 */
	if (plan->flow
			&& plan->flow->hashExpr)
	{
		plan->targetlist = add_to_flat_tlist(plan->targetlist, plan->flow->hashExpr, true /* resjunk */);
	}

	/*
	 * If there are any pseudoconstant clauses attached to this node, insert a
	 * gating Result node that evaluates the pseudoconstants as one-time
	 * quals.
	 */
	if (ctx->root->hasPseudoConstantQuals)
	{
		/* MPP-2328: don't create a gating plan for the hash-child of
		 * a NL-join */
		if (!IsA(plan, HashJoin) || outer_plan != NULL)
			plan = create_gating_plan(ctx, plan, best_path->joinrestrictinfo);
	}

#ifdef NOT_USED

	/*
	 * * Expensive function pullups may have pulled local predicates * into
	 * this path node.	Put them in the qpqual of the plan node. * JMH,
	 * 6/15/92
	 */
	if (get_loc_restrictinfo(best_path) != NIL)
		set_qpqual((Plan) plan,
				   list_concat(get_qpqual((Plan) plan),
					   get_actual_clauses(get_loc_restrictinfo(best_path))));
#endif

	return plan;
}

/*
 * create_append_plan
 *	  Create an Append plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan *
create_append_plan(CreatePlanContext *ctx, AppendPath *best_path)
{
	Append	   *plan;
	List	   *tlist = build_relation_tlist(best_path->path.parent);
	List	   *subplans = NIL;
	ListCell   *subpaths;

	/*
	 * It is possible for the subplans list to contain only one entry, or even
	 * no entries.	Handle these cases specially.
	 *
	 * XXX ideally, if there's just one entry, we'd not bother to generate an
	 * Append node but just return the single child.  At the moment this does
	 * not work because the varno of the child scan plan won't match the
	 * parent-rel Vars it'll be asked to emit.
	 */
	if (best_path->subpaths == NIL)
	{
		/* Generate a Result plan with constant-FALSE gating qual */
		return (Plan *) make_result(tlist,
									(Node *) list_make1(makeBoolConst(false,
																	  false)),
									NULL);
	}

	/* Normal case with multiple subpaths */
	foreach(subpaths, best_path->subpaths)
	{
		Path	   *subpath = (Path *) lfirst(subpaths);

		subplans = lappend(subplans, create_subplan(ctx, subpath));
	}

	plan = make_append(subplans, false, tlist);
	return (Plan *) plan;
}

/*
 * create_result_plan
 *	  Create a Result plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Result *
create_result_plan(CreatePlanContext *ctx, ResultPath *best_path)
{
	Result	   *plan;
	List	   *tlist;
	List	   *quals;
	Plan	   *subplan;

	if (best_path->path.parent)
		tlist = build_relation_tlist(best_path->path.parent);
	else
		tlist = NIL;			/* will be filled in later */

	if (best_path->subpath)
		subplan = create_subplan(ctx, best_path->subpath);
	else
		subplan = NULL;

	quals = order_qual_clauses(ctx->root, best_path->quals);

	plan = make_result(tlist, (Node *)quals, subplan);

	return plan;
}

/*
 * create_material_plan
 *	  Create a Material plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Material *
create_material_plan(CreatePlanContext *ctx, MaterialPath *best_path)
{
	Material   *plan;
	Plan	   *subplan;

	subplan = create_subplan(ctx, best_path->subpath);

	/* We don't want any excess columns in the materialized tuples */
	disuse_physical_tlist(subplan, best_path->subpath);

	plan = make_material(subplan);

    plan->cdb_strict = best_path->cdb_strict;

	copy_path_costsize(ctx->root, &plan->plan, (Path *) best_path);

	return plan;
}


/*
 * create_unique_plan
 *	  Create a Unique plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan *
create_unique_plan(CreatePlanContext *ctx, UniquePath *best_path)
{
	Plan	   *plan;
	Plan	   *subplan;
	List	   *uniq_exprs;
	List	   *newtlist;
	int			nextresno;
	bool		newitems;
	int			numGroupCols;
	AttrNumber *groupColIdx;
	int			groupColPos;
	ListCell   *l;

    subplan = create_subplan(ctx, best_path->subpath);

	/* Return naked subplan if we don't need to do any actual unique-ifying */
	if (best_path->umethod == UNIQUE_PATH_NOOP)
		return subplan;

    /* CDB: Result will surely be unique if we produce only its first row. */
    if (best_path->umethod == UNIQUE_PATH_LIMIT1)
    {
        Limit  *limitplan;

        /* Top off the subplan with a LIMIT 1 node. */
        limitplan = makeNode(Limit);
        copy_path_costsize(ctx->root, &limitplan->plan, &best_path->path);
        limitplan->plan.targetlist = subplan->targetlist;
        limitplan->plan.qual = NIL;
        limitplan->plan.lefttree = subplan;
        limitplan->plan.righttree = NULL;
        limitplan->limitOffset = NULL;
        limitplan->limitCount = (Node *)makeConst(INT8OID, -1, sizeof(int64),
                                                  Int64GetDatum(1),
                                                  false, true);
        return (Plan *)limitplan;
    }

	/*----------
	 * As constructed, the subplan has a "flat" tlist containing just the
	 * Vars needed here and at upper levels.  The values we are supposed
	 * to unique-ify may be expressions in these variables.  We have to
	 * add any such expressions to the subplan's tlist.
	 *
	 * The subplan may have a "physical" tlist if it is a simple scan plan.
	 * This should be left as-is if we don't need to add any expressions;
	 * but if we do have to add expressions, then a projection step will be
	 * needed at runtime anyway, and so we may as well remove unneeded items.
	 * Therefore newtlist starts from build_relation_tlist() not just a
	 * copy of the subplan's tlist; and we don't install it into the subplan
	 * unless stuff has to be added.
	 *----------
	 */
	uniq_exprs = best_path->distinct_on_exprs;  /*CDB*/
    Insist(uniq_exprs);

	/* initialize modified subplan tlist as just the "required" vars */
	newtlist = build_relation_tlist(best_path->path.parent);
	nextresno = list_length(newtlist) + 1;
	newitems = false;

	foreach(l, uniq_exprs)
	{
		Node	   *uniqexpr = lfirst(l);
		TargetEntry *tle;

		tle = tlist_member(uniqexpr, newtlist);
		if (!tle)
		{
			tle = makeTargetEntry((Expr *) uniqexpr,
								  nextresno,
								  NULL,
								  false);
			newtlist = lappend(newtlist, tle);
			nextresno++;
			newitems = true;
		}
	}

	/*
	 * If the top plan node can't do projections, we need to add a Result
	 * node to help it along.
	 */
	if (newitems)
		subplan = plan_pushdown_tlist(subplan, newtlist);

	/*
	 * Build control information showing which subplan output columns are to
	 * be examined by the grouping step.  Unfortunately we can't merge this
	 * with the previous loop, since we didn't then know which version of the
	 * subplan tlist we'd end up using.
	 */
	newtlist = subplan->targetlist;
	numGroupCols = list_length(uniq_exprs);
	groupColIdx = (AttrNumber *) palloc(numGroupCols * sizeof(AttrNumber));
	groupColPos = 0;

	foreach(l, uniq_exprs)
	{
		Node	   *uniqexpr = lfirst(l);
		TargetEntry *tle;

		tle = tlist_member(uniqexpr, newtlist);
		if (!tle)				/* shouldn't happen */
			elog(ERROR, "failed to find unique expression in subplan tlist");
		groupColIdx[groupColPos++] = tle->resno;
	}

	if (best_path->umethod == UNIQUE_PATH_HASH)
	{
		long		numGroups;

		numGroups = (long) Min(best_path->rows, (double) LONG_MAX);

		/*
		 * Since the Agg node is going to project anyway, we can give it the
		 * minimum output tlist, without any stuff we might have added to the
		 * subplan tlist.
		 */
		plan = (Plan *) make_agg(ctx->root,
								 build_relation_tlist(best_path->path.parent),
								 NIL,
								 AGG_HASHED, false,
								 numGroupCols,
								 groupColIdx,
								 numGroups,
								 0, /* num_nullcols */
								 0, /* input_grouping */
								 0, /* grouping */
								 0, /* rollup_gs_times */
								 0,
								 0,
								 subplan);
	}
	else
	{
		List	   *sortList = NIL;

		for (groupColPos = 0; groupColPos < numGroupCols; groupColPos++)
		{
			TargetEntry *tle;

			tle = get_tle_by_resno(subplan->targetlist,
								   groupColIdx[groupColPos]);
			Assert(tle != NULL);
			sortList = addTargetToSortList(NULL, tle,
										   sortList, subplan->targetlist,
										   SORTBY_ASC, NIL, false);
		}
		plan = (Plan *) make_sort_from_sortclauses(ctx->root, sortList, subplan);
		plan = (Plan *) make_unique(plan, sortList);
	}

	/* Adjust output size estimate (other fields should be OK already) */
	plan->plan_rows = cdbpath_rows(ctx->root, (Path *)best_path);

	return plan;
}


/*
 * create_motion_plan
 */
Plan *
create_motion_plan(CreatePlanContext *ctx, CdbMotionPath *path)
{
    Motion *motion;
    Path   *subpath = path->subpath;
    Plan   *subplan;

    /*
     * singleQE-->entry:  Elide the motion.  The subplan will run in the same
     * process with its parent: either the qDisp (if it is a top slice) or a
     * singleton gang on the entry db (otherwise).
     */
    if (CdbPathLocus_IsEntry(path->path.locus) &&
        CdbPathLocus_IsSingleQE(subpath->locus))
    {
        /* Push the MotionPath's locus and pathkeys down onto subpath. */
        subpath->locus = path->path.locus;
        subpath->pathkeys = path->path.pathkeys;

        subplan = create_subplan(ctx, subpath);
        return subplan;
    }

    subplan = create_subplan(ctx, subpath);

    /* Only the needed columns should be projected from base rel. */
    disuse_physical_tlist(subplan, subpath);

    /* Add motion operator. */
    motion = cdbpathtoplan_create_motion_plan(ctx->root, path, subplan);

    copy_path_costsize(ctx->root, &motion->plan, (Path *)path);
    return (Plan *)motion;
}                               /* create_motion_plan */


/*****************************************************************************
 *
 *	BASE-RELATION SCAN METHODS
 *
 *****************************************************************************/


/*
 * create_seqscan_plan
 *	 Returns a seqscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SeqScan *
create_seqscan_plan(CreatePlanContext *ctx, Path *best_path,
					List *tlist, List *scan_clauses)
{
	SeqScan    *scan_plan;
	Index		scan_relid = best_path->parent->relid;

	/* it should be a base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->parent->rtekind == RTE_RELATION);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(ctx->root, scan_clauses);

	scan_plan = make_seqscan(tlist,
							 scan_clauses,
							 scan_relid);

	copy_path_costsize(ctx->root, &scan_plan->plan, best_path);

	return scan_plan;
}

/*
 * create_appendonlyscan_plan
 *	 Returns a appendonlyscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static AppendOnlyScan *
create_appendonlyscan_plan(CreatePlanContext *ctx, Path *best_path,
						   List *tlist, List *scan_clauses)
{
	AppendOnlyScan		*scan_plan;
	Index				scan_relid = best_path->parent->relid;

	/* it should be a base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->parent->rtekind == RTE_RELATION);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(ctx->root, scan_clauses);

	scan_plan = make_appendonlyscan(tlist,
									scan_clauses,
									scan_relid);

	copy_path_costsize(ctx->root, &scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_parquetscan_plan
 *	 Returns a parquetscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static ParquetScan *
create_parquetscan_plan(CreatePlanContext *ctx, Path *best_path,
						   List *tlist, List *scan_clauses)
{
	ParquetScan		*scan_plan;
	Index				scan_relid = best_path->parent->relid;

	/* it should be a base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->parent->rtekind == RTE_RELATION);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(ctx->root, scan_clauses);

	scan_plan = make_parquetscan(tlist,
									scan_clauses,
									scan_relid);

	copy_path_costsize(ctx->root, &scan_plan->scan.plan, best_path);

	return scan_plan;
}


/*
 * is_pxf_protocol
 * tests if the external table custom protocol is an HADOOP protocol - pxf
 */
bool is_pxf_protocol(Uri *uri)
{	
	if (uri->protocol != URI_CUSTOM || uri->customprotocol == NULL)
		return false;
	
	if (strcmp(uri->customprotocol, "pxf") == 0)
		return true;

	return false;
}

/*
 * create plan for pxf
 */
static char** create_pxf_plan(char **segdb_file_map, RelOptInfo *rel, int total_segs, 
							  CreatePlanContext *ctx, Index scan_relid)
{
	int i;
	char **segdb_work_map;
	int segs_participating = pxf_calc_participating_segments(total_segs);
	char *uri_str = (char *) strVal(linitial(rel->locationlist));
	
	if(total_segs > segs_participating)
		elog(NOTICE, "External scan using PXF protocol will utilize %d out "
			 "of %d segment databases", segs_participating, total_segs);
	
	
	Relation relation = RelationIdGetRelation(planner_rt_fetch(scan_relid, ctx->root)->relid);
	segdb_work_map = map_hddata_2gp_segments(uri_str, 
											 total_segs, segs_participating,
											 relation, NULL);
	Assert(segdb_work_map != NULL);
	RelationClose(relation);
	
	for (i = 0; i < total_segs; i++)
	{
		if(segdb_work_map[i] == NULL)
			continue;
				
		/* 
		 * We require a data-fragments allocation in
		 * segdb_work_map for segment i in order to activate segment
		 * in segdb_file_map[i]
		 */
		char opt_delim = (strchr(uri_str, '?') == NULL) ? '?' : '&';
		StringInfoData seg_uri;
		
		initStringInfoOfSize(&seg_uri, 512);
		appendStringInfoString(&seg_uri, uri_str);
		appendStringInfoChar(&seg_uri, opt_delim);
		appendStringInfoString(&seg_uri, segdb_work_map[i]);
		segdb_file_map[i] = seg_uri.data;						
	}
	free_hddata_2gp_segments(segdb_work_map, total_segs);
	return segdb_file_map;
}


/*
 * create_externalscan_plan
 *	 Returns an externalscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 *
 *   The external plan also includes the data format specification and file
 *   location specification. Here is where we do the mapping of external file
 *   to segment database and add it to the plan (or bail out of the mapping
 *   rules are broken)
 *
 *   Mapping rules
 *   -------------
 *   - 'file' protocol: each location (URI of local file) gets mapped to one 
 *                      and one only primary segdb.
 *   - 'http' protocol: each location (URI of http server) gets mapped to one 
 *                      and one only primary segdb.
 *   - 'gpfdist' and 'gpfdists' protocols: all locations (URI of gpfdist(s) client) are mapped
 *                      to all primary segdbs. If there are less URIs than
 *                      segdbs (usually the case) the URIs are duplicated
 *                      so that there will be one for each segdb. However, if
 *                      the GUC variable gp_external_max_segs is set to a num
 *                      less than (total segdbs/total URIs) then we make sure
 *                      that no URI gets mapped to more than this GUC number by
 *                      skipping some segdbs randomly.
 *   - 'exec' protocol: all segdbs get mapped to execute the command (this is
 *                      soon to be changed though).
 *   - 'pxf' protocol: has its own mapping logic. See is_pxf_protocol() and
 *   					map_hddata_2gp_segments() below.
 */                     
static ExternalScan *
create_externalscan_plan(CreatePlanContext *ctx, Path *best_path,
						 List *tlist, List *scan_clauses)
{
	ExternalScan    *scan_plan;
	Index			scan_relid = best_path->parent->relid;
	RelOptInfo		*rel = best_path->parent;
	Uri				*uri = NULL;
	List			*filenames = NIL;
	List			*fmtopts = NIL;
	List			*modifiedloclist = NIL;
	ListCell		*c = NULL;
	char			**segdb_file_map = NULL;
	char			*first_uri_str = NULL;
	bool			ismasteronly = false;
	bool			islimitinrows = false;
	int				rejectlimit = -1;
	int				encoding = -1;
	int				total_primaries = 1;
	int				i;
	Oid				fmtErrTblOid = InvalidOid;
	List			*segments = NIL;
	ListCell		*lc;
	bool allocatedResource = (ctx->root->glob->resource != NULL);

	/* various processing flags */
	bool			using_execute = false; /* true if EXECUTE is used */
	bool			using_location = false; /* true if LOCATION is used */
	bool			found_candidate = false;
	bool			found_match = false;
	bool			done = false;

	/* gpfdist(s) or EXECUTE specific variables */
	int				total_to_skip = 0;
	int				max_participants_allowed = 0;
	int			    num_segs_participating = 0;
	bool			*skip_map = NULL;
	bool			should_skip_randomly = false;

	/* it should be an external rel... */
	Assert(scan_relid > 0);
	Assert(rel->rtekind == RTE_RELATION);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(ctx->root, scan_clauses);

	/* get the total valid primary segdb count */
	if (allocatedResource)
	{
	  segments = ctx->root->glob->resource->segments;
	  total_primaries = list_length(segments);
	}

	/*
	 * initialize a file-to-segdb mapping. segdb_file_map string array indexes
 	 * segindex and the entries are the external file path is assigned to this
 	 * segment datbase. For example if segdb_file_map[2] has "/tmp/emp.1" then
 	 * this file is assigned to primary segdb 2. if an entry has NULL then that
 	 * segdb isn't assigned any file.
 	 */
	segdb_file_map = (char **) palloc(total_primaries * sizeof(char *));
	MemSet(segdb_file_map, 0, total_primaries * sizeof(char *));
	Assert(rel->locationlist != NIL);

	/* is this an EXECUTE table or a LOCATION (URI) table */
	if(rel->execcommand)
	{
		using_execute = true;
		using_location = false;
	}
	else
	{
		using_execute = false;
		using_location = true;
	}

	/* various validations */

	if(rel->writable && allocatedResource)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("it is not possible to read from a WRITABLE external table."),
				errhint("Create the table as READABLE instead"),
						 errOmitLocation(true)));

	if(rel->rejectlimit != -1)
	{
		/* 
		 * single row error handling is requested, make sure reject 
		 * limit and error table (if requested) are valid.
		 *
		 * NOTE: this should never happen unless somebody modified the 
		 * catalog manually. We are just being pedantic here.
		 */
		VerifyRejectLimit(rel->rejectlimittype, rel->rejectlimit);
	}

	if(using_execute && !gp_external_enable_exec)
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_CONFIGURED), /* any better errcode? */
				errmsg("Using external tables with OS level commands "
					   "(EXECUTE clause) is disabled"),
				errhint("To enable set gp_external_enable_exec=on"),
						 errOmitLocation(true)));

	/* 
	 * take a peek at the first URI so we know which protocol we'll deal with 
	 */
	if(!using_execute)
	{
		first_uri_str = (char *) strVal(lfirst(list_head(rel->locationlist)));
		uri = ParseExternalTableUri(first_uri_str);		
	}


  /* in HAWQ 2.0, file protocol is not supported any more. */
  if (using_location && (uri->protocol == URI_FILE))
  {
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("the file protocol for external tables is deprecated"),
        errhint("use the gpfdist protocol or COPY FROM instead"),
        errOmitLocation(true)));
  }

	/*
	 * Now we do the actual assignment of work to the segment databases (where 
	 * work is either a URI to open or a command to execute). Due to the 
	 * big differences between the different protocols we handle each one
	 * separately. Unfortunately this means some code duplication, but keeping
	 * this separation makes the code much more understandable and (even) 
	 * more maintainable.
	 *
	 * Outline of the following code blocks (from simplest to most complex):
	 * (only one of these will get executed for a statement)
	 *
	 * 1) segment mapping for tables with LOCATION http:// or file:// . 
	 *
	 * These two protocols are very similar in that they enforce a 1-URI:1-segdb
	 * relationship. The only difference between them is that file:// URI must 
	 * be assigned to a segdb on a host that is local to that URI.
	 *
	 * 2) segment mapping for tables with LOCATION gpfdist(s):// or custom protocol
	 *
	 * This protocol is more complicated - in here we usually duplicate the user
	 * supplied gpfdist(s):// URIs until there is one available to every segdb.
	 * However, in some cases (as determined by gp_external_max_segs GUC) we
	 * don't want to use *all* segdbs but instead figure out how many and pick
	 * them randomly (this is mainly for better performance and resource mgmt).
	 *
	 * 3) segment mapping for tables with EXECUTE 'cmd' ON.
	 *
	 * In here we don't have URI's. We have a single command string and a
	 * specification of the segdb granularity it should get executed on (the
	 * ON clause). Depending on the ON clause specification we could go many
	 * different ways, for example: assign the command to all segdb, or one
	 * command per host, or assign to 5 random segments, etc...
	 */

	/* (1) */
	if(using_location && (uri->protocol == URI_FILE || uri->protocol == URI_HTTP))
	{
		/* 
		 * extract file path and name from URI strings and assign them a primary segdb 
		 */
		foreach(c, rel->locationlist)
		{
			const char	*uri_str = (char *) strVal(lfirst(c));

			uri = ParseExternalTableUri(uri_str);

			found_candidate = false;
			found_match = false;


			/*
			 * look through our segment database list and try to find
			 * a database that can handle this uri.
			 */
			foreach(lc, segments)
			{
				Segment	*segment = lfirst(lc);
				int segind = segment->segindex;

				/* 
				 * Assign mapping of external file to this segdb only if:
				 * 1) This segdb is a valid primary.
				 * 2) An external file wasn't already assigned to it. 
				 * 3) If 'file' protocol, host of segdb and file must be 
				 *    the same.
				 *
				 * This logic also guarantees that file that appears first in 
				 * the external location list for the same host gets assigned
				 * the segdb with the lowest index for this host.
				 */			
				if(uri->protocol == URI_FILE)
				{
				  /* Hack the code to pass some regression test.
				   * In the future, we will remove this feature.
				   */
					if (pg_strcasecmp(uri->hostname, segment->hostname) != 0 && pg_strcasecmp(uri->hostname, segment->hostip) != 0
					    && pg_strcasecmp(uri->hostname, "localhost") != 0)
						continue;
				}

				/* a valid primary segdb exist on this host */
				found_candidate = true;

				if(segdb_file_map[segind] == NULL)
				{
					/* segdb not taken yet. assign this URI to this segdb */
					segdb_file_map[segind] = pstrdup(uri_str);
					found_match = true;
					break;
				}

				/* too bad. this segdb already has an external source assigned */
			}

			/*
			 * We failed to find a segdb for this URI.
			 */
			if(allocatedResource && (!found_match))
			{
				if(uri->protocol == URI_FILE)
				{
					if(found_candidate)
					{
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								 errmsg("Could not assign a segment database for \"%s\". "
										"There are more external files than primary segment "
										"databases on host \"%s\"", uri_str, uri->hostname),
												 errOmitLocation(true)));
					}
					else
					{
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								 errmsg("Could not assign a segment database for \"%s\". "
										"There isn't a valid primary segment database "
										"on host \"%s\"", uri_str, uri->hostname),
												 errOmitLocation(true)));
					}
				}
				else /* HTTP */
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("Could not assign a segment database for \"%s\". "
									"There are more URIs than total primary segment "
									"databases", uri_str),
											 errOmitLocation(true)));
				}
			}		
		}


	} /* PXF */
	else if (using_location && uri->protocol == URI_CUSTOM && is_pxf_protocol(uri))
	{
		segdb_file_map = create_pxf_plan(segdb_file_map, rel, total_primaries, ctx, scan_relid);

	}
	/* (2) */
	else if(using_location && (uri->protocol == URI_GPFDIST || 
							   uri->protocol == URI_GPFDISTS || 
							   (uri->protocol == URI_CUSTOM && !is_pxf_protocol(uri)) ))
	{
		/*
		 * Re-write the location list for GPFDIST or GPFDISTS before mapping to segments.
		 *
		 * If we happen to be dealing with URI's with the 'gpfdist' (or 'gpfdists') protocol
		 * we do an extra step here. 
		 *
		 * (*) We modify the locationlist so that every
		 * primary segdb will get a URI (therefore we duplicate the existing
		 * URI's until the list is of size = total_primaries). 
		 * Example: 2 URIs, 7 total segdbs.
		 * Original LocationList: URI1->URI2
		 * Modified LocationList: URI1->URI2->URI1->URI2->URI1->URI2->URI1
		 *
		 * (**) We also make sure that we don't allocate more segdbs than 
		 * (# of URIs x gp_external_max_segs). 
		 * Example: 2 URIs, 7 total segdbs, gp_external_max_segs = 3
		 * Original LocationList: URI1->URI2
		 * Modified LocationList: URI1->URI2->URI1->URI2->URI1->URI2 (6 total).
		 *
		 * (***) In that case that we need to allocate only a subset of primary 
		 * segdbs and not all we then also create a random map of segments to skip.
		 * Using the previous example a we create a map of 7 entries and need to
		 * randomly select 1 segdb to skip (7 - 6 = 1). so it may look like this:
		 * [F F T F F F F] - in which case we know to skip the 3rd segment only.
		 */
		/* total num of segs that will participate in the external operation */
		num_segs_participating = total_primaries;

		/* max num of segs that are allowed to participate in the operation */
		if ((uri->protocol == URI_GPFDIST) || (uri->protocol == URI_GPFDISTS))
		{
			max_participants_allowed = list_length(rel->locationlist) * 
			gp_external_max_segs;
		}
		else
		{
			/* for custom protocol, set max_participants_allowed to num_segs_participating
			 * so that assignment to segments will use all available segments */
			max_participants_allowed = num_segs_participating;
		}

		elog(DEBUG5,
			 "num_segs_participating = %d. max_participants_allowed = %d. number of URIs = %d",
			 num_segs_participating, max_participants_allowed, list_length(rel->locationlist));

		/* see (**) above */
		if(num_segs_participating > max_participants_allowed)
		{
			total_to_skip = num_segs_participating - max_participants_allowed;
			num_segs_participating = max_participants_allowed;
			should_skip_randomly = true;

			elog(NOTICE, "External scan %s will utilize %d out "
				 "of %d segment databases", 
				 (((uri->protocol == URI_GPFDIST) || (uri->protocol == URI_GPFDISTS)) ?
				 "from gpfdist(s) server" : "using custom protocol"),
				 num_segs_participating,
				 total_primaries);
		}

		if(allocatedResource && (list_length(rel->locationlist) > num_segs_participating))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("There are more external files (URLs) than primary "
							"segments that can read them. Found %d URLs and "
							"%d primary segments.",list_length(rel->locationlist),
							num_segs_participating),
									 errOmitLocation(true)));

		/* 
		 * restart location list and fill in new list until number of
		 * locations equals the number of segments participating in this
		 * action (see (*) above for more details).
		 */
		while(!done)
		{
			foreach(c, rel->locationlist)
			{
				char *uri_str = (char *) strVal(lfirst(c));

				/* append to a list of Value nodes, size nelems */
				modifiedloclist = lappend(modifiedloclist, makeString(pstrdup(uri_str)));				

				if(list_length(modifiedloclist) >= num_segs_participating)
				{
					done = true;
					break;
				}

				if(allocatedResource && (list_length(modifiedloclist) > num_segs_participating))
				{
					elog(ERROR, "External scan location list failed building distribution.");
				}
			}
		}

		/* See (***) above for details */
		if(should_skip_randomly)
			skip_map = makeRandomSegMap(total_primaries, total_to_skip);
		
		/* 
		 * assign each URI from the new location list a primary segdb 
		 */
		foreach(c, modifiedloclist)
		{
			const char	*uri_str = (char *) strVal(lfirst(c));
			found_match = false;

			/*
			 * look through our segment database list and try to find
			 * a database that can handle this uri.
			 */
			foreach(lc, segments)
			{
				Segment	*segment = lfirst(lc);
				int segind = segment->segindex;

				/* 
				 * Assign mapping of external file to this segdb only if:
				 * 1) This segdb is a valid primary.
				 * 2) An external file wasn't already assigned to it. 
				 */			
				/*
				 * skip this segdb if skip_map for this seg
				 * index tells us to skip it (set to 'true').
				 */
				if(should_skip_randomly)
				{
					Assert(segind < total_primaries);

					if(skip_map[segind])
						continue; /* skip it */					
				}

				if (segdb_file_map[segind] == NULL) /* segment was not already activated */
				{
					segdb_file_map[segind] = pstrdup(uri_str);
					found_match = true;
					break;
				}
			}

			/* 
			 * We failed to find a segdb for this gpfdist(s) URI 
			 * when is_custom_hd is true it means that the HD segment allocation algorithm
			 * is activated and in this case it is not necessarily true that all segments are allocated
			 */
			if(allocatedResource && (!found_match))
			{
				/* should never happen */
				elog(LOG, "external tables gpfdist(s) allocation error. "
					 "total_primaries: %d, num_segs_participating %d "
					 "max_participants_allowed %d, total_to_skip %d",
					 total_primaries, num_segs_participating,
					 max_participants_allowed, total_to_skip);

				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERNAL_ERROR),
						 errmsg("Internal error in createplan for external tables"
								" when trying to assign segments for gpfdist(s)")));
			}		
		}
	}
	/* (3) */
	else if(using_execute)
	{
		const char  *command = rel->execcommand;
		const char  *prefix = "execute:";
		char		*prefixed_command = NULL;
		char		*on_clause = NULL;
		bool		match_found = false;

		/* build the command string for the executor - 'execute:command' */
		StringInfo buf = makeStringInfo();

		appendStringInfo(buf, "%s%s", prefix, command);
		prefixed_command = pstrdup(buf->data);

		pfree(buf->data);
		pfree(buf);
		buf = NULL;

		/* get the ON clause (execute location) information */
		on_clause = (char *) strVal(lfirst(list_head(rel->locationlist)));

		/*
		 * Now we handle each one of the ON locations separately:
		 *
		 * 1) all segs
		 * 2) one per host
		 * 3) all segs on host <foo>
		 * 4) seg <n> only
		 * 5) <n> random segs
		 * 6) master only
		 */
		if(strcmp(on_clause, "ALL_SEGMENTS") == 0)
		{
			/* all segments get a copy of the command to execute */

			foreach(lc, segments)
			{
				Segment	*segment = lfirst(lc);
				int segind = segment->segindex;

				segdb_file_map[segind] = pstrdup(prefixed_command);					
			}

		}
		else if(strcmp(on_clause, "PER_HOST") == 0)
		{
			/* 1 seg per host */

			List	 *visited_hosts = NIL;

			foreach(lc, segments)
			{
				Segment	*segment = lfirst(lc);
				int segind = segment->segindex;
				bool	host_taken = false;
				ListCell *lcc = NULL;

				foreach(lcc, visited_hosts)
				{
					const char	*hostname = (char *) strVal(lfirst(lcc));

					if (pg_strcasecmp(hostname, segment->hostname) == 0)
					{
						host_taken = true;
						break;
					}
				}

				/* 
				 * if not assigned to a seg on this host before - do it now
				 * and add this hostname to the list so that we don't use
				 * segs on this host again.
				 */
				if (!host_taken)
				{
					segdb_file_map[segind] = pstrdup(prefixed_command);
					visited_hosts = lappend(visited_hosts, 
											makeString(pstrdup(segment->hostname)));
				}
			}
		}
		else if(strncmp(on_clause, "HOST:", strlen("HOST:")) == 0)
		{
			/* all segs on the specified host get copy of the command */
			char	*hostname = on_clause + strlen("HOST:");

			foreach(lc, segments)
			{
				Segment	*segment = lfirst(lc);
				int segind = segment->segindex;

				if (pg_strcasecmp(hostname, segment->hostname) == 0)
				{
					segdb_file_map[segind] = pstrdup(prefixed_command);
					match_found = true;
				}
			}

			if (allocatedResource && (!match_found))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("Could not assign a segment database for "
								"command \"%s\". No valid primary segment was "
								"found in the requested host name \"%s\" ",
								command, hostname),
										 errOmitLocation(true)));
		}
		else if(strncmp(on_clause, "SEGMENT_ID:", strlen("SEGMENT_ID:")) == 0)
		{
			/* 1 seg with specified id gets a copy of the command */

			int		target_segid = atoi(on_clause + strlen("SEGMENT_ID:"));

			foreach(lc, segments)
			{
				Segment	*segment = lfirst(lc);
				int segind = segment->segindex;

				if (segind == target_segid)	
				{
					segdb_file_map[segind] = pstrdup(prefixed_command);
					match_found = true;
				}
			}

			if(allocatedResource && (!match_found))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("Could not assign a segment database for "
								"command \"%s\". The requested segment id "
								"%d is not a valid primary segment or doesn't "
								"exist in the database", command, target_segid),
										 errOmitLocation(true)));
		}
		else if(strncmp(on_clause, "TOTAL_SEGS:", strlen("TOTAL_SEGS:")) == 0)
		{
			/* total n segments selected randomly */

			int		num_segs_to_use = atoi(on_clause + strlen("TOTAL_SEGS:"));

			if(allocatedResource && (num_segs_to_use > total_primaries))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("Table defined with EXECUTE ON %d but there "
								"are only %d valid primary segments in the "
								"database.", num_segs_to_use, total_primaries)));

			total_to_skip = total_primaries - num_segs_to_use;
			/* skip_map = makeRandomSegMap(total_primaries, total_to_skip); */
			{
				int tmp_index;
				skip_map = (bool *) palloc(total_primaries * sizeof(bool));
				MemSet(skip_map, false, total_primaries * sizeof(bool));
				for(tmp_index = num_segs_to_use; tmp_index < total_primaries; tmp_index++)
				{
					skip_map[tmp_index] = true;
				}
			}


			foreach(lc, segments)
			{
				Segment	*segment = lfirst(lc);
				int segind = segment->segindex;

				Assert(segind < total_primaries);
				if(skip_map[segind])
					continue; /* skip it */					

				segdb_file_map[segind] = pstrdup(prefixed_command);					
			}
		}
		else if(strcmp(on_clause, "MASTER_ONLY") == 0)
		{
			/* 
			 * store the command in first array entry and indicate
			 * that it is meant for the master segment (not seg o).
			 */
			segdb_file_map[0] = pstrdup(prefixed_command);
			ismasteronly = true;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
					 errmsg("Internal error in createplan for external tables: "
							"got invalid ON clause code %s", on_clause)));
		}
	}
	else
	{
		/* should never get here */
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("Internal error in createplan for external tables")));
	}


   /*
    * convert array map to a list so it can be serialized as part of the plan
    */
	for (i = 0; i < total_primaries; i++)
	{
	    if(segdb_file_map[i] != NULL)
	    	filenames = lappend(filenames, makeString(segdb_file_map[i]));
	    else
		{
			/* no file for this segdb. add a null entry */
			Value *n = makeNode(Value);
			n->type = T_Null;
			filenames = lappend(filenames, n);
		}	
	}

	/* data format description */
	Assert(rel->fmtopts);
	fmtopts = lappend(fmtopts, makeString(pstrdup(rel->fmtopts)));

	/* single row error handling */
	if(rel->rejectlimit != -1)
	{
		islimitinrows = (rel->rejectlimittype == 'r' ? true : false);
		rejectlimit = rel->rejectlimit;
		fmtErrTblOid = rel->fmterrtbl;
	}

	/* data encoding */
	encoding = rel->ext_encoding;

	scan_plan = make_externalscan(tlist,
								  scan_clauses,
								  scan_relid,
								  filenames,
								  fmtopts,
								  rel->fmttype,
								  ismasteronly,
								  rejectlimit,
								  islimitinrows,
								  fmtErrTblOid,
								  encoding);

	copy_path_costsize(ctx->root, &scan_plan->scan.plan, best_path);

	pfree(segdb_file_map);

	if(skip_map)
		pfree(skip_map);

	return scan_plan;
}

/*
 * Calculate participating_segments for pxf. Calculation is based on gp_external_max_segs
 * set by the user, the number of gp hosts - num_hosts and the number of segments - total_segments 
 * There are three cases:
 * a. The guc gp_external_max_segs is greater or equal to the total number of segments:
 *    participating_segments will be equal to the total number of segments
 * b. The guc gp_external_max_segs is between the number of GP hosts and the total number of segments
 *    In this case we distinguish between two subcases: 
 *    b.1 The guc gp_external_max_segs is a multiplication of the number of hosts: 
 *        participating_segments equals the guc gp_external_max_segs
 *    b.2 The guc gp_external_max_segs is not a multiply of the number of hosts:
 *        participating_segments will equal the smallest multiplication of the number of hosts
 *        that is greater than the guc gp_external_max_segs
 * c. The guc gp_external_max_segs is smaller all equal to the number of hosts.
 *    participating_segments equals the guc gp_external_max_segs
 */
int
pxf_calc_participating_segments(int total_segments)
{
	int participating_segments = 0;
	int num_hosts =  getgphostCount();
	
	if (gp_external_max_segs >= total_segments)
		participating_segments = total_segments;
	else if ( gp_external_max_segs < total_segments && gp_external_max_segs > num_hosts)
		participating_segments = (gp_external_max_segs % num_hosts == 0) ? gp_external_max_segs : 
		    num_hosts * (gp_external_max_segs / num_hosts + 1);
	else /* gp_external_max_segs <= num_hosts */
		participating_segments = gp_external_max_segs;
				
	return participating_segments;
}

/*
 * create_indexscan_plan
 *	  Returns an indexscan plan for the base relation scanned by 'best_path'
 *	  with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 *
 * The indexquals list of the path contains implicitly-ANDed qual conditions.
 * The list can be empty --- then no index restrictions will be applied during
 * the scan.
 *
 * If nonlossy_clauses isn't NULL, *nonlossy_clauses receives a list of the
 * nonlossy indexquals.
 */
static IndexScan *
create_indexscan_plan(CreatePlanContext *ctx,
					  IndexPath *best_path,
					  List *tlist,
					  List *scan_clauses,
					  List **nonlossy_clauses)
{
	List	   *indexquals = best_path->indexquals;
	Index		baserelid = best_path->path.parent->relid;
	Oid			indexoid = best_path->indexinfo->indexoid;
	List	   *qpqual;
	List	   *stripped_indexquals;
	List	   *fixed_indexquals;
	List	   *nonlossy_indexquals;
	List	   *indexstrategy;
	List	   *indexsubtype;
	ListCell   *l;
	IndexScan  *scan_plan;

	/* it should be a base rel... */
	Assert(baserelid > 0);
	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	/*
	 * Build "stripped" indexquals structure (no RestrictInfos) to pass to
	 * executor as indexqualorig
	 */
	stripped_indexquals = get_actual_clauses(indexquals);

	/*
	 * The executor needs a copy with the indexkey on the left of each clause
	 * and with index attr numbers substituted for table ones. This pass also
	 * gets strategy info and looks for "lossy" operators.
	 */
	fix_indexqual_references(indexquals, best_path,
							 &fixed_indexquals,
							 &nonlossy_indexquals,
							 &indexstrategy,
							 &indexsubtype);

	/* pass back nonlossy quals if caller wants 'em */
	if (nonlossy_clauses)
		*nonlossy_clauses = nonlossy_indexquals;

	/*
	 * If this is an innerjoin scan, the indexclauses will contain join
	 * clauses that are not present in scan_clauses (since the passed-in value
	 * is just the rel's baserestrictinfo list).  We must add these clauses to
	 * scan_clauses to ensure they get checked.  In most cases we will remove
	 * the join clauses again below, but if a join clause contains a special
	 * operator, we need to make sure it gets into the scan_clauses.
	 *
	 * Note: pointer comparison should be enough to determine RestrictInfo
	 * matches.
	 */
	if (best_path->isjoininner)
		scan_clauses = list_union_ptr(scan_clauses, best_path->indexclauses);

	/*
	 * The qpqual list must contain all restrictions not automatically handled
	 * by the index.  All the predicates in the indexquals will be checked
	 * (either by the index itself, or by nodeIndexscan.c), but if there are
	 * any "special" operators involved then they must be included in qpqual.
	 * Also, any lossy index operators must be rechecked in the qpqual.  The
	 * upshot is that qpqual must contain scan_clauses minus whatever appears
	 * in nonlossy_indexquals.
	 *
	 * In normal cases simple pointer equality checks will be enough to spot
	 * duplicate RestrictInfos, so we try that first.  In some situations
	 * (particularly with OR'd index conditions) we may have scan_clauses that
	 * are not equal to, but are logically implied by, the index quals; so we
	 * also try a predicate_implied_by() check to see if we can discard quals
	 * that way.  (predicate_implied_by assumes its first input contains only
	 * immutable functions, so we have to check that.)
	 *
	 * We can also discard quals that are implied by a partial index's
	 * predicate, but only in a plain SELECT; when scanning a target relation
	 * of UPDATE/DELETE/SELECT FOR UPDATE, we must leave such quals in the
	 * plan so that they'll be properly rechecked by EvalPlanQual testing.
	 *
	 * While at it, we strip off the RestrictInfos to produce a list of plain
	 * expressions (this loop replaces extract_actual_clauses used in the
	 * other routines in this file).  We have to ignore pseudoconstants.
	 */
	qpqual = NIL;
	foreach(l, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		Assert(IsA(rinfo, RestrictInfo));
		if (rinfo->pseudoconstant)
			continue;
		if (list_member_ptr(nonlossy_indexquals, rinfo))
			continue;
		if (!contain_mutable_functions((Node *) rinfo->clause))
		{
			List	   *clausel = list_make1(rinfo->clause);

			if (predicate_implied_by(clausel, nonlossy_indexquals))
				continue;
			if (best_path->indexinfo->indpred)
			{
				if ((int)baserelid != ctx->root->parse->resultRelation &&
					!list_member_int(ctx->root->parse->rowMarks, baserelid))
					if (predicate_implied_by(clausel,
											 best_path->indexinfo->indpred))
						continue;
			}
		}
		qpqual = lappend(qpqual, rinfo->clause);
	}

	/* Sort clauses into best execution order */
	qpqual = order_qual_clauses(ctx->root, qpqual);

	/* Finally ready to build the plan node */
	scan_plan = make_indexscan(tlist,
							   qpqual,
							   baserelid,
							   indexoid,
							   fixed_indexquals,
							   stripped_indexquals,
							   indexstrategy,
							   indexsubtype,
							   best_path->indexscandir);

	copy_path_costsize(ctx->root, &scan_plan->scan.plan, &best_path->path);

	return scan_plan;
}

/*
 * create_bitmap_scan_plan
 *	  Returns a bitmap scan plan for the base relation scanned by 'best_path'
 *	  with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static BitmapHeapScan *
create_bitmap_scan_plan(CreatePlanContext *ctx,
						BitmapHeapPath *best_path,
						List *tlist,
						List *scan_clauses)
{
	Index		baserelid = best_path->path.parent->relid;
	Plan	   *bitmapqualplan;
	List	   *bitmapqualorig = NULL;
	List	   *indexquals = NULL;
	List	   *qpqual;
	ListCell   *l;
	BitmapHeapScan *scan_plan;

	/* it should be a base rel... */
	Assert(baserelid > 0);
	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	/* Process the bitmapqual tree into a Plan tree and qual lists */
	bitmapqualplan = create_bitmap_subplan(ctx, best_path->bitmapqual,
										   &bitmapqualorig, &indexquals);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/*
	 * If this is a innerjoin scan, the indexclauses will contain join clauses
	 * that are not present in scan_clauses (since the passed-in value is just
	 * the rel's baserestrictinfo list).  We must add these clauses to
	 * scan_clauses to ensure they get checked.  In most cases we will remove
	 * the join clauses again below, but if a join clause contains a special
	 * operator, we need to make sure it gets into the scan_clauses.
	 */
	if (best_path->isjoininner)
	{
		scan_clauses = list_concat_unique(scan_clauses, bitmapqualorig);
	}

	/*
	 * The qpqual list must contain all restrictions not automatically handled
	 * by the index.  All the predicates in the indexquals will be checked
	 * (either by the index itself, or by nodeBitmapHeapscan.c), but if there
	 * are any "special" or lossy operators involved then they must be added
	 * to qpqual.  The upshot is that qpqual must contain scan_clauses minus
	 * whatever appears in indexquals.
	 *
	 * In normal cases simple equal() checks will be enough to spot duplicate
	 * clauses, so we try that first.  In some situations (particularly with
	 * OR'd index conditions) we may have scan_clauses that are not equal to,
	 * but are logically implied by, the index quals; so we also try a
	 * predicate_implied_by() check to see if we can discard quals that way.
	 * (predicate_implied_by assumes its first input contains only immutable
	 * functions, so we have to check that.)
	 *
	 * Unlike create_indexscan_plan(), we need take no special thought here
	 * for partial index predicates; this is because the predicate conditions
	 * are already listed in bitmapqualorig and indexquals.  Bitmap scans have
	 * to do it that way because predicate conditions need to be rechecked if
	 * the scan becomes lossy.
	 */
	qpqual = NIL;
	foreach(l, scan_clauses)
	{
		Node	   *clause = (Node *) lfirst(l);

		if (list_member(indexquals, clause))
			continue;
		if (!contain_mutable_functions(clause))
		{
			List	   *clausel = list_make1(clause);

			if (predicate_implied_by(clausel, indexquals))
				continue;
		}
		qpqual = lappend(qpqual, clause);
	}

	/* Sort clauses into best execution order */
	qpqual = order_qual_clauses(ctx->root, qpqual);

	/*
	 * When dealing with special or lossy operators, we will at this point
	 * have duplicate clauses in qpqual and bitmapqualorig.  We may as well
	 * drop 'em from bitmapqualorig, since there's no point in making the
	 * tests twice.
	 */
	bitmapqualorig = list_difference_ptr(bitmapqualorig, qpqual);

	/*
	 * Copy the finished bitmapqualorig to make sure we have an independent
	 * copy --- needed in case there are subplans in the index quals
	 */
	bitmapqualorig = copyObject(bitmapqualorig);

	/* Finally ready to build the plan node */
	scan_plan = make_bitmap_heapscan(tlist,
									 qpqual,
									 bitmapqualplan,
									 bitmapqualorig,
									 baserelid);

	copy_path_costsize(ctx->root, &scan_plan->scan.plan, &best_path->path);

	return scan_plan;
}

/*
 * create_bitmap_appendonly_scan_plan
 *
 * NOTE: Copy of create_bitmap_scan_plan routine.
 */
static BitmapAppendOnlyScan *
create_bitmap_appendonly_scan_plan(CreatePlanContext *ctx,
						BitmapAppendOnlyPath *best_path,
						List *tlist,
						List *scan_clauses)
{
	Index		baserelid = best_path->path.parent->relid;
	Plan	   *bitmapqualplan;
	List	   *bitmapqualorig = NULL;
	List	   *indexquals = NULL;
	List	   *qpqual;
	ListCell   *l;
	BitmapAppendOnlyScan *scan_plan;

	/* it should be a base rel... */
	Assert(baserelid > 0);
	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	/* Process the bitmapqual tree into a Plan tree and qual lists */
	bitmapqualplan = create_bitmap_subplan(ctx, best_path->bitmapqual,
										   &bitmapqualorig, &indexquals);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/*
	 * If this is a innerjoin scan, the indexclauses will contain join clauses
	 * that are not present in scan_clauses (since the passed-in value is just
	 * the rel's baserestrictinfo list).  We must add these clauses to
	 * scan_clauses to ensure they get checked.  In most cases we will remove
	 * the join clauses again below, but if a join clause contains a special
	 * operator, we need to make sure it gets into the scan_clauses.
	 */
	if (best_path->isjoininner)
	{
		scan_clauses = list_concat_unique(scan_clauses, bitmapqualorig);
	}

	/*
	 * The qpqual list must contain all restrictions not automatically handled
	 * by the index.  All the predicates in the indexquals will be checked
	 * (either by the index itself, or by nodeBitmapHeapscan.c), but if there
	 * are any "special" or lossy operators involved then they must be added
	 * to qpqual.  The upshot is that qpqual must contain scan_clauses minus
	 * whatever appears in indexquals.
	 *
	 * In normal cases simple equal() checks will be enough to spot duplicate
	 * clauses, so we try that first.  In some situations (particularly with
	 * OR'd index conditions) we may have scan_clauses that are not equal to,
	 * but are logically implied by, the index quals; so we also try a
	 * predicate_implied_by() check to see if we can discard quals that way.
	 * (predicate_implied_by assumes its first input contains only immutable
	 * functions, so we have to check that.)
	 *
	 * Unlike create_indexscan_plan(), we need take no special thought here
	 * for partial index predicates; this is because the predicate conditions
	 * are already listed in bitmapqualorig and indexquals.  Bitmap scans have
	 * to do it that way because predicate conditions need to be rechecked if
	 * the scan becomes lossy.
	 */
	qpqual = NIL;
	foreach(l, scan_clauses)
	{
		Node	   *clause = (Node *) lfirst(l);

		if (list_member(indexquals, clause))
			continue;
		if (!contain_mutable_functions(clause))
		{
			List	   *clausel = list_make1(clause);

			if (predicate_implied_by(clausel, indexquals))
				continue;
		}
		qpqual = lappend(qpqual, clause);
	}

	/* Sort clauses into best execution order */
	qpqual = order_qual_clauses(ctx->root, qpqual);

	/*
	 * When dealing with special or lossy operators, we will at this point
	 * have duplicate clauses in qpqual and bitmapqualorig.  We may as well
	 * drop 'em from bitmapqualorig, since there's no point in making the
	 * tests twice.
	 */
	bitmapqualorig = list_difference_ptr(bitmapqualorig, qpqual);

	/*
	 * Copy the finished bitmapqualorig to make sure we have an independent
	 * copy --- needed in case there are subplans in the index quals
	 */
	bitmapqualorig = copyObject(bitmapqualorig);

	/* Finally ready to build the plan node */
	scan_plan = make_bitmap_appendonlyscan(tlist,
										   qpqual,
										   bitmapqualplan,
										   bitmapqualorig,
										   baserelid,
										   best_path->isAORow);

	copy_path_costsize(ctx->root, &scan_plan->scan.plan, &best_path->path);

	return scan_plan;
}

/*
 * Given a bitmapqual tree, generate the Plan tree that implements it
 *
 * As byproducts, we also return in *qual and *indexqual the qual lists
 * (in implicit-AND form, without RestrictInfos) describing the original index
 * conditions and the generated indexqual conditions.  The latter is made to
 * exclude lossy index operators.  Both lists include partial-index predicates,
 * because we have to recheck predicates as well as index conditions if the
 * bitmap scan becomes lossy.
 *
 * Note: if you find yourself changing this, you probably need to change
 * make_restrictinfo_from_bitmapqual too.
 */
static Plan *
create_bitmap_subplan(CreatePlanContext *ctx, Path *bitmapqual,
					  List **qual, List **indexqual)
{
	Plan	   *plan;

	if (IsA(bitmapqual, BitmapAndPath))
	{
		BitmapAndPath *apath = (BitmapAndPath *) bitmapqual;
		List	   *subplans = NIL;
		List	   *subquals = NIL;
		List	   *subindexquals = NIL;
		ListCell   *l;

		/*
		 * There may well be redundant quals among the subplans, since a
		 * top-level WHERE qual might have gotten used to form several
		 * different index quals.  We don't try exceedingly hard to eliminate
		 * redundancies, but we do eliminate obvious duplicates by using
		 * list_concat_unique.
		 */
		foreach(l, apath->bitmapquals)
		{
			Plan	   *subplan;
			List	   *subqual;
			List	   *subindexqual;

			subplan = create_bitmap_subplan(ctx, (Path *) lfirst(l),
											&subqual, &subindexqual);
			subplans = lappend(subplans, subplan);
			subquals = list_concat_unique(subquals, subqual);
			subindexquals = list_concat_unique(subindexquals, subindexqual);
		}
		plan = (Plan *) make_bitmap_and(subplans);
		plan->startup_cost = apath->path.startup_cost;
		plan->total_cost = apath->path.total_cost;
		plan->plan_rows =
			clamp_row_est(apath->bitmapselectivity * apath->path.parent->tuples);
		plan->plan_width = 0;	/* meaningless */
		*qual = subquals;
		*indexqual = subindexquals;
	}
	else if (IsA(bitmapqual, BitmapOrPath))
	{
		BitmapOrPath *opath = (BitmapOrPath *) bitmapqual;
		List	   *subplans = NIL;
		List	   *subquals = NIL;
		List	   *subindexquals = NIL;
		bool		const_true_subqual = false;
		bool		const_true_subindexqual = false;
		ListCell   *l;

		/*
		 * Here, we only detect qual-free subplans.  A qual-free subplan would
		 * cause us to generate "... OR true ..."  which we may as well reduce
		 * to just "true".	We do not try to eliminate redundant subclauses
		 * because (a) it's not as likely as in the AND case, and (b) we might
		 * well be working with hundreds or even thousands of OR conditions,
		 * perhaps from a long IN list.  The performance of list_append_unique
		 * would be unacceptable.
		 */
		foreach(l, opath->bitmapquals)
		{
			Plan	   *subplan;
			List	   *subqual;
			List	   *subindexqual;

			subplan = create_bitmap_subplan(ctx, (Path *) lfirst(l),
											&subqual, &subindexqual);
			subplans = lappend(subplans, subplan);
			if (subqual == NIL)
				const_true_subqual = true;
			else if (!const_true_subqual)
				subquals = lappend(subquals,
								   make_ands_explicit(subqual));
			if (subindexqual == NIL)
				const_true_subindexqual = true;
			else if (!const_true_subindexqual)
				subindexquals = lappend(subindexquals,
										make_ands_explicit(subindexqual));
		}

		/*
		 * In the presence of ScalarArrayOpExpr quals, we might have built
		 * BitmapOrPaths with just one subpath; don't add an OR step.
		 */
		if (list_length(subplans) == 1)
		{
			plan = (Plan *) linitial(subplans);
		}
		else
		{
			plan = (Plan *) make_bitmap_or(subplans);
			plan->startup_cost = opath->path.startup_cost;
			plan->total_cost = opath->path.total_cost;
			plan->plan_rows =
				clamp_row_est(opath->bitmapselectivity * opath->path.parent->tuples);
			plan->plan_width = 0;		/* meaningless */
		}

		/*
		 * If there were constant-TRUE subquals, the OR reduces to constant
		 * TRUE.  Also, avoid generating one-element ORs, which could happen
		 * due to redundancy elimination or ScalarArrayOpExpr quals.
		 */
		if (const_true_subqual)
			*qual = NIL;
		else if (list_length(subquals) <= 1)
			*qual = subquals;
		else
			*qual = list_make1(make_orclause(subquals));
		if (const_true_subindexqual)
			*indexqual = NIL;
		else if (list_length(subindexquals) <= 1)
			*indexqual = subindexquals;
		else
			*indexqual = list_make1(make_orclause(subindexquals));
	}
	else if (IsA(bitmapqual, IndexPath))
	{
		IndexPath  *ipath = (IndexPath *) bitmapqual;
		IndexScan  *iscan;
		List	   *nonlossy_clauses;
		ListCell   *l;

		/* Use the regular indexscan plan build machinery... */
		iscan = create_indexscan_plan(ctx, ipath, NIL, NIL,
									  &nonlossy_clauses);
		/* then convert to a bitmap indexscan */
		plan = (Plan *) make_bitmap_indexscan(iscan->scan.scanrelid,
											  iscan->indexid,
											  iscan->indexqual,
											  iscan->indexqualorig,
											  iscan->indexstrategy,
											  iscan->indexsubtype);
		plan->startup_cost = 0.0;
		plan->total_cost = ipath->indextotalcost;
		plan->plan_rows =
			clamp_row_est(ipath->indexselectivity * ipath->path.parent->tuples);
		plan->plan_width = 0;	/* meaningless */
		*qual = get_actual_clauses(ipath->indexclauses);
		*indexqual = get_actual_clauses(nonlossy_clauses);
		foreach(l, ipath->indexinfo->indpred)
		{
			Expr	   *pred = (Expr *) lfirst(l);

			/*
			 * We know that the index predicate must have been implied by the
			 * query condition as a whole, but it may or may not be implied by
			 * the conditions that got pushed into the bitmapqual.	Avoid
			 * generating redundant conditions.
			 */
			if (!predicate_implied_by(list_make1(pred), ipath->indexclauses))
			{
				*qual = lappend(*qual, pred);
				*indexqual = lappend(*indexqual, pred);
			}
		}
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d", nodeTag(bitmapqual));
		plan = NULL;			/* keep compiler quiet */
	}

	return plan;
}

/*
 * create_tidscan_plan
 *	 Returns a tidscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static TidScan *
create_tidscan_plan(CreatePlanContext *ctx, TidPath *best_path,
					List *tlist, List *scan_clauses)
{
	TidScan    *scan_plan;
	Index		scan_relid = best_path->path.parent->relid;
	List	   *ortidquals;

	/* it should be a base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/*
	 * Remove any clauses that are TID quals.  This is a bit tricky since the
	 * tidquals list has implicit OR semantics.
	 *
	 * In the case of CURRENT OF, however, we do want the CurrentOfExpr to 
	 * reside in both the tidlist and the qual, as CurrentOfExpr is effectively
	 * a ctid, gp_segment_id, and tableoid qual. Constant folding will
	 * finish up this qual rewriting to ensure what we dispatch is a sane interpretation
	 * of CURRENT OF behavior.
	 */
	if (!(list_length(scan_clauses) == 1 && IsA(linitial(scan_clauses), CurrentOfExpr)))
	{
		ortidquals = best_path->tidquals;
		if (list_length(ortidquals) > 1)
			ortidquals = list_make1(make_orclause(ortidquals));
		scan_clauses = list_difference(scan_clauses, ortidquals);
	}

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(ctx->root, scan_clauses);

	scan_plan = make_tidscan(tlist,
							 scan_clauses,
							 scan_relid,
							 best_path->tidquals);

	copy_path_costsize(ctx->root, &scan_plan->scan.plan, &best_path->path);

	return scan_plan;
}

/*
 * create_subqueryscan_plan
 *	 Returns a subqueryscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SubqueryScan *
create_subqueryscan_plan(CreatePlanContext *ctx, Path *best_path,
						 List *tlist, List *scan_clauses)
{
	SubqueryScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;

	/* it should be a subquery base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->parent->rtekind == RTE_SUBQUERY);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(ctx->root, scan_clauses);

	scan_plan = make_subqueryscan(ctx->root, tlist,
								  scan_clauses,
								  scan_relid,
								  best_path->parent->subplan,
								  best_path->parent->subrtable);

	copy_path_costsize(ctx->root, &scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_ctescan_plan
 *   Returns a ctescan plan for the base relatioon scanned by 'best_path'
 *   with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SubqueryScan *
create_ctescan_plan(CreatePlanContext *ctx, Path *best_path,
					List *tlist, List *scan_clauses)
{
	Assert(best_path->parent->rtekind == RTE_CTE);

	Index scan_relid = best_path->parent->relid;

	Assert(scan_relid > 0);
	
	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(ctx->root, scan_clauses);

	SubqueryScan *scan_plan = make_subqueryscan(ctx->root, tlist,
												scan_clauses,
												scan_relid,
												best_path->parent->subplan,
												best_path->parent->subrtable);

	copy_path_costsize(ctx->root, &scan_plan->scan.plan, best_path);

	return scan_plan;
	
}

/*
 * create_functionscan_plan
 *	 Returns a functionscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static FunctionScan *
create_functionscan_plan(CreatePlanContext *ctx, Path *best_path,
						 List *tlist, List *scan_clauses)
{
	FunctionScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;

	/* it should be a function base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->parent->rtekind == RTE_FUNCTION);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(ctx->root, scan_clauses);

	scan_plan = make_functionscan(tlist, scan_clauses, scan_relid);

	copy_path_costsize(ctx->root, &scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_tablefunction_plan
 *	 Returns a TableFunction plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static TableFunctionScan *
create_tablefunction_plan(CreatePlanContext *ctx, 
						   Path *best_path,
						   List *tlist, 
						   List *scan_clauses)
{
	TableFunctionScan	*tablefunc;
	Plan				*subplan    = best_path->parent->subplan;
	List				*subrtable	= best_path->parent->subrtable;
	Index				 scan_relid = best_path->parent->relid;

	/* it should be a function base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->parent->rtekind == RTE_TABLEFUNCTION);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the TableFunctionScan plan */
	tablefunc = make_tablefunction(tlist, scan_clauses, subplan, subrtable, 
								   scan_relid);

	/* Cost is determined largely by the cost of the underlying subplan */
	copy_plan_costsize(&tablefunc->scan.plan, subplan);

	copy_path_costsize(ctx->root, &tablefunc->scan.plan, best_path);

	return tablefunc;
}

/*
 * create_valuesscan_plan
 *	 Returns a valuesscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static ValuesScan *
create_valuesscan_plan(CreatePlanContext *ctx, Path *best_path,
					   List *tlist, List *scan_clauses)
{
	ValuesScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;

	/* it should be a values base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->parent->rtekind == RTE_VALUES);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(ctx->root, scan_clauses);

	scan_plan = make_valuesscan(tlist, scan_clauses, scan_relid);

	copy_path_costsize(ctx->root, &scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * remove_isnotfalse_expr
 */
static Expr *
remove_isnotfalse_expr(Expr *expr)
{
	if (IsA(expr, BooleanTest))
	{
		BooleanTest *bt = (BooleanTest *) expr;
		if (bt->booltesttype == IS_NOT_FALSE)
		{
			return bt->arg;
		}
	}
	return expr;
}

/*
 * remove_isnotfalse
 *	  Given a list of joinclauses, extract the bare clauses, removing any IS_NOT_FALSE
 *	  additions. The original data structure is not touched; a modified list is returned
 */
static List *
remove_isnotfalse(List *clauses)
{
	List	   *t_list = NIL;
	ListCell   *l;

	foreach (l, clauses)
	{
		Node *node = (Node *) lfirst(l);
		if (IsA(node, Expr) || IsA(node, BooleanTest))
		{
			Expr *expr = (Expr *) node;
			expr = remove_isnotfalse_expr(expr);
			t_list = lappend(t_list, expr);
		}
		else if (IsA(node, RestrictInfo))
		{
			RestrictInfo *restrictinfo = (RestrictInfo *) node;
			Expr *rclause = restrictinfo->clause;
			rclause = remove_isnotfalse_expr(rclause);
			t_list = lappend(t_list, rclause);
		}
		else
		{
			t_list = lappend(t_list, node);
		}
	}
	return t_list;
}
/*****************************************************************************
 *
 *	JOIN METHODS
 *
 *****************************************************************************/

static Plan *
create_nestloop_plan(CreatePlanContext *ctx,
					 NestPath *best_path,
					 Plan *outer_plan,
					 Plan *inner_plan)
{
	List		*tlist = build_relation_tlist(best_path->jpath.path.parent);
	List		*joinrestrictclauses = best_path->jpath.joinrestrictinfo;
	List		*joinclauses;
	List		*otherclauses;
	NestLoop	*join_plan = NULL;

	bool		prefetch = false;

	/* MPP-1459: subqueries are resolved after our deadlock checks in
	 * pathnode.c; so we have to check here to make sure that we catch
	 * all motion deadlocks.
	 *
	 * MPP-1487: if there is already a materialize node here, we don't
	 * want to insert another one. :-)
	 *
	 * NOTE: materialize_finished_plan() does *almost* what we want --
	 * except we aren't finished. */
	if (!IsA(best_path->jpath.innerjoinpath, MaterialPath) &&
		(best_path->jpath.innerjoinpath->motionHazard ||
		 !best_path->jpath.innerjoinpath->rescannable))
	{
		Material	*mat;
		Path		matpath; /* dummy for cost fixup */

		mat = make_material(inner_plan);

		/* Set cost data */
		cost_material(&matpath,
					  ctx->root,
					  inner_plan->total_cost,
					  inner_plan->plan_rows,
					  inner_plan->plan_width);
		mat->plan.startup_cost = matpath.startup_cost;
		mat->plan.total_cost = matpath.total_cost;
		mat->plan.plan_rows = inner_plan->plan_rows;
		mat->plan.plan_width = inner_plan->plan_width;

		if (best_path->jpath.outerjoinpath->motionHazard)
		{
			mat->cdb_strict = true;
			prefetch = true;
		}

		inner_plan = (Plan *)mat;
	}
	/* MPP-1657: if there is already a materialize here, we may need
	 * to update its strictness. */
	else if (IsA(best_path->jpath.innerjoinpath, MaterialPath) &&
			 best_path->jpath.innerjoinpath->motionHazard &&
			 best_path->jpath.outerjoinpath->motionHazard)
	{
		Material   *mat = (Material *)inner_plan;

		prefetch = true;
		mat->cdb_strict = true;
	}

	if (IsA(best_path->jpath.innerjoinpath, IndexPath))
	{
		/*
		 * An index is being used to reduce the number of tuples scanned in
		 * the inner relation.	If there are join clauses being used with the
		 * index, we may remove those join clauses from the list of clauses
		 * that have to be checked as qpquals at the join node.
		 *
		 * We can also remove any join clauses that are redundant with those
		 * being used in the index scan; prior redundancy checks will not have
		 * caught this case because the join clauses would never have been put
		 * in the same joininfo list.
		 *
		 * We can skip this if the index path is an ordinary indexpath and not
		 * a special innerjoin path.
		 */
		IndexPath  *innerpath = (IndexPath *) best_path->jpath.innerjoinpath;

		if (innerpath->isjoininner)
		{
			joinrestrictclauses =
				select_nonredundant_join_clauses(ctx->root,
												 joinrestrictclauses,
												 innerpath->indexclauses,
												 best_path->jpath.outerjoinpath->parent->relids,
												 best_path->jpath.innerjoinpath->parent->relids,
												 IS_OUTER_JOIN(best_path->jpath.jointype));
		}
	}
	else if (IsA(best_path->jpath.innerjoinpath, BitmapHeapPath) ||
			 IsA(best_path->jpath.innerjoinpath, BitmapAppendOnlyPath))
	{
		/*
		 * Same deal for bitmapped index scans.
		 *
		 * Note: both here and above, we ignore any implicit index
		 * restrictions associated with the use of partial indexes.  This is
		 * OK because we're only trying to prove we can dispense with some
		 * join quals; failing to prove that doesn't result in an incorrect
		 * plan.  It is the right way to proceed because adding more quals to
		 * the stuff we got from the original query would just make it harder
		 * to detect duplication.  (Also, to change this we'd have to be wary
		 * of UPDATE/DELETE/SELECT FOR UPDATE target relations; see notes
		 * above about EvalPlanQual.)
		 */
		bool isjoininner = false;
		Path *bitmapqual = NULL;
		
		if (IsA(best_path->jpath.innerjoinpath, BitmapHeapPath))
		{
			BitmapHeapPath *innerpath = (BitmapHeapPath *) best_path->jpath.innerjoinpath;
			isjoininner = innerpath->isjoininner;
			bitmapqual = innerpath->bitmapqual;
		}
		else
		{
			Assert(IsA(best_path->jpath.innerjoinpath, BitmapAppendOnlyPath));
			BitmapAppendOnlyPath *innerpath = (BitmapAppendOnlyPath *) best_path->jpath.innerjoinpath;
			isjoininner = innerpath->isjoininner;
			bitmapqual = innerpath->bitmapqual;
		}

		if (isjoininner)
		{
			List	   *bitmapclauses;

			bitmapclauses =
				make_restrictinfo_from_bitmapqual(bitmapqual,
												  true,
												  false);
			joinrestrictclauses =
				select_nonredundant_join_clauses(ctx->root,
												 joinrestrictclauses,
												 bitmapclauses,
												 best_path->jpath.outerjoinpath->parent->relids,
												 best_path->jpath.innerjoinpath->parent->relids,
												 IS_OUTER_JOIN(best_path->jpath.jointype));
		}
	}

	/* Get the join qual clauses (in plain expression form) */
	/* Any pseudoconstant clauses are ignored here */
	if (IS_OUTER_JOIN(best_path->jpath.jointype))
	{
		extract_actual_join_clauses(joinrestrictclauses,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(joinrestrictclauses, false);
		otherclauses = NIL;
	}

	if (best_path->jpath.jointype == JOIN_LASJ_NOTIN)
	{
		joinclauses = remove_isnotfalse(joinclauses);
	}

	/* Sort clauses into best execution order */
	joinclauses = order_qual_clauses(ctx->root, joinclauses);
	otherclauses = order_qual_clauses(ctx->root, otherclauses);

	join_plan = make_nestloop(tlist,
							  joinclauses,
							  otherclauses,
							  outer_plan,
							  inner_plan,
							  best_path->jpath.jointype);

	copy_path_costsize(ctx->root, &join_plan->join.plan, &best_path->jpath.path);

	if (IsA(best_path->jpath.innerjoinpath, MaterialPath))
	{
		MaterialPath *mp = (MaterialPath *)best_path->jpath.innerjoinpath;

		if (mp->cdb_strict)
			prefetch = true;
	}

	if (prefetch)
		join_plan->join.prefetch_inner = true;

	return (Plan *)join_plan;
}

static MergeJoin *
create_mergejoin_plan(CreatePlanContext *ctx,
					  MergePath *best_path,
					  Plan *outer_plan,
					  Plan *inner_plan)
{
	List	   *tlist = build_relation_tlist(best_path->jpath.path.parent);
	List	   *joinclauses;
	List	   *otherclauses;
	List	   *mergeclauses;
    Sort       *sort;
	MergeJoin  *join_plan;
	bool		prefetch=false;

	/* Get the join qual clauses (in plain expression form) */
	/* Any pseudoconstant clauses are ignored here */
	if (IS_OUTER_JOIN(best_path->jpath.jointype))
	{
		extract_actual_join_clauses(best_path->jpath.joinrestrictinfo,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(best_path->jpath.joinrestrictinfo,
											 false);
		otherclauses = NIL;
	}

	/*
	 * Remove the mergeclauses from the list of join qual clauses, leaving the
	 * list of quals that must be checked as qpquals.
	 */
	mergeclauses = get_actual_clauses(best_path->path_mergeclauses);
	joinclauses = list_difference(joinclauses, mergeclauses);

	/*
	 * Rearrange mergeclauses, if needed, so that the outer variable is always
	 * on the left.
	 */
	mergeclauses =
        get_switched_clauses(best_path->jpath.innerjoinpath->parent->relids,
                             best_path->path_mergeclauses);

	/* Sort clauses into best execution order */
	/* NB: do NOT reorder the mergeclauses */
	joinclauses = order_qual_clauses(ctx->root, joinclauses);
	otherclauses = order_qual_clauses(ctx->root, otherclauses);

	/*
	 * Create explicit sort nodes for the outer and inner join paths if
	 * necessary.  The sort cost was already accounted for in the path. Make
	 * sure there are no excess columns in the inputs if sorting.
	 */
	if (best_path->outersortkeys)
	{
		disuse_physical_tlist(outer_plan, best_path->jpath.outerjoinpath);
		sort =
			make_sort_from_pathkeys(ctx->root,
									outer_plan,
									best_path->outersortkeys,
                                    best_path->jpath.outerjoinpath->parent->relids,
                                    true);
        if (sort)
            outer_plan = (Plan *)sort;
	}

	if (best_path->innersortkeys)
	{
		disuse_physical_tlist(inner_plan, best_path->jpath.innerjoinpath);
		sort =
			make_sort_from_pathkeys(ctx->root,
									inner_plan,
									best_path->innersortkeys,
                                    best_path->jpath.innerjoinpath->parent->relids,
                                    true);
        if (sort)
            inner_plan = (Plan *)sort;
	}

	/*
	 * MPP-3300: very similar to the nested-loop join motion deadlock cases. But we may have already
	 * put some slackening operators below (e.g. a sort).
	 *
	 * We need some kind of strict slackening operator (something which consumes all of its
	 * input before producing a row of output) for our inner. And we need to prefetch that side
	 * first.
	 */
	if (best_path->jpath.outerjoinpath->motionHazard && best_path->jpath.innerjoinpath->motionHazard)
	{
		prefetch = true;
		if (!IsA(inner_plan, Sort))
		{
			if (IsA(inner_plan, Material))
			{
				((Material *)inner_plan)->cdb_strict = true;
			}
			else
			{
				Material *mat;

				/* need to add slack. */
				mat = make_material(inner_plan);
				mat->cdb_strict = true;
				inner_plan = (Plan *)mat;
			}
		}
	}

	/*
	 * Now we can build the mergejoin node.
	 */
	join_plan = make_mergejoin(tlist,
							   joinclauses,
							   otherclauses,
							   mergeclauses,
							   outer_plan,
							   inner_plan,
							   best_path->jpath.jointype);

	join_plan->join.prefetch_inner = prefetch;

	copy_path_costsize(ctx->root, &join_plan->join.plan, &best_path->jpath.path);

	return join_plan;
}

static HashJoin *
create_hashjoin_plan(CreatePlanContext *ctx,
					 HashPath *best_path,
					 Plan *outer_plan,
					 Plan *inner_plan)
{
	List	   *tlist = build_relation_tlist(best_path->jpath.path.parent);
	List	   *joinclauses;
	List	   *otherclauses;
	List	   *hashclauses;
	HashJoin   *join_plan;
	Hash	   *hash_plan;

	/* Get the join qual clauses (in plain expression form) */
	/* Any pseudoconstant clauses are ignored here */
	JoinType	jointype = best_path->jpath.jointype;
	if (IS_OUTER_JOIN(jointype))
	{
		extract_actual_join_clauses(best_path->jpath.joinrestrictinfo,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(best_path->jpath.joinrestrictinfo,
											 false);
		otherclauses = NIL;
	}

	/*
	 * Remove the hashclauses from the list of join qual clauses, leaving the
	 * list of quals that must be checked as qpquals.
	 */
	hashclauses = get_actual_clauses(best_path->path_hashclauses);
	joinclauses = list_difference(joinclauses, hashclauses);

	/*
	 * Rearrange hashclauses, if needed, so that the outer variable is always
	 * on the left.
	 */
	hashclauses =
        get_switched_clauses(best_path->jpath.innerjoinpath->parent->relids,
                             best_path->path_hashclauses);

	/* Sort clauses into best execution order */
	joinclauses = order_qual_clauses(ctx->root, joinclauses);
	otherclauses = order_qual_clauses(ctx->root, otherclauses);
	hashclauses = order_qual_clauses(ctx->root, hashclauses);

	/* We don't want any excess columns in the hashed tuples, or in the outer either! */
	disuse_physical_tlist(inner_plan, best_path->jpath.innerjoinpath);
    if (outer_plan)
	    disuse_physical_tlist(outer_plan, best_path->jpath.outerjoinpath);

	/*
	 * Build the hash node and hash join node.
	 */
	hash_plan = make_hash(inner_plan);

	join_plan = make_hashjoin(tlist,
							  joinclauses,
							  otherclauses,
							  hashclauses,
							  NIL, /* hashqualclauses */
							  outer_plan,
							  (Plan *) hash_plan,
							  jointype);

	/* 
	 * MPP-4635.  best_path->jpath.outerjoinpath may be NULL.  
	 * From the comment, it is adaptive nestloop join may cause this.
	 */
	/*
	 * MPP-4165: we need to descend left-first if *either* of the
	 * subplans have any motion.
	 */
	/*
	 * MPP-3300: unify motion-deadlock prevention for all join types.
	 * This allows us to undo the MPP-989 changes in nodeHashjoin.c
	 * (allowing us to check the outer for rows before building the
	 * hash-table).
	 */
	if (best_path->jpath.outerjoinpath == NULL ||
		best_path->jpath.outerjoinpath->motionHazard ||
		best_path->jpath.innerjoinpath->motionHazard)
	{
		join_plan->join.prefetch_inner = true;
	}

	copy_path_costsize(ctx->root, &join_plan->join.plan, &best_path->jpath.path);

	return join_plan;
}


/*****************************************************************************
 *
 *	SUPPORTING ROUTINES
 *
 *****************************************************************************/

/*
 * fix_indexqual_references
 *	  Adjust indexqual clauses to the form the executor's indexqual
 *	  machinery needs, and check for recheckable (lossy) index conditions.
 *
 * We have five tasks here:
 *	* Remove RestrictInfo nodes from the input clauses.
 *	* Index keys must be represented by Var nodes with varattno set to the
 *	  index's attribute number, not the attribute number in the original rel.
 *	* If the index key is on the right, commute the clause to put it on the
 *	  left.
 *	* We must construct lists of operator strategy numbers and subtypes
 *	  for the top-level operators of each index clause.
 *	* We must detect any lossy index operators.  The API is that we return
 *	  a list of the input clauses whose operators are NOT lossy.
 *
 * fixed_indexquals receives a modified copy of the indexquals list --- the
 * original is not changed.  Note also that the copy shares no substructure
 * with the original; this is needed in case there is a subplan in it (we need
 * two separate copies of the subplan tree, or things will go awry).
 *
 * nonlossy_indexquals receives a list of the original input clauses (with
 * RestrictInfos) that contain non-lossy operators.
 *
 * indexstrategy receives an integer list of strategy numbers.
 * indexsubtype receives an OID list of strategy subtypes.
 */
static void
fix_indexqual_references(List *indexquals, IndexPath *index_path,
						 List **fixed_indexquals,
						 List **nonlossy_indexquals,
						 List **indexstrategy,
						 List **indexsubtype)
{
	IndexOptInfo *index = index_path->indexinfo;
	ListCell   *l;

	*fixed_indexquals = NIL;
	*nonlossy_indexquals = NIL;
	*indexstrategy = NIL;
	*indexsubtype = NIL;

	/*
	 * For each qual clause, commute if needed to put the indexkey operand on
	 * the left, and then fix its varattno.  (We do not need to change the
	 * other side of the clause.)  Then determine the operator's strategy
	 * number and subtype number, and check for lossy index behavior.
	 */
	foreach(l, indexquals)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
		Expr	   *clause;
		Oid			clause_op;
		Oid			opclass = 0;
		int			stratno;
		Oid			stratsubtype;
		bool		recheck;

		Assert(IsA(rinfo, RestrictInfo));

		/*
		 * Make a copy that will become the fixed clause.
		 *
		 * We used to try to do a shallow copy here, but that fails if there
		 * is a subplan in the arguments of the opclause.  So just do a full
		 * copy.
		 */
		clause = (Expr *) copyObject((Node *) rinfo->clause);

		if (IsA(clause, OpExpr))
		{
			OpExpr	   *op = (OpExpr *) clause;

			if (list_length(op->args) != 2)
				elog(ERROR, "indexqual clause is not binary opclause");

			/*
			 * Check to see if the indexkey is on the right; if so, commute
			 * the clause. The indexkey should be the side that refers to
			 * (only) the base relation.
			 */
			if (!bms_equal(rinfo->left_relids, index->rel->relids))
				CommuteOpExpr(op);

			/*
			 * Now, determine which index attribute this is, change the
			 * indexkey operand as needed, and get the index opclass.
			 */
			linitial(op->args) = fix_indexqual_operand(linitial(op->args),
													   index,
													   &opclass);
			clause_op = op->opno;
		}
		else if (IsA(clause, RowCompareExpr))
		{
			RowCompareExpr *rc = (RowCompareExpr *) clause;
			ListCell   *lc;

			/*
			 * Check to see if the indexkey is on the right; if so, commute
			 * the clause. The indexkey should be the side that refers to
			 * (only) the base relation.
			 */
			if (!bms_overlap(pull_varnos(linitial(rc->largs)),
							 index->rel->relids))
				CommuteRowCompareExpr(rc);

			/*
			 * For each column in the row comparison, determine which index
			 * attribute this is and change the indexkey operand as needed.
			 *
			 * Save the index opclass for only the first column.  We will
			 * return the operator and opclass info for just the first column
			 * of the row comparison; the executor will have to look up the
			 * rest if it needs them.
			 */
			foreach(lc, rc->largs)
			{
				Oid			tmp_opclass;

				lfirst(lc) = fix_indexqual_operand(lfirst(lc),
												   index,
												   &tmp_opclass);
				if (lc == list_head(rc->largs))
					opclass = tmp_opclass;
			}
			clause_op = linitial_oid(rc->opnos);
		}
		else if (IsA(clause, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;

			/* Never need to commute... */

			/*
			 * Now, determine which index attribute this is, change the
			 * indexkey operand as needed, and get the index opclass.
			 */
			linitial(saop->args) = fix_indexqual_operand(linitial(saop->args),
														 index,
														 &opclass);
			clause_op = saop->opno;
		}
		else
		{
			elog(ERROR, "unsupported indexqual type: %d",
				 (int) nodeTag(clause));
			continue;			/* keep compiler quiet */
		}

		*fixed_indexquals = lappend(*fixed_indexquals, clause);

		/*
		 * Look up the (possibly commuted) operator in the operator class to
		 * get its strategy numbers and the recheck indicator.	This also
		 * double-checks that we found an operator matching the index.
		 */
		get_op_opclass_properties(clause_op, opclass,
								  &stratno, &stratsubtype, &recheck);

		*indexstrategy = lappend_int(*indexstrategy, stratno);
		*indexsubtype = lappend_oid(*indexsubtype, stratsubtype);

		/* If it's not lossy, add to nonlossy_indexquals */
		if (!recheck)
			*nonlossy_indexquals = lappend(*nonlossy_indexquals, rinfo);
	}
}

static Node *
fix_indexqual_operand(Node *node, IndexOptInfo *index, Oid *opclass)
{
	/*
	 * We represent index keys by Var nodes having the varno of the base table
	 * but varattno equal to the index's attribute number (index column
	 * position).  This is a bit hokey ... would be cleaner to use a
	 * special-purpose node type that could not be mistaken for a regular Var.
	 * But it will do for now.
	 */
	Var		   *result;
	int			pos;
	ListCell   *indexpr_item;

	/*
	 * Remove any binary-compatible relabeling of the indexkey
	 */
	if (IsA(node, RelabelType))
		node = (Node *) ((RelabelType *) node)->arg;

	if (IsA(node, Var) &&
		((Var *) node)->varno == index->rel->relid)
	{
		/* Try to match against simple index columns */
		int			varatt = ((Var *) node)->varattno;

		if (varatt != 0)
		{
			for (pos = 0; pos < index->ncolumns; pos++)
			{
				if (index->indexkeys[pos] == varatt)
				{
					result = (Var *) copyObject(node);
					result->varattno = pos + 1;
					/* return the correct opclass, too */
					*opclass = index->classlist[pos];
					return (Node *) result;
				}
			}
		}
	}

	/* Try to match against index expressions */
	indexpr_item = list_head(index->indexprs);
	for (pos = 0; pos < index->ncolumns; pos++)
	{
		if (index->indexkeys[pos] == 0)
		{
			Node	   *indexkey;

			if (indexpr_item == NULL)
				elog(ERROR, "too few entries in indexprs list");
			indexkey = (Node *) lfirst(indexpr_item);
			if (indexkey && IsA(indexkey, RelabelType))
				indexkey = (Node *) ((RelabelType *) indexkey)->arg;
			if (equal(node, indexkey))
			{
				/* Found a match */
				result = makeVar(index->rel->relid, pos + 1,
								 exprType(lfirst(indexpr_item)), -1,
								 0);
				/* return the correct opclass, too */
				*opclass = index->classlist[pos];
				return (Node *) result;
			}
			indexpr_item = lnext(indexpr_item);
		}
	}

	/* Ooops... */
	elog(ERROR, "node is not an index attribute");
	*opclass = InvalidOid;		/* keep compiler quiet */
	return NULL;
}

/*
 * get_switched_clauses
 *	  Given a list of merge or hash joinclauses (as RestrictInfo nodes),
 *	  extract the bare clauses, and rearrange the elements within the
 *	  clauses, if needed, so the outer join variable is on the left and
 *	  the inner is on the right.  The original data structure is not touched;
 *	  a modified list is returned.
 *
 * CDB:  Caller specifies inner relids instead of outer relids, because with
 * Adaptive NJ there can be HashPlan nodes whose outer relids are not directly
 * accessible because outerjoinpath == NULL.
 */
static List *
get_switched_clauses(Relids innerrelids, List *clauses)
{
	List	   *t_list = NIL;
	ListCell   *l;

	foreach(l, clauses)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);

		Expr *rclause = restrictinfo->clause;

		/**
		 * If this is a IS NOT FALSE boolean test, we can peek underneath.
		 */
		if (IsA(rclause, BooleanTest))
		{
			BooleanTest *bt = (BooleanTest *) rclause;

			if (bt->booltesttype == IS_NOT_FALSE)
			{
				rclause = bt->arg;
			}
		}

		Assert(is_opclause(rclause));
		OpExpr *clause = (OpExpr *) rclause;
		if (bms_is_subset(restrictinfo->left_relids, innerrelids))
		{
			/*
			 * Duplicate just enough of the structure to allow commuting the
			 * clause without changing the original list.  Could use
			 * copyObject, but a complete deep copy is overkill.
			 */
			OpExpr	   *temp = makeNode(OpExpr);

			temp->opno = clause->opno;
			temp->opfuncid = InvalidOid;
			temp->opresulttype = clause->opresulttype;
			temp->opretset = clause->opretset;
			temp->args = list_copy(clause->args);
			/* Commute it --- note this modifies the temp node in-place. */
			CommuteOpExpr(temp);
			t_list = lappend(t_list, temp);
		}
		else
			t_list = lappend(t_list, clause);
	}
	return t_list;
}

/*
 * order_qual_clauses
 *		Given a list of qual clauses that will all be evaluated at the same
 *		plan node, sort the list into the order we want to check the quals
 *		in at runtime.
 *
 * Ideally the order should be driven by a combination of execution cost and
 * selectivity, but unfortunately we have so little information about
 * execution cost of operators that it's really hard to do anything smart.
 * For now, we just move any quals that contain SubPlan references (but not
 * InitPlan references) to the end of the list.
 */
static List *
order_qual_clauses(PlannerInfo *root, List *clauses)
{
	List	   *nosubplans;
	List	   *withsubplans;
	ListCell   *l;

	/* No need to work hard if the query is subselect-free */
	if (!root->parse->hasSubLinks)
		return clauses;

	nosubplans = NIL;
	withsubplans = NIL;
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);

		if (contain_subplans(clause))
			withsubplans = lappend(withsubplans, clause);
		else
			nosubplans = lappend(nosubplans, clause);
	}

	return list_concat(nosubplans, withsubplans);
}

/*
 * Copy cost and size info from a Path node to the Plan node created from it.
 * The executor won't use this info, but it's needed by EXPLAIN.
 */
static void
copy_path_costsize(PlannerInfo *root, Plan *dest, Path *src)
{
	if (src)
	{
		dest->startup_cost = src->startup_cost;
		dest->total_cost = src->total_cost;
		dest->plan_rows = cdbpath_rows(root, src);
		dest->plan_width = src->parent->width;
	}
	else
	{
		dest->startup_cost = 0;
		dest->total_cost = 0;
		dest->plan_rows = 0;
		dest->plan_width = 0;
	}
}

/*
 * Copy cost and size info from a lower plan node to an inserted node.
 * This is not critical, since the decisions have already been made,
 * but it helps produce more reasonable-looking EXPLAIN output.
 * (Some callers alter the info after copying it.)
 */
static void
copy_plan_costsize(Plan *dest, Plan *src)
{
	if (src)
	{
		dest->startup_cost = src->startup_cost;
		dest->total_cost = src->total_cost;
		dest->plan_rows = src->plan_rows;
		dest->plan_width = src->plan_width;
	}
	else
	{
		dest->startup_cost = 0;
		dest->total_cost = 0;
		dest->plan_rows = 0;
		dest->plan_width = 0;
	}
}


/*****************************************************************************
 *
 *	PLAN NODE BUILDING ROUTINES
 *
 * Some of these are exported because they are called to build plan nodes
 * in contexts where we're not deriving the plan node from a path node.
 *
 *****************************************************************************/

static SeqScan *
make_seqscan(List *qptlist,
			 List *qpqual,
			 Index scanrelid)
{
	SeqScan    *node = makeNode(SeqScan);
	Plan	   *plan = &node->plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;

	node->scanrelid = scanrelid;

	return node;
}

static AppendOnlyScan *
make_appendonlyscan(List *qptlist,
					List *qpqual,
					Index scanrelid)
{
	AppendOnlyScan    *node = makeNode(AppendOnlyScan);
	Plan			  *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;

	node->scan.scanrelid = scanrelid;

	return node;
}

static ParquetScan *
make_parquetscan(List *qptlist,
					List *qpqual,
					Index scanrelid)
{
	ParquetScan    *node = makeNode(ParquetScan);
	Plan			  *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;

	node->scan.scanrelid = scanrelid;

	return node;
}

static ExternalScan *
make_externalscan(List *qptlist,
				  List *qpqual,
				  Index scanrelid,
				  List *urilist,
				  List *fmtopts,
				  char fmttype,
				  bool ismasteronly,
				  int rejectlimit,
				  bool rejectlimitinrows,
				  Oid fmterrtableOid,
				  int encoding)
{
	ExternalScan    *node = makeNode(ExternalScan);
	Plan	   *plan = &node->scan.plan;
	static uint32 scancounter = 0;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;

	/* external specifications */
	node->uriList = urilist;
	node->fmtOpts = fmtopts;
	node->fmtType = fmttype;
	node->isMasterOnly = ismasteronly;
	node->rejLimit = rejectlimit;
	node->rejLimitInRows = rejectlimitinrows;
	node->fmterrtbl = fmterrtableOid;
	node->encoding = encoding;
	node->scancounter = scancounter++;

	return node;
}

static IndexScan *
make_indexscan(List *qptlist,
			   List *qpqual,
			   Index scanrelid,
			   Oid indexid,
			   List *indexqual,
			   List *indexqualorig,
			   List *indexstrategy,
			   List *indexsubtype,
			   ScanDirection indexscandir)
{
	IndexScan  *node = makeNode(IndexScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;

	node->indexid = indexid;
	node->indexqual = indexqual;
	node->indexqualorig = indexqualorig;
	node->indexstrategy = indexstrategy;
	node->indexsubtype = indexsubtype;
	node->indexorderdir = indexscandir;

	return node;
}

static BitmapIndexScan *
make_bitmap_indexscan(Index scanrelid,
					  Oid indexid,
					  List *indexqual,
					  List *indexqualorig,
					  List *indexstrategy,
					  List *indexsubtype)
{
	BitmapIndexScan *node = makeNode(BitmapIndexScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = NIL;		/* not used */
	plan->qual = NIL;			/* not used */
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;

	node->indexid = indexid;
	node->indexqual = indexqual;
	node->indexqualorig = indexqualorig;
	node->indexstrategy = indexstrategy;
	node->indexsubtype = indexsubtype;

	return node;
}

static BitmapHeapScan *
make_bitmap_heapscan(List *qptlist,
					 List *qpqual,
					 Plan *lefttree,
					 List *bitmapqualorig,
					 Index scanrelid)
{
	BitmapHeapScan *node = makeNode(BitmapHeapScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;

	node->bitmapqualorig = bitmapqualorig;

	return node;
}

static BitmapAppendOnlyScan *
make_bitmap_appendonlyscan(List *qptlist,
						   List *qpqual,
						   Plan *lefttree,
						   List *bitmapqualorig,
						   Index scanrelid,
						   bool isAORow)
{
	BitmapAppendOnlyScan *node = makeNode(BitmapAppendOnlyScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;

	node->bitmapqualorig = bitmapqualorig;
	node->isAORow = isAORow;

	return node;
}

static TidScan *
make_tidscan(List *qptlist,
			 List *qpqual,
			 Index scanrelid,
			 List *tidquals)
{
	TidScan    *node = makeNode(TidScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;

	node->tidquals = tidquals;

	return node;
}

SubqueryScan *
make_subqueryscan(PlannerInfo *root,
				  List *qptlist,
				  List *qpqual,
				  Index scanrelid,
				  Plan *subplan,
				  List *subrtable)
{
	SubqueryScan *node = makeNode(SubqueryScan);
	Plan	   *plan = &node->scan.plan;

	/**
	 * Ensure that the plan we're going to attach to the subquery scan has all the
	 * parameter fields figured out.
	 */
	SS_finalize_plan(root, subrtable, subplan, false);

	/*
	 * Cost is figured here for the convenience of prepunion.c.  Note this is
	 * only correct for the case where qpqual is empty; otherwise caller
	 * should overwrite cost with a better estimate.
	 */
	copy_plan_costsize(plan, subplan);
	plan->total_cost += cpu_tuple_cost * subplan->plan_rows;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	plan->extParam = bms_copy(subplan->extParam);
	plan->allParam = bms_copy(subplan->allParam);

	/*
	 * Note that, in most scan nodes, scanrelid refers to an entry in the rtable of the 
	 * containing plan; in a subqueryscan node, the containing plan is the higher
	 * level plan!
	 */
	node->scan.scanrelid = scanrelid;

	node->subplan = subplan;
	node->subrtable = subrtable;

	return node;
}

static FunctionScan *
make_functionscan(List *qptlist,
				  List *qpqual,
				  Index scanrelid)
{
	FunctionScan *node = makeNode(FunctionScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;

	return node;
}

static ValuesScan *
make_valuesscan(List *qptlist,
				List *qpqual,
				Index scanrelid)
{
	ValuesScan *node = makeNode(ValuesScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;

	return node;
}

Append *
make_append(List *appendplans, bool isTarget, List *tlist)
{
	Append	   *node = makeNode(Append);
	Plan	   *plan = &node->plan;
	ListCell   *subnode;
	double          weighted_total_width = 0.0;     /* maintain weighted total */
	/*
	 * Compute cost as sum of subplan costs.  We charge nothing extra for the
	 * Append itself, which perhaps is too optimistic, but since it doesn't do
	 * any selection or projection, it is a pretty cheap node.
	 */
	plan->startup_cost = 0;
	plan->total_cost = 0;
	plan->plan_rows = 0;
	plan->plan_width = 0;

	foreach(subnode, appendplans)
	{
		Plan *subplan = (Plan *) lfirst(subnode);
		if (subnode == list_head(appendplans))  
		{
			/* first node */
			plan->startup_cost = subplan->startup_cost;
		}
		plan->total_cost += subplan->total_cost;
		plan->plan_rows += subplan->plan_rows;
		weighted_total_width += (double) subplan->plan_rows * (double) subplan->plan_width;
	}
	plan->plan_width = (int) ceil(weighted_total_width / Max(1.0,plan->plan_rows));
	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->appendplans = appendplans;
	node->isTarget = isTarget;
	node->isZapped = false;
	node->hasXslice = false;

	return node;
}

static BitmapAnd *
make_bitmap_and(List *bitmapplans)
{
	BitmapAnd  *node = makeNode(BitmapAnd);
	Plan	   *plan = &node->plan;

	/* cost should be inserted by caller */
	plan->targetlist = NIL;
	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->bitmapplans = bitmapplans;

	return node;
}

static BitmapOr *
make_bitmap_or(List *bitmapplans)
{
	BitmapOr   *node = makeNode(BitmapOr);
	Plan	   *plan = &node->plan;

	/* cost should be inserted by caller */
	plan->targetlist = NIL;
	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->bitmapplans = bitmapplans;

	return node;
}

NestLoop *
make_nestloop(List *tlist,
			  List *joinclauses,
			  List *otherclauses,
			  Plan *lefttree,
			  Plan *righttree,
			  JoinType jointype)
{
	NestLoop   *node = makeNode(NestLoop);
	Plan	   *plan = &node->join.plan;

	/* cost should be inserted by caller */
	plan->targetlist = tlist;
	plan->qual = otherclauses;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	node->join.jointype = jointype;
	node->join.joinqual = joinclauses;

    node->outernotreferencedbyinner = false;    /*CDB*/

	return node;
}

HashJoin *
make_hashjoin(List *tlist,
			  List *joinclauses,
			  List *otherclauses,
			  List *hashclauses,
			  List *hashqualclauses,
			  Plan *lefttree,
			  Plan *righttree,
			  JoinType jointype)
{
	HashJoin   *node = makeNode(HashJoin);
	Plan	   *plan = &node->join.plan;

	/* cost should be inserted by caller */
	plan->targetlist = tlist;
	plan->qual = otherclauses;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	node->hashclauses = hashclauses;
	node->hashqualclauses = hashqualclauses;
	node->join.jointype = jointype;
	node->join.joinqual = joinclauses;

	return node;
}

Hash *
make_hash(Plan *lefttree)
{
	Hash	   *node = makeNode(Hash);
	Plan	   *plan = &node->plan;

	copy_plan_costsize(plan, lefttree);

	/*
	 * For plausibility, make startup & total costs equal total cost of input
	 * plan; this only affects EXPLAIN display not decisions.
	 */
	plan->startup_cost = plan->total_cost;
	plan->targetlist = copyObject(lefttree->targetlist);
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

    node->rescannable = false;              /* CDB (unused for now) */

	return node;
}

MergeJoin *
make_mergejoin(List *tlist,
			   List *joinclauses,
			   List *otherclauses,
			   List *mergeclauses,
			   Plan *lefttree,
			   Plan *righttree,
			   JoinType jointype)
{
	MergeJoin  *node = makeNode(MergeJoin);
	Plan	   *plan = &node->join.plan;

	/* cost should be inserted by caller */
	plan->targetlist = tlist;
	plan->qual = otherclauses;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	node->mergeclauses = mergeclauses;
	node->join.jointype = jointype;
	node->join.joinqual = joinclauses;

	return node;
}

/*
 * make_sort --- basic routine to build a Sort plan node
 *
 * Caller must have built the sortColIdx and sortOperators arrays already.
 */
static Sort *
make_sort(PlannerInfo *root, Plan *lefttree, int numCols,
		  AttrNumber *sortColIdx, Oid *sortOperators)
{
	Sort	   *node = makeNode(Sort);
	Plan	   *plan = &node->plan;

	copy_plan_costsize(plan, lefttree); /* only care about copying size */
	plan = add_sort_cost(root, plan, numCols, sortColIdx, sortOperators);

	plan->targetlist = cdbpullup_targetlist(lefttree,
                            cdbpullup_exprHasSubplanRef((Expr *)lefttree->targetlist));
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	node->numCols = numCols;
	node->sortColIdx = sortColIdx;
	node->sortOperators = sortOperators;
	node->limitOffset = NULL; /* CDB */
	node->limitCount  = NULL; /* CDB */
	node->noduplicates = false; /* CDB */

	node->share_type = SHARE_NOTSHARED;
	node->share_id = SHARE_ID_NOT_SHARED;
	node->driver_slice = -1;
	node->nsharer = 0;
	node->nsharer_xslice = 0; 

	return node;
}

/*
 * add_sort_cost --- basic routine to accumulate Sort cost into a
 * plan node representing the input cost.
 *
 * Unused arguments (e.g., sortColIdx and sortOperators arrays) are 
 * included to allow for future improvements to sort costing.  Note 
 * that root may be NULL (e.g. when called outside make_sort).
 */
Plan *
add_sort_cost(PlannerInfo *root, Plan *input, int numCols, AttrNumber *sortColIdx, Oid *sortOperators)
{
	Path		sort_path;		/* dummy for result of cost_sort */

    UnusedArg(numCols);
    UnusedArg(sortColIdx);
    UnusedArg(sortOperators);

	cost_sort(&sort_path, root, NIL,
			  input->total_cost,
			  input->plan_rows,
			  input->plan_width);
	input->startup_cost = sort_path.startup_cost;
	input->total_cost = sort_path.total_cost;

	return input;
}

/*
 * add_sort_column --- utility subroutine for building sort info arrays
 *
 * We need this routine because the same column might be selected more than
 * once as a sort key column; if so, the extra mentions are redundant.
 *
 * Caller is assumed to have allocated the arrays large enough for the
 * max possible number of columns.	Return value is the new column count.
 */
static int
add_sort_column(AttrNumber colIdx, Oid sortOp,
				int numCols, AttrNumber *sortColIdx, Oid *sortOperators)
{
	int			i;

	for (i = 0; i < numCols; i++)
	{
		if (sortColIdx[i] == colIdx)
		{
			/* Already sorting by this col, so extra sort key is useless */
			return numCols;
		}
	}

	/* Add the column */
	sortColIdx[numCols] = colIdx;
	sortOperators[numCols] = sortOp;
	return numCols + 1;
}

/*
 * make_sort_from_pathkeys
 *	  Create sort plan to sort according to given pathkeys
 *
 *	  'lefttree' is the node which yields input tuples
 *	  'pathkeys' is the list of pathkeys by which the result is to be sorted
 *    'relids' is the set of relids that can be used in Var nodes here.
 *    'add_keys_to_targetlist' is true if it is ok to append to the subplan's
 *          targetlist or insert a Result node atop the subplan to evaluate
 *          sort key exprs that are not already present in the subplan's tlist.
 *
 * We must convert the pathkey information into arrays of sort key column
 * numbers and sort operator OIDs.
 *
 * If the pathkeys include expressions that aren't simple Vars, we will
 * usually need to add resjunk items to the input plan's targetlist to
 * compute these expressions (since the Sort node itself won't do it).
 * If the input plan type isn't one that can do projections, this means
 * adding a Result node just to do the projection.
 *
 * Returns a new Sort node if successful.
 *
 * Returns NULL if the sort key is degenerate (0 length after truncating
 * due to add_keys_to_targetlist==false and/or omitting key cols that are
 * equal to a constant expr.)
 */
Sort *
make_sort_from_pathkeys(PlannerInfo *root, Plan *lefttree, List *pathkeys,
                        Relids relids, bool add_keys_to_targetlist)
{
	List	   *tlist = lefttree->targetlist;
	ListCell   *i;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;

	/*
	 * We will need at most list_length(pathkeys) sort columns; possibly less
	 */
	numsortkeys = list_length(pathkeys);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));

	numsortkeys = 0;

	foreach(i, pathkeys)
	{
		List	   *keysublist = (List *) lfirst(i);
        ListCell   *j;
		PathKeyItem *pathkey = NULL;
		TargetEntry *tle = NULL;
        AttrNumber  resno;

		/*
		 * The column might already be selected as a sort key, if the pathkeys
		 * contain duplicate entries.  (This can happen in scenarios where
		 * multiple mergejoinable clauses mention the same var, for example.)
		 * Skip this sort key if it is the same as an earlier one.
		 */
        foreach(j, pathkeys)
            if ((List *)lfirst(j) == keysublist)
                break;
        if (j != i)
            continue;

		/*
		 * We can sort by any one of the sort key items listed in this
		 * sublist.  For now, we take the first one that corresponds to an
		 * available Var in the tlist.	If there isn't any, use the first one
		 * that is an expression in the input's vars.
		 *
		 * XXX if we have a choice, is there any way of figuring out which
		 * might be cheapest to execute?  (For example, int4lt is likely much
		 * cheaper to execute than numericlt, but both might appear in the
		 * same pathkey sublist...)  Not clear that we ever will have a choice
		 * in practice, so it may not matter.
		 */
        pathkey = cdbpullup_findPathKeyItemInTargetList(keysublist,
                                                        relids,
                                                        tlist,
                                                        &resno);
        if (!pathkey)
        {
            /* CDB: Truncate sort keys if caller said don't extend the tlist. */
            if (!add_keys_to_targetlist)
                break;
            elog(ERROR, "could not find pathkey item to sort");
        }

        /* Omit this sort key if equivalence class contains a constant expr. */
        if (pathkey->cdb_num_relids == 0)
            continue;

        /* If item is not in the tlist, but is computable from tlist vars... */
        if (resno == 0)
		{
            /* CDB: Truncate sort keys if caller said don't extend the tlist. */
            if (!add_keys_to_targetlist)
                break;

			/*
			 * Do we need to insert a Result node?
			 */
			if (!is_projection_capable_plan(lefttree))
			{
				tlist = copyObject(tlist);
				lefttree = (Plan *) make_result(tlist, NULL, lefttree);
			}

			/*
			 * Add resjunk entry to input's tlist
			 */
            resno = list_length(tlist) + 1;
			tle = makeTargetEntry((Expr *) pathkey->key,
								  resno,
								  NULL,
								  true);
			tlist = lappend(tlist, tle);
			lefttree->targetlist = tlist;		/* just in case NIL before */
		}

        /* Add column to sort arrays. */
        sortColIdx[numsortkeys] = resno;
        sortOperators[numsortkeys] = pathkey->sortop;
        numsortkeys++;
	}

    if (numsortkeys == 0)
        return NULL;

	return make_sort(root, lefttree, numsortkeys,
					 sortColIdx, sortOperators);
}

/*
 * make_sort_from_sortclauses
 *	  Create sort plan to sort according to given sortclauses
 *
 *	  'sortcls' is a list of SortClauses
 *	  'lefttree' is the node which yields input tuples
 */
Sort *
make_sort_from_sortclauses(PlannerInfo *root, List *sortcls, Plan *lefttree)
{
	List	   *sub_tlist = lefttree->targetlist;
	ListCell   *l;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;

	/*
	 * We will need at most list_length(sortcls) sort columns; possibly less
	 */
	numsortkeys = list_length(sortcls);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));

	numsortkeys = 0;

	foreach(l, sortcls)
	{
		SortClause *sortcl = (SortClause *) lfirst(l);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, sub_tlist);

		/*
		 * Check for the possibility of duplicate order-by clauses --- the
		 * parser should have removed 'em, but no point in sorting
		 * redundantly.
		 */
		numsortkeys = add_sort_column(tle->resno, sortcl->sortop,
									  numsortkeys, sortColIdx, sortOperators);
	}

	Assert(numsortkeys > 0);

	return make_sort(root, lefttree, numsortkeys,
					 sortColIdx, sortOperators);
}

/*
 * make_sort_from_groupcols
 *	  Create sort plan to sort based on grouping columns
 *
 * 'groupcls' is the list of GroupClauses
 * 'grpColIdx' gives the column numbers to use
 * 'appendGrouping' represents whether to append a Grouping
 *    as the last sort key, used for grouping extension.
 *
 * This might look like it could be merged with make_sort_from_sortclauses,
 * but presently we *must* use the grpColIdx[] array to locate sort columns,
 * because the child plan's tlist is not marked with ressortgroupref info
 * appropriate to the grouping node.  So, only the sortop is used from the
 * GroupClause entries.
 */
Sort *
make_sort_from_groupcols(PlannerInfo *root,
						 List *groupcls,
						 AttrNumber *grpColIdx,
						 bool appendGrouping,
						 Plan *lefttree)
{
	List	   *sub_tlist = lefttree->targetlist;
	int			grpno = 0;
	ListCell   *l;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	List       *flat_groupcls;

	/*
	 * We will need at most list_length(groupcls) sort columns; possibly less
	 */
	numsortkeys = num_distcols_in_grouplist(groupcls);

	if (appendGrouping)
		numsortkeys ++;

	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));

	numsortkeys = 0;

	flat_groupcls = flatten_grouping_list(groupcls);

	foreach(l, flat_groupcls)
	{
		GroupClause *grpcl = (GroupClause *) lfirst(l);
		TargetEntry *tle = get_tle_by_resno(sub_tlist, grpColIdx[grpno]);

		grpno++;

		/*
		 * Check for the possibility of duplicate group-by clauses --- the
		 * parser should have removed 'em, but no point in sorting
		 * redundantly.
		 */
		numsortkeys = add_sort_column(tle->resno, grpcl->sortop,
									  numsortkeys, sortColIdx, sortOperators);
	}

	if (appendGrouping)
	{
		Oid sort_op;
		/* Grouping will be the last entry in grpColIdx */
		TargetEntry *tle = get_tle_by_resno(sub_tlist, grpColIdx[grpno]);

		if (tle->resname == NULL)
			tle->resname = "grouping";

		sort_op = ordering_oper_opid(exprType((Node *)tle->expr));

		numsortkeys = add_sort_column(tle->resno, sort_op,
									  numsortkeys, sortColIdx, sortOperators);
	}


	Assert(numsortkeys > 0);

	return make_sort(root, lefttree, numsortkeys,
					 sortColIdx, sortOperators);
}


/*
 * make_sort_from_reordered_groupcols
 *	  Create sort plan to sort based on re-ordered grouping columns.
 *
 * 'groupcls' is the list of GroupClauses
 * 'orig_groupColIdx' is the original columns numbers for groupcls
 * 'new_grpColIdx' gives the re-ordered column numbers to use
 * 'grouping' represents the target entry for the Grouping column.
 *    If this value is not NULL, "Grouping" column will be added
 *    into the sorting list.
 */
Sort*
make_sort_from_reordered_groupcols(PlannerInfo *root,
								   List *groupcls,
								   AttrNumber *orig_grpColIdx,
								   AttrNumber *new_grpColIdx,
								   TargetEntry *grouping,
								   TargetEntry *groupid,
								   int req_ngrpkeys,
								   Plan *lefttree)
{
	List	   *sub_tlist = lefttree->targetlist;
	int			grpno = 0;
	int			numgrpkeys, numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	List       *flat_groupcls;

	/*
	 * We will need at most list_length(groupcls) sort columns; possibly less
	 */
	numgrpkeys = num_distcols_in_grouplist(groupcls);

	Assert(req_ngrpkeys <= numgrpkeys);

	if (grouping != NULL)
	{
		Assert(groupid != NULL);

		sortColIdx = (AttrNumber *) palloc((numgrpkeys + 2) * sizeof(AttrNumber));
		sortOperators = (Oid *) palloc((numgrpkeys + 2) * sizeof(Oid));
	}
	else
	{
		sortColIdx = (AttrNumber *) palloc(numgrpkeys * sizeof(AttrNumber));
		sortOperators = (Oid *) palloc(numgrpkeys * sizeof(Oid));
	}	

	numsortkeys = 0;

	flat_groupcls = flatten_grouping_list(groupcls);

	for (grpno=0; grpno < req_ngrpkeys; grpno++)
	{
		GroupClause *grpcl;
		TargetEntry *tle;
		int pos;

		/* Find the index position in orig_grpColIdx, in which 
		 * the element is equal to new_grpColIdx[grpno]. This index
		 * position points to the place that the corresponding
		 * GroupClause in flat_groupcls.
		 */
		for (pos=0; pos < numgrpkeys; pos++)
		{
			if (orig_grpColIdx[pos] == new_grpColIdx[grpno])
				break;
		}

		Assert (pos < numgrpkeys);

		grpcl = (GroupClause *)list_nth(flat_groupcls, pos);
		tle = get_tle_by_resno(sub_tlist, new_grpColIdx[grpno]);

		/*
		 * Check for the possibility of duplicate group-by clauses --- the
		 * parser should have removed 'em, but no point in sorting
		 * redundantly.
		 */
		numsortkeys = add_sort_column(tle->resno, grpcl->sortop,
									  numsortkeys, sortColIdx, sortOperators);
	}

	if (grouping != NULL)
	{
		Oid sort_op = ordering_oper_opid(exprType((Node *)grouping->expr));

		numsortkeys = add_sort_column(grouping->resno, sort_op,
									  numsortkeys, sortColIdx, sortOperators);

		sort_op = ordering_oper_opid(exprType((Node *)groupid->expr));

		numsortkeys = add_sort_column(groupid->resno, sort_op,
									  numsortkeys, sortColIdx, sortOperators);
	}


	Assert(numsortkeys > 0);

	return make_sort(root, lefttree, numsortkeys,
					 sortColIdx, sortOperators);
}

/*
 * Reconstruct a new list of GroupClause based on the given grpCols.
 *
 * The original grouping clauses may contain grouping extensions. This function
 * extract the raw grouping attributes and construct a list of GroupClauses
 * that contains only ordinary grouping.
 */
List *
reconstruct_group_clause(List *orig_groupClause, List *tlist, AttrNumber *grpColIdx, int numcols)
{
	List *flat_groupcls;
	List *new_groupClause = NIL;
	int numgrpkeys;
	int grpno;

	numgrpkeys = num_distcols_in_grouplist(orig_groupClause);
	flat_groupcls = flatten_grouping_list(orig_groupClause);
	for (grpno = 0; grpno < numcols; grpno++)
	{
		ListCell *lc = NULL;
		TargetEntry *te;
		GroupClause *gc = NULL;

		te = get_tle_by_resno(tlist, grpColIdx[grpno]);

		foreach (lc, flat_groupcls)
		{
			gc = (GroupClause *)lfirst(lc);

			if (gc->tleSortGroupRef == te->ressortgroupref)
				break;
		}
		if (lc != NULL)
			new_groupClause = lappend(new_groupClause, gc);
	}

	return new_groupClause;
}

Material *
make_material(Plan *lefttree)
{
	Material   *node = makeNode(Material);
	Plan	   *plan = &node->plan;

	/* cost should be inserted by caller */
	plan->targetlist = copyObject(lefttree->targetlist);
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	node->cdb_strict = false;
	node->share_type = SHARE_NOTSHARED;
	node->share_id = SHARE_ID_NOT_SHARED;
	node->driver_slice = -1;
	node->nsharer = 0;
	node->nsharer_xslice= 0;

	return node;
}

/*
 * materialize_finished_plan: stick a Material node atop a completed plan
 *
 * There are a couple of places where we want to attach a Material node
 * after completion of subquery_planner().	This currently requires hackery.
 * Since subquery_planner has already run SS_finalize_plan on the subplan
 * tree, we have to kluge up parameter lists for the Material node.
 * Possibly this could be fixed by postponing SS_finalize_plan processing
 * until setrefs.c is run?
 */
Plan *
materialize_finished_plan(PlannerInfo *root, Plan *subplan)
{
	Plan	   *matplan;
	Path		matpath;		/* dummy for result of cost_material */

	matplan = (Plan *) make_material(subplan);

	/* Set cost data */
	cost_material(&matpath,
				  root,
				  subplan->total_cost,
				  subplan->plan_rows,
				  subplan->plan_width);
	matplan->startup_cost = matpath.startup_cost;
	matplan->total_cost = matpath.total_cost;
	matplan->plan_rows = subplan->plan_rows;
	matplan->plan_width = subplan->plan_width;

	/* MPP -- propagate dispatch method and flow */
	matplan->dispatch = subplan->dispatch;
	matplan->flow = copyObject(subplan->flow);

	/* parameter kluge --- see comments above */
	matplan->extParam = bms_copy(subplan->extParam);
	matplan->allParam = bms_copy(subplan->allParam);

	return matplan;
}

Agg *
make_agg(PlannerInfo *root, List *tlist, List *qual,
		 AggStrategy aggstrategy, bool streaming,
		 int numGroupCols, AttrNumber *grpColIdx,
		 long numGroups, int num_nullcols,
		 uint64 input_grouping, uint64 grouping,
		 int rollupGSTimes,
		 int numAggs, int transSpace,
		 Plan *lefttree)
{
	Agg		   *node = makeNode(Agg);
	Plan	   *plan = &node->plan;

	node->aggstrategy = aggstrategy;
	node->numCols = numGroupCols;
	node->grpColIdx = grpColIdx;
	node->numGroups = numGroups;
	node->transSpace = transSpace;
	node->numNullCols = num_nullcols;
	node->inputGrouping = input_grouping;
	node->grouping = grouping;
	node->inputHasGrouping = false;
	node->rollupGSTimes = rollupGSTimes;
	node->lastAgg = false;
	node->streaming = streaming;

	copy_plan_costsize(plan, lefttree); /* only care about copying size */

	add_agg_cost(root, plan, tlist, qual, aggstrategy, streaming, 
				 numGroupCols, grpColIdx,
				 numGroups, num_nullcols,
				 numAggs, transSpace);

	plan->qual = qual;
	plan->targetlist = tlist;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	plan->extParam = bms_copy(lefttree->extParam);
	plan->allParam = bms_copy(lefttree->allParam);

	return node;
}

/* add_agg_cost -- basic routine to accumulate Agg cost into a
 * plan node representing the input cost.
 *
 * Unused arguments (e.g., streaming, grpColIdx, num_nullcols)
 * are included to allow for future improvements to aggregate
 * costing.  Note that root may be NULL (e.g., when called from
 * outside make_agg).
 */
Plan *add_agg_cost(PlannerInfo *root, Plan *plan, 
		 List *tlist, List *qual,
		 AggStrategy aggstrategy,
		 bool streaming,
		 int numGroupCols, AttrNumber *grpColIdx,
		 long numGroups, int num_nullcols,
		 int numAggs, int transSpace)
{
	Path		agg_path;		/* dummy for result of cost_agg */
	QualCost	qual_cost;
	HashAggTableSizes   hash_info;

    UnusedArg(grpColIdx);
    UnusedArg(num_nullcols);

    /* Solution for MPP-11942
     * Before this fix, we calculated the width from the sub_tlist which
     * only contains a subset of the tlist. For example, for the query
     * select a, min(b), max(b), min(c), max(c) from s group by a;
     * the sub_tlist has the columns {a,b,c} while the tlist
     * is {a, min(b), max(b), min(c), max(c)}. Therefore, the plan_width
     * for the above query is calculated to be 12 instead of 20.
     *
     * In this fix, we calculate the actual row width from the tlist.
     */

    plan->plan_width = get_row_width(tlist);

	if (aggstrategy == AGG_HASHED)
	{
		if (!calcHashAggTableSizes(global_work_mem(root),
								   numGroups,
								   numAggs,
								   /* The following estimate is very rough but good enough for planning. */
								   sizeof(HeapTupleData) + sizeof(HeapTupleHeaderData) + plan->plan_width,
								   transSpace,
								   true,
								   &hash_info))
		{
			elog(ERROR, "Planner committed to impossible hash aggregate.");
		}

		cost_agg(&agg_path, root,
				 aggstrategy, numAggs,
				 numGroupCols, numGroups,
				 plan->startup_cost,
				 plan->total_cost,
				 plan->plan_rows, hash_info.workmem_per_entry,
				 hash_info.nbatches, hash_info.hashentry_width, streaming);
	}
	else
		cost_agg(&agg_path, root,
				 aggstrategy, numAggs,
				 numGroupCols, numGroups,
				 plan->startup_cost,
				 plan->total_cost,
				 plan->plan_rows, 0.0, 0.0,
				 0.0, false);


	plan->startup_cost = agg_path.startup_cost;
	plan->total_cost = agg_path.total_cost;


	/*
	 * We will produce a single output tuple if not grouping, and a tuple per
	 * group otherwise.
	 */
	if (aggstrategy == AGG_PLAIN)
		plan->plan_rows = 1;
	else
		plan->plan_rows = numGroups;

	/*
	 * We also need to account for the cost of evaluation of the qual (ie, the
	 * HAVING clause) and the tlist.  Note that cost_qual_eval doesn't charge
	 * anything for Aggref nodes; this is okay since they are really
	 * comparable to Vars.
	 *
	 * See notes in grouping_planner about why this routine and make_group are
	 * the only ones in this file that worry about tlist eval cost.
	 */
	if (qual)
	{
		cost_qual_eval(&qual_cost, qual, root);
		plan->startup_cost += qual_cost.startup;
		plan->total_cost += qual_cost.startup;
		plan->total_cost += qual_cost.per_tuple * plan->plan_rows;
	}
	cost_qual_eval(&qual_cost, tlist, root);
	plan->startup_cost += qual_cost.startup;
	plan->total_cost += qual_cost.startup;
	plan->total_cost += qual_cost.per_tuple * plan->plan_rows;

	return plan;
}

Window *make_window(PlannerInfo *root, List *tlist,
		   int numPartCols, AttrNumber *partColIdx, List *windowKeys,
		   Plan *lefttree)
{
	Window *node = makeNode(Window);
	Plan	   *plan = &node->plan;
	Path		window_path;		/* dummy for result of cost_window */
	QualCost	qual_cost;
	int numOrderCols;
	ListCell *cell;

	node->numPartCols = numPartCols;
	node->partColIdx = partColIdx;
	node->windowKeys = windowKeys;

	numOrderCols = numPartCols;
	foreach(cell, windowKeys)
	{
		/* TODO Shouldn't we copy? */
		WindowKey	*key = (WindowKey *)lfirst(cell);
		numOrderCols += key->numSortCols;
	}


	copy_plan_costsize(plan, lefttree); /* only care about copying size */
	cost_window(&window_path, root,
			   numOrderCols,
			   lefttree->startup_cost,
			   lefttree->total_cost,
			   lefttree->plan_rows);
	plan->startup_cost = window_path.startup_cost;
	plan->total_cost = window_path.total_cost;

	cost_qual_eval(&qual_cost, tlist, root);
	plan->startup_cost += qual_cost.startup;
	plan->total_cost += qual_cost.startup;
	plan->total_cost += qual_cost.per_tuple * plan->plan_rows;

	plan->qual = NIL;
	plan->targetlist = tlist;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	return node;	
}

static TableFunctionScan*
make_tablefunction(List *tlist,
				   List *scan_quals,
				   Plan *subplan,
				   List *subrtable,
				   Index scanrelid)
{
	TableFunctionScan	*node = makeNode(TableFunctionScan);
	Plan				*plan = &node->scan.plan;

	copy_plan_costsize(plan, subplan);  /* only care about copying size */

	/* FIXME: fix costing */
	plan->startup_cost  = subplan->startup_cost;
	plan->total_cost    = subplan->total_cost;
	plan->total_cost   += 2 * plan->plan_rows;

	plan->qual			= scan_quals;
	plan->targetlist	= tlist;
	plan->righttree		= NULL;

	/* Fill in information for the subplan */
	plan->lefttree		 = subplan;
	node->subrtable		 = subrtable;
	node->scan.scanrelid = scanrelid;

	return node;	
}



/*
 * distinctList is a list of SortClauses, identifying the targetlist items
 * that should be considered by the Unique filter.
 */
Unique *
make_unique(Plan *lefttree, List *distinctList)
{
	Unique	   *node = makeNode(Unique);
	Plan	   *plan = &node->plan;
	int			numCols = list_length(distinctList);
	int			keyno = 0;
	AttrNumber *uniqColIdx;
	ListCell   *slitem;

	copy_plan_costsize(plan, lefttree);

	/*
	 * Charge one cpu_operator_cost per comparison per input tuple. We assume
	 * all columns get compared at most of the tuples.	(XXX probably this is
	 * an overestimate.)
	 */
	plan->total_cost += cpu_operator_cost * plan->plan_rows * numCols;

	/*
	 * plan->plan_rows is left as a copy of the input subplan's plan_rows; ie,
	 * we assume the filter removes nothing.  The caller must alter this if he
	 * has a better idea.
	 */

	plan->targetlist = cdbpullup_targetlist(lefttree,
                            cdbpullup_exprHasSubplanRef((Expr *)lefttree->targetlist));
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	/*
	 * convert SortClause list into array of attr indexes, as wanted by exec
	 */
	Assert(numCols > 0);
	uniqColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numCols);

	foreach(slitem, distinctList)
	{
		SortClause *sortcl = (SortClause *) lfirst(slitem);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, plan->targetlist);

		uniqColIdx[keyno++] = tle->resno;
	}

	node->numCols = numCols;
	node->uniqColIdx = uniqColIdx;

	/* CDB */ /* pass DISTINCT to sort */
	if (IsA(lefttree, Sort) && gp_enable_sort_distinct)
	{
		Sort* pSort = (Sort*)lefttree;
		pSort->noduplicates = true;
	}

	return node;
}

/*
 * distinctList is a list of SortClauses, identifying the targetlist items
 * that should be considered by the SetOp filter.
 */

SetOp *
make_setop(SetOpCmd cmd, Plan *lefttree,
		   List *distinctList, AttrNumber flagColIdx)
{
	SetOp	   *node = makeNode(SetOp);
	Plan	   *plan = &node->plan;
	int			numCols = list_length(distinctList);
	int			keyno = 0;
	AttrNumber *dupColIdx;
	ListCell   *slitem;

	copy_plan_costsize(plan, lefttree);

	/*
	 * Charge one cpu_operator_cost per comparison per input tuple. We assume
	 * all columns get compared at most of the tuples.
	 */
	plan->total_cost += cpu_operator_cost * plan->plan_rows * numCols;

	/*
	 * We make the unsupported assumption that there will be 10% as many
	 * tuples out as in.  Any way to do better?
	 */
	plan->plan_rows *= 0.1;
	if (plan->plan_rows < 1)
		plan->plan_rows = 1;

	plan->targetlist = cdbpullup_targetlist(lefttree,
                            cdbpullup_exprHasSubplanRef((Expr *)lefttree->targetlist));
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	/*
	 * convert SortClause list into array of attr indexes, as wanted by exec
	 */
	Assert(numCols > 0);
	dupColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numCols);

	foreach(slitem, distinctList)
	{
		SortClause *sortcl = (SortClause *) lfirst(slitem);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, plan->targetlist);

		dupColIdx[keyno++] = tle->resno;
	}

	node->cmd = cmd;
	node->numCols = numCols;
	node->dupColIdx = dupColIdx;
	node->flagColIdx = flagColIdx;

	return node;
}

/*
 * Note: offset_est and count_est are passed in to save having to repeat
 * work already done to estimate the values of the limitOffset and limitCount
 * expressions.  Their values are as returned by preprocess_limit (0 means
 * "not relevant", -1 means "couldn't estimate").  Keep the code below in sync
 * with that function!
 */
Limit *
make_limit(Plan *lefttree, Node *limitOffset, Node *limitCount,
		   int64 offset_est, int64 count_est)
{
	Limit	   *node = makeNode(Limit);
	Plan	   *plan = &node->plan;

	copy_plan_costsize(plan, lefttree);

	/*
	 * Adjust the output rows count and costs according to the offset/limit.
	 * This is only a cosmetic issue if we are at top level, but if we are
	 * building a subquery then it's important to report correct info to the
	 * outer planner.
	 *
	 * When the offset or count couldn't be estimated, use 10% of the
	 * estimated number of rows emitted from the subplan.
	 */
	if (offset_est != 0)
	{
		double		offset_rows;

		if (offset_est > 0)
			offset_rows = (double) offset_est;
		else
			offset_rows = clamp_row_est(lefttree->plan_rows * 0.10);
		if (offset_rows > plan->plan_rows)
			offset_rows = plan->plan_rows;
		if (plan->plan_rows > 0)
			plan->startup_cost +=
				(plan->total_cost - plan->startup_cost)
				* offset_rows / plan->plan_rows;
		plan->plan_rows -= offset_rows;
		if (plan->plan_rows < 1)
			plan->plan_rows = 1;
	}

	if (count_est != 0)
	{
		double		count_rows;

		if (count_est > 0)
			count_rows = (double) count_est;
		else
			count_rows = clamp_row_est(lefttree->plan_rows * 0.10);
		if (count_rows > plan->plan_rows)
			count_rows = plan->plan_rows;
		if (plan->plan_rows > 0)
			plan->total_cost = plan->startup_cost +
				(plan->total_cost - plan->startup_cost)
				* count_rows / plan->plan_rows;
		plan->plan_rows = count_rows;
		if (plan->plan_rows < 1)
			plan->plan_rows = 1;
	}

	plan->targetlist = cdbpullup_targetlist(lefttree,
                            cdbpullup_exprHasSubplanRef((Expr *)lefttree->targetlist));
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	node->limitOffset = limitOffset;
	node->limitCount = limitCount;

	/* CDB */ /* pass limit struct to sort */
	if (IsA(lefttree, Sort) && gp_enable_sort_limit)
	{
		Sort* pSort = (Sort*)lefttree;
		pSort->limitOffset = copyObject(limitOffset);
		pSort->limitCount  = copyObject(limitCount);
	}

	return node;
}

/*
 * make_result
 *	  Build a Result plan node
 *
 * If we have a subplan, assume that any evaluation costs for the gating qual
 * were already factored into the subplan's startup cost, and just copy the
 * subplan cost.  If there's no subplan, we should include the qual eval
 * cost.  In either case, tlist eval cost is not to be included here.
 */
Result *
make_result(/*PlannerInfo *root,*/
			List *tlist,
			Node *resconstantqual,
			Plan *subplan)
{
	Result	   *node = makeNode(Result);
	Plan	   *plan = &node->plan;

	if (subplan)
		copy_plan_costsize(plan, subplan);
	else
	{
		plan->startup_cost = 0;
		plan->total_cost = cpu_tuple_cost;
		plan->plan_rows = 1;	/* wrong if we have a set-valued function? */
		plan->plan_width = 0;	/* XXX is it worth being smarter? */
		if (resconstantqual)
		{
			QualCost	qual_cost;

			cost_qual_eval(&qual_cost, (List *) resconstantqual, NULL /*root*/);
			/* resconstantqual is evaluated once at startup */
			plan->startup_cost += qual_cost.startup + qual_cost.per_tuple;
			plan->total_cost += qual_cost.startup + qual_cost.per_tuple;
		}
	}

	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = subplan;
	plan->righttree = NULL;
	node->resconstantqual = resconstantqual;

	node->hashFilter = false;
	node->hashList = NIL;

	return node;
}

/*
 * make_repeat
 *    Build a Repeat plan node
 */
Repeat *
make_repeat(List *tlist,
			List *qual,
			Expr *repeatCountExpr,
			uint64 grouping,
			Plan *subplan)
{
	Repeat *node = makeNode(Repeat);
	Plan *plan = &node->plan;

	Assert(subplan != NULL);
	copy_plan_costsize(plan, subplan);

	plan->targetlist = tlist;
	plan->qual = qual;
	plan->lefttree = subplan;
	plan->righttree = NULL;

	node->repeatCountExpr = repeatCountExpr;
	node->grouping = grouping;

	return node;
}

/*
 * is_projection_capable_plan
 *		Check whether a given Plan node is able to do projection.
 */
bool
is_projection_capable_plan(Plan *plan)
{
	/* Most plan types can project, so just list the ones that can't */
	switch (nodeTag(plan))
	{
		case T_Hash:
		case T_Material:
		case T_Sort:
		case T_Unique:
		case T_SetOp:
		case T_Limit:
		case T_Append:
		case T_Motion:
		case T_ShareInputScan:
			return false;
		default:
			break;
	}
	return true;
}


/*
 * plan_pushdown_tlist
 *
 * If the given Plan node does projection, the same node is returned after
 * replacing its targetlist with the given targetlist.
 *
 * Otherwise, returns a Result node with the given targetlist, inserted atop
 * the given plan.
 */
Plan *
plan_pushdown_tlist(Plan *plan, List *tlist)
{
	if (is_projection_capable_plan(plan))
    {
        /* Fix up annotation of plan's distribution and ordering properties. */
        if (plan->flow)
            plan->flow = pull_up_Flow((Plan *)make_result(tlist, NULL, plan),
                                      plan, true);

        /* Install the new targetlist. */
        plan->targetlist = tlist;
    }
    else
    {
        Plan   *subplan = plan;

	    /* Insert a Result node to evaluate the targetlist. */
        plan = (Plan *)make_result(tlist, NULL, subplan);

        /* Propagate the subplan's distribution and ordering properties. */
		if (subplan->flow)
            plan->flow = pull_up_Flow(plan, subplan, true);
    }
    return plan;
}                               /* plan_pushdown_tlist */

/*
 * Return true if there is the same tleSortGroupRef in an entry in glist
 * as the tleSortGroupRef in gc.
 */
static bool
groupcol_in_list(GroupClause *gc, List *glist)
{
	bool found = false;
	ListCell *lc;

	foreach (lc, glist)
	{
		GroupClause *entry = (GroupClause *)lfirst(lc);
		Assert (IsA(entry, GroupClause));
		if (gc->tleSortGroupRef == entry->tleSortGroupRef)
		{
			found = true;
			break;
		}
	}
	return found;
}


/*
 * Construct a list of GroupClauses from the transformed GROUP BY clause.
 * This list of GroupClauses has unique tleSortGroupRefs.
 */
static List *
flatten_grouping_list(List *groupcls)
{
	List *result = NIL;
	ListCell *gc;

	foreach (gc, groupcls)
	{
		Node *node = (Node*)lfirst(gc);

		if (node == NULL)
			continue;

		Assert(IsA(node, GroupingClause) ||
			   IsA(node, GroupClause) ||
			   IsA(node, List));

		if (IsA(node, GroupingClause))
		{
			List *groupsets = ((GroupingClause*)node)->groupsets;
			List *flatten_groupsets =
				flatten_grouping_list(groupsets);
			ListCell *lc;

			foreach (lc, flatten_groupsets)
			{
				GroupClause *flatten_gc = (GroupClause *)lfirst(lc);
				Assert(IsA(flatten_gc, GroupClause));

				if (!groupcol_in_list(flatten_gc, result))
					result = lappend(result, flatten_gc);
			}

		}
		else if (IsA(node, List))
		{
			List *flatten_groupsets =
				flatten_grouping_list((List *)node);
			ListCell *lc;

			foreach (lc, flatten_groupsets)
			{
				GroupClause *flatten_gc = (GroupClause *)lfirst(lc);
				Assert(IsA(flatten_gc, GroupClause));

				if (!groupcol_in_list(flatten_gc, result))
					result = lappend(result, flatten_gc);
			}
		}
		else
		{
			Assert(IsA(node, GroupClause));

			if (!groupcol_in_list((GroupClause *)node, result))
				result = lappend(result, node);
		}
	}

	return result;
}
