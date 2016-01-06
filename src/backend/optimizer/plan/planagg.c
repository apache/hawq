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
 * planagg.c
 *	  Special planning for aggregate queries.
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/optimizer/plan/planagg.c,v 1.22.2.1 2007/02/06 06:50:33 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/subselect.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "cdb/cdbllize.h"                   /* pull_up_Flow() */


typedef struct
{
	Oid			aggfnoid;		/* pg_proc Oid of the aggregate */
	Oid			aggsortop;		/* Oid of its sort operator */
	Expr	   *target;			/* expression we are aggregating on */
	IndexPath  *path;			/* access path for index scan */
	Cost		pathcost;		/* estimated cost to fetch first row */
	Param	   *param;			/* param for subplan's output */
} MinMaxAggInfo;

static bool find_minmax_aggs_walker(Node *node, List **context);
static bool build_minmax_path(PlannerInfo *root, RelOptInfo *rel,
				  MinMaxAggInfo *info);
static ScanDirection match_agg_to_index_col(MinMaxAggInfo *info,
					   IndexOptInfo *index, int indexcol);
static void make_agg_subplan(PlannerInfo *root, MinMaxAggInfo *info);
static Node *replace_aggs_with_params_mutator(Node *node, List **context);
static Oid	fetch_agg_sort_op(Oid aggfnoid);


/*
 * optimize_minmax_aggregates - check for optimizing MIN/MAX via indexes
 *
 * This checks to see if we can replace MIN/MAX aggregate functions by
 * subqueries of the form
 *		(SELECT col FROM tab WHERE ... ORDER BY col ASC/DESC LIMIT 1)
 * Given a suitable index on tab.col, this can be much faster than the
 * generic scan-all-the-rows plan.
 *
 * We are passed the preprocessed tlist, and the best path
 * devised for computing the input of a standard Agg node.	If we are able
 * to optimize all the aggregates, and the result is estimated to be cheaper
 * than the generic aggregate method, then generate and return a Plan that
 * does it that way.  Otherwise, return NULL.
 */
Plan *
optimize_minmax_aggregates(PlannerInfo *root, List *tlist, Path *best_path)
{
	Query	   *parse = root->parse;
	FromExpr   *jtnode;
	RangeTblRef *rtr;
	RangeTblEntry *rte;
	RelOptInfo *rel;
	List	   *aggs_list;
	ListCell   *l;
	Cost		total_cost;
	Path		agg_p;
	Plan	   *plan;
	Node	   *hqual;
	QualCost	tlist_cost;

	/* Nothing to do if query has no aggregates */
	if (!parse->hasAggs)
		return NULL;

	Assert(!parse->setOperations);		/* shouldn't get here if a setop */
	Assert(parse->rowMarks == NIL);		/* nor if FOR UPDATE */

	/*
	 * Reject unoptimizable cases.
	 *
	 * We don't handle GROUP BY, because our current implementations of
	 * grouping require looking at all the rows anyway, and so there's not
	 * much point in optimizing MIN/MAX.
	 */
	if (parse->groupClause)
		return NULL;

	/*
	 * We also restrict the query to reference exactly one table, since join
	 * conditions can't be handled reasonably.  (We could perhaps handle a
	 * query containing cartesian-product joins, but it hardly seems worth the
	 * trouble.)  However, the single real table could be buried in several
	 * levels of FromExpr.
	 */
	jtnode = parse->jointree;
	while (IsA(jtnode, FromExpr))
	{
		if (list_length(jtnode->fromlist) != 1)
			return NULL;
		jtnode = linitial(jtnode->fromlist);
	}
	if (!IsA(jtnode, RangeTblRef))
		return NULL;
	rtr = (RangeTblRef *) jtnode;
	rte = rt_fetch(rtr->rtindex, parse->rtable);
	if (rte->rtekind != RTE_RELATION || rte->inh)
		return NULL;
	rel = find_base_rel(root, rtr->rtindex);

	/*
	 * Since this optimization is not applicable all that often, we want to
	 * fall out before doing very much work if possible.  Therefore we do the
	 * work in several passes.	The first pass scans the tlist and HAVING qual
	 * to find all the aggregates and verify that each of them is a MIN/MAX
	 * aggregate.  If that succeeds, the second pass looks at each aggregate
	 * to see if it is optimizable; if so we make an IndexPath describing how
	 * we would scan it.  (We do not try to optimize if only some aggs are
	 * optimizable, since that means we'll have to scan all the rows anyway.)
	 * If that succeeds, we have enough info to compare costs against the
	 * generic implementation. Only if that test passes do we build a Plan.
	 */

	/* Pass 1: find all the aggregates */
	aggs_list = NIL;
	if (find_minmax_aggs_walker((Node *) tlist, &aggs_list))
		return NULL;
	if (find_minmax_aggs_walker(parse->havingQual, &aggs_list))
		return NULL;

	/* Pass 2: see if each one is optimizable */
	total_cost = 0;
	foreach(l, aggs_list)
	{
		MinMaxAggInfo *info = (MinMaxAggInfo *) lfirst(l);

		if (!build_minmax_path(root, rel, info))
			return NULL;
		total_cost += info->pathcost;
	}

	/*
	 * Make the cost comparison.
	 *
	 * Note that we don't include evaluation cost of the tlist here; this is
	 * OK since it isn't included in best_path's cost either, and should be
	 * the same in either case.
	 */
	cost_agg(&agg_p, root, AGG_PLAIN, list_length(aggs_list),
			 0, 0,
			 best_path->startup_cost, best_path->total_cost,
			 best_path->parent->rows, 0.0, 0.0, 0.0, false);

	if (total_cost > agg_p.total_cost)
		return NULL;			/* too expensive */

	/*
	 * OK, we are going to generate an optimized plan.
	 */

	/* Pass 3: generate subplans and output Param nodes */
	foreach(l, aggs_list)
	{
		make_agg_subplan(root, (MinMaxAggInfo *) lfirst(l));
	}

	/*
	 * Modify the targetlist and HAVING qual to reference subquery outputs
	 */
	tlist = (List *) replace_aggs_with_params_mutator((Node *) tlist,
													  &aggs_list);
	hqual = replace_aggs_with_params_mutator(parse->havingQual,
											 &aggs_list);

	/*
	 * Generate the output plan --- basically just a Result
	 */
	plan = (Plan *) make_result(tlist, hqual, NULL);

	/* Account for evaluation cost of the tlist (make_result did the rest) */
	cost_qual_eval(&tlist_cost, tlist, root);
	plan->startup_cost += tlist_cost.startup;
	plan->total_cost += tlist_cost.startup + tlist_cost.per_tuple;

	return plan;
}

/*
 * find_minmax_aggs_walker
 *		Recursively scan the Aggref nodes in an expression tree, and check
 *		that each one is a MIN/MAX aggregate.  If so, build a list of the
 *		distinct aggregate calls in the tree.
 *
 * Returns TRUE if a non-MIN/MAX aggregate is found, FALSE otherwise.
 * (This seemingly-backward definition is used because expression_tree_walker
 * aborts the scan on TRUE return, which is what we want.)
 *
 * Found aggregates are added to the list at *context; it's up to the caller
 * to initialize the list to NIL.
 *
 * This does not descend into subqueries, and so should be used only after
 * reduction of sublinks to subplans.  There mustn't be outer-aggregate
 * references either.
 */
static bool
find_minmax_aggs_walker(Node *node, List **context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		Aggref	   *aggref = (Aggref *) node;
		Oid			aggsortop;
		Expr	   *curTarget;
		MinMaxAggInfo *info;
		ListCell   *l;

		Assert(aggref->agglevelsup == 0);
		if (list_length(aggref->args) != 1)
			return true;		/* it couldn't be MIN/MAX */
		/* note: we do not care if DISTINCT is mentioned ... */

		aggsortop = fetch_agg_sort_op(aggref->aggfnoid);
		if (!OidIsValid(aggsortop))
			return true;		/* not a MIN/MAX aggregate */

		/*
		 * Check whether it's already in the list, and add it if not.
		 */
		curTarget = linitial(aggref->args);
		foreach(l, *context)
		{
			info = (MinMaxAggInfo *) lfirst(l);
			if (info->aggfnoid == aggref->aggfnoid &&
				equal(info->target, curTarget))
				return false;
		}

		info = (MinMaxAggInfo *) palloc0(sizeof(MinMaxAggInfo));
		info->aggfnoid = aggref->aggfnoid;
		info->aggsortop = aggsortop;
		info->target = curTarget;

		*context = lappend(*context, info);

		/*
		 * We need not recurse into the argument, since it can't contain any
		 * aggregates.
		 */
		return false;
	}
	Assert(!IsA(node, SubLink));
	return expression_tree_walker(node, find_minmax_aggs_walker,
								  (void *) context);
}

/*
 * build_minmax_path
 *		Given a MIN/MAX aggregate, try to find an index it can be optimized
 *		with.  Build a Path describing the best such index path.
 *
 * Returns TRUE if successful, FALSE if not.  In the TRUE case, info->path
 * is filled in.
 *
 * XXX look at sharing more code with indxpath.c.
 *
 * Note: check_partial_indexes() must have been run previously.
 */
static bool
build_minmax_path(PlannerInfo *root, RelOptInfo *rel, MinMaxAggInfo *info)
{
	IndexPath  *best_path = NULL;
	Cost		best_cost = 0;
	ListCell   *l;

	foreach(l, rel->indexlist)
	{
		IndexOptInfo *index = (IndexOptInfo *) lfirst(l);
		ScanDirection indexscandir = NoMovementScanDirection;
		int			indexcol;
		int			prevcol;
		List	   *restrictclauses;
		IndexPath  *new_path;
		Cost		new_cost;
		bool		found_clause;

		/* Ignore non-btree indexes */
		if (index->relam != BTREE_AM_OID)
			continue;

		/* Ignore partial indexes that do not match the query */
		if (index->indpred != NIL && !index->predOK)
			continue;

		/*
		 * Look for a match to one of the index columns.  (In a stupidly
		 * designed index, there could be multiple matches, but we only care
		 * about the first one.)
		 */
		for (indexcol = 0; indexcol < index->ncolumns; indexcol++)
		{
			indexscandir = match_agg_to_index_col(info, index, indexcol);
			if (!ScanDirectionIsNoMovement(indexscandir))
				break;
		}
		if (ScanDirectionIsNoMovement(indexscandir))
			continue;

		/*
		 * If the match is not at the first index column, we have to verify
		 * that there are "x = something" restrictions on all the earlier
		 * index columns.  Since we'll need the restrictclauses list anyway to
		 * build the path, it's convenient to extract that first and then look
		 * through it for the equality restrictions.
		 */
		restrictclauses = group_clauses_by_indexkey(index,
												index->rel->baserestrictinfo,
													NIL,
													NULL,
													SAOP_FORBID,
													&found_clause);

		if (list_length(restrictclauses) < indexcol)
			continue;			/* definitely haven't got enough */
		for (prevcol = 0; prevcol < indexcol; prevcol++)
		{
			List	   *rinfos = (List *) list_nth(restrictclauses, prevcol);
			ListCell   *ll;

			foreach(ll, rinfos)
			{
				RestrictInfo *rinfo = (RestrictInfo *) lfirst(ll);
				int			strategy;

				Assert(is_opclause(rinfo->clause));
				strategy =
					get_op_opclass_strategy(((OpExpr *) rinfo->clause)->opno,
											index->classlist[prevcol]);
				if (strategy == BTEqualStrategyNumber)
					break;
			}
			if (ll == NULL)
				break;			/* none are Equal for this index col */
		}
		if (prevcol < indexcol)
			continue;			/* didn't find all Equal clauses */

		/*
		 * Build the access path.  We don't bother marking it with pathkeys.
		 */
		new_path = create_index_path(root, index,
									 restrictclauses,
									 NIL,
									 indexscandir,
									 NULL);

		/*
		 * Estimate actual cost of fetching just one row.
		 */
		if (new_path->rows > 1.0)
			new_cost = new_path->path.startup_cost +
				(new_path->path.total_cost - new_path->path.startup_cost)
				* 1.0 / new_path->rows;
		else
			new_cost = new_path->path.total_cost;

		/*
		 * Keep if first or if cheaper than previous best.
		 */
		if (best_path == NULL || new_cost < best_cost)
		{
			best_path = new_path;
			best_cost = new_cost;
		}
	}

	info->path = best_path;
	info->pathcost = best_cost;
	return (best_path != NULL);
}

/*
 * match_agg_to_index_col
 *		Does an aggregate match an index column?
 *
 * It matches if its argument is equal to the index column's data and its
 * sortop is either the LessThan or GreaterThan member of the column's opclass.
 *
 * We return ForwardScanDirection if match the LessThan member,
 * BackwardScanDirection if match the GreaterThan member,
 * and NoMovementScanDirection if there's no match.
 */
static ScanDirection
match_agg_to_index_col(MinMaxAggInfo *info, IndexOptInfo *index, int indexcol)
{
	int			strategy;

	/* Check for data match */
	if (!match_index_to_operand((Node *) info->target, indexcol, index))
		return NoMovementScanDirection;

	/* Look up the operator in the opclass */
	strategy = get_op_opclass_strategy(info->aggsortop,
									   index->classlist[indexcol]);
	if (strategy == BTLessStrategyNumber)
		return ForwardScanDirection;
	if (strategy == BTGreaterStrategyNumber)
		return BackwardScanDirection;
	return NoMovementScanDirection;
}

/*
 * Construct a suitable plan for a converted aggregate query
 */
static void
make_agg_subplan(PlannerInfo *root, MinMaxAggInfo *info)
{
	PlannerInfo subroot;
	Query	   *subparse;
	Plan	   *plan;
	Plan	   *iplan;
	TargetEntry *tle;
	SortClause *sortcl;
	NullTest   *ntest;

	/*
	 * Generate a suitably modified query.	Much of the work here is probably
	 * unnecessary in the normal case, but we want to make it look good if
	 * someone tries to EXPLAIN the result.
	 */
	memcpy(&subroot, root, sizeof(PlannerInfo));
	subroot.parse = subparse = (Query *) copyObject(root->parse);
	subparse->commandType = CMD_SELECT;
	subparse->resultRelation = 0;
	subparse->resultRelations = NIL;
	subparse->returningLists = NIL;
	subparse->intoClause = NULL;
	subparse->hasAggs = false;
	subparse->groupClause = NIL;
	subparse->havingQual = NULL;
	subparse->distinctClause = NIL;
	subroot.hasHavingQual = false;
	
	/* TODO Should we also generate a "temporary" root as in,
	 *       e.g., inheritance planning?
	 *
	 * Generate modified query with this rel as target.  We have to be
	 * prepared to translate varnos in in_info_list as well as in the
	 * Query proper.
	 *
	memcpy(&subroot, root, sizeof(PlannerInfo));
	subroot.parse = (Query *)
	adjust_appendrel_attrs((Node *) parse,
						   appinfo);
	subroot.in_info_list = (List *)
	adjust_appendrel_attrs((Node *) root->in_info_list,
						   appinfo);
	// There shouldn't be any OJ info to translate, as yet
	Assert(subroot.oj_info_list == NIL);
	
	subroot->resultRelations = NIL;
	subroot->returningLists = NIL;
	 */

	/* single tlist entry that is the aggregate target */
	tle = makeTargetEntry(copyObject(info->target),
						  1,
						  pstrdup("agg_target"),
						  false);
	subparse->targetList = list_make1(tle);

	/* set up the appropriate ORDER BY entry */
	sortcl = makeNode(SortClause);
	sortcl->tleSortGroupRef = assignSortGroupRef(tle, subparse->targetList);
	sortcl->sortop = info->aggsortop;
	subparse->sortClause = list_make1(sortcl);

	/* set up LIMIT 1 */
	subparse->limitOffset = NULL;
	subparse->limitCount = (Node *) makeConst(INT8OID, -1, sizeof(int64),
											  Int64GetDatum(1),
											  false, true /* not by val */ );

	/*
	 * Generate the plan for the subquery.	We already have a Path for the
	 * basic indexscan, but we have to convert it to a Plan and attach a LIMIT
	 * node above it.
	 *
	 * Also we must add a "WHERE foo IS NOT NULL" restriction to the
	 * indexscan, to be sure we don't return a NULL, which'd be contrary to
	 * the standard behavior of MIN/MAX.  XXX ideally this should be done
	 * earlier, so that the selectivity of the restriction could be included
	 * in our cost estimates.  But that looks painful, and in most cases the
	 * fraction of NULLs isn't high enough to change the decision.
	 *
	 * The NOT NULL qual has to go on the actual indexscan; create_plan might
	 * have stuck a gating Result atop that, if there were any pseudoconstant
	 * quals.
	 */
	plan = create_plan(&subroot, (Path *) info->path);

    /* Replace the plan's tlist with a copy of the one we built above. */
    plan = plan_pushdown_tlist(plan, copyObject(subparse->targetList));

	if (IsA(plan, Result))
		iplan = plan->lefttree;
	else
		iplan = plan;
	Assert(IsA(iplan, IndexScan));

	ntest = makeNode(NullTest);
	ntest->nulltesttype = IS_NOT_NULL;
	ntest->arg = copyObject(info->target);

	iplan->qual = lcons(ntest, iplan->qual);

	plan = (Plan *) make_limit(plan,
							   subparse->limitOffset,
							   subparse->limitCount,
							   0, 1);

    /* Decorate the Limit node with a Flow node. */
    plan->flow = pull_up_Flow(plan, plan->lefttree, false);

	/*
	 * Convert the plan into an InitPlan, and make a Param for its result.
	 */
	info->param = SS_make_initplan_from_plan(&subroot, plan,
											 exprType((Node *) tle->expr),
											 -1);
}

/*
 * Replace original aggregate calls with subplan output Params
 */
static Node *
replace_aggs_with_params_mutator(Node *node, List **context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Aggref))
	{
		Aggref	   *aggref = (Aggref *) node;
		ListCell   *l;
		Expr	   *curTarget = linitial(aggref->args);

		foreach(l, *context)
		{
			MinMaxAggInfo *info = (MinMaxAggInfo *) lfirst(l);

			if (info->aggfnoid == aggref->aggfnoid &&
				equal(info->target, curTarget))
				return (Node *) info->param;
		}
		elog(ERROR, "failed to re-find aggregate info record");
	}
	Assert(!IsA(node, SubLink));
	return expression_tree_mutator(node, replace_aggs_with_params_mutator,
								   (void *) context);
}

/*
 * Get the OID of the sort operator, if any, associated with an aggregate.
 * Returns InvalidOid if there is no such operator.
 */
static Oid
fetch_agg_sort_op(Oid aggfnoid)
{
	HeapTuple	aggTuple;
	Form_pg_aggregate aggform;
	Oid			aggsortop;

	/* fetch aggregate entry from pg_aggregate */
	aggTuple = SearchSysCache(AGGFNOID,
							  ObjectIdGetDatum(aggfnoid),
							  0, 0, 0);
	if (!HeapTupleIsValid(aggTuple))
		return InvalidOid;
	aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
	aggsortop = aggform->aggsortop;
	ReleaseSysCache(aggTuple);

	return aggsortop;
}
