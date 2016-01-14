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
 * subselect.c
 *	  Planning routines for subselects and parameters.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/optimizer/plan/subselect.c,v 1.112.2.2 2007/07/18 21:41:14 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/catalog.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/gp_policy.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/subselect.h"
#include "optimizer/var.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbmutate.h"

typedef struct convert_testexpr_context
{
	PlannerInfo *root;
	int			rtindex;		/* RT index for Vars, or 0 for Params */
	List	   *righthandIds;	/* accumulated list of Vars or Param IDs */
	List	   *sub_tlist;		/* subselect targetlist (if given) */
} convert_testexpr_context;

typedef struct process_sublinks_context
{
	PlannerInfo *root;
	bool		isTopQual;
} process_sublinks_context;

typedef struct finalize_primnode_context
{
	PlannerInfo *root;
	Bitmapset  *paramids;		/* Set of PARAM_EXEC paramids found */
	Bitmapset  *outer_params;	/* Set of accessible outer paramids */
} finalize_primnode_context;


static Node *build_subplan(PlannerInfo *root, Plan *plan, List *rtable,
						   SubLinkType subLinkType, Node *testexpr,
						   bool unknownEqFalse);
static Node *convert_testexpr_mutator(Node *node,
						 convert_testexpr_context *context);
static bool subplan_is_hashable(Plan *plan, PlannerInfo *root);
static bool testexpr_is_hashable(Node *testexpr);
static bool hash_ok_operator(OpExpr *expr);
static Node *replace_correlation_vars_mutator(Node *node, PlannerInfo *root);
static Node *process_sublinks_mutator(Node *node, process_sublinks_context *context);
static Bitmapset *finalize_plan(PlannerInfo *root, Plan *plan, List *rtable,
			  Bitmapset *outer_params,
			  Bitmapset *valid_params);
static bool finalize_primnode(Node *node, finalize_primnode_context *context);

extern	double global_work_mem(PlannerInfo *root);

/*
 * Generate a Param node to replace the given Var,
 * which is expected to have varlevelsup > 0 (ie, it is not local).
 */
static Param *
replace_outer_var(PlannerInfo *root, Var *var)
{
	Param	   *retval;
	ListCell   *ppl;
	PlannerParamItem *pitem;
	Index		abslevel;
	int			i;

	Assert(var->varlevelsup > 0 && var->varlevelsup < root->query_level);
	abslevel = root->query_level - var->varlevelsup;

	/*
	 * If there's already a paramlist entry for this same Var, just use it.
	 * NOTE: in sufficiently complex querytrees, it is possible for the same
	 * varno/abslevel to refer to different RTEs in different parts of the
	 * parsetree, so that different fields might end up sharing the same Param
	 * number.  As long as we check the vartype/typmod as well, I believe that
	 * this sort of aliasing will cause no trouble. The correct field should
	 * get stored into the Param slot at execution in each part of the tree.
	 *
	 * We need to demand a match on vartypmod.  This does not matter for the 
	 * Param itself, since those are not typmod-dependent, but it does matter
	 * when make_subplan() instantiates a modified copy of the Var for a
	 * subplan's args list.
	 */
	i = 0;
	foreach(ppl, root->glob->paramlist)
	{
		pitem = (PlannerParamItem *) lfirst(ppl);
		if (pitem->abslevel == abslevel && IsA(pitem->item, Var))
		{
			Var		   *pvar = (Var *) pitem->item;

			if (pvar->varno == var->varno &&
				pvar->varattno == var->varattno &&
				pvar->vartype == var->vartype &&
				pvar->vartypmod == var->vartypmod)
				break;
		}
		i++;
	}

	if (!ppl)
	{
		/* Nope, so make a new one */
		var = (Var *) copyObject(var);
		var->varlevelsup = 0;

		pitem = makeNode(PlannerParamItem);
		pitem->item = (Node *) var;
		pitem->abslevel = abslevel;

		root->glob->paramlist = lappend(root->glob->paramlist, pitem);
		/* i is already the correct index for the new item */
	}

	retval = makeNode(Param);
	retval->paramkind = PARAM_EXEC;
	retval->paramid = i;
	retval->paramtype = var->vartype;
	retval->paramtypmod = var->vartypmod;
	retval->location = -1;

	return retval;
}

/*
 * Generate a Param node to replace the given Aggref
 * which is expected to have agglevelsup > 0 (ie, it is not local).
 */
static Param *
replace_outer_agg(PlannerInfo *root, Aggref *agg)
{
	Param	   *retval;
	PlannerParamItem *pitem;
	Index		abslevel;
	int			i;

	Assert(agg->agglevelsup > 0 && agg->agglevelsup < root->query_level);
	abslevel = root->query_level - agg->agglevelsup;

	/*
	 * It does not seem worthwhile to try to match duplicate outer aggs. Just
	 * make a new slot every time.
	 */
	agg = (Aggref *) copyObject(agg);
	IncrementVarSublevelsUp((Node *) agg, -((int) agg->agglevelsup), 0);
	Assert(agg->agglevelsup == 0);

	pitem = makeNode(PlannerParamItem);
	pitem->item = (Node *) agg;
	pitem->abslevel = abslevel;

	root->glob->paramlist = lappend(root->glob->paramlist, pitem);
	i = list_length(root->glob->paramlist) - 1;

	retval = makeNode(Param);
	retval->paramkind = PARAM_EXEC;
	retval->paramid = i;
	retval->paramtype = agg->aggtype;
	retval->paramtypmod = -1;
	retval->location = -1;

	return retval;
}

/*
 * Generate a new Param node that will not conflict with any other.
 *
 * This is used to allocate PARAM_EXEC slots for subplan outputs.
 */
static Param *
generate_new_param(PlannerInfo *root, Oid paramtype, int32 paramtypmod)
{
	Param	   *retval;
	PlannerParamItem *pitem;

	retval = makeNode(Param);
	retval->paramkind = PARAM_EXEC;
	retval->paramid = list_length(root->glob->paramlist);
	retval->paramtype = paramtype;
	retval->paramtypmod = paramtypmod;
	retval->location = -1;

	pitem = makeNode(PlannerParamItem);
	pitem->item = (Node *) retval;
	pitem->abslevel = root->query_level;

	root->glob->paramlist = lappend(root->glob->paramlist, pitem);

	return retval;
}

/*
 * Get the datatype of the first column of the plan's output.
 *
 * This is stored for ARRAY_SUBLINK execution and for exprType()/exprTypmod(),
 * which have no way to get at the plan associated with a SubPlan node.
 * We really only need the info for EXPR_SUBLINK and ARRAY_SUBLINK subplans,
 * but for consistency we save it always.
 */
static void
get_first_col_type(Plan *plan, Oid *coltype, int32 *coltypmod)
{
	/* In cases such as EXISTS, tlist might be empty; arbitrarily use VOID */
	if (plan->targetlist)
	{
		TargetEntry *tent = (TargetEntry *) linitial(plan->targetlist);

		Assert(IsA(tent, TargetEntry));
		if (!tent->resjunk)
		{
			*coltype = exprType((Node *) tent->expr);
			*coltypmod = exprTypmod((Node *) tent->expr);
			return;
		}
	}
	*coltype = VOIDOID;
	*coltypmod = -1;
}

/**
 * Returns true if query refers to a distributed table.
 */
static bool QueryHasDistributedRelation(Query *q)
{
	ListCell   *rt = NULL;

	foreach(rt, q->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

		if (rte->relid != InvalidOid
				&& rte->rtekind == RTE_RELATION)
		{
			GpPolicy *policy = GpPolicyFetch(CurrentMemoryContext, rte->relid);
			bool result = (policy->ptype == POLICYTYPE_PARTITIONED);
			pfree(policy);
			if (result)
			{
				return true;
			}
		}
	}
	return false;
}

typedef struct CorrelatedVarWalkerContext
{
	int maxLevelsUp;
} CorrelatedVarWalkerContext;

/**
 *  Walker finds the deepest correlation nesting i.e. maximum levelsup among all
 *  vars in subquery.
 */
static bool CorrelatedVarWalker(Node *node, CorrelatedVarWalkerContext *ctx)
{
	Assert(ctx);

	if (node == NULL)
	{
		return false;
	}
	else if (IsA(node, Var))
	{
		Var * v = (Var *) node;
		if (v->varlevelsup > ctx->maxLevelsUp)
		{
			ctx->maxLevelsUp = v->varlevelsup;
		}
		return false;
	}
	else if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, CorrelatedVarWalker, ctx, 0 /* flags */);
	}

	return expression_tree_walker(node, CorrelatedVarWalker, ctx);
}

/**
 * Returns true if subquery is correlated
 */
bool IsSubqueryCorrelated(Query *sq)
{
	Assert(sq);
	CorrelatedVarWalkerContext ctx;
	ctx.maxLevelsUp = 0;
	CorrelatedVarWalker((Node *) sq, &ctx);
	return (ctx.maxLevelsUp > 0);
}

/**
 * Returns true if subquery contains references to more than its immediate outer query.
 */
bool IsSubqueryMultiLevelCorrelated(Query *sq)
{
	Assert(sq);
	CorrelatedVarWalkerContext ctx;
	ctx.maxLevelsUp = 0;
	CorrelatedVarWalker((Node *) sq, &ctx);
	return (ctx.maxLevelsUp > 1);
}

/*
 * Convert a SubLink (as created by the parser) into a SubPlan.
 *
 * We are given the SubLink's contained query, type, and testexpr.  We are
 * also told if this expression appears at top level of a WHERE/HAVING qual.
 *
 * Note: we assume that the testexpr has been AND/OR flattened (actually,
 * it's been through eval_const_expressions), but not converted to
 * implicit-AND form; and any SubLinks in it should already have been
 * converted to SubPlans.  The subquery is as yet untouched, however.
 *
 * The result is whatever we need to substitute in place of the SubLink
 * node in the executable expression.  This will be either the SubPlan
 * node (if we have to do the subplan as a subplan), or a Param node
 * representing the result of an InitPlan, or a row comparison expression
 * tree containing InitPlan Param nodes.
 */
static Node *
make_subplan(PlannerInfo *root, Query *orig_subquery, SubLinkType subLinkType,
			 Node *testexpr, bool isTopQual)
{
	Query	   *subquery = NULL;
	double		tuple_fraction = 1.0;
	bool hasResource = (root->glob->resource != NULL);
	/*
	 * Copy the source Query node.	This is a quick and dirty kluge to resolve
	 * the fact that the parser can generate trees with multiple links to the
	 * same sub-Query node, but the planner wants to scribble on the Query.
	 * Try to clean this up when we do querytree redesign...
	 */
	subquery = (Query *) copyObject(orig_subquery);

	/*
	 * For an EXISTS subplan, tell lower-level planner to expect that only the
	 * first tuple will be retrieved.  For ALL and ANY subplans, we will be
	 * able to stop evaluating if the test condition fails, so very often not
	 * all the tuples will be retrieved; for lack of a better idea, specify
	 * 50% retrieval.  For EXPR and ROWCOMPARE subplans, use default behavior
	 * (we're only expecting one row out, anyway).
	 *
	 * NOTE: if you change these numbers, also change cost_qual_eval_walker()
	 * in path/costsize.c.
	 *
	 * XXX If an ALL/ANY subplan is uncorrelated, we may decide to hash or
	 * materialize its result below.  In that case it would've been better to
	 * specify full retrieval.	At present, however, we can only detect
	 * correlation or lack of it after we've made the subplan :-(. Perhaps
	 * detection of correlation should be done as a separate step. Meanwhile,
	 * we don't want to be too optimistic about the percentage of tuples
	 * retrieved, for fear of selecting a plan that's bad for the
	 * materialization case.
	 */
	if (subLinkType == EXISTS_SUBLINK)
		tuple_fraction = 1.0;	/* just like a LIMIT 1 */
	else if (subLinkType == ALL_SUBLINK ||
			 subLinkType == ANY_SUBLINK)
		tuple_fraction = 0.5;	/* 50% */
	else
		tuple_fraction = 0.0;	/* default behavior */

	PlannerConfig *config = CopyPlannerConfig(root->config);

	if ((Gp_role == GP_ROLE_DISPATCH)
			&& IsSubqueryMultiLevelCorrelated(subquery)
			&& QueryHasDistributedRelation(subquery))
	{
	  if (hasResource)
	  {
	    elog(ERROR, "correlated subquery with skip-level correlations is not supported");
	  }
	}

	if ((Gp_role == GP_ROLE_DISPATCH)
			&& IsSubqueryCorrelated(subquery)
			&& QueryHasDistributedRelation(subquery))
	{
		/*
		 * Generate the plan for the subquery with certain options disabled.
		 */
		config->gp_enable_direct_dispatch = false;
		config->gp_enable_multiphase_agg = false;

		/*
		 * Only create subplans with sequential scans
		 */
		config->enable_indexscan = false;
		config->enable_bitmapscan = false;
		config->enable_tidscan = false;
		config->enable_seqscan = true;
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		config->gp_cte_sharing = IsSubqueryCorrelated(subquery) ||
				!(subLinkType == ROWCOMPARE_SUBLINK ||
				 subLinkType == ARRAY_SUBLINK ||
				 subLinkType == EXPR_SUBLINK ||
				 subLinkType == EXISTS_SUBLINK);
	}

	PlannerInfo *subroot = NULL;

	Plan *plan = subquery_planner(root->glob, subquery,
			root,
			tuple_fraction,
			&subroot,
			config);

	/* And convert to SubPlan or InitPlan format. */
	Node	   *result = build_subplan(root,
										plan,
										subroot->parse->rtable,
										subLinkType,
										testexpr,
										isTopQual);


	return result;
}

/*
 * Build a SubPlan node given the raw inputs --- subroutine for make_subplan
 *
 * Returns either the SubPlan, or an expression using initplan output Params,
 * as explained in the comments for make_subplan.
 */
static Node *
build_subplan(PlannerInfo *root, Plan *plan, List *rtable,
			  SubLinkType subLinkType, Node *testexpr,
			  bool unknownEqFalse)
{
	Node	   *result;
	SubPlan    *splan;
	Bitmapset  *tmpset;
	int			paramid;
	
	/*
	 * Initialize the SubPlan node.  Note plan_id, plan_name, and cost fields
	 * are set further down.
	 */
	splan = makeNode(SubPlan);
	splan->subLinkType = subLinkType;
    splan->qDispSliceId = 0;             /*CDB*/
	splan->testexpr = NULL;
	splan->paramIds = NIL;
	get_first_col_type(plan, &splan->firstColType, &splan->firstColTypmod);
	splan->useHashTable = false;
	splan->unknownEqFalse = unknownEqFalse;
	splan->is_initplan = false;
	splan->is_multirow = false;
	splan->is_parallelized = false;
	splan->setParam = NIL;
	splan->parParam = NIL;
	splan->args = NIL;

	/*
	 * Make parParam and args lists of param IDs and expressions that current
	 * query level will pass to this child plan.
	 */
	tmpset = bms_copy(plan->extParam);
	while ((paramid = bms_first_member(tmpset)) >= 0)
	{
		PlannerParamItem *pitem = list_nth(root->glob->paramlist, paramid);

		if (pitem->abslevel == root->query_level)
		{
			splan->parParam = lappend_int(splan->parParam, paramid);
			/*
			 * The Var or Aggref has already been adjusted to have the correct
			 * varlevelsup or agglevelsup.	We probably don't even need to
			 * copy it again, but be safe.
			 */
			splan->args = lappend(splan->args, copyObject(pitem->item));
		}
	}
	bms_free(tmpset);

	/*
	 * Un-correlated or undirect correlated plans of EXISTS, EXPR, ARRAY, or
	 * ROWCOMPARE types can be used as initPlans.  For EXISTS, EXPR, or ARRAY,
	 * we just produce a Param referring to the result of evaluating the
	 * initPlan.  For ROWCOMPARE, we must modify the testexpr tree to contain
	 * PARAM_EXEC Params instead of the PARAM_SUBLINK Params emitted by the
	 * parser.
	 */

	if (splan->parParam == NIL && subLinkType == EXISTS_SUBLINK && Gp_role == GP_ROLE_DISPATCH)
	{
		Param	   *prm;

		Assert(testexpr == NULL);
		prm = generate_new_param(root, BOOLOID, -1);
		splan->setParam = list_make1_int(prm->paramid);
		splan->is_initplan = true;
		result = (Node *) prm;
	}
	else if (splan->parParam == NIL && subLinkType == EXPR_SUBLINK && Gp_role == GP_ROLE_DISPATCH)
	{
		TargetEntry *te = linitial(plan->targetlist);
		Param	   *prm;

		Assert(!te->resjunk);
		Assert(testexpr == NULL);
		prm = generate_new_param(root,
								 exprType((Node *) te->expr),
								 exprTypmod((Node *) te->expr));
		splan->setParam = list_make1_int(prm->paramid);
		splan->is_initplan = true;
		result = (Node *) prm;
	}
	else if (splan->parParam == NIL && subLinkType == ARRAY_SUBLINK && Gp_role == GP_ROLE_DISPATCH)
	{		
		TargetEntry *te = linitial(plan->targetlist);
		Oid			arraytype;
		Param	   *prm;

		Assert(!te->resjunk);
		Assert(testexpr == NULL);
		arraytype = get_array_type(exprType((Node *) te->expr));
		if (!OidIsValid(arraytype))
			elog(ERROR, "could not find array type for datatype %s",
				 format_type_be(exprType((Node *) te->expr)));
		prm = generate_new_param(root,
								 arraytype,
								 exprTypmod((Node *) te->expr));
		splan->setParam = list_make1_int(prm->paramid);
		splan->is_initplan = true;
		result = (Node *) prm;
	}
	else if (splan->parParam == NIL && subLinkType == ROWCOMPARE_SUBLINK && Gp_role == GP_ROLE_DISPATCH)
	{
		/* Adjust the Params */
		result = convert_testexpr(root,
								  testexpr,
								  0,
								  &splan->paramIds,
								  NIL);
		splan->setParam = list_copy(splan->paramIds);
		splan->is_initplan = true;

		/*
		 * The executable expression is returned to become part of the outer
		 * plan's expression tree; it is not kept in the initplan node.
		 */
	}
	
	else
	{
		splan->is_multirow = true; /* CDB: take note. */

		/*
		 * We can't convert subplans of ALL_SUBLINK or ANY_SUBLINK types to
		 * initPlans, even when they are uncorrelated or undirect correlated,
		 * because we need to scan the output of the subplan for each outer
		 * tuple.  But if it's a not-direct-correlated IN (= ANY) test, we
		 * might be able to use a hashtable to avoid comparing all the tuples.
		 * TODO siva - I believe we should've pulled these up to be NL joins.
		 * We may want to assert that this is never exercised.
		 */
		if (subLinkType == ANY_SUBLINK &&
			splan->parParam == NIL &&
			subplan_is_hashable(plan, root) &&
			testexpr_is_hashable(splan->testexpr))
			splan->useHashTable = true;

		/*
		 * Adjust the Params in the testexpr.
		 */
		splan->testexpr = convert_testexpr(root,
										   testexpr,
										   0,
										   &splan->paramIds,
										   NIL);

		result = (Node *) splan;

		/* This cannot be an initplan because it is multi-row */
		splan->is_initplan = false;
	}

	AssertEquivalent(splan->is_initplan, !splan->is_multirow && splan->parParam == NIL);

	/*
	 * Add the subplan and its rtable to the global lists.
	 */
	root->glob->subplans = lappend(root->glob->subplans, plan);
	root->glob->subrtables = lappend(root->glob->subrtables, rtable);
	
	splan->plan_id = list_length(root->glob->subplans); /* instead of old PlannerPlanId */

	if (splan->is_initplan)
		root->init_plans = lappend(root->init_plans, splan);
	
	/* Label the subplan for EXPLAIN purposes */
	if (splan->is_initplan)
	{
		ListCell   *lc;
		
		StringInfo buf = makeStringInfo();
		
		appendStringInfo(buf, "InitPlan %d (returns ", splan->plan_id);
		
		foreach(lc, splan->setParam)
		{
			appendStringInfo(buf, "$%d%s",
							lfirst_int(lc),
							lnext(lc) ? "," : "");
		}
		appendStringInfoString(buf, ")");
		splan->plan_name = pstrdup(buf->data);
		pfree(buf->data);
		pfree(buf);
		buf = NULL;
	}
	else
	{
		StringInfo buf = makeStringInfo();
		appendStringInfo(buf, "SubPlan %d", splan->plan_id);
		splan->plan_name = pstrdup(buf->data);
		pfree(buf->data);
		pfree(buf);
		buf = NULL;
	}

	/* NB PostgreSQL calculates subplan cost here, but GPDB does it elsewhere. */

	return result;
}

/*
 * convert_testexpr: convert the testexpr given by the parser into
 * actually executable form.  This entails replacing PARAM_SUBLINK Params
 * with Params or Vars representing the results of the sub-select:
 *
 * If rtindex is 0, we build Params to represent the sub-select outputs.
 * The paramids of the Params created are returned in the *righthandIds list.
 *
 * If rtindex is not 0, we build Vars using that rtindex as varno.	Copies
 * of the Var nodes are returned in *righthandIds (this is a bit of a type
 * cheat, but we can get away with it).
 *
 * The subquery targetlist need be supplied only if rtindex is not 0.
 * We consult it to extract the correct typmods for the created Vars.
 * (XXX this is a kluge that could go away if Params carried typmod.)
 *
 * The given testexpr has already been recursively processed by
 * process_sublinks_mutator.  Hence it can no longer contain any
 * PARAM_SUBLINK Params for lower SubLink nodes; we can safely assume that
 * any we find are for our own level of SubLink.
 */
Node *
convert_testexpr(PlannerInfo *root, Node *testexpr,
				 int rtindex,
				 List **righthandIds,
				 List *sub_tlist)
{
	Node	   *result;
	convert_testexpr_context context;

	context.root = root;
	context.rtindex = rtindex;
	context.righthandIds = NIL;
	context.sub_tlist = sub_tlist;
	result = convert_testexpr_mutator(testexpr, &context);
	*righthandIds = context.righthandIds;
	return result;
}

static Node *
convert_testexpr_mutator(Node *node,
						 convert_testexpr_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_SUBLINK)
		{
			/*
			 * We expect to encounter the Params in column-number sequence. We
			 * could handle non-sequential order if necessary, but for now
			 * there's no need.  (This is also a useful cross-check that we
			 * aren't finding any unexpected Params.)
			 */
			if (param->paramid != list_length(context->righthandIds) + 1)
				elog(ERROR, "unexpected PARAM_SUBLINK ID: %d", param->paramid);

			if (context->rtindex)
			{
				/* Make the Var node representing the subplan's result */
				Var		   *newvar;

				/*
				 * XXX kluge: since Params don't carry typmod, we have to
				 * look into the subquery targetlist to find out the right
				 * typmod to assign to the Var.
				 */
				TargetEntry *ste = get_tle_by_resno(context->sub_tlist,
													param->paramid);

				if (ste == NULL || ste->resjunk)
					elog(ERROR, "subquery output %d not found",
						 param->paramid);
				Assert(param->paramtype == exprType((Node *) ste->expr));

				newvar = makeVar(context->rtindex,
								 param->paramid,
								 param->paramtype,
								 exprTypmod((Node *) ste->expr),
								 0);

				/*
				 * Copy it for caller.	NB: we need a copy to avoid having
				 * doubly-linked substructure in the modified parse tree.
				 */
				context->righthandIds = lappend(context->righthandIds,
												copyObject(newvar));
				return (Node *) newvar;
			}
			else
			{
				/* Make the Param node representing the subplan's result */
				Param	   *newparam;

				newparam = generate_new_param(context->root, param->paramtype, -1);
				/* Record its ID */
				context->righthandIds = lappend_int(context->righthandIds,
													newparam->paramid);
				return (Node *) newparam;
			}
		}
	}
	return expression_tree_mutator(node,
								   convert_testexpr_mutator,
								   (void *) context);
}

/*
 * subplan_is_hashable: can we implement an ANY subplan by hashing?
 */
static bool
subplan_is_hashable(Plan *plan, PlannerInfo *root)
{
	double		subquery_size;

	/*
	 * The estimated size of the subquery result must fit in work_mem. (Note:
	 * we use sizeof(HeapTupleHeaderData) here even though the tuples will
	 * actually be stored as MinimalTuples; this provides some fudge factor
	 * for hashtable overhead.)
	 */
	subquery_size = plan->plan_rows *
		(MAXALIGN(plan->plan_width) + MAXALIGN(sizeof(HeapTupleHeaderData)));
	if (subquery_size > global_work_mem(root))
		return false;

	return true;
}

/*
 * testexpr_is_hashable: is an ANY SubLink's test expression hashable?
 */
static bool
testexpr_is_hashable(Node *testexpr)
{
	/*
	 * The testexpr must be a single OpExpr, or an AND-clause containing
	 * only OpExprs.
	 *
	 * The combining operators must be hashable and strict. The need for
	 * hashability is obvious, since we want to use hashing. Without
	 * strictness, behavior in the presence of nulls is too unpredictable.	We
	 * actually must assume even more than plain strictness: they can't yield
	 * NULL for non-null inputs, either (see nodeSubplan.c).  However, hash
	 * indexes and hash joins assume that too.
	 */
	if (testexpr && IsA(testexpr, OpExpr))
	{
		if (hash_ok_operator((OpExpr *) testexpr))
			return true;
	}
	else if (and_clause(testexpr))
	{
		ListCell   *l;

		foreach(l, ((BoolExpr *) testexpr)->args)
		{
			Node	   *andarg = (Node *) lfirst(l);

			if (!IsA(andarg, OpExpr))
				return false;
			if (!hash_ok_operator((OpExpr *) andarg))
				return false;
		}
		return true;
	}

	return false;
}

static bool
hash_ok_operator(OpExpr *expr)
{
	Oid			opid = expr->opno;
	HeapTuple	tup;
	Form_pg_operator optup;

	tup = SearchSysCache(OPEROID,
						 ObjectIdGetDatum(opid),
						 0, 0, 0);
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for operator %u", opid);
	optup = (Form_pg_operator) GETSTRUCT(tup);
	if (!optup->oprcanhash || optup->oprcom != opid ||
		!func_strict(optup->oprcode))
	{
		ReleaseSysCache(tup);
		return false;
	}
	ReleaseSysCache(tup);
	return true;
}

/*
 * convert_IN_to_join: can we convert an IN SubLink to join style?
 *
 * CDB: This function has been moved into cdbsubselect.c.
 */

/*
 * Replace correlation vars (uplevel vars) with Params.
 *
 * Uplevel aggregates are replaced, too.
 *
 * Note: it is critical that this runs immediately after SS_process_sublinks.
 * Since we do not recurse into the arguments of uplevel aggregates, they will
 * get copied to the appropriate subplan args list in the parent query with
 * uplevel vars not replaced by Params, but only adjusted in level (see
 * replace_outer_agg).	That's exactly what we want for the vars of the parent
 * level --- but if an aggregate's argument contains any further-up variables,
 * they have to be replaced with Params in their turn.	That will happen when
 * the parent level runs SS_replace_correlation_vars.  Therefore it must do
 * so after expanding its sublinks to subplans.  And we don't want any steps
 * in between, else those steps would never get applied to the aggregate
 * argument expressions, either in the parent or the child level.
 */
Node *
SS_replace_correlation_vars(PlannerInfo *root, Node *expr)
{
	/* No setup needed for tree walk, so away we go */
	return replace_correlation_vars_mutator(expr, root);
}

static Node *
replace_correlation_vars_mutator(Node *node, PlannerInfo *root)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup > 0)
			return (Node *) replace_outer_var(root, (Var *) node);
	}
	if (IsA(node, Aggref))
	{
		if (((Aggref *) node)->agglevelsup > 0)
			return (Node *) replace_outer_agg(root, (Aggref *) node);
	}
	return expression_tree_mutator(node,
								   replace_correlation_vars_mutator,
								   root);
}

/*
 * Expand SubLinks to SubPlans in the given expression.
 *
 * The isQual argument tells whether or not this expression is a WHERE/HAVING
 * qualifier expression.  If it is, any sublinks appearing at top level need
 * not distinguish FALSE from UNKNOWN return values.
 */
Node *
SS_process_sublinks(PlannerInfo *root, Node *expr, bool isQual)
{
	process_sublinks_context context;

	context.root = root;
	context.isTopQual = isQual;
	return process_sublinks_mutator(expr, &context);
}

static Node *
process_sublinks_mutator(Node *node, process_sublinks_context *context)
{
	process_sublinks_context locContext;

	locContext.root = context->root;

	if (node == NULL)
		return NULL;
	if (IsA(node, SubLink))
	{
		SubLink    *sublink = (SubLink *) node;
		Node	   *testexpr;

		/*
		 * First, recursively process the lefthand-side expressions, if any.
		 * They're not top-level anymore.
		 */
		locContext.isTopQual = false;
		testexpr = process_sublinks_mutator(sublink->testexpr, &locContext);

		/*
		 * Now build the SubPlan node and make the expr to return.
		 */
		return make_subplan(context->root,
							(Query *) sublink->subselect,
							sublink->subLinkType,
							testexpr,
							context->isTopQual);
	}

	/*
	 * We should never see a SubPlan expression in the input (since this is
	 * the very routine that creates 'em to begin with).  We shouldn't find
	 * ourselves invoked directly on a Query, either.
	 */
	Assert(!is_subplan(node));
	Assert(!IsA(node, Query));

	/*
	 * Because make_subplan() could return an AND or OR clause, we have to
	 * take steps to preserve AND/OR flatness of a qual.  We assume the input
	 * has been AND/OR flattened and so we need no recursion here.
	 *
	 * If we recurse down through anything other than an AND node, we are
	 * definitely not at top qual level anymore.  (Due to the coding here, we
	 * will not get called on the List subnodes of an AND, so no check is
	 * needed for List.)
	 */
	if (and_clause(node))
	{
		List	   *newargs = NIL;
		ListCell   *l;

		/* Still at qual top-level */
		locContext.isTopQual = context->isTopQual;

		foreach(l, ((BoolExpr *) node)->args)
		{
			Node	   *newarg;

			newarg = process_sublinks_mutator(lfirst(l), &locContext);
			if (and_clause(newarg))
				newargs = list_concat(newargs, ((BoolExpr *) newarg)->args);
			else
				newargs = lappend(newargs, newarg);
		}
		return (Node *) make_andclause(newargs);
	}

	/* otherwise not at qual top-level */
	locContext.isTopQual = false;

	if (or_clause(node))
	{
		List	   *newargs = NIL;
		ListCell   *l;

		foreach(l, ((BoolExpr *) node)->args)
		{
			Node	   *newarg;

			newarg = process_sublinks_mutator(lfirst(l), &locContext);
			if (or_clause(newarg))
				newargs = list_concat(newargs, ((BoolExpr *) newarg)->args);
			else
				newargs = lappend(newargs, newarg);
		}
		return (Node *) make_orclause(newargs);
	}

	return expression_tree_mutator(node,
								   process_sublinks_mutator,
								   (void *) &locContext);
}

/*
 * SS_finalize_plan - do final sublink processing for a completed Plan.
 * Input:
 * 	root - PlannerInfo structure that is necessary for walking the tree
 * 	rtable - list of rangetable entries to look at for relids
 * 	attach_initplans - attach all initplans to the top plan node from root
 * Output:
 * 	plan->extParam and plan->allParam - attach params to top of the plan
 */
void
SS_finalize_plan(PlannerInfo *root, List *rtable, Plan *plan, bool attach_initplans)
{
	Bitmapset  *outer_params,
			   *valid_params,
			   *initExtParam,
			   *initSetParam;
	Cost		initplan_cost;
	int			paramid;
	ListCell   *l;

	/*
	 * First, scan the param list to discover the sets of params that are
	 * available from outer query levels and my own query level. We do this
	 * once to save time in the per-plan recursion steps.
	 */
	outer_params = valid_params = NULL;
	paramid = 0;
	foreach(l, root->glob->paramlist)
	{
		PlannerParamItem *pitem = (PlannerParamItem *) lfirst(l);
		AssertImply(IsA(pitem->item, Param), ((Param *) pitem->item)->paramid == paramid);

		valid_params = bms_add_member(valid_params, paramid);

		if (pitem->abslevel <= root->query_level)
		{
			/* valid outer-level parameter */
			outer_params = bms_add_member(outer_params, paramid);
		}

		paramid++;
	}

	/*
	 * Now recurse through plan tree.
	 */
	(void) finalize_plan(root, plan, rtable,
						 outer_params, valid_params);

	bms_free(outer_params);
	bms_free(valid_params);

	/*
	 * Finally, attach any initPlans to the topmost plan node, and add their
	 * extParams to the topmost node's, too.  However, any setParams of the
	 * initPlans should not be present in the topmost node's extParams, only
	 * in its allParams.  (As of PG 8.1, it's possible that some initPlans
	 * have extParams that are setParams of other initPlans, so we have to
	 * take care of this situation explicitly.)
	 *
	 * We also add the total_cost of each initPlan to the startup cost of the
	 * top node.  This is a conservative overestimate, since in fact each
	 * initPlan might be executed later than plan startup, or even not at all.
	 */
	if (attach_initplans)
	{
		Insist(!plan->initPlan);
		plan->initPlan = root->init_plans;
		root->init_plans = NIL;		/* make sure they're not attached twice */

		initExtParam = initSetParam = NULL;
		initplan_cost = 0;
		foreach(l, plan->initPlan)
		{
			SubPlan    *initplan = (SubPlan *) lfirst(l);
			Plan	   *subplan_plan = planner_subplan_get_plan(root, initplan);
			ListCell   *l2;

			initExtParam = bms_add_members(initExtParam, subplan_plan->extParam);
			foreach(l2, initplan->setParam)
			{
				initSetParam = bms_add_member(initSetParam, lfirst_int(l2));
			}
			initplan_cost += subplan_plan->total_cost;
		}
		/* allParam must include all these params */
		plan->allParam = bms_add_members(plan->allParam, initExtParam);
		plan->allParam = bms_add_members(plan->allParam, initSetParam);
		/* but extParam shouldn't include any setParams */
		initExtParam = bms_del_members(initExtParam, initSetParam);
		/* empty test ensures extParam is exactly NULL if it's empty */
		if (!bms_is_empty(initExtParam))
			plan->extParam = bms_join(plan->extParam, initExtParam);

		plan->startup_cost += initplan_cost;
		plan->total_cost += initplan_cost;
	}
}

/*
 * Recursive processing of all nodes in the plan tree
 *
 * The return value is the computed allParam set for the given Plan node.
 * This is just an internal notational convenience.
 */
static Bitmapset *
finalize_plan(PlannerInfo *root, Plan *plan, List *rtable,
			  Bitmapset *outer_params, Bitmapset *valid_params)
{
	finalize_primnode_context context;

	if (plan == NULL)
		return NULL;

	context.root = root;
	context.paramids = NULL;	/* initialize set to empty */
	context.outer_params = outer_params;

	/*
	 * When we call finalize_primnode, context.paramids sets are automatically
	 * merged together.  But when recursing to self, we have to do it the hard
	 * way.  We want the paramids set to include params in subplans as well as
	 * at this level.
	 */

	/* Find params in targetlist and qual */
	finalize_primnode((Node *) plan->targetlist, &context);
	finalize_primnode((Node *) plan->qual, &context);

	/* Check additional node-type-specific fields */
	switch (nodeTag(plan))
	{
		case T_Result:
			finalize_primnode(((Result *) plan)->resconstantqual,
							  &context);
			break;

		case T_IndexScan:
			finalize_primnode((Node *) ((IndexScan *) plan)->indexqual,
							  &context);

			/*
			 * we need not look at indexqualorig, since it will have the same
			 * param references as indexqual.
			 */
			break;

		case T_BitmapIndexScan:
			finalize_primnode((Node *) ((BitmapIndexScan *) plan)->indexqual,
							  &context);

			/*
			 * we need not look at indexqualorig, since it will have the same
			 * param references as indexqual.
			 */
			break;

		case T_BitmapHeapScan:
			finalize_primnode((Node *) ((BitmapHeapScan *) plan)->bitmapqualorig,
							  &context);
			break;

		case T_BitmapTableScan:
			finalize_primnode((Node *) ((BitmapTableScan *) plan)->bitmapqualorig,
							  &context);
			break;

		case T_TidScan:
			finalize_primnode((Node *) ((TidScan *) plan)->tidquals,
							  &context);
			break;

		case T_SubqueryScan:
			/*
			 * In a SubqueryScan, SS_finalize_plan has already been run on the
			 * subplan by the inner invocation of subquery_planner, so there's
			 * no need to do it again.	Instead, just pull out the subplan's
			 * extParams list, which represents the params it needs from my
			 * level and higher levels.
			 */
			context.paramids = bms_add_members(context.paramids,
								 ((SubqueryScan *) plan)->subplan->extParam);
			break;

		case T_TableFunctionScan:
			{
				RangeTblEntry *rte;

				rte = rt_fetch(((TableFunctionScan *) plan)->scan.scanrelid,
							   rtable);
				Assert(rte->rtekind == RTE_TABLEFUNCTION);
				finalize_primnode(rte->funcexpr, &context);
			}
			break;

		case T_FunctionScan:
			{
				RangeTblEntry *rte;

				rte = rt_fetch(((FunctionScan *) plan)->scan.scanrelid,
							   rtable);
				Assert(rte->rtekind == RTE_FUNCTION);
				finalize_primnode(rte->funcexpr, &context);
			}
			break;

		case T_ValuesScan:
			{
				RangeTblEntry *rte;

				rte = rt_fetch(((ValuesScan *) plan)->scan.scanrelid,
							   rtable);
				Assert(rte->rtekind == RTE_VALUES);
				finalize_primnode((Node *) rte->values_lists, &context);
			}
			break;

		case T_Append:
			{
				ListCell   *l;

				foreach(l, ((Append *) plan)->appendplans)
				{
					context.paramids =
						bms_add_members(context.paramids,
										finalize_plan(root,
													  (Plan *) lfirst(l),
													  rtable,
													  outer_params,
													  valid_params));
				}
			}
			break;

		case T_BitmapAnd:
			{
				ListCell   *l;

				foreach(l, ((BitmapAnd *) plan)->bitmapplans)
				{
					context.paramids =
						bms_add_members(context.paramids,
										finalize_plan(root,
													  (Plan *) lfirst(l),
													  rtable,
													  outer_params,
													  valid_params));
				}
			}
			break;

		case T_BitmapOr:
			{
				ListCell   *l;

				foreach(l, ((BitmapOr *) plan)->bitmapplans)
				{
					context.paramids =
						bms_add_members(context.paramids,
										finalize_plan(root,
													  (Plan *) lfirst(l),
													  rtable,
													  outer_params,
													  valid_params));
				}
			}
			break;

		case T_NestLoop:
			finalize_primnode((Node *) ((Join *) plan)->joinqual,
							  &context);
			break;

		case T_MergeJoin:
			finalize_primnode((Node *) ((Join *) plan)->joinqual,
							  &context);
			finalize_primnode((Node *) ((MergeJoin *) plan)->mergeclauses,
							  &context);
			break;

		case T_HashJoin:
			finalize_primnode((Node *) ((Join *) plan)->joinqual,
							  &context);
			finalize_primnode((Node *) ((HashJoin *) plan)->hashclauses,
							  &context);
			finalize_primnode((Node *) ((HashJoin *) plan)->hashqualclauses,
							  &context);
			break;

		case T_Motion:

			finalize_primnode((Node *) ((Motion *) plan)->hashExpr,
							  &context);
			break;

		case T_Limit:
			finalize_primnode(((Limit *) plan)->limitOffset,
							  &context);
			finalize_primnode(((Limit *) plan)->limitCount,
							  &context);
			break;

		case T_Hash:
		case T_Agg:
		case T_Window:
		case T_SeqScan:
		case T_AppendOnlyScan:
		case T_ParquetScan:
		case T_ExternalScan:
		case T_Material:
		case T_Sort:
		case T_ShareInputScan:
		case T_Unique:
		case T_SetOp:
		case T_Repeat:
			break;

		default:
            Assert(false);
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(plan));
	}

	/* Process left and right child plans, if any */
	context.paramids = bms_add_members(context.paramids,
									   finalize_plan(root,
													 plan->lefttree,
													 rtable,
													 outer_params,
													 valid_params));

	context.paramids = bms_add_members(context.paramids,
									   finalize_plan(root,
													 plan->righttree,
													 rtable,
													 outer_params,
													 valid_params));

	/* Now we have all the paramids */

	if (!bms_is_subset(context.paramids, valid_params))
		elog(ERROR, "plan should not reference subplan's variable");

	plan->extParam = bms_intersect(context.paramids, outer_params);
	plan->allParam = context.paramids;

	/*
	 * For speed at execution time, make sure extParam/allParam are actually
	 * NULL if they are empty sets.
	 */
	if (bms_is_empty(plan->extParam))
	{
		bms_free(plan->extParam);
		plan->extParam = NULL;
	}
	if (bms_is_empty(plan->allParam))
	{
		bms_free(plan->allParam);
		plan->allParam = NULL;
	}

	return plan->allParam;
}

/*
 * finalize_primnode: add IDs of all PARAM_EXEC params appearing in the given
 * expression tree to the result set.
 */
static bool
finalize_primnode(Node *node, finalize_primnode_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
	{
		if (((Param *) node)->paramkind == PARAM_EXEC)
		{
			int			paramid = ((Param *) node)->paramid;

			context->paramids = bms_add_member(context->paramids, paramid);
		}
		return false;			/* no more to do here */
	}
	if (is_subplan(node))
	{
		SubPlan    *subplan = (SubPlan *) node;
		Plan	   *subplan_plan = planner_subplan_get_plan(context->root, subplan);

		/* Add outer-level params needed by the subplan to paramids */
		context->paramids = bms_join(context->paramids,
									 bms_intersect(subplan_plan->extParam,
												   context->outer_params));
		/* fall through to recurse into subplan args */
	}
	return expression_tree_walker(node, finalize_primnode,
								  (void *) context);
}

/*
 * SS_make_initplan_from_plan - given a plan tree, make it an InitPlan
 *
 * The plan is expected to return a scalar value of the indicated type.
 * We build an EXPR_SUBLINK SubPlan node and put it into the initplan
 * list for the current query level.  A Param that represents the initplan's
 * output is returned.
 *
 * We assume the plan hasn't been put through SS_finalize_plan.
 *
 * We treat root->init_plans like the old PlannerInitPlan global here.
 */
Param *
SS_make_initplan_from_plan(PlannerInfo *root, Plan *plan,
						   Oid resulttype, int32 resulttypmod)
{
	SubPlan    *node;
	Param	   *prm;

	/*
	 * We must run SS_finalize_plan(), since that's normally done before a
	 * subplan gets put into the initplan list.  Tell it not to attach any
	 * pre-existing initplans to this one, since they are siblings not
	 * children of this initplan.  (This is something else that could perhaps
	 * be cleaner if we did extParam/allParam processing in setrefs.c instead
	 * of here?  See notes for materialize_finished_plan.)
	 */
	
	/*
	 * Build extParam/allParam sets for plan nodes.
	 */
	SS_finalize_plan(root, root->parse->rtable, plan, false);
	
	/*
	 * Add the subplan and its rtable to the global lists.
	 */
	root->glob->subplans = lappend(root->glob->subplans,
								   plan);
	root->glob->subrtables = lappend(root->glob->subrtables,
									 root->parse->rtable);
	
	/*
	 * Create a SubPlan node and add it to the outer list of InitPlans.
	 */
	node = makeNode(SubPlan);
	node->subLinkType = EXPR_SUBLINK;
	get_first_col_type(plan, &node->firstColType, &node->firstColTypmod);
    node->qDispSliceId = 0;             /*CDB*/
	node->plan_id = list_length(root->glob->subplans);
	node->is_initplan = true;
	root->init_plans = lappend(root->init_plans, node);

	/*
	 * The node can't have any inputs (since it's an initplan), so the
	 * parParam and args lists remain empty.
	 */
	
	/* NB PostgreSQL calculates subplan cost here, but GPDB does it elsewhere. */

	/*
	 * Make a Param that will be the subplan's output.
	 */
	prm = generate_new_param(root, resulttype, resulttypmod);
	node->setParam = list_make1_int(prm->paramid);
	
	/* Label the subplan for EXPLAIN purposes */
	StringInfo buf = makeStringInfo();
	appendStringInfo(buf, "InitPlan %d (returns $%d)",
			node->plan_id, prm->paramid);
	node->plan_name = pstrdup(buf->data);
	pfree(buf->data);
	pfree(buf);
	buf = NULL;
	
	return prm;
}

