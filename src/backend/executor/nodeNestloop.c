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
 * nodeNestloop.c
 *	  routines to support nest-loop joins
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/nodeNestloop.c,v 1.43.2.1 2007/02/02 00:07:28 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecNestLoop	 - process a nestloop join of two plans
 *		ExecInitNestLoop - initialize the join
 *		ExecEndNestLoop  - shut down the join
 */

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "executor/execdebug.h"
#include "executor/nodeNestloop.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"

#include "utils/memutils.h"

static void splitJoinQualExpr(NestLoopState *nlstate);
static void extractFuncExprArgs(FuncExprState *fstate, List **lclauses, List **rclauses);

/* ----------------------------------------------------------------
 *		ExecNestLoop(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer
 *				relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecNestLoop(NestLoopState *node)
{
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	List	   *joinqual;
	List	   *otherqual;
	ExprContext *econtext;

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * get the current outer tuple
	 */
	outerTupleSlot = node->js.ps.ps_OuterTupleSlot;
	econtext->ecxt_outertuple = outerTupleSlot;

	/*
	 * If we're doing an IN join, we want to return at most one row per outer
	 * tuple; so we can stop scanning the inner scan if we matched on the
	 * previous try.
	 */
	if (node->js.jointype == JOIN_IN &&
		node->nl_MatchedOuter)
		node->nl_NeedNewOuter = true;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.  Note this can't happen
	 * until we're done projecting out tuples from a join tuple.
	 */
	ResetExprContext(econtext);

	/*
	 * MPP-4165: My fix for MPP-3300 was correct in that we avoided
	 * the *deadlock* but had very unexpected (and painful)
	 * performance characteristics: we basically de-pipeline and
	 * de-parallelize execution of any query which has motion below
	 * us.
	 *
	 * So now prefetch_inner is set (see createplan.c) if we have *any* motion
	 * below us. If we don't have any motion, it doesn't matter.
	 */
	if (node->prefetch_inner)
	{
		innerTupleSlot = ExecProcNode(innerPlan);
		Gpmon_M_Incr(GpmonPktFromNLJState(node), GPMON_NLJ_INNERTUPLE);
		Gpmon_M_Incr(GpmonPktFromNLJState(node), GPMON_QEXEC_M_ROWSIN); 

		node->reset_inner = true;
		econtext->ecxt_innertuple = innerTupleSlot;

		if (TupIsNull(innerTupleSlot))
		{
			/*
			 * Finished one complete scan of the inner side. Mark it here
			 * so that we don't keep checking for inner nulls at subsequent
			 * iterations.
			 */
			node->nl_innerSideScanned = true;
            /* CDB: Quit if empty inner implies no outer rows can match. */
			/* See MPP-1146 and MPP-1694 */
			if (node->nl_QuitIfEmptyInner)
            {
                ExecSquelchNode(outerPlan);
                return NULL;
            }
		}

		if ((node->js.jointype == JOIN_LASJ_NOTIN) &&
				(!node->nl_innerSideScanned) &&
				(node->nl_InnerJoinKeys && isJoinExprNull(node->nl_InnerJoinKeys, econtext)))
		{
			/*
			 * If LASJ_NOTIN and a null was found on the inner side, then clean out.
			 * We'll read no more from either inner or outer subtree. To keep our
			 * sibling QEs from being starved, tell source QEs not to
			 * clog up the pipeline with our never-to-be-consumed
			 * data.
			 */
			ENL1_printf("Found NULL tuple on the inner side, clean out");
			ExecSquelchNode(outerPlan);
			ExecSquelchNode(innerPlan);
			return NULL;
		}

		ExecReScan(innerPlan, econtext);
		ResetExprContext(econtext);

		node->nl_innerSquelchNeeded = false; /* no need to squelch inner since it was completely prefetched */
		node->prefetch_inner = false;
		node->reset_inner = false;
	}

	/*
	 * Ok, everything is setup for the join so now loop until we return a
	 * qualifying join tuple.
	 */
	ENL1_printf("entering main loop");

	for (;;)
	{
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nl_NeedNewOuter)
		{
            ENL1_printf("getting new outer tuple");
            outerTupleSlot = ExecProcNode(outerPlan);
            Gpmon_M_Incr(GpmonPktFromNLJState(node), GPMON_NLJ_OUTERTUPLE);
            Gpmon_M_Incr(GpmonPktFromNLJState(node), GPMON_QEXEC_M_ROWSIN); 

			/*
			 * if there are no more outer tuples, then the join is complete..
			 */
			if (TupIsNull(outerTupleSlot))
			{
				ENL1_printf("no outer tuple, ending join");

				/*
				 * CDB: If outer tuple stream was empty, notify inner
				 * subplan that we won't fetch its results, so QEs in
				 * lower gangs won't keep trying to send to us.  Else
				 * we have reached inner end-of-data at least once and
				 * squelch is not needed.
				 */
				if (node->nl_innerSquelchNeeded)
				{
					ExecSquelchNode(innerPlan);
				}

				/*
				 * The memory used by child nodes might not be freed because
				 * they are not eager free safe. However, when the nestloop is done,
				 * we can free the memory used by the child nodes.
				 */
				if (!node->js.ps.delayEagerFree)
				{
					ExecEagerFreeChildNodes((PlanState *)node, false);
				}

				return NULL;
			}

			ENL1_printf("saving new outer tuple information");
			node->js.ps.ps_OuterTupleSlot = outerTupleSlot;
			econtext->ecxt_outertuple = outerTupleSlot;
			node->nl_NeedNewOuter = false;
			node->nl_MatchedOuter = false;

			/*
			 * now rescan the inner plan
			 */
			ENL1_printf("rescanning inner plan");

			/*
			 * The scan key of the inner plan might depend on the current
			 * outer tuple (e.g. in index scans), that's why we pass our expr
			 * context.
			 */
			if ( node->require_inner_reset || node->reset_inner )
			{
				ExecReScan(innerPlan, econtext);
				node->reset_inner = false;
			}
		}

		/*
		 * we have an outerTuple, try to get the next inner tuple.
		 */
		ENL1_printf("getting new inner tuple");

		innerTupleSlot = ExecProcNode(innerPlan);
		Gpmon_M_Incr(GpmonPktFromNLJState(node), GPMON_NLJ_INNERTUPLE);
          	CheckSendPlanStateGpmonPkt(&node->js.ps);

		node->reset_inner = true;
		econtext->ecxt_innertuple = innerTupleSlot;

		if (TupIsNull(innerTupleSlot))
		{
			ENL1_printf("no inner tuple, need new outer tuple");

			node->nl_NeedNewOuter = true;
			/*
			 * Finished one complete scan of the inner side. Mark it here
			 * so that we don't keep checking for inner nulls at subsequent
			 * iterations.
			 */
			node->nl_innerSideScanned = true;

			if (!node->nl_MatchedOuter &&
				(node->js.jointype == JOIN_LEFT || 
				 node->js.jointype == JOIN_LASJ ||
				 node->js.jointype == JOIN_LASJ_NOTIN))
			{
				/*
				 * We are doing an outer join and there were no join matches
				 * for this outer tuple.  Generate a fake join tuple with
				 * nulls for the inner tuple, and return it if it passes the
				 * non-join quals.
				 */
				econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

				ENL1_printf("testing qualification for outer-join tuple");

				if (ExecQual(otherqual, econtext, false))
				{
					/*
					 * qualification was satisfied so we project and return
					 * the slot containing the result tuple using
					 * ExecProject().
					 */
					ENL1_printf("qualification succeeded, projecting tuple");

					Gpmon_M_Incr_Rows_Out(GpmonPktFromNLJState(node)); 
                          	CheckSendPlanStateGpmonPkt(&node->js.ps);
					return ExecProject(node->js.ps.ps_ProjInfo, NULL);
				}
			}

            /* CDB: Quit if empty inner implies no outer rows can match. */
            if (node->nl_QuitIfEmptyInner)
            {
                ExecSquelchNode(outerPlan);
                return NULL;
            }

			/*
			 * Otherwise just return to top of loop for a new outer tuple.
			 */
			continue;
		}

        node->nl_QuitIfEmptyInner = false;  /*CDB*/

		if ((node->js.jointype == JOIN_LASJ_NOTIN) &&
				(!node->nl_innerSideScanned) &&
				(node->nl_InnerJoinKeys && isJoinExprNull(node->nl_InnerJoinKeys, econtext)))
		{
			/*
			 * If LASJ_NOTIN and a null was found on the inner side, then clean out.
			 * We'll read no more from either inner or outer subtree. To keep our
			 * sibling QEs from being starved, tell source QEs not to
			 * clog up the pipeline with our never-to-be-consumed
			 * data.
			 */
			ENL1_printf("Found NULL tuple on the inner side, clean out");
			ExecSquelchNode(outerPlan);
			ExecSquelchNode(innerPlan);
			return NULL;
		}

		/*
		 * at this point we have a new pair of inner and outer tuples so we
		 * test the inner and outer tuples to see if they satisfy the node's
		 * qualification.
		 *
		 * Only the joinquals determine MatchedOuter status, but all quals
		 * must pass to actually return the tuple.
		 */
		ENL1_printf("testing qualification");

		if (ExecQual(joinqual, econtext, node->nl_qualResultForNull))
		{
			node->nl_MatchedOuter = true;

			/* In an antijoin, we never return a matched tuple */
			if (node->js.jointype == JOIN_LASJ || node->js.jointype == JOIN_LASJ_NOTIN)
			{
				node->nl_NeedNewOuter = true;
				continue;		/* return to top of loop */
			}

			if (otherqual == NIL || ExecQual(otherqual, econtext, false))
			{
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				ENL1_printf("qualification succeeded, projecting tuple");

				Gpmon_M_Incr_Rows_Out(GpmonPktFromNLJState(node));
                     	CheckSendPlanStateGpmonPkt(&node->js.ps);
				return ExecProject(node->js.ps.ps_ProjInfo, NULL);
			}

			/* If we didn't return a tuple, may need to set NeedNewOuter */
			if (node->js.jointype == JOIN_IN)
				node->nl_NeedNewOuter = true;
		}

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);

		ENL1_printf("qualification failed, looping");
	}
}

/* ----------------------------------------------------------------
 *		ExecInitNestLoop
 * ----------------------------------------------------------------
 */
NestLoopState *
ExecInitNestLoop(NestLoop *node, EState *estate, int eflags)
{
	NestLoopState *nlstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	NL1_printf("ExecInitNestLoop: %s\n",
			   "initializing node");

	/*
	 * create state structure
	 */
	nlstate = makeNode(NestLoopState);
	nlstate->js.ps.plan = (Plan *) node;
	nlstate->js.ps.state = estate;

	nlstate->shared_outer = node->shared_outer;

	nlstate->prefetch_inner = node->join.prefetch_inner;
	
	/*CDB-OLAP*/
	nlstate->reset_inner = false;
	nlstate->require_inner_reset = !node->singleton_outer;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &nlstate->js.ps);

	/*
	 * If eflag contains EXEC_FLAG_REWIND or EXEC_FLAG_BACKWARD or EXEC_FLAG_MARK,
	 * then this node is not eager free safe.
	 */
	nlstate->js.ps.delayEagerFree =
		((eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) != 0);

	/*
	 * initialize child expressions
	 */
	nlstate->js.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->join.plan.targetlist,
					 (PlanState *) nlstate);
	nlstate->js.ps.qual = (List *)
		ExecInitExpr((Expr *) node->join.plan.qual,
					 (PlanState *) nlstate);
	nlstate->js.jointype = node->join.jointype;
	nlstate->js.joinqual = (List *)
		ExecInitExpr((Expr *) node->join.joinqual,
					 (PlanState *) nlstate);

	/*
	 * initialize child nodes
	 *
	 * Tell the inner child that cheap rescans would be good.  (This is
	 * unnecessary if we are doing nestloop with inner indexscan, because the
	 * rescan will always be with a fresh parameter --- but since
	 * nodeIndexscan doesn't actually care about REWIND, there's no point in
	 * dealing with that refinement.)
	 */
	/*
	 * XXX ftian: Because share input need to make the whole thing into a tree,
	 * we can put the underlying share only under one shareinputscan.  During execution,
	 * we need the shareinput node that has underlying subtree be inited/executed first.
	 * This means, 
	 * 	1. Init and first ExecProcNode call must be in the same order
	 *	2. Init order above is the same as the tree walking order in cdbmutate.c
	 * For nest loop join, it is more strange than others.  Depends on prefetch_inner,
	 * the execution order may change.  Handle this correctly here.
	 * 
	 * Until we find a better way to handle the dependency of ShareInputScan on 
	 * execution order, this is pretty much what we have to deal with.
	 */
	if (nlstate->prefetch_inner)
	{
		innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate,
				eflags | EXEC_FLAG_REWIND);
		if (!node->shared_outer)
			outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
	}
	else
	{
		if (!node->shared_outer)
			outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
		innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate,
				eflags | EXEC_FLAG_REWIND);
	}

#define NESTLOOP_NSLOTS 2

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &nlstate->js.ps);

	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_IN:
			break;
		case JOIN_LEFT:
		case JOIN_LASJ:
		case JOIN_LASJ_NOTIN:
			nlstate->nl_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(innerPlanState(nlstate)));
			break;
		default:
			insist_log(false, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&nlstate->js.ps);
	ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

	/*
	 * finally, wipe the current outer tuple clean.
	 */
	nlstate->js.ps.ps_OuterTupleSlot = NULL;
	nlstate->nl_NeedNewOuter = true;
	nlstate->nl_MatchedOuter = false;
	nlstate->nl_innerSquelchNeeded = true;		/*CDB*/

    /* CDB: Set flag if empty inner implies empty join result. */
    nlstate->nl_QuitIfEmptyInner = false;
    if (node->outernotreferencedbyinner &&
        (node->join.jointype == JOIN_INNER ||
		 node->join.jointype == JOIN_RIGHT ||
		 node->join.jointype == JOIN_IN))
        nlstate->nl_QuitIfEmptyInner = true;

    if (node->join.jointype == JOIN_LASJ_NOTIN)
    {
    	splitJoinQualExpr(nlstate);
    	/*
    	 * For LASJ_NOTIN, when we evaluate the join condition, we want to
    	 * return true when one of the conditions is NULL, so we exclude
    	 * that tuple from the output.
    	 */
		nlstate->nl_qualResultForNull = true;
    }
    else
    {
        nlstate->nl_qualResultForNull = false;
    }

	NL1_printf("ExecInitNestLoop: %s\n",
			   "node initialized");

	initGpmonPktForNestLoop((Plan *)node, &nlstate->js.ps.gpmon_pkt, estate);
	
	return nlstate;
}

int
ExecCountSlotsNestLoop(NestLoop *node)
{
	int count;

	count = ExecCountSlotsNode(outerPlan(node));
	count += ExecCountSlotsNode(innerPlan(node));
	count += NESTLOOP_NSLOTS;

	return count;
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void
ExecEndNestLoop(NestLoopState *node)
{
	NL1_printf("ExecEndNestLoop: %s\n",
			   "ending node processing");

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

	/*
	 * close down subplans.
	 */
	if (!node->shared_outer)
		ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));

	NL1_printf("ExecEndNestLoop: %s\n",
			   "node processing ended");

	EndPlanStateGpmonPkt(&node->js.ps);
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void
ExecReScanNestLoop(NestLoopState *node, ExprContext *exprCtxt)
{
	PlanState  *outerPlan = outerPlanState(node);

	/*
	 * If outerPlan->chgParam is not null then plan will be automatically
	 * re-scanned by first ExecProcNode. innerPlan is re-scanned for each new
	 * outer tuple and MUST NOT be re-scanned from here or you'll get troubles
	 * from inner index scans when outer Vars are used as run-time keys...
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan, exprCtxt);

	/* let outerPlan to free its result tuple ... */
	node->js.ps.ps_OuterTupleSlot = NULL;
	node->nl_NeedNewOuter = true;
	node->nl_MatchedOuter = false;
	node->nl_innerSideScanned = false;
	/* CDB: We intentionally leave node->nl_innerSquelchNeeded unchanged on ReScan */
}

void
initGpmonPktForNestLoop(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, NestLoop));
	
	{
		PerfmonNodeType type = PMNT_Invalid;

		switch(((NestLoop *)planNode)->join.jointype)
		{
			case JOIN_INNER:
				type = PMNT_NestedLoop;
			break;
			case JOIN_LEFT:
				type = PMNT_NestedLoopLeftJoin;
			break;
			case JOIN_LASJ:
			case JOIN_LASJ_NOTIN:
				type = PMNT_NestedLoopLeftAntiSemiJoin;
			break;
			case JOIN_FULL:
				type = PMNT_NestedLoopFullJoin;
			break;
			case JOIN_RIGHT:
				type = PMNT_NestedLoopRightJoin;
			break;
			case JOIN_IN:
				type = PMNT_NestedLoopExistsJoin;
			break;
			case JOIN_REVERSE_IN:
				type = PMNT_NestedLoopReverseInJoin;
			break;
			case JOIN_UNIQUE_OUTER:
				type = PMNT_NestedLoopUniqueOuterJoin;
			break;
			case JOIN_UNIQUE_INNER:
				type = PMNT_NestedLoopUniqueInnerJoin;
			break;
		}

		Assert(type != PMNT_Invalid);
		Assert(GPMON_NLJ_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, type,
							 (int64)planNode->plan_rows,
							 NULL);
	}
}

/* ----------------------------------------------------------------
 * splitJoinQualExpr
 *
 * Deconstruct the join clauses into outer and inner argument values, so
 * that we can evaluate those subexpressions separately. Note: for constant
 * expression we don't need to split (MPP-21294). However, if constant expressions
 * have peer splittable expressions we *do* split those.
 *
 * This is used for NOTIN joins, as we need to look for NULLs on both
 * inner and outer side.
 * ----------------------------------------------------------------
 */
static void
splitJoinQualExpr(NestLoopState *nlstate)
{
	List *lclauses = NIL;
	List *rclauses = NIL;
	ListCell *lc = NULL;

	foreach(lc, nlstate->js.joinqual)
	{
		GenericExprState *exprstate = (GenericExprState *) lfirst(lc);
		switch (exprstate->xprstate.type)
		{
		case T_FuncExprState:
			extractFuncExprArgs((FuncExprState *) exprstate, &lclauses, &rclauses);
			break;
		case T_BoolExprState:
		{
			BoolExprState *bstate = (BoolExprState *) exprstate;
			ListCell *argslc = NULL;
			foreach(argslc,bstate->args)
			{
				FuncExprState *fstate = (FuncExprState *) lfirst(argslc);
				Assert(IsA(fstate, FuncExprState));
				extractFuncExprArgs(fstate, &lclauses, &rclauses);
			}
			break;
		}
		case T_ExprState:
			/* For constant expression we don't need to split */
			if (exprstate->xprstate.expr->type == T_Const)
			{
				/*
				 * Constant expressions do not need to be splitted into left and
				 * right as they don't need to be considered for NULL value special
				 * cases
				 */
				continue;
			}

			insist_log(false, "unexpected expression type in NestLoopJoin qual");

			break; /* Unreachable */
		default:
			insist_log(false, "unexpected expression type in NestLoopJoin qual");
		}
	}
	Assert(NIL == nlstate->nl_InnerJoinKeys && NIL == nlstate->nl_OuterJoinKeys);
	nlstate->nl_InnerJoinKeys = rclauses;
	nlstate->nl_OuterJoinKeys = lclauses;
}


/* ----------------------------------------------------------------
 * extractFuncExprArgs
 *
 * Extract the arguments of a FuncExpr and append them into two
 * given lists:
 *   - lclauses for the left side of the expression,
 *   - rclauses for the right side
 * ----------------------------------------------------------------
 */
static void
extractFuncExprArgs(FuncExprState *fstate, List **lclauses, List **rclauses)
{
	*lclauses = lappend(*lclauses, linitial(fstate->args));
	*rclauses = lappend(*rclauses, lsecond(fstate->args));
}
