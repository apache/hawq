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
 * nodeLimit.c
 *	  Routines to handle limiting of query results where appropriate
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/nodeLimit.c,v 1.27.2.1 2006/12/03 21:40:13 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecLimit		- extract a limited range of tuples
 *		ExecInitLimit	- initialize node and subnodes..
 *		ExecEndLimit	- shutdown node and subnodes
 */

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "executor/nodeLimit.h"

static void recompute_limits(LimitState *node);


/* ----------------------------------------------------------------
 *		ExecLimit
 *
 *		This is a very simple node which just performs LIMIT/OFFSET
 *		filtering on the stream of tuples returned by a subplan.
 * ----------------------------------------------------------------
 */
TupleTableSlot *				/* return: a tuple or NULL */
ExecLimit(LimitState *node)
{
	ScanDirection direction;
	TupleTableSlot *slot;
	PlanState  *outerPlan;

	/*
	 * get information from the node
	 */
	direction = node->ps.state->es_direction;
	outerPlan = outerPlanState(node);

	/*
	 * The main logic is a simple state machine.
	 */
	switch (node->lstate)
	{
		case LIMIT_INITIAL:

			/*
			 * If backwards scan, just return NULL without changing state.
			 */
			if (!ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * First call for this scan, so compute limit/offset. (We can't do
			 * this any earlier, because parameters from upper nodes may not
			 * be set until now.)  This also sets position = 0.
			 */
			recompute_limits(node);

			/*
			 * Check for empty window; if so, treat like empty subplan.
			 */
			if (node->count <= 0 && !node->noCount)
			{
				node->lstate = LIMIT_EMPTY;

				/*
				 * CDB: We'll read no more from outer subtree. To keep our
				 * sibling QEs from being starved, tell source QEs not to clog
				 * up the pipeline with our never-to-be-consumed data.
				 */
				ExecSquelchNode(outerPlan);

				return NULL;
			}

			/*
			 * Fetch rows from subplan until we reach position > offset.
			 */
			for (;;)
			{
				slot = ExecProcNode(outerPlan);
				if (TupIsNull(slot))
				{
					/*
					 * The subplan returns too few tuples for us to produce
					 * any output at all.
					 */
					node->lstate = LIMIT_EMPTY;
					return NULL;
				}
				node->subSlot = slot;
				if (++node->position > node->offset)
					break;
			}

			/*
			 * Okay, we have the first tuple of the window.
			 */
			node->lstate = LIMIT_INWINDOW;
			break;

		case LIMIT_EMPTY:

			/*
			 * The subplan is known to return no tuples (or not more than
			 * OFFSET tuples, in general).	So we return no tuples.
			 */
			ExecSquelchNode(outerPlan); /* CDB */
			return NULL;

		case LIMIT_INWINDOW:
			if (ScanDirectionIsForward(direction))
			{
				/*
				 * Forwards scan, so check for stepping off end of window. If
				 * we are at the end of the window, return NULL without
				 * advancing the subplan or the position variable; but change
				 * the state machine state to record having done so.
				 */
				if (!node->noCount &&
					node->position >= node->offset + node->count)
				{
					node->lstate = LIMIT_WINDOWEND;
					ExecSquelchNode(outerPlan); /* CDB */
					return NULL;
				}

				/*
				 * Get next tuple from subplan, if any.
				 */
				slot = ExecProcNode(outerPlan);
				if (TupIsNull(slot))
				{
					node->lstate = LIMIT_SUBPLANEOF;
					return NULL;
				}
				node->subSlot = slot;
				node->position++;
			}
			else
			{
				/*
				 * Backwards scan, so check for stepping off start of window.
				 * As above, change only state-machine status if so.
				 */
				if (node->position <= node->offset + 1)
				{
					node->lstate = LIMIT_WINDOWSTART;
					ExecSquelchNode(outerPlan); /* CDB */
					return NULL;
				}

				/*
				 * Get previous tuple from subplan; there should be one!
				 */
				slot = ExecProcNode(outerPlan);
				insist_log(!TupIsNull(slot), "LIMIT subplan failed to run backwards");
				node->subSlot = slot;
				node->position--;
			}
			break;

		case LIMIT_SUBPLANEOF:
			if (ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Backing up from subplan EOF, so re-fetch previous tuple; there
			 * should be one!  Note previous tuple must be in window.
			 */
			slot = ExecProcNode(outerPlan);
			insist_log(!TupIsNull(slot), "LIMIT subplan failed to run backwards");
			node->subSlot = slot;
			node->lstate = LIMIT_INWINDOW;
			/* position does not change 'cause we didn't advance it before */
			break;

		case LIMIT_WINDOWEND:
			if (ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Backing up from window end: simply re-return the last tuple
			 * fetched from the subplan.
			 */
			slot = node->subSlot;
			node->lstate = LIMIT_INWINDOW;
			/* position does not change 'cause we didn't advance it before */
			break;

		case LIMIT_WINDOWSTART:
			if (!ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Advancing after having backed off window start: simply
			 * re-return the last tuple fetched from the subplan.
			 */
			slot = node->subSlot;
			node->lstate = LIMIT_INWINDOW;
			/* position does not change 'cause we didn't change it before */
			break;

		default:
			insist_log(false, "impossible LIMIT state: %d",
				 (int) node->lstate);
			slot = NULL;		/* keep compiler quiet */
			break;
	}

	/* Return the current tuple */
	Assert(!TupIsNull(slot));

        if (!TupIsNull(slot))
        {
            Gpmon_M_Incr_Rows_Out(GpmonPktFromLimitState(node));
            CheckSendPlanStateGpmonPkt(&node->ps); 
        }
	return slot;
}

/*
 * Evaluate the limit/offset expressions --- done at start of each scan.
 *
 * This is also a handy place to reset the current-position state info.
 */
static void
recompute_limits(LimitState *node)
{
	ExprContext *econtext = node->ps.ps_ExprContext;
	Datum		val;
	bool		isNull;

	if (node->limitOffset)
	{
		val = ExecEvalExprSwitchContext(node->limitOffset,
										econtext,
										&isNull,
										NULL);
		/* Interpret NULL offset as no offset */
		if (isNull)
			node->offset = 0;
		else
		{
			node->offset = DatumGetInt64(val);
			if (node->offset < 0)
				node->offset = 0;
		}
	}
	else
	{
		/* No OFFSET supplied */
		node->offset = 0;
	}

	if (node->limitCount)
	{
		val = ExecEvalExprSwitchContext(node->limitCount,
										econtext,
										&isNull,
										NULL);
		/* Interpret NULL count as no count (LIMIT ALL) */
		if (isNull)
		{
			node->count = 0;
			node->noCount = true;
		}
		else
		{
			node->count = DatumGetInt64(val);
			if (node->count < 0)
				node->count = 0;
			node->noCount = false;
		}
	}
	else
	{
		/* No COUNT supplied */
		node->count = 0;
		node->noCount = true;
	}

	/* Reset position to start-of-scan */
	node->position = 0;
	node->subSlot = NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitLimit
 *
 *		This initializes the limit node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
LimitState *
ExecInitLimit(Limit *node, EState *estate, int eflags)
{
	LimitState *limitstate;
	Plan	   *outerPlan;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * create state structure
	 */
	limitstate = makeNode(LimitState);
	limitstate->ps.plan = (Plan *) node;
	limitstate->ps.state = estate;

	limitstate->lstate = LIMIT_INITIAL;

	/*
	 * Miscellaneous initialization
	 *
	 * Limit nodes never call ExecQual or ExecProject, but they need an
	 * exprcontext anyway to evaluate the limit/offset parameters in.
	 */
	ExecAssignExprContext(estate, &limitstate->ps);

	/*
	 * initialize child expressions
	 */
	limitstate->limitOffset = ExecInitExpr((Expr *) node->limitOffset,
										   (PlanState *) limitstate);
	limitstate->limitCount = ExecInitExpr((Expr *) node->limitCount,
										  (PlanState *) limitstate);

#define LIMIT_NSLOTS 1

	/*
	 * Tuple table initialization (XXX not actually used...)
	 */
	ExecInitResultTupleSlot(estate, &limitstate->ps);

	/*
	 * then initialize outer plan
	 */
	outerPlan = outerPlan(node);
	outerPlanState(limitstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * limit nodes do no projections, so initialize projection info for this
	 * node appropriately
	 */
	ExecAssignResultTypeFromTL(&limitstate->ps);
	limitstate->ps.ps_ProjInfo = NULL;

	initGpmonPktForLimit((Plan *)node, &limitstate->ps.gpmon_pkt, estate);
	
	return limitstate;
}

int
ExecCountSlotsLimit(Limit *node)
{
	return ExecCountSlotsNode(outerPlan(node)) +
		ExecCountSlotsNode(innerPlan(node)) +
		LIMIT_NSLOTS;
}

/* ----------------------------------------------------------------
 *		ExecEndLimit
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void
ExecEndLimit(LimitState *node)
{
	ExecFreeExprContext(&node->ps);
	ExecEndNode(outerPlanState(node));

	EndPlanStateGpmonPkt(&node->ps);
}


void
ExecReScanLimit(LimitState *node, ExprContext *exprCtxt)
{
	/* resetting lstate will force offset/limit recalculation */
	node->lstate = LIMIT_INITIAL;

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (((PlanState *) node)->lefttree->chgParam == NULL)
		ExecReScan(((PlanState *) node)->lefttree, exprCtxt);
}

void
initGpmonPktForLimit(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, Limit));

	{
		Assert(GPMON_LIMIT_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_Limit, (int64)0, NULL);
	}
}
