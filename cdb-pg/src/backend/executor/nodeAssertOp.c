/*-------------------------------------------------------------------------
 *
 * nodeAssertOp.c
 *	  Implementation of nodeAssertOp.
 *
 * Copyright (c) 2012, EMC Corp.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "cdb/cdbpartition.h"
#include "commands/tablecmds.h"
#include "executor/nodeAssertOp.h"
#include "executor/instrument.h"

/* Number of slots and memory used by node.*/
#define ASSERTOP_NSLOTS 1
#define ASSERTOP_MEM 	1

/*
 * Estimated Memory Usage of AssertOp Node.
 * */
void
ExecAssertOpExplainEnd(PlanState *planstate, struct StringInfoData *buf)
{
	planstate->instrument->execmemused += ASSERTOP_MEM;
}

/*
 * Evaluate Constraints (in node->ps.qual) and project output TupleTableSlot.
 * */
TupleTableSlot*
ExecAssertOp(AssertOpState *node)
{

	PlanState *outerNode = outerPlanState(node);
	AssertOp *plannode = (AssertOp *) node->ps.plan;
	ProjectionInfo *projInfo = node->ps.ps_ProjInfo;

	TupleTableSlot *slot = ExecProcNode(outerNode);

	if (TupIsNull(slot))
	{
		return NULL;
	}

	ExprContext *econtext = node->ps.ps_ExprContext;

    ResetExprContext(econtext);

	List* predicates = node->ps.qual;

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_outertuple = slot;

	if (!ExecQual(predicates, econtext, true))
	{
		ereport(ERROR,
				(errcode(plannode->errcode),
				 errmsg("%s",plannode->errmessage),
				 errOmitLocation(true)));

	}

	ResetExprContext(econtext);

	return ExecProject(projInfo, NULL);
}

/**
 * Init AssertOp, which sets the ProjectInfo and
 * the Constraints to evaluate.
 * */
AssertOpState*
ExecInitAssertOp(AssertOp *node, EState *estate, int eflags)
{

	/* Check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND)));

	AssertOpState *assertOpState = makeNode(AssertOpState);
	assertOpState->ps.plan = (Plan *)node;
	assertOpState->ps.state = estate;
	PlanState *planState = &assertOpState->ps;


	ExecInitResultTupleSlot(estate, &assertOpState->ps);

	/* Create expression evaluation context */
	ExecAssignExprContext(estate, planState);

	assertOpState->ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) assertOpState);

	assertOpState->ps.qual = (List *)
			ExecInitExpr((Expr *) node->plan.qual,
						 (PlanState *) assertOpState);

	/*
	 * Initialize outer plan
	 */
	Plan *outerPlan = outerPlan(node);
	outerPlanState(assertOpState) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * Initialize projection info for this
	 * node appropriately
	 */
	ExecAssignResultTypeFromTL(&assertOpState->ps);
	TupleDesc tupDesc = ExecTypeFromTL(node->plan.targetlist, false);

	ExecAssignProjectionInfo(planState, tupDesc);

	if (estate->es_instrument)
	{
	        assertOpState->ps.cdbexplainbuf = makeStringInfo();

	        /* Request a callback at end of query. */
	        assertOpState->ps.cdbexplainfun = ExecAssertOpExplainEnd;
	}

	initGpmonPktForAssertOp((Plan *)node, &assertOpState->ps.gpmon_pkt, estate);

	return assertOpState;
}

/* Release Resources Requested by AssertOp node. */
void
ExecEndAssertOp(AssertOpState *node)
{
	ExecFreeExprContext(&node->ps);
	ExecEndNode(outerPlanState(node));
	EndPlanStateGpmonPkt(&node->ps);
}

/* Return number of TupleTableSlots used by AssertOp node.*/
int
ExecCountSlotsAssertOp(AssertOp *node)
{
	return ExecCountSlotsNode(outerPlan(node)) + ASSERTOP_NSLOTS;
}

/* Tracing execution for GP Monitor. */
void
initGpmonPktForAssertOp(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, AssertOp));

	PerfmonNodeType type = PMNT_AssertOp;

	InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, type,
								 (int64)planNode->plan_rows,
								 NULL);
}

