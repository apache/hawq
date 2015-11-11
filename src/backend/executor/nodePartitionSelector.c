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

/*-------------------------------------------------------------------------
 *
 * nodePartitionSelector.c
 *	  implement the execution of PartitionSelector for selecting partition
 *	  Oids based on a given set of predicates. It works for both constant
 *	  partition elimination and join partition elimination
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "cdb/cdbpartition.h"
#include "cdb/partitionselection.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/nodePartitionSelector.h"
#include "utils/memutils.h"

static void
partition_propagation(List *partOids, List *scanIds, int32 selectorId);

/* PartitionSelector Slots */
#define PARTITIONSELECTOR_NSLOTS 1

/* Return number of TupleTableSlots used by nodePartitionSelector.*/
int
ExecCountSlotsPartitionSelector(PartitionSelector *node)
{
	if (NULL != outerPlan(node))
	{
		return ExecCountSlotsNode(outerPlan(node)) + PARTITIONSELECTOR_NSLOTS;
	}
	return PARTITIONSELECTOR_NSLOTS;
}

void
initGpmonPktForPartitionSelector(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, PartitionSelector));

	InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_PartitionSelector,
							(int64)planNode->plan_rows,
							NULL);
}

/* ----------------------------------------------------------------
 *		ExecInitPartitionSelector
 *
 *		Create the run-time state information for PartitionSelector node
 *		produced by Orca and initializes outer child if exists.
 *
 * ----------------------------------------------------------------
 */
PartitionSelectorState *
ExecInitPartitionSelector(PartitionSelector *node, EState *estate, int eflags)
{
	/* check for unsupported flags */
	Assert (!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)));

	PartitionSelectorState *psstate = initPartitionSelection(true /*isRunTime*/, node, estate);

	/* tuple table initialization */
	ExecInitResultTupleSlot(estate, &psstate->ps);
	ExecAssignResultTypeFromTL(&psstate->ps);
	ExecAssignProjectionInfo(&psstate->ps, NULL);

	/* initialize child nodes */
	/* No inner plan for PartitionSelector */
	Assert(NULL == innerPlan(node));
	if (NULL != outerPlan(node))
	{
		outerPlanState(psstate) = ExecInitNode(outerPlan(node), estate, eflags);
	}

	initGpmonPktForPartitionSelector((Plan *)node, &psstate->ps.gpmon_pkt, estate);

	return psstate;
}

/* ----------------------------------------------------------------
 *		ExecPartitionSelector(node)
 *
 *		Compute and propagate partition table Oids that will be
 *		used by Dynamic table scan. There are two ways of
 *		executing PartitionSelector.
 *
 *		1. Constant partition elimination
 *		Plan structure:
 *			Sequence
 *				|--PartitionSelector
 *				|--DynamicTableScan
 *		In this case, PartitionSelector evaluates constant partition
 *		constraints to compute and propagate partition table Oids.
 *		It only need to be called once.
 *
 *		2. Join partition elimination
 *		Plan structure:
 *			...:
 *				|--DynamicTableScan
 *				|--...
 *					|--PartitionSelector
 *						|--...
 *		In this case, PartitionSelector is in the same slice as
 *		DynamicTableScan, DynamicIndexScan or DynamicBitmapHeapScan.
 *		It is executed for each tuple coming from its child node.
 *		It evaluates partition constraints with the input tuple and
 *		propagate matched partition table Oids.
 *
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecPartitionSelector(PartitionSelectorState *node)
{
	PartitionSelector *ps = (PartitionSelector *) node->ps.plan;
	if (ps->staticSelection)
	{
		/* propagate the part oids obtained via static partition selection */
		partition_propagation(ps->staticPartOids, ps->staticScanIds, ps->selectorId);
		*node->acceptedLeafPart = NULL;
		return NULL;
	}

	/* Retrieve PartitionNode and access method from root table.
	 * We cannot do it during node initialization as
	 * DynamicTableScanInfo is not properly initialized yet.
	 */
	if (NULL == node->rootPartitionNode)
	{
		Assert(NULL != dynamicTableScanInfo);
		getPartitionNodeAndAccessMethod
									(
									ps->relid,
									dynamicTableScanInfo->partsMetadata,
									dynamicTableScanInfo->memoryContext,
									&node->rootPartitionNode,
									&node->accessMethods
									);
	}

	TupleTableSlot *inputSlot = NULL;
	if (NULL != outerPlanState(node))
	{
		/* Join partition elimination */
		/* get tuple from outer children */
		PlanState *outerPlan = outerPlanState(node);
		Assert(outerPlan);
		inputSlot = ExecProcNode(outerPlan);

		if (TupIsNull(inputSlot))
		{
			/* no more tuples from outerPlan */
			return NULL;
		}
	}

	/* partition elimination with the given input tuple */
	SelectedParts *selparts = processLevel(node, 0 /* level */, inputSlot);

	/* partition propagation */
	if (NULL != ps->propagationExpression)
	{
		partition_propagation(selparts->partOids, selparts->scanIds, ps->selectorId);
	}

	list_free(selparts->partOids);
	list_free(selparts->scanIds);
	pfree(selparts);

	TupleTableSlot *candidateOutputSlot = NULL;
	if (NULL != inputSlot)
	{
		ExprContext *econtext = node->ps.ps_ExprContext;
		ResetExprContext(econtext);
		node->ps.ps_OuterTupleSlot = inputSlot;
		econtext->ecxt_outertuple = inputSlot;
		econtext->ecxt_scantuple = inputSlot;

		ExprDoneCond isDone = ExprSingleResult;
		candidateOutputSlot = ExecProject(node->ps.ps_ProjInfo, &isDone);
		Assert (ExprSingleResult == isDone);
	}

	/* reset acceptedLeafPart */
	if (NULL != *node->acceptedLeafPart)
	{
		pfree(*node->acceptedLeafPart);
		*node->acceptedLeafPart = NULL;
	}
	return candidateOutputSlot;
}

/* ----------------------------------------------------------------
 *		ExecReScanPartitionSelector(node)
 *
 *		ExecReScan routine for PartitionSelector.
 * ----------------------------------------------------------------
 */
void
ExecReScanPartitionSelector(PartitionSelectorState *node, ExprContext *exprCtxt)
{
	/* reset PartitionSelectorState */
	PartitionSelector *ps = (PartitionSelector *) node->ps.plan;
	
	Assert (NULL == *node->acceptedLeafPart);
	
	for(int iter = 0; iter < ps->nLevels; iter++)
	{
		node->levelPartConstraints[iter] = NULL;
	}

	/* free result tuple slot */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	/*
	 * If we are being passed an outer tuple, link it into the "regular"
	 * per-tuple econtext for possible qual eval.
	 */
	if (exprCtxt != NULL)
	{
		ExprContext *stdecontext = node->ps.ps_ExprContext;
		stdecontext->ecxt_outertuple = exprCtxt->ecxt_outertuple;
	}

	/* If the PartitionSelector is in the inner side of a nest loop join,
	 * it should be constant partition elimination and thus has no child node.*/
#if USE_ASSERT_CHECKING
	PlanState  *outerPlan = outerPlanState(node);
	Assert (NULL == outerPlan);
#endif

}

/* ----------------------------------------------------------------
 *		ExecEndPartitionSelector(node)
 *
 *		ExecEnd routine for PartitionSelector. Free resources
 *		and clear tuple.
 *
 * ----------------------------------------------------------------
 */
void
ExecEndPartitionSelector(PartitionSelectorState *node)
{
	ExecFreeExprContext(&node->ps);

	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	/* clean child node */
	if (NULL != outerPlanState(node))
	{
		ExecEndNode(outerPlanState(node));
	}

	EndPlanStateGpmonPkt(&node->ps);
}

/* ----------------------------------------------------------------
 *		partition_propagation
 *
 *		Propagate a list of leaf part Oids to the corresponding dynamic scans
 *
 * ----------------------------------------------------------------
 */
static void
partition_propagation(List *partOids, List *scanIds, int32 selectorId)
{
	Assert (list_length(partOids) == list_length(scanIds));

	ListCell *lcOid = NULL;
	ListCell *lcScanId = NULL;
	forboth (lcOid, partOids, lcScanId, scanIds)
	{
		Oid partOid = lfirst_oid(lcOid);
		int scanId = lfirst_int(lcScanId);

		InsertPidIntoDynamicTableScanInfo(scanId, partOid, selectorId);
	}
}

/* EOF */

