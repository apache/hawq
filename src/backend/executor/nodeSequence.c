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
/*
 * nodeSequence.c
 *   Routines to handle Sequence node.
 *
 * Sequence node contains a list of subplans, which will be processed in the 
 * order of left-to-right. Result tuples from the last subplan will be outputted
 * as the results of the Sequence node.
 *
 * Sequence does not make use of its left and right subtrees, and instead it
 * maintains a list of subplans explicitly.
 */

#include "postgres.h"

#include "executor/nodeSequence.h"
#include "executor/executor.h"
#include "miscadmin.h"

#define SEQUENCE_NSLOTS 1

SequenceState *
ExecInitSequence(Sequence *node, EState *estate, int eflags)
{
	/* Check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/* Sequence should not contain 'qual'. */
	Assert(node->plan.qual == NIL);

	SequenceState *sequenceState = makeNode(SequenceState);
	
	sequenceState->ps.plan = (Plan *)node;
	sequenceState->ps.state = estate;

	int numSubplans = list_length(node->subplans);
	Assert(numSubplans >= 1);
	sequenceState->subplans = (PlanState **)palloc0(numSubplans * sizeof(PlanState *));
	sequenceState->numSubplans = numSubplans;
	
	/* Initialize subplans */
	ListCell *lc;
	int no = 0;
	foreach (lc, node->subplans)
	{
		Plan *subplan = (Plan *)lfirst(lc);
		Assert(subplan != NULL);
		Assert(no < numSubplans);
		
		sequenceState->subplans[no] = ExecInitNode(subplan, estate, eflags);
		no++;
	}

	sequenceState->initState = true;
	
	/* Sequence does not need projection. */
	sequenceState->ps.ps_ProjInfo = NULL;

	/*
	 * tuple table initialization
	 *
	 * MPP-20528: Even though we don't actually use the result slot to
	 * return tuples, we need it to have the correct tuple descriptor
	 * for this node.
	 */
	ExecInitResultTupleSlot(estate, &sequenceState->ps);
	ExecAssignResultTypeFromTL(&sequenceState->ps);

	initGpmonPktForSequence((Plan *)node, &sequenceState->ps.gpmon_pkt, estate);

	return sequenceState;
}

int
ExecCountSlotsSequence(Sequence *node)
{
	Assert(list_length(node->subplans) > 0);

	int numSlots = 0;
	ListCell *lc = NULL;
	foreach(lc, node->subplans)
	{
		numSlots += ExecCountSlotsNode((Plan *)lfirst(lc));
	}

	return numSlots + SEQUENCE_NSLOTS;
}

/*
 * completeSubplan
 *   Execute a given subplan to completion.
 *
 * The outputs from the given subplan will be discarded.
 */
static void
completeSubplan(PlanState *subplan)
{
	while (ExecProcNode(subplan) != NULL)
	{
	}
}

TupleTableSlot *
ExecSequence(SequenceState *node)
{
	/*
	 * If no subplan has been executed yet, execute them here, except for
	 * the last subplan.
	 */
	if (node->initState)
	{
		for(int no = 0; no < node->numSubplans - 1; no++)
		{
			completeSubplan(node->subplans[no]);

			CHECK_FOR_INTERRUPTS();
		}

		node->initState = false;
	}

	Assert(!node->initState);
	
	PlanState *lastPlan = node->subplans[node->numSubplans - 1];
	TupleTableSlot *result = ExecProcNode(lastPlan);
	
	if (!TupIsNull(result))
	{
		Gpmon_M_Incr_Rows_Out(GpmonPktFromSequenceState(node));
		CheckSendPlanStateGpmonPkt(&node->ps);
	}

	/*
	 * Return the tuple as returned by the subplan as-is. We do
	 * NOT make use of the result slot that was set up in
	 * ExecInitSequence, because there's no reason to.
	 */
	return result;
}

void
ExecEndSequence(SequenceState *node)
{
	/* shutdown subplans */
	for(int no = 0; no < node->numSubplans; no++)
	{
		Assert(node->subplans[no] != NULL);
		ExecEndNode(node->subplans[no]);
	}

	EndPlanStateGpmonPkt(&node->ps);
}

void
ExecReScanSequence(SequenceState *node, ExprContext *exprCtxt)
{
	for (int i = 0; i < node->numSubplans; i++)
	{
		PlanState  *subnode = node->subplans[i];

		/*
		 * ExecReScan doesn't know about my subplans, so I have to do
		 * changed-parameter signaling myself.
		 */
		if (node->ps.chgParam != NULL)
		{
			UpdateChangedParamSet(subnode, node->ps.chgParam);
		}

		/*
		 * Always rescan the inputs immediately, to ensure we can pass down
		 * any outer tuple that might be used in index quals.
		 */
		ExecReScan(subnode, exprCtxt);
	}

	node->initState = true;
}

void
initGpmonPktForSequence(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, Sequence));

	Assert(GPMON_SEQUENCE_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
	InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_Sequence,
						 (int64)planNode->plan_rows,
						 NULL);
}
