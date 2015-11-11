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
 * nodeDML.c
 *	  Implementation of nodeDML.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "cdb/cdbpartition.h"
#include "commands/tablecmds.h"
#include "executor/execDML.h"
#include "executor/instrument.h"
#include "executor/nodeDML.h"
#include "utils/memutils.h"

/*DML Slots and default memory */
#define DML_NSLOTS 2
#define DML_MEM 1

/*
 * Estimated Memory Usage of DML Node.
 * */
void
ExecDMLExplainEnd(PlanState *planstate, struct StringInfoData *buf)
{
	planstate->instrument->execmemused += DML_MEM;
}

/*
 * Executes INSERT and DELETE DML operations. The
 * action is specified within the TupleTableSlot at
 * plannode->actionColIdx.The ctid of the tuple to delete
 * is in position plannode->ctidColIdx in the current slot.
 * */
TupleTableSlot*
ExecDML(DMLState *node)
{

	PlanState *outerNode = outerPlanState(node);
	DML *plannode = (DML *) node->ps.plan;

	Assert(outerNode != NULL);

	TupleTableSlot *slot = ExecProcNode(outerNode);

	if (TupIsNull(slot))
	{
		return NULL;
	}

	bool isnull = false;
	int action = DatumGetUInt32(slot_getattr(slot, plannode->actionColIdx, &isnull));

	Assert(!isnull);

	isnull = false;
	Datum oid = slot_getattr(slot, plannode->oidColIdx, &isnull);
	slot->tts_tableOid = DatumGetUInt32(oid);

	bool isUpdate = false;
	if (node->ps.state->es_plannedstmt->commandType == CMD_UPDATE)
	{
		isUpdate = true;
	}

	Assert(action == DML_INSERT || action == DML_DELETE);


	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ExprContext *econtext = node->ps.ps_ExprContext;
	ResetExprContext(econtext);

	/* Prepare cleaned-up tuple by projecting it and filtering junk columns */
	econtext->ecxt_outertuple = slot;
	TupleTableSlot *projectedSlot = ExecProject(node->ps.ps_ProjInfo, NULL);

	/* remove 'junk' columns from tuple */
	node->cleanedUpSlot = ExecFilterJunk(node->junkfilter, projectedSlot);

	if (DML_INSERT == action)
	{

		/* Respect any given tuple Oid when updating a tuple. */
		if(isUpdate &&
		    plannode->tupleoidColIdx != 0)
		{
			isnull = false;
			oid = slot_getattr(slot, plannode->tupleoidColIdx, &isnull);
			HeapTuple htuple = ExecFetchSlotHeapTuple(node->cleanedUpSlot);
			Assert(htuple == node->cleanedUpSlot->PRIVATE_tts_heaptuple);
			HeapTupleSetOid(htuple, oid);
		}

		/* The plan origin is required since ExecInsert performs different actions 
		 * depending on the type of plan (constraint enforcement and triggers.) 
		 */
		ExecInsert(node->cleanedUpSlot, NULL /* destReceiver */,
				node->ps.state, PLANGEN_OPTIMIZER /* Plan origin */, 
				isUpdate);
	}
	else /* DML_DELETE */
	{
		Datum ctid = slot_getattr(slot, plannode->ctidColIdx, &isnull);

		Assert(!isnull);

		ItemPointer  tupleid = (ItemPointer) DatumGetPointer(ctid);
		ItemPointerData tuple_ctid = *tupleid;
		tupleid = &tuple_ctid;

		/* Correct tuple count by ignoring deletes when splitting tuples. */
		ExecDelete(tupleid, node->cleanedUpSlot, NULL /* DestReceiver */, node->ps.state,
				PLANGEN_OPTIMIZER /* Plan origin */, isUpdate);
	}

	return slot;
}

/**
 * Init nodeDML, which initializes the insert TupleTableSlot.
 * */
DMLState*
ExecInitDML(DML *node, EState *estate, int eflags)
{
	
	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND)));
	
	DMLState *dmlstate = makeNode(DMLState);
	dmlstate->ps.plan = (Plan *)node;
	dmlstate->ps.state = estate;

	ExecInitResultTupleSlot(estate, &dmlstate->ps);

	dmlstate->ps.targetlist = (List *)
						ExecInitExpr((Expr *) node->plan.targetlist,
						(PlanState *) dmlstate);

	Plan *outerPlan  = outerPlan(node);
	outerPlanState(dmlstate) = ExecInitNode(outerPlan, estate, eflags);

	ExecAssignResultTypeFromTL(&dmlstate->ps);

	/* Create expression evaluation context. This will be used for projections */
	ExecAssignExprContext(estate, &dmlstate->ps);

	/*
	 * Create projection info from the child tuple descriptor and our target list
	 * Projection will be placed in the ResultSlot
	 */
	TupleTableSlot *childResultSlot = outerPlanState(dmlstate)->ps_ResultTupleSlot;
	ExecAssignProjectionInfo(&dmlstate->ps, childResultSlot->tts_tupleDescriptor);
	
	/*
	 * Initialize slot to insert/delete using output relation descriptor.
	 */
	dmlstate->cleanedUpSlot = ExecInitExtraTupleSlot(estate);

	/*
	 * Both input and output of the junk filter include dropped attributes, so
	 * the junk filter doesn't need to do anything special there about them
	 */
	TupleDesc cleanTupType = CreateTupleDescCopy(dmlstate->ps.state->es_result_relation_info->ri_RelationDesc->rd_att); 
	dmlstate->junkfilter = ExecInitJunkFilter(node->plan.targetlist,
			cleanTupType,
			dmlstate->cleanedUpSlot);

	if (estate->es_instrument)
	{
	        dmlstate->ps.cdbexplainbuf = makeStringInfo();

	        /* Request a callback at end of query. */
	        dmlstate->ps.cdbexplainfun = ExecDMLExplainEnd;
	}

	initGpmonPktForDML((Plan *)node, &dmlstate->ps.gpmon_pkt, estate);
	
	return dmlstate;
}

/* Release Resources Requested by nodeDML. */
void
ExecEndDML(DMLState *node)
{
	/* Release explicitly the TupleDesc for result relation */
	ReleaseTupleDesc(node->junkfilter->jf_cleanTupType);

	ExecFreeExprContext(&node->ps);
	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	ExecClearTuple(node->cleanedUpSlot);
	ExecEndNode(outerPlanState(node));
	EndPlanStateGpmonPkt(&node->ps);
}

/* Return number of TupleTableSlots used by nodeDML.*/
int
ExecCountSlotsDML(DML *node)
{
	return ExecCountSlotsNode(outerPlan(node)) + DML_NSLOTS;
}

/* Tracing execution for GP Monitor. */
void
initGpmonPktForDML(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, DML));

	PerfmonNodeType type = PMNT_DML;

	InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, type,
								 (int64)planNode->plan_rows,
								 NULL);
}

/* EOF */
