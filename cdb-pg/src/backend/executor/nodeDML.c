/*-------------------------------------------------------------------------
 *
 * nodeDML.c
 *	  Implementation of nodeDML.
 *
 * Copyright (c) 2012, EMC Corp.
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

	if (DML_INSERT == action)
	{
		/* remove `junk' columns from tuple */
		node->insertSlot = ExecFilterJunk(node->junkfilter, slot);

		/* The plan origin is required since ExecInsert performs different actions 
		 * depending on the type of plan (constraint enforcement and triggers.) 
		 */
		ExecInsert(node->insertSlot, NULL, NULL,
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
		ExecDelete(tupleid, slot, NULL /* DestReceiver */, node->ps.state,
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

	/*
	 * DML nodes do not project (ExecProject).
	 */
	ExecAssignResultTypeFromTL(&dmlstate->ps);
	ExecAssignProjectionInfo(&dmlstate->ps, NULL);
	
	/*
	 * Initialize slot to insert/delete using output relation descriptor.
	 */
	dmlstate->insertSlot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(dmlstate->insertSlot,
							dmlstate->ps.state->es_result_relation_info->ri_RelationDesc->rd_att);

	/* ExecInitJunkFilterConversion considers dropped attributes */
	dmlstate->junkfilter = ExecInitJunkFilterConversion(node->plan.targetlist, 
		dmlstate->ps.state->es_result_relation_info->ri_RelationDesc->rd_att, NULL /* slot */);

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
	ExecClearTuple(node->insertSlot);
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
