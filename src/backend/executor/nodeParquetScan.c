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
 * nodeParquetScan.c
 *
 *  Created on: Jul 4, 2013
 *      Author: malili
 */
#include "postgres.h"

#include "access/heapam.h"
#include "cdb/cdbparquetam.h"
#include "cdb/cdbvars.h"
#include "executor/execdebug.h"
#include "executor/nodeParquetScan.h"

static TupleTableSlot *ParquetNext(ParquetScanState *node);
static void OpenParquetScanRelation(ParquetScanState *node);
static void CloseParquetScanRelation(ParquetScanState *node);

static TupleTableSlot *ParquetNext(ParquetScanState *node)
{
	parquet_getnext(node->scandesc, node->ss.ps.state->es_direction, node->ss.ss_ScanTupleSlot);
	return node->ss.ss_ScanTupleSlot;
}

/*
 * Open/close underlying table.
 * Resource acquire and release
 */
static void OpenParquetScanRelation(ParquetScanState *node)
{
	Assert(node->ss.scan_state == SCAN_INIT || node->ss.scan_state == SCAN_DONE);
	Assert(!node->scandesc);

	node->scandesc = parquet_beginscan(
			node->ss.ss_currentRelation,
			node->ss.ps.state->es_snapshot,
			NULL /* relationTupleDesc */,
			node->proj);

	node->ss.scan_state = SCAN_SCAN;
}

static void CloseParquetScanRelation(ParquetScanState *node)
{
	Assert((node->ss.scan_state & SCAN_SCAN) != 0);
    parquet_endscan(node->scandesc);

	node->scandesc = NULL;
	node->ss.scan_state = SCAN_INIT;
}


/* ----------------------------------------------------------------
 *		ExecInitParquetScan
 * ----------------------------------------------------------------
 */
ParquetScanState *
ExecInitParquetScan(ParquetScan *node, EState *estate, int eflags){
	ParquetScanState		*state;
	Relation		currentRelation;

	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	state = makeNode(ParquetScanState);
	state->ss.ps.plan = (Plan *) node;
	state->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &state->ss.ps);

	/*
	 * initialize child expressions
	 */
	state->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) state);
	state->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) state);

#define ParquetSCAN_NSLOTS 2

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &state->ss.ps);
	ExecInitScanTupleSlot(estate, &state->ss);

	/*
	 * get the relation object id from the relid'th entry in the range table
	 * and open that relation.
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid);
	state->ss.ss_currentRelation = currentRelation;
	ExecAssignScanType(&state->ss, RelationGetDescr(currentRelation));

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&state->ss.ps);
	ExecAssignScanProjectionInfo(&state->ss);

	/* init aocs prj */
	{
		int i;
		state->ncol = currentRelation->rd_att->natts;
		state->proj = palloc0(sizeof(bool) * state->ncol);
		GetNeededColumnsForScan((Node *) node->scan.plan.targetlist, state->proj, state->ncol);
		GetNeededColumnsForScan((Node *) node->scan.plan.qual, state->proj, state->ncol);

		/* At least project one col */
		for(i=0; i<state->ncol; ++i)
		{
			if(state->proj[i])
				break;
		}

		if(i==state->ncol)
			state->proj[0] = true;
	}

//	initGpmonPktForParquetScan((Plan *)node, &state->ss.ps.gpmon_pkt, estate);

	return state;
}

/* ----------------------------------------------------------------
 *		ExecParquetScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		It calls the ExecScan() routine and passes it the access method
 *		which retrieve tuples sequentially.
 * ----------------------------------------------------------------
 */

TupleTableSlot *
ExecParquetScan(ParquetScanState *node){
	/*
	 * use AppendOnlyNext as access method
	 */
	TupleTableSlot *slot;

	if((node->ss.scan_state & SCAN_SCAN) == 0)
		OpenParquetScanRelation(node);

	slot = ExecScan((ScanState *) node, (ExecScanAccessMtd) ParquetNext);
     	if (!TupIsNull(slot))
        {
     		Gpmon_M_Incr_Rows_Out(GpmonPktFromParquetScanState(node));
                CheckSendPlanStateGpmonPkt(&node->ss.ps);
        }

	Assert((node->ss.scan_state & SCAN_MARKPOS) == 0);
	if(TupIsNull(slot))
		CloseParquetScanRelation(node);

	return slot;

}


/* ----------------------------------------------------------------
 *		ExecEndParquetScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndParquetScan(ParquetScanState * node)
{
	Relation	relation;
	/*
	 * get information from node
	 */
	relation = node->ss.ss_currentRelation;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	ExecEagerFreeParquetScan(node);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);

	EndPlanStateGpmonPkt(&node->ss.ps);

}

/* ----------------------------------------------------------------
 *		ExecParquetReScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecParquetReScan(ParquetScanState *node, ExprContext *exprCtxt)
{
	EState				*estate;
	Index				scanrelid;

	estate = node->ss.ps.state;
	scanrelid = ((ParquetScan *) node->ss.ps.plan)->scan.scanrelid;

	/* If this is re-scanning of PlanQual ... */
	if (estate->es_evTuple != NULL &&
		estate->es_evTuple[scanrelid - 1] != NULL)
	{
		estate->es_evTupleNull[scanrelid - 1] = false;
		return;
	}

	if((node->ss.scan_state & SCAN_SCAN) == 0)
		OpenParquetScanRelation(node);

	parquet_rescan(node->scandesc);

	Gpmon_M_Incr(GpmonPktFromParquetScanState(node), GPMON_ParquetSCAN_RESCAN);
	CheckSendPlanStateGpmonPkt(&node->ss.ps);

}

/* ----------------------------------------------------------------
 *		ExecEagerFreeParquetScan
 * ----------------------------------------------------------------
 */
void
ExecEagerFreeParquetScan(ParquetScanState *node)
{
	if((node->ss.scan_state & SCAN_SCAN) != 0)
		CloseParquetScanRelation(node);
}

/* ----------------------------------------------------------------
 *		ExecCountSlotsParquetScan
 * ----------------------------------------------------------------
 */
int
ExecCountSlotsParquetScan(ParquetScan *node)
{
	return ExecCountSlotsNode(outerPlan(node)) +
		ExecCountSlotsNode(innerPlan(node)) +
		ParquetSCAN_NSLOTS;
}


/* ----------------------------------------------------------------
 *		initGpmonPktForParquetScan
 * ----------------------------------------------------------------

void initGpmonPktForParquetScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate){
//	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, ParquetScan));
//
//	{
//		RangeTblEntry *rte = rt_fetch(((AOCSScan *)planNode)->scan.scanrelid,
//									  estate->es_range_table);
//		char schema_rel_name[SCAN_REL_NAME_BUF_SIZE] = {0};
//
//		Assert(GPMON_AOCSSCAN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
//		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_AppendOnlyColumnarScan,
//							 (int64)planNode->plan_rows,
//							 GetScanRelNameGpmon(rte->relid, schema_rel_name));
//	}

}*/


