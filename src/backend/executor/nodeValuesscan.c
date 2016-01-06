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
 * nodeValuesscan.c
 *	  Support routines for scanning Values lists
 *	  ("VALUES (...), (...), ..." in rangetable).
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/nodeValuesscan.c,v 1.3.2.1 2006/12/26 19:26:56 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecValuesScan			scans a values list.
 *		ExecValuesNext			retrieve next tuple in sequential order.
 *		ExecInitValuesScan		creates and initializes a valuesscan node.
 *		ExecEndValuesScan		releases any storage allocated.
 *		ExecValuesReScan		rescans the values list
 */
#include "postgres.h"

#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "executor/nodeValuesscan.h"
#include "optimizer/var.h"              /* CDB: contain_var_reference() */
#include "parser/parsetree.h"
#include "utils/memutils.h"


static TupleTableSlot *ValuesNext(ValuesScanState *node);


/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ValuesNext
 *
 *		This is a workhorse for ExecValuesScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ValuesNext(ValuesScanState *node)
{
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	ScanDirection direction;
	List	   *exprlist;

	/*
	 * get information from the estate and scan state
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;
	econtext = node->rowcontext;

	/*
	 * Get the next tuple. Return NULL if no more tuples.
	 */
	if (ScanDirectionIsForward(direction))
	{
		if (node->curr_idx < node->array_len)
			node->curr_idx++;
		if (node->curr_idx < node->array_len)
			exprlist = node->exprlists[node->curr_idx];
		else
			exprlist = NIL;
	}
	else
	{
		if (node->curr_idx >= 0)
			node->curr_idx--;
		if (node->curr_idx >= 0)
			exprlist = node->exprlists[node->curr_idx];
		else
			exprlist = NIL;
	}

	/*
	 * Always clear the result slot; this is appropriate if we are at the end
	 * of the data, and if we're not, we still need it as the first step of
	 * the store-virtual-tuple protocol.  It seems wise to clear the slot
	 * before we reset the context it might have pointers into.
	 */
	ExecClearTuple(slot);

	if (exprlist)
	{
		MemoryContext oldContext;
		List	   *exprstatelist;
		Datum	   *values;
		bool	   *isnull;
		ListCell   *lc;
		int			resind;

		/*
		 * Get rid of any prior cycle's leftovers.  We use ReScanExprContext
		 * not just ResetExprContext because we want any registered shutdown
		 * callbacks to be called.
		 */
		ReScanExprContext(econtext);

		/*
		 * Build the expression eval state in the econtext's per-tuple memory.
		 * This is a tad unusual, but we want to delete the eval state again
		 * when we move to the next row, to avoid growth of memory
		 * requirements over a long values list.
		 */
		oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		/*
		 * Pass NULL, not my plan node, because we don't want anything in this
		 * transient state linking into permanent state.  The only possibility
		 * is a SubPlan, and there shouldn't be any (any subselects in the
		 * VALUES list should be InitPlans).
		 */
		exprstatelist = (List *) ExecInitExpr((Expr *) exprlist, NULL);

		/* parser should have checked all sublists are the same length */
		Assert(list_length(exprstatelist) == slot->tts_tupleDescriptor->natts);

		/*
		 * Compute the expressions and build a virtual result tuple. We
		 * already did ExecClearTuple(slot).
		 */
		ExecClearTuple(slot); 
		values = slot_get_values(slot); 
		isnull = slot_get_isnull(slot);

		resind = 0;
		foreach(lc, exprstatelist)
		{
			ExprState  *estate = (ExprState *) lfirst(lc);

			values[resind] = ExecEvalExpr(estate,
										  econtext,
										  &isnull[resind],
										  NULL);
			resind++;
		}

		MemoryContextSwitchTo(oldContext);

		/*
		 * And return the virtual tuple.
		 */
		ExecStoreVirtualTuple(slot);

        /* CDB: Label each row with a synthetic ctid for subquery dedup. */
        if (node->cdb_want_ctid)
        {
            HeapTuple   tuple = ExecFetchSlotHeapTuple(slot); 

            ItemPointerSet(&tuple->t_self, node->curr_idx >> 16,
                           (OffsetNumber)node->curr_idx);
        }
	}

	return slot;
}


/* ----------------------------------------------------------------
 *		ExecValuesScan(node)
 *
 *		Scans the values lists sequentially and returns the next qualifying
 *		tuple.
 *		It calls the ExecScan() routine and passes it the access method
 *		which retrieves tuples sequentially.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecValuesScan(ValuesScanState *node)
{
	/*
	 * use ValuesNext as access method
	 */
	return ExecScan(&node->ss, (ExecScanAccessMtd) ValuesNext);
}

/* ----------------------------------------------------------------
 *		ExecInitValuesScan
 * ----------------------------------------------------------------
 */
ValuesScanState *
ExecInitValuesScan(ValuesScan *node, EState *estate, int eflags)
{
	ValuesScanState *scanstate;
	RangeTblEntry *rte;
	TupleDesc	tupdesc;
	ListCell   *vtl;
	int			i;
	PlanState  *planstate;

	/*
	 * ValuesScan should not have any children.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create new ScanState for node
	 */
	scanstate = makeNode(ValuesScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 */
	planstate = &scanstate->ss.ps;

	/*
	 * Create expression contexts.	We need two, one for per-sublist
	 * processing and one for execScan.c to use for quals and projections. We
	 * cheat a little by using ExecAssignExprContext() to build both.
	 */
	ExecAssignExprContext(estate, planstate);
	scanstate->rowcontext = planstate->ps_ExprContext;
	ExecAssignExprContext(estate, planstate);

#define VALUESSCAN_NSLOTS 2

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
	ExecInitScanTupleSlot(estate, &scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) scanstate);

	/* Check if targetlist or qual contains a var node referencing the ctid column */
	scanstate->cdb_want_ctid = contain_ctid_var_reference(&node->scan);

	/*
	 * get info about values list
	 */
	rte = rt_fetch(node->scan.scanrelid, estate->es_range_table);
	Assert(rte->rtekind == RTE_VALUES);
	tupdesc = ExecTypeFromExprList((List *) linitial(rte->values_lists));

	ExecAssignScanType(&scanstate->ss, tupdesc);

	/*
	 * Other node-specific setup
	 */
	scanstate->marked_idx = -1;
	scanstate->curr_idx = -1;
	scanstate->array_len = list_length(rte->values_lists);

	/* convert list of sublists into array of sublists for easy addressing */
	scanstate->exprlists = (List **)
		palloc(scanstate->array_len * sizeof(List *));
	i = 0;
	foreach(vtl, rte->values_lists)
	{
		scanstate->exprlists[i++] = (List *) lfirst(vtl);
	}

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	initGpmonPktForValuesScan((Plan *)node, &scanstate->ss.ps.gpmon_pkt, estate);
	
	return scanstate;
}

int
ExecCountSlotsValuesScan(ValuesScan *node)
{
	return ExecCountSlotsNode(outerPlan(node)) +
		ExecCountSlotsNode(innerPlan(node)) +
		VALUESSCAN_NSLOTS;
}

/* ----------------------------------------------------------------
 *		ExecEndValuesScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndValuesScan(ValuesScanState *node)
{
	/*
	 * Free both exprcontexts
	 */
	ExecFreeExprContext(&node->ss.ps);
	node->ss.ps.ps_ExprContext = node->rowcontext;
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	EndPlanStateGpmonPkt(&node->ss.ps);
}

/* ----------------------------------------------------------------
 *		ExecValuesMarkPos
 *
 *		Marks scan position.
 * ----------------------------------------------------------------
 */
void
ExecValuesMarkPos(ValuesScanState *node)
{
	node->marked_idx = node->curr_idx;
}

/* ----------------------------------------------------------------
 *		ExecValuesRestrPos
 *
 *		Restores scan position.
 * ----------------------------------------------------------------
 */
void
ExecValuesRestrPos(ValuesScanState *node)
{
	node->curr_idx = node->marked_idx;
}

/* ----------------------------------------------------------------
 *		ExecValuesReScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecValuesReScan(ValuesScanState *node, ExprContext *exprCtxt)
{
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	/*node->ss.ps.ps_TupFromTlist = false;*/

	node->curr_idx = -1;
}

void
initGpmonPktForValuesScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	RangeTblEntry *rte;
	Assert(planNode != NULL && gpmon_pkt != NULL);

	rte = rt_fetch(((ValuesScan *)planNode)->scan.scanrelid, estate->es_range_table);

	{
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_ValuesScan, (int64)0, rte->eref->aliasname);
	}
}
