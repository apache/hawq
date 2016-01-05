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
 * nodeBitmapAppendOnlyscan.c
 *	  Routines to support bitmapped scan from Append-Only relations
 *
 * This is a modified copy of nodeBitmapHeapscan.c converted to Append-Only.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2008-2009, Greenplum Inc.
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecBitmapAppendOnlyScan		scan from an AO relation using bitmap info
 *		ExecBitmapAppendOnlyNext		workhorse for above
 *		ExecInitBitmapAppendOnlyScan	creates and initializes state info.
 *		ExecBitmapAppendOnlyReScan	prepares to rescan the plan.
 *		ExecEndBitmapAppendOnlyScan	releases all storage.
 */
#include "postgres.h"

#include "access/heapam.h"
#include "executor/execdebug.h"
#include "executor/nodeBitmapAppendOnlyscan.h"
#include "cdb/cdbappendonlyam.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "cdb/cdbvars.h" /* gp_select_invisible */
#include "nodes/tidbitmap.h"

static TupleTableSlot *BitmapAppendOnlyScanNext(BitmapAppendOnlyScanState *node);

/*
 * Initialize the fetch descriptor for the BitmapAppendOnlyScanState if
 * it is not initialized.
 */
static void
initFetchDesc(BitmapAppendOnlyScanState *scanstate)
{
	BitmapAppendOnlyScan *node = (BitmapAppendOnlyScan *)(scanstate->ss.ps.plan);
	Relation currentRelation = scanstate->ss.ss_currentRelation;
	EState *estate = scanstate->ss.ps.state;

	if (node->isAORow)
	{
		if (scanstate->baos_currentAOFetchDesc == NULL)
		{
			scanstate->baos_currentAOFetchDesc = 
				appendonly_fetch_init(currentRelation,
									  estate->es_snapshot);
		}
	}
	
}

/*
 * Free fetch descriptor.
 */
static inline void
freeFetchDesc(BitmapAppendOnlyScanState *scanstate)
{
	if (scanstate->baos_currentAOFetchDesc != NULL)
	{
		Assert(((BitmapAppendOnlyScan *)(scanstate->ss.ps.plan))->isAORow);
		appendonly_fetch_finish(scanstate->baos_currentAOFetchDesc);
		pfree(scanstate->baos_currentAOFetchDesc);
		scanstate->baos_currentAOFetchDesc = NULL;
	}

}

/*
 * Initialize the state relevant to bitmaps.
 */
static inline void
initBitmapState(BitmapAppendOnlyScanState *scanstate)
{
	if (scanstate->baos_tbmres == NULL)
	{
		scanstate->baos_tbmres =
			palloc(sizeof(TBMIterateResult) +
					MAX_TUPLES_PER_PAGE * sizeof(OffsetNumber));

		/* initialize result header */
		MemSetAligned(scanstate->baos_tbmres, 0, sizeof(TBMIterateResult));
	}
}

/*
 * Free the state relevant to bitmaps
 */
static inline void
freeBitmapState(BitmapAppendOnlyScanState *scanstate)
{
	if (scanstate->baos_tbm != NULL)
	{
		if(IsA(scanstate->baos_tbm, HashBitmap))
			tbm_free((HashBitmap *)scanstate->baos_tbm);
		else
            tbm_bitmap_free(scanstate->baos_tbm);

		scanstate->baos_tbm = NULL;
	}
	if (scanstate->baos_tbmres != NULL)
	{
		pfree(scanstate->baos_tbmres);
		scanstate->baos_tbmres = NULL;
	}
}

/* ----------------------------------------------------------------
 *		BitmapAppendOnlyNext
 *
 *		Retrieve next tuple from the BitmapAppendOnlyScan node's currentRelation
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
BitmapAppendOnlyScanNext(BitmapAppendOnlyScanState *node)
{
	EState	   *estate;
	ExprContext *econtext;
	AppendOnlyFetchDesc aoFetchDesc;
	Index		scanrelid;
	Node  		*tbm;
	TBMIterateResult *tbmres;
	OffsetNumber psuedoHeapOffset;
	ItemPointerData psudeoHeapTid;
	AOTupleId aoTid;
	TupleTableSlot *slot;

	/*
	 * extract necessary information from index scan node
	 */
	estate = node->ss.ps.state;
	econtext = node->ss.ps.ps_ExprContext;
	slot = node->ss.ss_ScanTupleSlot;

	initBitmapState(node);
	initFetchDesc(node);

	aoFetchDesc = node->baos_currentAOFetchDesc;
	scanrelid = ((BitmapAppendOnlyScan *) node->ss.ps.plan)->scan.scanrelid;
	tbm = node->baos_tbm;
	tbmres = (TBMIterateResult *) node->baos_tbmres;
	Assert(tbmres != NULL);

	/*
	 * Check if we are evaluating PlanQual for tuple of this relation.
	 * Additional checking is not good, but no other way for now. We could
	 * introduce new nodes for this case and handle IndexScan --> NewNode
	 * switching in Init/ReScan plan...
	 */
	if (estate->es_evTuple != NULL &&
		estate->es_evTuple[scanrelid - 1] != NULL)
	{
		if (estate->es_evTupleNull[scanrelid - 1])
		{
			freeFetchDesc(node);
			freeBitmapState(node);
			
			return ExecClearTuple(slot);
		}

		ExecStoreGenericTuple(estate->es_evTuple[scanrelid - 1],
					   slot, false);

		/* Does the tuple meet the original qual conditions? */
		econtext->ecxt_scantuple = slot;

		ResetExprContext(econtext);

		if (!ExecQual(node->baos_bitmapqualorig, econtext, false))
		{
			ExecEagerFreeBitmapAppendOnlyScan(node);

			ExecClearTuple(slot);		/* would not be returned by scan */
		}

		/* Flag for the next call that no more tuples */
		estate->es_evTupleNull[scanrelid - 1] = true;

		if (!TupIsNull(slot))
		{
			Gpmon_M_Incr_Rows_Out(GpmonPktFromBitmapAppendOnlyScanState(node));
			CheckSendPlanStateGpmonPkt(&node->ss.ps);
		}
		return slot;
	}

	/*
	 * If we haven't yet performed the underlying index scan, or
	 * we have used up the bitmaps from the previous scan, do the next scan,
	 * and prepare the bitmap to be iterated over.
 	 */
	if (tbm == NULL)
	{
		tbm = (Node *) MultiExecProcNode(outerPlanState(node));

		if (tbm != NULL && (!(IsA(tbm, HashBitmap) ||
							  IsA(tbm, StreamBitmap))))
			elog(ERROR, "unrecognized result from subplan");

		/* When a HashBitmap is returned, set the returning bitmaps
		 * in the subplan to NULL, so that the subplan nodes do not
		 * mistakenly try to release the space during the rescan.
		 */
		if (tbm != NULL && IsA(tbm, HashBitmap))
			tbm_reset_bitmaps(outerPlanState(node));

		node->baos_tbm = tbm;
	}

	if (tbm == NULL)
	{
		ExecEagerFreeBitmapAppendOnlyScan(node);

		return ExecClearTuple(slot);
	}

	Assert(tbm != NULL);
	Assert(tbmres != NULL);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (!node->baos_gotpage)
		{
			/*
			 * Obtain the next psuedo-heap-page-info with item bit-map.  Later, we'll
			 * convert the (psuedo) heap block number and item number to an
			 * Append-Only TID.
			 */
			if (!tbm_iterate(tbm, tbmres))
			{
				/* no more entries in the bitmap */
				break;
			}

			/* If tbmres contains no tuples, continue. */
			if (tbmres->ntuples == 0)
				continue;

			Gpmon_M_Incr(GpmonPktFromBitmapAppendOnlyScanState(node), GPMON_BITMAPAPPENDONLYSCAN_PAGE);
			CheckSendPlanStateGpmonPkt(&node->ss.ps);

		 	node->baos_gotpage = true;

			/*
		 	* Set cindex to first slot to examine
		 	*/
			node->baos_cindex = 0;

			node->baos_lossy = (tbmres->ntuples < 0);
			if (!node->baos_lossy)
				node->baos_ntuples = tbmres->ntuples;
			else
				node->baos_ntuples = MAX_TUPLES_PER_PAGE;
				
		}
		else
		{
			/*
			 * Continuing in previously obtained page; advance cindex
			 */
			node->baos_cindex++;
		}

		/*
		 * Out of range?  If so, nothing more to look at on this page
		 */
		if (node->baos_cindex < 0 || node->baos_cindex >= node->baos_ntuples)
		{
		 	node->baos_gotpage = false;
			continue;
		}

		/*
		 * Must account for lossy page info...
		 */
		if (node->baos_lossy)
			psuedoHeapOffset = node->baos_cindex;	// We are iterating through all items.
		else
		{
			Assert(node->baos_cindex <= tbmres->ntuples);
			psuedoHeapOffset = tbmres->offsets[node->baos_cindex];
		}

		/*
		 * Okay to fetch the tuple
		 */
		ItemPointerSet(
				&psudeoHeapTid, 
				tbmres->blockno, 
				psuedoHeapOffset);

		tbm_convert_appendonly_tid_out(&psudeoHeapTid, &aoTid);

		if (aoFetchDesc != NULL)
		{
			appendonly_fetch(aoFetchDesc, &aoTid, slot);
		}
		
      	if (TupIsNull(slot))
			continue;

		pgstat_count_heap_fetch(node->ss.ss_currentRelation);

		/*
		 * If we are using lossy info, we have to recheck the qual
		 * conditions at every tuple.
		 */
		if (node->baos_lossy)
		{
			econtext->ecxt_scantuple = slot;
			ResetExprContext(econtext);

			if (!ExecQual(node->baos_bitmapqualorig, econtext, false))
			{
				/* Fails recheck, so drop it and loop back for another */
				ExecClearTuple(slot);
				continue;
			}
		}

		/* OK to return this tuple */
      	if (!TupIsNull(slot))
		{
			Gpmon_M_Incr_Rows_Out(GpmonPktFromBitmapAppendOnlyScanState(node));
			CheckSendPlanStateGpmonPkt(&node->ss.ps);
		}

		return slot;
	}

	/*
	 * if we get here it means we are at the end of the scan..
	 */
	ExecEagerFreeBitmapAppendOnlyScan(node);

	return ExecClearTuple(slot);
}

/* ----------------------------------------------------------------
 *		ExecBitmapAppendOnlyScan(node)
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecBitmapAppendOnlyScan(BitmapAppendOnlyScanState *node)
{
	/*
	 * use BitmapAppendOnlyNext as access method
	 */
	return ExecScan(&node->ss, (ExecScanAccessMtd) BitmapAppendOnlyScanNext);
}

/* ----------------------------------------------------------------
 *		ExecBitmapAppendOnlyReScan(node)
 * ----------------------------------------------------------------
 */
void
ExecBitmapAppendOnlyReScan(BitmapAppendOnlyScanState *node, ExprContext *exprCtxt)
{
	EState	   *estate;
	Index		scanrelid;

	estate = node->ss.ps.state;
	scanrelid = ((BitmapAppendOnlyScan *) node->ss.ps.plan)->scan.scanrelid;

	/* node->aofs.ps.ps_TupFromTlist = false; */

	/*
	 * If we are being passed an outer tuple, link it into the "regular"
	 * per-tuple econtext for possible qual eval.
	 */
	if (exprCtxt != NULL)
	{
		ExprContext *stdecontext;

		stdecontext = node->ss.ps.ps_ExprContext;
		stdecontext->ecxt_outertuple = exprCtxt->ecxt_outertuple;
	}

	/* If this is re-scanning of PlanQual ... */
	if (estate->es_evTuple != NULL &&
		estate->es_evTuple[scanrelid - 1] != NULL)
	{
		estate->es_evTupleNull[scanrelid - 1] = false;
	}

	/*
	 * NOTE: The appendonly_fetch routine can fetch randomly, so no need to reset it.
	 */

	freeBitmapState(node);
	tbm_reset_bitmaps(outerPlanState(node));

	/*
	 * Always rescan the input immediately, to ensure we can pass down any
	 * outer tuple that might be used in index quals.
	 */
	Gpmon_M_Incr(GpmonPktFromBitmapAppendOnlyScanState(node), GPMON_BITMAPAPPENDONLYSCAN_RESCAN);
	CheckSendPlanStateGpmonPkt(&node->ss.ps);

	ExecReScan(outerPlanState(node), exprCtxt);
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapAppendOnlyScan
 * ----------------------------------------------------------------
 */
void
ExecEndBitmapAppendOnlyScan(BitmapAppendOnlyScanState *node)
{
	Relation	relation;

	/*
	 * extract information from the node
	 */
	relation = node->ss.ss_currentRelation;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clear out tuple table slots
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));

	ExecEagerFreeBitmapAppendOnlyScan(node);
	
	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);

	node->baos_gotpage = false;
	node->baos_lossy = false;
	node->baos_cindex = 0;
	node->baos_ntuples = 0;

	EndPlanStateGpmonPkt(&node->ss.ps);
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapAppendOnlyScan
 *
 *		Initializes the scan's state information.
 * ----------------------------------------------------------------
 */
BitmapAppendOnlyScanState *
ExecInitBitmapAppendOnlyScan(BitmapAppendOnlyScan *node, EState *estate, int eflags)
{
	BitmapAppendOnlyScanState *scanstate;
	Relation	currentRelation;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	Assert(IsA(node, BitmapAppendOnlyScan));

	/*
	 * Assert caller didn't ask for an unsafe snapshot --- see comments at
	 * head of file.
	 *
	 * MPP-4703: the MVCC-snapshot restriction is required for correct results.
	 * our test-mode may deliberately return incorrect results, but that's OK.
	 */
	Assert(IsMVCCSnapshot(estate->es_snapshot) || gp_select_invisible);

	/*
	 * create state structure
	 */
	scanstate = makeNode(BitmapAppendOnlyScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;

	scanstate->baos_tbm = NULL;
	scanstate->baos_tbmres = NULL;
	scanstate->baos_gotpage = false;
	scanstate->baos_lossy = false;
	scanstate->baos_cindex = 0;
	scanstate->baos_ntuples = 0;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/* scanstate->aofs.ps.ps_TupFromTlist = false;*/

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) scanstate);
	scanstate->baos_bitmapqualorig = (List *)
		ExecInitExpr((Expr *) node->bitmapqualorig,
					 (PlanState *) scanstate);

#define BITMAPAPPENDONLYSCAN_NSLOTS 2

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
	ExecInitScanTupleSlot(estate, &scanstate->ss);

	/*
	 * open the base relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid);

	scanstate->ss.ss_currentRelation = currentRelation;

	/*
	 * get the scan type from the relation descriptor.
	 */
	ExecAssignScanType(&scanstate->ss, RelationGetDescr(currentRelation));

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	scanstate->baos_currentAOFetchDesc = NULL;
	
	/*
	 * initialize child nodes
	 *
	 * We do this last because the child nodes will open indexscans on our
	 * relation's indexes, and we want to be sure we have acquired a lock on
	 * the relation first.
	 */
	outerPlanState(scanstate) = ExecInitNode(outerPlan(node), estate, eflags);

	initGpmonPktForBitmapAppendOnlyScan((Plan *)node, &scanstate->ss.ps.gpmon_pkt, estate);

	/*
	 * all done.
	 */
	return scanstate;
}

int
ExecCountSlotsBitmapAppendOnlyScan(BitmapAppendOnlyScan *node)
{
	return ExecCountSlotsNode(outerPlan((Plan *) node)) +
		ExecCountSlotsNode(innerPlan((Plan *) node)) + BITMAPAPPENDONLYSCAN_NSLOTS;
}

void
initGpmonPktForBitmapAppendOnlyScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, BitmapAppendOnlyScan));

	{
		RangeTblEntry *rte = rt_fetch(((BitmapAppendOnlyScan *)planNode)->scan.scanrelid,
									  estate->es_range_table);
		char schema_rel_name[SCAN_REL_NAME_BUF_SIZE] = {0};
		
		Assert(GPMON_BITMAPAPPENDONLYSCAN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_BitmapAppendOnlyScan,
							 (int64)planNode->plan_rows,
							 GetScanRelNameGpmon(rte->relid, schema_rel_name));
	}
}

void
ExecEagerFreeBitmapAppendOnlyScan(BitmapAppendOnlyScanState *node)
{
	freeFetchDesc(node);
	freeBitmapState(node);
}
