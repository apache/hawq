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
 * execBitmapTableScan.c
 *	  Support routines for nodeBitmapTableScan.c
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Portions Copyright (c) 2014, Pivotal, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/transam.h"
#include "executor/execdebug.h"
#include "executor/nodeBitmapTableScan.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "cdb/cdbvars.h" /* gp_select_invisible */
#include "nodes/tidbitmap.h"

/*
 * Returns BitmapTableScanMethod for a given table type. Returns NULL
 * if the given type is TableTypeInvalid or not defined in TableType.
 */
static const ScanMethod *
getBitmapTableScanMethod(TableType tableType)
{
	/*
	 * scanMethods
	 *    Array that specifies different scan methods for various table types.
	 *
	 * The index in this array for a specific table type should match the enum value
	 * defined in TableType.
	 */
	static const ScanMethod scanMethods[] =
	{
		{
			&BitmapHeapScanNext, &BitmapHeapScanBegin, &BitmapHeapScanEnd,
			&BitmapHeapScanReScan, &MarkRestrNotAllowed, &MarkRestrNotAllowed
		}
	};

	/* COMPILE_ASSERT(ARRAY_SIZE(scanMethods) == TableTypeInvalid); */

	if (tableType < 0 && tableType >= TableTypeInvalid)
	{
		return NULL;
	}

	return &scanMethods[tableType];
}

/*
 * Initializes the state relevant to bitmaps.
 */
static inline void
initBitmapState(BitmapTableScanState *scanstate)
{
	if (scanstate->tbmres == NULL)
	{
		scanstate->tbmres =
			palloc0(sizeof(TBMIterateResult) +
					MAX_TUPLES_PER_PAGE * sizeof(OffsetNumber));
	}
}

/*
 * Frees the state relevant to bitmaps.
 */
static inline void
freeBitmapState(BitmapTableScanState *scanstate)
{
	if (scanstate->tbm != NULL)
	{
		if(IsA(scanstate->tbm, HashBitmap))
		{
			tbm_free((HashBitmap *)scanstate->tbm);
		}
		else
		{
            tbm_bitmap_free(scanstate->tbm);
		}

		scanstate->tbm = NULL;
	}

	if (scanstate->tbmres != NULL)
	{
		pfree(scanstate->tbmres);
		scanstate->tbmres = NULL;
	}

	tbm_reset_bitmaps(outerPlanState(scanstate));
}

static TupleTableSlot*
BitmapTableScanPlanQualTuple(BitmapTableScanState *node)
{
	EState	   *estate = node->ss.ps.state;
	Index		scanrelid = ((BitmapTableScan *) node->ss.ps.plan)->scan.scanrelid;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

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
			return ExecClearTuple(slot);
		}

		ExecStoreGenericTuple(estate->es_evTuple[scanrelid - 1], slot, false);

		/* Does the tuple meet the original qual conditions? */
		econtext->ecxt_scantuple = slot;

		ResetExprContext(econtext);

		if (!ExecQual(node->bitmapqualorig, econtext, false))
		{
			ExecClearTuple(slot);		/* would not be returned by scan */
		}

		/* Flag for the next call that no more tuples */
		estate->es_evTupleNull[scanrelid - 1] = true;

		return slot;
	}

	return ExecClearTuple(slot);
}

/*
 * Reads a bitmap (with possibly many pages) from the underlying node.
 */
static void
readBitmap(BitmapTableScanState *scanState)
{
	if (scanState->tbm != NULL)
	{
		return;
	}

	Node *tbm = (Node *) MultiExecProcNode(outerPlanState(scanState));

	if (tbm != NULL && (!(IsA(tbm, HashBitmap) ||
						  IsA(tbm, StreamBitmap))))
	{
		elog(ERROR, "unrecognized result from subplan");
	}

	/*
	 * When a HashBitmap is returned, set the returning bitmaps
	 * in the subplan to NULL, so that the subplan nodes do not
	 * mistakenly try to release the space during the rescan.
	 */
	if (tbm != NULL && IsA(tbm, HashBitmap))
	{
		tbm_reset_bitmaps(outerPlanState(scanState));
	}

	scanState->tbm = tbm;
	scanState->needNewBitmapPage = true;
}

/*
 * Reads the next bitmap page from the current bitmap.
 */
static bool
fetchNextBitmapPage(BitmapTableScanState *scanState)
{
	if (scanState->tbm == NULL)
	{
		return false;
	}

	TBMIterateResult *tbmres = (TBMIterateResult *)scanState->tbmres;

	Assert(scanState->needNewBitmapPage);

	bool gotBitmapPage = true;

	/* Set to 0 so that we can enter the while loop */
	tbmres->ntuples = 0;

	while (gotBitmapPage && tbmres->ntuples == 0)
	{
		gotBitmapPage = tbm_iterate(scanState->tbm, (TBMIterateResult *)scanState->tbmres);
	}

	if (gotBitmapPage)
	{
		scanState->iterator = NULL;
		scanState->needNewBitmapPage = false;

		if (tbmres->ntuples == BITMAP_IS_LOSSY)
		{
			scanState->isLossyBitmapPage = true;
		}
		else
		{
			scanState->isLossyBitmapPage = false;
		}
	}

	return gotBitmapPage;
}

/*
 * Initializes perfmon details for BitmapTableScan node.
 */
void
initGpmonPktForBitmapTableScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, BitmapTableScan));

	{
		RangeTblEntry *rte = rt_fetch(((BitmapTableScan *)planNode)->scan.scanrelid,
									  estate->es_range_table);
		char schema_rel_name[SCAN_REL_NAME_BUF_SIZE] = {0};

		Assert(GPMON_BITMAPTABLESCAN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_BitmapTableScan,
							 (int64)planNode->plan_rows,
							 GetScanRelNameGpmon(rte->relid, schema_rel_name));
	}
}

/*
 * Checks eligibility of a tuple.
 *
 * Note, a tuple may fail to meet visibility requirement. Moreover,
 * for a lossy bitmap, we need to check for every tuple to make sure
 * that it satisfies the qual.
 */
bool
BitmapTableScanRecheckTuple(BitmapTableScanState *scanState, TupleTableSlot *slot)
{
	/*
	 * If we are using lossy info or we are required to recheck each tuple
	 * because of visibility or other causes, then evaluate the tuple
	 * eligibility.
	 */
	if (scanState->isLossyBitmapPage || scanState->recheckTuples)
	{
		ExprContext *econtext = scanState->ss.ps.ps_ExprContext;

		econtext->ecxt_scantuple = slot;
		ResetExprContext(econtext);

		return ExecQual(scanState->bitmapqualorig, econtext, false);
	}

	return true;
}

/*
 * Prepares for a new scan such as initializing bitmap states, preparing
 * the corresponding scan method etc.
 */
void
BitmapTableScanBegin(BitmapTableScanState *scanState, Plan *plan, EState *estate, int eflags)
{
	DynamicScan_Begin((ScanState *)scanState, plan, estate, eflags);
}

/*
 * Prepares for scanning of a new partition/relation.
 */
void
BitmapTableScanBeginPartition(ScanState *node, bool initExpressions)
{
	Assert(node != NULL);
	BitmapTableScanState *scanState = (BitmapTableScanState *)node;

	Assert(SCAN_NEXT == scanState->ss.scan_state);

	initBitmapState(scanState);

	if (scanState->bitmapqualorig == NULL || initExpressions)
	{
		/* TODO rahmaf2 [JIRA: MPP-23293]: remap columns per-partition to handle dropped columns */
		scanState->bitmapqualorig = (List *)
			ExecInitExpr((Expr *) ((BitmapTableScan*)(node->ps.plan))->bitmapqualorig,
						 (PlanState *) scanState);
	}

	scanState->needNewBitmapPage = true;
	scanState->recheckTuples = true;

	getBitmapTableScanMethod(node->tableType)->beginScanMethod(node);

	/*
	 * Prepare child node to produce new bitmaps for the new partition (and cleanup
	 * any leftover state from old partition).
	 */
	ExecReScan(outerPlanState(node), NULL);
}

/*
 * Cleans up once scanning of a partition/relation is done.
 */
void
BitmapTableScanEndPartition(ScanState *node)
{
	Assert(SCAN_SCAN == node->scan_state);

	BitmapTableScanState *scanState = (BitmapTableScanState *) node;

	freeBitmapState(scanState);

	getBitmapTableScanMethod(node->tableType)->endScanMethod(node);

	Assert(scanState->tbm == NULL);
}

/*
 * Executes underlying scan method to fetch the next matching tuple.
 */
TupleTableSlot *
BitmapTableScanFetchNext(ScanState *node)
{
	BitmapTableScanState *scanState = (BitmapTableScanState *) node;
	TupleTableSlot *slot = BitmapTableScanPlanQualTuple(scanState);

	while (TupIsNull(slot))
	{
		/* If we haven't already obtained the required bitmap, do so */
		readBitmap(scanState);

		/* If we have exhausted the current bitmap page, fetch the next one */
		if (!scanState->needNewBitmapPage || fetchNextBitmapPage(scanState))
		{
			slot = ExecScan(&scanState->ss, (ExecScanAccessMtd) getBitmapTableScanMethod(scanState->ss.tableType)->accessMethod);
		}
		else
		{
			/*
			 * Needed a new bitmap page, but couldn't fetch one. Therefore,
			 * try the next partition.
			 */
			break;
		}
	}

	return slot;
}

/*
 * Cleans up after the scanning has finished.
 */
void
BitmapTableScanEnd(BitmapTableScanState *scanState)
{
	DynamicScan_End((ScanState *)scanState, BitmapTableScanEndPartition);
}

/*
 * Prepares for a rescan.
 */
void
BitmapTableScanReScan(BitmapTableScanState *node, ExprContext *exprCtxt)
{
	ScanState *scanState = &node->ss;
	Assert(scanState->tableType >= 0 && scanState->tableType < TableTypeInvalid);

	/*
	 * If we are being passed an outer tuple, link it into the "regular"
	 * per-tuple econtext for possible qual eval.
	 */
	if (exprCtxt != NULL)
	{
		ExprContext *stdecontext = node->ss.ps.ps_ExprContext;
		stdecontext->ecxt_outertuple = exprCtxt->ecxt_outertuple;
	}

	EState	   *estate = node->ss.ps.state;
	Index scanrelid = ((Scan *)(scanState->ps.plan))->scanrelid;

	/* If this is re-scanning of PlanQual ... */
	if (estate->es_evTuple != NULL &&
		estate->es_evTuple[scanrelid - 1] != NULL)
	{
		estate->es_evTupleNull[scanrelid - 1] = false;
	}

	DynamicScan_ReScan((ScanState *)node, BitmapTableScanEndPartition, exprCtxt);

	ExecReScan(outerPlanState(node), exprCtxt);
}
