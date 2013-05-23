/*-------------------------------------------------------------------------
 *
 * nodeBitmapIndexscan.c
 *	  Routines to support bitmapped index scans of relations
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/nodeBitmapIndexscan.c,v 1.21 2006/10/04 00:29:52 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		MultiExecBitmapIndexScan	scans a relation using index.
 *		ExecInitBitmapIndexScan		creates and initializes state info.
 *		ExecBitmapIndexReScan		prepares to rescan the plan.
 *		ExecEndBitmapIndexScan		releases all storage.
 */
#include "postgres.h"

#include "access/genam.h"
#include "cdb/cdbvars.h"
#include "executor/execdebug.h"
#include "executor/instrument.h"
#include "executor/nodeBitmapIndexscan.h"
#include "executor/nodeIndexscan.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "nodes/tidbitmap.h"

/* ----------------------------------------------------------------
 *		MultiExecBitmapIndexScan(node)
 * ----------------------------------------------------------------
 */
Node *
MultiExecBitmapIndexScan(BitmapIndexScanState *node)
{
	IndexScanDesc scandesc;
	bool		doscan;
	Node 		*bitmap = NULL;

	/* must provide our own instrumentation support */
	if (node->ss.ps.instrument)
		InstrStartNode(node->ss.ps.instrument);

	/*
	 * extract necessary information from index scan node
	 */
	scandesc = node->biss_ScanDesc;

	/*
	 * If we have runtime keys and they've not already been set up, do it now.
	 * Array keys are also treated as runtime keys; note that if ExecReScan
	 * returns with biss_RuntimeKeysReady still false, then there is an empty 
	 * array key so we should do nothing.
	 */
	if (!node->biss_RuntimeKeysReady &&
		(node->biss_NumRuntimeKeys != 0 || node->biss_NumArrayKeys != 0))
	{
		ExecReScan((PlanState *) node, NULL);
		doscan = node->biss_RuntimeKeysReady;
	}
	else
		doscan = true;


	/* Get bitmap from index */
	while (doscan)
	{
		bitmap = index_getmulti(scandesc, node->bitmap);

		if ((bitmap != NULL) &&
			!(IsA(bitmap, HashBitmap) ||
			  IsA(bitmap, StreamBitmap)))
			elog(ERROR, "unrecognized result from bitmap index scan");

		CHECK_FOR_INTERRUPTS();

        /* CDB: If EXPLAIN ANALYZE, let bitmap share our Instrumentation. */
        if (node->ss.ps.instrument)
            tbm_bitmap_set_instrument(bitmap, node->ss.ps.instrument);

		if(node->bitmap == NULL)
			node->bitmap = (Node *)bitmap;
		doscan = ExecIndexAdvanceArrayKeys(node->biss_ArrayKeys,
											   node->biss_NumArrayKeys);
		if (doscan)			/* reset index scan */
			index_rescan(node->biss_ScanDesc, node->biss_ScanKeys);
	}

	/* must provide our own instrumentation support */
	if (node->ss.ps.instrument)
		InstrStopNode(node->ss.ps.instrument, 1 /* XXX */);

	return (Node *) bitmap;
}

/* ----------------------------------------------------------------
 *		ExecBitmapIndexReScan(node)
 *
 *		Recalculates the value of the scan keys whose value depends on
 *		information known at runtime and rescans the indexed relation.
 * ----------------------------------------------------------------
 */
void
ExecBitmapIndexReScan(BitmapIndexScanState *node, ExprContext *exprCtxt)
{
	ExprContext *econtext;

	econtext = node->biss_RuntimeContext;		/* context for runtime keys */

	if (econtext)
	{
		/*
		 * If we are being passed an outer tuple, save it for runtime key
		 * calc.
		 */
		if (exprCtxt != NULL)
			econtext->ecxt_outertuple = exprCtxt->ecxt_outertuple;

		/*
		 * Reset the runtime-key context so we don't leak memory as each outer
		 * tuple is scanned.  Note this assumes that we will recalculate *all*
		 * runtime keys on each call.
		 */
		ResetExprContext(econtext);
	}

	/*
	 * If we are doing runtime key calculations (ie, the index keys depend on
	 * data from an outer scan), compute the new key values.
	 *
	 * Array keys are also treated as runtime keys; note that if we return 
	 * with biss_RuntimeKeysReady still false, then there is an empty array 
	 * key so no index scan is needed.
	 */
	if (node->biss_NumRuntimeKeys != 0)
		ExecIndexEvalRuntimeKeys(econtext,
								 node->biss_RuntimeKeys,
								 node->biss_NumRuntimeKeys);
	if (node->biss_NumArrayKeys != 0)
		node->biss_RuntimeKeysReady =
			ExecIndexEvalArrayKeys(econtext,
								   node->biss_ArrayKeys,
								   node->biss_NumArrayKeys);
	else
		node->biss_RuntimeKeysReady = true;

	/* reset index scan */
	if (node->biss_RuntimeKeysReady)
		index_rescan(node->biss_ScanDesc, node->biss_ScanKeys);

	/* Sanity check */
	if (node->bitmap)
	{
		if (!IsA(node->bitmap, HashBitmap) &&
			!IsA(node->bitmap, StreamBitmap))
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
					 errmsg("the returning bitmap in nodeBitmapIndexScan is corrupted.")));
	}

	/* reset hashBitmap */
	if(node->bitmap && IsA(node->bitmap, HashBitmap))
	{
        tbm_free((HashBitmap *)node->bitmap);
		node->bitmap = NULL;
	}
	else
	{
		/* XXX: we leak here */
		/* XXX: put in own memory context? */
		node->bitmap = NULL;
	}
     	Gpmon_M_Incr(GpmonPktFromBitmapIndexScanState(node), GPMON_BITMAPINDEXSCAN_RESCAN);
    	CheckSendPlanStateGpmonPkt(&node->ss.ps);
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapIndexScan
 * ----------------------------------------------------------------
 */
void
ExecEndBitmapIndexScan(BitmapIndexScanState *node)
{
	Relation	indexRelationDesc;
	IndexScanDesc indexScanDesc;

	/*
	 * extract information from the node
	 */
	indexRelationDesc = node->biss_RelationDesc;
	indexScanDesc = node->biss_ScanDesc;

	/*
	 * close the index relation
	 */
	index_endscan(indexScanDesc);

	index_close(indexRelationDesc, NoLock);

	EndPlanStateGpmonPkt(&node->ss.ps);
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapIndexScan
 *
 *		Initializes the index scan's state information.
 * ----------------------------------------------------------------
 */
BitmapIndexScanState *
ExecInitBitmapIndexScan(BitmapIndexScan *node, EState *estate, int eflags)
{
	BitmapIndexScanState *indexstate;
	bool		relistarget;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	indexstate = makeNode(BitmapIndexScanState);
	indexstate->ss.ps.plan = (Plan *) node;
	indexstate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * We do not need a standard exprcontext for this node, though we may
	 * decide below to create a runtime-key exprcontext
	 */

	/*
	 * initialize child expressions
	 *
	 * We don't need to initialize targetlist or qual since neither are used.
	 *
	 * Note: we don't initialize all of the indexqual expression, only the
	 * sub-parts corresponding to runtime keys (see below).
	 */

#define BITMAPINDEXSCAN_NSLOTS 0

	/*
	 * We do not open or lock the base relation here.  We assume that an
	 * ancestor BitmapHeapScan node is holding AccessShareLock (or better) on 
	 * the heap relation throughout the execution of the plan tree.
	 */

	indexstate->ss.ss_currentRelation = NULL;

	/*
	 * Open the index relation.
	 *
	 * If the parent table is one of the target relations of the query, then
	 * InitPlan already opened and write-locked the index, so we can avoid
	 * taking another lock here.  Otherwise we need a normal reader's lock.
	 */
	relistarget = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
	indexstate->biss_RelationDesc = index_open(node->indexid,
									 relistarget ? NoLock : AccessShareLock);

	/*
	 * Initialize index-specific scan state
	 */
	indexstate->biss_RuntimeKeysReady = false;

	/*
	 * build the index scan keys from the index qualification
	 */
	ExecIndexBuildScanKeys((PlanState *) indexstate,
						   indexstate->biss_RelationDesc,
						   node->indexqual,
						   node->indexstrategy,
						   node->indexsubtype,
						   &indexstate->biss_ScanKeys,
						   &indexstate->biss_NumScanKeys,
						   &indexstate->biss_RuntimeKeys,
						   &indexstate->biss_NumRuntimeKeys,
						   &indexstate->biss_ArrayKeys,
						   &indexstate->biss_NumArrayKeys);

	/*
	 * If we have runtime keys or array keys, we need an ExprContext to
	 * evaluate them. We could just create a "standard" plan node exprcontext,
	 * but to keep the code looking similar to nodeIndexscan.c, it seems
	 * better to stick with the approach of using a separate ExprContext.
	 */
	if (indexstate->biss_NumRuntimeKeys != 0 ||
		indexstate->biss_NumArrayKeys != 0)
	{
		ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;

		ExecAssignExprContext(estate, &indexstate->ss.ps);
		indexstate->biss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
		indexstate->ss.ps.ps_ExprContext = stdecontext;
	}
	else
	{
		indexstate->biss_RuntimeContext = NULL;
	}

	/*
	 * Initialize scan descriptor.
	 */
	indexstate->biss_ScanDesc =
		index_beginscan_multi(indexstate->biss_RelationDesc,
							  estate->es_snapshot,
							  indexstate->biss_NumScanKeys,
							  indexstate->biss_ScanKeys);
	/*
	 * all done.
	 */

	initGpmonPktForBitmapIndexScan((Plan *)node, &indexstate->ss.ps.gpmon_pkt, estate);

	return indexstate;
}

int
ExecCountSlotsBitmapIndexScan(BitmapIndexScan *node)
{
	return ExecCountSlotsNode(outerPlan((Plan *) node)) +
		ExecCountSlotsNode(innerPlan((Plan *) node)) + BITMAPINDEXSCAN_NSLOTS;
}


void
initGpmonPktForBitmapIndexScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, BitmapIndexScan));

	{
		char *relname = get_rel_name(((BitmapIndexScan *)planNode)->indexid);
		
		Assert(GPMON_BITMAPINDEXSCAN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_BitmapIndexScan,
							 (int64)planNode->plan_rows, 
							 relname);
		if (relname)
			pfree(relname);
	}
	
}
