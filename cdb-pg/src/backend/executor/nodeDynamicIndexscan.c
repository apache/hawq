/*
 * nodeDynamicIndexscan.c 
 *	Support routines for scanning one or more indexes that
 *	are determined at runtime.
 *
 *	DynamicIndexScan node scans each index one after the other.
 *	For each index, it opens the index, scans the index, and returns
 *	relevant tuples.
 *
 * Copyright (c) 2013 - present, EMC/Greenplum
 */

#include "postgres.h"

#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "executor/execIndexscan.h"
#include "executor/nodeIndexscan.h"
#include "executor/nodeDynamicIndexscan.h"
#include "cdb/cdbpartition.h"
#include "parser/parsetree.h"
#include "access/genam.h"
#include "access/catquery.h"

/* Number of slots required for DynamicIndexScan */
#define DYNAMICINDEXSCAN_NSLOTS 2

/*
 * Free resources from a partition.
 */
static inline void
CleanupOnePartition(IndexScanState *indexState);

/*
 * Account for the number of tuple slots required for DynamicIndexScan
 */
int 
ExecCountSlotsDynamicIndexScan(DynamicIndexScan *node)
{
	return DYNAMICINDEXSCAN_NSLOTS;
}

/*
 * Initialize ScanState in DynamicIndexScan.
 */
DynamicIndexScanState *
ExecInitDynamicIndexScan(DynamicIndexScan *node, EState *estate, int eflags)
{
	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND)));

	DynamicIndexScanState *dynamicIndexScanState = makeNode(DynamicIndexScanState);

	IndexScanState *indexState = &(dynamicIndexScanState->indexScanState);

	InitCommonIndexScanState((IndexScanState *)dynamicIndexScanState, (IndexScan *)node, estate, eflags);

	initGpmonPktForDynamicIndexScan((Plan *)node, &indexState->ss.ps.gpmon_pkt, estate);

	return dynamicIndexScanState;
}

/*
 * This function initializes a part and returns true if a new index has been prepared for scanning.
 */
static bool
initNextIndexToScan(DynamicIndexScanState *node)
{
	IndexScanState *indexState = &(node->indexScanState);

	DynamicIndexScan *dynamicIndexScan = (DynamicIndexScan *)node->indexScanState.ss.ps.plan;

	/* Load new index when the scanning of the previous index is done. */
	if (indexState->ss.scan_state == SCAN_INIT ||
		indexState->ss.scan_state == SCAN_DONE)
	{
		Oid *pid = hash_seq_search(&node->pidxStatus);
		if (pid == NULL)
		{
			/* Return if all parts have been scanned. */
			node->shouldCallHashSeqTerm = false;
			return false;
		}

		Oid rootOid = rel_partition_get_root(*pid);

		Oid pindex = getPhysicalIndexRelid(dynamicIndexScan->logicalIndexInfo,
					 rootOid,
					 *pid);

		Assert(OidIsValid(pindex));

		Relation currentRelation = OpenScanRelationByOid(*pid);
		indexState->ss.ss_currentRelation = currentRelation;

		for (int i=0; i < DYNAMICINDEXSCAN_NSLOTS; i++)
		{
			indexState->ss.ss_ScanTupleSlot[i].tts_tableOid = *pid;
		}

		ExecAssignScanType(&indexState->ss, RelationGetDescr(currentRelation));

		EState *estate = indexState->ss.ps.state;

		indexState->iss_RelationDesc = 
			OpenIndexRelation(estate, pindex, *pid);

		/*
		 * build the index scan keys from the index qualification
		 */
		ExecIndexBuildScanKeys((PlanState *) indexState,
						   indexState->iss_RelationDesc,
						   dynamicIndexScan->indexqual,
						   dynamicIndexScan->indexstrategy,
						   dynamicIndexScan->indexsubtype,
						   &indexState->iss_ScanKeys,
						   &indexState->iss_NumScanKeys,
						   &indexState->iss_RuntimeKeys,
						   &indexState->iss_NumRuntimeKeys,
						   NULL,
						   NULL);

		InitRuntimeKeysContext(indexState);
		indexState->iss_RuntimeKeysReady = false;

		/*
		 * Initialize result tuple type and projection info.
		 */
		ExecAssignResultTypeFromTL(&indexState->ss.ps);
		ExecAssignScanProjectionInfo(&indexState->ss);
		
		indexState->ss.scan_state = SCAN_SCAN;
	}

	return true;
}

/*
 * setPidIndex
 *   Set the hash table of Oids to scan.
 */
static void
setPidIndex(DynamicIndexScanState *node)
{
	Assert(node->pidxIndex == NULL);
	
	IndexScanState *indexState = (IndexScanState *)node;
	EState *estate = indexState->ss.ps.state;
	DynamicIndexScan *plan = (DynamicIndexScan *)indexState->ss.ps.plan;
	Assert(estate->dynamicTableScanInfo != NULL);
	
	if (estate->dynamicTableScanInfo->pidIndexes != NULL)
	{
		Assert(estate->dynamicTableScanInfo->numScans > plan->partIndex);
		node->pidxIndex = estate->dynamicTableScanInfo->pidIndexes[plan->partIndex];
	}
}

/*
 * Execution of DynamicIndexScan
 */
TupleTableSlot *
ExecDynamicIndexScan(DynamicIndexScanState *node)
{
	Assert(node);

	IndexScanState *indexState = &(node->indexScanState);

	TupleTableSlot *slot = NULL;
	
	/*
	 * If this is called the first time, find the pid index that contains all unique
	 * partition pids for this node to scan.
	 */
	if (node->pidxIndex == NULL)
	{
		setPidIndex(node);

		if (node->pidxIndex == NULL)
		{
			return NULL;
		}
		
		hash_seq_init(&node->pidxStatus, node->pidxIndex);
		node->shouldCallHashSeqTerm = true;
	}

	/*
	 * Scan index to find next tuple to return. If the current index
	 * is exhausted, close it and open the next index for scan.
	 */
	while (TupIsNull(slot) &&
		   initNextIndexToScan(node))
	{
		slot = ExecScan(&indexState->ss, (ExecScanAccessMtd) IndexNext);

		if (!TupIsNull(slot))
		{
			/* Report output rows to Gpmon */
			Gpmon_M_Incr_Rows_Out(GpmonPktFromDynamicIndexScanState(node));
			CheckSendPlanStateGpmonPkt(&indexState->ss.ps);
		}
		else
		{
			CleanupOnePartition(indexState);

			indexState->ss.scan_state = SCAN_INIT;
		}
	}
	return slot;
}

/*
 * Release resources for one part (this includes closing the index and
 * the relation.
 */
static inline void
CleanupOnePartition(IndexScanState *indexState)
{
	Assert(NULL != indexState);

	/* Reset index state and release locks. */
	ExecClearTuple(indexState->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(indexState->ss.ss_ScanTupleSlot);

	if ((indexState->ss.scan_state & SCAN_SCAN) != 0)
	{
		index_close(indexState->iss_RelationDesc, NoLock);
		ExecCloseScanRelation(indexState->ss.ss_currentRelation);
		indexState->ss.ss_currentRelation = NULL;
	}
}

/*
 * Release resources of DynamicIndexScan
 */
void 
ExecEndDynamicIndexScan(DynamicIndexScanState *node)
{
	IndexScanState *indexState = &(node->indexScanState);

	CleanupOnePartition(indexState);

	if (node->shouldCallHashSeqTerm)
	{
		hash_seq_term(&node->pidxStatus);
	}

	EndPlanStateGpmonPkt(&indexState->ss.ps);
}

/*
 * Allow rescanning an index.
 */
void 
ExecDynamicIndexReScan(DynamicIndexScanState *node, ExprContext *exprCtxt)
{
	/* Rescan not supported */
	insist_log(false, "dynamic index rescan is not supported");
}

/*
 * Method for reporting DynamicIndexScan progress to gpperfmon
 */
void
initGpmonPktForDynamicIndexScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, DynamicIndexScan));

	{
		RangeTblEntry *rte = rt_fetch(((Scan *)planNode)->scanrelid, estate->es_range_table);
		char schema_rel_name[SCAN_REL_NAME_BUF_SIZE] = {0};

		Assert(GPMON_DYNAMICINDEXSCAN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);

		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_DynamicIndexScan,
					(int64) planNode->plan_rows, GetScanRelNameGpmon(rte->relid, schema_rel_name));
	}
}
