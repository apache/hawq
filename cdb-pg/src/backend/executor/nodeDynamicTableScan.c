/*
 * nodeDynamicTableScan.c
 *    Support routines for scanning one or more relations that are determined
 * at run time. The relations could be Heap, AppendOnly Row, AppendOnly Columnar.
 *
 * DynamicTableScan node scans each relation one after the other. For each relation,
 * it opens the table, scans the tuple, and returns relevant tuples.
 *
 * Copyright (c) 2012 - present, EMC/Greenplum
 */
#include "postgres.h"

#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "executor/nodeDynamicTableScan.h"
#include "utils/hsearch.h"
#include "parser/parsetree.h"
#include "utils/faultinjector.h"

#define DYNAMIC_TABLE_SCAN_NSLOTS 2

static inline void
CleanupOnePartition(ScanState *scanState);

DynamicTableScanState *
ExecInitDynamicTableScan(DynamicTableScan *node, EState *estate, int eflags)
{
	Assert((eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

	DynamicTableScanState *state = makeNode(DynamicTableScanState);
	
	InitScanStateInternal((ScanState *)state, (Plan *)node, estate, eflags);

	initGpmonPktForDynamicTableScan((Plan *)node, &state->tableScanState.ss.ps.gpmon_pkt, estate);

	return state;
}

/*
 * initNextTableToScan
 *   Find the next table to scan and initiate the scan if the previous table
 * is finished.
 *
 * If scanning on the current table is not finished, or a new table is found,
 * this function returns true.
 * If no more table is found, this function returns false.
 */
static bool
initNextTableToScan(DynamicTableScanState *node)
{
	ScanState *scanState = (ScanState *)node;

	if (scanState->scan_state == SCAN_INIT ||
		scanState->scan_state == SCAN_DONE)
	{
		Oid *pid = hash_seq_search(&node->pidStatus);
		if (pid == NULL)
		{
			node->shouldCallHashSeqTerm = false;
			return false;
		}
		
		scanState->ss_currentRelation = OpenScanRelationByOid(*pid);

		/*
		 * Inside ExecInitScanTupleSlot() we set the tuple table slot's oid
		 * to range table entry's relid, which for partitioned table always set
		 * to parent table's oid. In queries where we need to read table oids
		 * (MPP-20736) we use the tuple table slot's saved oid (refer to slot_getsysattr()).
		 * This wrongly returns parent oid, instead of partition oid. Therefore,
		 * to return correct partition oid, we need to update
		 * our tuple table slot's oid to reflect the partition oid.
		 */
		for (int i = 0; i < DYNAMIC_TABLE_SCAN_NSLOTS; i++)
		{
			scanState->ss_ScanTupleSlot[i].tts_tableOid = *pid;
		}

		ExecAssignScanType(scanState, RelationGetDescr(scanState->ss_currentRelation));

		if ( NULL != scanState->ps.plan->targetlist )
		{
			/* Update targetlist to the current partition.
			 * This is important for heterogeneous partitions (e.g. dropped attributes.)
			 */
			scanState->ps.plan->targetlist =
				GetPartitionTargetlist(scanState->ss_currentRelation->rd_att, 
						scanState->ps.plan->targetlist);

			/* Update attribute number in expression target list. */
			UpdateGenericExprState(scanState->ps.plan->targetlist,
					 scanState->ps.targetlist);
		}

		ExecAssignScanProjectionInfo(scanState);
		
		scanState->tableType = getTableType(scanState->ss_currentRelation);
		BeginTableScanRelation(scanState);
	}

	return true;
}

/*
 * setPidIndex
 *   Set the pid index for the given dynamic table.
 */
static void
setPidIndex(DynamicTableScanState *node)
{
	Assert(node->pidIndex == NULL);

	ScanState *scanState = (ScanState *)node;
	EState *estate = scanState->ps.state;
	DynamicTableScan *plan = (DynamicTableScan *)scanState->ps.plan;
	Assert(estate->dynamicTableScanInfo != NULL);
	
	/*
	 * The pidIndexes might be NULL when no partitions are
	 * involved in the query.
	 */
	if (estate->dynamicTableScanInfo->pidIndexes != NULL)
	{
		Assert(estate->dynamicTableScanInfo->numScans > plan->partIndex);
		node->pidIndex = estate->dynamicTableScanInfo->pidIndexes[plan->partIndex];
	}
}

TupleTableSlot *
ExecDynamicTableScan(DynamicTableScanState *node)
{
	ScanState *scanState = (ScanState *)node;
	TupleTableSlot *slot = NULL;

	/*
	 * If this is called the first time, find the pid index that contains all unique
	 * partition pids for this node to scan.
	 */
	if (node->pidIndex == NULL)
	{
		setPidIndex(node);

		if (node->pidIndex == NULL)
		{
			return NULL;
		}
		
		hash_seq_init(&node->pidStatus, node->pidIndex);
		node->shouldCallHashSeqTerm = true;
	}

	/*
	 * Scan the table to find next tuple to return. If the current table
	 * is finished, close it and open the next table for scan.
	 */
	while (TupIsNull(slot) &&
		   initNextTableToScan(node))
	{
		slot = ExecTableScanRelation(scanState);
		
#ifdef FAULT_INJECTOR
    FaultInjector_InjectFaultIfSet(
    		FaultDuringExecDynamicTableScan,
            DDLNotSpecified,
            "",  // databaseName
            ""); // tableName
#endif

		if (!TupIsNull(slot))
		{
			Gpmon_M_Incr_Rows_Out(GpmonPktFromDynamicTableScanState(node));
			CheckSendPlanStateGpmonPkt(&scanState->ps);
		}
		else
		{
			CleanupOnePartition(scanState);
		}
	}

	return slot;
}

/*
 * CleanupOnePartition
 *		Cleans up a partition's relation and releases all locks.
 */
static inline void
CleanupOnePartition(ScanState *scanState)
{
	Assert(NULL != scanState);
	if ((scanState->scan_state & SCAN_SCAN) != 0)
	{
		EndTableScanRelation(scanState);

		Assert(scanState->ss_currentRelation != NULL);
		ExecCloseScanRelation(scanState->ss_currentRelation);
		scanState->ss_currentRelation = NULL;
	}
}

void
ExecEndDynamicTableScan(DynamicTableScanState *node)
{
	CleanupOnePartition((ScanState*)node);

	if (node->shouldCallHashSeqTerm)
	{
		hash_seq_term(&node->pidStatus);
	}
	FreeScanRelationInternal((ScanState *)node);
	EndPlanStateGpmonPkt(&node->tableScanState.ss.ps);
}

void
ExecDynamicTableReScan(DynamicTableScanState *node, ExprContext *exprCtxt)
{
	ReScanRelation((ScanState *)node);

	Gpmon_M_Incr(GpmonPktFromDynamicTableScanState(node), GPMON_DYNAMICTABLESCAN_RESCAN);
	CheckSendPlanStateGpmonPkt(&node->tableScanState.ss.ps);
}

void
ExecDynamicTableMarkPos(DynamicTableScanState *node)
{
	MarkRestrNotAllowed((ScanState *)node);
}

void
ExecDynamicTableRestrPos(DynamicTableScanState *node)
{
	MarkRestrNotAllowed((ScanState *)node);
}

int
ExecCountSlotsDynamicTableScan(DynamicTableScan *node)
{
	return DYNAMIC_TABLE_SCAN_NSLOTS;
}

void
initGpmonPktForDynamicTableScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, DynamicTableScan));

	{
		RangeTblEntry *rte = rt_fetch(((Scan *)planNode)->scanrelid, estate->es_range_table);
		char schema_rel_name[SCAN_REL_NAME_BUF_SIZE] = {0};
		
		Assert(GPMON_DYNAMICTABLESCAN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_DynamicTableScan,
							 (int64) planNode->plan_rows, GetScanRelNameGpmon(rte->relid, schema_rel_name));
	}
}
