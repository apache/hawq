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
 * nodeDynamicTableScan.c
 *    Support routines for scanning one or more relations that are determined
 * at run time. The relations could be Heap, AppendOnly Row, AppendOnly Columnar.
 *
 * DynamicTableScan node scans each relation one after the other. For each relation,
 * it opens the table, scans the tuple, and returns relevant tuples.
 *
 */
#include "postgres.h"

#include "access/filesplit.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "nodes/execnodes.h"
#include "executor/nodeDynamicTableScan.h"
#include "utils/hsearch.h"
#include "parser/parsetree.h"
#include "utils/faultinjector.h"
#include "commands/tablecmds.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdbvars.h"

#define DYNAMIC_TABLE_SCAN_NSLOTS 2

static inline void
CleanupOnePartition(ScanState *scanState);

DynamicTableScanState *
ExecInitDynamicTableScan(DynamicTableScan *node, EState *estate, int eflags)
{
	Assert((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

	DynamicTableScanState *state = makeNode(DynamicTableScanState);
	
	state->tableScanState.ss.scan_state = SCAN_INIT;

	/* We do not open the relation. We open it later, per-partition. */
	InitScanStateInternal((ScanState *)state, (Plan *)node, estate, eflags, false /* initCurrentRelation */);

	Oid reloid = getrelid(node->scanrelid, estate->es_range_table);

	Assert(OidIsValid(reloid));

	state->firstPartition = true;

	/* lastRelOid is used to remap varattno for heterogeneous partitions */
	state->lastRelOid = reloid;

	state->scanrelid = node->scanrelid;

	/*
	 * This context will be reset per-partition to free up per-partition
	 * qual and targetlist allocations
	 */
	state->partitionMemoryContext = AllocSetContextCreate(CurrentMemoryContext,
									 "DynamicTableScanPerPartition",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);

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
	EState *estate = scanState->ps.state;

	if (scanState->scan_state == SCAN_INIT ||
		scanState->scan_state == SCAN_DONE)
	{
		Oid *pid = hash_seq_search(&node->pidStatus);
		if (pid == NULL)
		{
			node->shouldCallHashSeqTerm = false;
			return false;
		}
		
		/* Collect number of partitions scanned in EXPLAIN ANALYZE */
		if (NULL != scanState->ps.instrument)
		{
			Instrumentation *instr = scanState->ps.instrument;
			instr->numPartScanned ++;
		}

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

		scanState->ss_currentRelation = OpenScanRelationByOid(*pid);

		/*
		 * Setup the splits for table scan of the current
		 * relation.
		 */
		{
			if (RelationIsAoRows(scanState->ss_currentRelation) || RelationIsParquet(scanState->ss_currentRelation))
			{
				scanState->splits = GetFileSplitsOfSegment(
						estate->es_plannedstmt->scantable_splits,
						scanState->ss_currentRelation->rd_id, GetQEIndex());
			}

		}

		Relation lastScannedRel = OpenScanRelationByOid(node->lastRelOid);
		TupleDesc lastTupDesc = RelationGetDescr(lastScannedRel);
		CloseScanRelation(lastScannedRel);

		TupleDesc partTupDesc = RelationGetDescr(scanState->ss_currentRelation);

		ExecAssignScanType(scanState, partTupDesc);

		AttrNumber	*attMap = NULL;

		attMap = varattnos_map(lastTupDesc, partTupDesc);

		/* If attribute remapping is not necessary, then do not change the varattno */
		if (attMap)
		{
			change_varattnos_of_a_varno((Node*)scanState->ps.plan->qual, attMap, node->scanrelid);
			change_varattnos_of_a_varno((Node*)scanState->ps.plan->targetlist, attMap, node->scanrelid);

			/*
			 * Now that the varattno mapping has been changed, change the relation that
			 * the new varnos correspond to
			 */
			node->lastRelOid = *pid;
		}

		/*
		 * For the very first partition, the targetlist of planstate is set to null. So, we must
		 * initialize quals and targetlist, regardless of remapping requirements. For later
		 * partitions, we only initialize quals and targetlist if a column re-mapping is necessary.
		 */
		if (attMap || node->firstPartition)
		{
			node->firstPartition = false;
			MemoryContextReset(node->partitionMemoryContext);
			MemoryContext oldCxt = MemoryContextSwitchTo(node->partitionMemoryContext);

			/* Initialize child expressions */
			scanState->ps.qual = (List *)ExecInitExpr((Expr *)scanState->ps.plan->qual, (PlanState*)scanState);
			scanState->ps.targetlist = (List *)ExecInitExpr((Expr *)scanState->ps.plan->targetlist, (PlanState*)scanState);

			MemoryContextSwitchTo(oldCxt);
		}

		if (attMap)
		{
			pfree(attMap);
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
	 * Ensure that the dynahash exists even if the partition selector
	 * didn't choose any partition for current scan node [MPP-24169].
	 */
	InsertPidIntoDynamicTableScanInfo(plan->partIndex, InvalidOid, InvalidPartitionSelectorId);

	Assert(NULL != estate->dynamicTableScanInfo->pidIndexes);
	Assert(estate->dynamicTableScanInfo->numScans >= plan->partIndex);
	node->pidIndex = estate->dynamicTableScanInfo->pidIndexes[plan->partIndex - 1];
	Assert(node->pidIndex != NULL);

	if (optimizer_partition_selection_log)
	{
		LogSelectedPartitionOids(node->pidIndex);
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
		Assert(node->pidIndex != NULL);
		
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

/*
 * DynamicTableScanEndCurrentScan
 *		Cleans up any ongoing scan.
 */
static void
DynamicTableScanEndCurrentScan(DynamicTableScanState *node)
{
	CleanupOnePartition((ScanState*)node);

	if (node->shouldCallHashSeqTerm)
	{
		hash_seq_term(&node->pidStatus);
		node->shouldCallHashSeqTerm = false;
	}
}

/*
 * ExecEndDynamicTableScan
 *		Ends the scanning of this DynamicTableScanNode and frees
 *		up all the resources.
 */
void
ExecEndDynamicTableScan(DynamicTableScanState *node)
{
	DynamicTableScanEndCurrentScan(node);

	/* We do not close the relation. We closed it in DynamicScan_CleanupOneRelation. */
	FreeScanRelationInternal((ScanState *)node, false /* closeCurrentRelation */);
	EndPlanStateGpmonPkt(&node->tableScanState.ss.ps);
}

/*
 * ExecDynamicTableReScan
 *		Prepares the internal states for a rescan.
 */
void
ExecDynamicTableReScan(DynamicTableScanState *node, ExprContext *exprCtxt)
{
	DynamicTableScanEndCurrentScan(node);

	/* Force reloading the partition hash table */
	node->pidIndex = NULL;

	ExprContext *econtext = node->tableScanState.ss.ps.ps_ExprContext;

	if (econtext)
	{
		/*
		 * If we are being passed an outer tuple, save it for any expression
		 * evaluation that may refer to the outer tuple.
		 */
		if (exprCtxt != NULL)
		{
			econtext->ecxt_outertuple = exprCtxt->ecxt_outertuple;
		}

		/*
		 * Reset the expression context so we don't leak memory as each outer
		 * tuple is scanned.
		 */
		ResetExprContext(econtext);
	}

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
