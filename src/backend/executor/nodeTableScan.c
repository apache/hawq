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
 * nodeTableScan.c
 *    Support routines for scanning a relation. This relation can be Heap,
 * AppendOnly Row, or AppendOnly Columnar.
 *
 */
#include "postgres.h"

#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "executor/nodeTableScan.h"
#include "utils/elog.h"
#include "parser/parsetree.h"

#define TABLE_SCAN_NSLOTS 2

TableScanState *
ExecInitTableScan(TableScan *node, EState *estate, int eflags)
{
	TableScanState *state = makeNode(TableScanState);
	state->ss.scan_state = SCAN_INIT;

	InitScanStateInternal((ScanState *)state, (Plan *)node, estate, eflags, true /* initCurrentRelation */);
	
	initGpmonPktForTableScan((Plan *)node, &state->ss.ps.gpmon_pkt, estate);

	return state;
}

TupleTableSlot *
ExecTableScan(TableScanState *node)
{
	ScanState *scanState = (ScanState *)node;

	if (scanState->scan_state == SCAN_INIT ||
		scanState->scan_state == SCAN_DONE)
	{
		BeginTableScanRelation(scanState);
	}

	TupleTableSlot *slot = ExecTableScanRelation(scanState);
	
	if (!TupIsNull(slot))
	{
		Gpmon_M_Incr_Rows_Out(GpmonPktFromTableScanState(node));
		CheckSendPlanStateGpmonPkt(&scanState->ps);
	}
	
	else if (!scanState->ps.delayEagerFree)
	{
		EndTableScanRelation(scanState);
	}

	return slot;
}

void
ExecEndTableScan(TableScanState *node)
{
	if ((node->ss.scan_state & SCAN_SCAN) != 0)
	{
		EndTableScanRelation(&(node->ss));
	}

	FreeScanRelationInternal((ScanState *)node, true /* closeCurrentRelation */);
	EndPlanStateGpmonPkt(&node->ss.ps);
}

void
ExecTableReScan(TableScanState *node, ExprContext *exprCtxt)
{
	ReScanRelation((ScanState *)node);

	Gpmon_M_Incr(GpmonPktFromTableScanState(node), GPMON_TABLESCAN_RESCAN);
	CheckSendPlanStateGpmonPkt(&node->ss.ps);
}

void
ExecTableMarkPos(TableScanState *node)
{
	MarkPosScanRelation((ScanState *)node);
}

void
ExecTableRestrPos(TableScanState *node)
{
	RestrPosScanRelation((ScanState *)node);

	Gpmon_M_Incr(GpmonPktFromTableScanState(node), GPMON_TABLESCAN_RESTOREPOS);
	CheckSendPlanStateGpmonPkt(&node->ss.ps);
}

int
ExecCountSlotsTableScan(TableScan *node)
{
	return TABLE_SCAN_NSLOTS;
}

void
initGpmonPktForTableScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL);
	Assert(IsA(planNode, TableScan) ||
		   IsA(planNode, SeqScan) ||
		   IsA(planNode, AppendOnlyScan) ||
		   IsA(planNode, ParquetScan));

	RangeTblEntry *rte = rt_fetch(((Scan *)planNode)->scanrelid, estate->es_range_table);
	char schema_rel_name[SCAN_REL_NAME_BUF_SIZE] = {0};
	
	Assert(GPMON_TABLESCAN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
	InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_TableScan,
						 (int64) planNode->plan_rows, GetScanRelNameGpmon(rte->relid, schema_rel_name));
}

void
ExecEagerFreeTableScan(TableScanState *node)
{
	if (node->ss.scan_state != SCAN_INIT &&
		node->ss.scan_state != SCAN_DONE)
	{
		EndTableScanRelation((ScanState *)node);
	}
}
