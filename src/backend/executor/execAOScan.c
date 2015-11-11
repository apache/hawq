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
 * execAppendOnlyScan.c
 *   Support routines for scanning AppendOnly tables.
 *
 */
#include "postgres.h"

#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "cdb/cdbappendonlyam.h"

TupleTableSlot *
AppendOnlyScanNext(ScanState *scanState)
{
	Assert(IsA(scanState, TableScanState) ||
		   IsA(scanState, DynamicTableScanState));
	AppendOnlyScanState *node = (AppendOnlyScanState *)scanState;
	
	AppendOnlyScanDesc scandesc;
	Index		scanrelid;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	Assert((node->ss.scan_state & SCAN_SCAN) != 0);
	/*
	 * get information from the estate and scan state
	 */
	estate = node->ss.ps.state;
	scandesc = node->aos_ScanDesc;
	scanrelid = ((AppendOnlyScan *) node->ss.ps.plan)->scan.scanrelid;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	/*
	 * put the next tuple from the access methods in our tuple slot
	 */
	appendonly_getnext(scandesc, direction, slot);

	return slot;
}

void
BeginScanAppendOnlyRelation(ScanState *scanState)
{
	Assert(IsA(scanState, TableScanState) ||
		   IsA(scanState, DynamicTableScanState));
	AppendOnlyScanState *node = (AppendOnlyScanState *)scanState;
	
	Assert(node->ss.scan_state == SCAN_INIT ||
		   node->ss.scan_state == SCAN_DONE);
	Assert(node->aos_ScanDesc == NULL);

	node->aos_ScanDesc = appendonly_beginscan(
			node->ss.ss_currentRelation, 
			node->ss.ps.state->es_snapshot, 
			0, NULL);

	node->aos_ScanDesc->splits = scanState->splits;
	node->ss.scan_state = SCAN_SCAN;
}

void
EndScanAppendOnlyRelation(ScanState *scanState)
{
	Assert(IsA(scanState, TableScanState) ||
		   IsA(scanState, DynamicTableScanState));
	AppendOnlyScanState *node = (AppendOnlyScanState *)scanState;
	Assert(node->aos_ScanDesc != NULL);

	Assert((node->ss.scan_state & SCAN_SCAN) != 0);
	appendonly_endscan(node->aos_ScanDesc);

	node->aos_ScanDesc = NULL;
	
	node->ss.scan_state = SCAN_INIT;
}

void
ReScanAppendOnlyRelation(ScanState *scanState)
{
	Assert(IsA(scanState, TableScanState) ||
		   IsA(scanState, DynamicTableScanState));
	AppendOnlyScanState *node = (AppendOnlyScanState *)scanState;
	Assert(node->aos_ScanDesc != NULL);

	appendonly_rescan(node->aos_ScanDesc, NULL /* new scan keys */);
}
