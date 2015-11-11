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
 * execParquetScan.c
 *
 *  Created on: Oct 11, 2013
 *      Author: malili
 */
#include "postgres.h"

#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "cdb/cdbparquetam.h"

static void
InitParquetScanOpaque(ScanState *scanState)
{
	ParquetScanState *state = (ParquetScanState *)scanState;
	Assert(state->opaque == NULL);
	state->opaque = palloc(sizeof(ParquetScanOpaqueData));

	/* Initialize Parquet projection info */
	ParquetScanOpaqueData *opaque = (ParquetScanOpaqueData *)state->opaque;
	Relation currentRelation = scanState->ss_currentRelation;
	Assert(currentRelation != NULL);

	opaque->ncol = currentRelation->rd_att->natts;
	opaque->proj = palloc0(sizeof(bool) * opaque->ncol);
	GetNeededColumnsForScan((Node *)scanState->ps.plan->targetlist, opaque->proj, opaque->ncol);
	GetNeededColumnsForScan((Node *)scanState->ps.plan->qual, opaque->proj, opaque->ncol);

	int i = 0;
	for (i = 0; i < opaque->ncol; i++)
	{
		if (opaque->proj[i])
		{
			break;
		}
	}

	/*
	 * In some cases (for example, count(*)), no columns are specified.
	 * We always scan the first column.
	 */
	if (i == opaque->ncol)
	{
		opaque->proj[0] = true;
	}
}

static void
FreeParquetScanOpaque(ScanState *scanState)
{
	ParquetScanState *state = (ParquetScanState *)scanState;
	Assert(state->opaque != NULL);

	ParquetScanOpaqueData *opaque = (ParquetScanOpaqueData *)state->opaque;
	Assert(opaque->proj != NULL);
	pfree(opaque->proj);
	pfree(state->opaque);
	state->opaque = NULL;
}


TupleTableSlot *
ParquetScanNext(ScanState *scanState)
{
	Assert(IsA(scanState, TableScanState) ||
		   IsA(scanState, DynamicTableScanState));
	ParquetScanState *node = (ParquetScanState *)scanState;
	Assert(node->opaque != NULL &&
		   node->opaque->scandesc != NULL);

	parquet_getnext(node->opaque->scandesc, node->ss.ps.state->es_direction, node->ss.ss_ScanTupleSlot);
	return node->ss.ss_ScanTupleSlot;
}

void
BeginScanParquetRelation(ScanState *scanState)
{
	Assert(IsA(scanState, TableScanState) ||
		   IsA(scanState, DynamicTableScanState));
	ParquetScanState *node = (ParquetScanState *)scanState;

	Assert(node->ss.scan_state == SCAN_INIT || node->ss.scan_state == SCAN_DONE);

	InitParquetScanOpaque(scanState);

	node->opaque->scandesc = parquet_beginscan(
			node->ss.ss_currentRelation,
			node->ss.ps.state->es_snapshot,
			NULL /* relationTupleDesc */,
			node->opaque->proj);

	node->opaque->scandesc->splits = scanState->splits;
	node->ss.scan_state = SCAN_SCAN;
}

void
EndScanParquetRelation(ScanState *scanState)
{
	Assert(IsA(scanState, TableScanState) ||
		   IsA(scanState, DynamicTableScanState));
	ParquetScanState *node = (ParquetScanState *)scanState;

	Assert((node->ss.scan_state & SCAN_SCAN) != 0);
	Assert(node->opaque != NULL &&
		   node->opaque->scandesc != NULL);

	parquet_endscan(node->opaque->scandesc);

	FreeParquetScanOpaque(scanState);

	node->ss.scan_state = SCAN_INIT;
}

void
ReScanParquetRelation(ScanState *scanState)
{
	Assert(IsA(scanState, TableScanState) ||
		   IsA(scanState, DynamicTableScanState));
	ParquetScanState *node = (ParquetScanState *)scanState;
	Assert(node->opaque != NULL &&
		   node->opaque->scandesc != NULL);

	parquet_rescan(node->opaque->scandesc);
}

