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
 * nodeTableScan.h
 *
 */
#ifndef NODETABLESCAN_H
#define NODETABLESCAN_H

#include "nodes/execnodes.h"

extern int	ExecCountSlotsTableScan(TableScan *node);
extern TableScanState *ExecInitTableScan(TableScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecTableScan(TableScanState *node);
extern void ExecEndTableScan(TableScanState *node);
extern void ExecTableMarkPos(TableScanState *node);
extern void ExecTableRestrPos(TableScanState *node);
extern void ExecTableReScan(TableScanState *node, ExprContext *exprCtxt);
extern void ExecEagerFreeTableScan(TableScanState *node);

enum 
{
	GPMON_TABLESCAN_PAGE = GPMON_QEXEC_M_NODE_START,
	GPMON_TABLESCAN_RESTOREPOS,
	GPMON_TABLESCAN_RESCAN,
	GPMON_TABLESCAN_TOTAL,
};

static inline gpmon_packet_t * GpmonPktFromTableScanState(TableScanState *node)
{
	return &node->ss.ps.gpmon_pkt;
}

#endif
