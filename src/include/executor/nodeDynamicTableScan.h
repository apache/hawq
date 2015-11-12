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
 * nodeDynamicTableScan.h
 *
 */
#ifndef NODEDYNAMICTABLESCAN_H
#define NODEDYNAMICTABLESCAN_H

#include "nodes/execnodes.h"

extern int	ExecCountSlotsDynamicTableScan(DynamicTableScan *node);
extern DynamicTableScanState *ExecInitDynamicTableScan(DynamicTableScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecDynamicTableScan(DynamicTableScanState *node);
extern void ExecEndDynamicTableScan(DynamicTableScanState *node);
extern void ExecDynamicTableMarkPos(DynamicTableScanState *node);
extern void ExecDynamicTableRestrPos(DynamicTableScanState *node);
extern void ExecDynamicTableReScan(DynamicTableScanState *node, ExprContext *exprCtxt);

enum 
{
	GPMON_DYNAMICTABLESCAN_RESCAN = GPMON_QEXEC_M_NODE_START,
	GPMON_DYNAMICTABLESCAN_TOTAL,
};

static inline gpmon_packet_t * GpmonPktFromDynamicTableScanState(DynamicTableScanState *node)
{
	return &node->tableScanState.ss.ps.gpmon_pkt;
}
#endif
