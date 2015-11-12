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
 * nodeDynamicIndexScan.h
 */
#ifndef NODEDYNAMICINDEXSCAN_H
#define NODEDYNAMICINDEXSCAN_H

#include "nodes/execnodes.h"

extern int ExecCountSlotsDynamicIndexScan(DynamicIndexScan *node);
extern DynamicIndexScanState *ExecInitDynamicIndexScan(DynamicIndexScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecDynamicIndexScan(DynamicIndexScanState *node);
extern void ExecEndDynamicIndexScan(DynamicIndexScanState *node);
extern void ExecDynamicIndexReScan(DynamicIndexScanState *node, ExprContext *exprCtxt);

enum 
{
	GPMON_DYNAMICINDEXSCAN_RESCAN = GPMON_QEXEC_M_NODE_START,
	GPMON_DYNAMICINDEXSCAN_TOTAL,
};

static inline gpmon_packet_t * GpmonPktFromDynamicIndexScanState(DynamicIndexScanState *node)
{
	return &node->indexScanState.ss.ps.gpmon_pkt;
}
#endif

