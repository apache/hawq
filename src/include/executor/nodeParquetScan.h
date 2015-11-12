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
 * nodeParquetScan.h
 *
 *  Created on: Jul 4, 2013
 *      Author: malili
 */

#ifndef NODEPARQUETSCAN_H_
#define NODEPARQUETSCAN_H_

#include "nodes/execnodes.h"

extern int ExecCountSlotsParquetScan(ParquetScan *node);
extern ParquetScanState *ExecInitParquetScan(ParquetScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecParquetScan(ParquetScanState *node);
extern void ExecEndParquetScan(ParquetScanState *node);
extern void ExecParquetReScan(ParquetScanState *node, ExprContext *exprCtxt);
extern void ExecEagerFreeParquetScan(ParquetScanState *node);

enum
{
	GPMON_ParquetSCAN_RESCAN = GPMON_QEXEC_M_NODE_START,
	GPMON_ParquetSCAN_TOTAL,
};

static inline gpmon_packet_t * GpmonPktFromParquetScanState (ParquetScanState *node)
{
	return &node->ss.ps.gpmon_pkt;
}


#endif /* NODEPARQUETSCAN_H_ */
