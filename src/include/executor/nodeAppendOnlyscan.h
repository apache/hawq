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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * nodeAppendOnlyscan.h
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2008, Greenplum Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEAPPENDONLYSCAN_H
#define NODEAPPENDONLYSCAN_H

#include "nodes/execnodes.h"

extern int	ExecCountSlotsAppendOnlyScan(AppendOnlyScan *node);
extern AppendOnlyScanState *ExecInitAppendOnlyScan(AppendOnlyScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecAppendOnlyScan(AppendOnlyScanState *node);
extern void ExecEndAppendOnlyScan(AppendOnlyScanState *node);
extern void ExecAppendOnlyReScan(AppendOnlyScanState *node, ExprContext *exprCtxt);
extern void ExecEagerFreeAppendOnlyScan(AppendOnlyScanState *node);

enum 
{
	GPMON_APPONLYSCAN_RESCAN = GPMON_QEXEC_M_NODE_START,
	GPMON_APPONLYSCAN_TOTAL,
};

static inline gpmon_packet_t * GpmonPktFromAppOnlyScanState (AppendOnlyScanState *node)
{
	return &node->ss.ps.gpmon_pkt;
}
#endif   /* NODEAPPENDONLYSCAN_H */
