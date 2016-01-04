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
 * nodeBitmapAppendOnlyscan.h
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2008-2009, Greenplum Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEBITMAPAPPENDONLYSCAN_H
#define NODEBITMAPAPPENDONLYSCAN_H

#include "nodes/execnodes.h"

extern int	ExecCountSlotsBitmapAppendOnlyScan(BitmapAppendOnlyScan *node);
extern BitmapAppendOnlyScanState *ExecInitBitmapAppendOnlyScan(BitmapAppendOnlyScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecBitmapAppendOnlyScan(BitmapAppendOnlyScanState *node);
extern void ExecEndBitmapAppendOnlyScan(BitmapAppendOnlyScanState *node);
extern void ExecBitmapAppendOnlyReScan(BitmapAppendOnlyScanState *node, ExprContext *exprCtxt);
extern void ExecEagerFreeBitmapAppendOnlyScan(BitmapAppendOnlyScanState *node);

enum
{
	GPMON_BITMAPAPPENDONLYSCAN_PAGE = GPMON_QEXEC_M_NODE_START, 
    	GPMON_BITMAPAPPENDONLYSCAN_RESCAN,
	GPMON_BITMAPAPPENDONLYSCAN_TOTAL
};

static inline gpmon_packet_t * GpmonPktFromBitmapAppendOnlyScanState(BitmapAppendOnlyScanState *node)
{
	return &node->ss.ps.gpmon_pkt;
}

#endif   /* NODEBITMAPAPPENDONLYSCAN_H */

