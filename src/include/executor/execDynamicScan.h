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
/*--------------------------------------------------------------------------
 *
 * execDynamicScan.h
 *	 Definitions and API functions for execDynamicScan.c
 *
 *--------------------------------------------------------------------------
 */
#ifndef EXECDYNAMICSCAN_H
#define EXECDYNAMICSCAN_H

#include "nodes/execnodes.h"
#include "utils/hsearch.h"
#include "utils/palloc.h"
#include "executor/tuptable.h"
#include "commands/tablecmds.h"

#define DYNAMIC_SCAN_NSLOTS 2

typedef void (PartitionInitMethod)(ScanState *scanState, bool initExpressions);
typedef void (PartitionEndMethod)(ScanState *scanState);
typedef TupleTableSlot * (PartitionScanTupleMethod)(ScanState *scanState);

extern void
DynamicScan_Begin(ScanState *scanState, Plan *plan, EState *estate, int eflags);

extern void
DynamicScan_End(ScanState *scanState, PartitionEndMethod *partitionEndMethod);

extern void
DynamicScan_ReScan(ScanState *scanState, PartitionEndMethod *partitionEndMethod, ExprContext *exprCtxt);

extern bool
DynamicScan_InitNextRelation(ScanState *scanState, PartitionEndMethod *partitionEndMethod, PartitionInitMethod *partitionInitMethod);

extern TupleTableSlot *
DynamicScan_GetNextTuple(ScanState *scanState, PartitionEndMethod *partitionEndMethod,
		PartitionInitMethod *partitionInitMethod, PartitionScanTupleMethod *partitionScanTupleMethod);

extern MemoryContext
DynamicScan_GetPartitionMemoryContext(ScanState *scanState);

extern AttrNumber*
DynamicScan_GetColumnMapping(Oid oldOid, Oid newOid);

extern Relation
DynamicScan_GetCurrentRelation(ScanState *scanState);

extern bool
IndexScan_MapLogicalIndexInfo(LogicalIndexInfo *logicalIndexInfo, AttrNumber *attMap, Index varno);

extern void
DynamicScan_BeginIndexScan(IndexScanState *indexScanState, bool initQual, bool initTargetList, bool supportsArrayKeys);

extern bool
DynamicScan_BeginIndexPartition(IndexScanState *indexScanState, bool initQual, bool initTargetList, bool supportsArrayKeys, bool isMultiScan);

extern void
DynamicScan_EndIndexPartition(IndexScanState *indexScanState);

extern void
DynamicScan_RescanIndex(IndexScanState *indexScanState, ExprContext *exprCtxt, bool initQual, bool initTargetList, bool supportsArrayKeys);

extern void
DynamicScan_EndIndexScan(IndexScanState *indexScanState);

#endif   /* EXECDYNAMICSCAN_H */
