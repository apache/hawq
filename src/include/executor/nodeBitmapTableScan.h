/*-------------------------------------------------------------------------
 *
 * nodeBitmapTableScan.h
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEBITMAPTABLESCAN_H
#define NODEBITMAPTABLESCAN_H

#include "nodes/execnodes.h"
#include "executor/execDynamicScan.h"

extern int	ExecCountSlotsBitmapTableScan(BitmapTableScan *node);
extern BitmapTableScanState *ExecInitBitmapTableScan(BitmapTableScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecBitmapTableScan(BitmapTableScanState *node);
extern void ExecEndBitmapTableScan(BitmapTableScanState *node);
extern void ExecBitmapTableReScan(BitmapTableScanState *node, ExprContext *exprCtxt);
extern void ExecEagerFreeBitmapTableScan(BitmapTableScanState *node);
extern void BitmapTableScanBeginPartition(ScanState *node, bool initExpressions);
extern void BitmapTableScanEndPartition(ScanState *node);
extern TupleTableSlot *BitmapTableScanFetchNext(ScanState *node);


enum
{
	GPMON_BITMAPTABLESCAN_PAGE = GPMON_QEXEC_M_NODE_START,
    GPMON_BITMAPTABLESCAN_RESCAN,
	GPMON_BITMAPTABLESCAN_TOTAL
};

static inline gpmon_packet_t * GpmonPktFromBitmapTableScanState(BitmapTableScanState *node)
{
	return &node->ss.ps.gpmon_pkt;
}

#endif   /* NODEBITMAPTABLESCAN_H */
