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
