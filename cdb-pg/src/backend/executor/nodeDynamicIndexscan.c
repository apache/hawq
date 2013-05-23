/*
 * nodeDynamicIndexScan.c 
 *
 * Copyright (c) 2013 - present, EMC/Greenplum
 */

#include "postgres.h"

#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "executor/execIndexscan.h"
#include "executor/nodeDynamicIndexscan.h"

/* Number of slots required for DynamicIndexScan */
#define DYNAMICINDEXSCAN_NSLOTS 2


/*
 * Account for the number of tuple slots required for DynamicIndexScan
 */
extern int 
ExecCountSlotsDynamicIndexScan(DynamicIndexScan *node)
{
	return ExecCountSlotsNode(outerPlan((Plan *) node)) +
		ExecCountSlotsNode(innerPlan((Plan *) node)) + DYNAMICINDEXSCAN_NSLOTS;;
}

/*
 * Initialize ScanState in DynamicIndexScan.
 */
extern DynamicIndexScanState *
ExecInitDynamicIndexScan(DynamicIndexScan *node, EState *estate, int eflags)
{
	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND)));

	DynamicIndexScanState *dynamicIndexScanState = makeNode(DynamicIndexScanState);

	InitCommonIndexScanState((IndexScanState *)dynamicIndexScanState, (IndexScan *)node, estate, eflags);

	return dynamicIndexScanState;
}

/*
 * Execution of DynamicIndexScan
 * TODO: 4/5/2013 garcic12: Implement execution of DynamicIndexScan
 */
extern TupleTableSlot *
ExecDynamicIndexScan(DynamicIndexScanState *node)
{
	return NULL;
}

/*
 * Release resource of DynamicIndexScan
 * TODO: 4/5/2013 garcic12: Implement end of DynamicIndexScan
 */
extern void 
ExecEndDynamicIndexScan(DynamicIndexScanState *node)
{
	return;
}

/*
 * Allow rescanning an index.
 * TODO: 4/5/2013 garcic12: Implement index rescanning
 */
extern void 
ExecDynamicIndexReScan(DynamicIndexScanState *node, ExprContext *exprCtxt)
{
	return;
}

/*
 * Method for reporting DynamicIndexScan progress to gpperfmon
 * TODO: 4/5/2013 garcic12: Implement initGpmonPktForDynamicIndexScan
 */
void
initGpmonPktForDynamicIndexScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	return;
}
