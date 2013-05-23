/*
 * nodeDynamicTableScan.c
 *    Support routines for scanning one or more relations that are determined
 * at run time. The relations could be Heap, AppendOnly Row, AppendOnly Columnar.
 *
 * DynamicTableScan node scans each relation one after the other. For each relation,
 * it opens the table, scans the tuple, and returns relevant tuples.
 *
 * Copyright (c) 2012 - present, EMC/Greenplum
 */
#include "postgres.h"

#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "executor/nodeDynamicTableScan.h"
#include "utils/hsearch.h"
#include "utils/elog.h"
#include "parser/parsetree.h"
#include "nodes/makefuncs.h"
#include "access/sysattr.h"

#define DYNAMIC_TABLE_SCAN_NSLOTS 2

DynamicTableScanState *
ExecInitDynamicTableScan(DynamicTableScan *node, EState *estate, int eflags)
{
	Assert((eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

	DynamicTableScanState *state = makeNode(DynamicTableScanState);
	
	InitScanStateInternal((ScanState *)state, (Plan *)node, estate, eflags);

	initGpmonPktForDynamicTableScan((Plan *)node, &state->tableScanState.ss.ps.gpmon_pkt, estate);

	return state;
}

/*
 * Constructs a new targetlist list that maps to a tuple descriptor.
 */
static List *
GetPartitionTargetlist(TupleDesc partDescr, List *targetlist)
{
	Assert(NIL != targetlist);
	Assert(partDescr);

	List *partitionTargetlist = NIL;

	AttrMap *attrmap = NULL;

	TupleDesc targetDescr = ExecTypeFromTL(targetlist, false);

	map_part_attrs_from_targetdesc(targetDescr, partDescr, &attrmap);
	
	ListCell *entry = NULL;
	int pos = 1;
	foreach(entry, targetlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(entry);

		/* Obtain corresponding attribute number in the part (this will be the resno). */
		int partAtt = (int)attrMap(attrmap, pos);

		/* A system attribute should be added to the target list with its original
		 * attribute number.
		 */
		if (te->resorigcol < 0)
		{
			/* te->resorigcol should be equivalent to ((Var *)te->expr)->varattno.
			 * te->resorigcol is used for simplicity.
			 */
			Assert(((Var *)te->expr)->varattno == te->resorigcol);

			/* Validate interval for system-defined attributes. */
			Assert(te->resorigcol > FirstLowInvalidHeapAttributeNumber &&
				te->resorigcol <= SelfItemPointerAttributeNumber);
 
			partAtt = te->resorigcol;
		}

		TargetEntry *newTe = flatCopyTargetEntry(te);

		/* Parts are not explicitly specified in the range table. Therefore, the original RTE index is kept. */
		Index rteIdx = ((Var *)te->expr)->varno;
		/* Variable expression required by the Target Entry. */
		Var *var = makeVar(rteIdx,
				partAtt,
				targetDescr->attrs[pos-1]->atttypid,
				targetDescr->attrs[pos-1]->atttypmod,
				0 /* resjunk */);
		

		/* Modify resno in the new TargetEntry */
		newTe->resno = partAtt;
		newTe->expr = (Expr *) var;

		partitionTargetlist = lappend(partitionTargetlist, newTe);

		pos++; 
	}
	
	Assert(attrmap);
	pfree(attrmap);
	Assert(partitionTargetlist);

	return partitionTargetlist;
}

/*
 * Replace all attribute numbers to the corresponding mapped value (resno)
 *  in GenericExprState list with the attribute numbers in the  target list. 
 */
static void
UpdateGenericExprState(List *teTargetlist, List *geTargetlist)
{
	Assert(list_length(teTargetlist) ==
		list_length(geTargetlist));

	ListCell   *ge = NULL;
	ListCell   *te = NULL;

	forboth(te, teTargetlist, ge, geTargetlist)
	{
		GenericExprState *gstate = (GenericExprState *)ge->data.ptr_value;
		TargetEntry *tle = (TargetEntry *)te->data.ptr_value;

		Var *variable = (Var *) gstate->arg->expr;
		variable->varattno = tle->resno;
	}
}

/*
 * initNextTableToScan
 *   Find the next table to scan and initiate the scan if the previous table
 * is finished.
 *
 * If scanning on the current table is not finished, or a new table is found,
 * this function returns true.
 * If no more table is found, this function returns false.
 */
static bool
initNextTableToScan(DynamicTableScanState *node)
{
	ScanState *scanState = (ScanState *)node;

	if (scanState->scan_state == SCAN_INIT ||
		scanState->scan_state == SCAN_DONE)
	{
		Oid *pid = hash_seq_search(&node->pidStatus);
		if (pid == NULL)
		{
			return false;
		}
		
		scanState->ss_currentRelation = OpenScanRelationByOid(*pid);
		ExecAssignScanType(scanState, RelationGetDescr(scanState->ss_currentRelation));

		if ( NULL != scanState->ps.plan->targetlist )
		{
			/* Update targetlist to the current partition.
			 * This is important for heterogeneous partitions (e.g. dropped attributes.)
			 */
			scanState->ps.plan->targetlist =
				GetPartitionTargetlist(scanState->ss_currentRelation->rd_att, 
						scanState->ps.plan->targetlist);

			/* Update attribute number in expression target list. */
			UpdateGenericExprState(scanState->ps.plan->targetlist,
					 scanState->ps.targetlist);
		}

		ExecAssignScanProjectionInfo(scanState);
		
		scanState->tableType = getTableType(scanState->ss_currentRelation);
		BeginTableScanRelation(scanState);
	}

	return true;
}

/*
 * setPidIndex
 *   Set the pid index for the given dynamic table.
 */
static void
setPidIndex(DynamicTableScanState *node)
{
	Assert(node->pidIndex == NULL);

	ScanState *scanState = (ScanState *)node;
	EState *estate = scanState->ps.state;
	DynamicTableScan *plan = (DynamicTableScan *)scanState->ps.plan;
	Assert(estate->dynamicTableScanInfo != NULL);
	
	/*
	 * The pidIndexes might be NULL when no partitions are
	 * involved in the query.
	 */
	if (estate->dynamicTableScanInfo->pidIndexes != NULL)
	{
		Assert(estate->dynamicTableScanInfo->numScans > plan->partIndex);
		node->pidIndex = estate->dynamicTableScanInfo->pidIndexes[plan->partIndex];
	}
}

TupleTableSlot *
ExecDynamicTableScan(DynamicTableScanState *node)
{
	ScanState *scanState = (ScanState *)node;
	TupleTableSlot *slot = NULL;

	/*
	 * If this is called the first time, find the pid index that contains all unique
	 * partition pids for this node to scan.
	 */
	if (node->pidIndex == NULL)
	{
		setPidIndex(node);

		if (node->pidIndex == NULL)
		{
			return NULL;
		}
		
		hash_seq_init(&node->pidStatus, node->pidIndex);
	}

	/*
	 * Scan the table to find next tuple to return. If the current table
	 * is finished, close it and open the next table for scan.
	 */
	while (TupIsNull(slot) &&
		   initNextTableToScan(node))
	{
		slot = ExecTableScanRelation(scanState);
		
		if (!TupIsNull(slot))
		{
			Gpmon_M_Incr_Rows_Out(GpmonPktFromDynamicTableScanState(node));
			CheckSendPlanStateGpmonPkt(&scanState->ps);
		}
		
		else
		{
			EndTableScanRelation(scanState);

			Assert(scanState->ss_currentRelation != NULL);
			ExecCloseScanRelation(scanState->ss_currentRelation);
			scanState->ss_currentRelation = NULL;
		}
	}

	return slot;
}

void
ExecEndDynamicTableScan(DynamicTableScanState *node)
{
	FreeScanRelationInternal((ScanState *)node);
	EndPlanStateGpmonPkt(&node->tableScanState.ss.ps);
}

void
ExecDynamicTableReScan(DynamicTableScanState *node, ExprContext *exprCtxt)
{
	ReScanRelation((ScanState *)node);

	Gpmon_M_Incr(GpmonPktFromDynamicTableScanState(node), GPMON_DYNAMICTABLESCAN_RESCAN);
	CheckSendPlanStateGpmonPkt(&node->tableScanState.ss.ps);
}

void
ExecDynamicTableMarkPos(DynamicTableScanState *node)
{
	MarkRestrNotAllowed((ScanState *)node);
}

void
ExecDynamicTableRestrPos(DynamicTableScanState *node)
{
	MarkRestrNotAllowed((ScanState *)node);
}

int
ExecCountSlotsDynamicTableScan(DynamicTableScan *node)
{
	return DYNAMIC_TABLE_SCAN_NSLOTS;
}

void
initGpmonPktForDynamicTableScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, DynamicTableScan));

	{
		RangeTblEntry *rte = rt_fetch(((Scan *)planNode)->scanrelid, estate->es_range_table);
		char schema_rel_name[SCAN_REL_NAME_BUF_SIZE] = {0};
		
		Assert(GPMON_DYNAMICTABLESCAN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_DynamicTableScan,
							 (int64) planNode->plan_rows, GetScanRelNameGpmon(rte->relid, schema_rel_name));
	}
}
