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
/*-------------------------------------------------------------------------
 *
 * nodeExternalscan.c
 *	  Support routines for scans of external relations (on flat files for example)
 *
 *-------------------------------------------------------------------------
 */

/*
 * INTERFACE ROUTINES
 *		ExecExternalScan				sequentially scans a relation.
 *		ExecExternalNext				retrieve next tuple in sequential order.
 *		ExecInitExternalScan			creates and initializes a externalscan node.
 *		ExecEndExternalScan				releases any storage allocated.
 *		ExecStopExternalScan			closes external resources before EOD.
 *		ExecExternalReScan				rescans the relation
 */
#include "postgres.h"
#include "fmgr.h"

#include "access/fileam.h"
#include "access/filesplit.h"
#include "access/heapam.h"
#include "access/plugstorage.h"
#include "cdb/cdbvars.h"
#include "executor/execdebug.h"
#include "executor/nodeExternalscan.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "parser/parsetree.h"
#include "optimizer/var.h"

static TupleTableSlot *ExternalNext(ExternalScanState *node);

/* ----------------------------------------------------------------
*						Scan Support
* ----------------------------------------------------------------
*/
/* ----------------------------------------------------------------
*		ExternalNext
*
*		This is a workhorse for ExecExtScan
* ----------------------------------------------------------------
*/
static TupleTableSlot *
ExternalNext(ExternalScanState *node)
{
	FileScanDesc scandesc;
	Index scanrelid;
	EState *estate = NULL;
	ScanDirection direction;
	TupleTableSlot *slot = NULL;
	ExternalSelectDesc externalSelectDesc = NULL;
	bool returnTuple = false;

	/*
	 * get information from the estate and scan state
	 */
	estate = node->ss.ps.state;
	scandesc = node->ess_ScanDesc;
	scanrelid = ((ExternalScan *) node->ss.ps.plan)->scan.scanrelid;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	/*
	 * get the next tuple from the file access methods
	 */
	if (scandesc->fs_formatter_type == ExternalTableType_Invalid)
	{
		elog(ERROR, "invalid formatter type for external table: %s", __func__);
	}
	else if (scandesc->fs_formatter_type != ExternalTableType_PLUG)
	{
		externalSelectDesc = external_getnext_init(&(node->ss.ps), node);

		returnTuple = external_getnext(scandesc, direction, externalSelectDesc,
		                               &(node->ss), slot);
	}
	else
	{
		Assert(scandesc->fs_formatter_name);

		FmgrInfo *getnextInitFunc = scandesc->fs_ps_scan_funcs.getnext_init;

		if (getnextInitFunc)
		{
			/*
			 * pg_strncasecmp(scandesc->fs_formatter_name, "orc", strlen("orc"))
			 * Performance improvement for string comparison.
			 */
			const char *formatter_name = "orc";
			if (*(int *)(scandesc->fs_formatter_name) != *(int *)formatter_name)
			{
				externalSelectDesc =
					InvokePlugStorageFormatGetNextInit(getnextInitFunc,
					                                   &(node->ss.ps),
					                                   node);
			}
		}
		else
		{
			elog(ERROR, "%s_getnext_init function was not found",
			            scandesc->fs_formatter_name);
		}

		FmgrInfo *getnextFunc = scandesc->fs_ps_scan_funcs.getnext;

		if (getnextFunc)
		{
			returnTuple = InvokePlugStorageFormatGetNext(getnextFunc,
			                                             scandesc,
			                                             direction,
			                                             externalSelectDesc,
			                                             &(node->ss),
			                                             slot);
		}
		else
		{
			elog(ERROR, "%s_getnext function was not found",
			            scandesc->fs_formatter_name);
		}
	}

	/*
	 * save the tuple and the buffer returned to us by the access methods in
	 * our scan tuple slot and return the slot.  Note: we pass 'false' because
	 * tuples returned by heap_getnext() are pointers onto disk pages and were
	 * not created with palloc() and so should not be pfree()'d.  Note also
	 * that ExecStoreTuple will increment the refcount of the buffer; the
	 * refcount will not be dropped until the tuple table slot is cleared.
	 */
	if (returnTuple)
	{
		/*
		 * Perfmon is not supported any more.
		 *
		 * Gpmon_M_Incr_Rows_Out(GpmonPktFromExtScanState(node));
		 * CheckSendPlanStateGpmonPkt(&node->ss.ps);
		 */

	    /*
	     * CDB: Label each row with a synthetic ctid if needed for subquery dedup.
	     */
	    if (node->cdb_want_ctid &&
	        !TupIsNull(slot))
	    {
	    	slot_set_ctid_from_fake(slot, &node->cdb_fake_ctid);
	    }
	}
	else
	{
		ExecClearTuple(slot);

		if (!node->ss.ps.delayEagerFree)
		{
			ExecEagerFreeExternalScan(node);
		}
	}
	if (externalSelectDesc)
	{
		pfree(externalSelectDesc);
	}

	return slot;
}

/* ----------------------------------------------------------------
*		ExecExternalScan(node)
*
*		Scans the external relation sequentially and returns the next qualifying
*		tuple.
*		It calls the ExecScan() routine and passes it the access method
*		which retrieve tuples sequentially.
*
*/

TupleTableSlot *
ExecExternalScan(ExternalScanState *node)
{
	/*
	 * use SeqNext as access method
	 */
	return ExecScan(&node->ss, (ExecScanAccessMtd) ExternalNext);
}


/* ----------------------------------------------------------------
 *		ExecInitExternalScan
 * ----------------------------------------------------------------
 */
ExternalScanState *
ExecInitExternalScan(ExternalScan *node, EState *estate, int eflags)
{
	ResultRelSegFileInfo *segfileinfo = NULL;
	ExternalScanState *externalstate = NULL;
	Relation currentRelation = NULL;
	FileScanDesc currentScanDesc = NULL;

	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	externalstate = makeNode(ExternalScanState);
	externalstate->ss.ps.plan = (Plan *) node;
	externalstate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &externalstate->ss.ps);

	/*
	 * initialize child expressions
	 */
	externalstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) externalstate);
	externalstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) externalstate);

	/* Check if targetlist or qual contains a var node referencing the ctid column */
	externalstate->cdb_want_ctid = contain_ctid_var_reference(&node->scan);
	ItemPointerSetInvalid(&externalstate->cdb_fake_ctid);

#define EXTSCAN_NSLOTS 2

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &externalstate->ss.ps);
	ExecInitScanTupleSlot(estate, &externalstate->ss);

	/*
	 * get the relation object id from the relid'th entry in the range table
	 * and open that relation.
	 */
	currentRelation = ExecOpenScanExternalRelation(estate, node->scan.scanrelid);

	if (Gp_role == GP_ROLE_EXECUTE && node->err_aosegfileinfos)
	{
		segfileinfo = (ResultRelSegFileInfo *)list_nth(node->err_aosegfileinfos, GetQEIndex());
	}
	else
	{
		segfileinfo = NULL;
	}

	externalstate->ss.splits = GetFileSplitsOfSegment(estate->es_plannedstmt->scantable_splits,
	                                                  currentRelation->rd_id,
	                                                  GetQEIndex());

	int   formatterType = ExternalTableType_Invalid;
	char *formatterName = NULL;
	getExternalTableTypeInList(node->fmtType, node->fmtOpts,
	                         &formatterType, &formatterName);

	if (formatterType == ExternalTableType_Invalid)
	{
		elog(ERROR, "invalid formatter type for external table: %s", __func__);
	}
	else if (formatterType != ExternalTableType_PLUG)
	{
		currentScanDesc = external_beginscan(node, currentRelation, segfileinfo,
		                                     formatterType, formatterName);
	}
	else
	{
		Assert(formatterName);

		Oid	procOid = LookupPlugStorageValidatorFunc(formatterName,
		                                             "beginscan");

		if (OidIsValid(procOid))
		{
			FmgrInfo beginScanFunc;
			fmgr_info(procOid, &beginScanFunc);

			currentScanDesc = InvokePlugStorageFormatBeginScan(&beginScanFunc,
			                                                   node,
			                                                   &(externalstate->ss),
			                                                   currentRelation,
			                                                   formatterType,
			                                                   formatterName);
		}
		else
		{
			elog(ERROR, "%s_beginscan function was not found", formatterName);
		}
	}

	externalstate->ss.ss_currentRelation = currentRelation;
	externalstate->ess_ScanDesc = currentScanDesc;

	ExecAssignScanType(&externalstate->ss, RelationGetDescr(currentRelation));

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&externalstate->ss.ps);
	ExecAssignScanProjectionInfo(&externalstate->ss);

	/*
	 * If eflag contains EXEC_FLAG_REWIND or EXEC_FLAG_BACKWARD or EXEC_FLAG_MARK,
	 * then this node is not eager free safe.
	 */
	externalstate->ss.ps.delayEagerFree =
		((eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) != 0);

	/*
	 * If eflag contains EXEC_FLAG_EXTERNAL_AGG_COUNT then notify the underlying storage level
	 */
	externalstate->parent_agg_type = (eflags & EXEC_FLAG_EXTERNAL_AGG_COUNT);

	initGpmonPktForExternalScan((Plan *)node, &externalstate->ss.ps.gpmon_pkt, estate);

	return externalstate;
}


int
ExecCountSlotsExternalScan(ExternalScan *node)
{
	return ExecCountSlotsNode(outerPlan(node)) +
	ExecCountSlotsNode(innerPlan(node)) +
	EXTSCAN_NSLOTS;
}

/* ----------------------------------------------------------------
*		ExecEndExternalScan
*
*		frees any storage allocated through C routines.
* ----------------------------------------------------------------
*/
void
ExecEndExternalScan(ExternalScanState *node)
{
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	ExecEagerFreeExternalScan(node);
	pfree(node->ess_ScanDesc);

	/*
	 * close the external relation.
	 *
	 * MPP-8040: make sure we don't close it if it hasn't completed setup, or
	 * if we've already closed it.
	 */
	if (node->ss.ss_currentRelation)
	{
		Relation	relation = node->ss.ss_currentRelation;

		node->ss.ss_currentRelation = NULL;
		ExecCloseScanRelation(relation);
	}
	EndPlanStateGpmonPkt(&node->ss.ps);
}

/* ----------------------------------------------------------------
*		ExecStopExternalScan
*
*		Performs identically to ExecEndExternalScan except that
*		closure errors are ignored.  This function is called for
*		normal termination when the external data source is NOT
*		exhausted (such as for a LIMIT clause).
* ----------------------------------------------------------------
*/
void
ExecStopExternalScan(ExternalScanState *node)
{
	FileScanDesc fileScanDesc;

	/*
	 * get information from node
	 */
	fileScanDesc = node->ess_ScanDesc;

	/*
	 * stop the file scan
	 */
	if (fileScanDesc->fs_formatter_type == ExternalTableType_Invalid)
	{
		elog(ERROR, "invalid formatter type for external table: %s", __func__);
	}
	else if (fileScanDesc->fs_formatter_type != ExternalTableType_PLUG)
	{
		external_stopscan(fileScanDesc);
	}
	else
	{
		FmgrInfo *stopScanFunc = fileScanDesc->fs_ps_scan_funcs.stopscan;

		if (stopScanFunc)
		{
			InvokePlugStorageFormatStopScan(stopScanFunc, fileScanDesc);
		}
		else
		{
			elog(ERROR, "%s_stopscan function was not found",
			            fileScanDesc->fs_formatter_name);
		}
	}
}


/* ----------------------------------------------------------------
*						Join Support
* ----------------------------------------------------------------
*/

/* ----------------------------------------------------------------
*		ExecExternalReScan
*
*		Rescans the relation.
* ----------------------------------------------------------------
*/
void
ExecExternalReScan(ExternalScanState *node, ExprContext *exprCtxt)
{
	EState	   *estate;
	Index		scanrelid;
	FileScanDesc fileScan;

	estate = node->ss.ps.state;
	scanrelid = ((SeqScan *) node->ss.ps.plan)->scanrelid;

	/* If this is re-scanning of PlanQual ... */
	if (estate->es_evTuple != NULL &&
		estate->es_evTuple[scanrelid - 1] != NULL)
	{
		estate->es_evTupleNull[scanrelid - 1] = false;
		return;
	}
	Gpmon_M_Incr(GpmonPktFromExtScanState(node), GPMON_EXTSCAN_RESCAN);
     	CheckSendPlanStateGpmonPkt(&node->ss.ps);
	fileScan = node->ess_ScanDesc;

	ItemPointerSet(&node->cdb_fake_ctid, 0, 0);

	if (fileScan->fs_formatter_type == ExternalTableType_Invalid)
	{
		elog(ERROR, "invalid formatter type for external table: %s", __func__);
	}
	else if (fileScan->fs_formatter_type != ExternalTableType_PLUG)
	{
		external_rescan(fileScan);
	}
	else
	{
		Assert(fileScan->fs_formatter_name);

		FmgrInfo *rescanFunc = fileScan->fs_ps_scan_funcs.rescan;

		if (rescanFunc)
		{
			InvokePlugStorageFormatReScan(rescanFunc, fileScan);
		}
		else
		{
			elog(ERROR, "%s_rescan function was not found",
			            fileScan->fs_formatter_name);
		}
	}
}

void
initGpmonPktForExternalScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, ExternalScan));

	{
		RangeTblEntry *rte = rt_fetch(((ExternalScan *)planNode)->scan.scanrelid,
									  estate->es_range_table);
		char schema_rel_name[SCAN_REL_NAME_BUF_SIZE] = {0};

		Assert(GPMON_EXTSCAN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_ExternalScan,
							 (int64)planNode->plan_rows,
							 GetScanRelNameGpmon(rte->relid, schema_rel_name));
	}
}

void
ExecEagerFreeExternalScan(ExternalScanState *node)
{
	Assert(node->ess_ScanDesc != NULL);

	FileScanDesc fileScanDesc = node->ess_ScanDesc;

	if (fileScanDesc->fs_formatter_type == ExternalTableType_Invalid)
	{
		elog(ERROR, "invalid formatter type for external table: %s", __func__);
	}
	else if (fileScanDesc->fs_formatter_type != ExternalTableType_PLUG)
	{
		external_endscan(fileScanDesc);
	}
	else
	{
		FmgrInfo *endScanFunc = fileScanDesc->fs_ps_scan_funcs.endscan;

		if (endScanFunc)
		{
			InvokePlugStorageFormatEndScan(endScanFunc, fileScanDesc);
		}
		else
		{
			elog(ERROR, "%s_endscan function was not found",
			            fileScanDesc->fs_formatter_name);
		}
	}
}
