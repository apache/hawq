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
 * execUtils.c
 *	  miscellaneous executor utility routines
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/execUtils.c,v 1.140.2.3 2007/02/06 17:35:27 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		CreateExecutorState		Create/delete executor working state
 *		CreateSubExecutorState
 *		FreeExecutorState
 *		CreateExprContext
 *		CreateStandaloneExprContext
 *		FreeExprContext
 *		ReScanExprContext
 *
 *		ExecAssignExprContext	Common code for plan node init routines.
 *		ExecAssignResultType
 *		etc
 *
 *		ExecOpenScanRelation	Common code for scan node init routines.
 *		ExecCloseScanRelation
 *
 *		ExecOpenIndices			\
 *		ExecCloseIndices		 | referenced by InitPlan, EndPlan,
 *		ExecInsertIndexTuples	/  ExecInsert, ExecUpdate
 *
 *		RegisterExprContextCallback    Register function shutdown callback
 *		UnregisterExprContextCallback  Deregister function shutdown callback
 *
 *	 NOTES
 *		This file has traditionally been the place to stick misc.
 *		executor support stuff that doesn't really go anyplace else.
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/appendonlywriter.h"
#include "catalog/index.h"
#include "executor/execdebug.h"
#include "parser/parsetree.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/workfile_mgr.h"

#include "cdb/cdbvars.h"
#include "nodes/primnodes.h"
#include "nodes/execnodes.h"

#include "cdb/cdbutil.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/ml_ipc.h"
#include "cdb/cdbmotion.h"
#include "cdb/cdbsreh.h"
#include "cdb/memquota.h"
#include "cdb/cdbsrlz.h"
#include "catalog/catalog.h" // isMasterOnly()
#include "executor/spi.h"
#include "utils/elog.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "cdb/dispatcher.h"
#include "postmaster/identity.h"

#include "storage/ipc.h"

/* ----------------------------------------------------------------
 *		global counters for number of tuples processed, retrieved,
 *		appended, replaced, deleted.
 * ----------------------------------------------------------------
 */
int			NTupleProcessed;
int			NTupleRetrieved;
int			NTupleReplaced;
int			NTupleAppended;
int			NTupleDeleted;
int			NIndexTupleInserted;
int			NIndexTupleProcessed;

DynamicTableScanInfo *dynamicTableScanInfo = NULL;

static EState *InternalCreateExecutorState(MemoryContext qcontext,
										   bool is_subquery);
static void ShutdownExprContext(ExprContext *econtext);


/* ----------------------------------------------------------------
 *						statistic functions
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ResetTupleCount
 * ----------------------------------------------------------------
 */
#ifdef NOT_USED
void
ResetTupleCount(void)
{
	NTupleProcessed = 0;
	NTupleRetrieved = 0;
	NTupleAppended = 0;
	NTupleDeleted = 0;
	NTupleReplaced = 0;
	NIndexTupleProcessed = 0;
}
#endif

/* ----------------------------------------------------------------
 *		PrintTupleCount
 * ----------------------------------------------------------------
 */
#ifdef NOT_USED
void
DisplayTupleCount(FILE *statfp)
{
	if (NTupleProcessed > 0)
		fprintf(statfp, "!\t%d tuple%s processed, ", NTupleProcessed,
				(NTupleProcessed == 1) ? "" : "s");
	else
	{
		fprintf(statfp, "!\tno tuples processed.\n");
		return;
	}
	if (NIndexTupleProcessed > 0)
		fprintf(statfp, "%d indextuple%s processed, ", NIndexTupleProcessed,
				(NIndexTupleProcessed == 1) ? "" : "s");
	if (NIndexTupleInserted > 0)
		fprintf(statfp, "%d indextuple%s inserted, ", NIndexTupleInserted,
				(NIndexTupleInserted == 1) ? "" : "s");
	if (NTupleRetrieved > 0)
		fprintf(statfp, "%d tuple%s retrieved. ", NTupleRetrieved,
				(NTupleRetrieved == 1) ? "" : "s");
	if (NTupleAppended > 0)
		fprintf(statfp, "%d tuple%s appended. ", NTupleAppended,
				(NTupleAppended == 1) ? "" : "s");
	if (NTupleDeleted > 0)
		fprintf(statfp, "%d tuple%s deleted. ", NTupleDeleted,
				(NTupleDeleted == 1) ? "" : "s");
	if (NTupleReplaced > 0)
		fprintf(statfp, "%d tuple%s replaced. ", NTupleReplaced,
				(NTupleReplaced == 1) ? "" : "s");
	fprintf(statfp, "\n");
}
#endif


/* ----------------------------------------------------------------
 *				 Executor state and memory management functions
 * ----------------------------------------------------------------
 */

/* ----------------
 *		CreateExecutorState
 *
 *		Create and initialize an EState node, which is the root of
 *		working storage for an entire Executor invocation.
 *
 * Principally, this creates the per-query memory context that will be
 * used to hold all working data that lives till the end of the query.
 * Note that the per-query context will become a child of the caller's
 * CurrentMemoryContext.
 * ----------------
 */
EState *
CreateExecutorState(void)
{
	MemoryContext qcontext;

	/*
	 * Create the per-query context for this Executor run.
	 */
	qcontext = AllocSetContextCreate(CurrentMemoryContext,
									 "ExecutorState",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);

	EState *estate = InternalCreateExecutorState(qcontext, false);

	/*
	 * Initialize dynamicTableScanInfo. Since this is shared by subqueries,
	 * this can not be put inside InternalCreateExecutorState.
	 */
	MemoryContext oldcontext = MemoryContextSwitchTo(qcontext);
	
	estate->dynamicTableScanInfo = palloc0(sizeof(DynamicTableScanInfo));
	estate->dynamicTableScanInfo->memoryContext = qcontext;

	MemoryContextSwitchTo(oldcontext);

	return estate;
}

/* ----------------
 *		CreateSubExecutorState
 *
 *		Create and initialize an EState node for a sub-query.
 *
 * Ideally, sub-queries probably shouldn't have their own EState at all,
 * but right now this is necessary because they have their own rangetables
 * and we access the rangetable via the EState.  It is critical that a
 * sub-query share the parent's es_query_cxt, else structures allocated by
 * the sub-query (especially its result tuple descriptor) may disappear
 * too soon during executor shutdown.
 * ----------------
 */
EState *
CreateSubExecutorState(EState *parent_estate)
{
    EState *es = InternalCreateExecutorState(parent_estate->es_query_cxt, true);

    es->showstatctx = parent_estate->showstatctx;                   /*CDB*/

	/* Subqueries share the same dynamicTableScanInfo with their parents. */
	es->dynamicTableScanInfo = parent_estate->dynamicTableScanInfo;
	es->subplanLevel = parent_estate->subplanLevel + 1;
    return es;
}

/*
 * Guts of CreateExecutorState/CreateSubExecutorState
 */
static EState *
InternalCreateExecutorState(MemoryContext qcontext, bool is_subquery)
{
	EState	   *estate;
	MemoryContext oldcontext;

	/*
	 * Make the EState node within the per-query context.  This way, we don't
	 * need a separate pfree() operation for it at shutdown.
	 */
	oldcontext = MemoryContextSwitchTo(qcontext);

	estate = makeNode(EState);

	/*
	 * Initialize all fields of the Executor State structure
	 */
	estate->es_direction = ForwardScanDirection;
	estate->es_snapshot = SnapshotNow;
	estate->es_crosscheck_snapshot = InvalidSnapshot;	/* no crosscheck */
	estate->es_range_table = NIL;

	estate->es_result_relations = NULL;
	estate->es_num_result_relations = 0;
	estate->es_result_relation_info = NULL;
	estate->es_last_parq_part = InvalidOid;

	estate->es_junkFilter = NULL;

	estate->es_trig_tuple_slot = NULL;

	estate->es_into_relation_descriptor = NULL;
	estate->es_into_relation_is_bulkload = false;

	MemSet(&estate->es_into_relation_last_heap_tid, 0, sizeof(ItemPointerData));
	
	estate->es_param_list_info = NULL;
	estate->es_param_exec_vals = NULL;

	estate->es_query_cxt = qcontext;

	estate->es_tupleTable = NULL;

	estate->es_processed = 0;
	estate->es_lastoid = InvalidOid;
	estate->es_rowMarks = NIL;

	estate->es_is_subquery = is_subquery;

	estate->es_instrument = false;
	estate->es_select_into = false;
	estate->es_into_oids = false;

	estate->es_exprcontexts = NIL;

	estate->es_per_tuple_exprcontext = NULL;

	estate->es_plannedstmt = NULL;
	estate->es_evalPlanQual = NULL;
	estate->es_evTupleNull = NULL;
	estate->es_evTuple = NULL;
	estate->es_useEvalPlan = false;

	estate->es_sliceTable = NULL;
	estate->interconnect_context = NULL;
	estate->motionlayer_context = NULL;
	estate->es_interconnect_is_setup = false;
	estate->active_recv_id = -1;
	estate->es_got_eos = false;
	estate->cancelUnfinished = false;

	estate->dispatch_data = NULL;

	estate->currentSliceIdInPlan = 0;
	estate->currentExecutingSliceId = 0;
	estate->subplanLevel = 0;
	estate->rootSliceId = 0;

	/*
	 * Return the executor state structure
	 */
	MemoryContextSwitchTo(oldcontext);

	return estate;
}

/*
 * freeDynamicTableScanInfo
 *   Free the space for DynamicTableScanInfo.
 */
static void
freeDynamicTableScanInfo(DynamicTableScanInfo *scanInfo)
{
	Assert(scanInfo != NULL);
	
	if (scanInfo->partsMetadata != NIL)
	{
		list_free_deep(scanInfo->partsMetadata);
	}
	
	if (scanInfo->numSelectorsPerScanId != NIL)
	{
		list_free(scanInfo->numSelectorsPerScanId);
	}

	pfree(scanInfo);
}

/* ----------------
 *		FreeExecutorState
 *
 *		Release an EState along with all remaining working storage.
 *
 * Note: this is not responsible for releasing non-memory resources,
 * such as open relations or buffer pins.  But it will shut down any
 * still-active ExprContexts within the EState.  That is sufficient
 * cleanup for situations where the EState has only been used for expression
 * evaluation, and not to run a complete Plan.
 *
 * This can be called in any memory context ... so long as it's not one
 * of the ones to be freed.
 * ----------------
 */
void
FreeExecutorState(EState *estate)
{
	/*
	 * Shut down and free any remaining ExprContexts.  We do this explicitly
	 * to ensure that any remaining shutdown callbacks get called (since they
	 * might need to release resources that aren't simply memory within the
	 * per-query memory context).
	 */
	while (estate->es_exprcontexts)
	{
		/*
		 * XXX: seems there ought to be a faster way to implement this than
		 * repeated list_delete(), no?
		 */
		FreeExprContext((ExprContext *) linitial(estate->es_exprcontexts));
		/* FreeExprContext removed the list link for us */
	}

	/*
	 * Free dynamicTableScanInfo. In a subquery, we don't do this, since
	 * the subquery shares the value with its parent.
	 */
	if (!estate->es_is_subquery &&
		estate->dynamicTableScanInfo != NULL)
	{
		/*
		 * In case of an abnormal termination such as elog(FATAL) we jump directly to
		 * proc_exit, instead of finishing ExecutorRun() that was supposed to restore
		 * dynamicTableScanInfo. Therefore, in such case we cannot assert that
		 * dynamicTableScanInfo != estate->dynamicTableScanInfo [JIRA: MPP-23562]
		 */
		Assert(proc_exit_inprogress || dynamicTableScanInfo != estate->dynamicTableScanInfo);
		freeDynamicTableScanInfo(estate->dynamicTableScanInfo);
		estate->dynamicTableScanInfo = NULL;
	}

	/*
	 * Free the per-query memory context, thereby releasing all working
	 * memory, including the EState node itself.  In a subquery, we don't
	 * do this, leaving the memory cleanup to happen when the topmost query
	 * is closed down.
	 */
	if (!estate->es_is_subquery)
		MemoryContextDelete(estate->es_query_cxt);
}

/* ----------------
 *		CreateExprContext
 *
 *		Create a context for expression evaluation within an EState.
 *
 * An executor run may require multiple ExprContexts (we usually make one
 * for each Plan node, and a separate one for per-output-tuple processing
 * such as constraint checking).  Each ExprContext has its own "per-tuple"
 * memory context.
 *
 * Note we make no assumption about the caller's memory context.
 * ----------------
 */
ExprContext *
CreateExprContext(EState *estate)
{
	ExprContext *econtext;
	MemoryContext oldcontext;

	/* Create the ExprContext node within the per-query memory context */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	econtext = makeNode(ExprContext);

	/* Initialize fields of ExprContext */
	econtext->ecxt_scantuple = NULL;
	econtext->ecxt_innertuple = NULL;
	econtext->ecxt_outertuple = NULL;

	econtext->ecxt_per_query_memory = estate->es_query_cxt;

	/*
	 * Create working memory for expression evaluation in this context.
	 */
	econtext->ecxt_per_tuple_memory =
	    AllocSetContextCreate(estate->es_query_cxt,
							  "ExprContext",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);

	econtext->ecxt_param_exec_vals = estate->es_param_exec_vals;
	econtext->ecxt_param_list_info = estate->es_param_list_info;

	econtext->ecxt_aggvalues = NULL;
	econtext->ecxt_aggnulls = NULL;

	econtext->caseValue_datum = (Datum) 0;
	econtext->caseValue_isNull = true;

	econtext->domainValue_datum = (Datum) 0;
	econtext->domainValue_isNull = true;

	econtext->ecxt_estate = estate;

	econtext->ecxt_callbacks = NULL;

	/*
	 * Link the ExprContext into the EState to ensure it is shut down when the
	 * EState is freed.  Because we use lcons(), shutdowns will occur in
	 * reverse order of creation, which may not be essential but can't hurt.
	 */
	estate->es_exprcontexts = lcons(econtext, estate->es_exprcontexts);

	MemoryContextSwitchTo(oldcontext);

	return econtext;
}

/* ----------------
 *		CreateStandaloneExprContext
 *
 *		Create a context for standalone expression evaluation.
 *
 * An ExprContext made this way can be used for evaluation of expressions
 * that contain no Params, subplans, or Var references (it might work to
 * put tuple references into the scantuple field, but it seems unwise).
 *
 * The ExprContext struct is allocated in the caller's current memory
 * context, which also becomes its "per query" context.
 *
 * It is caller's responsibility to free the ExprContext when done,
 * or at least ensure that any shutdown callbacks have been called
 * (ReScanExprContext() is suitable).  Otherwise, non-memory resources
 * might be leaked.
 * ----------------
 */
ExprContext *
CreateStandaloneExprContext(void)
{
	ExprContext *econtext;

	/* Create the ExprContext node within the caller's memory context */
	econtext = makeNode(ExprContext);

	/* Initialize fields of ExprContext */
	econtext->ecxt_scantuple = NULL;
	econtext->ecxt_innertuple = NULL;
	econtext->ecxt_outertuple = NULL;

	econtext->ecxt_per_query_memory = CurrentMemoryContext;

	/*
	 * Create working memory for expression evaluation in this context.
	 */
	econtext->ecxt_per_tuple_memory =
		AllocSetContextCreate(CurrentMemoryContext,
							  "ExprContext",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);

	econtext->ecxt_param_exec_vals = NULL;
	econtext->ecxt_param_list_info = NULL;

	econtext->ecxt_aggvalues = NULL;
	econtext->ecxt_aggnulls = NULL;

	econtext->caseValue_datum = (Datum) 0;
	econtext->caseValue_isNull = true;

	econtext->domainValue_datum = (Datum) 0;
	econtext->domainValue_isNull = true;

	econtext->ecxt_estate = NULL;

	econtext->ecxt_callbacks = NULL;

	return econtext;
}

/* ----------------
 *		FreeExprContext
 *
 *		Free an expression context, including calling any remaining
 *		shutdown callbacks.
 *
 * Since we free the temporary context used for expression evaluation,
 * any previously computed pass-by-reference expression result will go away!
 *
 * Note we make no assumption about the caller's memory context.
 * ----------------
 */
void
FreeExprContext(ExprContext *econtext)
{
	EState	   *estate;

	/* Call any registered callbacks */
	ShutdownExprContext(econtext);
	/* And clean up the memory used */
	MemoryContextDelete(econtext->ecxt_per_tuple_memory);
	/* Unlink self from owning EState, if any */
	estate = econtext->ecxt_estate;
	if (estate)
		estate->es_exprcontexts = list_delete_ptr(estate->es_exprcontexts,
												  econtext);
	/* And delete the ExprContext node */
	pfree(econtext);
}

/*
 * ReScanExprContext
 *
 *		Reset an expression context in preparation for a rescan of its
 *		plan node.	This requires calling any registered shutdown callbacks,
 *		since any partially complete set-returning-functions must be canceled.
 *
 * Note we make no assumption about the caller's memory context.
 */
void
ReScanExprContext(ExprContext *econtext)
{
	/* Call any registered callbacks */
	ShutdownExprContext(econtext);
	/* And clean up the memory used */
	MemoryContextReset(econtext->ecxt_per_tuple_memory);
}

/*
 * Build a per-output-tuple ExprContext for an EState.
 *
 * This is normally invoked via GetPerTupleExprContext() macro,
 * not directly.
 */
ExprContext *
MakePerTupleExprContext(EState *estate)
{
	if (estate->es_per_tuple_exprcontext == NULL)
		estate->es_per_tuple_exprcontext = CreateExprContext(estate);

	return estate->es_per_tuple_exprcontext;
}


/* ----------------------------------------------------------------
 *				 miscellaneous node-init support functions
 *
 * Note: all of these are expected to be called with CurrentMemoryContext
 * equal to the per-query memory context.
 * ----------------------------------------------------------------
 */

/* ----------------
 *		ExecAssignExprContext
 *
 *		This initializes the ps_ExprContext field.	It is only necessary
 *		to do this for nodes which use ExecQual or ExecProject
 *		because those routines require an econtext. Other nodes that
 *		don't have to evaluate expressions don't need to do this.
 * ----------------
 */
void
ExecAssignExprContext(EState *estate, PlanState *planstate)
{
	planstate->ps_ExprContext = CreateExprContext(estate);
}

/* ----------------
 *		ExecAssignResultType
 * ----------------
 */
void
ExecAssignResultType(PlanState *planstate, TupleDesc tupDesc)
{
	TupleTableSlot *slot = planstate->ps_ResultTupleSlot;

	ExecSetSlotDescriptor(slot, tupDesc);
}

/* ----------------
 *		ExecAssignResultTypeFromTL
 * ----------------
 */
void
ExecAssignResultTypeFromTL(PlanState *planstate)
{
	bool		hasoid;
	TupleDesc	tupDesc;

	if (ExecContextForcesOids(planstate, &hasoid))
	{
		/* context forces OID choice; hasoid is now set correctly */
	}
	else
	{
		/* given free choice, don't leave space for OIDs in result tuples */
		hasoid = false;
	}

	/*
	 * ExecTypeFromTL needs the parse-time representation of the tlist, not a
	 * list of ExprStates.	This is good because some plan nodes don't bother
	 * to set up planstate->targetlist ...
	 */
	tupDesc = ExecTypeFromTL(planstate->plan->targetlist, hasoid);
	ExecAssignResultType(planstate, tupDesc);
}

/* ----------------
 *		ExecGetResultType
 * ----------------
 */
TupleDesc
ExecGetResultType(PlanState *planstate)
{
	TupleTableSlot *slot = planstate->ps_ResultTupleSlot;

	return slot->tts_tupleDescriptor;
}

/* ----------------
 *		ExecBuildProjectionInfo
 *
 * Build a ProjectionInfo node for evaluating the given tlist in the given
 * econtext, and storing the result into the tuple slot.  (Caller must have
 * ensured that tuple slot has a descriptor matching the tlist!)  Note that
 * the given tlist should be a list of ExprState nodes, not Expr nodes.
 *
 * inputDesc can be NULL, but if it is not, we check to see whether simple
 * Vars in the tlist match the descriptor.  It is important to provide
 * inputDesc for relation-scan plan nodes, as a cross check that the relation
 * hasn't been changed since the plan was made.  At higher levels of a plan,
 * there is no need to recheck.
 * ----------------
 */
ProjectionInfo *
ExecBuildProjectionInfo(List *targetList,
						ExprContext *econtext,
						TupleTableSlot *slot,
						TupleDesc inputDesc)
{
	ProjectionInfo *projInfo = makeNode(ProjectionInfo);
	int			len;
	bool		isVarList;
	ListCell   *tl;

	len = ExecTargetListLength(targetList);

	projInfo->pi_targetlist = targetList;
	projInfo->pi_exprContext = econtext;
	projInfo->pi_slot = slot;

	/*
	 * Determine whether the target list consists entirely of simple Var
	 * references (ie, references to non-system attributes) that match the
	 * input.  If so, we can use the simpler ExecVariableList instead of
	 * ExecTargetList.  (Note: if there is a type mismatch then ExecEvalVar
	 * will probably throw an error at runtime, but we leave that to it.)
	 */
	isVarList = true;
	foreach(tl, targetList)
	{
		GenericExprState *gstate = (GenericExprState *) lfirst(tl);
		Var		   *variable = (Var *) gstate->arg->expr;
		Form_pg_attribute attr;

		if (variable == NULL ||
			!IsA(variable, Var) ||
			variable->varattno <= 0)
		{
			isVarList = false;
			break;
		}
		if (!inputDesc)
			continue;			/* can't check type, assume OK */
		if (variable->varattno > inputDesc->natts)
		{
			isVarList = false;
			break;
		}
		attr = inputDesc->attrs[variable->varattno - 1];
		if (attr->attisdropped || variable->vartype != attr->atttypid)
		{
			isVarList = false;
			break;
		}
	}
	projInfo->pi_isVarList = isVarList;

	if (isVarList)
	{
		int		   *varSlotOffsets;
		int		   *varNumbers;
		AttrNumber	lastInnerVar = 0;
		AttrNumber	lastOuterVar = 0;
		AttrNumber	lastScanVar = 0;

		projInfo->pi_itemIsDone = NULL; /* not needed */
		projInfo->pi_varSlotOffsets = varSlotOffsets = (int *)
			palloc0(len * sizeof(int));
		projInfo->pi_varNumbers = varNumbers = (int *)
			palloc0(len * sizeof(int));

		/*
		 * Set up the data needed by ExecVariableList.	The slots in which the
		 * variables can be found at runtime are denoted by the offsets of
		 * their slot pointers within the econtext.  This rather grotty
		 * representation is needed because the caller may not have given us
		 * the real econtext yet (see hacks in nodeSubplan.c).
		 */
		foreach(tl, targetList)
		{
			GenericExprState *gstate = (GenericExprState *) lfirst(tl);
			Var		   *variable = (Var *) gstate->arg->expr;
			AttrNumber	attnum = variable->varattno;
			TargetEntry *tle = (TargetEntry *) gstate->xprstate.expr;
			AttrNumber	resind = tle->resno - 1;

			Assert(resind >= 0 && resind < len);
			varNumbers[resind] = attnum;

			switch (variable->varno)
			{
				case INNER:
					varSlotOffsets[resind] = offsetof(ExprContext,
													  ecxt_innertuple);
					lastInnerVar = Max(lastInnerVar, attnum);
					break;

				case OUTER:
					varSlotOffsets[resind] = offsetof(ExprContext,
													  ecxt_outertuple);
					lastOuterVar = Max(lastOuterVar, attnum);
					break;

				default:
					varSlotOffsets[resind] = offsetof(ExprContext,
													  ecxt_scantuple);
					lastScanVar = Max(lastScanVar, attnum);
					break;
			}
		}
		projInfo->pi_lastInnerVar = lastInnerVar;
		projInfo->pi_lastOuterVar = lastOuterVar;
		projInfo->pi_lastScanVar = lastScanVar;
	}
	else
	{
		projInfo->pi_itemIsDone = (ExprDoneCond *)
			palloc0(len * sizeof(ExprDoneCond));
		projInfo->pi_varSlotOffsets = NULL;
		projInfo->pi_varNumbers = NULL;
	}

	return projInfo;
}

/* ----------------
 *		ExecAssignProjectionInfo
 *
 * forms the projection information from the node's targetlist
 *
 * Notes for inputDesc are same as for ExecBuildProjectionInfo: supply it
 * for a relation-scan node, can pass NULL for upper-level nodes
 * ----------------
 */
void
ExecAssignProjectionInfo(PlanState *planstate,
						 TupleDesc inputDesc)
{
	ProjectionInfo* pi = planstate->ps_ProjInfo;
	if (NULL != pi)
	{
		if (NULL != pi->pi_varNumbers)
		{
			pfree(pi->pi_varNumbers);
		}
		if (NULL != pi->pi_varSlotOffsets)
		{
			pfree(pi->pi_varSlotOffsets);
		}
		if (NULL != pi->pi_itemIsDone)
		{
			pfree(pi->pi_itemIsDone);
		}
		pfree(pi);
	}

	planstate->ps_ProjInfo =
		ExecBuildProjectionInfo(planstate->targetlist,
								planstate->ps_ExprContext,
								planstate->ps_ResultTupleSlot,
								inputDesc);
}

/*
 * Constructs a new targetlist list that maps to a tuple descriptor.
 */
List *
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
void
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

/* ----------------
 *		ExecFreeExprContext
 *
 * A plan node's ExprContext should be freed explicitly during executor
 * shutdown because there may be shutdown callbacks to call.  (Other resources
 * made by the above routines, such as projection info, don't need to be freed
 * explicitly because they're just memory in the per-query memory context.)
 *
 * However ... there is no particular need to do it during ExecEndNode,
 * because FreeExecutorState will free any remaining ExprContexts within
 * the EState.	Letting FreeExecutorState do it allows the ExprContexts to
 * be freed in reverse order of creation, rather than order of creation as
 * will happen if we delete them here, which saves O(N^2) work in the list
 * cleanup inside FreeExprContext.
 * ----------------
 */
void
ExecFreeExprContext(PlanState *planstate)
{
	/*
	 * Per above discussion, don't actually delete the ExprContext. We do
	 * unlink it from the plan node, though.
	 */
	planstate->ps_ExprContext = NULL;
}

/* ----------------------------------------------------------------
 *		the following scan type support functions are for
 *		those nodes which are stubborn and return tuples in
 *		their Scan tuple slot instead of their Result tuple
 *		slot..	luck fur us, these nodes do not do projections
 *		so we don't have to worry about getting the ProjectionInfo
 *		right for them...  -cim 6/3/91
 * ----------------------------------------------------------------
 */

/* ----------------
 *		ExecGetScanType
 * ----------------
 */
TupleDesc
ExecGetScanType(ScanState *scanstate)
{
	TupleTableSlot *slot = scanstate->ss_ScanTupleSlot;

	return slot->tts_tupleDescriptor;
}

/* ----------------
 *		ExecAssignScanType
 * ----------------
 */
void
ExecAssignScanType(ScanState *scanstate, TupleDesc tupDesc)
{
	TupleTableSlot *slot = scanstate->ss_ScanTupleSlot;

	ExecSetSlotDescriptor(slot, tupDesc);
}

/* ----------------
 *		ExecAssignScanTypeFromOuterPlan
 * ----------------
 */
void
ExecAssignScanTypeFromOuterPlan(ScanState *scanstate)
{
	PlanState  *outerPlan;
	TupleDesc	tupDesc;

	outerPlan = outerPlanState(scanstate);
	tupDesc = ExecGetResultType(outerPlan);

	ExecAssignScanType(scanstate, tupDesc);
}


/* ----------------------------------------------------------------
 *				  Scan node support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecRelationIsTargetRelation
 *
 *		Detect whether a relation (identified by rangetable index)
 *		is one of the target relations of the query.
 * ----------------------------------------------------------------
 */
bool
ExecRelationIsTargetRelation(EState *estate, Index scanrelid)
{
	ResultRelInfo *resultRelInfos;
	int			i;

	resultRelInfos = estate->es_result_relations;
	for (i = 0; i < estate->es_num_result_relations; i++)
	{
		if (resultRelInfos[i].ri_RangeTableIndex == scanrelid)
			return true;
	}
	return false;
}

/* ----------------------------------------------------------------
 *		ExecOpenScanRelation
 *
 *		Open the heap relation to be scanned by a base-level scan plan node.
 *		This should be called during the node's ExecInit routine.
 *
 * By default, this acquires AccessShareLock on the relation.  However,
 * if the relation was already locked by InitPlan, we don't need to acquire
 * any additional lock.  This saves trips to the shared lock manager.
 * ----------------------------------------------------------------
 */
Relation
ExecOpenScanRelation(EState *estate, Index scanrelid)
{
	Oid			reloid;
	LOCKMODE	lockmode;
	ResultRelInfo *resultRelInfos;
	int			i;

	/*
	 * First determine the lock type we need.  Scan to see if target relation
	 * is either a result relation or a FOR UPDATE/FOR SHARE relation.
	 */
	lockmode = AccessShareLock;
	resultRelInfos = estate->es_result_relations;
	for (i = 0; i < estate->es_num_result_relations; i++)
	{
		if (resultRelInfos[i].ri_RangeTableIndex == scanrelid)
		{
			lockmode = NoLock;
			break;
		}
	}

	if (lockmode == AccessShareLock)
	{
		ListCell   *l;

		foreach(l, estate->es_rowMarks)
		{
			ExecRowMark *erm = lfirst(l);

			if (erm->rti == scanrelid)
			{
				lockmode = NoLock;
				break;
			}
		}
	}

	/* OK, open the relation and acquire lock as needed */
	reloid = getrelid(scanrelid, estate->es_range_table);

	Assert(reloid != InvalidOid);
	
	return heap_open(reloid, lockmode);
}

/*
 * same as above, but for external table scans
 */
Relation
ExecOpenScanExternalRelation(EState *estate, Index scanrelid)
{
	RangeTblEntry *rtentry;
	Oid			reloid;
	LOCKMODE	lockmode;

	lockmode = NoLock;

	rtentry = rt_fetch(scanrelid, estate->es_range_table);
	reloid = rtentry->relid;

	return relation_open(reloid, NoLock);
}

/* ----------------------------------------------------------------
 *		ExecCloseScanRelation
 *
 *		Close the heap relation scanned by a base-level scan plan node.
 *		This should be called during the node's ExecEnd routine.
 *
 * Currently, we do not release the lock acquired by ExecOpenScanRelation.
 * This lock should be held till end of transaction.  (There is a faction
 * that considers this too much locking, however.)
 *
 * If we did want to release the lock, we'd have to repeat the logic in
 * ExecOpenScanRelation in order to figure out what to release.
 * ----------------------------------------------------------------
 */
void
ExecCloseScanRelation(Relation scanrel)
{
	heap_close(scanrel, NoLock);
}


/* ----------------------------------------------------------------
 *				  ExecInsertIndexTuples support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecOpenIndices
 *
 *		Find the indices associated with a result relation, open them,
 *		and save information about them in the result ResultRelInfo.
 *
 *		At entry, caller has already opened and locked
 *		resultRelInfo->ri_RelationDesc.
 * ----------------------------------------------------------------
 */
void
ExecOpenIndices(ResultRelInfo *resultRelInfo)
{
	Relation	resultRelation = resultRelInfo->ri_RelationDesc;
	List	   *indexoidlist;
	ListCell   *l;
	int			len,
				i;
	RelationPtr relationDescs;
	IndexInfo **indexInfoArray;

	resultRelInfo->ri_NumIndices = 0;

	/* fast path if no indexes */
	if (!RelationGetForm(resultRelation)->relhasindex)
		return;

	/*
	 * Get cached list of index OIDs
	 */
	indexoidlist = RelationGetIndexList(resultRelation);
	len = list_length(indexoidlist);
	if (len == 0)
		return;

	/*
	 * allocate space for result arrays
	 */
	relationDescs = (RelationPtr) palloc(len * sizeof(Relation));
	indexInfoArray = (IndexInfo **) palloc(len * sizeof(IndexInfo *));

	resultRelInfo->ri_NumIndices = len;
	resultRelInfo->ri_IndexRelationDescs = relationDescs;
	resultRelInfo->ri_IndexRelationInfo = indexInfoArray;

	/*
	 * For each index, open the index relation and save pg_index info. We
	 * acquire RowExclusiveLock, signifying we will update the index.
	 */
	i = 0;
	foreach(l, indexoidlist)
	{
		Oid			indexOid = lfirst_oid(l);
		Relation	indexDesc;
		IndexInfo  *ii;

		indexDesc = index_open(indexOid, RowExclusiveLock);

		/* extract index key information from the index's pg_index info */
		ii = BuildIndexInfo(indexDesc);

		relationDescs[i] = indexDesc;
		indexInfoArray[i] = ii;
		i++;
	}

	list_free(indexoidlist);
}

/* ----------------------------------------------------------------
 *		ExecCloseIndices
 *
 *		Close the index relations stored in resultRelInfo
 * ----------------------------------------------------------------
 */
void
ExecCloseIndices(ResultRelInfo *resultRelInfo)
{
	int			i;
	int			numIndices;
	RelationPtr indexDescs;

	numIndices = resultRelInfo->ri_NumIndices;
	indexDescs = resultRelInfo->ri_IndexRelationDescs;

	for (i = 0; i < numIndices; i++)
	{
		if (indexDescs[i] == NULL)
			continue;			/* shouldn't happen? */

		/* Drop lock acquired by ExecOpenIndices */
		index_close(indexDescs[i], RowExclusiveLock);
	}

	/*
	 * XXX should free indexInfo array here too?  Currently we assume that
	 * such stuff will be cleaned up automatically in FreeExecutorState.
	 */
}

/* ----------------------------------------------------------------
 *		ExecInsertIndexTuples
 *
 *		This routine takes care of inserting index tuples
 *		into all the relations indexing the result relation
 *		when a heap tuple is inserted into the result relation.
 *		Much of this code should be moved into the genam
 *		stuff as it only exists here because the genam stuff
 *		doesn't provide the functionality needed by the
 *		executor.. -cim 9/27/89
 * ----------------------------------------------------------------
 */
void
ExecInsertIndexTuples(TupleTableSlot *slot,
					  ItemPointer tupleid,
					  EState *estate,
					  bool is_vacuum)
{
	ResultRelInfo *resultRelInfo;
	int			i;
	int			numIndices;
	RelationPtr relationDescs;
	Relation	heapRelation;
	IndexInfo **indexInfoArray;
	ExprContext *econtext;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];

	/*
	 * Get information from the result relation info structure.
	 */
	resultRelInfo = estate->es_result_relation_info;
	numIndices = resultRelInfo->ri_NumIndices;
	relationDescs = resultRelInfo->ri_IndexRelationDescs;
	indexInfoArray = resultRelInfo->ri_IndexRelationInfo;
	heapRelation = resultRelInfo->ri_RelationDesc;

	/*
	 * We will use the EState's per-tuple context for evaluating predicates
	 * and index expressions (creating it if it's not already there).
	 */
	econtext = GetPerTupleExprContext(estate);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/*
	 * for each index, form and insert the index tuple
	 */
	for (i = 0; i < numIndices; i++)
	{
		IndexInfo  *indexInfo;

		if (relationDescs[i] == NULL)
			continue;

		indexInfo = indexInfoArray[i];

		/* Check for partial index */
		if (indexInfo->ii_Predicate != NIL)
		{
			List	   *predicate;

			/*
			 * If predicate state not set up yet, create it (in the estate's
			 * per-query context)
			 */
			predicate = indexInfo->ii_PredicateState;
			if (predicate == NIL)
			{
				predicate = (List *)
					ExecPrepareExpr((Expr *) indexInfo->ii_Predicate,
									estate);
				indexInfo->ii_PredicateState = predicate;
			}

			/* Skip this index-update if the predicate isn't satisfied */
			if (!ExecQual(predicate, econtext, false))
				continue;
		}

		/*
		 * FormIndexDatum fills in its values and isnull parameters with the
		 * appropriate values for the column(s) of the index.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/*
		 * The index AM does the rest.	Note we suppress unique-index checks
		 * if we are being called from VACUUM, since VACUUM may need to move
		 * dead tuples that have the same keys as live ones.
		 */
		index_insert(relationDescs[i],	/* index relation */
					 values,	/* array of index Datums */
					 isnull,	/* null flags */
					 tupleid,	/* tid of heap tuple */
					 heapRelation,
					 relationDescs[i]->rd_index->indisunique && !is_vacuum);

		/*
		 * keep track of index inserts for debugging
		 */
		IncrIndexInserted();
	}
}

/*
 * ExecUpdateAOtupCount
 *		Update the tuple count on the master for an append only relation segfile.
 */
static void
ExecUpdateAOtupCount(ResultRelInfo *result_rels,
					 Snapshot shapshot,
					 int num_result_rels,
					 uint64 processed)
{
	int		i;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	for (i = num_result_rels; i > 0; i--)
	{
		if(RelationIsAoRows(result_rels->ri_RelationDesc)
				|| RelationIsParquet(result_rels->ri_RelationDesc))
		{
			Assert(result_rels->ri_aosegnos != NIL);
			/* The total tuple count would be not necessary in HAWQ 2.0 */
			/*
			UpdateMasterAosegTotals(result_rels->ri_RelationDesc,
									result_rels->ri_aosegno,
									processed);
			*/
		}
		result_rels++;
	}
}

/*
 * UpdateChangedParamSet
 *		Add changed parameters to a plan node's chgParam set
 */
void
UpdateChangedParamSet(PlanState *node, Bitmapset *newchg)
{
	Bitmapset  *parmset;

	/*
	 * The plan node only depends on params listed in its allParam set. Don't
	 * include anything else into its chgParam set.
	 */
	parmset = bms_intersect(node->plan->allParam, newchg);

	/*
	 * Keep node->chgParam == NULL if there's not actually any members; this
	 * allows the simplest possible tests in executor node files.
	 */
	if (!bms_is_empty(parmset))
		node->chgParam = bms_join(node->chgParam, parmset);
	else
		bms_free(parmset);
}

/*
 * Register a shutdown callback in an ExprContext.
 *
 * Shutdown callbacks will be called (in reverse order of registration)
 * when the ExprContext is deleted or rescanned.  This provides a hook
 * for functions called in the context to do any cleanup needed --- it's
 * particularly useful for functions returning sets.  Note that the
 * callback will *not* be called in the event that execution is aborted
 * by an error.
 */
void
RegisterExprContextCallback(ExprContext *econtext,
							ExprContextCallbackFunction function,
							Datum arg)
{
	ExprContext_CB *ecxt_callback;

	/* Save the info in appropriate memory context */
	ecxt_callback = (ExprContext_CB *)
		MemoryContextAlloc(econtext->ecxt_per_query_memory,
						   sizeof(ExprContext_CB));

	ecxt_callback->function = function;
	ecxt_callback->arg = arg;

	/* link to front of list for appropriate execution order */
	ecxt_callback->next = econtext->ecxt_callbacks;
	econtext->ecxt_callbacks = ecxt_callback;
}

/*
 * Deregister a shutdown callback in an ExprContext.
 *
 * Any list entries matching the function and arg will be removed.
 * This can be used if it's no longer necessary to call the callback.
 */
void
UnregisterExprContextCallback(ExprContext *econtext,
							  ExprContextCallbackFunction function,
							  Datum arg)
{
	ExprContext_CB **prev_callback;
	ExprContext_CB *ecxt_callback;

	prev_callback = &econtext->ecxt_callbacks;

	while ((ecxt_callback = *prev_callback) != NULL)
	{
		if (ecxt_callback->function == function && ecxt_callback->arg == arg)
		{
			*prev_callback = ecxt_callback->next;
			pfree(ecxt_callback);
		}
		else
			prev_callback = &ecxt_callback->next;
	}
}

/*
 * Call all the shutdown callbacks registered in an ExprContext.
 *
 * The callback list is emptied (important in case this is only a rescan
 * reset, and not deletion of the ExprContext).
 */
static void
ShutdownExprContext(ExprContext *econtext)
{
	ExprContext_CB *ecxt_callback;
	MemoryContext oldcontext;

	/* Fast path in normal case where there's nothing to do. */
	if (econtext->ecxt_callbacks == NULL)
		return;

	/*
	 * Call the callbacks in econtext's per-tuple context.  This ensures that
	 * any memory they might leak will get cleaned up.
	 */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/*
	 * Call each callback function in reverse registration order.
	 */
	while ((ecxt_callback = econtext->ecxt_callbacks) != NULL)
	{
		econtext->ecxt_callbacks = ecxt_callback->next;
		(*ecxt_callback->function) (ecxt_callback->arg);
		pfree(ecxt_callback);
	}

	MemoryContextSwitchTo(oldcontext);
}


/* ---------------------------------------------------------------
 * 		Share Input utilities
 * ---------------------------------------------------------------
 */
ShareNodeEntry *
ExecGetShareNodeEntry(EState* estate, int shareidx, bool fCreate)
{
	Assert(shareidx >= 0);
	Assert(estate->es_sharenode != NULL);

	if(!fCreate)
	{
		if(shareidx >= list_length(*estate->es_sharenode))
			return NULL;
	}
	else
	{
		while(list_length(*estate->es_sharenode) <= shareidx)
		{
			ShareNodeEntry *n = makeNode(ShareNodeEntry);
			n->sharePlan = NULL;
			n->shareState = NULL;

			*estate->es_sharenode = lappend(*estate->es_sharenode, n);
		}
	}

	return (ShareNodeEntry *) list_nth(*estate->es_sharenode, shareidx);
}

/* ----------------------------------------------------------------
 *		CDB Slice Table utilities
 * ----------------------------------------------------------------
 */

static bool
is1GangSlice(Slice *slice, int numSegmentsInCluster)
{
	return slice->gangSize == 1 && numSegmentsInCluster != 1;
}

/* Attach a slice table to the given Estate structure.	It should
 * consist of blank slices, one for the root plan, one for each
 * Motion node (which roots a slice with a send node), and one for
 * each subplan (which acts as an initplan root node).
 */
void
InitSliceTable(EState *estate, int nMotions, int nSubplans)
{
	SliceTable *table;
	Slice	   *slice;
	int			i,
				n;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	table = makeNode(SliceTable);
	table->nMotions = nMotions;
	table->nInitPlans = nSubplans;
	table->slices = NIL;
    table->doInstrument = false;

	/* Each slice table has a unique-id. */
	table->ic_instance_id = ++gp_interconnect_id;

	n = 1 + nMotions + nSubplans;
	for (i = 0; i < n; i++)
	{
		slice = makeNode(Slice);

		slice->sliceIndex = i;
        slice->rootIndex = (i > 0 && i <= nMotions) ? -1 : i;
		slice->gangType = GANGTYPE_UNALLOCATED;
		slice->gangSize = 0;
		slice->numGangMembersToBeActive = 0;
		slice->directDispatch.isDirectDispatch = false;
		slice->directDispatch.contentIds = NIL;
		slice->primary_gang_id = 0;
		slice->parentIndex = -1;
		slice->is_writer = false;
		slice->children = NIL;
		slice->primaryProcesses = NIL;

		table->slices = lappend(table->slices, slice);
	}

	estate->es_sliceTable = table;

	MemoryContextSwitchTo(oldcontext);
}

/*
 * A forgiving slice table indexer that returns the indexed Slice* or NULL
 */
Slice *
getCurrentSlice(EState *estate, int sliceIndex)
{
	SliceTable *sliceTable = estate->es_sliceTable;

    if (sliceTable &&
        sliceIndex >= 0 &&
        sliceIndex < list_length(sliceTable->slices))
	    return (Slice *)list_nth(sliceTable->slices, sliceIndex);

    return NULL;
}

/* Should the slice run on the QD?
 *
 * N.B. Not the same as !sliceRunsOnQE(slice), when slice is NULL.
 */
bool
sliceRunsOnQD(Slice * slice)
{
	return (slice != NULL && slice->gangType == GANGTYPE_UNALLOCATED);
}


/* Should the slice run on a QE?
 *
 * N.B. Not the same as !sliceRunsOnQD(slice), when slice is NULL.
 */
bool
sliceRunsOnQE(Slice * slice)
{
	return (slice != NULL && slice->gangType != GANGTYPE_UNALLOCATED);
}

/**
 * Calculate the number of sending processes that should in be a slice.
 */
int
sliceCalculateNumSendingProcesses(Slice *slice, int numSegmentsInCluster)
{
	switch(slice->gangType)
	{
		case GANGTYPE_UNALLOCATED:
			return 0; /* does not send */

		case GANGTYPE_ENTRYDB_READER:
			return 1; /* on master */

		case GANGTYPE_PRIMARY_WRITER:
			return 0; /* writers don't send */

		case GANGTYPE_PRIMARY_READER:
			if ( is1GangSlice(slice, numSegmentsInCluster))
				return 1;
			else if ( slice->directDispatch.isDirectDispatch)
				return list_length(slice->directDispatch.contentIds);
			else return numSegmentsInCluster;

		default:
			Insist(false);
			return -1;
	}
}

/* Assign gang descriptions to the root slices of the slice forest.
 *
 * The root slices of initPlan slice trees will always run on the QD,
 * which, for the time being, we represent as
 *
 *	  (gangType, gangSize) = <GANGTYPE_UNALLOCATED, 0>.
 *
 * The root slice of the main plan wil run on the QD in case it's a
 * SELECT,	but will run on QE(s) in case it's an INSERT, UPDATE, or
 * DELETE. Because we restrict UPDATE and DELETE to have no motions
 * (i.e., one slice) and because INSERT must always route tuples,
 * the assigment for these will be primary and mirror writer gangs,
 * which we represent as
 *
 *	  (gangType, gangSize) =  <GANGTYPE_PRIMARY_WRITER, N>
 */
void
InitRootSlices(QueryDesc *queryDesc, int numSegmentsInCluster)
{
	EState	   *estate = queryDesc->estate;
	SliceTable *sliceTable = estate->es_sliceTable;
	ListCell   *cell;
	Slice	   *slice;
	int			i;

	foreach(cell, sliceTable->slices)
	{
		slice = (Slice *) lfirst(cell);
		i = slice->sliceIndex;
		if (i == 0)
		{
			/* Main plan root slice */
			switch (queryDesc->operation)
			{
				case CMD_SELECT:
					Assert(slice->gangType == GANGTYPE_UNALLOCATED && slice->gangSize == 0);
					if (queryDesc->plannedstmt->intoClause != NULL)
					{
						slice->gangType = GANGTYPE_PRIMARY_WRITER;
						slice->gangSize = numSegmentsInCluster;
						slice->numGangMembersToBeActive = sliceCalculateNumSendingProcesses(slice, numSegmentsInCluster);
					}
					break;

				case CMD_INSERT:
				case CMD_UPDATE:
				case CMD_DELETE:
				{
					/* if updating a master-only table: do not dispatch to segments */
					List *resultRelations = queryDesc->plannedstmt->resultRelations;
					Assert(list_length(resultRelations) > 0);
					int idx = list_nth_int(resultRelations, 0);
					Assert (idx > 0);
					Oid reloid = getrelid(idx, queryDesc->plannedstmt->rtable);
					if (!isMasterOnly(reloid))
					{
						slice->gangType = GANGTYPE_PRIMARY_WRITER;
						slice->gangSize = numSegmentsInCluster;
						slice->numGangMembersToBeActive = sliceCalculateNumSendingProcesses(slice, numSegmentsInCluster);
			        }
			        /* else: result relation is master-only, so top slice should run on the QD and not be dispatched */
					break;
				}
				default:
					Assert(FALSE);
			}
		}
		if (i <= sliceTable->nMotions)
		{
			/* Non-root slice */
			continue;
		}
		else
		{
			/* InitPlan root slice */
			Assert(slice->gangType == GANGTYPE_UNALLOCATED && slice->gangSize == 0);
		}
	}
}

/*
 * Context for AssignGangs() and helper functions.
 */
typedef struct SliceReq
{
	int			numNgangs;
	int			num1gangs_primary_reader;
    int         num1gangs_entrydb_reader;
	int			nxtNgang;
	int			nxt1gang_primary_reader;
	int			nxt1gang_entrydb_reader;
	Gang	  **vecNgangs;
	Gang	  **vec1gangs_primary_reader;
	Gang	  **vec1gangs_entrydb_reader;
	bool		writer;

}	SliceReq;

/* Forward declarations */
static void InitSliceReq(SliceReq * req);
static void AccumSliceReq(SliceReq * inv, SliceReq * req);
static void InventorySliceTree(Slice ** sliceMap, int sliceIndex, SliceReq * req, int numSegmentsInCluster);
static void AssociateSlicesToProcesses(Slice ** sliceMap, int sliceIndex, SliceReq * req, int numSegmentsInCluster);


/* Function AssignGangs runs on the QD and finishes construction of the
 * global slice table for a plan by assigning gangs allocated by the
 * executor factory to the slices of the slice table.
 *
 * On entry, the slice table (at queryDesc->estate->es_sliceTable) has
 * the correct structure (established by InitSliceTable) and has correct
 * gang types (established by function InitRootSlices).
 *
 * Gang assignment involves taking an inventory of the requirements of
 * each slice tree in the slice table, asking the executor factory to
 * allocate a minimal set of gangs that can satisfy any of the slice trees,
 * and associating the allocated gangs with slices in the slice table.
 *
 * The argument utility_segment_index is the segment index to use for
 * 1-gangs that run on QEs.
 *
 * TODO Currently (July 2005) this argument is always supplied as 0, but
 *		there are no cases of the planner specifying a fixed Motion to a
 *		QE, so we don't know the case works.
 *
 * On successful exit, the CDBProcess lists (primaryProcesses, mirrorProcesses)
 * and the Gang pointers (primaryGang, mirrorGang) are set correctly in each
 * slice in the slice table.
 */
void
AssignGangs(QueryDesc *queryDesc, int utility_segment_index)
{
	EState	   *estate = queryDesc->estate;
	SliceTable *sliceTable = estate->es_sliceTable;
	ListCell   *cell;
	Slice	   *slice;
	int			i,
				nslices;
	Slice	  **sliceMap;
	SliceReq	req,
				inv;

	/* Make a map so we can access slices quickly by index.  */
	nslices = list_length(sliceTable->slices);
	sliceMap = (Slice **) palloc(nslices * sizeof(Slice *));
	i = 0;
	foreach(cell, sliceTable->slices)
	{
		slice = (Slice *) lfirst(cell);
		Assert(i == slice->sliceIndex);
		sliceMap[i] = slice;
		i++;
	}

	/* Initialize gang requirement inventory */
	InitSliceReq(&inv);

	/* Capture main slice tree requirement. */
	InventorySliceTree(sliceMap, 0, &inv, queryDesc->planner_segments);

	/* Capture initPlan slice tree requirements. */
	for (i = sliceTable->nMotions + 1; i < nslices; i++)
	{
		InitSliceReq(&req);
		InventorySliceTree(sliceMap, i, &req, queryDesc->planner_segments);
		AccumSliceReq(&inv, &req);
	}

	/*
	 * Get the gangs we'll use.
	 *
	 * As a general rule the first gang is a writer and the rest are readers.
	 * If this happens to be an extended query protocol then all gangs are readers.
	 */
	if (inv.numNgangs > 0)
	{
		inv.vecNgangs = (Gang **) palloc(sizeof(Gang *) * inv.numNgangs);
		for (i = 0; i < inv.numNgangs; i++)
		{
			if (i == 0 && !queryDesc->extended_query)
			{
				inv.vecNgangs[i] = (void *)1;//allocateWriterGang();

				Assert(inv.vecNgangs[i] != NULL);
			}
			else
			{
				inv.vecNgangs[i] = NULL;//allocateGang(GANGTYPE_PRIMARY_READER, getgpsegmentCount(), GpAliveSegmentsInfo.singleton_segindex, queryDesc->portal_name);
			}
		}
	}
	if (inv.num1gangs_primary_reader > 0)
	{
		inv.vec1gangs_primary_reader = (Gang **) palloc(sizeof(Gang *) * inv.num1gangs_primary_reader);
		for (i = 0; i < inv.num1gangs_primary_reader; i++)
		{
			inv.vec1gangs_primary_reader[i] = NULL;//allocateGang(GANGTYPE_PRIMARY_READER, 1, utility_segment_index, queryDesc->portal_name);
		}
	}
	if (inv.num1gangs_entrydb_reader > 0)
	{
		inv.vec1gangs_entrydb_reader = (Gang **) palloc(sizeof(Gang *) * inv.num1gangs_entrydb_reader);
		for (i = 0; i < inv.num1gangs_entrydb_reader; i++)
		{
			inv.vec1gangs_entrydb_reader[i] = NULL;//allocateGang(GANGTYPE_ENTRYDB_READER, 1, -1, queryDesc->portal_name);
		}
	}

	/* Use the gangs to construct the CdbProcess lists in slices. */

	inv.nxtNgang = 0;
  inv.nxt1gang_primary_reader = 0;
  inv.nxt1gang_entrydb_reader = 0;
	AssociateSlicesToProcesses(sliceMap, 0, &inv, queryDesc->planner_segments);		/* Main tree. */

	for (i = sliceTable->nMotions + 1; i < nslices; i++)
	{
		inv.nxtNgang = 0;
        inv.nxt1gang_primary_reader = 0;
        inv.nxt1gang_entrydb_reader = 0;
		AssociateSlicesToProcesses(sliceMap, i, &inv, queryDesc->planner_segments);	/* An initPlan */
	}

	/* Clean up */
	pfree(sliceMap);
	if (inv.vecNgangs != NULL)
		pfree(inv.vecNgangs);
	if (inv.vec1gangs_primary_reader != NULL)
		pfree(inv.vec1gangs_primary_reader);
	if (inv.vec1gangs_entrydb_reader != NULL)
		pfree(inv.vec1gangs_entrydb_reader);

}

void
InitSliceReq(SliceReq * req)
{
	req->numNgangs = 0;
    req->num1gangs_primary_reader = 0;
    req->num1gangs_entrydb_reader = 0;
	req->writer = FALSE;
	req->vecNgangs = NULL;
	req->vec1gangs_primary_reader = NULL;
	req->vec1gangs_entrydb_reader = NULL;
}

void
AccumSliceReq(SliceReq * inv, SliceReq * req)
{
	inv->numNgangs = Max(inv->numNgangs, req->numNgangs);
	inv->num1gangs_primary_reader = Max(inv->num1gangs_primary_reader, req->num1gangs_primary_reader);
	inv->num1gangs_entrydb_reader = Max(inv->num1gangs_entrydb_reader, req->num1gangs_entrydb_reader);
	inv->writer = (inv->writer || req->writer);
}


/*
 * Helper for AssignGangs takes a simple inventory of the gangs required
 * by a slice tree.  Recursive.  Closely coupled with AssignGangs.	Not
 * generally useful.
 */
void
InventorySliceTree(Slice ** sliceMap, int sliceIndex, SliceReq * req,
    int numSegmentsInCluster)
{
	ListCell   *cell;
	int			childIndex;
	Slice	   *slice = sliceMap[sliceIndex];

	switch (slice->gangType)
	{
		case GANGTYPE_UNALLOCATED:
			/* Roots that run on the  QD don't need a gang. */
			break;

		case GANGTYPE_ENTRYDB_READER:
            Assert(slice->gangSize == 1);
			req->num1gangs_entrydb_reader++;
			break;

		case GANGTYPE_PRIMARY_WRITER:
			req->writer = TRUE;
			/* fall through */
		case GANGTYPE_PRIMARY_READER:
			if (is1GangSlice(slice, numSegmentsInCluster))
			{
				req->num1gangs_primary_reader++;
			}
			else
			{
				Assert(slice->gangSize == numSegmentsInCluster);
				req->numNgangs++;
			}
			break;
	}

	foreach(cell, slice->children)
	{
		childIndex = lfirst_int(cell);
		InventorySliceTree(sliceMap, childIndex, req, numSegmentsInCluster);
	}
}

/*
 * Helper for AssignGangs uses the gangs in the inventory to fill in the
 * CdbProcess lists in the slice tree.	Recursive.	Closely coupled with
 * AssignGangs.  Not generally useful.
 */
void
AssociateSlicesToProcesses(Slice ** sliceMap, int sliceIndex, SliceReq * req, int numSegmentsInCluster)
{
	ListCell   *cell;
	int			childIndex;
	Slice	   *slice = sliceMap[sliceIndex];

	switch (slice->gangType)
	{
		case GANGTYPE_UNALLOCATED:
			/* Roots that run on the  QD don't need a gang. */

			slice->primaryProcesses = getCdbProcessesForQD(true);
			break;

		case GANGTYPE_ENTRYDB_READER:
			req->nxt1gang_entrydb_reader++;
			break;

		case GANGTYPE_PRIMARY_WRITER:
			slice->is_writer = req->vecNgangs[req->nxtNgang] == (void *)1;
			req->nxtNgang++;
			break;

		case GANGTYPE_PRIMARY_READER:
			if (is1GangSlice(slice, numSegmentsInCluster))
			{
				req->nxt1gang_primary_reader++;
			}
			else
			{
				slice->is_writer = req->vecNgangs[req->nxtNgang] == (void *)1;
				req->nxtNgang++;
			}
			break;
	}

	foreach(cell, slice->children)
	{
		childIndex = lfirst_int(cell);
		AssociateSlicesToProcesses(sliceMap, childIndex, req, numSegmentsInCluster);
	}
}

/*
 * Choose the execution identity (who does this executor serve?).
 * There are types:
 *
 * 1. No-Op (ignore) -- this occurs when the specified direction is
 *	 NoMovementScanDirection or when Gp_role is GP_ROLE_DISPATCH
 *	 and the current slice belongs to a QE.
 *
 * 2. Executor serves a Root Slice -- this occurs when Gp_role is
 *   GP_ROLE_UTILITY or the current slice is a root.  It corresponds
 *   to the "normal" path through the executor in that we enter the plan
 *   at the top and count on the motion nodes at the fringe of the top
 *   slice to return without ever calling nodes below them.
 *
 * 3. Executor serves a Non-Root Slice on a QE -- this occurs when
 *   Gp_role is GP_ROLE_EXECUTE and the current slice is not a root
 *   slice. It corresponds to a QE running a slice with a motion node on
 *	 top.  The call, thus, returns no tuples (since they all go out
 *	 on the interconnect to the receiver version of the motion node),
 *	 but it does execute the indicated slice down to any fringe
 *	 motion nodes (as in case 2).
 */
GpExecIdentity
getGpExecIdentity(QueryDesc *queryDesc,
				  ScanDirection direction,
				  EState	   *estate)
{
	Slice *currentSlice;

	currentSlice = getCurrentSlice(estate, LocallyExecutingSliceIndex(estate));
	if (currentSlice)
    {
        if (Gp_role == GP_ROLE_EXECUTE ||
            sliceRunsOnQD(currentSlice))
            currentSliceId = currentSlice->sliceIndex;
    }

	/* select the strategy */
	if (direction == NoMovementScanDirection)
	{
		return GP_IGNORE;
	}
	else if (Gp_role == GP_ROLE_DISPATCH && sliceRunsOnQE(currentSlice))
	{
		return GP_IGNORE;
	}
	else if (Gp_role == GP_ROLE_EXECUTE && LocallyExecutingSliceIndex(estate) != RootSliceIndex(estate))
	{
		return GP_NON_ROOT_ON_QE;
	}
	else
	{
		return GP_ROOT_SLICE;
	}
}

/*
 * End the gp-specific part of the executor.
 *
 * In here we collect the dispatch results if there are any, tear
 * down the interconnect if it is set-up.
 */
void mppExecutorFinishup(QueryDesc *queryDesc)
{
	EState	   *estate;
	Slice      *currentSlice;

	/* caller must have switched into per-query memory context already */
	estate = queryDesc->estate;

	currentSlice = getCurrentSlice(estate, LocallyExecutingSliceIndex(estate));

	/*
	 * If QD, wait for QEs to finish and check their results.
	 */
	if (estate->dispatch_data && dispatch_get_results(estate->dispatch_data))
	{
		CdbDispatchResults *pr = dispatch_get_results(estate->dispatch_data);
		HTAB 			   *aopartcounts = NULL;

		/*
		 * If we are finishing a query before all the tuples of the query
		 * plan were fetched we must call ExecSquelchNode before checking
		 * the dispatch results in order to tell the nodes below we no longer
		 * need any more tuples.
		 */
		if (!estate->es_got_eos)
		{
			ExecSquelchNode(queryDesc->planstate);
		}

		/*
		 * Wait for completion of all QEs.
		 */
		dispatch_wait(estate->dispatch_data);

		/* If top slice was delegated to QEs, get num of rows processed. */
		if (sliceRunsOnQE(currentSlice))
		{
			estate->es_processed +=
				cdbdisp_sumCmdTuples(pr, LocallyExecutingSliceIndex(estate));
			estate->es_lastoid =
				cdbdisp_maxLastOid(pr, LocallyExecutingSliceIndex(estate));
			aopartcounts = cdbdisp_sumAoPartTupCount(estate->es_result_partitions, pr);
		}

		cdbdisp_handleModifiedCatalogOnSegments(pr, UpdateCatalogModifiedOnSegments);

		/* sum up rejected rows if any (single row error handling only) */
		cdbdisp_sumRejectedRows(pr);

		/* sum up inserted rows into any AO relation */
		if (aopartcounts)
		{
			/* counts from a partitioned AO table */

			ListCell *lc;

			foreach(lc, estate->es_result_aosegnos)
			{
				SegfileMapNode *map = lfirst(lc);
				struct {
					Oid relid;
			   		int64 tupcount;
				} *entry;
				bool found;

				entry = hash_search(aopartcounts,
									&(map->relid),
									HASH_FIND,
									&found);

				if (found)
				{
					Relation r = heap_open(map->relid, AccessShareLock);
					if(RelationIsAoRows(r) || RelationIsParquet(r))
					{
						/* The total tuple count would be not necessary in HAWQ 2.0 */
						/*
						UpdateMasterAosegTotals(r, map->segno, entry->tupcount);
						*/
					}
					heap_close(r, NoLock);
				}
			}

		}
		else
		{
			/* counts from a (non partitioned) AO table */

			ExecUpdateAOtupCount(estate->es_result_relations,
								 estate->es_snapshot,
								 estate->es_num_result_relations,
								 estate->es_processed);
		}

		/*
		 * Check and free the results of all gangs. If any QE had an
		 * error, report it and exit to our error handler via PG_THROW.
		 * NB: This call doesn't wait, because we already waited above.
		 */
		dispatch_cleanup(estate->dispatch_data);
		estate->dispatch_data = NULL;
	}

	/* Teardown the Interconnect */
	if (estate->es_interconnect_is_setup)
	{
		/*
		 * MPP-3413: If we got here during cancellation of a cursor,
		 * we need to set the "forceEos" argument correctly --
		 * otherwise we potentially hang (cursors cancel on the QEs,
		 * mark the estate to "cancelUnfinished" and then try to do a
		 * normal interconnect teardown).
		 */
		TeardownInterconnect(estate->interconnect_context, estate->motionlayer_context, estate->cancelUnfinished);
		estate->es_interconnect_is_setup = false;
	}
}

/*
 * Cleanup the gp-specific parts of the query executor.
 *
 * Will normally be called after an error from within a CATCH block.
 */
void mppExecutorCleanup(QueryDesc *queryDesc)
{
	EState	   *estate;

	/* caller must have switched into per-query memory context already */
	estate = queryDesc->estate;

	/*
	 * If this query is being canceled, record that when the gpperfmon
	 * is enabled.
	 */
	if (gp_enable_gpperfmon &&
		Gp_role == GP_ROLE_DISPATCH &&
		queryDesc->gpmon_pkt &&
		QueryCancelCleanup)
	{			
		gpmon_qlog_query_canceling(queryDesc->gpmon_pkt);

		if (gp_cancel_query_print_log)
		{
			elog(LOG, "canceling query (%d, %d)",
				 queryDesc->gpmon_pkt->u.qlog.key.ssid,
				 queryDesc->gpmon_pkt->u.qlog.key.ccnt);
		}
	}

	/*
	 * Delaying the cancellation for a specified time.
	 */
	if (Gp_role == GP_ROLE_DISPATCH &&
		QueryCancelCleanup &&
		gp_cancel_query_delay_time > 0)
	{
		pg_usleep(gp_cancel_query_delay_time * 1000);
	}

	/*
	 * Request any commands still executing on qExecs to stop.
	 * Wait for them to finish and clean up the dispatching structures.
	 * Replace current error info with QE error info if more interesting.
	 */
	if (estate->dispatch_data)
	{
		/*
		 * If we are finishing a query before all the tuples of the query
		 * plan were fetched we must call ExecSquelchNode before checking
		 * the dispatch results in order to tell the nodes below we no longer
		 * need any more tuples.
		 */
		if (estate->es_interconnect_is_setup && !estate->es_got_eos)
			ExecSquelchNode(queryDesc->planstate);

		dispatch_catch_error(estate->dispatch_data);
	}

	/* Clean up the interconnect. */
	if (estate->es_interconnect_is_setup)
	{
		TeardownInterconnect(estate->interconnect_context, estate->motionlayer_context, true /* force EOS */);
		estate->es_interconnect_is_setup = false;
	}
	
	/**
	 * Perfmon related stuff.
	 */
	if (gp_enable_gpperfmon 
			&& Gp_role == GP_ROLE_DISPATCH
			&& queryDesc->gpmon_pkt)
	{			
		gpmon_qlog_query_error(queryDesc->gpmon_pkt);
		pfree(queryDesc->gpmon_pkt);
		queryDesc->gpmon_pkt = NULL;
	}

	/* Workfile manager per-query resource accounting */
	WorkfileQueryspace_ReleaseEntry();

	ReportOOMConsumption();

	/**
	 * Since there was an error, clean up the function scan stack.
	 */
	if (gp_resqueue_memory_policy != RESQUEUE_MEMORY_POLICY_NONE)
	{
		SPI_InitMemoryReservation();
	}
}

void
initGpmonPktForDefunctOperators(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(IsA(planNode, SeqScan) ||
		   IsA(planNode, AppendOnlyScan) ||
           IsA(planNode, ParquetScan));
	insist_log(false, "SeqScan/AppendOnlyScan are defunct");
}

/*
 * The funcion pointers to init gpmon package for each plan node. 
 * The order of the function pointers are the same as the one defined in
 * NodeTag (nodes.h).
 */
void (*initGpmonPktFuncs[])(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate) = 
{
	&initGpmonPktForResult, /* T_Result */
	&initGpmonPktForAppend, /* T_Append */
	&initGpmonPktForSequence, /* T_Sequence */
	&initGpmonPktForBitmapAnd, /* T_BitmapAnd */
	&initGpmonPktForBitmapOr, /* T_BitmapOr */
	&initGpmonPktForDefunctOperators, /* T_SeqScan */
	&initGpmonPktForExternalScan, /* T_ExternalScan */
	&initGpmonPktForDefunctOperators, /* T_AppendOnlyScan */
	&initGpmonPktForTableScan, /* T_TableScan */
	&initGpmonPktForDynamicTableScan, /* reserved for T_DynamicTableScan */
	&initGpmonPktForIndexScan, /* T_IndexScan */
	&initGpmonPktForDynamicIndexScan, /* T_DynamicIndexScan */
	&initGpmonPktForBitmapIndexScan, /* T_BitmapIndexScan */
	&initGpmonPktForBitmapHeapScan, /* T_BitmapHeapScan */
	&initGpmonPktForBitmapAppendOnlyScan, /* T_BitmapAppendOnlyScan */
	&initGpmonPktForBitmapTableScan, /* T_BitmapTableScan */
	&initGpmonPktForTidScan, /* T_TidScan */
	&initGpmonPktForSubqueryScan, /* T_SubqueryScan */
	&initGpmonPktForFunctionScan, /*  T_FunctionScan */
	&initGpmonPktForValuesScan, /* T_ValuesScan */
	&initGpmonPktForNestLoop, /* T_NestLoop */
	&initGpmonPktForMergeJoin, /* T_MergeJoin */
	&initGpmonPktForHashJoin, /* T_HashJoin */
	&initGpmonPktForMaterial, /* T_Material */
	&initGpmonPktForSort, /* T_Sort */
	&initGpmonPktForAgg, /* T_Agg */
	&initGpmonPktForUnique, /* T_Unique */
	&initGpmonPktForHash, /* T_Hash */
	&initGpmonPktForSetOp, /* T_SetOp */
	&initGpmonPktForLimit, /* T_Limit */
	&initGpmonPktForMotion, /* T_Motion */
	&initGpmonPktForShareInputScan, /* T_ShareInputScan */
	&initGpmonPktForWindow, /* T_Window */
	&initGpmonPktForRepeat /* T_Repeat */
	/* T_Plan_End */
};

/*
 * Define a compile assert so that when a new executor node is added,
 * this assert will fire up, and the proper change will be made to
 * the above initGpmonPktFuncs array.
 */
typedef char assertion_failed_initGpmonPktFuncs \
	[((T_Plan_End - T_Plan_Start) == \
	  (sizeof(initGpmonPktFuncs) / sizeof(&initGpmonPktForResult))) - 1];


/*
 * sendInitGpmonPkts -- Send init Gpmon package for the node and its child
 * nodes that are running on the same slice of the given node.
 *
 * This function is only used by the Append executor node, since Append does
 * not call ExecInitNode() for each of its child nodes during initialization
 * time. However, Gpmon requires each node to be initialized to show the
 * whole plan tree.
 */
void
sendInitGpmonPkts(Plan *node, EState *estate)
{
	gpmon_packet_t gpmon_pkt;
   
	if (node == NULL)
		return;
	
	switch (nodeTag(node))
	{
		case T_Append:
		{
			int first_plan, last_plan;
			Append *appendnode = (Append *)node;

			initGpmonPktForAppend(node, &gpmon_pkt, estate);

			if (appendnode->isTarget && estate->es_evTuple != NULL)
			{
				first_plan = estate->es_result_relation_info - estate->es_result_relations;
				Assert(first_plan >= 0 && first_plan < list_length(appendnode->appendplans));
				last_plan = first_plan;
			}
			else
			{
				first_plan = 0;
				last_plan = list_length(appendnode->appendplans) - 1;
			}
			
			for (; first_plan <= last_plan; first_plan++)
			{
				Plan *initNode = (Plan *)list_nth(appendnode->appendplans, first_plan);
				
				sendInitGpmonPkts(initNode, estate);
			}

			break;
		}

		case T_Sequence:
		{
			Sequence *sequence = (Sequence *)node;

			ListCell *lc;
			foreach (lc, sequence->subplans)
			{
				Plan *subplan = (Plan *)lfirst(lc);
				
				sendInitGpmonPkts(subplan, estate);
			}
			break;
		}

		case T_BitmapAnd:
		{
			ListCell *lc;
			
			initGpmonPktForBitmapAnd(node, &gpmon_pkt, estate);
			foreach (lc, ((BitmapAnd*)node)->bitmapplans)
			{
				sendInitGpmonPkts((Plan *)lfirst(lc), estate);
			}
			
			break;
		}
		
		case T_BitmapOr:
		{
			ListCell *lc;
			
			initGpmonPktForBitmapOr(node, &gpmon_pkt, estate);
			foreach (lc, ((BitmapOr*)node)->bitmapplans)
			{
				sendInitGpmonPkts((Plan *)lfirst(lc), estate);
			}
			
			break;
		}

		case T_SeqScan:
		case T_AppendOnlyScan:
		case T_DynamicTableScan:
		case T_ParquetScan:
		case T_ExternalScan:
		case T_IndexScan:
		case T_BitmapIndexScan:
		case T_TidScan:
		case T_FunctionScan:
		case T_ValuesScan:
		{
			initGpmonPktFuncs[nodeTag(node) - T_Plan_Start](node, &gpmon_pkt, estate);

			break;
		}

		case T_Result:
		case T_BitmapHeapScan:
		case T_BitmapAppendOnlyScan:
		case T_BitmapTableScan:
		case T_ShareInputScan:
		case T_Material:
		case T_Sort:
		case T_Agg:
		case T_Window:
		case T_Unique:
		case T_Hash:
		case T_SetOp:
		case T_Limit:
		case T_Repeat:
		{
			initGpmonPktFuncs[nodeTag(node) - T_Plan_Start](node, &gpmon_pkt, estate);
			sendInitGpmonPkts(outerPlan(node), estate);
			
			break;
		}

		case T_SubqueryScan:
		{
			/**
			 * Recurse into subqueryscan node's subplan.
			 */
			sendInitGpmonPkts(((SubqueryScan *)node)->subplan, estate);
			break;
		}

		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
		{
			initGpmonPktFuncs[nodeTag(node) - T_Plan_Start](node, &gpmon_pkt, estate);
			sendInitGpmonPkts(outerPlan(node), estate);
			sendInitGpmonPkts(innerPlan(node), estate);
			
			break;
		}

		case T_Motion:
			/*
			 * Do not need to send init package since Motion node is always initialized
			 * Since all nodes under Motion are running on a different slice, we stop
			 * here.
			 */
			break;

		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
			break;
	}
}

void ResetExprContext(ExprContext *econtext)
{
	MemoryContext memctxt = econtext->ecxt_per_tuple_memory;
	if(memctxt->allBytesAlloc - memctxt->allBytesFreed > 50000)
		MemoryContextReset(memctxt);
}

/**
 * This method is used to determine how much memory a specific operator
 * is supposed to use (in KB). 
 */
uint64 PlanStateOperatorMemKB(const PlanState *ps)
{
	Assert(ps);
	Assert(ps->plan);
	uint64 result = 0;
	if (ps->plan->operatorMemKB == 0)
	{
		/**
		 * There are some statements that do not go through the resource queue and these
		 * plans dont get decorated with the operatorMemKB. Someday, we should fix resource queues.
		 */
		result = work_mem;
	}
	else
	{
		result = ps->plan->operatorMemKB;
	}
	
	return result;
}

/**
 * Methods to find motionstate object within a planstate tree given a motion id (which is the same as slice index)
 */
typedef struct MotionStateFinderContext
{
	int motionId; /* Input */
	MotionState *motionState; /* Output */
} MotionStateFinderContext;

/**
 * Walker method that finds motion state node within a planstate tree.
 */
static CdbVisitOpt
MotionStateFinderWalker(PlanState *node,
				  void *context)
{
	Assert(context);
	MotionStateFinderContext *ctx = (MotionStateFinderContext *) context;

	if (IsA(node, MotionState))
	{
		MotionState *ms = (MotionState *) node;
		Motion *m = (Motion *) ms->ps.plan;
		if (m->motionID == ctx->motionId)
		{
			Assert(ctx->motionState == NULL);
			ctx->motionState = ms;
			return CdbVisit_Skip;	/* don't visit subtree */
		}
	}

	/* Continue walking */
	return CdbVisit_Walk;
}

/**
 * Given a slice index, find the motionstate that corresponds to this slice index. This will iterate over the planstate tree
 * to get the right node.
 */
MotionState *getMotionState(struct PlanState *ps, int sliceIndex)
{
	Assert(ps);
	Assert(sliceIndex > -1);

	MotionStateFinderContext ctx;
	ctx.motionId = sliceIndex;
	ctx.motionState = NULL;
	planstate_walk_node(ps, MotionStateFinderWalker, &ctx);
	Assert(ctx.motionState != NULL);
	return ctx.motionState;
}

/**
 * Provide index of locally executing slice
 */
int LocallyExecutingSliceIndex(EState *estate)
{
	Assert(estate);
	return (!estate->es_sliceTable ? 0 : estate->es_sliceTable->localSlice);
}

/**
 * Provide root slice of locally executing slice.
 */
int RootSliceIndex(EState *estate)
{
	Assert(estate);
	int result = 0;

	if (estate->es_sliceTable)
	{
		Slice *localSlice = list_nth(estate->es_sliceTable->slices, LocallyExecutingSliceIndex(estate));
		result = localSlice->rootIndex;
	}

	return result;
}
