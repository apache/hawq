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
 * nodeSubplan.c
 *	  routines to support subselects
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/nodeSubplan.c,v 1.80.2.2 2007/02/02 00:07:28 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecSubPlan  - process a subselect
 *		ExecInitSubPlan - initialize a subselect
 *		ExecEndSubPlan	- shut down a subselect
 */
#include "postgres.h"

#include <math.h>

#include "access/heapam.h"
#include "executor/executor.h"
#include "executor/nodeSubplan.h"
#include "cdb/cdbexplain.h"             /* cdbexplain_recvSubplanStats */
#include "cdb/cdbvars.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbdisp.h"
#include "cdb/ml_ipc.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "cdb/dispatcher.h"


#include <math.h>                       /* ceil() */


static Datum ExecHashSubPlan(SubPlanState *node,
				ExprContext *econtext,
				bool *isNull);
static Datum ExecScanSubPlan(SubPlanState *node,
				ExprContext *econtext,
				bool *isNull);
static void buildSubPlanHash(SubPlanState *node, ExprContext * econtext);
static bool findPartialMatch(TupleHashTable hashtable, TupleTableSlot *slot);
static bool slotAllNulls(TupleTableSlot *slot);
static bool slotNoNulls(TupleTableSlot *slot);


/* ----------------------------------------------------------------
 *		ExecSubPlan
 * ----------------------------------------------------------------
 */
Datum
ExecSubPlan(SubPlanState *node,
			ExprContext *econtext,
			bool *isNull,
			ExprDoneCond *isDone)
{
	SubPlan    *subplan = (SubPlan *) node->xprstate.expr;

	/* Set default values for result flags: non-null, not a set result */
	*isNull = false;
	if (isDone)
		*isDone = ExprSingleResult;

	insist_log(subplan->setParam == NIL, "cannot set parent parameters from subquery");

	if (subplan->useHashTable)
		return ExecHashSubPlan(node, econtext, isNull);
	else
		return ExecScanSubPlan(node, econtext, isNull);
}

/*
 * ExecHashSubPlan: store subselect result in an in-memory hash table
 */
static Datum
ExecHashSubPlan(SubPlanState *node,
				ExprContext *econtext,
				bool *isNull)
{
	SubPlan    *subplan = (SubPlan *) node->xprstate.expr;
	PlanState  *planstate = node->planstate;
	TupleTableSlot *slot;

	/* Shouldn't have any direct correlation Vars */
	if (subplan->parParam != NIL || node->args != NIL)
		elog(ERROR, "hashed subplan with direct correlation not supported");

	/*
	 * If first time through or we need to rescan the subplan, build the hash
	 * table.
	 */
	if (node->hashtable == NULL || planstate->chgParam != NULL)
		buildSubPlanHash(node, econtext);

	/*
	 * The result for an empty subplan is always FALSE; no need to evaluate
	 * lefthand side.
	 */
	*isNull = false;
	if (!node->havehashrows && !node->havenullrows)
		return BoolGetDatum(false);

	/*
	 * Evaluate lefthand expressions and form a projection tuple. First we
	 * have to set the econtext to use (hack alert!).
	 */
	node->projLeft->pi_exprContext = econtext;
	slot = ExecProject(node->projLeft, NULL);

	/*
	 * Note: because we are typically called in a per-tuple context, we have
	 * to explicitly clear the projected tuple before returning. Otherwise,
	 * we'll have a double-free situation: the per-tuple context will probably
	 * be reset before we're called again, and then the tuple slot will think
	 * it still needs to free the tuple.
	 */

	/*
	 * If the LHS is all non-null, probe for an exact match in the main hash
	 * table.  If we find one, the result is TRUE. Otherwise, scan the
	 * partly-null table to see if there are any rows that aren't provably
	 * unequal to the LHS; if so, the result is UNKNOWN.  (We skip that part
	 * if we don't care about UNKNOWN.) Otherwise, the result is FALSE.
	 *
	 * Note: the reason we can avoid a full scan of the main hash table is
	 * that the combining operators are assumed never to yield NULL when both
	 * inputs are non-null.  If they were to do so, we might need to produce
	 * UNKNOWN instead of FALSE because of an UNKNOWN result in comparing the
	 * LHS to some main-table entry --- which is a comparison we will not even
	 * make, unless there's a chance match of hash keys.
	 */
	if (slotNoNulls(slot))
	{
		if (node->havehashrows &&
			LookupTupleHashEntry(node->hashtable, slot, NULL) != NULL)
		{
			ExecClearTuple(slot);
			return BoolGetDatum(true);
		}
		if (node->havenullrows &&
			findPartialMatch(node->hashnulls, slot))
		{
			ExecClearTuple(slot);
			*isNull = true;
			return BoolGetDatum(false);
		}
		ExecClearTuple(slot);
		return BoolGetDatum(false);
	}

	/*
	 * When the LHS is partly or wholly NULL, we can never return TRUE. If we
	 * don't care about UNKNOWN, just return FALSE.  Otherwise, if the LHS is
	 * wholly NULL, immediately return UNKNOWN.  (Since the combining
	 * operators are strict, the result could only be FALSE if the sub-select
	 * were empty, but we already handled that case.) Otherwise, we must scan
	 * both the main and partly-null tables to see if there are any rows that
	 * aren't provably unequal to the LHS; if so, the result is UNKNOWN.
	 * Otherwise, the result is FALSE.
	 */
	if (node->hashnulls == NULL)
	{
		ExecClearTuple(slot);
		return BoolGetDatum(false);
	}
	if (slotAllNulls(slot))
	{
		ExecClearTuple(slot);
		*isNull = true;
		return BoolGetDatum(false);
	}
	/* Scan partly-null table first, since more likely to get a match */
	if (node->havenullrows &&
		findPartialMatch(node->hashnulls, slot))
	{
		ExecClearTuple(slot);
		*isNull = true;
		return BoolGetDatum(false);
	}
	if (node->havehashrows &&
		findPartialMatch(node->hashtable, slot))
	{
		ExecClearTuple(slot);
		*isNull = true;
		return BoolGetDatum(false);
	}
	ExecClearTuple(slot);
	return BoolGetDatum(false);
}

/*
 * ExecScanSubPlan: default case where we have to rescan subplan each time
 */
static Datum
ExecScanSubPlan(SubPlanState *node,
				ExprContext *econtext,
				bool *isNull)
{
	SubPlan    *subplan = (SubPlan *) node->xprstate.expr;
	PlanState  *planstate = node->planstate;
	SubLinkType subLinkType = subplan->subLinkType;
	MemoryContext oldcontext;
	TupleTableSlot *slot;
	Datum		result;
	bool		found = false;	/* TRUE if got at least one subplan tuple */
	ListCell   *pvar;
	ListCell   *l;
	ArrayBuildState *astate = NULL;

	/*
	 * We are probably in a short-lived expression-evaluation context. Switch
	 * to the per-query context for manipulating the child plan's chgParam,
	 * calling ExecProcNode on it, etc.
	 */
	oldcontext = MemoryContextSwitchTo(node->sub_estate->es_query_cxt);

	/*
	 * Set Params of this plan from parent plan correlation values. (Any
	 * calculation we have to do is done in the parent econtext, since the
	 * Param values don't need to have per-query lifetime.)
	 */
	Assert(list_length(subplan->parParam) == list_length(node->args));

	forboth(l, subplan->parParam, pvar, node->args)
	{
		int			paramid = lfirst_int(l);
		ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

		prm->value = ExecEvalExprSwitchContext((ExprState *) lfirst(pvar),
											   econtext,
											   &(prm->isnull),
											   NULL);
		planstate->chgParam = bms_add_member(planstate->chgParam, paramid);
	}

	/*
	 * Now that we've set up its parameters, we can reset the subplan.
	 */
	ExecReScan(planstate, NULL);

	/*
	 * For all sublink types except EXPR_SUBLINK and ARRAY_SUBLINK, the result
	 * is boolean as are the results of the combining operators. We combine
	 * results across tuples (if the subplan produces more than one) using OR
	 * semantics for ANY_SUBLINK or AND semantics for ALL_SUBLINK.
	 * (ROWCOMPARE_SUBLINK doesn't allow multiple tuples from the subplan.)
	 * NULL results from the combining operators are handled according to the
	 * usual SQL semantics for OR and AND.	The result for no input tuples is
	 * FALSE for ANY_SUBLINK, TRUE for {ALL_SUBLINK, NOT_EXISTS_SUBLINK}, NULL for
	 * ROWCOMPARE_SUBLINK.
	 *
	 * For EXPR_SUBLINK we require the subplan to produce no more than one
	 * tuple, else an error is raised.	If zero tuples are produced, we return
	 * NULL.  Assuming we get a tuple, we just use its first column (there can
	 * be only one non-junk column in this case).
	 *
	 * For ARRAY_SUBLINK we allow the subplan to produce any number of tuples,
	 * and form an array of the first column's values.  Note in particular
	 * that we produce a zero-element array if no tuples are produced (this is
	 * a change from pre-8.3 behavior of returning NULL).
	 */
	result = BoolGetDatum(subLinkType == ALL_SUBLINK || subLinkType == NOT_EXISTS_SUBLINK);
	*isNull = false;

	for (slot = ExecProcNode(planstate);
		 !TupIsNull(slot);
		 slot = ExecProcNode(planstate))
	{
		Datum		rowresult;
		bool		rownull;
		int			col;
		ListCell   *plst;

		if (subLinkType == EXISTS_SUBLINK || subLinkType == NOT_EXISTS_SUBLINK)
		{
			found = true;
			bool val = true;
			if (subLinkType == NOT_EXISTS_SUBLINK)
			{
				val = false;
			}
			result = BoolGetDatum(val);
			break;
		}

		if (subLinkType == EXPR_SUBLINK)
		{
			/* cannot allow multiple input tuples for EXPR sublink */
			if (found)
				ereport(ERROR,
						(errcode(ERRCODE_CARDINALITY_VIOLATION),
						 errmsg("more than one row returned by a subquery used as an expression")));
			found = true;

			/*
			 * We need to copy the subplan's tuple in case the result is of
			 * pass-by-ref type --- our return value will point into this
			 * copied tuple!  Can't use the subplan's instance of the tuple
			 * since it won't still be valid after next ExecProcNode() call.
			 * node->curTuple keeps track of the copied tuple for eventual
			 * freeing.
			 */
			MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

			if (node->curTuple)
				pfree(node->curTuple);

			node->curTuple = ExecCopySlotMemTuple(slot);
			MemoryContextSwitchTo(node->sub_estate->es_query_cxt);

			result = memtuple_getattr(node->curTuple, slot->tts_mt_bind, 1, isNull);
			/* keep scanning subplan to make sure there's only one tuple */
			continue;
		}

		if (subLinkType == ARRAY_SUBLINK)
		{
			Datum		dvalue;
			bool		disnull;

			found = true;
			/* stash away current value */
			Assert(subplan->firstColType == slot->tts_tupleDescriptor->attrs[0]->atttypid);
			dvalue = slot_getattr(slot, 1, &disnull);
			astate = accumArrayResult(astate, dvalue, disnull,
									  subplan->firstColType, oldcontext);
			/* keep scanning subplan to collect all values */
			continue;
		}

		/* cannot allow multiple input tuples for ROWCOMPARE sublink either */
		if (subLinkType == ROWCOMPARE_SUBLINK && found)
			ereport(ERROR,
					(errcode(ERRCODE_CARDINALITY_VIOLATION),
					 errmsg("more than one row returned by a subquery used as an expression")));

		found = true;

		/*
		 * For ALL, ANY, and ROWCOMPARE sublinks, load up the Params
		 * representing the columns of the sub-select, and then evaluate the
		 * combining expression.
		 */
		col = 1;
		foreach(plst, subplan->paramIds)
		{
			int			paramid = lfirst_int(plst);
			ParamExecData *prmdata;

			prmdata = &(econtext->ecxt_param_exec_vals[paramid]);
			Assert(prmdata->execPlan == NULL);
			prmdata->value = slot_getattr(slot, col, &(prmdata->isnull));
			col++;
		}

		rowresult = ExecEvalExprSwitchContext(node->testexpr, econtext,
											  &rownull, NULL);

		if (subLinkType == ANY_SUBLINK)
		{
			/* combine across rows per OR semantics */
			if (rownull)
				*isNull = true;
			else if (DatumGetBool(rowresult))
			{
				result = BoolGetDatum(true);
				*isNull = false;
				break;			/* needn't look at any more rows */
			}
		}
		else if (subLinkType == ALL_SUBLINK)
		{
			/* combine across rows per AND semantics */
			if (rownull)
				*isNull = true;
			else if (!DatumGetBool(rowresult))
			{
				result = BoolGetDatum(false);
				*isNull = false;
				break;			/* needn't look at any more rows */
			}
		}
		else
		{
			/* must be ROWCOMPARE_SUBLINK */
			result = rowresult;
			*isNull = rownull;
		}
	}

	if (!found)
	{
		/*
		 * deal with empty subplan result.	result/isNull were previously
		 * initialized correctly for all sublink types except EXPR, ARRAY, and
		 * ROWCOMPARE; for those, return NULL.
		 */
		if (subLinkType == EXPR_SUBLINK ||
			subLinkType == ARRAY_SUBLINK ||
			subLinkType == ROWCOMPARE_SUBLINK)
		{
			result = (Datum) 0;
			*isNull = true;
		}
	}
	else if (subLinkType == ARRAY_SUBLINK)
	{
		Assert(astate != NULL);
		/* We return the result in the caller's context */
		result = makeArrayResult(astate, oldcontext);
	}

	MemoryContextSwitchTo(oldcontext);

	return result;
}

/*
 * buildSubPlanHash: load hash table by scanning subplan output.
 */
static void
buildSubPlanHash(SubPlanState *node, ExprContext *econtext)
{
	SubPlan    *subplan = (SubPlan *) node->xprstate.expr;
	PlanState  *planstate = node->planstate;
	int			ncols = list_length(subplan->paramIds);
	ExprContext *innerecontext = node->innerecontext;
	MemoryContext oldcontext;
	int			nbuckets;
	TupleTableSlot *slot;

	Assert(subplan->subLinkType == ANY_SUBLINK);

	/*
	 * If we already had any hash tables, destroy 'em; then create empty hash
	 * table(s).
	 *
	 * If we need to distinguish accurately between FALSE and UNKNOWN (i.e.,
	 * NULL) results of the IN operation, then we have to store subplan output
	 * rows that are partly or wholly NULL.  We store such rows in a separate
	 * hash table that we expect will be much smaller than the main table. (We
	 * can use hashing to eliminate partly-null rows that are not distinct. We
	 * keep them separate to minimize the cost of the inevitable full-table
	 * searches; see findPartialMatch.)
	 *
	 * If it's not necessary to distinguish FALSE and UNKNOWN, then we don't
	 * need to store subplan output rows that contain NULL.
	 */
	ExecEagerFreeSubPlan(node);
	
	nbuckets = (int) ceil(planstate->plan->plan_rows);
	if (nbuckets < 1)
		nbuckets = 1;

	node->hashtable = BuildTupleHashTable(ncols,
										  node->keyColIdx,
										  node->eqfunctions,
										  node->hashfunctions,
										  nbuckets,
										  sizeof(TupleHashEntryData),
										  node->hashtablecxt,
										  node->hashtempcxt);

	if (!subplan->unknownEqFalse)
	{
		if (ncols == 1)
			nbuckets = 1;		/* there can only be one entry */
		else
		{
			nbuckets /= 16;
			if (nbuckets < 1)
				nbuckets = 1;
		}
		node->hashnulls = BuildTupleHashTable(ncols,
											  node->keyColIdx,
											  node->eqfunctions,
											  node->hashfunctions,
											  nbuckets,
											  sizeof(TupleHashEntryData),
											  node->hashtablecxt,
											  node->hashtempcxt);
	}

	/*
	 * We are probably in a short-lived expression-evaluation context. Switch
	 * to the child plan's per-query context for calling ExecProcNode.
	 */
	oldcontext = MemoryContextSwitchTo(node->sub_estate->es_query_cxt);

	/*
	 * Reset subplan to start.
	 */
	ExecReScan(planstate, NULL);

	/*
	 * Scan the subplan and load the hash table(s).  Note that when there are
	 * duplicate rows coming out of the sub-select, only one copy is stored.
	 */
	for (slot = ExecProcNode(planstate);
		 !TupIsNull(slot);
		 slot = ExecProcNode(planstate))
	{
		int			col = 1;
		ListCell   *plst;
		bool		isnew;

		/*
		 * Load up the Params representing the raw sub-select outputs, then
		 * form the projection tuple to store in the hashtable.
		 */
		foreach(plst, subplan->paramIds)
		{
			int			paramid = lfirst_int(plst);
			ParamExecData *prmdata;

			prmdata = &(innerecontext->ecxt_param_exec_vals[paramid]);
			Assert(prmdata->execPlan == NULL);
			prmdata->value = slot_getattr(slot, col,
										  &(prmdata->isnull));
			col++;
		}
		slot = ExecProject(node->projRight, NULL);

		/*
		 * If result contains any nulls, store separately or not at all.
		 */
		if (slotNoNulls(slot))
		{
			(void) LookupTupleHashEntry(node->hashtable, slot, &isnew);
			node->havehashrows = true;
		}
		else if (node->hashnulls)
		{
			(void) LookupTupleHashEntry(node->hashnulls, slot, &isnew);
			node->havenullrows = true;
		}

		/*
		 * Reset innerecontext after each inner tuple to free any memory used
		 * in hash computation or comparison routines.
		 */
		ResetExprContext(innerecontext);
	}

	/*
	 * Since the projected tuples are in the sub-query's context and not the
	 * main context, we'd better clear the tuple slot before there's any
	 * chance of a reset of the sub-query's context.  Else we will have the
	 * potential for a double free attempt.  (XXX possibly no longer needed,
	 * but can't hurt.)
	 */
	ExecClearTuple(node->projRight->pi_slot);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * findPartialMatch: does the hashtable contain an entry that is not
 * provably distinct from the tuple?
 *
 * We have to scan the whole hashtable; we can't usefully use hashkeys
 * to guide probing, since we might get partial matches on tuples with
 * hashkeys quite unrelated to what we'd get from the given tuple.
 */
static bool
findPartialMatch(TupleHashTable hashtable, TupleTableSlot *slot)
{
	int			numCols = hashtable->numCols;
	AttrNumber *keyColIdx = hashtable->keyColIdx;
	TupleHashIterator hashiter;
	TupleHashEntry entry;

	InitTupleHashIterator(hashtable, &hashiter);
	while ((entry = ScanTupleHashTable(&hashiter)) != NULL)
	{
		ExecStoreMemTuple(entry->firstTuple, hashtable->tableslot, false);
		if (!execTuplesUnequal(hashtable->tableslot, slot,
							   numCols, keyColIdx,
							   hashtable->eqfunctions,
							   hashtable->tempcxt))
		{
			TermTupleHashIterator(&hashiter);
			return true;
		}
	}
	/* No TermTupleHashIterator call needed here */
	return false;
}

/*
 * slotAllNulls: is the slot completely NULL?
 *
 * This does not test for dropped columns, which is OK because we only
 * use it on projected tuples.
 */
static bool
slotAllNulls(TupleTableSlot *slot)
{
	int			ncols = slot->tts_tupleDescriptor->natts;
	int			i;

	for (i = 1; i <= ncols; i++)
	{
		if (!slot_attisnull(slot, i))
			return false;
	}
	return true;
}

/*
 * slotNoNulls: is the slot entirely not NULL?
 *
 * This does not test for dropped columns, which is OK because we only
 * use it on projected tuples.
 */
static bool
slotNoNulls(TupleTableSlot *slot)
{
	int			ncols = slot->tts_tupleDescriptor->natts;
	int			i;

	for (i = 1; i <= ncols; i++)
	{
		if (slot_attisnull(slot, i))
			return false;
	}
	return true;
}

/* ----------------------------------------------------------------
 *		ExecInitSubPlan
 *
 * Note: the eflags are those passed to the parent plan node of this
 * subplan; they don't directly describe the execution conditions the
 * subplan will face.
 * ----------------------------------------------------------------
 */
void
ExecInitSubPlan(SubPlanState *node, EState *estate, int eflags)
{
	SubPlan    *subplan = (SubPlan *) node->xprstate.expr;
	EState	   *sp_estate;

	/*
	 * initialize my state
	 */
	node->needShutdown = false;
	node->curTuple = NULL;
	node->projLeft = NULL;
	node->projRight = NULL;
	node->hashtable = NULL;
	node->hashnulls = NULL;
	node->hashtablecxt = NULL;
	node->hashtempcxt = NULL;
	node->innerecontext = NULL;
	node->keyColIdx = NULL;
	node->eqfunctions = NULL;
	node->hashfunctions = NULL;
    node->cdbextratextbuf = NULL;

	/*
	 * create an EState for the subplan
	 *
	 * The subquery needs its own EState because it has its own rangetable. It
	 * shares our Param ID space and es_query_cxt, however.  XXX if rangetable
	 * access were done differently, the subquery could share our EState,
	 * which would eliminate some thrashing about in this module...
	 */
	sp_estate = CreateSubExecutorState(estate);
	node->sub_estate = sp_estate;

	sp_estate->es_range_table = estate->es_range_table;
	sp_estate->es_plannedstmt = estate->es_plannedstmt;
	sp_estate->es_param_list_info = estate->es_param_list_info;
	sp_estate->es_param_exec_vals = estate->es_param_exec_vals;
	sp_estate->es_tupleTable =
		ExecCreateTupleTable(ExecCountSlotsNode(exec_subplan_get_plan(sp_estate->es_plannedstmt, subplan)) + 10);
	sp_estate->es_snapshot = estate->es_snapshot;
	sp_estate->es_crosscheck_snapshot = estate->es_crosscheck_snapshot;
	sp_estate->es_instrument = estate->es_instrument;

    sp_estate->es_sliceTable = estate->es_sliceTable;
	sp_estate->currentSliceIdInPlan = estate->currentSliceIdInPlan;
	sp_estate->currentExecutingSliceId = estate->currentExecutingSliceId;
	sp_estate->rootSliceId = estate->currentExecutingSliceId;
	sp_estate->motionlayer_context = estate->motionlayer_context;
	sp_estate->es_sharenode = estate->es_sharenode;

	/*
	 * Start up the subplan (this is a very cut-down form of InitPlan())
	 *
	 * The subplan will never need to do BACKWARD scan or MARK/RESTORE.
	 *
	 * We set the REWIND flag to notify the subplan that it is likely to be
	 * rescanned, and it must delay eagerfree.
	 */
	eflags &= EXEC_FLAG_EXPLAIN_ONLY;
	eflags |= EXEC_FLAG_REWIND;

	Plan *subplanplan = exec_subplan_get_plan(estate->es_plannedstmt, subplan);
	Assert(subplanplan);

	Assert(node->planstate == NULL);

	node->planstate = ExecInitNode(subplanplan, sp_estate, eflags);

	node->needShutdown = true;	/* now we need to shutdown the subplan */

	/*
	 * If this plan is un-correlated or undirect correlated one and want to
	 * set params for parent plan then mark parameters as needing evaluation.
	 *
	 * Note that in the case of un-correlated subqueries we don't care about
	 * setting parent->chgParam here: indices take care about it, for others -
	 * it doesn't matter...
	 */
	if (subplan->setParam != NIL)
	{
		ListCell   *lst;

		foreach(lst, subplan->setParam)
		{
			int			paramid = lfirst_int(lst);
			ParamExecData *prmExec = &(estate->es_param_exec_vals[paramid]);
			
			
			/**
			 * Has this parameter been already 
			 * evaluated as part of preprocess_initplan()? If so,
			 * we shouldn't re-evaluate it. If it has been evaluated,
			 * we will simply substitute the actual value from
			 * the external parameters.
			 */
			if (Gp_role == GP_ROLE_EXECUTE
					&& subplan->is_initplan)
			{	
				ParamListInfo paramInfo = estate->es_param_list_info;
				ParamExternData *prmExt = NULL;
				int extParamIndex = -1;
				
				Assert(paramInfo);
				Assert(paramInfo->numParams > 0);
				
				/* 
				 * To locate the value of this pre-evaluated parameter, we need to find
				 * its location in the external parameter list.  
				 */
				extParamIndex = paramInfo->numParams - estate->es_plannedstmt->nCrossLevelParams + paramid;
				
				/* Ensure that the plan is actually an initplan */
				Assert(subplan->is_initplan && "Subplan is not an initplan. Parameter has not been evaluated in preprocess_initplan.");
				
				prmExt = &paramInfo->params[extParamIndex];
								
				/* Make sure the types are valid */
				Assert(OidIsValid(prmExt->ptype) && "Invalid Oid for pre-evaluated parameter.");				
				
				/** Hurray! Copy value from external parameter and don't bother setting up execPlan. */
				prmExec->execPlan = NULL;
				prmExec->isnull = prmExt->isnull;
				prmExec->value = prmExt->value;
			}
			else
			{
				prmExec->execPlan = node;
			}
		}
	}

	/*
	 * If we are going to hash the subquery output, initialize relevant stuff.
	 * (We don't create the hashtable until needed, though.)
	 */
	if (subplan->useHashTable)
	{
		int			ncols,
					i;
		TupleDesc	tupDesc;
		TupleTable	tupTable;
		TupleTableSlot *slot;
		List	   *oplist,
				   *lefttlist,
				   *righttlist,
				   *leftptlist,
				   *rightptlist;
		ListCell   *l;

		/* We need a memory context to hold the hash table(s) */
		node->hashtablecxt =
			AllocSetContextCreate(CurrentMemoryContext,
								  "Subplan HashTable Context",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);
		/* and a small one for the hash tables to use as temp storage */
 		node->hashtempcxt =
 			AllocSetContextCreate(CurrentMemoryContext,
 								  "Subplan HashTable Temp Context",
 								  ALLOCSET_SMALL_MINSIZE,
 								  ALLOCSET_SMALL_INITSIZE,
 								  ALLOCSET_SMALL_MAXSIZE);

		/* and a short-lived exprcontext for function evaluation */
		node->innerecontext = CreateExprContext(estate);
		/* Silly little array of column numbers 1..n */
		ncols = list_length(subplan->paramIds);
		node->keyColIdx = (AttrNumber *) palloc(ncols * sizeof(AttrNumber));
		for (i = 0; i < ncols; i++)
			node->keyColIdx[i] = i + 1;

		/*
		 * We use ExecProject to evaluate the lefthand and righthand
		 * expression lists and form tuples.  (You might think that we could
		 * use the sub-select's output tuples directly, but that is not the
		 * case if we had to insert any run-time coercions of the sub-select's
		 * output datatypes; anyway this avoids storing any resjunk columns
		 * that might be in the sub-select's output.) Run through the
		 * combining expressions to build tlists for the lefthand and
		 * righthand sides.  We need both the ExprState list (for ExecProject)
		 * and the underlying parse Exprs (for ExecTypeFromTL).
		 *
		 * We also extract the combining operators themselves to initialize
		 * the equality and hashing functions for the hash tables.
		 */
		if (IsA(node->testexpr->expr, OpExpr))
		{
			/* single combining operator */
			oplist = list_make1(node->testexpr);
		}
		else if (and_clause((Node *) node->testexpr->expr))
		{
			/* multiple combining operators */
			Assert(IsA(node->testexpr, BoolExprState));
			oplist = ((BoolExprState *) node->testexpr)->args;
		}
		else
		{
			/* shouldn't see anything else in a hashable subplan */
			insist_log(false, "unrecognized testexpr type: %d",
				 (int) nodeTag(node->testexpr->expr));
			oplist = NIL;		/* keep compiler quiet */
		}
		Assert(list_length(oplist) == ncols);

		lefttlist = righttlist = NIL;
		leftptlist = rightptlist = NIL;
		node->eqfunctions = (FmgrInfo *) palloc(ncols * sizeof(FmgrInfo));
		node->hashfunctions = (FmgrInfo *) palloc(ncols * sizeof(FmgrInfo));
		i = 1;
		foreach(l, oplist)
		{
			FuncExprState *fstate = (FuncExprState *) lfirst(l);
			OpExpr	   *opexpr = (OpExpr *) fstate->xprstate.expr;
			ExprState  *exstate;
			Expr	   *expr;
			TargetEntry *tle;
			GenericExprState *tlestate;
			Oid			hashfn;

			Assert(IsA(fstate, FuncExprState));
			Assert(IsA(opexpr, OpExpr));
			Assert(list_length(fstate->args) == 2);

			/* Process lefthand argument */
			exstate = (ExprState *) linitial(fstate->args);
			expr = exstate->expr;
			tle = makeTargetEntry(expr,
								  i,
								  NULL,
								  false);
			tlestate = makeNode(GenericExprState);
			tlestate->xprstate.expr = (Expr *) tle;
			tlestate->xprstate.evalfunc = NULL;
			tlestate->arg = exstate;
			lefttlist = lappend(lefttlist, tlestate);
			leftptlist = lappend(leftptlist, tle);

			/* Process righthand argument */
			exstate = (ExprState *) lsecond(fstate->args);
			expr = exstate->expr;
			tle = makeTargetEntry(expr,
								  i,
								  NULL,
								  false);
			tlestate = makeNode(GenericExprState);
			tlestate->xprstate.expr = (Expr *) tle;
			tlestate->xprstate.evalfunc = NULL;
			tlestate->arg = exstate;
			righttlist = lappend(righttlist, tlestate);
			rightptlist = lappend(rightptlist, tle);

			/* Lookup the combining function */
			fmgr_info(opexpr->opfuncid, &node->eqfunctions[i - 1]);
			node->eqfunctions[i - 1].fn_expr = (Node *) opexpr;

			/* Lookup the associated hash function */
			hashfn = get_op_hash_function(opexpr->opno);
			if (!OidIsValid(hashfn))
				elog(ERROR, "could not find hash function for hash operator %u",
					 opexpr->opno);
			fmgr_info(hashfn, &node->hashfunctions[i - 1]);

			i++;
		}

		/*
		 * Create a tupletable to hold these tuples.  (Note: we never bother
		 * to free the tupletable explicitly; that's okay because it will
		 * never store raw disk tuples that might have associated buffer pins.
		 * The only resource involved is memory, which will be cleaned up by
		 * freeing the query context.)
		 */
		tupTable = ExecCreateTupleTable(2);

		/*
		 * Construct tupdescs, slots and projection nodes for left and right
		 * sides.  The lefthand expressions will be evaluated in the parent
		 * plan node's exprcontext, which we don't have access to here.
		 * Fortunately we can just pass NULL for now and fill it in later
		 * (hack alert!).  The righthand expressions will be evaluated in our
		 * own innerecontext.
		 */
		tupDesc = ExecTypeFromTL(leftptlist, false);
		slot = ExecAllocTableSlot(tupTable);
		ExecSetSlotDescriptor(slot, tupDesc);
		node->projLeft = ExecBuildProjectionInfo(lefttlist,
												 NULL,
												 slot,
												 NULL);

		tupDesc = ExecTypeFromTL(rightptlist, false);
		slot = ExecAllocTableSlot(tupTable);
		ExecSetSlotDescriptor(slot, tupDesc);
		node->projRight = ExecBuildProjectionInfo(righttlist,
												  node->innerecontext,
												  slot,
												  NULL);
	}
}

/* ----------------------------------------------------------------
 *		ExecSetParamPlan
 *
 *		Executes an InitPlan subplan and sets its output parameters.
 *
 * This is called from ExecEvalParam() when the value of a PARAM_EXEC
 * parameter is requested and the param's execPlan field is set (indicating
 * that the param has not yet been evaluated).	This allows lazy evaluation
 * of initplans: we don't run the subplan until/unless we need its output.
 * Note that this routine MUST clear the execPlan fields of the plan's
 * output parameters after evaluating them!
 * ----------------------------------------------------------------
 */

/*
 * Greenplum Database Changes:
 * In the case where this is running on the dispatcher, and it's a parallel dispatch
 * subplan, we need to dispatch the query to the qExecs as well, like in ExecutorRun.
 * except in this case we don't have to worry about insert statements.
 * In order to serialize the parameters (including PARAM_EXEC parameters that
 * are converted into PARAM_EXEC_REMOTE parameters, I had to add a parameter to this
 * function: ParamListInfo p.  This may be NULL in the non-dispatch case.
 */

/* Helper: SubplanQueryDesc derives a QueryDesc for use by the subplan. */
static QueryDesc *SubplanQueryDesc(QueryDesc * qd)
{
	QueryDesc *subqd = NULL;
	PlannedStmt *substmt = NULL;
	PlannedStmt *stmt = qd->plannedstmt;
	
	Assert(stmt != NULL);
	
	/*
	 * MPP-2869 and MPP-2859, single-row parameter-subquery inside
	 * CTAS: we don't want to create the the table during the
	 * initPlan execution. */
	
	/* build the PlannedStmt substmt */
	substmt = makeNode(PlannedStmt);
	
	substmt->commandType = stmt->commandType;
	substmt->canSetTag = stmt->canSetTag;
	substmt->transientPlan = stmt->transientPlan;
	substmt->planTree = stmt->planTree;
	substmt->rtable = stmt->rtable;
	substmt->resultRelations = stmt->resultRelations;
	substmt->utilityStmt = stmt->utilityStmt;
	substmt->intoClause = NULL;
	substmt->subplans = stmt->subplans;
	substmt->rewindPlanIDs = stmt->rewindPlanIDs;
	substmt->returningLists = stmt->returningLists;
	substmt->rowMarks = stmt->rowMarks;
	substmt->relationOids = stmt->relationOids;
	substmt->invalItems = stmt->invalItems;
	substmt->nCrossLevelParams = stmt->nCrossLevelParams;
	substmt->nMotionNodes = stmt->nMotionNodes;
	substmt->nInitPlans = stmt->nInitPlans;
	substmt->contextdisp = stmt->contextdisp;
	substmt->scantable_splits = stmt->scantable_splits;
	substmt->resource = stmt->resource;
	
	/*
	 * Fake a QueryDesc stucture for CdbDispatchPlan call. It should
	 * look like the one passed in as the argument which carries the
	 * global query, plan, parameters, and slice table, and specifies
	 * the initplan root of interest.
	 */
	subqd = CreateQueryDesc(substmt,
							pstrdup("(internal SELECT query for initplan)"),
							qd->snapshot,
							qd->crosscheck_snapshot,
							NULL,		/* Null destination for the QE */
							qd->params,
							qd->doInstrument);
	
	return subqd;
}

void
ExecSetParamPlan(SubPlanState *node, ExprContext *econtext, QueryDesc *gbl_queryDesc)
{
	SubPlan    *subplan = (SubPlan *) node->xprstate.expr;
	PlanState  *planstate = node->planstate;
	SubLinkType subLinkType = subplan->subLinkType;
	MemoryContext oldcontext = CurrentMemoryContext;
	TupleTableSlot *slot;
	ListCell   *l;
	bool		found = false;
	ArrayBuildState *astate = NULL;
    Size        savepeakspace = MemoryContextGetPeakSpace(planstate->state->es_query_cxt);

	QueryDesc  *queryDesc = NULL;

	bool		shouldDispatch = false;
	volatile bool   shouldTeardownInterconnect = false;
    volatile bool   explainRecvStats = false;

	if (Gp_role == GP_ROLE_DISPATCH &&
		planstate != NULL &&
		planstate->plan != NULL &&
		planstate->plan->dispatch == DISPATCH_PARALLEL)
		shouldDispatch = true;

    node->cdbextratextbuf = NULL;

    /* 
     * Reset memory high-water mark so EXPLAIN ANALYZE can report each
     * root slice's usage separately.
     */
    MemoryContextSetPeakSpace(planstate->state->es_query_cxt, 0);

	/*
	 * Let's initialize 'queryDesc' before our PG_TRY, to ensure the correct
	 * value will be seen inside the PG_CATCH block without having to declare
	 * it 'volatile'.  (setjmp/longjmp foolishness)
	 */
	if (shouldDispatch)
	{
		/*
		 * Fake a QueryDesc stucture for CdbDispatchPlan call. It should
		 * look like the one passed in as the argument which carries the
		 * global query, plan, parameters, and slice table, and specifies
		 * the initplan root of interest.
		 */
		queryDesc = SubplanQueryDesc(gbl_queryDesc);

        /* 
         * CDB TODO: Should this use CreateSubExecutorState()?  
         * Should FreeExecutorState() eventually be called?
         * Why do we need this at all?    ... kh 4/2007
         */
		queryDesc->estate = CreateExecutorState();

        queryDesc->showstatctx = gbl_queryDesc->showstatctx;
        queryDesc->estate->showstatctx = gbl_queryDesc->showstatctx;
		queryDesc->estate->es_sliceTable = gbl_queryDesc->estate->es_sliceTable;
		queryDesc->estate->es_param_exec_vals = gbl_queryDesc->estate->es_param_exec_vals;
		queryDesc->estate->motionlayer_context = gbl_queryDesc->estate->motionlayer_context;
		queryDesc->extended_query = gbl_queryDesc->extended_query;
		queryDesc->resource = gbl_queryDesc->resource;
		queryDesc->planner_segments = gbl_queryDesc->planner_segments;
	}

	/*
	 * Need a try/catch block here so that if an ereport is called from
	 * within ExecutePlan, we can clean up by calling CdbCheckDispatchResult.
	 * This cleans up the asynchronous commands running through the threads launched from
	 * CdbDispatchCommand.
	 */
	PG_TRY();
	{
		if (shouldDispatch)
		{			

			/*
			 * This call returns after launching the threads that send the
             * command to the appropriate segdbs.  It does not wait for them
             * to finish unless an error is detected before all are dispatched.
			 */
			queryDesc->estate->dispatch_data = initialize_dispatch_data(queryDesc->resource, false);
			prepare_dispatch_query_desc(queryDesc->estate->dispatch_data, queryDesc);
			dispatch_run(queryDesc->estate->dispatch_data);
			cleanup_dispatch_data(queryDesc->estate->dispatch_data);

			/*
			 * Set up the interconnect for execution of the initplan root slice.
			 */
			shouldTeardownInterconnect = true;
			Assert(!(queryDesc->estate->interconnect_context));
			SetupInterconnect(queryDesc->estate);
			Assert((queryDesc->estate->interconnect_context));

			ExecUpdateTransportState(planstate, queryDesc->estate->interconnect_context);
		}

		/*
		 * Must switch to child query's per-query memory context.
		 */
		oldcontext = MemoryContextSwitchTo(node->sub_estate->es_query_cxt);

		if (subLinkType == ANY_SUBLINK ||
			subLinkType == ALL_SUBLINK)
			elog(ERROR, "ANY/ALL subselect unsupported as initplan");

	    /*
	     * By definition, an initplan has no parameters from our query level, but
	     * it could have some from an outer level.	Rescan it if needed.
	     */
		if (planstate->chgParam != NULL)
			ExecReScan(planstate, NULL);

		for (slot = ExecProcNode(planstate);
			 !TupIsNull(slot);
			 slot = ExecProcNode(planstate))
		{
			int			i = 1;

			if (subLinkType == EXISTS_SUBLINK || subLinkType == NOT_EXISTS_SUBLINK)
			{
				/* There can be only one param... */
				int			paramid = linitial_int(subplan->setParam);
				ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

				prm->execPlan = NULL;
				bool val = true;
				if (subLinkType == NOT_EXISTS_SUBLINK)
				{
					val = false;
				}
				prm->value = BoolGetDatum(val);
				prm->isnull = false;
				found = true;

				if (shouldDispatch)
				{
					/* Tell MPP we're done with this plan. */
					ExecSquelchNode(planstate);
				}

				break;
			}

			if (subLinkType == ARRAY_SUBLINK)
			{
				Datum		dvalue;
				bool		disnull;

				found = true;
				/* stash away current value */
			    Assert(subplan->firstColType == slot->tts_tupleDescriptor->attrs[0]->atttypid);
				dvalue = slot_getattr(slot, 1, &disnull);
				astate = accumArrayResult(astate, dvalue, disnull,
									  subplan->firstColType, oldcontext);
				/* keep scanning subplan to collect all values */
				continue;
			}

			if (found &&
				(subLinkType == EXPR_SUBLINK ||
				 subLinkType == ROWCOMPARE_SUBLINK))
				ereport(ERROR,
						(errcode(ERRCODE_CARDINALITY_VIOLATION),
						 errmsg("more than one row returned by a subquery used as an expression")));

			found = true;

			/*
		 	 * We need to copy the subplan's tuple into our own context, in case
		 	 * any of the params are pass-by-ref type --- the pointers stored in
		 	 * the param structs will point at this copied tuple! node->curTuple
		 	 * keeps track of the copied tuple for eventual freeing.
			 */
			MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
			if (node->curTuple)
				pfree(node->curTuple);

			node->curTuple = ExecCopySlotMemTuple(slot);
			MemoryContextSwitchTo(node->sub_estate->es_query_cxt);

			/*
			 * Now set all the setParam params from the columns of the tuple
			 */
			foreach(l, subplan->setParam)
			{
				int			paramid = lfirst_int(l);
				ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

				prm->execPlan = NULL;
				prm->value = memtuple_getattr(node->curTuple, slot->tts_mt_bind, i, &(prm->isnull));
				i++;
			}
		}

		if (!found)
		{
			if (subLinkType == EXISTS_SUBLINK || subLinkType == NOT_EXISTS_SUBLINK)
			{
				/* There can be only one param... */
				int			paramid = linitial_int(subplan->setParam);
				ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

				prm->execPlan = NULL;
				bool val = false;
                                if (subLinkType == NOT_EXISTS_SUBLINK)
                                {
                                        val = true;
                                }
				prm->value = BoolGetDatum(val);
				prm->isnull = false;
			}
			else
			{
				foreach(l, subplan->setParam)
				{
					int			paramid = lfirst_int(l);
					ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

					prm->execPlan = NULL;
					prm->value = (Datum) 0;
					prm->isnull = true;
				}
			}
		}
		else if (subLinkType == ARRAY_SUBLINK)
		{
			/* There can be only one param... */
			int			paramid = linitial_int(subplan->setParam);
			ParamExecData *prm = &(econtext->ecxt_param_exec_vals[paramid]);

			Assert(astate != NULL);
			prm->execPlan = NULL;
			/* We build the result in query context so it won't disappear */
			prm->value = makeArrayResult(astate, econtext->ecxt_per_query_memory);
			prm->isnull = false;
		}

        /*
         * If we dispatched to QEs, wait for completion and check for errors.
         */
        if (shouldDispatch && 
			queryDesc && queryDesc->estate &&
			queryDesc->estate->dispatch_data)
        {
            /* If EXPLAIN ANALYZE, collect execution stats from qExecs. */
            if (planstate->instrument)
            {
                MemoryContext   savecxt;

                /* Wait for all gangs to finish. */
                dispatch_wait(queryDesc->estate->dispatch_data);	

                /* Allocate buffer to pass extra message text to cdbexplain. */
                savecxt = MemoryContextSwitchTo(gbl_queryDesc->estate->es_query_cxt);
                node->cdbextratextbuf = makeStringInfo();
                MemoryContextSwitchTo(savecxt);

                /* Jam stats into subplan's Instrumentation nodes. */
                explainRecvStats = true;
                cdbexplain_recvExecStats(planstate,
                                         dispatch_get_results(queryDesc->estate->dispatch_data),
                                         LocallyExecutingSliceIndex(queryDesc->estate),
                                         econtext->ecxt_estate->showstatctx,
                                         dispatch_get_segment_num(queryDesc->estate->dispatch_data));
            }

            /*
             * Wait for all gangs to finish.  Check and free the results.
             * If the dispatcher or any QE had an error, report it and
             * exit to our error handler (below) via PG_THROW.
             */
            dispatch_wait(queryDesc->estate->dispatch_data);
            dispatch_cleanup(queryDesc->estate->dispatch_data);
			queryDesc->estate->dispatch_data = NULL;
        }

		/* teardown the sequence server */
		TeardownSequenceServer();
		
        /* Clean up the interconnect. */
        if (shouldTeardownInterconnect)
        {
        	shouldTeardownInterconnect = false;

	        TeardownInterconnect(queryDesc->estate->interconnect_context, 
								 queryDesc->estate->motionlayer_context,
								 false); /* following success on QD */	
		}

    }
	PG_CATCH();
	{

        /* If EXPLAIN ANALYZE, collect local and distributed execution stats. */
        if (planstate->instrument)
        {
            cdbexplain_localExecStats(planstate, econtext->ecxt_estate->showstatctx);
            if (!explainRecvStats &&
				shouldDispatch)
            {
				Assert(queryDesc != NULL &&
					   queryDesc->estate != NULL);
                /* Wait for all gangs to finish.  Cancel slowpokes. */
                dispatch_wait(queryDesc->estate->dispatch_data);

                cdbexplain_recvExecStats(planstate,
                                         dispatch_get_results(queryDesc->estate->dispatch_data),
                                         LocallyExecutingSliceIndex(queryDesc->estate),
                                         econtext->ecxt_estate->showstatctx,
                                         dispatch_get_segment_num(queryDesc->estate->dispatch_data));
            }
        }

        /* Restore memory high-water mark for root slice of main query. */
        MemoryContextSetPeakSpace(planstate->state->es_query_cxt, savepeakspace);

        /*
		 * Request any commands still executing on qExecs to stop.
		 * Wait for them to finish and clean up the dispatching structures.
         * Replace current error info with QE error info if more interesting.
		 */
        if (shouldDispatch && queryDesc && queryDesc->estate && queryDesc->estate->dispatch_data)
			dispatch_catch_error(queryDesc->estate->dispatch_data);
		
		/* teardown the sequence server */
		TeardownSequenceServer();
		
        /*
         * Clean up the interconnect.
         * CDB TODO: Is this needed following failure on QD?
         */
        if (shouldTeardownInterconnect)
			TeardownInterconnect(queryDesc->estate->interconnect_context,
								 queryDesc->estate->motionlayer_context,
								 true);
		PG_RE_THROW();
	}
	PG_END_TRY();

    /* If EXPLAIN ANALYZE, collect local execution stats. */
    if (planstate->instrument)
        cdbexplain_localExecStats(planstate, econtext->ecxt_estate->showstatctx);

    /* Restore memory high-water mark for root slice of main query. */
    MemoryContextSetPeakSpace(planstate->state->es_query_cxt, savepeakspace);

    MemoryContextSwitchTo(oldcontext);
}

/* ----------------------------------------------------------------
 *		ExecEndSubPlan
 * ----------------------------------------------------------------
 */
void
ExecEndSubPlan(SubPlanState *node)
{
	if (node->needShutdown)
	{
		ExecEndPlan(node->planstate, node->sub_estate);
		FreeExecutorState(node->sub_estate);
		node->sub_estate = NULL;
		node->planstate = NULL;
		node->needShutdown = false;
	}
}

/*
 * Mark an initplan as needing recalculation
 */
void
ExecReScanSetParamPlan(SubPlanState *node, PlanState *parent)
{
	PlanState  *planstate = node->planstate;
	SubPlan    *subplan = (SubPlan *) node->xprstate.expr;
	EState	   *estate = parent->state;
	ListCell   *l;

	/* sanity checks */
	insist_log(subplan->parParam == NIL,
			"direct correlated subquery unsupported as initplan");
	insist_log(subplan->setParam != NIL,
			"setParam list of initplan is empty");
	insist_log(!bms_is_empty(planstate->plan->extParam),
		"extParam set of initplan is empty");

	/*
	 * Don't actually re-scan: ExecSetParamPlan does it if needed.
	 */

	/*
	 * Mark this subplan's output parameters as needing recalculation
	 */
	foreach(l, subplan->setParam)
	{
		int			paramid = lfirst_int(l);
		ParamExecData *prm = &(estate->es_param_exec_vals[paramid]);

		prm->execPlan = node;
		parent->chgParam = bms_add_member(parent->chgParam, paramid);
	}
}

void
ExecEagerFreeSubPlan(SubPlanState *node)
{
	Assert(node->hashtablecxt != NULL);
	MemoryContextReset(node->hashtablecxt);
	node->hashtable = NULL;
	node->hashnulls = NULL;
	node->havehashrows = false;
	node->havenullrows = false;
}
