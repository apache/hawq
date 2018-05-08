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
 * nodeAgg.c
 *	  Routines to handle aggregate nodes.
 *
 *	  ExecAgg evaluates each aggregate in the following steps:
 *
 *		 transvalue = initcond
 *		 foreach input_tuple do
 *			transvalue = transfunc(transvalue, input_value(s))
 *		 result = finalfunc(transvalue)
 *
 *	  If a finalfunc is not supplied then the result is just the ending
 *	  value of transvalue.
 *
 *	  If transfunc is marked "strict" in pg_proc and initcond is NULL,
 *	  then the first non-NULL input_value is assigned directly to transvalue,
 *	  and transfunc isn't applied until the second non-NULL input_value.
 *	  The agg's first input type and transtype must be the same in this case!
 *
 *	  If transfunc is marked "strict" then NULL input_values are skipped,
 *	  keeping the previous transvalue.	If transfunc is not strict then it
 *	  is called for every input tuple and must deal with NULL initcond
 *	  or NULL input_values for itself.
 *
 *	  If finalfunc is marked "strict" then it is not called when the
 *	  ending transvalue is NULL, instead a NULL result is created
 *	  automatically (this is just the usual handling of strict functions,
 *	  of course).  A non-strict finalfunc can make its own choice of
 *	  what to return for a NULL ending transvalue.
 *
 *	  We compute aggregate input expressions and run the transition functions
 *	  in a temporary econtext (aggstate->tmpcontext).  This is reset at
 *	  least once per input tuple, so when the transvalue datatype is
 *	  pass-by-reference, we have to be careful to copy it into a longer-lived
 *	  memory context, and free the prior value to avoid memory leakage.
 *
 *    Postgres stores transvalues in the memory context aggstate->aggcontext,
 *	  which is also used for the hashtable structures in AGG_HASHED mode.
 *    MPP (in order to support hybrid hash aggregation) stores hash table
 *    entries and associated transition values in aggstate->aggcontext.
 *
 *	  The node's regular econtext (aggstate->ss.ps.ps_ExprContext)
 *	  is used to run finalize functions and compute the output tuple;
 *	  this context can be reset once per output tuple.
 *
 *	  Beginning in PostgreSQL 8.1, the executor's AggState node is passed as
 *	  the fmgr "context" value in all transfunc and finalfunc calls.  It is
 *	  not really intended that the transition functions will look into the
 *	  AggState node, but they can use code like
 *			if (fcinfo->context && IsA(fcinfo->context, AggState))
 *	  to verify that they are being called by nodeAgg.c and not as ordinary
 *	  SQL functions.  The main reason a transition function might want to know
 *	  that is that it can avoid palloc'ing a fixed-size pass-by-ref transition
 *	  value on every call: it can instead just scribble on and return its left
 *	  input.  Ordinarily it is completely forbidden for functions to modify
 *	  pass-by-ref inputs, but in the aggregate case we know the left input is
 *	  either the initial transition value or a previous function result, and
 *	  in either case its value need not be preserved.  See int8inc() for an
 *	  example.	Notice that advance_transition_function() is coded to avoid a
 *	  data copy step when the previous transition value pointer is returned.
 *
 *	  In Greenplum 4.2.2, we add PercentileExpr support along with Aggref.
 *	  It is used to implement inverse distribution function support, namely
 *	  percentile_cont, percentile_disc and median.  The semantics for them
 *	  is almost same as Aggref, where the aggregate process is handled by
 *	  an individual function and the expression node only returns a pre-computed
 *	  result.  PercentileExpr is used in Agg node because we cannot change
 *	  the catalog in this release, and it may be removed and integrated to
 *	  standard Aggref itself.
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/nodeAgg.c,v 1.146.2.2 2007/08/08 18:07:03 neilc Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "catalog/catquery.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/execHHashagg.h"
#include "executor/nodeAgg.h"
#include "lib/stringinfo.h"             /* StringInfo */
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"

#include "cdb/cdbexplain.h"
#include "cdb/cdbvars.h" /* mpp_hybrid_hash_agg */

#define IS_HASHAGG(aggstate) (((Agg *) (aggstate)->ss.ps.plan)->aggstrategy == AGG_HASHED)

/*
 * AggStatePerAggData -- per-aggregate working state
 * AggStatePerGroupData - per-aggregate-per-group working state
 *
 * Definition moved to nodeAgg.c to provide visibility to execHHashagg.c
 */


/*
 * To implement hashed aggregation, we need a hashtable that stores a
 * representative tuple and an array of AggStatePerGroup structs for each
 * distinct set of GROUP BY column values.	We compute the hash key from
 * the GROUP BY columns.
 */
typedef struct AggHashEntryData *AggHashEntry;

typedef struct AggHashEntryData
{
	TupleHashEntryData shared;	/* common header for hash table entries */
	/* per-aggregate transition status array - must be last! */
	AggStatePerGroupData pergroup[1];	/* VARIABLE LENGTH ARRAY */
} AggHashEntryData;				/* VARIABLE LENGTH STRUCT */


static void process_ordered_aggregate_single(AggState *aggstate,
											 AggStatePerAgg peraggstate,
											 AggStatePerGroup pergroupstate);
static void process_ordered_aggregate_multi(AggState *aggstate,
											 AggStatePerAgg peraggstate,
											 AggStatePerGroup pergroupstate);
static void finalize_aggregate(AggState *aggstate,
				   AggStatePerAgg peraggstate,
				   AggStatePerGroup pergroupstate,
				   Datum *resultVal, bool *resultIsNull);

static Bitmapset *find_unaggregated_cols(AggState *aggstate);
static bool find_unaggregated_cols_walker(Node *node, Bitmapset **colnos);
static TupleTableSlot *agg_retrieve_direct(AggState *aggstate);
static void ExecAggExplainEnd(PlanState *planstate, struct StringInfoData *buf);
static int count_extra_agg_slots(Node *node);
static bool count_extra_agg_slots_walker(Node *node, int *count);

static TupleTableSlot *computeTupleWithFinalAggregate(AggState *aggstate,
                                                  TupleTableSlot *firstSlot);


Datum
datumCopyWithMemManager(Datum oldvalue, Datum value, bool typByVal, int typLen,
						MemoryManagerContainer *mem_manager)
{
	Datum		res;

	if (typByVal)
		res = value;
	else
	{
		Size realSize;
		Size old_realSize = 0;
		char *s;

		if (DatumGetPointer(value) == NULL)
			return PointerGetDatum(NULL);

		if (DatumGetPointer(oldvalue) != NULL)
			old_realSize = MAXALIGN(datumGetSize(oldvalue, typByVal, typLen));
		
		realSize = datumGetSize(value, typByVal, typLen);

		if (old_realSize == 0 || old_realSize < realSize)
		{
			int alloc_size = MAXALIGN(mem_manager->realloc_ratio * old_realSize);
			if (alloc_size < realSize)
				alloc_size = MAXALIGN(realSize);
			
			if (mem_manager->free)
			{
				(*mem_manager->free)(mem_manager->manager, DatumGetPointer(oldvalue));
			}
			
			s = (char *) (*mem_manager->alloc)(mem_manager->manager, alloc_size);
		}
			   
		else
			s = (char *) DatumGetPointer(oldvalue);
		
		memcpy(s, DatumGetPointer(value), realSize);
		res = PointerGetDatum(s);
	}
	return res;
}

/*
 * Initialize all aggregates for a new group of input values.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 *
 * Note that the memory allocation is done through provided memory manager.
 */
void
initialize_aggregates(AggState *aggstate,
					  AggStatePerAgg peragg,
					  AggStatePerGroup pergroup,
					  MemoryManagerContainer *mem_manager)
{
	int			aggno;

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &peragg[aggno];
		AggStatePerGroup pergroupstate = &pergroup[aggno];

		/*
		 * Start a fresh sort operation for each DISTINCT/ORDER BY aggregate.
		 */
		if (peraggstate->numSortCols > 0)
		{
			/*
			 * In case of rescan, maybe there could be an uncompleted sort
			 * operation?  Clean it up if so.
			 */
			if(gp_enable_mk_sort)
			{
				if (peraggstate->sortstate)
					tuplesort_end_mk((Tuplesortstate_mk *) peraggstate->sortstate);

				/*
				 * We use a plain Datum sorter when there's a single input column;
				 * otherwise sort the full tuple.  (See comments for
				 * process_ordered_aggregate_single.)
				 */
				peraggstate->sortstate =
					(peraggstate->numInputs == 1) ?
					tuplesort_begin_datum_mk(& aggstate->ss,
											 peraggstate->evaldesc->attrs[0]->atttypid,
											 peraggstate->sortOperators[0],
											 PlanStateOperatorMemKB((PlanState *) aggstate), false) :
					tuplesort_begin_heap_mk(& aggstate->ss,
											peraggstate->evaldesc,
											peraggstate->numSortCols,
											peraggstate->sortOperators,
											peraggstate->sortColIdx,
											PlanStateOperatorMemKB((PlanState *) aggstate), false);
				
				/* 
				 * CDB: If EXPLAIN ANALYZE, let all of our tuplesort operations
				 * share our Instrumentation object and message buffer.
				 */
				if (aggstate->ss.ps.instrument)
					tuplesort_set_instrument_mk((Tuplesortstate_mk *) peraggstate->sortstate,
							aggstate->ss.ps.instrument,
							aggstate->ss.ps.cdbexplainbuf);
			}
			else /* gp_enable_mk_sort is off */
			{
				if (peraggstate->sortstate)
					tuplesort_end((Tuplesortstate *) peraggstate->sortstate);

				/*
				 * We use a plain Datum sorter when there's a single input column;
				 * otherwise sort the full tuple.  (See comments for
				 * process_ordered_aggregate_single.)
				 */
				peraggstate->sortstate =
					(peraggstate->numInputs == 1) ?
					tuplesort_begin_datum(peraggstate->evaldesc->attrs[0]->atttypid,
										  peraggstate->sortOperators[0],
										  PlanStateOperatorMemKB((PlanState *) aggstate), false) :
					tuplesort_begin_heap(peraggstate->evaldesc,
										 peraggstate->numSortCols,
										 peraggstate->sortOperators,
										 peraggstate->sortColIdx,
										 PlanStateOperatorMemKB((PlanState *) aggstate), false);
				
				/* 
				 * CDB: If EXPLAIN ANALYZE, let all of our tuplesort operations
				 * share our Instrumentation object and message buffer.
				 */
				if (aggstate->ss.ps.instrument)
					tuplesort_set_instrument((Tuplesortstate *) peraggstate->sortstate,
							aggstate->ss.ps.instrument,
							aggstate->ss.ps.cdbexplainbuf);
			}

			/* CDB: Set enhanced sort options. */
			{
				int64 		limit = 0;
				int64 		offset = 0;
				int 		unique = peragg->aggref->aggdistinct &&
									 ( gp_enable_sort_distinct ? 1 : 0) ;
				int 		sort_flags = gp_sort_flags; /* get the guc */
				int         maxdistinct = gp_sort_max_distinct; /* get guc */

				if(gp_enable_mk_sort)
					cdb_tuplesort_init_mk((Tuplesortstate_mk *) peraggstate->sortstate, 
							offset, limit, unique, 
							sort_flags, maxdistinct);
				else
					cdb_tuplesort_init((Tuplesortstate *) peraggstate->sortstate, 
							offset, limit, unique, 
							sort_flags, maxdistinct);
			}
		}

		/*
		 * (Re)set transValue to the initial value.
		 *
		 * Note that when the initial value is pass-by-ref, we must copy it
		 * (into the aggcontext) since we will pfree the transValue later.
		 */
		if (peraggstate->initValueIsNull)
		{
			pergroupstate->transValue = peraggstate->initValue;
		}
		
		else
		{
			pergroupstate->transValue =
                                datumCopyWithMemManager(0,
                                                        peraggstate->initValue,
                                                        peraggstate->transtypeByVal,
                                                        peraggstate->transtypeLen,
                                                        mem_manager);
		}
		pergroupstate->transValueIsNull = peraggstate->initValueIsNull;

		/*
		 * If the initial value for the transition state doesn't exist in the
		 * pg_aggregate table then we will let the first non-NULL value
		 * returned from the outer procNode become the initial value. (This is
		 * useful for aggregates like max() and min().) The noTransValue flag
		 * signals that we still need to do this.
		 */
		pergroupstate->noTransValue = peraggstate->initValueIsNull;
	}
}

/*
 * Given new input value(s), advance the transition function of an aggregate.
 *
 * The new values (and null flags) have been preloaded into argument positions
 * 1 and up in fcinfo, so that we needn't copy them again to pass to the
 * transition function.  No other fields of fcinfo are assumed valid.
 *
 * It doesn't matter which memory context this is called in.
 */
void
advance_transition_function(AggState *aggstate,
							AggStatePerAgg peraggstate,
							AggStatePerGroup pergroupstate,
							FunctionCallInfoData *fcinfo,
							MemoryManagerContainer *mem_manager)
{
	pergroupstate->transValue = 
		invoke_agg_trans_func(&(peraggstate->transfn), 
							  peraggstate->numArguments,
							  pergroupstate->transValue,
							  &(pergroupstate->noTransValue),
							  &(pergroupstate->transValueIsNull),
							  peraggstate->transtypeByVal,
							  peraggstate->transtypeLen,
							  fcinfo, (void *)aggstate,
							  aggstate->tmpcontext->ecxt_per_tuple_memory,
							  mem_manager);
}

Datum
invoke_agg_trans_func(FmgrInfo *transfn, int numargs, Datum transValue,
					  bool *noTransvalue, bool *transValueIsNull,
					  bool transtypeByVal, int16 transtypeLen, 
					  FunctionCallInfoData *fcinfo, void *funcctx,
					  MemoryContext tuplecontext,
					  MemoryManagerContainer *mem_manager)
{
	MemoryContext oldContext;
	Datum		newVal;
	int			i;

	if (transfn->fn_strict)
	{
		/*
		 * For a strict transfn, nothing happens when there's a NULL input; we
		 * just keep the prior transValue.
		 */
		for (i = 1; i <= numargs; i++)
		{
			if (fcinfo->argnull[i])
				return transValue;
		}
		if (*noTransvalue)
		{
			/*
			 * transValue has not been initialized. This is the first non-NULL
			 * input value. We use it as the initial value for transValue. (We
			 * already checked that the agg's input type is binary-compatible
			 * with its transtype, so straight copy here is OK.)
			 *
			 * We must copy the datum into aggcontext if it is pass-by-ref.
			 * We do not need to pfree the old transValue, since it's NULL.
			 */
			newVal = datumCopyWithMemManager(transValue, fcinfo->arg[1], transtypeByVal,
											 transtypeLen, mem_manager);
			*transValueIsNull = false;
			*noTransvalue = false;
			fcinfo->isnull = false;

			return newVal;
		}
		if (*transValueIsNull)
		{
			/*
			 * Don't call a strict function with NULL inputs.  Note it is
			 * possible to get here despite the above tests, if the transfn is
			 * strict *and* returned a NULL on a prior cycle. If that happens
			 * we will propagate the NULL all the way to the end.
			 */
			fcinfo->isnull = true;
			return transValue;
		}
	}

	/* We run the transition functions in per-input-tuple memory context */
	oldContext = MemoryContextSwitchTo(tuplecontext);

	/*
	 * OK to call the transition function
	 */
	InitFunctionCallInfoData(*fcinfo, transfn,
							 numargs + 1,
							 (void *) funcctx, NULL);
	fcinfo->arg[0] = transValue;
	fcinfo->argnull[0] = *transValueIsNull;

	newVal = FunctionCallInvoke(fcinfo);

	/*
	 * If pass-by-ref datatype, must copy the new value into aggcontext and
	 * pfree the prior transValue.	But if transfn returned a pointer to its
	 * first input, we don't need to do anything.
	 */
	if (!transtypeByVal && 
		DatumGetPointer(newVal) != DatumGetPointer(transValue))
	{
		if (!fcinfo->isnull)
		{
			newVal = datumCopyWithMemManager(transValue, newVal, transtypeByVal,
											 transtypeLen, mem_manager);
		}
	}

	*transValueIsNull = fcinfo->isnull;
    if (!fcinfo->isnull) 
        *noTransvalue = false;

	MemoryContextSwitchTo(oldContext);
	return newVal;
}

/*
 * Advance all the aggregates for one input tuple.	The input tuple
 * has been stored in tmpcontext->ecxt_scantuple, so that it is accessible
 * to ExecEvalExpr.  pergroup is the array of per-group structs to use
 * (this might be in a hashtable entry).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
void
advance_aggregates(AggState *aggstate, AggStatePerGroup pergroup,
				   MemoryManagerContainer *mem_manager)
{
	int			aggno;

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		Datum value;
		bool isnull;
		AggStatePerAgg peraggstate = &aggstate->peragg[aggno];
		AggStatePerGroup pergroupstate = &pergroup[aggno];
		Aggref	   *aggref = peraggstate->aggref;
		PercentileExpr *perc = peraggstate->perc;
		int			i;
		TupleTableSlot *slot;
		int nargs;

		if (aggref)
			nargs = list_length(aggref->args);
		else
		{
			Assert (perc);
			nargs = list_length(perc->args);
		}

		/* Evaluate the current input expressions for this aggregate */
		slot = ExecProject(peraggstate->evalproj, NULL);
		slot_getallattrs(slot);	
		
		if (peraggstate->numSortCols > 0)
		{
			/* DISTINCT and/or ORDER BY case */
			Assert(slot->PRIVATE_tts_nvalid == peraggstate->numInputs);
			Assert(!perc);

			/*
			 * If the transfn is strict, we want to check for nullity before
			 * storing the row in the sorter, to save space if there are a lot
			 * of nulls.  Note that we must only check numArguments columns,
			 * not numInputs, since nullity in columns used only for sorting
			 * is not relevant here.
			 */
			if (peraggstate->transfn.fn_strict)
			{
				for (i = 0; i < nargs; i++)
				{
					value = slot_getattr(slot, i+1, &isnull);
					
					if (isnull)
						break; /* arg loop */
				}
				if (i < nargs)
					continue; /* aggno loop */
			}
			
			/* OK, put the tuple into the tuplesort object */
			if (peraggstate->numInputs == 1)
			{
				value = slot_getattr(slot, 1, &isnull);
				
				if (gp_enable_mk_sort)
					tuplesort_putdatum_mk((Tuplesortstate_mk*) peraggstate->sortstate,
									   value,
									   isnull);
				else 
					tuplesort_putdatum((Tuplesortstate*) peraggstate->sortstate,
									   value,
									   isnull);
			}
			else
			{
				if (gp_enable_mk_sort)
					tuplesort_puttupleslot_mk((Tuplesortstate_mk*) peraggstate->sortstate, 
											  slot);
				else 
					tuplesort_puttupleslot((Tuplesortstate*) peraggstate->sortstate, 
										   slot);
			}
		}
		else
		{
			/* We can apply the transition function immediately */
			FunctionCallInfoData fcinfo;
			
			/* Load values into fcinfo */
			/* Start from 1, since the 0th arg will be the transition value */
			Assert(slot->PRIVATE_tts_nvalid >= nargs);
			if (aggref)
			{
				for (i = 0; i < nargs; i++)
				{
					fcinfo.arg[i + 1] = slot_getattr(slot, i+1, &isnull);
					fcinfo.argnull[i + 1] = isnull;
				}

			}
			else
			{
				/*
				 * In case of percentile functions, put everything into
				 * fcinfo's argument since there should be the required
				 * attributes as arguments in the tuple.
				 */
				int		natts;

				Assert(perc);
				natts = slot->tts_tupleDescriptor->natts;
				for (i = 0; i < natts; i++)
				{
					fcinfo.arg[i + 1] = slot_getattr(slot, i + 1, &isnull);
					fcinfo.argnull[i + 1] = isnull;
				}
			}
			advance_transition_function(aggstate, peraggstate, pergroupstate,
										&fcinfo, mem_manager);
		}
	} /* aggno loop */
}

/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with only one input.  This is called after we have completed
 * entering all the input values into the sort object.	We complete the
 * sort, read out the values in sorted order, and run the transition
 * function on each value (applying DISTINCT if appropriate).
 *
 * Note that the strictness of the transition function was checked when
 * entering the values into the sort, so we don't check it again here;
 * we just apply standard SQL DISTINCT logic.
 *
 * The one-input case is handled separately from the multi-input case
 * for performance reasons: for single by-value inputs, such as the
 * common case of count(distinct id), the tuplesort_getdatum code path
 * is around 300% faster.  (The speedup for by-reference types is less
 * but still noticeable.)
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
process_ordered_aggregate_single(AggState *aggstate,
								 AggStatePerAgg peraggstate,
								 AggStatePerGroup pergroupstate)
{
	Datum		oldVal = (Datum) 0;
	bool		oldIsNull = true;
	bool		haveOldVal = false;
	MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
	MemoryContext oldContext;
	bool		isDistinct = peraggstate->aggref->aggdistinct;
	Datum	   *newVal;
	bool	   *isNull;
	FunctionCallInfoData fcinfo;
	
	Assert(peraggstate->numInputs == 1);
	
	if(gp_enable_mk_sort)
		tuplesort_performsort_mk((Tuplesortstate_mk *) peraggstate->sortstate);
	else
		tuplesort_performsort((Tuplesortstate *) peraggstate->sortstate);
	
	/* Load the column into argument 1 (arg 0 will be transition value) */
	
	newVal = fcinfo.arg + 1;
	isNull = fcinfo.argnull + 1;
	
	/*
	 * Note: if input type is pass-by-ref, the datums returned by the sort are
	 * freshly palloc'd in the per-query context, so we must be careful to
	 * pfree them when they are no longer needed.
	 */
	
	while (
		   gp_enable_mk_sort ? 
		   tuplesort_getdatum_mk((Tuplesortstate_mk *)peraggstate->sortstate, true, newVal, isNull)
		   :
		   tuplesort_getdatum((Tuplesortstate *)peraggstate->sortstate, true, newVal, isNull)
		   )
	{
		/*
		 * Clear and select the working context for evaluation of the equality
		 * function and transition function.
		 */
		MemoryContextReset(workcontext);
		oldContext = MemoryContextSwitchTo(workcontext);
		
		/*
		 * If DISTINCT mode, and not distinct from prior, skip it.
		 */
		if (isDistinct && *isNull ) 
		{ 
			/* per SQL, DISTINCT doesn't use nulls */
		}
		else if (isDistinct &&
				 haveOldVal &&
				 ((oldIsNull && *isNull) ||
				  (!oldIsNull && !*isNull &&
				   DatumGetBool(FunctionCall2(&peraggstate->equalfn,
											  oldVal, *newVal)))))
		{
			/* equal to prior, so forget this one */
			if (!peraggstate->inputtypeByVal && !*isNull)
				pfree(DatumGetPointer(*newVal));
		}
		else
		{
			advance_transition_function(aggstate, peraggstate, pergroupstate,
										&fcinfo, &(aggstate->mem_manager));
			/* forget the old value, if any */
			if (!oldIsNull && !peraggstate->inputtypeByVal)
				pfree(DatumGetPointer(oldVal));
			/* and remember the new one for subsequent equality checks */
			oldVal = *newVal;
			oldIsNull = *isNull;
			haveOldVal = true;
		}
		
		MemoryContextSwitchTo(oldContext);
	}
	
	if (!oldIsNull && !peraggstate->inputtypeByVal)
		pfree(DatumGetPointer(oldVal));
	
	if(gp_enable_mk_sort)
		tuplesort_end_mk((Tuplesortstate_mk *) peraggstate->sortstate);
	else
		tuplesort_end((Tuplesortstate *) peraggstate->sortstate);
	
	peraggstate->sortstate = NULL;
}

/*
 * Run the transition function for an ORDER BY aggregate with more than 
 * one input.  In PG DISTINCT aggregates may also have multiple columns,
 * but in GPDB, only ORDER BY aggregates do.  This is called after we have 
 * completed  entering all the input values into the sort object.	We 
 * complete the sort, read out the values in sorted order, and run the 
 * transition function on each value.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
process_ordered_aggregate_multi(AggState *aggstate,
								AggStatePerAgg peraggstate,
								AggStatePerGroup pergroupstate)
{
	MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
	FunctionCallInfoData fcinfo;
	TupleTableSlot *slot = peraggstate->evalslot;
	int			numArguments = peraggstate->numArguments;
	int			i;
	
	if(gp_enable_mk_sort)
		tuplesort_performsort_mk((Tuplesortstate_mk *) peraggstate->sortstate);
	else
		tuplesort_performsort((Tuplesortstate *) peraggstate->sortstate);
	
	ExecClearTuple(slot);
	
	PG_TRY();
	{
		while (
			gp_enable_mk_sort ? 
			tuplesort_gettupleslot_mk((Tuplesortstate_mk *)peraggstate->sortstate, true, slot)
			:
			tuplesort_gettupleslot((Tuplesortstate *)peraggstate->sortstate, true, slot)
			)
		{
			/*
			 * Extract the first numArguments as datums to pass to the transfn.
			 * (This will help execTuplesMatch too, so do it immediately.)
			 */
			slot_getsomeattrs(slot, numArguments);
			
			/* Load values into fcinfo */
			/* Start from 1, since the 0th arg will be the transition value */
			for (i = 0; i < numArguments; i++)
			{
				fcinfo.arg[i + 1] = slot_get_values(slot)[i];
				fcinfo.argnull[i + 1] = slot_get_isnull(slot)[i];
			}
		
			advance_transition_function(aggstate, peraggstate, pergroupstate,
										&fcinfo, &(aggstate->mem_manager));
		
			/* Reset context each time */
			MemoryContextReset(workcontext);
		
			ExecClearTuple(slot);
		}
	}
	PG_CATCH();
	{
		/* 
		 * The tuple is stored in a memory context that will be released during 
		 * the error handling.  If we don't clear it here we will attempt to 
		 * clear it later after the memory context has been released which would
		 * be a memory context error.
		 */
		ExecClearTuple(slot);

		/* Carry on with error handling. */
		PG_RE_THROW();
	}
	PG_END_TRY();

	if(gp_enable_mk_sort)
		tuplesort_end_mk((Tuplesortstate_mk *) peraggstate->sortstate);
	else
		tuplesort_end((Tuplesortstate *) peraggstate->sortstate);
	
	peraggstate->sortstate = NULL;
}

/*
 * finalize_aggregates
 *   Compute the final value for all aggregate functions.
 */
void
finalize_aggregates(AggState *aggstate, AggStatePerGroup pergroup)
{
        AggStatePerAgg peragg = aggstate->peragg;
        ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
        Datum *aggvalues = econtext->ecxt_aggvalues;
        bool *aggnulls = econtext->ecxt_aggnulls;

        for (int aggno = 0; aggno < aggstate->numaggs; aggno++)
        {
                AggStatePerAgg peraggstate = &peragg[aggno];
                AggStatePerGroup pergroupstate = &pergroup[aggno];
                if ( peraggstate->numSortCols > 0 )
                {
                        if ( peraggstate->numInputs == 1 )
                        {
                                process_ordered_aggregate_single(aggstate, peraggstate, pergroupstate);
                        }

                        else
                        {
                                process_ordered_aggregate_multi(aggstate, peraggstate, pergroupstate);
                        }
                }
                finalize_aggregate(aggstate, peraggstate, pergroupstate,
                                                   &aggvalues[aggno], &aggnulls[aggno]);
        }
}

/*
 * Compute the final value of one aggregate.
 *
 * The finalfunction will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 */
static void
finalize_aggregate(AggState *aggstate,
				   AggStatePerAgg peraggstate,
				   AggStatePerGroup pergroupstate,
				   Datum *resultVal, bool *resultIsNull)
{
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	/*
	 * Apply the agg's finalfn if one is provided, else return transValue.
	 */
	if (OidIsValid(peraggstate->finalfn_oid))
	{
		FunctionCallInfoData fcinfo;

		InitFunctionCallInfoData(fcinfo, &(peraggstate->finalfn), 1,
								 (void *) aggstate, NULL);
		fcinfo.arg[0] = pergroupstate->transValue;
		fcinfo.argnull[0] = pergroupstate->transValueIsNull;
		if (fcinfo.flinfo->fn_strict && pergroupstate->transValueIsNull)
		{
			/* don't call a strict function with NULL inputs */
			*resultVal = (Datum) 0;
			*resultIsNull = true;
		}
		else
		{
			*resultVal = FunctionCallInvoke(&fcinfo);
			*resultIsNull = fcinfo.isnull;
		}
	}
	else
	{
		*resultVal = pergroupstate->transValue;
		*resultIsNull = pergroupstate->transValueIsNull;
	}

	/*
	 * If result is pass-by-ref, make sure it is in the right context.
	 */
	if (!peraggstate->resulttypeByVal && !*resultIsNull &&
		!MemoryContextContainsGenericAllocation(CurrentMemoryContext,
							   DatumGetPointer(*resultVal)))
		*resultVal = datumCopy(*resultVal,
							   peraggstate->resulttypeByVal,
							   peraggstate->resulttypeLen);

	MemoryContextSwitchTo(oldContext);
}

/*
 * find_unaggregated_cols
 *	  Construct a bitmapset of the column numbers of un-aggregated Vars
 *	  appearing in our targetlist and qual (HAVING clause)
 */
static Bitmapset *
find_unaggregated_cols(AggState *aggstate)
{
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	Bitmapset *colnos;

	colnos = NULL;
	(void) find_unaggregated_cols_walker((Node *) node->plan.targetlist,
										 &colnos);
	(void) find_unaggregated_cols_walker((Node *) node->plan.qual,
										 &colnos);
	return colnos;
}

static bool
find_unaggregated_cols_walker(Node *node, Bitmapset **colnos)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		/* setrefs.c should have set the varno to 0 */
		Assert(var->varno == 0);
		Assert(var->varlevelsup == 0);
		*colnos = bms_add_member(*colnos, var->varattno);
		return false;
	}
	if (IsA(node, Aggref))		/* do not descend into aggregate exprs */
		return false;
	return expression_tree_walker(node, find_unaggregated_cols_walker,
								  (void *) colnos);
}

/* 
 * Create a list of the tuple columns that actually need to be stored
 * in hashtable entries.  The incoming tuples from the child plan node
 * will contain grouping columns, other columns referenced in our
 * targetlist and qual, columns used to compute the aggregate functions,
 * and perhaps just junk columns we don't use at all.  Only columns of the
 * first two types need to be stored in the hashtable, and getting rid of
 * the others can make the table entries significantly smaller.  To avoid
 * messing up Var numbering, we keep the same tuple descriptor for
 * hashtable entries as the incoming tuples have, but set unwanted columns
 * to NULL in the tuples that go into the table.
 *
 * To eliminate duplicates, we build a bitmapset of the needed columns,
 * then convert it to an integer list (cheaper to scan at runtime).
 * The list is in decreasing order so that the first entry is the largest;
 * lookup_hash_entry depends on this to use slot_getsomeattrs correctly.
 *
 * Note: at present, searching the tlist/qual is not really necessary
 * since the parser should disallow any unaggregated references to
 * ungrouped columns.  However, the search will be needed when we add
 * support for SQL99 semantics that allow use of "functionally dependent"
 * columns that haven't been explicitly grouped by.
 */
List *
get_agg_hash_collist(AggState *aggstate)
{
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	Bitmapset  *colnos;
	List	   *collist;
	int			i;

	/* Find Vars that will be needed in tlist and qual */
	colnos = find_unaggregated_cols(aggstate);
	/* Add in all the grouping columns */
	for (i = 0; i < node->numCols; i++)
		colnos = bms_add_member(colnos, node->grpColIdx[i]);
	/* Convert to list, using lcons so largest element ends up first */
	collist = NIL;
	while ((i = bms_first_member(colnos)) >= 0)
		collist = lcons_int(i, collist);
	return collist;
}


/*
 * Estimate per-hash-table-entry overhead for the planner.
 *
 * Note that the estimate does not include space for pass-by-reference
 * transition data values, nor for the representative tuple of each group.
 */
Size
hash_agg_entry_size(int numAggs)
{
	Size		entrysize;

	/* This must match build_hash_table */
	entrysize = sizeof(AggHashEntryData) +
		(numAggs - 1) *sizeof(AggStatePerGroupData);
	entrysize = MAXALIGN(entrysize);
	/* Account for hashtable overhead (assuming fill factor = 1) */
	entrysize += 3 * sizeof(void *);
	return entrysize;
}

/*
 * ExecAgg -
 *
 *	  ExecAgg receives tuples from its outer subplan and aggregates over
 *	  the appropriate attribute for each aggregate function use (Aggref
 *	  node) appearing in the targetlist or qual of the node.  The number
 *	  of tuples to aggregate over depends on whether grouped or plain
 *	  aggregation is selected.	In grouped aggregation, we produce a result
 *	  row for each group; in plain aggregation there's a single result row
 *	  for the whole query.	In either case, the value of each aggregate is
 *	  stored in the expression context to be used when ExecProject evaluates
 *	  the result tuple.
 *
 * XXX: Fix BTree code.
 *
 * Streaming bottom: forces end of passes when no tuple for underlying node.  
 *
 * MPP-2614: Btree scan will return null tuple at end of scan.  However,
 * if one calls ExecProNode again on a btree scan, it will restart from
 * beginning even though we did not call rescan.  This is a bug on
 * btree scan, but mask it off here for v3.1.  Really should fix Btree
 * code.
 */
TupleTableSlot *
ExecAgg(AggState *node)
{
	if (node->agg_done)
	{
		ExecEagerFreeAgg(node);
		return NULL;
	}
	

	if (((Agg *) node->ss.ps.plan)->aggstrategy == AGG_HASHED)
	{
		TupleTableSlot *tuple = NULL;
		bool streaming = ((Agg *) node->ss.ps.plan)->streaming;

		/*
		 * ExecAgg processing for hashed aggregation -- returns the
		 * next result tuple or NULL. When returning NULL also sets
		 * aggstate to prevent future calls.
		 */
	
		if (node->hhashtable == NULL)
		{
			bool tupremain;
			
			node->hhashtable = create_agg_hash_table(node);
			tupremain = agg_hash_initial_pass(node);
			
			if ( streaming )
			{
				if ( tupremain )
					node->hhashtable->state = HASHAGG_STREAMING;
				else
					node->hhashtable->state = HASHAGG_END_OF_PASSES;
			}
			else
				node->hhashtable->state = HASHAGG_BETWEEN_PASSES;
		}
		
		/* On each call we either return a tuple corresponding to a hash 
		 * entry (consuming the entry) or fall through to a state machine
		 * that tries to make additional hash entries available and continue
		 * the loop.  (This may result in reaching the "exit" state and
		 * returning a NULL tuple).
		 */
		for (;;)
		{
			if (!node->hhashtable->is_spilling)
			{
				tuple = agg_retrieve_hash_table(node);
				node->agg_done = false; /* Not done 'til batches used up. */
				
				if (tuple != NULL)
					return tuple;
			}
		
			switch (node->hhashtable->state)
			{
				case HASHAGG_BETWEEN_PASSES:
					Assert(!streaming);
					if (agg_hash_next_pass(node))
					{
						node->hhashtable->state = HASHAGG_BETWEEN_PASSES;
						continue;
					}
					node->hhashtable->state = HASHAGG_END_OF_PASSES;
					/*
					 * pass through. Be sure that the next case statment
					 * is HASHAGG_END_OF_PASSES.
					 */

				case HASHAGG_END_OF_PASSES:
					node->agg_done = true;
					if (gp_workfile_caching && node->workfiles_created)
					{
						/*
						 * HashAgg closes each spill file after it is done with
						 * them. Since we got here on the regular path, all
						 * files should be closed.
						 */
						Assert(node->hhashtable->work_set);
						Assert(node->hhashtable->spill_set == NULL);
						agg_hash_close_state_file(node->hhashtable);
						agg_hash_mark_spillset_complete(node);
					}
					ExecEagerFreeAgg(node);
					return NULL;

				case HASHAGG_STREAMING:
					Assert(streaming);
					if ( !agg_hash_stream(node) )
						node->hhashtable->state = HASHAGG_END_OF_PASSES;
					continue;

				case HASHAGG_BEFORE_FIRST_PASS:
				default:
					elog(ERROR,"hybrid hash aggregation sequencing error");
			}
		}
	}
	else 
	{
		return agg_retrieve_direct(node);
	}
}

/*
 * agg_retrieve_scalar
 *   Compute the scalar aggregates.
 */
static TupleTableSlot *
agg_retrieve_scalar(AggState *aggstate)
{
        AggStatePerAgg peragg = aggstate->peragg;
        AggStatePerGroup pergroup = aggstate->pergroup ;

        initialize_aggregates(aggstate, peragg, pergroup, &(aggstate->mem_manager));

        /*
         * We loop through input tuples, and compute the aggregates.
         */
        while (!aggstate->agg_done)
        {
                ExprContext *tmpcontext = aggstate->tmpcontext;
                /* Reset the per-input-tuple context */
                ResetExprContext(tmpcontext);
                PlanState *outerPlan = outerPlanState(aggstate);
                TupleTableSlot *outerslot = ExecProcNode(outerPlan);
                if (TupIsNull(outerslot))
                {
                        aggstate->agg_done = true;
                        break;
                }
                Gpmon_M_Incr(GpmonPktFromAggState(aggstate), GPMON_QEXEC_M_ROWSIN);
                CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);

                tmpcontext->ecxt_scantuple = outerslot;
                advance_aggregates(aggstate, pergroup, &(aggstate->mem_manager));
        }

        finalize_aggregates(aggstate, pergroup);

        ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
        Agg *node = (Agg*)aggstate->ss.ps.plan;
        econtext->grouping = node->grouping;
        econtext->group_id = node->rollupGSTimes;
        /* Check the qual (HAVING clause). */
        if (ExecQual(aggstate->ss.ps.qual, econtext, false))
        {
                Gpmon_M_Incr_Rows_Out(GpmonPktFromAggState(aggstate));
                CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);

                /*
                 * Form and return a projection tuple using the aggregate results
                 * and the representative input tuple.
                 */
                return ExecProject(aggstate->ss.ps.ps_ProjInfo, NULL);
        }
        return NULL;
}

/*
 * copyFirstTupleInGroup
 *   Make a copy of a given tuple as the first tuple in a group.
 */
static void
copyFirstTupleInGroup(AggState *aggstate, TupleTableSlot *outerslot)
{
        TupleTableSlot *firstSlot = aggstate->ss.ss_ScanTupleSlot;
        slot_getallattrs(outerslot);
        aggstate->grp_firstTuple = memtuple_form_to(firstSlot->tts_mt_bind,
                                                    slot_get_values(outerslot),
                                                    slot_get_isnull(outerslot),
                                                    NULL /* mtup */,
                                                    NULL /* destlen */,
                                                    false /* inline_toast */);
}

/*
 * processFirstTupleInGroup
 *   Process the first input tuple in a group: initialize the working state
 * for the new group, and accumulate the tuple into the aggregates.
 */
static void
processFirstTupleInGroup(AggState *aggstate)
{
        Assert(aggstate->grp_firstTuple != NULL);

        TupleTableSlot *firstSlot = aggstate->ss.ss_ScanTupleSlot;
        ExecStoreMemTuple(aggstate->grp_firstTuple,
                                          firstSlot,
                                          true);
        aggstate->grp_firstTuple = NULL;

        /* Clear the per-output-tuple context for each group */
        ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
        ResetExprContext(econtext);
        MemoryContextResetAndDeleteChildren(aggstate->aggcontext);

        /* Initialize working state for a new input tuple group */
        AggStatePerAgg peragg = aggstate->peragg;
        AggStatePerGroup pergroup = aggstate->pergroup;
        initialize_aggregates(aggstate, peragg, pergroup, &(aggstate->mem_manager));

        ExprContext *tmpcontext = aggstate->tmpcontext;
        tmpcontext->ecxt_scantuple = firstSlot;
        advance_aggregates(aggstate, pergroup, &(aggstate->mem_manager));
}

/*
 * processNextTuple
 *  Process an input tuple. If this tuple belongs to the current group,
 * its value is accumulated into the aggregates for the current group.
 * Otherwise, this tuple indicates a new group starts. This function
 * makes a copy for this tuple.
 *
 * This function returns true if a new group starts.
 */
static bool
processNextTuple(AggState *aggstate, TupleTableSlot *outerslot)
{
        Assert(aggstate->grp_firstTuple == NULL);

        TupleTableSlot *firstSlot = aggstate->ss.ss_ScanTupleSlot;
        Assert(!TupIsNull(firstSlot));
        Agg *node = (Agg*)aggstate->ss.ps.plan;
        ExprContext *tmpcontext = aggstate->tmpcontext;
        bool tuple_match = execTuplesMatch(firstSlot,
                                                                           outerslot,
                                                                           node->numCols - node->numNullCols,
                                                                           node->grpColIdx,
                                                                           aggstate->eqfunctions,
                                                                           tmpcontext->ecxt_per_tuple_memory);

        bool isNewGroup = false;
        if (tuple_match)
        {
                tmpcontext->ecxt_scantuple = outerslot;
                AggStatePerGroup pergroup = aggstate->pergroup;
                advance_aggregates(aggstate, pergroup, &(aggstate->mem_manager));
        }
        else
        {
                copyFirstTupleInGroup(aggstate, outerslot);
                isNewGroup = true;
        }
        return isNewGroup;
}

/*
 * agg_retrieve_group
 *    Compute an aggreate for each group.
 *
 * This function assumes that input tuples arrive in groups. A new tuple
 * with a different grouping key means the start of a new group.
 *
 * This function also computes Grouping(), Group_id() if they appear in
 * the target list.
 */
static TupleTableSlot *
agg_retrieve_group(AggState *aggstate)
{
        Assert(((Agg*)aggstate->ss.ps.plan)->aggstrategy == AGG_SORTED);
        TupleTableSlot *firstSlot = aggstate->ss.ss_ScanTupleSlot;
        PlanState *outerPlan = outerPlanState(aggstate);
        TupleTableSlot *outerslot = NULL;
        /* Read the first input tuple in a group if this is not done. */
        if (TupIsNull(firstSlot) &&
                aggstate->grp_firstTuple == NULL)
        {
                outerslot = ExecProcNode(outerPlan);
                if (TupIsNull(outerslot))
                {
                        aggstate->agg_done = true;
                        return NULL;
                }
                Gpmon_M_Incr(GpmonPktFromAggState(aggstate), GPMON_QEXEC_M_ROWSIN);
                CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);
                copyFirstTupleInGroup(aggstate, outerslot);
        }

        /*
         * We loop through input tuples. If a tuple has a different grouping key,
         * a new group starts. At this point, we have seen all tuples for the
         * previous group, and is able to compute and return the final aggregate
         * value for this group.
         */
        while (!aggstate->agg_done)
        {
                /*
                 * If grp_firstTuple is not null, it means that we have read the
                 * first tuple in a new group. Initialize the working state for
                 * the new group, and advance the aggregate state for this tuple.
                 */
                if (aggstate->grp_firstTuple != NULL)
                {
                        processFirstTupleInGroup(aggstate);
                }

                Assert(!TupIsNull(firstSlot));
                /*
                 * Read and process the next input tuple. If this tuple belongs to
                 * the same group, simply accumlate its value to the intermediate
                 * state, and go back to the beginning of the loop to process the
                 * next input tuple. Otherwise, a new group starts. We finalize
                 * the aggregate value for the current group, and return the value
                 * if it satisfies the qual.
                 */

                /* Reset the per-input-tuple context */
                ExprContext *tmpcontext = aggstate->tmpcontext;
                ResetExprContext(tmpcontext);
                bool isNewGroup = false;
                outerslot = ExecProcNode(outerPlan);
                if (TupIsNull(outerslot))
                {
                        aggstate->agg_done = true;
                        isNewGroup = true;
                }
                else
                {
                        Gpmon_M_Incr(GpmonPktFromAggState(aggstate), GPMON_QEXEC_M_ROWSIN);
                        CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);
                        isNewGroup = processNextTuple(aggstate, outerslot);
                }

                if (isNewGroup)
                {
                        Assert(!TupIsNull(firstSlot));
                        TupleTableSlot *result =
                                computeTupleWithFinalAggregate(aggstate, firstSlot);
                        if (!TupIsNull(result))
                        {
                                return result;
                        }
                }
        }
        return NULL;
}

/*
 * isGroupTuple
 *   Return true when the given tuple is a tuple in a group that
 * contribute to the final aggregates.
 *
 * This is only used in a ROLLUP Agg to distinguish tuples from
 * those that are only pass-through.
 */
static bool
isGroupTuple(AggState *aggstate,
                         TupleTableSlot *outerslot)
{
        Agg *node = (Agg*)aggstate->ss.ps.plan;
        Assert(node->numCols - node->numNullCols >= 2);
        uint64 inputGrouping = node->inputGrouping;
        int groupingAttNo = node->grpColIdx[node->numCols - node->numNullCols - 2];
        uint64 tupleGrouping =
                tuple_grouping(outerslot, inputGrouping, groupingAttNo);
        Assert(tupleGrouping <= inputGrouping);
        return (tupleGrouping == inputGrouping);
}

/*
 * preparePassThruTuple
 *   Prepare a input tuple to be outputted by the Agg node.
 * If this Agg node is the last rollup Agg node, the input tuple
 * needs to be finalized.
 *
 * This function returns NULL if the input tuple does not
 * satisfy the qual, or a result tuple that is ready to be
 * returned to the upper node.
 */
static TupleTableSlot *
preparePassThruTuple(AggState *aggstate,
                                         TupleTableSlot *outerslot)
{
        Agg *node = (Agg*)aggstate->ss.ps.plan;
        Assert(node->numCols - node->numNullCols >= 2);
        Assert(!TupIsNull(outerslot));

        int groupingAttNo = node->grpColIdx[(node->numCols - node->numNullCols) - 2];
        int groupIdAttNo = node->grpColIdx[(node->numCols - node->numNullCols) - 1];
        TupleTableSlot *result = outerslot;

        /*
         * If this node is the final rollup, the pass-through input tuples
         * need to be finalized before returning.
         */
        if (aggstate->aggType == AggTypeFinalRollup)
        {
                AggStatePerAgg peragg = aggstate->peragg;
                AggStatePerGroup perpassthru = aggstate->perpassthru;
                ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;

                initialize_aggregates(aggstate, peragg, perpassthru, &(aggstate->mem_manager));

                ExprContext *tmpcontext = aggstate->tmpcontext;
                tmpcontext->ecxt_scantuple = outerslot;
                advance_aggregates(aggstate, perpassthru, &(aggstate->mem_manager));

                finalize_aggregates(aggstate, perpassthru);

                econtext->ecxt_scantuple = outerslot;

                Assert(node->inputHasGrouping);
                econtext->grouping =
                        get_grouping_groupid(econtext->ecxt_scantuple, groupingAttNo);
                econtext->group_id =
                        get_grouping_groupid(econtext->ecxt_scantuple, groupIdAttNo);

                /*
                 * Check the qual. Form and return a projection tuple using the aggregate results
                 * and the representative input tuple.  Note we do not support
                 * aggregates returning sets ...
                 */
                result = NULL;
                if (ExecQual(aggstate->ss.ps.qual, econtext, false))
                {
                        Gpmon_M_Incr_Rows_Out(GpmonPktFromAggState(aggstate));
                        CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);

                        result = ExecProject(aggstate->ss.ps.ps_ProjInfo, NULL);
                }
        }

        return result;
}


/*
 * computeTupleWithFinalAggregate
 *   Finalize the aggregate values and check whether the result
 * satisfies the given qual. If so, form and return a projection
 * tuple.
 */
static TupleTableSlot *
computeTupleWithFinalAggregate(AggState *aggstate,
                                                           TupleTableSlot *firstSlot)
{
        AggStatePerGroup pergroup = aggstate->pergroup;
        finalize_aggregates(aggstate, pergroup);
        ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
        econtext->ecxt_scantuple = firstSlot;

        /*
         * If this is part of a rollup, we need to mask the grouping attributes that
         * are supposedly null in this Agg node.
         */
        Agg *node = (Agg*)aggstate->ss.ps.plan;
        if (node->numNullCols > 0)
        {
                slot_getallattrs(firstSlot);
                bool *isnull = slot_get_isnull(firstSlot);

                for (int attno = node->numCols - node->numNullCols; attno < node->numCols; attno++)
                {
                        isnull[node->grpColIdx[attno] - 1] = true;
                }
        }

        econtext->grouping = node->grouping;
        econtext->group_id = node->rollupGSTimes;
        /*
         * In the second stage of a rollup, grouping and group_id values in the
         * target list from the input tuples.
         */
        if (aggstate->aggType == AggTypeGroup && node->inputHasGrouping)
        {
                int groupingAttNo = node->grpColIdx[node->numCols - node->numNullCols - 2];
                int groupIdAttNo = node->grpColIdx[node->numCols-node->numNullCols - 1];
                econtext->grouping =
                        get_grouping_groupid(econtext->ecxt_scantuple, groupingAttNo);
                econtext->group_id =
                        get_grouping_groupid(econtext->ecxt_scantuple, groupIdAttNo);

        }

        /*
         * Check the qual (HAVING clause). If it is satisfied, form and return
         * a projection tuple using the aggregate results and the representative
         * input tuple.
         */
        TupleTableSlot *result = NULL;
        if (ExecQual(aggstate->ss.ps.qual, econtext, false))
        {
                Gpmon_M_Incr_Rows_Out(GpmonPktFromAggState(aggstate));
                CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);

                result = ExecProject(aggstate->ss.ps.ps_ProjInfo, NULL);
        }
        return result;
}


/*
 * agg_retrieve_rollup
 *   Compute aggregates for a group in a rollup.
 *
 * The difference between this Agg and the ordinary group agg is that
 * (1) not all input tuples need to be aggregated. Some of input tuples
 * are result tuples from 2 or more levels lower in the rollup hierarchy.
 * These tuples need to be passed up to the upper node (pass-through).
 * (2) all input tuples are part of the result tuples for this Agg node.
 */
static TupleTableSlot *
agg_retrieve_rollup(AggState *aggstate)
{
        Assert(((Agg*)aggstate->ss.ps.plan)->aggstrategy == AGG_SORTED);
        Assert(((Agg*)aggstate->ss.ps.plan)->inputHasGrouping);
        Assert(aggstate->aggType == AggTypeIntermediateRollup ||
                   aggstate->aggType == AggTypeFinalRollup);

        TupleTableSlot *firstSlot = aggstate->ss.ss_ScanTupleSlot;
        PlanState *outerPlan = outerPlanState(aggstate);
        TupleTableSlot *outerslot = NULL;

        /*
         * Read the input tuple until we read a tuple that is
         * the first tuple in a group to be aggregated. In the meanwhile,
         * all input tuples need to be returned.
         */
        while (TupIsNull(firstSlot) &&
                   aggstate->grp_firstTuple == NULL)
        {
                /* Reset the per-input-tuple context */
                ExprContext *tmpcontext = aggstate->tmpcontext;
                ResetExprContext(tmpcontext);
                outerslot = ExecProcNode(outerPlan);
                if (TupIsNull(outerslot))
                {
                        aggstate->agg_done = true;
                        return NULL;
                }
                Gpmon_M_Incr(GpmonPktFromAggState(aggstate), GPMON_QEXEC_M_ROWSIN);
                CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);
                /*
                 * If this tuple needs to be aggregated, this is the first tuple
                 * in a group. We make a copy of this tuple. Otherwise,
                 * outputted this tuple as a pass-through tuple. The first tuple
                 * in a new group will be passed-through below.
                 */
                if (isGroupTuple(aggstate, outerslot))
                {
                        copyFirstTupleInGroup(aggstate, outerslot);
                }
                else
                {
                        /*
                         * If this tuple needs to be pass-through, do it now.
                         */
                        TupleTableSlot *passThruTuple = preparePassThruTuple(aggstate, outerslot);
                        if (!TupIsNull(passThruTuple))
                        {
                                return passThruTuple;
                        }
                }
        }
        /*
         * We loop through input tuples. If a tuple has a different grouping key,
         * a new group starts. At this point, we have seen all tuples for the
         * previous group, and is able to compute and return the final aggregate
         * value for this group.
         *
         * Note that input tuples also need to be pass-through.
         */
        while (!aggstate->agg_done)
        {
                /*
                 * If grp_firstTuple is not null, it means that we have read the
                 * first tuple in a new group. Initialize the working state for
                 * the new group, and advance the aggregate state for this tuple.
                 *
                 * This tuple also needs to be outputted here.
                 */
                if (aggstate->grp_firstTuple != NULL)
                {
                        processFirstTupleInGroup(aggstate);

                        /* Pass through the first tuple in this new group now. */
                        TupleTableSlot *passThruTuple = preparePassThruTuple(aggstate, firstSlot);
                        if (!TupIsNull(passThruTuple))
                        {
                                return passThruTuple;
                        }
                }

                /*
                 * Read and process the next input tuple. If this tuple is a pass-through
                 * tuple, return it. If this tuple needs to be aggregated, and belongs to
                 * the same group, simply accumulate its value to the intermediate
                 * state, and go back to the beginning of the loop to process the
                 * next input tuple. Otherwise, a new group starts. We finalize
                 * the aggregate value for the current group, and return the value
                 * if it satisfies the qual.
                 */

                /* Reset the per-input-tuple context */
                ExprContext *tmpcontext = aggstate->tmpcontext;
                ResetExprContext(tmpcontext);

                bool isNewGroup = false;

                outerslot = ExecProcNode(outerPlan);
                if (TupIsNull(outerslot))
                {
                        aggstate->agg_done = true;
                        isNewGroup = true;
                }

                else
                {
                        Assert(!TupIsNull(outerslot));

                        Gpmon_M_Incr(GpmonPktFromAggState(aggstate), GPMON_QEXEC_M_ROWSIN);
                        CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);
                        if (isGroupTuple(aggstate, outerslot))
                        {
                                isNewGroup = processNextTuple(aggstate, outerslot);
                        }
                }
                if (isNewGroup)
                {
                        Assert(!TupIsNull(firstSlot));
                        TupleTableSlot *result = computeTupleWithFinalAggregate(aggstate, firstSlot);
                        if (!TupIsNull(result))
                        {
                                return result;
                        }
                }

                if (!TupIsNull(outerslot))
                {
                        TupleTableSlot *passThruTuple = preparePassThruTuple(aggstate, outerslot);
                        if (!TupIsNull(passThruTuple))
                        {
                                return passThruTuple;
                        }
                }
        }

        return NULL;
}

/*
 * ExecAgg for non-hashed case.
 */
static TupleTableSlot *
agg_retrieve_direct(AggState *aggstate)
{
        if (aggstate->agg_done)
        {
                return NULL;
        }

        switch(aggstate->aggType)
        {
                case AggTypeScalar:
                        return agg_retrieve_scalar(aggstate);

                case AggTypeGroup:
                        return agg_retrieve_group(aggstate);

                case AggTypeIntermediateRollup:
                case AggTypeFinalRollup:
                        return agg_retrieve_rollup(aggstate);
                default:
                        insist_log(false, "invalid Agg node: type %d", aggstate->aggType);
        }
        return NULL;
}


/*
 * ExecAgg for hashed case: retrieve groups from hash table
 */
TupleTableSlot *
agg_retrieve_hash_table(AggState *aggstate)
{
	ExprContext *econtext;
	ProjectionInfo *projInfo;
	Datum	   *aggvalues;
	bool	   *aggnulls;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	TupleTableSlot *firstSlot;
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	bool        input_has_grouping = node->inputHasGrouping;
	bool        is_final_rollup_agg =
		(node->lastAgg ||
		 (input_has_grouping && node->numNullCols == 0));

	/*
	 * get state info from node
	 */
	/* econtext is the per-output-tuple expression context */
	econtext = aggstate->ss.ps.ps_ExprContext;
	aggvalues = econtext->ecxt_aggvalues;
	aggnulls = econtext->ecxt_aggnulls;
	projInfo = aggstate->ss.ps.ps_ProjInfo;
	peragg = aggstate->peragg;
	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	if (aggstate->agg_done)
		return NULL;

	/*
	 * We loop retrieving groups until we find one satisfying
	 * aggstate->ss.ps.qual
	 */
	while (!aggstate->agg_done)
	{
		HashAggEntry *entry = agg_hash_iter(aggstate);
			
		if (entry == NULL)
		{
			aggstate->agg_done = TRUE;

			return NULL;
		}
			
		ResetExprContext(econtext);

		/*
		 * Store the copied first input tuple in the tuple table slot reserved
		 * for it, so that it can be used in ExecProject.
		 */
		ExecStoreMemTuple((MemTuple)entry->tuple_and_aggs, firstSlot, false);
		pergroup = (AggStatePerGroup)((char *)entry->tuple_and_aggs + 
					      MAXALIGN(memtuple_get_size((MemTuple)entry->tuple_and_aggs,
									 aggstate->hashslot->tts_mt_bind)));

                /*
                 * Finalize each aggregate calculation, and stash results in the
                 * per-output-tuple context.
                 */
		finalize_aggregates(aggstate, pergroup);

		/*
		 * Use the representative input tuple for any references to
		 * non-aggregated input columns in the qual and tlist.
		 */
		econtext->ecxt_scantuple = firstSlot;

		if (is_final_rollup_agg && input_has_grouping)
		{
			econtext->group_id =
				get_grouping_groupid(econtext->ecxt_scantuple,
									 node->grpColIdx[node->numCols-node->numNullCols-1]);
			econtext->grouping =
				get_grouping_groupid(econtext->ecxt_scantuple,
									 node->grpColIdx[node->numCols-node->numNullCols-2]);
		}
		else
		{
			econtext->group_id = node->rollupGSTimes;
			econtext->grouping = node->grouping;
		}

		/*
		 * Check the qual (HAVING clause); if the group does not match, ignore
		 * it and loop back to try to process another group.
		 */
		if (ExecQual(aggstate->ss.ps.qual, econtext, false))
		{
			/*
			 * Form and return a projection tuple using the aggregate results
			 * and the representative input tuple.	Note we do not support
			 * aggregates returning sets ...
			 */
			Gpmon_M_Incr_Rows_Out(GpmonPktFromAggState(aggstate)); 
			CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);
			return ExecProject(projInfo, NULL);
		}
	}

	/* No more groups */
	return NULL;
}

/*
 * getAggType
 *   Get the aggType for the given Agg node.
 *
 * We should really store the type in the Agg struct, and let the planner set the
 * correct type. As an intermediate step, we compute the type here.
 */
static int
getAggType(Agg *node)
{
        int aggType = AggTypeScalar;
        if (node->numCols > 0 &&
                (node->lastAgg &&
                 (node->inputHasGrouping && node->numNullCols > 0)))
        {
                aggType = AggTypeFinalRollup;
        }
        else if (node->numCols > 0 &&
                         (node->inputHasGrouping && node->numNullCols > 0))
        {
                aggType = AggTypeIntermediateRollup;
        }
        else if (node->numCols > 0)
        {
                aggType = AggTypeGroup;
        }
        else
        {
                insist_log(node->aggstrategy == AGG_PLAIN, "wrong Agg strategy: %d", node->aggstrategy);
        }

        return aggType;
}


/* -----------------
 * ExecInitAgg
 *
 *	Creates the run-time information for the agg node produced by the
 *	planner and initializes its outer subtree
 * -----------------
 */
AggState *
ExecInitAgg(Agg *node, EState *estate, int eflags)
{
	AggState   *aggstate;
	AggStatePerAgg peragg;
	Plan	   *outerPlan;
	ExprContext *econtext;
	int			numaggs,
				aggno;
	ListCell   *l;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	aggstate = makeNode(AggState);
	aggstate->ss.ps.plan = (Plan *) node;
	aggstate->ss.ps.state = estate;

	aggstate->aggs = NIL;
	aggstate->numaggs = 0;
	aggstate->eqfunctions = NULL;
	aggstate->hashfunctions = NULL;
	aggstate->peragg = NULL;
	aggstate->agg_done = false;
	aggstate->pergroup = NULL;
	aggstate->grp_firstTuple = NULL;
	aggstate->hashtable = NULL;
	agg_hash_reset_workfile_state(aggstate);

	/*
	 * Create expression contexts.	We need two, one for per-input-tuple
	 * processing and one for per-output-tuple processing.	We cheat a little
	 * by using ExecAssignExprContext() to build both.
	 */
	ExecAssignExprContext(estate, &aggstate->ss.ps);
	aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext;
	ExecAssignExprContext(estate, &aggstate->ss.ps);

	/*
	 * We also need a long-lived memory context for holding hashtable data
	 * structures and transition values.  NOTE: the details of what is stored
	 * in aggcontext and what is stored in the regular per-query memory
	 * context are driven by a simple decision: we want to reset the
	 * aggcontext in ExecReScanAgg to recover no-longer-wanted space.
	 */
	aggstate->aggcontext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "AggContext",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);

#define AGG_NSLOTS 3

	/*
	 * tuple table initialization
	 */
	aggstate->ss.ss_ScanTupleSlot = ExecInitExtraTupleSlot(estate);
	ExecInitResultTupleSlot(estate, &aggstate->ss.ps);
	aggstate->hashslot = ExecInitExtraTupleSlot(estate);

	/*
	 * initialize child expressions
	 *
	 * Note: ExecInitExpr finds Aggrefs for us, and also checks that no aggs
	 * contain other agg calls in their arguments.	This would make no sense
	 * under SQL semantics anyway (and it's forbidden by the spec). Because
	 * that is true, we don't need to worry about evaluating the aggs in any
	 * particular order.
	 */
	aggstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) aggstate);
	aggstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) aggstate);

    /* 
     * CDB: Offer extra info for EXPLAIN ANALYZE. 
     */
    if (estate->es_instrument)
    {
	    /* Allocate string buffer. */
        aggstate->ss.ps.cdbexplainbuf = makeStringInfo();        
        
        /* Request a callback at end of query. */
        aggstate->ss.ps.cdbexplainfun = ExecAggExplainEnd;
    }

	/*
	 * initialize child nodes
	 */
	outerPlan = outerPlan(node);
	if (IsA(outerPlan, ExternalScan)) {
		/*
		 * Hack to indicate to PXF when there is an external scan
		 */
		if (list_length(aggstate->aggs) == 1) {
				AggrefExprState *aggrefstate = (AggrefExprState *) linitial(aggstate->aggs);
				Aggref	   *aggref = (Aggref *) aggrefstate->xprstate.expr;
				//Only dealing with one agg
				if (aggref->aggfnoid == COUNT_ANY_OID || aggref->aggfnoid == COUNT_STAR_OID) {
					eflags |= EXEC_FLAG_EXTERNAL_AGG_COUNT;
				}
		}
	}
	outerPlanState(aggstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * initialize source tuple type.
	 */
	ExecAssignScanTypeFromOuterPlan(&aggstate->ss);

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&aggstate->ss.ps);
	ExecAssignProjectionInfo(&aggstate->ss.ps, NULL);

	/*
	 * get the count of aggregates in targetlist and quals
	 */
	numaggs = aggstate->numaggs;
	Assert(numaggs == list_length(aggstate->aggs) + list_length(aggstate->percs));
	if (numaggs <= 0)
	{
		/*
		 * This is not an error condition: we might be using the Agg node just
		 * to do hash-based grouping.  Even in the regular case,
		 * constant-expression simplification could optimize away all of the
		 * Aggrefs in the targetlist and qual.	So keep going, but force local
		 * copy of numaggs positive so that palloc()s below don't choke.
		 */
		numaggs = 1;
	}

	/*
	 * If we are grouping, precompute fmgr lookup data for inner loop. We need
	 * both equality and hashing functions to do it by hashing, but only
	 * equality if not hashing.
	 */
	if (node->numCols > 0)
	{
		if (node->aggstrategy == AGG_HASHED)
			execTuplesHashPrepare(ExecGetScanType(&aggstate->ss),
								  node->numCols,
								  node->grpColIdx,
								  &aggstate->eqfunctions,
								  &aggstate->hashfunctions);
		else
			aggstate->eqfunctions =
				execTuplesMatchPrepare(ExecGetScanType(&aggstate->ss),
									   node->numCols,
									   node->grpColIdx);
	}

	/*
	 * Set up aggregate-result storage in the output expr context, and also
	 * allocate my private per-agg working storage
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	econtext->ecxt_aggvalues = (Datum *) palloc0(sizeof(Datum) * numaggs);
	econtext->ecxt_aggnulls = (bool *) palloc0(sizeof(bool) * numaggs);

	peragg = (AggStatePerAgg) palloc0(sizeof(AggStatePerAggData) * numaggs);
	aggstate->peragg = peragg;

	if (node->aggstrategy == AGG_HASHED)
	{
		aggstate->hash_needed = get_agg_hash_collist(aggstate);
	}
	else
	{
		AggStatePerGroup pergroup;

		pergroup = (AggStatePerGroup) palloc0(sizeof(AggStatePerGroupData) * numaggs);
		aggstate->pergroup = pergroup;
	}

	/*
	 * Perform lookups of aggregate function info, and initialize the
	 * unchanging fields of the per-agg data.  We also detect duplicate
	 * aggregates (for example, "SELECT sum(x) ... HAVING sum(x) > 0"). When
	 * duplicates are detected, we only make an AggStatePerAgg struct for the
	 * first one.  The clones are simply pointed at the same result entry by
	 * giving them duplicate aggno values.
	 */
	aggno = -1;
	foreach(l, aggstate->aggs)
	{
		AggrefExprState *aggrefstate = (AggrefExprState *) lfirst(l);
		Aggref	   *aggref = (Aggref *) aggrefstate->xprstate.expr;
		AggStatePerAgg peraggstate;
		List	   *inputTargets = NIL;
		List	   *inputSortClauses = NIL;
		Oid		   *inputTypes = NULL;
		int			numInputs;
		int			numArguments;
		int			numSortCols;
		List	   *sortlist;
		HeapTuple	aggTuple;
		Form_pg_aggregate aggform;
		Oid			aggtranstype;
		AclResult	aclresult;
		Oid			transfn_oid = InvalidOid,
					finalfn_oid = InvalidOid;
		Expr	   *transfnexpr,
				   *finalfnexpr,
				   *prelimfnexpr;
		Datum		textInitVal;
		int			i;
		ListCell   *lc;
		cqContext  *pcqCtx;
		
		/* Planner should have assigned aggregate to correct level */
		Assert(aggref->agglevelsup == 0);

		/* Look for a previous duplicate aggregate */
		for (i = 0; i <= aggno; i++)
		{
			if (equal(aggref, peragg[i].aggref) &&
				!contain_volatile_functions((Node *) aggref))
				break;
		}
		if (i <= aggno)
		{
			/* Found a match to an existing entry, so just mark it */
			aggrefstate->aggno = i;
			continue;
		}

		/* Nope, so assign a new PerAgg record */
		peraggstate = &peragg[++aggno];

		/* Mark Aggref state node with assigned index in the result array */
		aggrefstate->aggno = aggno;

		/* Fill in the peraggstate data */
		peraggstate->aggrefstate = aggrefstate;
		peraggstate->aggref = aggref;
		numArguments = list_length(aggref->args);
		peraggstate->numArguments = numArguments;

		/*
		 * Use these information from ExecInitExpr for per agg info.
		 */
		inputTargets = aggrefstate->inputTargets;
		inputSortClauses = aggrefstate->inputSortClauses;
		numInputs = list_length(inputTargets);
		numSortCols = list_length(inputSortClauses);

		peraggstate->numSortCols = numSortCols;
		peraggstate->numInputs = numInputs;
		
		/* MPP has some restrictions. */
		Assert(!(aggref->aggdistinct && aggref->aggorder));
		Assert(numArguments == 1 || !aggref->aggdistinct);

		/* Get actual datatypes of the inputs.	These could be different from
		 * the agg's declared input types, when the agg accepts ANY, ANYARRAY
		 * or ANYELEMENT. The result will have argument types at 0 through 
		 * numArguments-1 and sort key types mixed in or at numArguments through 
		 * numInputs.
		 */
		inputTypes = (Oid*)palloc0(sizeof(Oid)*(numInputs));
		i = 0;
		foreach(lc, inputTargets)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			inputTypes[i++] = exprType((Node*)tle->expr);
		}

		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_aggregate "
					" WHERE aggfnoid = :1 ",
					ObjectIdGetDatum(aggref->aggfnoid)));

		aggTuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(aggTuple))
			elog(ERROR, "cache lookup failed for aggregate %u",
				 aggref->aggfnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);

		/* Check permission to call aggregate function */
		aclresult = pg_proc_aclcheck(aggref->aggfnoid, GetUserId(),
									 ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_PROC,
						   get_func_name(aggref->aggfnoid));

		switch ( aggref->aggstage) /* MPP */
		{
		case AGGSTAGE_NORMAL: /* Single-stage aggregation */
			peraggstate->transfn_oid = transfn_oid = aggform->aggtransfn;
			peraggstate->finalfn_oid = finalfn_oid = aggform->aggfinalfn;
			break;

		case AGGSTAGE_PARTIAL:/* Two-stage aggregation -- preliminary stage */
			/* the perliminary stage for two-stage aggregation */
			peraggstate->transfn_oid = transfn_oid = aggform->aggtransfn;
			peraggstate->finalfn_oid = finalfn_oid = InvalidOid;
			break;

		case AGGSTAGE_INTERMEDIATE:
			peraggstate->transfn_oid = transfn_oid = aggform->aggprelimfn;
			peraggstate->finalfn_oid = finalfn_oid = InvalidOid;
			break;

		case AGGSTAGE_FINAL: /* Two-stage aggregation - final stage */
			peraggstate->transfn_oid = transfn_oid = aggform->aggprelimfn;
			peraggstate->finalfn_oid = finalfn_oid = aggform->aggfinalfn;
			break;
		}

		peraggstate->prelimfn_oid = aggform->aggprelimfn;

		/* Check that aggregate owner has permission to call component fns */
		{
			Oid			aggOwner;
			int			fetchCount;

			aggOwner = caql_getoid_plus(
					NULL,
					&fetchCount,
					NULL,
					cql("SELECT proowner FROM pg_proc "
						" WHERE oid = :1 ",
						ObjectIdGetDatum(aggref->aggfnoid)));

			if (!fetchCount)
				elog(ERROR, "cache lookup failed for function %u",
					 aggref->aggfnoid);

			aclresult = pg_proc_aclcheck(transfn_oid, aggOwner,
										 ACL_EXECUTE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_PROC,
							   get_func_name(transfn_oid));
			if (OidIsValid(finalfn_oid))
			{
				aclresult = pg_proc_aclcheck(finalfn_oid, aggOwner,
											 ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, ACL_KIND_PROC,
								   get_func_name(finalfn_oid));
			}
			if (OidIsValid(peraggstate->prelimfn_oid))
			{
				aclresult = pg_proc_aclcheck(peraggstate->prelimfn_oid, aggOwner,
											 ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, ACL_KIND_PROC,
								   get_func_name(peraggstate->prelimfn_oid));
			}
		}

		/* check if the transition type is polymorphic and if so resolve it */
		aggtranstype = resolve_polymorphic_transtype(aggform->aggtranstype, 
													 aggref->aggfnoid,
											   		 inputTypes);
											   
		/* build expression trees using actual argument & result types */
		build_aggregate_fnexprs(inputTypes,
								numArguments,
								aggtranstype,
								aggref->aggtype,
								transfn_oid,
								finalfn_oid,
								InvalidOid /* prelim */,
								InvalidOid /* invtrans */,
								InvalidOid /* invprelim */,
								&transfnexpr,
								&finalfnexpr,
								&prelimfnexpr,
								NULL,
								NULL);
		
		fmgr_info(transfn_oid, &peraggstate->transfn);
		peraggstate->transfn.fn_expr = (Node *) transfnexpr;

		if (OidIsValid(finalfn_oid))
		{
			fmgr_info(finalfn_oid, &peraggstate->finalfn);
			peraggstate->finalfn.fn_expr = (Node *) finalfnexpr;
		}
		
		if (OidIsValid(peraggstate->prelimfn_oid))
		{
			fmgr_info(peraggstate->prelimfn_oid, &peraggstate->prelimfn);
			peraggstate->prelimfn.fn_expr = (Node *) prelimfnexpr;
		}
		
		get_typlenbyval(aggref->aggtype,
						&peraggstate->resulttypeLen,
						&peraggstate->resulttypeByVal);
		get_typlenbyval(aggtranstype,
						&peraggstate->transtypeLen,
						&peraggstate->transtypeByVal);

		/*
		 * initval is potentially null, so don't try to access it as a struct
		 * field. Must do it the hard way with caql_getattr
		 */
		textInitVal = caql_getattr(pcqCtx,
								   Anum_pg_aggregate_agginitval,
								   &peraggstate->initValueIsNull);

		if (peraggstate->initValueIsNull)
			peraggstate->initValue = (Datum) 0;
		else
			peraggstate->initValue = GetAggInitVal(textInitVal,
												   aggtranstype);

		/*
		 * If the transfn is strict and the initval is NULL, make sure input
		 * type and transtype are the same (or at least binary-compatible), so
		 * that it's OK to use the first input value as the initial
		 * transValue.	This should have been checked at agg definition time,
		 * but just in case...
		 */
		if (peraggstate->transfn.fn_strict && peraggstate->initValueIsNull)
		{
			if (numArguments < 1 ||
				!IsBinaryCoercible(inputTypes[0], aggtranstype))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("aggregate %u needs to have compatible input type and transition type",
								aggref->aggfnoid)));
		}

		/*
		 * Get a tupledesc corresponding to the inputs (including sort
		 * expressions) of the agg.
		 */
		peraggstate->evaldesc = ExecTypeFromTL(inputTargets, false);
		
		/* Create slot we're going to do argument evaluation in */
		peraggstate->evalslot = ExecInitExtraTupleSlot(estate);
		ExecSetSlotDescriptor(peraggstate->evalslot, peraggstate->evaldesc);

		/* Set up projection info for evaluation */
		peraggstate->evalproj = ExecBuildProjectionInfo(aggrefstate->args,
														aggstate->tmpcontext,
														peraggstate->evalslot,
														NULL);
		
		/*
		 * If we're doing either DISTINCT or ORDER BY, then we have a list of
		 * SortGroupClause nodes; fish out the data in them and stick them
		 * into arrays.
		 *
		 * Note that by construction, if there is a DISTINCT clause then the
		 * ORDER BY clause is a prefix of it (see transformDistinctClause).
		 */
		if (aggref->aggdistinct)
		{
			TargetEntry *tle;
			SortClause *sc;
			Oid			eq_function;

			/*
			 * GPDB 4 doesh't implement DISTINCT aggs for aggs having more than
			 * than one argument, nor does it allow an ordered aggregate to
			 * specify distinct, but PG 9 does.  The SQL standard allows the
			 * one-arg-for-DISTINCT restriction, but we really we ought to 
			 * implement it the way PG 9 does eventually.
			 *
			 * For now we use the scalar equalfn field of AggStatePerAggData
			 * for DQAs instead of treating DQAs more generally. 
			 */
			if (numArguments != 1)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("DISTINCT is supported only for single-argument aggregates")));

			eq_function = equality_oper_funcid(inputTypes[0]);
			fmgr_info(eq_function, &(peraggstate->equalfn));
			
			tle = (TargetEntry*)linitial(inputTargets);
			tle->ressortgroupref = 1;
			
			sc = makeNode(SortClause);
			sc->tleSortGroupRef = tle->ressortgroupref;
			sc->sortop = ordering_oper_opid(inputTypes[0]);
			
			sortlist = list_make1(sc);
			numSortCols = 1;
		}
		else if ( aggref->aggorder )
		{
			sortlist = aggref->aggorder->sortClause;
			numSortCols = list_length(sortlist);
		}
		else 
		{
			sortlist = NULL;
			numSortCols = 0;
		}

		
		peraggstate->numSortCols = numSortCols;

		if (numSortCols > 0)
		{
			/*
			 * We don't implement DISTINCT or ORDER BY aggs in the HASHED case
			 * (yet)
			 */
			Assert(node->aggstrategy != AGG_HASHED);
			
			/* If we have only one input, we need its len/byval info. */
			if (numInputs == 1)
			{
				get_typlenbyval(inputTypes[0],
								&peraggstate->inputtypeLen,
								&peraggstate->inputtypeByVal);
			}
			
			/* Extract the sort information for use later */
			peraggstate->sortColIdx =
				(AttrNumber *) palloc(numSortCols * sizeof(AttrNumber));
			peraggstate->sortOperators =
				(Oid *) palloc(numSortCols * sizeof(Oid));
			
			i = 0;
			foreach(lc, sortlist)
			{
				SortClause *sortcl = (SortClause *) lfirst(lc);
				TargetEntry *tle = get_sortgroupclause_tle(sortcl,
														   inputTargets);
				
				/* the parser should have made sure of this */
				Assert(OidIsValid(sortcl->sortop));
				
				peraggstate->sortColIdx[i] = tle->resno;
				peraggstate->sortOperators[i] = sortcl->sortop;
				i++;
			}
			Assert(i == numSortCols);
		}

		if (aggref->aggdistinct)
		{
			Oid eqfunc;
			
			Assert(numArguments == 1);
			Assert(numSortCols == 1);
			
			/*
			 * We need the equal function for the DISTINCT comparison we will
			 * make.
			 */
			eqfunc = equality_oper_funcid(inputTypes[0]);
			fmgr_info(eqfunc, &peraggstate->equalfn);
		}
		
		caql_endscan(pcqCtx);
	}

	/*
	 * Process percentile expressions.  These are treated separately from
	 * Aggref expressions at the moment as we cannot change the catalog, but
	 * this will be incorporated into the existing Agggref architecture
	 * when we can change the catalog.  The operation for percentile functions
	 * is very similar to the Aggref operation except that there is no
	 * function oid for transition function.  We manually manupilate
	 * FmgrInfo without the oid.
	 * In case the Agg handles PercentileExpr, there shouldn't be Aggref
	 * in conjunction with PercentileExpr in the target list (and havingQual),
	 * or vice versa, from the current design of percentile functions.
	 * However, we don't assert anything to keep that assumption, for the
	 * later extensibility.
	 */
	foreach (l, aggstate->percs)
	{
		PercentileExprState *percstate = (PercentileExprState *) lfirst(l);
		PercentileExpr	   *perc = (PercentileExpr *) percstate->xprstate.expr;
		AggStatePerAgg		peraggstate;
		FmgrInfo		   *transfn;
		int					numArguments;
		int					i;
		Oid					trans_argtypes[FUNC_MAX_ARGS];
		ListCell		   *lc;
		Expr			   *dummy_expr;

		/* Look for a previous duplicate aggregate */
		for (i = 0; i <= aggno; i++)
		{
			/*
			 * In practice, percentile expression doesn't contain
			 * volatile functions since everything is evaluated and
			 * becomes Var during the preprocess such as ordering operations.
			 * However, adding a check for volatile may be robust and
			 * consistent with Aggref initialization.
			 */
			if (equal(perc, peragg[i].perc) &&
				!contain_volatile_functions((Node *) perc))
				break;
		}
		if (i <= aggno)
		{
			/* Found a match to an existing entry, so just mark it */
			percstate->aggno = i;
			continue;
		}

		/* Nope, so assign a new PerAgg record */
		peraggstate = &peragg[++aggno];

		/* Mark Aggref state node with assigned index in the result array */
		percstate->aggno = aggno;

		/* Fill in the peraggstate data */
		peraggstate->percstate = percstate;
		peraggstate->perc = perc;
		/*
		 * numArguments = arg + ORDER BY + pc + tc
		 * See notes on percentile_cont_trans() and ExecInitExpr() for
		 * PercentileExpr.
		 */
		numArguments = list_length(perc->args) + list_length(perc->sortClause) + 2;
		peraggstate->numArguments = numArguments;

		/*
		 * Set up transfn.  In general, we should use fmgr_info, but we don't
		 * have the catalog function (thus no oid for functions) due to the
		 * difficulity of changing the catalog at the moment.  This should
		 * be cleaned when we can change the catalog.
		 */
		transfn = &peraggstate->transfn;
		transfn->fn_nargs = list_length(perc->args) + 1;
		transfn->fn_strict = false;
		transfn->fn_retset = false;
		transfn->fn_mcxt = CurrentMemoryContext;
		transfn->fn_addr = perc->perckind == PERC_DISC ?
			percentile_disc_trans : percentile_cont_trans;
		transfn->fn_oid = InvalidOid;

		/*
		 * trans type is the same as result type, as they don't have final func.
		 */
		trans_argtypes[0] = perc->perctype;
		i = 1;
		/*
		 * Literal arguments.
		 */
		foreach (lc, perc->args)
		{
			Node	   *arg = lfirst(lc);
			trans_argtypes[i++] = exprType(arg);
		}
		/*
		 * ORDER BY arguments.
		 */
		foreach (lc, perc->sortTargets)
		{
			TargetEntry	   *tle = lfirst(lc);
			trans_argtypes[i++] = exprType((Node *) tle->expr);
		}
		/*
		 * Peer count and total count.
		 */
		trans_argtypes[i++] = INT8OID;
		trans_argtypes[i++] = INT8OID;
		/*
		 * Build FuncExpr for the transition function.
		 */
		build_aggregate_fnexprs(trans_argtypes,
								i,
								perc->perctype,
								perc->perctype,
								InvalidOid,
								InvalidOid,
								InvalidOid,
								InvalidOid,
								InvalidOid,
								(Expr **) &transfn->fn_expr,
								&dummy_expr, NULL, NULL, NULL);

		get_typlenbyval(perc->perctype,
						&peraggstate->resulttypeLen,
						&peraggstate->resulttypeByVal);
		get_typlenbyval(perc->perctype,
						&peraggstate->transtypeLen,
						&peraggstate->transtypeByVal);

		/*
		 * Hard code for the known information.
		 */
		peraggstate->initValueIsNull = true;
		peraggstate->initValue = (Datum) 0;

		peraggstate->finalfn_oid = InvalidOid;
		peraggstate->prelimfn_oid = InvalidOid;

		/*
		 * Get a tupledesc corresponding to the inputs (including sort
		 * expressions) of the agg.
		 */
		peraggstate->evaldesc = ExecTypeFromTL(percstate->tlist, false);

		/* Create slot we're going to do argument evaluation in */
		peraggstate->evalslot = ExecInitExtraTupleSlot(estate);
		ExecSetSlotDescriptor(peraggstate->evalslot, peraggstate->evaldesc);

		/* Set up projection info for evaluation */
		peraggstate->evalproj = ExecBuildProjectionInfo(percstate->args,
														aggstate->tmpcontext,
														peraggstate->evalslot,
														NULL);
	}

	/* Update numaggs to match number of unique aggregates found */
	aggstate->numaggs = aggno + 1;
	
	/* MPP */
	aggstate->hhashtable = NULL;

	/* ROLLUP */
	aggstate->perpassthru = NULL;

	if (node->inputHasGrouping)
	{
		AggStatePerGroup perpassthru;

		perpassthru = (AggStatePerGroup) palloc0(sizeof(AggStatePerGroupData) * numaggs);
		aggstate->perpassthru = perpassthru;

	}

	aggstate->num_attrs = 0;

	aggstate->aggType = getAggType(node);

	/* Set the default memory manager */
	aggstate->mem_manager.alloc = cxt_alloc;
	aggstate->mem_manager.free = cxt_free;
	aggstate->mem_manager.manager = aggstate->aggcontext;
	aggstate->mem_manager.realloc_ratio = 1;

	initGpmonPktForAgg((Plan *)node, &aggstate->ss.ps.gpmon_pkt, estate);
	
	return aggstate;
}

Datum
GetAggInitVal(Datum textInitVal, Oid transtype)
{
	Oid			typinput,
				typioparam;
	char	   *strInitVal;
	Datum		initVal;

	getTypeInputInfo(transtype, &typinput, &typioparam);
	strInitVal = DatumGetCString(DirectFunctionCall1(textout, textInitVal));
	initVal = OidInputFunctionCall(typinput, strInitVal,
								   typioparam, -1);
	pfree(strInitVal);
	return initVal;
}

/*
 * Standard API to count tuple table slots used by an execution 
 * instance of an Agg node.  
 *
 * GPDB precomputes tuple table size, but use of projection means
 * aggregates use a slot.  Since the count is needed earlier, we 
 * than the determination of then number of different aggregate
 * call that happens during initializaiton, we just count Aggref 
 * nodes.  This may be an over count (in case some aggregate
 * calls are duplicated), but shouldn't be too bad.
 */
int
ExecCountSlotsAgg(Agg *node)
{
	int nextraslots = 0;
	
	nextraslots += count_extra_agg_slots((Node*)node->plan.targetlist);
	nextraslots += count_extra_agg_slots((Node*)node->plan.qual);
	
	return ExecCountSlotsNode(outerPlan(node)) +
	ExecCountSlotsNode(innerPlan(node)) +
	nextraslots + /* may be high due to duplicate Aggref nodes. */
	AGG_NSLOTS;
}

void
ExecEndAgg(AggState *node)
{
	PlanState  *outerPlan;

	ExecEagerFreeAgg(node);

	/*
	 * Free both the expr contexts.
	 */
	ExecFreeExprContext(&node->ss.ps);
	node->ss.ps.ps_ExprContext = node->tmpcontext;
	ExecFreeExprContext(&node->ss.ps);

	/* clean up tuple table */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	if (node->num_attrs > 0)
	{
		pfree(node->replValues);
		pfree(node->replIsnull);
		pfree(node->doReplace);
	}

	MemoryContextDelete(node->aggcontext);

	outerPlan = outerPlanState(node);
	ExecEndNode(outerPlan);

	EndPlanStateGpmonPkt(&node->ss.ps);
}

void
ExecReScanAgg(AggState *node, ExprContext *exprCtxt)
{
	ExprContext *econtext = node->ss.ps.ps_ExprContext;

	ExecEagerFreeAgg(node);

	/*
	 * Release all temp storage. Note that with AGG_HASHED, the hash table
	 * is allocated in a sub-context of the aggcontext. We're going to
	 * rebuild the hash table from scratch, so we need to use
	 * MemoryContextResetAndDeleteChildren() to avoid leaking the old hash
	 * table's memory context header.
	 */
	MemoryContextResetAndDeleteChildren(node->aggcontext);

	/* Re-initialize some variables */
	node->agg_done = false;

	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/* Forget current agg values */
	MemSet(econtext->ecxt_aggvalues, 0, sizeof(Datum) * node->numaggs);
	MemSet(econtext->ecxt_aggnulls, 0, sizeof(bool) * node->numaggs);

	if (!IS_HASHAGG(node))
	{
		/*
		 * Reset the per-group state (in particular, mark transvalues null)
		 */
		MemSet(node->pergroup, 0,
			   sizeof(AggStatePerGroupData) * node->numaggs);
	}

	if (((Agg *) node->ss.ps.plan)->inputHasGrouping)
	{
		/*
		 * Reset the per-passthru state (in particular, mark transvalues null)
		 */
		MemSet(node->perpassthru, 0,
			   sizeof(AggStatePerGroupData) * node->numaggs);
	}	

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (((PlanState *) node)->lefttree->chgParam == NULL)
		ExecReScan(((PlanState *) node)->lefttree, exprCtxt);
}


/*
 * ExecAggExplainEnd
 *      Called before ExecutorEnd to finish EXPLAIN ANALYZE reporting.
 */
void
ExecAggExplainEnd(PlanState *planstate, struct StringInfoData *buf)
{
    AggState   *aggstate = (AggState *)planstate;

    /* Report executor memory used by our memory context. */
    planstate->instrument->execmemused += 
        (double)MemoryContextGetPeakSpace(aggstate->aggcontext);
}                               /* ExecAggExplainEnd */

/*
 * aggregate_dummy - dummy execution routine for aggregate functions
 *
 * This function is listed as the implementation (prosrc field) of pg_proc
 * entries for aggregate functions.  Its only purpose is to throw an error
 * if someone mistakenly executes such a function in the normal way.
 *
 * Perhaps someday we could assign real meaning to the prosrc field of
 * an aggregate?
 */
Datum
aggregate_dummy(PG_FUNCTION_ARGS)
{
	elog(ERROR, "aggregate function %u called as normal function",
		 fcinfo->flinfo->fn_oid);
	return (Datum) 0;			/* keep compiler quiet */
}

/* resolve actual type of transition state, if polymorphic */
Oid
resolve_polymorphic_transtype(Oid aggtranstype, Oid aggfnoid,
							  Oid *inputTypes)
{
	if (aggtranstype == ANYARRAYOID || aggtranstype == ANYELEMENTOID)
	{
		/* have to fetch the agg's declared input types... */
		Oid		   *declaredArgTypes;
		int			agg_nargs;

		(void) get_func_signature(aggfnoid, &declaredArgTypes, &agg_nargs);
		aggtranstype = enforce_generic_type_consistency(inputTypes,
														declaredArgTypes,
														agg_nargs,
														aggtranstype);
		pfree(declaredArgTypes);
	}
	return aggtranstype;
}

/*
 * tuple_grouping - return the GROUPING value for an input tuple.
 *
 * This is used for a ROLLUP.
 */
int64
tuple_grouping(TupleTableSlot *outerslot,
               int input_grouping,
               int grpingIdx)
{
	Datum grping_datum;
	int64 grping;
	bool isnull;

	/* Simple return 0 if input_grouping is 0. */
	if (input_grouping == 0)
		return 0;

	grping_datum = slot_getattr(outerslot, grpingIdx, &isnull);

	Assert(!isnull);

	grping = DatumGetInt64(grping_datum);

	return grping;
}

/*
 * get_grouping_groupid() -- return either grouping or group_id
 * as given in 'grping_attno'.
 */
uint64
get_grouping_groupid(TupleTableSlot *slot, int grping_attno)
{
	bool isnull;
	uint64 grouping;
	
	/* Obtain grouping or group_id from input */
	Datum grping_datum = slot_getattr(slot,
									  grping_attno,
									  &isnull);
	Assert(!isnull);
	grouping = DatumGetInt64(grping_datum);

	return grouping;
}

void
initGpmonPktForAgg(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, Agg));

	{
		PerfmonNodeType type = PMNT_Invalid;

		switch(((Agg*)planNode)->aggstrategy)
		{
			case AGG_PLAIN:
				type = PMNT_Aggregate;
				break;
			case AGG_SORTED:
				type = PMNT_GroupAggregate;
				break;
			case AGG_HASHED:
				type = PMNT_HashAggregate;
				break;
		}
			
		Assert(type != PMNT_Invalid);
		Assert(GPMON_AGG_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, type,
							 (int64)planNode->plan_rows,
							 NULL);
	}
}

/*
 * Combine the argument and sortkey expressions of an Aggref
 * node into a single target list (of TargetEntry*) and, if
 * needed, an associated sort key list (of SortClause*).  
 *
 * The explicit result is a  palloc'd target list incorporating 
 * the underlying expressions by reference.  (Everything but
 * the expressions is newly allocated.)
 *
 * The implicit result, if requested by passing a non-null
 * pointer in sort_clauses, is a palloc'd sort key list.
 */
List *
combineAggrefArgs(Aggref *aggref, List **sort_clauses)
{
 	ListCell *lc;
 	TargetEntry *tle;
	List *inputTargets = NIL;
	List *inputSorts = NIL;
	int i = 0;
	
	/* In GPDB, can't have it both ways. */
	Assert( !aggref->aggdistinct || aggref->aggorder == NULL ); 
	
	/* Target list for cataloged aggregate arguments. */
	foreach(lc, aggref->args)
	{
		TargetEntry *tle = makeNode(TargetEntry);
		tle->expr = (Expr*)lfirst(lc);
		tle->resno = ++i;
		inputTargets = lappend(inputTargets, tle);
	}
	
	if ( aggref->aggorder != NULL )
	{
		/* Add targets and sort clauses for call supplied ordering. */
		inputSorts = aggref->aggorder->sortClause;
		if ( sort_clauses != NULL )
			inputSorts = (List*)copyObject(inputSorts);
		
		foreach(lc, inputSorts)
		{
			SortClause *sc = (SortClause*)lfirst(lc);
			TargetEntry *newtle;
			
			tle = get_sortgroupclause_tle(sc, aggref->aggorder->sortTargets);
			
			/* XXX Is it worth looking for tle->expr in the tlist so far to avoid copy? */
			newtle = makeNode(TargetEntry);
			newtle->expr = tle->expr; /* by reference */
			newtle->resno = ++i;
			newtle->resname = tle->resname ? pstrdup(tle->resname) : NULL;
			newtle->ressortgroupref = tle->ressortgroupref;
			
			inputTargets = lappend(inputTargets, newtle);
		}
	}
	else if ( aggref->aggdistinct )
	{ 
		SortClause *sc;
		
		/* In GPDB, DISTINCT implies single argument. */
		Assert( list_length(inputTargets) == 1 );
		
		
		/* Add targets and sort clauses for implied DISTINCT ordering. */
		tle = (TargetEntry*)linitial(inputTargets);
		tle->ressortgroupref = 1;
		
		if ( sort_clauses != NULL )
		{
			sc = makeNode(SortClause);
			sc->tleSortGroupRef = tle->ressortgroupref;
			inputSorts = list_make1(sc);
		}
	}
	
	if ( sort_clauses != NULL )
		*sort_clauses = inputSorts;
	
	return inputTargets;
}

/*
 * Combine the argument and ordering expression with the peer count
 * and the total count, to create the TupleTableSlot for this
 * expression.  This is similar to ordered aggregate Aggref,
 * but the difference is that PercentileExpr will accept those
 * additional values as the arguments to the transition function.
 */
List *
combinePercentileArgs(PercentileExpr *p)
{
	List			   *tlist;
	ListCell		   *l;
	AttrNumber			resno;
	TargetEntry		   *tle;

	tlist = NIL;
	resno = 1;
	foreach (l, p->args)
	{
		Expr		   *arg = lfirst(l);

		tle = makeTargetEntry((Expr *) arg,
							  resno++,
							  NULL,
							  false);
		tlist = lappend(tlist, tle);
	}

	/*
	 * Extract ordering expressions from sortTargets.
	 */
	foreach (l, p->sortClause)
	{
		SortClause	   *sc = lfirst(l);
		TargetEntry	   *sc_tle;

		sc_tle = get_sortgroupclause_tle(sc, p->sortTargets);
		tle = flatCopyTargetEntry(sc_tle);
		tle->resno = resno++;

		tlist = lappend(tlist, tle);
	}

	/*
	 * peer count expresssion.
	 */
	Assert(p->pcExpr);
	tle = makeTargetEntry((Expr *) p->pcExpr,
						  resno++,
						  "peer_count",
						  false);
	tlist = lappend(tlist, tle);

	/*
	 * total count expresssion.
	 */
	Assert(p->tcExpr);
	tle = makeTargetEntry((Expr *) p->tcExpr,
						  resno++,
						  "total_count",
						  false);
	tlist = lappend(tlist, tle);

	return tlist;
}

/*
 * Subroutines for ExecCountSlotsAgg.
 */
int
count_extra_agg_slots(Node *node)
{
	int count = 0;
	
	count_extra_agg_slots_walker(node, &count);
	return count;
}

bool 
count_extra_agg_slots_walker(Node *node, int *count)
{
	if (node == NULL)
 		return false;
	
	if (IsA(node, Aggref))
	{
		(*count)++;
	}
	else if (IsA(node, PercentileExpr))
	{
		(*count)++;
	}
	
	return expression_tree_walker(node, count_extra_agg_slots_walker, (void *) count);
}

void
ExecEagerFreeAgg(AggState *node)
{
	/* Close any open tuplesorts */
	for (int aggno = 0; aggno < node->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &node->peragg[aggno];

		if (!peraggstate->sortstate)
		{
			continue;
		}
		
		if (gp_enable_mk_sort)
		{
			tuplesort_end_mk((Tuplesortstate_mk *) peraggstate->sortstate);
		}
		
		else
		{
			tuplesort_end((Tuplesortstate *) peraggstate->sortstate);
		}

		peraggstate->sortstate = NULL;
	}
	
	if (IS_HASHAGG(node))
	{
		destroy_agg_hash_table(node);

		/**
		 * Clean out the tuple descriptor.
		 */
		if (node->hashslot
				&& node->hashslot->tts_tupleDescriptor)
		{
			ReleaseTupleDesc(node->hashslot->tts_tupleDescriptor);
			node->hashslot->tts_tupleDescriptor = NULL;
		}
	}

	/* Release first tuple of group, if we have made a copy. */
	if (node->grp_firstTuple != NULL)
	{
		pfree(node->grp_firstTuple);
		node->grp_firstTuple = NULL;
	}
}
