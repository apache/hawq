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
 * nodeHashjoin.c
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/nodeHashjoin.c,v 1.85.2.1 2007/02/02 00:07:28 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/instrument.h"        /* Instrumentation */
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "utils/faultinjector.h"
#include "utils/memutils.h"

#include "cdb/cdbvars.h"
#include "miscadmin.h" /* work_mem */

#define EMPTY_WORKFILE_NAME "empty_workfile"

static TupleTableSlot *ExecHashJoinOuterGetTuple(PlanState *outerNode,
						  HashJoinState *hjstate,
						  uint32 *hashvalue);
static TupleTableSlot *ExecHashJoinGetSavedTuple(HashJoinBatchSide *side,
						  uint32 *hashvalue,
						  TupleTableSlot *tupleSlot);
static int	ExecHashJoinNewBatch(HashJoinState *hjstate);
static bool isNotDistinctJoin(List *qualList);
static bool ExecHashJoinLoadBatchFiles(HashJoinTable hashtable);



static void ReleaseHashTable(HashJoinState *node);
static bool isHashtableEmpty(HashJoinTable hashtable);
static void ExecHashJoinResetWorkfileState(HashJoinState *node);
static void ExecHashJoinSaveState(HashJoinTable hashtable);


/* ----------------------------------------------------------------
 *		ExecHashJoin
 *
 *		This function implements the Hybrid Hashjoin algorithm.
 *
 *		Note: the relation we build hash table on is the "inner"
 *			  the other one is "outer".
 * ----------------------------------------------------------------
 */
TupleTableSlot *				/* return: a tuple or NULL */
ExecHashJoin(HashJoinState *node)
{
	EState	   *estate;
	PlanState  *outerNode;
	HashState  *hashNode;
	List	   *joinqual;
	List	   *otherqual;
	TupleTableSlot *inntuple;
	ExprContext *econtext;
	HashJoinTable hashtable;
	HashJoinTuple curtuple;
	TupleTableSlot *outerTupleSlot;
	uint32		hashvalue;
	int			batchno;

	/*
	 * get information from HashJoin node
	 */
	estate = node->js.ps.state;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	hashNode = (HashState *) innerPlanState(node);
	outerNode = outerPlanState(node);

	/*
	 * get information from HashJoin state
	 */
	hashtable = node->hj_HashTable;
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * If we're doing an IN join, we want to return at most one row per outer
	 * tuple; so we can stop scanning the inner scan if we matched on the
	 * previous try.
	 */
	if (node->js.jointype == JOIN_IN && node->hj_MatchedOuter)
		node->hj_NeedNewOuter = true;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.  Note this can't happen
	 * until we're done projecting out tuples from a join tuple.
	 */
	ResetExprContext(econtext);

	/*
	 * if this is the first call, build the hash table for inner relation
	 */
	if (hashtable == NULL)
	{
		/*
		 * MPP-4165: My fix for MPP-3300 was correct in that we avoided
		 * the *deadlock* but had very unexpected (and painful)
		 * performance characteristics: we basically de-pipeline and
		 * de-parallelize execution of any query which has motion below
		 * us.
		 *
		 * So now prefetch_inner is set (see createplan.c) if we have *any* motion
		 * below us. If we don't have any motion, it doesn't matter.
		 */
		if (!node->prefetch_inner)
		{
			/*
			 * If the outer relation is completely empty, we can quit without
			 * building the hash table.  However, for an inner join it is only a
			 * win to check this when the outer relation's startup cost is less
			 * than the projected cost of building the hash table.	Otherwise it's
			 * best to build the hash table first and see if the inner relation is
			 * empty.  (When it's an outer join, we should always make this check,
			 * since we aren't going to be able to skip the join on the strength
			 * of an empty inner relation anyway.)
			 *
			 * If we are rescanning the join, we make use of information gained on
			 * the previous scan: don't bother to try the prefetch if the previous
			 * scan found the outer relation nonempty.	This is not 100% reliable
			 * since with new parameters the outer relation might yield different
			 * results, but it's a good heuristic.
			 *
			 * The only way to make the check is to try to fetch a tuple from the
			 * outer plan node.  If we succeed, we have to stash it away for later
			 * consumption by ExecHashJoinOuterGetTuple.
			 */
			if ((node->js.jointype == JOIN_LEFT) ||
					(node->js.jointype == JOIN_LASJ) ||
					(node->js.jointype == JOIN_LASJ_NOTIN) ||
					(outerNode->plan->startup_cost < hashNode->ps.plan->total_cost &&
						!node->hj_OuterNotEmpty))
			{
				TupleTableSlot *slot;

                slot = ExecProcNode(outerNode);

				node->hj_FirstOuterTupleSlot = slot;
				if (TupIsNull(node->hj_FirstOuterTupleSlot))
				{
					node->hj_OuterNotEmpty = false;

					/* CDB: Tell inner subtree that its data will not be needed. */
					ExecSquelchNode((PlanState *)hashNode);

					return NULL;
				}
				else
					node->hj_OuterNotEmpty = true;
			}
			else
				node->hj_FirstOuterTupleSlot = NULL;
		}
		else
		{
			/* see MPP-989 comment above, for now we assume that we have
			 * at least one row on the outer. */
			node->hj_FirstOuterTupleSlot = NULL;
		}


		workfile_set *work_set = NULL;

		if (gp_workfile_caching)
		{
			Assert(!node->cached_workfiles_batches_buckets_loaded);
			Assert(!node->cached_workfiles_loaded);

			/* Look for cached workfiles. Mark here if found */
			work_set = workfile_mgr_find_set(&node->js.ps);
			if (work_set != NULL)
			{
				elog(gp_workfile_caching_loglevel, "HashJoin found matching existing spill file set");
				node->cached_workfiles_found = true;
			}
		}

		/*
		 * create the hash table
		 */
		hashtable = ExecHashTableCreate(hashNode,
										node,
										node->hj_HashOperators,
										PlanStateOperatorMemKB((PlanState *) hashNode),
										work_set);
		node->hj_HashTable = hashtable;

        /*
         * CDB: Offer extra info for EXPLAIN ANALYZE.
         */
        if (estate->es_instrument)
            ExecHashTableExplainInit(hashNode, node, hashtable);


		/*
		 * execute the Hash node, to build the hash table
		 */
		hashNode->hashtable = hashtable;

		/*
		 * Only if doing a LASJ_NOTIN join, we want to quit as soon as we find
		 * a NULL key on the inner side
		 */
		hashNode->hs_quit_if_hashkeys_null = (node->js.jointype == JOIN_LASJ_NOTIN);

		/* Store pointer to the HashJoinState in the hashtable, as we will need
		 * the HashJoin plan when creating the spill file set */
		hashtable->hjstate = node;

		/* Workfile caching: If possible, load the hashtable state
		 * from cached workfiles first.
		 */
		if (gp_workfile_caching && node->cached_workfiles_found)
		{
			Assert(node->cached_workfiles_batches_buckets_loaded);
			Assert(!node->cached_workfiles_loaded);
			Assert(hashtable->work_set != NULL);
			elog(gp_workfile_caching_loglevel, "In ExecHashJoin, loading hashtable from cached workfiles");

			ExecHashJoinLoadBatchFiles(hashtable);
			node->cached_workfiles_loaded = true;

			elog(gp_workfile_caching_loglevel, "HashJoin reusing cached workfiles, initiating Squelch walker on inner and outer subplans");
			ExecSquelchNode(outerNode);
			ExecSquelchNode((PlanState *)hashNode);

			if (node->js.ps.instrument)
			{
				node->js.ps.instrument->workfileReused = true;
			}

			/* Open the first batch and build hashtable from it. */
			hashtable->curbatch = -1;
			ExecHashJoinNewBatch(node);
#ifdef HJDEBUG
		elog(gp_workfile_caching_loglevel, "HashJoin built table with %.1f tuples by loading from disk for batch 0",
				hashtable->totalTuples);
#endif

		}
		else
		{
			/* No cached workfiles found. Execute the Hash node and build the hashtable */

			(void) MultiExecProcNode((PlanState *) hashNode);

#ifdef HJDEBUG
		elog(gp_workfile_caching_loglevel, "HashJoin built table with %.1f tuples by executing subplan for batch 0", hashtable->totalTuples);
#endif
		}

		/**
		 * If LASJ_NOTIN and a null was found on the inner side, then clean out.
		 */
		if (node->js.jointype == JOIN_LASJ_NOTIN && hashNode->hs_hashkeys_null)
		{
			/*
			 * CDB: We'll read no more from outer subtree. To keep sibling
			 * QEs from being starved, tell source QEs not to clog up the
			 * pipeline with our never-to-be-consumed data.
			 */
			ExecSquelchNode(outerNode);
			/* end of join */
			if (gp_eager_hashtable_release)
			{
				ExecEagerFreeHashJoin(node);
			}
			return NULL;
		}

		/*
		 * We just scanned the entire inner side and built the hashtable
		 * (and its overflow batches). Check here and remember if the inner
		 * side is empty.
		 */
		node->hj_InnerEmpty = isHashtableEmpty(hashtable);

		/*
		 * If the inner relation is completely empty, and we're not doing an
		 * outer join, we can quit without scanning the outer relation.
		 */
		if (node->js.jointype != JOIN_LEFT
				&& node->js.jointype != JOIN_LASJ
				&& node->js.jointype != JOIN_LASJ_NOTIN
				&& node->hj_InnerEmpty)
        {
			/*
			 * CDB: We'll read no more from outer subtree. To keep sibling
			 * QEs from being starved, tell source QEs not to clog up the
			 * pipeline with our never-to-be-consumed data.
			 */
			ExecSquelchNode(outerNode);
			/* end of join */
			if (gp_eager_hashtable_release)
			{
				ExecEagerFreeHashJoin(node);
			}
			return NULL;
        }

		/*
		 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a tuple
		 * above, because ExecHashJoinOuterGetTuple will immediately set it
		 * again.)
		 */
		node->hj_OuterNotEmpty = false;

	} /* if (hashtable == NULL) */

	/*
	 * run the hash join process
	 */
	for (;;)
	{
		/* We must never use an eagerly released hash table */
		Assert(!hashtable->eagerlyReleased);
		
		/*
		 * If we don't have an outer tuple, get the next one
		 */
		if (node->hj_NeedNewOuter)
		{
			outerTupleSlot = ExecHashJoinOuterGetTuple(outerNode,
													   node,
													   &hashvalue);
			if (TupIsNull(outerTupleSlot))
			{
				/* end of join */
				if (gp_eager_hashtable_release)
				{
					ExecEagerFreeHashJoin(node);
				}
				return NULL;
			}

			Gpmon_M_Incr(GpmonPktFromHashJoinState(node), GPMON_QEXEC_M_ROWSIN); 
			CheckSendPlanStateGpmonPkt(&node->js.ps);
			node->js.ps.ps_OuterTupleSlot = outerTupleSlot;
			econtext->ecxt_outertuple = outerTupleSlot;
			node->hj_NeedNewOuter = false;
			node->hj_MatchedOuter = false;

			/*
			 * now we have an outer tuple, find the corresponding bucket for
			 * this tuple from the hash table
			 */
			node->hj_CurHashValue = hashvalue;
			ExecHashGetBucketAndBatch(hashtable, hashvalue,
									  &node->hj_CurBucketNo, &batchno);
			node->hj_CurTuple = NULL;

			/*
			 * Save outer tuples for the batch 0 to disk if workfile caching is
			 * enabled. We do this only when there is spilling.
			 */
			if (gp_workfile_caching && batchno == 0 && hashtable->nbatch > 1 && !node->cached_workfiles_loaded)
			{
				Assert(batchno >= node->nbatch_loaded_state);
				ExecHashJoinSaveTuple(&node->js.ps, ExecFetchSlotMemTuple(outerTupleSlot, false),
									  hashvalue,
									  hashtable,
                                      &hashtable->batches[batchno]->outerside,
									  hashtable->bfCxt);
			}

			/*
			 * Now we've got an outer tuple and the corresponding hash bucket,
			 * but this tuple may not belong to the current batch.
			 */
			if (batchno != hashtable->curbatch && !node->cached_workfiles_found)
			{
				/*
				 * Need to postpone this outer tuple to a later batch. Save it
				 * in the corresponding outer-batch file.
				 */
				Assert(batchno != 0);
				Assert(batchno > hashtable->curbatch);
				Assert(batchno >= node->nbatch_loaded_state);
				ExecHashJoinSaveTuple(&node->js.ps, ExecFetchSlotMemTuple(outerTupleSlot, false),
									  hashvalue,
									  hashtable,
                                      &hashtable->batches[batchno]->outerside,
									  hashtable->bfCxt);
				node->hj_NeedNewOuter = true;
				continue;		/* loop around for a new outer tuple */
			}
		}  /* if (node->hj_NeedNewOuter) */

		/*
		 * OK, scan the selected hash bucket for matches
		 */
		for (;;)
		{
			/*
			 * OPT-3325: Handle NULLs in the outer side of LASJ_NOTIN
			 *  - if tuple is NULL and inner is not empty, drop outer tuple
			 *  - if tuple is NULL and inner is empty, keep going as we'll
			 *    find no match for this tuple in the inner side
			 */
			if (node->js.jointype == JOIN_LASJ_NOTIN &&
					!node->hj_InnerEmpty &&
					isJoinExprNull(node->hj_OuterHashKeys,econtext))
			{
				node->hj_MatchedOuter = true;
				break;		/* loop around for a new outer tuple */
			}

			curtuple = ExecScanHashBucket(hashNode, node, econtext);
			if (curtuple == NULL)
				break;			/* out of matches */

			/*
			 * we've got a match, but still need to test non-hashed quals
			 */
			inntuple = ExecStoreMemTuple(HJTUPLE_MINTUPLE(curtuple),
										 node->hj_HashTupleSlot,
										 false);	/* don't pfree */
			econtext->ecxt_innertuple = inntuple;

			/* reset temp memory each time to avoid leaks from qual expr */
			ResetExprContext(econtext);

			/*
			 * if we pass the qual, then save state for next call and have
			 * ExecProject form the projection, store it in the tuple table,
			 * and return the slot.
			 *
			 * Only the joinquals determine MatchedOuter status, but all quals
			 * must pass to actually return the tuple.
			 */
			if (joinqual == NIL || ExecQual(joinqual, econtext, false /* resultForNull */))
			{
				node->hj_MatchedOuter = true;

				/* In an antijoin, we never return a matched tuple */
				if (node->js.jointype == JOIN_LASJ || node->js.jointype == JOIN_LASJ_NOTIN)
				{
					node->hj_NeedNewOuter = true;
					break;		/* out of loop over hash bucket */
				}

				if (otherqual == NIL || ExecQual(otherqual, econtext, false))
				{
					Gpmon_M_Incr_Rows_Out(GpmonPktFromHashJoinState(node));
					CheckSendPlanStateGpmonPkt(&node->js.ps);
					return ExecProject(node->js.ps.ps_ProjInfo, NULL);
				}

				/*
				 * If we didn't return a tuple, may need to set NeedNewOuter
				 */
				if (node->js.jointype == JOIN_IN)
				{
					node->hj_NeedNewOuter = true;
					break;		/* out of loop over hash bucket */
				}
			}
		}

		/*
		 * Now the current outer tuple has run out of matches, so check
		 * whether to emit a dummy outer-join tuple. If not, loop around to
		 * get a new outer tuple.
		 */
		node->hj_NeedNewOuter = true;

		if (!node->hj_MatchedOuter &&
			(node->js.jointype == JOIN_LEFT ||
			node->js.jointype == JOIN_LASJ ||
			node->js.jointype == JOIN_LASJ_NOTIN))
		{
			/*
			 * We are doing an outer join and there were no join matches for
			 * this outer tuple.  Generate a fake join tuple with nulls for
			 * the inner tuple, and return it if it passes the non-join quals.
			 */
			econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;

			if (otherqual == NIL || ExecQual(otherqual, econtext, false))
			{
				Gpmon_M_Incr_Rows_Out(GpmonPktFromHashJoinState(node));
				CheckSendPlanStateGpmonPkt(&node->js.ps);
				return ExecProject(node->js.ps.ps_ProjInfo, NULL);
			}
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState *
ExecInitHashJoin(HashJoin *node, EState *estate, int eflags)
{
	HashJoinState *hjstate;
	Plan	   *outerNode;
	Hash	   *hashNode;
	List	   *lclauses;
	List	   *rclauses;
	List	   *hoperators;
	ListCell   *l;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hjstate = makeNode(HashJoinState);
	hjstate->js.ps.plan = (Plan *) node;
	hjstate->js.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hjstate->js.ps);

	if ( node->hashqualclauses != NIL )
	{
		/* CDB: This must be an IS NOT DISTINCT join!  */
		Insist( isNotDistinctJoin(node->hashqualclauses) );
		hjstate->hj_nonequijoin = true;
	}
	else
		hjstate->hj_nonequijoin = false;

	/*
	 * initialize child expressions
	 */
	hjstate->js.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->join.plan.targetlist,
					 (PlanState *) hjstate);
	hjstate->js.ps.qual = (List *)
		ExecInitExpr((Expr *) node->join.plan.qual,
					 (PlanState *) hjstate);
	hjstate->js.jointype = node->join.jointype;
	hjstate->js.joinqual = (List *)
		ExecInitExpr((Expr *) node->join.joinqual,
					 (PlanState *) hjstate);
	hjstate->hashclauses = (List *)
		ExecInitExpr((Expr *) node->hashclauses,
					 (PlanState *) hjstate);
	
	if ( node->hashqualclauses != NIL )
	{
		hjstate->hashqualclauses = (List *)
			ExecInitExpr((Expr *) node->hashqualclauses,
						 (PlanState *) hjstate);
	}
	else
	{
		hjstate->hashqualclauses = hjstate->hashclauses;
	}	

	/* MPP-3300, we only pre-build hashtable if we need to (this is
	 * relaxing the fix to MPP-989) */
	hjstate->prefetch_inner = node->join.prefetch_inner;

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */
	hashNode = (Hash *) innerPlan(node);
	outerNode = outerPlan(node);

	/* 
	 * XXX The following order are significant.  We init Hash first, then the outerNode
	 * this is the same order as we execute (in the sense of the first exec called).
	 * Until we have a better way to uncouple, share input needs this to be true.  If the
	 * order is wrong, when both hash and outer node have share input and (both ?) have 
	 * a subquery node, share input will fail because the estate of the nodes can not be
	 * set up correctly.
	 */
	innerPlanState(hjstate) = ExecInitNode((Plan *) hashNode, estate, eflags);
	((HashState *) innerPlanState(hjstate))->hs_keepnull = hjstate->hj_nonequijoin;

	outerPlanState(hjstate) = ExecInitNode(outerNode, estate, eflags);

#define HASHJOIN_NSLOTS 3

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &hjstate->js.ps);
	hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate);

	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_IN:
			break;
		case JOIN_LEFT:
		case JOIN_LASJ:
		case JOIN_LASJ_NOTIN:
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(innerPlanState(hjstate)));
			break;
		default:
			elog(LOG, "unrecognized join type: %d",
				 (int) node->join.jointype);
			Assert(false);
	}

	/*
	 * now for some voodoo.  our temporary tuple slot is actually the result
	 * tuple slot of the Hash node (which is our inner plan).  we do this
	 * because Hash nodes don't return tuples via ExecProcNode() -- instead
	 * the hash join node uses ExecScanHashBucket() to get at the contents of
	 * the hash table.	-cim 6/9/91
	 */
	{
		HashState  *hashstate = (HashState *) innerPlanState(hjstate);
		TupleTableSlot *slot = hashstate->ps.ps_ResultTupleSlot;

		hjstate->hj_HashTupleSlot = slot;
	}

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&hjstate->js.ps);
	ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

	ExecSetSlotDescriptor(hjstate->hj_OuterTupleSlot,
						  ExecGetResultType(outerPlanState(hjstate)));

	/*
	 * initialize hash-specific info
	 */
	hjstate->hj_HashTable = NULL;
	hjstate->hj_FirstOuterTupleSlot = NULL;

	hjstate->hj_CurHashValue = 0;
	hjstate->hj_CurBucketNo = 0;
	hjstate->hj_CurTuple = NULL;

	/*
	 * Deconstruct the hash clauses into outer and inner argument values, so
	 * that we can evaluate those subexpressions separately.  Also make a list
	 * of the hash operator OIDs, in preparation for looking up the hash
	 * functions to use.
	 */
	lclauses = NIL;
	rclauses = NIL;
	hoperators = NIL;
	foreach(l, hjstate->hashclauses)
	{
		FuncExprState *fstate = (FuncExprState *) lfirst(l);
		OpExpr	   *hclause;

		Assert(IsA(fstate, FuncExprState));
		hclause = (OpExpr *) fstate->xprstate.expr;
		Assert(IsA(hclause, OpExpr));
		lclauses = lappend(lclauses, linitial(fstate->args));
		rclauses = lappend(rclauses, lsecond(fstate->args));
		hoperators = lappend_oid(hoperators, hclause->opno);
	}
	hjstate->hj_OuterHashKeys = lclauses;
	hjstate->hj_InnerHashKeys = rclauses;
	hjstate->hj_HashOperators = hoperators;
	/* child Hash node needs to evaluate inner hash keys, too */
	((HashState *) innerPlanState(hjstate))->hashkeys = rclauses;

	hjstate->js.ps.ps_OuterTupleSlot = NULL;
	hjstate->hj_NeedNewOuter = true;
	hjstate->hj_MatchedOuter = false;
	hjstate->hj_OuterNotEmpty = false;

	ExecHashJoinResetWorkfileState(hjstate);

	initGpmonPktForHashJoin((Plan *)node, &hjstate->js.ps.gpmon_pkt, estate);
	
	return hjstate;
}

int
ExecCountSlotsHashJoin(HashJoin *node)
{
	return ExecCountSlotsNode(outerPlan(node)) +
		ExecCountSlotsNode(innerPlan(node)) +
		HASHJOIN_NSLOTS;
}

/* ----------------------------------------------------------------
 *		ExecEndHashJoin
 *
 *		clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
void
ExecEndHashJoin(HashJoinState *node)
{
	/*
	 * Free hash table
	 */
	if (node->hj_HashTable)
	{
		if (!node->hj_HashTable->eagerlyReleased)
		{
			HashState *hashState = (HashState *) innerPlanState(node);
			ExecHashTableDestroy(hashState, node->hj_HashTable);
		}
		pfree(node->hj_HashTable);
		node->hj_HashTable = NULL;
	}

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->hj_OuterTupleSlot);
	ExecClearTuple(node->hj_HashTupleSlot);

	/*
	 * clean up subtrees
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));

	EndPlanStateGpmonPkt(&node->js.ps);
}

/*
 * ExecHashJoinOuterGetTuple
 *
 *		get the next outer tuple for hashjoin: either by
 *		executing a plan node in the first pass, or from
 *		the temp files for the hashjoin batches.
 *
 * Returns a null slot if no more outer tuples.  On success, the tuple's
 * hash value is stored at *hashvalue --- this is either originally computed,
 * or re-read from the temp file.
 */
static TupleTableSlot *
ExecHashJoinOuterGetTuple(PlanState *outerNode,
		HashJoinState *hjstate,
		uint32 *hashvalue)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			curbatch = hashtable->curbatch;
	TupleTableSlot *slot;
	ExprContext    *econtext;

	HashState *hashState = (HashState *) innerPlanState(hjstate);

	/* Read tuples from outer relation only if it's the first batch
	 * and we're not loading from cached workfiles.  */
	if (curbatch == 0 && !hjstate->cached_workfiles_loaded)
	{
		for (;;)
		{
			/*
			 * Check to see if first outer tuple was already fetched by
			 * ExecHashJoin() and not used yet.
			 */
			slot = hjstate->hj_FirstOuterTupleSlot;
			if (!TupIsNull(slot))
				hjstate->hj_FirstOuterTupleSlot = NULL;
			else
			{
                slot = ExecProcNode(outerNode);
			}

			if (TupIsNull(slot))
				break;

			/*
			 * We have to compute the tuple's hash value.
			 */
			econtext = hjstate->js.ps.ps_ExprContext;
			econtext->ecxt_outertuple = slot;

			bool hashkeys_null = false;
			bool keep_nulls = (hjstate->js.jointype == JOIN_LEFT) ||
					(hjstate->js.jointype == JOIN_LASJ) ||
					(hjstate->js.jointype == JOIN_LASJ_NOTIN) ||
					hjstate->hj_nonequijoin;
			if (ExecHashGetHashValue(hashState, hashtable, econtext,
						hjstate->hj_OuterHashKeys,
						keep_nulls,
						hashvalue,
						&hashkeys_null
						))
			{
				/* remember outer relation is not empty for possible rescan */
				hjstate->hj_OuterNotEmpty = true;

				return slot;
			}
			/*
			 * That tuple couldn't match because of a NULL, so discard it
			 * and continue with the next one.
			 */
		} /* for (;;) */

		/*
		 * We have just reached the end of the first pass. Write out the first
		 * inner batch so that we can reuse it when the workfile caching is
		 * enabled.
		 */
		if (gp_workfile_caching) 
		  {
		    ExecHashJoinSaveFirstInnerBatch(hashtable);
		  }

		/*
		 * We have just reached the end of the first pass. Try to switch to a
		 * saved batch.
		 */

		/* SFR: This can cause re-spill! */
		curbatch = ExecHashJoinNewBatch(hjstate);


#ifdef HJDEBUG
		elog(gp_workfile_caching_loglevel, "HashJoin built table with %.1f tuples for batch %d", hashtable->totalTuples, curbatch);
#endif

		Gpmon_M_Incr_Rows_Out(GpmonPktFromHashJoinState(hjstate)); 
		CheckSendPlanStateGpmonPkt(&hjstate->js.ps);
	} /* if (curbatch == 0) */

	/*
	 * Try to read from a temp file. Loop allows us to advance to new batches
	 * as needed.  NOTE: nbatch could increase inside ExecHashJoinNewBatch, so
	 * don't try to optimize this loop.
	 */
	while (curbatch < hashtable->nbatch)
	{
		slot = ExecHashJoinGetSavedTuple(&hashtable->batches[curbatch]->outerside,
										 hashvalue,
										 hjstate->hj_OuterTupleSlot);
		if (!TupIsNull(slot))
			return slot;
		curbatch = ExecHashJoinNewBatch(hjstate);

#ifdef HJDEBUG
		elog(gp_workfile_caching_loglevel, "HashJoin built table with %.1f tuples for batch %d", hashtable->totalTuples, curbatch);
#endif

		Gpmon_M_Incr(GpmonPktFromHashJoinState(hjstate), GPMON_HASHJOIN_SPILLBATCH);
		CheckSendPlanStateGpmonPkt(&hjstate->js.ps);
	}

	/* Write spill file state to disk. */
	ExecHashJoinSaveState(hashtable);

	if (gp_workfile_caching && hjstate->workfiles_created)
	{
		workfile_mgr_mark_complete(hashtable->work_set);
	}

	/* Out of batches... */
	return NULL;
}

/*
 * ExecHashJoinNewBatch
 *		switch to a new hashjoin batch
 *
 * Returns the number of the new batch (1..nbatch-1), or nbatch if no more.
 * We will never return a batch number that has an empty outer batch file.
 */
static int
ExecHashJoinNewBatch(HashJoinState *hjstate)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
    HashJoinBatchData  *batch;
	int			nbatch;
	int			curbatch;
	TupleTableSlot *slot;
	uint32		hashvalue;

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
			FaultExecHashJoinNewBatch,
			DDLNotSpecified,
			"",  // databaseName
			""); // tableName
#endif


	HashState *hashState = (HashState *) innerPlanState(hjstate);

start_over:
	nbatch = hashtable->nbatch;
	curbatch = hashtable->curbatch;

    if (curbatch >= nbatch)
        return nbatch;

    if (curbatch >= 0 && hashtable->stats)
        ExecHashTableExplainBatchEnd(hashState, hashtable);

	if (curbatch > 0)
	{
		/*
		 * We no longer need the previous outer batch file; close it right
		 * away to free disk space.
		 *
		 * However, if workfile caching is enabled, and this is the first
		 * time to create cached workfiles, we can not close the batch file
		 * here, since we need to save the workfile names at the end.
		 */
		if (!(gp_workfile_caching &&
			  !hjstate->cached_workfiles_found))
		{
			batch = hashtable->batches[curbatch];
			if (batch->outerside.workfile != NULL)
			{
				workfile_mgr_close_file(hashtable->work_set, batch->outerside.workfile);
			}
			batch->outerside.workfile = NULL;
		}
	}

	/*
	 * We can always skip over any batches that are completely empty on both
	 * sides.  We can sometimes skip over batches that are empty on only one
	 * side, but there are exceptions:
	 *
	 * 1. In a LEFT JOIN, we have to process outer batches even if the inner
	 * batch is empty.
	 *
	 * 2. If we have increased nbatch since the initial estimate, we have to
	 * scan inner batches since they might contain tuples that need to be
	 * reassigned to later inner batches.
	 *
	 * 3. Similarly, if we have increased nbatch since starting the outer
	 * scan, we have to rescan outer batches in case they contain tuples that
	 * need to be reassigned.
	 */
	curbatch++;
	while (curbatch < nbatch &&
		   (hashtable->batches[curbatch]->outerside.workfile == NULL ||
			hashtable->batches[curbatch]->innerside.workfile == NULL))

	{
        batch = hashtable->batches[curbatch];
		if (batch->outerside.workfile != NULL &&
			((hjstate->js.jointype == JOIN_LEFT) || 
			 (hjstate->js.jointype == JOIN_LASJ) ||
			 (hjstate->js.jointype == JOIN_LASJ_NOTIN)))
			break;				/* must process due to rule 1 */
		if (batch->innerside.workfile != NULL &&
			nbatch != hashtable->nbatch_original)
			break;				/* must process due to rule 2 */
		if (batch->outerside.workfile != NULL &&
			nbatch != hashtable->nbatch_outstart)
			break;				/* must process due to rule 3 */
		/* We can ignore this batch. */
		/* Release associated temp files right away. */
		if (batch->innerside.workfile != NULL)
		{
			workfile_mgr_close_file(hashtable->work_set, batch->innerside.workfile);
		}
		batch->innerside.workfile = NULL;
		
		if (batch->outerside.workfile != NULL)
		{
			workfile_mgr_close_file(hashtable->work_set, batch->outerside.workfile);
		}
		batch->outerside.workfile = NULL;

		curbatch++;
	}

    hashtable->curbatch = curbatch;     /* CDB: upd before return, even if no
                                         * more data, so stats logic can see
                                         * whether join was run to completion
                                         */

    if (curbatch >= nbatch)
	    return curbatch;		/* no more batches */

    batch = hashtable->batches[curbatch];

    /*
     * Reload the hash table with the new inner batch (which could be empty)
     */
    ExecHashTableReset(hashState, hashtable);

	if (batch->innerside.workfile != NULL)
	{
		/* Rewind batch file only if it was created by this operator.
		 * If we're loading from cached workfiles, no need to rewind. */
		if (!hjstate->cached_workfiles_loaded)
		{
			bool result = ExecWorkFile_Rewind(batch->innerside.workfile);
			if (!result)
			{
			    ereport(ERROR, (errcode_for_file_access(),
			    	errmsg("could not access temporary file")));
			}
		}

	    for (;;)
	    {
		    CHECK_FOR_INTERRUPTS();

		    slot = ExecHashJoinGetSavedTuple(&batch->innerside,
				    &hashvalue,
				    hjstate->hj_HashTupleSlot);
		    if (!slot)
			    break;

		    /*
		     * NOTE: some tuples may be sent to future batches.  Also, it is
		     * possible for hashtable->nbatch to be increased here!
		     */
		    ExecHashTableInsert(hashState, hashtable, slot, hashvalue);
		    hashtable->totalTuples += 1;
	    }

	    /*
	     * after we build the hash table, the inner batch file is no longer
	     * needed.
		 *
		 * However, if workfile caching is enabled, and this is the first
		 * time to create cached workfiles, we can not close the batch file
		 * here, since we need to save the workfile names at the end.
	     */
		if (!(gp_workfile_caching &&
			  !hjstate->cached_workfiles_found))
		{
			if (hjstate->js.ps.instrument)
			{
				Assert(hashtable->stats);
				hashtable->stats->batchstats[curbatch].innerfilesize =
						ExecWorkFile_Tell64(hashtable->batches[curbatch]->innerside.workfile);
			}
			workfile_mgr_close_file(hashtable->work_set, batch->innerside.workfile);
			batch->innerside.workfile = NULL;
		}
    }

    /*
     * If there's no outer batch file, advance to next batch.
     */
	if (batch->outerside.workfile == NULL)
	    goto start_over;

    /*
     * Rewind outer batch file, so that we can start reading it.
     * We only need to do that if we created those files, and not using cached workfiles.
     */
	if (!hjstate->cached_workfiles_loaded)
	{
		bool result = ExecWorkFile_Rewind(batch->outerside.workfile);
		if (!result)
		{
			ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not access temporary file")));
		}
	}
	
    return curbatch;
}

/*
 * ExecHashJoinSaveTuple
 *		save a tuple to a batch file.
 *
 * The data recorded in the file for each tuple is its hash value,
 * then the tuple in MinimalTuple format.
 *
 * Note: it is important always to call this in the regular executor
 * context, not in a shorter-lived context; else the temp file buffers
 * will get messed up.
 */
void
ExecHashJoinSaveTuple(PlanState *ps, MemTuple tuple, uint32 hashvalue,
					  HashJoinTable hashtable, struct HashJoinBatchSide *batchside,
					  MemoryContext bfCxt)
{
	if (hashtable->work_set == NULL)
	{
		hashtable->hjstate->workfiles_created = true;
		if (hashtable->hjstate->js.ps.instrument)
		{
			hashtable->hjstate->js.ps.instrument->workfileCreated = true;
		}

		MemoryContext   oldcxt;
		oldcxt = MemoryContextSwitchTo(bfCxt);

		hashtable->work_set = workfile_mgr_create_set(gp_workfile_type_hashjoin,
				true, /* can_be_reused */
				&hashtable->hjstate->js.ps,
				NULL_SNAPSHOT);

		/* First time spilling. Before creating any spill files, create a metadata file */
		hashtable->state_file = workfile_mgr_create_fileno(hashtable->work_set, WORKFILE_NUM_HASHJOIN_METADATA);
		elog(gp_workfile_caching_loglevel, "created state file %s", ExecWorkFile_GetFileName(hashtable->state_file));

		MemoryContextSwitchTo(oldcxt);
	}

	if (batchside->workfile == NULL)
	{
		MemoryContext   oldcxt;
		oldcxt = MemoryContextSwitchTo(bfCxt);

		/* First write to this batch file, so create it */
		Assert(hashtable->work_set != NULL);
		batchside->workfile = workfile_mgr_create_file(hashtable->work_set);
		
		elog(gp_workfile_caching_loglevel, "create batch file %s with gp_workfile_compress_algorithm=%d",
			 ExecWorkFile_GetFileName(batchside->workfile),
			 hashtable->work_set->metadata.bfz_compress_type);

		MemoryContextSwitchTo(oldcxt);
	}

	if (!ExecWorkFile_Write(batchside->workfile, (void *)&hashvalue, sizeof(uint32)))
	{
		workfile_mgr_report_error();
	}

	if (!ExecWorkFile_Write(batchside->workfile, (void *) tuple, memtuple_get_size(tuple, NULL)))
	{
		workfile_mgr_report_error();
	}

	batchside->total_tuples++;

	if(ps)
	{
		Gpmon_M_Incr(&ps->gpmon_pkt, GPMON_HASHJOIN_SPILLTUPLE);
		Gpmon_M_Add(&ps->gpmon_pkt, GPMON_HASHJOIN_SPILLBYTE, memtuple_get_size(tuple, NULL));
		CheckSendPlanStateGpmonPkt(ps);
	}
}

/*
 * ExecHashJoinGetSavedTuple
 *		read the next tuple from a batch file.	Return NULL if no more.
 *
 * On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */
static TupleTableSlot *
ExecHashJoinGetSavedTuple(HashJoinBatchSide *batchside,
						  uint32 *hashvalue,
						  TupleTableSlot *tupleSlot)
{
	uint32		header[2];
	size_t		nread;
	MemTuple tuple;

	/*
	 * Since both the hash value and the MinimalTuple length word are uint32,
	 * we can read them both in one BufFileRead() call without any type
	 * cheating.
	 */
	nread = ExecWorkFile_Read(batchside->workfile, (void *) header, sizeof(header));
	if (nread != sizeof(header))				/* end of file */
	{
		ExecClearTuple(tupleSlot);
		return NULL;
	}

	*hashvalue = header[0];
	tuple = (MemTuple) palloc(memtuple_size_from_uint32(header[1]));
	memtuple_set_mtlen(tuple, NULL, header[1]);

	nread = ExecWorkFile_Read(batchside->workfile,
							  (void *) ((char *) tuple + sizeof(uint32)),
							  memtuple_size_from_uint32(header[1]) - sizeof(uint32));
	
	if (nread != memtuple_size_from_uint32(header[1]) - sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from temporary file")));
	return ExecStoreMemTuple(tuple, tupleSlot, true);
}


void
ExecReScanHashJoin(HashJoinState *node, ExprContext *exprCtxt)
{		
	/*
	 * In a multi-batch join, we currently have to do rescans the hard way,
	 * primarily because batch temp files may have already been released. But
	 * if it's a single-batch join, and there is no parameter change for the
	 * inner subnode, then we can just re-use the existing hash table without
	 * rebuilding it.
	 */
	if (node->hj_HashTable != NULL)
	{
		if (node->hj_HashTable->nbatch == 1 &&
			((PlanState *) node)->righttree->chgParam == NULL
			&& !node->hj_HashTable->eagerlyReleased)
		{
			/*
			 * okay to reuse the hash table; needn't rescan inner, either.
			 *
			 * What we do need to do is reset our state about the emptiness of
			 * the outer relation, so that the new scan of the outer will
			 * update it correctly if it turns out to be empty this time.
			 * (There's no harm in clearing it now because ExecHashJoin won't
			 * need the info.  In the other cases, where the hash table
			 * doesn't exist or we are destroying it, we leave this state
			 * alone because ExecHashJoin will need it the first time
			 * through.)
			 */
			node->hj_OuterNotEmpty = false;

			/* MPP-1600: reset the batch number */
			node->hj_HashTable->curbatch = 0;
		}
		else
		{
			/* must destroy and rebuild hash table */
			if (!node->hj_HashTable->eagerlyReleased)
			{
				HashState *hashState = (HashState *) innerPlanState(node);
				ExecHashTableDestroy(hashState, node->hj_HashTable);
			}
			pfree(node->hj_HashTable);
			node->hj_HashTable = NULL;

			/*
			 * if chgParam of subnode is not null then plan will be re-scanned
			 * by first ExecProcNode.
			 */
			if (((PlanState *) node)->righttree->chgParam == NULL)
				ExecReScan(((PlanState *) node)->righttree, exprCtxt);
		}
	}

	/* Always reset intra-tuple state */
	node->hj_CurHashValue = 0;
	node->hj_CurBucketNo = 0;
	node->hj_CurTuple = NULL;

	node->js.ps.ps_OuterTupleSlot = NULL;
	node->hj_NeedNewOuter = true;
	node->hj_MatchedOuter = false;
	node->hj_FirstOuterTupleSlot = NULL;

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (((PlanState *) node)->lefttree->chgParam == NULL)
		ExecReScan(((PlanState *) node)->lefttree, exprCtxt);
}

/**
 * This method releases the hash table's memory. It maintains some of the other
 * aspects of the hash table like memory usage statistics. These may be required
 * during an explain analyze. A hash table that has been released cannot perform
 * any useful function anymore. 
 */
static void ReleaseHashTable(HashJoinState *node)
{
	Assert(gp_eager_hashtable_release);
	
	if (node->hj_HashTable)
	{
		HashState *hashState = (HashState *) innerPlanState(node);

		/* This hashtable should not have been released already! */
		Assert(!node->hj_HashTable->eagerlyReleased);
	    if (node->hj_HashTable->stats)
	    {
	    	/* Report on batch in progress. */
	    	ExecHashTableExplainBatchEnd(hashState, node->hj_HashTable);
	    }
		ExecHashTableDestroy(hashState, node->hj_HashTable);
		node->hj_HashTable->eagerlyReleased = true;
	}
	
	/* Always reset intra-tuple state */
	node->hj_CurHashValue = 0;
	node->hj_CurBucketNo = 0;
	node->hj_CurTuple = NULL;

	node->js.ps.ps_OuterTupleSlot = NULL;
	node->hj_NeedNewOuter = true;
	node->hj_MatchedOuter = false;
	node->hj_FirstOuterTupleSlot = NULL;	

	ExecHashJoinResetWorkfileState(node);
}

/*
 * Reset workfile caching state
 */
static void ExecHashJoinResetWorkfileState(HashJoinState *node)
{
	node->cached_workfiles_batches_buckets_loaded = false;
	node->cached_workfiles_loaded = false;
	node->cached_workfiles_found = false;
	node->workfiles_created = false;
	node->nbatch_loaded_state = -1;
}

/* Is this an IS-NOT-DISTINCT-join qual list (as opposed the an equijoin)?
 *
 * XXX We perform an abbreviated test based on the assumptions that 
 *     these are the only possibilities and that all conjuncts are 
 *     alike in this regard.
 */
bool isNotDistinctJoin(List *qualList)
{
	ListCell *lc;
	
	foreach (lc, qualList)
	{
		BoolExpr *bex = (BoolExpr*)lfirst(lc);
		DistinctExpr *dex;
		
		if ( IsA(bex, BoolExpr) && bex->boolop == NOT_EXPR )
		{
			dex = (DistinctExpr*)linitial(bex->args);
			
			if ( IsA(dex, DistinctExpr) )
				return true; /* We assume the rest follow suit! */
		}
	}
	return false;
}

void
initGpmonPktForHashJoin(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, HashJoin));
	
	{
		PerfmonNodeType type = PMNT_Invalid;

		switch(((HashJoin *)planNode)->join.jointype)
		{
			case JOIN_INNER:
				type = PMNT_HashJoin;
				break;
			case JOIN_LEFT:
				type = PMNT_HashLeftJoin;
				break;
			case JOIN_LASJ:
			case JOIN_LASJ_NOTIN:
				type = PMNT_HashLeftAntiSemiJoin;
				break;
			case JOIN_FULL:
				type = PMNT_HashFullJoin;
				break;
			case JOIN_RIGHT:
				type = PMNT_HashRightJoin;
				break;
			case JOIN_IN:
				type = PMNT_HashExistsJoin;
				break;
			case JOIN_REVERSE_IN:
				type = PMNT_HashReverseInJoin;
				break;
			case JOIN_UNIQUE_OUTER:
				type = PMNT_HashUniqueOuterJoin;
				break;
			case JOIN_UNIQUE_INNER:
				type = PMNT_HashUniqueInnerJoin;
				break;
		}

		Assert(type != PMNT_Invalid);
		Assert(GPMON_HASHJOIN_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, type, 
							 (int64)planNode->plan_rows,
							 NULL);
	}
}

void
ExecEagerFreeHashJoin(HashJoinState *node)
{
	if (node->hj_HashTable != NULL && !node->hj_HashTable->eagerlyReleased)
	{
		ReleaseHashTable(node);
	}
}

/*
 * isHashtableEmpty
 *
 *  After populating the hashtable with all the tuples from the innerside,
 *  scan all the batches and return true if the hashtable is completely empty
 *
 */
static bool
isHashtableEmpty(HashJoinTable hashtable)
{
    int         i;
    bool isEmpty = true;

    /* Is there a nonempty batch? */
    for (i = 0; i < hashtable->nbatch; i++)
    {
        /*
         * For batch 0, the number of inner tuples is stored in batches[i].innertuples.
         * For batches on disk (1 and above), the batches[i].innertuples is 0,
         * but batches[i].innerside.workfile is non-NULL if any tuples were written to disk.
         * Check both here.
         */
        if ((hashtable->batches[i]->innertuples > 0) ||
                (NULL != hashtable->batches[i]->innerside.workfile))
        {
            /* Found a non-empty batch, stop the search */
        	isEmpty = false;
            break;
        }
    }

    return isEmpty;
}

void
ExecHashJoinSaveFirstInnerBatch(HashJoinTable hashtable)
{

	Assert(hashtable != NULL);

	if (hashtable->nbatch == 1)
	{
		/* Nothing to do, we're not spilling */
		return;
	}

	HashJoinBatchSide *batchside = &hashtable->batches[0]->innerside;

	int i;
	for (i = 0; i < hashtable->nbuckets; i++)
	{
		HashJoinTuple tuple;
		tuple = hashtable->buckets[i];

		while (tuple != NULL)
		{

#ifdef USE_ASSERT_CHECKING
			int			bucketno;
			int			batchno;

			ExecHashGetBucketAndBatch(hashtable, tuple->hashvalue,
					&bucketno, &batchno);
			Assert(bucketno == i);
			Assert(batchno == 0);
#endif

			ExecHashJoinSaveTuple(&hashtable->hjstate->js.ps, HJTUPLE_MINTUPLE(tuple),
								  tuple->hashvalue,
								  hashtable,
                                  batchside,
								  hashtable->bfCxt);


			tuple = tuple->next;
		}
	}
}

/* Writing a string to a Workfile.
 * Format: [length|string]
 * This must be the same format used in ExecHashJoinReadStringStateFile.
 * Terminating null character is not written to disk
 */
static bool
WriteStringWorkFile(ExecWorkFile *workfile, const char *str)
{
	bool res = false;
	size_t slen = strlen(str);

	res = ExecWorkFile_Write(workfile, & slen, sizeof(slen));
	if (res == false)
	{
		return false;
	}

	return(ExecWorkFile_Write(workfile, (void *) str, slen));
}

/*
 * Reads a string from a workfile.
 *  Format: [length|string]
 *  This must be the same format used in ExecHashJoinWriteStringStateFile.
 *  Returns the palloc-ed string in the current context, NULL if error occurs.
 */
static char *
ReadStringWorkFile(ExecWorkFile *workfile)
{

	size_t slen = 0;
	bool res = ExecWorkFile_Read(workfile, & slen, sizeof(slen));
	if (res == false)
	{
		return NULL;
	}

	char * read_string = palloc(slen+1);
	res = ExecWorkFile_Read(workfile, read_string, slen);

	if (res == false)
	{
		pfree(read_string);
		return NULL;
	}

	read_string[slen]='\0';
	return read_string;
}

/*
 * SaveBatchFileName
 *   Save the batch file name to the state file, and close the batch file.
 */
static void
SaveBatchFileNameAndClose(HashJoinTable hashtable, ExecWorkFile *workfile)
{

	char *batch_file_name = EMPTY_WORKFILE_NAME;
	bool free_name = false;
	if (workfile != NULL)
	{
		batch_file_name = pstrdup(ExecWorkFile_GetFileName(workfile));
		free_name = true;
		workfile_mgr_close_file(hashtable->work_set, workfile);
	}

	bool res = WriteStringWorkFile(hashtable->state_file, batch_file_name);
	if (!res)
	{
		workfile_mgr_report_error();
	}

	if (free_name)
	{
		pfree(batch_file_name);
	}
}


/*
 * Workfile caching: dump hashtable spill files state to disk after reading
 * all inner and outer relation tuples. This can be used to re-load the spill file set
 * at a later time.
 */
static void
ExecHashJoinSaveState(HashJoinTable hashtable)
{
	/* What do we need to save:
	 *  - nbuckets
	 *  - nbatches
	 *  - names of each file corresponding to each inner batch, in order
	 *  - names of each file corresponding to each inner batch, in order
	 */
	if (!gp_workfile_caching)
	{
		return;
	}

	if (!hashtable->hjstate->workfiles_created)
	{
		return;
	}

	/*
	 * If this is called when a spill set is used, we only need to save
	 * the spill file state when the number of batches is changed during execution.
	 */
	if (hashtable->hjstate->cached_workfiles_found &&
		hashtable->nbatch == hashtable->nbatch_original)
	{
		Assert(!hashtable->hjstate->workfiles_created);
		return;
	}

	elog(gp_workfile_caching_loglevel, "Saving HashJoin inner and outer relation spill file state to disk");

	bool res = false;

	res = ExecWorkFile_Write(hashtable->state_file,
			& hashtable->nbuckets, sizeof(hashtable->nbuckets));
	if(!res)
	{
		workfile_mgr_report_error();
	}

	res = ExecWorkFile_Write(hashtable->state_file,
			& hashtable->nbatch, sizeof(hashtable->nbatch));

	if(!res)
	{
		workfile_mgr_report_error();
	}

	int i;
	for (i=0; i < hashtable->nbatch; i++)
	{
		SaveBatchFileNameAndClose(hashtable,
								  hashtable->batches[i]->innerside.workfile);
		hashtable->batches[i]->innerside.workfile = NULL;

		elog(gp_workfile_caching_loglevel, "HashJoin inner batch %d: innerspace=%d, spaceAllowed=%d, innertuples=%d",
			 i, (int)hashtable->batches[i]->innerspace, (int)hashtable->spaceAllowed, hashtable->batches[i]->innertuples);

		SaveBatchFileNameAndClose(hashtable,
								  hashtable->batches[i]->outerside.workfile);
		hashtable->batches[i]->outerside.workfile = NULL;
	}

	workfile_mgr_close_file(hashtable->work_set, hashtable->state_file);
	hashtable->state_file = NULL;
}

/*
 * Opens the state workfile from a cached workfile set and reads nbuckets and
 * nbatch from it.
 */
bool
ExecHashJoinLoadBucketsBatches(HashJoinTable hashtable)
{
	/* What do we need to load:
	 *  - nbuckets
	 *  - nbatches
	 */

	Assert(hashtable != NULL);
	Assert(hashtable->work_set != NULL);
	Assert(!hashtable->hjstate->cached_workfiles_batches_buckets_loaded);

	/*
	 * We allocate the workfile data structures in the longer-lived context hashtable->bfCxt.
	 * This way we can find them and close them at transaction abort, even after hashtable
	 * went away.
	 */
	MemoryContext   oldcxt;
	oldcxt = MemoryContextSwitchTo(hashtable->bfCxt);

	hashtable->state_file = workfile_mgr_open_fileno(hashtable->work_set,
			WORKFILE_NUM_HASHJOIN_METADATA);

	elog(gp_workfile_caching_loglevel, "Loading HashJoin spill state from disk file %s",
			ExecWorkFile_GetFileName(hashtable->state_file));

	Assert(NULL != hashtable->state_file);

	int loaded_nbuckets = 0;
	int loaded_nbatch = 0;

	uint64 bytes_read = 0;
	bytes_read = ExecWorkFile_Read(hashtable->state_file,
			& loaded_nbuckets, sizeof(loaded_nbuckets));
	insist_log(bytes_read == sizeof(loaded_nbuckets),
			"Could not read from temporary work file: %m");

	hashtable->nbuckets = loaded_nbuckets;


	bytes_read = ExecWorkFile_Read(hashtable->state_file,
			& loaded_nbatch, sizeof(loaded_nbatch));
	insist_log(bytes_read == sizeof(loaded_nbatch),
			"Could not read from temporary work file: %m");

	hashtable->nbatch = loaded_nbatch;

	hashtable->hjstate->cached_workfiles_batches_buckets_loaded = true;

	MemoryContextSwitchTo(oldcxt);
	return true;
}

/*
 * OpenBatchFile
 *   Open a batch file that is stored in state_file.
 */
static ExecWorkFile*
OpenBatchFile(HashJoinTable hashtable, int batch_no)
{
	ExecWorkFile *workfile = NULL;
	
	/*
	 * We allocate the workfile data structures in the longer-lived context hashtable->bfCxt.
	 * This way we can find them and close them at transaction abort, even after hashtable
	 * went away.
	 */
	MemoryContext   oldcxt;
	oldcxt = MemoryContextSwitchTo(hashtable->bfCxt);

	char * batch_file_name = ReadStringWorkFile(hashtable->state_file);
	insist_log(batch_file_name != NULL, "Could not read from temporary work file: %m");

	if (strncmp(batch_file_name, EMPTY_WORKFILE_NAME, sizeof(EMPTY_WORKFILE_NAME)) != 0)
	{
		workfile = ExecWorkFile_Open(batch_file_name,
									 hashtable->work_set->metadata.type,
									 false /* delOnClose */,
									 hashtable->work_set->metadata.bfz_compress_type);
		Assert(NULL != workfile);

		elog(gp_workfile_caching_loglevel, "opened for re-use batch file %s for batch #%d",
			 batch_file_name, batch_no);
	}

	pfree(batch_file_name);

	MemoryContextSwitchTo(oldcxt);
	return workfile;
}


static bool
ExecHashJoinLoadBatchFiles(HashJoinTable hashtable)
{
	/* We already read:
	 *  - nbuckets
	 *  - nbatches
	 *
	 * What do we need to load:
	 *  - names of each file corresponding to each inner batch, in order
	 *  - names of each file corresponding to each outer batch, in order
	 */

	Assert(hashtable != NULL);
	Assert(hashtable->work_set != NULL);
	Assert(hashtable->state_file != NULL);
	Assert(hashtable->hjstate->cached_workfiles_batches_buckets_loaded);

	MemoryContext oldcxt = MemoryContextSwitchTo(hashtable->bfCxt);

	for (int i=0; i < hashtable->nbatch; i++)
	{
		Assert(hashtable->batches[i]->innerside.workfile == NULL);
		hashtable->batches[i]->innerside.workfile = OpenBatchFile(hashtable, i);

		Assert(hashtable->batches[i]->outerside.workfile == NULL);
		hashtable->batches[i]->outerside.workfile = OpenBatchFile(hashtable, i);
	}

	MemoryContextSwitchTo(oldcxt);
	return true;
}

/* EOF */
