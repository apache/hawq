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
 * nodeHash.c
 *	  Routines to hash relations for hashjoin
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/nodeHash.c,v 1.107.2.1 2007/06/01 15:58:01 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		MultiExecHash	- generate an in-memory hash table of the relation
 *		ExecInitHash	- initialize node and subnodes
 *		ExecEndHash		- shutdown node and subnodes
 */

#include "postgres.h"

#include <unistd.h>
#include <math.h>
#include <limits.h>

#include "access/hash.h"
#include "executor/execdebug.h"
#include "executor/hashjoin.h"
#include "executor/instrument.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "parser/parse_expr.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/debugbreak.h"
#include "utils/faultinjector.h"

#include "cdb/cdbexplain.h"
#include "cdb/cdbvars.h"

static void ExecHashIncreaseNumBatches(HashJoinTable hashtable);
static void ExecHashTableExplainEnd(PlanState *planstate, struct StringInfoData *buf);
static void
ExecHashTableExplainBatches(HashJoinTable   hashtable,
                            StringInfo      buf,
                            int             ibatch_begin,
                            int             ibatch_end,
                            const char     *title);
static void ExecHashTableReallocBatchData(HashJoinTable hashtable, int new_nbatch);
static int ExecChoosePrimeNBuckets(int nbuckets);

void ExecChooseHashTableSize(double ntuples, int tupwidth,
						int *numbuckets,
						int *numbatches,
						uint64 operatorMemKB
						);

#define BLOOMVAL(hk)  (((uint64)1) << (((hk) >> 13) & 0x3f))

/* Amount of metadata memory required per batch */
#define MD_MEM_PER_BATCH 	(sizeof(HashJoinBatchData *) + sizeof(HashJoinBatchData))

/* Amount of metadata memory required per bucket */
#define MD_MEM_PER_BUCKET (sizeof(HashJoinTuple) + sizeof(uint64))

/* ----------------------------------------------------------------
 *		ExecHash
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecHash(HashState *node)
{
	elog(ERROR, "Hash node does not support ExecProcNode call convention");
	return NULL;
}

/* ----------------------------------------------------------------
 *		MultiExecHash
 *
 *		build hash table for hashjoin, doing partitioning if more
 *		than one batch is required.
 * ----------------------------------------------------------------
 */
Node *
MultiExecHash(HashState *node)
{
	PlanState  *outerNode;
	List	   *hashkeys;
	HashJoinTable hashtable;
	TupleTableSlot *slot;
	ExprContext *econtext;
	uint32		hashvalue = 0;

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStartNode(node->ps.instrument);

	/*
	 * get state info from node
	 */
	outerNode = outerPlanState(node);
	hashtable = node->hashtable;

	/*
	 * set expression context
	 */
	hashkeys = node->hashkeys;
	econtext = node->ps.ps_ExprContext;

#ifdef FAULT_INJECTOR
    FaultInjector_InjectFaultIfSet(
    		MultiExecHashLargeVmem,
            DDLNotSpecified,
            "",  // databaseName
            ""); // tableName
#endif

	/*
	 * get all inner tuples and insert into the hash table (or temp files)
	 */
	for (;;)
	{
		slot = ExecProcNode(outerNode);
		if (TupIsNull(slot))
			break;

		Gpmon_M_Incr(GpmonPktFromHashState(node), GPMON_QEXEC_M_ROWSIN); 
                CheckSendPlanStateGpmonPkt(&node->ps);
		/* We have to compute the hash value */
		econtext->ecxt_innertuple = slot;
		bool hashkeys_null = false;

		if (ExecHashGetHashValue(node, hashtable, econtext, hashkeys, node->hs_keepnull, &hashvalue, &hashkeys_null))
		{
			ExecHashTableInsert(node, hashtable, slot, hashvalue);
		}

		if (hashkeys_null)
		{
			node->hs_hashkeys_null = true;
			if (node->hs_quit_if_hashkeys_null)
			{
				ExecSquelchNode(outerNode);
				return NULL;
			}
		}

	}

	/* Now we have set up all the initial batches & primary overflow batches. */
	hashtable->nbatch_outstart = hashtable->nbatch;

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStopNode(node->ps.instrument, hashtable->totalTuples);

	/*
	 * We do not return the hash table directly because it's not a subtype of
	 * Node, and so would violate the MultiExecProcNode API.  Instead, our
	 * parent Hashjoin node is expected to know how to fish it out of our node
	 * state.  Ugly but not really worth cleaning up, since Hashjoin knows
	 * quite a bit more about Hash besides that.
	 */
	return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitHash
 *
 *		Init routine for Hash node
 * ----------------------------------------------------------------
 */
HashState *
ExecInitHash(Hash *node, EState *estate, int eflags)
{
	HashState  *hashstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hashstate = makeNode(HashState);
	hashstate->ps.plan = (Plan *) node;
	hashstate->ps.state = estate;
	hashstate->hashtable = NULL;
	hashstate->hashkeys = NIL;	/* will be set by parent HashJoin */

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hashstate->ps);

#define HASH_NSLOTS 1

	/*
	 * initialize our result slot
	 */
	ExecInitResultTupleSlot(estate, &hashstate->ps);

	/*
	 * initialize child expressions
	 */
	hashstate->ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) hashstate);
	hashstate->ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) hashstate);

	/*
	 * initialize child nodes
	 */
	outerPlanState(hashstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * initialize tuple type. no need to initialize projection info because
	 * this node doesn't do projections
	 */
	ExecAssignResultTypeFromTL(&hashstate->ps);
	hashstate->ps.ps_ProjInfo = NULL;

	initGpmonPktForHash((Plan *)node, &hashstate->ps.gpmon_pkt, estate);

	return hashstate;
}

int
ExecCountSlotsHash(Hash *node)
{
	return ExecCountSlotsNode(outerPlan(node)) +
		ExecCountSlotsNode(innerPlan(node)) +
		HASH_NSLOTS;
}

/* ---------------------------------------------------------------
 *		ExecEndHash
 *
 *		clean up routine for Hash node
 * ----------------------------------------------------------------
 */
void
ExecEndHash(HashState *node)
{
	PlanState  *outerPlan;

	/*
	 * free exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * shut down the subplan
	 */
	outerPlan = outerPlanState(node);
	ExecEndNode(outerPlan);

	EndPlanStateGpmonPkt(&node->ps);
}

/* ----------------------------------------------------------------
 *		ExecHashTableCreate
 *
 *		create an empty hashtable data structure for hashjoin.
 * ----------------------------------------------------------------
 */
HashJoinTable
ExecHashTableCreate(HashState *hashState, HashJoinState *hjstate, List *hashOperators, uint64 operatorMemKB, workfile_set * workfile_set)
{
	HashJoinTable hashtable;
	Plan	   *outerNode;
	int			nbuckets;
	int			nbatch;
	int			nkeys;
	int			i;
	ListCell   *ho;
	MemoryContext oldcxt;

	START_MEMORY_ACCOUNT(hashState->ps.plan->memoryAccount);
	{

	Hash *node = (Hash *) hashState->ps.plan;

	/*
	 * Get information about the size of the relation to be hashed (it's the
	 * "outer" subtree of this node, but the inner relation of the hashjoin).
	 * Compute the appropriate size of the hash table.
	 */
	outerNode = outerPlan(node);

	/*
	 * Initialize the hash table control block.
	 *
	 * The hashtable control block is just palloc'd from the executor's
	 * per-query memory context.
	 */
	hashtable = (HashJoinTable)palloc0(sizeof(HashJoinTableData));
	hashtable->buckets = NULL;
	hashtable->bloom = NULL;
	hashtable->curbatch = 0;
	hashtable->growEnabled = true;
	hashtable->totalTuples = 0;
	hashtable->batches = NULL;
	hashtable->work_set = NULL;
	hashtable->state_file = NULL;
	hashtable->spaceAllowed = operatorMemKB * 1024L;
	hashtable->stats = NULL;
	hashtable->eagerlyReleased = false;
	hashtable->hjstate = hjstate;

	/*
	 * Create temporary memory contexts in which to keep the hashtable working
	 * storage.  See notes in executor/hashjoin.h.
	 */
	hashtable->hashCxt = AllocSetContextCreate(CurrentMemoryContext,
											   "HashTableContext",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	hashtable->batchCxt = AllocSetContextCreate(hashtable->hashCxt,
												"HashBatchContext",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	/* CDB */ /* track temp buf file allocations in separate context */
	hashtable->bfCxt = AllocSetContextCreate(CurrentMemoryContext,
											 "hbbfcxt",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);

	if (workfile_set != NULL)
	{
		hashtable->work_set = workfile_set;
		ExecHashJoinLoadBucketsBatches(hashtable);
		Assert(hjstate->nbatch_loaded_state == -1);
		Assert(hjstate->cached_workfiles_batches_buckets_loaded);

		hjstate->nbatch_loaded_state = hashtable->nbatch;
	}
	else
	{
		ExecChooseHashTableSize(outerNode->plan_rows, outerNode->plan_width,
				&hashtable->nbuckets, &hashtable->nbatch, operatorMemKB);
	}

	nbuckets = hashtable->nbuckets;
	nbatch = hashtable->nbatch;
	hashtable->nbatch_original = nbatch;
	hashtable->nbatch_outstart = nbatch;


#ifdef HJDEBUG
    elog(LOG, "HJ: nbatch = %d, nbuckets = %d\n", nbatch, nbuckets);
#endif


    /*
	 * Get info about the hash functions to be used for each hash key.
	 * Also remember whether the join operators are strict.
	 */
	nkeys = list_length(hashOperators);
	hashtable->hashfunctions = (FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));
	hashtable->hashStrict = (bool *) palloc(nkeys * sizeof(bool));
	i = 0;
	foreach(ho, hashOperators)
	{
		Oid			hashop = lfirst_oid(ho);
		Oid			hashfn;

		hashfn = get_op_hash_function(hashop);
		if (!OidIsValid(hashfn))
			elog(ERROR, "could not find hash function for hash operator %u",
				 hashop);
		fmgr_info(hashfn, &hashtable->hashfunctions[i]);
		hashtable->hashStrict[i] = op_strict(hashop);
		i++;
	}

	/*
     * Allocate data that will live for the life of the hashjoin
     */
	oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

#ifdef HJDEBUG
	{
		/* Memory needed to allocate hashtable->batches, which consists of nbatch pointers */
		int md_batch_size =  (nbatch * sizeof(hashtable->batches[0])) / (1024 * 1024);
		/* Memory needed to allocate hashtable->batches entries, which consist of nbatch HashJoinBatchData structures */
		int md_batch_data_size = (nbatch * sizeof(HashJoinBatchData)) / (1024 * 1024);

		/* Memory needed to allocate hashtable->buckets, which consists of nbuckets  HashJoinTuple structures*/
		int md_buckets_size = (nbuckets * sizeof(HashJoinTuple)) / (1024 * 1024);

		/* Memory needed to allocate hashtable->bloom, which consists of nbuckets int64 values */
		int md_bloom_size = (nbuckets * sizeof(uint64)) / (1024 * 1024);

		/* Total memory needed for the hashtable metadata */
		int md_tot = md_batch_size + md_batch_data_size + md_buckets_size + md_bloom_size;

		elog(LOG, "About to allocate HashTable. HT_MEMORY=%dMB Memory needed for metadata: MDBATCH_ARR=%dMB, MDBATCH_DATA=%dMB, MDBUCKETS_ARR=%dMB, MDBLOOM_ARR=%dMB, TOTAL=%dMB",
				(int) (hashtable->spaceAllowed / (1024 * 1024)),
				md_batch_size, md_batch_data_size, md_buckets_size, md_bloom_size, md_tot);

		elog(LOG, "sizeof(hashtable->batches[0])=%d, sizeof(HashJoinBatchData)=%d, sizeof(HashJoinTuple)=%d, sizeof(uint64)=%d",
				(int) sizeof(hashtable->batches[0]), (int) sizeof(HashJoinBatchData),
				(int) sizeof(HashJoinTuple), (int) sizeof(uint64));
	}
#endif

    /* array of BatchData ptrs */
    hashtable->batches =
        (HashJoinBatchData **)palloc(nbatch * sizeof(hashtable->batches[0]));

    /* one BatchData entry per initial batch */
    for (i = 0; i < nbatch; i++)
        hashtable->batches[i] =
            (HashJoinBatchData *)palloc0(sizeof(HashJoinBatchData));

	/*
	 * Prepare context for the first-scan space allocations; allocate the
	 * hashbucket array therein, and set each bucket "empty".
	 */
	MemoryContextSwitchTo(hashtable->batchCxt);

	hashtable->buckets = (HashJoinTuple *)
		palloc0(nbuckets * sizeof(HashJoinTuple));

	if(gp_hashjoin_bloomfilter!=0)
		hashtable->bloom = (uint64*) palloc0(nbuckets * sizeof(uint64));

	MemoryContextSwitchTo(oldcxt);
	}
	END_MEMORY_ACCOUNT();
	return hashtable;
}


/*
 * Compute appropriate size for hashtable given the estimated size of the
 * relation to be hashed (number of rows and average row width).
 *
 * This is exported so that the planner's costsize.c can use it.
 */

/* Target bucket loading (tuples per bucket) */
/*
 * CDB: we now use gp_hashjoin_tuples_per_bucket
 * #define NTUP_PER_BUCKET			10
 */

/* Prime numbers that we like to use as nbuckets values */
static const int hprimes[] = {
	1033, 2063, 4111, 8219, 16417, 32779, 65539, 131111,
	262151, 524341, 1048589, 2097211, 4194329, 8388619, 16777289, 33554473,
	67108913, 134217773, 268435463, 536870951, 1073741831
};

/*
 * We want nbuckets to be prime so as to avoid having bucket and batch
 * numbers depend on only some bits of the hash code.  Choose the next
 * larger prime from the list in hprimes[].  (This also enforces that
 * nbuckets is not very small, by the simple expedient of not putting any
 * very small entries in hprimes[].)
 */
static int
ExecChoosePrimeNBuckets(int nbuckets)
{
	for (int i = 0; i < (int) lengthof(hprimes); i++)
	{
		if (hprimes[i] >= nbuckets)
		{
			nbuckets = hprimes[i];
			break;
		}
	}
	return nbuckets;
}

void
ExecChooseHashTableSize(double ntuples, int tupwidth,
						int *numbuckets,
						int *numbatches,
						uint64 operatorMemKB)
{
	int			tupsize;
	double		inner_rel_bytes;
	long		hash_table_bytes;
	int			nbatch;
	int			nbuckets;

	/* num tuples is a global number. We should be receiving only part of that */
	if (Gp_role == GP_ROLE_EXECUTE)
	{
    int segment_num = GetQEGangNum();
    Assert(segment_num > 0);
    ntuples = ntuples / segment_num;
	}
	
	/* Force a plausible relation size if no info */
	if (ntuples <= 0.0)
		ntuples = 1000.0;

	/*
	 * Estimate tupsize based on footprint of tuple in hashtable... note this
	 * does not allow for any palloc overhead.	The manipulations of spaceUsed
	 * don't count palloc overhead either.
	 */
	tupsize = ExecHashRowSize(tupwidth);
	inner_rel_bytes = ntuples * tupsize;

	/*
	 * Target in-memory hashtable size is work_mem kilobytes.
	 */
	hash_table_bytes = operatorMemKB * 1024L;

	/*
	 * Set nbuckets to achieve an average bucket load of gp_hashjoin_tuples_per_bucket when
	 * memory is filled.  Set nbatch to the smallest power of 2 that appears
	 * sufficient.
	 */
	if (inner_rel_bytes > hash_table_bytes)
	{
		/* We'll need multiple batches */
		long		lbuckets;
		double		dbatch;
		int			minbatch;

		lbuckets = (hash_table_bytes / tupsize) / gp_hashjoin_tuples_per_bucket;
		lbuckets = Min(lbuckets, INT_MAX / 32);

		/* Pick the closest prime number for the number of buckets */
		nbuckets = ExecChoosePrimeNBuckets((int) lbuckets);

		dbatch = ceil(inner_rel_bytes / hash_table_bytes);
		dbatch = Min(dbatch, INT_MAX / 32);
		minbatch = (int) dbatch;
		nbatch = 2;
		while (nbatch < minbatch)
		{
			nbatch <<= 1;
		}

		/* Check to see if we're capping the number of workfiles we allow per query */
		if (gp_workfile_limit_files_per_query > 0)
		{
			int nbatch_lower = nbatch;
			/*
			 * We create two files per batch during spilling - one for
			 * outer and one of inner side. Lower the nbatch if necessary
			 * to fit under that limit. Don't go below two batches,
			 * because in that case we're basically disabling spilling.
			 */
			while ((nbatch_lower * 2 > gp_workfile_limit_files_per_query) && (nbatch_lower > 2))
			{
				nbatch_lower >>= 1;
			}

			Assert(nbatch_lower <= nbatch);
			if (nbatch_lower != nbatch)
			{
				elog(LOG,"HashJoin: Too many batches computed: nbatch=%d. gp_workfile_limit_files_per_query=%d, using nbatch=%d instead",
						nbatch, gp_workfile_limit_files_per_query, nbatch_lower);
				nbatch = nbatch_lower;
			}
		}

		/*
		 * Check to see if we're capping the amount of memory allowed for
		 * hasthable metadata (MPP-22417)
		 */
		if (gp_hashjoin_metadata_memory_percent > 0)
		{
			/* Compute how much memory we are willing to use for batch metadata */
			long md_mem_for_batches = ((float) hash_table_bytes * (((float) gp_hashjoin_metadata_memory_percent) / 100))
						- (nbuckets * MD_MEM_PER_BUCKET);

			if (md_mem_for_batches < 0)
			{
				/* We are already out of metadata memory, we can't execute query. Error out */
				ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
						errmsg(ERRMSG_GP_INSUFFICIENT_STATEMENT_MEMORY)));
			}

			if (nbatch * MD_MEM_PER_BATCH > md_mem_for_batches)
			{
				/*
				 * We're trying to allocate too many batches, it won't fit.
				 * Reduce the number of batches such that it fits in available metadata memory.
				 */
				int nbatch_lower = nbatch;
				while (nbatch_lower > 1 && nbatch_lower * MD_MEM_PER_BATCH > md_mem_for_batches)
				{
					nbatch_lower >>= 1;
				}

				Assert(nbatch_lower <= nbatch);
				elog(LOG,"HashJoin: Too many batches computed: nbatch=%d. Memory required for metadata=%dMB, available=%dMB. Chose nbatch=%d instead",
						nbatch, (int) (nbatch * MD_MEM_PER_BATCH) / (1024 * 1024),
						(int) md_mem_for_batches / (1024 * 1024),
						nbatch_lower);

				nbatch = nbatch_lower;
			}
		}

	}
	else
	{
		/* We expect the hashtable to fit in memory, we want to use
		 * more buckets if we have memory to spare */
		double		dbuckets_lower;
		double		dbuckets_upper;
		double		dbuckets;

		/* divide our tuple row-count estimate by our the number of
		 * tuples we'd like in a bucket: this produces a small bucket
		 * count independent of our work_mem setting */
		dbuckets_lower = (double)ntuples / (double)gp_hashjoin_tuples_per_bucket;

		/* if we have work_mem to spare, we'd like to use it -- so
		 * divide up our memory evenly (see the spill case above) */
		dbuckets_upper = (double)hash_table_bytes / ((double)tupsize * gp_hashjoin_tuples_per_bucket);

		/* we'll use our "lower" work_mem independent guess as a lower
		 * limit; but if we've got memory to spare we'll take the mean
		 * of the lower-limit and the upper-limit */
		if (dbuckets_upper > dbuckets_lower)
			dbuckets = (dbuckets_lower + dbuckets_upper)/2.0;
		else
			dbuckets = dbuckets_lower;

		dbuckets = ceil(dbuckets);
		dbuckets = Min(dbuckets, INT_MAX);

		/* Pick the closest prime number for the number of buckets */
		nbuckets = ExecChoosePrimeNBuckets((int) dbuckets);

		nbatch = 1;
	}

	*numbuckets = nbuckets;
	*numbatches = nbatch;
}


/* ----------------------------------------------------------------
 *		ExecHashTableDestroy
 *
 *		destroy a hash table
 * ----------------------------------------------------------------
 */
void
ExecHashTableDestroy(HashState *hashState, HashJoinTable hashtable)
{
	int			i;
	Assert(hashtable);
	Assert(!hashtable->eagerlyReleased);
	
	START_MEMORY_ACCOUNT(hashState->ps.plan->memoryAccount);
	{

	/*
	 * Make sure all the temp files are closed.
	 */
	for (i = 0; i < hashtable->nbatch; i++)
	{
		HashJoinBatchData  *batch = hashtable->batches[i];

		if (batch->innerside.workfile != NULL)
		{
			workfile_mgr_close_file(hashtable->work_set, batch->innerside.workfile);
			batch->innerside.workfile = NULL;
		}

		if (batch->outerside.workfile != NULL)
		{
			workfile_mgr_close_file(hashtable->work_set, batch->outerside.workfile);
			batch->outerside.workfile = NULL;
		}
	}

	/* Close state file as well */
	if (hashtable->state_file != NULL)
	{
		workfile_mgr_close_file(hashtable->work_set, hashtable->state_file);
		hashtable->state_file = NULL;
	}

	if (hashtable->work_set != NULL)
	{
		workfile_mgr_close_set(hashtable->work_set);
		hashtable->work_set = NULL;
	}

	/* Release working memory (batchCxt is a child, so it goes away too) */
	MemoryContextDelete(hashtable->hashCxt);
	hashtable->batches = NULL;
	}
	END_MEMORY_ACCOUNT();
}

/*
 * ExecHashIncreaseNumBatches
 *		increase the original number of batches in order to reduce
 *		current memory consumption
 */
static void
ExecHashIncreaseNumBatches(HashJoinTable hashtable)
{
	HashJoinBatchData  *fullbatch = hashtable->batches[hashtable->curbatch];
	int			oldnbatch = hashtable->nbatch;
	int			curbatch = hashtable->curbatch;
	int			nbatch;
	int			i;
	long		ninmemory;
	long		nfreed;
	Size        spaceFreed = 0;
	HashJoinTableStats *stats = hashtable->stats;

	/* do nothing if we've decided to shut off growth */
	if (!hashtable->growEnabled)
		return;

	/* safety check to avoid overflow */
	if (oldnbatch > INT_MAX / 2)
		return;

	nbatch = oldnbatch * 2;
	Assert(nbatch > 1);

#ifdef HJDEBUG
	elog(LOG, "Increasing number of batches from %d to %d", oldnbatch, nbatch);
#endif

	{
		/* Print a warning for the extreme bad cases. */
		unsigned mintup = hashtable->batches[0]->innertuples;
		unsigned maxtup = mintup;

		for(i=1; i<oldnbatch; ++i)
		{
			unsigned ntup = hashtable->batches[i]->innertuples;

			maxtup = Max(ntup, maxtup);
			mintup = Min(ntup, mintup);
		}

		if(maxtup > (mintup * 10))
			elog(LOG, "Extreme skew in the innerside of Hashjoin, nbatch %d, mintuples %u, maxtuples %u", oldnbatch, mintup, maxtup);
	}

	ExecHashTableReallocBatchData(hashtable, nbatch);
	Assert(hashtable->nbatch == nbatch);

	/*
	 * Scan through the existing hash table entries and dump out any that are
	 * no longer of the current batch.
	 */
	ninmemory = nfreed = 0;

	for (i = 0; i < hashtable->nbuckets; i++)
	{
		HashJoinTuple prevtuple;
		HashJoinTuple tuple;
		uint64 bloom = 0;

		prevtuple = NULL;
		tuple = hashtable->buckets[i];

		while (tuple != NULL)
		{
			/* save link in case we delete */
			HashJoinTuple nexttuple = tuple->next;
			int			bucketno;
			int			batchno;

			ninmemory++;
			ExecHashGetBucketAndBatch(hashtable, tuple->hashvalue,
					&bucketno, &batchno);
			Assert(bucketno == i);
			if (batchno == curbatch)
			{
				/* keep tuple */
				prevtuple = tuple;
				bloom |= BLOOMVAL(tuple->hashvalue);
			}
			else
			{
				Size    spaceTuple;

				/* dump it out */
				Assert(batchno > curbatch);
				Assert(batchno >= hashtable->hjstate->nbatch_loaded_state);
				ExecHashJoinSaveTuple(NULL, HJTUPLE_MINTUPLE(tuple),
						tuple->hashvalue,
						hashtable,
						&hashtable->batches[batchno]->innerside,
						hashtable->bfCxt);
				/* and remove from hash table */
				if (prevtuple)
					prevtuple->next = nexttuple;
				else
					hashtable->buckets[i] = nexttuple;
				/* prevtuple doesn't change */

				hashtable->totalTuples--;

				spaceTuple = HJTUPLE_OVERHEAD + memtuple_get_size(HJTUPLE_MINTUPLE(tuple), NULL); 
				spaceFreed += spaceTuple;
				if (stats)
					stats->batchstats[batchno].spillspace_in += spaceTuple;

				pfree(tuple);
				nfreed++;
			}

			tuple = nexttuple;
		}

		if(gp_hashjoin_bloomfilter!=0)
			hashtable->bloom[i] = bloom;
	}

#ifdef HJDEBUG
	elog(gp_workfile_caching_loglevel, "HJ batch %d: Freed %ld of %ld tuples, %lu of %lu bytes, space now %lu",
			curbatch,
			nfreed,
			ninmemory,
			(unsigned long)spaceFreed,
			(unsigned long)fullbatch->innerspace,
			(unsigned long)(fullbatch->innerspace - spaceFreed));
#endif

	/* Update work_mem high-water mark and amount spilled. */
	if (stats)
	{
		stats->workmem_max = Max(stats->workmem_max, fullbatch->innerspace);
		stats->batchstats[curbatch].spillspace_out += spaceFreed;
		stats->batchstats[curbatch].spillrows_out += nfreed;
	}

	/* Allow reuse of the space that has just been freed. */
	fullbatch->innerspace -= spaceFreed;
	fullbatch->innertuples -= nfreed;

	/*
	 * If we dumped out either all or none of the tuples in the table, disable
	 * further expansion of nbatch.  This situation implies that we have
	 * enough tuples of identical hashvalues to overflow spaceAllowed.
	 * Increasing nbatch will not fix it since there's no way to subdivide the
	 * group any more finely. We have to just gut it out and hope the server
	 * has enough RAM.
	 */
	if (nfreed == 0 || nfreed == ninmemory)
	{
		hashtable->growEnabled = false;
		elog(LOG, "HJ: Disabling further increase of nbatch");
	}

}

/*
 * Re-allocate the batch data array when the number of batches increases
 */
static void
ExecHashTableReallocBatchData(HashJoinTable hashtable, int new_nbatch)
{

	MemoryContext oldcxt;
	oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

	/*
	 * enlarge arrays and zero out added entries
	 */

	/* array of BatchData ptrs */
	hashtable->batches = (HashJoinBatchData **)
		repalloc(hashtable->batches, new_nbatch * sizeof(hashtable->batches[0]));

	int	old_nbatch = hashtable->nbatch;
	int	i;

	/* one BatchData entry per additional batch */
	for (i = old_nbatch; i < new_nbatch; i++)
	{
		hashtable->batches[i] =
			(HashJoinBatchData *)palloc0(sizeof(HashJoinBatchData));
	}

	/* EXPLAIN ANALYZE batch statistics */
	HashJoinTableStats *stats = hashtable->stats;
	if (stats && stats->nbatchstats < new_nbatch)
	{
		Size sz = new_nbatch * sizeof(stats->batchstats[0]);
		stats->batchstats =
			(HashJoinBatchStats *)repalloc(stats->batchstats, sz);
		sz = (new_nbatch - stats->nbatchstats) * sizeof(stats->batchstats[0]);
		memset(stats->batchstats + stats->nbatchstats, 0, sz);
		stats->nbatchstats = new_nbatch;
	}

	MemoryContextSwitchTo(oldcxt);

	hashtable->nbatch = new_nbatch;

}

/*
 * ExecHashTableInsert
 *		insert a tuple into the hash table depending on the hash value
 *		it may just go to a temp file for later batches
 *
 * Note: the passed TupleTableSlot may contain a regular, minimal, or virtual
 * tuple; the minimal case in particular is certain to happen while reloading
 * tuples from batch files.  We could save some cycles in the regular-tuple
 * case by not forcing the slot contents into minimal form; not clear if it's
 * worth the messiness required.
 */
void
ExecHashTableInsert(HashState *hashState, HashJoinTable hashtable,
					TupleTableSlot *slot,
					uint32 hashvalue)
{
	MemTuple tuple = ExecFetchSlotMemTuple(slot, false);
	HashJoinBatchData  *batch;
	int			bucketno;
	int			batchno;
	int			hashTupleSize;

	START_MEMORY_ACCOUNT(hashState->ps.plan->memoryAccount);
	{
	PlanState *ps = &hashState->ps;

	ExecHashGetBucketAndBatch(hashtable, hashvalue,
			&bucketno, &batchno);

	batch = hashtable->batches[batchno];
	hashTupleSize = HJTUPLE_OVERHEAD + memtuple_get_size(tuple, NULL); 

	/* Update batch size. */
	batch->innertuples++;
	batch->innerspace += hashTupleSize;

	/*
	 * decide whether to put the tuple in the hash table or a temp file
	 */
	if (batchno == hashtable->curbatch)
	{
		/*
		 * put the tuple in hash table
		 */
		HashJoinTuple hashTuple;

		hashTuple = (HashJoinTuple) MemoryContextAlloc(hashtable->batchCxt,
				hashTupleSize);
		hashTuple->hashvalue = hashvalue;
		memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, memtuple_get_size(tuple, NULL)); 
		hashTuple->next = hashtable->buckets[bucketno];
		hashtable->buckets[bucketno] = hashTuple;
		hashtable->totalTuples += 1;

		if(gp_hashjoin_bloomfilter!=0)
			hashtable->bloom[bucketno] |= BLOOMVAL(hashvalue);

		/* Double the number of batches when too much data in hash table. */
		if (batch->innerspace > hashtable->spaceAllowed ||
			batch->innertuples > UINT_MAX/2)
		{
			ExecHashIncreaseNumBatches(hashtable);

			if (ps && ps->instrument)
			{
				ps->instrument->workfileCreated = true;
			}

			/* Gpmon stuff */
			if(ps)
			{
				Gpmon_M_Set(&ps->gpmon_pkt, GPMON_HASH_SPILLBATCH, hashtable->nbatch);
				CheckSendPlanStateGpmonPkt(ps);
			}
		}
	}
	else
	{
		/*
		 * put the tuple into a temp file for later batches, only when the cached
		 * workfile is not used.
		 */
		if (!hashtable->hjstate->cached_workfiles_found)
		{
			Assert(batchno > hashtable->curbatch);
			ExecHashJoinSaveTuple(ps, tuple, hashvalue, hashtable, &batch->innerside, hashtable->bfCxt);
		}
	}
	}
	END_MEMORY_ACCOUNT();
}

/*
 * ExecHashGetHashValue
 *		Compute the hash value for a tuple
 *
 * The tuple to be tested must be in either econtext->ecxt_outertuple or
 * econtext->ecxt_innertuple.  Vars in the hashkeys expressions reference
 * either OUTER or INNER.
 *
 * A TRUE result means the tuple's hash value has been successfully computed
 * and stored at *hashvalue.  A FALSE result means the tuple cannot match
 * because it contains a null attribute, and hence it should be discarded
 * immediately.  (If keep_nulls is true then FALSE is never returned.)
 * Found_null indicates all the hashkeys are null.
 */
bool
ExecHashGetHashValue(HashState *hashState, HashJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool keep_nulls,
					 uint32 *hashvalue,
					 bool *hashkeys_null)
{
	uint32		hashkey = 0;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;
	bool		result = true;

	START_MEMORY_ACCOUNT(hashState->ps.plan->memoryAccount);
	{

	Assert(hashkeys_null);

	(*hashkeys_null) = true;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	foreach(hk, hashkeys)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull = false;

		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull, NULL);

		if (!isNull)
		{
			*hashkeys_null = false;
		}

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join,
		 * in which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little
		 * extra code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (hashtable->hashStrict[i] && !keep_nulls)
			{
				result = false;
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
		}
		else if (result)
		{
			/* Compute the hash function */
			uint32		hkey;

			hkey = DatumGetUInt32(FunctionCall1(&hashtable->hashfunctions[i],
												keyval));
			hashkey ^= hkey;
		}

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	}
	END_MEMORY_ACCOUNT();
	return result;
}

/*
 * ExecHashGetBucketAndBatch
 *		Determine the bucket number and batch number for a hash value
 *
 * Note: on-the-fly increases of nbatch must not change the bucket number
 * for a given hash code (since we don't move tuples to different hash
 * chains), and must only cause the batch number to remain the same or
 * increase.  Our algorithm is
 *		bucketno = hashvalue MOD nbuckets
 *		batchno = hash_uint32(hashvalue) MOD nbatch
 * which gives reasonably independent bucket and batch numbers in the face
 * of some rather poorly-implemented hash functions in hashfunc.c.  (This
 * will change in PG 8.3.)
 *
 * nbuckets doesn't change over the course of the join.
 *
 * nbatch is always a power of 2; we increase it only by doubling it.  This
 * effectively adds one more bit to the top of the batchno.
 */
void
ExecHashGetBucketAndBatch(HashJoinTable hashtable,
						  uint32 hashvalue,
						  int *bucketno,
						  int *batchno)
{
	uint32		nbuckets = (uint32) hashtable->nbuckets;
	uint32		nbatch = (uint32) hashtable->nbatch;

	if (nbatch > 1)
	{
		*bucketno = hashvalue % nbuckets;
		/* since nbatch is a power of 2, can do MOD by masking */
		*batchno = hash_uint32(hashvalue) & (nbatch - 1);
	}
	else
	{
		*bucketno = hashvalue % nbuckets;
		*batchno = 0;
	}
}

/*
 * ExecScanHashBucket
 *		scan a hash bucket for matches to the current outer tuple
 *
 * The current outer tuple must be stored in econtext->ecxt_outertuple.
 */
HashJoinTuple
ExecScanHashBucket(HashState *hashState, HashJoinState *hjstate,
		ExprContext *econtext)
{
	List	   *hjclauses = hjstate->hashqualclauses;
	HashJoinTable hashtable = hjstate->hj_HashTable;
	HashJoinTuple hashTuple = hjstate->hj_CurTuple;
	uint32		hashvalue = hjstate->hj_CurHashValue;

	START_MEMORY_ACCOUNT(hashState->ps.plan->memoryAccount);
	{
	/*
	 * hj_CurTuple is NULL to start scanning a new bucket, or the address of
	 * the last tuple returned from the current bucket.
	 */
	if (hashTuple == NULL)
	{
		/* if bloom filter fails, then no match - don't even bother to scan */
		if (gp_hashjoin_bloomfilter == 0 || 0 != (hashtable->bloom[hjstate->hj_CurBucketNo] & BLOOMVAL(hashvalue)))
			hashTuple = hashtable->buckets[hjstate->hj_CurBucketNo];
	}
	else
		hashTuple = hashTuple->next;

	while (hashTuple != NULL)
	{
		if (hashTuple->hashvalue == hashvalue)
		{
			TupleTableSlot *inntuple;

			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			inntuple = ExecStoreMemTuple(HJTUPLE_MINTUPLE(hashTuple),
					hjstate->hj_HashTupleSlot,
					false);	/* do not pfree */
			econtext->ecxt_innertuple = inntuple;

			/* reset temp memory each time to avoid leaks from qual expr */
			ResetExprContext(econtext);

			if (ExecQual(hjclauses, econtext, false))
			{
				hjstate->hj_CurTuple = hashTuple;
				return hashTuple;
			}
		}

		hashTuple = hashTuple->next;
	}
	}
	END_MEMORY_ACCOUNT();
	/*
	 * no match
	 */
	return NULL;
}

/*
 * ExecHashTableReset
 *
 *		reset hash table header for new batch
 */
void
ExecHashTableReset(HashState *hashState, HashJoinTable hashtable)
{	
	MemoryContext oldcxt;
	int			nbuckets = 0;

	START_MEMORY_ACCOUNT(hashState->ps.plan->memoryAccount);
	{
	Assert(hashtable);
	Assert(!hashtable->eagerlyReleased);

	nbuckets = hashtable->nbuckets;

	/*
	 * Release all the hash buckets and tuples acquired in the prior pass, and
	 * reinitialize the context for a new pass.
	 */
	MemoryContextReset(hashtable->batchCxt);
	oldcxt = MemoryContextSwitchTo(hashtable->batchCxt);

	/* Reallocate and reinitialize the hash bucket headers. */
	hashtable->buckets = (HashJoinTuple *)
		palloc0(nbuckets * sizeof(HashJoinTuple));

	if(gp_hashjoin_bloomfilter != 0)
		hashtable->bloom = (uint64*) palloc0(nbuckets * sizeof(uint64));

	hashtable->batches[hashtable->curbatch]->innerspace = 0;
	hashtable->batches[hashtable->curbatch]->innertuples = 0;
	hashtable->totalTuples = 0;

	MemoryContextSwitchTo(oldcxt);
	}
	END_MEMORY_ACCOUNT();
}

void
ExecReScanHash(HashState *node, ExprContext *exprCtxt)
{
	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (((PlanState *) node)->lefttree->chgParam == NULL)
		ExecReScan(((PlanState *) node)->lefttree, exprCtxt);
}


/*
 * ExecHashTableExplainInit
 *      Called after ExecHashTableCreate to set up EXPLAIN ANALYZE reporting.
 */
void
ExecHashTableExplainInit(HashState *hashState, HashJoinState *hjstate,
                         HashJoinTable  hashtable)
{
    MemoryContext   oldcxt;
    int             nbatch = Max(hashtable->nbatch, 1);

    START_MEMORY_ACCOUNT(hashState->ps.plan->memoryAccount);
    {
    /* Switch to a memory context that survives until ExecutorEnd. */
    oldcxt = MemoryContextSwitchTo(hjstate->js.ps.state->es_query_cxt);

    /* Request a callback at end of query. */
    hjstate->js.ps.cdbexplainfun = ExecHashTableExplainEnd;

    /* Create workarea and attach it to the HashJoinTable. */
    hashtable->stats = (HashJoinTableStats *)palloc0(sizeof(*hashtable->stats));
    hashtable->stats->endedbatch = -1;

    /* Create per-batch statistics array. */
    hashtable->stats->batchstats =
        (HashJoinBatchStats *)palloc0(nbatch * sizeof(hashtable->stats->batchstats[0]));
    hashtable->stats->nbatchstats = nbatch;

    /* Restore caller's memory context. */
    MemoryContextSwitchTo(oldcxt);
    }
    END_MEMORY_ACCOUNT();
}                               /* ExecHashTableExplainInit */


/*
 * ExecHashTableExplainEnd
 *      Called before ExecutorEnd to finish EXPLAIN ANALYZE reporting.
 */
void
ExecHashTableExplainEnd(PlanState *planstate, struct StringInfoData *buf)
{
    HashJoinState      *hjstate = (HashJoinState *)planstate;
    HashJoinTable       hashtable = hjstate->hj_HashTable;
    HashJoinTableStats *stats;
    Instrumentation    *jinstrument = hjstate->js.ps.instrument;
    int                 total_buckets;
    int                 i;

    if (!hashtable ||
        !hashtable->stats ||
        hashtable->nbatch < 1 ||
        !jinstrument)
        return;

    stats = hashtable->stats;

	/* Check batchstats not null: If nodeHash failed to palloc batchstats, it will
	 * throw.  Posgres will catch and handle it, but no matter what, postgres will 
	 * try to get some explain results.  We must check here in this case or we will
	 * segv.
	 */
	if(stats->batchstats == NULL)
		return;

	if (!hashtable->eagerlyReleased)
	{		
		HashState *hashState = (HashState *) innerPlanState(hjstate);

		/* Report on batch in progress, in case the join is being ended early. */
		ExecHashTableExplainBatchEnd(hashState, hashtable);

		/* Report executor memory used by our memory context. */
		jinstrument->execmemused +=
				(double)MemoryContextGetPeakSpace(hashtable->hashCxt);
	}
	else
	{
		/**
		 * Memory has been eagerly released. We can't get statistics
		 * from the memory context. We approximate from stats structure.
		 */

		Assert(gp_eager_hashtable_release);
		jinstrument->execmemused +=
				(double) stats->workmem_max;
	}
	
    /* Report actual work_mem high water mark. */
    jinstrument->workmemused = Max(jinstrument->workmemused, stats->workmem_max);

    /* How much work_mem would suffice to hold all inner tuples in memory? */
    if (hashtable->nbatch > 1)
    {
        uint64  workmemwanted = 0;

        /* Space actually taken by hash rows in completed batches... */
        for (i = 0; i <= stats->endedbatch; i++)
            workmemwanted += stats->batchstats[i].hashspace_final;

        /* ... plus workfile size for original batches not reached, plus... */
        for (; i < hashtable->nbatch_original; i++)
            workmemwanted += stats->batchstats[i].innerfilesize;

        /* ... rows spilled to unreached oflo batches, in case quitting early */
        for (; i < stats->nbatchstats; i++)
            workmemwanted += stats->batchstats[i].spillspace_in;

        /*
         * Sometimes workfiles are used even though all the data would fit
         * in work_mem.  For example, if the planner overestimated the inner
         * rel size, it might have instructed us to use more initial batches
         * than were actually needed, causing unnecessary workfile I/O.  To
         * avoid this I/O, the user would have to increase work_mem based on
         * the planner's estimate rather than our runtime observations.  For
         * now, we don't try to second-guess the planner; just keep quiet.
         */
        if (workmemwanted > PlanStateOperatorMemKB(planstate) * 1024L)
            jinstrument->workmemwanted =
                Max(jinstrument->workmemwanted, workmemwanted);
    }

    /* Report workfile I/O statistics. */
    if (hashtable->nbatch > 1)
    {
		if (!hjstate->cached_workfiles_loaded)
		{
			ExecHashTableExplainBatches(hashtable, buf, 0, 1, "Initial");
			ExecHashTableExplainBatches(hashtable,
										buf,
										1,
										hashtable->nbatch_original,
										"Initial");
			ExecHashTableExplainBatches(hashtable,
										buf,
										hashtable->nbatch_original,
										hashtable->nbatch_outstart,
										"Overflow");
			ExecHashTableExplainBatches(hashtable,
										buf,
										hashtable->nbatch_outstart,
										hashtable->nbatch,
										"Secondary Overflow");
		}
		else
		{
			ExecHashTableExplainBatches(hashtable, buf, 0, hashtable->nbatch, "Reuse");
		}
    }

    /* Report hash chain statistics. */
    total_buckets = stats->nonemptybatches * hashtable->nbuckets;
    if (total_buckets > 0)
    {
        appendStringInfo(buf,
                         "Hash chain length"
                         " %.1f avg, %.0f max, using %d of %d buckets.  ",
                         cdbexplain_agg_avg(&stats->chainlength),
                         stats->chainlength.vmax,
                         stats->chainlength.vcnt,
                         total_buckets);
        if (hashtable->nbatch > stats->nonemptybatches)
            appendStringInfo(buf,
                             "Skipped %d empty batches.",
                             hashtable->nbatch - stats->nonemptybatches);
        appendStringInfoChar(buf, '\n');
    }
}                               /* ExecHashTableExplainEnd */


/*
 * ExecHashTableExplainBatches
 *      Report summary of EXPLAIN ANALYZE stats for a set of batches.
 */
void
ExecHashTableExplainBatches(HashJoinTable   hashtable,
                            StringInfo      buf,
                            int             ibatch_begin,
                            int             ibatch_end,
                            const char     *title)
{
    HashJoinTableStats *stats = hashtable->stats;
    CdbExplain_Agg      irdbytes;
    CdbExplain_Agg      iwrbytes;
    CdbExplain_Agg      ordbytes;
    CdbExplain_Agg      owrbytes;
    int                 i;

    if (ibatch_begin >= ibatch_end)
        return;

    Assert(ibatch_begin >= 0 &&
           ibatch_end <= hashtable->nbatch &&
           hashtable->nbatch <= stats->nbatchstats &&
           stats->batchstats != NULL);

    cdbexplain_agg_init0(&irdbytes);
    cdbexplain_agg_init0(&iwrbytes);
    cdbexplain_agg_init0(&ordbytes);
    cdbexplain_agg_init0(&owrbytes);

    /* Add up the batch stats. */
    char hostname[SEGMENT_IDENTITY_NAME_LENGTH];
    gethostname(hostname,SEGMENT_IDENTITY_NAME_LENGTH);
    for (i = ibatch_begin; i < ibatch_end; i++)
    {
        HashJoinBatchStats *bs = &stats->batchstats[i];

        cdbexplain_agg_upd(&irdbytes, (double)bs->irdbytes, i, hostname);
        cdbexplain_agg_upd(&iwrbytes, (double)bs->iwrbytes, i, hostname);
        cdbexplain_agg_upd(&ordbytes, (double)bs->ordbytes, i, hostname);
        cdbexplain_agg_upd(&owrbytes, (double)bs->owrbytes, i, hostname);
    }

    if (iwrbytes.vcnt + irdbytes.vcnt + owrbytes.vcnt + ordbytes.vcnt > 0)
    {
        if (ibatch_begin == ibatch_end - 1)
            appendStringInfo(buf,
                             "%s batch %d:\n",
                             title,
                             ibatch_begin);
        else
            appendStringInfo(buf,
                             "%s batches %d..%d:\n",
                             title,
                             ibatch_begin,
                             ibatch_end - 1);
    }

    /* Inner bytes read from workfile */
    if (irdbytes.vcnt > 0)
    {
        appendStringInfo(buf,
                         "  Read %.0fK bytes from inner workfile",
                         ceil(irdbytes.vsum / 1024));
        if (irdbytes.vcnt > 1)
            appendStringInfo(buf,
                             ": %.0fK avg x %d nonempty batches"
                             ", %.0fK max",
                             ceil(cdbexplain_agg_avg(&irdbytes)/1024),
                             irdbytes.vcnt,
                             ceil(irdbytes.vmax / 1024));
        appendStringInfoString(buf, ".\n");
    }

    /* Inner rel bytes spilled to workfile */
    if (iwrbytes.vcnt > 0)
    {
        appendStringInfo(buf,
                         "  Wrote %.0fK bytes to inner workfile",
                         ceil(iwrbytes.vsum / 1024));
        if (iwrbytes.vcnt > 1)
            appendStringInfo(buf,
                             ": %.0fK avg x %d overflowing batches"
                             ", %.0fK max",
                             ceil(cdbexplain_agg_avg(&iwrbytes)/1024),
                             iwrbytes.vcnt,
                             ceil(iwrbytes.vmax / 1024));
        appendStringInfoString(buf, ".\n");
    }

    /* Outer bytes read from workfile */
    if (ordbytes.vcnt > 0)
    {
        appendStringInfo(buf,
                         "  Read %.0fK bytes from outer workfile",
                         ceil(ordbytes.vsum / 1024));
        if (ordbytes.vcnt > 1)
            appendStringInfo(buf,
                             ": %.0fK avg x %d nonempty batches"
                             ", %.0fK max",
                             ceil(cdbexplain_agg_avg(&ordbytes)/1024),
                             ordbytes.vcnt,
                             ceil(ordbytes.vmax / 1024));
        appendStringInfoString(buf, ".\n");
    }

    /* Outer rel bytes spilled to workfile */
    if (owrbytes.vcnt > 0)
    {
        appendStringInfo(buf,
                         "  Wrote %.0fK bytes to outer workfile",
                         ceil(owrbytes.vsum / 1024));
        if (owrbytes.vcnt > 1)
            appendStringInfo(buf,
                             ": %.0fK avg x %d overflowing batches"
                             ", %.0fK max",
                             ceil(cdbexplain_agg_avg(&owrbytes)/1024),
                             owrbytes.vcnt,
                             ceil(owrbytes.vmax / 1024));
        appendStringInfoString(buf, ".\n");
    }
}                               /* ExecHashTableExplainBatches */


/*
 * ExecHashTableExplainBatchEnd
 *      Called at end of each batch to collect statistics for EXPLAIN ANALYZE.
 */
void
ExecHashTableExplainBatchEnd(HashState *hashState, HashJoinTable hashtable)
{
    int                 curbatch = hashtable->curbatch;
    HashJoinTableStats *stats = hashtable->stats;
    HashJoinBatchStats *batchstats = &stats->batchstats[curbatch];
    HashJoinBatchData  *batch = NULL;
    int                 i;
    
    START_MEMORY_ACCOUNT(hashState->ps.plan->memoryAccount);
    {
    Assert(!hashtable->eagerlyReleased);
    Assert(hashtable->batches);

    batch = hashtable->batches[curbatch];

    /* Already reported on this batch? */
    if ( stats->endedbatch == curbatch 
			|| curbatch >= hashtable->nbatch)
        return;
    stats->endedbatch = curbatch;

    /* Update high-water mark for work_mem actually used at one time. */
    if (stats->workmem_max < batch->innerspace)
        stats->workmem_max = batch->innerspace;

    /* Final size of hash table for this batch */
    batchstats->hashspace_final = batch->innerspace;

    /* Collect workfile I/O statistics. */
    if (hashtable->nbatch > 1)
    {
        uint64      owrbytes = 0;
        uint64      iwrbytes = 0;

        Assert(stats->batchstats &&
               hashtable->nbatch <= stats->nbatchstats);

        /* How much was read from inner workfile for current batch? */
        batchstats->irdbytes = batchstats->innerfilesize;

        /* How much was read from outer workfiles for current batch? */
		if (hashtable->batches[curbatch]->outerside.workfile != NULL)
            batchstats->ordbytes =
				ExecWorkFile_Tell64(hashtable->batches[curbatch]->outerside.workfile);

        /*
		 * How much was written to workfiles for the remaining batches?
		 * In workfile caching, there is no need to write to the remaining batches.
		 */
		if (!hashtable->hjstate->cached_workfiles_loaded)
		{
			for (i = curbatch + 1; i < hashtable->nbatch; i++)
			{
				HashJoinBatchData  *batch = hashtable->batches[i];
				HashJoinBatchStats *bs = &stats->batchstats[i];
				uint64              filebytes = 0;
				
				if (batch->outerside.workfile != NULL)
					filebytes = ExecWorkFile_Tell64(batch->outerside.workfile); 
				
				Assert(filebytes >= bs->outerfilesize);
				owrbytes += filebytes - bs->outerfilesize;
				bs->outerfilesize = filebytes;
				
				filebytes = 0;
				
				if (batch->innerside.workfile)
					filebytes = ExecWorkFile_Tell64(batch->innerside.workfile);
				
				Assert(filebytes >= bs->innerfilesize);
				iwrbytes += filebytes - bs->innerfilesize;
				bs->innerfilesize = filebytes;
			}
			batchstats->owrbytes = owrbytes;
			batchstats->iwrbytes = iwrbytes;
		}
    }                           /* give workfile I/O statistics */

    /* Collect hash chain statistics. */
    if (batch->innertuples > 0)
    {
        stats->nonemptybatches++;
        char hostname[SEGMENT_IDENTITY_NAME_LENGTH];
        gethostname(hostname,SEGMENT_IDENTITY_NAME_LENGTH);
        for (i = 0; i < hashtable->nbuckets; i++)
        {
            HashJoinTuple   hashtuple = hashtable->buckets[i];
            int             chainlength;

            if (hashtuple)
            {
                for (chainlength = 0; hashtuple; hashtuple = hashtuple->next)
                    chainlength++;
                cdbexplain_agg_upd(&stats->chainlength, chainlength, i, hostname);
            }
        }
    }
    }
    END_MEMORY_ACCOUNT();
}                               /* ExecHashTableExplainBatchEnd */

void
initGpmonPktForHash(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, Hash));

	{
		Assert(GPMON_HASH_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_Hash,
							 (int64)planNode->plan_rows,
							 NULL); 
	}
}
