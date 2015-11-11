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
 * execDynamicScan.c
 *	  Support routines for iterating through dynamically chosen partitions of a relation
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbpartition.h"
#include "executor/execDynamicScan.h"
#include "executor/nodeIndexscan.h"
#include "executor/instrument.h"
#include "executor/execIndexscan.h"
#include "parser/parsetree.h"
#include "utils/memutils.h"
#include "access/genam.h"

static bool
IndexScan_RemapLogicalIndexInfo(IndexScanState *indexScanState);

static bool
DynamicScan_RemapIndexScanVars(IndexScanState *indexScanState, bool initQual, bool initTargetList);

static inline DynamicPartitionIterator*
DynamicScan_GetIterator(ScanState *scanState);

static Oid
DynamicScan_GetNextPartitionOid(ScanState *scanState, DynamicPartitionIterator *iterator, int32 numSelectors);

static void
DynamicScan_UpdateIteratorPartitionState(DynamicPartitionIterator *iterator, Oid newOid);

static void
DynamicScan_InitScanStateForNewPartition(ScanState *scanState, DynamicPartitionIterator *iterator);

static void
DynamicScan_InitExpressions(ScanState *scanState, DynamicPartitionIterator *iterator);

static void
DynamicScan_FinishInitialization(ScanState *scanState, PartitionInitMethod *partitionInitMethod);

static bool
DynamicScan_InitNextPartition(ScanState *scanState, PartitionInitMethod *partitionInitMethod);

static bool
DynamicScan_InitSingleRelation(ScanState *scanState, PartitionInitMethod *partitionInitMethod);

static void
DynamicScan_CreateIterator(ScanState *scanState, Scan *scan);

static void
DynamicScan_EndIterator(ScanState *scanState);

static inline void
DynamicScan_CleanupOneRelation(ScanState *scanState, PartitionEndMethod *partitionEndMethod);

static void
DynamicScan_EndCurrentScan(ScanState *scanState, PartitionEndMethod *partitionEndMethod);

static bool
DynamicScan_RemapExpression(ScanState *scanState, Node *expr);

static void
DynamicScan_CleanupOneIndexRelation(IndexScanState *indexScanState);

static Oid
DynamicScan_GetIndexOid(IndexScanState *indexScanState, Oid tableOid);

static Oid
DynamicScan_GetTableOid(ScanState *scanState);

static bool
DynamicScan_OpenIndexRelation(IndexScanState *scanState, Oid tableOid);

static void
DynamicScan_InitIndexExpressions(IndexScanState *indexScanState, bool initQual, bool initTargetList);

static void
DynamicScan_PrepareIndexScanKeys(IndexScanState *indexScanState, bool initQual, bool initTargetList, bool supportsArrayKeys);

static void
DynamicScan_PrepareExpressionContext(IndexScanState *indexScanState);

static void
DynamicScan_InitRuntimeKeys(IndexScanState *indexScanState);

/*
 * -------------------------------------
 * Static functions
 * -------------------------------------
 */

/*
 * DynamicScan_RemapIndexScanVars
 * 		Remaps the columns in the logical index descriptor, target list,
 * 		and qual to handle column alignment issues because of dropped columns.
 *
 * 		Returns true if remapping was needed.
 */
static bool
DynamicScan_RemapIndexScanVars(IndexScanState *indexScanState, bool initQual, bool initTargetList)
{
	IndexScan *indexScan = (IndexScan *) indexScanState->ss.ps.plan;

	bool remappedLogicalIndexInfo = IndexScan_RemapLogicalIndexInfo(indexScanState);

	if (remappedLogicalIndexInfo)
	{
		DynamicScan_RemapExpression((ScanState *)indexScanState, (Node*)indexScan->indexqual);

		if (initQual)
		{
			DynamicScan_RemapExpression((ScanState *)indexScanState, (Node*)indexScan->scan.plan.qual);
		}

		if (initTargetList)
		{
			DynamicScan_RemapExpression((ScanState *)indexScanState, (Node*)indexScan->scan.plan.targetlist);
		}
	}

	return remappedLogicalIndexInfo;
}

/*
 * DynamicScan_GetIterator
 * 		Returns the current iterator for the scanState's partIndex.
 */
static inline DynamicPartitionIterator*
DynamicScan_GetIterator(ScanState *scanState)
{
	Assert(isDynamicScan((Scan *)scanState->ps.plan));
	Assert(NULL != scanState->ps.state->dynamicTableScanInfo);

	int partIndex = ((Scan*)scanState->ps.plan)->partIndex;
	Assert(partIndex <= scanState->ps.state->dynamicTableScanInfo->numScans);

	DynamicPartitionIterator *iterator = scanState->ps.state->dynamicTableScanInfo->iterators[partIndex - 1];

	Assert(NULL != iterator);

	return iterator;
}

/*
 * DynamicScan_GetNextPartitionOid
 * 		Returns the oid of the next partition.
 */
static Oid
DynamicScan_GetNextPartitionOid(ScanState *scanState, DynamicPartitionIterator *iterator, int32 numSelectors)
{
	Oid pid = InvalidOid;
	while (InvalidOid == pid)
	{
		PartOidEntry *partOidEntry = hash_seq_search(iterator->partitionIterator);

		if (NULL == partOidEntry)
		{
			iterator->shouldCallHashSeqTerm = false;
			iterator->attMapRelOid = InvalidOid;

			scanState->scan_state = SCAN_DONE;

			return InvalidOid;
		}

		if (list_length(partOidEntry->selectorList) == numSelectors)
		{
			pid = partOidEntry->partOid;
		}
	}

	return pid;
}

/*
 * DynamicScan_UpdateIteratorPartitionState
 * 		Saves the relation that we need to scan in the iterator state. We also
 * 		save the column map to reconcile column alignment because of dropped columns.
 */
static void
DynamicScan_UpdateIteratorPartitionState(DynamicPartitionIterator *iterator, Oid newOid)
{
	Oid oldOid = iterator->attMapRelOid;

	Assert(OidIsValid(newOid));

	/* Make sure we haven't already updated the column mapping of the iterator */
	Assert(oldOid != newOid);

	/* We must have cleaned the current relation before starting another one */
	Assert(NULL == iterator->currentRelation);

	iterator->currentRelation = OpenScanRelationByOid(newOid);

	TupleDesc newTupDesc = RelationGetDescr(iterator->currentRelation);

	Relation lastScannedRel = OpenScanRelationByOid(oldOid);
	TupleDesc oldTupDesc = RelationGetDescr(lastScannedRel);
	CloseScanRelation(lastScannedRel);

	AttrNumber	*attMap = varattnos_map(oldTupDesc, newTupDesc);

	if (NULL != iterator->attMap)
	{
		pfree(iterator->attMap);
	}

	iterator->attMap = attMap;
	iterator->attMapRelOid = newOid;
}

/*
 * DynamicScan_InitScanStateForNewPartition
 * 		Initializes various properties of the scan state for a new
 * 		partition such as opening the relation, mapping dropped attributes
 * 		for qual and targetlist, setting the tuple slot's properties and
 * 		updating the tableType of the scanState.
 */
static void
DynamicScan_InitScanStateForNewPartition(ScanState *scanState, DynamicPartitionIterator *iterator)
{
	Assert(NULL == scanState->ss_currentRelation);

	scanState->ss_currentRelation = iterator->currentRelation;

	TupleDesc newTupDesc = RelationGetDescr(scanState->ss_currentRelation);

	ExecAssignScanType(scanState, newTupDesc);

	DynamicScan_RemapExpression(scanState, (Node*)scanState->ps.plan->qual);
	DynamicScan_RemapExpression(scanState, (Node*)scanState->ps.plan->targetlist);

	Oid newOid = RelationGetRelid(iterator->currentRelation);

	/*
	 * Inside ExecInitScanTupleSlot() we set the tuple table slot's oid
	 * to range table entry's relid, which for partitioned table always set
	 * to parent table's oid. In queries where we need to read table oids
	 * (MPP-20736) we use the tuple table slot's saved oid (refer to slot_getsysattr()).
	 * This wrongly returns parent oid, instead of partition oid. Therefore,
	 * to return correct partition oid, we need to update
	 * our tuple table slot's oid to reflect the partition oid.
	 */
	for (int i = 0; i < DYNAMIC_SCAN_NSLOTS; i++)
	{
		scanState->ss_ScanTupleSlot[i].tts_tableOid = newOid;
	}

	scanState->tableType = getTableType(scanState->ss_currentRelation);
}

/*
 * DynamicScan_InitExpressions
 *		Initializes the expression.
 *
 *		Note: we have qual and targetlist in the plan which
 *		we need to initialize to create plan state's qual
 *		and eval to evaluate the expression.
 */
static void
DynamicScan_InitExpressions(ScanState *scanState, DynamicPartitionIterator *iterator)
{
	/*
	 * We only initialize expression if this is the first partition
	 * or if the column mapping changes between two partitions.
	 * Otherwise, we reuse the previously initialized expression.
	 */
	if (iterator->firstPartition || NULL != iterator->attMap)
	{
		MemoryContextReset(iterator->partitionMemoryContext);

		/*
		 * Switch to partition memory context to prevent memory leak for
		 * per-partition data structures.
		 */
		MemoryContext oldCxt = MemoryContextSwitchTo(iterator->partitionMemoryContext);

		/* Initialize child expressions */
		scanState->ps.qual = (List *)ExecInitExpr((Expr *)scanState->ps.plan->qual, (PlanState*)scanState);
		scanState->ps.targetlist = (List *)ExecInitExpr((Expr *)scanState->ps.plan->targetlist, (PlanState*)scanState);
		ExecAssignScanProjectionInfo(scanState);

		MemoryContextSwitchTo(oldCxt);
	}
}

/*
 * DynamicScan_FinishInitialization
 *		Finishes the initialization phase and advances the scan_state
 *		to SCAN_NEXT. Note: for non-partitioned case we don't do anything
 *		extra here. For partitioned case, however, we create the partition
 *		iterator.
 */
static void
DynamicScan_FinishInitialization(ScanState *scanState, PartitionInitMethod *partitionInitMethod)
{
	Assert(SCAN_INIT == scanState->scan_state);

	Scan *scan = (Scan *)scanState->ps.plan;
	if (isDynamicScan(scan))
	{
		DynamicScan_CreateIterator(scanState, scan);
	}

	scanState->scan_state = SCAN_NEXT;
}

/*
 * DynamicScan_InitNextPartition
 *		Prepares the next partition for scanning by calling various
 *		helper methods to open relation, map dropped attributes,
 *		initialize expressions etc.
 */
static bool
DynamicScan_InitNextPartition(ScanState *scanState, PartitionInitMethod *partitionInitMethod)
{
	Assert(isDynamicScan((Scan *)scanState->ps.plan));

	Scan *scan = (Scan *)scanState->ps.plan;

	Assert(SCAN_NEXT == scanState->scan_state);

	DynamicTableScanInfo *partitionInfo = scanState->ps.state->dynamicTableScanInfo;

	Assert(partitionInfo->numScans >= scan->partIndex);

	DynamicPartitionIterator *iterator = partitionInfo->iterators[scan->partIndex - 1];
	int32 numSelectors = list_nth_int(partitionInfo->numSelectorsPerScanId, scan->partIndex);

	Assert(NULL != iterator);

	Oid partOid = DynamicScan_GetNextPartitionOid(scanState, iterator, numSelectors);

	if (!OidIsValid(partOid))
	{
		Assert(SCAN_DONE == scanState->scan_state);
		return false;
	}

	/* Collect number of partitions scanned in EXPLAIN ANALYZE */
	if(NULL != scanState->ps.instrument)
	{
		Instrumentation *instr = scanState->ps.instrument;
		instr->numPartScanned ++;
	}

	DynamicScan_UpdateIteratorPartitionState(iterator, partOid);
	DynamicScan_InitScanStateForNewPartition(scanState, iterator);
	DynamicScan_InitExpressions(scanState, iterator);

	bool initExpressions = iterator->firstPartition || iterator->attMap;

	partitionInitMethod(scanState, initExpressions);

	scanState->scan_state = SCAN_SCAN;

	return true;
}

/*
 * DynamicScan_InitSingleRelation
 *		Prepares a single relation for scanning by calling various
 *		helper methods to open relation, initialize expressions etc.
 *
 *		Note: this is for the non-partitioned relations.
 */
static bool
DynamicScan_InitSingleRelation(ScanState *scanState, PartitionInitMethod *partitionInitMethod)
{
	Assert(!isDynamicScan((Scan *)scanState->ps.plan));

	Assert(SCAN_SCAN != scanState->scan_state);
	Assert(SCAN_END != scanState->scan_state);

	if (SCAN_DONE != scanState->scan_state)
	{
		Assert(SCAN_NEXT == scanState->scan_state);
		/* In non-partitioned case, we only begin scan if we aren't already scanning it or we are not done */

		/* Open the relation and initalize the expressions (targetlist, qual etc.) */
		InitScanStateRelationDetails(scanState, scanState->ps.plan, scanState->ps.state);

		partitionInitMethod(scanState, true);
		scanState->scan_state = SCAN_SCAN;

		return true;
	}

	Assert(SCAN_DONE == scanState->scan_state);
	/* The non-partitioned table is already scanned, so nothing more to scan */
	return false;
}

/*
 * DynamicScan_CreateIterator
 * 		Creates an iterator state (i.e., DynamicPartitionIterator)
 * 		and saves it to the estate's DynamicTableScanInfo.
 */
static void
DynamicScan_CreateIterator(ScanState *scanState, Scan *scan)
{
	EState *estate = scanState->ps.state;

	Assert(NULL != estate);

	DynamicTableScanInfo *partitionInfo = estate->dynamicTableScanInfo;

	/*
	 * Ensure that the dynahash exists even if the partition selector
	 * didn't choose any partition for current scan node [MPP-24169].
	 */
	InsertPidIntoDynamicTableScanInfo(scan->partIndex, InvalidOid, InvalidPartitionSelectorId);

	Assert(NULL != estate->dynamicTableScanInfo->pidIndexes);
	Assert(partitionInfo->numScans >= scan->partIndex);
	Assert(NULL == partitionInfo->iterators[scan->partIndex - 1]);

	Oid reloid = getrelid(scan->scanrelid, estate->es_range_table);
	Assert(OidIsValid(reloid));

	DynamicPartitionIterator *iterator = palloc(sizeof(DynamicPartitionIterator));
	iterator->firstPartition = true;
	iterator->currentRelation = NULL;
	iterator->attMapRelOid = reloid;
	iterator->partitionMemoryContext = AllocSetContextCreate(CurrentMemoryContext,
			 "DynamicTableScanPerPartition",
			 ALLOCSET_DEFAULT_MINSIZE,
			 ALLOCSET_DEFAULT_INITSIZE,
			 ALLOCSET_DEFAULT_MAXSIZE);
	iterator->partitionOids = partitionInfo->pidIndexes[scan->partIndex - 1];
	Assert(iterator->partitionOids != NULL);
	iterator->attMap = NULL;
	iterator->shouldCallHashSeqTerm = true;

	HASH_SEQ_STATUS *partitionIterator = palloc(sizeof(HASH_SEQ_STATUS));
	hash_seq_init(partitionIterator, iterator->partitionOids);

	iterator->partitionIterator = partitionIterator;

	partitionInfo->iterators[scan->partIndex - 1] = iterator;
}

/*
 * DynamicScan_EndIterator
 * 		Frees the partition iterator for a scanState.
 */
static void
DynamicScan_EndIterator(ScanState *scanState)
{
	Assert(NULL != scanState);

	/*
	 * For EXPLAIN of a plan, we may never finish the initialization,
	 * and end up calling the End method directly.In such cases, we
	 * don't have any iterator to end.
	 */
	if (SCAN_INIT == scanState->scan_state)
	{
		return;
	}

	Scan *scan = (Scan *)scanState->ps.plan;

	DynamicTableScanInfo *partitionInfo = scanState->ps.state->dynamicTableScanInfo;

	Assert(partitionInfo->numScans >= scan->partIndex);
	DynamicPartitionIterator *iterator = partitionInfo->iterators[scan->partIndex - 1];

	Assert(NULL != iterator);

	if (iterator->shouldCallHashSeqTerm)
	{
		hash_seq_term(iterator->partitionIterator);
	}

	pfree(iterator->partitionIterator);

	if (NULL != iterator->attMap)
	{
		pfree(iterator->attMap);
	}

	MemoryContextDelete(iterator->partitionMemoryContext);

	pfree(iterator);

	partitionInfo->iterators[scan->partIndex - 1] = NULL;
}

/*
 * DynamicScan_CleanupOneRelation
 *		Cleans up a relation and releases all locks.
 */
static inline void
DynamicScan_CleanupOneRelation(ScanState *scanState, PartitionEndMethod *partitionEndMethod)
{
	Assert(NULL != scanState);
	Scan *scan = (Scan *)scanState->ps.plan;

	if (0 != (scanState->scan_state & SCAN_SCAN))
	{
		partitionEndMethod(scanState);

		Assert(NULL != scanState->ss_currentRelation);
		ExecCloseScanRelation(scanState->ss_currentRelation);
		scanState->ss_currentRelation = NULL;

		if (isDynamicScan(scan))
		{
			DynamicPartitionIterator *iterator = DynamicScan_GetIterator(scanState);
			iterator->currentRelation = NULL;
			iterator->firstPartition = false;

			scanState->scan_state = SCAN_NEXT;
		}
		else
		{
			/* No partition, so no more to scan */
			scanState->scan_state = SCAN_DONE;
		}
	}
}

/*
 * DynamicScan_EndCurrentScan
 *		Ends the current scan by cleaning up current relation
 *		and freeing up iterator states.
 */
static void
DynamicScan_EndCurrentScan(ScanState *scanState, PartitionEndMethod *partitionEndMethod)
{
	Assert(NULL != scanState);
	Assert(NULL != partitionEndMethod);
	Assert(SCAN_END != scanState->scan_state);

	Scan *scan = (Scan *)scanState->ps.plan;

	DynamicScan_CleanupOneRelation(scanState, partitionEndMethod);

	if (isDynamicScan(scan))
	{
		DynamicScan_EndIterator(scanState);
	}

	scanState->scan_state = SCAN_END;
}

/*
 * DynamicScan_RemapExpression
 * 		Re-maps the expression using the attMap of the partition iterator.
 */
static bool
DynamicScan_RemapExpression(ScanState *scanState, Node *expr)
{
	if (!isDynamicScan((Scan *)scanState->ps.plan))
	{
		return false;
	}

	DynamicPartitionIterator *iterator = DynamicScan_GetIterator(scanState);

	Assert(NULL != iterator);

	if (NULL != iterator->attMap)
	{
		change_varattnos_of_a_varno((Node*)expr, iterator->attMap, ((Scan *)scanState->ps.plan)->scanrelid);

		return true;
	}

	return false;
}

/*
 * DynamicScan_GetIndexOid
 * 		Returns the Oid of a suitable index for the IndexScan.
 *
 * 		Note: this assumes that the indexScan->logicalIndexInfo varattno
 * 		are already mapped.
 *
 * 		This method will return the indexScan->indexid if called during
 * 		the SCAN_INIT state or for non-partitioned case.
 */
static Oid
DynamicScan_GetIndexOid(IndexScanState *indexScanState, Oid tableOid)
{
	IndexScan *indexScan = (IndexScan *) indexScanState->ss.ps.plan;

	/*
	 * We return the plan node's indexid for non-partitioned case.
	 * For partitioned case, we also return the plan node's indexid
	 * if we are in the initialization phase (i.e., we don't yet know
	 * which partitions to scan).
	 */
	if (!isDynamicScan(&indexScan->scan) || SCAN_INIT == indexScanState->ss.scan_state)
	{
		return indexScan->indexid;
	}

	Assert(NULL != indexScan->logicalIndexInfo);
	Assert(OidIsValid(tableOid));

	/*
	 * The is the oid of the partition of an *index*. Note: a partitioned table
	 * has a root and a set of partitions (may be multi-level). An index
	 * on a partitioned table also has a root and a set of index partitions.
	 * We started at table level, and now we are fetching the oid of an index
	 * partition.
	 */
	Oid indexOid = getPhysicalIndexRelid(indexScan->logicalIndexInfo, tableOid);

	Assert(OidIsValid(indexOid));

	return indexOid;
}

/*
 * DynamicScan_GetTableOid
 * 		Returns the Oid of the table/partition to scan.
 *
 *		For partitioned case this method returns InvalidOid
 *		if the partition iterator hasn't been initialized yet.
 */
static Oid
DynamicScan_GetTableOid(ScanState *scanState)
{
	/* For non-partitioned scan, just lookup the RTE */
	if (!isDynamicScan((Scan *)scanState->ps.plan))
	{
		return getrelid(((Scan *)scanState->ps.plan)->scanrelid, scanState->ps.state->es_range_table);
	}

	/* We are yet to initialize the iterator, so return InvalidOid */
	if (SCAN_INIT == scanState->scan_state)
	{
		return InvalidOid;
	}

	/* Get the iterator and look up the oid of the current relation */
	DynamicPartitionIterator *iterator = DynamicScan_GetIterator(scanState);
	Assert(NULL != iterator);
	Assert(OidIsValid(iterator->attMapRelOid));

	return iterator->attMapRelOid;
}

/*
 * DynamicScan_OpenIndexRelation
 * 		Opens the index relation of the scanState using proper locks.
 *
 * 		Returns false if the previously opened index relation can be
 * 		reused.
 *
 * 		Otherwise it closes the previously opened index relation and
 * 		opens the newly requested one, and returns true.
 */
static bool
DynamicScan_OpenIndexRelation(IndexScanState *scanState, Oid tableOid)
{
	/* Look up the correct index oid for the provided tableOid */
	Oid indexOid = DynamicScan_GetIndexOid(scanState, tableOid);
	Assert(OidIsValid(indexOid));

	/*
	 * If we already have an open index, and that index's relation
	 * oid is the same as the newly determined one, then we don't
	 * open any new relation and we just return false.
	 */
	if (NULL != scanState->iss_RelationDesc &&
			indexOid == RelationGetRelid(scanState->iss_RelationDesc))
	{
		/*
		 * Put the tableOid as we may not have a valid
		 * tableOid, if the original relation was opened
		 * during initialization based on indexScan->indexid.
		 */
		scanState->tableOid = tableOid;
		return false;
	}

	/* We cannot reuse, therefore, clean up previously opened index */
	DynamicScan_CleanupOneIndexRelation(scanState);

	Assert(NULL == scanState->iss_RelationDesc);
	Assert(!OidIsValid(scanState->tableOid));

	LOCKMODE lockMode = AccessShareLock;

	if (!isDynamicScan((Scan *)&scanState->ss.ps.plan) &&
			ExecRelationIsTargetRelation(scanState->ss.ps.state, ((Scan *)scanState->ss.ps.plan)->scanrelid))
	{
		lockMode = NoLock;
	}

	Assert(NULL == scanState->iss_RelationDesc);
	scanState->iss_RelationDesc = index_open(indexOid, lockMode);
	scanState->tableOid = tableOid;

	return true;
}

/*
 * DynamicScan_InitIndexExpressions
 * 		Initializes the plan state's qual and target list from the
 * 		corresponding plan's qual and targetlist for an index.
 */
static void
DynamicScan_InitIndexExpressions(IndexScanState *indexScanState, bool initQual, bool initTargetList)
{
	ScanState *scanState = (ScanState *)indexScanState;

	Plan *plan = scanState->ps.plan;

	if (initQual)
	{
		scanState->ps.qual = (List *)ExecInitExpr((Expr *)plan->qual, (PlanState*)scanState);
	}

	if (initTargetList)
	{
		scanState->ps.targetlist = (List *)ExecInitExpr((Expr *)plan->targetlist, (PlanState*)scanState);
	}
}

/*
 * DynamicScan_PrepareIndexScanKeys
 * 		Prepares the various scan keys for an index, such
 * 		as scan keys, runtime keys and array keys.
 */
static void
DynamicScan_PrepareIndexScanKeys(IndexScanState *indexScanState, bool initQual, bool initTargetList, bool supportsArrayKeys)
{
	IndexScan *plan = (IndexScan *)indexScanState->ss.ps.plan;

	MemoryContext oldCxt = NULL;

	MemoryContext partitionContext = DynamicScan_GetPartitionMemoryContext((ScanState*)indexScanState);

	if (NULL != partitionContext)
	{
		oldCxt = MemoryContextSwitchTo(partitionContext);
	}

	IndexArrayKeyInfo **arrayKeys = supportsArrayKeys ? &indexScanState->iss_ArrayKeys : NULL;
	int *numArrayKeys = supportsArrayKeys ? &indexScanState->iss_NumArrayKeys : NULL;

	/*
	 * initialize child expressions
	 *
	 * We don't need to initialize targetlist or qual since neither are used.
	 *
	 * Note: we don't initialize all of the indexqual expression, only the
	 * sub-parts corresponding to runtime keys (see below).
	 *
	 * TODO rahmaf2 [JIRA: MPP-23297] we need to change indexqual
	 * varattno mapping. (Do we need to change other varattno
	 * mapping too?).
	 *
	 * build the index scan keys from the index qualification
	 */
	ExecIndexBuildScanKeys((PlanState *) indexScanState,
						   indexScanState->iss_RelationDesc,
						   plan->indexqual,
						   plan->indexstrategy,
						   plan->indexsubtype,
						   &indexScanState->iss_ScanKeys,
						   &indexScanState->iss_NumScanKeys,
						   &indexScanState->iss_RuntimeKeys,
						   &indexScanState->iss_NumRuntimeKeys,
						   arrayKeys,
						   numArrayKeys);

	if (NULL != oldCxt)
	{
		MemoryContextSwitchTo(oldCxt);
	}
}

/*
 * DynamicScan_PrepareExpressionContext
 * 		Prepares a new expression context for the evaluation of the
 * 		runtime keys of an index.
 */
static void
DynamicScan_PrepareExpressionContext(IndexScanState *indexScanState)
{
	Assert(NULL == indexScanState->iss_RuntimeContext);

	ExprContext *stdecontext = indexScanState->ss.ps.ps_ExprContext;

	ExecAssignExprContext(indexScanState->ss.ps.state, &indexScanState->ss.ps);
	indexScanState->iss_RuntimeContext = indexScanState->ss.ps.ps_ExprContext;
	indexScanState->ss.ps.ps_ExprContext = stdecontext;
}

/*
 * DynamicScan_InitRuntimeKeys
 * 		Initializes the runtime keys and array keys for an index scan
 * 		by evaluating them.
 */
static void
DynamicScan_InitRuntimeKeys(IndexScanState *indexScanState)
{
	ExprContext *econtext = indexScanState->iss_RuntimeContext;		/* context for runtime keys */

	indexScanState->iss_RuntimeKeysReady = true;

	if (0 != indexScanState->iss_NumRuntimeKeys)
	{
		Assert(NULL != econtext);
		Assert(NULL != indexScanState->iss_RuntimeKeys);

		ExecIndexEvalRuntimeKeys(econtext,
				indexScanState->iss_RuntimeKeys,
				indexScanState->iss_NumRuntimeKeys);
	}

	if (0 != indexScanState->iss_NumArrayKeys)
	{
		Assert(NULL != econtext);
		Assert(NULL != indexScanState->iss_ArrayKeys);

		indexScanState->iss_RuntimeKeysReady =
			ExecIndexEvalArrayKeys(econtext,
								   indexScanState->iss_ArrayKeys,
								   indexScanState->iss_NumArrayKeys);
	}
}

/*
 * IndexScan_ReMapLogicalIndexInfo
 * 		Remaps the columns of the expressions of a the logicalIndexInfo
 * 		in a IndexScanState.
 *
 * 		Returns true if remapping was done.
 */
static bool
IndexScan_RemapLogicalIndexInfo(IndexScanState *indexScanState)
{
	IndexScan *indexScan = (IndexScan*)indexScanState->ss.ps.plan;

	if (!isDynamicScan((Scan *)(&indexScan->scan.plan)))
	{
		return false;
	}

	LogicalIndexInfo *logicalIndexInfo = indexScan->logicalIndexInfo;

	Assert(NULL != logicalIndexInfo);

	DynamicPartitionIterator *iterator = DynamicScan_GetIterator((ScanState *)indexScanState);
	Assert(NULL != iterator);

	AttrNumber *attMap = iterator->attMap;

	return IndexScan_MapLogicalIndexInfo(logicalIndexInfo, attMap, indexScan->scan.scanrelid);
}

/*
 * DynamicScan_CleanupOneIndexRelation
 * 		Cleans up one index relation by closing the scan
 * 		descriptor and the index relation.
 */
static void
DynamicScan_CleanupOneIndexRelation(IndexScanState *indexScanState)
{
	Assert(NULL != indexScanState);

	Assert(SCAN_SCAN != indexScanState->ss.scan_state);
	Assert(SCAN_END != indexScanState->ss.scan_state);

	/*
	 * For SCAN_INIT and SCAN_FIRST we don't yet have any open
	 * scan descriptor.
	 */
	Assert(NULL != indexScanState->iss_ScanDesc ||
			SCAN_FIRST == indexScanState->ss.scan_state ||
			SCAN_INIT == indexScanState->ss.scan_state);

	if (NULL != indexScanState->iss_ScanDesc)
	{
		index_endscan(indexScanState->iss_ScanDesc);
		indexScanState->iss_ScanDesc = NULL;
	}

	if (NULL != indexScanState->iss_RelationDesc)
	{
		index_close(indexScanState->iss_RelationDesc, NoLock);
		indexScanState->iss_RelationDesc = NULL;
	}

	/*
	 * We use the tableOid to figure out if we can reuse an
	 * existing open relation. So, we set this field to InvalidOid
	 * when we clean up an open relation.
	 */
	indexScanState->tableOid = InvalidOid;
}

/*
 * DynamicScan_GetCurrentRelation
 * 		Returns the current relation to scan.
 */
Relation
DynamicScan_GetCurrentRelation(ScanState *scanState)
{
	Assert(T_BitmapTableScanState == scanState->ps.type
			|| T_BitmapIndexScanState == scanState->ps.type);

	Scan *scan = (Scan *)scanState->ps.plan;

	if (isDynamicScan(scan))
	{
		DynamicTableScanInfo *partitionInfo = scanState->ps.state->dynamicTableScanInfo;

		Assert(partitionInfo->numScans >= scan->partIndex);
		DynamicPartitionIterator *iterator = partitionInfo->iterators[scan->partIndex - 1];

		if (NULL == iterator)
		{
			/* We haven't started iterating any partition yet */
			return NULL;
		}

		return iterator->currentRelation;
	}

	return scanState->ss_currentRelation;
}

/*
 * DynamicScan_Begin
 *		Prepares for the iteration of one or more relations (partitions).
 */
void
DynamicScan_Begin(ScanState *scanState, Plan *plan, EState *estate, int eflags)
{
	/* Currently only BitmapTableScanState supports this unified iteration of partitions */
	Assert(T_BitmapTableScanState == scanState->ps.type);

	Assert(SCAN_INIT == scanState->scan_state);

	InitScanStateInternal(scanState, plan, estate, eflags, false /* Do not open the relation. We open it later, per-partition */);
}

/*
 * DynamicScan_ReScan
 *		Prepares for the rescanning of one or more relations (partitions).
 */
void
DynamicScan_ReScan(ScanState *scanState, PartitionEndMethod *partitionEndMethod, ExprContext *exprCtxt)
{
	/* Only BitmapTableScanState supports this unified iteration of partitions */
	Assert(T_BitmapTableScanState == scanState->ps.type);

	if (SCAN_SCAN == scanState->scan_state)
	{
		DynamicScan_CleanupOneRelation(scanState, partitionEndMethod);
	}

	Assert(SCAN_SCAN != scanState->scan_state && SCAN_END != scanState->scan_state);

	Scan *scan = (Scan *)scanState->ps.plan;

	if (isDynamicScan(scan) && SCAN_INIT != scanState->scan_state)
	{
		/*
		 * TODO rahmaf2: May be consider just resetting the iterator position,
		 * instead of destroying prev iterator and creating a new one.
		 */
		DynamicScan_EndIterator(scanState);
		DynamicScan_CreateIterator(scanState, (Scan *)scanState->ps.plan);
	}
	else
	{
		/*
		 * Non-partitioned case. Nothing to do. The table is already
		 * closed during the last scan, which should have finished.
		 * Now, we are just going to reopen the relation when the
		 * DynamicScan_PartitionIterator_Next will be called.
		 */
	}

	/*
	 * If ReScan was called even before we finish our initialization
	 * during our first partition processing, then we still need to
	 * maintain SCAN_INIT. Otherwise, just prepare for the next scan.
	 */
	if (SCAN_INIT != scanState->scan_state)
	{
		scanState->scan_state = SCAN_NEXT;
	}
}

/*
 * DynamicScan_End
 *		Ends partition/relation iteration/scanning and cleans up everything.
 */
void
DynamicScan_End(ScanState *scanState, PartitionEndMethod *partitionEndMethod)
{
	Assert(T_BitmapTableScanState == scanState->ps.type);

	if (SCAN_END == scanState->scan_state)
	{
		return;
	}

	DynamicScan_EndCurrentScan(scanState, partitionEndMethod);

	FreeScanRelationInternal(scanState, false /* Do not close the relation. We closed it in DynamicScan_CleanupOneRelation */);
}

/*
 * DynamicScan_InitNextRelation
 *		Initializes states to process the next relation (if any).
 *		For partitioned scan we advance the iterator and prepare
 *		the next partition to scan. For non-partitioned case, we
 *		just start with the *only* relation, if we haven't
 *		already processed that relation.
 *
 *		Returns false if we don't have any more partition to iterate.
 */
bool
DynamicScan_InitNextRelation(ScanState *scanState, PartitionEndMethod *partitionEndMethod, PartitionInitMethod *partitionInitMethod)
{
	Assert(T_BitmapTableScanState == scanState->ps.type);

	if (SCAN_DONE == scanState->scan_state || SCAN_END == scanState->scan_state)
	{
		return false;
	}

	Scan *scan = (Scan *)scanState->ps.plan;

	if (SCAN_INIT == scanState->scan_state)
	{
		DynamicScan_FinishInitialization(scanState, partitionInitMethod);
	}

	DynamicScan_CleanupOneRelation(scanState, partitionEndMethod);

	Assert(SCAN_NEXT == scanState->scan_state || SCAN_DONE == scanState->scan_state);

	if (isDynamicScan(scan))
	{
		return DynamicScan_InitNextPartition(scanState, partitionInitMethod);
	}

	return DynamicScan_InitSingleRelation(scanState, partitionInitMethod);
}

/*
 * DynamicScan_GetNextTuple
 *		Gets the next tuple. If it needs to open a new relation,
 *		it takes care of that by asking for the next relation
 *		using DynamicScan_GetNextRelation.
 *
 *		Returns the tuple fetched, or a NULL tuple
 *		if it exhausts all the relations/partitions.
 */
TupleTableSlot *
DynamicScan_GetNextTuple(ScanState *scanState, PartitionEndMethod *partitionEndMethod,
		PartitionInitMethod *partitionInitMethod, PartitionScanTupleMethod *partitionScanTupleMethod)
{
	TupleTableSlot *slot = NULL;

	while (TupIsNull(slot) && (SCAN_SCAN == scanState->scan_state ||
			DynamicScan_InitNextRelation(scanState, partitionEndMethod, partitionInitMethod)))
	{
		slot = partitionScanTupleMethod(scanState);

		if (TupIsNull(slot))
		{
			/* The underlying scanner should not change the scan status */
			Assert(SCAN_SCAN == scanState->scan_state);

			bool morePartition = DynamicScan_InitNextRelation(scanState, partitionEndMethod, partitionInitMethod);

			if (!morePartition)
			{
				break;
			}

			Assert(SCAN_SCAN == scanState->scan_state);
		}
	}

	return slot;
}

/*
 * DynamicScan_GetPartitionMemoryContext
 * 		Returns the current partition's (if any) memory context.
 */
MemoryContext
DynamicScan_GetPartitionMemoryContext(ScanState *scanState)
{
	Assert(T_BitmapTableScanState == scanState->ps.type
			|| T_BitmapIndexScanState == scanState->ps.type);

	Scan *scan = (Scan *)scanState->ps.plan;

	/*
	 * TODO rahmaf2 05/08/2014 [JIRA: MPP-23513] We currently
	 * return NULL memory context during initialization. So,
	 * for partitioned case, we will leak memory for all the
	 * expression initialization allocations during ExecInit.
	 */
	if (!isDynamicScan(scan) || SCAN_INIT == scanState->scan_state)
	{
		return NULL;
	}

	DynamicTableScanInfo *partitionInfo = scanState->ps.state->dynamicTableScanInfo;
	Assert(NULL != partitionInfo);

	Assert(partitionInfo->numScans >= scan->partIndex);
	DynamicPartitionIterator *iterator = partitionInfo->iterators[scan->partIndex - 1];

	Assert(NULL != iterator);
	return iterator->partitionMemoryContext;
}


/*
 * IndexScan_ReMapLogicalIndexInfoDirect
 * 		Remaps the columns of the expressions of a provided logicalIndexInfo
 * 		using attMap for a given varno.
 *
 * 		 Returns true if mapping was done.
 */
bool
IndexScan_MapLogicalIndexInfo(LogicalIndexInfo *logicalIndexInfo, AttrNumber *attMap, Index varno)
{
	if (NULL == attMap)
	{
		/* Columns are already aligned, and therefore, no mapping is necessary */
		return false;
	}
	/* map the attrnos if necessary */
	for (int i = 0; i < logicalIndexInfo->nColumns; i++)
	{
		if (logicalIndexInfo->indexKeys[i] != 0)
		{
			logicalIndexInfo->indexKeys[i] = attMap[(logicalIndexInfo->indexKeys[i]) - 1];
		}
	}

	/* map the attrnos in the indExprs and indPred */
	change_varattnos_of_a_varno((Node *)logicalIndexInfo->indPred, attMap, varno);
	change_varattnos_of_a_varno((Node *)logicalIndexInfo->indExprs, attMap, varno);

	return true;
}

/*
 * DynamicScan_GetColumnMapping
 * 		Finds the mapping of columns between two relations because of
 * 		dropped attributes.
 */
AttrNumber*
DynamicScan_GetColumnMapping(Oid oldOid, Oid newOid)
{
	AttrNumber	*attMap = NULL;

	Relation oldRel = heap_open(oldOid, AccessShareLock);
	Relation newRel = heap_open(newOid, AccessShareLock);

	TupleDesc oldTupDesc = oldRel->rd_att;
	TupleDesc newTupDesc = newRel->rd_att;

	heap_close(oldRel, AccessShareLock);
	heap_close(newRel, AccessShareLock);

	attMap = varattnos_map(oldTupDesc, newTupDesc);

	return attMap;
}

/*
 * isDynamicScan
 * 		Returns true if the scan node is dynamic (i.e., determining
 * 		relations to scan at runtime).
 */
bool
isDynamicScan(const Scan *scan)
{
	return (scan->partIndex != INVALID_PART_INDEX);
}

/*
 * DynamicScan_BeginIndexScan
 * 		Prepares the index scan state for a new index scan by calling
 * 		various helper methods.
 */
void
DynamicScan_BeginIndexScan(IndexScanState *indexScanState, bool initQual, bool initTargetList, bool supportsArrayKeys)
{
	Assert(NULL == indexScanState->iss_RelationDesc);
	Assert(NULL == indexScanState->iss_ScanDesc);

	Assert(SCAN_INIT == indexScanState->ss.scan_state);

	/*
	 * Create the expression context now, so that we can save any passed
	 * tuple in ReScan call for later runtime key evaluation. Note: a ReScan
	 * call may precede any BeginIndexPartition call.
	 */
	DynamicScan_PrepareExpressionContext(indexScanState);

	DynamicScan_OpenIndexRelation(indexScanState, InvalidOid /* SCAN_INIT doesn't need any valid table Oid */);
	Assert(NULL != indexScanState->iss_RelationDesc);

	DynamicScan_PrepareIndexScanKeys(indexScanState, initQual, initTargetList, supportsArrayKeys);
	DynamicScan_InitIndexExpressions(indexScanState, initQual, initTargetList);

	/*
	 * SCAN_FIRST implies that we haven't scanned any
	 * relation yet. Therefore, a call to scan the very
	 * same relation would not result in a SCAN_DONE.
	 */
	indexScanState->ss.scan_state = SCAN_FIRST;
}

/*
 * DynamicScan_BeginIndexPartition
 * 		Prepares index scan state for scanning an index relation
 * 		(possibly a dynamically determined relation).
 *
 * Parameters:
 * 		indexScanState: The scan state for this index scan node
 *		initQual: Should we initialize qualifiers? BitmapIndexScan
 *			doesn't evaluate qualifiers, as it depends on BitmapTableScan
 *			to do so. However, Btree index scan (i.e., regular DynamicIndexScan)
 *			would initialize and evaluate qual.
 *		initTargetList: Similar to initQual, it indicates whether to evaluate
 *			target list expressions. Again, BitmapIndexScan doesn't have
 *			target list, while DynamicIndexScan does have.
 *		supportArrayKeys: Whether to calculate array keys from scan keys. Bitmap
 *		index has array keys, while Btree doesn't.
 *
 * 		Returns true if we have a valid relation to scan.
 */
bool
DynamicScan_BeginIndexPartition(IndexScanState *indexScanState, bool initQual, bool initTargetList, bool supportsArrayKeys, bool isMultiScan)
{
	/*
	 * Either the SCAN_INIT should open the relation during SCAN_INIT -> SCAN_FIRST
	 * transition or we already have an open relation from SCAN_SCAN state of the
	 * last scanned relation.
	 */
	Assert(NULL != indexScanState->iss_RelationDesc);
	Assert(NULL != indexScanState->iss_ScanDesc ||
			SCAN_FIRST == indexScanState->ss.scan_state);

	/*
	 * Once the previous scan is done, we don't allow any
	 * further scanning of this relation. Note, we don't
	 * close the relation yet, and we hope that a rescan
	 * call would be able to reuse this relation.
	 */
	if (SCAN_DONE == indexScanState->ss.scan_state)
	{
		return false;
	}

	/*
	 * We already have a scan in progress. So, we don't initalize
	 * any new relation.
	 */
	if (SCAN_SCAN == indexScanState->ss.scan_state)
	{
		return true;
	}

	/* These are the valid ancestor state of the SCAN_SCAN */
	Assert(SCAN_FIRST == indexScanState->ss.scan_state ||
			SCAN_NEXT == indexScanState->ss.scan_state ||
			SCAN_RESCAN == indexScanState->ss.scan_state);

	bool isNewRelation = false;
	bool isDynamic = isDynamicScan((Scan *)indexScanState->ss.ps.plan);

	if (isDynamic)
	{
		Oid newTableOid = DynamicScan_GetTableOid((ScanState *)indexScanState);
		Assert(OidIsValid(indexScanState->tableOid) ||
				SCAN_FIRST == indexScanState->ss.scan_state);

		if (!OidIsValid(newTableOid))
		{
			indexScanState->ss.scan_state = SCAN_DONE;
			return false;
		}

		if ((newTableOid != indexScanState->tableOid))
		{
			/* We have a new relation, so ensure proper column mapping */
			bool remappedVars = DynamicScan_RemapIndexScanVars(indexScanState, initQual, initTargetList);
			/*
			 * During SCAN_INIT -> SCAN_FIRST we opened a relation based on
			 * optimizer provided indexid. However, the corresponding tableOid
			 * of the indexid was set to invalid. Therefore, it is possible to
			 * assume that we have a new index, while in reality, after resolving
			 * logicalIndexInfo, we may not have a new index relation.
			 */
			isNewRelation = DynamicScan_OpenIndexRelation(indexScanState, newTableOid);

			AssertImply(remappedVars, isNewRelation);

			if (remappedVars)
			{
				/*
				 * If remapping is necessary, we also need to prepare the scan keys,
				 * and initialize the expressions one more time.
				 */
				DynamicScan_PrepareIndexScanKeys(indexScanState, initQual, initTargetList, supportsArrayKeys);
				DynamicScan_InitIndexExpressions(indexScanState, initQual, initTargetList);
			}

			/*
			 * Either a rescan requires recomputation of runtime keys (if an expression context is
			 * provided during rescan) or we have a new relation that must recompute index's runtime
			 * keys. Therefore, we need to preserve an already outstanding request for recomputation
			 * [MPP-24628] even if we don't have a new relation.
			 */
			if (indexScanState->iss_RuntimeKeysReady)
			{
				/* A new relation would require us to recompute runtime keys */
				indexScanState->iss_RuntimeKeysReady = !isNewRelation;
			}
		}
	}
	else if (SCAN_FIRST != indexScanState->ss.scan_state &&
			SCAN_RESCAN != indexScanState->ss.scan_state)
	{
		/*
		 * For non-partitioned case, if we already scanned
		 * the relation, and we don't have a rescan request
		 * we flag the scan as "done".
		 */
		indexScanState->ss.scan_state = SCAN_DONE;
		return false;
	}

	Assert(NULL != indexScanState->iss_RelationDesc);

	if (!indexScanState->iss_RuntimeKeysReady)
	{
		DynamicScan_InitRuntimeKeys(indexScanState);
	}

	Assert(indexScanState->iss_RuntimeKeysReady);

	/*
	 * If this is the first relation to scan or if we
	 * had to open a new relation, then we also need
	 * a new scan descriptor.
	 */
	if (SCAN_FIRST == indexScanState->ss.scan_state ||
			isNewRelation)
	{
		Assert(NULL == indexScanState->iss_ScanDesc);
		/*
		 * Initialize scan descriptor.
		 */
		indexScanState->iss_ScanDesc =
				index_beginscan_generic(
					indexScanState->ss.ss_currentRelation,
					indexScanState->iss_RelationDesc,
					indexScanState->ss.ps.state->es_snapshot,
					indexScanState->iss_NumScanKeys,
					indexScanState->iss_ScanKeys, isMultiScan);
	}
	else
	{
		Assert(NULL != indexScanState->iss_ScanDesc);
		/* We already have a reusable scan descriptor, so just initiate a rescan */
		index_rescan(indexScanState->iss_ScanDesc, indexScanState->iss_ScanKeys);
	}

	Assert(NULL != indexScanState->iss_ScanDesc);
	indexScanState->ss.scan_state = SCAN_SCAN;

	return true;
}

/*
 * DynamicScan_EndIndexPartition
 * 		Ends scanning of *one* partition.
 */
void
DynamicScan_EndIndexPartition(IndexScanState *indexScanState)
{
	Assert(NULL != indexScanState);
	Assert(SCAN_SCAN == indexScanState->ss.scan_state);

	/* For non-partitioned case, we flag the scan as "done" */
	if (!isDynamicScan((Scan *)indexScanState->ss.ps.plan))
	{
		indexScanState->ss.scan_state = SCAN_DONE;
	}
	else
	{
		/*
		 * For dynamic scan, we decide if the scanning is done
		 * during fetching of the next partition.
		 */
		indexScanState->ss.scan_state = SCAN_NEXT;
	}
}

/*
 * DynamicScan_RescanIndex
 * 		Prepares for a rescan of an index by saving the
 * 		outer tuple (if any) and starting a new index scan.
 *
 * Parameters:
 * 		indexScanState: The scan state for this index scan node
 *		exprCtxt: The expression context that might contain the
 *			outer tuple during rescanning (e.g., NLJ)
 *		initQual: Should we initialize qualifiers? BitmapIndexScan
 *			doesn't evaluate qualifiers, as it depends on BitmapTableScan
 *			to do so. However, Btree index scan (i.e., regular DynamicIndexScan)
 *			would initialize and evaluate qual.
 *		initTargetList: Similar to initQual, it indicates whether to evaluate
 *			target list expressions. Again, BitmapIndexScan doesn't have
 *			target list, while DynamicIndexScan does have.
 *		supportArrayKeys: Whether to calculate array keys from scan keys. Bitmap
 *		index has array keys, while Btree doesn't.
 */
void
DynamicScan_RescanIndex(IndexScanState *indexScanState, ExprContext *exprCtxt, bool initQual, bool initTargetList, bool supportsArrayKeys)
{
	/*
	 * Rescan should only be called after we passed ExecInit
	 * or after we are done scanning one relation or after
	 * we are done with all relations.
	 */
	Assert(SCAN_FIRST == indexScanState->ss.scan_state ||
			SCAN_NEXT == indexScanState->ss.scan_state ||
			SCAN_DONE == indexScanState->ss.scan_state ||
			SCAN_RESCAN == indexScanState->ss.scan_state);

	Assert(NULL != indexScanState->iss_RuntimeContext);

	/* Context for runtime keys */
	ExprContext *econtext = indexScanState->iss_RuntimeContext;

	Assert(NULL != econtext);

	/*
	 * If we are being passed an outer tuple, save it for runtime key
	 * calc.
	 */
	if (NULL != exprCtxt)
	{
		econtext->ecxt_outertuple = exprCtxt->ecxt_outertuple;
	}

	/* There might be a change of parameter values. Therefore, recompute the runtime keys */
	indexScanState->iss_RuntimeKeysReady = false;

	/*
	 * Reset the runtime-key context so we don't leak memory as each outer
	 * tuple is scanned.  Note this assumes that we will recalculate *all*
	 * runtime keys on each call.
	 */
	ResetExprContext(econtext);

	/*
	 * If we have already passed SCAN_FIRST (i.e., have a valid scan descriptor)
	 * then don't trigger a "forced" re-initialization.
	 */
	if (SCAN_FIRST != indexScanState->ss.scan_state)
	{
		indexScanState->ss.scan_state = SCAN_RESCAN;
	}
}

/*
 * DynamicScan_RescanIndex
 * 		Prepares for a rescan of an index by saving the
 * 		outer tuple (if any) and starting a new index scan
 * 		to evaluate the runtime keys.
 */
void
DynamicScan_EndIndexScan(IndexScanState *indexScanState)
{
	if (SCAN_END == indexScanState->ss.scan_state)
	{
		Assert(NULL == indexScanState->iss_RelationDesc);
		Assert(NULL == indexScanState->iss_ScanDesc);
		Assert(NULL == indexScanState->iss_RuntimeContext);
		return;
	}

	DynamicScan_CleanupOneIndexRelation(indexScanState);
	Assert(NULL == indexScanState->iss_RelationDesc);
	Assert(NULL == indexScanState->iss_ScanDesc);

	Assert(NULL != indexScanState->iss_RuntimeContext);
	FreeRuntimeKeysContext(indexScanState);
	Assert(NULL == indexScanState->iss_RuntimeContext);

	indexScanState->ss.scan_state = SCAN_END;
}
