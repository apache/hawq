/*
 * gp_partition_functions.c
 *    Define dynamic partition selection related functions in GPDB.
 *
 * gp_partition_propagation: This function accumulates unique partition
 *    oids for a specified dynamic table scan. A dynamic table scan node
 *    will be executed only after this function is called.
 *
 * gp_partition_selection: This function finds the child partition of
 *    a given parent partition oid, which satisfies a given partition
 *    key value.
 *
 * gp_partition_expansion: This function finds all child partition oids
 *    for the given parent oid.
 *
 * gp_partition_inverse: This function returns all child partitition oids
 *    with their constarints for a given parent oid.
 *
 * Copyright(c) 2012 - present, EMC/Greenplum
 */

#include "postgres.h"

#include "access/heapam.h"
#include "cdb/cdbpartition.h"
#include "funcapi.h"
#include "nodes/execnodes.h"
#include "utils/array.h"
#include "utils/hsearch.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/elog.h"
#include "utils/guc.h"

#define PARTITION_INVERSE_RECORD_NUM_ATTRS 5
#define PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO 1
#define PARTITION_INVERSE_RECORD_MINKEY_ATTNO 2
#define PARTITION_INVERSE_RECORD_MININCLUDED_ATTNO 3
#define PARTITION_INVERSE_RECORD_MAXKEY_ATTNO 4
#define PARTITION_INVERSE_RECORD_MAXINCLUDED_ATTNO 5

/*
 * increaseScanArraySize
 *   Increase the array size for dynamic table scans.
 *
 * The final array size is the maximum of the following two values:
 *   (1) (dynamicTableScanInfo->numScans + NUM_PID_INDEXES_ADDED)
 *   (2) newMaxPartIndex + 1.
 */
static void
increaseScanArraySize(int newMaxPartIndex)
{
	int oldNumScans = dynamicTableScanInfo->numScans;
	int newNumScans = oldNumScans + NUM_PID_INDEXES_ADDED;
	if (newNumScans <= newMaxPartIndex)
	{
		newNumScans = newMaxPartIndex + 1;
	}
	
	dynamicTableScanInfo->numScans = newNumScans;
	
	if (dynamicTableScanInfo->pidIndexes == NULL)
	{
		dynamicTableScanInfo->pidIndexes = (HTAB **)
			palloc0(dynamicTableScanInfo->numScans * sizeof(HTAB*));
	}
	else
	{
		dynamicTableScanInfo->pidIndexes = (HTAB **)
			repalloc(dynamicTableScanInfo->pidIndexes,
					 dynamicTableScanInfo->numScans * sizeof(HTAB*));

		for (int scanNo = oldNumScans; scanNo < dynamicTableScanInfo->numScans; scanNo++)
		{
			dynamicTableScanInfo->pidIndexes[scanNo] = NULL;
		}
	}
}

/*
 * createPidIndex
 *   Create the pid index for a given dynamic table scan.
 */
static HTAB *
createPidIndex(int index)
{
	Assert((dynamicTableScanInfo->pidIndexes)[index] == NULL);

	HASHCTL hashCtl;
	MemSet(&hashCtl, 0, sizeof(HASHCTL));
	hashCtl.keysize = sizeof(Oid);
	hashCtl.entrysize = sizeof(Oid);
	hashCtl.hash = oid_hash;
	hashCtl.hcxt = dynamicTableScanInfo->memoryContext;
	
	return hash_create("Dynamic Table Scan Pid Index",
					   INITIAL_NUM_PIDS,
					   &hashCtl,
					   HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
}

PG_FUNCTION_INFO_V1(gp_partition_propagation);

/*
 * gp_partition_propagation
 *    Insert a partition oid into its pid-index.
 */
Datum
gp_partition_propagation(PG_FUNCTION_ARGS)
{
	int32 index = PG_GETARG_INT32(0);
	Oid partOid = PG_GETARG_OID(1);

	Assert(dynamicTableScanInfo != NULL &&
		   dynamicTableScanInfo->memoryContext != NULL);

	if (index >= dynamicTableScanInfo->numScans)
	{
		increaseScanArraySize(index);
	}
	
	Assert(index < dynamicTableScanInfo->numScans);
	if ((dynamicTableScanInfo->pidIndexes)[index] == NULL)
	{
		dynamicTableScanInfo->pidIndexes[index] = createPidIndex(index);
	}

	Assert(dynamicTableScanInfo->pidIndexes[index] != NULL);
	
#ifdef USE_ASSERT_CHECKING
	Oid *pidInHash = 
#endif
		hash_search(dynamicTableScanInfo->pidIndexes[index],
					&partOid, HASH_ENTER, NULL /* foundPtr */);
	
	Assert(*pidInHash == partOid);

	PG_RETURN_VOID();
}

/*
 * findPartitionMetadataEntry
 *   Find PartitionMetadata object for a given partition oid from a list.
 */
static PartitionMetadata *
findPartitionMetadataEntry(List *partsMetadata, Oid partOid)
{
	ListCell *lc = NULL;
	PartitionMetadata *metadata = NULL;
	
	foreach (lc, partsMetadata)
	{
		metadata = (PartitionMetadata *)lfirst(lc);
		if (metadata->partsAndRules->part->parrelid == partOid)
		{
			return metadata;
		}
	}

	return NULL;
}

/*
 * createValueArrays
 *   Create an Datum/bool array that will be used to populate partition key value.
 *
 * The size of this array is based on the attribute number of the partition key.
 */
static void
createValueArrays(int keyAttno, Datum **values, bool **isnull)
{
	*values = palloc0(keyAttno * sizeof(Datum));
	*isnull = palloc(keyAttno * sizeof(bool));
	
	MemSet(*isnull, true, keyAttno * sizeof(bool));
}

/*
 * freeValueArrays
 *    Free Datum/bool array.
 */
static void
freeValueArrays(Datum *values, bool *isnull)
{
	pfree(values);
	pfree(isnull);
}

PG_FUNCTION_INFO_V1(gp_partition_selection);

/*
 * gp_partition_selection
 *    Find the child partition oid for a given parent partition, which
 * satisfies the given partition key value.
 *
 * This function assumes that there is only one partition key in this level.
 * Currently, this function also assumes that the parent partition is the root
 * partition.
 *
 * If no such a child partition is found, return NULL.
 */
Datum
gp_partition_selection(PG_FUNCTION_ARGS)
{
	Oid parentOid = PG_GETARG_OID(0);

	Assert(dynamicTableScanInfo != NULL);
	Assert(dynamicTableScanInfo->memoryContext != NULL);

	if (dynamicTableScanInfo->partsMetadata == NULL)
	{
		PG_RETURN_NULL();
	}
	
	PartitionMetadata *metadata =
		findPartitionMetadataEntry(dynamicTableScanInfo->partsMetadata,
								   parentOid);
	if (metadata == NULL)
	{
		PG_RETURN_NULL();
	}

	Assert(metadata->partsAndRules != NULL &&
		   metadata->accessMethods != NULL);
	Partition *part = metadata->partsAndRules->part;

	Assert(part->parnatts == 1);
	AttrNumber partAttno = part->paratts[0];

	Relation rel = relation_open(parentOid, NoLock);
	TupleDesc tupDesc = RelationGetDescr(rel);
	Assert(tupDesc->natts >= partAttno);

	Datum *values = NULL;
	bool *isnull = NULL;
	createValueArrays(partAttno, &values, &isnull);

	isnull[partAttno - 1] = PG_ARGISNULL(1);
	if (!isnull[partAttno - 1])
	{
		values[partAttno - 1] = PG_GETARG_DATUM(1);
	}

	/* set the memory context for the access methods */
	metadata->accessMethods->part_cxt = dynamicTableScanInfo->memoryContext;
	
	Oid childOid = selectPartition(metadata->partsAndRules,
								   values,
								   isnull,
								   tupDesc,
								   metadata->accessMethods);

	freeValueArrays(values, isnull);
	
	relation_close(rel, NoLock);

	/*
	 * There might not be a child partition that satisfies the given
	 * value. In that case, this function returns NULL.
	 */
	if (OidIsValid(childOid))
	{
		PG_RETURN_OID(childOid);
	}

	PG_RETURN_NULL();
}

/*
 * PartitionIterator
 *   Contains the state that are necessary to iterate through all
 * child partitions, one at a time.
 *
 * This is used by set-returning partition functions.
 */
typedef struct PartitionIterator
{
	PartitionMetadata *metadata;

	/*
	 * The cell to the next PartitionRule.
	 */
	ListCell *nextRuleCell;

	/*
	 * The current child partition that is being processed.
	 */
	PartitionRule *currentRule;

	/*
	 * Indicate whether the information about the default partition
	 * has been returned.
	 */
	bool defaultPartReturned;
	
} PartitionIterator;

/*
 * createPartitionIterator
 *    create a new PartitionIterator object for a given parent oid.
 *
 * The metadata information for the given parent oid is found in
 * dynamicTableScanInfo.
 */
static PartitionIterator *
createPartitionIterator(Oid parentOid)
{
	PartitionIterator *partitionIterator = palloc(sizeof(PartitionIterator));
	PartitionMetadata *metadata =
		findPartitionMetadataEntry(dynamicTableScanInfo->partsMetadata,
								   parentOid);
	partitionIterator->metadata = metadata;
	partitionIterator->currentRule = NULL;
	partitionIterator->nextRuleCell = NULL;
	
	Assert(metadata != NULL);
	Assert(metadata->partsAndRules != NULL);
	partitionIterator->nextRuleCell = list_head(metadata->partsAndRules->rules);
	partitionIterator->defaultPartReturned = true;
	if (metadata->partsAndRules->default_part != NULL)
	{
		partitionIterator->defaultPartReturned = false;
	}

	return partitionIterator;
}

PG_FUNCTION_INFO_V1(gp_partition_expansion);

/*
 * gp_partition_expansion
 *   Find all child partition oids for the given parent oid.
 *
 * Currently, this function also assumes that the parent partition is the root
 * partition.
 *
 * This function is a set-returning function, returning a set of
 * child oids.
 */
Datum
gp_partition_expansion(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcCallContext = NULL;
	
	/*
	 * Setup the function call context for set-returning functions.
	 * At the first time of calling this function, we find the partition
	 * metadata for the given parent oid, and store that in an PartitionIterator
	 * structure.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		funcCallContext = SRF_FIRSTCALL_INIT();

		Oid parentOid = PG_GETARG_OID(0);

		MemoryContext oldContext = MemoryContextSwitchTo(funcCallContext->multi_call_memory_ctx);

		funcCallContext->user_fctx = createPartitionIterator(parentOid);

		MemoryContextSwitchTo(oldContext);
	}
	funcCallContext = SRF_PERCALL_SETUP();

	PartitionIterator *partitionIterator = (PartitionIterator *)funcCallContext->user_fctx;
	Assert(partitionIterator != NULL);
	ListCell *ruleCell = partitionIterator->nextRuleCell;

	if (ruleCell != NULL)
	{
		partitionIterator->nextRuleCell = lnext(ruleCell);

		partitionIterator->currentRule = (PartitionRule *)lfirst(ruleCell);
		Oid childOid = partitionIterator->currentRule->parchildrelid;
		
		SRF_RETURN_NEXT(funcCallContext, ObjectIdGetDatum(childOid));
	}

	/*
	 * Return default partition oid if any.
	 */
	if (!partitionIterator->defaultPartReturned)
	{
		Assert(partitionIterator->metadata != NULL &&
			   partitionIterator->metadata->partsAndRules != NULL &&
			   partitionIterator->metadata->partsAndRules->default_part != NULL);
		PartitionRule *defaultPart = partitionIterator->metadata->partsAndRules->default_part;
		Oid childOid = defaultPart->parchildrelid;
		partitionIterator->defaultPartReturned = true;

		SRF_RETURN_NEXT(funcCallContext, ObjectIdGetDatum(childOid));
	}

	pfree(partitionIterator);
	
	SRF_RETURN_DONE(funcCallContext);
}

/*
 * createInverseTupleDesc
 *    Create a tuple descriptor for the record returned by gp_partition_inverse.
 *
 * The record has the following format:
 *  Oid: child partition oid
 *  typeOid: the date type for the low end of a range partition;
 *           the data type for the value in a list partition
 *  bool: whether to include the low end of a range partition;
 *        always true for a list partition
 *  typeOid: used by range partitions only;
 *           represents the data type for the high end of a range partition
 *  bool: used by range partitions only;
 *        represents whether to include the high end of a range partition.
 */
static TupleDesc
createInverseTupleDesc(Oid typeOid, int32 typeMod)
{
	TupleDesc tupleDesc = CreateTemplateTupleDesc(PARTITION_INVERSE_RECORD_NUM_ATTRS, false);
	TupleDescInitEntry(tupleDesc, (AttrNumber) PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO, 
					   "partchildrelid", OIDOID, -1, 0);
	TupleDescInitEntry(tupleDesc, (AttrNumber) PARTITION_INVERSE_RECORD_MINKEY_ATTNO,
					   "minkey", typeOid, typeMod, 0);
	TupleDescInitEntry(tupleDesc, (AttrNumber) PARTITION_INVERSE_RECORD_MININCLUDED_ATTNO,
					   "minincluded", BOOLOID, -1, 0);
	TupleDescInitEntry(tupleDesc, (AttrNumber) PARTITION_INVERSE_RECORD_MAXKEY_ATTNO,
					   "maxkey", typeOid, typeMod, 0);
	TupleDescInitEntry(tupleDesc, (AttrNumber) PARTITION_INVERSE_RECORD_MAXINCLUDED_ATTNO,
					   "maxincluded", BOOLOID, -1, 0);

	return tupleDesc;
}

/*
 * InverseContext
 *    Context data for gp_partition_inverse function.
 *
 * This is the base structure to maintain context information for
 * various partition types.
 */
typedef struct InverseContext InverseContext;
struct InverseContext
{
	/*
	 * The iterator to iterate through all child partitions,
	 * one at a time.
	 */
	PartitionIterator *partitionIterator;
	
	/*
	 * The arrays to hold output record.
	 */
	Datum values[PARTITION_INVERSE_RECORD_NUM_ATTRS];
	bool nulls[PARTITION_INVERSE_RECORD_NUM_ATTRS];

	/*
	 * The pointer to the function that produces the next output record.
	 * The function returns false when no record is found. Otherwise, this
	 * function returns true.
	 */
	bool (*findNextRecord)(InverseContext *inverseContext);
	
};

/*
 * InverseContextForRange
 *   Context data for gp_partition_inverse function on range partitions.
 */
typedef InverseContext InverseContextForRange;

/*
 * InverseContextForList
 *   Context data for gp_partition_inverse function on list partitions.
 */
typedef struct InverseContextForList
{
	InverseContext context;

	/*
	 * The cell for the next value in a list partition.
	 */
	ListCell *listValueCell;

}InverseContextForList;

/*
 * setInverseRecordForRange
 *	Set the record value array for the inverse function on a range partition, based
 *	on the given partition rule.
 *
 *	This function does not handle the default partition.
 * 
 *	Range partitions can be of the form:
 *
 *	(-inf ,e], (-inf, e), (s, e), [s, e], (s,e], [s,e), (s,inf),
 *	and [s, inf).
 */
static void
setInverseRecordForRange(PartitionRule *rule,
						 Datum *values,
						 bool *nulls,
						 int numAttrs)
{
	Assert(numAttrs == PARTITION_INVERSE_RECORD_NUM_ATTRS);
	Assert(rule != NULL);

	/* Default partitions should not be handled here. */
	Assert(!rule->parisdefault);

	MemSet(nulls, true, sizeof(bool) * PARTITION_INVERSE_RECORD_NUM_ATTRS);
	MemSet(values, 0, sizeof(Datum) * PARTITION_INVERSE_RECORD_NUM_ATTRS);

	if (NULL != rule->parrangestart)
	{
		Assert(IsA(rule->parrangestart, List) &&
			list_length((List *)rule->parrangestart) == 1);

		Node *rangeStart = (Node *)linitial((List *)rule->parrangestart);
		Assert(IsA(rangeStart, Const));
		Const *rangeStartConst = (Const *)rangeStart;

		values[PARTITION_INVERSE_RECORD_MINKEY_ATTNO - 1] = rangeStartConst->constvalue;
		nulls[PARTITION_INVERSE_RECORD_MINKEY_ATTNO - 1] = rangeStartConst->constisnull;

		values[PARTITION_INVERSE_RECORD_MININCLUDED_ATTNO - 1] = BoolGetDatum(rule->parrangestartincl);
		nulls[PARTITION_INVERSE_RECORD_MININCLUDED_ATTNO - 1] = false;
	}

	if (NULL != rule->parrangeend)
	{
		Assert(IsA(rule->parrangeend, List) && 
			list_length((List *)rule->parrangeend) == 1);

		Node *rangeEnd = (Node *)linitial((List *)rule->parrangeend);
		Assert(IsA(rangeEnd, Const));
		Const *rangeEndConst = (Const *)rangeEnd;

		values[PARTITION_INVERSE_RECORD_MAXKEY_ATTNO - 1] = rangeEndConst->constvalue;
		nulls[PARTITION_INVERSE_RECORD_MAXKEY_ATTNO - 1] = rangeEndConst->constisnull;

		values[PARTITION_INVERSE_RECORD_MAXINCLUDED_ATTNO - 1] = BoolGetDatum(rule->parrangeendincl);
		nulls[PARTITION_INVERSE_RECORD_MAXKEY_ATTNO - 1] = false;
	}

	values[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = ObjectIdGetDatum(rule->parchildrelid);
	nulls[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = false;
}

/*
 * setInverseRecordForList
 *    Set the record value array for the inverse function on a list partition, based
 * on the given partition rule.
 *
 * This function only supports single-column partition key in the partition level.
 */
static void
setInverseRecordForList(PartitionRule *rule,
						ListCell *listValueCell,
						Datum *values,
						bool *nulls,
						int numAttrs)
{
	Assert(numAttrs == PARTITION_INVERSE_RECORD_NUM_ATTRS);
	Assert(rule != NULL &&
		   rule->parlistvalues != NULL &&
		   listValueCell != NULL);

	/*
	 * Note that in partition rule, list values are stored in a list of lists to support
	 * multi-column partitions.
	 */
	List *listValue = (List *)lfirst(listValueCell);
		
	/* This function only supports single-column partition key. */
	Assert(list_length(listValue) == 1);
	
	Const *listValueConst = (Const *)linitial(listValue);
	Assert(IsA(listValueConst, Const));

	values[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = ObjectIdGetDatum(rule->parchildrelid);
	nulls[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = false;

	values[PARTITION_INVERSE_RECORD_MINKEY_ATTNO - 1] = listValueConst->constvalue;
	nulls[PARTITION_INVERSE_RECORD_MINKEY_ATTNO - 1] = listValueConst->constisnull;

	values[PARTITION_INVERSE_RECORD_MININCLUDED_ATTNO - 1] = BoolGetDatum(true);
	nulls[PARTITION_INVERSE_RECORD_MININCLUDED_ATTNO - 1] = false;

	values[PARTITION_INVERSE_RECORD_MAXKEY_ATTNO - 1] = listValueConst->constvalue;
	nulls[PARTITION_INVERSE_RECORD_MAXKEY_ATTNO - 1] = false;

	values[PARTITION_INVERSE_RECORD_MAXINCLUDED_ATTNO - 1] = BoolGetDatum(true);
	nulls[PARTITION_INVERSE_RECORD_MAXINCLUDED_ATTNO - 1] = false;
}

/*
 * setInverseRecordForDefaultPart
 *    Set the record value array for the inverse function on both range and list default partitions.
 *
 * The default partition does not contain any constraint information,
 * this function simple returns the default partition oid with null values on other
 * columns in the return record.
 */
static void
setInverseRecordForDefaultPart(PartitionRule *rule,
							   Datum *values,
							   bool *nulls,
							   int numAttrs)
{
	Assert(numAttrs == PARTITION_INVERSE_RECORD_NUM_ATTRS);
	Assert(rule != NULL &&
		   ((rule->parrangestart == NULL &&
			 rule->parrangeend == NULL) ||
			(rule->parlistvalues == NULL)));

	MemSet(nulls, true, sizeof(bool) * PARTITION_INVERSE_RECORD_NUM_ATTRS);
	MemSet(values, 0, sizeof(Datum) * PARTITION_INVERSE_RECORD_NUM_ATTRS);
	values[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = ObjectIdGetDatum(rule->parchildrelid);
	nulls[PARTITION_INVERSE_RECORD_PARCHILDRELID_ATTNO - 1] = false;
}

/*
 * findNextRecordForRange
 *    Find the next return record for range partitions in gp_partition_inverse() calls,
 * and store the record in the given values/nulls array pair.
 *
 * This function returns false when no record is found. Otherwise, this function
 * returns true.
 */
static bool
findNextRecordForRange(InverseContext *inverseContext)
{
	Assert(inverseContext != NULL &&
		   inverseContext->partitionIterator);
	PartitionIterator *partitionIterator = inverseContext->partitionIterator;
	bool hasNext = false;

	ListCell *ruleCell =  partitionIterator->nextRuleCell;

	if (ruleCell != NULL)
	{
		partitionIterator->nextRuleCell = lnext(ruleCell);
		partitionIterator->currentRule = (PartitionRule *)lfirst(ruleCell);

		setInverseRecordForRange(partitionIterator->currentRule,
								 inverseContext->values,
								 inverseContext->nulls,
								 PARTITION_INVERSE_RECORD_NUM_ATTRS);

		hasNext = true;
	}

	/* Return the default partition if any. */
	else if (!partitionIterator->defaultPartReturned)
	{
		Assert(partitionIterator->metadata != NULL &&
			   partitionIterator->metadata->partsAndRules != NULL &&
			   partitionIterator->metadata->partsAndRules->default_part != NULL);
		PartitionRule *defaultPart = partitionIterator->metadata->partsAndRules->default_part;
		setInverseRecordForDefaultPart(defaultPart,
									   inverseContext->values,
									   inverseContext->nulls,
									   PARTITION_INVERSE_RECORD_NUM_ATTRS);
		partitionIterator->defaultPartReturned = true;
		
		hasNext = true;
	}

	return hasNext;
}

/*
 * findNextRecordForList
 *    Find the next return record for list partitions in gp_partition_inverse() calls,
 * and store it into the values/nulls array pair.
 *
 * This function returns false when no record is found. Otherwise, this function
 * returns true.
 */
static bool
findNextRecordForList(InverseContext *inverseContext)
{
	Assert(inverseContext != NULL);
	PartitionIterator *partitionIterator = inverseContext->partitionIterator;
	Assert(partitionIterator != NULL);
	InverseContextForList *contextForList = (InverseContextForList *)inverseContext;
	bool hasNext = false;

	/*
	 * There might be multiple values for the same partition. Each call to
	 * gp_partition_inverse() will produce a record containing one such value.
	 */
	if (contextForList->listValueCell != NULL)
	{
		ListCell *currentListValueCell = contextForList->listValueCell;
		
		contextForList->listValueCell = lnext(contextForList->listValueCell);
		setInverseRecordForList(partitionIterator->currentRule, currentListValueCell,
								inverseContext->values, inverseContext->nulls,
								PARTITION_INVERSE_RECORD_NUM_ATTRS);

		hasNext = true;
	}

	/*
	 * After processing all values in a partition, if there are more partitions
	 * left, process the next one.
	 */
	else if (partitionIterator->nextRuleCell != NULL)
	{
		ListCell *ruleCell = partitionIterator->nextRuleCell;
		partitionIterator->nextRuleCell = lnext(ruleCell);

		partitionIterator->currentRule = (PartitionRule *)lfirst(ruleCell);
		Assert(partitionIterator->currentRule->parlistvalues != NULL);
		ListCell *currentListValueCell = list_head(partitionIterator->currentRule->parlistvalues);
		contextForList->listValueCell = lnext(currentListValueCell);
		setInverseRecordForList(partitionIterator->currentRule, currentListValueCell,
								inverseContext->values, inverseContext->nulls,
								PARTITION_INVERSE_RECORD_NUM_ATTRS);

		hasNext = true;
	}
	
	/* Return the default partition if any. */
	else if (!partitionIterator->defaultPartReturned)
	{
		Assert(partitionIterator->metadata != NULL &&
			   partitionIterator->metadata->partsAndRules != NULL &&
			   partitionIterator->metadata->partsAndRules->default_part != NULL);
		PartitionRule *defaultPart = partitionIterator->metadata->partsAndRules->default_part;
		setInverseRecordForDefaultPart(defaultPart,
									   inverseContext->values,
									   inverseContext->nulls,
									   PARTITION_INVERSE_RECORD_NUM_ATTRS);
		partitionIterator->defaultPartReturned = true;

		hasNext = true;
	}

	return hasNext;
}
		
/*
 * createInverseContext
 *   Create the context for gp_partition_inverse for a given parent oid.
 */
static InverseContext*
createInverseContext(Oid parentOid)
{
	InverseContext *inverseContext = NULL;

	PartitionIterator *partitionIterator = createPartitionIterator(parentOid);

	PartitionMetadata *metadata = partitionIterator->metadata;
	Assert(metadata != NULL &&
		   metadata->partsAndRules != NULL &&
		   metadata->partsAndRules->part != NULL);
	switch(metadata->partsAndRules->part->parkind)
	{
		case 'r':
			inverseContext = palloc(sizeof(InverseContextForRange));
			inverseContext->partitionIterator = partitionIterator;
			inverseContext->findNextRecord = findNextRecordForRange;
			break;

		case 'l':
			inverseContext = palloc(sizeof(InverseContextForList));
			inverseContext->partitionIterator = partitionIterator;
			inverseContext->findNextRecord = findNextRecordForList;
			((InverseContextForList *)inverseContext)->listValueCell = NULL;
			break;

		default:
			elog(ERROR, "partitioning kind '%c' not allowed",
				 metadata->partsAndRules->part->parkind);
	}

	return inverseContext;
}

/*
 * freeInverseContext
 *    Free the context for gp_partition_inverse.
 */
static void
freeInverseContext(InverseContext *inverseContext)
{
	Assert(inverseContext != NULL);
	pfree(inverseContext->partitionIterator);
	pfree(inverseContext);
}

/*
 * findPartitionKeyType
 *   Find the type oid and typeMod for the given partition key.
 */
static void
findPartitionKeyType(Oid parentOid,
					 int keyAttNo,
					 Oid *typeOid,
					 int32 *typeMod)
{
	Relation rel = relation_open(parentOid, NoLock);
	TupleDesc tupDesc = RelationGetDescr(rel);

	Assert(tupDesc->natts >= keyAttNo);

	*typeOid = tupDesc->attrs[keyAttNo - 1]->atttypid;
	*typeMod = tupDesc->attrs[keyAttNo - 1]->atttypmod;
	
	relation_close(rel, NoLock);
}

/*
 * gp_partition_inverse
 *   Returns all child partitition oids with their constarints for a given parent oid.
 *
 * Currently, this function assumes that the parent partition is the root partition.
 *
 * This function is a set-returning function.
 */
Datum
gp_partition_inverse(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcCallContext = NULL;
	InverseContext *inverseContext = NULL;
	
	/*
	 * Setup the function call context for set-returning functions.
	 * At the first time of calling this function, we create and initialize
	 * necessary context data in inverseContext, such as finding the partition
	 * metadata for the given parent oid.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		funcCallContext = SRF_FIRSTCALL_INIT();

		Oid parentOid = PG_GETARG_OID(0);

		MemoryContext oldContext = MemoryContextSwitchTo(funcCallContext->multi_call_memory_ctx);

		funcCallContext->user_fctx = createInverseContext(parentOid);
		inverseContext = (InverseContext *)funcCallContext->user_fctx;

		Assert(inverseContext != NULL &&
			   inverseContext->partitionIterator != NULL);
		PartitionMetadata *metadata = inverseContext->partitionIterator->metadata;
		Assert(metadata != NULL &&
			   metadata->partsAndRules != NULL &&
			   metadata->partsAndRules->part != NULL);
		Partition *part = metadata->partsAndRules->part;

		Oid typeOid = 0;
		int32 typeMod = 0;
		findPartitionKeyType(parentOid, part->paratts[0], &typeOid, &typeMod);

		TupleDesc tupleDesc = createInverseTupleDesc(typeOid, typeMod);
		funcCallContext->tuple_desc = BlessTupleDesc(tupleDesc);

		MemoryContextSwitchTo(oldContext);
	}
	funcCallContext = SRF_PERCALL_SETUP();

	inverseContext = (InverseContext *)funcCallContext->user_fctx;
	Assert(inverseContext != NULL &&
		   inverseContext->partitionIterator != NULL);

	if (inverseContext->findNextRecord(inverseContext))
	{
		HeapTuple tuple = heap_form_tuple(funcCallContext->tuple_desc,
										  inverseContext->values,
										  inverseContext->nulls);
		Datum result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcCallContext, result);
	}
	
	freeInverseContext(inverseContext);

	SRF_RETURN_DONE(funcCallContext);
}

/*
 * dumpDynamicTableScanPidIndex
 *   Write out pids for a given dynamic table scan.
 */
void
dumpDynamicTableScanPidIndex(int index)
{
	if (index < 0 ||
		dynamicTableScanInfo == NULL ||
		index > dynamicTableScanInfo->numScans ||
		dynamicTableScanInfo->pidIndexes[index] == NULL)
	{
		return;
	}
	
	Assert(dynamicTableScanInfo != NULL &&
		   index < dynamicTableScanInfo->numScans &&
		   dynamicTableScanInfo->pidIndexes[index] != NULL);
	
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, dynamicTableScanInfo->pidIndexes[index]);

	StringInfoData pids;
	initStringInfo(&pids);

	Oid *partOid = NULL;
	while ((partOid = (Oid *)hash_seq_search(&status)) != NULL)
	{
		appendStringInfo(&pids, "%d ", *partOid);
	}

	elog(LOG, "Dynamic Table Scan %d pids: %s", index, pids.data);
	pfree(pids.data);
}
