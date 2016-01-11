/*-------------------------------------------------------------------------
 *
 * memquota.c
 * Routines related to memory quota for queries.
 *
 *
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
 * 
 *-------------------------------------------------------------------------*/

#include "cdb/memquota.h"
#include "access/heapam.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_class.h"
#include "catalog/pg_resqueue.h"
#include "cdb/cdbllize.h"
#include "cdb/cdbparquetam.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdbvars.h"
#include "commands/queue.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "executor/execdesc.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "storage/lwlock.h"
#include "utils/guc_tables.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/resscheduler.h"

/**
 * This contains information that will be used memory quota assignment.
 */
typedef struct MemQuotaContext
{
	plan_tree_base_prefix base; /* Required prefix for plan_tree_walker/mutator */
	uint64 numNonMemIntensiveOperators; /* number of non-blocking operators */
	uint64 numMemIntensiveOperators; /* number of blocking operators */
	uint64 queryMemKB;
	PlannedStmt *plannedStmt; /* pointer to the planned statement */
	uint64 parquetOpReservedMemKB; /* memory reserved for parquet related operators */

	/* hash table of root Oids of parquet tables for Planner's plan */
	HTAB * parquetRootOids;
} MemQuotaContext;

/**
 * Forward declarations.
 */
static bool PrelimWalker(Node *node, MemQuotaContext *context);
static bool IsAggMemoryIntensive(Agg *agg);
static bool IsMemoryIntensiveOperator(Node *node, PlannedStmt *stmt);
static bool IsParquetScanOperator(Node *node, PlannedStmt *stmt);
static bool IsParquetInsertOperator(Node *node, PlannedStmt *stmt);
static uint64 MemoryReservedForParquetInsertForPlannerPlan(PlannedStmt *stmt);
static List* GetScannedColumnsForTable(Node *node, Oid rel_oid);
static uint64 MemoryReservedForParquetScan(Node *node, PlannedStmt *stmt, HTAB *parquetRootOids);
static uint64 MemoryReservedForParquetInsert(Oid rel_oid);
static Oid GetPartOidForParquetScan(Node *node, Oid rootOid, uint64 *maxMemRequired, bool needRemap);
static Oid GetPartOidForParquetInsert(Oid root_oid, uint64 *maxMemRequired);
static HTAB* createOidHTAB();

struct OperatorGroupNode;

/*
 * OperatorGroupNode
 *    Store the information regarding an operator group.
 */
typedef struct OperatorGroupNode
{
	/* The id for this group */
	uint32 groupId;

	/*
	 * The number of non-memory-intensive operators and memory-intensive
	 * operators in the group.
	 */
	uint64 numNonMemIntenseOps;
	uint64 numMemIntenseOps;

	/*
	 * The maximal number of non-memory-intensive and memory-intensive
	 * operators in all child groups of this group which might be active
	 * concurrently.
	 */
	uint64 maxNumConcNonMemIntenseOps;
	uint64 maxNumConcMemIntenseOps;

	/* The list of child groups */
	List *childGroups;

	/* The parent group */
	struct OperatorGroupNode *parentGroup;

	/* The memory limit for this group and its child groups */
	uint64 groupMemKB;

	/* memory reserved for parquet related operators in this group */
	uint64 parquetOpReservedMemKB;

	/* hash table of root Oids of parquet tables for Planner's plan */
	HTAB * parquetRootOids;

} OperatorGroupNode;

/*
 * PolicyEagerFreeContext
 *   Store the intermediate states during the tree walking for the optimize
 * memory distribution policy.
 */
typedef struct PolicyEagerFreeContext
{
	plan_tree_base_prefix base;
	OperatorGroupNode *groupTree; /* the root of the group tree */
	OperatorGroupNode *groupNode; /* the current group node in the group tree */
	uint32 nextGroupId; /* the group id for a new group node */
	uint64 queryMemKB; /* the query memory limit */
	PlannedStmt *plannedStmt; /* pointer to the planned statement */
} PolicyEagerFreeContext;

/**
 * GUCs
 */
int		gp_resqueue_memory_policy_auto_fixed_mem;
bool	gp_resqueue_print_operator_memory_limits;

/**
 * create a HTAB for Oids
 */
static HTAB*
createOidHTAB()
{
	HASHCTL hashCtl;
	MemSet(&hashCtl, 0, sizeof(HASHCTL));
	hashCtl.keysize = sizeof(Oid);
	hashCtl.entrysize = sizeof(Oid);
	hashCtl.hash = oid_hash;
	hashCtl.hcxt = CurrentMemoryContext;

	return hash_create("Parquet Table Root Oids",
	                   INITIAL_NUM_PIDS,
	                   &hashCtl,
	                   HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
}

/**
 * Is an agg operator memory intensive? The following cases mean it is:
 * 1. If agg strategy is hashed
 * 2. If targetlist or qual contains a DQA
 * 3. If there is an ordered aggregated.
 */
static bool IsAggMemoryIntensive(Agg *agg)
{
	Assert(agg);

	/* Case 1 */
	if (agg->aggstrategy == AGG_HASHED)
	{
		return true;
	}

	AggClauseCounts aggInfo;
	
	/* Zero it out */
	MemSet(&aggInfo, 0, sizeof(aggInfo));

	Plan *plan = (Plan *) &(agg->plan);
	count_agg_clauses((Node *) plan->targetlist, &aggInfo);
	count_agg_clauses((Node *) plan->qual, &aggInfo);
	
	/* Case 2 */
	if (aggInfo.numDistinctAggs >0)
	{
		return true;
	}
	
	/* Case 3 */
	if (aggInfo.aggOrder != NIL )
	{
		return true;
	}
	
	return false;
}

/*
 * IsAggBlockingOperator
 *    Return true if an Agg node is a blocking operator.
 *
 * Agg is a blocking operator when it is
 * 1. Scalar Agg
 * 2. Second stage HashAgg when streaming is on.
 * 3. First and Second stage HashAgg when streaming is off.
 */
static bool
IsAggBlockingOperator(Agg *agg)
{
	if (agg->aggstrategy == AGG_PLAIN)
	{
		return true;
	}

	return (agg->aggstrategy == AGG_HASHED && !agg->streaming);
}

/*
 * isMaterialBlockingOperator
 *     Return true if a Material node is a blocking operator.
 *
 * Material node is a blocking operator when cdb_strict is on.
 */
static bool
IsMaterialBlockingOperator(Material *material)
{
	return material->cdb_strict;
}

/*
 * IsBlockingOperator
 *     Return true when the given plan node is a blocking operator.
 */
static bool
IsBlockingOperator(Node *node)
{
	switch(nodeTag(node))
	{
		case T_BitmapIndexScan: 
		case T_Hash:
		case T_Sort:
			return true;

		case T_Material:
			return IsMaterialBlockingOperator((Material *)node);
		   
		case T_Agg:
			return IsAggBlockingOperator((Agg *)node);
			
		default:
			return false;
	}
}

/**
 * Is a result node memory intensive? It is if it contains function calls.
 */
bool
IsResultMemoryIntesive(Result *res)
{
	return contains_node_type(NULL, (Node *) ((Plan *) res)->targetlist, T_FuncExpr);
}

/**
 * Is an operator memory intensive?
 */
static bool
IsMemoryIntensiveOperator(Node *node, PlannedStmt *stmt)
{
	Assert(is_plan_node(node));
	switch(nodeTag(node))
	{
		case T_Material:
		case T_Sort:
		case T_ShareInputScan:
		case T_Hash:
		case T_BitmapIndexScan:
		case T_FunctionScan:
		case T_Window:
		case T_TableFunctionScan:
			return true;
		case T_Agg:
			{
				Agg *agg = (Agg *) node;
				return IsAggMemoryIntensive(agg);
			}
		case T_Result:
			{
				Result *res = (Result *) node;
				return IsResultMemoryIntesive(res);
			}
		default:
			return false;
	}
}

/**
 * Calculate memory required for a parquet table scan
 *
 * node:   Plan node
 * stmt:   Plan statement
 * return: memory in byte that is reserved for parquet table scan
 */
static uint64
MemoryReservedForParquetScan(Node *node, PlannedStmt *stmt, HTAB *parquetRootOids)
{
	Assert(NULL != node);
	Assert(NULL != stmt);
	Assert(NULL != parquetRootOids);

	Oid rel_oid = getrelid(((Scan *) node)->scanrelid, stmt->rtable);
	uint64 memoryReservedForParquetScan = 0;

	/* For dynamic table scan, we need to iterate through all the part tables and find out the
	 * parquet part table that has the maximum memory requirement. */
	if (IsA(node, DynamicTableScan))
	{
#if USE_ASSERT_CHECKING
		Oid part_oid =
#endif
		GetPartOidForParquetScan(node, rel_oid, &memoryReservedForParquetScan, true /* need remap */);
		Assert(InvalidOid != part_oid);
	}
	/* Planner's plan for parquet table scan */
	else if(IsA(node, ParquetScan))
	{
		Oid root_oid = rel_partition_get_master(rel_oid);
		if (InvalidOid == root_oid)
		{
			/* it is not a partitioned table */
			List *scanned_columns_list = GetScannedColumnsForTable(node, rel_oid);
			memoryReservedForParquetScan = memReservedForParquetScan(rel_oid, scanned_columns_list);
			list_free(scanned_columns_list);
		}
		/* it is a partitioned table */
		else
		{
			/**
			 * GPSQL-2477: we should assign parquet required memory to one
			 * partitioned table scan for Planner's plan.
			 */
			bool foundPtr = false;
			hash_search(parquetRootOids, &root_oid, HASH_ENTER, &foundPtr);

			if (foundPtr)
			{
				/*
				 * the root_oid is already in the hash table which means the
				 * parquet required memory has been assigned to some other ParquetScan.
				 * Assign 100K to this operator.
				 */
				memoryReservedForParquetScan = (uint64) gp_resqueue_memory_policy_auto_fixed_mem * 1024;
			}
			else
			{
				/*
				 * this is the first time we encounter ParquetScan for a partitioned parquet table.
				 * Assign the maximum memory required for this table to this operator.
				 */
#if USE_ASSERT_CHECKING
				Oid part_oid =
#endif
				GetPartOidForParquetScan(node, root_oid, &memoryReservedForParquetScan, false /* no need remap */);
				Assert(InvalidOid != part_oid);
			}
		}
	}
	else
	{
		List *scanned_columns_list = GetScannedColumnsForTable(node, rel_oid);
		memoryReservedForParquetScan = memReservedForParquetScan(rel_oid, scanned_columns_list);
		list_free(scanned_columns_list);
	}
	return memoryReservedForParquetScan;
}

/**
 * Calculate memory required for parquet table insert
 *
 * rel_oid: target table's Oid, it can be a root table of a partitioned table
 * return:  memory in byte that is reserved for parquet table insert
 *          It will return 0 if the target table is not a parquet table or
 *          no parquet part table exists
 */
static uint64
MemoryReservedForParquetInsert(Oid rel_oid)
{
	Assert(InvalidOid != rel_oid);
	uint64 memoryReserved = 0;
	/* partitioned case */
	if (rel_is_partitioned(rel_oid))
	{
		GetPartOidForParquetInsert(rel_oid, &memoryReserved);
	}
	/* non-partition case, parquet table */
	else if(relstorage_is_aoparquet(get_rel_relstorage(rel_oid)))
	{
		memoryReserved = memReservedForParquetInsert(rel_oid);
	}

	return memoryReserved;
}

/**
 * Is an operator parquet scan?
 */
static bool
IsParquetScanOperator(Node *node, PlannedStmt *stmt)
{
	Assert(NULL != node);
	Assert(NULL != stmt);
	Assert(is_plan_node(node));
	switch(nodeTag(node))
	{
		case T_ParquetScan:
			return true;
		case T_TableScan:
			{
				Oid rel_oid = getrelid(((Scan *) node)->scanrelid, stmt->rtable);
				if (relstorage_is_aoparquet(get_rel_relstorage(rel_oid)))
				{
					return true;
				}
				else
				{
					return false;
				}
			}
		case T_DynamicTableScan:
			{
				uint64 memRequired = 0;
				Oid rel_oid = getrelid(((Scan *) node)->scanrelid, stmt->rtable);
				if (InvalidOid == GetPartOidForParquetScan(node, rel_oid, &memRequired, true /* need remap */))
				{
					return false;
				}
				else
				{
					return true;
				}
			}
		default:
			return false;
	}
}

/**
 * Is an operator parquet Insert?
 */
static bool
IsParquetInsertOperator(Node *node, PlannedStmt *stmt)
{
	Assert(NULL != node);
	Assert(NULL != stmt);
	Assert(is_plan_node(node));
	if (IsA(node, DML))
	{
		if (CMD_INSERT == stmt->commandType)
		{
			Oid rel_oid = getrelid(((Scan *) node)->scanrelid, stmt->rtable);
			/* partitioned case */
			if (rel_is_partitioned(rel_oid))
			{
				uint64 memRequired = 0;
				if (InvalidOid == GetPartOidForParquetInsert(rel_oid, &memRequired))
				{
					return false;
				}
				else
				{
					return true;
				}
			}
			/* non-partition case, parquet table */
			else if (relstorage_is_aoparquet(get_rel_relstorage(rel_oid)))
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		else
		{
			return false;
		}
	}
	else
	{
		return false;
	}
}

/**
 * Compute memory required for Parquet table insert if the plan
 * is produced by Planner. Because unlike ORCA, Planner does not put
 * insert in its plan nodes. We collect memory by traversing
 * the resultRelations and sum up the memory needed for each relation.
 *
 * stmt:   Plan statement
 * return: memory in byte that is reserved for parquet table insert
 *         It will return 0 if no parquet table exists.
 */
static uint64
MemoryReservedForParquetInsertForPlannerPlan(PlannedStmt *stmt)
{
	Assert(PLANGEN_PLANNER == stmt->planGen);
	Assert(CMD_INSERT == stmt->commandType);
	uint64 memRequired = 0;
	ListCell *lc = NULL;
	foreach(lc, stmt->resultRelations)
	{
		Oid rel_oid = getrelid(lfirst_int(lc), stmt->rtable);
		memRequired += MemoryReservedForParquetInsert(rel_oid);
	}
	return memRequired;
}

/*
 * Get list of columns need to be scanned for a parquet table
 *
 * The return list is palloc-ed in the current memory context, and the caller
 * is required to free it.
 *
 * node:    Plan node
 * rel_oid: target table's Oid
 * return:  List of columns that need to be scanned
 */
static List* 
GetScannedColumnsForTable(Node *node, Oid rel_oid)
{
	Plan *plan = (Plan *) &((Scan *) node)->plan;
	int num_col = get_relnatts(rel_oid);
	bool *proj = (bool *)palloc0(sizeof(bool) * num_col);
	GetNeededColumnsForScan((Node *)plan->targetlist, proj, num_col);
	GetNeededColumnsForScan((Node *)plan->qual, proj, num_col);
	List *result = NIL;
	for(int col_no = 0; col_no < num_col; col_no++)
	{
		if(proj[col_no])
		{
			result = lappend_int(result, col_no + 1);
		}
	}
	pfree(proj);
	
	/* In some cases (e.g.: count(*)), no column is specified.
	 * We always scan the first column. 	 
	 */
	if (0 == list_length(result))
	{
		result = lappend_int(result, 1);
	}
	return result;
}

/*
 * Iterate through all part tables and try to find the parquet part table that
 * has the maximum memory requirement.
 *
 * Return InvalidOid if no parquet part table exists.
 *
 * node:           Plan node
 * root_oid:       Oid of root table
 * maxMemRequired: Output argument for memory in byte that is reserved for the part table scan
 *
 */
static Oid GetPartOidForParquetScan(Node *node, Oid rootOid, uint64 *maxMemRequired, bool needRemap)
{
	Assert(NULL != maxMemRequired);
	Assert(rel_is_partitioned(rootOid));
	Oid maxRowgroupsizePartTableOid = InvalidOid;
	*maxMemRequired = 0;

	PartitionNode *pn = get_parts(rootOid, 0 /* level */, 0 /* parent */, false /* inctemplate */,
	                              CurrentMemoryContext, true /*include_subparts*/);
	Assert(pn);

	List *scanned_columns_list = GetScannedColumnsForTable(node, rootOid);
	
	/*
	 * If we have dropped columns in root table, we need to remap
	 * the attribute numbers for part tables to get accurate columns
	 * to be scanned.
	 */ 
	Relation rootRel = heap_open(rootOid, AccessShareLock);
	TupleDesc rootTupDesc = rootRel->rd_att;

	List *lRelOids = all_partition_relids(pn);
	ListCell *lc_part_oid = NULL;
	foreach (lc_part_oid, lRelOids)
	{
		Oid part_oid = lfirst_oid(lc_part_oid);
		if (relstorage_is_aoparquet(get_rel_relstorage(part_oid)))
		{
			/* remap scanned columns for part table if needed */
			Relation partRel = heap_open(part_oid, AccessShareLock);
			TupleDesc partTupDesc = partRel->rd_att;
			AttrNumber *attMap = varattnos_map(rootTupDesc, partTupDesc);
			List *remap_scanned_columns_list = NIL;
			if (attMap && needRemap)
			{
				ListCell *cell = NULL;
				foreach(cell, scanned_columns_list)
				{
					AttrNumber remap_att_num = attMap[lfirst_int(cell)-1];
					remap_scanned_columns_list = lappend_int(remap_scanned_columns_list, remap_att_num);
				}
			}
			/* no mapping needed */
			else
			{
				remap_scanned_columns_list = scanned_columns_list;
			}
			heap_close(partRel, AccessShareLock);
				
			uint64 memRequired = memReservedForParquetScan(part_oid, remap_scanned_columns_list);
			if (memRequired > *maxMemRequired)
			{
				*maxMemRequired = memRequired;
				maxRowgroupsizePartTableOid = part_oid;
			}

			/* clean up */
			if (attMap && needRemap)
			{
				list_free(remap_scanned_columns_list);
				pfree(attMap);
			}
		}
	}
	heap_close(rootRel, AccessShareLock);
	list_free(scanned_columns_list);
	
	return maxRowgroupsizePartTableOid;
}

/*
 * Iterate through all part tables and try to find the parquet part table that
 * has the maximum memory requirement for insert.
 *
 * Return InvalidOid if no parquet part table exists.
 *
 * root_oid:       Oid of root table
 * maxMemRequired: Output argument for memory in byte that is reserved for the part table insert
 *
 */
static Oid
GetPartOidForParquetInsert(Oid root_oid, uint64 *maxMemRequired)
{
	Assert(NULL != maxMemRequired);
	Assert(rel_is_partitioned(root_oid));
	Oid maxRowgroupsizePartTableOid = InvalidOid;
	*maxMemRequired = 0;

	PartitionNode *pn = get_parts(root_oid, 0 /* level */, 0 /* parent */, false /* inctemplate */,
	                              CurrentMemoryContext, true /*include_subparts*/);
	Assert(pn);

	List *lRelOids = all_partition_relids(pn);
	ListCell *lc_part_oid = NULL;
	foreach (lc_part_oid, lRelOids)
	{
		Oid part_oid = lfirst_oid(lc_part_oid);
		if (relstorage_is_aoparquet(get_rel_relstorage(part_oid)))
		{
			uint64 memRequired = memReservedForParquetInsert(part_oid);
			if (memRequired > *maxMemRequired)
			{
				*maxMemRequired = memRequired;
				maxRowgroupsizePartTableOid = part_oid;
			}
		}
	}
	return maxRowgroupsizePartTableOid;
}

/*
 * IsRootOperatorInGroup
 *    Return true if the given node is the root operator in an operator group.
 *
 * A node can be a root operator in a group if it satisfies the following three
 * conditions:
 * (1) a Plan node.
 * (2) a Blocking operator.
 * (3) not rescan required (no external parameters).
 */
static bool
IsRootOperatorInGroup(Node *node)
{
	return (is_plan_node(node) && IsBlockingOperator(node) && ((Plan *)(node))->extParam == NULL);
}

/**
 * This walker counts the number of memory intensive and non-memory intensive operators
 * in a plan.
 */

static bool PrelimWalker(Node *node, MemQuotaContext *context)
{
	if (node == NULL)
	{
		return false;
	}
	
	Assert(node);
	Assert(context);	
	if (is_plan_node(node))
	{
		if (IsParquetScanOperator(node, context->plannedStmt))
		{
			if (NULL == context->parquetRootOids)
			{
				context->parquetRootOids = createOidHTAB();
			}
			uint64 memResevedForParquetScan = MemoryReservedForParquetScan(node, context->plannedStmt, context->parquetRootOids);
			uint64 memResevedForParquetScanKB = (uint64) ( (double) memResevedForParquetScan / 1024);
			context->parquetOpReservedMemKB += memResevedForParquetScanKB;
			/* Assign the memory required to the operator so we do not need to compute it again */
			Plan *planNode = (Plan *) node;
			planNode->operatorMemKB = memResevedForParquetScanKB;
		}
		else if (IsParquetInsertOperator(node, context->plannedStmt))
		{
			/* get memory required for parquet table insert */
			Oid rel_oid = getrelid(((Scan *) node)->scanrelid, context->plannedStmt->rtable);
			uint64 memResevedForParquetInsert = MemoryReservedForParquetInsert(rel_oid);
			uint64 memResevedForParquetInsertKB = (uint64) ( (double) memResevedForParquetInsert / 1024);
			context->parquetOpReservedMemKB += memResevedForParquetInsertKB;
			/* Assign the memory required to the operator so we do not need to compute it again */
			Plan *planNode = (Plan *) node;
			planNode->operatorMemKB = memResevedForParquetInsertKB;
		}
		else if (IsMemoryIntensiveOperator(node, context->plannedStmt))
		{
			context->numMemIntensiveOperators++;
		}
		else
		{
			context->numNonMemIntensiveOperators++;
		}
	}
	return plan_tree_walker(node, PrelimWalker, context);
}

 
/**
 * What should be query mem such that memory intensive operators get a certain
 * minimum amount of memory. Return value is in KB.
 */
 uint64 StatementMemForNoSpillKB(PlannedStmt *stmt, uint64 minOperatorMemKB)
 {
	 Assert(stmt);
	 Assert(minOperatorMemKB > 0);
	 
	 const uint64 nonMemIntenseOpMemKB = (uint64) gp_resqueue_memory_policy_auto_fixed_mem;

	 MemQuotaContext ctx;
	 exec_init_plan_tree_base(&ctx.base, stmt);
	 ctx.queryMemKB = (uint64) (stmt->query_mem / 1024);
	 ctx.numMemIntensiveOperators = 0;
	 ctx.numNonMemIntensiveOperators = 0;
	 ctx.parquetOpReservedMemKB = 0;
	 ctx.plannedStmt = stmt;
	 ctx.parquetRootOids = NULL;
	 
#ifdef USE_ASSERT_CHECKING
	 bool result = 
#endif
			 	   PrelimWalker((Node *) stmt->planTree, &ctx);
	 
	 Assert(!result);
	 Assert(ctx.numMemIntensiveOperators + ctx.numNonMemIntensiveOperators > 0);
	 
	 /**
	  * Right now, the inverse is straightforward.
	  * TODO: Siva - employ binary search to find the right value.
	  */
	 uint64 requiredStatementMemKB = ctx.numNonMemIntensiveOperators * nonMemIntenseOpMemKB 
			 + ctx.numMemIntensiveOperators * minOperatorMemKB
			 + ctx.parquetOpReservedMemKB;

	 return requiredStatementMemKB;
 }

/*
 * CreateOperatorGroup
 *    create a new operator group with a specified id.
 */
static OperatorGroupNode *
CreateOperatorGroupNode(uint32 groupId, OperatorGroupNode *parentGroup)
{
	OperatorGroupNode *node = palloc0(sizeof(OperatorGroupNode));
	node->groupId = groupId;
	node->parentGroup = parentGroup;
	node->parquetOpReservedMemKB = 0;
	node->parquetRootOids = NULL;

	return node;
}

/*
 * IncrementOperatorCount
 *    Increment the count of operators in the current group based
 * on the type of the operator.
 */
static void
IncrementOperatorCount(Node *node, OperatorGroupNode *groupNode, PlannedStmt *stmt)
{
	Assert(node != NULL);
	Assert(groupNode != NULL);
	
	if (IsParquetScanOperator(node, stmt))
	{
		if (NULL == groupNode->parquetRootOids)
		{
			groupNode->parquetRootOids = createOidHTAB();
		}
		uint64 memResevedForParquetScan = MemoryReservedForParquetScan(node, stmt, groupNode->parquetRootOids);
		uint64 memResevedForParquetScanKB = (uint64) ((double) memResevedForParquetScan / 1024);
		groupNode->parquetOpReservedMemKB += memResevedForParquetScanKB;
		/* Assign the memory required to the operator so we do not need to compute it again */
		Plan *planNode = (Plan *) node;
		planNode->operatorMemKB = memResevedForParquetScanKB;

	}
	else if (IsParquetInsertOperator(node, stmt))
	{
		/* get memory required for parquet table insert */
		Oid rel_oid = getrelid(((Scan *) node)->scanrelid, stmt->rtable);
		uint64 memResevedForParquetInsert = MemoryReservedForParquetInsert(rel_oid);
		uint64 memResevedForParquetInsertKB = (uint64) ( (double) memResevedForParquetInsert / 1024);
		groupNode->parquetOpReservedMemKB += memResevedForParquetInsertKB;
		/* Assign the memory required to the operator so we do not need to compute it again */
		Plan *planNode = (Plan *) node;
		planNode->operatorMemKB = memResevedForParquetInsertKB;
	}
	else if (IsMemoryIntensiveOperator(node, stmt))
	{
		groupNode->numMemIntenseOps++;
	}
	else
	{
		groupNode->numNonMemIntenseOps++;
	}
}

/*
 * GetParentOperatorNode
 *    Return the parent operator group for a given group.
 */
static OperatorGroupNode *
GetParentOperatorGroup(OperatorGroupNode *groupNode)
{
	Assert(groupNode != NULL);
	return groupNode->parentGroup;
}


/*
 * CreateOperatorGroupForOperator
 *    create an operator group for a given operator node if the given operator node
 * is a potential root of an operator group.
 */
static OperatorGroupNode *
CreateOperatorGroupForOperator(Node *node,
							   PolicyEagerFreeContext *context)
{
	Assert(is_plan_node(node));

	OperatorGroupNode *groupNode = context->groupNode;
	
	/*
	 * If the group tree has not been built, we create the first operator
	 * group here.
	 */
	if (context->groupTree == NULL)
	{
		groupNode = CreateOperatorGroupNode(context->nextGroupId, NULL);
		Assert(groupNode != NULL);
		
		context->groupTree = groupNode;
		context->groupTree->groupMemKB = context->queryMemKB;
		context->nextGroupId++;
	}
	/*
	 * If this node is a potential root of an operator group, this means that
	 * the current group ends, and a new group starts. we create a new operator
	 * group.
	 */
	else if (IsRootOperatorInGroup(node))
	{
		Assert(groupNode != NULL);
		
		OperatorGroupNode *parentGroupNode = groupNode;
		groupNode = CreateOperatorGroupNode(context->nextGroupId, groupNode);
		if (parentGroupNode != NULL)
		{
			parentGroupNode->childGroups = lappend(parentGroupNode->childGroups, groupNode);
		}
		context->nextGroupId++;
	}

	return groupNode;
}

/*
 * FindOperatorGroupForOperator
 *   find the operator group for a given operator node that is the root operator
 * of the returning group.
 */
static OperatorGroupNode *
FindOperatorGroupForOperator(Node *node,
							 PolicyEagerFreeContext *context)
{
	Assert(is_plan_node(node));
	Assert(context->groupTree != NULL);

	OperatorGroupNode *groupNode = context->groupNode;

	/*
	 * If this is the beginning of the walk (or the current group is NULL), 
	 * this operator node belongs to the first operator group.
	 */
	if (groupNode == NULL)
	{
		groupNode = context->groupTree;
		context->nextGroupId++;
	}

	/*
	 * If this operator is a potential root of an operator group, this means
	 * the current group ends, and a new group start. We find the group that
	 * has this node as its root.
	 */
	else if (IsRootOperatorInGroup(node))
	{
		Assert(context->groupNode != NULL);
		
		ListCell *lc = NULL;
		OperatorGroupNode *childGroup = NULL;
		foreach(lc, context->groupNode->childGroups)
		{
			childGroup = (OperatorGroupNode *)lfirst(lc);
			if (childGroup->groupId == context->nextGroupId)
			{
				break;
			}
		}
		
		Assert(childGroup != NULL);
		groupNode = childGroup;
		context->nextGroupId++;
	}

	return groupNode;
}

#if 0
/*
 * FindOperatorGroupForNode
 *    find the operator group for a given node.
 *
 * If this is during prelim walker phase, this function creates such a group
 * if it does not exist. Otherwise, this function finds such a group in
 * the group tree.
 *
 * This function also returns the parentGroupNode pointer, and a boolean
 * indicating if the given node is the first node in the group.
 */
static OperatorGroupNode *
FindOperatorGroupForNode(Node *node,
						 PolicyEagerFreeContext *context,
						 bool isPrelimWalker,
						 OperatorGroupNode **parentGroupNodeP,
						 bool *isFirstInGroupP)
{
	Assert(is_plan_node(node));

	OperatorGroupNode *groupNode = context->groupNode;
	
	if (context->groupNode == NULL)
	{
		if (isPrelimWalker)
		{
			Assert(context->groupTree == NULL);
			context->groupTree = CreateOperatorGroupNode(context->nextGroupId, NULL);
			groupNode = context->groupTree;

			/* The memory limit for the first group is the query memory limit. */
			groupNode->groupMemKB = context->queryMemKB;
		}
		else
		{
			Assert(context->groupTree != NULL);
			groupNode = context->groupTree;
			Assert(groupNode->groupMemKB > 0);
		}

		context->nextGroupId++;
		*isFirstInGroupP = true;
	}

	/*
	 * If this node is a blocking operator and this node does not contain an extParam,
	 * it means that the current group ends, and a new group starts.
	 */
	if (!*isFirstInGroupP &&
		IsRootOperatorInGroup(node))
	{
		Assert(context->groupNode != NULL);
		if (isPrelimWalker)
		{
			groupNode = CreateOperatorGroupNode(context->nextGroupId, context->groupNode);
			context->groupNode->childGroups = lappend(context->groupNode->childGroups, groupNode);
		}
		else
		{
			ListCell *lc = NULL;
			OperatorGroupNode *childGroup = NULL;
			foreach(lc, context->groupNode->childGroups)
			{
				childGroup = (OperatorGroupNode *)lfirst(lc);
				if (childGroup->groupId == context->nextGroupId)
				{
					break;
				}
			}

			Assert(childGroup != NULL);
			groupNode = childGroup;
		}

		context->nextGroupId++;
		*isFirstInGroupP = true;
		*parentGroupNodeP = context->groupNode;
	}

	return groupNode;
}
#endif

/*
 * ComputeAvgMemKBForMemIntenseOp
 *    Compute the average memory limit for each memory-intensive operators
 * in a given group.
 *
 * If there is no memory-intensive operators in this group, return 0.
 */
static uint64
ComputeAvgMemKBForMemIntenseOp(OperatorGroupNode *groupNode)
{
	if (groupNode->numMemIntenseOps == 0)
	{
		return 0;
	}
	
	const uint64 nonMemIntenseOpMemKB = (uint64)gp_resqueue_memory_policy_auto_fixed_mem;

	return (((double)groupNode->groupMemKB
			 - (double)groupNode->numNonMemIntenseOps * nonMemIntenseOpMemKB
			 - (double) groupNode->parquetOpReservedMemKB ) /
			groupNode->numMemIntenseOps);
}

/*
 * ComputeMemLimitForChildGroups
 *    compute the query memory limit for all child groups of a given
 * parent group if it has not been computed before.
 */
static void
ComputeMemLimitForChildGroups(OperatorGroupNode *parentGroupNode)
{
	Assert(parentGroupNode != NULL);
	
	uint64 totalNumMemIntenseOps = 0;
	uint64 totalNumNonMemIntenseOps = 0;

	ListCell *lc;
	foreach(lc, parentGroupNode->childGroups)
	{
		OperatorGroupNode *childGroup = (OperatorGroupNode *)lfirst(lc);

		/*
		 * If the memory limit has been computed, then we are done.
		 */
		if (childGroup->groupMemKB != 0)
		{
			return;
		}
		
		totalNumMemIntenseOps += 
			Max(childGroup->maxNumConcMemIntenseOps, childGroup->numMemIntenseOps);
		totalNumNonMemIntenseOps += 
			Max(childGroup->maxNumConcNonMemIntenseOps, childGroup->numNonMemIntenseOps);
	}

	const uint64 nonMemIntenseOpMemKB = (uint64)gp_resqueue_memory_policy_auto_fixed_mem;

	foreach(lc, parentGroupNode->childGroups)
	{
		OperatorGroupNode *childGroup = (OperatorGroupNode *)lfirst(lc);
		
		Assert(childGroup->groupMemKB == 0);
		
		if (parentGroupNode->groupMemKB < totalNumNonMemIntenseOps * nonMemIntenseOpMemKB
											+ parentGroupNode->parquetOpReservedMemKB)
		{
			if (parentGroupNode->parquetOpReservedMemKB > 0)
			{
				ereport(ERROR,
				        (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				         errmsg("insufficient memory reserved for statement"),
				         errhint("Increase statement memory or reduce the number of Parquet tables to be scanned.")));
			}
			else
			{
				ereport(ERROR,
				        (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				         errmsg("insufficient memory reserved for statement")));
			}

		}

		double memIntenseOpMemKB = 0;
		if (totalNumMemIntenseOps > 0)
		{
			memIntenseOpMemKB =
				((double)(parentGroupNode->groupMemKB
						  - totalNumNonMemIntenseOps * nonMemIntenseOpMemKB
						  - parentGroupNode->parquetOpReservedMemKB)) /
				((double)totalNumMemIntenseOps);
		}

		Assert(parentGroupNode->groupMemKB > 0);
		
		/*
		 * MPP-23130
		 * In memory policy eager free, scaleFactor is used to balance memory allocated to
		 * each child group based on the number of memory-intensive and non-memory-intensive
		 * operators they have. The calculation of scaleFactor is as follows:
		 * scaleFactor = (memIntensiveOpMem *
		 *               Max(childGroup->maxNumConcMemIntenseOps, childGroup->numMemIntenseOps)
		 *              + nonMemIntenseOpMemKB *
		 *               Max(childGroup->maxNumConcNonMemIntenseOps, childGroup->numNonMemIntenseOps))
		 *               / parentGroupNode->groupMemKB
		 * Child group's memory: childGroup->groupMemKB = scaleFactor * parentGroupNode->groupMemKB,
		 * which is the denominator of the scaleFactor formula.
		 */ 	
		childGroup->groupMemKB  = (uint64) (memIntenseOpMemKB *
					           Max(childGroup->maxNumConcMemIntenseOps, childGroup->numMemIntenseOps) +
					           nonMemIntenseOpMemKB * 
					           Max(childGroup->maxNumConcNonMemIntenseOps, childGroup->numNonMemIntenseOps) +
					           childGroup->parquetOpReservedMemKB);
	}
}

/*
 * PolicyEagerFreePrelimWalker
 *    Walk the plan tree to build a group tree by dividing the plan tree
 * into several groups, each of which has a block operator as its border
 * node (except for the leaves of the leave groups). At the same time,
 * we collect some stats information about operators in each group.
 */
static bool
PolicyEagerFreePrelimWalker(Node *node, PolicyEagerFreeContext *context)
{
	if (node == NULL)
	{
		return false;
	}
	
	Assert(node);
	Assert(context);

	OperatorGroupNode *parentGroupNode = NULL;
	bool isTopPlanNode = false;
	
	if (is_plan_node(node))
	{
		if (context->groupTree == NULL)
		{
			isTopPlanNode = true;
		}
		
		context->groupNode = CreateOperatorGroupForOperator(node, context);
		Assert(context->groupNode != NULL);

		IncrementOperatorCount(node, context->groupNode, context->plannedStmt);
		
		/*
		 * We also increment the parent group's counter if this node
		 * is the root node in a new group.
		 */
		parentGroupNode = GetParentOperatorGroup(context->groupNode);
		if (IsRootOperatorInGroup(node) && parentGroupNode != NULL)
		{
			IncrementOperatorCount(node, parentGroupNode, context->plannedStmt);
		}
	}

	bool result = plan_tree_walker(node, PolicyEagerFreePrelimWalker, context);
	Assert(!result);
	
	/*
	 * If this node is the top nodoe in a group, at this point, we should have all info about
	 * its child groups. We then calculate the maximum number of potential concurrently
	 * active memory-intensive operators and non-memory-intensive operators in all
	 * child groups.
	 */
	if (isTopPlanNode || IsRootOperatorInGroup(node))
	{
		Assert(context->groupNode != NULL);
			
		uint64 maxNumConcNonMemIntenseOps = 0;
		uint64 maxNumConcMemIntenseOps = 0;
		uint64 childrenParquetOpReservedMem = 0;	
		ListCell *lc;
		foreach(lc, context->groupNode->childGroups)
		{
			OperatorGroupNode *childGroup = (OperatorGroupNode *)lfirst(lc);
			maxNumConcNonMemIntenseOps += 
				Max(childGroup->maxNumConcNonMemIntenseOps, childGroup->numNonMemIntenseOps);
			maxNumConcMemIntenseOps +=
				Max(childGroup->maxNumConcMemIntenseOps, childGroup->numMemIntenseOps);
			childrenParquetOpReservedMem += childGroup->parquetOpReservedMemKB;
		}
		
		Assert(context->groupNode->maxNumConcNonMemIntenseOps == 0 &&
			   context->groupNode->maxNumConcMemIntenseOps == 0);
		context->groupNode->maxNumConcNonMemIntenseOps = maxNumConcNonMemIntenseOps;
		context->groupNode->maxNumConcMemIntenseOps = maxNumConcMemIntenseOps;
		context->groupNode->parquetOpReservedMemKB += childrenParquetOpReservedMem;
		/* Reset the groupNode to point to its parentGroupNode */
		context->groupNode = GetParentOperatorGroup(context->groupNode);
	}
	
	return result;
}

/*
 * PolicyEagerFreeAssignWalker
 *    Walk the plan tree and assign the memory to each plan node.
 */
static bool
PolicyEagerFreeAssignWalker(Node *node, PolicyEagerFreeContext *context)
{
	if (node == NULL)
	{
		return false;
	}
	
	Assert(node);
	Assert(context);
	
	const uint64 nonMemIntenseOpMemKB = (uint64)gp_resqueue_memory_policy_auto_fixed_mem;

	if (is_plan_node(node))
	{
		Plan *planNode = (Plan *)node;
		
		context->groupNode = FindOperatorGroupForOperator(node, context);
		Assert(context->groupNode != NULL);

		/*
		 * If this is the root node in a group, compute the new query limit for
		 * all child groups of the parent group.
		 */
		if (IsRootOperatorInGroup(node) &&
			GetParentOperatorGroup(context->groupNode) != NULL)
		{
			ComputeMemLimitForChildGroups(GetParentOperatorGroup(context->groupNode));
		}

		if (IsParquetScanOperator(node, context->plannedStmt) || IsParquetInsertOperator(node, context->plannedStmt))
		{
			/* do nothing as we already assigned the memory to the operator */
		}
		else if (!IsMemoryIntensiveOperator(node, context->plannedStmt))
		{
			planNode->operatorMemKB = nonMemIntenseOpMemKB;
		}
		else
		{
			/*
			 * Evenly distribute the remaining memory among all memory-intensive
			 * operators.
			 */
			uint64 memKB = ComputeAvgMemKBForMemIntenseOp(context->groupNode);

			Assert(planNode->operatorMemKB == 0);

			planNode->operatorMemKB = memKB;

			OperatorGroupNode *parentGroupNode = GetParentOperatorGroup(context->groupNode);

			/*
			 * If this is the root node in the group, we also calculate the memory
			 * for this node as it appears in the parent group. The final memory limit
			 * for this node is the minimal value of the two.
			 */
			if (IsRootOperatorInGroup(node) &&
				parentGroupNode != NULL)
			{
				uint64 memKBInParentGroup = ComputeAvgMemKBForMemIntenseOp(parentGroupNode);

				if (memKBInParentGroup < planNode->operatorMemKB)
				{
					planNode->operatorMemKB = memKBInParentGroup;
				}
			}
		}
	}

	bool result = plan_tree_walker(node, PolicyEagerFreeAssignWalker, context);
	Assert(!result);
	
	/*
	 * If this node is the root in a group, we reset some values in the context.
	 */
	if (IsRootOperatorInGroup(node))
	{
		Assert(context->groupNode != NULL);
		context->groupNode = GetParentOperatorGroup(context->groupNode);
	}

	return result;
}

/*
 * AssignOperatorMemoryKB
 *    Main entry point for memory quota OPTIMIZE. This function distributes the memory
 * among all operators in a more optimized way than the AUTO policy.
 *
 * This function considers not all memory-intensive operators will be active concurrently,
 * and distributes the memory accordingly.
 */
void
AssignOperatorMemoryKB(PlannedStmt *stmt, uint64 memAvailableBytes)
{
	PolicyEagerFreeContext ctx;
	exec_init_plan_tree_base(&ctx.base, stmt);
	ctx.groupTree = NULL;
	ctx.groupNode = NULL;
	ctx.nextGroupId = 0;
	ctx.queryMemKB = memAvailableBytes / 1024;
	ctx.plannedStmt = stmt;

	/* If it is a planner's plan for parquet insert, we need to reserve
	 * the memory before traverse the plan nodes. Because unlike ORCA,
	 * insert is not in the plan nodes. So we compute the memory required
	 * for parquet table insert and subtract it from the available query memory.
	*/
	if (PLANGEN_PLANNER == stmt->planGen && CMD_INSERT == stmt->commandType)
	{
		uint64 memRequiredForParquetInsert = MemoryReservedForParquetInsertForPlannerPlan(stmt);
		uint64 memRequiredForParquetInsertKB = (uint64) ( (double) memRequiredForParquetInsert / 1024);
		 if (ctx.queryMemKB <= memRequiredForParquetInsertKB)
		 {
			 ereport(ERROR,
			         (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			          errmsg("insufficient memory reserved for statement"),
			          errhint("Increase statement memory for parquet table insert.")));
		 }
		 ctx.queryMemKB -= memRequiredForParquetInsertKB;
	}

#ifdef USE_ASSERT_CHECKING
	bool result = 
#endif
		PolicyEagerFreePrelimWalker((Node *) stmt->planTree, &ctx);
	 
	Assert(!result);
	
	/*
	 * Reset groupNode and nextGroupId so that we can start from the
	 * beginning of the group tree.
	 */
	ctx.groupNode = NULL;
	ctx.nextGroupId = 0;

	/*
	 * Check if memory exceeds the limit in the root group
	 */
	const uint64 nonMemIntenseOpMemKB = (uint64)gp_resqueue_memory_policy_auto_fixed_mem;
	if (ctx.groupTree->groupMemKB < ctx.groupTree->numNonMemIntenseOps * nonMemIntenseOpMemKB
									+ ctx.groupTree->parquetOpReservedMemKB)
	{
		if(ctx.groupTree->parquetOpReservedMemKB > 0)
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			         errmsg("insufficient memory reserved for statement"),
			         errhint("Increase statement memory or reduce the number of Parquet tables to be scanned.")));
		}
		else
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			         errmsg("insufficient memory reserved for statement")));
		}
	}

#ifdef USE_ASSERT_CHECKING
	result = 
#endif			 
		PolicyEagerFreeAssignWalker((Node *) stmt->planTree, &ctx);
	 
	Assert(!result);
}



