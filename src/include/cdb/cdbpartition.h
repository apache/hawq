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

/*--------------------------------------------------------------------------
 *
 * cdbpartition.h
 *	 Definitions and API functions for cdbpartition.c
 *
 *--------------------------------------------------------------------------
 */
#ifndef CDBPARTITION_H
#define CDBPARTITION_H

#include "catalog/gp_policy.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_rule.h"
#include "nodes/execnodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/plancat.h"
#include "utils/hsearch.h"
#include "utils/palloc.h"
#include "utils/relcache.h"

typedef enum
{
	PART_STATUS_NONE,	/* a regular table */
	PART_STATUS_ROOT,	/* a partitioned table */
	PART_STATUS_INTERIOR,	/* a non-leaf part of a partitioned table */
	PART_STATUS_LEAF	/* a leaf part of a partitioned table */
} PartStatus;

typedef enum
{
	PART_TABLE,
	PART_PART,
	PART_CAND
} PartExchangeRole;

/* cache of function lookups for range partition selection */
typedef struct PartitionRangeState
{
	FmgrInfo *ltfuncs_direct; /* comparator expr < partRule */
	FmgrInfo *lefuncs_direct; /* comparator expr <= partRule */
	FmgrInfo *ltfuncs_inverse; /* comparator partRule < expr */
	FmgrInfo *lefuncs_inverse; /* comparator partRule <= expr */
	int last_rule; /* cache offset to the last rule and test if it matches */
	PartitionRule **rules;
} PartitionRangeState;

/* likewise, for list */
typedef struct PartitionListState
{
	FmgrInfo *eqfuncs;
	bool *eqinit;
} PartitionListState;

/* likewise, for hash */
typedef struct PartitionHashState
{
	FmgrInfo *hashfuncs;
	bool *hashinit;
} PartitionHashState;

/* part rule update */
typedef struct part_rule_cxt
{
	Oid old_oid;
	Oid new_oid;
} part_rule_cxt;


typedef struct LogicalIndexes
{
	int			numLogicalIndexes;
	LogicalIndexInfo	**logicalIndexInfo;
} LogicalIndexes;

#define parkindIsHash(c)   ((c) == 'h')
#define parkindIsRange(c)  ((c) == 'r')
#define parkindIsList(c)   ((c) == 'l')

extern bool rel_is_partitioned(Oid relid);

extern List *rel_partition_key_attrs(Oid relid);

extern List *rel_partition_keys_ordered(Oid relid);

extern bool rel_is_child_partition(Oid relid);

extern bool rel_is_leaf_partition(Oid relid);

extern bool rel_partitioning_is_uniform(Oid rootOid);

extern PartStatus rel_part_status(Oid relid);

extern List *
cdb_exchange_part_constraints(Relation table, Relation part, Relation cand, 
							  bool validate, bool is_split);

extern void
add_reg_part_dependency(Oid tableconid, Oid partconid);

extern PartitionByType
char_to_parttype(char c);

extern int
del_part_template(Oid rootrelid, int16 parlevel, Oid parent);

extern void
add_part_to_catalog(Oid relid, PartitionBy *pby, bool bTemplate_Only);

extern void
parruleord_reset_rank(Oid partid, int2 level, Oid parent, int2 ruleord, MemoryContext mcxt);

extern void
parruleord_open_gap(Oid partid, int2 level, Oid parent, int2 ruleord, int stopkey, MemoryContext mcxt);

extern AttrNumber 
max_partition_attr(PartitionNode *pn);

extern List *
get_partition_attrs(PartitionNode *pn);

extern int 
num_partition_levels(PartitionNode *pn);

extern PartitionRule *
ruleMakePartitionRule(HeapTuple tuple,
											TupleDesc tupdesc,
											MemoryContext mcxt);

extern Partition *
partMakePartition(HeapTuple tuple,
									TupleDesc tupdesc,
									MemoryContext mcxt);

extern List *
all_partition_relids(PartitionNode *pn);

extern Node *
get_relation_part_constraints(Oid rootOid, List **defaultLevels);

extern List *
all_prule_relids(PartitionRule *prule);

extern PgPartRule *
get_part_rule1(Relation rel, AlterPartitionId *pid,
								  bool bExistError, bool bMustExist,
								  MemoryContext mcxt, int *pSearch,
								  PartitionNode *pNode,
								  char *relname, PartitionNode **ppNode);

extern PgPartRule *
get_part_rule(Relation rel, AlterPartitionId *pid,
								 bool bExistError, bool bMustExist,
								 MemoryContext mcxt, int *pSearch,
								 bool inctemplate);


extern char *
rel_get_part_path_pretty(Oid relid, char *separator, char *lastsep);

extern bool 
partition_policies_equal(GpPolicy *p, PartitionNode *pn);

extern void 
partition_get_policies_attrs(PartitionNode *pn,
										 GpPolicy *master_policy,
							             List **cols);

/* RelationBuildPartitionDesc is built from get_parts */
extern PartitionNode *
get_parts(Oid relid, int2 level, Oid parent, bool inctemplate,
		  MemoryContext mcxt, bool includesubparts);

extern List *
rel_get_leaf_children_relids(Oid relid);

extern Oid 
rel_partition_get_root(Oid relid);

extern Oid  
rel_partition_get_master(Oid relid);

extern char *
ChoosePartitionName(const char *tablename, int partDepth, const char *partname, Oid nspace);

extern Oid selectPartition(PartitionNode *partnode, Datum *values,
						   bool *isnull, TupleDesc tupdesc,
						   PartitionAccessMethods *accessMethods);

extern Node *atpxPartAddList(Relation rel, 
							 AlterPartitionCmd *pc,
							 PartitionNode *pNode, 
							 Node *pUtl,
							 Node *partName, 
							 bool isDefault,
							 PartitionElem *pelem,
							 PartitionByType part_type,
							 PgPartRule* par_prule,
							 char *lrelname,
							 bool bSetTemplate,
							 Oid ownerid);
extern List *
atpxDropList(Relation rel, PartitionNode *pNode);

extern void 
exchange_part_rule(Oid oldrelid, Oid newrelid);

extern void
exchange_permissions(Oid oldrelid, Oid newrelid);

extern bool
atpxModifyListOverlap (Relation rel,
					   AlterPartitionId *pid,
					   PgPartRule   	*prule,
					   PartitionElem 	*pelem,
					   bool bAdd);
extern bool
atpxModifyRangeOverlap (Relation 		 		 rel,
						AlterPartitionId 		*pid,
						PgPartRule   			*prule,
						PartitionElem 			*pelem);
extern List *
atpxRenameList(PartitionNode *pNode,
			   char *old_parentname, const char *new_parentname, int *skipped);

extern List *
basic_AT_oids(Relation rel, AlterTableCmd *cmd);

extern AlterTableCmd *basic_AT_cmd(AlterTableCmd *cmd);
extern bool can_implement_dist_on_part(Relation rel, List *dist_cnames);
extern bool is_exchangeable(Relation rel, Relation oldrel, Relation newrel, bool fthrow);

extern char *
ChooseConstraintNameForPartitionEarly(Relation rel, ConstrType contype, Node *expr);

extern void
fixCreateStmtForPartitionedTable(CreateStmt *stmt);

extern void
checkUniqueConstraintVsPartitioning(Relation rel, AttrNumber *indattr, int nidxatts, bool primary);

extern List *
selectPartitionMulti(PartitionNode *partnode, Datum *values, bool *isnull,
				 TupleDesc tupdesc, PartitionAccessMethods *accessMethods);

extern void AddPartitionEncoding(Oid relid, Oid paroid, AttrNumber attnum,
								 bool deflt, List *encoding);
extern void RemovePartitionEncodingByRelid(Oid relid);
extern void RemovePartitionEncodingByRelidAttribute(Oid relid, AttrNumber attnum);
extern Datum *get_partition_encoding_attoptions(Relation rel, Oid paroid);

extern LogicalIndexes * BuildLogicalIndexInfo(Oid relid);
extern Oid getPhysicalIndexRelid(LogicalIndexInfo *iInfo, Oid partOid);
extern Oid	exprType(Node *expr);
extern Node *makeBoolConst(bool value, bool isnull);

extern LogicalIndexInfo *logicalIndexInfoForIndexOid(Oid rootOid, Oid indexOid);

extern void InsertPidIntoDynamicTableScanInfo(int32 index, Oid partOid, int32 selectorId);

extern char *
DebugPartitionOid(Datum *elements, int n);

extern void
GetSelectedPartitionOids(HTAB *partOidHash, Datum **partOids, long *partCount);

extern void
LogSelectedPartitionOids(HTAB *pidIndex);

extern List *
all_leaf_partition_relids(PartitionNode *pn);

extern List *
all_interior_partition_relids(PartitionNode *pn);

extern List *rel_get_part_path1(Oid relid);

extern int
countLeafPartTables(Oid rootOid);

extern void
findPartitionMetadataEntry(List *partsMetadata, Oid partOid, PartitionNode **partsAndRules,
							PartitionAccessMethods **accessMethods);

extern void
createValueArrays(int keyAttno, Datum **values, bool **isnull);

extern void
freeValueArrays(Datum *values, bool *isnull);

extern PartitionRule*
get_next_level_matched_partition(PartitionNode *partnode, Datum *values, bool *isnull,
								TupleDesc tupdesc, PartitionAccessMethods *accessMethods, Oid exprTypid);

#endif   /* CDBPARTITION_H */
