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
 * newPlanner.h
 *	  convert hawq plan to common plan in google protobuf
 *
 *-------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_OPTIMIZER_NEWPLANNER_H_
#define SRC_INCLUDE_OPTIMIZER_NEWPLANNER_H_

#include "postgres.h"

#include "nodes/plannodes.h"
#include "optimizer/walkers.h"
#include "univplan/cwrapper/univplan-c.h"

extern const char *new_executor_mode_on;
extern const char *new_executor_mode_auto;
extern const char *new_executor_mode_off;
extern char *new_executor_mode;
extern char *new_executor_enable_partitioned_hashagg_mode;
extern char *new_executor_enable_partitioned_hashjoin_mode;
extern char *new_executor_enable_external_sort_mode;
extern int new_executor_partitioned_hash_recursive_depth_limit;
extern int new_executor_ic_tcp_client_limit_per_query_per_segment;
extern int new_executor_external_sort_memory_limit_size_mb;

extern const char *new_executor_runtime_filter_mode;
extern const char *new_executor_runtime_filter_mode_local;
extern const char *new_executor_runtime_filter_mode_global;

extern int new_interconnect_type;
extern const char *show_new_interconnect_type();

#define MAGMATYPE "magma"
#define ORCTYPE "orc"
#define TEXTTYPE "text"
#define CSVTYPE "csv"

typedef struct CommonPlanContext {
  plan_tree_base_prefix base;
  UnivPlanC *univplan;
  bool convertible;
  bool querySelect;  // flag of query statement
  bool isMagma;  // flag to indicate whether there is a magma table in the plan
  int magmaRelIndex;
  PlannedStmt *stmt;
  bool setDummyTListRef;
  bool scanReadStatsOnly;
  Expr *parent;                // used for T_Var, T_Const
  List *exprBufStack;          // used for T_Case
  int rangeNum;                // magma range num
  bool isConvertingIndexQual;  // flag to indicate if we are doing indexqual
                               // conversion
  List *idxColumns;            // orc index columns info
} CommonPlanContext;

extern bool can_convert_common_plan(QueryDesc *queryDesc,
                                    CommonPlanContext *ctx);
extern void convert_to_common_plan(PlannedStmt *stmt, CommonPlanContext *ctx);
extern void convert_rangenum_to_common_plan(PlannedStmt *stmt,
                                            CommonPlanContext *ctx);
extern void convert_extscan_to_common_plan(Plan *node, List *splits,
                                           Relation rel,
                                           CommonPlanContext *ctx);
extern void *convert_orcscan_qual_to_common_plan(Plan *node,
                                                 CommonPlanContext *ctx);
extern void *convert_orcscan_indexqualorig_to_common_plan(
    Plan *node, CommonPlanContext *ctx, List *idxColumns);
extern void convert_querydesc_to_common_plan(QueryDesc *queryDesc,
                                             CommonPlanContext *ctx);
extern void planner_init_common_plan_context(PlannedStmt *stmt,
                                             CommonPlanContext *ctx);
extern void planner_destroy_common_plan_context(CommonPlanContext *ctx,
                                                bool enforce);

#endif // SRC_INCLUDE_OPTIMIZER_NEWPLANNER_H_
