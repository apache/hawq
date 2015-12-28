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
/*
 * walkers.h
 *
 *  Created on: Feb 8, 2011
 *      Author: siva
 */

#ifndef WALKERS_H_
#define WALKERS_H_

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/relation.h"

/* flags bits for query_tree_walker and query_tree_mutator */
#define QTW_IGNORE_RT_SUBQUERIES	0x01		/* subqueries in rtable */
#define QTW_IGNORE_CTE_SUBQUERIES	0x02		/* subqueries in cteList */
#define QTW_IGNORE_RC_SUBQUERIES	0x03		/* both of above */
#define QTW_IGNORE_JOINALIASES		0x04		/* JOIN alias var lists */
#define QTW_EXAMINE_RTES			0x08		/* examine RTEs */
#define QTW_DONT_COPY_QUERY			0x10		/* do not copy top Query */

extern bool expression_tree_walker(Node *node, bool (*walker) (),
											   void *context);

extern bool query_tree_walker(Query *query, bool (*walker) (),
										  void *context, int flags);

extern bool range_table_walker(List *rtable, bool (*walker) (),
										   void *context, int flags);

extern bool query_or_expression_tree_walker(Node *node, bool (*walker) (),
												   void *context, int flags);


/* The plan associated with a SubPlan is found in a list.  During planning this is in
 * the global structure found through the root PlannerInfo.  After planning this is in
 * the PlannedStmt.
 *
 * Structure plan_tree_base_prefix carries the appropriate pointer for GPDB's general plan
 * tree walker/mutator framework.  All users of the framework must prefix their context
 * structure with a plan_tree_base_prefix and initialize it appropriately.
 */
typedef struct plan_tree_base_prefix
{
	Node *node; /* PlannerInfo* or PlannedStmt* */
} plan_tree_base_prefix;

extern void planner_init_plan_tree_base(plan_tree_base_prefix *base, PlannerInfo *root);
extern void global_init_plan_tree_base(plan_tree_base_prefix *base, PlannerGlobal *glob);
extern void exec_init_plan_tree_base(plan_tree_base_prefix *base, PlannedStmt *stmt);
extern void null_init_plan_tree_base(plan_tree_base_prefix *base);
extern Plan *plan_tree_base_subplan_get_plan(plan_tree_base_prefix *base, SubPlan *subplan);
extern void plan_tree_base_subplan_put_plan(plan_tree_base_prefix *base, SubPlan *subplan, Plan *plan);

extern bool walk_plan_node_fields(Plan *plan, bool (*walker) (), void *context);

extern bool plan_tree_walker(Node *node, bool (*walker) (), void *context);

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Useful functions that aggregate information from expressions or plans.
 */
extern List *extract_nodes(PlannerGlobal *glob, Node *node, int nodeTag);
extern bool contains_node_type(PlannerGlobal *glob, Node *node, int nodeTag);
extern List *extract_nodes_plan(Plan *pl, int nodeTag, bool descendIntoSubqueries);
extern List *extract_nodes_expression(Node *node, int nodeTag, bool descendIntoSubqueries);
extern int find_nodes(Node *node, List *nodeTags);

#ifdef __cplusplus
}
#endif

#endif /* WALKERS_H_ */
