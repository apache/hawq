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
 * cdbllize.h
 *	  definitions for cdbplan.c utilities
 *
 *
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBLLIZE_H
#define CDBLLIZE_H

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/params.h"
#include "cdb/cdbplan.h"

extern Plan *cdbparallelize(struct PlannerInfo *root, Plan *plan, Query *query,
							int cursorOptions, 
							ParamListInfo boundParams);

extern bool is_plan_node(Node *node);

extern Flow *makeFlow(FlowType flotype);

extern Flow *pull_up_Flow(Plan *plan, Plan *subplan, bool withSort);

extern bool focusPlan(Plan *plan, bool stable, bool rescannable);
extern bool repartitionPlan(Plan *plan, bool stable, bool rescannable, List *hashExpr);
extern bool broadcastPlan(Plan *plan, bool stable, bool rescannable);

#endif   /* CDBLLIZE_H */
