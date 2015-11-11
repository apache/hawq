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
 *	  definitions for cdbmutate.c utilities
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBMUTATE_H
#define CDBMUTATE_H

#include "nodes/plannodes.h"
#include "nodes/params.h"
#include "nodes/relation.h"

extern Plan *apply_motion(struct PlannerInfo *root, Plan *plan, Query *query);

extern Motion *make_union_motion(Plan *lefttree,
		                                int destSegIndex, bool useExecutorVarFormat);
extern Motion *make_sorted_union_motion(Plan *lefttree,
                                        int destSegIndex,
				int numSortCols, AttrNumber *sortColIdx, Oid *sortOperators, bool useExecutorVarFormat);
extern Motion *make_hashed_motion(Plan *lefttree,
				    List *hashExpr, bool useExecutorVarFormat);

extern Motion *make_broadcast_motion(Plan *lefttree, bool useExecutorVarFormat);

extern Motion *make_explicit_motion(Plan *lefttree, AttrNumber segidColIdx, bool useExecutorVarFormat);

void 
cdbmutate_warn_ctid_without_segid(struct PlannerInfo *root, struct RelOptInfo *rel);

extern void add_slice_to_motion(Motion *m,
		MotionType motionType, List *hashExpr, 
		int numOutputSegs, int *outputSegIdx 
		);

extern Plan *zap_trivial_result(PlannerInfo *root, Plan *plan); 
extern Plan *apply_shareinput_dag_to_tree(Plan *plan, ApplyShareInputContext *ctxt); 
extern Plan *apply_shareinput_xslice(Plan *plan, PlannerGlobal *glob);
extern void assign_plannode_id(PlannedStmt *stmt);

extern List *getExprListFromTargetList(List *tlist, int numCols, AttrNumber *colIdx,
									   bool useExecutorVarFormat);

extern int32 cdbhash_const(Const *pconst, int iSegments);
extern int32 cdbhash_const_list(List *plConsts, int iSegments);

#endif   /* CDBMUTATE_H */
