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
 * cdbgroup.h
 *	  prototypes for cdbgroup.c.
 *
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBGROUP_H
#define CDBGROUP_H

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"   /* GroupContext */

#include "optimizer/clauses.h" /* AggClauseCounts */

extern Plan *
cdb_grouping_planner(PlannerInfo* root,
					 AggClauseCounts *agg_counts,
					 GroupContext *group_context);

extern bool cdbpathlocus_collocates(CdbPathLocus locus, List *pathkeys, bool exact_match);
extern CdbPathLocus cdbpathlocus_from_flow(Flow *flow);
extern void adapt_flow_to_targetlist(Plan *plan);
extern void generate_three_tlists(List *tlist,
								  bool twostage,
								  List *sub_tlist,
								  Node *havingQual,
								  int numGroupCols,
								  AttrNumber *groupColIdx,
								  List **p_tlist1,
								  List **p_tlist2,
								  List **p_tlist3,
								  List **p_final_qual);
extern Plan *add_second_stage_agg(PlannerInfo *root,
								  bool is_agg,
								  List *prelim_tlist,
								  List *final_tlist,
								  List *final_qual,
								  AggStrategy aggstrategy,
								  int numGroupCols,
								  AttrNumber *prelimGroupColIdx,
								  int num_nullcols,
								  uint64 input_grouping,
								  uint64 grouping,
								  int rollup_gs_times,
								  double numGroups,
								  int numAggs,
								  int transSpace,
								  const char *alias,
								  List **p_current_pathkeys,
								  Plan *result_plan,
								  bool use_root,
								  bool adjust_scatter);
extern List *generate_subquery_tlist(Index varno, List *input_tlist,
									 bool keep_resjunk, int **p_resno_map);
extern List *reconstruct_pathkeys(PlannerInfo *root, List *pathkeys, int *resno_map,
								  List *orig_tlist, List *new_tlist);
extern List *augment_subplan_tlist(List *tlist, List *exprs, int *pnum, AttrNumber **pcols, bool return_resno);

extern Plan *within_agg_planner(PlannerInfo *root, AggClauseCounts *agg_counts,
								GroupContext *group_context);

#endif   /* CDBGROUP_H */
