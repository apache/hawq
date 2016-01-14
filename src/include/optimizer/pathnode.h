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
 * pathnode.h
 *	  prototypes for pathnode.c, relnode.c.
 *
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/optimizer/pathnode.h,v 1.72 2006/10/04 00:30:09 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PATHNODE_H
#define PATHNODE_H

#include "nodes/relation.h"
#include "cdb/cdbdef.h"                 /* CdbVisitOpt */

/*
 * prototypes for pathnode.c
 */

Path *
pathnode_copy_node(const Path *s);

CdbVisitOpt
pathnode_walk_node(Path            *path,
			       CdbVisitOpt    (*walker)(Path *path, void *context),
			       void            *context);
CdbVisitOpt
pathnode_walk_kids(Path            *path,
			       CdbVisitOpt    (*walker)(Path *path, void *context),
			       void            *context);
CdbVisitOpt
pathnode_walk_list(List            *pathlist,
			       CdbVisitOpt    (*walker)(Path *path, void *context),
			       void            *context);

extern int compare_path_costs(Path *path1, Path *path2,
				   CostSelector criterion);
extern int compare_fractional_path_costs(Path *path1, Path *path2,
							  double fraction);
extern void set_cheapest(PlannerInfo *root, RelOptInfo *parent_rel);    /*CDB*/
extern void add_path(PlannerInfo *root, RelOptInfo *parent_rel, Path *new_path);

extern Path *create_seqscan_path(PlannerInfo *root, RelOptInfo *rel);
extern ExternalPath *create_external_path(PlannerInfo *root, RelOptInfo *rel);
extern AppendOnlyPath *create_appendonly_path(PlannerInfo *root, RelOptInfo *rel);
extern ParquetPath *create_parquet_path(PlannerInfo *root, RelOptInfo *rel);
extern IndexPath *create_index_path(PlannerInfo *root,
				  IndexOptInfo *index,
				  List *clause_groups,
				  List *pathkeys,
				  ScanDirection indexscandir,
				  RelOptInfo *outer_rel);
extern BitmapHeapPath *create_bitmap_heap_path(PlannerInfo *root,
						RelOptInfo *rel,
						Path *bitmapqual,
						RelOptInfo *outer_rel);
extern BitmapTableScanPath *create_bitmap_table_scan_path(PlannerInfo *root,
						RelOptInfo *rel,
						Path *bitmapqual,
						RelOptInfo *outer_rel);
extern BitmapAndPath *create_bitmap_and_path(PlannerInfo *root,
					   RelOptInfo *rel,
					   List *bitmapquals);
extern BitmapOrPath *create_bitmap_or_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  List *bitmapquals);
extern TidPath *create_tidscan_path(PlannerInfo *root, RelOptInfo *rel,
					List *tidquals);
extern AppendPath *create_append_path(PlannerInfo *root, RelOptInfo *rel, List *subpaths);
extern ResultPath *create_result_path(RelOptInfo *rel, Path *subpath,
				   List *quals);
extern MaterialPath *create_material_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath);
UniquePath *create_unique_exprlist_path(PlannerInfo    *root,
                                        Path           *subpath,
                                        List           *distinct_on_exprs);
UniquePath *create_unique_rowid_path(PlannerInfo *root,
                                     Path        *subpath,
                                     Relids       dedup_relids);
extern Path *create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel, List *pathkeys);
extern Path *create_functionscan_path(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
extern Path *create_tablefunction_path(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
extern Path *create_valuesscan_path(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
extern Path *create_ctescan_path(PlannerInfo *root, RelOptInfo *rel, List *pathkeys);

extern NestPath *create_nestloop_path(PlannerInfo *root,
									  RelOptInfo *joinrel,
									  JoinType jointype,
									  Path *outer_path,
									  Path *inner_path,
									  List *restrict_clauses,
									  List *mergeclause_list,    /*CDB*/
									  List *pathkeys);

extern MergePath *create_mergejoin_path(PlannerInfo *root,
					  RelOptInfo *joinrel,
					  JoinType jointype,
					  Path *outer_path,
					  Path *inner_path,
					  List *restrict_clauses,
					  List *pathkeys,
					  List *mergeclauses,
                      List *allmergeclauses,    /*CDB*/
					  List *outersortkeys,
					  List *innersortkeys);

extern HashPath *create_hashjoin_path(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 JoinType jointype,
					 Path *outer_path,
					 Path *inner_path,
					 List *restrict_clauses,
                     List *mergeclause_list,    /*CDB*/
					 List *hashclauses,
                     bool  freeze_outer_path);

/*
 * prototypes for relnode.c
 */
extern RelOptInfo *build_simple_rel(PlannerInfo *root, int relid,
				 RelOptKind reloptkind);
extern RelOptInfo *find_base_rel(PlannerInfo *root, int relid);
extern RelOptInfo *find_join_rel(PlannerInfo *root, Relids relids);
extern RelOptInfo *build_join_rel(PlannerInfo *root,
			   Relids joinrelids,
			   RelOptInfo *outer_rel,
			   RelOptInfo *inner_rel,
			   JoinType jointype,
			   List **restrictlist_ptr);
void
build_joinrel_tlist(PlannerInfo *root, RelOptInfo *joinrel, List *input_tlist);

void
add_vars_to_targetlist(PlannerInfo *root, List *vars, Relids where_needed);

extern List *build_relation_tlist(RelOptInfo *rel);

Var *
cdb_define_pseudo_column(PlannerInfo   *root,
                         RelOptInfo    *rel,
                         const char    *colname,
                         Expr          *defexpr,
                         int32          width);

CdbRelColumnInfo *
cdb_find_pseudo_column(PlannerInfo *root, Var *var);

CdbRelColumnInfo *
cdb_rte_find_pseudo_column(RangeTblEntry *rte, AttrNumber attno);

CdbRelDedupInfo *
cdb_make_rel_dedup_info(PlannerInfo *root, RelOptInfo *rel);

#endif   /* PATHNODE_H */
