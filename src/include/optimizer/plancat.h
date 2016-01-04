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
 * plancat.h
 *	  prototypes for plancat.c.
 *
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/optimizer/plancat.h,v 1.42 2006/10/04 00:30:09 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANCAT_H
#define PLANCAT_H

#include "nodes/relation.h"
#include "utils/relcache.h"


extern void get_relation_info(PlannerInfo *root, Oid relationObjectId,
				  bool inhparent, RelOptInfo *rel);

extern void get_external_relation_info(Oid relationObjectId, RelOptInfo *rel);

extern void estimate_rel_size(Relation rel, int32 *attr_widths, BlockNumber *pages, double *tuples);

extern bool relation_excluded_by_constraints(PlannerInfo *root, RelOptInfo *rel,
								 RangeTblEntry *rte);

extern List *build_physical_tlist(PlannerInfo *root, RelOptInfo *rel);

extern List *find_inheritance_children(Oid inhparent);

extern bool has_unique_index(RelOptInfo *rel, AttrNumber attno);

extern Selectivity restriction_selectivity(PlannerInfo *root, Oid oper, List *args, int varRelid);

extern Selectivity join_selectivity(PlannerInfo *root, Oid op, List *args, JoinType jointype);

void
cdb_default_stats_warning_for_table(Oid reloid);

#endif   /* PLANCAT_H */
