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
 * tlist.h
 *	  prototypes for tlist.c.
 *
 *
 * Portions Copyright (c) 2007-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/optimizer/tlist.h,v 1.44 2006/03/05 15:58:57 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef TLIST_H
#define TLIST_H

#include "nodes/relation.h"

// return the first target entries that match the node expression
extern TargetEntry *tlist_member(Node *node, List *targetlist);

// return a list a target entries that match the node expression
extern List *tlist_members(Node *node, List *targetlist);

extern TargetEntry *tlist_member_ignoring_RelabelType(Expr *expr, List *targetlist);

extern List *flatten_tlist(List *tlist);
extern List *add_to_flat_tlist(List *tlist, List *vars, bool resjunk);

extern TargetEntry *get_sortgroupclause_tle(SortClause *sortClause,
						List *targetList);
extern Node *get_sortgroupclause_expr(SortClause *sortClause,
						 List *targetList);
extern List *get_sortgrouplist_exprs(List *sortClauses,
						List *targetList);
extern AttrNumber *get_grouplist_colidx(List *sortClauses,
						List *targetList, int *numCols);

extern List *get_grouplist_exprs(List *groupClause, List *targetList);
extern List *get_sortgroupclauses_tles(List *clauses, List *targetList);

extern bool tlist_same_datatypes(List *tlist, List *colTypes, bool junkOK);

extern Index maxSortGroupRef(List *targetlist, bool include_orderedagg);

extern int get_row_width(List *tlist);

/* check that two target lists are aligned */
extern void insist_target_lists_aligned(List *tlistFst, List *tlistSnd);

#endif   /* TLIST_H */
