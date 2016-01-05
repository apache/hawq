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
 * var.h
 *	  prototypes for optimizer/util/var.c.
 *
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/optimizer/var.h,v 1.34 2006/03/05 15:58:57 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef VAR_H
#define VAR_H

#include "nodes/relation.h"


typedef bool (*Cdb_walk_vars_callback_Aggref)(Aggref *aggref, void *context, int sublevelsup);
typedef bool (*Cdb_walk_vars_callback_Var)(Var *var, void *context, int sublevelsup);
typedef bool (*Cdb_walk_vars_callback_CurrentOf)(CurrentOfExpr *expr, void *context, int sublevelsup);
bool        cdb_walk_vars(Node                         *node,
                          Cdb_walk_vars_callback_Var    callback_var,
                          Cdb_walk_vars_callback_Aggref callback_aggref,
                          Cdb_walk_vars_callback_CurrentOf callback_currentof,
                          void                         *context,
                          int                           levelsup);

extern Relids pull_varnos(Node *node);

extern bool contain_ctid_var_reference(Scan *scan);
extern bool contain_var_clause(Node *node);
extern bool contain_vars_of_level(Node *node, int levelsup);
extern bool contain_vars_of_level_or_above(Node *node, int levelsup);
extern int	find_minimum_var_level(Node *node);
extern List *pull_var_clause(Node *node, bool includeUpperVars);

extern Node *flatten_join_alias_vars(PlannerInfo *root, Node *node);

#endif   /* VAR_H */
