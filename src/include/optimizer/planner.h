/*-------------------------------------------------------------------------
 *
 * planner.h
 *	  prototypes for planner.c.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/optimizer/planner.h,v 1.35 2006/03/05 15:58:57 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANNER_H
#define PLANNER_H

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "executor/execdesc.h"

/* Hook for plugins to get control in planner() */
typedef PlannedStmt *(*planner_hook_type) (Query *parse,
									int cursorOptions,
									ParamListInfo boundParams,
									QueryResourceLife resourceLife);

extern PGDLLIMPORT planner_hook_type planner_hook;

extern ParamListInfo PlannerBoundParamList;	 /* current boundParams */

extern PlannedStmt *planner(Query *parse,
							int cursorOptions,
							ParamListInfo boundParams,
							QueryResourceLife resourceLife);

extern Plan *subquery_planner(PlannerGlobal *glob,
							  Query *parse,
							  PlannerInfo *parent_root,
							  double tuple_fraction,
							  PlannerInfo **subroot,
							  PlannerConfig *config);

extern bool choose_hashed_grouping(PlannerInfo *root, 
								   double tuple_fraction,
								   Path *cheapest_path, 
								   Path *sorted_path,
								   double dNumGroups, 
								   AggClauseCounts *agg_counts);

extern bool is_in_planning_phase(void);
extern void increase_planning_depth(void);
extern void decrease_planning_depth(void);

#endif   /* PLANNER_H */
