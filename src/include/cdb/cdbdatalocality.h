/*-------------------------------------------------------------------------
 *
 * cdbdatalocality.h
 *	  Manages data locality.
 *
 * Copyright (c) 2014, Pivotal Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDATALOCALITY_H
#define CDBDATALOCALITY_H

#include "postgres.h"

#include "catalog/gp_policy.h"
#include "nodes/parsenodes.h"
#include "executor/execdesc.h"

#define minimum_segment_num 1
/*
 * structure containing information about data residence
 * at the host.
 */
typedef struct SplitAllocResult
{
  QueryResource *resource;
  List *alloc_results;
  int planner_segments;
  List *relsType;// relation type after datalocality changing
  StringInfo datalocalityInfo;
} SplitAllocResult;

/*
 * structure containing rel and type when execution
 */
typedef struct CurrentRelType {
	Oid relid;
	bool isHash;
} CurrentRelType;

/*
 * structure for virtual segment.
 */
typedef struct VirtualSegmentNode
{
	NodeTag type;
	char *hostname;
} VirtualSegmentNode;

/*
 * calculate_planner_segment_num: based on the parse tree,
 * we calculate the appropriate planner segment_num.
 */
SplitAllocResult * calculate_planner_segment_num(Query *query, QueryResourceLife resourceLife,
                                                List *rtable, GpPolicy *intoPolicy, int sliceNum);

FILE *fp;
FILE *fpaoseg;
FILE *fpsegnum;
FILE *fpratio;
#endif /* CDBDATALOCALITY_H */
