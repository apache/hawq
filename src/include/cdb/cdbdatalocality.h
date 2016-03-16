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
 * cdbdatalocality.h
 *	  Manages data locality.
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
  QueryResourceParameters *resource_parameters;
  List *alloc_results;
  int planner_segments;
  List *relsType;// relation type after datalocality changing
  StringInfo datalocalityInfo;
} SplitAllocResult;

/*
 * structure containing all relation range table entries.
 */
typedef struct udf_collector_context {
	bool udf_exist;
} udf_collector_context;

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
 * saveQueryResourceParameters: save QueryResourceParameters
 * in prepare statement along with query plan so that the query
 * resource can be re-allocated during multiple executions of
 * the plan
 */
void saveQueryResourceParameters(
		QueryResourceParameters	*resource_parameters,
		QueryResourceLife       life,
		int32                   slice_size,
		int64_t                 iobytes,
		int                     max_target_segment_num,
		int                     min_target_segment_num,
		HostnameVolumeInfo      *vol_info,
		int                     vol_info_size);

/*
 * calculate_planner_segment_num: based on the parse tree,
 * we calculate the appropriate planner segment_num.
 */
SplitAllocResult * calculate_planner_segment_num(Query *query, QueryResourceLife resourceLife,
                                                List *rtable, GpPolicy *intoPolicy, int sliceNum, int fixedVsegNum);

/*
 * udf_collector_walker: the routine to file udfs.
 */
bool udf_collector_walker(Node *node,	udf_collector_context *context);

/*
 * find_udf: collect all udf, and store them into the udf_collector_context.
 */
void find_udf(Query *query, udf_collector_context *context);

FILE *fp;
FILE *fpaoseg;
FILE *fpsegnum;
FILE *fpratio;
#endif /* CDBDATALOCALITY_H */
