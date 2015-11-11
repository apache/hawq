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
 *  cdbsetop.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBSETOP_H
#define CDBSETOP_H

#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"

/*
 * GpSetOpType represents a strategy by which to construct a parallel
 * execution plan for a set operation.
 *
 * PSETOP_PARALLEL_PARTITIONED
 *    The plans input to the Append node are (or are coerced to) partitioned 
 *    loci (hashed, scattered, or single QE).  The result of the Append is
 *    assumed to be scattered and unordered is redistributed (if necessary) 
 *    to suit the particular set operation. 
 *
 * PSETOP_SEQUENTIAL_QD
 *    The plans input to the Append node are (or are coerced to) root loci. 
 *    The result of the Append is, therefore, root and unordered.  The set
 *    operation is performed on the QD as if it were sequential.
 *
 * PSETOP_SEQUENTIAL_QE
 *    The plans input to the Append node are (or are coerced to) single QE
 *    loci.  The result of the Append is, therefore, single QE and assumed
 *    unordered.  The set operation is performed on the QE as if it were 
 *    sequential.
 *
 * PSETOP_PARALLEL_REPLICATED
 *    The plans input to the Append node are replicated loci.  The result of 
 *    the Append is, therefore, replicated.  The set operation is performed 
 *    in parallel (and redundantly) on the QEs as if it were sequential.
 *
 * PSETOP_GENERAL
 *    The plans input to the Append node are all general loci.  The result
 *    of the Append is, therefore general as well.
 */
typedef enum GpSetOpType
{
	PSETOP_NONE = 0,
	PSETOP_PARALLEL_PARTITIONED,
	PSETOP_SEQUENTIAL_QD,
	PSETOP_SEQUENTIAL_QE,
	PSETOP_PARALLEL_REPLICATED,
	PSETOP_GENERAL
} GpSetOpType;

extern 
GpSetOpType choose_setop_type(List *planlist); 

extern
void adjust_setop_arguments(List *planlist, GpSetOpType setop_type);


extern
Motion* make_motion_hash_all_targets(PlannerInfo *root, Plan *subplan);

extern
Motion* make_motion_hash(PlannerInfo *root, Plan *subplan, List *hashexprs);

extern
Motion* make_motion_multiplex(PlannerInfo *root, Plan *subplan);

extern
Motion* make_motion_gather_to_QD(Plan *subplan, bool keep_ordering);

extern
Motion* make_motion_gather_to_QE(Plan *subplan, bool keep_ordering);

extern
Motion* make_motion_cull_to_QE(Plan *subplan);

extern
Motion* make_motion_gather(Plan *subplan, int segindex, bool keep_ordering);

extern
void mark_append_locus(Plan *plan, GpSetOpType optype);

extern
void mark_passthru_locus(Plan *plan, bool with_hash, bool with_sort);

extern
void mark_sort_locus(Plan *plan);

extern
void mark_plan_general(Plan* plan);

extern
void mark_plan_strewn(Plan* plan);

extern
void mark_plan_replicated(Plan* plan);

extern
void mark_plan_entry(Plan* plan);

extern
void mark_plan_singleQE(Plan* plan);

#endif   /* CDBSETOP_H */
