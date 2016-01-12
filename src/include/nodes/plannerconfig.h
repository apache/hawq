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
/*
 * plannerconfig.h
 *
 *  Created on: May 19, 2011
 *      Author: siva
 */

#ifndef PLANNERCONFIG_H_
#define PLANNERCONFIG_H_

/**
 * Planning configuration information
 */
typedef struct PlannerConfig
{
	bool		enable_seqscan;
	bool		enable_indexscan;
	bool		enable_bitmapscan;
	bool		enable_tidscan;
	bool		enable_sort;
	bool		enable_hashagg;
	bool		enable_groupagg;
	bool		enable_nestloop;
	bool		enable_mergejoin;
	bool		enable_hashjoin;
	bool        gp_enable_hashjoin_size_heuristic;
	bool		gp_enable_fallback_plan;
	bool        gp_enable_predicate_propagation;
	bool		mpp_trying_fallback_plan;
	int			cdbpath_segments;
	bool		constraint_exclusion;

	bool		gp_enable_multiphase_agg;
	bool		gp_enable_preunique;
	bool		gp_eager_preunique;
	bool		gp_enable_sequential_window_plans;
	bool 		gp_hashagg_streambottom;
	bool		gp_enable_agg_distinct;
	bool		gp_enable_dqa_pruning;
	bool		gp_eager_dqa_pruning;
	bool		gp_eager_one_phase_agg;
	bool		gp_eager_two_phase_agg;
	bool        gp_enable_groupext_distinct_pruning;
	bool        gp_enable_groupext_distinct_gather;
	bool		gp_enable_sort_limit;
	bool		gp_enable_sort_distinct;
	bool		gp_enable_mk_sort;
	bool		gp_enable_motion_mk_sort;

	bool		gp_enable_direct_dispatch;
	bool		gp_dynamic_partition_pruning;

	bool		gp_cte_sharing; /* Indicate whether sharing is to be disabled on any CTEs */

	/* These ones are tricky */
	//GpRoleValue	Gp_role; // TODO: this one is tricky
	//int			gp_singleton_segindex; // TODO: change this.
} PlannerConfig;

extern PlannerConfig *DefaultPlannerConfig(void);
extern PlannerConfig *CopyPlannerConfig(const PlannerConfig *c1);

#endif /* PLANNERCONFIG_H_ */
