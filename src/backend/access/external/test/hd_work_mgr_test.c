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

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "hd_work_mgr_mock.c"
#include "hd_work_mgr_do_segment_clustering_by_host_test.c"
#include "hd_work_mgr_allocate_fragments_to_datanodes_test.c"
#include "hd_work_mgr_distribute_work_2_gp_segments_test.c"

int 
main(int argc, char* argv[]) 
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__do_segment_clustering_by_host__10SegmentsOn3Hosts),
			unit_test(test__get_dn_processing_load),
			unit_test(test__create_allocated_fragment__NoUserData),
			unit_test(test__create_allocated_fragment__WithUserData),
			unit_test(test__allocate_fragments_to_datanodes__4Fragments10Hosts3Replicates),
			unit_test(test__allocate_fragments_to_datanodes__4Fragments3Hosts2Replicates),
			unit_test(test__allocate_fragments_to_datanodes__4Fragments3Hosts1Replicates),
			unit_test(test__allocate_fragments_to_datanodes__7Fragments10Hosts1Replicates),
			unit_test(test__distribute_work_to_gp_segments__big_cluster_few_active_nodes),	
			unit_test(test__distribute_work_to_gp_segments__big_cluster_many_active_nodes),
			unit_test(test__distribute_work_to_gp_segments__small_cluster),
			unit_test(test__distribute_work_to_gp_segments__small_cluster_many_active_nodes),
			unit_test(test__distribute_work_to_gp_segments__small_cluster_few_replicas)
	};
	return run_tests(tests);
}
