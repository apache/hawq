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

/*
 * Serialize output string when all fields of AllocatedDataFragment are passed
 */
void
test__make_allocation_output_string(void **state)
{

	List *segment_fragments = NIL;
	AllocatedDataFragment* frag = palloc0(sizeof(AllocatedDataFragment));

	frag->index = 42;
	frag->host = "HOST";
	frag->rest_port = 1312;
	frag->source_name = "TABLE_NAME";
	frag->fragment_md = "FRAGMENT_METADATA";
	frag->user_data = "USER_DATA";
	frag->profile = "PROFILE";

	segment_fragments = lappend(segment_fragments, frag);
	char *output_str = make_allocation_output_string(segment_fragments);

	assert_string_equal(output_str, "segwork=60@HOST@1312@TABLE_NAME@42@FRAGMENT_METADATA@USER_DATA@PROFILE@");

	pfree(frag);
	pfree(output_str);
	list_free(segment_fragments);
}

/*
 * Serialize output string when profile is empty
 */
void
test__make_allocation_output_string__empty_profile(void **state)
{

	List *segment_fragments = NIL;
	AllocatedDataFragment* frag = palloc0(sizeof(AllocatedDataFragment));

	frag->index = 42;
	frag->host = "HOST";
	frag->rest_port = 1312;
	frag->source_name = "TABLE_NAME";
	frag->fragment_md = "FRAGMENT_METADATA";
	frag->user_data = "USER_DATA";

	segment_fragments = lappend(segment_fragments, frag);
	char *output_str = make_allocation_output_string(segment_fragments);

	assert_string_equal(output_str, "segwork=53@HOST@1312@TABLE_NAME@42@FRAGMENT_METADATA@USER_DATA@@");

	pfree(frag);
	pfree(output_str);
	list_free(segment_fragments);
}

/*
 * Serialize output string when user data is empty
 */
void
test__make_allocation_output_string__empty_user_data(void **state)
{

	List *segment_fragments = NIL;
	AllocatedDataFragment* frag = palloc0(sizeof(AllocatedDataFragment));

	frag->index = 42;
	frag->host = "HOST";
	frag->rest_port = 1312;
	frag->source_name = "TABLE_NAME";
	frag->fragment_md = "FRAGMENT_METADATA";
	frag->profile = "PROFILE";

	segment_fragments = lappend(segment_fragments, frag);
	char *output_str = make_allocation_output_string(segment_fragments);

	assert_string_equal(output_str, "segwork=51@HOST@1312@TABLE_NAME@42@FRAGMENT_METADATA@@PROFILE@");

	pfree(frag);
	pfree(output_str);
	list_free(segment_fragments);
}

/*
 * Serialize output string when profile and user data are empty
 */
void
test__make_allocation_output_string__empty_user_data_profile(void **state)
{

	List *segment_fragments = NIL;
	AllocatedDataFragment* frag = palloc0(sizeof(AllocatedDataFragment));

	frag->index = 42;
	frag->host = "HOST";
	frag->rest_port = 1312;
	frag->source_name = "TABLE_NAME";
	frag->fragment_md = "FRAGMENT_METADATA";

	segment_fragments = lappend(segment_fragments, frag);
	char *output_str = make_allocation_output_string(segment_fragments);

	assert_string_equal(output_str, "segwork=44@HOST@1312@TABLE_NAME@42@FRAGMENT_METADATA@@@");

	pfree(frag);
	pfree(output_str);
	list_free(segment_fragments);
}

int 
main(int argc, char* argv[]) 
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__make_allocation_output_string),
			unit_test(test__make_allocation_output_string__empty_profile),
			unit_test(test__make_allocation_output_string__empty_user_data),
			unit_test(test__make_allocation_output_string__empty_user_data_profile),
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
