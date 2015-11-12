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
#include "hd_work_mgr_mock.h"


/*
 * check that datanode dn has the expected
 * ip, port, dn blocks list, and number of read & residing fragments
 */
void check_dn_load(DatanodeProcessingLoad* dn,
				   const char* expected_hostip,
				   int expected_port,
				   List* expected_blocks,
				   int expected_read_fragments,
				   int expected_residing_fragments)
{

	ListCell* cell = NULL;
	int blocks_number = 0;

	assert_string_equal(dn->dataNodeIp, expected_hostip);
	assert_int_equal(dn->port, expected_port);
	assert_int_equal(dn->num_fragments_read, expected_read_fragments);
	assert_int_equal(dn->num_fragments_residing, expected_residing_fragments);

	assert_int_equal(list_length(expected_blocks), list_length(dn->datanodeBlocks));
	blocks_number = list_length(expected_blocks);
	for (int i = 0; i < blocks_number; ++i)
	{
		AllocatedDataFragment* block =
				(AllocatedDataFragment*)lfirst(list_nth_cell(dn->datanodeBlocks, i));
		AllocatedDataFragment* expected_block =
				(AllocatedDataFragment*)lfirst(list_nth_cell(expected_blocks, i));
		assert_string_equal(block->host, expected_block->host);
		assert_int_equal(block->rest_port, expected_block->rest_port);
		assert_string_equal(block->source_name, expected_block->source_name);
		assert_string_equal(block->fragment_md, expected_block->fragment_md);
		assert_string_equal(block->user_data, expected_block->user_data);
		assert_int_equal(block->index, expected_block->index);
	}

}

/*
 * Test get_dn_processing_load:
 * starting with an empty list, add fragment location
 * and check that the returned element equals the given fragment
 * and that if the location is new a new element was added to the list.
 */
void 
test__get_dn_processing_load(void **state)
{
	List* allDNProcessingLoads = NIL;
	ListCell* cell = NULL;
	FragmentHost *fragment_loc = NULL;
	DatanodeProcessingLoad* dn_found = NULL;

	char* ip_array[] =
	{
		"1.2.3.1",
		"1.2.3.2",
		"1.2.3.3",
		"1.2.3.1",
		"1.2.3.1",
		"1.2.3.3",
		"1.2.3.1"
	};
	int port_array[] =
	{
		100,
		100,
		100,
		100,
		100,
		100,
		200
	};
	int expected_list_size[] =
	{
		1, 2, 3, 3, 3, 3, 4
	};
	int array_size = 7;
	/* sanity */
	assert_int_equal(array_size, (sizeof(ip_array) / sizeof(ip_array[0])));
	assert_int_equal(array_size, (sizeof(port_array) / sizeof(port_array[0])));
	assert_int_equal(array_size, (sizeof(expected_list_size) / sizeof(expected_list_size[0])));

	fragment_loc = (FragmentHost*) palloc0(sizeof(FragmentHost));

	for (int i = 0; i < array_size; ++i)
	{
		dn_found = NULL;
		fragment_loc->ip = ip_array[i];
		fragment_loc->rest_port = port_array[i];

		dn_found = get_dn_processing_load(&allDNProcessingLoads, fragment_loc);

		assert_true(dn_found != NULL);
		assert_int_equal(list_length(allDNProcessingLoads), expected_list_size[i]);

		check_dn_load(dn_found, ip_array[i], port_array[i], NIL, 0, 0);
	}

	pfree(fragment_loc);
	/* free allDNProcessingLoads */
	foreach(cell, allDNProcessingLoads)
	{
		DatanodeProcessingLoad* cell_data = (DatanodeProcessingLoad*)lfirst(cell);
		pfree(cell_data->dataNodeIp);
	}
	list_free(allDNProcessingLoads);
}

/*
 * create, verify and free AllocatedDataFragment
 */
void check_create_allocated_fragment(DataFragment* fragment, bool has_user_data)
{
	AllocatedDataFragment* allocated = create_allocated_fragment(fragment);

	assert_true(allocated != NULL);
	assert_int_equal(allocated->index, fragment->index);
	assert_string_equal(allocated->source_name, fragment->source_name);
	assert_string_equal(allocated->fragment_md, fragment->fragment_md);
	if (has_user_data)
		assert_string_equal(allocated->user_data, fragment->user_data);
	else
		assert_true(allocated->user_data == NULL);
	assert_true(allocated->host == NULL);
	assert_int_equal(allocated->rest_port, 0);

	if (allocated->source_name)
		pfree(allocated->source_name);
	if (allocated->user_data)
		pfree(allocated->user_data);
	pfree(allocated);
}


/*
 * Test create_allocated_fragment without user_data.
 */
void
test__create_allocated_fragment__NoUserData(void **state)
{
	AllocatedDataFragment* allocated = NULL;

	DataFragment* fragment = (DataFragment*) palloc0(sizeof(DataFragment));
	fragment->index = 13;
	fragment->source_name = "source name!!!";
	fragment->user_data = NULL;
	fragment->fragment_md = "METADATA";

	check_create_allocated_fragment(fragment, false);

	pfree(fragment);
}

/*
 * Test create_allocated_fragment without user_data.
 */
void
test__create_allocated_fragment__WithUserData(void **state)
{
	AllocatedDataFragment* allocated = NULL;

	DataFragment* fragment = (DataFragment*) palloc0(sizeof(DataFragment));
	fragment->index = 13;
	fragment->source_name = "source name!!!";
	fragment->user_data = "Wish You Were Here";
	fragment->fragment_md = "METADATA";

	check_create_allocated_fragment(fragment, true);

	pfree(fragment);
}

/*
 * Creates a list of DataFragment for one file ("file.txt").
 * The important thing here is the fragments' location. It is determined by the parameters:
 * replication_factor - number of copies of each fragment on the different hosts.
 * number_of_hosts - number of hosts, so that the IP pool we use is 1.2.3.{1-number_of_hosts}
 * number_of_fragments - number of fragments in the file.
 *
 * Each fragment will have <replication_factor> hosts,
 * starting from IP 1.2.3.<fragment_number> to IP 1.2.3.<fragment_number + replication_factor> modulo <number_of_hosts>.
 * That way there is some overlapping between the hosts of each fragment.
 */
List* build_data_fragments_list(int number_of_fragments, int number_of_hosts, int replication_factor)
{
	List* fragments_list = NIL;
	StringInfoData string_info;
	initStringInfo(&string_info);

	for (int i = 0; i < number_of_fragments; ++i)
	{
		DataFragment* fragment = (DataFragment*) palloc0(sizeof(DataFragment));

		fragment->index = i;
		fragment->source_name = pstrdup("file.txt");

		for (int j = 0; j < replication_factor; ++j)
		{
			FragmentHost* fhost = (FragmentHost*)palloc0(sizeof(FragmentHost));
			appendStringInfo(&string_info, "1.2.3.%d", ((j + i) % number_of_hosts) + 1);
			fhost->ip = pstrdup(string_info.data);
			resetStringInfo(&string_info);
			fragment->replicas = lappend(fragment->replicas, fhost);
		}
		assert_int_equal(list_length(fragment->replicas), replication_factor);
		appendStringInfo(&string_info, "metadata %d", i);
		fragment->fragment_md = pstrdup(string_info.data);
		resetStringInfo(&string_info);
		appendStringInfo(&string_info, "user data %d", i);
		fragment->user_data = pstrdup(string_info.data);
		resetStringInfo(&string_info);
		fragments_list = lappend(fragments_list, fragment);
	}

	pfree(string_info.data);
	return fragments_list;
}

/*
 * Returns a list of AllocatedDataFragment.
 */
List* build_blank_blocks_list(int list_size)
{
	List* blocks_list = NIL;
	for (int i = 0; i < list_size; ++i)
	{
		AllocatedDataFragment* block = (AllocatedDataFragment*) palloc0(sizeof(AllocatedDataFragment));
		blocks_list = lappend(blocks_list, block);
	}
	return blocks_list;
}

/*
 * Tests allocate_fragments_to_datanodes with 4 fragments over 10 hosts, with 3 replicates each,
 * so that they are distributed as follows:
 * Fragment 0 : on hosts 1.2.3.1, 1.2.3.2, 1.2.3.3.
 * Fragment 1 : on hosts 1.2.3.2, 1.2.3.3, 1.2.3.4.
 * Fragment 2 : on hosts 1.2.3.3, 1.2.3.4, 1.2.3.5.
 * Fragment 3 : on hosts 1.2.3.4, 1.2.3.5, 1.2.3.6.
 *
 * The expected distribution is as follows:
 * Host 1.2.3.1: fragment 0.
 * Host 1.2.3.2: fragment 1.
 * Host 1.2.3.3: fragment 2.
 * Host 1.2.3.4: fragment 3.
 * Hosts 1.2.3.5, 1.2.3.6: no fragments.
 */
void
test__allocate_fragments_to_datanodes__4Fragments10Hosts3Replicates(void **state)
{
	List* allDNProcessingLoads = NIL;
	List* data_fragment_list = NIL;
	List* blocks_list = NIL;
	ListCell* cell = NULL;
	DatanodeProcessingLoad* datanode_load = NULL;
	AllocatedDataFragment* allocated = NULL;

	data_fragment_list = build_data_fragments_list(4, 10, 3);
	assert_int_equal(4, list_length(data_fragment_list));

	allDNProcessingLoads = allocate_fragments_to_datanodes(data_fragment_list);

	assert_true(allDNProcessingLoads != NULL);
	assert_int_equal(6, list_length(allDNProcessingLoads));

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 0));
	blocks_list = build_blank_blocks_list(1);
	allocated = lfirst(list_nth_cell(blocks_list, 0));
	allocated->host = "1.2.3.1";
	allocated->index = 0;
	allocated->rest_port = 0;
	allocated->source_name = "file.txt";
	allocated->fragment_md = "metadata 0";
	allocated->user_data = "user data 0";
	check_dn_load(datanode_load,
			      "1.2.3.1", 0, blocks_list, 1, 1);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 1));
	allocated->host = "1.2.3.2";
	allocated->index = 1;
	allocated->fragment_md = "metadata 1";
	allocated->user_data = "user data 1";
	check_dn_load(datanode_load,
				  "1.2.3.2", 0, blocks_list, 1, 2);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 2));
	allocated->host = "1.2.3.3";
	allocated->index = 2;
	allocated->fragment_md = "metadata 2";
	allocated->user_data = "user data 2";
	check_dn_load(datanode_load,
				  "1.2.3.3", 0, blocks_list, 1, 3);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 3));
	allocated->host = "1.2.3.4";
	allocated->index = 3;
	allocated->fragment_md = "metadata 3";
	allocated->user_data = "user data 3";
	check_dn_load(datanode_load,
				  "1.2.3.4", 0, blocks_list, 1, 3);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 4));
	check_dn_load(datanode_load,
				  "1.2.3.5", 0, NIL, 0, 2);
	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 5));
	check_dn_load(datanode_load,
				  "1.2.3.6", 0, NIL, 0, 1);

	/* cleanup */
	list_free(blocks_list);
	foreach (cell, allDNProcessingLoads)
	{
		DatanodeProcessingLoad* dn = (DatanodeProcessingLoad*)lfirst(cell);
		free_allocated_frags(dn->datanodeBlocks);
	}
	list_free(allDNProcessingLoads);
}

/*
 * Tests allocate_fragments_to_datanodes with 4 fragments over 3 hosts, with 2 replicates each,
 * so that they are distributed as follows:
 * Fragment 0 : on hosts 1.2.3.1, 1.2.3.2.
 * Fragment 1 : on hosts 1.2.3.2, 1.2.3.3.
 * Fragment 2 : on hosts 1.2.3.3, 1.2.3.1.
 * Fragment 3 : on hosts 1.2.3.1, 1.2.3.2.
 *
 * The expected distribution is as follows:
 * Host 1.2.3.1: fragments 0, 3.
 * Host 1.2.3.2: fragment 1.
 * Host 1.2.3.3: fragment 2.
 */
void
test__allocate_fragments_to_datanodes__4Fragments3Hosts2Replicates(void **state)
{
	List* allDNProcessingLoads = NIL;
	List* data_fragment_list = NIL;
	List* blocks_list = NIL;
	ListCell* cell = NULL;
	DatanodeProcessingLoad* datanode_load = NULL;
	AllocatedDataFragment* allocated = NULL;

	data_fragment_list = build_data_fragments_list(4, 3, 2);
	assert_int_equal(4, list_length(data_fragment_list));

	allDNProcessingLoads = allocate_fragments_to_datanodes(data_fragment_list);

	assert_true(allDNProcessingLoads != NULL);
	assert_int_equal(3, list_length(allDNProcessingLoads));

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 0));
	blocks_list = build_blank_blocks_list(2);
	allocated = lfirst(list_nth_cell(blocks_list, 0));
	allocated->host = "1.2.3.1";
	allocated->index = 0;
	allocated->rest_port = 0;
	allocated->source_name = "file.txt";
	allocated->fragment_md = "metadata 0";
	allocated->user_data = "user data 0";
	allocated = lfirst(list_nth_cell(blocks_list, 1));
	allocated->host = "1.2.3.1";
	allocated->index = 3;
	allocated->rest_port = 0;
	allocated->source_name = "file.txt";
	allocated->fragment_md = "metadata 3";
	allocated->user_data = "user data 3";
	check_dn_load(datanode_load,
			      "1.2.3.1", 0, blocks_list, 2, 3);
	list_free(blocks_list);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 1));
	blocks_list = build_blank_blocks_list(1);
	allocated = lfirst(list_nth_cell(blocks_list, 0));
	allocated->host = "1.2.3.2";
	allocated->index = 1;
	allocated->rest_port = 0;
	allocated->source_name = "file.txt";
	allocated->fragment_md = "metadata 1";
	allocated->user_data = "user data 1";
	check_dn_load(datanode_load,
				  "1.2.3.2", 0, blocks_list, 1, 3);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 2));
	allocated->host = "1.2.3.3";
	allocated->index = 2;
	allocated->fragment_md = "metadata 2";
	allocated->user_data = "user data 2";
	check_dn_load(datanode_load,
				  "1.2.3.3", 0, blocks_list, 1, 2);

	/* cleanup */
	list_free(blocks_list);
	foreach (cell, allDNProcessingLoads)
	{
		DatanodeProcessingLoad* dn = (DatanodeProcessingLoad*)lfirst(cell);
		free_allocated_frags(dn->datanodeBlocks);
	}
	list_free(allDNProcessingLoads);
}

/*
 * Tests allocate_fragments_to_datanodes with 4 fragments over 3 hosts, with 1 replicates each,
 * so that they are distributed as follows:
 * Fragment 0 : on hosts 1.2.3.1.
 * Fragment 1 : on hosts 1.2.3.2.
 * Fragment 2 : on hosts 1.2.3.3.
 * Fragment 3 : on hosts 1.2.3.1.
 *
 * The expected distribution is as follows:
 * Host 1.2.3.1: fragments 0, 3.
 * Host 1.2.3.2: fragment 1.
 * Host 1.2.3.3: fragment 2.
 */
void
test__allocate_fragments_to_datanodes__4Fragments3Hosts1Replicates(void **state)
{
	List* allDNProcessingLoads = NIL;
	List* data_fragment_list = NIL;
	List* blocks_list = NIL;
	ListCell* cell = NULL;
	DatanodeProcessingLoad* datanode_load = NULL;
	AllocatedDataFragment* allocated = NULL;

	data_fragment_list = build_data_fragments_list(4, 3, 1);
	assert_int_equal(4, list_length(data_fragment_list));

	allDNProcessingLoads = allocate_fragments_to_datanodes(data_fragment_list);

	assert_true(allDNProcessingLoads != NULL);
	assert_int_equal(3, list_length(allDNProcessingLoads));

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 0));
	blocks_list = build_blank_blocks_list(2);
	allocated = lfirst(list_nth_cell(blocks_list, 0));
	allocated->host = "1.2.3.1";
	allocated->index = 0;
	allocated->rest_port = 0;
	allocated->source_name = "file.txt";
	allocated->fragment_md = "metadata 0";
	allocated->user_data = "user data 0";
	allocated = lfirst(list_nth_cell(blocks_list, 1));
	allocated->host = "1.2.3.1";
	allocated->index = 3;
	allocated->rest_port = 0;
	allocated->source_name = "file.txt";
	allocated->fragment_md = "metadata 3";
	allocated->user_data = "user data 3";
	check_dn_load(datanode_load,
			      "1.2.3.1", 0, blocks_list, 2, 2);
	list_free(blocks_list);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 1));
	blocks_list = build_blank_blocks_list(1);
	allocated = lfirst(list_nth_cell(blocks_list, 0));
	allocated->host = "1.2.3.2";
	allocated->index = 1;
	allocated->rest_port = 0;
	allocated->source_name = "file.txt";
	allocated->fragment_md = "metadata 1";
	allocated->user_data = "user data 1";
	check_dn_load(datanode_load,
				  "1.2.3.2", 0, blocks_list, 1, 1);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 2));
	allocated->host = "1.2.3.3";
	allocated->index = 2;
	allocated->fragment_md = "metadata 2";
	allocated->user_data = "user data 2";
	check_dn_load(datanode_load,
				  "1.2.3.3", 0, blocks_list, 1, 1);

	/* cleanup */
	list_free(blocks_list);
	foreach (cell, allDNProcessingLoads)
	{
		DatanodeProcessingLoad* dn = (DatanodeProcessingLoad*)lfirst(cell);
		free_allocated_frags(dn->datanodeBlocks);
	}
	list_free(allDNProcessingLoads);
}

/*
 * Tests allocate_fragments_to_datanodes with 7 fragments over 10 hosts, with 1 replicates each,
 * so that they are distributed as follows:
 * Fragment 0 : on hosts 1.2.3.1.
 * Fragment 1 : on hosts 1.2.3.2.
 * Fragment 2 : on hosts 1.2.3.3.
 * Fragment 3 : on hosts 1.2.3.4.
 * Fragment 4 : on hosts 1.2.3.5.
 * Fragment 5 : on hosts 1.2.3.6.
 * Fragment 6 : on hosts 1.2.3.7.
 *
 * The expected distribution is as follows:
 * Host 1.2.3.1: fragment 0.
 * Host 1.2.3.2: fragment 1.
 * Host 1.2.3.3: fragment 2.
 * Host 1.2.3.4: fragment 3.
 * Host 1.2.3.5: fragment 4.
 * Host 1.2.3.6: fragment 5.
 * Host 1.2.3.7: fragment 6.
 * Hosts 1.2.3.8, 1.2.3.9, 1.2.3.10: no fragments.
 */
void
test__allocate_fragments_to_datanodes__7Fragments10Hosts1Replicates(void **state)
{
	List* allDNProcessingLoads = NIL;
	List* data_fragment_list = NIL;
	List* blocks_list = NIL;
	ListCell* cell = NULL;
	DatanodeProcessingLoad* datanode_load = NULL;
	AllocatedDataFragment* allocated = NULL;

	data_fragment_list = build_data_fragments_list(7, 10, 1);
	assert_int_equal(7, list_length(data_fragment_list));

	allDNProcessingLoads = allocate_fragments_to_datanodes(data_fragment_list);

	assert_true(allDNProcessingLoads != NULL);
	assert_int_equal(7, list_length(allDNProcessingLoads));

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 0));
	blocks_list = build_blank_blocks_list(1);
	allocated = lfirst(list_nth_cell(blocks_list, 0));
	allocated->host = "1.2.3.1";
	allocated->index = 0;
	allocated->rest_port = 0;
	allocated->source_name = "file.txt";
	allocated->fragment_md = "metadata 0";
	allocated->user_data = "user data 0";
	check_dn_load(datanode_load,
			      "1.2.3.1", 0, blocks_list, 1, 1);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 1));
	allocated->host = "1.2.3.2";
	allocated->index = 1;
	allocated->fragment_md = "metadata 1";
	allocated->user_data = "user data 1";
	check_dn_load(datanode_load,
				  "1.2.3.2", 0, blocks_list, 1, 1);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 2));
	allocated->host = "1.2.3.3";
	allocated->index = 2;
	allocated->fragment_md = "metadata 2";
	allocated->user_data = "user data 2";
	check_dn_load(datanode_load,
			"1.2.3.3", 0, blocks_list, 1, 1);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 3));
	allocated->host = "1.2.3.4";
	allocated->index = 3;
	allocated->fragment_md = "metadata 3";
	allocated->user_data = "user data 3";
	check_dn_load(datanode_load,
			"1.2.3.4", 0, blocks_list, 1, 1);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 4));
	allocated->host = "1.2.3.5";
	allocated->index = 4;
	allocated->fragment_md = "metadata 4";
	allocated->user_data = "user data 4";
	check_dn_load(datanode_load,
			"1.2.3.5", 0, blocks_list, 1, 1);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 5));
	allocated->host = "1.2.3.6";
	allocated->index = 5;
	allocated->fragment_md = "metadata 5";
	allocated->user_data = "user data 5";
	check_dn_load(datanode_load,
			"1.2.3.6", 0, blocks_list, 1, 1);

	datanode_load = (DatanodeProcessingLoad*)lfirst(list_nth_cell(allDNProcessingLoads, 6));
	allocated->host = "1.2.3.7";
	allocated->index = 6;
	allocated->fragment_md = "metadata 6";
	allocated->user_data = "user data 6";
	check_dn_load(datanode_load,
			"1.2.3.7", 0, blocks_list, 1, 1);

	/* cleanup */
	list_free(blocks_list);
	foreach (cell, allDNProcessingLoads)
	{
		DatanodeProcessingLoad* dn = (DatanodeProcessingLoad*)lfirst(cell);
		free_allocated_frags(dn->datanodeBlocks);
	}
	list_free(allDNProcessingLoads);
}
