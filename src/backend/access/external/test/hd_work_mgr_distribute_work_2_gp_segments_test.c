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
 * In test__distribute_work_2_gp_segments we are testing the function distribute_work_2_gp_segments().
 * distribute_work_2_gp_segments() implements the algorithm that allocates the fragments of an external   
 * data source to the Hawq segments for processing.
 * This unitest verifies the algorithm output, and ensures the following algorithm behaviour:
 * a. The number of fragments allocated is equal to the input number of fragments
 * b. distribution of  work between segments: if there are two segments required and more than 
 *    one host, each segment will be on a different host
 * c. percent of local datanodes out of all datanodes
 * d. percent of local fragments out of all fragments
 * e. number of actual working segments is bigger than half of the initial working segments
 */

static void print_allocated_fragments(List **allocated_fragments, int total_segs);
static char* print_one_allocated_data_fragment(AllocatedDataFragment *frag, int seg_index);
static char* find_segment_ip_by_index(int seg_index);
static char** create_cluster(int num_hosts);
static void clean_cluster(char** cluster, int num_hosts);
static char** create_array_of_segs(char **cluster, int num_hosts, int num_segments_on_host);
static void clean_array_of_segs(char **array_of_segs, int number_of_segments);
static void print_cluster(char** cluster, int num_hosts);
static void print_segments_list();
void clean_allocated_fragments(List **allocated_fragments, int total_segs);
static void validate_total_fragments_allocated(List **allocated_fragments, int total_segs, int input_total_fragments);
static void validate_max_load_per_segment(List **allocated_fragments, int total_segs, int working_segs, int input_total_fragments);
static int calc_load_per_segment(int input_total_fragments, int working_segs);
static void validate_all_working_segments_engagement(List **allocated_fragments, 
													 int total_segs, 
													 int working_segs, 
													 int input_total_fragments,
													 int num_hosts_in_cluster);
static bool is_host_uniq(List** ips_list, char* ip);
static List* spread_fragments_in_cluster(int number_of_fragments, 
								  int number_of_hosts, 
								  int replication_factor, 
								  char **cluster,
								  int cluster_size);

/* test input data*/
typedef struct sTestInputData
{
	int m_num_hosts_in_cluster; /* cluster size mustn't exceed 65025 - see function create_cluster() */
	int m_num_data_fragments; /* number of fragments in the data we intend to allocate between the hawq segments */
	/* 
	 * number of datanodes that hold the 'querried' data - there is one datanode 
	 * on each cluster host - so there are <num_hosts_in_cluster> datanodes 
	 */
	int m_num_active_data_nodes; 
	int m_num_of_fragment_replicas;
	int m_num_segments_on_host;/* number of Hawq segments on each cluster host - we assume all cluster hosts have Hawq segments installed */
	/* 
	 * the subset of Hawq segments that will do the processing  - not all the Hawqs segments 
	 * in the cluster are involved.
	 * This parameter plays the role of max_participants_allowed that is passed to map_hddata_2gp_segments()
	 * in createplan.c
	 */
	int m_num_working_segs; 
	bool m_enable_print_input_cluster;
	bool m_enable_print_input_fragments;
	bool m_enable_print_input_segments;
	bool m_enable_print_allocated_fragments;	
} TestInputData;

static void test__distribute_work_to_gp_segments(TestInputData *input);
/*
 * TRACING CAPABILITIES
 * The unitest validates the behaviour of the SUT function distribute_work_2_gp_segments() using
 * the assert_XXX_... functions. But in order to understand the behaviour of the allocation algorithm
 * it can be helpful to look  at the various data structures involved. For this purpose we have 
 * several print functions:
 * a. print_cluster(...)
 * b. print_fragment_list(...)
 * c. print_segments_list(...)
 * d. print_allocated_fragments(...)
 * All these trace function have the output disabled by default. To enable the output of any print
 * function set the booleans enable_trace_... for the respective function
 * test__distribute_work_2_gp_segments()
 */


void
test__distribute_work_to_gp_segments__big_cluster_few_active_nodes(void **state)
{
	TestInputData *input = (TestInputData*)palloc0(sizeof(TestInputData));
	
	input->m_num_hosts_in_cluster = 1000; /* cluster size musn't exceed 65025 - see function create_cluster() */
	input->m_num_data_fragments = 100; /* number of fragments in the data we intend to allocate between the hawq segments */
	input->m_num_active_data_nodes = 10; /* number of datanodes that hold the 'querried' data - there one datanode om each cluster host - so there are <num_hosts_in_cluster> datanodes */
	input->m_num_of_fragment_replicas = 3;
	input->m_num_segments_on_host = 1;/* number of Hawq segments on each cluster host - we assume all cluster hosts have Hawq segments installed */
	input->m_num_working_segs = 64; /* the subset of Hawq segments that will do the processing  - not all the Hawqs segments in the cluster are involved */
	input->m_enable_print_input_cluster = false;
	input->m_enable_print_input_fragments = false;
	input->m_enable_print_input_segments = false;
	input->m_enable_print_allocated_fragments = false;

	test__distribute_work_to_gp_segments(input);

	pfree(input);
}

void
test__distribute_work_to_gp_segments__big_cluster_many_active_nodes(void **state)
{
	TestInputData *input = (TestInputData*)palloc0(sizeof(TestInputData));
	
	input->m_num_hosts_in_cluster = 1000; /* cluster size musn't exceed 65025 - see function create_cluster() */
	input->m_num_data_fragments = 100; /* number of fragments in the data we intend to allocate between the hawq segments */
	input->m_num_active_data_nodes = 100; /* number of datanodes that hold the 'querried' data - there one datanode om each cluster host - so there are <num_hosts_in_cluster> datanodes */
	input->m_num_of_fragment_replicas = 3;
	input->m_num_segments_on_host = 4;/* number of Hawq segments on each cluster host - we assume all cluster hosts have Hawq segments installed */
	input->m_num_working_segs = 64; /* the subset of Hawq segments that will do the processing  - not all the Hawqs segments in the cluster are involved */
	input->m_enable_print_input_cluster = false;
	input->m_enable_print_input_fragments = false;
	input->m_enable_print_input_segments = false;
	input->m_enable_print_allocated_fragments = false;
	
	test__distribute_work_to_gp_segments(input);
	pfree(input);
}

void
test__distribute_work_to_gp_segments__small_cluster(void **state)
{
	TestInputData *input = (TestInputData*)palloc0(sizeof(TestInputData));
	
	input->m_num_hosts_in_cluster = 100; /* cluster size musn't exceed 65025 - see function create_cluster() */
	input->m_num_data_fragments = 100; /* number of fragments in the data we intend to allocate between the hawq segments */
	input->m_num_active_data_nodes = 50; /* number of datanodes that hold the 'querried' data - there one datanode om each cluster host - so there are <num_hosts_in_cluster> datanodes */
	input->m_num_of_fragment_replicas = 3;
	input->m_num_segments_on_host = 4;/* number of Hawq segments on each cluster host - we assume all cluster hosts have Hawq segments installed */
	input->m_num_working_segs = 64; /* the subset of Hawq segments that will do the processing  - not all the Hawqs segments in the cluster are involved */
	input->m_enable_print_input_cluster = false;
	input->m_enable_print_input_fragments = false;
	input->m_enable_print_input_segments = false;
	input->m_enable_print_allocated_fragments = false;
	
	test__distribute_work_to_gp_segments(input);
	pfree(input);
}

void
test__distribute_work_to_gp_segments__small_cluster_many_active_nodes(void **state)
{
	TestInputData *input = (TestInputData*)palloc0(sizeof(TestInputData));
	
	input->m_num_hosts_in_cluster = 100; /* cluster size musn't exceed 65025 - see function create_cluster() */
	input->m_num_data_fragments = 100; /* number of fragments in the data we intend to allocate between the hawq segments */
	input->m_num_active_data_nodes = 90; /* number of datanodes that hold the 'querried' data - there one datanode om each cluster host - so there are <num_hosts_in_cluster> datanodes */
	input->m_num_of_fragment_replicas = 3;
	input->m_num_segments_on_host = 4;/* number of Hawq segments on each cluster host - we assume all cluster hosts have Hawq segments installed */
	input->m_num_working_segs = 64; /* the subset of Hawq segments that will do the processing  - not all the Hawqs segments in the cluster are involved */
	input->m_enable_print_input_cluster = false;
	input->m_enable_print_input_fragments = false;
	input->m_enable_print_input_segments = false;
	input->m_enable_print_allocated_fragments = false;
	
	test__distribute_work_to_gp_segments(input);
	pfree(input);
}

void
test__distribute_work_to_gp_segments__small_cluster_few_replicas(void **state)
{
	TestInputData *input = (TestInputData*)palloc0(sizeof(TestInputData));
	
	input->m_num_hosts_in_cluster = 100; /* cluster size musn't exceed 65025 - see function create_cluster() */
	input->m_num_data_fragments = 100; /* number of fragments in the data we intend to allocate between the hawq segments */
	input->m_num_active_data_nodes = 90; /* number of datanodes that hold the 'querried' data - there one datanode om each cluster host - so there are <num_hosts_in_cluster> datanodes */
	input->m_num_of_fragment_replicas = 2;
	input->m_num_segments_on_host = 4;/* number of Hawq segments on each cluster host - we assume all cluster hosts have Hawq segments installed */
	input->m_num_working_segs = 64; /* the subset of Hawq segments that will do the processing  - not all the Hawqs segments in the cluster are involved */
	input->m_enable_print_input_cluster = false;
	input->m_enable_print_input_fragments = false;
	input->m_enable_print_input_segments = false;
	input->m_enable_print_allocated_fragments = false;
	
	test__distribute_work_to_gp_segments(input);
	pfree(input);
}

/*
 * Testing distribute_work_2_gp_segments
 */
static void test__distribute_work_to_gp_segments(TestInputData *input)
{
	List **segs_allocated_data = NULL;
	List * input_fragments_list = NIL;
	char** array_of_segs = NULL;
	int total_segs;
	bool cluster_size_not_exceeded = input->m_num_hosts_in_cluster <=  65025;
	
	assert_true(cluster_size_not_exceeded);
	/*  
	 * 1. Initialize the test input parameters
	 * We are testing an N hosts cluster. The size of the cluster is set in this section - section 1. 
	 * Basic test assumptions:
	 * a. There is one datanode on each host in the cluster
	 * b. There are Hawq segments on each host in the cluster.
	 * c. There is an equal number of Hawq segments on each host - hardcoded in this section
	 */
	int num_hosts_in_cluster = input->m_num_hosts_in_cluster; /* cluster size musn't exceed 65025 - see function create_cluster() */
	int num_data_fragments = input->m_num_data_fragments; /* number of fragments in the data we intend to allocate between the hawq segments */
	int num_active_data_nodes = input->m_num_active_data_nodes; /* number of datanodes that hold the 'querried' data - there one datanode om each cluster host - so there are <num_hosts_in_cluster> datanodes */
	int num_of_fragment_replicas = input->m_num_of_fragment_replicas;
	int num_segments_on_host = input->m_num_segments_on_host;/* number of Hawq segments on each cluster host - we assume all cluster hosts have Hawq segments installed */
	int num_working_segs = input->m_num_working_segs; /* the subset of Hawq segments that will do the processing  - not all the Hawqs segments in the cluster are involved */
	bool enable_print_input_cluster = input->m_enable_print_input_cluster;
	bool enable_print_input_fragments = input->m_enable_print_input_fragments;
	bool enable_print_input_segments = input->m_enable_print_input_segments;
	bool enable_print_allocated_fragments = input->m_enable_print_allocated_fragments;
		
	/* 2. Create the cluster */
	char **cluster = create_cluster(num_hosts_in_cluster);
	
	if (enable_print_input_cluster)
		print_cluster(cluster, num_hosts_in_cluster);
	 	
	/* 3. Input - data fragments */
	input_fragments_list = spread_fragments_in_cluster(num_data_fragments, /* number of fragments in the data we are about to allocate */
													   num_active_data_nodes, /* hosts */
													   num_of_fragment_replicas, /* replicas */
													   cluster, /* the whole cluster*/
													   num_hosts_in_cluster/* the number of hosts in the cluster */);
	if (enable_print_input_fragments)
		print_fragment_list(input_fragments_list); 
	
	/* 4. Input - hawq segments */
	total_segs = num_hosts_in_cluster * num_segments_on_host;
	array_of_segs = create_array_of_segs(cluster, num_hosts_in_cluster, num_segments_on_host);	
		
    /* 5. Build QueryResource (acting hawq segments) */
    buildQueryResource(total_segs, array_of_segs);
    if (enable_print_input_segments)
    	print_segments_list();

    will_return(GetActiveQueryResource, resource);
    will_return(GetActiveQueryResource, resource);

	/* 6. The actual unitest of distribute_work_2_gp_segments() */
	segs_allocated_data = distribute_work_2_gp_segments(input_fragments_list, total_segs, num_working_segs);
	if (enable_print_allocated_fragments)
		print_allocated_fragments(segs_allocated_data, total_segs);
	
	/* 7. The validations - verifying that the expected output was obtained */
	validate_total_fragments_allocated(segs_allocated_data, total_segs, num_data_fragments);
	validate_max_load_per_segment(segs_allocated_data, total_segs, num_working_segs, num_data_fragments);
	validate_all_working_segments_engagement(segs_allocated_data, total_segs, num_working_segs, num_data_fragments, num_hosts_in_cluster);
	
	/* 8. Cleanup */
	freeQueryResource();
	clean_cluster(cluster, num_hosts_in_cluster);
	clean_array_of_segs(array_of_segs, total_segs);
	clean_allocated_fragments(segs_allocated_data, total_segs);
}

/* create an array of segments based on the host in the cluster and the number of Hawq segments on host */
static char** create_array_of_segs(char **cluster, int num_hosts, int num_segments_on_host)
{
	int i, j;
	int total_segs = num_hosts * num_segments_on_host;
	char **array_of_segs = (char**)palloc0(total_segs * sizeof(char *));
	
	for (i = 0; i < num_hosts; i++)
	{
		for (j = 0; j < num_segments_on_host; j++)
		{
			array_of_segs[i * num_segments_on_host + j] = pstrdup(cluster[i]);
		}
	}

	return array_of_segs;
}

/* clean the array of Hawq segments */
static void clean_array_of_segs(char **array_of_segs, int total_segments)
{
	int i;
	
	for (i = 0; i < total_segments; i++)
		pfree(array_of_segs[i]);
	pfree(array_of_segs);
}

/* gives an ip to each host in a num_hosts size cluster */
static char** create_cluster(int num_hosts)
{
	char** cluster = (char**)palloc0(num_hosts * sizeof(char *));
	int i;
	char *prefix = "1.2.%d.%d";
	int third_octet = 1; /* let's begin at 1 */
	int fourth_octet = 1;
	StringInfoData ip;
	initStringInfo(&ip);
	
	for (i = 0; i < num_hosts; i++)
	{
		appendStringInfo(&ip, prefix, third_octet, fourth_octet);
		cluster[i] = pstrdup(ip.data);
		/* this naming scheme will accomodate a cluster size up to 255x255 = 65025. */
		fourth_octet++;
		if (fourth_octet == 256)
		{
			fourth_octet = 1;
			third_octet++;
		}
		resetStringInfo(&ip);
	}

	return  cluster;
}

/* release memory */
static void clean_cluster(char** cluster, int num_hosts)
{
	int i;
	
	for (i = 0; i < num_hosts; i++)
	{
		if (cluster[i])
			pfree(cluster[i]);
	}
	pfree(cluster);
}

/* show the cluster*/
static void print_cluster(char** cluster, int num_hosts)
{
	int i;
	StringInfoData msg;
	initStringInfo(&msg);
	
	appendStringInfo(&msg, "cluster size: %d\n", num_hosts);
	for (i = 0; i < num_hosts; i++)
	{
		if (cluster[i])
			appendStringInfo(&msg, "cluster #%d:   %s\n", i + 1, cluster[i]);
		else
			appendStringInfo(&msg, "cluster naming error \n");
	}
	
	elog(FRAGDEBUG, "%s", msg.data);
	pfree(msg.data);
}

/* prints for each segments, the index and the host ip */
static void print_segments_list()
{
	StringInfoData msg;
	initStringInfo(&msg);

	for (int i = 0; i < resource->numSegments; ++i)
	{
		Segment* seg = list_nth(resource->segments, i);
		appendStringInfo(&msg, "\nsegment -- index: %d, ip: %s", seg->segindex, seg->hostip);
	}
	
	elog(FRAGDEBUG, "%s", msg.data);
	pfree(msg.data);
}

/* returns the ip  of the segment's host */
static char* find_segment_ip_by_index(int seg_index)
{	
	if (seg_index < 0 || seg_index >= resource->numSegments)
		assert_true(false);
		
	for (int i = 0; i < resource->numSegments; ++i)
	{
		Segment* seg = list_nth(resource->segments, i);
		if (seg->segindex == seg_index)
			return seg->hostip;
	}
	
	/* we assert if an index outside the boundaries was supplied */
	assert_true(false);
	return NULL;
}

/* 
 * print the allocated fragments list 
 * allocated_fragments is an array of lists. The size of the array is total_segs.
 * The list located at index i in the array , holds the fragments that will be processed
 * by Hawq segment i
 */
static void print_allocated_fragments(List **allocated_fragments, int total_segs)
{
	StringInfoData msg;
	initStringInfo(&msg);
	appendStringInfo(&msg, "ALLOCATED FRAGMENTS FOR EACH SEGMENT:\n");

	for (int i = 0; i < total_segs; i++)
	{
		if (allocated_fragments[i])
		{
			ListCell *frags_cell = NULL;
			foreach(frags_cell, allocated_fragments[i])
			{
				AllocatedDataFragment *frag = (AllocatedDataFragment*)lfirst(frags_cell);
				appendStringInfo(&msg, "%s\n", print_one_allocated_data_fragment(frag, i));
			}
		}
	}
		
	elog(FRAGDEBUG, "%s", msg.data);
	if (msg.data)
		pfree(msg.data);
}

/* print one allocated fragment */
static char* print_one_allocated_data_fragment(AllocatedDataFragment *frag, int seg_index)
{
	StringInfoData msg;
	initStringInfo(&msg);
	char* seg_ip = find_segment_ip_by_index(seg_index);
	if (!seg_ip)
		seg_ip = "INVALID SEGMENT INDEX";
	bool locality = (strcmp(frag->host, seg_ip) == 0) ? true : false;
	
	appendStringInfo(&msg, 
	                 "locality: %d, segment number: %d , segment ip: %s --- fragment index: %d, datanode host: %s, file: %s", 
	                 locality, seg_index, seg_ip, frag->index, frag->host, frag->source_name);

	return msg.data;
}

/* release memory of allocated_fragments */
void clean_allocated_fragments(List **allocated_fragments, int total_segs)
{
	for (int i = 0; i < total_segs; i++)
		if (allocated_fragments[i])
			free_allocated_frags(allocated_fragments[i]);
	pfree(allocated_fragments);
}

/* calculate the optimal load distribution per segment */
static int calc_load_per_segment(int input_total_fragments, int working_segs)
{
	return (input_total_fragments % working_segs) ? input_total_fragments / working_segs + 1: 
	input_total_fragments / working_segs;
}

/* 
 * test that a host is uniq.
 * the functions ensures that ip names are unique by managing a set of ips
 * the set is implemented with a linked list
 */
static bool is_host_uniq(List** ips_list, char* ip)
{
	ListCell* cell;
	foreach(cell, *ips_list)
	{
		char* foundip = (char*)lfirst(cell);
		if (strcmp(foundip, ip) == 0)
			return false;
	}
	
	lappend(*ips_list, ip);
	return true;
}

/* validate that all input blocks were allocated */
static void validate_total_fragments_allocated(List **allocated_fragments, int total_segs, int input_total_fragments)
{
	int total_fragments_allocated = 0; 

	for (int i = 0; i < total_segs; i++)
	{
		if (allocated_fragments[i])
			total_fragments_allocated += list_length(allocated_fragments[i]);
	}

	assert_int_equal(total_fragments_allocated, input_total_fragments);
}

/* validate that the load per segment does not exceed the expected load */
static void validate_max_load_per_segment(List **allocated_fragments, int total_segs, int working_segs, int input_total_fragments)
{
	int max_load = 0;
	int load_per_segment =  calc_load_per_segment(input_total_fragments, working_segs);
	
	for (int i = 0; i < total_segs; i++)
	{
		if (allocated_fragments[i] && list_length(allocated_fragments[i]) > max_load)
			max_load = list_length(allocated_fragments[i]);
	}
	
	bool load_per_segment_not_exceeded = load_per_segment >=  max_load;
	elog(FRAGDEBUG, "actual max_load: %d, expected load_per_segment: %d", max_load, load_per_segment);
	assert_true(load_per_segment_not_exceeded);
}

/* 
 * we validate that every working segment is engaged, by verifying that for the case when 
 * the load_per_segment is greater than one, then every working_segment has allocated fragments,
 * and for the case when load_per_segment is 1, then the number of segments that got work
 * equals the number of fragments
 */
static void validate_all_working_segments_engagement(List **allocated_fragments, 
													 int total_segs, 
													 int working_segs, 
													 int input_total_fragments,
													 int num_hosts_in_cluster)
{
	List* ips_list = NIL;
	ListCell* cell;
	int total_segs_engaged = 0;
	int load_per_segment =  calc_load_per_segment(input_total_fragments, working_segs);
	bool require_full_distribution = num_hosts_in_cluster >= working_segs;
	
	for (int i = 0; i < total_segs; i++)
		if (allocated_fragments[i] && list_length(allocated_fragments[i]) > 0)
		{
			char *ip;
			bool isuniq;
			total_segs_engaged++;
			if (require_full_distribution)
			{
				ip = find_segment_ip_by_index(i);
				isuniq = is_host_uniq(&ips_list, ip);
				assert_true(isuniq);
			}
		}
	
	if (load_per_segment == 1)
		assert_int_equal(total_segs_engaged, input_total_fragments);
	else
	{
		bool total_segs_engaged_not_exceeded = total_segs_engaged <= working_segs;
		assert_true(total_segs_engaged_not_exceeded);
	}
	
	/* clean memory */
	foreach(cell, ips_list)
		pfree(lfirst(cell));
	list_free(ips_list);
}

/*
 * Creates a list of DataFragment for one file ("file.txt").
 * The important thing here is the fragments' location. It is deteremined by the parameters:
 * replication_factor - number of copies of each fragment on the different hosts.
 * number_of_hosts - number of hosts
 * number_of_fragments - number of fragments in the file.
 * cluster - holds the ips of all hosts in the cluster
 *
 * Each fragment will have <replication_factor> hosts from the cluster
 */
static List* 
spread_fragments_in_cluster(int number_of_fragments, 
								  int number_of_hosts, 
								  int replication_factor, 
								  char **cluster, 
								  int cluster_size)
{
	int first_host, target_host;
	List* fragments_list = NIL;
	StringInfoData string_info;
	initStringInfo(&string_info);

	/* pick the first host in the cluster that will host the data. The fragments will be spread from this host onward */
	first_host = 0;
	
	target_host = first_host;
	
	for (int i = 0; i < number_of_fragments; ++i)
	{
		DataFragment* fragment = (DataFragment*) palloc0(sizeof(DataFragment));
		
		fragment->index = i;
		fragment->source_name = pstrdup("file.txt");
		
		for (int j = 0; j < replication_factor; ++j)
		{
			FragmentHost* fhost = (FragmentHost*)palloc0(sizeof(FragmentHost));
			appendStringInfo(&string_info, cluster[target_host]);
			fhost->ip = pstrdup(string_info.data);
			resetStringInfo(&string_info);
			fragment->replicas = lappend(fragment->replicas, fhost);
			
			target_host = ((j + i + first_host) % number_of_hosts);
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

