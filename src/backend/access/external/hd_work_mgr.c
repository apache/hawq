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
 * hd_work_mgr.c
 *	  distributes PXF data fragments for processing between GP segments
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <curl/curl.h>
#include <json-c/json.h>
#include "access/pxfmasterapi.h"
#include "access/hd_work_mgr.h"
#include "access/libchurl.h"
#include "access/pxfheaders.h"
#include "access/pxffilters.h"
#include "access/pxfutils.h"
#include "cdb/cdbfilesystemcredential.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/elog.h"

/*
 * Contains the information about the allocated data fragment that will be sent to the
 * GP segment
 */
typedef struct sAllocatedDataFragment
{
	char *host; /* ip / hostname */
	int   rest_port;
	int   index; /* index per source name */
	char *source_name; /* source name */
	char *fragment_md; /* fragment meta data */
	char *user_data; /* additional user data */
	char *profile; /* recommended profile to work with fragment */
} AllocatedDataFragment;

/*
 * Internal Structure which records how many blocks will be processed on each DN.
 * A given block can be replicated on several data nodes. We use this structure
 * in order to achieve a fair distribution of the processing over the data nodes. 
 * Example: Let's say we have 10 blocks, and all 10 blocks are replicated on 2 data
 * nodes. Meaning dn0 has a copy of each one of the the 10 blocks and also dn1 
 * has a copy of each one of the 10 blocks. We wouldn't like to process the whole 
 * 10 blocks only on dn0 or only on dn1. What we would like to achieve, is that
 * blocks 0 - 4 are processed on dn0 and blocks 5 - 9 are processed in dn1.
 * sDatanodeProcessingLoad is used to achieve this.
 */
typedef struct sDatanodeProcessingLoad
{
	char *dataNodeIp;
	int   port;
	/* 
	 * The number of fragments that the segments are going to read. It is generally smaller than 
	 * than the num_fragments_residing, because replication strategies will place a given fragment
	 * on several datanodes, and we are going to read the fragment from only one of the data nodes.
	 */
	int   num_fragments_read; 
	int   num_fragments_residing; /* the total number of fragments located on this data node*/
	List  *datanodeBlocks;
} DatanodeProcessingLoad;

/* the group is comprised from all the segments that sit on the same host*/
typedef struct sGpHost
{
	char *ip;
	List *segs; /* list of Segment* */
} GpHost;



/*
 * segwork is the output string that describes the data fragments that were
 * allocated for processing to one segment. It is of the form:
 * "segwork=<size1>@<host1>@<port1>@<sourcename1>@<fragment_index1><size2>@<host2>@@<port2>@<sourcename1><fragment_index2><size3>@...<sizeN>@<hostN>@@<portN>@<sourcenameM><fragment_indexN>"
 * Example: "segwork=32@123.45.78.90@50080@sourcename1@332@123.45.78.90@50080@sourcename1@833@123.45.78.91@50081@sourcename2@1033@123.45.78.91@50081@sourcename2@11"
 */

static const char *SEGWORK_PREFIX = "segwork=";
static const char SEGWORK_IN_PAIR_DELIM = '@';

static List* free_fragment_list(List *fragments);
static List** distribute_work_2_gp_segments(List *data_fragments_list, int num_segs, int working_segs);
static void  print_fragment_list(List *frags_list);
static char* make_allocation_output_string(List *segment_fragments);
static List* free_allocated_frags(List *segment_fragments);
static void init_client_context(ClientContext *client_context);
static GPHDUri* init(char* uri, ClientContext* cl_context);
static List* allocate_fragments_to_datanodes(List *whole_data_fragments_list);
static DatanodeProcessingLoad* get_dn_processing_load(List **allDNProcessingLoads, FragmentHost *fragment_host);
static void print_data_nodes_allocation(List *allDNProcessingLoads, int total_data_frags);
static List* do_segment_clustering_by_host(void);
static ListCell* pick_random_cell_in_list(List* list);
static void clean_gphosts_list(List *hosts_list);
static AllocatedDataFragment* create_allocated_fragment(DataFragment *fragment);
static char** create_output_strings(List **allocated_fragments, int total_segs);
static void assign_pxf_port_to_fragments(int remote_rest_port, List *fragments);
static void generate_delegation_token(PxfInputData *inputData);
static void cancel_delegation_token(PxfInputData *inputData);

/*
 * the interface function of the hd_work_mgr module
 * coordinates the work involved in generating the mapping 
 * of the Hadoop data fragments to the GP segments for processing.
 * From a total_segs number of segments only  working_segs number
 * of segments will be allocated data fragments.
 * returns a string array, where string i holds the indexes of the
 * data fragments that will be processed by the GP segment i.
 * The data fragments is a term that comes to represent any fragments
 * or a supported data source (e.g, HBase region, HDFS splits, etc).
 *
 * The function will generate a delegation token when secure filesystem mode 
 * is on and cancel it right after. PXF segment code will get a new token.
 */
char** map_hddata_2gp_segments(char* uri, int total_segs, int working_segs, Relation relation, List* quals)
{
	char **segs_work_map = NULL;
	List **segs_data = NULL;
	List *data_fragments = NIL;
	ClientContext client_context; /* holds the communication info */
	PxfInputData inputData = {0};

	if (!RelationIsValid(relation))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid parameter relation")));
	
	/*
	 * 1. Cherrypick the data relevant for HADOOP from the input uri and init curl headers
	 */
	GPHDUri* hadoop_uri = init(uri, &client_context);
	if (!hadoop_uri)
		return (char**)NULL;

	/*
	 * Enrich the curl HTTP header
	 */
	inputData.headers = client_context.http_headers;
	inputData.gphduri = hadoop_uri;
	inputData.rel = relation;
	inputData.filterstr = serializePxfFilterQuals(quals); /* We do supply filter data to the HTTP header */
    generate_delegation_token(&inputData);
	build_http_header(&inputData);
	
	/*
	 * 2. Get the fragments data from the PXF service
	 */
	data_fragments = get_data_fragment_list(hadoop_uri, &client_context);

	assign_pxf_port_to_fragments(atoi(hadoop_uri->port), data_fragments);

	/* debug - enable when tracing */
	print_fragment_list(data_fragments);

	/*
	 * 3. Finally, call the actual work allocation algorithm
	 */
	segs_data = distribute_work_2_gp_segments(data_fragments, total_segs, working_segs);

	/*
	 * 4. For each segment transform the list of allocated fragments into an output string
	 */
	segs_work_map = create_output_strings(segs_data, total_segs);

	/*
	 * 5. Release memory
	 */
	free_fragment_list(data_fragments);
	freeGPHDUri(hadoop_uri);
	churl_headers_cleanup(client_context.http_headers);
    cancel_delegation_token(&inputData);
		
	return segs_work_map;
}

/*
 * Assign the remote rest port to each fragment
 */
static void assign_pxf_port_to_fragments(int remote_rest_port, List *fragments)
{
	ListCell *frag_c = NULL;
	
	foreach(frag_c, fragments)
	{
		ListCell 		*host_c 		= NULL;
		DataFragment 	*fragdata	= (DataFragment*)lfirst(frag_c);
		char* ip = NULL;
		int port = remote_rest_port;
		
		foreach(host_c, fragdata->replicas)
		{
			FragmentHost *fraghost = (FragmentHost*)lfirst(host_c);
			/* In case there are several fragments on the same host, we assume
			 * there are multiple DN residing together.
			 * The port is incremented by one, to match singlecluster convention */
			if (pxf_service_singlecluster && !pxf_isilon)
			{
				if (ip == NULL)
				{
					ip = fraghost->ip;
				}
				else if (are_ips_equal(ip, fraghost->ip))
				{
					port++;
				}
			}
			fraghost->rest_port = port;
		}
	}	

	if (pxf_service_singlecluster && pxf_isilon)
			elog(INFO, "Both flags pxf_service_singlecluster and pxf_isilon are ON. There is no possibility to use several PXF "
					    "instances on the same machine when Isilon is on. Only one PXF instance will be used.");
}

/*
 * Fetches fragments statistics of the PXF datasource from the PXF service
 *
 * The function will generate a delegation token when secure filesystem mode
 * is on and cancel it right after.
 */
PxfFragmentStatsElem *get_pxf_fragments_statistics(char *uri, Relation rel)
{
	ClientContext client_context; /* holds the communication info */
	PxfInputData inputData = {0};
	PxfFragmentStatsElem *result = NULL;

	GPHDUri* hadoop_uri = init(uri, &client_context);
	if (!hadoop_uri)
	{
		elog(ERROR, "Failed to parse PXF location %s", uri);
	}

	/*
	 * Enrich the curl HTTP header
	 */
	inputData.headers = client_context.http_headers;
	inputData.gphduri = hadoop_uri;
	inputData.rel = rel;
	inputData.filterstr = NULL; /* We do not supply filter data to the HTTP header */
    generate_delegation_token(&inputData);
	build_http_header(&inputData);

	result = get_fragments_statistics(hadoop_uri, &client_context);

	cancel_delegation_token(&inputData);
	return result;
}

/*
 * Preliminary uri parsing and curl initializations for the REST communication
 */
static GPHDUri* init(char* uri, ClientContext* cl_context)
{	
	char *fragmenter = NULL;
	char *profile = NULL;

	/*
	 * 1. Cherrypick the data relevant for HADOOP from the input uri
	 */
	GPHDUri* hadoop_uri = parseGPHDUri(uri);

	/*
	 * 2. Communication with the Hadoop back-end
	 *    Initialize churl client context and header
	 */
	init_client_context(cl_context);
	cl_context->http_headers = churl_headers_init();
	
	/* set HTTP header that guarantees response in JSON format */
	churl_headers_append(cl_context->http_headers, REST_HEADER_JSON_RESPONSE, NULL);
	if (!cl_context->http_headers)
		return NULL;
	
	/*
	 * 3. Test that Fragmenter or Profile was specified in the URI
	 */
	if(GPHDUri_get_value_for_opt(hadoop_uri, "fragmenter", &fragmenter, false) != 0 
		&& GPHDUri_get_value_for_opt(hadoop_uri, "profile", &profile, false) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("FRAGMENTER or PROFILE option must exist in %s", hadoop_uri->uri)));
	}

	return hadoop_uri;	
}


/*
 * Interface function that releases the memory allocated for the strings array
 * which represents the mapping of the Hadoop data fragments to the GP segments
 */
void free_hddata_2gp_segments(char **segs_work_map, int num_segs)
{
	if (segs_work_map == NULL)
		return;
	
	for (int i = 0; i < num_segs; i++)
	{
		if (segs_work_map[i])
			pfree(segs_work_map[i]);
	}
	pfree(segs_work_map);
}

/*
 * The algorithm that decides which Hadoop data fragments will be processed
 * by each gp segment.
 *
 * Returns an array of Lists of AllocatedDataFragments
 * In case  a  given GP segment will not be allocated data fragments for processing,
 * we will put a NULL at it's index in the output  array.
 * total_segs - total number of active primary segments in the cluster
 * working_segs - the number of Hawq segments that will be used for processing the PXF data. It is smaller or equal to total_segs,
 *                and was obtained using the guc gp_external_max_segs
 */
static List** 
distribute_work_2_gp_segments(List *whole_data_fragments_list, int total_segs, int working_segs)
{
	/*
	 * Example of a string representing the data fragments allocated to one segment: 
	 * 'segwork=<size>data_node_host@0-data_node_host@1-data_node_host@5<size>data_node_host@8-data_node_host@9'
	 */
	StringInfoData msg;
	int count_total_blocks_allocated = 0;
	int total_data_frags = 0;
	int blks_per_seg = 0;
	List *allDNProcessingLoads = NIL; /* how many blocks are allocated for processing on each data node */
	List *gpHosts = NIL; /* the hosts of the gp_cluster. Every host has several gp segments */
	List *reserve_gpHosts = NIL;
	List **segs_data = (List **)palloc0(total_segs * sizeof(List *));
	QueryResource *resource = GetActiveQueryResource();

	if (resource == NULL)
	{
	  return segs_data;
	}

	initStringInfo(&msg);
	
	/* 
	 * We copy each fragment from whole_data_fragments_list into allDNProcessingLoads, but now
	 * each fragment will point to just one datanode - the processing data node, instead of several replicas.
	 */
	allDNProcessingLoads = allocate_fragments_to_datanodes(whole_data_fragments_list);
	/* arrange all segments in groups where each group has all the segments which sit on the same host */
	gpHosts =  do_segment_clustering_by_host();
	
	/* 
	 * define the job at hand: how many fragments we have to allocate and what is the allocation load on each 
	 * GP segment. We will distribute the load evenly between the segments
	 */
	total_data_frags = list_length(whole_data_fragments_list);
	if (total_data_frags > working_segs)
		blks_per_seg = (total_data_frags % working_segs == 0) ? total_data_frags / working_segs :
				total_data_frags / working_segs + 1; /* rounding up - when total_data_frags is not divided by working_segs */
	else /* total_data_frags < working_segs */
	{
		blks_per_seg = 1;
		working_segs = total_data_frags;
	}
	
	print_data_nodes_allocation(allDNProcessingLoads, total_data_frags);
	
	/* Allocate blks_per_seg to each working segment */
	for (int i = 0; i < working_segs; i++) 
	{
		ListCell *gp_host_cell = NULL;
		ListCell *seg_cell = NULL;
		ListCell *datanode_cell = NULL;
		ListCell *block_cell = NULL;
		List *allocatedBlocksPerSegment = NIL; /* list of the data fragments for one segment */
		Segment *seg = NULL;
		
		if (!allDNProcessingLoads) /* the blocks are finished */
			break;
		
		/*
		 * If gpHosts is empty, it means that we removed one segment from each host 
		 * and went over all hosts. Let's go over the hosts again and take another segment  
		 */
		if (gpHosts == NIL)
		{
			gpHosts = reserve_gpHosts;
			reserve_gpHosts = NIL;
		}
		Assert(gpHosts != NIL);
		
		/* pick the starting segment on the host randomly */
		gp_host_cell = pick_random_cell_in_list(gpHosts);
		GpHost* gp_host = (GpHost*)lfirst(gp_host_cell);
		seg_cell = pick_random_cell_in_list(gp_host->segs);
		
		/* We found our working segment. let's remove it from the list so it won't get picked up again*/
		seg = (Segment *)lfirst(seg_cell);
		gp_host->segs = list_delete_ptr(gp_host->segs, seg); /* we remove the segment from the group but do not delete the segment */
		
		/* 
		 * We pass the group to the reserve groups so there is no chance, next iteration will pick the same host
		 * unless we went over all hosts and took a segment from each one.
		 */
		gpHosts = list_delete_ptr(gpHosts, gp_host);
		if (gp_host->segs != NIL)
			reserve_gpHosts = lappend(reserve_gpHosts, gp_host);
		else
			pfree(gp_host);
		
		/*
		 * Allocating blocks for this segment. We are going to look in two places.
		 * The first place that supplies block for the segment wins.
		 * a. Look in allDNProcessingLoads for a datanode on the same host with the segment
		 * b. Look for a datanode at the head of the allDNProcessingLoads list
		 */
		while (list_length(allocatedBlocksPerSegment) < blks_per_seg && count_total_blocks_allocated < total_data_frags)
		{
			DatanodeProcessingLoad *found_dn = NULL;
			char *host_ip = seg->hostip;
			
			/* 
			 * locality logic depends on whether we require optimizations (pxf_enable_locality_optimizations guc)
			 */
			if (pxf_enable_locality_optimizations && !pxf_isilon)
			{
				foreach(datanode_cell, allDNProcessingLoads) /* an attempt at locality - try and find a datanode sitting on the same host with the segment */
				{
					DatanodeProcessingLoad *dn = (DatanodeProcessingLoad*)lfirst(datanode_cell);
					if (are_ips_equal(host_ip, dn->dataNodeIp))
					{
						found_dn = dn;
						appendStringInfo(&msg, "PXF - Allocating the datanode blocks to a local segment. IP: %s\n", found_dn->dataNodeIp);
						break;
					}
				}
			}
			if (allDNProcessingLoads && !found_dn) /* there is no datanode on the segment's host. Let's just pick the first data node */
			{
				found_dn = linitial(allDNProcessingLoads);
				if (found_dn)
					appendStringInfo(&msg, "PXF - Allocating the datanode blocks to a remote segment. Datanode IP: %s Segment IP: %s\n",
						 found_dn->dataNodeIp, host_ip);
			}
			
			if (!found_dn) /* there is no datanode found at the head of allDNProcessingLoads, it means we just finished the blocks*/
				break;
			
			/* we have a datanode */
			while ( (block_cell = list_head(found_dn->datanodeBlocks)) )
			{
				AllocatedDataFragment* allocated = (AllocatedDataFragment*)lfirst(block_cell);
				/*
				 * in case of remote storage, the segment host is also where the PXF will be running
				 * so we set allocated->host accordingly, instead of the remote storage system - datanode ip.
				 */
				if (pxf_isilon)
				{
					pfree(allocated->host);
					allocated->host = pstrdup(host_ip);
				}
				allocatedBlocksPerSegment = lappend(allocatedBlocksPerSegment, allocated);
				
				found_dn->datanodeBlocks = list_delete_first(found_dn->datanodeBlocks);
				found_dn->num_fragments_read--;
				count_total_blocks_allocated++;
				if (list_length(allocatedBlocksPerSegment) == blks_per_seg || count_total_blocks_allocated == total_data_frags) /* we load blocks on a segment up to blks_per_seg */
					break;
			}
			
			/* test if the DatanodeProcessingLoad is empty */
			if (found_dn->num_fragments_read == 0)
			{
				Assert(found_dn->datanodeBlocks == NIL); /* ensure datastructure is consistent */
				allDNProcessingLoads = list_delete_ptr(allDNProcessingLoads, found_dn); /* clean allDNProcessingLoads */
				pfree(found_dn->dataNodeIp); /* this one is ours */
				pfree(found_dn);
			}
		} /* Finished allocating blocks for this segment */
		elog(FRAGDEBUG, "%s", msg.data);
		resetStringInfo(&msg);
		
		if (allocatedBlocksPerSegment)
			segs_data[seg->segindex] = allocatedBlocksPerSegment;
			
	} /* i < working_segs; */
	
	Assert(count_total_blocks_allocated == total_data_frags); /* guarantee we allocated all the blocks */ 
	
	/* cleanup */
	pfree(msg.data);
	clean_gphosts_list(gpHosts);
	clean_gphosts_list(reserve_gpHosts);
			
	return segs_data;
}

/* 
 * create the allocation strings for each segments from the list of AllocatedDataFragment instances
 * that each segment holds
 * Returns an array of strings where each string in the array represents the fragments
 * allocated to the corresponding GP segment: The string contains the fragment
 * indexes starting with a byte containing the fragment's size. Example of a string that allocates fragments 0, 3, 7
 * 'segwork=<size>data_node_host@0<size>data_node_host@3<size>data_node_host@7'. <segwork=> prefix is required to integrate this string into
 * <CREATE EXTERNAL TABLE> syntax. 
 * In case  a  given GP segment will not be allocated data fragments for processing,
 * we will put a NULL at it's index in the output strings array.
 * total_segs - total number of active primary segments in the cluster
 * The allocated_fragments are freed after the data is transported to a strings array
 */
static char** create_output_strings(List **allocated_fragments, int total_segs)
{
	StringInfoData msg;
	char** segs_work_map = (char **)palloc0(total_segs * sizeof(char *));
	QueryResource *resource = GetActiveQueryResource();
	if (resource == NULL)
	{
	  return segs_work_map;
	}
	initStringInfo(&msg);
	
	for (int i = 0; i < total_segs; i++)
	{
		if (allocated_fragments[i])
		{
			segs_work_map[i] = make_allocation_output_string(allocated_fragments[i]);
			appendStringInfo(&msg, "PXF data fragments allocation for seg %d: %s\n", i, segs_work_map[i]);
			free_allocated_frags(allocated_fragments[i]);
		}
	}
	
	elog(FRAGDEBUG, "%s", msg.data);
	if (msg.data)
		pfree(msg.data);
	pfree(allocated_fragments);
	
	return segs_work_map;
}

/* do not free CdbComponentDatabaseInfo *seg, it's not ours */
static void clean_gphosts_list(List *hosts_list)
{
	ListCell *cell = NULL;
	
	foreach(cell, hosts_list)
	{
		GpHost *host = (GpHost*)lfirst(cell);
		pfree(host); /* don't touch host->host, does not belong to us */
	}
	
	if(hosts_list)
		list_free(hosts_list);
}

/* Randomly picks up a ListCell from a List */
static ListCell*
pick_random_cell_in_list(List* list)
{
	ListCell * cell  = NULL;
	int num_items = list_length(list);
	int wanted = cdb_randint(0, num_items - 1);

	cell = list_nth_cell(list, wanted);
	return cell;
}

/* arrange all segments in groups where each group has all the segments which sit on the same host */
static List*
do_segment_clustering_by_host(void)
{
	List *gpHosts = NIL;
	QueryResource *resource = GetActiveQueryResource();
	List *segments;
	ListCell *lc;

	Assert(resource);
	segments = resource->segments;

	foreach(lc, segments)
	{
	  bool group_exist = false;
	  ListCell *gp_host_cell = NULL;
	  Segment *segment = (Segment *)lfirst(lc);
	  char *candidate_ip = segment->hostip;

	  if (!segment->alive)
	  {
	    continue;
	  }

	  foreach(gp_host_cell, gpHosts)
	  {
	    GpHost *group = (GpHost *)lfirst(gp_host_cell);
	    if (are_ips_equal(candidate_ip, group->ip))
	    {
	      group->segs = lappend(group->segs, segment);
	      group_exist = true;
	      break;
	    }
	  }

	  if (!group_exist)
	  {
	    GpHost *group = (GpHost *)palloc0(sizeof(GpHost));
	    group->ip = candidate_ip;
	    group->segs = lappend(group->segs, segment);
	    gpHosts = lappend(gpHosts, group);
	  }
	}

	return gpHosts;
}

/* trace the datanodes allocation */
static void
print_data_nodes_allocation(List *allDNProcessingLoads, int total_data_frags)
{
	StringInfoData msg;
	ListCell *datanode_cell = NULL;
	initStringInfo(&msg);
	
	elog(FRAGDEBUG, "Total number of data fragments: %d", total_data_frags);
	foreach(datanode_cell, allDNProcessingLoads)
	{
		ListCell *data_frag_cell = NULL;
		
		DatanodeProcessingLoad *dn = (DatanodeProcessingLoad*)lfirst(datanode_cell);
		appendStringInfo(&msg, "DATANODE ALLOCATION LOAD  --  host: %s, port: %d, num_fragments_read: %d, num_fragments_residing: %d\n",
						 dn->dataNodeIp, dn->port, dn->num_fragments_read, dn->num_fragments_residing);
		
		foreach (data_frag_cell, dn->datanodeBlocks)
		{
			AllocatedDataFragment *frag = (AllocatedDataFragment*)lfirst(data_frag_cell);
			appendStringInfo(&msg, "file name: %s, split index: %d, dn host: %s, dn port: %d, fragment_md: %s\n",
							 frag->source_name, frag->index, frag->host, frag->rest_port, frag->fragment_md);
		}
		elog(FRAGDEBUG, "%s", msg.data);
		resetStringInfo(&msg);
	}
	pfree(msg.data);
}

/* 
 * We copy each fragment from whole_data_fragments_list into frags_distributed_for_processing, but now
 * each fragment will point to just one datanode - the processing data node, instead of several replicas.
 */
static List*
allocate_fragments_to_datanodes(List *whole_data_fragments_list)
{
	List* allDNProcessingLoads = NIL;
	AllocatedDataFragment* allocated = NULL;
	ListCell *cur_frag_cell = NULL;
	ListCell *fragment_host_cell = NULL;
		
	foreach(cur_frag_cell, whole_data_fragments_list)
	{
		DatanodeProcessingLoad* previous_dn = NULL;
		DataFragment* fragment = (DataFragment*)lfirst(cur_frag_cell);
		allocated = create_allocated_fragment(fragment);
		
		/* 
		 * From all the replicas that hold this block we are going to pick just one. 
		 * What is the criteria for picking? We pick the data node that until now holds
		 * the least number of blocks. The number of processing blocks for each dn is
		 * held in the list allDNProcessingLoads
		 */
		foreach(fragment_host_cell, fragment->replicas)
		{
			FragmentHost *host = lfirst(fragment_host_cell);
			DatanodeProcessingLoad* loc_dn = get_dn_processing_load(&allDNProcessingLoads, host);
			loc_dn->num_fragments_residing++;
			if (!previous_dn)
				previous_dn = loc_dn;
			else if (previous_dn->num_fragments_read  > loc_dn->num_fragments_read)
				previous_dn = loc_dn;
				
		}
		previous_dn->num_fragments_read++;
		
		allocated->host = pstrdup(previous_dn->dataNodeIp);
		allocated->rest_port = previous_dn->port;
		
		previous_dn->datanodeBlocks = lappend(previous_dn->datanodeBlocks, allocated);
	}
	
	return allDNProcessingLoads;
}

/* Initializes an AllocatedDataFragment */
static AllocatedDataFragment*
create_allocated_fragment(DataFragment *fragment)
{
	AllocatedDataFragment* allocated = palloc0(sizeof(AllocatedDataFragment));
	allocated->index = fragment->index; /* index per source */
	allocated->source_name = pstrdup(fragment->source_name);
	allocated->fragment_md = (fragment->fragment_md) ? pstrdup(fragment->fragment_md) : NULL;
	allocated->user_data = (fragment->user_data) ? pstrdup(fragment->user_data) : NULL;
	allocated->profile = (fragment->profile) ? pstrdup(fragment->profile) : NULL;
	return allocated;
}

/* 
 * find out how many fragments are already processed on the data node with this fragment's host_ip and port 
 * If this fragment's datanode is not represented in the allDNProcessingLoads, we will create a DatanodeProcessingLoad
 * structure for this fragment's datanode and, and add this new structure to allDNProcessingLoads
 */
static DatanodeProcessingLoad* get_dn_processing_load(List **allDNProcessingLoads, FragmentHost *fragment_host)
{
	ListCell *dn_blocks_cell = NULL;
	DatanodeProcessingLoad *dn_found = NULL;
	
	foreach(dn_blocks_cell, *allDNProcessingLoads)
	{
		DatanodeProcessingLoad *dn_blocks = (DatanodeProcessingLoad*)lfirst(dn_blocks_cell);
		if ( are_ips_equal(dn_blocks->dataNodeIp, fragment_host->ip) &&
			 (dn_blocks->port == fragment_host->rest_port) )
		{
			dn_found = dn_blocks;
			break;
		}
	}
	
	if (dn_found == NULL)
	{
		dn_found = palloc0(sizeof(DatanodeProcessingLoad));
		dn_found->datanodeBlocks = NIL;
		dn_found->dataNodeIp = pstrdup(fragment_host->ip);
		dn_found->port = fragment_host->rest_port;
		dn_found->num_fragments_read = 0;
		dn_found->num_fragments_residing = 0;
		*allDNProcessingLoads = lappend(*allDNProcessingLoads, dn_found);
	}
		
	return dn_found;
}

/*
 * Create the output string from the allocated fragments list
 */
static char*
make_allocation_output_string(List *segment_fragments)
{
	if (list_length(segment_fragments) == 0)
		return NULL;
	
	ListCell *frag_cell = NULL;
	StringInfoData segwork;
	StringInfoData fragment_str;
	int fragment_size;
	
	initStringInfo(&segwork);
	appendStringInfoString(&segwork, SEGWORK_PREFIX);
	
	foreach(frag_cell, segment_fragments)
	{
		AllocatedDataFragment *frag = (AllocatedDataFragment*)lfirst(frag_cell);

		initStringInfo(&fragment_str);
		appendStringInfoString(&fragment_str, frag->host);
		appendStringInfoChar(&fragment_str, SEGWORK_IN_PAIR_DELIM);
		appendStringInfo(&fragment_str, "%d", frag->rest_port);
		appendStringInfoChar(&fragment_str, SEGWORK_IN_PAIR_DELIM);
		appendStringInfo(&fragment_str, "%s", frag->source_name);
		appendStringInfoChar(&fragment_str, SEGWORK_IN_PAIR_DELIM);
		appendStringInfo(&fragment_str, "%d", frag->index);
		appendStringInfoChar(&fragment_str, SEGWORK_IN_PAIR_DELIM);
		if (frag->fragment_md)
		{
			appendStringInfo(&fragment_str, "%s", frag->fragment_md);
		}

		appendStringInfoChar(&fragment_str, SEGWORK_IN_PAIR_DELIM);
		if (frag->user_data)
		{
			appendStringInfo(&fragment_str, "%s", frag->user_data);
		}
		appendStringInfoChar(&fragment_str, SEGWORK_IN_PAIR_DELIM);
		if (frag->profile)
		{
			appendStringInfo(&fragment_str, "%s", frag->profile);
		}
		appendStringInfoChar(&fragment_str, SEGWORK_IN_PAIR_DELIM);

		fragment_size = strlen(fragment_str.data);

		appendStringInfo(&segwork, "%d", fragment_size);
		appendStringInfoChar(&segwork, SEGWORK_IN_PAIR_DELIM);
		appendStringInfoString(&segwork, fragment_str.data);
		pfree(fragment_str.data);

	}

	return segwork.data;
}

/*
 * Free memory used for the fragments list of a segment
 */
static List* 
free_allocated_frags(List *segment_fragments)
{
	ListCell *frag_cell = NULL;
	
	foreach(frag_cell, segment_fragments)
	{
		AllocatedDataFragment *frag = (AllocatedDataFragment*)lfirst(frag_cell);
		pfree(frag->source_name);
		pfree(frag->host);
		if (frag->fragment_md)
			pfree(frag->fragment_md);
		if (frag->user_data)
			pfree(frag->user_data);
		if (frag->profile)
			pfree(frag->profile);
		pfree(frag);
	}
	list_free(segment_fragments);
	
	return NIL;
}

/*
 * Debug function - print the splits data structure obtained from the namenode
 * response to <GET_BLOCK_LOCATIONS> request
 */
static void 
print_fragment_list(List *fragments)
{
	ListCell *fragment_cell = NULL;
	StringInfoData log_str;
	initStringInfo(&log_str);

	appendStringInfo(&log_str, "Fragment list: (%d elements, pxf_isilon = %s)\n",
			fragments ? fragments->length : 0, pxf_isilon ? "true" : "false");

	foreach(fragment_cell, fragments)
	{
		DataFragment	*frag	= (DataFragment*)lfirst(fragment_cell);
		ListCell		*lc		= NULL;

		appendStringInfo(&log_str, "Fragment index: %d\n", frag->index);

		foreach(lc, frag->replicas)
		{
			FragmentHost* host = (FragmentHost*)lfirst(lc);
			appendStringInfo(&log_str, "replicas: host: %s\n", host->ip);
		}
		appendStringInfo(&log_str, "metadata: %s\n", frag->fragment_md ? frag->fragment_md : "NULL");

		if (frag->user_data)
		{
			appendStringInfo(&log_str, "user data: %s\n", frag->user_data);
		}

		if (frag->profile)
		{
			appendStringInfo(&log_str, "profile: %s\n", frag->profile);
		}
	}

	elog(FRAGDEBUG, "%s", log_str.data);
	pfree(log_str.data);
}

/*
 * release the fragment list
 */
static List* 
free_fragment_list(List *fragments)
{
	ListCell *frag_cell = NULL;

	foreach(frag_cell, fragments)
	{
		DataFragment *fragment = (DataFragment*)lfirst(frag_cell);
		free_fragment(fragment);
	}
	list_free(fragments);
	return NIL;
}

/*
 * Initializes the client context
 */
static void init_client_context(ClientContext *client_context)
{
	client_context->http_headers = NULL;
	client_context->handle = NULL;
	initStringInfo(&(client_context->the_rest_buf));
}

/*
 * The function will use libhdfs API to get a delegation token.
 * The serialized token will be stored in inputData.
 *
 * The function does nothing when Hawq isn't in secure filesystem mode.
 *
 * On regular tables, a delegation token is created when a portal 
 * is created. We cannot use that token because hd_work_mgr.c code gets 
 * executed before a portal is created.
 *
 * The function uses a hdfs uri in the form of hdfs://host:port/path
 * or hdfs://nameservice/path.
 * This value is taken from pg_filespace_entry which is populated
 * based on hawq-site.xml's hawq_dfs_url entry.
 */
static void generate_delegation_token(PxfInputData *inputData)
{
	char* dfs_address = NULL;

	if (!enable_secure_filesystem)
		return;

	get_hdfs_location_from_filespace(&dfs_address);

    elog(DEBUG2, "about to acquire delegation token for %s", dfs_address);

	inputData->token = palloc0(sizeof(PxfHdfsTokenData));

	inputData->token->hdfs_token = HdfsGetDelegationToken(dfs_address,
														  &inputData->token->hdfs_handle);

	if (inputData->token->hdfs_token == NULL)
		elog(ERROR, "Failed to acquire a delegation token for uri %s", dfs_address);

	pfree(dfs_address);
}

/*
 * The function will cancel the delegation token in inputData
 * and free the allocated memory reserved for token details
 *
 * TODO might need to pfree(hdfs_token)
 */
static void cancel_delegation_token(PxfInputData *inputData)
{
	if (inputData->token == NULL)
		return;

	Insist(inputData->token->hdfs_handle != NULL);
	Insist(inputData->token->hdfs_token != NULL);

	HdfsCancelDelegationToken(inputData->token->hdfs_handle, 
							  inputData->token->hdfs_token);

	pfree(inputData->token);
	inputData->token = NULL;
}

/*
 * Fetches metadata for the item from PXF
 * Returns the list of metadata for PXF items
 * Caches data if dboid is not NULL
 *
 */
List *get_pxf_item_metadata(char *profile, char *pattern, Oid dboid)
{
	ClientContext client_context; /* holds the communication info */
	PxfInputData inputData = {0};
	List *objects = NIL;

	/* Define pxf service url address  */
	StringInfoData uri;
	initStringInfo(&uri);
	appendStringInfo(&uri, "%s/", pxf_service_address);
	GPHDUri* hadoop_uri = parseGPHDUriForMetadata(uri.data);
	pfree(uri.data);

	init_client_context(&client_context);
	client_context.http_headers = churl_headers_init();
	/* set HTTP header that guarantees response in JSON format */
	churl_headers_append(client_context.http_headers, REST_HEADER_JSON_RESPONSE, NULL);

	/*
	 * Enrich the curl HTTP header
	 */
	inputData.headers = client_context.http_headers;
	inputData.gphduri = hadoop_uri;
	inputData.rel = NULL; /* We do not supply relation data to the HTTP header */
	inputData.filterstr = NULL; /* We do not supply filter data to the HTTP header */
	generate_delegation_token(&inputData);
	build_http_header(&inputData);

	objects = get_external_metadata(hadoop_uri, profile, pattern, &client_context, dboid);

	freeGPHDUriForMetadata(hadoop_uri);
	cancel_delegation_token(&inputData);
	return objects;
}
