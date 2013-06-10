/*-------------------------------------------------------------------------
 *
 * hd_work_mgr.c
 *	  distributes PXF data fragments for processing between GP segments
 *
 * Copyright (c) 2007-2012, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <curl/curl.h>
#include <json/json.h>
#include "access/pxfuriparser.h"
#include "access/hd_work_mgr.h"
#include "access/libchurl.h"
#include "access/pxfheaders.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/copy.h"
#include "utils/guc.h"
#include "utils/elog.h"

/*
 * One debug level for all log messages from the data allocation algorithm
 */
#define FRAGDEBUG DEBUG2

/*
 * Represents a fragment location replica
 */
typedef struct sFragmentLocation
{
	char *ip;
	int   rest_port;
} FragmentLocation;

/*
 * A DataFragment instance contains the fragment data necessary
 * for the allocation algorithm
 */
typedef struct sDataFragment
{
	int   index; /* index per source name */
	char *source_name;
	List *locations;
	char *user_data;
} DataFragment;

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
	char *user_data; /* additional user data */
} AllocatedDataFragment;

/*
 * Internal Structure which records how many blocks will be processed on each DN.
 * A given block can be replicated on several data nodes. We use this structure
 * in order to achieve a fair distribution of the processing over the data nodes. 
 * Example: Let's say we have 10 blocks, and all 10 blocks are replicated on 2 data
 * nodes. Meaning dn0 has a copy of each one of the the 10 blocks and also dn1 
 * has a copy of each one of the 10 blocks. We wouldn't like to process the whole 
 * 10 blocks only on dn0 or only on dn1. What we would like to achive, is that 
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
	List *segs; /* list of CdbComponentDatabaseInfo* */
} GpHost;

typedef struct sDataNodeRestSrv
{
	char *host;
	int port;
} DataNodeRestSrv;

typedef struct sClientContext
{
	CHURL_HEADERS http_headers;
	CHURL_HANDLE handle;
	char chunk_buf[RAW_BUF_SIZE];	/* part of the HTTP response - received	*/
									/* from one call to churl_read 			*/
	StringInfoData the_rest_buf; 	/* contains the complete HTTP response 	*/
} ClientContext;

static const char *REST_HEADER_JSON_RESPONSE = "Accept: application/json";

/*
 * segwork is the output string that describes the data fragments that were
 * allocated for processing to one segment. It is of the form:
 * "segwork=<size1>@<host1>@<port1>@<sourcename1>@<fragment_index1><size2>@<host2>@@<port2>@<sourcename1><fragment_index2><size3>@...<sizeN>@<hostN>@@<portN>@<sourcenameM><fragment_indexN>"
 * Example: "segwork=32@123.45.78.90@50080@sourcename1@332@123.45.78.90@50080@sourcename1@833@123.45.78.91@50081@sourcename2@1033@123.45.78.91@50081@sourcename2@11"
 */

static const char *SEGWORK_PREFIX = "segwork=";
static const char SEGWORK_IN_PAIR_DELIM = '@';

static List* parse_get_fragments_response(List* fragments, StringInfo rest_buf);
static void free_fragment(DataFragment *fragment);
static List* free_fragment_list(List *fragments);
static List* get_data_fragment_list(GPHDUri *hadoop_uri,  ClientContext* client_context);
static List** distribute_work_2_gp_segments(List *data_fragments_list, int num_segs, int working_segs);
static void  print_fragment_list(List *frags_list);
static char* make_allocation_output_string(List *segment_fragments);
static List* free_allocated_frags(List *segment_fragments);
static List* get_datanode_rest_servers(GPHDUri *hadoop_uri, ClientContext* client_context);
static void assign_rest_ports_to_fragments(ClientContext *client_context, GPHDUri *hadoop_uri, List *fragments);
static List* parse_datanodes_response(List *rest_srvrs, StringInfo rest_buf);
static void free_datanode_rest_servers(List *srvrs);
static void process_request(ClientContext* client_context, char *uri);
static void init_client_context(ClientContext *client_context);
static GPHDUri* init(char* uri, ClientContext* cl_context);
static PxfStatsElem *get_data_statistics(GPHDUri* hadoop_uri, ClientContext *cl_context, StringInfo err_msg);
static PxfStatsElem* parse_get_stats_response(StringInfo rest_buf);
static List* allocate_fragments_to_datanodes(List *whole_data_fragments_list);
static DatanodeProcessingLoad* get_dn_processing_load(List **allDNProcessingLoads, FragmentLocation *fragment_loc);
static void print_data_nodes_allocation(List *allDNProcessingLoads, int total_data_frags);
static List* do_segment_clustering_by_host(void);
static ListCell* pick_random_cell_in_list(List* list);
static void clean_gphosts_list(List *hosts_list);
static AllocatedDataFragment* create_allocated_fragment(DataFragment *fragment);
static bool are_ips_equal(char *ip1, char *ip2);
static char** create_output_strings(List **allocated_fragments, int total_segs);

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
 */
char** map_hddata_2gp_segments(char* uri, int total_segs, int working_segs, Relation relation)
{
	char **segs_work_map = NULL;
	List **segs_data = NULL;
	List *data_fragments = NIL;
	ClientContext client_context; /* holds the communication info */
	PxfInputData inputData;

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
	inputData.filterstr = NULL; /* We do not supply filter data to the HTTP header */	
	build_http_header(&inputData);
	
	/*
	 * 2. Get the fragments data from the PXF service
	 */
	data_fragments = get_data_fragment_list(hadoop_uri, &client_context);

	/*
	 * Get a list of all REST servers and assign the listening port to
	 * each fragment that lives on a matching host. This is a temporary
	 * solution. See function header for more info.
	 */
	assign_rest_ports_to_fragments(&client_context, hadoop_uri, data_fragments);

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
		
	return segs_work_map;
}

/*
 * Fetches statistics of the PXF datasource from the PXF service
 */
PxfStatsElem *get_pxf_statistics(char *uri, Relation rel, StringInfo err_msg)
{
	ClientContext client_context; /* holds the communication info */
	char *analyzer = NULL;
	PxfInputData inputData;
	
	GPHDUri* hadoop_uri = init(uri, &client_context);
	if (!hadoop_uri)
		return NULL;

	/*
	 * Get the statistics info from REST only if analyzer is defined
     */
	if(GPHDUri_get_value_for_opt(hadoop_uri, "analyzer", &analyzer) != 0)
	{
		if (err_msg)
			appendStringInfo(err_msg, "no ANALYZER option in table definition");
		return NULL;
	}
	
	/*
	 * Enrich the curl HTTP header
	 */
	inputData.headers = client_context.http_headers;
	inputData.gphduri = hadoop_uri;
	inputData.rel = rel; 
	inputData.filterstr = NULL; /* We do not supply filter data to the HTTP header */	
	build_http_header(&inputData);
	
	return get_data_statistics(hadoop_uri, &client_context, err_msg);
}

/*
 * Fetch the statistics from the PXF service
 */
static PxfStatsElem *get_data_statistics(GPHDUri* hadoop_uri,
										 ClientContext *cl_context,
										 StringInfo err_msg)
{
	StringInfoData request;	
	initStringInfo(&request);
	
	/* construct the request */
	appendStringInfo(&request, "http://%s:%s/%s/%s/Analyzer/getEstimatedStats?path=%s",
					 hadoop_uri->host,
					 hadoop_uri->port,
					 GPDB_REST_PREFIX,
					 PFX_VERSION,
					 hadoop_uri->data);

	/* send the request. The response will exist in rest_buf.data */
	PG_TRY();
	{
		process_request(cl_context, request.data);
	}
	PG_CATCH();
	{
		/* 
		 * communication problems with PXF service
		 * Statistics for a table can be done as part of an ANALYZE procedure on many tables,
		 * and we don't want to stop because of a communication error. So we catch the exception,
		 * append its error to err_msg, and return a NULL,
		 * which will force the the analyze code to use former calculated values or defaults.
		 */
		if (err_msg)
		{
			char* message = elog_message();
			if (message)
				appendStringInfo(err_msg, "%s", message);
			else
				appendStringInfo(err_msg, "Unknown error");

		}
		/* release error state */
		if (!elog_dismiss(DEBUG5))
			PG_RE_THROW(); /* hope to never get here! */

		return NULL;	
	}
	PG_END_TRY();
	
	/* parse the JSON response and form a fragments list to return */
	return parse_get_stats_response(&(cl_context->the_rest_buf));
}

/*
 * Parse the json response from the PXF Fragmenter.getSize
 */
static PxfStatsElem *parse_get_stats_response(StringInfo rest_buf)
{
	PxfStatsElem* statsElem = (PxfStatsElem*)palloc0(sizeof(PxfStatsElem));
	struct json_object	*whole	= json_tokener_parse(rest_buf->data);
	struct json_object	*head	= json_object_object_get(whole, "PXFDataSourceStats");
				
	/* 0. block size */
	struct json_object *js_block_size = json_object_object_get(head, "blockSize");
	statsElem->blockSize = json_object_get_int(js_block_size);
	
	/* 1. number of blocks */
	struct json_object *js_num_blocks = json_object_object_get(head, "numberOfBlocks");
	statsElem->numBlocks = json_object_get_int(js_num_blocks);
	
	/* 2. number of tuples */
	struct json_object *js_num_tuples = json_object_object_get(head, "numberOfTuples");
	statsElem->numTuples = json_object_get_int(js_num_tuples);
		
	return statsElem;	
}

/*
 * Preliminary uri parsing and curl initializations for the REST communication
 */
static GPHDUri* init(char* uri, ClientContext* cl_context)
{	
	char *fragmenter = NULL;

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
	 * 3. Test that the Fragmenter was specified in the URI
	 */
	if(!GPHDUri_get_value_for_opt(hadoop_uri, "fragmenter", &fragmenter))
	{
		if (!fragmenter)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("No value assigned to the FRAGMENTER option in "
							"the pxf uri: %s", hadoop_uri->uri)));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Missing FRAGMENTER option in the pxf uri: %s",
						hadoop_uri->uri)));
	}

	return hadoop_uri;	
}

/*
 * Wrapper for libchurl
 */
static void process_request(ClientContext* client_context, char *uri)
{
	size_t n = 0;

	print_http_headers(client_context->http_headers);
	client_context->handle = churl_init_download(uri, client_context->http_headers);
	memset(client_context->chunk_buf, 0, RAW_BUF_SIZE);
	resetStringInfo(&(client_context->the_rest_buf));
	
	while ((n = churl_read(client_context->handle, client_context->chunk_buf, sizeof(client_context->chunk_buf))) != 0)
	{
		appendBinaryStringInfo(&(client_context->the_rest_buf), client_context->chunk_buf, n);
		memset(client_context->chunk_buf, 0, RAW_BUF_SIZE);
	}
	
	churl_cleanup(client_context->handle);
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
		CdbComponentDatabaseInfo *seg = NULL;
		
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
		
		/* pick the starting segment on the host randomaly */
		gp_host_cell = pick_random_cell_in_list(gpHosts);
		GpHost* gp_host = (GpHost*)lfirst(gp_host_cell);
		seg_cell = pick_random_cell_in_list(gp_host->segs);
		
		/* We found our working segment. let's remove it from the list so it won't get picked up again*/
		seg = (CdbComponentDatabaseInfo*)lfirst(seg_cell);
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
			
			/* locality logic depends on pxf_enable_locality_optimizations guc */
			if (pxf_enable_locality_optimizations)
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
			
			if (!found_dn) /* there is No datanode found at the head of allDNProcessingLoads, it means we just finished the blocks*/
				break;
			
			/* we have a datanode */
			while ( (block_cell = list_head(found_dn->datanodeBlocks)) )
			{
				AllocatedDataFragment* allocated = (AllocatedDataFragment*)lfirst(block_cell);
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
	int i;
	List *gpHosts = NIL;
	CdbComponentDatabases *cdb = getCdbComponentDatabasesForGangs();
	
	for (i = 0; i < cdb->total_segment_dbs; i++)
	{
		bool group_exist = false;
		ListCell *gp_host_cell = NULL;
		CdbComponentDatabaseInfo *p = &cdb->segment_db_info[i];
		char *candidate_ip = p->hostip;
		
		if (!SEGMENT_IS_ACTIVE_PRIMARY(p))
			continue;
		
		foreach(gp_host_cell, gpHosts)
		{
			GpHost *group = (GpHost*)lfirst(gp_host_cell);
			if (are_ips_equal(candidate_ip, group->ip))
			{
				group->segs = lappend(group->segs, p);
				group_exist = true;
				break;
			}
		}
		
		if (!group_exist)
		{
			GpHost* group = (GpHost*)palloc0(sizeof(GpHost));
			group->ip = candidate_ip;
			group->segs = lappend(group->segs, p);
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
			appendStringInfo(&msg, "file name: %s, split index: %d, dn host: %s, dn port: %d\n", 
							 frag->source_name, frag->index, frag->host, frag->rest_port);
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
	ListCell *fragment_location_cell = NULL;
		
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
		foreach(fragment_location_cell, fragment->locations)
		{
			FragmentLocation *loc = lfirst(fragment_location_cell);
			DatanodeProcessingLoad* loc_dn = get_dn_processing_load(&allDNProcessingLoads, loc);
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
	allocated->user_data = (fragment->user_data) ? pstrdup(fragment->user_data) : NULL;
	return allocated;
}

/* 
 * find out how many fragments are already processed on the data node with this fragment's host_ip and port 
 * If this fragment's datanode is not represented in the allDNProcessingLoads, we will create a DatanodeProcessingLoad
 * structure for this fragment's datanode and, and add this new structure to allDNProcessingLoads
 */
static DatanodeProcessingLoad* get_dn_processing_load(List **allDNProcessingLoads, FragmentLocation *fragment_loc)
{
	ListCell *dn_blocks_cell = NULL;
	DatanodeProcessingLoad *dn_found = NULL;
	
	foreach(dn_blocks_cell, *allDNProcessingLoads)
	{
		DatanodeProcessingLoad *dn_blocks = (DatanodeProcessingLoad*)lfirst(dn_blocks_cell);
		if ( are_ips_equal(dn_blocks->dataNodeIp, fragment_loc->ip) && 
			 (dn_blocks->port == fragment_loc->rest_port) )
		{
			dn_found = dn_blocks;
			break;
		}
	}
	
	if (dn_found == NULL)
	{
		dn_found = palloc0(sizeof(DatanodeProcessingLoad));
		dn_found->datanodeBlocks = NIL;
		dn_found->dataNodeIp = pstrdup(fragment_loc->ip);
		dn_found->port = fragment_loc->rest_port;
		dn_found->num_fragments_read = 0;
		dn_found->num_fragments_residing = 0;
		*allDNProcessingLoads = lappend(*allDNProcessingLoads, dn_found);
	}
		
	return dn_found;
}

/* checks if two ip strings are equal */
static bool
are_ips_equal(char *ip1, char *ip2)
{
	return (strcmp(ip1, ip2) == 0);
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
		if (frag->user_data)
		{
			appendStringInfoChar(&fragment_str, SEGWORK_IN_PAIR_DELIM);
			appendStringInfo(&fragment_str, "%s", frag->user_data);
		}
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
		if (frag->user_data)
			pfree(frag->user_data);
		pfree(frag);
	}
	list_free(segment_fragments);
	
	return NIL;
}

/*
 * parse the response of the PXF Fragments call. An example:
 *
 * Request:
 * 		curl --header "X-GP-FRAGMENTER: HdfsDataFragmenter" "http://goldsa1mac.local:50070/gpdb/v2/Fragmenter/getFragments?path=demo" (demo is a directory)
 *
 * Response (left as a single line purposefully):
 * {"PXFFragments":[{"hosts":["isengoldsa1mac.corp.emc.com","isengoldsa1mac.corp.emc.com","isengoldsa1mac.corp.emc.com"],"sourceName":"text2.csv"},{"hosts":["isengoldsa1mac.corp.emc.com","isengoldsa1mac.corp.emc.com","isengoldsa1mac.corp.emc.com"],"sourceName":"text_data.csv"}]}
 */
static List* 
parse_get_fragments_response(List *fragments, StringInfo rest_buf)
{
	struct json_object	*whole	= json_tokener_parse(rest_buf->data);
	struct json_object	*head	= json_object_object_get(whole, "PXFFragments");
	List* 				ret_frags = fragments;
	int 				length	= json_object_array_length(head);
    char *cur_source_name = NULL;
	int cur_index_count = 0;
	
	/* obtain split information from the block */
	for (int i = 0; i < length; i++)
	{
		struct json_object *js_fragment = json_object_array_get_idx(head, i);
		DataFragment* fragment = (DataFragment*)palloc0(sizeof(DataFragment));
		
		/* 0. source name */
		struct json_object *block_data = json_object_object_get(js_fragment, "sourceName");
		fragment->source_name = pstrdup(json_object_get_string(block_data));

		/* 1. fragment index, incremented per source name */
		if ((cur_source_name==NULL) || (strcmp(cur_source_name, fragment->source_name) != 0))
		{
			elog(FRAGDEBUG, "pxf: parse_get_fragments_response: "
						  "new source name %s, old name %s, init index counter %d",
						  fragment->source_name,
						  cur_source_name ? cur_source_name : "NONE",
						  cur_index_count);

			cur_index_count = 0;

			if (cur_source_name)
				pfree(cur_source_name);
			cur_source_name = pstrdup(fragment->source_name);
		}
		fragment->index = cur_index_count;
		++cur_index_count;

		/* 2. hosts - list of all machines that contain this fragment */
		struct json_object *js_fragment_hosts = json_object_object_get(js_fragment, "hosts");
		int num_hosts = json_object_array_length(js_fragment_hosts);
		
		for (int j = 0; j < num_hosts; j++)
		{
			FragmentLocation* floc = (FragmentLocation*)palloc(sizeof(FragmentLocation));
			struct json_object *loc = json_object_array_get_idx(js_fragment_hosts, j);

			floc->ip = pstrdup(json_object_get_string(loc));
			
			fragment->locations = lappend(fragment->locations, floc);
		}

		/* 3. userdata - additional user information */
		struct json_object *js_user_data = json_object_object_get(js_fragment, "userData");
		if (js_user_data)
			fragment->user_data = pstrdup(json_object_get_string(js_user_data));

		/*
		 * HD-2547:
		 * Ignore fragment if it doesn't contain any host locations,
		 * for example if the file is empty.
		 */
		if (fragment->locations)
			ret_frags = lappend(ret_frags, fragment);
		else
			free_fragment(fragment);

	}
	if (cur_source_name)
		pfree(cur_source_name);
	return ret_frags;	
}

/*
 * Obtain the datanode REST servers host/port data
 */
static List*
parse_datanodes_response(List *rest_srvrs, StringInfo rest_buf)
{
	struct json_object *whole = json_tokener_parse(rest_buf->data);
	struct json_object *nodes = json_object_object_get(whole, "regions");
	int length = json_object_array_length(nodes);
	
	/* obtain host/port data for the REST server running on each HDFS data node */
	for (int i = 0; i < length; i++)
	{
		DataNodeRestSrv* srv = (DataNodeRestSrv*)palloc(sizeof(DataNodeRestSrv));
		
		struct json_object *js_node = json_object_array_get_idx(nodes, i);
		struct json_object *js_host = json_object_object_get(js_node, "host");
		srv->host = pstrdup(json_object_get_string(js_host));
		struct json_object *js_port = json_object_object_get(js_node, "port");
		srv->port = json_object_get_int(js_port);
		
		rest_srvrs = lappend(rest_srvrs, srv);
	}
	
	return rest_srvrs;	
}

/*
 * Get host/port data for the REST servers running on each datanode
 */
static List* 
get_datanode_rest_servers(GPHDUri *hadoop_uri, ClientContext* client_context)
{
	List* rest_srvrs_list = NIL;
	StringInfoData get_srvrs_uri;
	initStringInfo(&get_srvrs_uri);
	
	Assert(hadoop_uri->host != NULL && hadoop_uri->port != NULL);

	appendStringInfo(&get_srvrs_uri, "http://%s:%s/%s/%s/HadoopCluster/getNodesInfo",
											hadoop_uri->host,
											hadoop_uri->port,
											GPDB_REST_PREFIX,
											PFX_VERSION);
	
	process_request(client_context, get_srvrs_uri.data);

	/* 
	 * the curl client finished the communication. The response from
	 * the backend lies at rest_buf->data
	 */
	rest_srvrs_list = parse_datanodes_response(rest_srvrs_list, &(client_context->the_rest_buf));		
	pfree(get_srvrs_uri.data);

	return rest_srvrs_list;	
}

/*
 * find the port of the REST server on a given region host
 */
static int
find_datanode_rest_server_port(List *rest_servers, char *host)
{
	ListCell *lc = NULL;

	foreach(lc, rest_servers)
	{
		DataNodeRestSrv *rest_srv = (DataNodeRestSrv*)lfirst(lc);
		if (are_ips_equal(rest_srv->host, host))
			return rest_srv->port;
	}
	
	/* in case we found nothing */
	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("Couldn't find the REST server on the data-node:  %s", host)));
	return -1;
}

/*
 *    Get a list of all REST servers and assign the listening port to
 *    each fragment that lives on a matching host.
 * NOTE:
 *    This relies on the assumption that PXF services are always
 *    located on DataNode REST servers. In the future this won't be
 *    true, and we'll need to get this info from elsewhere.
 */
static void
assign_rest_ports_to_fragments(ClientContext	*client_context,
							   GPHDUri 			*hadoop_uri,
							   List				*fragments)
{
	List	 *rest_servers = NIL;
	ListCell *frag_c = NULL;

	rest_servers = get_datanode_rest_servers(hadoop_uri, client_context);

	foreach(frag_c, fragments)
	{
		ListCell 		*loc_c 		= NULL;
		DataFragment 	*fragdata	= (DataFragment*)lfirst(frag_c);

		foreach(loc_c, fragdata->locations)
		{
			FragmentLocation *fraglocs = (FragmentLocation*)lfirst(loc_c);

			fraglocs->rest_port = find_datanode_rest_server_port(rest_servers,
																 fraglocs->ip);
		}
	}

	free_datanode_rest_servers(rest_servers);
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

	appendStringInfo(&log_str, "Fragment list: (%d elements)\n", fragments ? fragments->length : 0);

	foreach(fragment_cell, fragments)
	{
		DataFragment 	*frag 	= (DataFragment*)lfirst(fragment_cell);
		ListCell 		*lc 	= NULL;

		appendStringInfo(&log_str, "Fragment index: %d\n", frag->index);

		foreach(lc, frag->locations)
		{
			FragmentLocation* location = (FragmentLocation*)lfirst(lc);
			appendStringInfo(&log_str, "location: host: %s\n", location->ip);
		}
		if (frag->user_data)
		{
			appendStringInfo(&log_str, "user data: %s\n", frag->user_data);
		}
	}

	elog(FRAGDEBUG, "%s", log_str.data);
	pfree(log_str.data);
}

/*
 * release memory of a single fragment
 */
static void free_fragment(DataFragment *fragment)
{
	ListCell *loc_cell = NULL;

	Assert(fragment != NULL);

	if (fragment->source_name)
		pfree(fragment->source_name);

	foreach(loc_cell, fragment->locations)
	{
		FragmentLocation* location = (FragmentLocation*)lfirst(loc_cell);

		if (location->ip)
			pfree(location->ip);
		pfree(location);
	}
	list_free(fragment->locations);

	if (fragment->user_data)
		pfree(fragment->user_data);
	pfree(fragment);
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

static void
free_datanode_rest_servers(List *srvrs)
{
	ListCell *srv_cell = NULL;
	foreach(srv_cell, srvrs)
	{
		DataNodeRestSrv* srv = (DataNodeRestSrv*)lfirst(srv_cell);
		if (srv->host)
			pfree(srv->host);
		pfree(srv);
	}
	list_free(srvrs);
}

/*
 * get_data_fragment_list
 *
 * 1. Request a list of fragments from the PXF Fragmenter class
 * that was specified in the pxf URL.
 *
 * 2. Process the returned list and create a list of DataFragment with it.
 */
static List* 
get_data_fragment_list(GPHDUri *hadoop_uri,
					   ClientContext *client_context)
{
	List *data_fragments = NIL;
	StringInfoData request;
	
	initStringInfo(&request);

	/* construct the request */
	appendStringInfo(&request, "http://%s:%s/%s/%s/Fragmenter/getFragments?path=%s",
								hadoop_uri->host,
								hadoop_uri->port,
								GPDB_REST_PREFIX,
								PFX_VERSION,
								hadoop_uri->data);

	/* send the request. The response will exist in rest_buf.data */
	process_request(client_context, request.data);

	/* parse the JSON response and form a fragments list to return */
	data_fragments = parse_get_fragments_response(data_fragments, &(client_context->the_rest_buf));

	return data_fragments;
}

/*
 * Initializes the client context
 */
static void init_client_context(ClientContext *client_context)
{
	client_context->http_headers = NULL;
	client_context->handle = NULL;
	memset(client_context->chunk_buf, 0, RAW_BUF_SIZE);
	initStringInfo(&(client_context->the_rest_buf));
}
