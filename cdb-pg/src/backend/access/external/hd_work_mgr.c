/*-------------------------------------------------------------------------
 *
 * hd_work_mgr.c
 *	  distributes GPXF data fragments for processing between GP segments
 *
 * Copyright (c) 2007-2012, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "lib/stringinfo.h"
#include <curl/curl.h>
#include <json/json.h>
#include "access/gpxfuriparser.h"
#include "access/hd_work_mgr.h"
#include "access/libchurl.h"
#include "commands/copy.h"
#include "utils/guc.h"
#include "utils/elog.h"

/*
 * The name of the GPXF protocol, exposed to the GPDB engine
 */
const char  *gpxf_protocol_name = "gpxf";

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
static List* free_fragment_list(List *fragments);
static List* get_data_fragment_list(GPHDUri *hadoop_uri,  ClientContext* client_context);
static char** distribute_work_2_gp_segments(List *data_fragments_list, int num_segs);
static void  print_fragment_list(List *frags_list);
static List* allocate_fragments_2_segment(int seg_idx, int num_segs, List *all_frags_list, List* allocated_frags);
static char* make_allocation_output_string(List *segment_fragments);
static List* free_allocated_frags(List *segment_fragments);
static List* allocate_one_frag(int segment_idx, int num_segs, int frags_num, int frag_idx, void *cur_frag, List * allocated_frags);
static List* get_datanode_rest_servers(GPHDUri *hadoop_uri, ClientContext* client_context);
static void assign_rest_ports_to_fragments(ClientContext *client_context, GPHDUri *hadoop_uri, List *fragments);
static List* parse_datanodes_response(List *rest_srvrs, StringInfo rest_buf);
static void free_datanode_rest_servers(List *srvrs);
static void process_request(ClientContext* client_context, char *uri);
static void init_client_context(ClientContext *client_context);
static GPHDUri* init(char* uri, ClientContext* cl_context);
static List *get_data_statistics_list(GPHDUri* hadoop_uri, ClientContext *cl_context);
static List* parse_get_stats_response(List* stats, StringInfo rest_buf);

/*
 * the interface function of the hd_work_mgr module
 * coordinates the work involved in generating the mapping 
 * of the Hadoop data fragments to each GP segment
 * returns a string array, where string i holds the indexes of the
 * data fragments that will be processed by the GP segment i.
 * The data fragments is a term that comes to represent any fragments
 * or a supported data source (e.g, HBase region, HDFS splits, etc).
 */
char** map_hddata_2gp_segments(char* uri, int num_segs)
{
	char **segs_work_map = NULL;
	List *data_fragments = NIL;
	ClientContext client_context; /* holds the communication info */
	
	/*
	 * 1. Cherrypick the data relevant for HADOOP from the input uri and init curl headers
	 */
	GPHDUri* hadoop_uri = init(uri, &client_context);
	if (!hadoop_uri)
		return (char**)NULL;
	
	/*
	 * 2. Get the fragments data from the GPXF service
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
	segs_work_map = distribute_work_2_gp_segments(data_fragments, num_segs);
	
	/*
	 * 4. Release memory
	 */
	free_fragment_list(data_fragments);
	freeGPHDUri(hadoop_uri);
	churl_headers_cleanup(client_context.http_headers);
		
	return segs_work_map;
}

/*
 * Fetches statistics of the GPXF datasource from the GPXF service
 */
List *get_gpxf_statistics(char *uri)
{
	ClientContext client_context; /* holds the communication info */
	
	GPHDUri* hadoop_uri = init(uri, &client_context);
	if (!hadoop_uri)
		return NULL;
	
	return get_data_statistics_list(hadoop_uri, &client_context);
}

/*
 * Fetch the statistics from the GPXF service
 */
static List *get_data_statistics_list(GPHDUri* hadoop_uri, ClientContext *cl_context)
{
	List *data_stats = NIL;
	StringInfoData request;
	
	initStringInfo(&request);
	
	/* construct the request */
	appendStringInfo(&request, "http://%s:%s/%s/%s/Fragmenter/getStats?path=%s",
					 hadoop_uri->host,
					 hadoop_uri->port,
					 GPDB_REST_PREFIX,
					 GPFX_VERSION,
					 hadoop_uri->data);
	
	/* send the request. The response will exist in rest_buf.data */
	PG_TRY();
	{
		process_request(cl_context, request.data);
	}
	PG_CATCH();
	{
		/* 
		 * communication problems with GPXF service 
		 * Statistics for a table can be done as part of an ANALYZE procedure on many tables,
		 * and we don't want to stop because of a communication error. So we catch the exception
		 * and return a NULL, which will force the the analyze code to use defaults
		 */
		return data_stats;	
	}
	PG_END_TRY();
	
	/* parse the JSON response and form a fragments list to return */
	data_stats = parse_get_stats_response(data_stats, &(cl_context->the_rest_buf));
	
	return data_stats;
}

/*
 * Parse the json response from the GPXF Fragmenter.getSize
 */
static List *parse_get_stats_response(List* stats, StringInfo rest_buf)
{
	struct json_object	*whole	= json_tokener_parse(rest_buf->data);
	struct json_object	*head	= json_object_object_get(whole, "GPXFDataSourceStats");
	List* 				ret_stats = stats;
	int 				length	= json_object_array_length(head);
    	
	/* obtain stats information from the element */
	for (int i = 0; i < length; i++)
	{
		struct json_object *js_stats = json_object_array_get_idx(head, i);
		GpxfStatsElem* statsElem = (GpxfStatsElem*)palloc0(sizeof(GpxfStatsElem));
		
		/* 0. block size */
		struct json_object *js_block_size = json_object_object_get(js_stats, "blockSize");
		statsElem->blockSize = json_object_get_int(js_block_size);
		
		/* 1. number of blocks */
		struct json_object *js_num_blocks = json_object_object_get(js_stats, "numberOfBlocks");
		statsElem->numBlocks = json_object_get_int(js_num_blocks);
		
		/* 2. number of tuples */
		struct json_object *js_num_tuples = json_object_object_get(js_stats, "numberOfTuples");
		statsElem->numTuples = json_object_get_int(js_num_tuples);
		
		ret_stats = lappend(ret_stats, statsElem);
	}

	return ret_stats;	
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
	 * 3. Get the fragmenter name from the hadoop uri and invoke it
	 *    to obtain a data fragment list
	 */
	if(!GPHDUri_get_value_for_opt(hadoop_uri, "fragmenter", &fragmenter))
	{
		if (!fragmenter)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("No value assigned to the FRAGMENTER option in "
							"the gpxf uri: %s", hadoop_uri->uri)));
		
		churl_headers_append(cl_context->http_headers, "X-GP-FRAGMENTER",
							 fragmenter);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Missing FRAGMENTER option in the gpxf uri: %s",
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
 * Returns an array of strings where each string in the array represents the fragments
 * allocated to the corresponding GP segment: The string contains the fragment
 * indexes starting with a byte containing the fragment's size. Example of a string that allocates fragments 0, 3, 7
 * 'segwork=<size>data_node_host@0<size>data_node_host@3<size>data_node_host@7'. <segwork=> prefix is required to integrate this string into
 * <CREATE EXTERNAL TABLE> syntax. 
 * In case  a  given GP segment will not be allocated data fragments for processing,
 * we will put a NULL at it's index in the output strings array.
 */
static char** 
distribute_work_2_gp_segments(List *whole_data_fragments_list, int num_segs)
{
	/*
	 * Example of a string representing the data fragments allocated to one segment: 
	 * 'segwork=<size>data_node_host@0-data_node_host@1-data_node_host@5<size>data_node_host@8-data_node_host@9'
	 */
	
	List* allocated_frags = NIL; /* list of the data fragments for one segment */
	char** segs_work_map = (char **)palloc(num_segs * sizeof(char *));
	
	/*
	 * For each segment allocate the data fragments. Each fragment is represented by its residential host
	 * and its index. We first receive the allocated fragments in a list and then make a string out of the
	 * list. This operation is repeated for each segment.
	 */
	for (int i = 0; i < num_segs; ++i)
	{
		allocated_frags = allocate_fragments_2_segment(i, num_segs, whole_data_fragments_list, allocated_frags);
		segs_work_map[i] = make_allocation_output_string(allocated_frags);
		elog(DEBUG2, "allocated for seg %d: %s", i, segs_work_map[i]);
		allocated_frags = free_allocated_frags(allocated_frags);
	}
	
	return segs_work_map;
}

/*
 * The brain of the whole module. All other functions in the hd_work_mgr module are acquiring data and preparing it
 * for this point where we look at the relevant data gathered and decide whether segment_idx will receive the data
 * fragment cur_frag. If the answer is yes than we create an AllocatedDataFragment object and insert it into the
 * allocated_frags list
 */
static List*
allocate_one_frag(int segment_idx, int num_segs,
				  int frags_num/* will be used in the future */,
				  int frag_idx /* general fragments index */, void *cur_frag, List *allocated_frags)
{

	AllocatedDataFragment* allocated = NULL;
	
	DataFragment* split = (DataFragment*)cur_frag;
	/* allocate fragment based on general fragments index */
	if (frag_idx % num_segs != segment_idx)
		return allocated_frags;

	allocated = palloc0(sizeof(AllocatedDataFragment));
	allocated->index = split->index; /* index per source */
	allocated->source_name = pstrdup(split->source_name);
	/* TODO: change allocation by locality */
	allocated->host = pstrdup(((FragmentLocation*)linitial(split->locations))->ip);
	allocated->rest_port = ((FragmentLocation*)linitial(split->locations))->rest_port;
	if (split->user_data)
		allocated->user_data = pstrdup(split->user_data);
	
	allocated_frags = lappend(allocated_frags, allocated);

	return allocated_frags;
}

/*
 * Allocate data fragments to one segment
 */
static List*
allocate_fragments_2_segment(int seg_idx, int num_segs, List *all_frags_list, List* allocated_frags)
{
	int all_frags_num = list_length(all_frags_list);
	int frag_idx = 0;
	ListCell *data_frag_cell = NULL;
	
	/*
	 * This function is called once for each segment, and for each segment we go over all data fragments
	 * and use function allocate_one_frag() to decide whether to allocate a fragment to a segment.
	 * This approach is not very saving on processing time, since every segment will see all data fragments, when
	 * it is obvious that once a data fragment was allocated to segment i, it shouldn't be seen by segment
	 * i + 1. So we have got an optimization opportunity here. It will be exploited once we dive into the
	 * allocation algorithm
	 */
	foreach(data_frag_cell, all_frags_list)
	{
		allocated_frags = allocate_one_frag(seg_idx,
											num_segs,
											all_frags_num,
                                            frag_idx,
											lfirst(data_frag_cell),
											allocated_frags);
		/* The index here is incremented by the general fragment list,
		 * to ensure distribution of fragments over all of the segments. */
		++frag_idx;
	}
				
	return allocated_frags;
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
 * parse the response of the GPXF Fragments call. An example:
 *
 * Request:
 * 		curl --header "X-GP-FRAGMENTER: HdfsDataFragmenter" "http://goldsa1mac.local:50070/gpdb/v2/Fragmenter/getFragments?path=demo" (demo is a directory)
 *
 * Response (left as a single line purposefully):
 * {"GPXFFragments":[{"hosts":["isengoldsa1mac.corp.emc.com","isengoldsa1mac.corp.emc.com","isengoldsa1mac.corp.emc.com"],"sourceName":"text2.csv"},{"hosts":["isengoldsa1mac.corp.emc.com","isengoldsa1mac.corp.emc.com","isengoldsa1mac.corp.emc.com"],"sourceName":"text_data.csv"}]}
 */
static List* 
parse_get_fragments_response(List *fragments, StringInfo rest_buf)
{
	struct json_object	*whole	= json_tokener_parse(rest_buf->data);
	struct json_object	*head	= json_object_object_get(whole, "GPXFFragments");
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
			elog(DEBUG2, "gpxf: parse_get_fragments_response: "
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

		ret_frags = lappend(ret_frags, fragment);
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
											GPFX_VERSION);
	
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
		if (strcmp(rest_srv->host, host) == 0)
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
 *    This relies on the assumption that GPXF services are always
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
print_fragment_list(List *splits)
{
	ListCell *split_cell = NULL;

	foreach(split_cell, splits)
	{
		DataFragment 	*frag 	= (DataFragment*)lfirst(split_cell);
		ListCell 		*lc 	= NULL;

		elog(DEBUG2, "Fragment index: %d\n", frag->index);

		foreach(lc, frag->locations)
		{
			FragmentLocation* location = (FragmentLocation*)lfirst(lc);
			elog(DEBUG2, "location: host: %s\n", location->ip);
		}
		if (frag->user_data)
		{
			elog(DEBUG2, "user data: %s\n", frag->user_data);
		}
	}
}

/*
 * release the fragment data structure
 */
static List* 
free_fragment_list(List *fragments)
{
	ListCell *frag_cell = NULL;

	foreach(frag_cell, fragments)
	{
		DataFragment *fragment = (DataFragment*)lfirst(frag_cell);
		ListCell *loc_cell = NULL;

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
 * 1. Request a list of fragments from the GPXF Fragmenter class
 * that was specified in the gpxf URL.
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
								GPFX_VERSION,
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
