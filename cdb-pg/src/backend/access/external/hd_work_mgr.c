/*-------------------------------------------------------------------------
 *
 * hd_work_mgr.c
 *	  distributes hdfs file splits or hbase table regions for processing 
 *    between GP segments
 *
 * Copyright (c) 2007-2012, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include <curl/curl.h>
#include <json/json.h>
#include "access/hd_work_mgr.h"

/*
 * Represents an HDFS split replica
 */
typedef struct sHdfsSplitReplica
{
	char *ip;
	int  rest_port;
	char *rack;
} HdfsSplitReplica;

/*
 * An HdfsSplit instance contains the split data necessary for the allocation algorithm
 */
typedef struct sHdfsSplit
{
	int   index;
	int   block_size;
	char *block_start; 
	List *replicas_list;
} HdfsSplit;

/*
 * An sHBaseRegion instance contains the hbase region data necessary for the allocation algorithm
 */
typedef struct sHBaseRegion
{
	int   index;
	char *start_key;
	char *end_key;
	char *ip;
	int   rest_port;
} HBaseRegion;

/*
 * Contains the subset of information from the input URI, that is relevant to the Hadoop 
 * target
 */
typedef struct sHadoopUri
{
	bool is_gphdfs; /* true - gphdfs, false - gphbase */
	char *host;
	char *port;
	char *data_path; /* it can be a single HDFS file, an HDFS directory or an HBase table */
} HadoopUri;

/*
 * Contains the information about the allocated data fragment that will be sent to the
 * GP segment
 */
typedef struct sAllocatedDataFragment
{
	int   index;
	char *host; /* ip for hdfs and hostname for hbase */
	int   rest_port;
	
} AllocatedDataFragment;

typedef struct sDataNodeRestSrv
{
	char *host;
	int port;
} DataNodeRestSrv;

static const char *REST_HTTP = "http://";
static const char *REST_NAMENODE = "/webhdfs/v1";
static const char *REST_GETBLOCKLOCS = "?op=GET_BLOCK_LOCATIONS&start=0";
static const char *REST_HEADER_JSON_RESPONSE = "Accept: application/json";
static const char *REST_HBASE_REQUEST_REGIONS = "/regions";
static const char *HADOOP_APP_GPHDFS = "hdfs";
static const char *HADOOP_APP_GPHBASE = "hbase";
static const char *GPDB_REST_PREFIX = "gpdb";
static const char *GPDB_REST_CLUSTER_QUERIES = "hadoopCluster";
static const char *GPDB_REST_GET_NODES = "getNodesInfo";
static const char *GPDB_GLOB_STATUS = "/gpdb/GLOBSTATUS?path=";

/*
 * segwork is the output string that describes the data fragments that were allocated for processing 
 * to one segment. It is of the form:
 * "segwork=<host1>@<fragment_index1>+<host2>@<fragment_index2>-...+<hostN>@<fragment_indexN>"
 * Example: "segwork=123.45.78.90@3-123.45.78.90@8-123.45.78.91@10-123.45.78.91@11"
 */
static const char *SEGWORK_PREFIX = "segwork=";
static const char SEGWORK_IN_PAIR_DELIM = '@';
static const char SEGWORK_BETWEEN_PAIRS_DELIM = '+';

static CURL* init_curl(struct curl_slist **httpheader, StringInfo rest_buf);
static CURL* clean_curl(CURL* handle, struct curl_slist *httpheader);
static size_t write_callback(char *buffer, size_t size, size_t nitems, void *userp);
static List* parse_namenode_blocklocs_response(List* splits, int *splits_counter, StringInfo rest_buf);
static List* parse_namenode_globstat_response(List* files, StringInfo rest_buf);
static List* parse_hmaster_response(CURL *handle, HadoopUri *hadoop_uri, List* regions, 
									int *regions_counter, StringInfo rest_buf);
static char* get_ip_from_hbase_location(char *location);
static void print_hdfs_fragments(List *splits);
static List* free_hdfs_fragments(List *fragments);
static List* free_hbase_fragments(List *fragments);
static HadoopUri* make_hadoop_uri(char *in_uri);
static void free_hadoop_uri(HadoopUri *hadoop_uri);
static List* get_rest_uris_list(CURL *handle, HadoopUri *hadoop_uri, StringInfo rest_buf);
static List* get_files_list(CURL *handle, HadoopUri *hadoop_uri, StringInfo rest_buf);
static void make_globstatus_uri(StringInfo ls_uri, HadoopUri *hadoop_uri);
static char** distribute_work_2_gp_segments(bool hdfs, List *data_fragments_list, int num_segs);
static List* fill_fragments_list(CURL* handle, HadoopUri *hadoop_uri , List *frags_list, int *frags_counter, StringInfo rest_buf);
static List* free_fragments_list(List *frags_list, bool hdfs);
static void  print_fragments_list(List *frags_list, bool hfds);
static void  print_hbase_fragments(List *regions);
static void validate_in_format(char *token, char *whole_input);
static List* allocate_fragments_2_segment(int seg_idx, int num_segs, bool hdfs, List *all_frags_list, List* allocated_frags);
static char* make_allocation_output_string(List *segment_fragments);
static List* free_allocated_frags(List *segment_fragments);
static List* allocate_one_frag(int segment_idx, int num_segs, bool hdfs, int frags_num, void *cur_frag, List * allocated_frags);
static bool is_gphdfs(char *hadoop_target_app);
static List* get_datanode_rest_servers(CURL *handle, HadoopUri *hadoop_uri, StringInfo rest_buf);
static void make_srvrs_uri(StringInfo uri, HadoopUri *hadoop_uri);
static int find_datanode_rest_server_port(List *srvrs, char *srv_host);
static List* parse_datanodes_response(List *rest_srvrs, StringInfo rest_buf);
static void free_datanode_rest_servers(List *srvrs);
static char* get_http_error_msg(long http_ret_code, char* msg);
static void send_message(CURL *handle, StringInfo rest_buf);

/*
 * the interface function of the hd_work_mgr module
 * coordinates the work involved in generating the mapping 
 * of the Hadoop data fragments to each GP segment
 * returns a string array, where string i holds the indexes of the
 * Hadoop data fragments that will be processed by the GP segment i.
 * The Hadoop data fragments is a term that comes to represent both 
 * the HDFS file splits and the HBase table regions.
 */
char** map_hddata_2gp_segments(char* uri, int num_segs)
{
	int  frags_counter            = 0;
	char **segs_work_map          = NULL;
	List* data_fragments          = NIL;
	List* uris                    = NIL; /* This is the list webhdfs URIs obtanied from the hadoop_uri. Used for both hdfs and hbase */
	ListCell         *uri_cell    = NULL;
	struct curl_slist *httpheader = NULL;
	CURL *curl_handle      = NULL;
	StringInfoData the_rest_buf;
	
	/*
	 * 1. Cherrypick the data relevant for HADOP from the input uri
	 */
	HadoopUri* hadoop_uri      = make_hadoop_uri(uri);
	
	/*
	 * 2. Communication with the Hadoop back-end
	 *    Initialize curl client and the buffer that will receive the data from
	 *    the curl client call-back
	 */
	initStringInfo(&the_rest_buf);
	if ((curl_handle = init_curl(&httpheader, &the_rest_buf)) == NULL)
		return (char**)NULL;
	
	/*
	 * 3. Obtain a list of webhdfs uris, based on the input URI.
	 *    Each webhdfs uri will generate a request to the HADOOP backend
	 *    that will send in return the coresponding data fragments.
	 */
	uris = get_rest_uris_list(curl_handle, hadoop_uri, &the_rest_buf);
	
	/*
	 * 4. For each  webhdfs uri send a request for data fragments to the HADOOP backend.
	 *    HADOOP backend: namenode for HDFS file or HMaster for HBase table
	 *    Parse the backend response and generate an application data fragment that contains
	 *    the necessary info required by the allocation algorithm to make its decision
	 *    The application data fragments are accumulated into the data_fragments list
	 */
	foreach(uri_cell, uris)
	{
		long http_ret = 0;
		char *str_uri = (char*)lfirst(uri_cell);
		curl_easy_setopt(curl_handle, CURLOPT_URL, str_uri);
		send_message(curl_handle, &the_rest_buf);
		
		curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &http_ret);
		if (http_ret != 200)
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("%s", get_http_error_msg(http_ret, the_rest_buf.data) )));
		/* 
		 * the curl client finished the communication with hadoop backend (namenode or HMaster)
		 * now the response from the backend lies at rest_buf.data
		 */		
		data_fragments = fill_fragments_list(curl_handle, hadoop_uri, data_fragments, &frags_counter, &the_rest_buf);
	}
	
	/*
	 * debug - enable when tracing 
	 */
	print_fragments_list(data_fragments, hadoop_uri->is_gphdfs);

	/*
	 * 5. Finally, call  the actual work allocation algorithm	 
	 */
	segs_work_map = distribute_work_2_gp_segments(hadoop_uri->is_gphdfs, data_fragments, num_segs);
	
	/*
	 * 6. Release memory
	 */
	free_fragments_list(data_fragments, hadoop_uri->is_gphdfs);
	free_hadoop_uri(hadoop_uri);
	
	/*
	 * 7. Release curl structures
	 */
	clean_curl(curl_handle, httpheader);
		
	return segs_work_map;
}

/*
 * Wrapper for curl_easy_perform
 */
static void send_message(CURL *handle, StringInfo rest_buf)
{
	resetStringInfo(rest_buf);
	curl_easy_perform(handle);
}

/*
 * Calls the actual fill method depending on the target application:
 * hadoop/hbase
 */
static List* fill_fragments_list(CURL *handle, HadoopUri *hadoop_uri, List *frags_list, int *frgs_counter, StringInfo rest_buf)
{	
	if (hadoop_uri->is_gphdfs) /* gphdfs */
		frags_list = parse_namenode_blocklocs_response(frags_list, frgs_counter, rest_buf);
	else /* gphbase */ 
		frags_list = parse_hmaster_response(handle, hadoop_uri, frags_list, frgs_counter, rest_buf);
	
	return frags_list;
}

/*
 * Calls the actual free list method depending on the target application:
 * hadoop/hbase
 */
static List*  free_fragments_list(List *frags_list, bool hdfs)
{
	List* ret = NIL;
	if (hdfs) /* gphdfs */
		ret = free_hdfs_fragments(frags_list);
	else /* gphbase */
		ret = free_hbase_fragments(frags_list);
	
	return ret;
}

/*
 * Calls the actual print list method depending on the target application:
 * hadoop/hbase
 */
static void  print_fragments_list(List *frags_list, bool hdfs)
{
	if (hdfs) /* gphdfs */
		print_hdfs_fragments(frags_list);
	else /* gphbase */
		print_hbase_fragments(frags_list);		
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
 * by each gp segment. An Hadoop data fragment is a file split for an HDFS file
 * or a table region for an HBase table
 * Returns an array of strings where each string in the array represents the fragments
 * allocated to the coresponding GP segment: The string contains the fragment 
 * indexes delimited by '-'. Example of a string that allocates fragments 0, 3, 7
 * 'segwork=data_node_host@0-data_node_host@3-data_node_host@7'. <segwork=> prefix is required to integrate this string into
 * <CREATE EXTERNAL TABLE> syntax. 
 * In case  a  given GP segment will not be allocated data fragments for processing,
 * we will put a NULL at it's index in the output strings array.
 */
static char** 
distribute_work_2_gp_segments(bool hdfs, List *whole_data_fragments_list, int num_segs)
{
	/*
	 * Example of a string representing the data fragments allocated to one segment: 
	 * 'segwork=data_node_host@0-data_node_host@1-data_node_host@5-data_node_host@8-data_node_host@9'
	 */
	
	List* allocated_frags = NIL; /* list of the data fragments for one segment */
	char** segs_work_map = (char **)palloc(num_segs * sizeof(char *));
	
	/*
	 * For each segment allocate the data fragments. Each fragment is represented by its residential host
	 * and its index. We first receive the allocated fragments in a list and then make a string out of the
	 * list. This operation is repeated for each segment.
	 */
	for (int i = 0; i < num_segs; i++)
	{
		allocated_frags = allocate_fragments_2_segment(i, num_segs, hdfs, whole_data_fragments_list, allocated_frags);
		segs_work_map[i] = make_allocation_output_string(allocated_frags);
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
allocate_one_frag(int segment_idx, int num_segs, bool hdfs,
				  int frags_num/* will be used in the future */, void *cur_frag, List *allocated_frags)
{
	
	AllocatedDataFragment* allocated = NULL; 
	
	if (hdfs) /* gphdfs */
	{
		HdfsSplit* split = (HdfsSplit*)cur_frag;
		if (split->index%num_segs != segment_idx)
			return allocated_frags;

		allocated = palloc(sizeof(AllocatedDataFragment));
		allocated->index = split->index;
		allocated->host = pstrdup(((HdfsSplitReplica*)linitial(split->replicas_list))->ip);
		allocated->rest_port = ((HdfsSplitReplica*)linitial(split->replicas_list))->rest_port;
	}
	else /* hbase */
	{
		HBaseRegion* region = (HBaseRegion*)cur_frag;
		
		if (region->index%num_segs != segment_idx)
			return allocated_frags;
		
		allocated = palloc(sizeof(AllocatedDataFragment));
		allocated->index = region->index;
		allocated->host = pstrdup(region->ip);
		allocated->rest_port =  region->rest_port;
	}
	
	allocated_frags = lappend(allocated_frags, allocated);
	return allocated_frags;
}

/*
 * Allocate data fragments to one segment
 */
static List*
allocate_fragments_2_segment(int seg_idx, int num_segs, bool hdfs, List *all_frags_list, List* allocated_frags)
{
	int all_frags_num = list_length(all_frags_list);
	ListCell *data_frag_cell = NULL;
	
	/*
	 * This function is called once for each segment, and for each segment we go over all data fragments
	 * and use function allocate_one_frag() to decide whether to allocate a fragment to a segment.
	 * This aproach is not very saving on processing time, since every segment will see all data fragments, when
	 * it is obvious that once a data fragment was allocated to segment i, it shouldn't be seen by segment
	 * i + 1. So we have got an optimization oportunity here. It will be exploited once we dive into the
	 * allocation agorithm
	 */
	foreach(data_frag_cell, all_frags_list)
	{
		allocated_frags = allocate_one_frag(seg_idx, num_segs, hdfs, 
											all_frags_num, lfirst(data_frag_cell), allocated_frags);
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
	ListCell *last_cell = list_tail(segment_fragments);
	StringInfoData segwork;
	
	initStringInfo(&segwork);
	appendStringInfoString(&segwork, SEGWORK_PREFIX);
	
	foreach(frag_cell, segment_fragments)
	{
		AllocatedDataFragment *frag = (AllocatedDataFragment*)lfirst(frag_cell);
		appendStringInfoString(&segwork, frag->host);
		appendStringInfoChar(&segwork, SEGWORK_IN_PAIR_DELIM);
		appendStringInfo(&segwork, "%d", frag->rest_port);
		appendStringInfoChar(&segwork, SEGWORK_IN_PAIR_DELIM);
		appendStringInfo(&segwork, "%d", frag->index);
		if (frag_cell != last_cell)
			appendStringInfoChar(&segwork, SEGWORK_BETWEEN_PAIRS_DELIM);
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
		pfree(frag->host);
		pfree(frag);
	}
	list_free(segment_fragments);
	
	return NIL;
}

/*
 * Initialize the curl library which is used for doing REST client operations
 * against the webhdfs serves implemented by both the NameNode and the HMaster
 */
static CURL* init_curl(struct curl_slist **httpheader, StringInfo rest_buf)
{	
	CURL* handle = curl_easy_init();
	if (handle == NULL)
		return NULL;
	
	/* set URL to get */ 
	if (curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, write_callback) != CURLE_OK)
		return clean_curl(handle, *httpheader);
	
	/* set HTTP header that guarantees response in JSON format */
	*httpheader = curl_slist_append((struct curl_slist*)NULL, REST_HEADER_JSON_RESPONSE);
	if (curl_easy_setopt(handle, CURLOPT_HTTPHEADER, *httpheader) != CURLE_OK)
		return clean_curl(handle, *httpheader);
	
	/* transfer the data buffer to the CURL callback */
	if (curl_easy_setopt(handle, CURLOPT_WRITEDATA, rest_buf) != CURLE_OK)
		return clean_curl(handle, *httpheader);
	
	return handle;
}

/*
 * Release the curl library
 */
static CURL* clean_curl(CURL* handle, struct curl_slist *httpheader)
{
	if (httpheader)
		curl_slist_free_all(httpheader);
	
	if (handle)
		curl_easy_cleanup(handle);
	
	return NULL;
}

/*
 * write_callback
 * gets the data from the URL server and writes it into the URL buffer
 */
static size_t
write_callback(char *curl_buffer,
               size_t size,
               size_t nitems,
               void *stream)
{
	const int 	nbytes = size * nitems;
	StringInfo gp_buffer = (StringInfo)stream;
	appendBinaryStringInfo(gp_buffer, curl_buffer, nbytes);	
	return nbytes;
}

/*
 * parse the response of the GET_BLOCK_LOCATION call on the namenode
 */
static List* 
parse_namenode_blocklocs_response(List *splits, int *splits_counter, StringInfo rest_buf)
{
	List* ret_splits = splits;
	struct json_object *whole = json_tokener_parse(rest_buf->data);
	struct json_object *head = json_object_object_get(whole, "LocatedBlocks");
	struct json_object *blocks = json_object_object_get(head, "locatedBlocks");
	int length = json_object_array_length(blocks);
	
	/* obtain split information from the block */
	for (int i = 0; i < length; i++)
	{
		struct json_object *block = json_object_array_get_idx(blocks, i);
		HdfsSplit* split = (HdfsSplit*)palloc0(sizeof(HdfsSplit));
		
		/* 0. split index */
		split->index = (*splits_counter)++;
		
		/* 1. block size - same as split size. We set the split size equal to block size on the Java side */
		struct json_object *block_data = json_object_object_get(block, "block");
		struct json_object *js_block_size = json_object_object_get(block_data, "numBytes");
		split->block_size = json_object_get_int(js_block_size);
		
		/* 2. block start (beginning of the block in the file) */
		struct json_object *js_block_start = json_object_object_get(block, "startOffset");
		split->block_start = pstrdup(json_object_to_json_string(js_block_start));
		
		/* 3. replicas - get the list of all machines that contain a replica of this block/split */
		struct json_object *js_block_locations = json_object_object_get(block, "locations");
		int locs_length = json_object_array_length(js_block_locations);
		for (int j = 0; j < locs_length; j++)
		{
			HdfsSplitReplica* replica = (HdfsSplitReplica*)palloc(sizeof(HdfsSplitReplica));
			struct json_object *loc = json_object_array_get_idx(js_block_locations, j);
			
			struct json_object *js_host = json_object_object_get(loc, "hostName");
			replica->ip = pstrdup(json_object_get_string(js_host));
			
			struct json_object *js_port = json_object_object_get(loc, "infoPort");
			replica->rest_port = json_object_get_int(js_port);

			struct json_object *js_location_rack = json_object_object_get(loc, "networkLocation");
			replica->rack = pstrdup(json_object_get_string(js_location_rack));
			
			split->replicas_list = lappend(split->replicas_list, replica);			
		}
		ret_splits = lappend(ret_splits, split);
	}
	return ret_splits;
}

/*
 * parse the response of the GLOBSTATUS call on the namenode
 */
static List* 
parse_namenode_globstat_response(List* files, StringInfo rest_buf)
{
	List* ret_files = files;
	struct json_object *whole = json_tokener_parse(rest_buf->data);
	struct json_object *stats = json_object_object_get(whole, "files");
	int length = json_object_array_length(stats);
	
	/* obtain the file  information from the FileStatus json object */
	for (int i = 0; i < length; i++)
	{
		StringInfoData full_path;
		initStringInfo(&full_path);
		
		struct json_object *stf = json_object_array_get_idx(stats, i);
		struct json_object *js_file = json_object_object_get(stf, "name");
		char *file = json_object_get_string(js_file);
		appendStringInfoString(&full_path, file);
		ret_files = lappend(ret_files, full_path.data);
	}
	
	return ret_files;
}

/*
 * Compile the uri for the rest_region_servers info request 
 * The request is sent to thr HBase REST server
 */
static void
make_srvrs_uri(StringInfo uri, HadoopUri *hadoop_uri)
{
	Assert(hadoop_uri->host != NULL && hadoop_uri->port != NULL);
	
	appendStringInfoString(uri, REST_HTTP);
	appendStringInfoString(uri, hadoop_uri->host);
	appendStringInfoString(uri, ":");
	appendStringInfoString(uri, hadoop_uri->port);
	appendStringInfoString(uri, "/");
	appendStringInfoString(uri, GPDB_REST_PREFIX);
	appendStringInfoString(uri, "/");
	appendStringInfoString(uri, GPDB_REST_CLUSTER_QUERIES);
	appendStringInfoString(uri, "/");
	appendStringInfoString(uri, GPDB_REST_GET_NODES);
}

/*
 * Obtain the REST servers host/port data from the HMaster response 
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
 * Get host/port data for the HBASE REST servelet installed on each 
 * region server
 */
static List* 
get_datanode_rest_servers(CURL *handle, HadoopUri *hadoop_uri, StringInfo rest_buf)
{
	long http_ret = 0;
	List* rest_srvrs_list = NIL;
	StringInfoData get_srvrs_uri;
	initStringInfo(&get_srvrs_uri);
	
	make_srvrs_uri(&get_srvrs_uri, hadoop_uri);
	curl_easy_setopt(handle, CURLOPT_URL, get_srvrs_uri.data);
	send_message(handle, rest_buf);
	
	curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &http_ret);
	if (http_ret != 200)
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("%s", get_http_error_msg(http_ret, rest_buf->data) )));	
	/* 
	 * the curl client finished the communication with HBase stargate REST server 
	 * now the response from the backend lies at rest_buf->data
	 */
	rest_srvrs_list = parse_datanodes_response(rest_srvrs_list, rest_buf);		
	pfree(get_srvrs_uri.data);	
	return rest_srvrs_list;	
}

/*
 * find the port of the REST server on a given region host
 */
int find_datanode_rest_server_port(List *srvrs, char *srv_host)
{
	ListCell *srv_cell = NULL;
	foreach(srv_cell, srvrs)
	{
		DataNodeRestSrv* srv = (DataNodeRestSrv*)lfirst(srv_cell);
		if (strcmp(srv->host, srv_host) == 0)
			return srv->port;
	}
	
	/* in case we found nothing */
	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("Couldn't find the REST server on the data-node:  %s", srv_host)));
	return -1;
}

/*
 * parse the response of the <regions> call on the hmaster
 */
static List* 
parse_hmaster_response(CURL* handle, HadoopUri *hadoop_uri, List* regions, 
					   int *regions_counter, StringInfo rest_buf)
{
	/*
	 * The HMaster access code that brings the regions data
	 * and populates the regions list
	 */
	List* ret_regions = regions;
	struct json_object *whole = json_tokener_parse(rest_buf->data);
	struct json_object *j_regions = json_object_object_get(whole, "Region");
	int length = json_object_array_length(j_regions);
	
	/*
	 * Get host/port data for the HBASE REST servelet installed on each 
	 * region server
	 */
	List* datanode_rest_servers = get_datanode_rest_servers(handle, hadoop_uri, rest_buf);
	
	/* obtain region information from the j_reg */
	for (int i = 0; i < length; i++)
	{
		struct json_object *j_obj = NULL;
		struct json_object *j_reg = json_object_array_get_idx(j_regions, i);
		HBaseRegion* reg = (HBaseRegion*)palloc0(sizeof(HBaseRegion));
		
		/* 1. Region index */
		reg->index = (*regions_counter)++;
		
		/* 2. Region start key */
		j_obj = json_object_object_get(j_reg, "startKey");
		reg->start_key = pstrdup(json_object_get_string(j_obj));
		
		/* 3. Region end key */
		j_obj = json_object_object_get(j_reg, "endKey");
		reg->end_key = pstrdup(json_object_get_string(j_obj));
		
		/* 4. HRegion server ip */
		j_obj = json_object_object_get(j_reg, "location");
		reg->ip = get_ip_from_hbase_location(json_object_get_string(j_obj));
		
		/* 5. PORT of the HDFS REST server located on the same host as the regionserver */
		reg->rest_port = find_datanode_rest_server_port(datanode_rest_servers, reg->ip);
		
		ret_regions = lappend(ret_regions, reg);
	}
	
	free_datanode_rest_servers(datanode_rest_servers);
	return ret_regions;
}

/*
 * get the ip from hbase location field where location is of form (host:port):
 * 10.76.72.73:60020
 */
static char*
get_ip_from_hbase_location(char *location)
{
	char *hregion_ip = NULL;
	char *end = strchr(location, ':');
	
	validate_in_format(end, location);
	hregion_ip = pnstrdup(location, end - location);
	return hregion_ip;
}

/*
 * Debug function - print the splits data structure obtained from the namenode
 * response to <GET_BLOCK_LOCATIONS> request
 */
static void 
print_hdfs_fragments(List *splits)
{
	ListCell *split_cell = NULL;
	foreach(split_cell, splits)
	{
		HdfsSplit *split = (HdfsSplit*)lfirst(split_cell);
		elog(DEBUG2, "split index: %d, split start: %s, split size: %d\n", split->index, split->block_start, split->block_size);
		ListCell *replica_cell = NULL;
		foreach(replica_cell, split->replicas_list)
		{
			HdfsSplitReplica* replica = (HdfsSplitReplica*)lfirst(replica_cell);
			elog(DEBUG2, "replica host: %s, replica rack: %s\n", replica->ip, replica->rack);
		}
	}
}

/*
 * Debug function - print the regions data structure obtained from the hmaster
 * response to <regions> request
 */
static void 
print_hbase_fragments(List *regions)
{
	
	ListCell *reg_cell = NULL;
	foreach(reg_cell, regions)
	{
		HBaseRegion *reg = (HBaseRegion*)lfirst(reg_cell);
		elog(DEBUG2, "region index: %d, start key: %s, end key: %s, hregion ip: %s\n", 
				                          reg->index, reg->start_key, reg->end_key, reg->ip);
	}
}	

/*
 * release the splits data structure
 */
static List* 
free_hdfs_fragments(List *splits)
{
	ListCell *split_cell = NULL;
	foreach(split_cell, splits)
	{
		HdfsSplit *split = (HdfsSplit*)lfirst(split_cell);
		if (split->block_start)
			pfree(split->block_start);
		ListCell *replica_cell = NULL;
		foreach(replica_cell, split->replicas_list)
		{
			HdfsSplitReplica* replica = (HdfsSplitReplica*)lfirst(replica_cell);
			if (replica->ip)
				pfree(replica->ip); 
			if (replica->rack)
				pfree(replica->rack);
			pfree(replica);
		}
		list_free(split->replicas_list);
		pfree(split);
	}
	list_free(splits);
	return NIL;
}

/*
 * release the regions data structure
 */
static List* 
free_hbase_fragments(List *regions)
{
	ListCell *reg_cell = NULL;
	foreach(reg_cell, regions)
	{
		HBaseRegion *reg = (HBaseRegion*)lfirst(reg_cell);
		if (reg->start_key)
			pfree(reg->start_key);
		if (reg->end_key)
			pfree(reg->end_key);
		if (reg->ip)
			pfree(reg->ip);
		
		pfree(reg);
	}
	list_free(regions);	
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
 * Extracts the Hadoop relevant data from in_uri and populates HadoopUri fields
 */
static HadoopUri* 
make_hadoop_uri(char *in_uri)
{
	/*
	 * Example of URI parsed:
	 * 1. hdfs file:
	 *    gpfusion://localhost:8020/hdfs/directory/path/file_name?accessor=SequenceFileAccessor&resolver=WritableResolver&schema=CustomWritable
	 * 2. hbase table (table name - play):
	 *    gpfusion://localhost:8080/hbase/play
	 */
	
	char *start = NULL;
	char *end   = NULL;
	char *target_app = NULL;
	HadoopUri* hadoop_uri = (HadoopUri*)palloc(sizeof(HadoopUri));
	hadoop_uri->host = hadoop_uri->data_path = hadoop_uri->port = NULL;
	
	/* 1. skip protocol (gpfusion://) */
	start = in_uri;	
	end = strchr(start, ':');
	validate_in_format(end, in_uri);
	end += 3; /* skip :// */
	
	/* 2. host: name of the host machine on which the NameNode or the HMaseter is running */
	start = end;
	end = strchr(start, ':'); /* don't need the port, just the hostname */
	validate_in_format(end, in_uri);
	hadoop_uri->host = pnstrdup(start, end - start);
	
	/* 3. REST port */
	start = end;
	start++;
	end = strchr(start, '/');
	validate_in_format(end, in_uri);
	hadoop_uri->port = pnstrdup(start, end - start);

	/* 4. target app, hdfs or hbase */
	start = end;
	start++;
	end = strchr(start, '/');
	validate_in_format(end, in_uri);
	target_app = pnstrdup(start, end - start);
	hadoop_uri->is_gphdfs = is_gphdfs(target_app);
	pfree(target_app);
	
	
	if (hadoop_uri->is_gphdfs) /* gphdfs */
	{		
		/* 4. data_path - the file path or the directory path inside in_uri */
		start = end;
		validate_in_format(start, in_uri);
		end = strchr(start, '?');
		if (end == NULL) /* here we permit the input string not to have the pattern '?' - it means no options */
			hadoop_uri->data_path = pstrdup(start);
		else
			hadoop_uri->data_path = pnstrdup(start, end - start);
	}
	else /* gphbase */
	{
		/* 4. data_path - the name of the hbase table */
		start = end;
		start++; /* beginning of table name */
		hadoop_uri->data_path = pstrdup(start);
	}
	
	return hadoop_uri;
}

/*
 * Frees HadoopUri memory
 */
static void 
free_hadoop_uri(HadoopUri *hadoop_uri)
{
	if (hadoop_uri == NULL)
		return;
	
	if (hadoop_uri->host)
		pfree(hadoop_uri->host);
	if (hadoop_uri->data_path)
		pfree(hadoop_uri->data_path);
		
	pfree(hadoop_uri);
}

/*
 * Compiles the list of webhdfs URIs from the hadoop_uri. Each URI in the webhdfs list
 * will generate one request to the Hadoop backend
 */
static List* 
get_rest_uris_list(CURL *handle, HadoopUri *hadoop_uri, StringInfo rest_buf)
{
	List *uris_list = NIL;
	StringInfoData uri;
	
	if (hadoop_uri->is_gphdfs) /* gphdfs */
	{
		ListCell *file_cell = NULL;
		List * files_list = get_files_list(handle, hadoop_uri, rest_buf);
		foreach(file_cell, files_list)
		{
			char *file = (char*)lfirst(file_cell);
			
			initStringInfo(&uri);
			appendStringInfoString(&uri, REST_HTTP);
			appendStringInfoString(&uri, hadoop_uri->host);
			appendStringInfoString(&uri, ":");
			appendStringInfoString(&uri, hadoop_uri->port);			
			appendStringInfoString(&uri, REST_NAMENODE);
			appendStringInfoString(&uri, file);
			appendStringInfoString(&uri, REST_GETBLOCKLOCS);
			
			uris_list = lappend(uris_list, uri.data);
			pfree(file);
		}
		
		/* free files list */
		list_free(files_list);
	}
	else /* gphbase */ 
	{
		initStringInfo(&uri);
		
		appendStringInfoString(&uri, REST_HTTP);
		appendStringInfoString(&uri, hadoop_uri->host);
		appendStringInfoString(&uri, ":");
		appendStringInfoString(&uri, hadoop_uri->port);
		appendStringInfoString(&uri, "/");
		appendStringInfoString(&uri, hadoop_uri->data_path);
		appendStringInfoString(&uri, REST_HBASE_REQUEST_REGIONS);
		
		uris_list = lappend(uris_list, uri.data);
	}
	
	return uris_list;
}

/*
 * Extracts the error message from the full HTTP response
 * We test for several conditions in the http_ret_code and the HTTP response message.
 * The first condition that matches, defines the final message string and ends the function.
 * The layout of the HTTP response message is:
 
 <html>
 <head>
 <meta meta_data_attributes />
 <title> title_content_which_has_a_brief_description_of_the_error </title>
 </head>
 <body>
 <h2> heading_containing_the_error_code </h2>
 <p> 
 main_body_paragraph_with_a_detailed_description_of_the_error_on_the_rest_server_side
 <pre> the_error_in_original_format_not_HTML_ususally_the_title_of_the_java_exception</pre>
 </p>
 <h3>Caused by:</h3>
 <pre>
 the_full_java_exception_with_the_stack_output
 </pre>
 <hr /><i><small>Powered by Jetty://</small></i>
 <br/>                                               
 <br/>                                               								 
 </body>
 </html> 
 
 * Our first priority is to get the paragraph <p> inside <body>, and in case we don't find it, then we try to get
 * the <title>.
 */
static char*
get_http_error_msg(long http_ret_code, char* msg)
{
	char *start, *end, *ret;
	StringInfoData errMsg;
	initStringInfo(&errMsg);
	
	/* 
	 * 1. The server not listening on the port specified in the <create external...> statement" 
	 *    In this case there is no Response from the server, so we issue our own message
	 */
	if (http_ret_code == 0) 
		return "There is no gpfusion servlet listening on the port specified in the external table url";
	
	/*
	 * 2. There is a response from the server since the http_ret_code is not 0, but there is no response message.
	 *    This is an abnormal situation that could be the result of a bug, libraries incompatibility or versioning issue
	 *    in the Rest server or our curl client. In this case we again issue our own message. 
	 */
	if (!msg || (msg && strlen(msg) == 0) )
	{
		appendStringInfo(&errMsg, "HTTP status code is %ld but HTTP response string is empty", http_ret_code);
		ret = pstrdup(errMsg.data);
		pfree(errMsg.data);
		return ret;
	}
	
	/*
	 * 3. The "normal" case - There is an HTTP response and the response has a <body> section inside where 
	 *    there is a paragraph contained by the <p> tag.
	 */
	start =  strstr(msg, "<body>");
	if (start != NULL)
	{
		start = strstr(start, "<p>");
		if (start != NULL)
		{
			char *tmp;
			bool skip = false;
			
			start += 3;
			end = strstr(start, "</p>");	/* assuming where is a <p>, there is a </p> */
			if (end != NULL)
			{
				tmp =  start;
				
				/*
				 * Right now we have the full paragraph inside the <body>. We need to extract from it
				 * the <pre> tags, the '\n' and the '\r'.
				 */
				while (tmp != end)
				{
					if (*tmp == '>') // skipping the <pre> tags 
						skip = false;
					else if (*tmp == '<') // skipping the <pre> tags 
						skip = true;
					else if (*tmp != '\n' && *tmp != '\r' && skip == false)
						appendStringInfoChar(&errMsg, *tmp);
					tmp++;
				}
				
				ret = pstrdup(errMsg.data);
				pfree(errMsg.data);
				return ret;
			}
		}
	}
	
 	/*
	 * 4. We did not find the <body>. So we try to print the <title>. 
	 */
	start = strstr(msg, "<title>");
	if (start != NULL)  
	{
		start += 7;
		end = strstr(start, "</title>"); // no need to check if end is null, if <title> exists then also </title> exists
		if (end != NULL)
		{
			ret = pnstrdup(start, end - start);
			return ret;
		}
	}
	
	/*
	 * 5. This is an unexpected situation. We recevied an error message from the server but it does not have neither a <body>
	 *    nor a <title>. In this case we return the error message we received as-is.
	 */
	return msg;
}

/*
 * Get all the files that belong to an HDFS URI.
 */
static List* 
get_files_list(CURL *handle, HadoopUri *hadoop_uri, StringInfo rest_buf)
{
	long  http_ret = 0;
	List* files_list = NIL;
	StringInfoData globstatus_uri;
	initStringInfo(&globstatus_uri);
	
	make_globstatus_uri(&globstatus_uri, hadoop_uri);
	
	curl_easy_setopt(handle, CURLOPT_URL, globstatus_uri.data);
	send_message(handle, rest_buf);
	curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &http_ret);
	if (http_ret != 200)
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("%s", get_http_error_msg(http_ret, rest_buf->data) )));
	
	/* 
	 * the curl client finished the communication with hadoop backend (namenode or HMaster)
	 * now the response from the backend lies at the global variable rest_buf->data
	 */
	files_list = parse_namenode_globstat_response(files_list, rest_buf);	
	
	pfree(globstatus_uri.data);	
	return files_list;
}

/*
 * Concatanate the uri for the namenode request GLOBSTATUS
 * Result ls_uri example: http://<resthost>:<restport>/gpdb/GLOBSTATUS?path=/finance/2012/part*.txt
 */
static void 
make_globstatus_uri(StringInfo ls_uri, HadoopUri *hadoop_uri)
{
	appendStringInfo(ls_uri, "%s%s%s%s%s%s", 
					 REST_HTTP,
					 hadoop_uri->host,
					 ":",
					 hadoop_uri->port,
					 GPDB_GLOB_STATUS,
					 hadoop_uri->data_path);
}

/*
 * Utility function called during the parsing of an input string.
 * Parameter token is a substring of the input string whole_input.
 * token was obtained while searching for some pattern in  whole_input.
 * Here we verify that token is not null. If it is null, it means the parsing
 * failed and we throw a coresponding error.
 */
static void 
validate_in_format(char *token, char *whole_input)
{
	if (token == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("input string has incorrect format:  %s", whole_input)));
}

/*
 * Encapsulate the string verification of the subprotocol substring (hadoop_target_app)
 * from the user input uri
 */
static bool is_gphdfs(char *hadoop_target_app)
{
	bool gphdfs = false;
	
	if (strcmp(hadoop_target_app, HADOOP_APP_GPHDFS) == 0) /* hdfs */
		gphdfs = true;
	else if (strcmp(hadoop_target_app, HADOOP_APP_GPHBASE) == 0) /* hbase */
		gphdfs = false;
	else /* user inserted an hadoop target which is neither gphdfs nor gphbase - this is an error */
		Assert(false);
	
	return gphdfs;
}
