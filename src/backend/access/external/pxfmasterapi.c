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
 * pxfnamenode.c
 *	  Functions for getting info from the PXF master.
 *	  Used by hd_work_mgr and gpbridgeapi
 *
 *-------------------------------------------------------------------------
 */
#include <json/json.h>
#include "access/pxfmasterapi.h"
#include "catalog/hcatalog/externalmd.h"

static List* parse_datanodes_response(List *rest_srvrs, StringInfo rest_buf);
static PxfFragmentStatsElem *parse_get_frag_stats_response(StringInfo rest_buf);
static List* parse_get_fragments_response(List* fragments, StringInfo rest_buf);
static void ha_failover(GPHDUri *hadoop_uri, ClientContext *client_context, char* rest_msg);
static void rest_request(GPHDUri *hadoop_uri, ClientContext* client_context, char *rest_msg);
static char* concat(char *body, char *tail);

/*
 * Obtain the datanode REST servers host/port data
 */
static List*
parse_datanodes_response(List *rest_srvrs, StringInfo rest_buf)
{
	struct json_object *whole = json_tokener_parse(rest_buf->data);
	if ((whole == NULL) || is_error(whole))
	{
		elog(ERROR, "Failed to parse datanode list from PXF");
	}
	struct json_object *nodes = json_object_object_get(whole, "regions");
	int length = json_object_array_length(nodes);

	/* obtain host/port data for the REST server running on each HDFS data node */
	for (int i = 0; i < length; i++)
	{
		PxfServer* srv = (PxfServer*)palloc(sizeof(PxfServer));

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
 * Wrap the REST call with a retry for the HA HDFS scenario
 */
static void
rest_request(GPHDUri *hadoop_uri, ClientContext* client_context, char *rest_msg)
{
	Assert(hadoop_uri->host != NULL && hadoop_uri->port != NULL);

	/* construct the request */
	PG_TRY();
	{
		call_rest(hadoop_uri, client_context, rest_msg);
	}
	PG_CATCH();
	{
		if (hadoop_uri->ha_nodes) /* if we are in the HA scenario will try to access the second Namenode machine */
		{
			char* message = elog_message();
			elog(DEBUG2, "rest_request: calling first HA namenode failed, trying second (%s)", message ? message : "Unknown error");

			/* release error state - we finished handling this error and need to clean the error stack.
			 * ha_failover might fail, but that will generate its own error. */
			if (!elog_dismiss(DEBUG5))
				PG_RE_THROW(); /* hope to never get here! */

			ha_failover(hadoop_uri, client_context, rest_msg);
		}
		else /*This is not HA - so let's re-throw */
			PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Get host/port data for the REST servers running on each datanode
 */
List*
get_datanode_rest_servers(GPHDUri *hadoop_uri, ClientContext* client_context)
{
	List* rest_srvrs_list = NIL;
	char *restMsg = "http://%s:%s/%s/%s/HadoopCluster/getNodesInfo";

	rest_request(hadoop_uri, client_context, restMsg);

	/*
	 * the curl client finished the communication. The response from
	 * the backend lies at rest_buf->data
	 */
	rest_srvrs_list = parse_datanodes_response(rest_srvrs_list, &(client_context->the_rest_buf));
	return rest_srvrs_list;
}

void
free_datanode_rest_servers(List *srvrs)
{
	ListCell *srv_cell = NULL;
	foreach(srv_cell, srvrs)
	{
		PxfServer* srv = (PxfServer*)lfirst(srv_cell);
		free_datanode_rest_server(srv);
	}
	list_free(srvrs);
}

void free_datanode_rest_server(PxfServer* srv)
{
	if (srv->host)
		pfree(srv->host);
	pfree(srv);
}

/*
 * Fetch fragment statistics from the PXF service
 */
PxfFragmentStatsElem *get_fragments_statistics(GPHDUri* hadoop_uri,
											   ClientContext *client_context,
											   StringInfo err_msg)
{
	char *restMsg = concat("http://%s:%s/%s/%s/Fragmenter/getFragmentsStats?path=", hadoop_uri->data);

	/* send the request. The response will exist in rest_buf.data */
	PG_TRY();
	{
		rest_request(hadoop_uri, client_context, restMsg);
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

	/* parse the JSON response and form a statistics struct to return */
	return parse_get_frag_stats_response(&(client_context->the_rest_buf));
}

/*
 * Parse the json response from the PXF Fragmenter.getFragmentsStats
 */
static PxfFragmentStatsElem *parse_get_frag_stats_response(StringInfo rest_buf)
{
	PxfFragmentStatsElem* statsElem = (PxfFragmentStatsElem*)palloc0(sizeof(PxfFragmentStatsElem));
	struct json_object	*whole	= json_tokener_parse(rest_buf->data);
	if ((whole == NULL) || is_error(whole))
	{
		elog(ERROR, "Failed to parse statistics data from PXF");
	}
	struct json_object	*head	= json_object_object_get(whole, "PXFFragmentsStats");

	/* 0. number of fragments */
	struct json_object *js_num_fragments = json_object_object_get(head, "fragmentsNumber");
	statsElem->numFrags = json_object_get_int(js_num_fragments);

	/* 1. first fragment size */
	struct json_object *js_first_frag_size = json_object_object_get(head, "firstFragmentSize");
	statsElem->firstFragSize = json_object_get_int(js_first_frag_size);

	/* 2. total size */
	struct json_object *js_total_size = json_object_object_get(head, "totalSize");
	statsElem->totalSize = json_object_get_int(js_total_size);

	return statsElem;
}

/*
 * ha_failover
 *
 * Handle the HA failover logic for the REST call.
 * Change the active NN located in <hadoop_uri->host>:<hadoop_uri->port>
 * and issue the REST call again
 */
static void
ha_failover(GPHDUri *hadoop_uri,
		    ClientContext *client_context,
		    char* rest_msg)
{
	int i;
	NNHAConf *conf = hadoop_uri->ha_nodes;

	for (i = 0; i < conf->numn; i++)
	{
		/* There are two HA Namenodes inside NNHAConf. We tried one. Let's try the next one */
		if ((hadoop_uri->host == NULL) || (strcmp(conf->nodes[i], hadoop_uri->host) != 0) ||
			(hadoop_uri->port == NULL) || (strcmp(conf->restports[i], hadoop_uri->port) != 0))
		{
			if (hadoop_uri->host)
				pfree(hadoop_uri->host);
			if (hadoop_uri->port)
				pfree(hadoop_uri->port);
			hadoop_uri->host = pstrdup(conf->nodes[i]);
			hadoop_uri->port = pstrdup(conf->restports[i]);
			call_rest(hadoop_uri, client_context, rest_msg);
			break;
		}
	}
	if (i == conf->numn) /* call_rest was not called - can happen if two NN run on the same machine */
	{
		elog(DEBUG2, "ha_failover did not run (former call was to address %s:%s, ha nodes: %s:%s, %s:%s)",
			 hadoop_uri->host, hadoop_uri->port,
			 conf->nodes[0], conf->restports[0],
			 conf->nodes[1], conf->restports[1]);
		elog(ERROR, "Standby NameNode of HA nameservice %s was not found after call to Active NameNode failed - failover aborted", conf->nameservice);
	}
}

/* Concatenate two literal strings using stringinfo */
char* concat(char *body, char *tail)
{
	StringInfoData str;
	initStringInfo(&str);

	appendStringInfoString(&str, body);
	appendStringInfoString(&str, tail);
	return str.data;
}

/*
 * get_data_fragment_list
 *
 * 1. Request a list of fragments from the PXF Fragmenter class
 * that was specified in the pxf URL.
 *
 * 2. Process the returned list and create a list of DataFragment with it.
 */
List*
get_data_fragment_list(GPHDUri *hadoop_uri,
					   ClientContext *client_context)
{
	List *data_fragments = NIL;
	char *restMsg = concat("http://%s:%s/%s/%s/Fragmenter/getFragments?path=",hadoop_uri->data);

	rest_request(hadoop_uri, client_context, restMsg);

	/* parse the JSON response and form a fragments list to return */
	data_fragments = parse_get_fragments_response(data_fragments, &(client_context->the_rest_buf));

	return data_fragments;
}

/*
 * parse the response of the PXF Fragments call. An example:
 *
 * Request:
 * 		curl --header "X-GP-FRAGMENTER: HdfsDataFragmenter" "http://goldsa1mac.local:50070/pxf/v2/Fragmenter/getFragments?path=demo" (demo is a directory)
 *
 * Response (left as a single line purposefully):
 * {"PXFFragments":[{"index":0,"userData":null,"sourceName":"demo/text2.csv","metadata":"rO0ABXcQAAAAAAAAAAAAAAAAAAAABXVyABNbTGphdmEubGFuZy5TdHJpbmc7rdJW5+kde0cCAAB4cAAAAAN0ABxhZXZjZWZlcm5hczdtYnAuY29ycC5lbWMuY29tdAAcYWV2Y2VmZXJuYXM3bWJwLmNvcnAuZW1jLmNvbXQAHGFldmNlZmVybmFzN21icC5jb3JwLmVtYy5jb20=","replicas":["10.207.4.23","10.207.4.23","10.207.4.23"]},{"index":0,"userData":null,"sourceName":"demo/text_csv.csv","metadata":"rO0ABXcQAAAAAAAAAAAAAAAAAAAABnVyABNbTGphdmEubGFuZy5TdHJpbmc7rdJW5+kde0cCAAB4cAAAAAN0ABxhZXZjZWZlcm5hczdtYnAuY29ycC5lbWMuY29tdAAcYWV2Y2VmZXJuYXM3bWJwLmNvcnAuZW1jLmNvbXQAHGFldmNlZmVybmFzN21icC5jb3JwLmVtYy5jb20=","replicas":["10.207.4.23","10.207.4.23","10.207.4.23"]}]}
 */
static List*
parse_get_fragments_response(List *fragments, StringInfo rest_buf)
{
	struct json_object	*whole	= json_tokener_parse(rest_buf->data);
	if ((whole == NULL) || is_error(whole))
	{
		elog(ERROR, "Failed to parse fragments list from PXF");
	}
	struct json_object	*head	= json_object_object_get(whole, "PXFFragments");
	List* 				ret_frags = fragments;
	int 				length	= json_object_array_length(head);

	/* obtain split information from the block */
	for (int i = 0; i < length; i++)
	{
		struct json_object *js_fragment = json_object_array_get_idx(head, i);
		DataFragment* fragment = (DataFragment*)palloc0(sizeof(DataFragment));

		/* 0. source name */
		struct json_object *block_data = json_object_object_get(js_fragment, "sourceName");
		fragment->source_name = pstrdup(json_object_get_string(block_data));

		/* 1. fragment index, incremented per source name */
		struct json_object *index = json_object_object_get(js_fragment, "index");
		fragment->index = json_object_get_int(index);

		/* 2. replicas - list of all machines that contain this fragment */
		struct json_object *js_fragment_replicas = json_object_object_get(js_fragment, "replicas");
		int num_replicas = json_object_array_length(js_fragment_replicas);

		for (int j = 0; j < num_replicas; j++)
		{
			FragmentHost* fhost = (FragmentHost*)palloc(sizeof(FragmentHost));
			struct json_object *host = json_object_array_get_idx(js_fragment_replicas, j);

			fhost->ip = pstrdup(json_object_get_string(host));

			fragment->replicas = lappend(fragment->replicas, fhost);
		}

		/* 3. location - fragment meta data */
		struct json_object *js_fragment_metadata = json_object_object_get(js_fragment, "metadata");
		if (js_fragment_metadata)
			fragment->fragment_md = pstrdup(json_object_get_string(js_fragment_metadata));


		/* 4. userdata - additional user information */
		struct json_object *js_user_data = json_object_object_get(js_fragment, "userData");
		if (js_user_data)
			fragment->user_data = pstrdup(json_object_get_string(js_user_data));

		/*
		 * HD-2547:
		 * Ignore fragment if it doesn't contain any host locations,
		 * for example if the file is empty.
		 */
		if (fragment->replicas)
			ret_frags = lappend(ret_frags, fragment);
		else
			free_fragment(fragment);

	}

	return ret_frags;
}

/*
 * release memory of a single fragment
 */
void free_fragment(DataFragment *fragment)
{
	ListCell *loc_cell = NULL;

	Assert(fragment != NULL);

	if (fragment->source_name)
		pfree(fragment->source_name);

	foreach(loc_cell, fragment->replicas)
	{
		FragmentHost* host = (FragmentHost*)lfirst(loc_cell);

		if (host->ip)
			pfree(host->ip);
		pfree(host);
	}
	list_free(fragment->replicas);

	if (fragment->fragment_md)
		pfree(fragment->fragment_md);

	if (fragment->user_data)
		pfree(fragment->user_data);
	pfree(fragment);
}

/*
 * get_hcat_metadata
 *
 * Request hcatalog metadata from the PXF MetadataResource
 * Process the hcat response
 * TODO: The hcat response processing might be pushed out of this function's context
 *
 */
List* get_hcat_metadata(GPHDUri* hadoop_uri, char *location, ClientContext *client_context)
{
	List *hcat_tables = NIL;
	char *restMsg = concat("http://%s:%s/%s/%s/Metadata/getTableMetadata?table=", location);

	rest_request(hadoop_uri, client_context, restMsg);

	/* parse the JSON response and form a fragments list to return */
	hcat_tables = ParseHCatalogEntries(&(client_context->the_rest_buf));

	return hcat_tables;
}

