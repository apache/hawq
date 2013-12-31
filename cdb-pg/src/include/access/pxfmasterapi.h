/*-------------------------------------------------------------------------
*
* pxfmasterapi.h
*	  Functions for getting info from the PXF master.
*	  Used by hd_work_mgr and gpbridgeapi
*
* Copyright (c) 2007-2012, Greenplum inc
*
*-------------------------------------------------------------------------
*/
#ifndef _PXF_NAMENODE_H_
#define _PXF_NAMENODE_H_

#include "access/libchurl.h"
#include "commands/copy.h"
#include "access/pxfuriparser.h"
#include "access/hd_work_mgr.h"

/*
 * One debug level for all log messages from the data allocation algorithm
 */
#define FRAGDEBUG DEBUG2

#define REST_HEADER_JSON_RESPONSE "Accept: application/json"

typedef struct sClientContext
{
	CHURL_HEADERS http_headers;
	CHURL_HANDLE handle;
	char chunk_buf[RAW_BUF_SIZE];	/* part of the HTTP response - received	*/
									/* from one call to churl_read 			*/
	StringInfoData the_rest_buf; 	/* contains the complete HTTP response 	*/
} ClientContext;

typedef struct sDataNodeRestSrv
{
	char *host;
	int port;
} DataNodeRestSrv;

/*
 * A DataFragment instance contains the fragment data necessary
 * for the allocation algorithm
 */
typedef struct sDataFragment
{
	int   index; /* index per source name */
	char *source_name;
	List *replicas;
	char *fragment_md; /* fragment meta data (start, end, length, etc.) */
	char *user_data;
} DataFragment;

/*
 * Represents a fragment location replica
 */
typedef struct sFragmentHost
{
	char *ip;
	int   rest_port;
} FragmentHost;

extern void process_request(ClientContext* client_context, char *uri);
extern List* get_datanode_rest_servers(GPHDUri *hadoop_uri, ClientContext* client_context);
extern void free_datanode_rest_servers(List *srvrs);
extern void free_datanode_rest_server(DataNodeRestSrv* srv);
extern PxfStatsElem *get_data_statistics(GPHDUri* hadoop_uri, ClientContext *cl_context, StringInfo err_msg);
extern List* get_data_fragment_list(GPHDUri *hadoop_uri,  ClientContext* client_context);
extern void free_fragment(DataFragment *fragment);


#endif //_PXF_NAMENODE_H_


