#include "common.h"
#include "access/pxffilters.h"
#include "access/libchurl.h"
#include "access/pxfuriparser.h"
#include "access/pxfheaders.h"
#include "access/pxfmasterapi.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbfilesystemcredential.h"

typedef struct
{
	CHURL_HEADERS churl_headers;
	CHURL_HANDLE churl_handle;
	GPHDUri *gphd_uri;
	StringInfoData uri;
	ListCell* current_fragment;
	StringInfoData write_file_name;
} gphadoop_context;

void	gpbridge_check_inside_extproto(PG_FUNCTION_ARGS, const char* func_name);
bool	gpbridge_last_call(PG_FUNCTION_ARGS);
bool	gpbridge_first_call(PG_FUNCTION_ARGS);
int		gpbridge_cleanup(PG_FUNCTION_ARGS);
void	cleanup_churl_handle(gphadoop_context* context);
void	cleanup_churl_headers(gphadoop_context* context);
void	cleanup_gphd_uri(gphadoop_context* context);
void	cleanup_context(PG_FUNCTION_ARGS, gphadoop_context* context);
gphadoop_context*	create_context(PG_FUNCTION_ARGS);
void	add_querydata_to_http_header(gphadoop_context* context, PG_FUNCTION_ARGS);
void	append_churl_header_if_exists(gphadoop_context* context,
									  const char* key, const char* value);
void    set_current_fragment_headers(gphadoop_context* context);
void	gpbridge_import_start(PG_FUNCTION_ARGS);
void	gpbridge_export_start(PG_FUNCTION_ARGS);
DataNodeRestSrv* get_pxf_server(GPHDUri* gphd_uri, const Relation rel);
size_t	gpbridge_read(PG_FUNCTION_ARGS);
size_t	gpbridge_write(PG_FUNCTION_ARGS);
void	parse_gphd_uri(gphadoop_context* context, bool is_import, PG_FUNCTION_ARGS);
void	build_uri_for_read(gphadoop_context* context);
void 	build_file_name_for_write(gphadoop_context* context);
void 	build_uri_for_write(gphadoop_context* context, DataNodeRestSrv* rest_server);
size_t	fill_buffer(gphadoop_context* context, char* start, size_t size);
void	add_delegation_token(PxfInputData *inputData);
void	free_token_resources(PxfInputData *inputData);

/* Custom protocol entry point for read
 */
Datum gpbridge_import(PG_FUNCTION_ARGS)
{
	gpbridge_check_inside_extproto(fcinfo, "gpbridge_import");

	if (gpbridge_last_call(fcinfo))
		PG_RETURN_INT32(gpbridge_cleanup(fcinfo));

	if (gpbridge_first_call(fcinfo))
		gpbridge_import_start(fcinfo);

	PG_RETURN_INT32((int)gpbridge_read(fcinfo));
}

Datum gpbridge_export(PG_FUNCTION_ARGS)
{
	gpbridge_check_inside_extproto(fcinfo, "gpbridge_export");

	if (gpbridge_last_call(fcinfo))
	{
		PG_RETURN_INT32(gpbridge_cleanup(fcinfo));
	}

	if (gpbridge_first_call(fcinfo))
	{
		gpbridge_export_start(fcinfo);
	}

	PG_RETURN_INT32((int)gpbridge_write(fcinfo));

}

void gpbridge_check_inside_extproto(PG_FUNCTION_ARGS, const char* func_name)
{
	if (!CALLED_AS_EXTPROTOCOL(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cannot execute %s outside protocol manager", func_name)));
}

bool gpbridge_last_call(PG_FUNCTION_ARGS)
{
	return EXTPROTOCOL_IS_LAST_CALL(fcinfo);
}

bool gpbridge_first_call(PG_FUNCTION_ARGS)
{
	return (EXTPROTOCOL_GET_USER_CTX(fcinfo) == NULL);
}

int gpbridge_cleanup(PG_FUNCTION_ARGS)
{
	gphadoop_context* context = EXTPROTOCOL_GET_USER_CTX(fcinfo);
	if (!context)
		return 0;

	cleanup_churl_handle(context);
	cleanup_churl_headers(context);
	cleanup_gphd_uri(context);
	cleanup_context(fcinfo, context);

	return 0;
}

void cleanup_churl_handle(gphadoop_context* context)
{
	churl_cleanup(context->churl_handle);
	context->churl_handle = NULL;
}

void cleanup_churl_headers(gphadoop_context* context)
{
	churl_headers_cleanup(context->churl_headers);
	context->churl_headers = NULL;
}

void cleanup_gphd_uri(gphadoop_context* context)
{
	if (context->gphd_uri == NULL)
		return;
	freeGPHDUri(context->gphd_uri);
	context->gphd_uri = NULL;
}

void cleanup_context(PG_FUNCTION_ARGS, gphadoop_context* context)
{
	pfree(context->uri.data);
	pfree(context->write_file_name.data);
	pfree(context);
	EXTPROTOCOL_SET_USER_CTX(fcinfo, NULL);
}

gphadoop_context* create_context(PG_FUNCTION_ARGS)
{
	gphadoop_context* context = NULL;

	context = palloc0(sizeof(gphadoop_context));
	/* first thing we do, store the context */
	EXTPROTOCOL_SET_USER_CTX(fcinfo, context);

	initStringInfo(&context->uri);
	initStringInfo(&context->write_file_name);
	return context;
}

/*
 * Add key/value pairs to connection header.
 * These values are the context of the query and used
 * by the remote component.
 */
void add_querydata_to_http_header(gphadoop_context* context, PG_FUNCTION_ARGS)
{
	PxfInputData inputData = {0};
	inputData.headers = context->churl_headers;
	inputData.gphduri = context->gphd_uri;
	inputData.rel = EXTPROTOCOL_GET_RELATION(fcinfo);
	inputData.filterstr = serializePxfFilterQuals(EXTPROTOCOL_GET_SCANQUALS(fcinfo));
	add_delegation_token(&inputData);
	
	build_http_header(&inputData);
	free_token_resources(&inputData);
}

void append_churl_header_if_exists(gphadoop_context* context, const char* key, const char* value)
{
	if (value)
		churl_headers_append(context->churl_headers, key, value);
}

/*
 * Change the headers with current fragment information:
 * 1. X-GP-DATA-DIR header is changed to the source name of the current fragment.
 * We reuse the same http header to send all requests for specific fragments.
 * The original header's value contains the name of the general path of the query
 * (can be with wildcard or just a directory name), and this value is changed here
 * to the specific source name of each fragment name.
 * 2. X-GP-FRAGMENT-USER-DATA header is changed to the current fragment's user data.
 * If the fragment doesn't have user data, the header will be removed.
 */
void set_current_fragment_headers(gphadoop_context* context)
{
	FragmentData* frag_data = (FragmentData*)lfirst(context->current_fragment);
	elog(DEBUG2, "pxf: set_current_fragment_source_name: source_name %s, index %s, has user data: %s ",
		 frag_data->source_name, frag_data->index, frag_data->user_data ? "TRUE" : "FALSE");

	churl_headers_override(context->churl_headers, "X-GP-DATA-DIR", frag_data->source_name);
	churl_headers_override(context->churl_headers, "X-GP-DATA-FRAGMENT", frag_data->index);
	churl_headers_override(context->churl_headers, "X-GP-FRAGMENT-METADATA", frag_data->fragment_md);

	if (frag_data->user_data)
	{
		churl_headers_override(context->churl_headers, "X-GP-FRAGMENT-USER-DATA", frag_data->user_data);
	}
	else
	{
		churl_headers_remove(context->churl_headers, "X-GP-FRAGMENT-USER-DATA", true);
	}

}

void gpbridge_import_start(PG_FUNCTION_ARGS)
{
	gphadoop_context* context = create_context(fcinfo);

	parse_gphd_uri(context, true, fcinfo);
	context->current_fragment = list_head(context->gphd_uri->fragments);
	build_uri_for_read(context);
	context->churl_headers = churl_headers_init();
	add_querydata_to_http_header(context, fcinfo);

	set_current_fragment_headers(context);

	context->churl_handle = churl_init_download(context->uri.data,
												context->churl_headers);
}

void gpbridge_export_start(PG_FUNCTION_ARGS)
{
	gphadoop_context* context = create_context(fcinfo);

	parse_gphd_uri(context, false, fcinfo);

	/* get rest servers list and choose one */
	Relation rel = EXTPROTOCOL_GET_RELATION(fcinfo);
	DataNodeRestSrv* rest_server = get_pxf_server(context->gphd_uri, rel);

	if (!rest_server)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("No REST servers were found (by accessing PXF URI %s)",
							   context->gphd_uri->uri)));

	elog(DEBUG2, "chosen pxf rest_server = %s:%d", rest_server->host, rest_server->port);

	build_file_name_for_write(context);
	build_uri_for_write(context, rest_server);
	free_datanode_rest_server(rest_server);

	context->churl_headers = churl_headers_init();
	add_querydata_to_http_header(context, fcinfo);

	context->churl_handle = churl_init_upload(context->uri.data,
											  context->churl_headers);

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

/*
 * get list of data nodes' rest servers,
 * and choose one (based on modulo segment id)
 * TODO: add locality
 */
DataNodeRestSrv* get_pxf_server(GPHDUri* gphd_uri, const Relation rel)
{

	ClientContext client_context; /* holds the communication info */
	PxfInputData inputData = {0};
	List	 	*rest_servers = NIL;
	DataNodeRestSrv *found_server = NULL, *ret_server = NULL;
	int 		 server_index = 0;

	Assert(gphd_uri);

	/* init context */
	init_client_context(&client_context);
	client_context.http_headers = churl_headers_init();

	/* set HTTP header that guarantees response in JSON format */
	churl_headers_append(client_context.http_headers, REST_HEADER_JSON_RESPONSE, NULL);
	if (!client_context.http_headers)
		return NULL;

	/*
	 * Enrich the curl HTTP header
	 */
	inputData.headers = client_context.http_headers;
	inputData.gphduri = gphd_uri;
	inputData.rel = rel;
	inputData.filterstr = NULL; /* We do not supply filter data to the HTTP header */
	add_delegation_token(&inputData);
	build_http_header(&inputData);

	/* send request */
	rest_servers = get_datanode_rest_servers(gphd_uri, &client_context);

	/* choose server by segment id */
	server_index = Gp_segment % list_length(rest_servers);
	elog(DEBUG3, "get_pxf_server: server index %d, segment id %d, rest servers number %d",
			server_index, Gp_segment, list_length(rest_servers));

	found_server = (DataNodeRestSrv*)list_nth(rest_servers, server_index);
	ret_server = (DataNodeRestSrv*)palloc(sizeof(DataNodeRestSrv));
	ret_server->host = pstrdup(found_server->host);
	ret_server->port = found_server->port;

	/* cleanup */
	free_datanode_rest_servers(rest_servers);
	free_token_resources(&inputData);
	churl_headers_cleanup(client_context.http_headers);

	/* return chosen server */
	return ret_server;
}


/* read as much as possible until we get a zero.
 * if necessary, move to next uri
 */
size_t gpbridge_read(PG_FUNCTION_ARGS)
{
	char* databuf;
	size_t datalen;
	size_t n = 0;
	gphadoop_context* context;

	context = EXTPROTOCOL_GET_USER_CTX(fcinfo);
	databuf = EXTPROTOCOL_GET_DATABUF(fcinfo);
	datalen = EXTPROTOCOL_GET_DATALEN(fcinfo);

	while ((n = fill_buffer(context, databuf, datalen)) == 0)
	{
		context->current_fragment = lnext(context->current_fragment);

		if (context->current_fragment == NULL)
			return 0;

		set_current_fragment_headers(context);
		churl_download_restart(context->churl_handle, context->uri.data, context->churl_headers);
	}

	return n;
}

/* write data into churl handler: send it as a chunked POST message.
 */
size_t gpbridge_write(PG_FUNCTION_ARGS)
{
	char* databuf;
	size_t datalen;
	size_t n = 0;
	gphadoop_context* context;

	context = EXTPROTOCOL_GET_USER_CTX(fcinfo);
	databuf = EXTPROTOCOL_GET_DATABUF(fcinfo);
	datalen = EXTPROTOCOL_GET_DATALEN(fcinfo);

	if (datalen > 0)
	{
		n = churl_write(context->churl_handle, databuf, datalen);
		elog(DEBUG5, "pxf gpbridge_write: wrote %zu bytes to %s", n, context->write_file_name.data);
	}

	return n;
}


void parse_gphd_uri(gphadoop_context* context, bool is_import, PG_FUNCTION_ARGS)
{
	context->gphd_uri = parseGPHDUri(EXTPROTOCOL_GET_URL(fcinfo));
	if (is_import)
		Assert(context->gphd_uri->fragments != NULL);
}

void build_uri_for_read(gphadoop_context* context)
{
	FragmentData* data = (FragmentData*)lfirst(context->current_fragment);
	resetStringInfo(&context->uri);
	appendStringInfo(&context->uri, "http://%s/%s/%s/Bridge/",
					 data->authority, GPDB_REST_PREFIX, PXF_VERSION);
	elog(DEBUG2, "pxf: uri %s for read", context->uri.data);
}

/*
 * Builds a unique file name for write per segment, based on
 * directory name from the table's URI, the transaction id (XID) and segment id.
 * e.g. with path in URI '/data/writable/table1', XID 1234 and segment id 3,
 * the file name will be '/data/writable/table1/1234_3'.
 */
void build_file_name_for_write(gphadoop_context* context)
{
	appendStringInfo(&context->write_file_name, "/%s/%d_%d",
					 context->gphd_uri->data, GetMasterTransactionId() /* xid */, Gp_segment);
	elog(DEBUG2, "pxf: file name for write: %s", context->write_file_name.data);
}

void build_uri_for_write(gphadoop_context* context, DataNodeRestSrv* rest_server )
{
	appendStringInfo(&context->uri, "http://%s:%d/%s/%s/Writable/stream?path=%s",
					 rest_server->host, rest_server->port,
					 GPDB_REST_PREFIX, PXF_VERSION, context->write_file_name.data);
	elog(DEBUG2, "pxf: uri %s with file name for write: %s", context->uri.data, context->write_file_name.data);

}

size_t fill_buffer(gphadoop_context* context, char* start, size_t size)
{
	size_t n = 0;
	char* ptr = start;
	char* end = ptr + size;

	while (ptr < end)
	{
		n = churl_read(context->churl_handle, ptr, end - ptr);
		if (n == 0)
			break;

		ptr += n;
	}

	return ptr - start;
}

/*
 * The function will get the cached delegation token
 * for remote host and add it to inputData
 * 
 * TODO Get "hdfs" and port from somplace else.
 */
void add_delegation_token(PxfInputData *inputData)
{
	if (!enable_secure_filesystem)
		return;

	PxfHdfsTokenData *token = NULL;
	token = palloc0(sizeof(PxfHdfsTokenData));

	token->hdfs_token = find_filesystem_credential("hdfs", 
												   inputData->gphduri->host, 
												   8020,
												   &token->hdfs_token_size);
	if (token->hdfs_token == NULL)
		elog(ERROR, "failed to find delegation token for %s", inputData->gphduri->host);
	elog(DEBUG2, "Delegation token for %s found", inputData->gphduri->host);

	inputData->token = token;
}

void free_token_resources(PxfInputData *inputData)
{
	if (inputData->token == NULL)
		return;

	pfree(inputData->token);
}
