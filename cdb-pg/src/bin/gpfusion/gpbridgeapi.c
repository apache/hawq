#include "common.h"
#include "gphdfilters.h"
#include "access/libchurl.h"
#include "access/gpxfuriparser.h"
#include "access/gpxfheaders.h"

static const char* remote_uri_template = "http://%s/%s/%s/Bridge/?fragments=%s";

typedef struct
{
	CHURL_HEADERS churl_headers;
	CHURL_HANDLE churl_handle;
	GPHDUri *gphd_uri;
	StringInfoData uri;
	ListCell* current_fragment;
} gphadoop_context;

void	gpbridge_check_inside_extproto(PG_FUNCTION_ARGS);
bool	gpbridge_last_call(PG_FUNCTION_ARGS);
bool	gpbridge_first_call(PG_FUNCTION_ARGS);
int		gpbridge_cleanup(PG_FUNCTION_ARGS);
void	cleanup_churl_handle(gphadoop_context* context);
void	cleanup_context(PG_FUNCTION_ARGS, gphadoop_context* context);
void	cleanup_gphd_uri(gphadoop_context* context);
void	add_querydata_to_http_header(gphadoop_context* context, PG_FUNCTION_ARGS);
void	append_churl_header_if_exists(gphadoop_context* context,
									  const char* key, const char* value);
void    set_current_fragment_headers(gphadoop_context* context);
void	gpbridge_import_start(PG_FUNCTION_ARGS);
size_t	gpbridge_read(PG_FUNCTION_ARGS);
void	cleanup_churl_headers(gphadoop_context* context);
void	parse_gphd_uri(gphadoop_context* context, PG_FUNCTION_ARGS);
gphadoop_context*	create_context(PG_FUNCTION_ARGS);
void	build_uri_from_current_fragment(gphadoop_context* context);
size_t	fill_buffer(gphadoop_context* context, char* start, size_t size);

/* Custom protocol entry point for read
 */
Datum gpbridge_import(PG_FUNCTION_ARGS)
{
	gpbridge_check_inside_extproto(fcinfo);

	if (gpbridge_last_call(fcinfo))
		PG_RETURN_INT32(gpbridge_cleanup(fcinfo));

	if (gpbridge_first_call(fcinfo))
		gpbridge_import_start(fcinfo);

	PG_RETURN_INT32((int)gpbridge_read(fcinfo));
}

void gpbridge_check_inside_extproto(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_EXTPROTOCOL(fcinfo))
		elog(ERROR, "cannot execute gpbridge_import outside protocol manager");
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
	return context;
}

/*
 * Add key/value pairs to connection header.
 * These values are the context of the query and used
 * by the remote component.
 */
void add_querydata_to_http_header(gphadoop_context* context, PG_FUNCTION_ARGS)
{
	GpxfInputData inputData;
	inputData.headers = context->churl_headers;
	inputData.gphduri = context->gphd_uri;
	inputData.rel = EXTPROTOCOL_GET_RELATION(fcinfo);
	inputData.filterstr = serializeGPHDFilterQuals(EXTPROTOCOL_GET_SCANQUALS(fcinfo));
	
	build_http_header(&inputData);
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
 * If the fragment doesn't user data, the header will be removed.
 */
void set_current_fragment_headers(gphadoop_context* context)
{
	FragmentData* frag_data = (FragmentData*)lfirst(context->current_fragment);
	elog(DEBUG2, "gpxf: set_current_fragment_source_name: source_name %s, index %s, has user data: %s ",
		 frag_data->source_name, frag_data->index, frag_data->user_data ? "TRUE" : "FALSE");
	churl_headers_override(context->churl_headers, "X-GP-DATA-DIR", frag_data->source_name);

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

	parse_gphd_uri(context, fcinfo);
	context->current_fragment = list_head(context->gphd_uri->fragments);
	build_uri_from_current_fragment(context);
	context->churl_headers = churl_headers_init();
	add_querydata_to_http_header(context, fcinfo);

	set_current_fragment_headers(context);

	context->churl_handle = churl_init_download(context->uri.data,
												context->churl_headers);
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

		build_uri_from_current_fragment(context);
		set_current_fragment_headers(context);
		churl_download_restart(context->churl_handle, context->uri.data, context->churl_headers);
	}

	return n;
}

void parse_gphd_uri(gphadoop_context* context, PG_FUNCTION_ARGS)
{
	context->gphd_uri = parseGPHDUri(EXTPROTOCOL_GET_URL(fcinfo));
	Assert(context->gphd_uri->fragments != NULL);
}

void build_uri_from_current_fragment(gphadoop_context* context)
{
	FragmentData* data = (FragmentData*)lfirst(context->current_fragment);
	resetStringInfo(&context->uri);
	appendStringInfo(&context->uri, remote_uri_template,
					 data->authority, GPDB_REST_PREFIX, GPFX_VERSION, data->index);
	elog(DEBUG2, "gpxf: uri with current fragment: %s", context->uri.data);

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