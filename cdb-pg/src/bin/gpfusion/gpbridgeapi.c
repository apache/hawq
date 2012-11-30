#include "common.h"
#include "gphdfilters.h"
#include "libchurl.h"
#include "uriparser.h"

///////////////////////////////////////////////////////////////

static const char* remote_uri_template = "http://%s/gpdb/v1/?fragments=%s";

///////////////////////////////////////////////////////////////
typedef struct
{
	CHURL_HEADERS churl_headers;
	CHURL_HANDLE churl_handle;
	GPHDUri *gphd_uri;
	StringInfoData uri;
	ListCell* current_fragment;
} gphadoop_context;

///////////////////////////////////////////////////////////////
void	gpbridge_check_inside_extproto(PG_FUNCTION_ARGS);
bool	gpbridge_last_call(PG_FUNCTION_ARGS);
bool	gpbridge_first_call(PG_FUNCTION_ARGS);
int		gpbridge_cleanup(PG_FUNCTION_ARGS);
void	cleanup_churl_handle(gphadoop_context* context);
void	cleanup_context(PG_FUNCTION_ARGS, gphadoop_context* context);
void	cleanup_gphd_uri(gphadoop_context* context);
void	build_http_header(gphadoop_context* context, PG_FUNCTION_ARGS);
void	add_alignment_size_httpheader(gphadoop_context* context);
void	add_tuple_desc_httpheader(gphadoop_context* context, Relation rel);
void	append_churl_header_if_exists(gphadoop_context* context,
									  const char* key, const char* value);
void	gpbridge_import_start(PG_FUNCTION_ARGS);
size_t	gpbridge_read(PG_FUNCTION_ARGS);
void	cleanup_churl_headers(gphadoop_context* context);
void	parse_gphd_uri(gphadoop_context* context, PG_FUNCTION_ARGS);
gphadoop_context*	create_context(PG_FUNCTION_ARGS);
void	build_uri_from_current_fragment(gphadoop_context* context);
size_t	fill_buffer(gphadoop_context* context, char* start, size_t size);
///////////////////////////////////////////////////////////////

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
	// first thing we do, store the context
	EXTPROTOCOL_SET_USER_CTX(fcinfo, context);

	initStringInfo(&context->uri);
	return context;
}

/*
 * Add key/value pairs to connection header.
 * These values are the context of the query and used
 * by the remote component.
 */
void build_http_header(gphadoop_context* context, PG_FUNCTION_ARGS)
{
	Relation rel;
	ExtTableEntry *exttbl;
	char *format;
	char *filterstr = NULL;
	extvar_t ev;

	/* NOTE: I've to assume that if it's not TEXT, it's going to be the RIGHT
	 * custom format. There's no easy way to find out the name of the formatter here.
	 * If the wrong formatter is used, we'll see some error in the protocol.
	 * No big deal.
	 */
	rel    = EXTPROTOCOL_GET_RELATION(fcinfo);
	exttbl = GetExtTableEntry(rel->rd_id);
	format = (fmttype_is_text(exttbl->fmtcode)) ? "TEXT":"GPDBWritable";

	/* parse and serialize the scan qualifiers (if any) */
	filterstr = serializeGPHDFilterQuals(EXTPROTOCOL_GET_SCANQUALS(fcinfo));

	external_set_env_vars(&ev, context->uri.data, false, NULL, NULL, false, 0);

	churl_headers_append(context->churl_headers, "X-GP-SEGMENT-ID", ev.GP_SEGMENT_ID);
	churl_headers_append(context->churl_headers, "X-GP-SEGMENT-COUNT", ev.GP_SEGMENT_COUNT);
	churl_headers_append(context->churl_headers, "X-GP-XID", ev.GP_XID);
	churl_headers_append(context->churl_headers, "X-GP-FORMAT", format);
	add_alignment_size_httpheader(context);
	add_tuple_desc_httpheader(context, rel);

    switch(context->gphd_uri->type)
    {
    	case URI_GPHDFS:
			churl_headers_append(context->churl_headers, "X-GP-HDTYPE", "hdfs");
			churl_headers_append(context->churl_headers, "X-GP-URL-HOST", context->gphd_uri->host);
			churl_headers_append(context->churl_headers, "X-GP-URL-PORT", context->gphd_uri->port);
			churl_headers_append(context->churl_headers, "X-GP-DATA-DIR", context->gphd_uri->u.hdfs.directory);
			churl_headers_append(context->churl_headers, "X-GP-ACCESSOR", context->gphd_uri->u.hdfs.accessor);
    		append_churl_header_if_exists(context, "X-GP-DATA-SCHEMA", context->gphd_uri->u.hdfs.data_schema);
			churl_headers_append(context->churl_headers, "X-GP-RESOLVER", context->gphd_uri->u.hdfs.resolver);
			// should be for write only
			churl_headers_append(context->churl_headers, "X-GP-URI", context->gphd_uri->uri);

    		break;

    	case URI_GPHBASE:
			churl_headers_append(context->churl_headers, "X-GP-HDTYPE", "hbase");
			churl_headers_append(context->churl_headers, "X-GP-HBASE-TABLE", context->gphd_uri->u.hbase.table_name);

    		if (filterstr)
    		{
				churl_headers_append(context->churl_headers, "X-GP-HAS-FILTER", "1");
				churl_headers_append(context->churl_headers, "X-GP-FILTER", filterstr);
    		}
    		else
				churl_headers_append(context->churl_headers, "X-GP-HAS-FILTER", "0");

    		break;

    	default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unsupported type %d in uri %s", 
							context->gphd_uri->type, context->gphd_uri->uri)));
    }
}

/* Report alignment size to remote component
 * GPDBWritable uses alignment that has to be the same as
 * in the C code.
 * Since the C code can be compiled for both 32 and 64 bits,
 * the alignment can be either 4 or 8.
 */
void add_alignment_size_httpheader(gphadoop_context* context)
{
    char tmp[sizeof(char*)];
    pg_ltoa(sizeof(char*), tmp);
    churl_headers_append(context->churl_headers, "X-GP-ALIGNMENT", tmp);
}

/*
 * Report tuple description to remote component
 * Currently, number of attributes, attributes names and types
 * Each attribute has a pair of key/value
 * where X is the number of the attribute
 * X-GP-ATTR-NAMEX - attribute X's name
 * X-GP-ATTR-TYPEX - attribute X's type
 */
void add_tuple_desc_httpheader(gphadoop_context* context, Relation rel)
{
    char long_number[32];
    StringInfoData formatter;
    TupleDesc tuple;

    initStringInfo(&formatter);

    /* Get tuple descrition it self */
    tuple = RelationGetDescr(rel);

    /* Convert the number of attributes to a string */
    pg_ltoa(tuple->natts, long_number);
    churl_headers_append(context->churl_headers, "X-GP-ATTRS", long_number);

    /* Iterate attributes */
    for (int i = 0; i < tuple->natts; ++i)
    {
        /* Add a key/value pair for attribute name */
        resetStringInfo(&formatter);
        appendStringInfo(&formatter, "X-GP-ATTR-NAME%u", i);
        churl_headers_append(context->churl_headers, formatter.data, tuple->attrs[i]->attname.data);

        /* Add a key/value pair for attribute type */
        resetStringInfo(&formatter);
        appendStringInfo(&formatter, "X-GP-ATTR-TYPE%u", i);
        pg_ltoa(tuple->attrs[i]->atttypid, long_number);
        churl_headers_append(context->churl_headers, formatter.data, long_number);
    }
	pfree(formatter.data);
}

void append_churl_header_if_exists(gphadoop_context* context, const char* key, const char* value)
{
	if (value)
		churl_headers_append(context->churl_headers, key, value);
}

void gpbridge_import_start(PG_FUNCTION_ARGS)
{
	gphadoop_context* context = create_context(fcinfo);

	parse_gphd_uri(context, fcinfo);
	context->current_fragment = list_head(context->gphd_uri->fragments);
	build_uri_from_current_fragment(context);
	context->churl_headers = churl_headers_init();
	build_http_header(context, fcinfo);
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
		churl_download_restart(context->churl_handle, context->uri.data);
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
					 data->authority, data->index);
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
