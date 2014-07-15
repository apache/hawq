#include "access/pxfutils.h"
#include "utils/builtins.h"

/* Wrapper for libchurl */
static void process_request(ClientContext* client_context, char *uri);

/* checks if two ip strings are equal */
bool are_ips_equal(char *ip1, char *ip2)
{
	if ((ip1 == NULL) || (ip2 == NULL))
		return false;
	return (strcmp(ip1, ip2) == 0);
}

/* override port str with given new port int */
void port_to_str(char **port, int new_port)
{
	char tmp[10];

	if (!port)
		elog(ERROR, "unexpected internal error in pxfutils.c");
	if (*port)
		pfree(*port);

	Assert((new_port <= 65535) && (new_port >= 1)); /* legal port range */
	pg_ltoa(new_port, tmp);
	*port = pstrdup(tmp);
}

/*
 * call_rest
 *
 * Creates the REST message and sends it to the PXF service located on
 * <hadoop_uri->host>:<hadoop_uri->port>
 */
void
call_rest(GPHDUri *hadoop_uri,
		  ClientContext *client_context,
		  char *rest_msg)
{
	StringInfoData request;
	initStringInfo(&request);

	appendStringInfo(&request, rest_msg,
								hadoop_uri->host,
								hadoop_uri->port,
								PXF_SERVICE_PREFIX,
								PXF_VERSION);

	/* send the request. The response will exist in rest_buf.data */
	process_request(client_context, request.data);
	pfree(request.data);
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

	/* read some bytes to make sure the connection is established */
	churl_read_check_connectivity(client_context->handle);

	while ((n = churl_read(client_context->handle, client_context->chunk_buf, sizeof(client_context->chunk_buf))) != 0)
	{
		appendBinaryStringInfo(&(client_context->the_rest_buf), client_context->chunk_buf, n);
		memset(client_context->chunk_buf, 0, RAW_BUF_SIZE);
	}

	churl_cleanup(client_context->handle);
}


