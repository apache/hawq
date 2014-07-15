#ifndef _PXF_UTILS_H_
#define _PXF_UTILS_H_

#include "postgres.h"
#include "access/libchurl.h"
#include "access/pxfuriparser.h"
#include "commands/copy.h"

typedef struct sClientContext
{
	CHURL_HEADERS http_headers;
	CHURL_HANDLE handle;
	char chunk_buf[RAW_BUF_SIZE];	/* part of the HTTP response - received	*/
									/* from one call to churl_read 			*/
	StringInfoData the_rest_buf; 	/* contains the complete HTTP response 	*/
} ClientContext;

/* checks if two ip strings are equal */
bool are_ips_equal(char *ip1, char *ip2);

/* override port str with given new port int */
void port_to_str(char** port, int new_port);

/* Parse the REST message and issue the libchurl call */
void call_rest(GPHDUri *hadoop_uri, ClientContext *client_context, char* rest_msg);

#endif	// _PXF_UTILS_H_
