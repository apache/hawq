#ifndef _PXF_HEADERS_H_
#define _PXF_HEADERS_H_

#include "access/libchurl.h"
#include "access/pxfuriparser.h"

/*
 * Contains the data necessary to build the HTTP headers required for calling on the pxf service
 */
typedef struct sPxfInputData
{	
	CHURL_HEADERS headers;
	GPHDUri       *gphduri;
	Relation      rel;
	char      *filterstr;
} PxfInputData;

void build_http_header(PxfInputData *input);

#endif


