#ifndef _GPXF_HEADERS_H_
#define _GPXF_HEADERS_H_

#include "access/libchurl.h"
#include "access/gpxfuriparser.h"

/*
 * Contains the data necessary to build the HTTP headers required for calling on the GPXF service
 */
typedef struct GpxfInputData
{	
	CHURL_HEADERS headers;
	GPHDUri       *gphduri;
	Relation      rel;
	char      *filterstr;
} GpxfInputData;

void build_http_header(GpxfInputData *input);

#endif


