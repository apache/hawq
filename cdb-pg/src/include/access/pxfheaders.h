#ifndef _PXF_HEADERS_H_
#define _PXF_HEADERS_H_

#include "access/libchurl.h"
#include "access/pxfuriparser.h"

/*
 * Contains the information necessary to use delegation token
 * API from libhdfs3
 *
 * see fd.c Hdfs* functions
 */
typedef struct sPxfHdfsTokenInfo
{
	void	*hdfs_token;
	int		hdfs_token_size;
	void	*hdfs_handle;
} *PxfHdfsToken, PxfHdfsTokenData;

/*
 * Contains the data necessary to build the HTTP headers required for calling on the pxf service
 */
typedef struct sPxfInputData
{	
	CHURL_HEADERS	headers;
	GPHDUri			*gphduri;
	Relation		rel;
	char			*filterstr;
	PxfHdfsToken	token;
} PxfInputData;

void build_http_header(PxfInputData *input);

#endif
