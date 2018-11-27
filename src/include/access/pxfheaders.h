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
	char	*hdfs_token;
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
	ProjectionInfo  *proj_info;
	List			*quals;
} PxfInputData;

void build_http_header(PxfInputData *input);

#endif
