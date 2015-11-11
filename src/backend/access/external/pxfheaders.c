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

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/extprotocol.h"
#include "access/fileam.h"
#include "access/url.h"
#include "catalog/namespace.h"
#include "catalog/pg_exttable.h"
#include "access/pxfheaders.h"
#include "utils/guc.h"

static void add_alignment_size_httpheader(CHURL_HEADERS headers);
static void add_tuple_desc_httpheader(CHURL_HEADERS headers, Relation rel);
static void add_location_options_httpheader(CHURL_HEADERS headers, GPHDUri *gphduri);
static char* prepend_x_gp(const char* key);
static void add_delegation_token_headers(CHURL_HEADERS headers, PxfInputData *inputData);
static void add_remote_credentials(CHURL_HEADERS headers);

/* 
 * Add key/value pairs to connection header. 
 * These values are the context of the query and used 
 * by the remote component. 
 */
void build_http_header(PxfInputData *input)
{
	extvar_t ev;
	CHURL_HEADERS headers = input->headers; 
	GPHDUri *gphduri = input->gphduri;
	Relation rel = input->rel;
	char *filterstr = input->filterstr;
	
	if (rel != NULL)
	{
		/* format */
		ExtTableEntry *exttbl = GetExtTableEntry(rel->rd_id);
        /* pxf treats CSV as TEXT */
		char* format = (fmttype_is_text(exttbl->fmtcode) || fmttype_is_csv(exttbl->fmtcode)) ? "TEXT":"GPDBWritable";
		churl_headers_append(headers, "X-GP-FORMAT", format);
		
		/* Record fields - name and type of each field */
		add_tuple_desc_httpheader(headers, rel);
	}
	
	/* GP cluster configuration */
	external_set_env_vars(&ev, gphduri->uri, false, NULL, NULL, false, 0);
	
	churl_headers_append(headers, "X-GP-SEGMENT-ID", ev.GP_SEGMENT_ID);
	churl_headers_append(headers, "X-GP-SEGMENT-COUNT", ev.GP_SEGMENT_COUNT);
	churl_headers_append(headers, "X-GP-XID", ev.GP_XID);
	
	/* Report alignment size to remote component
	 * GPDBWritable uses alignment that has to be the same as
	 * in the C code.
	 * Since the C code can be compiled for both 32 and 64 bits,
	 * the alignment can be either 4 or 8.
	 */
	add_alignment_size_httpheader(headers);
	
	/* headers for uri data */
	churl_headers_append(headers, "X-GP-URL-HOST", gphduri->host);
	churl_headers_append(headers, "X-GP-URL-PORT", gphduri->port);
	churl_headers_append(headers, "X-GP-DATA-DIR", gphduri->data);

	/* location options */
	add_location_options_httpheader(headers, gphduri);
	
	/* full uri */
	churl_headers_append(headers, "X-GP-URI", gphduri->uri);
	
	/* filters */
	if (filterstr)
	{
		churl_headers_append(headers, "X-GP-HAS-FILTER", "1");
		churl_headers_append(headers, "X-GP-FILTER", filterstr);
	}
	else
		churl_headers_append(headers, "X-GP-HAS-FILTER", "0");

	add_delegation_token_headers(headers, input);
	add_remote_credentials(headers);
}

/* Report alignment size to remote component
 * GPDBWritable uses alignment that has to be the same as
 * in the C code. 
 * Since the C code can be compiled for both 32 and 64 bits, 
 * the alignment can be either 4 or 8. 
 */
static void add_alignment_size_httpheader(CHURL_HEADERS headers)
{	
    char tmp[sizeof(char*)];	
    pg_ltoa(sizeof(char*), tmp);	
    churl_headers_append(headers, "X-GP-ALIGNMENT", tmp);	
}

/* 
 * Report tuple description to remote component 
 * Currently, number of attributes, attributes names and types 
 * Each attribute has a pair of key/value 
 * where X is the number of the attribute
 * X-GP-ATTR-NAMEX - attribute X's name 
 * X-GP-ATTR-TYPECODEX - attribute X's type OID (e.g, 16)
 * X-GP-ATTR-TYPENAMEX - attribute X's type name (e.g, "boolean")
 */
static void add_tuple_desc_httpheader(CHURL_HEADERS headers, Relation rel)
{	
    char long_number[32];	
    StringInfoData formatter;	
    TupleDesc tuple;		
    initStringInfo(&formatter);
	
    /* Get tuple description itself */	
    tuple = RelationGetDescr(rel);	
	
    /* Convert the number of attributes to a string */	
    pg_ltoa(tuple->natts, long_number);	
    churl_headers_append(headers, "X-GP-ATTRS", long_number);	
	
    /* Iterate attributes */	
    for (int i = 0; i < tuple->natts; ++i)		
    {		
        /* Add a key/value pair for attribute name */		
        resetStringInfo(&formatter);		
        appendStringInfo(&formatter, "X-GP-ATTR-NAME%u", i);		
        churl_headers_append(headers, formatter.data, tuple->attrs[i]->attname.data);
		
		/* Add a key/value pair for attribute type */		
        resetStringInfo(&formatter);		
        appendStringInfo(&formatter, "X-GP-ATTR-TYPECODE%u", i);
        pg_ltoa(tuple->attrs[i]->atttypid, long_number);		
        churl_headers_append(headers, formatter.data, long_number);

        /* Add a key/value pair for attribute type name */
        resetStringInfo(&formatter);
        appendStringInfo(&formatter, "X-GP-ATTR-TYPENAME%u", i);
        churl_headers_append(headers, formatter.data, TypeOidGetTypename(tuple->attrs[i]->atttypid));
    }
	
	pfree(formatter.data);
}

/* 
 * The options in the LOCATION statement of "create extenal table"
 * FRAGMENTER=HdfsDataFragmenter&ACCESSOR=SequenceFileAccessor... 
 */
static void add_location_options_httpheader(CHURL_HEADERS headers, GPHDUri *gphduri)
{
	ListCell *option = NULL;
	
	foreach(option, gphduri->options)
	{
		OptionData *data = (OptionData*)lfirst(option);
		char *x_gp_key = prepend_x_gp(data->key);
		churl_headers_append(headers, x_gp_key, data->value);
		pfree(x_gp_key);
	}
}

/* Full name of the HEADER KEY expected by the PXF service */
static char* prepend_x_gp(const char* key)
{	
	StringInfoData formatter;
	initStringInfo(&formatter);
	appendStringInfo(&formatter, "X-GP-%s", key);
	
	return formatter.data;
}

/*
 * The function will add delegation token headers.
 * Each token information piece will be serialized as HEX string.
 * e.g. AABBCC
 *
 * The function does nothing when secure filesystem mode is disabled.
 */
static void add_delegation_token_headers(CHURL_HEADERS headers, PxfInputData *inputData)
{
	if (inputData->token == NULL)
		return;

	churl_headers_append(headers, "X-GP-TOKEN", inputData->token->hdfs_token);
}

/*
 * Add contents of pxf_remote_service_login and
 * pxf_remote_service_secret as headers.
 *
 * Content of a GUC is not added if it is NULL
 */
static void add_remote_credentials(CHURL_HEADERS headers)
{
	if (pxf_remote_service_login != NULL)
		churl_headers_append(headers, "X-GP-REMOTE-USER", pxf_remote_service_login);

	if (pxf_remote_service_secret != NULL)
	churl_headers_append(headers, "X-GP-REMOTE-PASS", pxf_remote_service_secret);
}
