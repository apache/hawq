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
#ifndef _PXF_URIPARSER_H_
#define _PXF_URIPARSER_H_

#include "postgres.h"
#include "fmgr.h"

#include "access/ha_config.h"
#include "utils/builtins.h"

/*
 * Path constants for accessing PXF.
 * All PXF's resources are under /PXF_SERVICE_PREFIX/PXF_VERSION/...
 */
#define PXF_SERVICE_PREFIX "pxf"
#define PXF_VERSION "v14" /* PXF version */

/*
 * FragmentData - describes a single Hadoop file split / HBase table region
 * in means of location (ip, port), the source name of the specific file/table that is being accessed,
 * and the index of a of list of fragments (splits/regions) for that source name.
 * The index refers to the list of the fragments of that source name.
 * user_data is optional.
 */
typedef struct FragmentData
{
	char	 *authority;
	char	 *index;
	char	 *source_name;
	char	 *fragment_md;
	char	 *user_data;
	char	 *profile;
} FragmentData;

typedef struct OptionData
{
	char	 *key;
	char	 *value;
} OptionData;

/*
 * GPHDUri - Describes the contents of a hadoop uri.
 */
typedef struct GPHDUri
{

    /* common */
    char 			*uri;		/* the unparsed user uri	*/
	char			*protocol;	/* the protocol name		*/
	char			*host;		/* host name str			*/
	char			*port;		/* port number as string	*/
	char			*data;      /* data location (path)     */
	char			*profile;   /* profile option			*/
	List			*fragments; /* list of FragmentData		*/

	/* options */
	List			*options;   /* list of OptionData 		*/

	/* HA
	 * list of nodes for the HDFS HA case - the active host and port from
	 * NNHAConf  will also occupy <host> and <port> members
	 */
	NNHAConf        *ha_nodes;
} GPHDUri;

GPHDUri	*parseGPHDUri(const char *uri_str);
GPHDUri	*parseGPHDUriForMetadata(char *uri_str);
void 	 freeGPHDUri(GPHDUri *uri);
void 	 freeGPHDUriForMetadata(GPHDUri *uri);
char	*GPHDUri_dup_without_segwork(const char* uri);
void	 GPHDUri_debug_print(GPHDUri *uri);
int		 GPHDUri_get_value_for_opt(GPHDUri *uri, char *key, char **val, bool emit_error);
bool 	 RelationIsExternalPxfReadOnly(Relation rel, StringInfo location);
void 	 GPHDUri_verify_no_duplicate_options(GPHDUri *uri);
void 	 GPHDUri_verify_core_options_exist(GPHDUri *uri, List *coreOptions);
char* normalize_key_name(const char* key);

#endif	// _PXF_URIPARSER_H_
