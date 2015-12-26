/*-------------------------------------------------------------------------
*
* uri.h
*	  Definitions for URI strings
*
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
*
*-------------------------------------------------------------------------
*/
#ifndef URI_H
#define URI_H

typedef enum UriProtocol
{
	URI_FILE,
	URI_FTP,
	URI_HTTP,
	URI_GPFDIST,
	URI_CUSTOM,
	URI_GPFDISTS
}	UriProtocol;

#define PROTOCOL_FILE		"file://"
#define PROTOCOL_FTP		"ftp://"
#define PROTOCOL_HTTP		"http://"
#define PROTOCOL_GPFDIST	"gpfdist://"
#define PROTOCOL_GPFDISTS	"gpfdists://"
#define PROTOCOL_PXF		"pxf://"

/* 
 * sometimes we don't want to parse the whole URI but just take a peek at
 * which protocol it is. We can use these macros to do just that.
 */
#define IS_FILE_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_FILE, strlen(PROTOCOL_FILE)) == 0)
#define IS_HTTP_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_HTTP, strlen(PROTOCOL_HTTP)) == 0)
#define IS_GPFDIST_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_GPFDIST, strlen(PROTOCOL_GPFDIST)) == 0)
#define IS_GPFDISTS_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_GPFDISTS, strlen(PROTOCOL_GPFDISTS)) == 0) 
#define IS_FTP_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_FTP, strlen(PROTOCOL_FTP)) == 0)
#define IS_PXF_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_PXF, strlen(PROTOCOL_PXF)) == 0)

typedef struct Uri
{
	UriProtocol protocol;
	char	   *hostname;
	int         port;
	char	   *path;
	char	   *customprotocol;
}	Uri;

extern Uri *ParseExternalTableUri(const char *uri);
extern void FreeExternalTableUri(Uri *uri);
extern char *CleanseUriString(char *uri);

#endif   /* URI_H */
