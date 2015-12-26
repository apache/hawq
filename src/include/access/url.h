/*-------------------------------------------------------------------------
 *
 * url.h
 * routines for external table access to urls.
 * to the qExec processes.
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
 * $Id: //cdb2/main/cdb-pg/src/include/cdb/cdbdisp.h#78 $
 *
 *-------------------------------------------------------------------------
 */
#ifndef URL_H
#define URL_H

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>

#include <curl/curl.h>

/*#include <fstream/fstream.h>*/

#include "access/extprotocol.h"

#include "commands/copy.h"

#ifndef FALSE
#define FALSE 0
#endif

#ifndef TRUE
#define TRUE 1
#endif

enum fcurl_type_e 
{ 
	CFTYPE_NONE = 0, 
	CFTYPE_FILE = 1, 
	CFTYPE_CURL = 2, 
	CFTYPE_EXEC = 3, 
	CFTYPE_CUSTOM = 4 
};

#if BYTE_ORDER == BIG_ENDIAN
#define local_htonll(n)  (n)
#define local_ntohll(n)  (n)
#else
#define local_htonll(n)  ((((uint64) htonl(n)) << 32LL) | htonl((n) >> 32LL))
#define local_ntohll(n)  ((((uint64) ntohl(n)) << 32LL) | (uint32) ntohl(((uint64)n) >> 32LL))
#endif

typedef struct curlctl_t {
	
	CURL *handle;
	char *curl_url;
	struct curl_slist *x_httpheader;
	
	struct 
	{
		char* ptr;	   /* malloc-ed buffer */
		int   max;
		int   bot, top;
	} in;

	struct 
	{
		char* ptr;	   /* malloc-ed buffer */
		int   max;
		int   bot, top;
	} out;

	int still_running;          /* Is background url fetch still in progress */
	int for_write;				/* 'f' when we SELECT, 't' when we INSERT    */
	int error, eof;				/* error & eof flags */
	int gp_proto;
	char *http_response;
	
	struct 
	{
		int   datalen;  /* remaining datablock length */
	} block;
	
} curlctl_t;

#define EXEC_DATA_P 0 /* index to data pipe */
#define EXEC_ERR_P 1 /* index to error pipe  */

typedef struct URL_FILE
{
    enum fcurl_type_e type;     /* type of handle */
	char *url;

    union {
		
    	struct {
			struct fstream_t*fp;
		} file;

		curlctl_t curl;

		struct {
			int 	pid;
			int 	pipes[2]; /* only out and err needed */
			char*	shexec;
		} exec;
		
		struct {
			FmgrInfo   *protocol_udf;
			ExtProtocol extprotocol;
			List	   *scanquals;
			MemoryContext protcxt;
		} custom;
		
	} u;
} URL_FILE;

typedef struct extvar_t
{
	char* GP_MASTER_HOST;
	char* GP_MASTER_PORT;
	char* GP_DATABASE;
	char* GP_USER;
	char* GP_SEG_PG_CONF;   /*location of the segments pg_conf file*/
	char* GP_SEG_DATADIR;   /*location of the segments datadirectory*/
	char GP_DATE[9];		/* YYYYMMDD */
	char GP_TIME[7];		/* HHMMSS */
	char GP_XID[TMGIDSIZE];		/* global transaction id */
	char GP_CID[10];		/* command id */
	char GP_SN[10];		/* scan number */
	char GP_SEGMENT_ID[6];  /*segments content id*/
	char GP_SEG_PORT[10];
	char GP_SESSION_ID[10];  /* session id */
 	char GP_SEGMENT_COUNT[6]; /* total number of (primary) segs in the system */
 	char GP_CSVOPT[13]; /* "m.x...q...h." former -q, -h and -x options for gpfdist.*/
} extvar_t;

/* exported functions */
extern URL_FILE *url_fopen(char *url, bool forwrite, extvar_t *ev, CopyState pstate, List *quals, int *response_code, const char **response_string);
extern int url_fclose(URL_FILE *file, bool failOnError, const char *relname);
extern bool url_feof(URL_FILE *file, int bytesread);
extern bool url_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen);
extern size_t url_fread(void *ptr, size_t size, size_t nmemb, URL_FILE *file, CopyState pstate);
extern size_t url_fwrite(void *ptr, size_t size, size_t nmemb, URL_FILE *file, CopyState pstate);
extern void url_rewind(URL_FILE *file, const char *relname);
extern void url_fflush(URL_FILE *file, CopyState pstate);

extern URL_FILE *url_execute_fopen(char* url, char *cmd, bool forwrite, extvar_t *ev);

#endif
