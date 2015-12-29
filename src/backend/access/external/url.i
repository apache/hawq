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

#include <stdarg.h>
#include <sys/stat.h>

#include "access/fileam.h"
#include "access/heapam.h"
#include "access/valid.h"
#include "commands/dbcommands.h"
#include "catalog/namespace.h"
#include "cdb/cdbutil.h"
#include "libpq/libpq-be.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/uri.h"
#include "mb/pg_wchar.h"
#include "commands/copy.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "nodes/makefuncs.h"
#include "cdb/cdbvars.h"
#include "postmaster/postmaster.h"  /* postmaster port*/
#include <arpa/inet.h>
#include <fstream/gfile.h>

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>

#include <curl/curl.h>

#include <fstream/fstream.h>

#include "cdb/url.h"

#ifndef FALSE
#define FALSE 0
#endif

#ifndef TRUE
#define TRUE 1
#endif

/* we use a global one for convenience */
CURLM *multi_handle = 0;

/* curl calls this routine to return HTTP header from server */
static size_t 
header_callback(void *ptr_, size_t size, size_t nmemb, void *userp)
{
    URL_FILE *url = (URL_FILE *)userp;
	char *ptr = ptr_;
	int len = size * nmemb;
	int i;
	char buf[20];

	if (len > 10 && *ptr == 'X' && 0 == strncmp("X-GP-PROTO", ptr, 10))
	{
		ptr += 10, len -= 10;
		while (len > 0 && (*ptr == ' ' || *ptr == '\t'))
			ptr++, len--;

		if (len > 0 && *ptr == ':')
		{
			ptr++, len--;
			while (len > 0 && (*ptr == ' ' || *ptr == '\t'))
				ptr++, len--;
			for (i = 0; i < sizeof(buf) - 1 && i < len; i++) 
				buf[i] = ptr[i];
			buf[i] = 0;
			url->u.curl.gp_proto = strtol(buf, 0, 0);
			// elog(NOTICE, "X-GP-PROTO: %s (%d)", buf, url->u.curl.gp_proto);
		}
	}
	
	return size*nmemb;
}


/* curl calls this routine to get more data */
static size_t
write_callback(char *buffer,
               size_t size,
               size_t nitems,
               void *userp)
{
    URL_FILE *file = (URL_FILE *)userp;
	curlctl_t *curl = &file->u.curl;
	const int nbytes = size * nitems;
	int n;

	// elog(NOTICE, "write_callback %d", nbytes);

	/* if insufficient space in buffer ... */
	if (curl->buf.top + nbytes >= curl->buf.max)
	{
		/* compact ? */
		if (curl->buf.bot)
		{
			n = curl->buf.top - curl->buf.bot;
			memmove(curl->buf.ptr, curl->buf.ptr + curl->buf.bot, n);
			curl->buf.bot = 0;
			curl->buf.top = n;
		}
		/* if still insufficient space in buffer, then do realloc */
		if (curl->buf.top + nbytes >= curl->buf.max)
		{
			char *newbuf;

			n = curl->buf.top - curl->buf.bot + nbytes + 1024;
			newbuf = realloc(curl->buf.ptr, n);
			if (!newbuf)
			{
				elog(ERROR, "out of memory");
				return -1;
			}
			curl->buf.ptr = newbuf;
			curl->buf.max = n;
			// elog(NOTICE, "max now at %d", n);
			Assert(curl->buf.top + nbytes < curl->buf.max);
		}
	}
	/* enough space. copy buffer into curl->buf */
	memcpy(curl->buf.ptr + curl->buf.top, buffer, nbytes);
	curl->buf.top += nbytes;
	return nbytes;
}


static int
check_response(URL_FILE *file, int *rc, const char **response_string)
{
	long response_code;
	char *effective_url;
	int e;
	static char buffer[30];
	CURL* curl = file->u.curl.handle;

	if (0 != (e = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code)))
	{
		*rc = 500;
		*response_string = "curl_easy_getinfo failed";
		return -1;
	}
	*rc = response_code;
	snprintf(buffer, sizeof buffer, "Response Code=%d", (int)response_code);
	*response_string = buffer;

	if (0 != (e = curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &effective_url)))
		return -1;

	if (! (200 <= response_code && response_code < 300))
	{
		if (response_code == 0) 
			elog(ERROR, "connection failed while %s", file->url);
		else
		{
			/* we need to sleep 1 sec to avoid this condition:
			   1- seg X gets an error message from gpfdist
			   2- I gets a 500 error
			   3- I report error before seg X, and error message 
			   in seg X is thrown away.
			*/
			sleep(1);
			elog(ERROR, "http response code %ld while %s", response_code, file->url);
		}
		return -1;
	}

	return 0;
}


/* use to attempt to fill the read buffer up to requested number of bytes */
/* return 0 if successful, -1 otherwise */
static int
fill_buffer(URL_FILE *file,int want)
{
    fd_set fdread;
    fd_set fdwrite;
    fd_set fdexcep;
    int maxfd;
    struct timeval timeout;
    int nfds, e;
	curlctl_t* curl = &file->u.curl;

	/* elog(NOTICE, "= still_running %d, bot %d, top %d, want %d", 
	   file->u.curl.still_running, curl->buf.bot, curl->buf.top, want);
	*/

    /* attempt to fill buffer */
	while (curl->still_running && curl->buf.top - curl->buf.bot < want)
    {
        FD_ZERO(&fdread);
        FD_ZERO(&fdwrite);
        FD_ZERO(&fdexcep);

        /* set a suitable timeout to fail on */
        timeout.tv_sec = 60; /* 1 minute */
        timeout.tv_usec = 0;

        /* get file descriptors from the transfers */
        if (0 != (e = curl_multi_fdset(multi_handle, &fdread, &fdwrite, &fdexcep, &maxfd)))
		{
			elog(ERROR, "internal error: curl_multi_fdset failed (%d - %s)", 
				 e, curl_easy_strerror(e));
			return -1;
		}

		if (maxfd <= 0)
		{
			curl->still_running = 0;
			break;
		}

        if (-1 == (nfds = select(maxfd+1, &fdread, &fdwrite, &fdexcep, &timeout))) 
		{
			if (errno == EINTR || errno == EAGAIN)
				continue;
			elog(ERROR, "internal error: select failed on curl_multi_fdset (maxfd %d) (%d - %s)",
				 maxfd, errno, strerror(errno));
			return -1;
		}

		if (nfds > 0) 
		{
            /* timeout or readable/writable sockets */
            /* note we *could* be more efficient and not wait for
             * CURLM_CALL_MULTI_PERFORM to clear here and check it on re-entry
             * but that gets messy */
			while (CURLM_CALL_MULTI_PERFORM == 
				   (e = curl_multi_perform(multi_handle, &file->u.curl.still_running)));
			if (e != 0) 
			{
				elog(ERROR, "internal error: curl_multi_perform failed (%d - %s)", 
					 e, curl_easy_strerror(e));
				return -1;
			}
        }

		/* elog(NOTICE, "- still_running %d, bot %d, top %d, want %d", 
		   file->u.curl.still_running, curl->buf.bot, curl->buf.top, want);
		*/
    } 
    return 0;
}



static int
set_httpheader(URL_FILE *fcurl, const char *name, const char *value)
{
	char tmp[1024];

	if (strlen(name) + strlen(value) + 5 > sizeof(tmp))
	{
		elog(ERROR, "set_httpheader name/value is too long. name = %s, value=%s", name, value);
		return -1;
	}
	sprintf(tmp, "%s: %s", name, value);
	fcurl->u.curl.x_httpheader = curl_slist_append(fcurl->u.curl.x_httpheader, tmp);

	return 0;
}


static int
make_export(char *name, const char *value, char *buf)
{
	char *p = buf;
	int ch;

	if (buf)
		strcpy(p, name);
	p += strlen(name);

	if (buf)
		strcpy(p, "='");
	p += 2;

	for ( ; 0 != (ch = *value); value++) 
	{
		if (ch == '\'' || ch == '\\')
		{
			if (buf)
				*p = '\\';
			p++;
		}
		if (buf)
			*p = ch;
		p++;
	}

	if (buf)
		strcpy(p, "' && export ");
	p += strlen("' && export ");
	
	if (buf)
		strcpy(p, name);
	p += strlen(name);

	if (buf)
		strcpy(p, " && ");
	p += 4;
	
	return p - buf;
}


static int
make_command(const char *cmd, extvar_t * ev, char *buf)
{
	int sz = 0;

	sz += make_export("GP_MASTER_HOST", ev->GP_MASTER_HOST, buf ? buf + sz : 0);
	sz += make_export("GP_MASTER_PORT", ev->GP_MASTER_PORT, buf ? buf + sz : 0);
	sz += make_export("GP_SEG_PG_CONF", ev->GP_SEG_PG_CONF, buf ? buf + sz : 0);
	sz += make_export("GP_SEG_DATADIR", ev->GP_SEG_DATADIR, buf ? buf + sz : 0);
	sz += make_export("GP_DATABASE", ev->GP_DATABASE, buf ? buf + sz : 0);
	sz += make_export("GP_USER", ev->GP_USER, buf ? buf + sz : 0);
	sz += make_export("GP_DATE", ev->GP_DATE, buf ? buf + sz : 0);
	sz += make_export("GP_TIME", ev->GP_TIME, buf ? buf + sz : 0);
	sz += make_export("GP_XID", ev->GP_XID, buf ? buf + sz : 0);
	sz += make_export("GP_CID", ev->GP_CID, buf ? buf + sz : 0);
	sz += make_export("GP_SN", ev->GP_SN, buf ? buf + sz : 0);
	sz += make_export("GP_SEGMENT_ID", ev->GP_SEGMENT_ID, buf ? buf + sz : 0);
	sz += make_export("GP_SEG_PORT", ev->GP_SEG_PORT, buf ? buf + sz : 0);
	sz += make_export("GP_SESSION_ID", ev->GP_SESSION_ID, buf ? buf + sz : 0);
	sz += make_export("GP_SEGMENT_COUNT", ev->GP_SEGMENT_COUNT, buf ? buf + sz : 0);
	
	if (buf)
		strcpy(buf + sz, cmd);
	sz += strlen(cmd);

	return sz + 1;				/* add NUL terminator */
}


static char *
local_strstr(const char *str1, const char *str2)
{
	char *cp = (char *) str1;
	char *s1, *s2;

	if ( !*str2 )
		return((char *)str1);

	while (*cp)
    {
		s1 = cp;
		s2 = (char *) str2;

		while (*s1 && (*s1==*s2))
			s1++, s2++;

		if (!*s2)
			return(cp);

		cp++;
	}

	return(NULL);
}

static int
make_url(const char *url, extvar_t *ev, char *buf)
{
	char *authority_start = local_strstr(url, "//");
	char *authority_end;
	char *hostname_start;
	char *hostname_end;
	char hostname[100];
	char hostip[20];
	struct hostent * he;
	int len;
	char *p;

	if (! authority_start)
	{
		elog(ERROR, "illegal url '%s'", url);
		return -1;
	}

	authority_start += 2;
	authority_end = strchr(authority_start, '/');
	if (! authority_end)
		authority_end = authority_start + strlen(authority_start);

	hostname_start = strchr(authority_start, '@');
	if (! (hostname_start && hostname_start < authority_end))
		hostname_start = authority_start;
	
	hostname_end = strchr(hostname_start, ':');
	if (! (hostname_end && hostname_end < authority_end))
		hostname_end = authority_end;

	if (hostname_end - hostname_start >= sizeof(hostname))
	{
		elog(ERROR, "hostname too long for url '%s'", url);
		return -1;
	}

	memcpy(hostname, hostname_start, hostname_end - hostname_start);
	hostname[hostname_end - hostname_start] = 0;
	he = gethostbyname(hostname);
	if (he)
	{
		strcpy(hostip, inet_ntoa(*(struct in_addr*)he->h_addr));
		/* elog(NOTICE, "IP: %s", hostip); */
	} 
	else
	{
#if 0
		char line[1024];
		FILE* fp;
		int a, b, c, d;

		snprintf(line, sizeof(line), 
				 "python -c 'from socket import *; print gethostbyname(\"%s\") ' 2> /tmp/tt",
				 hostname);
		line[sizeof(line)-1] = 0;
		if (! (fp = popen(line, "r"))) {
			line[0] = 0;
		}
		else {
			if (0 == fgets(line, sizeof(line), fp)) 
				line[0] = 0;
			pclose(fp);
			line[sizeof(line)-1] = 0;
		}
		if (4 != sscanf(line, "%d.%d.%d.%d", &a, &b, &c, &d)) {
			elog(ERROR, "unable to resolve host '%s' in url '%s'", hostname, url);
			return -1;
		}
		sprintf(hostip, "%d.%d.%d.%d", a, b, c, d);
		/* elog(NOTICE, "IP: %s", hostip); */
#else
		elog(ERROR, "unable to resolve host '%s' in url '%s'", hostname, url);
		return -1;
#endif
	}

	if (!buf)
		return strlen(url) + 1 - strlen(hostname) + strlen(hostip);

	p = buf;
	len = hostname_start - url;
	strncpy(p, url, len); p += len; url += len;

	len = strlen(hostname);
	url += len;

	len = strlen(hostip);
	strncpy(p, hostip, len); p += len;

	strcpy(p, url);
	return p - buf;
}

URL_FILE *
url_fopen(char *url, const char *operation, extvar_t *ev, CopyState pstate, int *response_code, const char **response_string)
{
    /* this code checks for URLs or types in the 'url' and
     * basicly use the real fopen() for standard files, or
	 * if the url happens to be a command to execute it uses
	 * popen to execute it.
	 */
	
	/* an EXECUTE string will always be prefixed like this */
	const char *exec_prefix = "execute:";
	
	/*char  sessionIDBuf[80];*/
	URL_FILE *file;
	int e;
    (void)operation;
	

    if (! (file = (URL_FILE *)malloc(sizeof(URL_FILE) + strlen(url) + 1)))
	{
		elog(ERROR, "out of memory");
		*response_code = 500;
		*response_string = "Out of memory";
		return NULL;
	}
    memset(file, 0, sizeof(URL_FILE));

	file->url = ((char *) file) + sizeof(URL_FILE);
	strcpy(file->url, url);

	/* 
	 * if 'url' starts with "execute:" then it's a command to execute and 
	 * not a url (the command specified in CREATE EXTERNAL TABLE .. EXECUTE) 
	 */
	if (pg_strncasecmp(url, exec_prefix, strlen(exec_prefix)) == 0)
	{
		char *cmd = url + strlen(exec_prefix);
		int sz = make_command(cmd, ev, 0);
		char *shexec = palloc(sz);
		make_command(cmd, ev, shexec);
 		
		file->type = CFTYPE_EXEC; /* marked as a EXEC */	
		file->u.exec.fp = popen(shexec, "r");
		file->u.exec.shexec = shexec;
		if (file->u.exec.fp == NULL)
			elog( ERROR, "External Table error executing cmd: '%s'", cmd );
	}
    else if (IS_FILE_URI(url))
    {
		char *path = strchr(url + strlen(PROTOCOL_FILE), '/');
		struct fstream_options fo;
		memset(&fo,0,sizeof fo);
		if (!path) 
		{
			elog(ERROR, "External Table error opening file: '%s'", url);
			*response_code = 400;
			*response_string = "Bad Request";
			return NULL;
		}

		file->type = CFTYPE_FILE; /* marked as local FILE */
		fo.is_csv = pstate->csv_mode;
		fo.quote = pstate->quote?*pstate->quote:0;
		fo.escape = pstate->escape?*pstate->escape:0;
		fo.header = pstate->header_line;
		pstate->header_line = 0;
		file->u.file.fp = fstream_open(path,&fo,response_code,response_string);
		
		/* couldn't open local file. return NULL to display proper error */
		if (!file->u.file.fp)
		{
			free(file);
			return NULL;
		}
    }
    else if (IS_HTTP_URI(url) || IS_GPFDIST_URI(url) || IS_GPFDISTS_URI(url))
    {
		int sz = make_url(file->url, ev, 0);

        file->type = CFTYPE_CURL; /* marked as URL */
		if (sz < 0) 
		{
			elog(ERROR, "illegal URL: %s", file->url);
			url_fclose(file);
			*response_code = 400;
			*response_string = "Bad Request";
			return NULL;
		}

		file->u.curl.curl_url = palloc(sz);
		make_url(file->url, ev, file->u.curl.curl_url);
		
		if (IS_GPFDIST_URI(file->u.curl.curl_url) || IS_GPFDISTS_URI(file->u.curl.curl_url))
		{
			/* replace gpfdist:// with http:// */
			file->u.curl.curl_url += 3;
			memcpy(file->u.curl.curl_url, "http", 4);
			pstate->header_line = 0;
		}

        if (! (file->u.curl.handle = curl_easy_init()))
		{
			elog(ERROR, "internal error: curl_easy_init failed");
			url_fclose(file);
			*response_code = 500;
			*response_string = "curl_easy_init failed";
			return NULL;
		}

		/* elog(NOTICE, "URL: %s", file->u.curl.curl_url); */

        if (0 != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_URL, file->u.curl.curl_url))) 
		{
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_URL %s error (%d - %s)",
				 file->u.curl.curl_url, 
				 e, curl_easy_strerror(e));
			url_fclose(file);
			*response_code = 500;
			*response_string = "curl_easy_setopt failed";
			return NULL;
		}
        if (0 != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_VERBOSE, FALSE))) 
		{
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_VERBOSE error (%d - %s)", 
				 e, curl_easy_strerror(e));
			url_fclose(file);
			*response_code = 500;
			*response_string = "curl_easy_setopt failed";
			return NULL;
		}
		/* set callback for each header received from server */
		if (0 != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_HEADERFUNCTION, header_callback)))
		{
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_HEADERFUNCTION error (%d - %s)", 
				 e, curl_easy_strerror(e));
			url_fclose(file);
			*response_code = 500;
			*response_string = "curl_easy_setopt failed";
			return NULL;
		}
		/* file gets passed to header_callback */
		if (0 != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_WRITEHEADER, file)))
		{
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_WRITEHEADER error (%d - %s)",
				 e, curl_easy_strerror(e));
			url_fclose(file);
			*response_code = 500;
			*response_string = "curl_easy_setopt failed";
			return NULL;
		}
		/* set callback for each data block received from server */
        if (0 != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_WRITEFUNCTION, write_callback)))
		{
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_WRITEFUNCTION error (%d - %s)", 
				 e, curl_easy_strerror(e));
			url_fclose(file);
			*response_code = 500;
			*response_string = "curl_easy_setopt failed";
			return NULL;
		}
		/* file gets passed to write_callback */
        if (0 != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_WRITEDATA, file)))
		{
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_WRITEDATA error (%d - %s)",
				 e, curl_easy_strerror(e));
			url_fclose(file);
			*response_code = 500;
			*response_string = "curl_easy_setopt failed";
			return NULL;
		}
		if (0 != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4)))
		{
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_IPRESOLVE error (%d - %s)",
				 e, curl_easy_strerror(e));
			url_fclose(file);
			*response_code = 500;
			*response_string = "curl_easy_setopt failed";
			return NULL;
		}
	
		file->u.curl.x_httpheader = NULL;
		if (set_httpheader(file, "X-GP-MASTER_HOST", ev->GP_MASTER_HOST) ||
			set_httpheader(file, "X-GP-MASTER_PORT", ev->GP_MASTER_PORT) ||
			set_httpheader(file, "X-GP-CSVOPT", ev->GP_CSVOPT) ||
			set_httpheader(file, "X-GP_SEG_PG_CONF", ev->GP_SEG_PG_CONF) ||
			set_httpheader(file, "X-GP_SEG_DATADIR", ev->GP_SEG_DATADIR) ||
			set_httpheader(file, "X-GP-DATABASE", ev->GP_DATABASE) ||
			set_httpheader(file, "X-GP-USER", ev->GP_USER) ||
			set_httpheader(file, "X-GP-XID", ev->GP_XID) ||
			set_httpheader(file, "X-GP-CID", ev->GP_CID) ||
			set_httpheader(file, "X-GP-SN", ev->GP_SN) ||
			set_httpheader(file, "X-GP-PROTO", "1") ||
			set_httpheader(file, "X-GP-SEGMENT-ID", ev->GP_SEGMENT_ID) ||
			set_httpheader(file, "X-GP-SEG-PORT", ev->GP_SEG_PORT) ||
			set_httpheader(file, "X-GP-SESSION-ID", ev->GP_SESSION_ID) ||
			set_httpheader(file, "X-GP-SEGMENT-COUNT", ev->GP_SEGMENT_COUNT))
		{
			elog(ERROR, "internal error: some variable value is too long");
			url_fclose(file);
			*response_code = 500;
			*response_string = "Internal Server Error";
			return NULL;
		}

		if (0 != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_HTTPHEADER, file->u.curl.x_httpheader)))
		{
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_WRITEDATA error (%d - %s)",
				 e, curl_easy_strerror(e));
			url_fclose(file);
			*response_code = 500;
			*response_string = "curl_easy_setopt failed";
			return NULL;
        }

        if (!multi_handle) 
		{
            if (! (multi_handle = curl_multi_init()))
			{
				elog(ERROR, "internal error: curl_multi_init failed");
				url_fclose(file);
				*response_code = 500;
				*response_string = "curl_multi_init failed";
				return NULL;
			}
		}

        if (0 != (e = curl_multi_add_handle(multi_handle, file->u.curl.handle)))
		{
			if (CURLM_CALL_MULTI_PERFORM != e)
			{
				elog(ERROR, "internal error: curl_multi_add_handle failed (%d - %s)", 
					 e, curl_easy_strerror(e));
				url_fclose(file);
				*response_code = 500;
				*response_string = "curl_multi_init_add_handle failed";
				return NULL;
			}
		}

        /* lets start the fetch */
		while (CURLM_CALL_MULTI_PERFORM == 
			   (e = curl_multi_perform(multi_handle, &file->u.curl.still_running)));

		if (e != 0) 
		{
			elog(ERROR, "internal error: curl_multi_perform failed (%d - %s)",
				 e, curl_easy_strerror(e));
			url_fclose(file);
			*response_code = 500;
			*response_string = "curl_multi_perform failed";
			return NULL;
		}
		
		/* read some bytes to make sure the connection is established */
		fill_buffer(file, 1);

		/* check the connection */
		if (check_response(file,response_code,response_string))
		{
			url_fclose(file);
			return NULL;
		}
    }
    return file;
}

int
url_fclose(URL_FILE *file)
{
    int ret=0;/* default is good return */

    switch (file->type)
    {
		case CFTYPE_FILE:
			fstream_close(file->u.file.fp);
			break;

		case CFTYPE_EXEC:
			ret = pclose(file->u.exec.fp);
			break;

		case CFTYPE_CURL:
			if (file->u.curl.x_httpheader)
			{
				curl_slist_free_all(file->u.curl.x_httpheader);
				file->u.curl.x_httpheader = NULL;
			}

			/* make sure the easy handle is not in the multi handle anymore */
			if (file->u.curl.handle)
			{
				curl_multi_remove_handle(multi_handle, file->u.curl.handle);
				/* cleanup */
				curl_easy_cleanup(file->u.curl.handle);
				file->u.curl.handle = NULL;
			}

			/* free any allocated buffer space */
			if (file->u.curl.buf.ptr)
			{
				free(file->u.curl.buf.ptr);
				file->u.curl.buf.ptr = NULL;
			}
			file->u.curl.gp_proto = 0;
			file->u.curl.error = file->u.curl.eof = 0;
			memset(&file->u.curl.buf, 0, sizeof(file->u.curl.buf));
			memset(&file->u.curl.block, 0, sizeof(file->u.curl.block));
			break;

		default: /* unknown or supported type - oh dear */
			ret=EOF;
			errno=EBADF;
			break;
    }

    free(file);

    return ret;
}

int
url_feof(URL_FILE *file)
{
    int ret = 0;

    switch (file->type)
    {
		case CFTYPE_FILE:
			ret = fstream_eof(file->u.file.fp);
			break;

		case CFTYPE_EXEC:
			ret = feof(file->u.exec.fp);
			break;    	

		case CFTYPE_CURL:
			ret = file->u.curl.eof;
			break;

		default: /* unknown or supported type - oh dear */
			ret = -1;
			errno = EBADF;
			break;
    }
    return ret;
}


int url_ferror(URL_FILE *file)
{
	int ret = 0;

	switch (file->type)
	{
		case CFTYPE_FILE:
			ret = fstream_get_error(file->u.file.fp)!=0;
			break;

		case CFTYPE_EXEC:
			ret = ferror(file->u.exec.fp);
			break;    	

		case CFTYPE_CURL:
			ret = file->u.curl.error ? -1 : 0;
			break;

		default: /* unknown or supported type - oh dear */
			ret = -1;
			errno = EBADF;
			break;
	}

	return ret;
}


static size_t gp_proto0_read(char *buf, int bufsz, URL_FILE* file)
{
	int n = 0;
	curlctl_t* curl = &file->u.curl;

	fill_buffer(file, bufsz);

	/* check if there's data in the buffer - if not fill_buffer()
	 * either errored or EOF. For proto0, we cannot distinguish
	 * between error and EOF. */
	n = curl->buf.top - curl->buf.bot;
	if (n == 0 && !curl->still_running) 
		curl->eof = 1;

	if (n > bufsz)
		n = bufsz;

	/* xfer data to caller */
	memcpy(buf, curl->buf.ptr, n);
	curl->buf.bot += n;
	return n;
}


static size_t gp_proto1_read(char *buf, int bufsz, URL_FILE *file, CopyState pstate, char *buf2)
{
	char type;
	int  n, len;
	curlctl_t* curl = &file->u.curl;

	while (curl->block.datalen == 0 && !curl->eof) {

		/* need 5 bytes, 1 byte type + 4 bytes length */
		fill_buffer(file, 5);
		n = curl->buf.top - curl->buf.bot;
		if (n == 0)
		{
			elog(ERROR, "gpfdist error: server closed connection.\n");
			return -1;
		}
		if (n < 5)
		{
			elog(ERROR, "gpfdist error: incomplete packet - packet len %d\n", n);
			return -1;
		}

		/* read type */
		type = curl->buf.ptr[curl->buf.bot++];

		/* read len */
		memcpy(&len, &curl->buf.ptr[curl->buf.bot], 4);
		len = ntohl(len);		/* change order */
		curl->buf.bot += 4;
		if (len < 0)
		{
			elog(ERROR, "gpfdist error: bad packet type %d len %d", 
				 type, len);
			return -1;
		}

		/* elog(NOTICE, "HEADER %c %d, bot %d, top %d", type, len, 
		   curl->buf.bot, curl->buf.top);
		*/

		if (type == 'E')
		{
			fill_buffer(file, len);
			n = curl->buf.top - curl->buf.bot;
			if (n > len) 
				n = len;
			if (n > 0)
			{
				/*
				 * cheat a little. swap last char and
				 * NUL-terminator. then print string (without last
				 * char) and print last char artificially
				 */
				char x = curl->buf.ptr[curl->buf.bot + n - 1];
				curl->buf.ptr[curl->buf.bot + n - 1] = 0;
				elog(ERROR, "gpfdist error - %s%c", &curl->buf.ptr[curl->buf.bot], x);

				return -1;
			}
			elog(ERROR, "gpfdist error: please check gpfdist log messages.");

			return -1;
		}

		if (type == 'F')
		{
			if (buf != buf2)
			{
				curl->buf.bot -= 5;
				return 0;
			}
			if (len > 256)
			{
				elog(ERROR, "gpfdist error: filename too long (%d)", len);
				return -1;
			}
			if (-1 == fill_buffer(file, len))
			{
				elog(ERROR, "gpfdist error: stream ends suddenly");
				return -1;
			}

			if (pstate->cdbsreh)
			{
				char fname[257];

				memcpy(fname,curl->buf.ptr+curl->buf.bot,len);
				fname[len] = 0;
				snprintf(pstate->cdbsreh->filename,sizeof pstate->cdbsreh->filename,"%s [%s]",pstate->filename,fname);
			}
			curl->buf.bot += len;
			Assert(curl->buf.bot <= curl->buf.top);
			continue;
		}

		if (type == 'O')
		{
			if (len != 8)
			{
				elog(ERROR, "gpfdist error: offset not of length 8 (%d)", len);
				return -1;
			}
			if (-1 == fill_buffer(file, len))
			{
				elog(ERROR, "gpfdist error: stream ends suddenly");
				return -1;
			}
			curl->buf.bot += 8;
			Assert(curl->buf.bot <= curl->buf.top);
			continue;
		}
		if (type == 'L')
		{
			int64_t line_number;

			if (len != 8)
			{
				elog(ERROR, "gpfdist error: line number not of length 8 (%d)", len);
				return -1;
			}
			if (-1 == fill_buffer(file, len))
			{
				elog(ERROR, "gpfdist error: stream ends suddenly");
				return -1;
			}
			memcpy(&line_number,curl->buf.ptr+curl->buf.bot,len);
			line_number = local_ntohll(line_number);
			pstate->cur_lineno = line_number?line_number-1:(int64_t)-1<<63;
			curl->buf.bot += 8;
			Assert(curl->buf.bot <= curl->buf.top);
			continue;
		}

		if (type == 'D')
		{
			curl->block.datalen = len;
			curl->eof = (len == 0);
			// elog(NOTICE, "D %d", curl->block.datalen);
			break;
		}

		elog(ERROR, "gpfdist error: unknown meta type %d", type);
		return -1;
	}

	/* read data block */
	if (bufsz > curl->block.datalen)
		bufsz = curl->block.datalen;
	
	n = curl->buf.top - curl->buf.bot;
	// elog(NOTICE, "n = %d", n);
	fill_buffer(file, bufsz);
	n = curl->buf.top - curl->buf.bot;
	if (n == 0 && !curl->eof)
	{
		//elog(NOTICE, "bufsz = %d, not eof -> error", bufsz);
		curl->error = 1;
	}

	if (n > bufsz)
		n = bufsz;
	memcpy(buf, curl->buf.ptr + curl->buf.bot, n);
	curl->buf.bot += n;
	curl->block.datalen -= n;
	// elog(NOTICE, "returning %d bytes, %d bytes left \n", n, curl->block.datalen);
	return n;
}


static size_t curl_fread(char *buf, int bufsz, URL_FILE* file,CopyState pstate)
{
	curlctl_t *curl = &file->u.curl;
	char *p = buf;
	char *q = buf + bufsz;
	int n;
	const int gp_proto = curl->gp_proto;
	
	if (gp_proto != 0 && gp_proto != 1)
	{
		elog(ERROR, "unknown gp protocol %d", curl->gp_proto);
		return 0;
	}

	for (; p < q; p += n)
	{
		if (gp_proto == 0)
			n = gp_proto0_read(p, q - p, file);
		else
			n = gp_proto1_read(p, q - p, file,pstate,buf);
		
		//elog(NOTICE, "curl_fread %d bytes", n);

		if (n <= 0)
			break;
	}
	return p - buf;
}


size_t
url_fread(void *ptr, size_t size, size_t nmemb, URL_FILE *file, CopyState pstate)
{
    size_t want;
	int n;

    switch (file->type)
    {
		case CFTYPE_FILE:
		{
			struct fstream_filename_and_offset fo;

			assert(size==1);
			n = fstream_read(file->u.file.fp,ptr,nmemb,&fo,0);
			want = n > 0 ? n : 0;
			if (n > 0 && fo.line_number)
			{
				pstate->cur_lineno = fo.line_number-1;
				if (pstate->cdbsreh)
					snprintf(pstate->cdbsreh->filename,sizeof pstate->cdbsreh->filename,"%s [%s]",pstate->filename,fo.fname);
			}
			break;
		}
		case CFTYPE_EXEC:
			want = fread(ptr, size, nmemb, file->u.exec.fp);
			break;
    	
		case CFTYPE_CURL:
		
			n = curl_fread(ptr, nmemb * size, file,pstate);
			/* number of items - nb correct op - checked
			 * with glibc code*/
			want = n / size;

			/*printf("(fread) return %d bytes %d left\n", want,file->u.curl.buffer_pos);*/
			break;

		default: /* unknown or supported type */
			want = 0;
			errno = EBADF;
			break;
    }

    return want;
}


void
url_rewind(URL_FILE *file)
{
	char *url = file->url;
    switch(file->type)
    {
		case CFTYPE_FILE:
			fstream_rewind(file->u.file.fp);
			break;
       
		case CFTYPE_EXEC:
			/* we'll need to execute the command again */
			assert(0); /* There is no way the following code is right. */
			url_fclose(file);

			/* most of these are null fo us. */
			url_fopen(url, NULL, NULL,NULL,NULL,0);
			break; 

		case CFTYPE_CURL:
			/* halt transaction */
			curl_multi_remove_handle(multi_handle, file->u.curl.handle);

			/* restart */
			curl_multi_add_handle(multi_handle, file->u.curl.handle);

			/* ditch buffer - write will recreate - resets stream pos*/
			if (file->u.curl.buf.ptr)
				free(file->u.curl.buf.ptr);

			file->u.curl.gp_proto = 0;
			file->u.curl.error = file->u.curl.eof = 0;
			memset(&file->u.curl.buf, 0, sizeof(file->u.curl.buf));
			memset(&file->u.curl.block, 0, sizeof(file->u.curl.block));
			break;

		default: /* unknown or supported type - oh dear */
			break;

    }
}
