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
#include <sys/wait.h>

#include "access/fileam.h"
#include "access/heapam.h"
#include "access/valid.h"
#include "catalog/pg_extprotocol.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#include "commands/dbcommands.h"
#include "libpq/libpq-be.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "port.h"
#include "postmaster/postmaster.h"  /* postmaster port*/
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/uri.h"
#include "utils/guc.h"
#include "utils/builtins.h"

#include <arpa/inet.h>
#include <fstream/gfile.h>

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <stdint.h>

#include <curl/curl.h>

#include <fstream/fstream.h>

#include "cdb/cdbsreh.h"
#include "cdb/cdbtimer.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "access/url.h"

#include "tcop/tcopprot.h"

#ifndef FALSE
#define FALSE 0
#endif

#ifndef TRUE
#define TRUE 1
#endif

#define HOST_NAME_SIZE 100

static char *interpretError(int exitCode, char *buf, size_t buflen, char *err, size_t errlen);
static const char *getSignalNameFromCode(int signo);
static int popen_with_stderr(int *rwepipe, const char *exe, bool forwrite);
static int pclose_with_stderr(int pid, int *rwepipe, char *buf, int len);
static void gp_proto0_write_done(URL_FILE *file);
static int32  InvokeExtProtocol(void		*ptr, 
								size_t 		nbytes, 
								URL_FILE 	*file, 
								CopyState 	pstate,
								bool		last_call,
								ExternalSelectDesc desc);
void extract_http_domain(char* i_path, char* o_domain, int dlen);


/* we use a global one for convenience */
CURLM *multi_handle = 0;

/*
 * header_callback
 *
 * when a header arrives from the server curl calls this routine. In here we
 * extract the information we are interested in from the header, and store it
 * in the passed in callback argument (URL_FILE *) which lives in our
 * application.
 */
static size_t
header_callback(void *ptr_, size_t size, size_t nmemb, void *userp)
{
    URL_FILE*	url = (URL_FILE *)userp;
	char*		ptr = ptr_;
	int 		len = size * nmemb;
	int 		i;
	char 		buf[20];

	Assert(size == 1);

	/*
	 * parse the http response line (code and message) from
	 * the http header that we get. Basically it's the whole
	 * first line (e.g: "HTTP/1.0 400 time out"). We do this
	 * in order to capture any error message that comes from
	 * gpfdist, and later use it to report the error string in
	 * check_response() to the database user.
	 */
	if (url->u.curl.http_response == 0)
	{
		int 	n = nmemb;
		char* 	p;

		if (n > 0 && 0 != (p = palloc(n+1)))
		{
			memcpy(p, ptr, n);
			p[n] = 0;

			if (n > 0 && (p[n-1] == '\r' || p[n-1] == '\n'))
				p[--n] = 0;

			if (n > 0 && (p[n-1] == '\r' || p[n-1] == '\n'))
				p[--n] = 0;

			url->u.curl.http_response = p;
		}
	}

	/*
	 * extract the GP-PROTO value from the HTTP header.
	 */
	if (len > 10 && *ptr == 'X' && 0 == strncmp("X-GP-PROTO", ptr, 10))
	{
		ptr += 10;
		len -= 10;

		while (len > 0 && (*ptr == ' ' || *ptr == '\t'))
		{
			ptr++;
			len--;
		}

		if (len > 0 && *ptr == ':')
		{
			ptr++;
			len--;

			while (len > 0 && (*ptr == ' ' || *ptr == '\t'))
			{
				ptr++;
				len--;
			}

			for (i = 0; i < sizeof(buf) - 1 && i < len; i++)
				buf[i] = ptr[i];

			buf[i] = 0;
			url->u.curl.gp_proto = strtol(buf, 0, 0);

			// elog(NOTICE, "X-GP-PROTO: %s (%d)", buf, url->u.curl.gp_proto);
		}
	}

	return size * nmemb;
}


/*
 * write_callback
 *
 * when data arrives from gpfdist server and curl is ready to write it
 * to our application, it calls this routine. In here we will store the
 * data in the application variable (URL_FILE *)file which is the passed 
 * in the fourth argument as a part of the callback settings.
 *
 * we return the number of bytes written to the application buffer
 */
static size_t
write_callback(char *buffer,
               size_t size,
               size_t nitems,
               void *userp)
{
    URL_FILE*	file = (URL_FILE *)userp;
	curlctl_t*	curl = &file->u.curl;
	const int 	nbytes = size * nitems;
	int 		n;

	//elog(NOTICE, "write_callback %d", nbytes);

	/*
	 * if insufficient space in buffer make more space
	 */
	if (curl->in.top + nbytes >= curl->in.max)
	{
		/* compact ? */
		if (curl->in.bot)
		{
			n = curl->in.top - curl->in.bot;
			memmove(curl->in.ptr, curl->in.ptr + curl->in.bot, n);
			curl->in.bot = 0;
			curl->in.top = n;
		}

		/* if still insufficient space in buffer, then do realloc */
		if (curl->in.top + nbytes >= curl->in.max)
		{
			char *newbuf;

			n = curl->in.top - curl->in.bot + nbytes + 1024;
			newbuf = realloc(curl->in.ptr, n);

			if (!newbuf)
			{
				elog(ERROR, "out of memory (curl write_callback)");
			}

			curl->in.ptr = newbuf;
			curl->in.max = n;
			// elog(NOTICE, "max now at %d", n);

			Assert(curl->in.top + nbytes < curl->in.max);
		}
	}

	/* enough space. copy buffer into curl->buf */
	memcpy(curl->in.ptr + curl->in.top, buffer, nbytes);
	curl->in.top += nbytes;

	return nbytes;
}

/*
 * check_response
 *
 * If got an HTTP response with an error code from the server (gpfdist), report
 * the error code and message it to the database user and abort operation.
 */
static int
check_response(URL_FILE *file, int *rc, const char **response_string)
{
	long 		response_code;
	char*		effective_url = NULL;
	CURL* 		curl = file->u.curl.handle;
	static char buffer[30];

	/* get the response code from curl */
	if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code) != CURLE_OK)
	{
		*rc = 500;
		*response_string = "curl_easy_getinfo failed";
		return -1;
	}
	*rc = response_code;
	snprintf(buffer, sizeof buffer, "Response Code=%d", (int)response_code);
	*response_string = buffer;

	if (curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &effective_url) != CURLE_OK)
		return -1;

	if (! (200 <= response_code && response_code < 300))
	{
		if (response_code == 0)
		{
			long 		oserrno = 0;
			static char	connmsg[64];
			
			/* get the os level errno, and string representation of it */
			if (curl_easy_getinfo(curl, CURLINFO_OS_ERRNO, &oserrno) == CURLE_OK)
			{
				if (oserrno != 0)
					snprintf(connmsg, sizeof connmsg, "error code = %d (%s)", 
							 (int) oserrno, strerror((int)oserrno));
			}
			
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("connection with gpfdist failed for %s. "
									"effective url: %s. %s", file->url, effective_url,
									(oserrno != 0 ? connmsg : "")),
						    errOmitLocation(true)));
		}
		else
		{
			/* we need to sleep 1 sec to avoid this condition:
			   1- seg X gets an error message from gpfdist
			   2- seg Y gets a 500 error
			   3- seg Y report error before seg X, and error message
			   in seg X is thrown away.
			*/
			pg_usleep(1000000);
			elog(ERROR, "http response code %ld from gpfdist (%s): %s",
				 response_code, file->url,
				 file->u.curl.http_response ? file->u.curl.http_response : "?");
		}
	}

	return 0;
}


/*
 * fill_buffer
 *
 * Attempt to fill the read buffer up to requested number of bytes.
 * We first check if we already have the number of bytes that we
 * want already in the buffer (from write_callback), and we do
 * a select on the socket only if we don't have enough.
 * 
 * return 0 if successful; raises ERROR otherwise.
 */
static int
fill_buffer(URL_FILE *file, int want)
{
    fd_set 	fdread;
    fd_set 	fdwrite;
    fd_set 	fdexcep;
    int 	maxfd;
    struct 	timeval timeout;
    int 	nfds, e;
	curlctl_t* curl = &file->u.curl;

	/* elog(NOTICE, "= still_running %d, bot %d, top %d, want %d",
	   file->u.curl.still_running, curl->in.bot, curl->in.top, want);
	*/

    /* attempt to fill buffer */
	while (curl->still_running && curl->in.top - curl->in.bot < want)
    {
        FD_ZERO(&fdread);
        FD_ZERO(&fdwrite);
        FD_ZERO(&fdexcep);

		CHECK_FOR_INTERRUPTS();

        /* set a suitable timeout to fail on */
        timeout.tv_sec = 5;
        timeout.tv_usec = 0;

        /* get file descriptors from the transfers */
        if (0 != (e = curl_multi_fdset(multi_handle, &fdread, &fdwrite, &fdexcep, &maxfd)))
		{
			elog(ERROR, "internal error: curl_multi_fdset failed (%d - %s)",
						e, curl_easy_strerror(e));
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
			}
        }

		/* elog(NOTICE, "- still_running %d, bot %d, top %d, want %d",
		   file->u.curl.still_running, curl->in.bot, curl->in.top, want);
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

/*
 * This function purpose is to make sure that the URL string contains a numerical IP address.
 * The input URL is in the parameter url. The output result URL is in the output parameter - buf.
 * When parameter - url already contains a numerical ip, then output parameter - buf will be a copy of url.
 * For this case calling getDnsAddress method inside make_url, will serve the purpose of IP validation.
 * But when parameter - url will contain a domain name, then the domain name substring will be changed to a numerical
 * ip address in the buf output parameter.
 */
static int
make_url(const char *url, char *buf, bool is_ipv6)
{
	char *authority_start = local_strstr(url, "//");
	char *authority_end;
	char *hostname_start;
	char *hostname_end;
	char hostname[HOST_NAME_SIZE];
	char *hostip = NULL;
	char portstr[9];
	int len;
	char *p;
	int port = 80; /* default for http */
	bool  domain_resolved_to_ipv6 = false;
		
	if (! authority_start)
	{
		elog(ERROR, "illegal url '%s'", url);
	}
	
	authority_start += 2;
	authority_end = strchr(authority_start, '/');
	if (! authority_end)
		authority_end = authority_start + strlen(authority_start);
	
	hostname_start = strchr(authority_start, '@');
	if (! (hostname_start && hostname_start < authority_end))
		hostname_start = authority_start;
	
	
	if ( is_ipv6 ) /* IPV6 */
	{
		int len;
		
		hostname_end = strchr(hostname_start, ']');
		hostname_end += 1;
		
		/* port number exists in this url. get it */
		len = authority_end - hostname_end;
		if (len > 8)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("<port> substring size must not exceed %d characters", 8)));
		
		memcpy(portstr, hostname_end + 1, len);
		portstr[len] = 0;
		port = atoi(portstr);
		
		/* skippping the brackets */
		hostname_end -=1;
		hostname_start +=1;
	}
	else 
	{
		hostname_end = strchr(hostname_start, ':');
		if (! (hostname_end && hostname_end < authority_end))
		{
			hostname_end = authority_end;
		}
		else
		{
			/* port number exists in this url. get it */
			int len = authority_end - hostname_end;
			if (len > 8)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("<port> substring size must not exceed %d characters", 8)));			
			
			memcpy(portstr, hostname_end + 1, len);
			portstr[len] = 0;
			port = atoi(portstr);
		}
	}
	
	if ( !port ) 
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("<port> substring must contain only digits")));	
	
	if (hostname_end - hostname_start >= sizeof(hostname))
	{
		elog(ERROR, "hostname too long for url '%s'", url);
	}
	
	memcpy(hostname, hostname_start, hostname_end - hostname_start);
	hostname[hostname_end - hostname_start] = 0;
	
	hostip = getDnsAddress(hostname, port, ERROR);
	
	/*
	 * test for the case where the URL originaly contained a domain name (so is_ipv6 was set to false)
	 * but the DNS resolution in getDnsAddress returned an IPv6 address so know we also have to put the
	 * square brackets [..] in the URL string.
	 */
	if ( strchr(hostip, ':') != NULL && !is_ipv6 ) /* hit the case where a domain name returned an IPv6 address */
		domain_resolved_to_ipv6 = true;
	 
	
	if (!buf)
	{
		int len = strlen(url) + 1 - strlen(hostname) + strlen(hostip);
		if ( domain_resolved_to_ipv6 )
			len += 2; /* for the square brackets */
		return len;
	}
	
	p = buf;
	len = hostname_start - url;
	strncpy(p, url, len); 
	p += len; 
	url += len;
	
	len = strlen(hostname);
	url += len;
	
	len = strlen(hostip);
	if ( domain_resolved_to_ipv6 )
	{
		*p = '[';
		p++;
	}
	strncpy(p, hostip, len); 
	p += len;
	if ( domain_resolved_to_ipv6 )
	{
		*p = ']';
		p++;
	}	
	
	strcpy(p, url);
	return p - buf;
}

/*
 * extract_http_domain
 *
 * extracts the domain string from a http url
 */
void extract_http_domain(char* i_path, char* o_domain, int dlen)
{
	int domsz, cpsz;
	char* p_st = (char*)local_strstr(i_path, "//");
	p_st = p_st + 2;
	char* p_en = strchr(p_st, '/');
	
	domsz = p_en - p_st;
	cpsz = ( domsz < dlen ) ? domsz : dlen;
	memcpy(o_domain, p_st, cpsz);
}

/**
 * execute_fopen()
 *
 * refactor the fopen code for execute into this routine
 */
URL_FILE *
url_execute_fopen(char* url, char *cmd, bool forwrite, extvar_t *ev)
{
	URL_FILE*      file;
	int            save_errno;
	struct itimers savetimers;
	pqsigfunc      save_SIGPIPE;

	int 	sz;
	char*	shexec;

    if (! (file = (URL_FILE *)malloc(sizeof(URL_FILE) + strlen(url) + 1)))
	{
		elog(ERROR, "out of memory");
	}
    memset(file, 0, sizeof(URL_FILE));
	file->url = ((char *) file) + sizeof(URL_FILE);
	strcpy(file->url, url);

	/* Execute command */
	sz     = make_command(cmd, ev, 0);
	shexec = palloc(sz);
	make_command(cmd, ev, shexec);

	file->type = CFTYPE_EXEC; /* marked as a EXEC */
	file->u.exec.shexec = shexec;

	/* Clear process interval timers */
	resetTimers(&savetimers);

	/*
	 * Preserve the SIGPIPE handler and set to default handling.  This
	 * allows "normal" SIGPIPE handling in the command pipeline.  Normal
	 * for PG is to *ignore* SIGPIPE.
	 */
	save_SIGPIPE = pqsignal(SIGPIPE, SIG_DFL);

	/* execute the user command */
	file->u.exec.pid = popen_with_stderr(file->u.exec.pipes, shexec, forwrite);

	save_errno = errno;

	/* Restore the SIGPIPE handler */
	pqsignal(SIGPIPE, save_SIGPIPE);

	/* Restore process interval timers */
	restoreTimers(&savetimers);

	if (file->u.exec.pid == -1)
	{
		errno = save_errno;
		free(file);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
						errmsg("cannot start external table command: %m"),
						errdetail("Command: %s", cmd),
						errOmitLocation(true)));

	}

	return file;
}

static bool 
url_has_ipv6_format (char *url)
{
	bool is6 = false;
	char *ipv6 = local_strstr(url, "://[");
	
	if ( ipv6 )
		ipv6 = strchr(ipv6, ']');
	if ( ipv6 )
		is6 = true;
		
	return is6;
}

/*
 * url_fopen
 *
 * checks for URLs or types in the 'url' and basically use the real fopen() for
 * standard files, or if the url happens to be a command to execute it uses
 * popen to execute it.
 * 
 * Note that we need to clean up (with url_fclose) upon an error in this function.
 * This is the only function that should use url_fclose() directly. Run time errors
 * (e.g in url_fread) will invoke a cleanup via the ABORT handler (AtAbort_ExtTable), 
 * which will internally call url_fclose().
 */
URL_FILE *
url_fopen(char *url,
		  bool forwrite,
		  extvar_t *ev,
		  CopyState pstate,
		  List *scanquals,
		  int *response_code,
		  const char **response_string)
{

	/* an EXECUTE string will always be prefixed like this */
	const char*	exec_prefix = "execute:";
	URL_FILE*	file;
	int 		e;
	int         ip_mode;

	/*
	 * if 'url' starts with "execute:" then it's a command to execute and
	 * not a url (the command specified in CREATE EXTERNAL TABLE .. EXECUTE)
	 */
	if (pg_strncasecmp(url, exec_prefix, strlen(exec_prefix)) == 0)
	{
		char* cmd;

		/* Execute command */
		cmd  = url + strlen(exec_prefix);
		return url_execute_fopen(url, cmd, forwrite, ev);
	}

	if (!(file = (URL_FILE *)malloc(sizeof(URL_FILE) + strlen(url) + 1)))
	{
		elog(ERROR, "out of memory");
	}

    memset(file, 0, sizeof(URL_FILE));

	file->url = ((char *) file) + sizeof(URL_FILE);
	strcpy(file->url, url);

    if (IS_FILE_URI(url))
    {
		char*	path = strchr(url + strlen(PROTOCOL_FILE), '/');
		struct fstream_options fo;

		Insist(!forwrite);
		
		memset(&fo, 0, sizeof fo);

		if (!path)
		{
			free(file);
			elog(ERROR, "External Table error opening file: '%s', invalid "
						"file path", url);
		}

		file->type = CFTYPE_FILE; /* marked as local FILE */
		fo.is_csv = pstate->csv_mode;
		fo.quote = pstate->quote ? *pstate->quote : 0;
		fo.escape = pstate->escape ? *pstate->escape : 0;
		fo.header = pstate->header_line;
		fo.bufsize = 32 * 1024;
		pstate->header_line = 0;

		/*
		 * Open the file stream. This includes opening the first file to be read
		 * and finding and preparing any other files to be opened later (if a
		 * wildcard or directory is used). In addition we check ahead of time
		 * that all the involved files exists and have proper read permissions.
		 */
		file->u.file.fp = fstream_open(path, &fo, response_code, response_string);

		/* couldn't open local file. return NULL to display proper error */
		if (!file->u.file.fp)
		{
			free(file);
			return NULL;
		}
    }
    else if (IS_HTTP_URI(url) || IS_GPFDIST_URI(url))
    {
		int sz;

		bool is_ipv6 = url_has_ipv6_format(file->url);
		
		sz = make_url(file->url, 0, is_ipv6);
		
        file->type = CFTYPE_CURL; /* marked as URL */

		if (sz < 0)
		{
			const char* url_cpy = pstrdup(file->url);
			
			url_fclose(file, false, pstate->cur_relname);
			elog(ERROR, "illegal URL: %s", url_cpy);
		}

		file->u.curl.curl_url = palloc(sz);
		memset(file->u.curl.curl_url, 0, sz);
		file->u.curl.for_write = forwrite;
		
		make_url(file->url, file->u.curl.curl_url, is_ipv6);
		/*
		 * We need to call is_url_ipv6 for the case where inside make_url function
		 * a domain name was transformed to an IPv6 address.
		 */
		if ( !is_ipv6 )
			is_ipv6 = url_has_ipv6_format( file->u.curl.curl_url);

		if (IS_GPFDIST_URI(file->u.curl.curl_url))
		{
			/* replace gpfdist:// with http:// */
			file->u.curl.curl_url += 3;
			memcpy(file->u.curl.curl_url, "http", 4);
			pstate->header_line = 0;
		}

		/* initialize a curl session and get a libcurl handle for it */
        if (! (file->u.curl.handle = curl_easy_init()))
		{
			url_fclose(file, false, pstate->cur_relname);
			elog(ERROR, "internal error: curl_easy_init failed");
		}


        if (CURLE_OK != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_URL, file->u.curl.curl_url)))
		{
			url_fclose(file, false, pstate->cur_relname);
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_URL error (%d - %s)",
				 e, curl_easy_strerror(e));
		}
        if (CURLE_OK != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_VERBOSE, FALSE)))
		{
			url_fclose(file, false, pstate->cur_relname);
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_VERBOSE error (%d - %s)",
				 e, curl_easy_strerror(e));
		}
		/* set callback for each header received from server */
		if (CURLE_OK != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_HEADERFUNCTION, header_callback)))
		{
			url_fclose(file, false, pstate->cur_relname);
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_HEADERFUNCTION error (%d - %s)",
				 e, curl_easy_strerror(e));
		}
		/* 'file' is the application variable that gets passed to header_callback */
		if (CURLE_OK != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_WRITEHEADER, file)))
		{
			url_fclose(file, false, pstate->cur_relname);
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_WRITEHEADER error (%d - %s)",
				 e, curl_easy_strerror(e));
		}
		/* set callback for each data block arriving from server to be written to application */
        if (CURLE_OK != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_WRITEFUNCTION, write_callback)))
		{
			url_fclose(file, false, pstate->cur_relname);
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_WRITEFUNCTION error (%d - %s)",
				 e, curl_easy_strerror(e));
		}
		/* 'file' is the application variable that gets passed to write_callback */
        if (CURLE_OK != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_WRITEDATA, file)))
		{
			url_fclose(file, false, pstate->cur_relname);
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_WRITEDATA error (%d - %s)",
				 e, curl_easy_strerror(e));
		}
				
		if ( !is_ipv6 )
			ip_mode = CURL_IPRESOLVE_V4;
		else 
			ip_mode = CURL_IPRESOLVE_V6;
		if (CURLE_OK != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_IPRESOLVE, ip_mode)))
		{
			url_fclose(file, false, pstate->cur_relname);
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_IPRESOLVE error (%d - %s)",
				 e, curl_easy_strerror(e));
		}
		
		/*
		 * set up a linked list of http headers. start with common headers
		 * needed for read and write operations, and continue below with 
		 * more specifics
		 */			
		file->u.curl.x_httpheader = NULL;
		
		/*
		 * support multihomed http use cases. see MPP-11874
		 */
		if (IS_HTTP_URI(url))
		{
			char domain[HOST_NAME_SIZE] = {0};
			extract_http_domain(file->url, domain, HOST_NAME_SIZE);
			set_httpheader(file, "Host", domain);
		}
		
		
		if (set_httpheader(file, "X-GP-XID", ev->GP_XID) ||
			set_httpheader(file, "X-GP-CID", ev->GP_CID) ||
			set_httpheader(file, "X-GP-SN", ev->GP_SN) ||
			set_httpheader(file, "X-GP-SEGMENT-ID", ev->GP_SEGMENT_ID) ||
			set_httpheader(file, "X-GP-SEGMENT-COUNT", ev->GP_SEGMENT_COUNT))
		{
			url_fclose(file, false, pstate->cur_relname);
			elog(ERROR, "internal error: some header value is too long");
		}

		if(forwrite)
		{
			/* write specific headers */
			if(set_httpheader(file, "X-GP-PROTO", "0") ||
			   set_httpheader(file, "Content-Type", "text/xml"))
			{
				url_fclose(file, false, pstate->cur_relname);
				elog(ERROR, "internal error: some header value is too long");
			}
		}
		else
		{
			/* read specific - (TODO: unclear why some of these are needed) */
			if(set_httpheader(file, "X-GP-PROTO", "1") ||
			   set_httpheader(file, "X-GP-MASTER_HOST", ev->GP_MASTER_HOST) ||
			   set_httpheader(file, "X-GP-MASTER_PORT", ev->GP_MASTER_PORT) ||
			   set_httpheader(file, "X-GP-CSVOPT", ev->GP_CSVOPT) ||
			   set_httpheader(file, "X-GP_SEG_PG_CONF", ev->GP_SEG_PG_CONF) ||
			   set_httpheader(file, "X-GP_SEG_DATADIR", ev->GP_SEG_DATADIR) ||
			   set_httpheader(file, "X-GP-DATABASE", ev->GP_DATABASE) ||
			   set_httpheader(file, "X-GP-USER", ev->GP_USER) ||
			   set_httpheader(file, "X-GP-SEG-PORT", ev->GP_SEG_PORT) ||
			   set_httpheader(file, "X-GP-SESSION-ID", ev->GP_SESSION_ID))
			{
				url_fclose(file, false, pstate->cur_relname);
				elog(ERROR, "internal error: some header value is too long");
			}
		}
		
		{
			/*
			 * MPP-13031
			 * copy #transform fragment, if present, into X-GP-TRANSFORM header
			 */
			char* p = local_strstr(file->url, "#transform=");
			if (p && p[11]) 
			{
				if (set_httpheader(file, "X-GP-TRANSFORM", p+11))
				{
					url_fclose(file, false, pstate->cur_relname);
					elog(ERROR, "internal error: X-GP-TRANSFORM header value is too long");
				}
			}
		}
		
		if (CURLE_OK != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_HTTPHEADER, file->u.curl.x_httpheader)))
		{
			url_fclose(file, false, pstate->cur_relname);
			elog(ERROR, "internal error: curl_easy_setopt CURLOPT_HTTPHEADER error (%d - %s)",
				 e, curl_easy_strerror(e));
        }

        if (!multi_handle)
		{
            if (! (multi_handle = curl_multi_init()))
			{
				url_fclose(file, false, pstate->cur_relname);
				elog(ERROR, "internal error: curl_multi_init failed");
			}
		}

        /* 
         * lets check our connection.
         * start the fetch if we're SELECTing (GET request), or write an
         * empty message if we're INSERTing (POST request) 
         */
        if (!forwrite)
        {
          if (CURLE_OK != (e = curl_multi_add_handle(multi_handle, file->u.curl.handle)))
		      {
			      if (CURLM_CALL_MULTI_PERFORM != e)
			      {
				      url_fclose(file, false, pstate->cur_relname);
				      elog(ERROR, "internal error: curl_multi_add_handle failed (%d - %s)",
					        e, curl_easy_strerror(e));
			      }
		      }
            
    		while (CURLM_CALL_MULTI_PERFORM ==
    			   (e = curl_multi_perform(multi_handle, &file->u.curl.still_running)));

    		if (e != CURLE_OK)
    		{
    			url_fclose(file, false, pstate->cur_relname);
    			elog(ERROR, "internal error: curl_multi_perform failed (%d - %s)",
    				 e, curl_easy_strerror(e));
    		}

    		/* read some bytes to make sure the connection is established */
    		fill_buffer(file, 1);
        }
        else
        {
        	/* use empty message */
            if (CURLE_OK != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_POSTFIELDS, "")))
        	{
        		url_fclose(file, false, pstate->cur_relname);
        		elog(ERROR, "internal error: curl_easy_setopt CURLOPT_POSTFIELDS error (%d - %s)",
        			 e, curl_easy_strerror(e));
        	}

            /* post away! */
            if (CURLE_OK != (e = curl_easy_perform(file->u.curl.handle)))
        	{
        		url_fclose(file, false, pstate->cur_relname);
        		elog(ERROR, "error: %s",
        			 curl_easy_strerror(e));
        	}
        }
        
		/* check the connection */
		if (check_response(file, response_code, response_string))
		{
			url_fclose(file, false, pstate->cur_relname);
			return NULL;
		}

    }
    else
    {
    	/* we're using a custom protocol */
    	
    	MemoryContext	oldcontext;
    	ExtPtcFuncType	ftype;
		Oid				procOid;
    	char   		   *prot_name 	= pstrdup(file->url);
    	int				url_len 	= strlen(file->url); 
    	int				i 			= 0;
    	
    	file->type = CFTYPE_CUSTOM;
    	ftype = (forwrite ? EXTPTC_FUNC_WRITER : EXTPTC_FUNC_READER);
    	
    	/* extract protocol name from url string */    	
    	while (file->url[i] != ':' && i < url_len - 1)
    		i++;
    	
    	prot_name[i] = '\0';
    	procOid = LookupExtProtocolFunction(prot_name, ftype, true);

        /*
    	 * Create a memory context to store all custom UDF private
    	 * memory. We do this in order to allow resource cleanup in
    	 * cases of query abort. We use TopTransactionContext as a
    	 * parent context so that it lives longer than Portal context.
    	 * Note that we always Delete our new context, in normal execution
    	 * and in abort (see url_fclose()).
    	 */
    	file->u.custom.protcxt = AllocSetContextCreate(TopTransactionContext,
													   "CustomProtocolMemCxt",
													   ALLOCSET_DEFAULT_MINSIZE,
													   ALLOCSET_DEFAULT_INITSIZE,
													   ALLOCSET_DEFAULT_MAXSIZE);
    	
    	oldcontext = MemoryContextSwitchTo(file->u.custom.protcxt);
		
    	file->u.custom.protocol_udf = palloc(sizeof(FmgrInfo));
		file->u.custom.extprotocol = (ExtProtocolData *) palloc (sizeof(ExtProtocolData));

		/* we found our function. set it in custom file handler */
		fmgr_info(procOid, file->u.custom.protocol_udf);

		/* have the custom file handler point at the qual list */
		file->u.custom.scanquals = scanquals;

		MemoryContextSwitchTo(oldcontext);
		
		file->u.custom.extprotocol->prot_user_ctx = NULL;
		file->u.custom.extprotocol->prot_last_call = false;
		file->u.custom.extprotocol->prot_url = NULL;
		file->u.custom.extprotocol->prot_databuf = NULL;
		file->u.custom.extprotocol->prot_scanquals = NIL;

		pfree(prot_name);
    }
	
    return file;
}

/*
 * url_fclose: Disposes of resources associated with this external web table.
 *
 * If failOnError is true, errors encountered while closing the resource results
 * in raising an ERROR.  This is particularly true for "execute:" resources where
 * command termination is not reflected until close is called.  If failOnClose is
 * false, close errors are just logged.  failOnClose should be false when closure
 * is due to LIMIT clause satisfaction.
 * 
 * relname is passed in for being available in data messages only.
 */
int
url_fclose(URL_FILE *file, bool failOnError, const char *relname)
{
	enum fcurl_type_e type = file->type;
    int 	ret = 0;/* default is good return */
    int 	errlen = 512;
	char	err[512];
    char* 	url = pstrdup(file->url);
	
    switch (type)
    {
		case CFTYPE_FILE:
			fstream_close(file->u.file.fp);
			/* fstream_close() returns no error indication. */
			break;

		case CFTYPE_EXEC:
		
			/* close the child process and related pipes */
			ret = pclose_with_stderr(file->u.exec.pid, file->u.exec.pipes, err, errlen);
			
			if (ret == 0)
			{
				/* pclose() ended successfully; no errors to reflect */
				;
			}
			else if (ret == -1)
			{
				/* pclose()/wait4() ended with an error; errno should be valid */
				if (failOnError) free(file);
				ereport( (failOnError ? ERROR : LOG),
						(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
								errmsg("cannot close external table %s command: %m", 
										(relname ? relname : "")),
								errdetail("command: %s", url),
								errOmitLocation(true)));
			}
			else
			{
				/*
				 * pclose() returned the process termination state.  The interpretExitCode() function
				 * generates a descriptive message from the exit code.
				 */
				char buf[512];
				
				/* change linefeeds to comma for better error display */
				/* added to black list of ARD 'to-revert' items		  */
				int		i = 0;
				int		len = strlen(err);

				for(i = 0; i < len - 1; i++)
				{
					if (err[i] == '\n' || err[i] == '\r')
						err[i] = ',';
				}

				if (failOnError) free(file);
				ereport( (failOnError ? ERROR : LOG),
						(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
								errmsg("external table %s command ended with %s",
										(relname ? relname : ""),
										interpretError(ret, buf, sizeof(buf)/sizeof(char), err, strlen(err))),
								errdetail("Command: %s", url),
								errOmitLocation(true)));
			}
			break;

		case CFTYPE_CURL:
			
			/*
			 * if WET, send a final "I'm done" request from this segment.
			 */
			if(file->u.curl.for_write)
				gp_proto0_write_done(file);
			
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
			if (file->u.curl.in.ptr)
			{
				free(file->u.curl.in.ptr);
				file->u.curl.in.ptr = NULL;
			}
				
			if (file->u.curl.out.ptr)
			{
				Assert(file->u.curl.for_write);
				pfree(file->u.curl.out.ptr);
				file->u.curl.out.ptr = NULL;
			}
			
			file->u.curl.gp_proto = 0;
			file->u.curl.error = file->u.curl.eof = 0;
			memset(&file->u.curl.in, 0, sizeof(file->u.curl.in));
			memset(&file->u.curl.block, 0, sizeof(file->u.curl.block));
			break;

		case CFTYPE_CUSTOM:
			
			/* last call. let the user close custom resources */
			if(file->u.custom.protocol_udf)
				(void) InvokeExtProtocol(NULL, 0, file, NULL, true, NULL);

			/* now clean up everything not cleaned by user */
			MemoryContextDelete(file->u.custom.protcxt);

			break;
			
		default: /* unknown or supported type - oh dear */
			free(file);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg_internal("external table type not implemented: %d", type)));
			break;
    }

    free(file);
    pfree(url);
    
    return ret;
}

bool
url_feof(URL_FILE *file, int bytesread)
{
    bool ret = false;

    switch (file->type)
    {
		case CFTYPE_FILE:
			ret = (fstream_eof(file->u.file.fp) != 0);
			break;

		case CFTYPE_EXEC:
			ret = (bytesread == 0);
			break;

		case CFTYPE_CURL:
			ret = (file->u.curl.eof != 0);
			break;

		case CFTYPE_CUSTOM:
			ret = (bytesread == 0);
			break;
			
		default: /* unknown or supported type - oh dear */
			ret = true;
			errno = EBADF;
			break;
    }
    return ret;
}


bool url_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen)
{
	bool ret = 0;
	int nread = 0;
	
	switch (file->type)
	{
		case CFTYPE_FILE:
			ret = fstream_get_error(file->u.file.fp) != 0;
			break;

		case CFTYPE_EXEC:
			ret = (bytesread == -1);
			if(ret == true && ebuflen > 0 && ebuf != NULL)
			{
				nread = piperead(file->u.exec.pipes[EXEC_ERR_P], ebuf, ebuflen);
			
				if(nread != -1)
					ebuf[nread] = 0;
				else
					strncpy(ebuf,"error string unavailable due to read error",ebuflen-1);
			}
			break;

		case CFTYPE_CURL:
			ret = (file->u.curl.error != 0);
			break;

		case CFTYPE_CUSTOM:
			ret = (bytesread == -1);
			break;

		default: /* unknown or supported type - oh dear */
			ret = true;
			errno = EBADF;
			break;
	}

	return ret;
}

/*
 * gp_proto0_read
 *
 * get data from the server and handle it according to PROTO 0. In PROTO 0 we
 * expect the content of the file without any kind of meta info. Simple.
 */
static size_t gp_proto0_read(char *buf, int bufsz, URL_FILE* file)
{
	int 		n = 0;
	curlctl_t* 	curl = &file->u.curl;

	fill_buffer(file, bufsz);

	/* check if there's data in the buffer - if not fill_buffer()
	 * either errored or EOF. For proto0, we cannot distinguish
	 * between error and EOF. */
	n = curl->in.top - curl->in.bot;
	if (n == 0 && !curl->still_running)
		curl->eof = 1;

	if (n > bufsz)
		n = bufsz;

	/* xfer data to caller */
	memcpy(buf, curl->in.ptr, n);
	curl->in.bot += n;

	return n;
}

/*
 * gp_proto1_read
 *
 * get data from the server and handle it according to PROTO 1. In this protocol
 * each data block is tagged by meta info like this:
 * byte 0: type (can be 'F'ilename, 'O'ffset, 'D'ata, 'E'rror, 'L'inenumber)
 * byte 1-4: length. # bytes of following data block. in network-order.
 * byte 5-X: the block itself.
 */
static size_t gp_proto1_read(char *buf, int bufsz, URL_FILE *file, CopyState pstate, char *buf2)
{
	char type;
	int  n, len;
	curlctl_t* curl = &file->u.curl;

	/*
	 * Loop through and get all types of messages, until we get actual data,
	 * or until there's no more data. Then quit the loop to process it and
	 * return it.
	 */
	while (curl->block.datalen == 0 && !curl->eof)
	{
		/* need 5 bytes, 1 byte type + 4 bytes length */
		fill_buffer(file, 5);
		n = curl->in.top - curl->in.bot;

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
		type = curl->in.ptr[curl->in.bot++];

		/* read len */
		memcpy(&len, &curl->in.ptr[curl->in.bot], 4);
		len = ntohl(len);		/* change order */
		curl->in.bot += 4;

		if (len < 0)
		{
			elog(ERROR, "gpfdist error: bad packet type %d len %d",
				 type, len);
			return -1;
		}

		/* elog(NOTICE, "HEADER %c %d, bot %d, top %d", type, len,
		   curl->in.bot, curl->in.top);
		*/

		/* Error */
		if (type == 'E')
		{
			fill_buffer(file, len);
			n = curl->in.top - curl->in.bot;

			if (n > len)
				n = len;

			if (n > 0)
			{
				/*
				 * cheat a little. swap last char and
				 * NUL-terminator. then print string (without last
				 * char) and print last char artificially
				 */
				char x = curl->in.ptr[curl->in.bot + n - 1];
				curl->in.ptr[curl->in.bot + n - 1] = 0;
				elog(ERROR, "gpfdist error - %s%c", &curl->in.ptr[curl->in.bot], x);

				return -1;
			}

			elog(ERROR, "gpfdist error: please check gpfdist log messages.");

			return -1;
		}

		/* Filename */
		if (type == 'F')
		{
			if (buf != buf2)
			{
				curl->in.bot -= 5;
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

			/*
			 * If SREH is used we now update it with the actual file that the
			 * gpfdist server is reading. This is because SREH (or the client
			 * in general) doesn't know which file gpfdist is reading, since
			 * the original URL may include a wildcard or a directory listing.
			 */
			if (pstate->cdbsreh)
			{
				char fname[257];

				memcpy(fname, curl->in.ptr + curl->in.bot, len);
				fname[len] = 0;
				snprintf(pstate->cdbsreh->filename, sizeof pstate->cdbsreh->filename,"%s [%s]", pstate->filename, fname);
			}

			curl->in.bot += len;
			Assert(curl->in.bot <= curl->in.top);
			continue;
		}

		/* Offset */
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

			curl->in.bot += 8;
			Assert(curl->in.bot <= curl->in.top);
			continue;
		}

		/* Line number */
		if (type == 'L')
		{
			int64 line_number;

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

			/*
			 * update the line number of the first line we're about to get from
			 * gpfdist. pstate will update the following lines when processing
			 * the data
			 */
			memcpy(&line_number, curl->in.ptr + curl->in.bot, len);
			line_number = local_ntohll(line_number);
			pstate->cur_lineno = line_number ? line_number - 1 : INT64_MIN;
			curl->in.bot += 8;
			Assert(curl->in.bot <= curl->in.top);
			continue;
		}

		/* Data */
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

	fill_buffer(file, bufsz);
	n = curl->in.top - curl->in.bot;

	/* if gpfdist closed connection prematurely or died catch it here */
	if (n == 0 && !curl->eof)
	{
		curl->error = 1;
		
		if(!curl->still_running)
			elog(ERROR, "gpfdist server closed connection.\n");
	}

	if (n > bufsz)
		n = bufsz;

	memcpy(buf, curl->in.ptr + curl->in.bot, n);
	curl->in.bot += n;
	curl->block.datalen -= n;
	// elog(NOTICE, "returning %d bytes, %d bytes left \n", n, curl->block.datalen);
	return n;
}

/*
 * gp_proto0_write
 * 
 * use curl to write data to a the remote gpfdist server. We use
 * a push model with a POST request. 
 * 
 */
static void gp_proto0_write(URL_FILE *file, CopyState pstate)
{
	curlctl_t*	curl = &file->u.curl;
	char*		buf = curl->out.ptr;
	int			nbytes = curl->out.top;
	int 		e;
	int 		response_code;
	const char*	response_string;

	if (nbytes == 0)
		return;
	
	/* post binary data */  
    if (CURLE_OK != (e = curl_easy_setopt(curl->handle, CURLOPT_POSTFIELDS, buf)))
		elog(ERROR, "internal error: curl_easy_setopt CURLOPT_POSTFIELDS error (%d - %s)",
			 e, curl_easy_strerror(e));

	 /* set the size of the postfields data */  
    if (CURLE_OK != (e = curl_easy_setopt(curl->handle, CURLOPT_POSTFIELDSIZE, nbytes)))
		elog(ERROR, "internal error: curl_easy_setopt CURLOPT_POSTFIELDSIZE error (%d - %s)",
			 e, curl_easy_strerror(e));

    /* post away! */
    if (CURLE_OK != (e = curl_easy_perform(curl->handle)))
		elog(ERROR, "%s error (%d - %s)",
			 file->u.curl.curl_url,
			 e, curl_easy_strerror(e));
    
	/* check the response from server */
	if (check_response(file, &response_code, &response_string))
		elog(ERROR, "error while writing data to gpfdist on %s (code %d, msg %s)",
				file->u.curl.curl_url, response_code, response_string);

}

/*
 * Send an empty POST request, with an added X-GP-DONE header.
 */
static void gp_proto0_write_done(URL_FILE *file)
{
    int 	e;

	set_httpheader(file, "X-GP-DONE", "1");

	/* use empty message */
	if (CURLE_OK != (e = curl_easy_setopt(file->u.curl.handle, CURLOPT_POSTFIELDS, "")))
	{
		elog(ERROR, "internal error: curl_easy_setopt CURLOPT_POSTFIELDS %s error (%d - %s)",
			 file->u.curl.curl_url,
			 e, curl_easy_strerror(e));
	}

    /* post away! */
    if (CURLE_OK != (e = curl_easy_perform(file->u.curl.handle)))
	{
		elog(ERROR, "%s error: %s",
			 file->u.curl.curl_url,
			 curl_easy_strerror(e));
	}

}

static size_t curl_fread(char *buf, int bufsz, URL_FILE* file, CopyState pstate)
{
	curlctl_t*	curl = &file->u.curl;
	char*		p = buf;
	char*		q = buf + bufsz;
	int 		n;
	const int 	gp_proto = curl->gp_proto;

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
			n = gp_proto1_read(p, q - p, file, pstate, buf);

		//elog(NOTICE, "curl_fread %d bytes", n);

		if (n <= 0)
			break;
	}

	return p - buf;
}

static size_t curl_fwrite(char *buf, int nbytes, URL_FILE* file, CopyState pstate)
{
	curlctl_t*	curl = &file->u.curl;
	
	if (curl->gp_proto != 0 && curl->gp_proto != 1)
	{
		elog(ERROR, "unknown gp protocol %d", curl->gp_proto);
		return 0;
	}

	/*
	 * allocate data buffer if not done already
	 */
	if(!curl->out.ptr)
	{
		const int bufsize = 64 * 1024 * sizeof(char);
		MemoryContext oldcontext = CurrentMemoryContext;
		
		MemoryContextSwitchTo(CurTransactionContext); /* TODO: is there a better cxt to use? */
		curl->out.ptr = (char *)palloc(bufsize);
		curl->out.max = bufsize;
		curl->out.bot = curl->out.top = 0;
		MemoryContextSwitchTo(oldcontext);
	}
	
	/*
	 * if buffer is full (current item can't fit) - write it out to
	 * the server. if item still doesn't fit after we emptied the
	 * buffer, make more room.
	 */
	if (curl->out.top + nbytes >= curl->out.max)
	{
		/* item doesn't fit */
		if (curl->out.top > 0)
		{
			/* write out existing data, empty the buffer */
			gp_proto0_write(file, pstate);
			curl->out.top = 0;
		}
		
		/* does it still not fit? enlarge buffer */
		if (curl->out.top + nbytes >= curl->out.max)
		{
			int 	n = nbytes + 1024;
			char*	newbuf;
			MemoryContext oldcontext = CurrentMemoryContext;

			MemoryContextSwitchTo(CurTransactionContext); /* TODO: is there a better cxt to use? */
			newbuf = repalloc(curl->out.ptr, n);
			MemoryContextSwitchTo(oldcontext);

			if (!newbuf)
				elog(ERROR, "out of memory (curl_fwrite)");

			curl->out.ptr = newbuf;
			curl->out.max = n;

			Assert(nbytes < curl->out.max);
		}
	}

	/* copy buffer into curl->buf */
	memcpy(curl->out.ptr + curl->out.top, buf, nbytes);
	curl->out.top += nbytes;
	
	return nbytes;
}


size_t
url_fread(void *ptr, size_t size, size_t nmemb, URL_FILE *file, CopyState pstate, ExternalSelectDesc desc)
{
    size_t 	want;
	int 	n;

    switch (file->type)
    {
		case CFTYPE_FILE:
		{
			struct fstream_filename_and_offset fo;
			const int whole_rows = 0; /* get as much data as possible */

			assert(size == 1);

			/* FIXME: GP FUSION: ??? */
			n = fstream_read(file->u.file.fp, ptr, nmemb, &fo, whole_rows, 0, 0);
			want = n > 0 ? n : 0;

			if (n > 0 && fo.line_number)
			{
				pstate->cur_lineno = fo.line_number - 1;

				if (pstate->cdbsreh)
					snprintf(pstate->cdbsreh->filename,
							 sizeof pstate->cdbsreh->filename,
							 "%s [%s]", pstate->filename, fo.fname);
			}

			break;
		}
		case CFTYPE_EXEC:
			want = piperead(file->u.exec.pipes[EXEC_DATA_P], ptr, nmemb * size);
			break;

		case CFTYPE_CURL:

			/* get data (up to nmemb * size) from the http/gpfdist server */
			n = curl_fread(ptr, nmemb * size, file, pstate);

			/* number of items - nb correct op - checked with glibc code*/
			want = n / size;

			/*printf("(fread) return %d bytes %d left\n", want,file->u.curl.buffer_pos);*/
			break;

		case CFTYPE_CUSTOM:
			
			want = (size_t) InvokeExtProtocol(ptr, nmemb * size, file, pstate, false, desc);
			break;
				
		default: /* unknown or supported type */
			want = 0;
			errno = EBADF;
			break;
    }

    return want;
}

size_t
url_fwrite(void *ptr, size_t size, size_t nmemb, URL_FILE *file, CopyState pstate)
{
    size_t 	want = 0, n = 0;

    switch (file->type)
    {
		case CFTYPE_FILE:

			elog(ERROR, "CFTYPE_FILE not yet supported in url.c");
			break;

		case CFTYPE_EXEC:
			
			want = pipewrite(file->u.exec.pipes[EXEC_DATA_P], ptr, nmemb * size);
			break;

		case CFTYPE_CURL:
			
			/* write data to the gpfdist server via curl */
			n = curl_fwrite(ptr, nmemb * size, file, pstate);
			want = n / size;
			break;
		
		case CFTYPE_CUSTOM:
						
			want = (size_t) InvokeExtProtocol(ptr, nmemb * size, file, pstate, false, NULL);
			break;
			
		default: /* unknown or unsupported type */
			want = 0;
			errno = EBADF;
			break;
    }

    return want;
}

/*
 * flush all remaining buffered data waiting to be written out to external source
 */
void
url_fflush(URL_FILE *file, CopyState pstate)
{
	
    switch (file->type)
    {
		case CFTYPE_FILE:
		{
			elog(ERROR, "CFTYPE_FILE not yet supported in url.c");
			break;
		}
		case CFTYPE_EXEC:
		case CFTYPE_CUSTOM:
			/* data isn't buffered on app level. no op */
			break;

		case CFTYPE_CURL:
			gp_proto0_write(file, pstate);
			break;

		default: /* unknown or unsupported type */
			break;
    }
}

void
url_rewind(URL_FILE *file, const char *relname)
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
			url_fclose(file, true, relname);

			/* most of these are null fo us. */
			url_fopen(url, false, NULL, NULL, NIL, NULL, 0);
			break;

		case CFTYPE_CURL:
			/* halt transaction */
			curl_multi_remove_handle(multi_handle, file->u.curl.handle);

			/* restart */
			curl_multi_add_handle(multi_handle, file->u.curl.handle);

			/* ditch buffer - write will recreate - resets stream pos*/
			if (file->u.curl.in.ptr)
				free(file->u.curl.in.ptr);

			file->u.curl.gp_proto = 0;
			file->u.curl.error = file->u.curl.eof = 0;
			memset(&file->u.curl.in, 0, sizeof(file->u.curl.in));
			memset(&file->u.curl.block, 0, sizeof(file->u.curl.block));
			break;

		case CFTYPE_CUSTOM:
			elog(ERROR, "rewind support not yet implemented in custom protocol");
			break;
			
		default: /* unknown or supported type - oh dear */
			break;

    }
}


/*
 * interpretError - formats a brief message and/or the exit code from pclose()
 * 		(or wait4()).
 */
static char *
interpretError(int rc, char *buf, size_t buflen, char *err, size_t errlen)
{
	if (WIFEXITED(rc))
	{
		int exitCode = WEXITSTATUS(rc);
		if (exitCode >= 128)
		{
			/*
			 * If the exit code has the 128-bit set, the exit code represents
			 * a shell exited by signal where the signal number is exitCode - 128.
			 */
			exitCode -= 128;
			snprintf(buf, buflen, "SHELL TERMINATED by signal %s (%d)", getSignalNameFromCode(exitCode), exitCode);
		}
		else if (exitCode == 0)
		{
			snprintf(buf, buflen, "EXITED; rc=%d", exitCode);
		}
		else
		{
			/* Exit codes from commands rarely map to strerror() strings. In here
			 * we show the error string returned from pclose, and omit the non
			 * friendly exit code interpretation */
			snprintf(buf, buflen, "error. %s", err);
		}
	}
	else if (WIFSIGNALED(rc))
	{
		int signalCode = WTERMSIG(rc);
		snprintf(buf, buflen, "TERMINATED by signal %s (%d)", getSignalNameFromCode(signalCode), signalCode);
	}
#ifndef WIN32
	else if (WIFSTOPPED(rc))
	{
		int signalCode = WSTOPSIG(rc);
		snprintf(buf, buflen, "STOPPED by signal %s (%d)", getSignalNameFromCode(signalCode), signalCode);
	}
#endif
	else
	{
		snprintf(buf, buflen, "UNRECOGNIZED termination; rc=%#x", rc);
	}

	return buf;
}


struct signalDef {
	const int			signalCode;
	const char		   *signalName;
};

/*
 * Table mapping signal numbers to signal identifiers (names).
 */
struct signalDef signals[] = {
#ifdef SIGHUP
		{ SIGHUP,    "SIGHUP" },
#endif
#ifdef SIGINT
		{ SIGINT,    "SIGINT" },
#endif
#ifdef SIGQUIT
		{ SIGQUIT,   "SIGQUIT" },
#endif
#ifdef SIGILL
		{ SIGILL,    "SIGILL" },
#endif
#ifdef SIGTRAP
		{ SIGTRAP,   "SIGTRAP" },
#endif
#ifdef SIGABRT
		{ SIGABRT,   "SIGABRT" },
#endif
#ifdef SIGEMT
		{ SIGEMT,    "SIGEMT" },
#endif
#ifdef SIGFPE
		{ SIGFPE,    "SIGFPE" },
#endif
#ifdef SIGKILL
		{ SIGKILL,   "SIGKILL" },
#endif
#ifdef SIGBUS
		{ SIGBUS,    "SIGBUS" },
#endif
#ifdef SIGSEGV
		{ SIGSEGV,   "SIGSEGV" },
#endif
#ifdef SIGSYS
		{ SIGSYS,    "SIGSYS" },
#endif
#ifdef SIGPIPE
		{ SIGPIPE,   "SIGPIPE" },
#endif
#ifdef SIGALRM
		{ SIGALRM,   "SIGALRM" },
#endif
#ifdef SIGTERM
		{ SIGTERM,   "SIGTERM" },
#endif
#ifdef SIGURG
		{ SIGURG,    "SIGURG" },
#endif
#ifdef SIGSTOP
		{ SIGSTOP,   "SIGSTOP" },
#endif
#ifdef SIGTSTP
		{ SIGTSTP,   "SIGTSTP" },
#endif
#ifdef SIGCONT
		{ SIGCONT,   "SIGCONT" },
#endif
#ifdef SIGCHLD
		{ SIGCHLD,   "SIGCHLD" },
#endif
#ifdef SIGTTIN
		{ SIGTTIN,   "SIGTTIN" },
#endif
#ifdef SIGTTOU
		{ SIGTTOU,   "SIGTTOU" },
#endif
#ifdef SIGIO
		{ SIGIO,     "SIGIO" },
#endif
#ifdef SIGXCPU
		{ SIGXCPU,   "SIGXCPU" },
#endif
#ifdef SIGXFSZ
		{ SIGXFSZ,   "SIGXFSZ" },
#endif
#ifdef SIGVTALRM
		{ SIGVTALRM, "SIGVTALRM" },
#endif
#ifdef SIGPROF
		{ SIGPROF,   "SIGPROF" },
#endif
#ifdef SIGWINCH
		{ SIGWINCH,  "SIGWINCH" },
#endif
#ifdef SIGINFO
		{ SIGINFO,   "SIGINFO" },
#endif
#ifdef SIGUSR1
		{ SIGUSR1,   "SIGUSR1" },
#endif
#ifdef SIGUSR2
		{ SIGUSR2,   "SIGUSR2" },
#endif
		{ -1, "" }
};


/*
 * getSignalNameFromCode - gets the signal name given the signal number.
 */
static const char *
getSignalNameFromCode(int signo)
{
	int i;
	for (i = 0; signals[i].signalCode != -1; i++)
	{
		if (signals[i].signalCode == signo)
			return signals[i].signalName;
	}

	return "UNRECOGNIZED";
}

/*
 * popen_with_stderr
 * 
 * standard popen doesn't redirect stderr from the child process.
 * we need stderr in order to display the error that child process
 * encountered and show it to the user. This is, therefore, a wrapper
 * around a set of file descriptor redirections and a fork.
 *
 * if 'forwrite' is set then we set the data pipe write side on the 
 * parent. otherwise, we set the read side on the parent.
 */
static int popen_with_stderr(int *pipes, const char *exe, bool forwrite)
{
	int data[2];	/* pipe to send data child <--> parent */
	int err[2];		/* pipe to send errors child --> parent */
	int pid = -1;
	
	const int READ = 0;
	const int WRITE = 1;

	if (pgpipe(data) < 0)
		return -1;

	if (pgpipe(err) < 0)
	{
		close(data[READ]);
		close(data[WRITE]);
		return -1;
	}
#ifndef WIN32

	pid = fork();
	
	if (pid > 0) /* parent */
	{

		if (forwrite)
		{
			/* parent writes to child */
			close(data[READ]);
			pipes[EXEC_DATA_P] = data[WRITE];
		}
		else
		{
			/* parent reads from child */
			close(data[WRITE]);
			pipes[EXEC_DATA_P] = data[READ]; 			
		}
		
		close(err[WRITE]);
		pipes[EXEC_ERR_P] = err[READ];
		
		return pid;
	} 
	else if (pid == 0) /* child */
	{		

		/*
		 * set up the data pipe
		 */
		if (forwrite)
		{
			close(data[WRITE]);
			close(fileno(stdin));
			
			/* assign pipes to parent to stdin */
			if (dup2(data[READ], fileno(stdin)) < 0)
			{
				perror("dup2 error");
				exit(EXIT_FAILURE);
			}

			/* no longer needed after the duplication */
			close(data[READ]);			
		}
		else
		{
			close(data[READ]);
			close(fileno(stdout));
			
			/* assign pipes to parent to stdout */
			if (dup2(data[WRITE], fileno(stdout)) < 0)
			{
				perror("dup2 error");
				exit(EXIT_FAILURE);
			}

			/* no longer needed after the duplication */
			close(data[WRITE]);
		}
		
		/*
		 * now set up the error pipe
		 */
		close(err[READ]);
		close(fileno(stderr));
		
		if (dup2(err[WRITE], fileno(stderr)) < 0)
		{
			if(forwrite)
				close(data[WRITE]);
			else
				close(data[READ]); 
			
			perror("dup2 error");
			exit(EXIT_FAILURE);			
		}

		close(err[WRITE]);
				
		/* go ahead and execute the user command */
		execl("/bin/sh", "sh", "-c", exe, NULL);

		/* if we're here an error occurred */
		exit(EXIT_FAILURE);
	} 
	else
	{
		if(forwrite)
		{
			close(data[WRITE]);
			close(data[READ]);
		}
		else
		{
			close(data[READ]);
			close(data[WRITE]);
		}
		close(err[READ]);
		close(err[WRITE]);
		
		return -1;		
	}
#endif

	return pid;
}

/*
 * pclose_with_stderr
 * 
 * close our data and error pipes and return the child process 
 * termination status. if child terminated with error, 'buf' will
 * point to the error string retrieved from child's stderr.
 */
static int pclose_with_stderr(int pid, int *pipes, char *buf, int len)
{
	int status, nread;
	
	/* close the data pipe. we can now read from error pipe without being blocked */
	close(pipes[EXEC_DATA_P]);
	
	nread = piperead(pipes[EXEC_ERR_P], buf, len); /* read from stderr */
	
	if(nread != -1)
		buf[nread] = 0;
	else
		strcpy(buf,"error string unavailable due to read error");

	close(pipes[EXEC_ERR_P]);

#ifndef WIN32

	waitpid(pid, &status, 0);
#else
    status = -1;
#endif 

	return status;
}

static int32 
InvokeExtProtocol(void 	   		*ptr, 
				  size_t 		 nbytes, 
				  URL_FILE 		*file, 
				  CopyState 	 pstate,
				  bool			 last_call,
				  ExternalSelectDesc desc)
{
	FunctionCallInfoData	fcinfo;
	ExtProtocolData*		extprotocol = file->u.custom.extprotocol;
	FmgrInfo*				extprotocol_udf = file->u.custom.protocol_udf;
	Datum					d;
	MemoryContext			oldcontext;
	
	/* must have been created during url_fopen() */
	Assert(extprotocol);
	
	extprotocol->type = T_ExtProtocolData;
	extprotocol->prot_url = file->url;
	extprotocol->prot_relation = (last_call ? NULL : pstate->rel);
	extprotocol->prot_databuf  = (last_call ? NULL : (char *)ptr);
	extprotocol->prot_maxbytes = nbytes;
	extprotocol->prot_scanquals = file->u.custom.scanquals;
	extprotocol->prot_last_call = last_call;
	extprotocol->desc = desc;
	
	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo, 
							 /* FmgrInfo */ extprotocol_udf, 
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) extprotocol, 
							 /* ResultSetInfo */ NULL);
	
	/* invoke the protocol within a designated memory context */
	oldcontext = MemoryContextSwitchTo(file->u.custom.protcxt);
	d = FunctionCallInvoke(&fcinfo);
	MemoryContextSwitchTo(oldcontext);

	/* We do not expect a null result */
	if (fcinfo.isnull)
		elog(ERROR, "function %u returned NULL", fcinfo.flinfo->fn_oid);

	return DatumGetInt32(d);
}

