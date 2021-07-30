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
#include "access/filesplit.h"
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

#include <fstream/gfile.h>

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <stdint.h>

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

static char *interpretError(int exitCode, char *buf, size_t buflen, char *err, size_t errlen);
static const char *getSignalNameFromCode(int signo);
static int popen_with_stderr(int *rwepipe, const char *exe, bool forwrite);
static int pclose_with_stderr(int pid, int *rwepipe, char *buf, int len);
static int32  InvokeExtProtocol(void		*ptr, 
								size_t 		nbytes, 
								URL_FILE 	*file, 
								CopyState 	pstate,
								bool		last_call,
								ExternalSelectDesc desc,
								List		**psplits);


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
	} else if (IS_HTTP_URI(url) || IS_GPFDIST_URI(url)) {
    return url_curl_fopen(url, forwrite, ev, pstate);
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
		  url_curl_fclose(file, failOnError, relname);
		  return ret;

		case CFTYPE_CUSTOM:
			
			/* last call. let the user close custom resources */
			if(file->u.custom.protocol_udf)
				(void) InvokeExtProtocol(NULL, 0, file, NULL, true, NULL, NULL);

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
		  return url_curl_feof(file, bytesread);

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
		  return url_curl_ferror(file, bytesread, ebuf, ebuflen);

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

size_t
url_fread(void *ptr, size_t size, size_t nmemb, URL_FILE *file, CopyState pstate, ExternalSelectDesc desc, List **splits)
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
			n = url_curl_fread(ptr, nmemb * size, file, pstate);

			/* number of items - nb correct op - checked with glibc code*/
			want = n / size;

			/*printf("(fread) return %d bytes %d left\n", want,file->u.curl.buffer_pos);*/
			break;

		case CFTYPE_CUSTOM:
			
			want = (size_t) InvokeExtProtocol(ptr, nmemb * size, file, pstate, false, desc, splits);
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
			n = url_curl_fwrite(ptr, nmemb * size, file, pstate);
			want = n / size;
			break;
		
		case CFTYPE_CUSTOM:
						
			want = (size_t) InvokeExtProtocol(ptr, nmemb * size, file, pstate, false, NULL, NULL);
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
		  url_curl_fflush(file, pstate);
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
		  url_curl_rewind(file, relname);
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
				  ExternalSelectDesc desc,
				  List		   **psplits)
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
	extprotocol->splits = NULL;
	
	if (*psplits != NULL) {
		/*
		 * We move to read splits from arg to this context structure, so that
		 * means we passed split data only the first time this is called.
		 */
		while( list_length(*psplits)>0 )
		{
			FileSplit split = (FileSplit)lfirst(list_head(*psplits));
			elog(LOG, "split %s:" INT64_FORMAT ", " INT64_FORMAT,
					  split->ext_file_uri_string,
					  split->offsets, split->lengths);
			extprotocol->splits = lappend(extprotocol->splits, split);
			*psplits = list_delete_first(*psplits);
		}
	}

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

