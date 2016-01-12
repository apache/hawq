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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * gp-libpq-int.h
 *	  This file contains internal definitions meant to be used only by
 *	  the frontend libpq library, not by applications that call it.
 *
 *	  An application can include this file if it wants to bypass the
 *	  official API defined by libpq-fe.h, but code that does so is much
 *	  more likely to break across PostgreSQL releases than code that uses
 *	  only the official API.
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/interfaces/libpq/libpq-int.h,v 1.143 2009/06/23 18:13:23 mha Exp $
 *
 *-------------------------------------------------------------------------
 */

#ifndef GP_LIBPQ_INT_H
#define GP_LIBPQ_INT_H

/* We assume gp-libpq-fe.h has already been included. */
#include "postgres_fe.h"
//#include "libpq-events.h"

#include <time.h>
#include <sys/types.h>
#ifndef WIN32
#include <sys/time.h>
#endif

#ifdef ENABLE_THREAD_SAFETY
#include <pthread.h>
#include <signal.h>
#endif

/* include stuff common to fe and be */
#include "getaddrinfo.h"
#include "libpq/pqcomm.h"
/* include stuff found in fe only */
#include "pqexpbuffer.h"

#include "cdb/cdbquerycontextdispatching.h"

/*
 * POSTGRES backend dependent Constants.
 */
#define CMDSTATUS_LEN 72  	/* should match COMPLETION_TAG_BUFSIZE.  GPDB -- this is different from PostgreSQL  */

/*
 * PGresult and the subsidiary types PGresAttDesc, PGresAttValue
 * represent the result of a query (or more precisely, of a single SQL
 * command --- a query string given to PQexec can contain multiple commands).
 * Note we assume that a single command can return at most one tuple group,
 * hence there is no need for multiple descriptor sets.
 */

/* Subsidiary-storage management structure for PGresult.
 * See space management routines in fe-exec.c for details.
 * Note that space[k] refers to the k'th byte starting from the physical
 * head of the block --- it's a union, not a struct!
 */
typedef union pgresult_data PGresult_data;

union pgresult_data
{
	PGresult_data *next;		/* link to next block, or NULL */
	char		space[1];		/* dummy for accessing block as bytes */
};

/* Data about a single parameter of a prepared statement */
typedef struct pgresParamDesc
{
	Oid			typid;			/* type id */
} PGresParamDesc;

/*
 * Data for a single attribute of a single tuple
 *
 * We use char* for Attribute values.
 *
 * The value pointer always points to a null-terminated area; we add a
 * null (zero) byte after whatever the backend sends us.  This is only
 * particularly useful for text values ... with a binary value, the
 * value might have embedded nulls, so the application can't use C string
 * operators on it.  But we add a null anyway for consistency.
 * Note that the value itself does not contain a length word.
 *
 * A NULL attribute is a special case in two ways: its len field is NULL_LEN
 * and its value field points to null_field in the owning PGresult.  All the
 * NULL attributes in a query result point to the same place (there's no need
 * to store a null string separately for each one).
 */

#define NULL_LEN		(-1)	/* pg_result len for NULL value */

typedef struct pgresAttValue
{
	int			len;			/* length in bytes of the value */
	char	   *value;			/* actual value, plus terminating zero byte */
} PGresAttValue;

/* Typedef for message-field list entries */
typedef struct pgMessageField
{
	struct pgMessageField *next;	/* list link */
	char		code;			/* field code */
	char		contents[1];	/* field value (VARIABLE LENGTH) */
} PGMessageField;

/* Fields needed for notice handling */
typedef struct
{
	PQnoticeReceiver noticeRec; /* notice message receiver */
	void	   *noticeRecArg;
	PQnoticeProcessor noticeProc;		/* notice message processor */
	void	   *noticeProcArg;
} PGNoticeHooks;

#if 0
typedef struct PGEvent
{
	PGEventProc proc;			/* the function to call on events */
	char	   *name;			/* used only for error messages */
	void	   *passThrough;	/* pointer supplied at registration time */
	void	   *data;			/* optional state (instance) data */
	bool		resultInitialized;		/* T if RESULTCREATE/COPY succeeded */
} PGEvent;
#endif

/* CDB: Statistical response message list element */
typedef struct pgCdbStatCell
{
    struct pgCdbStatCell   *next;
    int         len;
    char       *data;
} pgCdbStatCell;


struct pg_result
{
	int			ntups;
	int			numAttributes;
	PGresAttDesc *attDescs;
	PGresAttValue **tuples;		/* each PGresTuple is an array of
								 * PGresAttValue's */
	int			tupArrSize;		/* allocated size of tuples array */
	int			numParameters;
	PGresParamDesc *paramDescs;
	ExecStatusType resultStatus;
	char		cmdStatus[CMDSTATUS_LEN];		/* cmd status from the query */
	int			binary;			/* binary tuple values if binary == 1,
								 * otherwise text */

	/*
	 * These fields are copied from the originating PGconn, so that operations
	 * on the PGresult don't have to reference the PGconn.
	 */
	PGNoticeHooks noticeHooks;
	int			client_encoding;	/* encoding id */

	/* Transaction information passed from EXECUTOR to DISPATCHER */
	bool						QEWriter_HaveInfo;
	DistributedTransactionId 	QEWriter_DistributedTransactionId;
	CommandId 					QEWriter_CommandId;
	bool 						QEWriter_Dirty;
	
	/* XLOG passed from Standby Master to Primary */
	bool						Standby_HaveInfo;
	uint32						Standby_xlogid;		/* log file #, 0 based */
	uint32						Standby_xrecoff;	/* byte offset of location in log file */
	
	/*
	 * Error information (all NULL if not an error result).  errMsg is the
	 * "overall" error message returned by PQresultErrorMessage.  If we have
	 * per-field info then it is stored in a linked list.
	 */
	char	   *errMsg;			/* error message, or NULL if no error */
	PGMessageField *errFields;	/* message broken into fields */

	/* All NULL attributes in the query result point to this null string */
	char		null_field[1];

	/*
	 * Space management information.  Note that attDescs and error stuff, if
	 * not null, point into allocated blocks.  But tuples points to a
	 * separately malloc'd block, so that we can realloc it.
	 */
	PGresult_data *curBlock;	/* most recently allocated block */
	int			curOffset;		/* start offset of free space in block */
	int			spaceLeft;		/* number of free bytes remaining in block */

    /* CDB: List of statistical response messages ('Y') from qExec. */
    pgCdbStatCell  *cdbstats;   /* ordered from newest to oldest */
    
	/*
	 * Used for gang management commands and stats collected from QEs for
	 * Vacuum and Analyze commands.
	 */
    void * extras;
    int	   extraslen;

	/* GPDB: number of rows rejected in SREH (protocol message 'j') */
	int			numRejected;
	/* GPDB: number of processed tuples for each AO partition */
	int			naotupcounts;
	PQaoRelTupCount *aotupcounts;
	/* HAWQ: send back modified catalog on segments */
	int			numSendback;
	QueryContextDispatchingSendBack sendback;
};

/* PGAsyncStatusType defines the state of the query-execution state machine */
typedef enum
{
	PGASYNC_IDLE,				/* nothing's happening, dude */
	PGASYNC_BUSY,				/* query in progress */
	PGASYNC_READY,				/* result ready for PQgetResult */
	PGASYNC_COPY_IN,			/* Copy In data transfer in progress */
	PGASYNC_COPY_OUT			/* Copy Out data transfer in progress */
} PGAsyncStatusType;

/* PGQueryClass tracks which query protocol we are now executing */
typedef enum
{
	PGQUERY_SIMPLE,				/* simple Query protocol (PQexec) */
	PGQUERY_EXTENDED,			/* full Extended protocol (PQexecParams) */
	PGQUERY_PREPARE,			/* Parse only (PQprepare) */
	PGQUERY_DESCRIBE			/* Describe Statement or Portal */
} PGQueryClass;

/* PGSetenvStatusType defines the state of the PQSetenv state machine */
/* (this is used only for 2.0-protocol connections) */
typedef enum
{
	SETENV_STATE_OPTION_SEND,	/* About to send an Environment Option */
	SETENV_STATE_OPTION_WAIT,	/* Waiting for above send to complete */
	SETENV_STATE_QUERY1_SEND,	/* About to send a status query */
	SETENV_STATE_QUERY1_WAIT,	/* Waiting for query to complete */
	SETENV_STATE_QUERY2_SEND,	/* About to send a status query */
	SETENV_STATE_QUERY2_WAIT,	/* Waiting for query to complete */
	SETENV_STATE_IDLE
} PGSetenvStatusType;

/* Typedef for the EnvironmentOptions[] array */
typedef struct PQEnvironmentOption
{
	const char *envName,		/* name of an environment variable */
			   *pgName;			/* name of corresponding SET variable */
} PQEnvironmentOption;

/* Typedef for parameter-status list entries */
typedef struct pgParameterStatus
{
	struct pgParameterStatus *next;		/* list link */
	char	   *name;			/* parameter name */
	char	   *value;			/* parameter value */
	/* Note: name and value are stored in same malloc block as struct is */
} pgParameterStatus;

/* large-object-access data ... allocated only if large-object code is used. */
typedef struct pgLobjfuncs
{
	Oid			fn_lo_open;		/* OID of backend function lo_open		*/
	Oid			fn_lo_close;	/* OID of backend function lo_close		*/
	Oid			fn_lo_creat;	/* OID of backend function lo_creat		*/
	Oid			fn_lo_create;	/* OID of backend function lo_create	*/
	Oid			fn_lo_unlink;	/* OID of backend function lo_unlink	*/
	Oid			fn_lo_lseek;	/* OID of backend function lo_lseek		*/
	Oid			fn_lo_tell;		/* OID of backend function lo_tell		*/
	Oid			fn_lo_truncate; /* OID of backend function lo_truncate	*/
	Oid			fn_lo_read;		/* OID of backend function LOread		*/
	Oid			fn_lo_write;	/* OID of backend function LOwrite		*/
} PGlobjfuncs;

/*
 * PGconn stores all the state data associated with a single connection
 * to a backend.
 */
struct pg_conn
{
	/* Saved values of connection options */
	char	   *pghost;			/* the machine on which the server is running */
	char	   *pghostaddr;		/* the numeric IP address of the machine on
								 * which the server is running.  Takes
								 * precedence over above. */
	char	   *pgport;			/* the server's communication port */
	char	   *pgunixsocket;	/* the Unix-domain socket that the server is
								 * listening on; if NULL, uses a default
								 * constructed from pgport */
	char	   *pgtty;			/* tty on which the backend messages is
								 * displayed (OBSOLETE, NOT USED) */
	char	   *connect_timeout;	/* connection timeout (numeric string) */
	char	   *pgoptions;		/* options to start the backend with */
	char	   *appname;		/* application name */
	char	   *fbappname;		/* fallback application name */
	char	   *dbName;			/* database name */
	char	   *pguser;			/* Postgres username and password, if any */
	char	   *pgpass;
	char	   *keepalives;		/* use TCP keepalives? */
	char	   *keepalives_idle;	/* time between TCP keepalives */
	char	   *keepalives_interval;	/* time between TCP keepalive
										 * retransmits */
	char	   *keepalives_count;		/* maximum number of TCP keepalive
										 * retransmits */

	char	   *dboid; 		 	/* gpsql */
	char	   *dbdtsoid;		/* gpsql */
	char	   *bootstrap_user; /* gpsql */
	char	   *encoding;		/* gpsql */

    char       *gpqeid;        /* MPP: session id & startup info for qExec */

	/* Optional file to write trace info to */
	FILE	   *Pfdebug;

	/* Callback procedures for notice message processing */
	PGNoticeHooks noticeHooks;

	/* Status indicators */
	ConnStatusType status;
	PGAsyncStatusType asyncStatus;
	PGTransactionStatusType xactStatus; /* never changes to ACTIVE */

	/* Transaction information passed from EXECUTOR to DISPATCHER */
	bool						utility_mode;
	bool						QEWriter_HaveInfo;
	DistributedTransactionId 	QEWriter_DistributedTransactionId;
	CommandId 					QEWriter_CommandId;
	bool 						QEWriter_Dirty;
	
	/* XLOG passed from Standby Master to Primary */
	bool						Standby_HaveInfo;
	uint32						Standby_xlogid;		/* log file #, 0 based */
	uint32						Standby_xrecoff;	/* byte offset of location in log file */
		
	PGQueryClass queryclass;
	char	   *last_query;		/* last SQL command, or NULL if unknown */
	bool		options_valid;	/* true if OK to attempt connection */
	bool		nonblocking;	/* whether this connection is using nonblock
								 * sending semantics */
	char		copy_is_binary; /* 1 = copy binary, 0 = copy text */
	int			copy_already_done;		/* # bytes already returned in COPY
										 * OUT */
	PGnotify   *notifyHead;		/* oldest unreported Notify msg */
	PGnotify   *notifyTail;		/* newest unreported Notify msg */

	/* Connection data */
	int			sock;			/* Unix FD for socket, -1 if not connected */
	SockAddr	laddr;			/* Local address */
	SockAddr	raddr;			/* Remote address */
	ProtocolVersion pversion;	/* FE/BE protocol version in use */
	int			sversion;		/* server version, e.g. 70401 for 7.4.1 */
	bool		password_needed;	/* true if server demanded a password */
	bool		dot_pgpass_used;	/* true if used .pgpass */
	bool		sigpipe_so;		/* have we masked SIGPIPE via SO_NOSIGPIPE? */
	bool		sigpipe_flag;	/* can we mask SIGPIPE via MSG_NOSIGNAL? */

	/* Transient state needed while establishing connection */
	struct addrinfo *addrlist;	/* list of possible backend addresses */
	struct addrinfo *addr_cur;	/* the one currently being tried */
	int			addrlist_family;	/* needed to know how to free addrlist */
	PGSetenvStatusType setenv_state;	/* for 2.0 protocol only */
	const PQEnvironmentOption *next_eo;
	bool		send_appname;	/* okay to send application_name? */

	/* Miscellaneous stuff */
	int			be_pid;			/* PID of backend --- needed for cancels */
	int			be_key;			/* key of backend --- needed for cancels */
	
	int			motion_listener; /* CDB tcp port for the interconnect listener. */
    int64      mop_high_watermark;   /* highwater mark for mop */
	char		*qe_version;
	
	char		md5Salt[4];		/* password salt received from backend */
	pgParameterStatus *pstatus; /* ParameterStatus data */
	int			client_encoding;	/* encoding id */
	bool		std_strings;	/* standard_conforming_strings */
	PGVerbosity verbosity;		/* error/notice message verbosity */
	PGlobjfuncs *lobjfuncs;		/* private state for large-object access fns */

	/* Buffer for data received from backend and not yet processed */
	char	   *inBuffer;		/* currently allocated buffer */
	int			inBufSize;		/* allocated size of buffer */
	int			inStart;		/* offset to first unconsumed data in buffer */
	int			inCursor;		/* next byte to tentatively consume */
	int			inEnd;			/* offset to first position after avail data */

	/* Buffer for data not yet sent to backend */
	char	   *outBuffer;		/* currently allocated buffer */
	int			outBufSize;		/* allocated size of buffer */
	int			outCount;		/* number of chars waiting in buffer */

	bool		outBuffer_shared;

	/* State for constructing messages in outBuffer */
	int			outMsgStart;	/* offset to msg start (length word); if -1,
								 * msg has no length word */
	int			outMsgEnd;		/* offset to msg end (so far) */

	/* Status for asynchronous result construction */
	PGresult   *result;			/* result being constructed */
	PGresAttValue *curTuple;	/* tuple currently being read */


	/* Buffer for current error message */
	PQExpBufferData errorMessage;		/* expansible string */

	/* Buffer for receiving various parts of messages */
	PQExpBufferData workBuffer; /* expansible string */
};

/* PGcancel stores all data necessary to cancel a connection. A copy of this
 * data is required to safely cancel a connection running on a different
 * thread.
 */
struct pg_cancel
{
	SockAddr	raddr;			/* Remote address */
	int			be_pid;			/* PID of backend --- needed for cancels */
	int			be_key;			/* key of backend --- needed for cancels */
};


/* String descriptions of the ExecStatusTypes.
 * direct use of this array is deprecated; call PQresStatus() instead.
 */
extern char *const pgresStatus[];

/* ----------------
 * Internal functions of libpq
 * Functions declared here need to be visible across files of libpq,
 * but are not intended to be called by applications.  We use the
 * convention "pqXXX" for internal functions, vs. the "PQxxx" names
 * used for application-visible routines.
 * ----------------
 */

/* === in fe-connect.c === */

extern int pqPacketSend(PGconn *conn, char pack_type,
			 const void *buf, size_t buf_len);
extern bool pqGetHomeDirectory(char *buf, int bufsize);

#ifdef ENABLE_THREAD_SAFETY
extern pgthreadlock_t pg_g_threadlock;

#define PGTHREAD_ERROR(msg) \
	do { \
		fprintf(stderr, "%s\n", msg); \
		exit(1); \
	} while (0)


#define pglock_thread()		pg_g_threadlock(true)
#define pgunlock_thread()	pg_g_threadlock(false)
#else
#define pglock_thread()		((void) 0)
#define pgunlock_thread()	((void) 0)
#endif

/* === in fe-exec.c === */

extern void pqSetResultError(PGresult *res, const char *msg);
extern void pqCatenateResultError(PGresult *res, const char *msg);
extern void *pqResultAlloc(PGresult *res, size_t nBytes, bool isBinary);
extern char *pqResultStrdup(PGresult *res, const char *str);
extern void pqClearAsyncResult(PGconn *conn);
extern void pqSaveErrorResult(PGconn *conn);
extern PGresult *pqPrepareAsyncResult(PGconn *conn);
extern void
pqInternalNotice(const PGNoticeHooks *hooks, const char *fmt,...)
/* This lets gcc check the format string for consistency. */
__attribute__((format(printf, 2, 3)));
extern int	pqAddTuple(PGresult *res, PGresAttValue *tup);
extern void pqSaveMessageField(PGresult *res, char code,
				   const char *value);
extern void pqSaveParameterStatus(PGconn *conn, const char *name,
					  const char *value);
extern void pqHandleSendFailure(PGconn *conn);

/* === in fe-protocol2.c === */
/*
 *  fe-protocol2 isn't used for QD to QE communications
 *
extern PostgresPollingStatusType pqSetenvPoll(PGconn *conn);

extern char *pqBuildStartupPacket2(PGconn *conn, int *packetlen,
					  const PQEnvironmentOption *options);
extern void pqParseInput2(PGconn *conn);
extern int	pqGetCopyData2(PGconn *conn, char **buffer, int async);
extern int	pqGetline2(PGconn *conn, char *s, int maxlen);
extern int	pqGetlineAsync2(PGconn *conn, char *buffer, int bufsize);
extern int	pqEndcopy2(PGconn *conn);
extern PGresult *pqFunctionCall2(PGconn *conn, Oid fnid,
				int *result_buf, int *actual_result_len,
				int result_is_int,
				const PQArgBlock *args, int nargs);
*/
/* === in fe-protocol3.c === */

extern char *pqBuildStartupPacket3(PGconn *conn, int *packetlen,
					  const PQEnvironmentOption *options);
extern void pqParseInput3(PGconn *conn);
extern int	pqGetErrorNotice3(PGconn *conn, bool isError);
extern int	pqGetCopyData3(PGconn *conn, char **buffer, int async);
extern int	pqGetline3(PGconn *conn, char *s, int maxlen);
extern int	pqGetlineAsync3(PGconn *conn, char *buffer, int bufsize);
extern int	pqEndcopy3(PGconn *conn);
extern PGresult *pqFunctionCall3(PGconn *conn, Oid fnid,
				int *result_buf, int *actual_result_len,
				int result_is_int,
				const PQArgBlock *args, int nargs);

/* === in fe-misc.c === */

 /*
  * "Get" and "Put" routines return 0 if successful, EOF if not. Note that for
  * Get, EOF merely means the buffer is exhausted, not that there is
  * necessarily any error.
  */
extern int	pqCheckOutBufferSpace(size_t bytes_needed, PGconn *conn);
extern int	pqCheckInBufferSpace(size_t bytes_needed, PGconn *conn);
extern int	pqGetc(char *result, PGconn *conn);
extern int	pqPutc(char c, PGconn *conn);
extern int	pqGets(PQExpBuffer buf, PGconn *conn);
extern int	pqGets_append(PQExpBuffer buf, PGconn *conn);
extern int	pqPuts(const char *s, PGconn *conn);
extern int	pqGetnchar(char *s, size_t len, PGconn *conn);
extern int	pqPutnchar(const char *s, size_t len, PGconn *conn);
extern int	pqGetInt(int *result, size_t bytes, PGconn *conn);
extern int64 pqGetInt64(int64 *result, PGconn *conn);  /* GPDB only */
extern int	pqPutInt(int value, size_t bytes, PGconn *conn);
extern int	pqPutMsgStart(char msg_type, bool force_len, PGconn *conn);
extern int	pqPutMsgEnd(PGconn *conn);
extern void pqPutMsgEndNoAutoFlush(PGconn *conn);   /* GPDB only */
extern int	pqReadData(PGconn *conn);
extern int	pqFlush(PGconn *conn);
extern int	pqFlushNonBlocking(PGconn *conn);
extern int	pqWait(int forRead, int forWrite, PGconn *conn);
extern int pqWaitTimed(int forRead, int forWrite, PGconn *conn,
			time_t finish_time);
extern int pqWaitTimeout(int forRead, int forWrite, PGconn *conn,
			time_t finish_time);
extern int	pqReadReady(PGconn *conn);
extern int	pqWriteReady(PGconn *conn);

/* === in fe-secure.c === */
/*
extern int	pqsecure_initialize(PGconn *);
extern void pqsecure_destroy(void);
extern PostgresPollingStatusType pqsecure_open_client(PGconn *);
extern void pqsecure_close(PGconn *);
extern ssize_t pqsecure_read(PGconn *, void *ptr, size_t len);
extern ssize_t pqsecure_write(PGconn *, const void *ptr, size_t len);
*/

/*

#if defined(ENABLE_THREAD_SAFETY) && !defined(WIN32)
extern int	pq_block_sigpipe(sigset_t *osigset, bool *sigpipe_pending);
extern void pq_reset_sigpipe(sigset_t *osigset, bool sigpipe_pending,
				 bool got_epipe);
#endif
*/

/*
 * this is so that we can check if a connection is non-blocking internally
 * without the overhead of a function call
 */
#define pqIsnonblocking(conn)	((conn)->nonblocking)

#ifdef ENABLE_NLS
#define libpq_gettext(x) gettext(x)
#else
#define libpq_gettext(x) (x)
#endif

/*
 * These macros are needed to let error-handling code be portable between
 * Unix and Windows.  (ugh)
 */
#ifdef WIN32
#define SOCK_ERRNO (WSAGetLastError())
#define SOCK_STRERROR winsock_strerror
#define SOCK_ERRNO_SET(e) WSASetLastError(e)
#else
#define SOCK_ERRNO errno
#define SOCK_STRERROR pqStrerror
#define SOCK_ERRNO_SET(e) (errno = (e))
#endif

#endif   /* LIBPQ_INT_H */
