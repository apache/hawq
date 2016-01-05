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
 * elog.h
 *	  POSTGRES error reporting/logging definitions.
 *
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/utils/elog.h,v 1.82 2006/03/05 15:59:07 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef ELOG_H
#define ELOG_H

#include "c.h"
#include <sys/time.h>
#include <setjmp.h>

/* Error level codes */
#define DEBUG5		10			/* Debugging messages, in categories of
								 * decreasing detail. */
#define DEBUG4		11
#define DEBUG3		12
#define DEBUG2		13
#define DEBUG1		14			/* used by GUC debug_* variables */
#define LOG			15			/* Server operational messages; sent only to
								 * server log by default. */
#define COMMERROR	16			/* Client communication problems; same as LOG
								 * for server reporting, but never sent to
								 * client. */
#define INFO		17			/* Messages specifically requested by user
								 * (eg VACUUM VERBOSE output); always sent to
								 * client regardless of client_min_messages,
								 * but by default not sent to server log. */
#define NOTICE		18			/* Helpful messages to users about query
								 * operation;  sent to client and server log
								 * by default. */
#define WARNING		19			/* Warnings.  NOTICE is for expected messages
								 * like implicit sequence creation by SERIAL.
								 * WARNING is for unexpected messages. */
#define ERROR		20			/* user error - abort transaction; return to
								 * known state */
/* Save ERROR value in PGERROR so it can be restored when Win32 includes
 * modify it.  We have to use a constant rather than ERROR because macros
 * are expanded only when referenced outside macros.
 */
#ifdef WIN32
#define PGERROR		20
#endif
#define FATAL		21			/* fatal error - abort process */
#define PANIC		22			/* take down the other backends with me */

 /* #define DEBUG DEBUG1 */	/* Backward compatibility with pre-7.3 */


/* macros for representing SQLSTATE strings compactly */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define PGUNSIXBIT(val) (((val) & 0x3F) + '0')

#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

/* These macros depend on the fact that '0' becomes a zero in SIXBIT */
#define ERRCODE_TO_CATEGORY(ec)  ((ec) & ((1 << 12) - 1))
#define ERRCODE_IS_CATEGORY(ec)  (((ec) & ~((1 << 12) - 1)) == 0)

/* SQLSTATE codes for errors are defined in a separate file */
#include "utils/errcodes.h"

/* Common error messages */
#define ERRMSG_GP_INSUFFICIENT_STATEMENT_MEMORY "insufficient memory reserved for statement"


/* Which __func__ symbol do we have, if any? */
#ifdef HAVE_FUNCNAME__FUNC
#define PG_FUNCNAME_MACRO	__func__
#else
#ifdef HAVE_FUNCNAME__FUNCTION
#define PG_FUNCNAME_MACRO	__FUNCTION__
#else
#define PG_FUNCNAME_MACRO	NULL
#endif
#endif

/* threaded thing. 
 * Caller beware: ereport and elog can only be called from main thread.
 */
#include <pthread.h>
extern pthread_t main_tid;
#ifndef _WIN32
#define mythread() ((unsigned long) pthread_self())
#define mainthread() ((unsigned long) main_tid)
#else
#define mythread() ((unsigned long) pthread_self().p)
#define mainthread() ((unsigned long) main_tid.p)
#endif 

/*
 * Insist(assertion)
 *
 * Returns true if assertion is true; else issues a generic internal
 * error message and exits to the closest enclosing PG_TRY block via
 * plain old ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), ...)).
 *
 ** Use instead of Assert() for internal errors that should always be checked
 *  even in release builds; and to suppress warnings (such as uninitialized
 *  variables) along paths that should never be executed (eg. switch defaults).
 ** Use instead of ExceptionalCondition() for internal errors that are not
 *  so severe as to necessitate aborting the process, where ordinary error
 *  handling should suffice.
 ** Use when elog()/ereport() is just not worth it because no user would
 *  ever see the nice customized message that you would otherwise code up
 *  for that weird case that should never happen.
 ** Can be used in macros and conditional expressions.
 */
/*
 * TODO  Chuck asks:  Why aren't we passing the text of the assertion to elog_internalerror?
 * How is anybody supposed to know what was wrong?
 */
#define Insist(assertion)   \
    ( (assertion)           \
        ? true              \
	    : (elog_internalerror(__FILE__, __LINE__, PG_FUNCNAME_MACRO), false) )

/*
 * Insist(bool assertion, const char * fmt, ... )
 * Similar to Insist() except it outputs a custom message using elog(LOG, ...)
 * before exiting.
 */
#define insist_log(assertion, ... ) 	\
	( (assertion)						\
		? true							\
		: ({ elog(LOG, __VA_ARGS__);  elog_internalerror(__FILE__, __LINE__, PG_FUNCNAME_MACRO); } ))

#ifdef _MSC_VER
__declspec(noreturn)
#endif
void elog_internalerror(const char *filename, int lineno, const char *funcname)
                       __attribute__((__noreturn__));


/*----------
 * New-style error reporting API: to be used in this way:
 *		ereport(ERROR,
 *				(errcode(ERRCODE_UNDEFINED_CURSOR),
 *				 errmsg("portal \"%s\" not found", stmt->portalname),
 *				 ... other errxxx() fields as needed ...));
 *
 * The error level is required, and so is a primary error message (errmsg
 * or errmsg_internal).  All else is optional.	errcode() defaults to
 * ERRCODE_INTERNAL_ERROR if elevel is ERROR or more, ERRCODE_WARNING
 * if elevel is WARNING, or ERRCODE_SUCCESSFUL_COMPLETION if elevel is
 * NOTICE or below.
 *
 * ereport_domain() allows a message domain to be specified, for modules that
 * wish to use a different message catalog from the backend's.	To avoid having
 * one copy of the default text domain per .o file, we define it as NULL here
 * and have errstart insert the default text domain.  Modules can either use
 * ereport_domain() directly, or preferably they can override the TEXTDOMAIN
 * macro.
 *----------
 */
#define ereport_domain(elevel, domain, rest)	\
	(errstart(elevel, __FILE__, __LINE__, PG_FUNCNAME_MACRO, domain) ? \
	 (errfinish rest) : (void) 0)

#define ereport(elevel, rest)	\
	ereport_domain(elevel, TEXTDOMAIN, rest)

#define TEXTDOMAIN NULL

extern bool errstart(int elevel, const char *filename, int lineno,
		 const char *funcname, const char *domain);
extern void errfinish(int dummy,...);

extern int	errcode(int sqlerrcode);

extern int	errcode_for_file_access(void);
extern int	errcode_for_socket_access(void);

extern int
errmsg(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int
errmsg_internal(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int
errmsg_plural(const char *fmt_singular, const char *fmt_plural,
			  unsigned long n, ...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 4)))
__attribute__((format(printf, 2, 4)));

extern int
errdetail(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int
errdetail_log(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int
errdetail_plural(const char *fmt_singular, const char *fmt_plural,
				 unsigned long n, ...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 4)))
__attribute__((format(printf, 2, 4)));

extern int
errhint(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int
errcontext(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

extern int	errhidestmt(bool hide_stmt);

extern int	errfunction(const char *funcname);
extern int	errposition(int cursorpos);

extern int	errprintstack(bool printstack);

extern int	internalerrposition(int cursorpos);
extern int	internalerrquery(const char *query);

extern int	geterrcode(void);
extern int	geterrposition(void);
extern int	getinternalerrposition(void);

extern int errOmitLocation(bool omitLocation);   /* GPDB */

extern int errFatalReturn(bool fatalReturn); /* GPDB: true => return on FATAL error */

extern int errSendAlert(bool sendAlert);		/* GPDB: Send alert via e-mail or SNMP */

extern int errSuppressOutputToLog(void);   /* GP */

// Generally a good idea!
extern bool SuppressPanic;   /* GP */

/*----------
 * Old-style error reporting API: to be used in this way:
 *		elog(ERROR, "portal \"%s\" not found", stmt->portalname);
 *----------
 */
#define elog	elog_start(__FILE__, __LINE__, PG_FUNCNAME_MACRO), elog_finish

extern void elog_start(const char *filename, int lineno, const char *funcname);
extern void
elog_finish(int elevel, const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 2, 3)));


/* Support for attaching context information to error reports */

typedef struct ErrorContextCallback
{
	struct ErrorContextCallback *previous;
	void		(*callback) (void *arg);
	void	   *arg;
} ErrorContextCallback;

extern PGDLLIMPORT ErrorContextCallback *error_context_stack;


/*----------
 * API for catching ereport(ERROR) exits.  Use these macros like so:
 *
 *		PG_TRY();
 *		{
 *			... code that might throw ereport(ERROR) ...
 *		}
 *		PG_CATCH();
 *		{
 *			... error recovery code ...
 *		}
 *		PG_END_TRY();
 *
 * (The braces are not actually necessary, but are recommended so that
 * pg_indent will indent the construct nicely.)  The error recovery code
 * can optionally do PG_RE_THROW() to propagate the same error outwards.
 *
 * Note: while the system will correctly propagate any new ereport(ERROR)
 * occurring in the recovery section, there is a small limit on the number
 * of levels this will work for.  It's best to keep the error recovery
 * section simple enough that it can't generate any new errors, at least
 * not before popping the error stack.
 *
 * Note: an ereport(FATAL) will not be caught by this construct; control will
 * exit straight through proc_exit().  Therefore, do NOT put any cleanup
 * of non-process-local resources into the error recovery section, at least
 * not without taking thought for what will happen during ereport(FATAL).
 * The PG_ENSURE_ERROR_CLEANUP macros provided by storage/ipc.h may be
 * helpful in such cases.
 *----------
 */
#define PG_TRY()  \
	do { \
		sigjmp_buf *save_exception_stack = PG_exception_stack; \
		ErrorContextCallback *save_context_stack = error_context_stack; \
		sigjmp_buf local_sigjmp_buf; \
		if (sigsetjmp(local_sigjmp_buf, 0) == 0) \
		{ \
			PG_exception_stack = &local_sigjmp_buf

#define PG_CATCH()	\
		} \
		else \
		{ \
			PG_exception_stack = save_exception_stack; \
			error_context_stack = save_context_stack

#define PG_END_TRY()  \
		} \
		PG_exception_stack = save_exception_stack; \
		error_context_stack = save_context_stack; \
	} while (0)

#define PG_RE_THROW()  \
	siglongjmp(*PG_exception_stack, 1)

extern PGDLLIMPORT sigjmp_buf *PG_exception_stack;


/* Stuff that error handlers might want to use */

/*
 * ErrorData holds the data accumulated during any one ereport() cycle.
 * Any non-NULL pointers must point to palloc'd data.
 * (The const pointers are an exception; we assume they point at non-freeable
 * constant strings.)
 */
typedef struct ErrorData
{
	int			elevel;			/* error level */
	bool		output_to_server;		/* will report to server log? */
	bool		output_to_client;		/* will report to client? */
	bool		show_funcname;	/* true to force funcname inclusion */
    bool        omit_location;  /* GPDB: don't add filename:line# and stack trace */
    bool        fatal_return;   /* GPDB: true => return instead of proc_exit() */
	bool		hide_stmt;		/* true to prevent STATEMENT: inclusion */
	bool		send_alert;		/* GPDB: send e-mail alert and/or SNMP trap/inform */
	const char *filename;		/* __FILE__ of ereport() call */
	int			lineno;			/* __LINE__ of ereport() call */
	const char *funcname;		/* __func__ of ereport() call */
	const char *domain;			/* message domain */
	int			sqlerrcode;		/* encoded ERRSTATE */
	char	   *message;		/* primary error message */
	char	   *detail;			/* detail error message */
	char	   *detail_log;		/* detail error message for server log only */
	char	   *hint;			/* hint message */
	char	   *context;		/* context message */
	int			cursorpos;		/* cursor index into query string */
	int			internalpos;	/* cursor index into internalquery */
	char	   *internalquery;	/* text of internally-generated query */
	int			saved_errno;	/* errno at entry */
	void	   *stacktracearray[30];
	size_t		stacktracesize;
	bool		printstack;		/* force output stack trace */
} ErrorData;

extern void EmitErrorReport(void);
extern ErrorData *CopyErrorData(void);
extern void FreeErrorData(ErrorData *edata);
extern void FlushErrorState(void);
extern void ReThrowError(ErrorData *edata)  __attribute__((__noreturn__));
extern void pg_re_throw(void) __attribute__((noreturn));

/*
 * CDB: elog_demote
 *
 * A PG_CATCH() handler can call this to downgrade the error that it is
 * currently handling to a level lower than ERROR.  The caller should
 * then do PG_RE_THROW() to proceed to the next error handler.  
 *
 * Clients using libpq cannot receive normal output together with an error.
 * The libpq frontend discards any results already buffered when a command
 * completes with an error notification of level ERROR or higher.  
 *
 * elog_demote() can be used to reduce the error level reported to the client
 * so that libpq won't suppress normal output, while the backend still frees
 * resources, aborts the transaction, etc, as usual.
 *
 * Returns true if successful, false if the request is disallowed.
 */
bool        elog_demote(int downgrade_to_elevel);

/*
 * CDB: elog_dismiss 
 *
 * A PG_CATCH() handler can call this to downgrade the error that it is
 * currently handling to a level lower than ERROR, report it to the log
 * and/or client as appropriate, and purge it from the error system.
 *
 * This shouldn't be attempted unless the caller is certain that the
 * error does not need the services of upper level error handlers to
 * release resources, abort the transaction, etc.
 *
 * Returns true if successful, in which case the error has been expunged
 * and the caller should not do PG_RE_THROW(), but should instead fall or
 * jump out of the PG_CATCH() handler and resume normal execution.
 *
 * Returns false if unsuccessful; then the caller should carry on as 
 * PG_CATCH() handlers ordinarily do, and exit via PG_RE_THROW().  
 */
bool        elog_dismiss(int downgrade_to_elevel);   

/*
 * CDB: elog_geterrcode
 * Return the SQLSTATE code for the error currently being handled, or 0.
 *
 * This is only intended for use in error handlers.
 */
int         elog_geterrcode(void);      
int         elog_getelevel(void);      
char        *elog_message(void);


/* GUC-configurable parameters */

typedef enum
{
	PGERROR_TERSE,				/* single-line error messages */
	PGERROR_DEFAULT,			/* recommended style */
	PGERROR_VERBOSE				/* all the facts, ma'am */
} PGErrorVerbosity;

extern int	Log_error_verbosity;
extern char *Log_line_prefix;
extern int	Log_destination;

/* Log destination bitmap */
#define LOG_DESTINATION_STDERR	 1
#define LOG_DESTINATION_SYSLOG	 2
#define LOG_DESTINATION_EVENTLOG 4
#define LOG_DESTINATION_CSVLOG	 8

/* Other exported functions */
extern void DebugFileOpen(void);
extern char *unpack_sql_state(int sql_state);
extern bool in_error_recursion_trouble(void);

#ifdef HAVE_SYSLOG
extern void set_syslog_parameters(const char *ident, int facility);
#endif

/*
 * Write errors to stderr (or by equal means when stderr is
 * not available). Used before ereport/elog can be used
 * safely (memory context, GUC load etc)
 */
extern void
write_stderr(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));

#if defined(pg_on_solaris)
extern size_t backtrace(void **buffer, int size);
extern char **backtrace_symbols(void *const *buffer, int size);
#endif

extern void GetLastLogTimeVal(struct timeval *lastLogTimeVal);
extern void write_message_to_server_log(int elevel,
										int sqlerrcode,
										const char *message,
										const char *detail,
										const char *hint,
										const char *query_text,
										int cursorpos,
										int internalpos,
										const char *internalquery,
										const char *context,
										const char *funcname,
										bool show_funcname,
										const char *filename,
										int lineno,
										int stacktracesize,
										bool omit_location,
										bool send_alert,
										void* const *stacktracearray,
										bool printstack);

extern void debug_backtrace(void);
extern uint32 gp_backtrace(void **stackAddresses, uint32 maxStackDepth);
extern char *gp_stacktrace(void **stackAddresses, uint32 stackDepth);

/* stack base pointer, defined in postgres.c */
extern char *stack_base_ptr;

/* GUCs */
extern bool gp_log_stack_trace_lines;   /* session GUC, controls line info in stack traces */

extern const char *SegvBusIllName(int signal);
extern void StandardHandlerForSigillSigsegvSigbus_OnMainThread(char * processName, SIGNAL_ARGS);

extern void pending_for_debug(int timeout);

#endif   /* ELOG_H */
