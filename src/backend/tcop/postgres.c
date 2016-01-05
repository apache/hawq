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
 * postgres.c
 *	  POSTGRES C Backend Interface
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/tcop/postgres.c,v 1.518.2.1 2007/01/04 00:58:01 tgl Exp $
 *
 * NOTES
 *	  this is the "main" module of the postgres backend and
 *	  hence the main module of the "traffic cop".
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "gpmon/gpmon.h"

#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/socket.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#ifndef HAVE_GETRUSAGE
#include "rusagestub.h"
#endif

#include "access/printtup.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/async.h"
#include "commands/prepare.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"            /* Slice, SliceTable */
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#include "pg_trace.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "rewrite/rewriteHandler.h"
#include "storage/freespace.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "tcop/fastpath.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/flatfiles.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/datum.h"
#include "utils/debugbreak.h"
#include "mb/pg_wchar.h"
#include "cdb/cdbvars.h"
#include "cdb/cdblogsync.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbfilesystemcredential.h"
#include "cdb/ml_ipc.h"
#include "utils/guc.h"
#include "access/twophase.h"
#include <pthread.h>
#include "utils/resscheduler.h"
#include "pgstat.h"
#include "executor/nodeFunctionscan.h"
#include "executor/execdesc.h"
#include "cdb/cdbfilerep.h"
#include "postmaster/primary_mirror_mode.h"
#include "postmaster/identity.h"
#include "utils/vmem_tracker.h"

#include "cdb/cdbinmemheapam.h"

#include "resourcemanager/dynrm.h"
#include "resourcemanager/envswitch.h"
#include "resourcemanager/communication/rmcomm_QD2RM.h"
#include "resourcemanager/communication/rmcomm_MessageHandler.h"
#include "resourcemanager/communication/rmcomm_QE_RMSEG_Protocol.h"
#include "resourcemanager/communication/rmcomm_SyncComm.h"
#include "resourcemanager/communication/rmcomm_QE2RMSEG.h"
#include "resourcemanager/errorcode.h"
#include "resourcemanager/utils/linkedlist.h"
#include "resourcemanager/resourcemanager.h"
#include "resourcemanager/resourceenforcer/resourceenforcer_hash.h"
#include "resourcemanager/resourceenforcer/resourceenforcer.h"

extern int	optind;
extern char *optarg;

extern char *savedSeqServerHost;
extern int savedSeqServerPort;

/* ----------------
 *		global variables
 * ----------------
 */
const char *debug_query_string; /* for pgmonitor and log_min_error_statement */

/* Note: whereToSendOutput is initialized for the bootstrap/standalone case */
CommandDest whereToSendOutput = DestDebug;

/* flag for logging end of session */
bool		Log_disconnections = false;

int			log_statement = LOGSTMT_NONE;

/* GUC variable for maximum stack depth (measured in kilobytes) */
int			max_stack_depth = 100;

/* wait N seconds to allow attach from a debugger */
int			PostAuthDelay = 0;


/* ----------------
 *		private variables
 * ----------------
 */

/* Priority of the postmaster process */
static int PostmasterPriority = 0;

/* max_stack_depth converted to bytes for speed of checking */
static long max_stack_depth_bytes = 100 * 1024L;

/*
 * Stack base pointer -- initialized by PostgresMain. This is not static
 * so that PL/Java can modify it.
 */
char	   *stack_base_ptr = NULL;


/*
 * Flag to mark SIGHUP. Whenever the main loop comes around it
 * will reread the configuration file. (Better than doing the
 * reading in the signal handler, ey?)
 */
static volatile sig_atomic_t got_SIGHUP = false;

/*
 * Flag to keep track of whether we have started a transaction.
 * For extended query protocol this has to be remembered across messages.
 */
static bool xact_started = false;

/*
 * Flag to indicate that we are doing the outer loop's read-from-client,
 * as opposed to any random read from client that might happen within
 * commands like COPY FROM STDIN.
 *
 * GPDB:  I've made this extern so we can test it in the sigalarm handler
 * in proc.c.
 */
extern bool DoingCommandRead;
bool DoingCommandRead = false;

/*
 * Flags to implement skip-till-Sync-after-error behavior for messages of
 * the extended query protocol.
 */
static bool doing_extended_query_message = false;
static bool ignore_till_sync = false;

/*
 * If an unnamed prepared statement exists, it's stored here.
 * We keep it separate from the hashtable kept by commands/prepare.c
 * in order to reduce overhead for short-lived queries.
 */
static MemoryContext unnamed_stmt_context = NULL;
static PreparedStatement *unnamed_stmt_pstmt = NULL;


static bool EchoQuery = false;	/* default don't echo */

extern pthread_t main_tid;
#ifndef _WIN32
pthread_t main_tid = (pthread_t)0;
#else
pthread_t main_tid = {0,0};
#endif

/* if we're in the middle of dying, let our threads exit with some dignity */
static volatile sig_atomic_t in_quickdie = false;

/*
 * people who want to use EOF should #define DONTUSENEWLINE in
 * tcop/tcopdebug.h
 */
#ifndef TCOP_DONTUSENEWLINE
static int	UseNewLine = 1;		/* Use newlines query delimiters (the default) */
#else
static int	UseNewLine = 0;		/* Use EOF as query delimiters */
#endif   /* TCOP_DONTUSENEWLINE */


extern void CheckForQDMirroringWork(void);

extern bool ResourceScheduler;

static int64 master_start_time		  = 0;
static bool  has_been_moved_to_cgroup = false;

/* ----------------------------------------------------------------
 *		decls for routines only used in this file
 * ----------------------------------------------------------------
 */
static int	InteractiveBackend(StringInfo inBuf);
static int	SocketBackend(StringInfo inBuf);
static int	ReadCommand(StringInfo inBuf);
static List *pg_rewrite_queries(List *querytree_list);
static bool check_log_statement(List *stmt_list);
static int	errdetail_execute(List *raw_parsetree_list);
static int	errdetail_params(ParamListInfo params);
static void start_xact_command(void);
static void finish_xact_command(void);
static bool IsTransactionExitStmt(Node *parsetree);
static bool IsTransactionExitStmtList(List *parseTrees);
static bool IsTransactionStmtList(List *nodes);
static void SigHupHandler(SIGNAL_ARGS);
static void log_disconnections(int code, Datum arg);
static bool renice_current_process(int nice_level);

/*
 * Change the priority of the current process to the specified level
 * (bigger nice_level values correspond to lower priority).
*/
static bool renice_current_process(int nice_level)
{
#ifdef WIN32
	elog(DEBUG2, "Renicing of processes on Windows currently not supported.");
	return false;
#else
	int prio_out = -1;
	elog(DEBUG2, "Current nice level of the process: %d",
			getpriority(PRIO_PROCESS, 0));
	prio_out = setpriority(PRIO_PROCESS, 0, nice_level);
	if (prio_out == -1)
	{
		switch (errno)
		{
		case EACCES:
			elog(DEBUG1, "Could not change priority of the query process, errno: %d (%m).", errno);
			break;
		case ESRCH:
			/* ignore this, the backend went away when we weren't looking */
			break;
		default:
			elog(DEBUG1, "Could not change priority of the query process, errno: %d (%m).", errno);
		}
		return false;
	}

	elog(DEBUG2, "Reniced process to level %d", getpriority(PRIO_PROCESS, 0));
	return true;
#endif
}

/* ----------------------------------------------------------------
 *		routines to obtain user input
 * ----------------------------------------------------------------
 */

/* ----------------
 *	InteractiveBackend() is called for user interactive connections
 *
 *	the string entered by the user is placed in its parameter inBuf,
 *	and we act like a Q message was received.
 *
 *	EOF is returned if end-of-file input is seen; time to shut down.
 * ----------------
 */

static int
InteractiveBackend(StringInfo inBuf)
{
	int			c;				/* character read from getc() */
	bool		end = false;	/* end-of-input flag */
	bool		backslashSeen = false;	/* have we seen a \ ? */

	/*
	 * display a prompt and obtain input from the user
	 */
	printf("backend> ");
	fflush(stdout);

	/* Reset inBuf to empty */
	inBuf->len = 0;
	inBuf->data[0] = '\0';
	inBuf->cursor = 0;

	for (;;)
	{
		if (UseNewLine)
		{
			/*
			 * if we are using \n as a delimiter, then read characters until
			 * the \n.
			 */
			while ((c = getc(stdin)) != EOF)
			{
				if (c == '\n')
				{
					if (backslashSeen)
					{
						/* discard backslash from inBuf */
						inBuf->data[--inBuf->len] = '\0';
						backslashSeen = false;
						continue;
					}
					else
					{
						/* keep the newline character */
						appendStringInfoChar(inBuf, '\n');
						break;
					}
				}
				else if (c == '\\')
					backslashSeen = true;
				else
					backslashSeen = false;

				appendStringInfoChar(inBuf, (char) c);
			}

			if (c == EOF)
				end = true;
		}
		else
		{
			/*
			 * otherwise read characters until EOF.
			 */
			while ((c = getc(stdin)) != EOF)
				appendStringInfoChar(inBuf, (char) c);

			if (inBuf->len == 0)
				end = true;
		}

		if (end)
			return EOF;

		/*
		 * otherwise we have a user query so process it.
		 */
		break;
	}

	/* Add '\0' to make it look the same as message case. */
	appendStringInfoChar(inBuf, (char) '\0');

	/*
	 * if the query echo flag was given, print the query..
	 */
	if (EchoQuery)
		printf("statement: %s\n", inBuf->data);
	fflush(stdout);

	return 'Q';
}

/* ----------------
 *	SocketBackend()		Is called for frontend-backend connections
 *
 *	Returns the message type code, and loads message body data into inBuf.
 *
 *	EOF is returned if the connection is lost.
 * ----------------
 */
static int
SocketBackend(StringInfo inBuf)
{
	int			qtype;

	/*
	 * Get message type code from the frontend.
	 */
	qtype = pq_getbyte();

	if (!disable_sig_alarm(false))
			elog(FATAL, "could not disable timer for client wiat timeout");

	if (qtype == EOF)			/* frontend disconnected */
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected EOF on client connection")));
		return qtype;
	}

	/*
	 * Validate message type code before trying to read body; if we have lost
	 * sync, better to say "command unknown" than to run out of memory because
	 * we used garbage as a length word.
	 *
	 * This also gives us a place to set the doing_extended_query_message flag
	 * as soon as possible.
	 */
	switch (qtype)
	{
		case 'Q':				/* simple query */
			doing_extended_query_message = false;
			if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
			{
				/* old style without length word; convert */
				if (pq_getstring(inBuf))
				{
					ereport(COMMERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("unexpected EOF on client connection")));
					return EOF;
				}
			}
			break;

		case 'M':				/* Greenplum Database dispatched statement from QD */

			doing_extended_query_message = false;

			/* don't support old protocols with this. */
			if( PG_PROTOCOL_MAJOR(FrontendProtocol) < 3 )
					ereport(COMMERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("Greenplum Database dispatch unsupported for old FrontendProtocols.")));


			break;

		case 'T':				/* Greenplum Database dispatched transaction protocol from QD */

			doing_extended_query_message = false;

			/* don't support old protocols with this. */
			if( PG_PROTOCOL_MAJOR(FrontendProtocol) < 3 )
					ereport(COMMERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("Greenplum Database dispatch unsupported for old FrontendProtocols.")));


			break;
		case 'G':				/* Greenplum Gang Management */
			doing_extended_query_message = false;
			break;

		case 'F':				/* fastpath function call */
			/* we let fastpath.c cope with old-style input of this */
			doing_extended_query_message = false;
			break;

		case 'X':				/* terminate */
			doing_extended_query_message = false;
			ignore_till_sync = false;
			break;

		case 'B':				/* bind */
		case 'C':				/* close */
		case 'D':				/* describe */
		case 'E':				/* execute */
		case 'H':				/* flush */
		case 'P':				/* parse */
			doing_extended_query_message = true;
			/* these are only legal in protocol 3 */
			if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d", qtype)));
			break;

		case 'S':				/* sync */
			/* stop any active skip-till-Sync */
			ignore_till_sync = false;
			/* mark not-extended, so that a new error doesn't begin skip */
			doing_extended_query_message = false;
			/* only legal in protocol 3 */
			if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d", qtype)));
			break;

		case 'd':				/* copy data */
		case 'c':				/* copy done */
		case 'f':				/* copy fail */
			doing_extended_query_message = false;
			/* these are only legal in protocol 3 */
			if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d", qtype)));
			break;

		case 'W':   /* Greenplum Database command for transmitting listener port. */

			break;

		default:

			/*
			 * Otherwise we got garbage from the frontend.	We treat this as
			 * fatal because we have probably lost message boundary sync, and
			 * there's no good way to recover.
			 */
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid frontend message type %d", qtype)));
			break;
	}

	/*
	 * In protocol version 3, all frontend messages have a length word next
	 * after the type code; we can read the message contents independently of
	 * the type.
	 */
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		if (pq_getmessage(inBuf, 0))
			return EOF;			/* suitable message already logged */
	}

	return qtype;
}

/* ----------------
 *		ReadCommand reads a command from either the frontend or
 *		standard input, places it in inBuf, and returns the
 *		message type code (first byte of the message).
 *		EOF is returned if end of file.
 * ----------------
 */
static int
ReadCommand(StringInfo inBuf)
{
	int			result;

	if (whereToSendOutput == DestRemote)
		result = SocketBackend(inBuf);
	else
		result = InteractiveBackend(inBuf);
	return result;
}

/*
 * prepare_for_client_read -- set up to possibly block on client input
 *
 * This must be called immediately before any low-level read from the
 * client connection.  It is necessary to do it at a sufficiently low level
 * that there won't be any other operations except the read kernel call
 * itself between this call and the subsequent client_read_ended() call.
 * In particular there mustn't be use of malloc() or other potentially
 * non-reentrant libc functions.  This restriction makes it safe for us
 * to allow interrupt service routines to execute nontrivial code while
 * we are waiting for input.
 */
void
prepare_for_client_read(void)
{
	if (DoingCommandRead)
	{
		/* Enable immediate processing of asynchronous signals */
		EnableNotifyInterrupt();
		EnableCatchupInterrupt();
		EnableClientWaitTimeoutInterrupt();

		/* Allow "die" interrupt to be processed while waiting */
		ImmediateInterruptOK = true;

		/* And don't forget to detect one that already arrived */
		QueryCancelPending = false;
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * client_read_ended -- get out of the client-input state
 */
void
client_read_ended(void)
{
	if (DoingCommandRead)
	{
		ImmediateInterruptOK = false;
		QueryCancelPending = false;		/* forget any CANCEL signal */

		DisableClientWaitTimeoutInterrupt();
		DisableNotifyInterrupt();
		DisableCatchupInterrupt();
	}
}


/*
 * Parse a query string and pass it through the rewriter.
 *
 * A list of Query nodes is returned, since the string might contain
 * multiple queries and/or the rewriter might expand one query to several.
 *
 * NOTE: this routine is no longer used for processing interactive queries,
 * but it is still needed for parsing of SQL function bodies.
 */
List *
pg_parse_and_rewrite(const char *query_string,	/* string to execute */
					 Oid *paramTypes,	/* parameter types */
					 int numParams)		/* number of parameters */
{
	List	   *raw_parsetree_list;
	List	   *querytree_list;
	ListCell   *list_item;

	/*
	 * (1) parse the request string into a list of raw parse trees.
	 */
	raw_parsetree_list = pg_parse_query(query_string);

	/*
	 * (2) Do parse analysis and rule rewrite.
	 */
	querytree_list = NIL;
	foreach(list_item, raw_parsetree_list)
	{
		Node	   *parsetree = (Node *) lfirst(list_item);

		querytree_list = list_concat(querytree_list,
									 pg_analyze_and_rewrite(parsetree,
															query_string,
															paramTypes,
															numParams));
	}

	return querytree_list;
}

/*
 * Do raw parsing (only).
 *
 * A list of parsetrees is returned, since there might be multiple
 * commands in the given string.
 *
 * NOTE: for interactive queries, it is important to keep this routine
 * separate from the analysis & rewrite stages.  Analysis and rewriting
 * cannot be done in an aborted transaction, since they require access to
 * database tables.  So, we rely on the raw parser to determine whether
 * we've seen a COMMIT or ABORT command; when we are in abort state, other
 * commands are not processed any further than the raw parse stage.
 */
List *
pg_parse_query(const char *query_string)
{
	List	   *raw_parsetree_list;

	if (log_parser_stats)
		ResetUsage();

	raw_parsetree_list = raw_parser(query_string);

	if (log_parser_stats)
		ShowUsage("PARSER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
	/* Optional debugging check: pass raw parsetrees through copyObject() */
	{
		List	   *new_list = (List *) copyObject(raw_parsetree_list);

		/* This checks both copyObject() and the equal() routines... */
		if (!equal(new_list, raw_parsetree_list))
			elog(WARNING, "copyObject() failed to produce an equal raw parse tree");
		else
			raw_parsetree_list = new_list;
	}
#endif

	return raw_parsetree_list;
}

/*
 * Given a raw parsetree (gram.y output), and optionally information about
 * types of parameter symbols ($n), perform parse analysis and rule rewriting.
 *
 * A list of Query nodes is returned, since either the analyzer or the
 * rewriter might expand one query to several.
 *
 * NOTE: for reasons mentioned above, this must be separate from raw parsing.
 */
List *
pg_analyze_and_rewrite(Node *parsetree, const char *query_string,
					   Oid *paramTypes, int numParams)
{
	List	   *querytree_list;

	/*
	 * (1) Perform parse analysis.
	 */
	if (log_parser_stats)
		ResetUsage();

	querytree_list = parse_analyze(parsetree, query_string,
								   paramTypes, numParams);

	if (log_parser_stats)
		ShowUsage("PARSE ANALYSIS STATISTICS");

	/*
	 * (2) Rewrite the queries, as necessary
	 */
	querytree_list = pg_rewrite_queries(querytree_list);

	return querytree_list;
}

/*
 * Perform rewriting of a list of queries produced by parse analysis.
 *
 * Note: queries must just have come from the parser, because we do not do
 * AcquireRewriteLocks() on them.
 */
static List *
pg_rewrite_queries(List *querytree_list)
{
	List	   *new_list = NIL;
	ListCell   *list_item;

	if (log_parser_stats)
		ResetUsage();

	/*
	 * rewritten queries are collected in new_list.  Note there may be more or
	 * fewer than in the original list.
	 */
	foreach(list_item, querytree_list)
	{
		Query	   *querytree = (Query *) lfirst(list_item);

		if (Debug_print_parse)
			elog_node_display(DEBUG1, "parse tree", querytree,
							  Debug_pretty_print);

		if (querytree->commandType == CMD_UTILITY)
		{
			/* don't rewrite utilities, just dump 'em into new_list */
			new_list = lappend(new_list, querytree);
		}
		else
		{
			/* rewrite regular queries */
			List	   *rewritten = QueryRewrite(querytree);

			new_list = list_concat(new_list, rewritten);
		}
	}

	querytree_list = new_list;

	if (log_parser_stats)
		ShowUsage("REWRITER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES

	/*
	 * Optional debugging check: pass querytree output through copyObject()
	 */
	new_list = (List *) copyObject(querytree_list);
	/* This checks both copyObject() and the equal() routines... */
	if (!equal(new_list, querytree_list))
		elog(WARNING, "copyObject() failed to produce an equal parse tree");
	else
		querytree_list = new_list;
#endif

	if (Debug_print_rewritten)
		elog_node_display(DEBUG1, "rewritten parse tree", querytree_list,
						  Debug_pretty_print);

	return querytree_list;
}


/* Generate a plan for a single already-rewritten query. */
PlannedStmt *
pg_plan_query(Query *querytree, ParamListInfo boundParams, QueryResourceLife resource_life)
{
	PlannedStmt	*stmt;

	/* Utility commands have no plans. */
	if (querytree->commandType == CMD_UTILITY)
		return NULL;


	if (log_planner_stats)
		ResetUsage();

	/* call the optimizer */
	stmt = planner(querytree, 0, boundParams, resource_life);

	if (log_planner_stats)
		ShowUsage("PLANNER STATISTICS");

#ifdef COPY_PARSE_PLAN_TREES
	/* Optional debugging check: pass plan output through copyObject() */
	{
		Plan	   *new_plan = (Plan *) copyObject(stmt->planTree);

		/*
		 * equal() currently does not have routines to compare Plan nodes, so
		 * don't try to test equality here.  Perhaps fix someday?
		 */
#ifdef NOT_USED
		/* This checks both copyObject() and the equal() routines... */
		if (!equal(new_plan, stmt->planTree))
			elog(WARNING, "copyObject() failed to produce an equal plan tree");
		else
#endif
			stmt->planTree = new_plan;
	}
#endif

	/*
	 * Print plan if debugging.
	 */
	if (Debug_print_plan)
		elog_node_display(DEBUG1, "plan", stmt->planTree, Debug_pretty_print);
	if (Debug_print_plannedstmt)
		elog_node_display(DEBUG1, "plannedstmt", stmt, Debug_pretty_print);

	return stmt;
}

/*
 * Generate plans for a list of already-rewritten queries.
 *
 * Normal optimizable statements generate PlannedStmt entries in the result
 * list.  Utility statements are simply represented by their statement nodes.
 */
List *
pg_plan_queries(List *querytrees, ParamListInfo boundParams,
				bool needSnapshot, QueryResourceLife resource_life)
{
	List	   *stmt_list = NIL;
	ListCell   *query_list;

	foreach(query_list, querytrees)
	{
		Query	   *query = (Query *) lfirst(query_list);
		Node *stmt;

		/*
		 * If needSnapshot is TRUE, we haven't yet set a snapshot for the current
		 * query.  A snapshot must be set before invoking the planner, since it
		 * might try to evaluate user-defined functions.  But we must not set a
		 * snapshot if the list contains only utility statements, because some
		 * utility statements depend on not having frozen the snapshot yet.
		 * (We assume that such statements cannot appear together with plannable
		 * statements in the rewriter's output.)
		 */
		if (query->commandType == CMD_UTILITY)
		{
			/* Utility commands have no plans. */
			stmt = query->utilityStmt;
		}
		else
		{
			if (needSnapshot)
			{
				ActiveSnapshot = CopySnapshot(GetTransactionSnapshot());
				needSnapshot = false;
			}
			stmt = (Node *) pg_plan_query(query, boundParams, resource_life);
		}

		stmt_list = lappend(stmt_list, stmt);
	}

	return stmt_list;
}

/*
 * exec_mpp_query
 *
 * Called in a qExec process to read and execute a query plan sent by
 * cdbdisp_dispatchPlan().
 *
 * query_string -- optional query text (C string).
 * serializedQuerytree[len]  -- Query node or (NULL,0) if plan provided.
 * serializedPlantree[len] -- PlannedStmt node, or (NULL,0) if query provided.
 * serializedParms[len] -- optional parameters
 * serializedSliceInfo[len] -- optional SliceTable
 * localSlice -- slice table index
 *
 * Caller may supply either a Query (representing utility command) or
 * a PlannedStmt (representing a planned DML command), but not both.
 */
static void
exec_mpp_query(const char *query_string,
			   const char * serializedQuerytree, int serializedQuerytreelen,
			   const char * serializedPlantree, int serializedPlantreelen,
			   const char * serializedParams, int serializedParamslen,
			   const char * serializedSliceInfo, int serializedSliceInfolen,
			   const char * serializedResource, int serializedResourceLen,
			   const char * seqServerHost, int seqServerPort,
			   int localSlice)
{
	CommandDest dest = whereToSendOutput;
	MemoryContext oldcontext;
	bool		save_log_statement_stats = log_statement_stats;
	bool		was_logged = false;
	char		msec_str[32];
	Node		   *utilityStmt = NULL;
	PlannedStmt	   *plan = NULL;
	CmdType		commandType = CMD_UNKNOWN;
	SliceTable *sliceTable = NULL;
    Slice      *slice = NULL;
	ParamListInfo paramLI = NULL;
	QueryResource *resource = NULL;
	MemoryContext currentFilesystemCredentialsMemoryContext = NULL;
	HTAB * currentFilesystemCredentials = NULL;

	Assert(Gp_role == GP_ROLE_EXECUTE);

  if (Debug_print_execution_detail) {
    instr_time  time;
    INSTR_TIME_SET_CURRENT(time);
    elog(DEBUG1,"The time on entering exec_mpp_query: %.3f ms",
                         1000.0 * INSTR_TIME_GET_DOUBLE(time));
  }

	/* 
	 * If we didn't get passed a query string, dummy something up for ps display and pg_stat_activity
	 */
	if (query_string == NULL || strlen(query_string)==0)
		query_string = "mppexec";

	/*
	 * Report query to various monitoring facilities.
	 */

	debug_query_string = query_string;

	pgstat_report_activity(query_string);

	/*
	 * We use save_log_statement_stats so ShowUsage doesn't report incorrect
	 * results because ResetUsage wasn't called.
	 */
	if (save_log_statement_stats)
		ResetUsage();

	/*
	 * Start up a transaction command.	All queries generated by the
	 * query_string will be in this same command block, *unless* we find a
	 * BEGIN/COMMIT/ABORT statement; we have to force a new xact command after
	 * one of those, else bad things will happen in xact.c. (Note that this
	 * will normally change current memory context.)
	 */
	start_xact_command();

	/*
	 * Zap any pre-existing unnamed statement.	(While not strictly necessary,
	 * it seems best to define simple-Query mode as if it used the unnamed
	 * statement and portal; this ensures we recover any storage used by prior
	 * unnamed operations.)
	 */
	unnamed_stmt_pstmt = NULL;
	if (unnamed_stmt_context)
	{
		DropDependentPortals(unnamed_stmt_context);
		MemoryContextDelete(unnamed_stmt_context);
	}
	unnamed_stmt_context = NULL;

	/*
	 * Switch to appropriate context for constructing parsetrees.
	 */
	oldcontext = MemoryContextSwitchTo(MessageContext);

	QueryContext = CurrentMemoryContext;
	
	InitOidInMemHeapMapping(10, MessageContext, INMEM_HEAP_MAPPING);

	/* 
	 * Deserialize the Query node, if there is one.  If this is a planned stmt, then
	 * there isn't one, but there must be a PlannedStmt later on.
	 */
	if (serializedQuerytree != NULL && serializedQuerytreelen > 0)
	{
		Query *query = (Query *) deserializeNode(serializedQuerytree,serializedQuerytreelen);

		if ( !IsA(query, Query) || query->commandType != CMD_UTILITY )
			elog(ERROR, "MPPEXEC: received non-utility Query node.");

		utilityStmt = query->utilityStmt;

		/*
		 * only few Query should be dispatched to QE,
		 * I assume they should be dispatched with query context.
		 */
		/*Assert(NULL != query->contextdisp);*/
		if (query->contextdisp)
		{
			RebuildQueryContext(query->contextdisp, &currentFilesystemCredentials,
			        &currentFilesystemCredentialsMemoryContext);
			FinalizeQueryContextInfo(query->contextdisp);
		}
	}

	/*
	 * Deserialize the slice table, if there is one, and set up the local slice.
	 */
    if (serializedSliceInfo != NULL && serializedSliceInfolen > 0)
	{
        sliceTable = (SliceTable *) deserializeNode(serializedSliceInfo, serializedSliceInfolen);

		sliceTable->localSlice = localSlice;

        if (!sliceTable ||
            !IsA(sliceTable, SliceTable) ||
            sliceTable->localSlice < 0 ||
            sliceTable->localSlice >= list_length(sliceTable->slices))
            elog(ERROR, "MPPEXEC: received invalid slice table:, %d", localSlice);

        slice = (Slice *)list_nth(sliceTable->slices, sliceTable->localSlice);
        Insist(IsA(slice, Slice));

        /* Set global sliceid variable for elog. */
        currentSliceId = sliceTable->localSlice;
    }

 	/*
     * Deserialize the query execution plan (a PlannedStmt node), if there is one.
     */
    if (serializedPlantree != NULL && serializedPlantreelen > 0)
    {
    	plan = (PlannedStmt *) deserializeNode(serializedPlantree,serializedPlantreelen);
		if ( !plan ||
			!IsA(plan, PlannedStmt) ||
			plan->sliceTable != NULL ||
			plan->memoryAccount != NULL)
		{
			elog(ERROR, "MPPEXEC: receive invalid planned statement");
		}

    	/*
		 * Since we're running as a QE, we need to put the slice table 
		 * and associated values determined by the QD that called MPPEXEC 
		 * into the EState before we run the executor.  We can't do it now 
		 * because our EState isn't ready. Instead, put it in PlannedStmt 
		 * and let the code that sets up the QueryDesc sort it out.
		 */
		plan->sliceTable = (Node *) sliceTable; /* Cache for CreateQueryDesc */

		Assert(NULL != plan->contextdisp);
		RebuildQueryContext(plan->contextdisp, &currentFilesystemCredentials,
                &currentFilesystemCredentialsMemoryContext);
		FinalizeQueryContextInfo(plan->contextdisp);
    }

	/*
	 * Deserialize the query resource.
	 */
    if ( (serializedResource != NULL) && (serializedResourceLen > 0) && (GetQEIndex() != -1) )
    {
    	resource = (QueryResource *) deserializeNode(serializedResource, serializedResourceLen);

    	/* Set to planned statement with dispatched resource memory quota.
    	if ( plan != NULL )
    	{
    		plan->query_mem = resource->segment_memory_mb;
    		plan->query_mem *= 1024L * 1024L;
    	}
	*/

    	ShowCGroupEnablementInformation("cpu");

    	if ( isCGroupEnabled("cpu") && isCGroupSetup("cpu") )
    	{
    		int res = FUNC_RETURN_OK;
    		char errorbuf[ERRORMESSAGE_SIZE];

    		if ( !has_been_moved_to_cgroup )
    		{
				res = MoveToCGroupForQE(resource->master_start_time,
										gp_session_id,
										0,
										getpid(),
										errorbuf,
										sizeof(errorbuf));
				if ( res != FUNC_RETURN_OK )
				{
					elog(ERROR, "Resource enforcer fails to move QE to CGroup, "
								"%s",
								errorbuf);
				}
				has_been_moved_to_cgroup = true;
				master_start_time = resource->master_start_time;
    		}

    		int mySegIdx = GetQEIndex();
    		if (IsWriter() && resource->segment_vcore_writer[mySegIdx] == mySegIdx)
    		{
				resource->segment_vcore *= resource->segment_vcore_agg[mySegIdx];
				res = SetWeightCGroupForQE(master_start_time,
										   gp_session_id,
										   0,
										   resource,
										   getpid(),
										   errorbuf,
										   sizeof(errorbuf));
	    		if ( res != FUNC_RETURN_OK )
				{
					elog(ERROR, "Resource enforcer fails to set CGroup weight "
								"for QE, %s",
					            errorbuf);
				}
    		}
    	}
    }

	/*
	 * Choose the command type from either the Query or the PlannedStmt.
	 */
    if ( utilityStmt )
    	commandType = CMD_UTILITY;
    else
	/*
	 * Get (possibly 0) parameters.
	 */
    {
    	if ( !plan )
    		elog(ERROR, "MPPEXEC: received neither Query nor Plan");

    	/* This must be a planned statement. */
	    if (plan->commandType != CMD_SELECT &&
        	plan->commandType != CMD_INSERT &&
        	plan->commandType != CMD_UPDATE &&
        	plan->commandType != CMD_DELETE)
        	elog(ERROR, "MPPEXEC: received non-DML Plan");

        commandType = plan->commandType;
	}
	if ( slice )
	{
		/* Non root slices don't need update privileges. */
		if (sliceTable->localSlice != slice->rootIndex)
		{
			ListCell       *rtcell;
			RangeTblEntry  *rte;
			AclMode         removeperms = ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_SELECT_FOR_UPDATE;

			/* Just reading, so don't check INS/DEL/UPD permissions. */
			foreach(rtcell, plan->rtable)
			{
				rte = (RangeTblEntry *)lfirst(rtcell);
				if (rte->rtekind == RTE_RELATION &&
					0 != (rte->requiredPerms & removeperms))
					rte->requiredPerms &= ~removeperms;
			}
		}
	}


	if (log_statement != LOGSTMT_NONE && !gp_mapreduce_define)
	{
		/*
		 * TODO need to log SELECT INTO as DDL
		 */
		if (log_statement == LOGSTMT_ALL ||
			(utilityStmt && log_statement == LOGSTMT_DDL) ||
			(plan && log_statement >= LOGSTMT_MOD))

		{
			ereport(LOG, (errmsg("statement: %s", query_string)
						   ));
			was_logged = true;
		}

	}

	/*
	 * Get (possibly 0) parameters.
	 */
	paramLI = NULL;
	if (serializedParams != NULL && serializedParamslen > 0)
	{
		ParamListInfoData   paramhdr;
		Size                length;
		const char         *cpos;
		const char         *epos;

		/* Peek at header using an aligned workarea. */
		length = offsetof(ParamListInfoData, params);
		Insist(length <= serializedParamslen);
		memcpy(&paramhdr, serializedParams, length);

		/* Get ParamListInfoData header and ParamExternData array. */
		length += paramhdr.numParams * sizeof(paramhdr.params[0]);
		Insist(paramhdr.numParams > 0 &&
			   length <= serializedParamslen);
		paramLI = palloc(length);
		memcpy(paramLI, serializedParams, length);

		/* Get pass-by-reference data. */
		cpos = serializedParams + length;
		epos = serializedParams + serializedParamslen;
		while (cpos < epos)
		{
			ParamExternData    *pxd;
			int32               iparam;

			/* param index */
			memcpy(&iparam, cpos, sizeof(iparam));
			cpos += sizeof(iparam);
			Insist(cpos <= epos &&
				   iparam >= 0 &&
				   iparam < paramhdr.numParams);

			/* length */
			pxd = &paramLI->params[iparam];
			length = DatumGetInt32(pxd->value);

			/* value */
			Insist((int)length >= 0 &&
				   length <= epos - cpos);
			if (length > 0)
			{
				char   *v = (char *)palloc(length);

				pxd->value = PointerGetDatum(v);
				memcpy(v, cpos, length);
				cpos += length;
			}
		}
		Insist(cpos == epos);
	}


	/*
	 * Switch back to transaction context to enter the loop.
	 */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * All unpacked and checked.  Process the command.
	 */
	{
		const char *commandTag;
		char		completionTag[COMPLETION_TAG_BUFSIZE];

		Portal		portal;
		DestReceiver *receiver;
		int16		format;

		/*
		 * Get the command name for use in status display (it also becomes the
		 * default completion tag, down inside PortalRun).	Set ps_status and
		 * do any special start-of-SQL-command processing needed by the
		 * destination.
		 */
		if (commandType == CMD_UTILITY)
			commandTag = "MPPEXEC UTILITY";
		else if (commandType == CMD_SELECT)
			commandTag = "MPPEXEC SELECT";
		else if (commandType == CMD_INSERT)
			commandTag = "MPPEXEC INSERT";
		else if (commandType == CMD_UPDATE)
			commandTag = "MPPEXEC UPDATE";
		else if (commandType == CMD_DELETE)
			commandTag = "MPPEXEC DELETE";
		else
			commandTag = "MPPEXEC";


		set_ps_display(commandTag, false);

		BeginCommand(commandTag, dest);

        /* Downgrade segworker process priority */
		if (gp_segworker_relative_priority != 0)
		{
			renice_current_process(PostmasterPriority + gp_segworker_relative_priority);
		}

		/*
		 * If we are in an aborted transaction, reject all commands except
		 * COMMIT/ABORT.  It is important that this test occur before we try
		 * to do parse analysis, rewrite, or planning, since all those phases
		 * try to do database accesses, which may fail in abort state. (It
		 * might be safe to allow some additional utility commands in this
		 * state, but not many...)
		 */
		if (IsAbortedTransactionBlockState() /*&&*/
			/*!IsTransactionExitStmt(parsetree)*/)
			ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
					 errmsg("current transaction is aborted, "
							"commands ignored until end of transaction block")));

		/* Make sure we are in a transaction command */
		start_xact_command();

		/* If we got a cancel signal in parsing or prior command, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * OK to analyze, rewrite, and plan this query.
		 *
		 * Switch to appropriate context for constructing querytrees (again,
		 * these must outlive the execution context).
		 */
		oldcontext = MemoryContextSwitchTo(MessageContext);



		/* If we got a cancel signal in analysis or planning, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Create unnamed portal to run the query or queries in. If there
		 * already is one, silently drop it.
		 */
		portal = CreatePortal("", true, true);
		/* Don't display the portal in pg_cursors */
		portal->visible = false;

		/* Copy the plan's query string into the portal */
		Assert(query_string != NULL);
		char *portal_query_string = MemoryContextStrdup(PortalGetHeapMemory(portal),
				query_string);

		PortalDefineQuery(portal,
						  NULL,
						  portal_query_string,
						  T_Query, /* not a parsed statement, so not T_SelectStmt */
						  commandTag,
						  list_make1(plan ? (Node*)plan : (Node*)utilityStmt),
						  MessageContext);

		set_filesystem_credentials(portal, currentFilesystemCredentials,
                currentFilesystemCredentialsMemoryContext);

		/*
		 * Start the portal.  No parameters here.
		 */
		PortalStart(portal, paramLI, InvalidSnapshot,
					seqServerHost, seqServerPort);

		/*
		 * Select text output format, the default.
		 */
		format = 0;
		PortalSetResultFormat(portal, 1, &format);

		/*
		 * Now we can create the destination receiver object.
		 */
		receiver = CreateDestReceiver(dest, portal);

		/*
		 * Switch back to transaction context for execution.
		 */
		MemoryContextSwitchTo(oldcontext);

		/*
		 * Run the portal to completion, and then drop it (and the receiver).
		 */
		(void) PortalRun(portal,
						 FETCH_ALL,
						 true, /* Effectively always top level. */
						 receiver,
						 receiver,
						 completionTag);

		(*receiver->rDestroy) (receiver);


		/*
		 * cleanup all temporary in-memory heap table after each query
		 * PortalDrop will release the resource owner,
		 * should drop all in-memory heap table first.
		 */
		InMemHeap_DropAll(INMEM_HEAP_MAPPING);
		CleanupOidInMemHeapMapping(INMEM_HEAP_MAPPING);

		PortalDrop(portal, false);

		/*
		 * Close down transaction statement before reporting command-complete.  
		 * This is so that any end-of-transaction errors are reported before
		 * the command-complete message is issued, to avoid confusing
		 * clients who will expect either a command-complete message or an
		 * error, not one and then the other.
		 */
		finish_xact_command();

		/*
		 * Tell client that we're done with this query.  Note we emit exactly
		 * one EndCommand report for each raw parsetree, thus one for each SQL
		 * command the client sent, regardless of rewriting. (But a command
		 * aborted by error will not send an EndCommand report at all.)
		 */
		EndCommand(completionTag, dest);

	}							/* end loop over parsetrees */

	/*
	 * Close down transaction statement, if one is open.
	 */
	finish_xact_command();

	QueryContext = NULL;

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, was_logged))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errOmitLocation(true),
					 errhidestmt(true)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  statement: %s",
							msec_str, query_string),
					 errhidestmt(true),
					 errOmitLocation(true)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("QUERY STATISTICS");

	debug_query_string = NULL;
}

/*
 * exec_simple_query
 *
 * Execute a "simple Query" protocol message.
 */
static void
exec_simple_query(const char *query_string, const char *seqServerHost, int seqServerPort)
{
	CommandDest dest = whereToSendOutput;
	MemoryContext oldcontext;
	List	   *parsetree_list;
	ListCell   *parsetree_item;
	bool		save_log_statement_stats = log_statement_stats;
	bool		was_logged = false;
	bool		isTopLevel = false;
	char		msec_str[32];

	if (Gp_role != GP_ROLE_EXECUTE)
	{
		increment_command_count();

		MyProc->queryCommandId = gp_command_count;
		if (gp_cancel_query_print_log)
		{
			elog(NOTICE, "running query (sessionId, commandId): (%d, %d)",
				 MyProc->mppSessionId, gp_command_count);
		}
	}

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = query_string;

	pgstat_report_activity(query_string);

	/*
	 * We use save_log_statement_stats so ShowUsage doesn't report incorrect
	 * results because ResetUsage wasn't called.
	 */
	if (save_log_statement_stats)
		ResetUsage();

	/*
	 * Start up a transaction command.	All queries generated by the
	 * query_string will be in this same command block, *unless* we find a
	 * BEGIN/COMMIT/ABORT statement; we have to force a new xact command after
	 * one of those, else bad things will happen in xact.c. (Note that this
	 * will normally change current memory context.)
	 */
	start_xact_command();

	/*
	 * Zap any pre-existing unnamed statement.	(While not strictly necessary,
	 * it seems best to define simple-Query mode as if it used the unnamed
	 * statement and portal; this ensures we recover any storage used by prior
	 * unnamed operations.)
	 */
	unnamed_stmt_pstmt = NULL;
	if (unnamed_stmt_context)
	{
		DropDependentPortals(unnamed_stmt_context);
		MemoryContextDelete(unnamed_stmt_context);
	}
	unnamed_stmt_context = NULL;

	/*
	 * Switch to appropriate context for constructing parsetrees.
	 */
	oldcontext = MemoryContextSwitchTo(MessageContext);

	QueryContext = CurrentMemoryContext;

	/*
	 * Do basic parsing of the query or queries (this should be safe even if
	 * we are in aborted transaction state!)
	 */
	parsetree_list = pg_parse_query(query_string);

	/* Disable statement logging during mapreduce */
	if (!gp_mapreduce_define)
	{
		/* Log immediately if dictated by log_statement */
		if (check_log_statement(parsetree_list))
		{
			ereport(LOG,
				(errmsg("statement: %s", query_string),
				 errhidestmt(true),
				 errOmitLocation(true),
				 errdetail_execute(parsetree_list)));
			was_logged = true;
		}
	}

	/*
	 * Switch back to transaction context to enter the loop.
	 */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * We'll tell PortalRun it's a top-level command iff there's exactly one
	 * raw parsetree.  If more than one, it's effectively a transaction block
	 * and we want PreventTransactionChain to reject unsafe commands. (Note:
	 * we're assuming that query rewrite cannot add commands that are
	 * significant to PreventTransactionChain.)
	 */
	isTopLevel = (list_length(parsetree_list) == 1);

	/*
	 * Run through the raw parsetree(s) and process each one.
	 */
	foreach(parsetree_item, parsetree_list)
	{
		Node	   *parsetree = (Node *) lfirst(parsetree_item);
		Snapshot        mySnapshot = NULL;
		const char *commandTag;
		char		completionTag[COMPLETION_TAG_BUFSIZE];
		List	   *querytree_list,
				   *plantree_list;
		Portal		portal;
		DestReceiver *receiver;
		int16		format;

		/*
		 * Get the command name for use in status display (it also becomes the
		 * default completion tag, down inside PortalRun).	Set ps_status and
		 * do any special start-of-SQL-command processing needed by the
		 * destination.
		 */
		commandTag = CreateCommandTag(parsetree);

		set_ps_display(commandTag, false);

		BeginCommand(commandTag, dest);

		/*
		 * If are connected in utility mode, disallow PREPARE TRANSACTION statements.
		 */
		TransactionStmt *transStmt = (TransactionStmt *)parsetree;
		if (Gp_role == GP_ROLE_UTILITY && IsA(parsetree, TransactionStmt) && transStmt->kind == TRANS_STMT_PREPARE)
		{
		  ereport( ERROR
			   , (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			    errmsg("PREPARE TRANSACTION is not supported in utility mode"))
			 );
		}

		/*
		 * If we are in an aborted transaction, reject all commands except
		 * COMMIT/ABORT.  It is important that this test occur before we try
		 * to do parse analysis, rewrite, or planning, since all those phases
		 * try to do database accesses, which may fail in abort state. (It
		 * might be safe to allow some additional utility commands in this
		 * state, but not many...)
		 */
		if (IsAbortedTransactionBlockState() &&
			!IsTransactionExitStmt(parsetree))
			ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
					 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block"),
					 errOmitLocation(true)));

		/* Make sure we are in a transaction command */
		start_xact_command();

		/* If we got a cancel signal in parsing or prior command, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Set up a snapshot if parse analysis/planning will need one.
		 */
		if (analyze_requires_snapshot(parsetree))
		{
			mySnapshot = CopySnapshot(GetTransactionSnapshot());
			ActiveSnapshot = mySnapshot;
		}

		/*
		 * OK to analyze, rewrite, and plan this query.
		 *
		 * Switch to appropriate context for constructing querytrees (again,
		 * these must outlive the execution context).
		 */
		oldcontext = MemoryContextSwitchTo(MessageContext);

		InitOidInMemHeapMapping(10, MessageContext, INMEM_HEAP_MAPPING);

		querytree_list = pg_analyze_and_rewrite(parsetree, query_string,
												NULL, 0);

		plantree_list = pg_plan_queries(querytree_list, NULL, true, QRL_ONCE);

		/* Done with the snapshot used for parsing/planning */
		ActiveSnapshot = NULL;
		if (mySnapshot)
			FreeSnapshot(mySnapshot);

		/* If we got a cancel signal in analysis or planning, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Create unnamed portal to run the query or queries in. If there
		 * already is one, silently drop it.
		 */
		portal = CreatePortal("", true, true);
		/* Don't display the portal in pg_cursors */
		portal->visible = false;

		/* Copy the plan's query string into the portal */
		Assert(query_string != NULL);
		char *portal_query_string = MemoryContextStrdup(PortalGetHeapMemory(portal),
				query_string);

		PortalDefineQuery(portal,
						  NULL,
						  portal_query_string,
						  nodeTag(parsetree),
						  commandTag,
						  plantree_list,
						  MessageContext);

		create_filesystem_credentials(portal);

		/*
		 * Start the portal.  No parameters here.
		 */
		PortalStart(portal, NULL, InvalidSnapshot,
					seqServerHost, seqServerPort);

		/*
		 * Select the appropriate output format: text unless we are doing a
		 * FETCH from a binary cursor.	(Pretty grotty to have to do this here
		 * --- but it avoids grottiness in other places.  Ah, the joys of
		 * backward compatibility...)
		 */
		format = 0;				/* TEXT is default */
		if (IsA(parsetree, FetchStmt))
		{
			FetchStmt  *stmt = (FetchStmt *) parsetree;

			if (!stmt->ismove)
			{
				Portal		fportal = GetPortalByName(stmt->portalname);

				if (PortalIsValid(fportal) &&
					(fportal->cursorOptions & CURSOR_OPT_BINARY))
					format = 1; /* BINARY */
			}
		}
		PortalSetResultFormat(portal, 1, &format);

		/*
		 * Now we can create the destination receiver object.
		 */
		receiver = CreateDestReceiver(dest, portal);

		/*
		 * Switch back to transaction context for execution.
		 */
		MemoryContextSwitchTo(oldcontext);

		/*
		 * Run the portal to completion, and then drop it (and the receiver).
		 */
		(void) PortalRun(portal,
						 FETCH_ALL,
						 isTopLevel,
						 receiver,
						 receiver,
						 completionTag);

		(*receiver->rDestroy) (receiver);

		/*
		 * cleanup all temporary in-memory heap table after each query
		 * PortalDrop will release the resource owner,
		 * should drop all in-memory heap table first.
		 */
		InMemHeap_DropAll(INMEM_HEAP_MAPPING);
		CleanupOidInMemHeapMapping(INMEM_HEAP_MAPPING);

		PortalDrop(portal, false);

		if (IsA(parsetree, TransactionStmt))
		{
			/*
			 * If this was a transaction control statement, commit it. We will
			 * start a new xact command for the next command (if any).
			 */
			finish_xact_command();
		}
		else if (lnext(parsetree_item) == NULL)
		{
			/*
			 * If this is the last parsetree of the query string, close down
			 * transaction statement before reporting command-complete.  This
			 * is so that any end-of-transaction errors are reported before
			 * the command-complete message is issued, to avoid confusing
			 * clients who will expect either a command-complete message or an
			 * error, not one and then the other.  But for compatibility with
			 * historical Postgres behavior, we do not force a transaction
			 * boundary between queries appearing in a single query string.
			 */
			finish_xact_command();
		}
		else
		{
			/*
			 * We need a CommandCounterIncrement after every query, except
			 * those that start or end a transaction block.
			 */
			CommandCounterIncrement();
		}

		/*
		 * Tell client that we're done with this query.  Note we emit exactly
		 * one EndCommand report for each raw parsetree, thus one for each SQL
		 * command the client sent, regardless of rewriting. (But a command
		 * aborted by error will not send an EndCommand report at all.)
		 */
		EndCommand(completionTag, dest);
	}							/* end loop over parsetrees */

	/*
	 * Close down transaction statement, if one is open.
	 */
	finish_xact_command();

	/*
	 * If there were no parsetrees, return EmptyQueryResponse message.
	 */
	if (!parsetree_list)
		NullCommand(dest);

	QueryContext = NULL;

	/*
	 * Emit duration logging if appropriate.
	 */
	if (!gp_mapreduce_define)
	{
		switch (check_log_duration(msec_str, was_logged))
		{
			case 1:
				ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errOmitLocation(true),
					 errhidestmt(true)));
				break;
			case 2:
				ereport(LOG, (errmsg("duration: %s ms  statement: %s",
									 msec_str, query_string),
							  errdetail_execute(parsetree_list),
							  errOmitLocation(true),
							  errhidestmt(true)));
				break;
		}
	}

	if (save_log_statement_stats)
		ShowUsage("QUERY STATISTICS");

	debug_query_string = NULL;
}

/*
 * exec_parse_message
 *
 * Execute a "Parse" protocol message.
 */
static void
exec_parse_message(const char *query_string,	/* string to execute */
				   const char *stmt_name,		/* name for prepared stmt */
				   Oid *paramTypes,		/* parameter types */
				   int numParams)		/* number of parameters */
{
	MemoryContext oldcontext;
	List	   *parsetree_list;
	const char *commandTag;
	List		*querytree_list = NIL;
	List		*param_list = NIL;
	bool		is_named;
	bool		save_log_statement_stats = log_statement_stats;
	char		msec_str[32];
	NodeTag		sourceTag = T_Query;

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = query_string;

	pgstat_report_activity(query_string);

	set_ps_display("PARSE", false);

	if (save_log_statement_stats)
		ResetUsage();

	ereport(DEBUG2,
			(errmsg("parse %s: %s",
					*stmt_name ? stmt_name : "<unnamed>",
					query_string)));

	/*
	 * Start up a transaction command so we can run parse analysis etc. (Note
	 * that this will normally change current memory context.) Nothing happens
	 * if we are already in one.
	 */
	start_xact_command();

	/*
	 * Switch to appropriate context for constructing parsetrees.
	 *
	 * We have two strategies depending on whether the prepared statement is
	 * named or not.  For a named prepared statement, we do parsing in
	 * MessageContext and copy the finished trees into the prepared
	 * statement's private context; then the reset of MessageContext releases
	 * temporary space used by parsing and planning.  For an unnamed prepared
	 * statement, we assume the statement isn't going to hang around long, so
	 * getting rid of temp space quickly is probably not worth the costs of
	 * copying parse/plan trees.  So in this case, we set up a special context
	 * for the unnamed statement, and do all the parsing work therein.
	 */
	is_named = (stmt_name[0] != '\0');
	if (is_named)
	{
		/* Named prepared statement --- parse in MessageContext */
		oldcontext = MemoryContextSwitchTo(MessageContext);
	}
	else
	{
		/* Unnamed prepared statement --- release any prior unnamed stmt */
		unnamed_stmt_pstmt = NULL;
		if (unnamed_stmt_context)
		{
			DropDependentPortals(unnamed_stmt_context);
			MemoryContextDelete(unnamed_stmt_context);
		}
		unnamed_stmt_context = NULL;
		/* create context for parsing/planning */
		unnamed_stmt_context =
			AllocSetContextCreate(TopMemoryContext,
								  "unnamed prepared statement",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);
		oldcontext = MemoryContextSwitchTo(unnamed_stmt_context);
	}

	QueryContext = CurrentMemoryContext;

	/*
	 * Do basic parsing of the query or queries (this should be safe even if
	 * we are in aborted transaction state!)
	 */
	parsetree_list = pg_parse_query(query_string);
	/*
	 * We only allow a single user statement in a prepared statement. This is
	 * mainly to keep the protocol simple --- otherwise we'd need to worry
	 * about multiple result tupdescs and things like that.
	 */
	if (list_length(parsetree_list) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
		errmsg("cannot insert multiple commands into a prepared statement")));

	if (parsetree_list != NIL)
	{
		Node	   *parsetree = (Node *) linitial(parsetree_list);
		Snapshot        mySnapshot = NULL;
		int			i;

		/*
		 * Get the command name for possible use in status display.
		 */
		commandTag = CreateCommandTag(parsetree);

		/*
		 * If we are in an aborted transaction, reject all commands except
		 * COMMIT/ROLLBACK.  It is important that this test occur before we
		 * try to do parse analysis, rewrite, or planning, since all those
		 * phases try to do database accesses, which may fail in abort state.
		 * (It might be safe to allow some additional utility commands in this
		 * state, but not many...)
		 */
		if (IsAbortedTransactionBlockState() &&
			!IsTransactionExitStmt(parsetree))
			ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
					 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block")));

        /*
         * Set up a snapshot if parse analysis/planning will need one.
         */
		if (analyze_requires_snapshot(parsetree))
		{
			mySnapshot = CopySnapshot(GetTransactionSnapshot());
			ActiveSnapshot = mySnapshot;
		}

		/*
		 * OK to analyze, rewrite, and plan this query.  Note that the
		 * originally specified parameter set is not required to be complete,
		 * so we have to use parse_analyze_varparams().
		 */
		if (log_parser_stats)
			ResetUsage();

		querytree_list = parse_analyze_varparams(parsetree,
												 query_string,
												 &paramTypes,
												 &numParams);

		/*
		 * Check all parameter types got determined, and convert array
		 * representation to a list for storage.
		 */
		param_list = NIL;
		for (i = 0; i < numParams; i++)
		{
			Oid			ptype = paramTypes[i];

			if (ptype == InvalidOid || ptype == UNKNOWNOID)
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_DATATYPE),
					 errmsg("could not determine data type of parameter $%d",
							i + 1)));
			param_list = lappend_oid(param_list, ptype);
		}

		if (log_parser_stats)
			ShowUsage("PARSE ANALYSIS STATISTICS");

		querytree_list = pg_rewrite_queries(querytree_list);

		if (parsetree_list)
		{
			Node	   *parsetree = (Node *) linitial(parsetree_list);
			sourceTag = nodeTag(parsetree);
		}

		/**
		 * Even though we do not store the plan, we want to perform planning
		 * to know if SQL syntax is supported.
		 */
		if (sourceTag == T_SelectStmt)
		{
			List *ql = (List *) copyObject(querytree_list);
			List *pstmt = pg_plan_queries(ql, NULL, false, QRL_NONE);
			list_free(ql);
			list_free(pstmt);
		}

		/* Done with the snapshot used for parsing/planning */
		ActiveSnapshot = NULL;
		if (mySnapshot)
			FreeSnapshot(mySnapshot);

	}
	else
	{
		/* Empty input string.	This is legal. */
		commandTag = NULL;
		querytree_list = NIL;
		param_list = NIL;
	}

	/* If we got a cancel signal in analysis or planning, quit */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Store the query as a prepared statement.  See above comments.
	 */
	if (is_named)
	{

		StorePreparedStatement(stmt_name,
							   query_string,
							   sourceTag,
							   commandTag,
							   querytree_list,
							   param_list,
							   false);
	}
	else
	{
		PreparedStatement	*pstmt;

		pstmt = (PreparedStatement *) palloc0(sizeof(PreparedStatement));
		/* query_string needs to be copied into unnamed_stmt_context */
		pstmt->query_string = pstrdup(query_string);
		/* the rest is there already */
		pstmt->commandTag = commandTag;
		pstmt->sourceTag = sourceTag;
		pstmt->query_list = querytree_list;
		pstmt->argtype_list = param_list;
		pstmt->from_sql = false;
		pstmt->context = unnamed_stmt_context;
		/* XXX prepare_time and from_sql default to 0! Correct? */
		/* Now the unnamed statement is complete and valid */
		unnamed_stmt_pstmt = pstmt;
	}

	MemoryContextSwitchTo(oldcontext);

	QueryContext = NULL;

	/*
	 * We do NOT close the open transaction command here; that only happens
	 * when the client sends Sync.	Instead, do CommandCounterIncrement just
	 * in case something happened during parse/plan.
	 */
	CommandCounterIncrement();

	/*
	 * Send ParseComplete.
	 */
	if (whereToSendOutput == DestRemote)
		pq_putemptymessage('1');

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, false))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errOmitLocation(true),
					 errhidestmt(true)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  parse %s: %s",
							msec_str,
							*stmt_name ? stmt_name : "<unnamed>",
							query_string),
							errOmitLocation(true),
							errhidestmt(true)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("PARSE MESSAGE STATISTICS");

	debug_query_string = NULL;
}

/*
 * exec_bind_message
 *
 * Process a "Bind" message to create a portal from a prepared statement
 */
static void
exec_bind_message(StringInfo input_message)
{
	const char *portal_name;
	const char *stmt_name;
	int			numPFormats;
	int16	   *pformats = NULL;
	int			numParams;
	int			numRFormats;
	int16	   *rformats = NULL;
	PreparedStatement *pstmt;
	Portal		portal;
	ParamListInfo params;
	List	   *stmt_list;
	MemoryContext qContext = NULL;
	bool		save_log_statement_stats = log_statement_stats;
	char		msec_str[32];


	/* Get the fixed part of the message */
	portal_name = pq_getmsgstring(input_message);
	stmt_name = pq_getmsgstring(input_message);

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "Bind: portal %s stmt_name %s", portal_name, stmt_name);

	ereport(DEBUG2,
			(errmsg("bind %s to %s",
					*portal_name ? portal_name : "<unnamed>",
					*stmt_name ? stmt_name : "<unnamed>")));

	/* Find prepared statement */
	if (stmt_name[0] != '\0')
		pstmt = FetchPreparedStatement(stmt_name, true);
	else
	{
		/* special-case the unnamed statement */
		pstmt = unnamed_stmt_pstmt;
		if (!pstmt)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_PSTATEMENT),
					 errmsg("unnamed prepared statement does not exist")));
	}

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = pstmt->query_string ? pstmt->query_string : "<BIND>";

	pgstat_report_activity(debug_query_string);

	set_ps_display("BIND", false);

	if (save_log_statement_stats)
		ResetUsage();

	/*
	 * Start up a transaction command so we can call functions etc. (Note that
	 * this will normally change current memory context.) Nothing happens if
	 * we are already in one.
	 */
	start_xact_command();
	
	/* Switch back to message context */
	MemoryContextSwitchTo(MessageContext);

	/* Get the parameter format codes */
	numPFormats = pq_getmsgint(input_message, 2);
	if (numPFormats > 0)
	{
		int			i;

		pformats = (int16 *) palloc(numPFormats * sizeof(int16));
		for (i = 0; i < numPFormats; i++)
			pformats[i] = pq_getmsgint(input_message, 2);
	}

	/* Get the parameter value count */
	numParams = pq_getmsgint(input_message, 2);

	if (numPFormats > 1 && numPFormats != numParams)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
			errmsg("bind message has %d parameter formats but %d parameters",
				   numPFormats, numParams)));

	if (numParams != list_length(pstmt->argtype_list))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("bind message supplies %d parameters, but prepared statement \"%s\" requires %d",
				   numParams, stmt_name, list_length(pstmt->argtype_list))));

	/*
	 * If we are in aborted transaction state, the only portals we can
	 * actually run are those containing COMMIT or ROLLBACK commands. We
	 * disallow binding anything else to avoid problems with infrastructure
	 * that expects to run inside a valid transaction.	We also disallow
	 * binding any parameters, since we can't risk calling user-defined I/O
	 * functions.
	 */
	if (IsAbortedTransactionBlockState() &&
		(!IsTransactionExitStmtList(pstmt->query_list) ||
		 numParams != 0))
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block")));

	/*
	 * Create the portal.  Allow silent replacement of an existing portal only
	 * if the unnamed portal is specified.
	 */
	if (portal_name[0] == '\0')
		portal = CreatePortal(portal_name, true, true);
	else
		portal = CreatePortal(portal_name, false, false);

	portal->is_extended_query = true;

	/*
	 * Fetch parameters, if any, and store in the portal's memory context.
	 */
	if (numParams > 0)
	{
		Snapshot        mySnapshot;
		ListCell   *l;
		MemoryContext oldContext;
		int			paramno;

		 /*
		  * Set a snapshot if we have parameters to fetch (since the input
		  * functions might need it).
		  */
		mySnapshot = CopySnapshot(GetTransactionSnapshot());
		ActiveSnapshot = mySnapshot;

		oldContext = MemoryContextSwitchTo(PortalGetHeapMemory(portal));

		/* sizeof(ParamListInfoData) includes the first array element */
		params = (ParamListInfo) palloc(sizeof(ParamListInfoData) +
								   (numParams - 1) *sizeof(ParamExternData));
		params->numParams = numParams;

		paramno = 0;
		foreach(l, pstmt->argtype_list)
		{
			Oid			ptype = lfirst_oid(l);
			int32		plength;
			Datum		pval;
			bool		isNull;
			StringInfoData pbuf;
			char		csave;
			int16		pformat;

			plength = pq_getmsgint(input_message, 4);
			isNull = (plength == -1);

			if (!isNull)
			{
				const char *pvalue = pq_getmsgbytes(input_message, plength);

				/*
				 * Rather than copying data around, we just set up a phony
				 * StringInfo pointing to the correct portion of the message
				 * buffer.	We assume we can scribble on the message buffer so
				 * as to maintain the convention that StringInfos have a
				 * trailing null.  This is grotty but is a big win when
				 * dealing with very large parameter strings.
				 */
				pbuf.data = (char *) pvalue;
				pbuf.maxlen = plength + 1;
				pbuf.len = plength;
				pbuf.cursor = 0;

				csave = pbuf.data[plength];
				pbuf.data[plength] = '\0';
			}
			else
			{
				pbuf.data = NULL;		/* keep compiler quiet */
				csave = 0;
			}

			if (numPFormats > 1)
				pformat = pformats[paramno];
			else if (numPFormats > 0)
				pformat = pformats[0];
			else
				pformat = 0;	/* default = text */

			if (pformat == 0)	/* text mode */
			{
				Oid			typinput;
				Oid			typioparam;
				char	   *pstring;

				getTypeInputInfo(ptype, &typinput, &typioparam);

				/*
				 * We have to do encoding conversion before calling the
				 * typinput routine.
				 */
				if (isNull)
					pstring = NULL;
				else
					pstring = pg_client_to_server(pbuf.data, plength);

				pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);

				/* Free result of encoding conversion, if any */
				if (pstring && pstring != pbuf.data)
					pfree(pstring);
			}
			else if (pformat == 1)		/* binary mode */
			{
				Oid			typreceive;
				Oid			typioparam;
				StringInfo	bufptr;

				/*
				 * Call the parameter type's binary input converter
				 */
				getTypeBinaryInputInfo(ptype, &typreceive, &typioparam);

				if (isNull)
					bufptr = NULL;
				else
					bufptr = &pbuf;

				pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);

				/* Trouble if it didn't eat the whole buffer */
				if (!isNull && pbuf.cursor != pbuf.len)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
							 errmsg("incorrect binary data format in bind parameter %d",
									paramno + 1)));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unsupported format code: %d",
								pformat)));
				pval = 0;		/* keep compiler quiet */
			}

			/* Restore message buffer contents */
			if (!isNull)
				pbuf.data[plength] = csave;

			params->params[paramno].value = pval;
			params->params[paramno].isnull = isNull;

			/*
			 * We mark the params as CONST.  This has no effect if we already
			 * did planning, but if we didn't, it licenses the planner to
			 * substitute the parameters directly into the one-shot plan we
			 * will generate below.
			 */
			params->params[paramno].pflags = PARAM_FLAG_CONST;
			params->params[paramno].ptype = ptype;

			paramno++;
		}

		MemoryContextSwitchTo(oldContext);

		/* Done with the snapshot used for parameter I/O */
		ActiveSnapshot = NULL;
		FreeSnapshot(mySnapshot);
	}
	else
		params = NULL;

	/* Get the result format codes */
	numRFormats = pq_getmsgint(input_message, 2);
	if (numRFormats > 0)
	{
		int			i;

		rformats = (int16 *) palloc(numRFormats * sizeof(int16));
		for (i = 0; i < numRFormats; i++)
			rformats[i] = pq_getmsgint(input_message, 2);
	}

	pq_getmsgend(input_message);

	/*
	 * If we didn't plan the query before, do it now.  This allows the planner
	 * to make use of the concrete parameter values we now have.  Because we
	 * use PARAM_FLAG_CONST, the plan is good only for this set of param
	 * values, and so we generate the plan in the portal's own memory context
	 * where it will be thrown away after use.	As in exec_parse_message, we
	 * make no attempt to recover planner temporary memory until the end of
	 * the operation.
	 *
	 * XXX because the planner has a bad habit of scribbling on its input, we
	 * have to make a copy of the parse trees, just in case someone binds and
	 * executes an unnamed statement multiple times; this also means that the
	 * portal's queryContext becomes its own heap context rather than the
	 * prepared statement's context.  FIXME someday
	 */

	{
		List	   *query_list_copy = NIL;
		Assert(portal);
		MemoryContext oldContext = NULL;
		qContext = PortalGetHeapMemory(portal);
		oldContext = MemoryContextSwitchTo(qContext);
		query_list_copy = copyObject(pstmt->query_list); /* planner scribbles on query tree */
		stmt_list = pg_plan_queries(query_list_copy, params, true, QRL_ONCE);
		MemoryContextSwitchTo(oldContext);
	}

	Assert(qContext);

	/* Copy the plan's query string into the portal */
	Assert(debug_query_string != NULL);
	char *query_string = MemoryContextStrdup(PortalGetHeapMemory(portal),
						 debug_query_string);

	/*
	 * Define portal and start execution.
	 */
	PortalDefineQuery(portal,
					  *pstmt->stmt_name ? pstmt->stmt_name : NULL,
					  query_string,
					  pstmt->sourceTag,
					  pstmt->commandTag,
					  stmt_list,
					  qContext);

	create_filesystem_credentials(portal);

	PortalStart(portal, params, InvalidSnapshot,
				savedSeqServerHost, savedSeqServerPort);

	/*
	 * Apply the result format requests to the portal.
	 */
	PortalSetResultFormat(portal, numRFormats, rformats);

	/*
	 * Send BindComplete.
	 */
	if (whereToSendOutput == DestRemote)
		pq_putemptymessage('2');

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, false))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errOmitLocation(true),
					 errhidestmt(true)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  bind %s%s%s: %s",
							msec_str,
							*stmt_name ? stmt_name : "<unnamed>",
							*portal_name ? "/" : "",
							*portal_name ? portal_name : "",
							pstmt->query_string ? pstmt->query_string : "<source not stored>"),
					 errdetail_params(params),
					 errOmitLocation(true),
					 errhidestmt(true)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("BIND MESSAGE STATISTICS");

	debug_query_string = NULL;
}

/*
 * exec_execute_message
 *
 * Process an "Execute" message for a portal
 */
static void
exec_execute_message(const char *portal_name, int64 max_rows)
{
	CommandDest dest;
	DestReceiver *receiver;
	Portal		portal;
	bool		completed;
	char		completionTag[COMPLETION_TAG_BUFSIZE];
	const char *sourceText = NULL;
	const char *prepStmtName;
	ParamListInfo portalParams;
	bool		save_log_statement_stats = log_statement_stats;
	bool		is_xact_command;
	bool		execute_is_fetch = false;
	bool		was_logged = false;
	char		msec_str[32];

	/* Adjust destination to tell printtup.c what to do */
	dest = whereToSendOutput;
	if (dest == DestRemote)
		dest = DestRemoteExecute;

	portal = GetPortalByName(portal_name);
	if (!PortalIsValid(portal))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("portal \"%s\" does not exist", portal_name)));

	/*
	 * If the original query was a null string, just return
	 * EmptyQueryResponse.
	 */
	if (portal->commandTag == NULL)
	{
		Assert(portal->stmts == NIL);
		NullCommand(dest);
		return;
	}

	if (Gp_role != GP_ROLE_EXECUTE)
	{

		/*
		 * MPP-20924
		 * Increment command_count only if we're executing utility statements
		 * In all other cases, we already incremented it in CreateQueryDescr
		 */
		bool is_utility_stmt = true;
		ListCell   *stmtlist_item = NULL;
		foreach(stmtlist_item, portal->stmts)
		{
			Node *stmt = lfirst(stmtlist_item);
			if (IsA(stmt, PlannedStmt))
			{
				is_utility_stmt = false;
				break;
			}
		}
		if (is_utility_stmt)
		{
			increment_command_count();

			MyProc->queryCommandId = gp_command_count;
			if (gp_cancel_query_print_log)
			{
				elog(NOTICE, "running query (sessionId, commandId): (%d, %d)",
						MyProc->mppSessionId, gp_command_count);
				elog(LOG, "In exec_execute_message found utility statement, incrementing command_count");
			}
		}
		else
		{
			if (gp_cancel_query_print_log)
			{
				elog(LOG, "In exec_execute_message found non-utility statement, NOT incrementing command count");
			}
		}
	}

	/* Does the portal contain a transaction command? */
	is_xact_command = IsTransactionStmtList(portal->stmts);

	/*
	 * We must copy the sourceText and prepStmtName into MessageContext in
	 * case the portal is destroyed during finish_xact_command. Can avoid the
	 * copy if it's not an xact command, though.
	 */
	if (is_xact_command)
	{
		sourceText = portal->sourceText ? pstrdup(portal->sourceText) : NULL;
		if (portal->prepStmtName)
			prepStmtName = pstrdup(portal->prepStmtName);
		else
			prepStmtName = "<unnamed>";

		/*
		 * An xact command shouldn't have any parameters, which is a good
		 * thing because they wouldn't be around after finish_xact_command.
		 */
		portalParams = NULL;
	}
	else
	{
		sourceText = portal->sourceText;
		if (portal->prepStmtName)
			prepStmtName = portal->prepStmtName;
		else
			prepStmtName = "<unnamed>";
		portalParams = portal->portalParams;
	}

	/*
	 * Report query to various monitoring facilities.
	 */
	debug_query_string = sourceText ? sourceText : "<EXECUTE>";

	pgstat_report_activity(debug_query_string);

	set_ps_display(portal->commandTag, false);

	if (save_log_statement_stats)
		ResetUsage();

	BeginCommand(portal->commandTag, dest);

	/*
	 * Create dest receiver in MessageContext (we don't want it in transaction
	 * context, because that may get deleted if portal contains VACUUM).
	 */
	receiver = CreateDestReceiver(dest, portal);

	/*
	 * Ensure we are in a transaction command (this should normally be the
	 * case already due to prior BIND).
	 */
	start_xact_command();

	/*
	 * If we re-issue an Execute protocol request against an existing portal,
	 * then we are only fetching more rows rather than completely re-executing
	 * the query from the start. atStart is never reset for a v3 portal, so we
	 * are safe to use this check.
	 */
	execute_is_fetch = !portal->atStart;

	/* Log immediately if dictated by log_statement */
	if (check_log_statement(portal->stmts))
	{
		ereport(LOG,
				(errmsg("%s %s%s%s%s%s",
						execute_is_fetch ?
						_("execute fetch from") :
						_("execute"),
						prepStmtName,
						*portal_name ? "/" : "",
						*portal_name ? portal_name : "",
						sourceText ? ": " : "",
						sourceText ? sourceText : ""),
				 errdetail_params(portalParams),
				 errOmitLocation(true),
				 errhidestmt(true)));
		was_logged = true;
	}

	/*
	 * If we are in aborted transaction state, the only portals we can
	 * actually run are those containing COMMIT or ROLLBACK commands.
	 */
	if (IsAbortedTransactionBlockState() &&
		!IsTransactionExitStmtList(portal->stmts))
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block")));

	/* Check for cancel signal before we start execution */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Okay to run the portal.
	 */
	if (max_rows <= 0)
		max_rows = FETCH_ALL;

	completed = PortalRun(portal,
						  max_rows,
						  true, /* always top level */
						  receiver,
						  receiver,
						  completionTag);

	(*receiver->rDestroy) (receiver);

	if (completed)
	{
		if (is_xact_command)
		{
			/*
			 * If this was a transaction control statement, commit it.	We
			 * will start a new xact command for the next command (if any).
			 */
			finish_xact_command();
		}
		else
		{
			/*
			 * We need a CommandCounterIncrement after every query, except
			 * those that start or end a transaction block.
			 */
			CommandCounterIncrement();
		}

		/* Send appropriate CommandComplete to client */
		EndCommand(completionTag, dest);
	}
	else
	{
		/* Portal run not complete, so send PortalSuspended */
		if (whereToSendOutput == DestRemote)
			pq_putemptymessage('s');
	}

	/*
	 * Emit duration logging if appropriate.
	 */
	switch (check_log_duration(msec_str, was_logged))
	{
		case 1:
			ereport(LOG,
					(errmsg("duration: %s ms", msec_str),
					 errOmitLocation(true),
					 errhidestmt(true)));
			break;
		case 2:
			ereport(LOG,
					(errmsg("duration: %s ms  %s %s%s%s%s%s",
							msec_str,
							execute_is_fetch ?
							_("execute fetch from") :
							_("execute"),
							prepStmtName,
							*portal_name ? "/" : "",
							*portal_name ? portal_name : "",
							sourceText ? ": " : "",
							sourceText ? sourceText : ""),
					 errdetail_params(portalParams),
					 errOmitLocation(true),
					 errhidestmt(true)));
			break;
	}

	if (save_log_statement_stats)
		ShowUsage("EXECUTE MESSAGE STATISTICS");

	debug_query_string = NULL;
}

/*
 * check_log_statement
 *		Determine whether command should be logged because of log_statement
 *
 * parsetree_list can be either raw grammar output or a list of planned
 * statements
 */
static bool
check_log_statement(List *stmt_list)
{
	ListCell   *stmt_item;

	if (log_statement == LOGSTMT_NONE)
		return false;
	if (log_statement == LOGSTMT_ALL)
		return true;

	/* Else we have to inspect the statement(s) to see whether to log */
	foreach(stmt_item, stmt_list)
	{
		Node	   *stmt = (Node *) lfirst(stmt_item);

		if (GetCommandLogLevel(stmt) <= log_statement)
			return true;
	}

	return false;
}

/*
 * check_log_duration
 *		Determine whether current command's duration should be logged
 *
 * Returns:
 *		0 if no logging is needed
 *		1 if just the duration should be logged
 *		2 if duration and query details should be logged
 *
 * If logging is needed, the duration in msec is formatted into msec_str[],
 * which must be a 32-byte buffer.
 *
 * was_logged should be TRUE if caller already logged query details (this
 * essentially prevents 2 from being returned).
 */
int
check_log_duration(char *msec_str, bool was_logged)
{
	if (log_duration || log_min_duration_statement >= 0)
	{
		long		secs;
		int			usecs;
		int			msecs;
		bool		exceeded;

		TimestampDifference(GetCurrentStatementStartTimestamp(),
							GetCurrentTimestamp(),
							&secs, &usecs);
		msecs = usecs / 1000;

		/*
		 * This odd-looking test for log_min_duration_statement being exceeded
		 * is designed to avoid integer overflow with very long durations:
		 * don't compute secs * 1000 until we've verified it will fit in int.
		 */
		exceeded = (log_min_duration_statement == 0 ||
					(log_min_duration_statement > 0 &&
					 (secs > log_min_duration_statement / 1000 ||
					  secs * 1000 + msecs >= log_min_duration_statement)));

		if (exceeded || log_duration)
		{
			snprintf(msec_str, 32, "%ld.%03d",
					 secs * 1000 + msecs, usecs % 1000);
			if (exceeded && !was_logged)
				return 2;
			else
				return 1;
		}
	}

	return 0;
}

/*
 * errdetail_execute
 *
 * Add an errdetail() line showing the query referenced by an EXECUTE, if any.
 * The argument is the raw parsetree list.
 */
static int
errdetail_execute(List *raw_parsetree_list)
{
	ListCell   *parsetree_item;

	foreach(parsetree_item, raw_parsetree_list)
	{
		Node	   *parsetree = (Node *) lfirst(parsetree_item);

		if (IsA(parsetree, ExecuteStmt))
		{
			ExecuteStmt *stmt = (ExecuteStmt *) parsetree;
			PreparedStatement *pstmt;

			pstmt = FetchPreparedStatement(stmt->name, false);
			if (pstmt && pstmt->query_string)
			{
				errdetail("prepare: %s", pstmt->query_string);
				return 0;
			}
		}
	}

	return 0;
}

/*
 * errdetail_params
 *
 * Add an errdetail() line showing bind-parameter data, if available.
 */
static int
errdetail_params(ParamListInfo params)
{
	/* We mustn't call user-defined I/O functions when in an aborted xact */
	if (params && params->numParams > 0 && !IsAbortedTransactionBlockState())
	{
		StringInfoData param_str;
		MemoryContext oldcontext;
		int			paramno;

		/* Make sure any trash is generated in MessageContext */
		oldcontext = MemoryContextSwitchTo(MessageContext);

		initStringInfo(&param_str);

		for (paramno = 0; paramno < params->numParams; paramno++)
		{
			ParamExternData *prm = &params->params[paramno];
			Oid			typoutput;
			bool		typisvarlena;
			char	   *pstring;
			char	   *p;

			appendStringInfo(&param_str, "%s$%d = ",
							 paramno > 0 ? ", " : "",
							 paramno + 1);

			if (prm->isnull || !OidIsValid(prm->ptype))
			{
				appendStringInfoString(&param_str, "NULL");
				continue;
			}

			getTypeOutputInfo(prm->ptype, &typoutput, &typisvarlena);

			pstring = OidOutputFunctionCall(typoutput, prm->value);

			appendStringInfoCharMacro(&param_str, '\'');
			for (p = pstring; *p; p++)
			{
				if (*p == '\'') /* double single quotes */
					appendStringInfoCharMacro(&param_str, *p);
				appendStringInfoCharMacro(&param_str, *p);
			}
			appendStringInfoCharMacro(&param_str, '\'');

			pfree(pstring);
		}

		errdetail("parameters: %s", param_str.data);

		pfree(param_str.data);

		MemoryContextSwitchTo(oldcontext);
	}

	return 0;
}

/*
 * exec_describe_statement_message
 *
 * Process a "Describe" message for a prepared statement
 */
static void
exec_describe_statement_message(const char *stmt_name)
{
	PreparedStatement *pstmt;
	TupleDesc	tupdesc;
	ListCell   *l;
	StringInfoData buf;

	/*
	 * Start up a transaction command. (Note that this will normally change
	 * current memory context.) Nothing happens if we are already in one.
	 */
	start_xact_command();

	/* Switch back to message context */
	MemoryContextSwitchTo(MessageContext);

	/* Find prepared statement */
	if (stmt_name[0] != '\0')
		pstmt = FetchPreparedStatement(stmt_name, true);
	else
	{
		/* special-case the unnamed statement */
		pstmt = unnamed_stmt_pstmt;
		if (!pstmt)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_PSTATEMENT),
					 errmsg("unnamed prepared statement does not exist")));
	}

	/*
	 * If we are in aborted transaction state, we can't safely create a result
	 * tupledesc, because that needs catalog accesses.	Hence, refuse to
	 * Describe statements that return data.  (We shouldn't just refuse all
	 * Describes, since that might break the ability of some clients to issue
	 * COMMIT or ROLLBACK commands, if they use code that blindly Describes
	 * whatever it does.)  We can Describe parameters without doing anything
	 * dangerous, so we don't restrict that.
	 */
	if (IsAbortedTransactionBlockState() &&
		PreparedStatementReturnsTuples(pstmt))
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block")));

	if (whereToSendOutput != DestRemote)
		return;					/* can't actually do anything... */

	/*
	 * First describe the parameters...
	 */
	pq_beginmessage(&buf, 't'); /* parameter description message type */
	pq_sendint(&buf, list_length(pstmt->argtype_list), 2);

	foreach(l, pstmt->argtype_list)
	{
		Oid			ptype = lfirst_oid(l);

		pq_sendint(&buf, (int) ptype, 4);
	}
	pq_endmessage(&buf);

	/*
	 * Next send RowDescription or NoData to describe the result...
	 */
	tupdesc = FetchPreparedStatementResultDesc(pstmt);
	if (tupdesc)
		SendRowDescriptionMessage(tupdesc,
								  FetchPreparedStatementTargetList(pstmt),
								  NULL);
	else
		pq_putemptymessage('n');	/* NoData */

}

/*
 * exec_describe_portal_message
 *
 * Process a "Describe" message for a portal
 */
static void
exec_describe_portal_message(const char *portal_name)
{
	Portal		portal;

	/*
	 * Start up a transaction command. (Note that this will normally change
	 * current memory context.) Nothing happens if we are already in one.
	 */
	start_xact_command();

	/* Switch back to message context */
	MemoryContextSwitchTo(MessageContext);

	portal = GetPortalByName(portal_name);
	if (!PortalIsValid(portal))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("portal \"%s\" does not exist", portal_name)));

	/*
	 * If we are in aborted transaction state, we can't run
	 * SendRowDescriptionMessage(), because that needs catalog accesses.
	 * Hence, refuse to Describe portals that return data.	(We shouldn't just
	 * refuse all Describes, since that might break the ability of some
	 * clients to issue COMMIT or ROLLBACK commands, if they use code that
	 * blindly Describes whatever it does.)
	 */
	if (IsAbortedTransactionBlockState() &&
		portal->tupDesc)
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block")));

	if (whereToSendOutput != DestRemote)
		return;					/* can't actually do anything... */

	if (portal->tupDesc)
		SendRowDescriptionMessage(portal->tupDesc,
								  FetchPortalTargetList(portal),
								  portal->formats);
	else
		pq_putemptymessage('n');	/* NoData */
}


/*
 * Convenience routines for starting/committing a single command.
 */
static void
start_xact_command(void)
{
	if (!xact_started)
	{
		/* Cancel any active statement timeout before committing */
		disable_sig_alarm(true);

		/* Now commit the command */
		ereport(DEBUG3,
				(errmsg_internal("StartTransactionCommand")));
		StartTransactionCommand();

		/* Set statement timeout running, if any */
		/* NB: this mustn't be enabled until we are within an xact */
		if (StatementTimeout > 0 && Gp_role != GP_ROLE_EXECUTE)
			enable_sig_alarm(StatementTimeout, true);
		else
			cancel_from_timeout = false;

		xact_started = true;
	}
}

static void
finish_xact_command(void)
{
	if (xact_started)
	{
		/* Cancel any active statement timeout before committing */
		disable_sig_alarm(true);

		/* Now commit the command */
		ereport(DEBUG3,
				(errmsg_internal("CommitTransactionCommand")));

		CommitTransactionCommand();

#ifdef MEMORY_CONTEXT_CHECKING
		/* Check all memory contexts that weren't freed during commit */
		/* (those that were, were checked before being deleted) */
		MemoryContextCheck(TopMemoryContext);
#endif

#ifdef SHOW_MEMORY_STATS
		/* Print mem stats after each commit for leak tracking */
		if (ShowStats)
			MemoryContextStats(TopMemoryContext);
#endif

		xact_started = false;
	}
}


/*
 * Convenience routines for checking whether a statement is one of the
 * ones that we allow in transaction-aborted state.
 */

/* Test a bare parsetree */
static bool
IsTransactionExitStmt(Node *parsetree)
{
	if (parsetree && IsA(parsetree, TransactionStmt))
	{
		TransactionStmt *stmt = (TransactionStmt *) parsetree;

		if (stmt->kind == TRANS_STMT_COMMIT ||
			stmt->kind == TRANS_STMT_PREPARE ||
			stmt->kind == TRANS_STMT_ROLLBACK ||
			stmt->kind == TRANS_STMT_ROLLBACK_TO)
			return true;
	}
	return false;
}

/*
 * IsTransactionExitStmtList
 *    Return true if this is a singleton list of Query wrapping a transaction
 * ending statement, or a singleton list of transaction ending statement.
 */
static bool
IsTransactionExitStmtList(List *parseTrees)
{
	if (list_length(parseTrees) == 1)
	{
		Node *node = linitial(parseTrees);

		if (IsTransactionExitStmt(node))
		{
			return true;
		}

		if (IsA(node, Query))
		{
			Query *query = (Query *)node;
			if (query->commandType == CMD_UTILITY &&
				IsTransactionExitStmt(query->utilityStmt))
			{
				return true;
			}
		}
	}
	return false;
}

static bool
IsTransactionStmtList(List *nodes)
{
	if (list_length(nodes) == 1)
	{
		Node	   *stmt = (Node *) linitial(nodes);

		if (IsA(stmt, TransactionStmt))
			return true;

	}
	return false;
}

/* --------------------------------
 *		signal handler routines used in PostgresMain()
 * --------------------------------
 */

/*
 * quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 *
 * NOTE: see MPP-9518 and MPP-7564, there are other backend processes
 * which come through here, there isn't anything specific to any particular
 * backend here. If these other processes need to do their own handling
 * they should override the signal handler for SIGQUIT (and in that handler
 * call this one ?).
 *
 *
 * @param SIGNAL_ARGS -- so the signature matches a signal handler.  Nore that
 */
void
quickdie(SIGNAL_ARGS)
{
	quickdie_impl();
}

/**
 * implementation of quick-die that does take SIGNAL_ARGS parameter
 */
void
quickdie_impl()
{
	PG_SETMASK(&BlockSig);

	in_quickdie=true;
	/*
	 * DO NOT proc_exit() -- we're here because shared memory may be
	 * corrupted, so we don't want to try to clean up our transaction. Just
	 * nail the windows shut and get out of town.
	 *
	 * Note we do exit(2) not exit(0).	This is to force the postmaster into a
	 * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.
	 */

	/*
	 * MPP-7564: exit(2) will call proc_exit()'s on_exit/at_exit
	 * hooks.  We want to skip them. (quickdie is equivalent to a
	 * crash -- we're depending on crash recovery to save us on
	 * restart). So first we reset the exit hooks.
	 *
	 * This is a merge from Postgres.
	 */
	on_exit_reset();

	/*
	 * MPP-17167: We need to release any filrep or primary/mirror spin locks.
	 * to allow the possibility that filerep itself has sent us a SIGQUIT
	 * message as part of filerep transition.
	 */
	primaryMirrorModeResetSpinLocks();

	_exit(2);
}

/*
 * Shutdown signal from postmaster: abort transaction and exit
 * at soonest convenient time
 */
void
die(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/* Don't joggle the elbow of proc_exit */
	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;

		/* although we don't strictly need to set this to true since the
		 * ProcDiePending will occur first.  We set this anyway since the
		 * MPP dispatch code is triggered only off of QueryCancelPending
		 * and not any of the others.
		 */
		QueryCancelPending = true;

		/*
		 * If it's safe to interrupt, and we're waiting for input or a lock,
		 * service the interrupt immediately
		 */
		if (ImmediateInterruptOK && InterruptHoldoffCount == 0 &&
			CritSectionCount == 0)
		{
			/* bump holdoff count to make ProcessInterrupts() a no-op */
			/* until we are done getting ready for it */
			InterruptHoldoffCount++;
			DisableNotifyInterrupt();
			DisableCatchupInterrupt();
			/* Make sure CheckDeadLock won't run while shutting down... */
			LockWaitCancel();
			InterruptHoldoffCount--;
			ProcessInterrupts();
		}
	}

	errno = save_errno;
}

/*
 * Timeout or shutdown signal from postmaster during client authentication.
 * Simply exit(1).
 *
 * XXX: possible future improvement: try to send a message indicating
 * why we are disconnecting.  Problem is to be sure we don't block while
 * doing so, nor mess up the authentication message exchange.
 */
void
authdie(SIGNAL_ARGS)
{
	exit(1);
}

/*
 * Query-cancel signal from postmaster: abort current transaction
 * at soonest convenient time
 */
void
StatementCancelHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/*
	 * Don't joggle the elbow of proc_exit
	 */
	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		QueryCancelPending = true;
		QueryCancelCleanup = true;

		/*
		 * If it's safe to interrupt, and we're waiting for a lock, service
		 * the interrupt immediately.  No point in interrupting if we're
		 * waiting for input, however.
		 */
		if (ImmediateInterruptOK && InterruptHoldoffCount == 0 &&
			CritSectionCount == 0)
		{
			/* bump holdoff count to make ProcessInterrupts() a no-op */
			/* until we are done getting ready for it */
			InterruptHoldoffCount++;
			if (LockWaitCancel())
			{
				DisableNotifyInterrupt();
				DisableCatchupInterrupt();
				InterruptHoldoffCount--;
				ProcessInterrupts();
			}
			else
				InterruptHoldoffCount--;
		}
	}

	errno = save_errno;
}


/* CDB: Signal handler for program errors */
void
CdbProgramErrorHandler(SIGNAL_ARGS)
{
    int			save_errno = errno;
    char       *pts = "process";

	if (!pthread_equal(main_tid, pthread_self()))
	{
#ifndef _WIN32
		write_stderr("\nUnexpected internal error: Master %d received signal %d in worker thread %lu (forwarding signal to main thread)\n\n",
					 MyProcPid, postgres_signal_arg, (unsigned long)pthread_self());
#else
		write_stderr("\nUnexpected internal error: Master %d received signal %d in worker thread %lu (forwarding signal to main thread)\n\n",
					 MyProcPid, postgres_signal_arg, (unsigned long)pthread_self().p);
#endif 
		/* Only forward if the main thread isn't quick-dying. */
		if (!in_quickdie)
			pthread_kill(main_tid, postgres_signal_arg);

		/*
		 * Don't exit the thread when we reraise SEGV/BUS/ILL signals to the OS.
		 * This thread will die together with the main thread after the OS reraises
		 * the signal. This is to ensure that the dumped core file contains the call
		 * stack on this thread for later debugging.
		 */
		if (!(gp_reraise_signal &&
			  (postgres_signal_arg == SIGSEGV ||
			   postgres_signal_arg == SIGILL ||
			   postgres_signal_arg == SIGBUS)))
		{
			pthread_exit(NULL);
		}

		return;
	}


    if (Gp_role == GP_ROLE_DISPATCH)
        pts = "Master process";
    else if (Gp_role == GP_ROLE_EXECUTE)
        pts = "Segment process";
    else
        pts = "Process";

    errno = save_errno;
    StandardHandlerForSigillSigsegvSigbus_OnMainThread(pts, PASS_SIGNAL_ARGS);
}                               /* CdbProgramErrorHandler */

/* signal handler for floating point exception */
void
FloatExceptionHandler(SIGNAL_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FLOATING_POINT_EXCEPTION),
			 errmsg("floating-point exception"),
			 errdetail("An invalid floating-point operation was signaled. "
					   "This probably means an out-of-range result or an "
					   "invalid operation, such as division by zero.")));
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
SigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}


/*
 * ProcessInterrupts: out-of-line portion of CHECK_FOR_INTERRUPTS() macro
 *
 * If an interrupt condition is pending, and it's safe to service it,
 * then clear the flag and accept the interrupt.  Called only when
 * InterruptPending is true.
 */
void
ProcessInterrupts(void)
{

#ifdef USE_TEST_UTILS
	int simex_run = gp_simex_run;
#endif   /* USE_TEST_UTILS */

	/* OK to accept interrupt now? */
	if (InterruptHoldoffCount != 0 || CritSectionCount != 0)
		return;

#ifdef USE_TEST_UTILS
	/* disable SimEx for CHECK_FOR_INTERRUPTS */
	if (gp_simex_init && gp_simex_run && gp_simex_class == SimExESClass_Cancel)
	{
		gp_simex_run = 0;
	}
#endif   /* USE_TEST_UTILS */

	InterruptPending = false;
	if (ProcDiePending)
	{
		ProcDiePending = false;
		QueryCancelPending = false;		/* ProcDie trumps QueryCancel */
		ImmediateInterruptOK = false;	/* not idle anymore */
		DisableNotifyInterrupt();
		DisableCatchupInterrupt();

		/*
		 * Make sure threads get cleaned up: FATAL will fast-track
		 * exit, it will not be caught by the executor cleanup code,
		 * so if we want the QEs to get a cancel notification we need
		 * to call CdbCheckDispatchResult(). Also we're about to
		 * yank the carpet out from under any dispatched threads!
		 */
		if (Gp_role == GP_ROLE_DISPATCH)
			CdbShutdownPortals();

		ereport(FATAL,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("terminating connection due to administrator command"),
			 errSendAlert(false)));
	}

	if (ClientConnectionLost)
	{
		QueryCancelPending = false;		/* lost connection trumps QueryCancel */
		ImmediateInterruptOK = false;	/* not idle anymore */
		DisableNotifyInterrupt();
		DisableCatchupInterrupt();
		/* don't send to client, we already know the connection to be dead. */
		whereToSendOutput = DestNone;
		ereport(FATAL,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("connection to client lost")));
	}

	if (QueryCancelPending)
	{
		elog(LOG,"Process interrupt for 'query cancel pending'.");

		QueryCancelPending = false;
			ImmediateInterruptOK = false;	/* not idle anymore */
			DisableNotifyInterrupt();
			DisableCatchupInterrupt();
			if (Gp_role == GP_ROLE_EXECUTE)
				ereport(ERROR,
						(errcode(ERRCODE_GP_OPERATION_CANCELED),
						 errmsg("canceling MPP operation")));
			else if (cancel_from_timeout)
				ereport(ERROR,
						(errcode(ERRCODE_QUERY_CANCELED),
						 errmsg("canceling statement due to statement timeout")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_QUERY_CANCELED),
						 errmsg("canceling statement due to user request")));
		}
	/* If we get here, do nothing (probably, QueryCancelPending was reset) */

#ifdef USE_TEST_UTILS
	/* restore SimEx for CHECK_FOR_INTERRUPTS */
	if (gp_simex_init && simex_run && gp_simex_class == SimExESClass_Cancel)
	{
		gp_simex_run = simex_run;
	}
#endif   /* USE_TEST_UTILS */
}

/*
 * Set up the thread signal mask, we don't want to run our signal handlers
 * in our threads (gang-create, dispatch or interconnect threads)
 */
void
gp_set_thread_sigmasks(void)
{
#ifndef WIN32
	sigset_t sigs;

	if (pthread_equal(main_tid, pthread_self()))
	{
		elog(LOG, "thread_mask called from main thread!");
		return;
	}

	sigemptyset(&sigs);

	/* make our thread ignore these signals (which should allow that
	 * they be delivered to the main thread) */
	sigaddset(&sigs, SIGHUP);
	sigaddset(&sigs, SIGINT);
	sigaddset(&sigs, SIGTERM);
	sigaddset(&sigs, SIGUSR1);
	sigaddset(&sigs, SIGUSR2);

	pthread_sigmask(SIG_BLOCK, &sigs, NULL);
#endif

	return;
}

/*
 * check_stack_depth: check for excessively deep recursion
 *
 * This should be called someplace in any recursive routine that might possibly
 * recurse deep enough to overflow the stack.  Most Unixen treat stack
 * overflow as an unrecoverable SIGSEGV, so we want to error out ourselves
 * before hitting the hardware limit.
 */
void
check_stack_depth(void)
{
	char		stack_top_loc;
	long		stack_depth;

	/*
	 * Compute distance from PostgresMain's local variables to my own
	 */
	stack_depth = (long) (stack_base_ptr - &stack_top_loc);

	/*
	 * Take abs value, since stacks grow up on some machines, down on others
	 */
	if (stack_depth < 0)
		stack_depth = -stack_depth;

	/*
	 * Trouble?
	 *
	 * The test on stack_base_ptr prevents us from erroring out if called
	 * during process setup or in a non-backend process.  Logically it should
	 * be done first, but putting it here avoids wasting cycles during normal
	 * cases.
	 */
	if (stack_depth > max_stack_depth_bytes &&
		stack_base_ptr != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				 errmsg("stack depth limit exceeded"),
				 errhint("Increase the configuration parameter \"max_stack_depth\", "
						 "after ensuring the platform's stack depth limit is adequate.")));
	}
}

/* GUC assign hook for max_stack_depth */
bool
assign_max_stack_depth(int newval, bool doit, GucSource source)
{
	long		newval_bytes = newval * 1024L;
	long		stack_rlimit = get_stack_depth_rlimit();

	if (stack_rlimit > 0 && newval_bytes > stack_rlimit - STACK_DEPTH_SLOP)
	{
		ereport((source >= PGC_S_INTERACTIVE) ? ERROR : LOG,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"max_stack_depth\" must not exceed %ldkB",
						(stack_rlimit - STACK_DEPTH_SLOP) / 1024L),
				 errhint("Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent.")));
		return false;
	}
	if (doit)
		max_stack_depth_bytes = newval_bytes;
	return true;
}


/*
 * set_debug_options --- apply "-d N" command line option
 *
 * -d is not quite the same as setting log_min_messages because it enables
 * other output options.
 */
void
set_debug_options(int debug_flag, GucContext context, GucSource source)
{
	if (debug_flag > 0)
	{
		char		debugstr[64];

		sprintf(debugstr, "debug%d", debug_flag);
		SetConfigOption("log_min_messages", debugstr, context, source);
	}
	else
		SetConfigOption("log_min_messages", "notice", context, source);

	if (debug_flag >= 1 && context == PGC_POSTMASTER)
	{
		SetConfigOption("log_connections", "true", context, source);
		SetConfigOption("log_disconnections", "true", context, source);
	}
	if (debug_flag >= 2)
		SetConfigOption("log_statement", "all", context, source);
	if (debug_flag >= 3)
		SetConfigOption("debug_print_parse", "true", context, source);
	if (debug_flag >= 4)
		SetConfigOption("debug_print_plan", "true", context, source);
	if (debug_flag >= 5)
		SetConfigOption("debug_print_rewritten", "true", context, source);
}


bool
set_plan_disabling_options(const char *arg, GucContext context, GucSource source)
{
	char	   *tmp = NULL;

	switch (arg[0])
	{
		case 's':				/* seqscan */
			tmp = "enable_seqscan";
			break;
		case 'i':				/* indexscan */
			tmp = "enable_indexscan";
			break;
		case 'b':				/* bitmapscan */
			tmp = "enable_bitmapscan";
			break;
		case 't':				/* tidscan */
			tmp = "enable_tidscan";
			break;
		case 'n':				/* nestloop */
			tmp = "enable_nestloop";
			break;
		case 'm':				/* mergejoin */
			tmp = "enable_mergejoin";
			break;
		case 'h':				/* hashjoin */
			tmp = "enable_hashjoin";
			break;
	}
	if (tmp)
	{
		SetConfigOption(tmp, "false", context, source);
		return true;
	}
	else
		return false;
}


const char *
get_stats_option_name(const char *arg)
{
	switch (arg[0])
	{
		case 'p':
			if (optarg[1] == 'a')		/* "parser" */
				return "log_parser_stats";
			else if (optarg[1] == 'l')	/* "planner" */
				return "log_planner_stats";
			break;

		case 'e':				/* "executor" */
			return "log_executor_stats";
			break;
	}

	return NULL;
}

/* ----------------------------------------------------------------
 * PostgresMain
 *	   postgres main loop -- all backends, interactive or otherwise start here
 *
 * argc/argv are the command line arguments to be used.  (When being forked
 * by the postmaster, these are not the original argv array of the process.)
 * username is the (possibly authenticated) PostgreSQL user name to be used
 * for the session.
 * ----------------------------------------------------------------
 */
int
PostgresMain(int argc, char *argv[], const char *username)
{
	int			flag;
	const char *dbname = NULL;
	char	   *userDoption = NULL;
	bool		secure;
	int			errs = 0;
	int			debug_flag = -1;	/* -1 means not given */
	List	   *guc_names = NIL;	/* for SUSET options */
	List	   *guc_values = NIL;
	GucContext	ctx;
	GucSource	gucsource;
	bool		am_superuser;
	int			firstchar;
	char		stack_base;
	StringInfoData input_message;
	sigjmp_buf	local_sigjmp_buf;
	volatile bool send_ready_for_query = true;
	int			topErrCode;


	MemoryAccount *postgresMainMemoryAccount = NULL;

    /*
	 * CDB: Catch program error signals.
	 *
	 * Save our main thread-id for comparison during signals.
	 */
	main_tid = pthread_self();

#define PendingConfigOption(name,val) \
	(guc_names = lappend(guc_names, pstrdup(name)), \
	 guc_values = lappend(guc_values, pstrdup(val)))

	/*
	 * initialize globals (already done if under postmaster, but not if
	 * standalone; cheap enough to do over)
	 */
	MyProcPid = getpid();

#ifndef WIN32
	PostmasterPriority = getpriority(PRIO_PROCESS, 0);
#endif
	/*
	 * Fire up essential subsystems: error and memory management
	 *
	 * If we are running under the postmaster, this is done already.
	 */
	if (!IsUnderPostmaster)
		MemoryContextInit();

	/*
	 * Do not save the return value in any oldMemoryAccount variable.
	 * In that case, we risk switching to a stale memoryAccount that is no
	 * longer valid. This is because we reset the memory accounts frequently.
	 */
	postgresMainMemoryAccount = MemoryAccounting_CreateAccount(0, MEMORY_OWNER_TYPE_MainEntry);
	MemoryAccounting_SwitchAccount(postgresMainMemoryAccount);

	set_ps_display("startup", false);

	SetProcessingMode(InitProcessing);

	/* Set up reference point for stack depth checking */
	stack_base_ptr = &stack_base;

	/* Compute paths, if we didn't inherit them from postmaster */
	if (my_exec_path[0] == '\0')
	{
		if (find_my_exec(argv[0], my_exec_path) < 0)
			elog(FATAL, "%s: could not locate my own executable path",
				 argv[0]);
	}

	if (pkglib_path[0] == '\0')
		get_pkglib_path(my_exec_path, pkglib_path);

	/*
	 * Set default values for command-line options.
	 */
	EchoQuery = false;

	if (!IsUnderPostmaster)
		InitializeGUCOptions();


	/* ----------------
	 *	parse command line arguments
	 *
	 *	There are now two styles of command line layout for the backend:
	 *
	 *	For interactive use (not started from postmaster) the format is
	 *		postgres [switches] [databasename]
	 *	If the databasename is omitted it is taken to be the user name.
	 *
	 *	When started from the postmaster, the format is
	 *		postgres [secure switches] -p databasename [insecure switches]
	 *	Switches appearing after -p came from the client (via "options"
	 *	field of connection request).  For security reasons we restrict
	 *	what these switches can do.
	 * ----------------
	 */

		/* Ignore the initial --single argument, if present */
		if (argc > 1 && strcmp(argv[1], "--single") == 0)
		{
			argv++;
			argc--;
		}

	/* all options are allowed until '-p' */
	secure = true;
	ctx = PGC_POSTMASTER;
	gucsource = PGC_S_ARGV;		/* initial switches came from command line */

	/*
	 * Parse command-line options.  CAUTION: keep this in sync with
	 * postmaster/postmaster.c (the option sets should not conflict)
	 * and with the common help() function in main/main.c.
	 */
	while ((flag = getopt(argc, argv, "A:B:b:C:c:D:d:EeFf:h:ijk:m:lN:nOo:Pp:r:S:sTt:Uv:W:x:y:z:-:")) != -1)
	{
		switch (flag)
		{
			case 'A':
				SetConfigOption("debug_assertions", optarg, ctx, gucsource);
				break;

			case 'B':
				SetConfigOption("shared_buffers", optarg, ctx, gucsource);
				break;

			case 'D':
				if (secure)
					userDoption = optarg;
				break;

			case 'd':
				debug_flag = atoi(optarg);
				break;

			case 'E':
				EchoQuery = true;
				break;

			case 'e':
				SetConfigOption("datestyle", "euro", ctx, gucsource);
				break;

			case 'F':
				SetConfigOption("fsync", "false", ctx, gucsource);
				break;

			case 'f':
				if (!set_plan_disabling_options(optarg, ctx, gucsource))
					errs++;
				break;

			case 'h':
				SetConfigOption("listen_addresses", optarg, ctx, gucsource);
				break;

			case 'i':
				SetConfigOption("listen_addresses", "*", ctx, gucsource);
				break;

			case 'j':
				UseNewLine = 0;
				break;

			case 'k':
				SetConfigOption("unix_socket_directory", optarg, ctx, gucsource);
				break;

			case 'l':
				SetConfigOption("ssl", "true", ctx, gucsource);
				break;

			case 'm':
				/*
				 * In maintenance mode:
				 * 	1. allow DML on catalog table
				 * 	2. allow DML on segments
				 */
				SetConfigOption("maintenance_mode",         "true", ctx, gucsource);
				SetConfigOption("allow_segment_DML",        "true", ctx, gucsource);
				SetConfigOption("allow_system_table_mods",  "dml",  ctx, gucsource);
				break;

			case 'N':
				SetConfigOption("max_connections", optarg, ctx, gucsource);
				break;

			case 'n':
				/* ignored for consistency with postmaster */
				break;

			case 'O':
				/* Only use in single user mode */
				SetConfigOption("allow_system_table_mods", "all", ctx, gucsource);
				break;

			case 'o':
				errs++;
				break;

			case 'P':
				SetConfigOption("ignore_system_indexes", "true", ctx, gucsource);
				break;

			case 'p':
				SetConfigOption("port", optarg, ctx, gucsource);
				break;

			case 'r':
				/* send output (stdout and stderr) to the given file */
				if (secure)
					StrNCpy(OutputFileName, optarg, MAXPGPATH);
				break;

			case 'S':
				SetConfigOption("work_mem", optarg, ctx, gucsource);
				break;

			case 's':

				/*
				 * Since log options are SUSET, we need to postpone unless
				 * still in secure context
				 */
				if (ctx == PGC_BACKEND)
					PendingConfigOption("log_statement_stats", "true");
				else
				SetConfigOption("log_statement_stats", "true",
								ctx, gucsource);
				break;

			case 'T':
				/* ignored for consistency with postmaster */
				break;

			case 't':
				{
					const char *tmp = get_stats_option_name(optarg);

					if (tmp)
					{
						if (ctx == PGC_BACKEND)
							PendingConfigOption(tmp, "true");
						else
						SetConfigOption(tmp, "true", ctx, gucsource);
					}
					else
						errs++;
					break;
				}

			case 'U':
				/*
				 * In upgrade mode, we indicate we're in upgrade mode and
				 * 1. allow DML on persistent table & catalog table
				 * 2. alter DDL on catalog table (NOTE: upgrade_mode must set beforehand)
				 * 3. TODO: disable the 4.1 xlog format (stick with the old)
				 */
				SetConfigOption("upgrade_mode",                         "true", ctx, gucsource);
				SetConfigOption("gp_permit_persistent_metadata_update", "true", ctx, gucsource);
				SetConfigOption("allow_segment_DML",  		            "true", ctx, gucsource);
				SetConfigOption("allow_system_table_mods",              "all",  ctx, gucsource);
				break;

			case 'v':
				if (secure)
					FrontendProtocol = (ProtocolVersion) atoi(optarg);
				break;

			case 'W':
				SetConfigOption("post_auth_delay", optarg, ctx, gucsource);
				break;


			case 'y':

				/*
				 * y - special flag passed if backend was forked by a
				 * postmaster.
				 */
				if (secure)
				{
					dbname = strdup(optarg);

					secure = false;		/* subsequent switches are NOT secure */
					ctx = PGC_BACKEND;
					gucsource = PGC_S_CLIENT;
				}
				break;

			case 'c':
			case '-':
				{
					char	   *name,
							   *value;

					ParseLongOption(optarg, &name, &value);
					if (!value)
					{
						if (flag == '-')
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("--%s requires a value",
											optarg)));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("-c %s requires a value",
											optarg)));
					}

					/*
					 * If a SUSET option, must postpone evaluation, unless we
					 * are still reading secure switches.
					 */
					if (ctx == PGC_BACKEND && IsSuperuserConfigOption(name))
						PendingConfigOption(name, value);
					else
					SetConfigOption(name, value, ctx, gucsource);
					free(name);
					if (value)
						free(value);
					break;
				}

			case 'x': /* standby master dbid */
				SetConfigOption("gp_standby_dbid", optarg, ctx, gucsource);
				break;
            case 'z':
                SetConfigOption("gp_num_contents_in_cluster",
								optarg, ctx, gucsource);
                break;

			default:
				errs++;
				break;
		}
	}

	/*
	 * MPP:  If we were started in utility mode then we only want to allow
	 * incoming sessions that specify gp_session_role=utility as well.  This
	 * lets the bash scripts start the QD in utility mode and connect in but
	 * protect ourselves from normal clients who might be trying to connect to
	 * the system while we startup.
	 */
	if ((Gp_role == GP_ROLE_UTILITY) && (Gp_session_role != GP_ROLE_UTILITY))
	{
			ereport(FATAL,
				(errcode(ERRCODE_CANNOT_CONNECT_NOW),
				 errmsg("System was started in master-only utility mode - only utility mode connections are allowed")));
	}

	/*
	 * Process any additional GUC variable settings passed in startup packet.
	 * These are handled exactly like command-line variables.
	 */
	if (MyProcPort != NULL)
	{
		ListCell   *gucopts = list_head(MyProcPort->guc_options);

		while (gucopts)
		{
			char	   *name;
			char	   *value;

			name = lfirst(gucopts);
			gucopts = lnext(gucopts);

			value = lfirst(gucopts);
			gucopts = lnext(gucopts);

			if (IsSuperuserConfigOption(name))
				PendingConfigOption(name, value);
			else
				SetConfigOption(name, value, PGC_BACKEND, PGC_S_CLIENT);
		}
	}

	/* Acquire configuration parameters, unless inherited from postmaster */
	if (!IsUnderPostmaster)
	{
		if (!SelectConfigFiles(userDoption, argv[0]))
			proc_exit(1);
		/* If timezone is not set, determine what the OS uses */
		pg_timezone_initialize();
		/* If timezone_abbreviations is not set, select default */
		pg_timezone_abbrev_initialize();

        /*
	     * Remember stand-alone backend startup time.
         * CDB: Moved this up from below for use in error message headers.
         */
	    PgStartTime = GetCurrentTimestamp();
	}

	if (PostAuthDelay)
		pg_usleep(PostAuthDelay * 1000000L);

	/*
	 * You might expect to see a setsid() call here, but it's not needed,
	 * because if we are under a postmaster then BackendInitialize() did it.
	 */

	/*
	 * Set up signal handlers and masks.
	 *
	 * Note that postmaster blocked all signals before forking child process,
	 * so there is no race condition whereby we might receive a signal before
	 * we have set up the handler.
	 *
	 * Also note: it's best not to use any signals that are SIG_IGNored in the
	 * postmaster.	If such a signal arrives before we are able to change the
	 * handler to non-SIG_IGN, it'll get dropped.  Instead, make a dummy
	 * handler in the postmaster to reserve the signal. (Of course, this isn't
	 * an issue for signals that are locally generated, such as SIGALRM and
	 * SIGPIPE.)
	 */
		pqsignal(SIGHUP, SigHupHandler);	/* set flag to read config file */
		pqsignal(SIGINT, StatementCancelHandler);	/* cancel current query */
		pqsignal(SIGTERM, die);		/* cancel current query and exit */
		pqsignal(SIGQUIT, quickdie);	/* hard crash time */
		pqsignal(SIGALRM, handle_sig_alarm);		/* timeout conditions */

		/*
		 * Ignore failure to write to frontend. Note: if frontend closes
		 * connection, we will notice it and exit cleanly when control next
		 * returns to outer loop.  This seems safer than forcing exit in the midst
		 * of output during who-knows-what operation...
		 */
		pqsignal(SIGPIPE, SIG_IGN);
		pqsignal(SIGUSR1, procsignal_sigusr1_handler);
		pqsignal(SIGUSR2, NotifyInterruptHandler);
		pqsignal(SIGFPE, FloatExceptionHandler);

		/*
		 * Reset some signals that are accepted by postmaster but not by backend
		 */
		pqsignal(SIGCHLD, SIG_DFL); /* system() requires this on some platforms */

#ifndef _WIN32
#ifdef SIGILL
		pqsignal(SIGILL, CdbProgramErrorHandler);
#endif
#ifdef SIGSEGV
		pqsignal(SIGSEGV, CdbProgramErrorHandler);
#endif
#ifdef SIGBUS
		pqsignal(SIGBUS, CdbProgramErrorHandler);
#endif
#endif

	pqinitmask();

	/* We allow SIGQUIT (quickdie) at all times */
#ifdef HAVE_SIGPROCMASK
	sigdelset(&BlockSig, SIGQUIT);
#else
	BlockSig &= ~(sigmask(SIGQUIT));
#endif

	PG_SETMASK(&BlockSig);		/* block everything except SIGQUIT */

	if (IsUnderPostmaster)
	{
		/* noninteractive case: nothing should be left after switches */
		if (errs || argc != optind || dbname == NULL)
		{
			ereport(FATAL,
					(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid command-line arguments for server process"),
			   errhint("Try \"%s --help\" for more information.", argv[0])));
		}

		BaseInit();
	}
	else
	{
		/* interactive case: database name can be last arg on command line */
		if (errs || argc - optind > 1)
		{
			ereport(FATAL,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("%s: invalid command-line arguments",
							argv[0]),
			   errhint("Try \"%s --help\" for more information.", argv[0])));
		}
		else if (argc - optind == 1)
			dbname = argv[optind];
		else if ((dbname = username) == NULL)
		{
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s: no database nor user name specified",
							argv[0])));
		}

		/*
		 * Validate we have been given a reasonable-looking DataDir (if under
		 * postmaster, assume postmaster did this already).
		 */
		Assert(DataDir);
		ValidatePgVersion(DataDir);

		/* Change into DataDir (if under postmaster, was done already) */
		ChangeToDataDir();

		/*
		 * Create lockfile for data directory.
		 */
		CreateDataDirLockFile(false);

		BaseInit();

		/*
		 * Start up xlog for standalone backend, and register to have it
		 * closed down at exit.
		 */
		StartupXLOG();
		on_shmem_exit(ShutdownXLOG, 0);

		/*
		 * Read any existing FSM cache file, and register to write one out at
		 * exit.
		 */
		LoadFreeSpaceMap();
		on_shmem_exit(DumpFreeSpaceMap, 0);

		/*
		 * We have to build the flat file for pg_database, but not for the
		 * user and group tables, since we won't try to do authentication.
		 */
		BuildFlatFiles(true);
	}


	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifdef EXEC_BACKEND
	if (!IsUnderPostmaster)
		InitProcess();
#else
	InitProcess();
#endif

	/*
	 * General initialization.
	 *
	 * NOTE: if you are tempted to add code in this vicinity, consider putting
	 * it inside InitPostgres() instead.  In particular, anything that
	 * involves database access should be there, not here.
	 */
	ereport(DEBUG3,
			(errmsg_internal("InitPostgres")));

	/* This is the initdb process */
	if (MyProcPort == NULL ||
	    AmIMaster() || AmIStandby() ||
	    MyProcPort->bootstrap_user == NULL){
		am_superuser = InitPostgres(dbname, InvalidOid, username, NULL);
	}
	else {
		am_superuser = InitPostgres("template1", InvalidOid, MyProcPort->bootstrap_user, NULL);
	}

	SetProcessingMode(NormalProcessing);

	/**
	 * Initialize resource queue hash structure.
	 */
#if 0
	if (Gp_role == GP_ROLE_DISPATCH && ResourceScheduler)
		InitResQueues();
#endif

	/*
	 * Now that we know if client is a superuser, we can try to apply SUSET
	 * GUC options that came from the client.
	 */
	ctx = am_superuser ? PGC_SUSET : PGC_USERSET;

	if (debug_flag >= 0)
		set_debug_options(debug_flag, ctx, PGC_S_CLIENT);

	if (guc_names != NIL)
	{
		ListCell   *namcell,
				   *valcell;

		forboth(namcell, guc_names, valcell, guc_values)
		{
			char	   *name = (char *) lfirst(namcell);
			char	   *value = (char *) lfirst(valcell);

			SetConfigOption(name, value, ctx, PGC_S_CLIENT);
			pfree(name);
			pfree(value);
		}
	}

	/*
	 * Now all GUC states are fully set up.  Report them to client if
	 * appropriate.
	 */
	BeginReportingGUCOptions();

	/*
	 * Also set up handler to log session end; we have to wait till now to be
	 * sure Log_disconnections has its final value.
	 */
	if (IsUnderPostmaster && Log_disconnections)
		on_proc_exit(log_disconnections, 0);

	/*
	 * process any libraries that should be preloaded at backend start (this
	 * likewise can't be done until GUC settings are complete)
	 */
	process_local_preload_libraries();

	/*
	 * Send this backend's cancellation info to the frontend.
	 */
	if (whereToSendOutput == DestRemote &&
		PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2)
	{
		StringInfoData buf;

		pq_beginmessage(&buf, 'K');
		pq_sendint(&buf, (int32) MyProcPid, sizeof(int32));
		pq_sendint(&buf, (int32) MyCancelKey, sizeof(int32));
		pq_endmessage(&buf);
		/* Need not flush since ReadyForQuery will do it. */
	}

	/* Welcome banner for standalone case */
	if (whereToSendOutput == DestDebug)
		printf("\nPostgreSQL stand-alone backend %s\n", PG_VERSION);

	/*
	 * Create the memory context we will use in the main loop.
	 *
	 * MessageContext is reset once per iteration of the main loop, ie, upon
	 * completion of processing of each command message from the client.
	 */
	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);


	/*
	 * POSTGRES main processing loop begins here
	 *
	 * If an exception is encountered, processing resumes here so we abort the
	 * current transaction and start a new one.
	 *
	 * You might wonder why this isn't coded as an infinite loop around a
	 * PG_TRY construct.  The reason is that this is the bottom of the
	 * exception stack, and so with PG_TRY there would be no exception handler
	 * in force at all during the CATCH part.  By leaving the outermost setjmp
	 * always active, we have at least some chance of recovering from an error
	 * during error recovery.  (If we get into an infinite loop thereby, it
	 * will soon be stopped by overflow of elog.c's internal state stack.)
	 */

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/*
		 * NOTE: if you are tempted to add more code in this if-block,
		 * consider the high probability that it should be in
		 * AbortTransaction() instead.	The only stuff done directly here
		 * should be stuff that is guaranteed to apply *only* for outer-level
		 * error recovery, such as adjusting the FE/BE protocol status.
		 */

		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/*
		 * Forget any pending QueryCancel request, since we're returning to
		 * the idle loop anyway, and cancel the statement timer if running.
		 */
		QueryCancelPending = false;
		disable_sig_alarm(true);
		QueryCancelPending = false;		/* again in case timeout occurred */

		/*
		 * Turn off these interrupts too.  This is only needed here and not in
		 * other exception-catching places since these interrupts are only
		 * enabled while we wait for client input.
		 */
		DoingCommandRead = false;
		DisableNotifyInterrupt();
		DisableCatchupInterrupt();
		DisableClientWaitTimeoutInterrupt();

		/* Make sure libpq is in a good state */
		pq_comm_reset();

		/* Report the error to the client and/or server log */
		EmitErrorReport();

		/*
		 * Make sure debug_query_string gets reset before we possibly clobber
		 * the storage it points at.
		 */
		debug_query_string = NULL;

		topErrCode = elog_geterrcode();
		if (Gp_role == GP_ROLE_EXECUTE &&
			(topErrCode == ERRCODE_GP_OPERATION_CANCELED || topErrCode == ERRCODE_QUERY_CANCELED))
		{
			elog((Debug_print_full_dtm ? LOG : DEBUG5), "PostgresMain seeing cancel 0x%x",
			     topErrCode);
		}
				
		/*
		 * cleanup all temporary in-memory heap table after each query
		 * AbortCurrentTransaction will release the resource owner,
		 * should drop all in-memory heap table first.
		 */
		InMemHeap_DropAll(INMEM_HEAP_MAPPING);
		CleanupOidInMemHeapMapping(INMEM_HEAP_MAPPING);

		/*
		 * Abort the current transaction in order to recover.
		 */
		AbortCurrentTransaction();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(TopMemoryContext);
		FlushErrorState();
		QueryContext = NULL;

		/*
		 * If we were handling an extended-query-protocol message, initiate
		 * skip till next Sync.  This also causes us not to issue
		 * ReadyForQuery (until we get Sync).
		 */
		if (doing_extended_query_message)
			ignore_till_sync = true;

		/* We don't have a transaction command open anymore */
		xact_started = false;

		/* Inform Vmem tracker that the current process has finished cleanup */
		RunawayCleaner_RunawayCleanupDoneForProcess(false /* ignoredCleanup */);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	PG_SETMASK(&UnBlockSig);

	if (!ignore_till_sync)
		send_ready_for_query = true;	/* initially, or after error */

	/*
	 * Non-error queries loop here.
	 */

	for (;;)
	{
		/*
		 * At top of loop, reset extended-query-message flag, so that any
		 * errors encountered in "idle" state don't provoke skip.
		 */
		doing_extended_query_message = false;

		/*
		 * Release storage left over from prior query cycle, and create a new
		 * query input buffer in the cleared MessageContext.
		 */
		MemoryContextSwitchTo(MessageContext);
		MemoryContextResetAndDeleteChildren(MessageContext);
		VmemTracker_ResetMaxVmemReserved();
		MemoryAccounting_ResetPeakBalance();

		initStringInfo(&input_message);

        /* Reset elog globals */
        currentSliceId = UNSET_SLICE_ID;
        if (Gp_role == GP_ROLE_EXECUTE)
            gp_command_count = 0;

		/*
		 * (1) If we've reached idle state, tell the frontend we're ready for
		 * a new query.
		 *
		 * Note: this includes fflush()'ing the last of the prior output.
		 *
		 * This is also a good time to send collected statistics to the
		 * collector, and to update the PS stats display.  We avoid doing
		 * those every time through the message loop because it'd slow down
		 * processing of batched messages, and because we don't want to report
		 * uncommitted updates (that confuses autovacuum).
		 */
		if (send_ready_for_query)
		{
			if (IsTransactionOrTransactionBlock())
			{
				set_ps_display("idle in transaction", false);
				pgstat_report_activity("<IDLE> in transaction");
			}
			else
			{
				pgstat_report_stat(false);
				pgstat_report_queuestat();

				set_ps_display("idle", false);
				pgstat_report_activity("<IDLE>");

				if (Gp_role == GP_ROLE_DISPATCH)
				{
				  CleanupGlobalQueryResources();
				}
			}

			ReadyForQuery(whereToSendOutput);
			send_ready_for_query = false;
		}

		/*
		 * (2) Allow asynchronous signals to be executed immediately if they
		 * come in while we are waiting for client input. (This must be
		 * conditional since we don't want, say, reads on behalf of COPY FROM
		 * STDIN doing the same thing.)
		 */
		QueryCancelPending = false;		/* forget any earlier CANCEL signal */
		DoingCommandRead = true;

#ifdef USE_TEST_UTILS
		/* reset time slice */
		TimeSliceReset();
#endif /* USE_TEST_UTILS */

		/*
		 * (2b) Check for disable work for QD mirroring.
		 */
		if (Gp_role == GP_ROLE_DISPATCH)
			CheckForQDMirroringWork();

		/*
		 * (2c) Check for temp table delete reset session work.
		 */
		if (Gp_role == GP_ROLE_DISPATCH)
			CheckForResetSession();

		/*
		 * (3) read a command (loop blocks here)
		 */
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			/*
			 * We want to check to see if our session goes "idle" (nobody sending us work to do)
			 * We decide this it true if after waiting a while, we don't get a message from the client.
			 * We can then free resources (right now, just the gangs on the segDBs).
			 *
			 * A Bit ugly:  We share the sig alarm timer with the deadlock detection.
			 * We know which it is (deadlock detection needs to run or idle
			 * session resource release) based on the DoingCommandRead flag.
			 *
			 * Perhaps instead of calling enable_sig_alarm, we should just call
			 * setitimer() directly (we don't need to worry about the statement timeout timer
			 * because it can't be running when we are idle).
			 *
			 * We want the time value to be long enough so we don't free gangs prematurely.
			 * This means giving the end user enough time to type in the next SQL statement
			 *
			 */
			if (IdleSessionGangTimeout > 0 && gangsExist())
				if (!enable_sig_alarm( IdleSessionGangTimeout /* ms */, false))
					elog(FATAL, "could not set timer for client wait timeout");
		}

		IdleTracker_DeactivateProcess();
		firstchar = ReadCommand(&input_message);
		IdleTracker_ActivateProcess();

		if (!IsTransactionOrTransactionBlock()){
			/* Reset memory accounting */

			/*
			 * We finished processing the last query and currently we are not under
			 * any transaction. So reset memory accounting. Note: any memory
			 * allocated before resetting will go into the rollover memory account,
			 * allocated under top memory context.
			 */
			MemoryAccounting_Reset();

			postgresMainMemoryAccount = MemoryAccounting_CreateAccount(0, MEMORY_OWNER_TYPE_MainEntry);
			/*
			 * Don't attempt to save previous memory account. This will be invalid by the time we attempt to restore.
			 * This is why we are not using our START_MEMORY_ACCOUNT and END_MEMORY_ACCOUNT macros
			 */
			MemoryAccounting_SwitchAccount(postgresMainMemoryAccount);

			/* End of memory accounting setup */
		}

		/*
		 * (4) disable async signal conditions again.
		 */
		DoingCommandRead = false;

		/*
		 * (5) check for any other interesting events that happened while we
		 * slept.
		 */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * (6) process the command.  But ignore it if we're skipping till
		 * Sync.
		 */
		if (ignore_till_sync && firstchar != EOF)
			continue;

		elog((Debug_print_full_dtm ? LOG : DEBUG5), "First char: '%c'; gp_role = '%s'.",firstchar,role_to_string(Gp_role));

		switch (firstchar)
		{
			case 'Q':			/* simple query */
				{
					const char *query_string = NULL;

                    elog(DEBUG1, "Message type %c received by from libpq, len = %d", firstchar, input_message.len); /* TODO: Remove this */

					/* Set statement_timestamp() */
 					SetCurrentStatementStartTimestamp();

                    query_string = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);

					elog((Debug_print_full_dtm ? LOG : DEBUG5), "Simple query stmt: %s.",query_string);

					/*
					 * in hawq, there is no distributed transaction
					 */
					/*setupRegularDtxContext();*/

					exec_simple_query(query_string, NULL, -1);

					send_ready_for_query = true;
				}
				break;
            case 'M':           /* MPP dispatched stmt from QD */
				{
					/* This is exactly like 'Q' above except we peel off and
					 * set the snapshot information right away.
					 */
					/*
					 * Since PortalDefineQuery() does not take NULL query string,
					 * we initialize it with a contant empty string.
					 */
					const char *query_string = pstrdup("");

					const char *serializedSnapshot = NULL;
					const char *serializedQuerytree = NULL;
					const char *serializedPlantree = NULL;
					const char *serializedParams = NULL;
					const char *serializedSliceInfo = NULL;
					const char *seqServerHost = NULL;
					const char *serializedIdentity = NULL;
					const char *serializedResource = NULL;
					
					int query_string_len = 0;
					int serializedSnapshotlen = 0;
					int serializedQuerytreelen = 0;
					int serializedPlantreelen = 0;
					int serializedParamslen = 0;
					int serializedSliceInfolen = 0;
					int seqServerHostlen = 0;
					int seqServerPort = -1;
					int	serializedIdentityLen = 0;
					int serializedResourceLen = 0;
					int		localSlice;
					int		rootIdx;
					int		primary_gang_id;
					TimestampTz statementStart;
					Oid 	suid;
					Oid 	ouid;
					Oid 	cuid;
					bool	suid_is_super = false;
					bool	ouid_is_super = false;

					int unusedFlags;

					/* Set statement_timestamp() */
 					SetCurrentStatementStartTimestamp();

					/* get the client command serial# */
					gp_command_count = pq_getmsgint(&input_message, 4);

					elog(DEBUG1, "Message type %c received by from libpq, len = %d", firstchar, input_message.len); /* TODO: Remove this */

					/* Get the userid info  (session, outer, current) */
					suid = pq_getmsgint(&input_message, 4);
					if(pq_getmsgbyte(&input_message) == 1)
						suid_is_super = true;

					ouid = pq_getmsgint(&input_message, 4);
					if(pq_getmsgbyte(&input_message) == 1)
						ouid_is_super = true;
					cuid = pq_getmsgint(&input_message, 4);

					/* get the slice number# */
					localSlice = pq_getmsgint(&input_message, 4);

					rootIdx = pq_getmsgint(&input_message, 4);

					primary_gang_id = pq_getmsgint(&input_message, 4);

					statementStart = pq_getmsgint64(&input_message);
					/*
					 * Should we set the CurrentStatementStartTimestamp to the
					 * original start of the statement (as seen by the masterDB?
					 *
					 * Or have it be the time this particular QE received it's work?
					 *
					 * Or both?
					 */
					SetCurrentStatementStartTimestampToMaster(statementStart);

					/* read ser string lengths */
					query_string_len = pq_getmsgint(&input_message, 4);
					serializedQuerytreelen = pq_getmsgint(&input_message, 4);
					serializedPlantreelen = pq_getmsgint(&input_message, 4);
					serializedParamslen = pq_getmsgint(&input_message, 4);
					serializedSliceInfolen = pq_getmsgint(&input_message, 4);
					serializedSnapshotlen = pq_getmsgint(&input_message, 4);
					serializedIdentityLen = pq_getmsgint(&input_message, 4);
					serializedResourceLen = pq_getmsgint(&input_message, 4);
						
					/* read in the snapshot info */
					if (serializedSnapshotlen == 0)
						serializedSnapshot = NULL;
					else
						serializedSnapshot = pq_getmsgbytes(&input_message,serializedSnapshotlen);

					/* get the transaction options */
					unusedFlags = pq_getmsgint(&input_message, 4);
					Assert(0 == unusedFlags);

					seqServerHostlen = pq_getmsgint(&input_message, 4);
					seqServerPort = pq_getmsgint(&input_message, 4);

					/* get the query string and kick off processing. */
					if (query_string_len > 0)
						query_string = pq_getmsgbytes(&input_message,query_string_len);

					if (serializedQuerytreelen > 0)
						serializedQuerytree = pq_getmsgbytes(&input_message,serializedQuerytreelen);

					if (serializedPlantreelen > 0)
						serializedPlantree = pq_getmsgbytes(&input_message,serializedPlantreelen);

					if (serializedParamslen > 0)
						serializedParams = pq_getmsgbytes(&input_message,serializedParamslen);

					if (serializedSliceInfolen > 0)
						serializedSliceInfo = pq_getmsgbytes(&input_message,serializedSliceInfolen);

					if (serializedIdentityLen > 0)
						serializedIdentity = pq_getmsgbytes(&input_message, serializedIdentityLen);

					if (serializedResourceLen > 0)
						serializedResource = pq_getmsgbytes(&input_message, serializedResourceLen);

					if (seqServerHostlen > 0)
						seqServerHost = pq_getmsgbytes(&input_message, seqServerHostlen);

					pq_getmsgend(&input_message);

					elog((Debug_print_full_dtm ? LOG : DEBUG5), "MPP dispatched stmt from QD: %s.",query_string);

					if (suid > 0)
						SetSessionUserId(suid, suid_is_super);  /* Set the session UserId */

					if (ouid > 0 && ouid != GetSessionUserId())
						SetCurrentRoleId(ouid, ouid_is_super);		/* Set the outer UserId */

					if (cuid > 0)
						SetUserIdAndContext(cuid, false); /* Set current userid */

					if (serializedIdentityLen > 0)
						SetupProcessIdentity(serializedIdentity);

					if (serializedQuerytreelen==0 && serializedPlantreelen==0)
					{
						if (strncmp(query_string, "BEGIN", 5) == 0)
						{
							CommandDest dest = whereToSendOutput;

							/*
							 * Special explicit BEGIN for COPY, etc.
							 * We've already begun it as part of setting up the context.
							 */
							elog((Debug_print_full_dtm ? LOG : DEBUG5), "PostgresMain explicit %s", query_string);

							// UNDONE: HACK
							pgstat_report_activity("BEGIN");

							set_ps_display("BEGIN", false);

							BeginCommand("BEGIN", dest);

							EndCommand("BEGIN", dest);

						}
						else
						{
							exec_simple_query(query_string, seqServerHost, seqServerPort);
						}
					}
					else
						exec_mpp_query(query_string,
									   serializedQuerytree, serializedQuerytreelen,
									   serializedPlantree, serializedPlantreelen,
									   serializedParams, serializedParamslen,
									   serializedSliceInfo, serializedSliceInfolen,
									   serializedResource, serializedResourceLen,
									   seqServerHost, seqServerPort, localSlice);

					SetUserIdAndContext(GetOuterUserId(), false);

					send_ready_for_query = true;
				}
				break;

			case 'P':			/* parse */
				{
					const char *stmt_name;
					const char *query_string;
					int			numParams;
					Oid		   *paramTypes = NULL;

					/* Set statement_timestamp() */
					SetCurrentStatementStartTimestamp();

					stmt_name = pq_getmsgstring(&input_message);
					query_string = pq_getmsgstring(&input_message);
					numParams = pq_getmsgint(&input_message, 2);
					if (numParams > 0)
					{
						int			i;

						paramTypes = (Oid *) palloc(numParams * sizeof(Oid));
						for (i = 0; i < numParams; i++)
							paramTypes[i] = pq_getmsgint(&input_message, 4);
					}
					pq_getmsgend(&input_message);

					elog((Debug_print_full_dtm ? LOG : DEBUG5), "Parse: %s.",query_string);

					/*
					 *  in hawq, there is no distributed transaction
					 */
					/*setupRegularDtxContext();*/

					exec_parse_message(query_string, stmt_name,
									   paramTypes, numParams);
				}
				break;
			case 'W':    /* GPDB QE-backend startup info (motion listener, version). */
				{
					sendQEDetails();
					pq_flush();
				}
				break;

			case 'B':			/* bind */
				/* Set statement_timestamp() */
				SetCurrentStatementStartTimestamp();

				/*
				 *  in hawq, there is no distributed transaction
				 */
                /*setupRegularDtxContext();*/

				/*
				 * this message is complex enough that it seems best to put
				 * the field extraction out-of-line
				 */
				exec_bind_message(&input_message);
				break;

			case 'E':			/* execute */
				{
					const char *portal_name;
					int64			max_rows;

					/* Set statement_timestamp() */
					SetCurrentStatementStartTimestamp();

					portal_name = pq_getmsgstring(&input_message);

					 /*Get the max rows but cast to int64 internally. */
					max_rows = (int64)pq_getmsgint(&input_message, 4);
					pq_getmsgend(&input_message);

					elog((Debug_print_full_dtm ? LOG : DEBUG5), "Execute: %s.",portal_name);

					/*
					 *  in hawq, there is no distributed transaction
					 */
					/*setupRegularDtxContext();*/

					exec_execute_message(portal_name, max_rows);
				}
				break;

			case 'F':			/* fastpath function call */

                /* Set statement_timestamp() */
 				SetCurrentStatementStartTimestamp();

				/* Tell the collector what we're doing */
				pgstat_report_activity("<FASTPATH> function call");

				elog((Debug_print_full_dtm ? LOG : DEBUG5), "Fast path function call.");

				/*
				 *  in hawq, there is no distributed transaction
				 */
				/*setupRegularDtxContext();*/

				/* start an xact for this function invocation */
				start_xact_command();

				/*
				 * Note: we may at this point be inside an aborted
				 * transaction.  We can't throw error for that until we've
				 * finished reading the function-call message, so
				 * HandleFunctionRequest() must check for it after doing so.
				 * Be careful not to do anything that assumes we're inside a
				 * valid transaction here.
				 */

				/* switch back to message context */
				MemoryContextSwitchTo(MessageContext);

				if (HandleFunctionRequest(&input_message) == EOF)
				{
					/* lost frontend connection during F message input */

					/*
					 * Reset whereToSendOutput to prevent ereport from
					 * attempting to send any more messages to client.
					 */
					if (whereToSendOutput == DestRemote)
						whereToSendOutput = DestNone;

					proc_exit(0);
				}

				/* commit the function-invocation transaction */
				finish_xact_command();

				send_ready_for_query = true;
				break;

			case 'C':			/* close */
				{
					int			close_type;
					const char *close_target;

					close_type = pq_getmsgbyte(&input_message);
					close_target = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);

					switch (close_type)
					{
						case 'S':
							if (close_target[0] != '\0')
								DropPreparedStatement(close_target, false);
							else
							{
								/* special-case the unnamed statement */
								unnamed_stmt_pstmt = NULL;
								if (unnamed_stmt_context)
								{
									DropDependentPortals(unnamed_stmt_context);
									MemoryContextDelete(unnamed_stmt_context);
								}
								unnamed_stmt_context = NULL;
							}
							break;
						case 'P':
							{
								Portal		portal;

								portal = GetPortalByName(close_target);
								if (PortalIsValid(portal))
									PortalDrop(portal, false);
							}
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
								   errmsg("invalid CLOSE message subtype %d",
										  close_type)));
							break;
					}

					if (whereToSendOutput == DestRemote)
						pq_putemptymessage('3');		/* CloseComplete */
				}
				break;

			case 'D':			/* describe */
				{
					int			describe_type;
					const char *describe_target;

					/* Set statement_timestamp() (needed for xact) */
					SetCurrentStatementStartTimestamp();

					describe_type = pq_getmsgbyte(&input_message);
					describe_target = pq_getmsgstring(&input_message);
					pq_getmsgend(&input_message);

					elog((Debug_print_full_dtm ? LOG : DEBUG5), "Describe: %s.", describe_target);

					/*
					 *  in hawq, there is no distributed transaction
					 */
					/*setupRegularDtxContext();*/

					switch (describe_type)
					{
						case 'S':
							exec_describe_statement_message(describe_target);
							break;
						case 'P':
							exec_describe_portal_message(describe_target);
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
								errmsg("invalid DESCRIBE message subtype %d",
									   describe_type)));
							break;
					}
				}
				break;

			case 'H':			/* flush */
				pq_getmsgend(&input_message);
				if (whereToSendOutput == DestRemote)
					pq_flush();
				break;

			case 'S':			/* sync */
				pq_getmsgend(&input_message);
				finish_xact_command();
				send_ready_for_query = true;
				break;

				/*
				 * 'X' means that the frontend is closing down the socket. EOF
				 * means unexpected loss of frontend connection. Either way,
				 * perform normal shutdown.
				 */
			case 'X':
			case EOF:

				/*
				 * Reset whereToSendOutput to prevent ereport from attempting
				 * to send any more messages to client.
				 */
				if (whereToSendOutput == DestRemote)
					whereToSendOutput = DestNone;

				/*
				 * NOTE: if you are tempted to add more code here, DON'T!
				 * Whatever you had in mind to do should be set up as an
				 * on_proc_exit or on_shmem_exit callback, instead. Otherwise
				 * it will fail to be called during other backend-shutdown
				 * scenarios.
				 */
				proc_exit(0);

			case 'd':			/* copy data */
			case 'c':			/* copy done */
			case 'f':			/* copy fail */

				/*
				 * Accept but ignore these messages, per protocol spec; we
				 * probably got here because a COPY failed, and the frontend
				 * is still sending data.
				 */
				break;

			default:
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d ('%c')",
								firstchar,firstchar)));
		}
	}							/* end of input-reading loop */

	/* can't get here because the above loop never exits */
	Assert(false);

	return 1;					/* keep compiler quiet */
}


/*
 * Obtain platform stack depth limit (in bytes)
 *
 * Return -1 if unlimited or not known
 */
long
get_stack_depth_rlimit(void)
{
#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_STACK)
	static long val = 0;

	/* This won't change after process launch, so check just once */
	if (val == 0)
	{
		struct rlimit rlim;

		if (getrlimit(RLIMIT_STACK, &rlim) < 0)
			val = -1;
		else if (rlim.rlim_cur == RLIM_INFINITY)
			val = -1;
		else
			val = rlim.rlim_cur;
	}
	return val;
#else /* no getrlimit */
#if defined(WIN32) || defined(__CYGWIN__)
	/* On Windows we set the backend stack size in src/backend/Makefile */
	return WIN32_STACK_RLIMIT;
#else  /* not windows ... give up */
	return -1;
#endif
#endif
}


static struct rusage Save_r;
static struct timeval Save_t;

void
ResetUsage(void)
{
	getrusage(RUSAGE_SELF, &Save_r);
	gettimeofday(&Save_t, NULL);
	ResetBufferUsage();
	/* ResetTupleCount(); */
}

#ifdef  pg_on_solaris
#if defined(_LP64) || _FILE_OFFSET_BITS != 64
#include <procfs.h>
#endif 
#endif 

void
ShowUsage(const char *title)
{
	StringInfoData str;
	struct timeval user,
				sys;
	struct timeval elapse_t;
	struct rusage r;
	char	   *bufusage;

	getrusage(RUSAGE_SELF, &r);
	gettimeofday(&elapse_t, NULL);
	memcpy((char *) &user, (char *) &r.ru_utime, sizeof(user));
	memcpy((char *) &sys, (char *) &r.ru_stime, sizeof(sys));
	if (elapse_t.tv_usec < Save_t.tv_usec)
	{
		elapse_t.tv_sec--;
		elapse_t.tv_usec += 1000000;
	}
	if (r.ru_utime.tv_usec < Save_r.ru_utime.tv_usec)
	{
		r.ru_utime.tv_sec--;
		r.ru_utime.tv_usec += 1000000;
	}
	if (r.ru_stime.tv_usec < Save_r.ru_stime.tv_usec)
	{
		r.ru_stime.tv_sec--;
		r.ru_stime.tv_usec += 1000000;
	}

#ifdef  pg_on_solaris
#if defined(_LP64) || _FILE_OFFSET_BITS != 64
	{
		char pathname[100];
		int fd;
		psinfo_t psinfo;
		psinfo_t *psi = &psinfo;

		(void) sprintf(pathname, "/proc/%d/psinfo", (int)getpid());
		if ((fd = open(pathname, O_RDONLY)) >= 0)
		{
			if (read(fd, &psinfo, sizeof (psinfo)) == sizeof (psinfo))
			{
				uint_t value; /* need 32 bits to compute with */

				elog(LOG,"Process size:..............%ld KB",(long)psi->pr_size);
				elog(LOG,"Resident Set Size:.........%ld KB",(long)psi->pr_rssize);

				value = psi->pr_pctmem;
				value = ((value * 1000) + 0x7000) >> 15; /* [0 .. 1000] */
				elog(LOG,"Percent of memory:.........%3u.%u%%", value / 10, value % 10);

			}
		}

		(void) close(fd);

	}


#endif 
#endif 

	/*
	 * the only stats we don't show here are for memory usage -- i can't
	 * figure out how to interpret the relevant fields in the rusage struct,
	 * and they change names across o/s platforms, anyway. if you can figure
	 * out what the entries mean, you can somehow extract resident set size,
	 * shared text size, and unshared data and stack sizes.
	 */
	initStringInfo(&str);

	appendStringInfo(&str, "! system usage stats:\n");
	appendStringInfo(&str,
				"!\t%ld.%06ld elapsed %ld.%06ld user %ld.%06ld system sec\n",
					 (long) (elapse_t.tv_sec - Save_t.tv_sec),
					 (long) (elapse_t.tv_usec - Save_t.tv_usec),
					 (long) (r.ru_utime.tv_sec - Save_r.ru_utime.tv_sec),
					 (long) (r.ru_utime.tv_usec - Save_r.ru_utime.tv_usec),
					 (long) (r.ru_stime.tv_sec - Save_r.ru_stime.tv_sec),
					 (long) (r.ru_stime.tv_usec - Save_r.ru_stime.tv_usec));
	appendStringInfo(&str,
					 "!\t[%ld.%06ld user %ld.%06ld sys total]\n",
					 (long) user.tv_sec,
					 (long) user.tv_usec,
					 (long) sys.tv_sec,
					 (long) sys.tv_usec);
#if defined(HAVE_GETRUSAGE)
	appendStringInfo(&str,
					 "!\t%ld/%ld [%ld/%ld] filesystem blocks in/out\n",
					 r.ru_inblock - Save_r.ru_inblock,
	/* they only drink coffee at dec */
					 r.ru_oublock - Save_r.ru_oublock,
					 r.ru_inblock, r.ru_oublock);
	appendStringInfo(&str,
			  "!\t%ld/%ld [%ld/%ld] page faults/reclaims, %ld [%ld] swaps\n",
					 r.ru_majflt - Save_r.ru_majflt,
					 r.ru_minflt - Save_r.ru_minflt,
					 r.ru_majflt, r.ru_minflt,
					 r.ru_nswap - Save_r.ru_nswap,
					 r.ru_nswap);
	appendStringInfo(&str,
		 "!\t%ld [%ld] signals rcvd, %ld/%ld [%ld/%ld] messages rcvd/sent\n",
					 r.ru_nsignals - Save_r.ru_nsignals,
					 r.ru_nsignals,
					 r.ru_msgrcv - Save_r.ru_msgrcv,
					 r.ru_msgsnd - Save_r.ru_msgsnd,
					 r.ru_msgrcv, r.ru_msgsnd);
	appendStringInfo(&str,
			 "!\t%ld/%ld [%ld/%ld] voluntary/involuntary context switches\n",
					 r.ru_nvcsw - Save_r.ru_nvcsw,
					 r.ru_nivcsw - Save_r.ru_nivcsw,
					 r.ru_nvcsw, r.ru_nivcsw);
#endif   /* HAVE_GETRUSAGE */

	bufusage = ShowBufferUsage();
	appendStringInfo(&str, "! buffer usage stats:\n%s", bufusage);
	pfree(bufusage);

	/* remove trailing newline */
	if (str.data[str.len - 1] == '\n')
		str.data[--str.len] = '\0';

	ereport(LOG,
			(errmsg_internal("%s", title),
			 errdetail("%s", str.data),
			 errOmitLocation(true)));

	pfree(str.data);
}

/*
 * on_proc_exit handler to log end of session
 */
static void
log_disconnections(int code, Datum arg __attribute__((unused)))
{
	Port	   *port = MyProcPort;
	long		secs;
	int			usecs;
	int			msecs;
	int			hours,
				minutes,
				seconds;

	TimestampDifference(port->SessionStartTime,
						GetCurrentTimestamp(),
						&secs, &usecs);
	msecs = usecs / 1000;

	hours = secs / SECS_PER_HOUR;
	secs %= SECS_PER_HOUR;
	minutes = secs / SECS_PER_MINUTE;
	seconds = secs % SECS_PER_MINUTE;

	ereport(LOG,
			(errmsg("disconnection: session time: %d:%02d:%02d.%03d "
					"user=%s database=%s host=%s%s%s",
					hours, minutes, seconds, msecs,
					port->user_name, port->database_name, port->remote_host,
					port->remote_port[0] ? " port=" : "", port->remote_port),
					errOmitLocation(true)));
}

void OnMoveOutCGroupForQE(void)
{
	char errorbuf[ERRORMESSAGE_SIZE];
	if (Gp_role == GP_ROLE_EXECUTE)
	{
		if (isCGroupEnabled("cpu") && isCGroupSetup("cpu"))
		{
			MoveOutCGroupForQE(master_start_time,
							   gp_session_id,
							   0,
							   getpid(),
							   errorbuf,
							   sizeof(errorbuf));
		}
	}
}

int
SyncAgentMain(int argc, char *argv[], const char *username)
{
	int			flag;
	const char *dbname = NULL;
	char	   *userDoption = NULL;
	bool		secure;
	int			errs = 0;
	int			debug_flag = -1;		/* -1 means not given */
	List	   *guc_names = NIL;		/* for SUSET options */
	List	   *guc_values = NIL;
	GucContext	ctx;
	GucSource	gucsource;
	int			firstchar;
	char		stack_base;
	StringInfoData input_message;
	sigjmp_buf	local_sigjmp_buf;
	volatile bool send_rfq = true;
	bool		shutdownTooFarBehind = false;

#define PendingConfigOption(name,val) \
	(guc_names = lappend(guc_names, pstrdup(name)), \
	 guc_values = lappend(guc_values, pstrdup(val)))

	/*
	 * Catch standard options before doing much else.  This even works on
	 * systems without getopt_long.
	 */
	if (!IsUnderPostmaster && argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			printf("gpsyncmaster takes the same options as postgres\n");
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts(PG_VERSIONSTR);
			exit(0);
		}
	}

	/*
	 * initialize globals (already done if under postmaster, but not if
	 * standalone; cheap enough to do over)
	 */
	MyProcPid = getpid();

	/*
	 * Fire up essential subsystems: error and memory management
	 *
	 * If we are running under the postmaster, this is done already.
	 */
	if (!IsUnderPostmaster)
		MemoryContextInit();

	set_ps_display("startup", false);

	SetProcessingMode(InitProcessing);

	/* Set up reference point for stack depth checking */
	stack_base_ptr = &stack_base;

	/* Compute paths, if we didn't inherit them from postmaster */
	if (my_exec_path[0] == '\0')
	{
		if (find_my_exec(argv[0], my_exec_path) < 0)
			elog(FATAL, "%s: could not locate my own executable path",
				 argv[0]);
	}

	if (pkglib_path[0] == '\0')
		get_pkglib_path(my_exec_path, pkglib_path);

	/*
	 * Set default values for command-line options.
	 */
	EchoQuery = false;

	if (!IsUnderPostmaster)
		InitializeGUCOptions();

	/* ----------------
	 *	parse command line arguments
	 *
	 *	There are now two styles of command line layout for the backend:
	 *
	 *	For interactive use (not started from postmaster) the format is
	 *		postgres [switches] [databasename]
	 *	If the databasename is omitted it is taken to be the user name.
	 *
	 *	When started from the postmaster, the format is
	 *		postgres [secure switches] -p databasename [insecure switches]
	 *	Switches appearing after -p came from the client (via "options"
	 *	field of connection request).  For security reasons we restrict
	 *	what these switches can do.
	 * ----------------
	 */

	/* Ignore the initial --single argument, if present */
	if (argc > 1 && strcmp(argv[1], "--single") == 0)
	{
		argv++;
		argc--;
	}

	/* all options are allowed until '-p' */
	secure = true;
	ctx = PGC_POSTMASTER;
	gucsource = PGC_S_ARGV;		/* initial switches came from command line */

	while ((flag = getopt(argc, argv, "A:B:c:D:d:EeFf:h:ijk:lN:nOo:Pp:r:S:sTt:v:W:y:-:")) != -1)
	{
		switch (flag)
		{
			case 'A':
				SetConfigOption("debug_assertions", optarg, ctx, gucsource);
				break;

			case 'B':
				SetConfigOption("shared_buffers", optarg, ctx, gucsource);
				break;

			case 'D':
				if (secure)
					userDoption = optarg;
				break;

			case 'd':
				debug_flag = atoi(optarg);
				break;

			case 'E':
				EchoQuery = true;
				break;

			case 'e':
				SetConfigOption("datestyle", "euro", ctx, gucsource);
				break;

			case 'F':
				SetConfigOption("fsync", "false", ctx, gucsource);
				break;

			case 'f':
				if (!set_plan_disabling_options(optarg, ctx, gucsource))
					errs++;
				break;

			case 'h':
				SetConfigOption("listen_addresses", optarg, ctx, gucsource);
				break;

			case 'i':
				SetConfigOption("listen_addresses", "*", ctx, gucsource);
				break;

			case 'j':
				UseNewLine = 0;
				break;

			case 'k':
				SetConfigOption("unix_socket_directory", optarg, ctx, gucsource);
				break;

			case 'l':
				SetConfigOption("ssl", "true", ctx, gucsource);
				break;

			case 'N':
				SetConfigOption("max_connections", optarg, ctx, gucsource);
				break;

			case 'n':
				/* ignored for consistency with postmaster */
				break;

			case 'O':
				/* Only use in single user mode */
				SetConfigOption("allow_system_table_mods", "all", PGC_USERSET, gucsource);
				break;

			case 'o':
				errs++;
				break;

			case 'P':
				SetConfigOption("ignore_system_indexes", "true", ctx, gucsource);
				break;

			case 'p':
				SetConfigOption("port", optarg, ctx, gucsource);
				break;

			case 'r':
				/* send output (stdout and stderr) to the given file */
				if (secure)
					StrNCpy(OutputFileName, optarg, MAXPGPATH);
				break;

			case 'S':
				SetConfigOption("work_mem", optarg, ctx, gucsource);
				break;

			case 's':

				/*
				 * Since log options are SUSET, we need to postpone unless
				 * still in secure context
				 */
				if (ctx == PGC_BACKEND)
					PendingConfigOption("log_statement_stats", "true");
				else
					SetConfigOption("log_statement_stats", "true",
									ctx, gucsource);
				break;

			case 'T':
				/* ignored for consistency with postmaster */
				break;

			case 't':
				{
					const char *tmp = get_stats_option_name(optarg);

					if (tmp)
					{
						if (ctx == PGC_BACKEND)
							PendingConfigOption(tmp, "true");
						else
							SetConfigOption(tmp, "true", ctx, gucsource);
					}
					else
						errs++;
					break;
				}

			case 'v':
				if (secure)
					FrontendProtocol = (ProtocolVersion) atoi(optarg);
				break;

			case 'W':
				SetConfigOption("post_auth_delay", optarg, ctx, gucsource);
				break;


			case 'y':

				/*
				 * y - special flag passed if backend was forked by a
				 * postmaster.
				 */
				if (secure)
				{
					dbname = strdup(optarg);

					secure = false;		/* subsequent switches are NOT secure */
					ctx = PGC_BACKEND;
					gucsource = PGC_S_CLIENT;
				}
				break;

			case 'c':
			case '-':
				{
					char	   *name,
							   *value;

					ParseLongOption(optarg, &name, &value);
					if (!value)
					{
						if (flag == '-')
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("--%s requires a value",
											optarg)));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("-c %s requires a value",
											optarg)));
					}

					/*
					 * If a SUSET option, must postpone evaluation, unless we
					 * are still reading secure switches.
					 */
					if (ctx == PGC_BACKEND && IsSuperuserConfigOption(name))
						PendingConfigOption(name, value);
					else
						SetConfigOption(name, value, ctx, gucsource);
					free(name);
					if (value)
						free(value);
					break;
				}

			default:
				errs++;
				break;
		}
	}

	/*
	 * Process any additional GUC variable settings passed in startup
	 * packet.  These are handled exactly like command-line variables.
	 */
	if (MyProcPort != NULL)
	{
		ListCell   *gucopts = list_head(MyProcPort->guc_options);

		while (gucopts)
		{
			char	   *name;
			char	   *value;

			name = lfirst(gucopts);
			gucopts = lnext(gucopts);

			value = lfirst(gucopts);
			gucopts = lnext(gucopts);

			if (IsSuperuserConfigOption(name))
				PendingConfigOption(name, value);
			else
				SetConfigOption(name, value, PGC_BACKEND, PGC_S_CLIENT);
		}
	}

	/* Acquire configuration parameters, unless inherited from postmaster */
	if (!IsUnderPostmaster)
	{
		if (!SelectConfigFiles(userDoption, argv[0]))
			proc_exit(1);
	}

	/*
	 * Set up signal handlers and masks.
	 *
	 * Note that postmaster blocked all signals before forking child process,
	 * so there is no race condition whereby we might receive a signal
	 * before we have set up the handler.
	 *
	 * Also note: it's best not to use any signals that are SIG_IGNored in
	 * the postmaster.	If such a signal arrives before we are able to
	 * change the handler to non-SIG_IGN, it'll get dropped.  Instead,
	 * make a dummy handler in the postmaster to reserve the signal. (Of
	 * course, this isn't an issue for signals that are locally generated,
	 * such as SIGALRM and SIGPIPE.)
	 */
	pqsignal(SIGHUP, SigHupHandler);	/* set flag to read config file */
	pqsignal(SIGINT, StatementCancelHandler);	/* cancel current query */
	pqsignal(SIGTERM, die);		/* cancel current query and exit */
	pqsignal(SIGQUIT, quickdie);	/* hard crash time */
	pqsignal(SIGALRM, handle_sig_alarm);		/* timeout conditions */

	/*
	 * Ignore failure to write to frontend. Note: if frontend closes
	 * connection, we will notice it and exit cleanly when control next
	 * returns to outer loop.  This seems safer than forcing exit in the
	 * midst of output during who-knows-what operation...
	 */
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, NotifyInterruptHandler);
	pqsignal(SIGFPE, FloatExceptionHandler);

	/*
	 * Reset some signals that are accepted by postmaster but not by
	 * backend
	 */
	pqsignal(SIGCHLD, SIG_DFL); /* system() requires this on some
								 * platforms */

	pqinitmask();

	/* We allow SIGQUIT (quickdie) at all times */
#ifdef HAVE_SIGPROCMASK
	sigdelset(&BlockSig, SIGQUIT);
#else
	BlockSig &= ~(sigmask(SIGQUIT));
#endif

	PG_SETMASK(&BlockSig);		/* block everything except SIGQUIT */

	// UNDONE: To allow us to perform recovery.
	SetProcessingMode(BootstrapProcessing);

	if (IsUnderPostmaster)
	{
		/* noninteractive case: nothing should be left after switches */
		if (errs || argc != optind || dbname == NULL)
		{
			ereport(FATAL,
					(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("invalid command-line arguments for server process"),
			errhint("Try \"%s --help\" for more information.", argv[0])));
		}


		BaseInit();
	}
	else
	{
		/* interactive case: database name can be last arg on command line */
		if (errs || argc - optind > 1)
		{
			ereport(FATAL,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("%s: invalid command-line arguments",
							argv[0]),
			errhint("Try \"%s --help\" for more information.", argv[0])));
		}
		else if (argc - optind == 1)
			dbname = argv[optind];
		else if ((dbname = username) == NULL)
		{
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s: no database nor user name specified",
							argv[0])));
		}

		/*
		 * Validate we have been given a reasonable-looking DataDir (if
		 * under postmaster, assume postmaster did this already).
		 */
		Assert(DataDir);
		ValidatePgVersion(DataDir);

		/*
		 * Create lockfile for data directory.
		 */
		CreateDataDirLockFile(false);

		BaseInit();

		/*
		 * Start up xlog for standalone backend, and register to have it
		 * closed down at exit.
		 */
		StartupXLOG();
		on_shmem_exit(ShutdownXLOG, 0);

		/*
		 * Read any existing FSM cache file, and register to write one out
		 * at exit.
		 */
		LoadFreeSpaceMap();
		on_shmem_exit(DumpFreeSpaceMap, 0);
	}

	/* Syncmaster doesn't need to InitPostgres() */

	/*
	 * The syncmaster process needs access to synchronization primitives
	 * which require that Process structures be configured and assigned.
	 */
	InitProcess();

	SetProcessingMode(NormalProcessing);

	/*
	 * Now that we know if client is a superuser, we can try to apply SUSET
	 * GUC options that came from the client.
	 */
	ctx = PGC_SUSET;

	if (debug_flag >= 0)
		set_debug_options(debug_flag, ctx, PGC_S_CLIENT);

	if (guc_names != NIL)
	{
		ListCell   *namcell,
				   *valcell;

		forboth(namcell, guc_names, valcell, guc_values)
		{
			char	   *name = (char *) lfirst(namcell);
			char	   *value = (char *) lfirst(valcell);

			SetConfigOption(name, value, ctx, PGC_S_CLIENT);
			pfree(name);
			pfree(value);
		}
	}

	/*
	 * Also set up handler to log session end; we have to wait till now
	 * to be sure Log_disconnections has its final value.
	 */
	if (IsUnderPostmaster && Log_disconnections)
		on_proc_exit(log_disconnections, 0);

	/*
	 * Send this backend's cancellation info to the frontend.
	 */
	if (whereToSendOutput == DestRemote &&
		PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2)
	{
		StringInfoData buf;

		pq_beginmessage(&buf, 'K');
		pq_sendint(&buf, (int32) MyProcPid, sizeof(int32));
		pq_sendint(&buf, (int32) MyCancelKey, sizeof(int32));				
		pq_endmessage(&buf);
		/* Need not flush since ReadyForQuery will do it. */
	}

	/* Welcome banner for standalone case */
	if (whereToSendOutput == DestDebug)
		printf("\nPostgreSQL stand-alone backend %s\n", PG_VERSION);

	/*
	 * POSTGRES main processing loop begins here
	 *
	 * If an exception is encountered, processing resumes here so we abort
	 * the current transaction and start a new one.
	 *
	 * You might wonder why this isn't coded as an infinite loop around a
	 * PG_TRY construct.  The reason is that this is the bottom of the
	 * exception stack, and so with PG_TRY there would be no exception
	 * handler in force at all during the CATCH part.  By leaving the
	 * outermost setjmp always active, we have at least some chance of
	 * recovering from an error during error recovery.	(If we get into an
	 * infinite loop thereby, it will soon be stopped by overflow of
	 * elog.c's internal state stack.)
	 */

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/*
		 * NOTE: if you are tempted to add more code in this if-block,
		 * consider the high probability that it should be in
		 * AbortTransaction() instead.	The only stuff done directly here
		 * should be stuff that is guaranteed to apply *only* for
		 * outer-level error recovery, such as adjusting the FE/BE
		 * protocol status.
		 */

		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/*
		 * Forget any pending QueryCancel request, since we're returning
		 * to the idle loop anyway, and cancel the statement timer if
		 * running.
		 */
		QueryCancelPending = false;
		disable_sig_alarm(true);
		QueryCancelPending = false;		/* again in case timeout occurred */

		/*
		 * Turn off these interrupts too.  This is only needed here and
		 * not in other exception-catching places since these interrupts
		 * are only enabled while we wait for client input.
		 */
		DisableNotifyInterrupt();
		DisableCatchupInterrupt();

		/* Make sure libpq is in a good state */
		pq_comm_reset();

		/* Report the error to the client and/or server log */
		EmitErrorReport();

		/*
		 * Make sure debug_query_string gets reset before we possibly
		 * clobber the storage it points at.
		 */
		debug_query_string = NULL;

		FlushErrorState();
		QueryContext = NULL;

		/*
		 * If we were handling an extended-query-protocol message,
		 * initiate skip till next Sync.  This also causes us not to issue
		 * ReadyForQuery (until we get Sync).
		 */
		if (doing_extended_query_message)
			ignore_till_sync = true;

		/* We don't have a transaction command open anymore */
		xact_started = false;

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	PG_SETMASK(&UnBlockSig);

	if (!ignore_till_sync)
		send_rfq = true;		/* initially, or after error */

	/*
	 * Non-error queries loop here.
	 */

	for (;;)
	{
		/*
		 * At top of loop, reset extended-query-message flag, so that any
		 * errors encountered in "idle" state don't provoke skip.
		 */
		doing_extended_query_message = false;

		initStringInfo(&input_message);

		/*
		 * (1) If we've reached idle state, tell the frontend we're ready
		 * for a new query.
		 *
		 * Note: this includes fflush()'ing the last of the prior output.
		 *
		 * This is also a good time to send collected statistics to the
		 * collector, and to update the PS stats display.  We avoid doing
		 * those every time through the message loop because it'd slow
		 * down processing of batched messages, and because we don't want
		 * to report uncommitted updates (that confuses autovacuum).
		 */
		if (send_rfq)
		{
			set_ps_display("idle", false);

			ReadyForQuery(whereToSendOutput);
			send_rfq = false;
		}

		/*
		 * (2b)
		 * If we need to shutdown because we are too far behind,
		 * do it here...
		 */
		if (shutdownTooFarBehind)
			cdb_shutdown_too_far_behind();

		/*
		 * (2) deal with pending asynchronous NOTIFY from other backends,
		 * and enable async.c's signal handler to execute NOTIFY directly.
		 * Then set up other stuff needed before blocking for input.
		 */
		QueryCancelPending = false;		/* forget any earlier CANCEL
										 * signal */

		EnableNotifyInterrupt();
		EnableCatchupInterrupt();

#ifdef USE_TEST_UTILS
		/* reset time slice */
		TimeSliceReset();
#endif /* USE_TEST_UTILS */

		/* Allow "die" interrupt to be processed while waiting */
		ImmediateInterruptOK = true;
		/* and don't forget to detect one that already arrived */
		QueryCancelPending = false;
		CHECK_FOR_INTERRUPTS();

		/*
		 * (3) read a command (loop blocks here)
		 */
		firstchar = ReadCommand(&input_message);

		/*
		 * (4) disable async signal conditions again.
		 */
		ImmediateInterruptOK = false;
		QueryCancelPending = false;		/* forget any CANCEL signal */

		DisableNotifyInterrupt();
		DisableCatchupInterrupt();

		/*
		 * (5) check for any other interesting events that happened while
		 * we slept.
		 */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * (6) process the command.  But ignore it if we're skipping till
		 * Sync.
		 */
		if (ignore_till_sync && firstchar != EOF)
			continue;

		switch (firstchar)
		{
			case 'G':			/* Gang management */
				{
					insist_log(false, "cannot handle segworker group management commands");
				}
				break;
            case 'Q':           /* simple query */
                {
                    const char *query_string;

                    query_string = pq_getmsgstring(&input_message);
                    pq_getmsgend(&input_message);

                    shutdownTooFarBehind = cdb_sync_command(query_string);

                    send_rfq = true;
                    pq_getmsgend(&input_message);
                }
                break;

            case 'X':
            case EOF:

                /* No active snapshot any more either */
                ActiveSnapshot = NULL;

                /* No active snapshot any more either */
                ActiveSnapshot = NULL;

                /*
                 * Reset whereToSendOutput to prevent ereport from
                 * attempting to send any more messages to client.
                 */
                if (whereToSendOutput == DestRemote)
                    whereToSendOutput = DestNone;

                /*
                 * NOTE: if you are tempted to add more code here, DON'T!
                 * Whatever you had in mind to do should be set up as an
                 * on_proc_exit or on_shmem_exit callback, instead.
                 * Otherwise it will fail to be called during other
                 * backend-shutdown scenarios.
                 */
                proc_exit(0);

            default:
                ereport(FATAL,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                         errmsg("invalid frontend message type %d",
                                firstchar)));
        }
	}							/* end of input-reading loop */

	/* can't get here because the above loop never exits */
	Assert(false);

	return 1;					/* keep compiler quiet */
}

