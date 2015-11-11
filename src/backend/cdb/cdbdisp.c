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

/*-------------------------------------------------------------------------
 *
 * cdbdisp.c
 *	  Functions to dispatch commands to QExecutors.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <pthread.h>
#include <limits.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "catalog/catquery.h"
#include "executor/execdesc.h"	/* QueryDesc */
#include "storage/ipc.h"		/* For proc_exit_inprogress  */
#include "miscadmin.h"
#include "utils/memutils.h"

#include "utils/tqual.h" 			/*for the snapshot */
#include "storage/proc.h"  			/* MyProc */
#include "storage/procarray.h"      /* updateSharedLocalSnapshot */
#include "access/xact.h"  			/*for GetCurrentTransactionId */


#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/faultinjector.h"
#include "executor/executor.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "tcop/tcopprot.h"
#include "cdb/cdbplan.h"
#include "postmaster/syslogger.h"

#include "cdb/cdbselect.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbgang.h"
#include "cdb/cdblink.h"		/* just for our CdbProcess population hack. */
#include "cdb/cdbsrlz.h"
#include "cdb/cdbsubplan.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbllize.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbrelsize.h"
#include "gp-libpq-fe.h"
#include "libpq/libpq-be.h"
#include "commands/vacuum.h" /* VUpdatedStats */
#include "cdb/cdbanalyze.h"  /* cdbanalyze_get_columnstats */

#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/portal.h"

#include "cdb/cdbinmemheapam.h"

extern bool Test_print_direct_dispatch_info;

extern pthread_t main_tid;
#ifndef _WIN32
#define mythread() ((unsigned long) pthread_self())
#else
#define mythread() ((unsigned long) pthread_self().p)
#endif 

static void
bindCurrentOfParams(char *cursor_name, 
					Oid target_relid, 
					ItemPointer ctid, 
					int *gp_segment_id, 
					Oid *tableoid);


#define GP_PARTITION_SELECTION_OID 6084
#define GP_PARTITION_EXPANSION_OID 6085
#define GP_PARTITION_INVERSE_OID 6086



/*
 * We need an array describing the relationship between a slice and
 * the number of "child" slices which depend on it.
 */
typedef struct {
	int sliceIndex;
	int children;
	Slice *slice;
} sliceVec;

/* determines which dispatchOptions need to be set. */
/*static int generateTxnOptions(bool needTwoPhase);*/

typedef struct
{
	plan_tree_base_prefix base; /* Required prefix for plan_tree_walker/mutator */
	bool		single_row_insert;
}	pre_dispatch_function_evaluation_context;

static Node *pre_dispatch_function_evaluation_mutator(Node *node,
						 pre_dispatch_function_evaluation_context * context);

/*
 * We can't use elog to write to the log if we are running in a thread.
 * 
 * So, write some thread-safe routines to write to the log.
 * 
 * Ugly:  This write in a fixed format, and ignore what the log_prefix guc says.
 */
static pthread_mutex_t send_mutex = PTHREAD_MUTEX_INITIALIZER;

#ifdef WIN32
static void
write_eventlog(int level, const char *line);
/*
 * Write a message line to the windows event log
 */
static void
write_eventlog(int level, const char *line)
{
	int			eventlevel = EVENTLOG_ERROR_TYPE;
	static HANDLE evtHandle = INVALID_HANDLE_VALUE;

	if (evtHandle == INVALID_HANDLE_VALUE)
	{
		evtHandle = RegisterEventSource(NULL, "PostgreSQL");
		if (evtHandle == NULL)
		{
			evtHandle = INVALID_HANDLE_VALUE;
			return;
		}
	}

	ReportEvent(evtHandle,
				eventlevel,
				0,
				0,				/* All events are Id 0 */
				NULL,
				1,
				0,
				&line,
				NULL);
}
#endif   /* WIN32 */

void get_timestamp(char * strfbuf, int length)
{
	pg_time_t		stamp_time;
	char			msbuf[8];
	struct timeval tv;

	gettimeofday(&tv, NULL);
	stamp_time = tv.tv_sec;

	pg_strftime(strfbuf, length,
	/* leave room for microseconds... */
	/* Win32 timezone names are too long so don't print them */
#ifndef WIN32
			 "%Y-%m-%d %H:%M:%S        %Z",
#else
			 "%Y-%m-%d %H:%M:%S        ",
#endif
			 pg_localtime(&stamp_time, log_timezone ? log_timezone : gmt_timezone));

	/* 'paste' milliseconds into place... */
	sprintf(msbuf, ".%06d", (int) (tv.tv_usec));
	strncpy(strfbuf + 19, msbuf, 7);	
}

void
write_log(const char *fmt,...)
{
	char logprefix[1024];
	char tempbuf[25];
	va_list		ap;

	fmt = _(fmt);

	va_start(ap, fmt);

	if (Redirect_stderr && gp_log_format == 1)
	{
		char		errbuf[2048]; /* Arbitrary size? */
		
		vsnprintf(errbuf, sizeof(errbuf), fmt, ap);

		/* Write the message in the CSV format */
		write_message_to_server_log(LOG,
									0,
									errbuf,
									NULL,
									NULL,
									NULL,
									0,
									0,
									NULL,
									NULL,
									NULL,
									false,
									NULL,
									0,
									0,
									true,
									/* This is a real hack... We want to send alerts on these errors, but we aren't using ereport() */
									strstr(errbuf, "Master unable to connect") != NULL ||
									strstr(errbuf, "Found a fault with a segment") != NULL,
									NULL,
									false);

		va_end(ap);
		return;
	}
	
	get_timestamp(logprefix, sizeof(logprefix));
	strcat(logprefix,"|");
	if (MyProcPort)
	{
		const char *username = MyProcPort->user_name;
		if (username == NULL || *username == '\0')
			username = "";
		strcat(logprefix,username); /* user */
	}
	
	strcat(logprefix,"|");
	if (MyProcPort)
	{
		const char *dbname = MyProcPort->database_name;

		if (dbname == NULL || *dbname == '\0')
			dbname = "";
		strcat(logprefix, dbname);
	}
	strcat(logprefix,"|");
	sprintf(tempbuf,"%d",MyProcPid);
	strcat(logprefix,tempbuf); /* pid */
	strcat(logprefix,"|");
	sprintf(tempbuf,"con%d cmd%d",gp_session_id,gp_command_count);
	strcat(logprefix,tempbuf);

	strcat(logprefix,"|");
	strcat(logprefix,":-THREAD ");
	if (pthread_equal(main_tid, pthread_self()))
		strcat(logprefix,"MAIN");
	else
	{
		sprintf(tempbuf,"%lu",mythread());
		strcat(logprefix,tempbuf);
	}
	strcat(logprefix,":  ");
	
	strcat(logprefix,fmt);
	
	if (fmt[strlen(fmt)-1]!='\n')
		strcat(logprefix,"\n");
	 
	/*
	 * We don't trust that vfprintf won't get confused if it 
	 * is being run by two threads at the same time, which could
	 * cause interleaved messages.  Let's play it safe, and
	 * make sure only one thread is doing this at a time.
	 */
	pthread_mutex_lock(&send_mutex);
#ifndef WIN32
	/* On Unix, we just fprintf to stderr */
	vfprintf(stderr, logprefix, ap);
    fflush(stderr);
#else

	/*
	 * On Win32, we print to stderr if running on a console, or write to
	 * eventlog if running as a service
	 */
	if (pgwin32_is_service())	/* Running as a service */
	{
		char		errbuf[2048];		/* Arbitrary size? */

		vsnprintf(errbuf, sizeof(errbuf), logprefix, ap);

		write_eventlog(EVENTLOG_ERROR_TYPE, errbuf);
	}
	else
    {
        /* Not running as service, write to stderr */
        vfprintf(stderr, logprefix, ap);
        fflush(stderr);
    }
#endif
	pthread_mutex_unlock(&send_mutex);
	va_end(ap);
}

/*--------------------------------------------------------------------*/

/*
 * I refactored this code out of the two routines
 *    cdbdisp_dispatchRMCommand  and  cdbdisp_dispatchDtxProtocolCommand
 * when I thought I might need it in a third place.
 * 
 * Not sure if this makes things cleaner or not
 */
struct pg_result **
cdbdisp_returnResults(int segmentNum,
					  CdbDispatchResults *primaryResults,
					  StringInfo errmsgbuf,
					  int *numresults)
{
	CdbDispatchResults *gangResults;
	CdbDispatchResult *dispatchResult;
	PGresult  **resultSets = NULL;
	int			nslots;
	int			nresults = 0;
	int			i;
	int			totalResultCount=0;

	/*
	 * Allocate result set ptr array. Make room for one PGresult ptr per
	 * primary segment db, plus a null terminator slot after the
	 * last entry. The caller must PQclear() each PGresult and free() the
	 * array.
	 */
	nslots = 2 * segmentNum + 1;
	resultSets = (struct pg_result **)calloc(nslots, sizeof(*resultSets));

	if (!resultSets)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("cdbdisp_returnResults failed: out of memory")));

	/* Collect results from primary gang. */
	gangResults = primaryResults;
	if (gangResults)
	{
		totalResultCount = gangResults->resultCount;

		for (i = 0; i < gangResults->resultCount; ++i)
		{
			dispatchResult = &gangResults->resultArray[i];

			/* Append error messages to caller's buffer. */
			cdbdisp_dumpDispatchResult(dispatchResult, false, errmsgbuf);

			/* Take ownership of this QE's PGresult object(s). */
			nresults += cdbdisp_snatchPGresults(dispatchResult,
												resultSets + nresults,
												nslots - nresults - 1);
		}
		cdbdisp_destroyDispatchResults(gangResults);
	}

	/* Put a stopper at the end of the array. */
	Assert(nresults < nslots);
	resultSets[nresults] = NULL;

	/* If our caller is interested, tell them how many sets we're returning. */
	if (numresults != NULL)
		*numresults = totalResultCount;

	return resultSets;
}

/*
 * Let's evaluate all STABLE functions that have constant args before dispatch, so we get a consistent
 * view across QEs
 *
 * Also, if this is a single_row insert, let's evaluate nextval() and currval() before dispatching
 *
 */

static Node *
pre_dispatch_function_evaluation_mutator(Node *node,
										 pre_dispatch_function_evaluation_context * context)
{
	Node * new_node = 0;
	
	if (node == NULL)
		return NULL;

	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		/* Not replaceable, so just copy the Param (no need to recurse) */
		return (Node *) copyObject(param);
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;
		List	   *args;
		ListCell   *arg;
		Expr	   *simple;
		FuncExpr   *newexpr;
		bool		has_nonconst_input;

		Form_pg_proc funcform;
		EState	   *estate;
		ExprState  *exprstate;
		MemoryContext oldcontext;
		Datum		const_val;
		bool		const_is_null;
		int16		resultTypLen;
		bool		resultTypByVal;

		Oid			funcid;
		HeapTuple	func_tuple;


		/*
		 * Reduce constants in the FuncExpr's arguments.  We know args is
		 * either NIL or a List node, so we can call expression_tree_mutator
		 * directly rather than recursing to self.
		 */
		args = (List *) expression_tree_mutator((Node *) expr->args,
												pre_dispatch_function_evaluation_mutator,
												(void *) context);
										
		funcid = expr->funcid;

		newexpr = makeNode(FuncExpr);
		newexpr->funcid = expr->funcid;
		newexpr->funcresulttype = expr->funcresulttype;
		newexpr->funcretset = expr->funcretset;
		newexpr->funcformat = expr->funcformat;
		newexpr->args = args;

		/*
		 * Check for constant inputs
		 */
		has_nonconst_input = false;
		
		foreach(arg, args)
		{
			if (!IsA(lfirst(arg), Const))
			{
				has_nonconst_input = true;
				break;
			}
		}
		
		if (!has_nonconst_input)
		{
			bool is_seq_func = false;
			bool tup_or_set;
			cqContext	*pcqCtx;

			pcqCtx = caql_beginscan(
					NULL,
					cql("SELECT * FROM pg_proc "
						" WHERE oid = :1 ",
						ObjectIdGetDatum(funcid)));

			func_tuple = caql_getnext(pcqCtx);

			if (!HeapTupleIsValid(func_tuple))
				elog(ERROR, "cache lookup failed for function %u", funcid);

			funcform = (Form_pg_proc) GETSTRUCT(func_tuple);

			/* can't handle set returning or row returning functions */
			tup_or_set = (funcform->proretset || 
						  type_is_rowtype(funcform->prorettype));

			caql_endscan(pcqCtx);
			
			/* can't handle it */
			if (tup_or_set)
			{
				/* 
				 * We haven't mutated this node, but we still return the
				 * mutated arguments.
				 *
				 * If we don't do this, we'll miss out on transforming function
				 * arguments which are themselves functions we need to mutated.
				 * For example, select foo(now()).
				 *
				 * See MPP-3022 for what happened when we didn't do this.
				 */
				return (Node *)newexpr;
			}

			/* 
			 * Ignored evaluation of gp_partition stable functions.
			 * TODO: garcic12 - May 30, 2013, refactor gp_partition stable functions to be truly
			 * stable (JIRA: MPP-19541).
			 */
			if (funcid == GP_PARTITION_SELECTION_OID 
				|| funcid == GP_PARTITION_EXPANSION_OID 
				|| funcid == GP_PARTITION_INVERSE_OID)
			{
				return (Node *)newexpr;
			}

			/* 
			 * Related to MPP-1429.  Here we want to mark any statement that is
			 * going to use a sequence as dirty.  Doing this means that the
			 * QD will flush the xlog which will also flush any xlog writes that
			 * the sequence server might do. 
			 */
			if (funcid == NEXTVAL_FUNC_OID || funcid == CURRVAL_FUNC_OID ||
				funcid == SETVAL_FUNC_OID)
			{
				ExecutorMarkTransactionUsesSequences();
				is_seq_func = true;
			}

			if (funcform->provolatile == PROVOLATILE_IMMUTABLE)
				/* okay */ ;
			else if (funcform->provolatile == PROVOLATILE_STABLE)
				/* okay */ ;
			else if (context->single_row_insert && is_seq_func)
				;				/* Volatile, but special sequence function */
			else
				return (Node *)newexpr;

			/*
			 * Ok, we have a function that is STABLE (or IMMUTABLE), with
			 * constant args. Let's try to evaluate it.
			 */

			/*
			 * To use the executor, we need an EState.
			 */
			estate = CreateExecutorState();

			/* We can use the estate's working context to avoid memory leaks. */
			oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

			/*
			 * Prepare expr for execution.
			 */
			exprstate = ExecPrepareExpr((Expr *) newexpr, estate);

			/*
			 * And evaluate it.
			 *
			 * It is OK to use a default econtext because none of the
			 * ExecEvalExpr() code used in this situation will use econtext.
			 * That might seem fortuitous, but it's not so unreasonable --- a
			 * constant expression does not depend on context, by definition,
			 * n'est-ce pas?
			 */
			const_val =
				ExecEvalExprSwitchContext(exprstate,
										  GetPerTupleExprContext(estate),
										  &const_is_null, NULL);

			/* Get info needed about result datatype */
			get_typlenbyval(expr->funcresulttype, &resultTypLen, &resultTypByVal);

			/* Get back to outer memory context */
			MemoryContextSwitchTo(oldcontext);

			/* Must copy result out of sub-context used by expression eval */
			if (!const_is_null)
				const_val = datumCopy(const_val, resultTypByVal, resultTypLen);

			/* Release all the junk we just created */
			FreeExecutorState(estate);

			/*
			 * Make the constant result node.
			 */
			simple = (Expr *) makeConst(expr->funcresulttype, -1, resultTypLen,
										const_val, const_is_null,
										resultTypByVal);

			if (simple)			/* successfully simplified it */
				return (Node *) simple;
		}

		/*
		 * The expression cannot be simplified any further, so build and
		 * return a replacement FuncExpr node using the possibly-simplified
		 * arguments.
		 */
		return (Node *) newexpr;
	}
	else if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;
		List	   *args;

		OpExpr	   *newexpr;

		/*
		 * Reduce constants in the OpExpr's arguments.  We know args is either
		 * NIL or a List node, so we can call expression_tree_mutator directly
		 * rather than recursing to self.
		 */
		args = (List *) expression_tree_mutator((Node *) expr->args,
												pre_dispatch_function_evaluation_mutator,
												(void *) context);

		/*
		 * Need to get OID of underlying function.	Okay to scribble on input
		 * to this extent.
		 */
		set_opfuncid(expr);

		newexpr = makeNode(OpExpr);
		newexpr->opno = expr->opno;
		newexpr->opfuncid = expr->opfuncid;
		newexpr->opresulttype = expr->opresulttype;
		newexpr->opretset = expr->opretset;
		newexpr->args = args;

		return (Node *) newexpr;
	}
	else if (IsA(node, CurrentOfExpr))
	{
		/*
		 * updatable cursors 
		 *
		 * During constant folding, the CurrentOfExpr's gp_segment_id, ctid, 
		 * and tableoid fields are filled in with observed values from the 
		 * referenced cursor. For more detail, see bindCurrentOfParams below.
		 */
		CurrentOfExpr *expr = (CurrentOfExpr *) node,
					  *newexpr = copyObject(expr);

		bindCurrentOfParams(newexpr->cursor_name,
							newexpr->target_relid,
			   		   		&newexpr->ctid,
					  	   	&newexpr->gp_segment_id,
					  	   	&newexpr->tableoid);
		return (Node *) newexpr;
	}
	
	/*
	 * For any node type not handled above, we recurse using
	 * plan_tree_mutator, which will copy the node unchanged but try to
	 * simplify its arguments (if any) using this routine.
	 */
	new_node =  plan_tree_mutator(node, pre_dispatch_function_evaluation_mutator,
								  (void *) context);

	return new_node;
}

/*
 * bindCurrentOfParams
 *
 * During constant folding, we evaluate STABLE functions to give QEs a consistent view
 * of the query. At this stage, we will also bind observed values of 
 * gp_segment_id/ctid/tableoid into the CurrentOfExpr.
 * This binding must happen only after planning, otherwise we disrupt prepared statements.
 * Furthermore, this binding must occur before dispatch, because a QE lacks the 
 * the information needed to discern whether it's responsible for the currently 
 * positioned tuple.
 *
 * The design of this parameter binding is very tightly bound to the parse/analyze
 * and subsequent planning of DECLARE CURSOR. We depend on the "is_simply_updatable"
 * calculation of parse/analyze to decide whether CURRENT OF makes sense for the
 * referenced cursor. Moreover, we depend on the ensuing planning of DECLARE CURSOR
 * to provide the junk metadata of gp_segment_id/ctid/tableoid (per tuple).
 *
 * This function will lookup the portal given by "cursor_name". If it's simply updatable,
 * we'll glean gp_segment_id/ctid/tableoid from the portal's most recently fetched 
 * (raw) tuple. We bind this information into the CurrentOfExpr to precisely identify
 * the currently scanned tuple, ultimately for consumption of TidScan/execQual by the QEs.
 */
static void
bindCurrentOfParams(char *cursor_name, Oid target_relid, ItemPointer ctid, int *gp_segment_id, Oid *tableoid)
{
	char 			*table_name;
	Portal			portal;
	QueryDesc		*queryDesc;
	bool			found_attribute, isnull;
	Datum			value;

	portal = GetPortalByName(cursor_name);
	if (!PortalIsValid(portal))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" does not exist", cursor_name)));

	queryDesc = PortalGetQueryDesc(portal);
	if (queryDesc == NULL)
		ereport(ERROR, 
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is held from a previous transaction", cursor_name)));

	/* obtain table_name for potential error messages */
	table_name = get_rel_name(target_relid);

	/* 
	 * The referenced cursor must be simply updatable. This has already
	 * been discerned by parse/analyze for the DECLARE CURSOR of the given
	 * cursor. This flag assures us that gp_segment_id, ctid, and tableoid (if necessary)
 	 * will be available as junk metadata, courtesy of preprocess_targetlist.
	 */
	if (!portal->is_simply_updatable)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is not a simply updatable scan of table \"%s\"",
						cursor_name, table_name)));

	/* 
	 * The target relation must directly match the cursor's relation. This throws out
	 * the simple case in which a cursor is declared against table X and the update is
	 * issued against Y. Moreover, this disallows some subtler inheritance cases where
	 * Y inherits from X. While such cases could be implemented, it seems wiser to
	 * simply error out cleanly.
	 */
	Index varno = extractSimplyUpdatableRTEIndex(queryDesc->plannedstmt->rtable);
	Oid cursor_relid = getrelid(varno, queryDesc->plannedstmt->rtable);
	if (target_relid != cursor_relid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is not a simply updatable scan of table \"%s\"",
						cursor_name, table_name)));
	/* 
	 * The cursor must have a current result row: per the SQL spec, it's 
	 * an error if not.
	 */
	if (portal->atStart || portal->atEnd)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is not positioned on a row", cursor_name)));

	/*
	 * As mentioned above, if parse/analyze recognized this cursor as simply
	 * updatable during DECLARE CURSOR, then its subsequent planning must have
	 * made gp_segment_id, ctid, and tableoid available as junk for each tuple.
	 *
	 * To retrieve this junk metadeta, we leverage the EState's junkfilter against
	 * the raw tuple yielded by the highest most node in the plan.
	 */
	TupleTableSlot *slot = queryDesc->planstate->ps_ResultTupleSlot;
	Insist(!TupIsNull(slot));
	Assert(queryDesc->estate->es_junkFilter);

	/* extract gp_segment_id metadata */
	found_attribute = ExecGetJunkAttribute(queryDesc->estate->es_junkFilter,
										   slot,
						 				   "gp_segment_id",
						 				   &value,
						 				   &isnull);
	Insist(found_attribute);
	Assert(!isnull);
	*gp_segment_id = DatumGetInt32(value);

	/* extract ctid metadata */
	found_attribute = ExecGetJunkAttribute(queryDesc->estate->es_junkFilter,
						 				   slot,
						 				   "ctid",
						 				   &value,
						 				   &isnull);
	Insist(found_attribute);
	Assert(!isnull);
	ItemPointerCopy(DatumGetItemPointer(value), ctid);

	/* 
	 * extract tableoid metadata
	 *
	 * DECLARE CURSOR planning only includes tableoid metadata when
	 * scrolling a partitioned table, as this is the only case in which
	 * gp_segment_id/ctid alone do not suffice to uniquely identify a tuple.
	 */
	found_attribute = ExecGetJunkAttribute(queryDesc->estate->es_junkFilter,
										   slot,
						 				   "tableoid",
						 				   &value,
						 				   &isnull);
	if (found_attribute)
	{
		Assert(!isnull);	
		*tableoid = DatumGetObjectId(value);
		
		/*
		 * This is our last opportunity to verify that the physical table given
		 * by tableoid is, indeed, simply updatable.
		 */
		if (!isSimplyUpdatableRelation(*tableoid))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("%s is not updatable",
							get_rel_name_partition(*tableoid))));
	} else
		*tableoid = InvalidOid;

	pfree(table_name);
}

/*
 * Evaluate functions to constants.
 */
Node *
exec_make_plan_constant(struct PlannedStmt *stmt, bool is_SRI)
{
	pre_dispatch_function_evaluation_context pcontext;

	Assert(stmt);
	exec_init_plan_tree_base(&pcontext.base, stmt);
	pcontext.single_row_insert = is_SRI;

	return plan_tree_mutator((Node *)stmt->planTree, pre_dispatch_function_evaluation_mutator, &pcontext);
}

Node *
planner_make_plan_constant(struct PlannerInfo *root, Node *n, bool is_SRI)
{
	pre_dispatch_function_evaluation_context pcontext;

	planner_init_plan_tree_base(&pcontext.base, root);
	pcontext.single_row_insert = is_SRI;

	return plan_tree_mutator(n, pre_dispatch_function_evaluation_mutator, &pcontext);
}



