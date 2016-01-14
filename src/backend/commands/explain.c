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
 * explain.c
 *	  Explain query execution plans
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/commands/explain.c,v 1.152 2006/10/04 00:29:51 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "commands/prepare.h"
#include "commands/trigger.h"
#include "executor/instrument.h"
#include "nodes/pg_list.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"             /* AllocSetContextCreate() */
#include "utils/resscheduler.h"
#include "cdb/cdbdisp.h"                /* CheckDispatchResult() */
#include "cdb/cdbexplain.h"             /* cdbexplain_recvExecStats */
#include "cdb/cdblink.h"                /* getgpsegmentCount() */
#include "cdb/cdbpartition.h"
#include "cdb/cdbpullup.h"              /* cdbpullup_targetlist() */
#include "cdb/cdbvars.h"
#include "cdb/cdbpathlocus.h"
#include "cdb/memquota.h"
#include "miscadmin.h"
#include "cdb/cdbgang.h"
#include "cdb/dispatcher.h"

#ifdef USE_ORCA
extern char *SzDXLPlan(Query *parse);
extern StringInfo OptVersion();
#endif

typedef struct ExplainState
{
	/* options */
	bool		printTList;		/* print plan targetlists */
	bool		printAnalyze;	/* print actual times */
	/* other states */
	PlannedStmt *pstmt;			/* top of plan */
	List	   *rtable;			/* range table */

    /* CDB */
	int				segmentNum;
    struct CdbExplain_ShowStatCtx  *showstatctx;    /* EXPLAIN ANALYZE info */
    Slice          *currentSlice;   /* slice whose nodes we are visiting */
    ErrorData      *deferredError;  /* caught error to be re-thrown */
    MemoryContext   explaincxt;     /* mem pool for palloc()ing buffers etc. */
    TupOutputState *tupOutputState; /* for sending output to client */
	StringInfoData  outbuf;         /* the output buffer */
    StringInfoData  workbuf;        /* a scratch buffer */
} ExplainState;

extern bool Test_print_direct_dispatch_info;

static void ExplainOneQuery(Query *query, ExplainStmt *stmt,
							const char *queryString,
							ParamListInfo params, TupOutputState *tstate);
static void
ExplainOnePlan_internal(PlannedStmt *plannedstmt,
                        ExplainStmt    *stmt,
                        const char *queryString, ParamListInfo params,
                        TupOutputState *tstate,
                        ExplainState   *es,bool isSequential);

#ifdef USE_ORCA
static void ExplainDXL(Query *query, ExplainStmt *stmt,
							const char *queryString,
							ParamListInfo params, TupOutputState *tstate);
#endif

static double elapsed_time(instr_time *starttime);
static ErrorData *explain_defer_error(ExplainState *es);
static void explain_outNode(StringInfo str,
				Plan *plan, PlanState *planstate,
				Plan *outer_plan,
				int indent, ExplainState *es, bool isSequential);
static void show_plan_tlist(Plan *plan,
				StringInfo str, int indent, ExplainState *es);
static void show_scan_qual(List *qual, const char *qlabel,
			   int scanrelid, Plan *scan_plan, Plan *outer_plan,
			   StringInfo str, int indent, ExplainState *es);
static void show_upper_qual(List *qual, const char *qlabel,
				const char *outer_name, Plan *outer_plan,
				const char *inner_name, Plan *inner_plan,
				StringInfo str, int indent, ExplainState *es);
static void show_sort_keys(Plan *sortplan, int nkeys, AttrNumber *keycols,
			   const char *qlabel,
			   StringInfo str, int indent, ExplainState *es);
static void
show_grouping_keys(Plan        *plan,
                   int          numCols,
                   AttrNumber  *subplanColIdx,
                   const char  *qlabel,
			       StringInfo str, int indent, ExplainState *es);
static void
show_motion_keys(Plan *plan, List *hashExpr, int nkeys, AttrNumber *keycols,
			     const char *qlabel,
                 StringInfo str, int indent, ExplainState *es);
static void
show_static_part_selection(Plan *plan, int indent, StringInfo str);

static const char *explain_get_index_name(Oid indexId);

/*
 * ExplainQuery -
 *	  execute an EXPLAIN command
 */
void
ExplainQuery(ExplainStmt *stmt, const char *queryString,
			 ParamListInfo params, DestReceiver *dest)
{
	Query	   *query = stmt->query;
	TupOutputState *tstate;
	List	   *rewritten;
	ListCell   *l;

	/*
	 * Because the planner is not cool about not scribbling on its input, we
	 * make a preliminary copy of the source querytree.  This prevents
	 * problems in the case that the EXPLAIN is in a portal or plpgsql
	 * function and is executed repeatedly.  (See also the same hack in
	 * DECLARE CURSOR and PREPARE.)  XXX the planner really shouldn't modify
	 * its input ... FIXME someday.
	 */
	query = copyObject(query);

	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, ExplainResultDesc(stmt));

	if (query->commandType == CMD_UTILITY)
	{
		/* Rewriter will not cope with utility statements */
		if (query->utilityStmt && IsA(query->utilityStmt, DeclareCursorStmt))
			ExplainOneQuery(query, stmt, queryString, params, tstate);
		else if (query->utilityStmt && IsA(query->utilityStmt, ExecuteStmt))
			ExplainExecuteQuery((ExecuteStmt *) stmt->query->utilityStmt, stmt, queryString, params, tstate);
		else
			do_text_output_oneline(tstate, "Utility statements have no plan structure");
	}
	else
	{
		/*
		 * Must acquire locks in case we didn't come fresh from the parser.
		 * XXX this also scribbles on query, another reason for copyObject
		 */
		AcquireRewriteLocks(query);

		/* Rewrite through rule system */
		rewritten = QueryRewrite(query);

		if (rewritten == NIL)
		{
			/* In the case of an INSTEAD NOTHING, tell at least that */
			do_text_output_oneline(tstate, "Query rewrites to nothing");
		}
		else
		{
			/* Explain every plan */
			foreach(l, rewritten)
			{
				ExplainOneQuery(lfirst(l), stmt, queryString, params, tstate);
				/* put a blank line between plans */
				if (lnext(l) != NULL)
					do_text_output_oneline(tstate, "");
			}
		}
	}

	end_tup_output(tstate);
}

/*
 * ExplainResultDesc -
 *	  construct the result tupledesc for an EXPLAIN
 */
TupleDesc
ExplainResultDesc(ExplainStmt *stmt)
{
	TupleDesc	tupdesc;

	/* need a tuple descriptor representing a single TEXT column */
	tupdesc = CreateTemplateTupleDesc(1, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "QUERY PLAN",
					   TEXTOID, -1, 0);
	return tupdesc;
}

#ifdef USE_ORCA
/*
 * ExplainDXL -
 *	  print out the execution plan for one Query in DXL format
 *	  this function implicitly uses optimizer
 */
static void
ExplainDXL(Query *query, ExplainStmt *stmt, const char *queryString,
				ParamListInfo params, TupOutputState *tstate)
{
    MemoryContext   oldcxt = CurrentMemoryContext;
    MemoryContext   explaincxt;
	ExplainState    explainState;
    ExplainState   *es = &explainState;

    /* Create EXPLAIN memory context. */
    explaincxt = AllocSetContextCreate(CurrentMemoryContext,
                                       "EXPLAIN working storage",
                                       ALLOCSET_DEFAULT_MINSIZE,
                                       ALLOCSET_DEFAULT_INITSIZE,
                                       ALLOCSET_DEFAULT_MAXSIZE);

    /* Initialize ExplainState structure. */
    memset(es, 0, sizeof(*es));
    es->explaincxt = explaincxt;
    es->showstatctx = NULL;
    es->deferredError = NULL;
    es->tupOutputState = tstate;
    es->pstmt = NULL;

    /* Allocate output buffer and a scratch buffer. */
    MemoryContextSwitchTo(explaincxt);
    initStringInfoOfSize(&es->workbuf, 1000);
	initStringInfoOfSize(&es->outbuf, 16000);
    MemoryContextSwitchTo(oldcxt);

    bool enumerate = optimizer_enumerate_plans;

    /* Do the EXPLAIN. */
    PG_TRY();
    {
    	// enable plan enumeration before calling optimizer
    	optimizer_enumerate_plans = true;

    	// optimize query using optimizer and get generated plan in DXL format
    	char *dxl = SzDXLPlan(query);

    	// restore old value of enumerate plans GUC
    	optimizer_enumerate_plans = enumerate;

    	if (NULL == dxl)
    	{
    		elog(NOTICE, "Optimizer failed to produce plan");
    	}
    	else
    	{
    		do_text_output_multiline(es->tupOutputState, dxl);
    		do_text_output_oneline(es->tupOutputState, ""); /* separator line */
    		pfree(dxl);
    	}

    	 /* Free the memory we used. */
    	MemoryContextSwitchTo(oldcxt);
    	MemoryContextDelete(explaincxt);
    }
    PG_CATCH();
    {
    	// restore old value of enumerate plans GUC
    	optimizer_enumerate_plans = enumerate;

    	/* Free the memory we used. */
    	if (CurrentMemoryContext == explaincxt)
    	{
    		MemoryContextSwitchTo(oldcxt);
    	}

    	MemoryContextDelete(explaincxt);

    	/* Exit to next error handler. */
    	PG_RE_THROW();
    }
    PG_END_TRY();
}
#endif

/*
 * ExplainOneQuery -
 *	  print out the execution plan for one Query
 */
static void
ExplainOneQuery(Query *query, ExplainStmt *stmt, const char *queryString,
				ParamListInfo params, TupOutputState *tstate)
{
	PlannedStmt	*plan = NULL;

#ifdef USE_ORCA
    if (stmt->dxl)
    {
    	ExplainDXL(query, stmt, queryString, params, tstate);
    	return;
    }
#endif

	/* planner will not cope with utility statements */
	if (query->commandType == CMD_UTILITY)
	{
		ExplainOneUtility(query->utilityStmt, stmt,
						  queryString, params, tstate);
		return;
	}

	/* plan the query */
	//pstmt = planner(query, cursorOptions, params);
 	plan = pg_plan_query(query,/*0,*/ params, QRL_ONCE);

	/*
	 * Update snapshot command ID to ensure this query sees results of any
	 * previously executed queries.  (It's a bit cheesy to modify
	 * ActiveSnapshot without making a copy, but for the limited ways in which
	 * EXPLAIN can be invoked, I think it's OK, because the active snapshot
	 * shouldn't be shared with anything else anyway.)
	 */
	ActiveSnapshot->curcid = GetCurrentCommandId();

	/* Create a QueryDesc requesting no output */
	//queryDesc = CreateQueryDesc(pstmt, queryString,
	//							ActiveSnapshot, InvalidSnapshot,
	//							None_Receiver, params,
	//							stmt->analyze);

	ExplainOnePlan(plan, stmt, queryString, params, tstate);
}

/*
 * ExplainOneUtility -
 *	  print out the execution plan for one utility statement
 *	  (In general, utility statements don't have plans, but there are some
 *	  we treat as special cases)
 *
 * This is exported because it's called back from prepare.c in the
 * EXPLAIN EXECUTE case
 */
void
ExplainOneUtility(Node *utilityStmt, ExplainStmt *stmt,
				  const char *queryString, ParamListInfo params,
				  TupOutputState *tstate)
{
	if (utilityStmt == NULL)
		return;

	if (IsA(utilityStmt, ExecuteStmt))
		ExplainExecuteQuery((ExecuteStmt *) utilityStmt, stmt,
							queryString, params, tstate);
	else if (IsA(utilityStmt, NotifyStmt))
		do_text_output_oneline(tstate, "NOTIFY");
	else
		do_text_output_oneline(tstate,
							   "Utility statements have no plan structure");
}

/*
 * ExplainOnePlan -
 *		given a planned query, execute it if needed, and then print
 *		EXPLAIN output
 *
 * Since we ignore any DeclareCursorStmt that might be attached to the query,
 * if you say EXPLAIN ANALYZE DECLARE CURSOR then we'll actually run the
 * query.  This is different from pre-8.3 behavior but seems more useful than
 * not running the query.  No cursor will be created, however.
 *
 * This is exported because it's called back from prepare.c in the
 * EXPLAIN EXECUTE case, and because an index advisor plugin would need
 * to call it.
 */
void
ExplainOnePlan(PlannedStmt *plannedstmt, ExplainStmt *stmt,
			   const char *queryString,
			   ParamListInfo params,
			   TupOutputState *tstate)
{
    MemoryContext   oldcxt = CurrentMemoryContext;
    MemoryContext   explaincxt;
	ExplainState    explainState;
    ExplainState   *es = &explainState;

    /* Create EXPLAIN memory context. */
    explaincxt = AllocSetContextCreate(CurrentMemoryContext,
                                       "EXPLAIN working storage",
                                       ALLOCSET_DEFAULT_MINSIZE,
                                       ALLOCSET_DEFAULT_INITSIZE,
                                       ALLOCSET_DEFAULT_MAXSIZE);

    /* Initialize ExplainState structure. */
    memset(es, 0, sizeof(*es));
    es->explaincxt = explaincxt;
    es->showstatctx = NULL;
    es->deferredError = NULL;
    es->tupOutputState = tstate;
    es->pstmt = plannedstmt;
    /* TODO: HAWQ2: Refactor the resource to empty set with segmentNum. */
    es->segmentNum = plannedstmt->planner_segments;

    /* Allocate output buffer and a scratch buffer. */
    MemoryContextSwitchTo(explaincxt);
    initStringInfoOfSize(&es->workbuf, 1000);
    initStringInfoOfSize(&es->outbuf, 16000);
    MemoryContextSwitchTo(oldcxt);

    /* Do the EXPLAIN. */
    PG_TRY();
    {
    			bool isSequential =false;
    			if(plannedstmt->planTree->dispatch  == DISPATCH_SEQUENTIAL){
    				isSequential=true;
    			}
        ExplainOnePlan_internal(plannedstmt, stmt,
                                queryString, params, tstate, es,isSequential);
    }
    PG_CATCH();
    {
        /* Free the memory we used. */
        if (CurrentMemoryContext == explaincxt)
            MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(explaincxt);

        /* Exit to next error handler. */
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* Free the memory we used. */
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(explaincxt);
}                               /* ExplainOnePlan */

void
ExplainOnePlan_internal(PlannedStmt *plannedstmt,
                        ExplainStmt    *stmt,
                        const char *queryString, ParamListInfo params,
                        TupOutputState *tstate,
                        ExplainState   *es,
												bool isSequential)
{
    MemoryContext   oldcxt = CurrentMemoryContext;
	QueryDesc  *queryDesc;
	instr_time	starttime;
	double		totaltime = 0;
    StringInfo  buf = &es->outbuf;
    EState     *estate = NULL;
	int			eflags;
    int         nb;

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.
	 */
	//PushUpdatedSnapshot(GetActiveSnapshot());

	/* Create a QueryDesc requesting no output */
	queryDesc = CreateQueryDesc(plannedstmt, queryString,
			                    ActiveSnapshot, InvalidSnapshot,
								None_Receiver, params,
								stmt->analyze);

    /*
     * Start timing.
     */
    INSTR_TIME_SET_CURRENT(starttime);

    if (Debug_print_execution_detail)
      elog(DEBUG1,"The time before doing explain analyze: %.3f ms",
           1000.0 * INSTR_TIME_GET_DOUBLE(starttime));

	/* If analyzing, we need to cope with queued triggers */
	if (stmt->analyze)
        AfterTriggerBeginQuery();

    /* Allocate workarea for summary stats. */
    if (stmt->analyze)
    {
        es->showstatctx = cdbexplain_showExecStatsBegin(queryDesc,
                                                        es->explaincxt,
                                                        starttime);

        /* Attach workarea to QueryDesc so ExecSetParamPlan() can find it. */
        queryDesc->showstatctx = es->showstatctx;
    }

	/* Select execution options */
	if (stmt->analyze)
        eflags = 0;				/* default run-to-completion flags */
	else
		eflags = EXEC_FLAG_EXPLAIN_ONLY;

	if ( queryDesc->resource != NULL )
	{
		queryDesc->plannedstmt->query_mem = queryDesc->resource->segment_memory_mb;
		queryDesc->plannedstmt->query_mem *= 1024L * 1024L;
	}
	else
	{
		queryDesc->plannedstmt->query_mem = statement_mem * 1024;
	}

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, eflags);

    estate = queryDesc->estate;

    /* CDB: Find slice table entry for the root slice. */
    es->currentSlice = getCurrentSlice(estate, LocallyExecutingSliceIndex(estate));

    /*
     * Execute the plan for statistics if asked for.  Attempt to proceed
     * with our report even if there is an error.
     */
	if (stmt->analyze)
	{
		/* run the plan */
        PG_TRY();
        {
		    ExecutorRun(queryDesc, ForwardScanDirection, 0L);
        }
        PG_CATCH();
        {
            es->deferredError = explain_defer_error(es);
        }
        PG_END_TRY();

        /* Wait for completion of all qExec processes. */
        PG_TRY();
        {
            if (estate->dispatch_data)
			{
                dispatch_wait(estate->dispatch_data); 
			}
        }
        PG_CATCH();
        {
            es->deferredError = explain_defer_error(es);
        }
        PG_END_TRY();

        /* Suspend timing. */
	    totaltime += elapsed_time(&starttime);
	    if (Debug_print_execution_detail) {
	      instr_time  endtime;
	      INSTR_TIME_SET_CURRENT(endtime);
	      elog(DEBUG1,"The time after doing explain analyze: %.3f ms",
	                       1000.0 * INSTR_TIME_GET_DOUBLE(endtime));
	    }



        /* Get local stats if root slice was executed here in the qDisp. */
        if (!es->currentSlice ||
            sliceRunsOnQD(es->currentSlice))
            cdbexplain_localExecStats(queryDesc->planstate, es->showstatctx);

        /* Fill in the plan's Instrumentation with stats from qExecs. */
        if (estate->dispatch_data)
            cdbexplain_recvExecStats(queryDesc->planstate,
                                     dispatch_get_results(estate->dispatch_data),
                                     LocallyExecutingSliceIndex(estate),
                                     es->showstatctx,
                                     dispatch_get_segment_num(queryDesc->estate->dispatch_data));
	}

	es->rtable = queryDesc->plannedstmt->rtable;

    if (stmt->verbose)
	{
		char	   *s;
		char	   *f;

        /* Switch to EXPLAIN memory context. */
        MemoryContextSwitchTo(es->explaincxt);

		if (queryDesc->plannedstmt->planTree && estate->es_sliceTable)
		{
        	Node   *saved_es_sliceTable;

			/* Little two-step to get EXPLAIN VERBOSE to show slice table. */
			saved_es_sliceTable = queryDesc->plannedstmt->planTree->sliceTable;		/* probably NULL */
			queryDesc->plannedstmt->planTree->sliceTable = (Node *) queryDesc->estate->es_sliceTable;
			s = nodeToString(queryDesc->plannedstmt);
			queryDesc->plannedstmt->planTree->sliceTable = saved_es_sliceTable;
		}
		else
		{
			s = nodeToString(queryDesc->plannedstmt);
		}

		if (s)
		{
			if (Explain_pretty_print)
				f = pretty_format_node_dump(s);
			else
				f = format_node_dump(s);
			pfree(s);

            /* Restore caller's memory context. */
            MemoryContextSwitchTo(oldcxt);

			do_text_output_multiline(es->tupOutputState, f);
			pfree(f);
			do_text_output_oneline(es->tupOutputState, ""); /* separator line */
		}

        /* Restore caller's memory context. */
        MemoryContextSwitchTo(oldcxt);
	}

    /*
     * Produce the EXPLAIN report into buf.  (Sometimes we get internal errors
     * while doing this; try to proceed with a partial report anyway.)
     */
    PG_TRY();
    {
     	int indent = 0;
    	CmdType cmd = queryDesc->plannedstmt->commandType;
    	Plan *childPlan = queryDesc->plannedstmt->planTree;

    	if ( (cmd == CMD_DELETE || cmd == CMD_INSERT || cmd == CMD_UPDATE) &&
    		  queryDesc->plannedstmt->planGen == PLANGEN_PLANNER )
    	{
    	   	/* Set sliceNum to the slice number of the outer-most query plan node */
    	   	int sliceNum = 0;
    	   	int numSegments = es->segmentNum;
	    	char *cmdName = NULL;

   			switch (cmd)
			{
				case CMD_DELETE:
					cmdName = "Delete";
					break;
				case CMD_INSERT:
					cmdName = "Insert";
					break;
				case CMD_UPDATE:
					cmdName = "Update";
					break;
				default:
					/* This should never be reached */
					Assert(!"Unexpected statement type");
					break;
			}
			appendStringInfo(buf, "%s", cmdName);

			if (IsA(childPlan, Motion))
			{
				Motion	   *pMotion = (Motion *) childPlan;
				if (pMotion->motionType == MOTIONTYPE_FIXED && pMotion->numOutputSegs != 0)
				{
					numSegments = 1;
				}
				/* else: other motion nodes execute on all segments */
			}
			else if ((childPlan->directDispatch).isDirectDispatch)
			{
				numSegments = 1;
			}
			appendStringInfo(buf, " (slice%d; segments: %d)", sliceNum, numSegments);
			appendStringInfo(buf, "  (rows=%.0f width=%d)\n", ceil(childPlan->plan_rows / numSegments), childPlan->plan_width);
			appendStringInfo(buf, "  ->  ");
			indent = 3;
		}
	    explain_outNode(buf, childPlan, queryDesc->planstate,
					    NULL, indent, es, isSequential);
    }
    PG_CATCH();
    {
        es->deferredError = explain_defer_error(es);

        /* Keep a NUL at the end of the output buffer. */
        buf->data[Min(buf->len, buf->maxlen-1)] = '\0';
    }
    PG_END_TRY();

	/*
	 * If we ran the command, run any AFTER triggers it queued.  (Note this
	 * will not include DEFERRED triggers; since those don't run until end of
	 * transaction, we can't measure them.)  Include into total runtime.
     * Skip triggers if there has been an error.
	 */
	if (stmt->analyze &&
        !es->deferredError)
	{
		ResultRelInfo *rInfo;
		int			numrels = queryDesc->estate->es_num_result_relations;
		int			nr;

		INSTR_TIME_SET_CURRENT(starttime);

		AfterTriggerEndQuery(queryDesc->estate);
		totaltime += elapsed_time(&starttime);

	    /* Print info about runtime of triggers */
		rInfo = queryDesc->estate->es_result_relations;
		for (nr = 0; nr < numrels; rInfo++, nr++)
		{
			int			nt;

			if (!rInfo->ri_TrigDesc || !rInfo->ri_TrigInstrument)
				continue;
			for (nt = 0; nt < rInfo->ri_TrigDesc->numtriggers; nt++)
			{
				Trigger    *trig = rInfo->ri_TrigDesc->triggers + nt;
				Instrumentation *instr = rInfo->ri_TrigInstrument + nt;
				char	   *conname;

				/* Must clean up instrumentation state */
				InstrEndLoop(instr);

				/*
				 * We ignore triggers that were never invoked; they likely
				 * aren't relevant to the current query type.
				 */
				if (instr->ntuples == 0)
					continue;

				if (trig->tgisconstraint &&
				(conname = GetConstraintNameForTrigger(trig->tgoid)) != NULL)
				{
					appendStringInfo(buf, "Trigger for constraint %s",
									 conname);
					pfree(conname);
				}
				else
					appendStringInfo(buf, "Trigger %s", trig->tgname);

				if (numrels > 1)
					appendStringInfo(buf, " on %s",
							RelationGetRelationName(rInfo->ri_RelationDesc));

				appendStringInfo(buf, ": time=%.3f calls=%.0f\n",
								 1000.0 * instr->total,
								 instr->ntuples);
			}
		}
	}

    /*
     * Display per-slice and whole-query statistics.
     */
    if (stmt->analyze)
        cdbexplain_showExecStatsEnd(queryDesc->plannedstmt, es->showstatctx, buf, estate);

    /*
     * Show non-default GUC settings that might have affected the plan.
     */
    nb = gp_guc_list_show(buf, "Settings:  ", "%s=%s; ", PGC_S_DEFAULT,
                           gp_guc_list_for_explain);
    if (nb > 0)
    {
        truncateStringInfo(buf, buf->len - 2);  /* drop final "; " */
        appendStringInfoChar(buf, '\n');
    }

#ifdef USE_ORCA
    /* Display optimizer status: either 'legacy query optimizer' or Orca version number */
    if (optimizer_explain_show_status)
    {

    	appendStringInfo(buf, "Optimizer status: ");
    	if (queryDesc->plannedstmt->planGen == PLANGEN_PLANNER)
    	{
    		appendStringInfo(buf, "legacy query optimizer\n");
    	}
    	else /* PLANGEN_OPTIMIZER */
    	{
    		StringInfo str = OptVersion();
    		appendStringInfo(buf, "PQO version %s\n", str->data);
			pfree(str->data);
			pfree(str);
    	}
    }
#endif

    /*
     * Display final elapsed time.
     */
	if (stmt->analyze || stmt->verbose)
	{
		dispatcher_print_statistics(buf, estate->dispatch_data);
		appendStringInfo(buf, "Data locality statistics:\n");
		if(plannedstmt->datalocalityInfo ==NULL){
		  appendStringInfo(buf, "  no data locality information in this query\n");
		}else{
		  appendStringInfo(buf, "  %s\n", plannedstmt->datalocalityInfo->data);
		}
		appendStringInfo(buf, "Total runtime: %.3f ms\n",
						 1000.0 * totaltime);

	}

  if (Debug_print_execution_detail) {
    instr_time  endtime;
    INSTR_TIME_SET_CURRENT(endtime);
    elog(DEBUG1,"The time before quit explain analyze: %.3f ms",
                     1000.0 * INSTR_TIME_GET_DOUBLE(endtime));
  }

    /*
     * Send EXPLAIN report to client.  Some might have been sent already
     * by explain_outNode().
     */
    if (buf->len > 0)
        do_text_output_multiline(es->tupOutputState, buf->data);

    /*
	 * Close down the query and free resources.
     *
     * We don't have to pfree() our buffers because the caller frees them by
     * deleting explaincxt.
     *
     * For EXPLAIN ANALYZE, if a qExec failed or gave an error, ExecutorEnd()
     * will reissue the error locally at this point.  Intercept any such error
     * and reduce it to a NOTICE so it won't interfere with our output.
	 */
    PG_TRY();
    {
	    ExecutorEnd(queryDesc);
    }
    PG_CATCH();
    {
        es->deferredError = explain_defer_error(es);
    }
    PG_END_TRY();

    /*
     * If we intercepted an error, now's the time to re-throw it.
     * Although we have marked it as a NOTICE instead of an ERROR,
     * it will still get the same error handling and cleanup treatment.
     *
     * We must call EndCommand() to send a successful completion response;
     * otherwise libpq clients just discard the nice report they have received.
     * Oddly, the NOTICE will be sent *after* the success response; that
     * should be good enough for now.
     */
    if (es->deferredError)
    {
        ErrorData  *edata = es->deferredError;

        /* Tell client the command ended successfully. */
        EndCommand("EXPLAIN", es->tupOutputState->dest->mydest);

        /* Resume handling the error.  Clean up and send the NOTICE message. */
        es->deferredError = NULL;
        ReThrowError(edata);
    }

    FreeQueryDesc(queryDesc);

	/* We need a CCI just in case query expanded to multiple plans */
	if (stmt->analyze)
		CommandCounterIncrement();
}                               /* ExplainOnePlan_internal */

/* Compute elapsed time in seconds since given timestamp */
static double
elapsed_time(instr_time *starttime)
{
	instr_time	endtime;

	INSTR_TIME_SET_CURRENT(endtime);
	INSTR_TIME_SUBTRACT(endtime, *starttime);
	return INSTR_TIME_GET_DOUBLE(endtime);
}


/*
 * explain_defer_error
 *    Called within PG_CATCH handler to demote and save the current error.
 *
 * We'll try to postpone the error cleanup until after we have produced
 * the EXPLAIN ANALYZE report, and then reflect the error to the client as
 * merely a NOTICE (because an ERROR causes libpq clients to discard the
 * report).
 *
 * If successful, upon return we fall thru the bottom of the PG_CATCH
 * handler and continue sequentially.  Otherwise we re-throw to the
 * next outer error handler.
 */
ErrorData *
explain_defer_error(ExplainState *es)
{
    ErrorData  *edata;

    /* Already saved an earlier error?  Rethrow it now. */
    if (es->deferredError)
        ReThrowError(es->deferredError);    /* does not return */

    /* Try to downgrade the error to a NOTICE.  Rethrow if disallowed. */
    if (!elog_demote(NOTICE))
        PG_RE_THROW();

    /* Save the error info and expunge it from the error system. */
    MemoryContextSwitchTo(es->explaincxt);
    edata = CopyErrorData();
    FlushErrorState();

    /* Caller must eventually ReThrowError() for proper cleanup. */
    return edata;
}                               /* explain_defer_error */

static void
appendGangAndDirectDispatchInfo(StringInfo str, PlanState *planstate, int sliceId)
{
	SliceTable *sliceTable = planstate->state->es_sliceTable;
	Slice *slice = (Slice *)list_nth(sliceTable->slices, sliceId);

	switch (slice->gangType)
	{
		case GANGTYPE_UNALLOCATED:
		case GANGTYPE_ENTRYDB_READER:
			appendStringInfo(str, "  (slice%d)", sliceId);
			break;

		case GANGTYPE_PRIMARY_WRITER:
		case GANGTYPE_PRIMARY_READER:
		{
			int numSegments;
			appendStringInfo(str, "  (slice%d;", sliceId);

			if (slice->directDispatch.isDirectDispatch)
			{
				Assert( list_length(slice->directDispatch.contentIds) == 1);
				numSegments = list_length(slice->directDispatch.contentIds);
			}
			else
			{
				numSegments = slice->numGangMembersToBeActive;
			}
			appendStringInfo(str, " segments: %d)", numSegments);
			break;
		}
	}
}


/*
 * explain_outNode -
 *	  converts a Plan node into ascii string and appends it to 'str'
 *
 * planstate points to the executor state node corresponding to the plan node.
 * We need this to get at the instrumentation data (if any) as well as the
 * list of subplans.
 *
 * outer_plan, if not null, references another plan node that is the outer
 * side of a join with the current node.  This is only interesting for
 * deciphering runtime keys of an inner indexscan.
 */
static void
explain_outNode(StringInfo str,
				Plan *plan, PlanState *planstate,
				Plan *outer_plan,
				int indent, ExplainState *es, bool isSequential)
{
	const char	   *pname = NULL;
    Slice      *currentSlice = es->currentSlice;    /* save */
	int			i;
	bool		skip_outer=false;
	char       *skip_outer_msg = NULL;
	float		scaleFactor = 1.0; /* we will divide planner estimates by this factor to produce
									  per-segment estimates */

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/**
		 * Estimates will have to be scaled down to be per-segment (except in a few cases).
		 */
		if ((plan->directDispatch).isDirectDispatch)
		{
			scaleFactor = 1.0;
		}
		else if (plan->flow != NULL && CdbPathLocus_IsBottleneck(*(plan->flow)))
		{
			/**
			 * Data is unified in one place (singleQE or QD), or executed on a single segment.
			 * We scale up estimates to make it global.
			 * We will later amend this for Motion nodes.
			 */
			scaleFactor = 1.0;
		}else if(isSequential){
			scaleFactor = 1.0;
		}
		else
		{
			/* the plan node is executed on multiple nodes, so scale down the number of rows seen by each segment */
			scaleFactor = es->segmentNum;
		}
	}

	if (plan == NULL)
	{
		appendStringInfoChar(str, '\n');
		return;
	}

	switch (nodeTag(plan))
	{
		case T_Result:
			pname = "Result";
			break;
		case T_Repeat:
			pname = "Repeat";
			break;
		case T_Append:
			pname = "Append";
			break;
		case T_Sequence:
			pname = "Sequence";
			break;
		case T_BitmapAnd:
			pname = "BitmapAnd";
			break;
		case T_BitmapOr:
			pname = "BitmapOr";
			break;
		case T_NestLoop:
			if (((NestLoop *)plan)->shared_outer)
			{
				skip_outer = true;
				skip_outer_msg = "See first subplan of Hash Join";
			}

			switch (((NestLoop *) plan)->join.jointype)
			{
				case JOIN_INNER:
					pname = "Nested Loop";
					break;
				case JOIN_LEFT:
					pname = "Nested Loop Left Join";
					break;
				case JOIN_FULL:
					pname = "Nested Loop Full Join";
					break;
				case JOIN_RIGHT:
					pname = "Nested Loop Right Join";
					break;
				case JOIN_IN:
					pname = "Nested Loop EXISTS Join";
					break;
				case JOIN_LASJ:
					pname = "Nested Loop Left Anti Semi Join";
					break;
				case JOIN_LASJ_NOTIN:
					pname = "Nested Loop Left Anti Semi Join (Not-In)";
					break;
				default:
					pname = "Nested Loop ??? Join";
					break;
			}
			break;
		case T_MergeJoin:
			switch (((MergeJoin *) plan)->join.jointype)
			{
				case JOIN_INNER:
					pname = "Merge Join";
					break;
				case JOIN_LEFT:
					pname = "Merge Left Join";
					break;
				case JOIN_FULL:
					pname = "Merge Full Join";
					break;
				case JOIN_RIGHT:
					pname = "Merge Right Join";
					break;
				case JOIN_IN:
					pname = "Merge EXISTS Join";
					break;
				case JOIN_LASJ:
					pname = "Merge Left Anti Semi Join";
					break;
				case JOIN_LASJ_NOTIN:
					pname = "Merge Left Anti Semi Join (Not-In)";
					break;
				default:
					pname = "Merge ??? Join";
					break;
			}
			break;
		case T_HashJoin:
			switch (((HashJoin *) plan)->join.jointype)
			{
				case JOIN_INNER:
					pname = "Hash Join";
					break;
				case JOIN_LEFT:
					pname = "Hash Left Join";
					break;
				case JOIN_FULL:
					pname = "Hash Full Join";
					break;
				case JOIN_RIGHT:
					pname = "Hash Right Join";
					break;
				case JOIN_IN:
					pname = "Hash EXISTS Join";
					break;
				case JOIN_LASJ:
					pname = "Hash Left Anti Semi Join";
					break;
				case JOIN_LASJ_NOTIN:
					pname = "Hash Left Anti Semi Join (Not-In)";
					break;
				default:
					pname = "Hash ??? Join";
					break;
			}
			break;
		case T_SeqScan:
			pname = "Seq Scan";
			break;
		case T_AppendOnlyScan:
			pname = "Append-only Scan";
			break;
		case T_TableScan:
			pname = "Table Scan";
			break;
		case T_DynamicTableScan:
			pname = "Dynamic Table Scan";
			break;
		case T_ParquetScan:
			pname = "Parquet table Scan";
			break;
		case T_ExternalScan:
			pname = "External Scan";
			break;
		case T_IndexScan:
			pname = "Index Scan";
			break;
		case T_DynamicIndexScan:
			pname = "Dynamic Index Scan";
			break;
		case T_BitmapIndexScan:
			pname = "Bitmap Index Scan";
			break;
		case T_BitmapHeapScan:
			pname = "Bitmap Heap Scan";
			break;
		case T_BitmapTableScan:
			pname = "Bitmap Table Scan";
			break;
		case T_TidScan:
			pname = "Tid Scan";
			break;
		case T_SubqueryScan:
			pname = "Subquery Scan";
			break;
		case T_FunctionScan:
			pname = "Function Scan";
			break;
		case T_ValuesScan:
			pname = "Values Scan";
			break;
		case T_ShareInputScan:
			{
				ShareInputScan *sisc = (ShareInputScan *) plan;
				appendStringInfo(str, "Shared Scan (share slice:id %d:%d)",
						currentSlice ? currentSlice->sliceIndex : -1, sisc->share_id);
				pname = "";
			}
			break;
		case T_Material:
			pname = "Materialize";
			break;
		case T_Sort:
			pname = "Sort";
			break;
		case T_Agg:
			switch (((Agg *) plan)->aggstrategy)
			{
				case AGG_PLAIN:
					pname = "Aggregate";
					break;
				case AGG_SORTED:
					pname = "GroupAggregate";
					break;
				case AGG_HASHED:
					pname = "HashAggregate";
					break;
				default:
					pname = "Aggregate ???";
					break;
			}
			break;
		case T_Window:
			pname = "Window";
			break;
		case T_TableFunctionScan:
			pname = "Table Function Scan";
			break;
		case T_Unique:
			pname = "Unique";
			break;
		case T_SetOp:
			switch (((SetOp *) plan)->cmd)
			{
				case SETOPCMD_INTERSECT:
					pname = "SetOp Intersect";
					break;
				case SETOPCMD_INTERSECT_ALL:
					pname = "SetOp Intersect All";
					break;
				case SETOPCMD_EXCEPT:
					pname = "SetOp Except";
					break;
				case SETOPCMD_EXCEPT_ALL:
					pname = "SetOp Except All";
					break;
				default:
					pname = "SetOp ???";
					break;
			}
			break;
		case T_Limit:
			pname = "Limit";
			break;
		case T_Hash:
			pname = "Hash";
			break;
		case T_Motion:
			{
				Motion	   *pMotion = (Motion *) plan;
				SliceTable *sliceTable = planstate->state->es_sliceTable;
				Slice *slice = (Slice *)list_nth(sliceTable->slices, pMotion->motionID);

        int         nSenders = slice->numGangMembersToBeActive;
				int         nReceivers = 0;

				/* scale the number of rows by the number of segments sending data */
				scaleFactor = nSenders;
				
				switch (pMotion->motionType)
				{
					case MOTIONTYPE_HASH:
						nReceivers = pMotion->numOutputSegs;
						pname = "Redistribute Motion";
						break;
					case MOTIONTYPE_FIXED:
						nReceivers = pMotion->numOutputSegs;
						if (nReceivers == 0)
						{
							pname = "Broadcast Motion";
							nReceivers = es->segmentNum;
						}
						else
						{
							scaleFactor = 1;
							pname = "Gather Motion";
						}
						break;
					case MOTIONTYPE_EXPLICIT:
						nReceivers = es->segmentNum;
						pname = "Explicit Redistribute Motion";
						break;
					default:
						pname = "Motion ???";
						break;
				}

				appendStringInfo(str, "%s %d:%d", pname,
						nSenders, nReceivers);

				appendGangAndDirectDispatchInfo(str, planstate, pMotion->motionID);
				pname = "";

			}
			break;
		case T_DML:
			{
				switch (es->pstmt->commandType)
				{
					case CMD_INSERT:
						pname = "Insert";
						break;
					case CMD_DELETE:
						pname = "Delete";
						break;
					case CMD_UPDATE:
						pname = "Update";
						break;
					default:
						pname = "DML ???";
						break;
				}
			}
			break;
		case T_SplitUpdate:
			pname = "Split";
			break;
		case T_AssertOp:
			pname = "Assert";
			break;
		case T_PartitionSelector:
			pname = "Partition Selector";
			break;
		case T_RowTrigger:
 			pname = "RowTrigger";
 			break;
		default:
			pname = "???";
			break;
	}

	appendStringInfoString(str, pname);
	switch (nodeTag(plan))
	{
		case T_IndexScan:
			if (ScanDirectionIsBackward(((IndexScan *) plan)->indexorderdir))
				appendStringInfoString(str, " Backward");
			appendStringInfo(str, " using %s",
			  quote_identifier(get_rel_name(((IndexScan *) plan)->indexid)));
			/* FALL THRU */
		case T_SeqScan:
		case T_ExternalScan:
		case T_AppendOnlyScan:
		case T_ParquetScan:
		case T_TableScan:
		case T_DynamicTableScan:
		case T_DynamicIndexScan:
		case T_BitmapHeapScan:
		case T_BitmapTableScan:
		case T_TidScan:
			if (((Scan *) plan)->scanrelid > 0)
			{
				RangeTblEntry *rte = rt_fetch(((Scan *) plan)->scanrelid,
											  es->rtable);
				char	   *relname;

				/* Assume it's on a real relation */
				Assert(rte->rtekind == RTE_RELATION);

				/* We only show the rel name, not schema name */
				relname = get_rel_name(rte->relid);

				appendStringInfo(str, " on %s",
								 quote_identifier(relname));
				if (strcmp(rte->eref->aliasname, relname) != 0)
					appendStringInfo(str, " %s",
									 quote_identifier(rte->eref->aliasname));

				/* Print dynamic scan id for dytnamic scan operators */
				if (isDynamicScan((Scan *)plan))
				{
					appendStringInfo(str, " (dynamic scan id: %d)",
									 ((Scan *)plan)->partIndexPrintable);
				}
			}
			break;
		case T_BitmapIndexScan:
			appendStringInfo(str, " on %s",
				explain_get_index_name(((BitmapIndexScan *) plan)->indexid));
			break;
		case T_SubqueryScan:
			if (((Scan *) plan)->scanrelid > 0)
			{
				RangeTblEntry *rte = rt_fetch(((Scan *) plan)->scanrelid,
											  es->rtable);

				appendStringInfo(str, " %s",
								 quote_identifier(rte->eref->aliasname));
			}
			break;
		case T_TableFunctionScan:
			{
				RangeTblEntry	*rte;
				FuncExpr		*funcexpr;
				char			*proname;

				/* Get the range table, it should be a TableFunction */
				rte = rt_fetch(((Scan *) plan)->scanrelid, es->rtable);
				Assert(rte->rtekind == RTE_TABLEFUNCTION);
				
				/* 
				 * Lookup the function name.
				 *
				 * Unlike RTE_FUNCTION there should be no cases where the
				 * optimizer could have evaluated away the function call.
				 */
				Insist(rte->funcexpr && IsA(rte->funcexpr, FuncExpr));
				funcexpr = (FuncExpr *) rte->funcexpr;
				proname	 = get_func_name(funcexpr->funcid);

				/* Build the output description */
				appendStringInfo(str, " on %s", quote_identifier(proname));
				if (strcmp(rte->eref->aliasname, proname) != 0)
					appendStringInfo(str, " %s",
									 quote_identifier(rte->eref->aliasname));
				
				/* might be nice to add order by and scatter by info */
				
			}
			break;
		case T_FunctionScan:
			if (((Scan *) plan)->scanrelid > 0)
			{
				RangeTblEntry *rte = rt_fetch(((Scan *) plan)->scanrelid,
											  es->rtable);
				char	   *proname;

				/* Assert it's on a RangeFunction */
				Assert(rte->rtekind == RTE_FUNCTION);

				/*
				 * If the expression is still a function call, we can get the
				 * real name of the function.  Otherwise, punt (this can
				 * happen if the optimizer simplified away the function call,
				 * for example).
				 */
				if (rte->funcexpr && IsA(rte->funcexpr, FuncExpr))
				{
					FuncExpr   *funcexpr = (FuncExpr *) rte->funcexpr;
					Oid			funcid = funcexpr->funcid;

					/* We only show the func name, not schema name */
					proname = get_func_name(funcid);
				}
				else
					proname = rte->eref->aliasname;

				appendStringInfo(str, " on %s",
								 quote_identifier(proname));
				if (strcmp(rte->eref->aliasname, proname) != 0)
					appendStringInfo(str, " %s",
									 quote_identifier(rte->eref->aliasname));
			}
			break;
		case T_ValuesScan:
			if (((Scan *) plan)->scanrelid > 0)
			{
				RangeTblEntry *rte = rt_fetch(((Scan *) plan)->scanrelid,
											  es->rtable);
				char	   *valsname;

				/* Assert it's on a values rte */
				Assert(rte->rtekind == RTE_VALUES);

				valsname = rte->eref->aliasname;

				appendStringInfo(str, " on %s",
								 quote_identifier(valsname));
			}
			break;
		case T_PartitionSelector:
			{
				PartitionSelector *ps = (PartitionSelector *)plan;
				char *relname = get_rel_name(ps->relid);
				appendStringInfo(str, " for %s", quote_identifier(relname));
				if (0 != ps->scanId)
				{
					appendStringInfo(str, " (dynamic scan id: %d)", ps->scanId);
				}
			}
			break;
		default:
			break;
	}

	Assert(scaleFactor > 0.0);

	appendStringInfo(str, "  (cost=%.2f..%.2f rows=%.0f width=%d)",
					 plan->startup_cost, plan->total_cost,
					 ceil(plan->plan_rows / scaleFactor), plan->plan_width);

	if (gp_resqueue_print_operator_memory_limits)
	{
		appendStringInfo(str, " (operatorMem=" UINT64_FORMAT "KB)",
						 PlanStateOperatorMemKB(planstate));
	}

    appendStringInfoChar(str, '\n');

#ifdef DEBUG_EXPLAIN
				appendStringInfo(str, "plan->targetlist=%s\n", nodeToString(plan->targetlist));
#endif
	/* target list */
	if (es->printTList)
		show_plan_tlist(plan, str, indent, es);

    /* quals, sort keys, etc */
	switch (nodeTag(plan))
	{
		case T_IndexScan:
		case T_DynamicIndexScan:
			show_scan_qual(((IndexScan *) plan)->indexqualorig,
						   "Index Cond",
						   ((Scan *) plan)->scanrelid,
						   plan, outer_plan,
						   str, indent, es);
			show_scan_qual(plan->qual,
						   "Filter",
						   ((Scan *) plan)->scanrelid,
						   plan, outer_plan,
						   str, indent, es);
			break;
		case T_BitmapIndexScan:
			show_scan_qual(((BitmapIndexScan *) plan)->indexqualorig,
						   "Index Cond",
						   ((Scan *) plan)->scanrelid,
						   plan, outer_plan,
						   str, indent, es);
			break;
		case T_BitmapHeapScan:
		case T_BitmapTableScan:
			/* XXX do we want to show this in production? */
			if (nodeTag(plan) == T_BitmapHeapScan)
			{
				show_scan_qual(((BitmapHeapScan *) plan)->bitmapqualorig,
							   "Recheck Cond",
							   ((Scan *) plan)->scanrelid,
							   plan, outer_plan,
							   str, indent, es);
			}
			else if (nodeTag(plan) == T_BitmapTableScan)
			{
				show_scan_qual(((BitmapTableScan *) plan)->bitmapqualorig,
							   "Recheck Cond",
							   ((Scan *) plan)->scanrelid,
							   plan, outer_plan,
							   str, indent, es);
			}
			/* FALL THRU */
		case T_SeqScan:
		case T_ExternalScan:
		case T_AppendOnlyScan:
		case T_ParquetScan:
		case T_TableScan:
		case T_DynamicTableScan:
		case T_FunctionScan:
		case T_ValuesScan:
			show_scan_qual(plan->qual,
						   "Filter",
						   ((Scan *) plan)->scanrelid,
						   plan, outer_plan,
						   str, indent, es);
			break;
		case T_SubqueryScan:
			show_scan_qual(plan->qual,
						   "Filter",
						   ((Scan *) plan)->scanrelid,
						   plan, outer_plan,
						   str, indent, es);
			break;
		case T_TidScan:
			{
				/*
				 * The tidquals list has OR semantics, so be sure to show it
				 * as an OR condition.
				 */
				List	   *tidquals = ((TidScan *) plan)->tidquals;

				if (list_length(tidquals) > 1)
					tidquals = list_make1(make_orclause(tidquals));
				show_scan_qual(tidquals,
							   "TID Cond",
							   ((Scan *) plan)->scanrelid,
							   plan, outer_plan,
							   str, indent, es);
				show_scan_qual(plan->qual,
							   "Filter",
							   ((Scan *) plan)->scanrelid,
							   plan, outer_plan,
							   str, indent, es);
			}
			break;
		case T_NestLoop:
			show_upper_qual(((NestLoop *) plan)->join.joinqual,
							"Join Filter",
							"outer", outerPlan(plan),
							"inner", innerPlan(plan),
							str, indent, es);
			show_upper_qual(plan->qual,
							"Filter",
							"outer", outerPlan(plan),
							"inner", innerPlan(plan),
							str, indent, es);
			break;
		case T_MergeJoin:
			show_upper_qual(((MergeJoin *) plan)->mergeclauses,
							"Merge Cond",
							"outer", outerPlan(plan),
							"inner", innerPlan(plan),
							str, indent, es);
			show_upper_qual(((MergeJoin *) plan)->join.joinqual,
							"Join Filter",
							"outer", outerPlan(plan),
							"inner", innerPlan(plan),
							str, indent, es);
			show_upper_qual(plan->qual,
							"Filter",
							"outer", outerPlan(plan),
							"inner", innerPlan(plan),
							str, indent, es);
			break;
		case T_HashJoin: {
			HashJoin *hash_join = (HashJoin *) plan;
			/*
			 * In the case of an "IS NOT DISTINCT" condition, we display
			 * hashqualclauses instead of hashclauses.
			 */
			List *cond_to_show = hash_join->hashclauses;
			if (list_length(hash_join->hashqualclauses) > 0) {
				cond_to_show = hash_join->hashqualclauses;
			}
			show_upper_qual(cond_to_show,
							"Hash Cond",
							"outer", outerPlan(plan),
							"inner", innerPlan(plan),
							str, indent, es);
			show_upper_qual(hash_join->join.joinqual,
							"Join Filter",
							"outer", outerPlan(plan),
							"inner", innerPlan(plan),
							str, indent, es);
			show_upper_qual(plan->qual,
							"Filter",
							"outer", outerPlan(plan),
							"inner", innerPlan(plan),
							str, indent, es);
			break;
		}
		case T_Agg:
			show_upper_qual(plan->qual,
							"Filter",
							NULL, outerPlan(plan),
							NULL, NULL,
							str, indent, es);
			show_grouping_keys(plan,
						       ((Agg *) plan)->numCols,
						       ((Agg *) plan)->grpColIdx,
						       "Group By",
						       str, indent, es);
			break;
		case T_Window:
			{
				Window *window = (Window *)plan;
				ListCell *cell;
				char orderKeyStr[32]; /* XXX big enough */
				int i;

				if ( window->numPartCols > 0 )
				{
					show_grouping_keys(plan,
									   window->numPartCols,
									   window->partColIdx,
									   "Partition By",
									   str, indent, es);
				}

				if (list_length(window->windowKeys) > 1)
					i = 0;
				else
					i = -1;

				foreach(cell, window->windowKeys)
				{
					WindowKey *key = (WindowKey *) lfirst(cell);

					if ( i < 0 )
						sprintf(orderKeyStr, "Order By");
					else
					{
						sprintf(orderKeyStr, "Order By (level %d)", ++i);
					}

					show_sort_keys(outerPlan(plan),
								   key->numSortCols,
								   key->sortColIdx,
								   orderKeyStr,
								   str, indent, es);
				}
				/* XXX don't show framing for now */
			}
			break;
		case T_TableFunctionScan:
		{
			show_scan_qual(plan->qual,
						   "Filter",
						   ((Scan *) plan)->scanrelid,
						   plan, outer_plan,
						   str, indent, es);

			/* Partitioning and ordering information */
			
		}
		break;

		case T_Unique:
			show_motion_keys(plan,
                             NIL,
						     ((Unique *) plan)->numCols,
						     ((Unique *) plan)->uniqColIdx,
						     "Group By",
						     str, indent, es);
			break;
		case T_Sort:
		{
			bool bLimit = (((Sort *) plan)->limitCount
						   || ((Sort *) plan)->limitOffset);

			bool bNoDup = ((Sort *) plan)->noduplicates;

			char *SortKeystr = "Sort Key";

			if ((bLimit && bNoDup))
				SortKeystr = "Sort Key (Limit Distinct)";
			else if (bLimit)
				SortKeystr = "Sort Key (Limit)";
			else if (bNoDup)
				SortKeystr = "Sort Key (Distinct)";

			show_sort_keys(plan,
						   ((Sort *) plan)->numCols,
						   ((Sort *) plan)->sortColIdx,
						   SortKeystr,
						   str, indent, es);
		}
			break;
		case T_Result:
			show_upper_qual((List *) ((Result *) plan)->resconstantqual,
							"One-Time Filter",
							NULL, outerPlan(plan),
							NULL, NULL,
							str, indent, es);
			show_upper_qual(plan->qual,
							"Filter",
							NULL, outerPlan(plan),
							NULL, NULL,
							str, indent, es);
			break;
		case T_Repeat:
			show_upper_qual(plan->qual,
							"Filter",
							NULL, outerPlan(plan),
							NULL, NULL,
							str, indent, es);
			break;
		case T_Motion:
			{
				Motion	   *pMotion = (Motion *) plan;
                SliceTable *sliceTable = planstate->state->es_sliceTable;

				if (pMotion->sendSorted || pMotion->motionType == MOTIONTYPE_HASH)
					show_motion_keys(plan,
							pMotion->hashExpr,
							pMotion->numSortCols,
							pMotion->sortColIdx,
							"Merge Key",
							str, indent, es);

                /* Descending into a new slice. */
                if (sliceTable)
                    es->currentSlice = (Slice *)list_nth(sliceTable->slices,
                                                         pMotion->motionID);
			}
			break;
		case T_AssertOp:
			{
				show_upper_qual(plan->qual,
								"Assert Cond",
								NULL, outerPlan(plan),
								NULL, NULL,
								str, indent, es);
			}
			break;
		case T_PartitionSelector:
			{
				List *list_qual = NULL;
				Node *printablePredicate = ((PartitionSelector *) plan)->printablePredicate;
				if (NULL != printablePredicate)
				{
					list_qual = list_make1(printablePredicate);
				}
				show_upper_qual(list_qual,
								"Filter",
								NULL, outerPlan(plan),
								NULL, NULL,
								str, indent, es);
				show_static_part_selection(plan, indent, str);
			}
			break;
		default:
			break;
	}

    /* CDB: Show actual row count, etc. */
	if (planstate->instrument)
	{
        cdbexplain_showExecStats(planstate,
                                 str,
                                 indent+1,
                                 es->showstatctx);
	}
	/* initPlan-s */
	if (plan->initPlan)
	{
        Slice      *saved_slice = es->currentSlice;
		//List	   *saved_rtable = es->rtable;
		ListCell   *lst;

		foreach(lst, planstate->initPlan)
		{
			SubPlanState *sps = (SubPlanState *) lfirst(lst);
			SubPlan    *sp = (SubPlan *) sps->xprstate.expr;
            SliceTable *sliceTable = planstate->state->es_sliceTable;

			appendStringInfoFill(str, 2*indent, ' ');
		    appendStringInfoString(str, "  InitPlan");

            /* Subplan might have its own root slice */
            if (sliceTable &&
                sp->qDispSliceId > 0)
            {
                es->currentSlice = (Slice *)list_nth(sliceTable->slices,
                                                     sp->qDispSliceId);
    		    appendGangAndDirectDispatchInfo(str, planstate, sp->qDispSliceId );
            }
            else
            {
                /*
                 * CDB TODO: In non-parallel query, all qDispSliceId's are 0.
                 * Should fill them in properly before ExecutorStart(), but
                 * for now, just omit the slice id.
                 */
            }

            appendStringInfoChar(str, '\n');

            //es->rtable = sp->subplan_rtable;
			for (i = 0; i < indent; i++)
				appendStringInfo(str, "  ");
			appendStringInfo(str, "    ->  ");
			explain_outNode(str,
							exec_subplan_get_plan(es->pstmt, sp),
							sps->planstate,
							NULL,
							indent + 4, es,isSequential);
		}
        es->currentSlice = saved_slice;
		//es->rtable = saved_rtable;
	}

	/* lefttree */
	if (outerPlan(plan) && !skip_outer)
	{
		for (i = 0; i < indent; i++)
			appendStringInfo(str, "  ");
		appendStringInfo(str, "  ->  ");

		/*
		 * Ordinarily we don't pass down our own outer_plan value to our child
		 * nodes, but in bitmap scan trees we must, since the bottom
		 * BitmapIndexScan nodes may have outer references.
		 */
		explain_outNode(str, outerPlan(plan),
						outerPlanState(planstate),
						(IsA(plan, BitmapHeapScan) |
						 IsA(plan, BitmapTableScan)) ? outer_plan : NULL,
						indent + 3, es,isSequential);
	}
    else if (skip_outer)
    {
		for (i = 0; i < indent; i++)
			appendStringInfo(str, "  ");
		appendStringInfo(str, "  ->  ");
		appendStringInfoString(str, skip_outer_msg);
		appendStringInfo(str, "\n");
    }

	/* righttree */
	if (innerPlan(plan))
	{
		for (i = 0; i < indent; i++)
			appendStringInfo(str, "  ");
		appendStringInfo(str, "  ->  ");
		explain_outNode(str, innerPlan(plan),
						innerPlanState(planstate),
						outerPlan(plan),
						indent + 3, es,isSequential);
	}

	if (IsA(plan, Append))
	{
		Append	   *appendplan = (Append *) plan;
		AppendState *appendstate = (AppendState *) planstate;
		ListCell   *lst;
		int			j;

		j = 0;
		foreach(lst, appendplan->appendplans)
		{
			Plan	   *subnode = (Plan *) lfirst(lst);

			for (i = 0; i < indent; i++)
			{
				appendStringInfo(str, "  ");
			}

			appendStringInfo(str, "  ->  ");

			/*
			 * Ordinarily we don't pass down our own outer_plan value to our
			 * child nodes, but in an Append we must, since we might be
			 * looking at an appendrel indexscan with outer references from
			 * the member scans.
			 */
			explain_outNode(str, subnode,
							appendstate->appendplans[j],
							outer_plan,
							indent + 3, es,isSequential);
			j++;
		}
	}

	if (IsA(plan, Sequence))
	{
		Sequence *sequence = (Sequence *) plan;
		SequenceState *sequenceState = (SequenceState *) planstate;
		ListCell *lc;
		int j = 0;
		foreach(lc, sequence->subplans)
		{
			Plan *subnode = (Plan *) lfirst(lc);

			for (i = 0; i < indent; i++)
			{
				appendStringInfo(str, "  ");
			}
			
			appendStringInfo(str, "  ->  ");

			explain_outNode(str, subnode,
							sequenceState->subplans[j],
							outer_plan,
							indent + 3, es,isSequential);
			j++;
		}
	}

	if (IsA(plan, BitmapAnd))
	{
		BitmapAnd  *bitmapandplan = (BitmapAnd *) plan;
		BitmapAndState *bitmapandstate = (BitmapAndState *) planstate;
		ListCell   *lst;
		int			j;

		j = 0;
		foreach(lst, bitmapandplan->bitmapplans)
		{
			Plan	   *subnode = (Plan *) lfirst(lst);

			for (i = 0; i < indent; i++)
				appendStringInfo(str, "  ");
			appendStringInfo(str, "  ->  ");

			explain_outNode(str, subnode,
							bitmapandstate->bitmapplans[j],
							outer_plan, /* pass down same outer plan */
							indent + 3, es,isSequential);
			j++;
		}
	}

	if (IsA(plan, BitmapOr))
	{
		BitmapOr   *bitmaporplan = (BitmapOr *) plan;
		BitmapOrState *bitmaporstate = (BitmapOrState *) planstate;
		ListCell   *lst;
		int			j;

		j = 0;
		foreach(lst, bitmaporplan->bitmapplans)
		{
			Plan	   *subnode = (Plan *) lfirst(lst);

			for (i = 0; i < indent; i++)
				appendStringInfo(str, "  ");
			appendStringInfo(str, "  ->  ");

			explain_outNode(str, subnode,
							bitmaporstate->bitmapplans[j],
							outer_plan, /* pass down same outer plan */
							indent + 3, es,isSequential);
			j++;
		}
	}

	if (IsA(plan, SubqueryScan))
	{
		SubqueryScan *subqueryscan = (SubqueryScan *) plan;
		SubqueryScanState *subquerystate = (SubqueryScanState *) planstate;
		Plan	   *subnode = subqueryscan->subplan;

		for (i = 0; i < indent; i++)
			appendStringInfo(str, "  ");
		appendStringInfo(str, "  ->  ");

		explain_outNode(str, subnode,
						subquerystate->subplan,
						NULL,
						indent + 3, es,isSequential);
	}

	/* subPlan-s */
	if (planstate->subPlan)
	{
		ListCell   *lst;

		foreach(lst, planstate->subPlan)
		{
			SubPlanState *sps = (SubPlanState *) lfirst(lst);
			SubPlan    *sp = (SubPlan *) sps->xprstate.expr;

			for (i = 0; i < indent; i++)
				appendStringInfo(str, "  ");
			appendStringInfo(str, "  %s\n", sp->plan_name);
            Assert(!sps->cdbextratextbuf);

			for (i = 0; i < indent; i++)
				appendStringInfo(str, "  ");
			appendStringInfo(str, "    ->  ");
			explain_outNode(str,
							exec_subplan_get_plan(es->pstmt, sp),
							sps->planstate,
							NULL,
							indent + 4, es,isSequential);
		}
	}

    /* CDB: Empty the output buffer if it's more than half full. */
    if (str->len*2 > str->maxlen)
    {
        do_text_output_multiline(es->tupOutputState, str->data);
        truncateStringInfo(str, 0);
    }

    es->currentSlice = currentSlice;    /* restore */
}                               /* explain_outNode */

/*
 * Show the targetlist of a plan node
 */
static void show_plan_tlist(Plan *plan, StringInfo str, int indent,
		ExplainState *es)
{
	List *context;
	bool useprefix;
	ListCell *lc;
	int i;

	/* No work if empty tlist (this occurs eg in bitmap indexscans) */
	if (plan->targetlist == NIL)
		return;
	/* Suppress printing the targetlist of Append and Sequence. */
	if (IsA(plan, Append) || IsA(plan, Sequence))
		return;

	/* TODO: Likewise for RecursiveUnion */
	//if (IsA(plan, RecursiveUnion))
	//        return;

	/* Set up deparsing context */
	context = deparse_context_for_plan(OUTER, (Node *) plan, INNER,
			(Node *) plan, es->rtable);
	//es->pstmt->subplans);
	useprefix = list_length(es->rtable) > 1;

	/* Emit line prefix */
	for (i = 0; i < indent; i++)
		appendStringInfo(str, "  ");
	appendStringInfo(str, "  Output: ");

	/* Deparse each non-junk result column */
	i = 0;
	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (tle->resjunk)
			continue;
		if (i++ > 0)
			appendStringInfo(str, ", ");
		appendStringInfoString(str, deparse_expression((Node *) tle->expr,
				context, useprefix, false));
	}

	appendStringInfoChar(str, '\n');
}

/*
 * Show a qualifier expression for a scan plan node
 *
 * Note: outer_plan is the referent for any OUTER vars in the scan qual;
 * this would be the outer side of a nestloop plan.  inner_plan should be
 * NULL except for a SubqueryScan plan node, where it should be the subplan.
 */
static void
show_scan_qual(List *qual, const char *qlabel,
			   int scanrelid, Plan *scan_plan, Plan *outer_plan,
			   StringInfo str, int indent, ExplainState *es)
{
	Node	   *outercontext;
	List	   *context;
	//bool		useprefix;
	Node	   *node;
	char	   *exprstr;
	int			i;

	/* No work if empty qual */
	if (qual == NIL)
		return;

	/* Convert AND list to explicit AND */
	node = (Node *) make_ands_explicit(qual);

	//useprefix = (outer_plan != NULL || inner_plan != NULL);
	/*
	 * If we have an outer plan that is referenced by the qual, add it to the
	 * deparse context.  If not, don't (so that we don't force prefixes
	 * unnecessarily).
	 */
	if (outer_plan)
	{
		Relids		varnos = pull_varnos(node);

		if (bms_is_member(OUTER, varnos))
			outercontext = deparse_context_for_subplan("outer",
													   (Node *) outer_plan);
		else
			outercontext = NULL;
		bms_free(varnos);
	}
	else
		outercontext = NULL;

	context = deparse_context_for_plan(OUTER, outercontext,
									   0, NULL,
									   es->rtable);

	/* Deparse the expression */
	exprstr = deparse_expr_sweet(node, context, (outercontext != NULL), false);

	/* And add to str */
	for (i = 0; i < indent; i++)
		appendStringInfo(str, "  ");
	appendStringInfo(str, "  %s: %s\n", qlabel, exprstr);
}

/*
 * Show a qualifier expression for an upper-level plan node
 */
static void
show_upper_qual(List *qual, const char *qlabel,
				const char *outer_name, Plan *outer_plan,
				const char *inner_name, Plan *inner_plan,
				StringInfo str, int indent, ExplainState *es)
{
	List	   *context;
	Node	   *outercontext;
	Node	   *innercontext;
	int			outer_varno;
	int			inner_varno;
	Node	   *node;
	char	   *exprstr;
	int			i;
    bool		useprefix = inner_plan || list_length(es->rtable) > 1;  /*CDB*/

	/* No work if empty qual */
	if (qual == NIL)
		return;

	/* Generate deparse context */
	if (outer_plan)
	{
		outercontext = deparse_context_for_subplan(outer_name,
												   (Node *) outer_plan);
		outer_varno = OUTER;
	}
	else
	{
		outercontext = NULL;
		outer_varno = 0;
	}
	if (inner_plan)
	{
		innercontext = deparse_context_for_subplan(inner_name,
												   (Node *) inner_plan);
		inner_varno = INNER;
	}
	else
	{
		innercontext = NULL;
		inner_varno = 0;
	}
	context = deparse_context_for_plan(outer_varno, outercontext,
									   inner_varno, innercontext,
									   es->rtable);

	/* Deparse the expression */
	node = (Node *) make_ands_explicit(qual);
	exprstr = deparse_expr_sweet(node, context, useprefix, false);

	/* And add to str */
	for (i = 0; i < indent; i++)
		appendStringInfo(str, "  ");
	appendStringInfo(str, "  %s: %s\n", qlabel, exprstr);
}

/*
 * CDB: Show GROUP BY keys for an Agg or Group node.
 */
void
show_grouping_keys(Plan        *plan,
                   int          numCols,
                   AttrNumber  *subplanColIdx,
                   const char  *qlabel,
			       StringInfo str, int indent, ExplainState *es)
{
    Plan       *subplan = plan->lefttree;
    List	   *context;
    char	   *exprstr;
    bool		useprefix = list_length(es->rtable) > 1;
    int			keyno;
    int			i;
	int         num_null_cols = 0;
	int         rollup_gs_times = 0;

    if (numCols <= 0)
        return;

    for (i = 0; i < indent; i++)
        appendStringInfoString(str, "  ");
    appendStringInfo(str, "  %s: ", qlabel);

    /* Generate deparse context */
	if (cdbpullup_exprHasSubplanRef((Expr *)subplan->targetlist))
	{
		Node	   *outerctx;
		Node	   *innerctx;

        if (subplan->righttree)
        {
            outerctx = deparse_context_for_subplan("outer",
                                                   (Node *)subplan->lefttree);
            innerctx = deparse_context_for_subplan("inner",
                                                   (Node *)subplan->righttree);
	        context = deparse_context_for_plan(OUTER, outerctx,
									           INNER, innerctx,
									           es->rtable);
            useprefix = true;
        }
        else
        {
            outerctx = deparse_context_for_subplan(NULL,
                                                   (Node *)subplan->lefttree);
	        context = deparse_context_for_plan(OUTER, outerctx,
									           0, NULL,
									           es->rtable);
        }
	}
	else
		context = deparse_context_for_plan(0, NULL,
										   0, NULL,
										   es->rtable);

	if (IsA(plan, Agg))
	{
		num_null_cols = ((Agg*)plan)->numNullCols;
		rollup_gs_times = ((Agg*)plan)->rollupGSTimes;
	}

    for (keyno = 0; keyno < numCols - num_null_cols; keyno++)
    {
	    /* find key expression in tlist */
	    AttrNumber      keyresno = subplanColIdx[keyno];
	    TargetEntry    *target = get_tle_by_resno(subplan->targetlist, keyresno);
		char grping_str[50];

	    if (!target)
		    elog(ERROR, "no tlist entry for key %d", keyresno);

		if (IsA(target->expr, Grouping))
		{
			sprintf(grping_str, "grouping");
			/* Append "grouping" explicitly. */
			exprstr = grping_str;
		}

		else if (IsA(target->expr, GroupId))
		{
			sprintf(grping_str, "groupid");
			/* Append "groupid" explicitly. */
			exprstr = grping_str;
		}

		else
			/* Deparse the expression, showing any top-level cast */
			exprstr = deparse_expr_sweet((Node *) target->expr, context,
										 useprefix, true);

		/* And add to str */
		if (keyno > 0)
			appendStringInfoString(str, ", ");
		appendStringInfoString(str, exprstr);
    }

	if (rollup_gs_times > 1)
		appendStringInfo(str, " (%d times)", rollup_gs_times);

    appendStringInfoChar(str, '\n');
}                               /* show_grouping_keys */


/*
 * Show the sort keys for a Sort node.
 */
static void
show_sort_keys(Plan *sortplan, int nkeys, AttrNumber *keycols,
			   const char *qlabel,
			   StringInfo str, int indent, ExplainState *es)
{
	List	   *context;
    bool		useprefix = list_length(es->rtable) > 1;    /*CDB*/
	int			keyno;
	char	   *exprstr;
	int			i;

	if (nkeys <= 0)
		return;

	for (i = 0; i < indent; i++)
		appendStringInfo(str, "  ");
	appendStringInfo(str, "  %s: ", qlabel);

	/*
	 * In this routine we expect that the plan node's tlist has not been
	 * processed by set_plan_references().	Normally, any Vars will contain
	 * valid varnos referencing the actual rtable.	But we might instead be
	 * looking at a dummy tlist generated by prepunion.c; if there are Vars
	 * with varno OUTER, use the tlist itself to determine their names.
     * (NB: CDB uses varno OUTER; in PostgreSQL it was varno 0.)
	 */
	if (cdbpullup_exprHasSubplanRef((Expr *)sortplan->targetlist))
	{
		Node	   *outercontext;

		outercontext = deparse_context_for_subplan(NULL, (Node *)sortplan);
		context = deparse_context_for_plan(OUTER, outercontext,
										   0, NULL,
										   es->rtable);
	}
	else
		context = deparse_context_for_plan(0, NULL,
										   0, NULL,
										   es->rtable);

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		/* find key expression in tlist */
		AttrNumber	keyresno = keycols[keyno];
		TargetEntry *target = get_tle_by_resno(sortplan->targetlist, keyresno);

		if (!target)
			elog(ERROR, "no tlist entry for key %d", keyresno);
		/* Deparse the expression, showing any top-level cast */
		exprstr = deparse_expr_sweet((Node *) target->expr, context,
									 useprefix, true);
		/* And add to str */
		if (keyno > 0)
			appendStringInfo(str, ", ");
		appendStringInfoString(str, exprstr);
	}

	appendStringInfo(str, "\n");
}


/*
 * CDB: Show the hash and merge keys for a Motion node.
 */
void
show_motion_keys(Plan *plan, List *hashExpr, int nkeys, AttrNumber *keycols,
			     const char *qlabel,
                 StringInfo str, int indent, ExplainState *es)
{
    List	   *context;
    char	   *exprstr;
    bool		useprefix = list_length(es->rtable) > 1;
    int			keyno;
    int			i;

    if (!nkeys &&
        !hashExpr)
        return;

    /* Generate deparse context */
	if (cdbpullup_exprHasSubplanRef((Expr *)plan->targetlist) ||
        cdbpullup_exprHasSubplanRef((Expr *)hashExpr))
	{
		Node	   *outercontext;

	    outercontext = deparse_context_for_subplan(NULL, (Node *)plan);
	    context = deparse_context_for_plan(OUTER, outercontext,
									       0, NULL,
									       es->rtable);
	}
	else
		context = deparse_context_for_plan(0, NULL,
										   0, NULL,
										   es->rtable);

    /* Merge Receive ordering key */
    if (nkeys > 0)
    {
        for (i = 0; i < indent; i++)
            appendStringInfoString(str, "  ");
        appendStringInfo(str, "  %s: ", qlabel);

	    for (keyno = 0; keyno < nkeys; keyno++)
	    {
		    /* find key expression in tlist */
		    AttrNumber	keyresno = keycols[keyno];
		    TargetEntry *target = get_tle_by_resno(plan->targetlist, keyresno);

		    /* Deparse the expression, showing any top-level cast */
		    if (target)
		        exprstr = deparse_expr_sweet((Node *) target->expr, context,
									         useprefix, true);
            else
            {
                elog(WARNING, "Gather Motion %s error: no tlist item %d",
                     qlabel, keyresno);
                exprstr = "*BOGUS*";
            }

		    /* And add to str */
		    if (keyno > 0)
			    appendStringInfoString(str, ", ");
		    appendStringInfoString(str, exprstr);
	    }

	    appendStringInfoChar(str, '\n');
    }

    /* Hashed repartitioning key */
    if (hashExpr)
    {
	    /* Deparse the expression */
	    exprstr = deparse_expr_sweet((Node *)hashExpr, context, useprefix, true);

	    /* And add to str */
	    for (i = 0; i < indent; i++)
		    appendStringInfoString(str, "  ");
	    appendStringInfo(str, "  %s: %s\n", "Hash Key", exprstr);
    }
}                               /* show_motion_keys */

/*
 * Show the number of statically selected partitions if available.
 */
static void
show_static_part_selection(Plan *plan, int indent, StringInfo str)
{
	PartitionSelector *ps = (PartitionSelector *) plan;

	if (! ps->staticSelection)
	{
		return;
	}

	int nPartsSelected = list_length(ps->staticPartOids);
	int nPartsTotal = countLeafPartTables(ps->relid);
	for (int i = 0; i < indent; i++)
	{
		appendStringInfoString(str, "  ");
	}

	appendStringInfo(str, "  Partitions selected:  %d (out of %d)\n", nPartsSelected, nPartsTotal);
}

/*
 * Fetch the name of an index in an EXPLAIN
 *
 * We allow plugins to get control here so that plans involving hypothetical
 * indexes can be explained.
 */
static const char *
explain_get_index_name(Oid indexId)
{
	const char *result = NULL;

	//if (explain_get_index_name_hook)
	//	result = (*explain_get_index_name_hook) (indexId);
	//else
	//	result = NULL;
	if (result == NULL)
	{
		/* default behavior: look in the catalogs and quote it */
		result = get_rel_name(indexId);
		if (result == NULL)
			elog(ERROR, "cache lookup failed for index %u", indexId);
		result = quote_identifier(result);
	}
	return result;
}


