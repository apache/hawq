/*-------------------------------------------------------------------------
 *
 * explain.h
 *	  prototypes for explain.c
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/commands/explain.h,v 1.28 2006/10/04 00:30:08 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPLAIN_H
#define EXPLAIN_H

#include "cdb/cdbgang.h"
#include "executor/executor.h"

typedef struct ExplainState
{
	/* options */
	bool		printTList;		/* print plan targetlists */
	bool		printAnalyze;	/* print actual times */
	bool		printUdfPlan;  /* print udf plan */
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

extern void ExplainQuery(ExplainStmt *stmt, const char *queryString,
						 ParamListInfo params, DestReceiver *dest);

extern TupleDesc ExplainResultDesc(ExplainStmt *stmt);

extern void ExplainOneUtility(Node *utilityStmt, ExplainStmt *stmt,
				  const char *queryString,
				  ParamListInfo params,
				  TupOutputState *tstate);

extern void ExplainOnePlan(PlannedStmt *plannedstmt, ExplainStmt *stmt,
		   const char *queryString, ParamListInfo params,
		   TupOutputState *tstate);

#endif   /* EXPLAIN_H */
