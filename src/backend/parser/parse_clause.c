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
 * parse_clause.c
 *	  handle clauses in parser
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/parser/parse_clause.c,v 1.159 2006/11/28 12:54:41 petere Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/catquery.h"
#include "access/heapam.h"
#include "catalog/heap.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_window.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/print.h" /* XXX: remove after debugging !! */
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteManip.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#include "cdb/cdbvars.h"
#include "catalog/catalog.h"
#include "miscadmin.h"

/* clause types for findTargetlistEntrySQL92 */
#define ORDER_CLAUSE 0
#define GROUP_CLAUSE 1
#define DISTINCT_ON_CLAUSE 2

static const char *clauseText[] = {
    "ORDER BY", 
    "GROUP BY", 
    "DISTINCT ON"
};

static void extractRemainingColumns(List *common_colnames,
						List *src_colnames, List *src_colvars,
						List **res_colnames, List **res_colvars);
static Node *transformJoinUsingClause(ParseState *pstate,
						 List *leftVars, List *rightVars);
static Node *transformJoinOnClause(ParseState *pstate, JoinExpr *j,
					  RangeTblEntry *l_rte,
					  RangeTblEntry *r_rte,
					  List *relnamespace,
					  Relids containedRels);
static RangeTblEntry *transformTableEntry(ParseState *pstate, RangeVar *r);
static RangeTblEntry *transformRangeSubselect(ParseState *pstate,
						RangeSubselect *r);
static RangeTblEntry *transformRangeFunction(ParseState *pstate,
					   RangeFunction *r);
static Node *transformFromClauseItem(ParseState *pstate, Node *n,
									 RangeTblEntry **top_rte, int *top_rti,
									 List **relnamespace,
									 Relids *containedRels);
static Node *buildMergedJoinVar(ParseState *pstate, JoinType jointype,
				   Var *l_colvar, Var *r_colvar);
static TargetEntry *findTargetlistEntrySQL92(ParseState *pstate, Node *node,
					List **tlist, int clause);
static TargetEntry *findTargetlistEntrySQL99(ParseState *pstate, Node *node,
					List **tlist);
static List *findListTargetlistEntries(ParseState *pstate, Node *node,
									   List **tlist, bool in_grpext,
									   bool ignore_in_grpext,
                                       bool useSQL99);
static TargetEntry *getTargetBySortGroupRef(Index ref, List *tl);
static List *reorderGroupList(List *grouplist);
static List *transformRowExprToList(ParseState *pstate, RowExpr *rowexpr,
									List *groupsets, List *targetList);
static List *transformRowExprToGroupClauses(ParseState *pstate, RowExpr *rowexpr,
											List *groupsets, List *targetList);
static void freeGroupList(List *grouplist);

typedef struct grouping_rewrite_ctx
{
	List *grp_tles;
	ParseState *pstate;
} grouping_rewrite_ctx;

typedef struct winref_check_ctx
{
	ParseState *pstate;
	Index win_clause;
	bool has_order;
	bool has_frame;
} winref_check_ctx;

/*
 * transformFromClause -
 *	  Process the FROM clause and add items to the query's range table,
 *	  joinlist, and namespaces.
 *
 * Note: we assume that pstate's p_rtable, p_joinlist, p_relnamespace, and
 * p_varnamespace lists were initialized to NIL when the pstate was created.
 * We will add onto any entries already present --- this is needed for rule
 * processing, as well as for UPDATE and DELETE.
 *
 * The range table may grow still further when we transform the expressions
 * in the query's quals and target list. (This is possible because in
 * POSTQUEL, we allowed references to relations not specified in the
 * from-clause.  PostgreSQL keeps this extension to standard SQL.)
 */
void
transformFromClause(ParseState *pstate, List *frmList)
{
	ListCell   *fl = NULL;

	/*
	 * The grammar will have produced a list of RangeVars, RangeSubselects,
	 * RangeFunctions, and/or JoinExprs. Transform each one (possibly adding
	 * entries to the rtable), check for duplicate refnames, and then add it
	 * to the joinlist and namespaces.
	 */
	foreach(fl, frmList)
	{
		Node *n = lfirst(fl);
		RangeTblEntry *rte = NULL;
		int	rtindex = 0;
		List *relnamespace = NULL;
		Relids containedRels = NULL;

		n = transformFromClauseItem(pstate, n,
									&rte,
									&rtindex,
									&relnamespace,
									&containedRels);
		checkNameSpaceConflicts(pstate, pstate->p_relnamespace, relnamespace);
		pstate->p_joinlist = lappend(pstate->p_joinlist, n);
		pstate->p_relnamespace = list_concat(pstate->p_relnamespace,
											 relnamespace);
		pstate->p_varnamespace = lappend(pstate->p_varnamespace, rte);
		bms_free(containedRels);
	}
}

static bool
expr_null_check_walker(Node *node, void *context)
{
	if (!node)
		return false;

	if (IsA(node, Const))
	{
		Const *con = (Const *)node;

		if (con->constisnull)
			return true;
	}
	return expression_tree_walker(node, expr_null_check_walker, context);
}

static bool
expr_contains_null_const(Expr *expr)
{
	return expr_null_check_walker((Node *)expr, NULL);
}

static void
transformWindowFrameEdge(ParseState *pstate, WindowFrameEdge *e,
						 WindowSpec *spec, Query *qry, bool is_rows)
{
	/* Only bound frame edges will have a value */
	if (e->kind == WINDOW_BOUND_PRECEDING ||
		e->kind == WINDOW_BOUND_FOLLOWING)
	{
		if (is_rows)
		{
			/* the e->val should already be transformed */
			if (IsA(e->val, Const))
			{
				Const *con = (Const *)e->val;

				if (con->consttype != INT4OID)
					ereport(ERROR,
							(errcode(ERROR_INVALID_WINDOW_FRAME_PARAMETER),
							 errmsg("ROWS parameter must be an integer expression"),
									   errOmitLocation(true)));
				if (DatumGetInt32(con->constvalue) < 0)
					ereport(ERROR,
							(errcode(ERROR_INVALID_WINDOW_FRAME_PARAMETER),
							 errmsg("ROWS parameter cannot be negative"),
									   errOmitLocation(true)));
			}

			if (expr_contains_null_const((Expr *)e->val))
				ereport(ERROR,
						(errcode(ERROR_INVALID_WINDOW_FRAME_PARAMETER),
						 errmsg("ROWS parameter cannot contain NULL value"),
								   errOmitLocation(true)));
		}
		else
		{
			TargetEntry *te;
			Oid otype;
			Oid rtype;
			Oid newrtype;
			SortClause *sort;
			Oid oprresult;
			List *oprname;
			Operator tup;
			int32 typmod;

			if (list_length(spec->order) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("only one ORDER BY column may be specified when"
								" RANGE is used in a window specification"),
										   errOmitLocation(true)));

			/* e->val should already be transformed */
			typmod = exprTypmod(e->val);

			if (IsA(e->val, Const))
			{
				Const *con = (Const *)e->val;

				if (con->constisnull)
					ereport(ERROR,
							(errcode(ERROR_INVALID_WINDOW_FRAME_PARAMETER),
							 errmsg("RANGE parameter cannot be NULL"),
									   errOmitLocation(true)));
			}

			sort = (SortClause *)linitial(spec->order);
			te = getTargetBySortGroupRef(sort->tleSortGroupRef,
										 qry->targetList);
			otype = exprType((Node *)te->expr);
			rtype = exprType(e->val);

			/* XXX: Reverse these if user specified DESC */
			if (e->kind == WINDOW_BOUND_FOLLOWING)
				oprname = lappend(NIL, makeString("+"));
			else
				oprname = lappend(NIL, makeString("-"));

			tup = oper(pstate, oprname, otype, rtype, true, 0);

			if (!HeapTupleIsValid(tup))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("window specification RANGE parameter type "
								"must be coercible to ORDER BY column type"),
										   errOmitLocation(true)));

			oprresult = ((Form_pg_operator)GETSTRUCT(tup))->oprresult;
			newrtype = ((Form_pg_operator)GETSTRUCT(tup))->oprright;
			ReleaseOperator(tup);
			list_free_deep(oprname);

			if (rtype != newrtype)
			{
				/*
				 * We have to coerce the RHS to the new type so that we'll be
				 * able to trivially find an operator later on.
				 */

				/* XXX: we assume that the typmod for the new RHS type
				 * is the same as before... is that safe?
				 */
				Expr *expr =
					(Expr *)coerce_to_target_type(NULL,
												  e->val,
											 	  rtype,
											 	  newrtype, typmod,
									 			  COERCION_EXPLICIT,
												  COERCE_IMPLICIT_CAST,
												  -1);
				if (!PointerIsValid(expr))
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("type mismatch between ORDER BY and RANGE "
									"parameter in window specification"),
							 errhint("Operations between window specification "
									 "the ORDER BY column and RANGE parameter "
									 "must result in a data type which can be "
									 "cast back to the ORDER BY column type"),
											   errOmitLocation(true)));
				}
				else
					e->val = (Node *)expr;
			}

			if (oprresult != otype)
			{
				/* see if it can be coerced */
				if (!can_coerce_type(1, &oprresult, &otype, COERCION_EXPLICIT))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("invalid RANGE parameter"),
							 errhint("Operations between window specification "
									 "the ORDER BY column and RANGE parameter "
									 "must result in a data type which can be "
									 "cast back to the ORDER BY column type"),
											   errOmitLocation(true)));

				/* The executor will do the rest of the work */
			}

			if (IsA(e->val, Const))
			{
				/* see if RANGE parameter is negative */
				tup = ordering_oper(newrtype, false);
				if (HeapTupleIsValid(tup))
				{
					Type typ = typeidType(newrtype);
					Oid funcoid = oprfuncid(tup);
					Datum zero;
					Datum result;
					Const *con = (Const *)e->val;

					zero = stringTypeDatum(typ, "0", exprTypmod(e->val));

					/*
					 * As we know the value is a const and since transformExpr()
					 * will have parsed the type into its internal format, we can
					 * just poke directly into the Const structure.
					 */
					result = OidFunctionCall2(funcoid, con->constvalue, zero);

					if (result)
						ereport(ERROR,
								(errcode(ERROR_INVALID_WINDOW_FRAME_PARAMETER),
								 errmsg("RANGE parameter cannot be negative"),
										   errOmitLocation(true)));

					ReleaseOperator(tup);
					ReleaseType(typ);
				}
			}
		}
	}
}

/*
 * winref_checkspec_walker
 */
static bool
winref_checkspec_walker(Node *node, void *ctx)
{
	winref_check_ctx *ref = (winref_check_ctx *)ctx;

	if (!node)
		return false;
	else if (IsA(node, WindowRef))
	{
		WindowRef *winref = (WindowRef *)node;

		/*
		 * Look at functions pointing to the interesting spec only.
		 */
		if (winref->winspec != ref->win_clause)
			return false;

		if (winref->windistinct)
		{
			if (ref->has_order)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("DISTINCT cannot be used with "
								"window specification containing an "
								"ORDER BY clause"),
										   errOmitLocation(true)));

			if (ref->has_frame)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("DISTINCT cannot be used with "
								"window specification containing a "
								"framing clause"),
										   errOmitLocation(true)));
		}

		/*
		 * Check compatibilities between function's requirement and
		 * window specification by looking up pg_window catalog.
		 */
		if (!ref->has_order || ref->has_frame)
		{
			HeapTuple		tuple;
			Form_pg_window	wf;
			cqContext	   *pcqCtx;

			pcqCtx = caql_beginscan(
					NULL,
					cql("SELECT * FROM pg_window "
						" WHERE winfnoid = :1 ",
						ObjectIdGetDatum(winref->winfnoid)));

			tuple = caql_getnext(pcqCtx);

			/*
			 * Check only "true" window function.
			 * Otherwise, it must be an aggregate.
			 */
			if (HeapTupleIsValid(tuple))
			{
				wf = (Form_pg_window) GETSTRUCT(tuple);
				if (wf->winrequireorder && !ref->has_order)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("window function \"%s\" requires a window "
									"specification with an ordering clause",
									get_func_name(wf->winfnoid)),
								parser_errposition(ref->pstate, winref->location)));

				if (!wf->winallowframe && ref->has_frame)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("window function \"%s\" cannot be used with "
									"a framed window specification",
									get_func_name(wf->winfnoid)),
								parser_errposition(ref->pstate, winref->location)));
			}
			caql_endscan(pcqCtx);
		}
	}

	return expression_tree_walker(node, winref_checkspec_walker, ctx);
}

/*
 * winref_checkspec
 *
 * See if any WindowRefs using this spec are DISTINCT qualified.
 *
 * In addition, we're going to check winrequireorder / winallowframe.
 * You might want to do it in ParseFuncOrColumn,
 * but we need to do this here after all the transformations
 * (especially parent inheritance) was done.
 */
static bool
winref_checkspec(ParseState *pstate, Query *qry, Index clauseno,
				 bool has_order, bool has_frame)
{
	winref_check_ctx ctx;

	ctx.pstate = pstate;
	ctx.win_clause = clauseno;
	ctx.has_order = has_order;
	ctx.has_frame = has_frame;

	return expression_tree_walker((Node *) qry->targetList,
								  winref_checkspec_walker, (void *) &ctx);
}

/*
 * transformWindowClause
 * 		Process window clause specifications in a SELECT query
 *
 * There's a fair bit to do here: column references in the PARTITION and
 * ORDER clauses must be valid; ORDER clause must present if the function
 * requires; the frame clause must be checked to ensure that the function
 * supports framing, that the framed column is of the right type, that the
 * offset is sane, that the start and end of the frame are sane.
 * Then we translate it to use the proper parse nodes for the respective
 * part of the clause.
 */
void
transformWindowClause(ParseState *pstate, Query *qry)
{
	ListCell *w;
	List *winout = NIL;
	List *winin = pstate->p_win_clauses;
	Index clauseno = -1;

	/*
	 * We have two lists of window specs: one in the ParseState -- put there
	 * when we find the OVER(...) clause in the targetlist and the other
	 * is windowClause, a list of named window clauses. So, we concatenate
	 * them together.
	 *
	 * Note that we're careful place those found in the target list at
	 * the end because the spec might refer to a named clause and we'll
	 * after to know about those first.
	 */

	foreach(w, winin)
	{
		WindowSpec *ws = lfirst(w);
		WindowSpec *newspec = makeNode(WindowSpec);
		ListCell   *tmp;
		bool		found = false;

		clauseno++;

        /* Include this WindowSpec's location in error messages. */
        pstate->p_breadcrumb.node = (Node *)ws;

        if (checkExprHasWindFuncs((Node *)ws))
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cannot use window function in a window "
							"specification"),
									   errOmitLocation(true)));
		}
		/*
		 * Loop through those clauses we've already processed to
		 * a) check that the name passed in is not already taken and
		 * b) look up the parent window spec.
		 *
		 * This is obvious O(n^2) but n is small.
		 */
		if (ws->parent || ws->name)
		{
			/*
			 * Firstly, check that the parent is not a reference to this
			 * window specification.
			 */
			if (ws->parent && ws->name && strcmp(ws->parent, ws->name) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("window \"%s\" cannot reference itself",
								ws->name),
										   errOmitLocation(true)));

			foreach(tmp, winout)
			{
				WindowSpec *ws2 = lfirst(tmp);

				/* Only check if the name exists if wc->name is not NULL */
				if (ws->name != NULL && strcmp(ws2->name, ws->name) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("window name \"%s\" occurs more than once "
									"in WINDOW clause", ws->name),
											   errOmitLocation(true)));

				/*
				 * If this spec has a parent reference, we need to test that
				 * the following rules are met. Given the following:
				 *
				 * 		OVER (myspec ...) ... WINDOW myspec AS (...)
				 *
				 * the OVER clause cannot have a partitioning clause; only
				 * the OVER clause or myspec can have an ORDER clause; myspec
				 * cannot have a framing clause.
				 */

				/*
				 * XXX: these errors could apply to any number of clauses in the
				 * query and may be considered ambiguous by the user. Perhaps a
				 * location (see FuncCall) would help?
				 */
				if (ws->parent && ws2->name &&
					strcmp(ws->parent, ws2->name) == 0)
				{
					found = true;
					if (ws->partition != NIL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("PARTITION BY not allowed when "
										"an existing window name is specified"),
												   errOmitLocation(true)));

					if (ws->order != NIL && ws2->order != NIL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("conflicting ORDER BY clauses in window "
										"specification"),
												   errOmitLocation(true)));

					/*
					 * We only want to disallow the specification of a
					 * framing clause when the target list form is like:
					 *
					 *  foo() OVER (w1 ORDER BY baz) ...
					 */
					if (!(ws->partition == NIL && ws->order == NIL &&
						  ws->name == NULL) &&
						ws2->frame)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("window specification \"%s\" cannot have "
										"a framing clause", ws2->name),
								 errhint("Window specifications which are "
										 "referenced by other window "
										 "specifications cannot have framing "
										 "clauses"),
								 errOmitLocation(true),
                                 parser_errposition(pstate, ws2->location)
                                 ));

					/*
					 * The specifications are valid so just copy the details
					 * from the parent spec.
					 */
					newspec->parent = ws2->name;
					/* XXX: some parameters might not be processed! */
					newspec->partition = copyObject(ws2->partition);

					if (ws->order == NIL && ws2->order != NIL)
						newspec->order = copyObject(ws2->order);

					newspec->frame = copyObject(ws2->frame);

				}
			}

			if(!found && ws->parent)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("window specification \"%s\" not found",
							   ws->parent),
									   errOmitLocation(true)));
		}

		newspec->name = ws->name;
		newspec->location = ws->location;

		/*
		 * Process partition, if one is defined and if it isn't already
		 * in newspec.
		 */
		if (!newspec->partition && ws->partition)
		{
			newspec->partition = 
                transformSortClause(pstate,
                                    ws->partition,
                                    &qry->targetList,
                                    true /* fix unknowns */ ,
                                    false /* use SQL92 rules */ );
		}
		/* order is just like partition */
		if (ws->order || newspec->order)
		{
			/*
			 * Only do this if it came from the new definition
			 */
			if (ws->order != NIL && newspec->order == NIL)
			{
				newspec->order = 
                    transformSortClause(pstate,
                                        ws->order,
                                        &qry->targetList,
                                        true /* fix unknowns */ ,
                                        false /* use SQL92 rules */ );
			}
		}

        /* Refresh our breadcrumb in case transformSortClause stepped on it. */
        pstate->p_breadcrumb.node = (Node *)ws;

		/*
		 * Finally, process the framing clause. parseProcessWindFunc() will
		 * have picked up window functions that do not support framing.
		 *
		 * What we do need to do is the following:
		 * - If BETWEEN has been specified, the trailing bound is not
		 *   UNBOUNDED FOLLOWING; the leading bound is not UNBOUNDED
		 *   PRECEDING; if the first bound specifies CURRENT ROW, the
		 *   second bound shall not specify a PRECEDING bound; if the
		 *   first bound specifies a FOLLOWING bound, the second bound
		 *   shall not specify a PRECEDING or CURRENT ROW bound.
		 *
		 * - If the user did not specify BETWEEN, the bound is assumed to be
		 *   a trailing bound and the leading bound is set to CURRENT ROW.
		 *   We're careful not to set is_between here because the user did not
		 *   specify it.
		 *
		 * - If RANGE is specified: the ORDER BY clause of the window spec
		 *   may specify only one column; the type of that column must support
		 *   +/- <integer> operations and must be merge-joinable.
		 */
		if (ws->frame)
		{
			/* with that out of the way, we may proceed */
			WindowFrame *nf = copyObject(ws->frame);

			/*
			 * Framing is only supported on specifications with an ordering
			 * clause.
			 */
			if (!ws->order && !newspec->order)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("window specifications with a framing clause "
								"must have an ORDER BY clause"),
										   errOmitLocation(true)));

			if (nf->is_between)
			{
				Assert(PointerIsValid(nf->trail));
				Assert(PointerIsValid(nf->lead));

				if (nf->trail->kind == WINDOW_UNBOUND_FOLLOWING)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting bounds in window framing "
									"clause"),
							 errhint("First bound of BETWEEN clause in window "
									"specification cannot be UNBOUNDED FOLLOWING"),
											   errOmitLocation(true)));
				if (nf->lead->kind == WINDOW_UNBOUND_PRECEDING)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting bounds in window framing "
									"clause"),
							 errhint("Second bound of BETWEEN clause in window "
									"specification cannot be UNBOUNDED PRECEDING"),
											   errOmitLocation(true)));
				if (nf->trail->kind == WINDOW_CURRENT_ROW &&
					(nf->lead->kind == WINDOW_BOUND_PRECEDING ||
					 nf->lead->kind == WINDOW_UNBOUND_PRECEDING))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting bounds in window framing "
									"clause"),
							 errhint("Second bound cannot be PRECEDING "
									 "when first bound is CURRENT ROW"),
											   errOmitLocation(true)));
				if ((nf->trail->kind == WINDOW_BOUND_FOLLOWING ||
					 nf->trail->kind == WINDOW_UNBOUND_FOLLOWING) &&
					!(nf->lead->kind == WINDOW_BOUND_FOLLOWING ||
					  nf->lead->kind == WINDOW_UNBOUND_FOLLOWING))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting bounds in window framing "
									"clause"),
							 errhint("Second bound must be FOLLOWING if first "
									 "bound is FOLLOWING"),
											   errOmitLocation(true)));

			}
			else
			{
				/*
				 * If only a single bound has been specified, set the
				 * leading bound to CURRENT ROW
				 */
				WindowFrameEdge *e = makeNode(WindowFrameEdge);

				Assert(!PointerIsValid(nf->lead));

				e->kind = WINDOW_CURRENT_ROW;
				nf->lead = e;
			}

			transformWindowFrameEdge(pstate, nf->trail, newspec, qry,
									 nf->is_rows);
			transformWindowFrameEdge(pstate, nf->lead, newspec, qry,
									 nf->is_rows);
			newspec->frame = nf;
		}

		/* finally, check function restriction with this spec. */
		winref_checkspec(pstate, qry, clauseno,
						 PointerIsValid(newspec->order),
						 PointerIsValid(newspec->frame));

		winout = lappend(winout, newspec);
	}

	/* If there are no window functions in the targetlist,
	 * forget the window clause.
	 */
	if (!pstate->p_hasWindFuncs)
	{
		pstate->p_win_clauses = NIL;
		qry->windowClause = NIL;
	}
	else
	{
		qry->windowClause = winout;
	}
}

/*
 * setTargetTable
 *	  Add the target relation of INSERT/UPDATE/DELETE to the range table,
 *	  and make the special links to it in the ParseState.
 *
 *	  We also open the target relation and acquire a write lock on it.
 *	  This must be done before processing the FROM list, in case the target
 *	  is also mentioned as a source relation --- we want to be sure to grab
 *	  the write lock before any read lock.
 *
 *	  If alsoSource is true, add the target to the query's joinlist and
 *	  namespace.  For INSERT, we don't want the target to be joined to;
 *	  it's a destination of tuples, not a source.	For UPDATE/DELETE,
 *	  we do need to scan or join the target.  (NOTE: we do not bother
 *	  to check for namespace conflict; we assume that the namespace was
 *	  initially empty in these cases.)
 *
 *	  Finally, we mark the relation as requiring the permissions specified
 *	  by requiredPerms.
 *
 *	  Returns the rangetable index of the target relation.
 */
int
setTargetTable(ParseState *pstate, RangeVar *relation,
			   bool inh, bool alsoSource, AclMode requiredPerms)
{
	RangeTblEntry *rte;
	int			rtindex;

	/* Close old target; this could only happen for multi-action rules */
	if (pstate->p_target_relation != NULL)
		heap_close(pstate->p_target_relation, NoLock);

	/*
	 * Open target rel and grab suitable lock (which we will hold till end of
	 * transaction).
	 *
	 * analyze.c will eventually do the corresponding heap_close(), but *not*
	 * release the lock.
     *
     * CDB: Acquire ExclusiveLock if it is a distributed relation and we are
     * doing UPDATE or DELETE activity
	 */
	if (pstate->p_is_insert && !pstate->p_is_update)
	{
		pstate->p_target_relation = heap_openrv(relation, RowExclusiveLock);
	}
	else
	{
    	pstate->p_target_relation = CdbOpenRelationRv(relation, 
													  RowExclusiveLock, 
													  false, NULL);
	}
	
	/*
	 * Now build an RTE.
	 */
	rte = addRangeTableEntryForRelation(pstate, pstate->p_target_relation,
										relation->alias, inh, false);
	pstate->p_target_rangetblentry = rte;

	/* assume new rte is at end */
	rtindex = list_length(pstate->p_rtable);
	Assert(rte == rt_fetch(rtindex, pstate->p_rtable));

	/*
	 * Special check for DML on system relations,
	 * allow DML when:
	 * 	- in single user mode: initdb insert PIN entries to pg_depend,...
	 * 	- in maintenance mode, upgrade mode or
	 *  - allow_system_table_mods = dml
	 */
	if (IsUnderPostmaster && !allowSystemTableModsDML
		&& IsSystemRelation(pstate->p_target_relation))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("permission denied: \"%s\" is a system catalog",
						 RelationGetRelationName(pstate->p_target_relation)),
				 errOmitLocation(true)));

	/* special check for DML on foreign relations */
	if(RelationIsForeign(pstate->p_target_relation))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("foreign tables are read only. cannot change \"%s\"",
						 RelationGetRelationName(pstate->p_target_relation)),
				 errOmitLocation(true)));


	/* special check for DML on external relations */
	if(RelationIsExternal(pstate->p_target_relation))
	{
		if (requiredPerms != ACL_INSERT)
		{
			/* UPDATE/DELETE */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot update or delete from external relation \"%s\"",
							RelationGetRelationName(pstate->p_target_relation)),
					 errOmitLocation(true)));
		}
		else
		{
			/* INSERT */
			Oid reloid = RelationGetRelid(pstate->p_target_relation);
			ExtTableEntry* 	extentry;
			
			extentry = GetExtTableEntry(reloid);
			
			if(!extentry->iswritable)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("cannot change a readable external table \"%s\"",
								 RelationGetRelationName(pstate->p_target_relation)),
						 errOmitLocation(true)));

			pfree(extentry);
		}
	}
	

	/*
	 * Override addRangeTableEntry's default ACL_SELECT permissions check, and
	 * instead mark target table as requiring exactly the specified
	 * permissions.
	 *
	 * If we find an explicit reference to the rel later during parse
	 * analysis, we will add the ACL_SELECT bit back again; see
	 * scanRTEForColumn (for simple field references), ExpandColumnRefStar
	 * (for foo.*) and ExpandAllTables (for *).
	 */
	rte->requiredPerms = requiredPerms;

	/*
	 * If UPDATE/DELETE, add table to joinlist and namespaces.
	 */
	if (alsoSource)
		addRTEtoQuery(pstate, rte, true, true, true);

	return rtindex;
}

/*
 * Simplify InhOption (yes/no/default) into boolean yes/no.
 *
 * The reason we do things this way is that we don't want to examine the
 * SQL_inheritance option flag until parse_analyze is run.	Otherwise,
 * we'd do the wrong thing with query strings that intermix SET commands
 * with queries.
 */
bool
interpretInhOption(InhOption inhOpt)
{
	switch (inhOpt)
	{
		case INH_NO:
			return false;
		case INH_YES:
			return true;
		case INH_DEFAULT:
			return SQL_inheritance;
	}
	elog(ERROR, "bogus InhOption value: %d", inhOpt);
	return false;				/* keep compiler quiet */
}

/*
 * Given a relation-options list (of DefElems), return true iff the specified
 * table/result set should be created with OIDs. This needs to be done after
 * parsing the query string because the return value can depend upon the
 * default_with_oids GUC var.
 */
bool
interpretOidsOption(List *defList)
{
	ListCell   *cell;

	/* Scan list to see if OIDS was included */
	foreach(cell, defList)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (pg_strcasecmp(def->defname, "oids") == 0)
			return defGetBoolean(def);
	}

	/* OIDS option was not specified, so use default. */
	return default_with_oids;
}

/*
 * Extract all not-in-common columns from column lists of a source table
 */
static void
extractRemainingColumns(List *common_colnames,
						List *src_colnames, List *src_colvars,
						List **res_colnames, List **res_colvars)
{
	List	   *new_colnames = NIL;
	List	   *new_colvars = NIL;
	ListCell   *lnames,
			   *lvars;

	Assert(list_length(src_colnames) == list_length(src_colvars));

	forboth(lnames, src_colnames, lvars, src_colvars)
	{
		char	   *colname = strVal(lfirst(lnames));
		bool		match = false;
		ListCell   *cnames;

		foreach(cnames, common_colnames)
		{
			char	   *ccolname = strVal(lfirst(cnames));

			if (strcmp(colname, ccolname) == 0)
			{
				match = true;
				break;
			}
		}

		if (!match)
		{
			new_colnames = lappend(new_colnames, lfirst(lnames));
			new_colvars = lappend(new_colvars, lfirst(lvars));
		}
	}

	*res_colnames = new_colnames;
	*res_colvars = new_colvars;
}

/* transformJoinUsingClause()
 *	  Build a complete ON clause from a partially-transformed USING list.
 *	  We are given lists of nodes representing left and right match columns.
 *	  Result is a transformed qualification expression.
 */
static Node *
transformJoinUsingClause(ParseState *pstate, List *leftVars, List *rightVars)
{
	Node	   *result = NULL;
	ListCell   *lvars,
			   *rvars;

	/*
	 * We cheat a little bit here by building an untransformed operator tree
	 * whose leaves are the already-transformed Vars.  This is OK because
	 * transformExpr() won't complain about already-transformed subnodes.
	 */
	forboth(lvars, leftVars, rvars, rightVars)
	{
		Node	   *lvar = (Node *) lfirst(lvars);
		Node	   *rvar = (Node *) lfirst(rvars);
		A_Expr	   *e;

		e = makeSimpleA_Expr(AEXPR_OP, "=",
							 copyObject(lvar), copyObject(rvar),
							 -1);

		if (result == NULL)
			result = (Node *) e;
		else
		{
			A_Expr	   *a;

			a = makeA_Expr(AEXPR_AND, NIL, result, (Node *) e, -1);
			result = (Node *) a;
		}
	}

	/*
	 * Since the references are already Vars, and are certainly from the input
	 * relations, we don't have to go through the same pushups that
	 * transformJoinOnClause() does.  Just invoke transformExpr() to fix up
	 * the operators, and we're done.
	 */
	result = transformExpr(pstate, result);

	result = coerce_to_boolean(pstate, result, "JOIN/USING");

	return result;
}

/* transformJoinOnClause()
 *	  Transform the qual conditions for JOIN/ON.
 *	  Result is a transformed qualification expression.
 */
static Node *
transformJoinOnClause(ParseState *pstate, JoinExpr *j,
					  RangeTblEntry *l_rte,
					  RangeTblEntry *r_rte,
					  List *relnamespace,
					  Relids containedRels)
{
	Node	   *result;
	List	   *save_relnamespace;
	List	   *save_varnamespace;
	Relids		clause_varnos;
	int			varno;

	/*
	 * This is a tad tricky, for two reasons.  First, the namespace that the
	 * join expression should see is just the two subtrees of the JOIN plus
	 * any outer references from upper pstate levels.  So, temporarily set
	 * this pstate's namespace accordingly.  (We need not check for refname
	 * conflicts, because transformFromClauseItem() already did.) NOTE: this
	 * code is OK only because the ON clause can't legally alter the namespace
	 * by causing implicit relation refs to be added.
	 */
	save_relnamespace = pstate->p_relnamespace;
	save_varnamespace = pstate->p_varnamespace;

	pstate->p_relnamespace = relnamespace;
	pstate->p_varnamespace = list_make2(l_rte, r_rte);

	result = transformWhereClause(pstate, j->quals, "JOIN/ON");

	pstate->p_relnamespace = save_relnamespace;
	pstate->p_varnamespace = save_varnamespace;

	/*
	 * Second, we need to check that the ON condition doesn't refer to any
	 * rels outside the input subtrees of the JOIN.  It could do that despite
	 * our hack on the namespace if it uses fully-qualified names. So, grovel
	 * through the transformed clause and make sure there are no bogus
	 * references.	(Outer references are OK, and are ignored here.)
	 */
	clause_varnos = pull_varnos(result);
	clause_varnos = bms_del_members(clause_varnos, containedRels);
	if ((varno = bms_first_member(clause_varnos)) >= 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		 errmsg("JOIN/ON clause refers to \"%s\", which is not part of JOIN",
				rt_fetch(varno, pstate->p_rtable)->eref->aliasname)));
	}
	bms_free(clause_varnos);

	return result;
}

/*
 * transformTableEntry --- transform a RangeVar (simple relation reference)
 */
static RangeTblEntry *
transformTableEntry(ParseState *pstate, RangeVar *r)
{
	RangeTblEntry *rte;

	/*
	 * mark this entry to indicate it comes from the FROM clause. In SQL, the
	 * target list can only refer to range variables specified in the from
	 * clause but we follow the more powerful POSTQUEL semantics and
	 * automatically generate the range variable if not specified. However
	 * there are times we need to know whether the entries are legitimate.
	 */
	rte = addRangeTableEntry(pstate, r, r->alias,
							 interpretInhOption(r->inhOpt), true);

	return rte;
}


/*
 * transformRangeSubselect --- transform a sub-SELECT appearing in FROM
 */
static RangeTblEntry *
transformRangeSubselect(ParseState *pstate, RangeSubselect *r)
{
	List	   *parsetrees;
	Query	   *query;
	RangeTblEntry *rte;

	/*
	 * We require user to supply an alias for a subselect, per SQL92. To relax
	 * this, we'd have to be prepared to gin up a unique alias for an
	 * unlabeled subselect.
	 */
	if (r->alias == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("subquery in FROM must have an alias")));

	/*
	 * Analyze and transform the subquery.
	 */
	parsetrees = parse_sub_analyze(r->subquery, pstate);

	/*
	 * Check that we got something reasonable.	Most of these conditions are
	 * probably impossible given restrictions of the grammar, but check 'em
	 * anyway.
	 */
	if (list_length(parsetrees) != 1)
		elog(ERROR, "unexpected parse analysis result for subquery in FROM");
	query = (Query *) linitial(parsetrees);
	if (query == NULL || !IsA(query, Query))
		elog(ERROR, "unexpected parse analysis result for subquery in FROM");

	if (query->commandType != CMD_SELECT)
		elog(ERROR, "expected SELECT query from subquery in FROM");
	if (query->intoClause != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("subquery in FROM may not have SELECT INTO")));

	/*
	 * The subquery cannot make use of any variables from FROM items created
	 * earlier in the current query.  Per SQL92, the scope of a FROM item does
	 * not include other FROM items.  Formerly we hacked the namespace so that
	 * the other variables weren't even visible, but it seems more useful to
	 * leave them visible and give a specific error message.
	 *
	 * XXX this will need further work to support SQL99's LATERAL() feature,
	 * wherein such references would indeed be legal.
	 *
	 * We can skip groveling through the subquery if there's not anything
	 * visible in the current query.  Also note that outer references are OK.
	 */
	if (pstate->p_relnamespace || pstate->p_varnamespace)
	{
		if (contain_vars_of_level((Node *) query, 1))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("subquery in FROM may not refer to other relations of same query level"),
							 errOmitLocation(true)));
	}

	/*
	 * OK, build an RTE for the subquery.
	 */
	rte = addRangeTableEntryForSubquery(pstate, query, r->alias, true);

	return rte;
}


/*
 * transformRangeFunction --- transform a function call appearing in FROM
 */
static RangeTblEntry *
transformRangeFunction(ParseState *pstate, RangeFunction *r)
{
	Node	   *funcexpr;
	char	   *funcname;
	RangeTblEntry *rte;

	/*
	 * Get function name for possible use as alias.  We use the same
	 * transformation rules as for a SELECT output expression.	For a FuncCall
	 * node, the result will be the function name, but it is possible for the
	 * grammar to hand back other node types.
	 */
	funcname = FigureColname(r->funccallnode);

	if (funcname)
	{
		if (pg_strncasecmp(funcname, GP_DIST_RANDOM_NAME, sizeof(GP_DIST_RANDOM_NAME)) == 0)
		{
			/* OK, now we need to check the arguments and generate a RTE */
			FuncCall *fc;
			RangeVar *rel;

			fc = (FuncCall *)r->funccallnode;

			if (list_length(fc->args) != 1)
				elog(ERROR, "Invalid %s syntax.", GP_DIST_RANDOM_NAME);

			if (IsA(linitial(fc->args), A_Const))
			{
				A_Const *arg_val;
				char *schemaname;
				char *tablename;

				arg_val = linitial(fc->args);
				if (!IsA(&arg_val->val, String))
				{
					elog(ERROR, "%s: invalid argument type, non-string in value", GP_DIST_RANDOM_NAME);
				}

				schemaname = strVal(&arg_val->val);
				tablename = strchr(schemaname, '.');
				if (tablename)
				{
					*tablename = 0;
					tablename++;
				}
				else
				{
					/* no schema */
					tablename = schemaname;
					schemaname = NULL;
				}

				/* Got the name of the table, now we need to build the RTE for the table. */
				rel = makeRangeVar(NULL /*catalogname*/, schemaname, tablename, arg_val->location);
				rel->location = arg_val->location;

				rte = addRangeTableEntry(pstate, rel, r->alias, false, true);

				/* Now we set our special attribute in the rte. */
				rte->forceDistRandom = true;

				return rte;
			}
			else
			{
				elog(ERROR, "%s: invalid argument type", GP_DIST_RANDOM_NAME);
			}
		}
	}

	/*
	 * Transform the raw expression.
	 */
	funcexpr = transformExpr(pstate, r->funccallnode);

	/*
	 * The function parameters cannot make use of any variables from other
	 * FROM items.	(Compare to transformRangeSubselect(); the coding is
	 * different though because we didn't parse as a sub-select with its own
	 * level of namespace.)
	 *
	 * XXX this will need further work to support SQL99's LATERAL() feature,
	 * wherein such references would indeed be legal.
	 */
	if (pstate->p_relnamespace || pstate->p_varnamespace)
	{
		if (contain_vars_of_level(funcexpr, 0))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("function expression in FROM may not refer to other relations of same query level"),
					 errOmitLocation(true)));
	}

	/*
	 * Disallow aggregate functions in the expression.	(No reason to postpone
	 * this check until parseCheckAggregates.)
	 */
	if (pstate->p_hasAggs)
	{
		if (checkExprHasAggs(funcexpr))
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
					 errmsg("cannot use aggregate function in function expression in FROM"),
					 errOmitLocation(true)));
	}

	/*
	 * OK, build an RTE for the function.
	 */
	rte = addRangeTableEntryForFunction(pstate, funcname, funcexpr,
										r, true);

	/*
	 * If a coldeflist was supplied, ensure it defines a legal set of names
	 * (no duplicates) and datatypes (no pseudo-types, for instance).
	 * addRangeTableEntryForFunction looked up the type names but didn't check
	 * them further than that.
	 */
	if (r->coldeflist)
	{
		TupleDesc	tupdesc;

		tupdesc = BuildDescFromLists(rte->eref->colnames,
									 rte->funccoltypes,
									 rte->funccoltypmods);
		CheckAttributeNamesTypes(tupdesc, RELKIND_COMPOSITE_TYPE);
	}

	return rte;
}


/*
 * transformFromClauseItem -
 *	  Transform a FROM-clause item, adding any required entries to the
 *	  range table list being built in the ParseState, and return the
 *	  transformed item ready to include in the joinlist and namespaces.
 *	  This routine can recurse to handle SQL92 JOIN expressions.
 *
 * The function return value is the node to add to the jointree (a
 * RangeTblRef or JoinExpr).  Additional output parameters are:
 *
 * *top_rte: receives the RTE corresponding to the jointree item.
 * (We could extract this from the function return node, but it saves cycles
 * to pass it back separately.)
 *
 * *top_rti: receives the rangetable index of top_rte.	(Ditto.)
 *
 * *relnamespace: receives a List of the RTEs exposed as relation names
 * by this item.
 *
 * *containedRels: receives a bitmap set of the rangetable indexes
 * of all the base and join relations represented in this jointree item.
 * This is needed for checking JOIN/ON conditions in higher levels.
 *
 * We do not need to pass back an explicit varnamespace value, because
 * in all cases the varnamespace contribution is exactly top_rte.
 */
static Node *
transformFromClauseItem(ParseState *pstate, Node *n,
						RangeTblEntry **top_rte, int *top_rti,
						List **relnamespace,
						Relids *containedRels)
{
    Node                   *result;
    ParseStateBreadCrumb    savebreadcrumb;

    /* CDB: Push error location stack.  Must pop before return! */
    Assert(pstate);
    savebreadcrumb = pstate->p_breadcrumb;
    pstate->p_breadcrumb.pop = &savebreadcrumb;
    pstate->p_breadcrumb.node = n;

	if (IsA(n, RangeVar))
	{
		/* Plain relation reference */
		RangeTblRef *rtr;
		RangeTblEntry *rte = NULL;
		int	rtindex;
		RangeVar *rangeVar = (RangeVar *)n;

		/*
		 * If it is an unqualified name, it might be a CTE reference.
		 */
		if (rangeVar->schemaname == NULL)
		{
			CommonTableExpr *cte;
			Index levelsup;

			cte = scanNameSpaceForCTE(pstate, rangeVar->relname, &levelsup);
			if (cte)
			{
				rte = addRangeTableEntryForCTE(pstate, cte, levelsup, rangeVar, true);
			}
		}

		/* If it is not a CTE reference, it must be a simple relation reference. */
		if (rte == NULL)
		{
			rte = transformTableEntry(pstate, rangeVar);
		}
		
		/* assume new rte is at end */
		rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
		*top_rte = rte;
		*top_rti = rtindex;
		*relnamespace = list_make1(rte);
		*containedRels = bms_make_singleton(rtindex);
		rtr = makeNode(RangeTblRef);
		rtr->rtindex = rtindex;
		result = (Node *) rtr;
	}
	else if (IsA(n, RangeSubselect))
	{
		/* sub-SELECT is like a plain relation */
		RangeTblRef *rtr;
		RangeTblEntry *rte;
		int			rtindex;

		rte = transformRangeSubselect(pstate, (RangeSubselect *) n);
		/* assume new rte is at end */
		rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
		*top_rte = rte;
		*top_rti = rtindex;
		*relnamespace = list_make1(rte);
		*containedRels = bms_make_singleton(rtindex);
		rtr = makeNode(RangeTblRef);
		rtr->rtindex = rtindex;
		result = (Node *) rtr;
	}
	else if (IsA(n, RangeFunction))
	{
		/* function is like a plain relation */
		RangeTblRef *rtr;
		RangeTblEntry *rte;
		int			rtindex;

		rte = transformRangeFunction(pstate, (RangeFunction *) n);
		/* assume new rte is at end */
		rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
		*top_rte = rte;
		*top_rti = rtindex;
		*relnamespace = list_make1(rte);
		*containedRels = bms_make_singleton(rtindex);
		rtr = makeNode(RangeTblRef);
		rtr->rtindex = rtindex;
		result = (Node *) rtr;
	}
	else if (IsA(n, JoinExpr))
	{
		/* A newfangled join expression */
		JoinExpr   *j = (JoinExpr *) n;
		RangeTblEntry *l_rte;
		RangeTblEntry *r_rte;
		int			l_rtindex;
		int			r_rtindex;
		Relids		l_containedRels,
					r_containedRels,
					my_containedRels;
		List	   *l_relnamespace,
				   *r_relnamespace,
				   *my_relnamespace,
				   *l_colnames,
				   *r_colnames,
				   *res_colnames,
				   *l_colvars,
				   *r_colvars,
				   *res_colvars;
		RangeTblEntry *rte;

		/*
		 * Recursively process the left and right subtrees
		 */
		j->larg = transformFromClauseItem(pstate, j->larg,
										  &l_rte,
										  &l_rtindex,
										  &l_relnamespace,
										  &l_containedRels);
		j->rarg = transformFromClauseItem(pstate, j->rarg,
										  &r_rte,
										  &r_rtindex,
										  &r_relnamespace,
										  &r_containedRels);

		/*
		 * Check for conflicting refnames in left and right subtrees. Must do
		 * this because higher levels will assume I hand back a self-
		 * consistent namespace subtree.
		 */
		checkNameSpaceConflicts(pstate, l_relnamespace, r_relnamespace);

		/*
		 * Generate combined relation membership info for possible use by
		 * transformJoinOnClause below.
		 */
		my_relnamespace = list_concat(l_relnamespace, r_relnamespace);
		my_containedRels = bms_join(l_containedRels, r_containedRels);

		pfree(r_relnamespace);	/* free unneeded list header */

		/*
		 * Extract column name and var lists from both subtrees
		 *
		 * Note: expandRTE returns new lists, safe for me to modify
		 */
		expandRTE(l_rte, l_rtindex, 0, false, -1,
				  &l_colnames, &l_colvars);
		expandRTE(r_rte, r_rtindex, 0, false, -1,
				  &r_colnames, &r_colvars);

		/*
		 * Natural join does not explicitly specify columns; must generate
		 * columns to join. Need to run through the list of columns from each
		 * table or join result and match up the column names. Use the first
		 * table, and check every column in the second table for a match.
		 * (We'll check that the matches were unique later on.) The result of
		 * this step is a list of column names just like an explicitly-written
		 * USING list.
		 */
		if (j->isNatural)
		{
			List	   *rlist = NIL;
			ListCell   *lx,
					   *rx;

			Assert(j->usingClause == NIL);	/* shouldn't have USING() too */

			foreach(lx, l_colnames)
			{
				char	   *l_colname = strVal(lfirst(lx));
				Value	   *m_name = NULL;

				foreach(rx, r_colnames)
				{
					char	   *r_colname = strVal(lfirst(rx));

					if (strcmp(l_colname, r_colname) == 0)
					{
						m_name = makeString(l_colname);
						break;
					}
				}

				/* matched a right column? then keep as join column... */
				if (m_name != NULL)
					rlist = lappend(rlist, m_name);
			}

			j->usingClause = rlist;
		}

		/*
		 * Now transform the join qualifications, if any.
		 */
		res_colnames = NIL;
		res_colvars = NIL;

		if (j->usingClause)
		{
			/*
			 * JOIN/USING (or NATURAL JOIN, as transformed above). Transform
			 * the list into an explicit ON-condition, and generate a list of
			 * merged result columns.
			 */
			List	   *ucols = j->usingClause;
			List	   *l_usingvars = NIL;
			List	   *r_usingvars = NIL;
			ListCell   *ucol;

			Assert(j->quals == NULL);	/* shouldn't have ON() too */

			foreach(ucol, ucols)
			{
				char	   *u_colname = strVal(lfirst(ucol));
				ListCell   *col;
				int			ndx;
				int			l_index = -1;
				int			r_index = -1;
				Var		   *l_colvar,
						   *r_colvar;

				/* Check for USING(foo,foo) */
				foreach(col, res_colnames)
				{
					char	   *res_colname = strVal(lfirst(col));

					if (strcmp(res_colname, u_colname) == 0)
						ereport(ERROR,
								(errcode(ERRCODE_DUPLICATE_COLUMN),
								 errmsg("column name \"%s\" appears more than once in USING clause",
										u_colname)));
				}

				/* Find it in left input */
				ndx = 0;
				foreach(col, l_colnames)
				{
					char	   *l_colname = strVal(lfirst(col));

					if (strcmp(l_colname, u_colname) == 0)
					{
						if (l_index >= 0)
							ereport(ERROR,
									(errcode(ERRCODE_AMBIGUOUS_COLUMN),
									 errmsg("common column name \"%s\" appears more than once in left table",
											u_colname)));
						l_index = ndx;
					}
					ndx++;
				}
				if (l_index < 0)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" specified in USING clause does not exist in left table",
									u_colname)));

				/* Find it in right input */
				ndx = 0;
				foreach(col, r_colnames)
				{
					char	   *r_colname = strVal(lfirst(col));

					if (strcmp(r_colname, u_colname) == 0)
					{
						if (r_index >= 0)
							ereport(ERROR,
									(errcode(ERRCODE_AMBIGUOUS_COLUMN),
									 errmsg("common column name \"%s\" appears more than once in right table",
											u_colname)));
						r_index = ndx;
					}
					ndx++;
				}
				if (r_index < 0)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" specified in USING clause does not exist in right table",
									u_colname)));

				l_colvar = list_nth(l_colvars, l_index);
				l_usingvars = lappend(l_usingvars, l_colvar);
				r_colvar = list_nth(r_colvars, r_index);
				r_usingvars = lappend(r_usingvars, r_colvar);

				res_colnames = lappend(res_colnames, lfirst(ucol));
				res_colvars = lappend(res_colvars,
									  buildMergedJoinVar(pstate,
														 j->jointype,
														 l_colvar,
														 r_colvar));
			}

			j->quals = transformJoinUsingClause(pstate,
												l_usingvars,
												r_usingvars);
		}
		else if (j->quals)
		{
			/* User-written ON-condition; transform it */
			j->quals = transformJoinOnClause(pstate, j,
											 l_rte, r_rte,
											 my_relnamespace,
											 my_containedRels);
		}
		else
		{
			/* CROSS JOIN: no quals */
		}

		/* Add remaining columns from each side to the output columns */
		extractRemainingColumns(res_colnames,
								l_colnames, l_colvars,
								&l_colnames, &l_colvars);
		extractRemainingColumns(res_colnames,
								r_colnames, r_colvars,
								&r_colnames, &r_colvars);
		res_colnames = list_concat(res_colnames, l_colnames);
		res_colvars = list_concat(res_colvars, l_colvars);
		res_colnames = list_concat(res_colnames, r_colnames);
		res_colvars = list_concat(res_colvars, r_colvars);

		/*
		 * Check alias (AS clause), if any.
		 */
		if (j->alias)
		{
			if (j->alias->colnames != NIL)
			{
				if (list_length(j->alias->colnames) > list_length(res_colnames))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("column alias list for \"%s\" has too many entries",
									j->alias->aliasname)));
			}
		}

		/*
		 * Now build an RTE for the result of the join
		 */
		rte = addRangeTableEntryForJoin(pstate,
										res_colnames,
										j->jointype,
										res_colvars,
										j->alias,
										true);

		/* assume new rte is at end */
		j->rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(j->rtindex, pstate->p_rtable));

		*top_rte = rte;
		*top_rti = j->rtindex;

		/*
		 * Prepare returned namespace list.  If the JOIN has an alias then it
		 * hides the contained RTEs as far as the relnamespace goes;
		 * otherwise, put the contained RTEs and *not* the JOIN into
		 * relnamespace.
		 */
		if (j->alias)
		{
			*relnamespace = list_make1(rte);
			list_free(my_relnamespace);
		}
		else
			*relnamespace = my_relnamespace;

		/*
		 * Include join RTE in returned containedRels set
		 */
		*containedRels = bms_add_member(my_containedRels, j->rtindex);

		result = (Node *) j;
	}
	else
    {
        result = NULL;
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(n));
    }

    /* CDB: Pop error location stack. */
    Assert(pstate->p_breadcrumb.pop == &savebreadcrumb);
    pstate->p_breadcrumb = savebreadcrumb;

	return result;
}

/*
 * buildMergedJoinVar -
 *	  generate a suitable replacement expression for a merged join column
 */
static Node *
buildMergedJoinVar(ParseState *pstate, JoinType jointype,
				   Var *l_colvar, Var *r_colvar)
{
	Oid			outcoltype;
	int32		outcoltypmod;
	Node	   *l_node,
			   *r_node,
			   *res_node;

	/*
	 * Choose output type if input types are dissimilar.
	 */
	outcoltype = l_colvar->vartype;
	outcoltypmod = l_colvar->vartypmod;
	if (outcoltype != r_colvar->vartype)
	{
		outcoltype = select_common_type(list_make2_oid(l_colvar->vartype,
													   r_colvar->vartype),
										"JOIN/USING");
		outcoltypmod = -1;		/* ie, unknown */
	}
	else if (outcoltypmod != r_colvar->vartypmod)
	{
		/* same type, but not same typmod */
		outcoltypmod = -1;		/* ie, unknown */
	}

	/*
	 * Insert coercion functions if needed.  Note that a difference in typmod
	 * can only happen if input has typmod but outcoltypmod is -1. In that
	 * case we insert a RelabelType to clearly mark that result's typmod is
	 * not same as input.  We never need coerce_type_typmod.
	 */
	if (l_colvar->vartype != outcoltype)
		l_node = coerce_type(pstate, (Node *) l_colvar, l_colvar->vartype,
							 outcoltype, outcoltypmod,
							 COERCION_IMPLICIT, COERCE_IMPLICIT_CAST,
							 -1);
	else if (l_colvar->vartypmod != outcoltypmod)
		l_node = (Node *) makeRelabelType((Expr *) l_colvar,
										  outcoltype, outcoltypmod,
										  COERCE_IMPLICIT_CAST);
	else
		l_node = (Node *) l_colvar;

	if (r_colvar->vartype != outcoltype)
		r_node = coerce_type(pstate, (Node *) r_colvar, r_colvar->vartype,
							 outcoltype, outcoltypmod,
							 COERCION_IMPLICIT, COERCE_IMPLICIT_CAST,
							 -1);
	else if (r_colvar->vartypmod != outcoltypmod)
		r_node = (Node *) makeRelabelType((Expr *) r_colvar,
										  outcoltype, outcoltypmod,
										  COERCE_IMPLICIT_CAST);
	else
		r_node = (Node *) r_colvar;

	/*
	 * Choose what to emit
	 */
	switch (jointype)
	{
		case JOIN_INNER:

			/*
			 * We can use either var; prefer non-coerced one if available.
			 */
			if (IsA(l_node, Var))
				res_node = l_node;
			else if (IsA(r_node, Var))
				res_node = r_node;
			else
				res_node = l_node;
			break;
		case JOIN_LEFT:
			/* Always use left var */
			res_node = l_node;
			break;
		case JOIN_RIGHT:
			/* Always use right var */
			res_node = r_node;
			break;
		case JOIN_FULL:
			{
				/*
				 * Here we must build a COALESCE expression to ensure that the
				 * join output is non-null if either input is.
				 */
				CoalesceExpr *c = makeNode(CoalesceExpr);

				c->coalescetype = outcoltype;
				c->args = list_make2(l_node, r_node);
				res_node = (Node *) c;
				break;
			}
		default:
			elog(ERROR, "unrecognized join type: %d", (int) jointype);
			res_node = NULL;	/* keep compiler quiet */
			break;
	}

	return res_node;
}


/*
 * transformWhereClause -
 *	  Transform the qualification and make sure it is of type boolean.
 *	  Used for WHERE and allied clauses.
 *
 * constructName does not affect the semantics, but is used in error messages
 */
Node *
transformWhereClause(ParseState *pstate, Node *clause,
					 const char *constructName)
{
	Node	   *qual;

	if (clause == NULL)
		return NULL;

	qual = transformExpr(pstate, clause);

	qual = coerce_to_boolean(pstate, qual, constructName);

	return qual;
}


/*
 * transformLimitClause -
 *	  Transform the expression and make sure it is of type bigint.
 *	  Used for LIMIT and allied clauses.
 *
 * Note: as of Postgres 8.2, LIMIT expressions are expected to yield int8,
 * rather than int4 as before.
 *
 * constructName does not affect the semantics, but is used in error messages
 */
Node *
transformLimitClause(ParseState *pstate, Node *clause,
					 const char *constructName)
{
	Node	   *qual;

	if (clause == NULL)
		return NULL;

	qual = transformExpr(pstate, clause);

	qual = coerce_to_bigint(pstate, qual, constructName);

	/*
	 * LIMIT can't refer to any vars or aggregates of the current query; we
	 * don't allow subselects either (though that case would at least be
	 * sensible)
	 */
	if (contain_vars_of_level(qual, 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		/* translator: %s is name of a SQL construct, eg LIMIT */
				 errmsg("argument of %s must not contain variables",
						constructName)));
	}
	if (checkExprHasAggs(qual))
	{
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
		/* translator: %s is name of a SQL construct, eg LIMIT */
				 errmsg("argument of %s must not contain aggregates",
						constructName)));
	}
	if (contain_subplans(qual))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/* translator: %s is name of a SQL construct, eg LIMIT */
				 errmsg("argument of %s must not contain subqueries",
						constructName)));
	}

	return qual;
}

/*
 * findListTargetlistEntries -
 *   Returns a list of targetlist entries matching the given node that
 *   corresponds to a grouping clause.
 *
 * This is similar to findTargetlistEntry(), but works for all
 * grouping clauses, including both the ordinary grouping clauses and the
 * grouping extension clauses, including ROLLUP, CUBE, and GROUPING
 * SETS.
 *
 * All targets will be added in the order that they appear in the grouping
 * clause.
 *
 * Param 'in_grpext' represents if 'node' is immediately enclosed inside
 * a GROUPING SET clause. For example,
 *     GROUPING SETS ( (a,b), ( (c,d), e ) )
 * or
 *     ROLLUP ( (a,b), ( (c,d), e ) )
 * '(a,b)' is immediately inside a GROUPING SET clause, while '(c,d)'
 * is not. '(c,d)' is immediately inside '( (c,d), e )', which is
 * considered as an ordinary grouping set.
 *
 * Note that RowExprs are handled differently with other expressions.
 * RowExprs themselves are not added into targetlists and result
 * list. However, all of their arguments will be added into
 * the targetlist. They will also be appended into the return
 * list if ignore_in_grpext is set or RowExprs do not appear
 * immediate inside a grouping extension.
 */
static List *findListTargetlistEntries(ParseState *pstate, Node *node,
									   List **tlist, bool in_grpext,
									   bool ignore_in_grpext,
                                       bool useSQL99)
{
	List *result_list = NIL;

	/*
	 * In GROUP BY clauses, empty grouping set () is supported as 'NIL'
	 * in the list. If this is the case, we simply skip it.
	 */
	if (node == NULL)
		return result_list;

	if (IsA(node, GroupingClause))
	{
		ListCell *gl;
		GroupingClause *gc = (GroupingClause*)node;

		foreach(gl, gc->groupsets)
		{
			List *subresult_list;

			subresult_list = findListTargetlistEntries(pstate, lfirst(gl),
													   tlist, true, 
                                                       ignore_in_grpext,
                                                       useSQL99);

			result_list = list_concat(result_list, subresult_list);
		}
	}

	/*
	 * In GROUP BY clause, we handle RowExpr specially here. When
	 * RowExprs appears immediately inside a grouping extension, we do
	 * not want to append the target entries for their arguments into
	 * result_list. This is because we do not want the order of
	 * these target entries in the result list from transformGroupClause()
	 * to be affected by ORDER BY.
	 *
	 * However, if ignore_in_grpext is set, we will always append
	 * these target enties.
	 */
	else if (IsA(node, RowExpr))
	{
		List *args = ((RowExpr *)node)->args;
		ListCell *lc;

		foreach (lc, args)
		{
			Node *rowexpr_arg = lfirst(lc);
			TargetEntry *tle;

            if (useSQL99)
                tle = findTargetlistEntrySQL99(pstate, rowexpr_arg, tlist);
            else
                tle = findTargetlistEntrySQL92(pstate, rowexpr_arg, tlist,
                                               GROUP_CLAUSE);

			/* If RowExpr does not appear immediately inside a GROUPING SETS,
			 * we append its targetlit to the given targetlist.
			 */
			if (ignore_in_grpext || !in_grpext)
				result_list = lappend(result_list, tle);
		}
	}

	else
	{
		TargetEntry *tle;

        if (useSQL99)
            tle = findTargetlistEntrySQL99(pstate, node, tlist);
        else
            tle = findTargetlistEntrySQL92(pstate, node, tlist, GROUP_CLAUSE);

		result_list = lappend(result_list, tle);
	}

	return result_list;
}

/*
 *	findTargetlistEntrySQL92 -
 *	  Returns the targetlist entry matching the given (untransformed) node.
 *	  If no matching entry exists, one is created and appended to the target
 *	  list as a "resjunk" node.
 *
 *    This function supports the old SQL92 ORDER BY interpretation, where the
 *    expression is an output column name or number.  If we fail to find a match
 *    of that sort, we fall through to the SQL99 rules. For historical reasons,
 *    Postgres also allows this interpretation for GROUP BY, though the standard
 *    never did. However, for GROUP BY we prefer a SQL99 match.  This function
 *    is *not* used for WINDOW definitions.
 *
 *    node    : the ORDER BY, GROUP BY, or DISTINCT ON expression to be matched
 *    tlist   : the target list (passed by reference so we can append to it)
 *    clause  : identifies clause type being processed
 */
static TargetEntry *
findTargetlistEntrySQL92(ParseState *pstate, Node *node, List **tlist, 
                         int clause)
{
	TargetEntry *target_result = NULL;
	ListCell   *tl;

    /* CDB: Drop a breadcrumb in case of error. */
    pstate->p_breadcrumb.node = node;

	/*----------
	 * Handle two special cases as mandated by the SQL92 spec:
	 *
	 * 1. Bare ColumnName (no qualifier or subscripts)
	 *	  For a bare identifier, we search for a matching column name
	 *	  in the existing target list.	Multiple matches are an error
	 *	  unless they refer to identical values; for example,
	 *	  we allow	SELECT a, a FROM table ORDER BY a
	 *	  but not	SELECT a AS b, b FROM table ORDER BY b
	 *	  If no match is found, we fall through and treat the identifier
	 *	  as an expression.
	 *	  For GROUP BY, it is incorrect to match the grouping item against
	 *	  targetlist entries: according to SQL92, an identifier in GROUP BY
	 *	  is a reference to a column name exposed by FROM, not to a target
	 *	  list column.	However, many implementations (including pre-7.0
	 *	  PostgreSQL) accept this anyway.  So for GROUP BY, we look first
	 *	  to see if the identifier matches any FROM column name, and only
	 *	  try for a targetlist name if it doesn't.  This ensures that we
	 *	  adhere to the spec in the case where the name could be both.
	 *	  DISTINCT ON isn't in the standard, so we can do what we like there;
	 *	  we choose to make it work like ORDER BY, on the rather flimsy
	 *	  grounds that ordinary DISTINCT works on targetlist entries.
	 *
	 * 2. IntegerConstant
	 *	  This means to use the n'th item in the existing target list.
	 *	  Note that it would make no sense to order/group/distinct by an
	 *	  actual constant, so this does not create a conflict with our
	 *	  extension to order/group by an expression.
	 *	  GROUP BY column-number is not allowed by SQL92, but since
	 *	  the standard has no other behavior defined for this syntax,
	 *	  we may as well accept this common extension.
	 *
	 * Note that pre-existing resjunk targets must not be used in either case,
	 * since the user didn't write them in his SELECT list.
	 *
	 * If neither special case applies, fall through to treat the item as
	 * an expression per SQL99.
	 *----------
	 */
	if (IsA(node, ColumnRef) &&
		list_length(((ColumnRef *) node)->fields) == 1)
	{
		char	   *name = strVal(linitial(((ColumnRef *) node)->fields));
		int			location = ((ColumnRef *) node)->location;

		if (clause == GROUP_CLAUSE)
		{
			/*
			 * In GROUP BY, we must prefer a match against a FROM-clause
			 * column to one against the targetlist.  Look to see if there is
			 * a matching column.  If so, fall through to use SQL99 rules
             * NOTE: if name could refer ambiguously to more than one column
			 * name exposed by FROM, colNameToVar will ereport(ERROR). That's
			 * just what we want here.
			 *
			 * Small tweak for 7.4.3: ignore matches in upper query levels.
			 * This effectively changes the search order for bare names to (1)
			 * local FROM variables, (2) local targetlist aliases, (3) outer
			 * FROM variables, whereas before it was (1) (3) (2). SQL92 and
			 * SQL99 do not allow GROUPing BY an outer reference, so this
			 * breaks no cases that are legal per spec, and it seems a more
			 * self-consistent behavior.
			 */
			if (colNameToVar(pstate, name, true, location) != NULL)
				name = NULL;
		}

		if (name != NULL)
		{
			foreach(tl, *tlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(tl);

				if (!tle->resjunk &&
					strcmp(tle->resname, name) == 0)
				{
					if (target_result != NULL)
					{
						if (!equal(target_result->expr, tle->expr))
							ereport(ERROR,
									(errcode(ERRCODE_AMBIGUOUS_COLUMN),

							/*------
							  translator: first %s is name of a SQL construct, eg ORDER BY */
									 errmsg("%s \"%s\" is ambiguous",
											clauseText[clause], name),
									 parser_errposition(pstate, location)));
					}
					else
						target_result = tle;
					/* Stay in loop to check for ambiguity */
				}
			}
			if (target_result != NULL)
				return target_result;	/* return the first match */
		}
	}
	if (IsA(node, A_Const))
	{
		Value	   *val = &((A_Const *) node)->val;
		int			targetlist_pos = 0;
		int			target_pos;

		if (!IsA(val, Integer))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
			/* translator: %s is name of a SQL construct, eg ORDER BY */
					 errmsg("non-integer constant in %s",
							clauseText[clause])));
		target_pos = intVal(val);
		foreach(tl, *tlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tl);

			if (!tle->resjunk)
			{
				if (++targetlist_pos == target_pos)
					return tle; /* return the unique match */
			}
		}
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		/* translator: %s is name of a SQL construct, eg ORDER BY */
				 errmsg("%s position %d is not in select list",
						clauseText[clause], target_pos),
				 errOmitLocation(true)));
	}


	/*
	 * Otherwise, we have an expression, so process it per SQL99 rules.
	 */
	return findTargetlistEntrySQL99(pstate, node, tlist);
}

/*
 *	findTargetlistEntrySQL99 -
 *	  Returns the targetlist entry matching the given (untransformed) node.
 *	  If no matching entry exists, one is created and appended to the target
 *	  list as a "resjunk" node.
 *
 * This function supports the SQL99 interpretation, wherein the expression
 * is just an ordinary expression referencing input column names.
 *
 * node		the ORDER BY, GROUP BY, etc expression to be matched
 * tlist	the target list (passed by reference so we can append to it)
 */
static TargetEntry *
findTargetlistEntrySQL99(ParseState *pstate, Node *node, List **tlist)
{
	TargetEntry *target_result;
	ListCell   *tl;
	Node	   *expr;

	/*
	 * Convert the untransformed node to a transformed expression, and search
	 * for a match in the tlist.  NOTE: it doesn't really matter whether there
	 * is more than one match.	Also, we are willing to match an existing
	 * resjunk target here, though the SQL92 cases above must ignore resjunk
	 * targets.
	 */
	expr = transformExpr(pstate, node);

	foreach(tl, *tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tl);

		if (equal(expr, tle->expr))
			return tle;
	}

	/*
	 * If no matches, construct a new target entry which is appended to the
	 * end of the target list.	This target is given resjunk = TRUE so that it
	 * will not be projected into the final tuple.
	 */
	target_result = transformTargetEntry(pstate, node, expr, NULL, true);

	*tlist = lappend(*tlist, target_result);

	return target_result;
}

static GroupClause *
make_group_clause(TargetEntry *tle, List *targetlist, Oid sortop)
{
	GroupClause *result;

	result = makeNode(GroupClause);
	result->tleSortGroupRef = assignSortGroupRef(tle, targetlist);
	result->sortop = sortop;
	return result;
}

/*
 * make_grouping_clause -
 *     Generate a new GroupingClause object from a given one.
 *
 * The given GroupingClause object generated by the parser contain either
 * GroupingClauses or expressions. The RowExpr expressions are handled
 * differently with other expressions -- they are transformed into a list
 * GroupingcClauses or GroupClauses, which is appended into the 'groupsets'
 * in the returning GroupingClause as a whole.
 *
 * The 'groupsets' in the returning GroupingClause may contain GroupClause,
 * GroupingClause, or List.
 *
 * Note that RowExprs are not added into the final targetlist.
 */
static GroupingClause *
make_grouping_clause(ParseState *pstate, GroupingClause *grpcl, List* targetList)
{
	GroupingClause *result;
	ListCell* gc;

	result = makeNode(GroupingClause);
	result->groupType = grpcl->groupType;
	result->groupsets = NIL;

	foreach (gc, grpcl->groupsets)
	{
		Node *node = (Node*)lfirst(gc);

		if (node == NULL)
		{
			result->groupsets =
				lappend(result->groupsets, list_make1(NIL));
		}

		else if (IsA(node, GroupingClause))
		{
			result->groupsets =
				lappend(result->groupsets,
						make_grouping_clause(pstate,
											 (GroupingClause*)node, targetList));
		}

		else if (IsA(node, RowExpr))
		{
			/*
			 * Since this RowExpr is immediately inside a GROUPING SETS, we convert it
			 * into a list of GroupClauses, which will be considered as a single
			 * grouping set in the planner.
			 */
			result->groupsets =
				transformRowExprToList(pstate, (RowExpr *)node,
									   result->groupsets, targetList);
		}

		else
		{
			TargetEntry *tle = findTargetlistEntrySQL92(pstate, node,
                                                        &targetList, GROUP_CLAUSE);
			Oid          sort_op;

			/* Unlike ordinary grouping sets, we will create duplicate
			 * expression entries. For example, rollup(a,a) consists
			 * of three grouping sets "(a,a), (a), ()".
			 */
			sort_op = ordering_oper_opid(exprType((Node *) tle->expr));
			result->groupsets =
				lappend(result->groupsets,
						make_group_clause(tle, targetList, sort_op));
		}
	}

	return result;
}

static bool
grouping_rewrite_walker(Node *node, void *context)
{
	grouping_rewrite_ctx *ctx = (grouping_rewrite_ctx *)context;

	if (node == NULL)
		return false;

	if (IsA(node, A_Const))
	{
		return false;
	}
	else if(IsA(node, A_Expr))
	{
		/* could be seen inside an untransformed window clause */
		return false;
	}
	else if(IsA(node, ColumnRef))
	{
		/* could be seen inside an untransformed window clause */
		return false;
	}
	else if (IsA(node, TypeCast))
	{
		return false;
	}
	else if (IsA(node, GroupingFunc))
	{
		GroupingFunc *gf = (GroupingFunc *)node;
		ListCell *arg_lc;
		List *newargs = NIL;

		gf->ngrpcols = list_length(ctx->grp_tles);

		/*
		 * For each argument in gf->args, find its position in grp_tles,
		 * and increment its counts. Note that this is a O(n^2) algorithm,
		 * but it should not matter that much.
		 */
		foreach (arg_lc, gf->args)
		{
			long i = 0;
			Node *node = lfirst(arg_lc);
			ListCell *grp_lc = NULL;

			foreach (grp_lc, ctx->grp_tles)
			{
				TargetEntry *grp_tle = (TargetEntry *)lfirst(grp_lc);

				if (equal(grp_tle->expr, node))
					break;
				i++;
			}

			/* Find a column not in GROUP BY clause */
			if (grp_lc == NULL)
			{
				RangeTblEntry *rte;
				const char *attname;
				Var *var = (Var *) node;

				/* Do not allow expressions inside a grouping function. */
				if (IsA(node, RowExpr))
					ereport(ERROR,
							(errcode(ERRCODE_GROUPING_ERROR),
							 errmsg("row type can not be used inside a grouping function."),
									   errOmitLocation(true)));

				if (!IsA(node, Var))
					ereport(ERROR,
							(errcode(ERRCODE_GROUPING_ERROR),
							 errmsg("expression in a grouping fuction does not appear in GROUP BY."),
									   errOmitLocation(true)));

				Assert(IsA(node, Var));
				Assert(var->varno > 0);
				Assert(var->varno <= list_length(ctx->pstate->p_rtable));

				rte = rt_fetch(var->varno, ctx->pstate->p_rtable);
				attname = get_rte_attribute_name(rte, var->varattno);

				ereport(ERROR,
						(errcode(ERRCODE_GROUPING_ERROR),
						 errmsg("column \"%s\".\"%s\" is not in GROUP BY",
								rte->eref->aliasname, attname),
										   errOmitLocation(true)));
			}

			newargs = lappend(newargs, makeInteger(i));
		}

		/* Free gf->args since we do not need it any more. */
		list_free_deep(gf->args);
		gf->args = newargs;
	}
	else if(IsA(node, SortBy))
	{
		/*
		 * When WindowSpec leaves the main parser, partition and order
		 * clauses will be lists of SortBy structures. Process them here to
		 * avoid muddying up the expression_tree_walker().
		 */
		SortBy *s = (SortBy *)node;
		return grouping_rewrite_walker(s->node, context);
	}
	return expression_tree_walker(node, grouping_rewrite_walker, context);
}


/*
 *
 * create_group_clause
 * 	Order group clauses based on equivalent sort clauses to allow plans
 * 	with sort-based grouping implementation,
 *
 * 	given a list of a GROUP-BY tle's, return a list of group clauses in the same order
 * 	of matching ORDER-BY tle's
 *
 *  the remaining GROUP-BY tle's are stored in tlist_remainder
 *
 *
 */
static List *
create_group_clause(List *tlist_group, List *targetlist,
					List *sortClause, List **tlist_remainder)
{
	List	   *result = NIL;
	ListCell   *l;
	List *tlist = list_copy(tlist_group);

	/*
	 * Iterate through the ORDER BY clause. If we find a grouping element
	 * that matches the ORDER BY element, append the grouping element to the
	 * result set immediately. Otherwise, stop iterating. The effect of this
	 * is to look for a prefix of the ORDER BY list in the grouping clauses,
	 * and to move that prefix to the front of the GROUP BY.
	 */
	foreach(l, sortClause)
	{
		SortClause *sc = (SortClause *) lfirst(l);
		ListCell   *prev = NULL;
		ListCell   *tl = NULL;
		bool		found = false;

		foreach(tl, tlist)
		{
			Node        *node = (Node*)lfirst(tl);

			if (IsA(node, TargetEntry))
			{
				TargetEntry *tle = (TargetEntry *) lfirst(tl);

				if (!tle->resjunk &&
					sc->tleSortGroupRef == tle->ressortgroupref)
				{
					GroupClause *gc;

					tlist = list_delete_cell(tlist, tl, prev);

					/* Use the sort clause's sorting operator */
					gc = make_group_clause(tle, targetlist, sc->sortop);
					result = lappend(result, gc);
					found = true;
					break;
				}

				prev = tl;
			}
		}

		/* As soon as we've failed to match an ORDER BY element, stop */
		if (!found)
			break;
	}

	/* Save remaining GROUP-BY tle's */
	*tlist_remainder = tlist;

	return result;
}



/*
 * transformGroupClause -
 *	  transform a GROUP BY clause
 *
 * The given GROUP BY clause can contain both GroupClauses and
 * GroupingClauses.
 *
 * GROUP BY items will be added to the targetlist (as resjunk columns)
 * if not already present, so the targetlist must be passed by reference.
 *
 * The order of the elements of the grouping clause does not affect
 * the semantics of the query. However, the optimizer is not currently
 * smart enough to reorder the grouping clause, so we try to do some
 * primitive reordering here.
 */
List *
transformGroupClause(ParseState *pstate, List *grouplist,
					 List **targetlist, List *sortClause,
                     bool useSQL99)
{
	List	   *result = NIL;
	List	   *tle_list = NIL;
	ListCell   *l;
	List       *reorder_grouplist = NIL;

	/* Preprocess the grouping clause, lookup TLEs */
	foreach(l, grouplist)
	{
		List        *tl;
		ListCell    *tl_cell;
		TargetEntry *tle;
		Oid			restype;
		Node        *node;

		node = (Node*)lfirst(l);
		tl = findListTargetlistEntries(pstate, node, targetlist, false, false, 
                                       useSQL99);

        /* CDB: Cursor position not available for errors below this point. */
        pstate->p_breadcrumb.node = NULL;

		foreach(tl_cell, tl)
		{
			tle = (TargetEntry*)lfirst(tl_cell);

			/* if tlist item is an UNKNOWN literal, change it to TEXT */
			restype = exprType((Node *) tle->expr);

			if (restype == UNKNOWNOID)
				tle->expr = (Expr *) coerce_type(pstate, (Node *) tle->expr,
												 restype, TEXTOID, -1,
												 COERCION_IMPLICIT,
												 COERCE_IMPLICIT_CAST,
												 -1);

			/*
			 * The tle_list will be used to match with the ORDER by element below.
			 * We only append the tle to tle_list when node is not a
			 * GroupingClause or tle->expr is not a RowExpr.
			 */
			 if (node != NULL &&
				 !IsA(node, GroupingClause) &&
				 !IsA(tle->expr, RowExpr))
				 tle_list = lappend(tle_list, tle);
		}
	}

	/* create first group clauses based on sort clauses */
	List *tle_list_remainder = NIL;
	result = create_group_clause(tle_list,
								*targetlist,
								sortClause,
								&tle_list_remainder);

	/*
	 * Now add all remaining elements of the GROUP BY list to the result list.
	 * The result list is a list of GroupClauses and/or GroupingClauses.
	 * In each grouping set, all GroupClauses will appear in front of
	 * GroupingClauses. See the following GROUP BY clause:
	 *
	 *   GROUP BY ROLLUP(b,c),a, ROLLUP(e,d)
	 *
	 * the result list can be roughly represented as follows.
	 *
	 *    GroupClause(a),
	 *    GroupingClause(ROLLUP,groupsets(GroupClause(b),GroupClause(c))),
	 *    GroupingClause(CUBE,groupsets(GroupClause(e),GroupClause(d)))
	 *
	 *   XXX: the above transformation doesn't make sense -gs
	 */
	reorder_grouplist = reorderGroupList(grouplist);

	foreach(l, reorder_grouplist)
	{
		Node *node = (Node*) lfirst(l);
		TargetEntry *tle;
		GroupClause *gc;
		Oid			sort_op;

		if (node == NULL) /* the empty grouping set */
			result = list_concat(result, list_make1(NIL));

		else if (IsA(node, GroupingClause))
		{
			GroupingClause *tmp = make_grouping_clause(pstate,
													   (GroupingClause*)node,
													   *targetlist);
			result = lappend(result, tmp);
		}

		else if (IsA(node, RowExpr))
		{
			/* The top level RowExprs are handled differently with other expressions.
			 * We convert each argument into GroupClause and append them
			 * one by one into 'result' list.
			 *
			 * Note that RowExprs are not added into the final targetlist.
			 */
			result =
				transformRowExprToGroupClauses(pstate, (RowExpr *)node,
											   result, *targetlist);
		}

		else
		{

            if (useSQL99)
                tle = findTargetlistEntrySQL99(pstate, node, targetlist);
            else
                tle = findTargetlistEntrySQL92(pstate, node, targetlist, 
                                               GROUP_CLAUSE);

			/* avoid making duplicate expression entries */
			if (targetIsInSortGroupList(tle, result))
				continue;

			sort_op = ordering_oper_opid(exprType((Node *) tle->expr));
			gc = make_group_clause(tle, *targetlist, sort_op);
			result = lappend(result, gc);
		}
	}

	/* We're doing extended grouping for both ordinary grouping and grouping
	 * extensions.
	 */
	{
		List *grp_tles = NIL;
		ListCell *lc;
		grouping_rewrite_ctx ctx;

		/* Find all unique target entries appeared in reorder_grouplist. */
		foreach (lc, reorder_grouplist)
		{
			grp_tles = list_concat_unique(
				grp_tles,
				findListTargetlistEntries(pstate, lfirst(lc),
										  targetlist, false, true, useSQL99));
		}

        /* CDB: Cursor position not available for errors below this point. */
        pstate->p_breadcrumb.node = NULL;

		/*
		 * For each GROUPING function, check if its argument(s) appear in the
		 * GROUP BY clause. We also set ngrpcols, nargs and grpargs values for
		 * each GROUPING function here. These values are used together with
		 * GROUPING_ID to calculate the final value for each GROUPING function
		 * in the executor.
		 */

		ctx.grp_tles = grp_tles;
		ctx.pstate = pstate;
		expression_tree_walker((Node *)*targetlist, grouping_rewrite_walker,
							   (void *)&ctx);

		/*
		 * The expression might be present in a window clause as well
		 * so process those.
		 */
		expression_tree_walker((Node *)pstate->p_win_clauses,
							   grouping_rewrite_walker, (void *)&ctx);

		/*
		 * The expression might be present in the having clause as well.
		 */
		expression_tree_walker(pstate->having_qual,
							   grouping_rewrite_walker, (void *)&ctx);
	}

	list_free(tle_list);
	list_free(tle_list_remainder);
	freeGroupList(reorder_grouplist);

	return result;
}

/*
 * transformSortClause -
 *	  transform an ORDER BY clause
 *
 * ORDER BY items will be added to the targetlist (as resjunk columns)
 * if not already present, so the targetlist must be passed by reference.
 */
List *
transformSortClause(ParseState *pstate,
					List *orderlist,
					List **targetlist,
					bool resolveUnknown,
                    bool useSQL99)
{
	List	   *sortlist = NIL;
	ListCell   *olitem;

	foreach(olitem, orderlist)
	{
		SortBy	   *sortby = lfirst(olitem);
		TargetEntry *tle;

        if (useSQL99)
            tle = findTargetlistEntrySQL99(pstate, sortby->node, targetlist);
        else
            tle = findTargetlistEntrySQL92(pstate, sortby->node, targetlist, 
                                           ORDER_CLAUSE);

		sortlist = addTargetToSortList(pstate, tle,
									   sortlist, *targetlist,
									   sortby->sortby_kind,
									   sortby->useOp,
									   resolveUnknown);
	}

	return sortlist;
}

/*
 *
 * transformDistinctToGroupBy
 *
 * 		transform DISTINCT clause to GROUP-BY clause
 *
 */
static List *
transformDistinctToGroupBy(ParseState *pstate, List **targetlist,
							List **sortClause, List **groupClause)
{
	List *group_tlist = list_copy(*targetlist);

	/*
	 * create first group clauses based on matching sort clauses, if any
	 */
	List *group_tlist_remainder = NIL;
	List *group_clause_list = create_group_clause(group_tlist,
												*targetlist,
												*sortClause,
												&group_tlist_remainder);

	if (list_length(group_tlist_remainder) > 0)
	{
		/*
		 * append remaining group clauses to the end of group clause list
		 */
		ListCell *lc = NULL;
		foreach(lc, group_tlist_remainder)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			if (!tle->resjunk)
			{
				group_clause_list = addTargetToSortList(pstate, tle,
											   	   group_clause_list, *targetlist,
											   	   SORTBY_ASC, NIL, true);
			}
		}

		/*
		 * fix tags of group clauses
		 */
		foreach(lc, group_clause_list)
		{
			Node *node = lfirst(lc);
			if (IsA(node, SortClause))
			{
				SortClause *sc = (SortClause *) node;
				sc->type = T_GroupClause;
			}
		}
	}

	*groupClause = group_clause_list;

	list_free(group_tlist);
	list_free(group_tlist_remainder);

	/*
	 * return empty distinct list, since we have created a grouping clause to do the job
	 */
	return NIL;
}


/*
 * transformDistinctClause -
 *	  transform a DISTINCT or DISTINCT ON clause
 *
 * Since we may need to add items to the query's sortClause list, that list
 * is passed by reference.	Likewise for the targetlist.
 */
List *
transformDistinctClause(ParseState *pstate, List *distinctlist,
						List **targetlist, List **sortClause, List **groupClause)
{
	List	   *result = NIL;
	ListCell   *slitem;
	ListCell   *dlitem;

	/* No work if there was no DISTINCT clause */
	if (distinctlist == NIL)
		return NIL;

	if (linitial(distinctlist) == NULL)
	{
		/* We had SELECT DISTINCT */

		if (!pstate->p_hasAggs && !pstate->p_hasWindFuncs && *groupClause == NIL)
		{
			/*
			 * MPP-15040
			 * turn distinct clause into grouping clause to make both sort-based
			 * and hash-based grouping implementations viable plan options
			 */

			return transformDistinctToGroupBy(pstate, targetlist, sortClause, groupClause);
		}

		/*
		 * All non-resjunk elements from target list that are not already in
		 * the sort list should be added to it.  (We don't really care what
		 * order the DISTINCT fields are checked in, so we can leave the
		 * user's ORDER BY spec alone, and just add additional sort keys to it
		 * to ensure that all targetlist items get sorted.)
		 */
		*sortClause = addAllTargetsToSortList(pstate,
											  *sortClause,
											  *targetlist,
											  true);

		/*
		 * Now, DISTINCT list consists of all non-resjunk sortlist items.
		 * Actually, all the sortlist items had better be non-resjunk!
		 * Otherwise, user wrote SELECT DISTINCT with an ORDER BY item that
		 * does not appear anywhere in the SELECT targetlist, and we can't
		 * implement that with only one sorting pass...
		 */
		foreach(slitem, *sortClause)
		{
			SortClause *scl = (SortClause *) lfirst(slitem);
			TargetEntry *tle = get_sortgroupclause_tle(scl, *targetlist);

			if (tle->resjunk)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("for SELECT DISTINCT, ORDER BY expressions must appear in select list"),
								   errOmitLocation(true)));
			else
				result = lappend(result, copyObject(scl));
		}
	}
	else
	{
		/* We had SELECT DISTINCT ON (expr, ...) */

		/*
		 * If the user writes both DISTINCT ON and ORDER BY, then the two
		 * expression lists must match (until one or the other runs out).
		 * Otherwise the ORDER BY requires a different sort order than the
		 * DISTINCT does, and we can't implement that with only one sort pass
		 * (and if we do two passes, the results will be rather
		 * unpredictable). However, it's OK to have more DISTINCT ON
		 * expressions than ORDER BY expressions; we can just add the extra
		 * DISTINCT values to the sort list, much as we did above for ordinary
		 * DISTINCT fields.
		 *
		 * Actually, it'd be OK for the common prefixes of the two lists to
		 * match in any order, but implementing that check seems like more
		 * trouble than it's worth.
		 */
		ListCell   *nextsortlist = list_head(*sortClause);

		foreach(dlitem, distinctlist)
		{
			TargetEntry *tle;

			tle = findTargetlistEntrySQL92(pstate, lfirst(dlitem),
                                           targetlist, DISTINCT_ON_CLAUSE);

			if (nextsortlist != NULL)
			{
				SortClause *scl = (SortClause *) lfirst(nextsortlist);

				if (tle->ressortgroupref != scl->tleSortGroupRef)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
							 errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
									   errOmitLocation(true)));
				result = lappend(result, copyObject(scl));
				nextsortlist = lnext(nextsortlist);
			}
			else
			{
				*sortClause = addTargetToSortList(pstate, tle,
												  *sortClause, *targetlist,
												  SORTBY_ASC, NIL, true);

				/*
				 * Probably, the tle should always have been added at the end
				 * of the sort list ... but search to be safe.
				 */
				foreach(slitem, *sortClause)
				{
					SortClause *scl = (SortClause *) lfirst(slitem);

					if (tle->ressortgroupref == scl->tleSortGroupRef)
					{
						result = lappend(result, copyObject(scl));
						break;
					}
				}
				if (slitem == NULL)		/* should not happen */
					elog(ERROR, "failed to add DISTINCT ON clause to target list");
			}
		}
	}

	return result;
}

/*
 * transformScatterClause -
 *	  transform a SCATTER BY clause
 *
 * SCATTER BY items will be added to the targetlist (as resjunk columns)
 * if not already present, so the targetlist must be passed by reference.
 *
 */
List *
transformScatterClause(ParseState *pstate,
					   List *scatterlist,
					   List **targetlist)
{
	List	   *outlist = NIL;
	ListCell   *olitem;

	/* Special case handling for SCATTER RANDOMLY */
	if (list_length(scatterlist) == 1 && linitial(scatterlist) == NULL)
		return list_make1(NULL);
	
	/* preprocess the scatter clause, lookup TLEs */
	foreach(olitem, scatterlist)
	{
		Node			*node = lfirst(olitem);
		TargetEntry		*tle;

		tle = findTargetlistEntrySQL99(pstate, node, targetlist);

		/* coerce unknown to text */
		if (exprType((Node *) tle->expr) == UNKNOWNOID)
		{
			tle->expr = (Expr *) coerce_type(pstate, (Node *) tle->expr,
											 UNKNOWNOID, TEXTOID, -1,
											 COERCION_IMPLICIT,
											 COERCE_IMPLICIT_CAST,
											 -1);
		}

		outlist = lappend(outlist, tle->expr);
	}
	return outlist;
}


/*
 * addAllTargetsToSortList
 *		Make sure all non-resjunk targets in the targetlist are in the
 *		ORDER BY list, adding the not-yet-sorted ones to the end of the list.
 *		This is typically used to help implement SELECT DISTINCT.
 *
 * See addTargetToSortList for info about pstate and resolveUnknown inputs.
 *
 * Returns the updated ORDER BY list.
 * May modify targetlist in place even when resolveUnknown is FALSE.
 */
List *
addAllTargetsToSortList(ParseState *pstate, List *sortlist,
						List *targetlist, bool resolveUnknown)
{
	ListCell   *l;

	foreach(l, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (!tle->resjunk)
			sortlist = addTargetToSortList(pstate, tle,
										   sortlist, targetlist,
										   SORTBY_ASC, NIL,
										   resolveUnknown);
	}
	return sortlist;
}

/*
 * We use this function to determine whether data can be sorted using a given
 * operator over a given data type. There are two uses for the function:
 *
 * Firstly, to determine if the operator specified in an ORDER BY ... USING <op> 
 * has a btree sorting operator (either < or >)? We determine that the operator
 * has these properties by looking up pg_amop for the operator AM properties.
 * If the operator is not a member of a btree operator class and if it does not 
 * have < > AM strategies, it cannot be used to sort. In this case we return false.
 * If we find a candidate, we return true.
 *
 * Secondly, to determine if we support merge join using the given operator.
 * If mergejoin is set to true, then we're looking for an equality btree
 * strategy.
 * 
 */
bool
sort_op_can_sort(Oid opid, bool mergejoin)
{
	CatCList *catlist;
	int i;
	bool result = false;

	catlist = caql_begin_CacheList(
								   NULL,
								   cql("SELECT * FROM pg_amop "
									   " WHERE amopopr = :1 "
									   " ORDER BY amopopr, "
									   " amopclaid ",
									   ObjectIdGetDatum(opid)));

	/* not associated with an AM, so can't be a match */
	if (catlist->n_members == 0)
	{
		caql_end_CacheList(catlist);
		return false;
	}

	for (i = 0; i < catlist->n_members; i++)
	{
		HeapTuple tuple = &catlist->members[i]->tuple;
		Form_pg_amop amop = (Form_pg_amop)GETSTRUCT(tuple);
		
		if (!opclass_is_btree(amop->amopclaid))
			continue;

		if (amop->amopsubtype != InvalidOid)
			continue;

		if (mergejoin)
		{
			if (amop->amopstrategy == BTEqualStrategyNumber)
			{
				result = true;
				break;
			}
		}
		else
		{
			if (amop->amopstrategy == BTLessStrategyNumber ||
				amop->amopstrategy == BTGreaterStrategyNumber)
			{
				result = true;
				break;
			}
		}
	}
	caql_end_CacheList(catlist);
	return result;
}

/*
 * addTargetToSortList
 *		If the given targetlist entry isn't already in the ORDER BY list,
 *		add it to the end of the list, using the sortop with given name
 *		or the default sort operator if opname == NIL.
 *
 * If resolveUnknown is TRUE, convert TLEs of type UNKNOWN to TEXT.  If not,
 * do nothing (which implies the search for a sort operator will fail).
 * pstate should be provided if resolveUnknown is TRUE, but can be NULL
 * otherwise.
 *
 * Returns the updated ORDER BY list.
 * May modify targetlist entry in place even when resolveUnknown is FALSE.
 */
List *
addTargetToSortList(ParseState *pstate, TargetEntry *tle,
					List *sortlist, List *targetlist,
					int sortby_kind, List *sortby_opname,
					bool resolveUnknown)
{
	/* avoid making duplicate sortlist entries */
	if (!targetIsInSortGroupList(tle, sortlist))
	{
		SortClause *sortcl = makeNode(SortClause);
		Oid			restype = exprType((Node *) tle->expr);

		/* if tlist item is an UNKNOWN literal, change it to TEXT */
		if (restype == UNKNOWNOID && resolveUnknown)
		{
			Oid		tobe_type = InvalidOid;
			int32	tobe_typmod;

			if (pstate->p_setopTypes)
			{
				/* UNION, etc. case. */
				int		idx = tle->resno - 1;

				Assert(pstate->p_setopTypmods);
				tobe_type = list_nth_oid(pstate->p_setopTypes, idx);
				tobe_typmod = list_nth_int(pstate->p_setopTypmods, idx);
			}

			if (!OidIsValid(tobe_type))
			{
				tobe_type = TEXTOID;
				tobe_typmod = -1;
			}
			tle->expr = (Expr *) coerce_type(pstate, (Node *) tle->expr,
											 restype, tobe_type, tobe_typmod,
											 COERCION_IMPLICIT,
											 COERCE_IMPLICIT_CAST,
											 -1);
			restype = tobe_type;
		}

		sortcl->tleSortGroupRef = assignSortGroupRef(tle, targetlist);

		switch (sortby_kind)
		{
			case SORTBY_ASC:
				sortcl->sortop = ordering_oper_opid(restype);
				break;
			case SORTBY_DESC:
				sortcl->sortop = reverse_ordering_oper_opid(restype);
				break;
			case SORTBY_USING:
				Assert(sortby_opname != NIL);
				sortcl->sortop = compatible_oper_opid(sortby_opname,
													  restype,
													  restype,
													  false);
				if (!sort_op_can_sort(sortcl->sortop, false))
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					   		 errmsg("operator %s is not a valid ordering operator",
							  		strVal(llast(sortby_opname))),
						 			errhint("Ordering operators must be \"<\" or \">\" members of btree operator families."),
						 				   errOmitLocation(true)));

				break;
			default:
				elog(ERROR, "unrecognized sortby_kind: %d", sortby_kind);
				break;
		}

		sortlist = lappend(sortlist, sortcl);
	}
	return sortlist;
}

/*
 * assignSortGroupRef
 *	  Assign the targetentry an unused ressortgroupref, if it doesn't
 *	  already have one.  Return the assigned or pre-existing refnumber.
 *
 * 'tlist' is the targetlist containing (or to contain) the given targetentry.
 */
Index
assignSortGroupRef(TargetEntry *tle, List *tlist)
{
	Index		maxRef;
	ListCell   *l;

	if (tle->ressortgroupref)	/* already has one? */
		return tle->ressortgroupref;

	/* easiest way to pick an unused refnumber: max used + 1 */
	maxRef = 0;
	foreach(l, tlist)
	{
		Index		ref = ((TargetEntry *) lfirst(l))->ressortgroupref;

		if (ref > maxRef)
			maxRef = ref;
	}
	tle->ressortgroupref = maxRef + 1;
	return tle->ressortgroupref;
}

/*
 * targetIsInSortGroupList
 *		Is the given target item already in the sortlist or grouplist?
 *
 * Works for SortClause, GroupClause and GroupingClause lists.  Note
 * that the main reason we need this routine (and not just a quick
 * test for nonzeroness of ressortgroupref) is that a TLE might be in
 * only one of the lists.
 *
 * Any GroupingClauses in the list will be skipped during comparison.
 */
bool
targetIsInSortGroupList(TargetEntry *tle, List *sortgroupList)
{
	Index		ref = tle->ressortgroupref;
	ListCell   *l;

	/* no need to scan list if tle has no marker */
	if (ref == 0)
		return false;

	foreach(l, sortgroupList)
	{
		Node *node = (Node *) lfirst(l);

		/* Skip the empty grouping set */
		if (node == NULL)
			continue;

		if (IsA(node, GroupClause) || IsA(node, SortClause))
		{
			GroupClause *gc = (GroupClause*) node;
			if (gc->tleSortGroupRef == ref)
				return true;
		}
	}
	return false;
}

/*
 * Given a sort group reference, find the TargetEntry for it.
 */

static TargetEntry *
getTargetBySortGroupRef(Index ref, List *tl)
{
	ListCell *tmp;

	foreach(tmp, tl)
	{
		TargetEntry *te = (TargetEntry *)lfirst(tmp);

		if (te->ressortgroupref == ref)
			return te;
	}
	return NULL;
}

/*
 * Re-order entries in a given GROUP BY list, which includes expressions or
 * grouping extension clauses, such as ROLLUP, CUBE, GROUPING_SETS.
 *
 * In each grouping set level, all non grouping extension clauses (or
 * expressions) will appear in front of grouping extension clauses.
 * See the following GROUP BY clause:
 *
 *   GROUP BY ROLLUP(b,c),a, ROLLUP(e,d)
 *
 * The re-ordered list is like below:
 *
 *   a,ROLLUP(b,c), ROLLUP(e,d)
 *
 * We make a fresh copy for each entries in the result list. The caller
 * needs to free the list eventually.
 */
static List *
reorderGroupList(List *grouplist)
{
	List *result = NIL;
	ListCell *gl;
	List *sub_list = NIL;

	foreach(gl, grouplist)
	{
		Node *node = (Node*)lfirst(gl);

		if (node == NULL)
		{
			/* Append an empty set. */
			result = list_concat(result, list_make1(NIL));
		}

		else if (IsA(node, GroupingClause))
		{
			GroupingClause *gc = (GroupingClause *)node;
			GroupingClause *new_gc = makeNode(GroupingClause);

			new_gc->groupType = gc->groupType;
			new_gc->groupsets = reorderGroupList(gc->groupsets);

			sub_list = lappend(sub_list, new_gc);
		}
		else
		{
			Node *new_node = (Node *)copyObject(node);
			result = lappend(result, new_node);
		}
	}

	result = list_concat(result, sub_list);
	return result;
}

/*
 * Free all the cells of the group list, the list itself and all the
 * objects pointed-by the cells of the list. The element in
 * the group list can be NULL.
 */
static void
freeGroupList(List *grouplist)
{
	ListCell *gl;

	if (grouplist == NULL)
		return;

	foreach (gl, grouplist)
	{
		Node *node = (Node *)lfirst(gl);
		if (node == NULL)
			continue;
		if (IsA(node, GroupingClause))
		{
			GroupingClause *gc = (GroupingClause *)node;
			freeGroupList(gc->groupsets);
			pfree(gc);
		}

		else
		{
			pfree(node);
		}
	}

	pfree(grouplist);
}

/*
 * Transform a RowExp into a list of GroupClauses, and store this list
 * as a whole into a given List. The new list is returned.
 *
 * This function should be used when a RowExpr is immediately inside
 * a grouping extension clause. For example,
 *    GROUPING SETS ( ( (a,b), c ), (c,d) )
 * or
 *    ROLLUP ( ( (a,b), c ), (c,d) )
 *
 * '(c,d)' is immediately inside a grouping extension clause,
 * while '(a,b)' is not.
 */
static List *
transformRowExprToList(ParseState *pstate, RowExpr *rowexpr,
					   List *groupsets, List *targetList)
{
	List *args = rowexpr->args;
	List *grping_set = NIL;
	ListCell *arglc;
	Oid sort_op;

	foreach (arglc, args)
	{
		Node *node = lfirst(arglc);

		if (IsA(node, RowExpr))
		{
			groupsets =
				transformRowExprToGroupClauses(pstate, (RowExpr *)node,
											   groupsets, targetList);
		}

		else
		{
			/* Find the TargetEntry for this argument. This should have been
			 * generated in findListTargetlistEntries().
			 */
			TargetEntry *arg_tle =
				findTargetlistEntrySQL92(pstate, node, &targetList, GROUP_CLAUSE);
			sort_op = ordering_oper_opid(exprType((Node *)arg_tle->expr));
			grping_set = lappend(grping_set,
								 make_group_clause(arg_tle, targetList, sort_op));
		}
	}
	groupsets = lappend (groupsets, grping_set);

	return groupsets;
}

/*
 * Transform a RowExpr into a list of GroupClauses, and append these
 * GroupClausesone by one into 'groupsets'. The new list is returned.
 *
 * This function should be used when a RowExpr is not immediately inside
 * a grouping extension clause. For example,
 *    GROUPING SETS ( ( (a,b), c ), (c,d) )
 * or
 *    ROLLUP ( ( (a,b), c ), (c,d) )
 *
 * '(c,d)' is immediately inside a grouping extension clause,
 * while '(a,b)' is not.
 */
static List *
transformRowExprToGroupClauses(ParseState *pstate, RowExpr *rowexpr,
							   List *groupsets, List *targetList)
{
	List *args = rowexpr->args;
	ListCell *arglc;
	Oid sort_op;

	foreach (arglc, args)
	{
		Node *node = lfirst(arglc);

		if (IsA(node, RowExpr))
		{
			transformRowExprToGroupClauses(pstate, (RowExpr *)node,
										   groupsets, targetList);
		}

		else
		{
			/* Find the TargetEntry for this argument. This should have been
			 * generated in findListTargetlistEntries().
			 */
			TargetEntry *arg_tle =
				findTargetlistEntrySQL92(pstate, node, &targetList, GROUP_CLAUSE);

			/* avoid making duplicate expression entries */
			if (targetIsInSortGroupList(arg_tle, groupsets))
				continue;

			sort_op = ordering_oper_opid(exprType((Node *)arg_tle->expr));
			groupsets = lappend(groupsets,
								make_group_clause(arg_tle, targetList, sort_op));
		}
	}

	return groupsets;
}
