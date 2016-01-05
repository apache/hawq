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
 * clauses.c
 *	  routines to manipulate qualification clauses
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/optimizer/util/clauses.c,v 1.223.2.1 2007/02/02 00:03:17 tgl Exp $
 *
 * HISTORY
 *	  AUTHOR			DATE			MAJOR EVENT
 *	  Andrew Yu			Nov 3, 1994		clause.c and clauses.c combined
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/catquery.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_language.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/functions.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


typedef struct
{
	ParamListInfo boundParams;
	List	   *active_fns;
	Node	   *case_val;
	bool		transform_stable_funcs;
	bool		recurse_queries; /* recurse into query structures */
	bool		recurse_sublink_testexpr; /* recurse into sublink test expressions */
	bool		transform_functions_returning_composite_values; /* transform functions returning constants of composite values */
	bool		transform_saop; /* transform scalar array ops */
	Size        max_size; /* max constant binary size in bytes, 0: no restrictions */
} eval_const_expressions_context;

typedef struct
{
	int			nargs;
	List	   *args;
	int		   *usecounts;
} substitute_actual_parameters_context;

static bool contain_agg_clause_walker(Node *node, void *context);
static bool count_agg_clauses_walker(Node *node, AggClauseCounts *counts);
static bool expression_returns_set_walker(Node *node, void *context);
static bool contain_subplans_walker(Node *node, void *context);
static bool contain_mutable_functions_walker(Node *node, void *context);
static bool contain_volatile_functions_walker(Node *node, void *context);
static bool contain_window_functions_walker(Node *node, void *context);
static bool contain_nonstrict_functions_walker(Node *node, void *context);
static Relids find_nonnullable_rels_walker(Node *node, bool top_level);
static bool is_strict_saop(ScalarArrayOpExpr *expr, bool falseOK);
static bool set_coercionform_dontcare_walker(Node *node, void *context);
static Node *eval_const_expressions_mutator(Node *node,
							   eval_const_expressions_context *context);
static List *simplify_or_arguments(List *args,
					  eval_const_expressions_context *context,
					  bool *haveNull, bool *forceTrue);
static List *simplify_and_arguments(List *args,
					   eval_const_expressions_context *context,
					   bool *haveNull, bool *forceFalse);
static Expr *simplify_boolean_equality(List *args);
static Expr *simplify_function(Oid funcid, Oid result_type, List *args,
				  bool allow_inline,
				  eval_const_expressions_context *context);
static bool large_const(Expr *expr, Size max_size);
static Expr *evaluate_function(Oid funcid, Oid result_type, List *args,
				  HeapTuple func_tuple,
				  eval_const_expressions_context *context);
static Expr *inline_function(Oid funcid, Oid result_type, List *args,
				cqContext *pcqCtx,
				HeapTuple func_tuple,
				eval_const_expressions_context *context);
static Node *substitute_actual_parameters(Node *expr, int nargs, List *args,
							 int *usecounts);
static Node *substitute_actual_parameters_mutator(Node *node,
							  substitute_actual_parameters_context *context);
static void sql_inline_error_callback(void *arg);
static bool contain_grouping_clause_walker(Node *node, void *context);

extern bool		optimizer; /* Enable the optimizer */

/*****************************************************************************
 *		OPERATOR clause functions
 *****************************************************************************/

/*
 * make_opclause
 *	  Creates an operator clause given its operator info, left operand,
 *	  and right operand (pass NULL to create single-operand clause).
 */
Expr *
make_opclause(Oid opno, Oid opresulttype, bool opretset,
			  Expr *leftop, Expr *rightop)
{
	OpExpr	   *expr = makeNode(OpExpr);

	expr->opno = opno;
	expr->opfuncid = InvalidOid;
	expr->opresulttype = opresulttype;
	expr->opretset = opretset;
	if (rightop)
		expr->args = list_make2(leftop, rightop);
	else
		expr->args = list_make1(leftop);
	return (Expr *) expr;
}

/*
 * get_leftop
 *
 * Returns the left operand of a clause of the form (op expr expr)
 *		or (op expr)
 */
Node *
get_leftop(Expr *clause)
{
	OpExpr	   *expr = (OpExpr *) clause;

	if (expr->args != NIL)
		return linitial(expr->args);
	else
		return NULL;
}

/*
 * get_rightop
 *
 * Returns the right operand in a clause of the form (op expr expr).
 * NB: result will be NULL if applied to a unary op clause.
 */
Node *
get_rightop(Expr *clause)
{
	OpExpr	   *expr = (OpExpr *) clause;

	if (list_length(expr->args) >= 2)
		return lsecond(expr->args);
	else
		return NULL;
}

/*****************************************************************************
 *		NOT clause functions
 *****************************************************************************/

/*
 * not_clause
 *
 * Returns t iff this is a 'not' clause: (NOT expr).
 */
bool
not_clause(Node *clause)
{
	return (clause != NULL &&
			IsA(clause, BoolExpr) &&
			((BoolExpr *) clause)->boolop == NOT_EXPR);
}

/*
 * make_notclause
 *
 * Create a 'not' clause given the expression to be negated.
 */
Expr *
make_notclause(Expr *notclause)
{
	BoolExpr   *expr = makeNode(BoolExpr);

	expr->boolop = NOT_EXPR;
	expr->args = list_make1(notclause);
	return (Expr *) expr;
}

/*
 * get_notclausearg
 *
 * Retrieve the clause within a 'not' clause
 */
Expr *
get_notclausearg(Expr *notclause)
{
	return linitial(((BoolExpr *) notclause)->args);
}

/*****************************************************************************
 *		OR clause functions
 *****************************************************************************/

/*
 * or_clause
 *
 * Returns t iff the clause is an 'or' clause: (OR { expr }).
 */
bool
or_clause(Node *clause)
{
	return (clause != NULL &&
			IsA(clause, BoolExpr) &&
			((BoolExpr *) clause)->boolop == OR_EXPR);
}

/*
 * make_orclause
 *
 * Creates an 'or' clause given a list of its subclauses.
 */
Expr *
make_orclause(List *orclauses)
{
	BoolExpr   *expr = makeNode(BoolExpr);

	expr->boolop = OR_EXPR;
	expr->args = orclauses;
	return (Expr *) expr;
}

/*****************************************************************************
 *		AND clause functions
 *****************************************************************************/


/*
 * and_clause
 *
 * Returns t iff its argument is an 'and' clause: (AND { expr }).
 */
bool
and_clause(Node *clause)
{
	return (clause != NULL &&
			IsA(clause, BoolExpr) &&
			((BoolExpr *) clause)->boolop == AND_EXPR);
}

/*
 * make_andclause
 *
 * Creates an 'and' clause given a list of its subclauses.
 */
Expr *
make_andclause(List *andclauses)
{
	BoolExpr   *expr = makeNode(BoolExpr);

	expr->boolop = AND_EXPR;
	expr->args = andclauses;
	return (Expr *) expr;
}

/*
 * make_and_qual
 *
 * Variant of make_andclause for ANDing two qual conditions together.
 * Qual conditions have the property that a NULL nodetree is interpreted
 * as 'true'.
 *
 * NB: this makes no attempt to preserve AND/OR flatness; so it should not
 * be used on a qual that has already been run through prepqual.c.
 */
Node *
make_and_qual(Node *qual1, Node *qual2)
{
	if (qual1 == NULL)
		return qual2;
	if (qual2 == NULL)
		return qual1;
	return (Node *) make_andclause(list_make2(qual1, qual2));
}

/*
 * Sometimes (such as in the input of ExecQual), we use lists of expression
 * nodes with implicit AND semantics.
 *
 * These functions convert between an AND-semantics expression list and the
 * ordinary representation of a boolean expression.
 *
 * Note that an empty list is considered equivalent to TRUE.
 */
Expr *
make_ands_explicit(List *andclauses)
{
	if (andclauses == NIL)
		return (Expr *) makeBoolConst(true, false);
	else if (list_length(andclauses) == 1)
		return (Expr *) linitial(andclauses);
	else
		return make_andclause(andclauses);
}

List *
make_ands_implicit(Expr *clause)
{
	/*
	 * NB: because the parser sets the qual field to NULL in a query that has
	 * no WHERE clause, we must consider a NULL input clause as TRUE, even
	 * though one might more reasonably think it FALSE.  Grumble. If this
	 * causes trouble, consider changing the parser's behavior.
	 */
	if (clause == NULL)
	{
		return NIL;				/* NULL -> NIL list == TRUE */
	}
	
	if (and_clause((Node *) clause))
	{
		return ((BoolExpr *) clause)->args;
	}
	
	if (IsA(clause, Const) &&
			 !((Const *) clause)->constisnull &&
			 DatumGetBool(((Const *) clause)->constvalue))
	{
		return NIL;				/* constant TRUE input -> NIL list */
	}
	
	if (IsA(clause, List))
	{
		/* already a list */
		Assert(list_length((List *) clause) == 0 || !IsA(list_nth((List *) clause, 0), List));
		return (List *) clause;
	}
	
	return list_make1(clause);
}


/*****************************************************************************
 *		Aggregate-function clause manipulation
 *****************************************************************************/

/*
 * contain_agg_clause
 *	  Recursively search for Aggref nodes, GroupId, GroupingFunc within a clause.
 *
 *	  Returns true if any aggregate found.
 *
 * This does not descend into subqueries, and so should be used only after
 * reduction of sublinks to subplans, or in contexts where it's known there
 * are no subqueries.  There mustn't be outer-aggregate references either.
 *
 * (If you want something like this but able to deal with subqueries,
 * see rewriteManip.c's checkExprHasAggs().)
 */
bool
contain_agg_clause(Node *clause)
{
	return contain_agg_clause_walker(clause, NULL);
}

static bool
contain_agg_clause_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		Assert(((Aggref *) node)->agglevelsup == 0);
		return true;			/* abort the tree traversal and return true */
	}

	if (IsA(node, GroupId) || IsA(node, GroupingFunc))
		return true;

	if (IsA(node, PercentileExpr))
		return true;

	Assert(!IsA(node, SubLink));
	return expression_tree_walker(node, contain_agg_clause_walker, context);
}

/*
 * count_agg_clauses
 *	  Recursively count the Aggref nodes in an expression tree.
 *
 *	  Note: this also checks for nested aggregates, which are an error.
 *
 * We not only count the nodes, but attempt to estimate the total space
 * needed for their transition state values if all are evaluated in parallel
 * (as would be done in a HashAgg plan).  See AggClauseCounts for the exact
 * set of statistics returned.
 *
 * NOTE that the counts are ADDED to those already in *counts ... so the
 * caller is responsible for zeroing the struct initially.
 *
 * This does not descend into subqueries, and so should be used only after
 * reduction of sublinks to subplans, or in contexts where it's known there
 * are no subqueries.  There mustn't be outer-aggregate references either.
 */
void
count_agg_clauses(Node *clause, AggClauseCounts *counts)
{
	/* no setup needed */
	count_agg_clauses_walker(clause, counts);
}

static bool
count_agg_clauses_walker(Node *node, AggClauseCounts *counts)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		Aggref	   *aggref = (Aggref *) node;
		Oid		   *inputTypes = NULL;
		int			numArguments;
		HeapTuple	aggTuple;
		Form_pg_aggregate aggform;
		Oid			aggtranstype;
		Oid			aggprelimfn;
		int			i;
		ListCell   *l;
		cqContext  *pcqCtx;

		Assert(aggref->agglevelsup == 0);
		counts->numAggs++;
		if (aggref->aggdistinct)
		{
			Node *arg;
			
			counts->numDistinctAggs++;
			
			/* This check anticipates the one in ExecInitAgg(). */
			if ( list_length(aggref->args) != 1 )
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("DISTINCT is supported only for single-argument aggregates")));
				
			arg = (Node*)linitial(aggref->args);
			if ( !list_member(counts->dqaArgs, arg) )
				counts->dqaArgs = lappend(counts->dqaArgs, arg);
		}

		/*
		 * Build up a list of all ordering clauses from any ordered aggregates
		 */
		if (aggref->aggorder)
		{
			if ( !list_member(counts->aggOrder, aggref->aggorder) )
				counts->aggOrder = lappend(counts->aggOrder, aggref->aggorder);
		}

		/* extract argument types */
		numArguments = list_length(aggref->args);
		inputTypes = (Oid *) palloc(sizeof(Oid) * numArguments);
		i = 0;
		foreach(l, aggref->args)
		{
			inputTypes[i++] = exprType((Node *) lfirst(l));
		}

		/* fetch aggregate transition datatype from pg_aggregate */
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_aggregate "
					" WHERE aggfnoid = :1 ",
					ObjectIdGetDatum(aggref->aggfnoid)));

		aggTuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(aggTuple))
			elog(ERROR, "cache lookup failed for aggregate %u",
				 aggref->aggfnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
		aggtranstype = aggform->aggtranstype;
		aggprelimfn = aggform->aggprelimfn;
		
		caql_endscan(pcqCtx);
		
		/* CDB wants to know whether the function can do 2-stage aggregation */
		if ( aggprelimfn == InvalidOid )
		{
			counts->missing_prelimfunc = true; /* Nope! */
		}

		/* resolve actual type of transition state, if polymorphic */
		if (aggtranstype == ANYARRAYOID || aggtranstype == ANYELEMENTOID)
		{
			/* have to fetch the agg's declared input types... */
			Oid		   *declaredArgTypes;
			int			agg_nargs;

			(void) get_func_signature(aggref->aggfnoid,
									  &declaredArgTypes, &agg_nargs);
			Assert(agg_nargs == numArguments);
			aggtranstype = enforce_generic_type_consistency(inputTypes,
															declaredArgTypes,
															agg_nargs,
															aggtranstype);
			pfree(declaredArgTypes);
			
			counts->missing_prelimfunc = true; /* CDB: Disable 2P aggregation */
		}

		/*
		 * If the transition type is pass-by-value then it doesn't add
		 * anything to the required size of the hashtable.	If it is
		 * pass-by-reference then we have to add the estimated size of the
		 * value itself, plus palloc overhead.
		 */
		if (!get_typbyval(aggtranstype))
		{
			int32		aggtranstypmod;
			int32		avgwidth;

			/*
			 * If transition state is of same type as first input, assume it's
			 * the same typmod (same width) as well.  This works for cases
			 * like MAX/MIN and is probably somewhat reasonable otherwise.
			 */
			if (numArguments > 0 && aggtranstype == inputTypes[0])
				aggtranstypmod = exprTypmod((Node *) linitial(aggref->args));
			else
				aggtranstypmod = -1;

			avgwidth = get_typavgwidth(aggtranstype, aggtranstypmod);
			avgwidth = MAXALIGN(avgwidth);

			counts->transitionSpace += avgwidth + 2 * sizeof(void *);
		}
		
		if ( inputTypes != NULL )
			pfree(inputTypes);

		/*
		 * Complain if the aggregate's arguments contain any aggregates;
		 * nested agg functions are semantically nonsensical.
		 */
		if (contain_agg_clause((Node *) aggref->args) ||
			contain_agg_clause((Node *) aggref->aggorder))
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
					 errmsg("aggregate function calls may not be nested")));

		/*
		 * Having checked that, we need not recurse into the argument.
		 */
		return false;
	}
	Assert(!IsA(node, SubLink));
	return expression_tree_walker(node, count_agg_clauses_walker,
								  (void *) counts);
}


/*****************************************************************************
 *		Support for expressions returning sets
 *****************************************************************************/

/*
 * expression_returns_set
 *	  Test whether an expression returns a set result.
 *
 * Because we use expression_tree_walker(), this can also be applied to
 * whole targetlists; it'll produce TRUE if any one of the tlist items
 * returns a set.
 */
bool
expression_returns_set(Node *clause)
{
	return expression_returns_set_walker(clause, NULL);
}

static bool
expression_returns_set_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;

		if (expr->funcretset)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;

		if (expr->opretset)
			return true;
		/* else fall through to check args */
	}

	return expression_tree_walker(node, expression_returns_set_walker,
								  context);
}

/*****************************************************************************
 *		Subplan clause manipulation
 *****************************************************************************/

/*
 * contain_subplans
 *	  Recursively search for subplan nodes within a clause.
 *
 * If we see a SubLink node, we will return TRUE.  This is only possible if
 * the expression tree hasn't yet been transformed by subselect.c.  We do not
 * know whether the node will produce a true subplan or just an initplan,
 * but we make the conservative assumption that it will be a subplan.
 *
 * Returns true if any subplan found.
 */
bool
contain_subplans(Node *clause)
{
	return contain_subplans_walker(clause, NULL);
}

static bool
contain_subplans_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, SubPlan) ||
		IsA(node, SubLink))
		return true;			/* abort the tree traversal and return true */
	return expression_tree_walker(node, contain_subplans_walker, context);
}




/*****************************************************************************
 *		Check clauses for mutable functions
 *****************************************************************************/

/*
 * contain_mutable_functions
 *	  Recursively search for mutable functions within a clause.
 *
 * Returns true if any mutable function (or operator implemented by a
 * mutable function) is found.	This test is needed so that we don't
 * mistakenly think that something like "WHERE random() < 0.5" can be treated
 * as a constant qualification.
 *
 * XXX we do not examine sub-selects to see if they contain uses of
 * mutable functions.  It's not real clear if that is correct or not...
 */
bool
contain_mutable_functions(Node *clause)
{
	return contain_mutable_functions_walker(clause, NULL);
}

static bool
contain_mutable_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

    /* the functions in predtest.c handle expressions and
     * RestrictInfo objects -- so make this function handle
     * them too for convenience */
    if (IsA(node, RestrictInfo))
    {
        RestrictInfo * info = (RestrictInfo *) node;
        return contain_mutable_functions_walker((Node*)info->clause, context);
    }

	if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;

		if (func_volatile(expr->funcid) != PROVOLATILE_IMMUTABLE)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;

		if (op_volatile(expr->opno) != PROVOLATILE_IMMUTABLE)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, DistinctExpr))
	{
		DistinctExpr *expr = (DistinctExpr *) node;

		if (op_volatile(expr->opno) != PROVOLATILE_IMMUTABLE)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;

		if (op_volatile(expr->opno) != PROVOLATILE_IMMUTABLE)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, NullIfExpr))
	{
		NullIfExpr *expr = (NullIfExpr *) node;

		if (op_volatile(expr->opno) != PROVOLATILE_IMMUTABLE)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, RowCompareExpr))
	{
		RowCompareExpr *rcexpr = (RowCompareExpr *) node;
		ListCell   *opid;

		foreach(opid, rcexpr->opnos)
		{
			if (op_volatile(lfirst_oid(opid)) != PROVOLATILE_IMMUTABLE)
				return true;
		}
		/* else fall through to check args */
	}
	return expression_tree_walker(node, contain_mutable_functions_walker,
								  context);
}


/*****************************************************************************
 *		Check clauses for volatile functions
 *****************************************************************************/

/*
 * contain_volatile_functions
 *	  Recursively search for volatile functions within a clause.
 *
 * Returns true if any volatile function (or operator implemented by a
 * volatile function) is found. This test prevents invalid conversions
 * of volatile expressions into indexscan quals.
 *
 * XXX we do not examine sub-selects to see if they contain uses of
 * volatile functions.	It's not real clear if that is correct or not...
 */
bool
contain_volatile_functions(Node *clause)
{
	return contain_volatile_functions_walker(clause, NULL);
}

static bool
contain_volatile_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;

		if (func_volatile(expr->funcid) == PROVOLATILE_VOLATILE)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;

		if (op_volatile(expr->opno) == PROVOLATILE_VOLATILE)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, DistinctExpr))
	{
		DistinctExpr *expr = (DistinctExpr *) node;

		if (op_volatile(expr->opno) == PROVOLATILE_VOLATILE)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;

		if (op_volatile(expr->opno) == PROVOLATILE_VOLATILE)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, NullIfExpr))
	{
		NullIfExpr *expr = (NullIfExpr *) node;

		if (op_volatile(expr->opno) == PROVOLATILE_VOLATILE)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, RowCompareExpr))
	{
		/* RowCompare probably can't have volatile ops, but check anyway */
		RowCompareExpr *rcexpr = (RowCompareExpr *) node;
		ListCell   *opid;

		foreach(opid, rcexpr->opnos)
		{
			if (op_volatile(lfirst_oid(opid)) == PROVOLATILE_VOLATILE)
				return true;
		}
		/* else fall through to check args */
	}
	return expression_tree_walker(node, contain_volatile_functions_walker,
								  context);
}


/*****************************************************************************
 *		Check clauses for calls to window functions
 *****************************************************************************/

/*
 * contain_window_functions
 *	  Recursively search for window functions within a clause.
 *
 * Returns true if any window function is found.
 *
 * Window function calls are only allowed in the target list of window
 * queries and may not be nested. We do not examine sub-selects for uses of
 * window functions because if there are any, they pertain to a different
 * planning level.
 */
bool
contain_window_functions(Node *clause)
{
	return contain_window_functions_walker(clause, NULL);
}

static bool
contain_window_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, WindowRef))
	{
		return true;
		/* else fall through to check args */
	}
	return expression_tree_walker(node, contain_window_functions_walker,
								  context);
}


/*****************************************************************************
 *		Check clauses for nonstrict functions
 *****************************************************************************/

/*
 * contain_nonstrict_functions
 *	  Recursively search for nonstrict functions within a clause.
 *
 * Returns true if any nonstrict construct is found --- ie, anything that
 * could produce non-NULL output with a NULL input.
 *
 * The idea here is that the caller has verified that the expression contains
 * one or more Var or Param nodes (as appropriate for the caller's need), and
 * now wishes to prove that the expression result will be NULL if any of these
 * inputs is NULL.	If we return false, then the proof succeeded.
 */
bool
contain_nonstrict_functions(Node *clause)
{
	return contain_nonstrict_functions_walker(clause, NULL);
}

static bool
contain_nonstrict_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		/* an aggregate could return non-null with null input */
		return true;
	}
	if (IsA(node, WindowRef))
	{
		/* a window function could return non-null with null input */
		return true;
	}
	if (IsA(node, ArrayRef))
	{
		/* array assignment is nonstrict, but subscripting is strict */
		if (((ArrayRef *) node)->refassgnexpr != NULL)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;

		if (!func_strict(expr->funcid))
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;

		if (!op_strict(expr->opno))
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, DistinctExpr))
	{
		/* IS DISTINCT FROM is inherently non-strict */
		return true;
	}
	if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;

		if (!is_strict_saop(expr, false))
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, BoolExpr))
	{
		BoolExpr   *expr = (BoolExpr *) node;

		switch (expr->boolop)
		{
			case AND_EXPR:
			case OR_EXPR:
				/* AND, OR are inherently non-strict */
				return true;
			default:
				break;
		}
	}
	if (IsA(node, SubLink))
	{
		/* In some cases a sublink might be strict, but in general not */
		return true;
	}
	if (IsA(node, SubPlan))
		return true;
	if (IsA(node, FieldStore))
		return true;
	if (IsA(node, CaseExpr))
		return true;
	if (IsA(node, CaseWhen))
		return true;
	if (IsA(node, ArrayExpr))
		return true;
	if (IsA(node, RowExpr))
		return true;
	if (IsA(node, RowCompareExpr))
		return true;
	if (IsA(node, CoalesceExpr))
		return true;
	if (IsA(node, MinMaxExpr))
		return true;
	if (IsA(node, NullIfExpr))
		return true;
	if (IsA(node, NullTest))
		return true;
	if (IsA(node, BooleanTest))
		return true;
	return expression_tree_walker(node, contain_nonstrict_functions_walker,
								  context);
}


/*
 * find_nonnullable_rels
 *		Determine which base rels are forced nonnullable by given clause.
 *
 * Returns the set of all Relids that are referenced in the clause in such
 * a way that the clause cannot possibly return TRUE if any of these Relids
 * is an all-NULL row.	(It is OK to err on the side of conservatism; hence
 * the analysis here is simplistic.)
 *
 * The semantics here are subtly different from contain_nonstrict_functions:
 * that function is concerned with NULL results from arbitrary expressions,
 * but here we assume that the input is a Boolean expression, and wish to
 * see if NULL inputs will provably cause a FALSE-or-NULL result.  We expect
 * the expression to have been AND/OR flattened and converted to implicit-AND
 * format.
 *
 * We don't use expression_tree_walker here because we don't want to
 * descend through very many kinds of nodes; only the ones we can be sure
 * are strict.	We can descend through the top level of implicit AND'ing,
 * but not through any explicit ANDs (or ORs) below that, since those are not
 * strict constructs.  The List case handles the top-level implicit AND list
 * as well as lists of arguments to strict operators/functions.
 */
Relids
find_nonnullable_rels(Node *clause)
{
	return find_nonnullable_rels_walker(clause, true);
}

static Relids
find_nonnullable_rels_walker(Node *node, bool top_level)
{
	Relids		result = NULL;

	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup == 0)
			result = bms_make_singleton(var->varno);
	}
	else if (IsA(node, List))
	{
		ListCell   *l;

		foreach(l, (List *) node)
		{
			result = bms_join(result,
							  find_nonnullable_rels_walker(lfirst(l),
														   top_level));
		}
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;

		if (func_strict(expr->funcid))
			result = find_nonnullable_rels_walker((Node *) expr->args, false);
	}
	else if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;

		if (op_strict(expr->opno))
			result = find_nonnullable_rels_walker((Node *) expr->args, false);
	}
	else if (IsA(node, ScalarArrayOpExpr))
	{
		/* Strict if it's "foo op ANY array" and op is strict */
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;

		if (expr->useOr && op_strict(expr->opno))
			result = find_nonnullable_rels_walker((Node *) expr->args, false);
	}
	else if (IsA(node, BoolExpr))
	{
		BoolExpr   *expr = (BoolExpr *) node;

		/* NOT is strict, others are not */
		if (expr->boolop == NOT_EXPR)
			result = find_nonnullable_rels_walker((Node *) expr->args, false);
	}
	else if (IsA(node, RelabelType))
	{
		RelabelType *expr = (RelabelType *) node;

		result = find_nonnullable_rels_walker((Node *) expr->arg, top_level);
	}
	else if (IsA(node, ConvertRowtypeExpr))
	{
		/* not clear this is useful, but it can't hurt */
		ConvertRowtypeExpr *expr = (ConvertRowtypeExpr *) node;

		result = find_nonnullable_rels_walker((Node *) expr->arg, top_level);
	}
	else if (IsA(node, NullTest))
	{
		NullTest   *expr = (NullTest *) node;

		/*
		 * IS NOT NULL can be considered strict, but only at top level; else
		 * we might have something like NOT (x IS NOT NULL).
		 */
		if (top_level && expr->nulltesttype == IS_NOT_NULL)
			result = find_nonnullable_rels_walker((Node *) expr->arg, false);
	}
	else if (IsA(node, BooleanTest))
	{
		BooleanTest *expr = (BooleanTest *) node;

		/*
		 * Appropriate boolean tests are strict at top level.
		 */
		if (top_level &&
			(expr->booltesttype == IS_TRUE ||
			 expr->booltesttype == IS_FALSE ||
			 expr->booltesttype == IS_NOT_UNKNOWN))
			result = find_nonnullable_rels_walker((Node *) expr->arg, false);
	}
	return result;
}

/*
 * Can we treat a ScalarArrayOpExpr as strict?
 *
 * If "falseOK" is true, then a "false" result can be considered strict,
 * else we need to guarantee an actual NULL result for NULL input.
 *
 * "foo op ALL array" is strict if the op is strict *and* we can prove
 * that the array input isn't an empty array.  We can check that
 * for the cases of an array constant and an ARRAY[] construct.
 *
 * "foo op ANY array" is strict in the falseOK sense if the op is strict.
 * If not falseOK, the test is the same as for "foo op ALL array".
 */
static bool
is_strict_saop(ScalarArrayOpExpr *expr, bool falseOK)
{
	Node	   *rightop;

	/* The contained operator must be strict. */
	if (!op_strict(expr->opno))
		return false;
	/* If ANY and falseOK, that's all we need to check. */
	if (expr->useOr && falseOK)
		return true;
	/* Else, we have to see if the array is provably non-empty. */
	Assert(list_length(expr->args) == 2);
	rightop = (Node *) lsecond(expr->args);
	if (rightop && IsA(rightop, Const))
	{
		Datum		arraydatum = ((Const *) rightop)->constvalue;
		bool		arrayisnull = ((Const *) rightop)->constisnull;
		ArrayType  *arrayval;
		int			nitems;

		if (arrayisnull)
			return false;
		arrayval = DatumGetArrayTypeP(arraydatum);
		nitems = ArrayGetNItems(ARR_NDIM(arrayval), ARR_DIMS(arrayval));
		if (nitems > 0)
			return true;
	}
	else if (rightop && IsA(rightop, ArrayExpr))
	{
		ArrayExpr  *arrayexpr = (ArrayExpr *) rightop;

		if (arrayexpr->elements != NIL && !arrayexpr->multidims)
			return true;
	}
	return false;
}


/*****************************************************************************
 *		Check for "pseudo-constant" clauses
 *****************************************************************************/

/*
 * is_pseudo_constant_clause
 *	  Detect whether an expression is "pseudo constant", ie, it contains no
 *	  variables of the current query level and no uses of volatile functions.
 *	  Such an expr is not necessarily a true constant: it can still contain
 *	  Params and outer-level Vars, not to mention functions whose results
 *	  may vary from one statement to the next.	However, the expr's value
 *	  will be constant over any one scan of the current query, so it can be
 *	  used as, eg, an indexscan key.
 */
bool
is_pseudo_constant_clause(Node *clause)
{
	/*
	 * We could implement this check in one recursive scan.  But since the
	 * check for volatile functions is both moderately expensive and unlikely
	 * to fail, it seems better to look for Vars first and only check for
	 * volatile functions if we find no Vars.
	 */
	if (!contain_var_clause(clause) &&
		!contain_volatile_functions(clause))
		return true;
	return false;
}

/*
 * is_pseudo_constant_clause_relids
 *	  Same as above, except caller already has available the var membership
 *	  of the expression; this lets us avoid the contain_var_clause() scan.
 */
bool
is_pseudo_constant_clause_relids(Node *clause, Relids relids)
{
	if (bms_is_empty(relids) &&
		!contain_volatile_functions(clause))
		return true;
	return false;
}


/*****************************************************************************
 *		Tests on clauses of queries
 *
 * Possibly this code should go someplace else, since this isn't quite the
 * same meaning of "clause" as is used elsewhere in this module.  But I can't
 * think of a better place for it...
 *****************************************************************************/

/*
 * Test whether a query uses DISTINCT ON, ie, has a distinct-list that is
 * not the same as the set of output columns.
 */
bool
has_distinct_on_clause(Query *query)
{
	ListCell   *l;

	/* Is there a DISTINCT clause at all? */
	if (query->distinctClause == NIL)
		return false;

	/*
	 * If the DISTINCT list contains all the nonjunk targetlist items, and
	 * nothing else (ie, no junk tlist items), then it's a simple DISTINCT,
	 * else it's DISTINCT ON.  We do not require the lists to be in the same
	 * order (since the parser may have adjusted the DISTINCT clause ordering
	 * to agree with ORDER BY).  Furthermore, a non-DISTINCT junk tlist item
	 * that is in the sortClause is also evidence of DISTINCT ON, since we
	 * don't allow ORDER BY on junk tlist items when plain DISTINCT is used.
	 *
	 * This code assumes that the DISTINCT list is valid, ie, all its entries
	 * match some entry of the tlist.
	 */
	foreach(l, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (tle->ressortgroupref == 0)
		{
			if (tle->resjunk)
				continue;		/* we can ignore unsorted junk cols */
			return true;		/* definitely not in DISTINCT list */
		}
		if (targetIsInSortGroupList(tle, query->distinctClause))
		{
			if (tle->resjunk)
				return true;	/* junk TLE in DISTINCT means DISTINCT ON */
			/* else this TLE is okay, keep looking */
		}
		else
		{
			/* This TLE is not in DISTINCT list */
			if (!tle->resjunk)
				return true;	/* non-junk, non-DISTINCT, so DISTINCT ON */
			if (targetIsInSortGroupList(tle, query->sortClause))
				return true;	/* sorted, non-distinct junk */
			/* unsorted junk is okay, keep looking */
		}
	}
	/* It's a simple DISTINCT */
	return false;
}

/*
 * Test whether a query uses simple DISTINCT, ie, has a distinct-list that
 * is the same as the set of output columns.
 */
bool
has_distinct_clause(Query *query)
{
	/* Is there a DISTINCT clause at all? */
	if (query->distinctClause == NIL)
		return false;

	/* It's DISTINCT if it's not DISTINCT ON */
	return !has_distinct_on_clause(query);
}


/*****************************************************************************
 *																			 *
 *		General clause-manipulating routines								 *
 *																			 *
 *****************************************************************************/

/*
 * NumRelids
 *		(formerly clause_relids)
 *
 * Returns the number of different relations referenced in 'clause'.
 */
int
NumRelids(Node *clause)
{
	Relids		varnos = pull_varnos(clause);
	int			result = bms_num_members(varnos);

	bms_free(varnos);
	return result;
}

/*
 * CommuteOpExpr: commute a binary operator clause
 *
 * XXX the clause is destructively modified!
 */
void
CommuteOpExpr(OpExpr *clause)
{
	Oid			opoid;
	Node	   *temp;

	/* Sanity checks: caller is at fault if these fail */
	if (!is_opclause(clause) ||
		list_length(clause->args) != 2)
		elog(ERROR, "cannot commute non-binary-operator clause");

	opoid = get_commutator(clause->opno);

	if (!OidIsValid(opoid))
		elog(ERROR, "could not find commutator for operator %u",
			 clause->opno);

	/*
	 * modify the clause in-place!
	 */
	clause->opno = opoid;
	clause->opfuncid = InvalidOid;
	/* opresulttype and opretset are assumed not to change */

	temp = linitial(clause->args);
	linitial(clause->args) = lsecond(clause->args);
	lsecond(clause->args) = temp;
}

/*
 * CommuteRowCompareExpr: commute a RowCompareExpr clause
 *
 * XXX the clause is destructively modified!
 */
void
CommuteRowCompareExpr(RowCompareExpr *clause)
{
	List	   *newops;
	List	   *temp;
	ListCell   *l;

	/* Sanity checks: caller is at fault if these fail */
	if (!IsA(clause, RowCompareExpr))
		elog(ERROR, "expected a RowCompareExpr");

	/* Build list of commuted operators */
	newops = NIL;
	foreach(l, clause->opnos)
	{
		Oid			opoid = lfirst_oid(l);

		opoid = get_commutator(opoid);
		if (!OidIsValid(opoid))
			elog(ERROR, "could not find commutator for operator %u",
				 lfirst_oid(l));
		newops = lappend_oid(newops, opoid);
	}

	/*
	 * modify the clause in-place!
	 */
	switch (clause->rctype)
	{
		case ROWCOMPARE_LT:
			clause->rctype = ROWCOMPARE_GT;
			break;
		case ROWCOMPARE_LE:
			clause->rctype = ROWCOMPARE_GE;
			break;
		case ROWCOMPARE_GE:
			clause->rctype = ROWCOMPARE_LE;
			break;
		case ROWCOMPARE_GT:
			clause->rctype = ROWCOMPARE_LT;
			break;
		default:
			elog(ERROR, "unexpected RowCompare type: %d",
				 (int) clause->rctype);
			break;
	}

	clause->opnos = newops;

	/*
	 * Note: we don't bother to update the opclasses list, but just set it to
	 * empty.  This is OK since this routine is currently only used for index
	 * quals, and the index machinery won't use the opclass information.  The
	 * original opclass list is NOT valid if we have commuted any cross-type
	 * comparisons, so don't leave it in place.
	 */
	clause->opclasses = NIL;	/* XXX */

	temp = clause->largs;
	clause->largs = clause->rargs;
	clause->rargs = temp;
}

/*
 * strip_implicit_coercions: remove implicit coercions at top level of tree
 *
 * Note: there isn't any useful thing we can do with a RowExpr here, so
 * just return it unchanged, even if it's marked as an implicit coercion.
 */
Node *
strip_implicit_coercions(Node *node)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, FuncExpr))
	{
		FuncExpr   *f = (FuncExpr *) node;

		if (f->funcformat == COERCE_IMPLICIT_CAST)
			return strip_implicit_coercions(linitial(f->args));
	}
	else if (IsA(node, RelabelType))
	{
		RelabelType *r = (RelabelType *) node;

		if (r->relabelformat == COERCE_IMPLICIT_CAST)
			return strip_implicit_coercions((Node *) r->arg);
	}
	else if (IsA(node, ConvertRowtypeExpr))
	{
		ConvertRowtypeExpr *c = (ConvertRowtypeExpr *) node;

		if (c->convertformat == COERCE_IMPLICIT_CAST)
			return strip_implicit_coercions((Node *) c->arg);
	}
	else if (IsA(node, CoerceToDomain))
	{
		CoerceToDomain *c = (CoerceToDomain *) node;

		if (c->coercionformat == COERCE_IMPLICIT_CAST)
			return strip_implicit_coercions((Node *) c->arg);
	}
	return node;
}

/*
 * set_coercionform_dontcare: set all CoercionForm fields to COERCE_DONTCARE
 *
 * This is used to make index expressions and index predicates more easily
 * comparable to clauses of queries.  CoercionForm is not semantically
 * significant (for cases where it does matter, the significant info is
 * coded into the coercion function arguments) so we can ignore it during
 * comparisons.  Thus, for example, an index on "foo::int4" can match an
 * implicit coercion to int4.
 *
 * Caution: the passed expression tree is modified in-place.
 */
void
set_coercionform_dontcare(Node *node)
{
	(void) set_coercionform_dontcare_walker(node, NULL);
}

static bool
set_coercionform_dontcare_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, FuncExpr))
		((FuncExpr *) node)->funcformat = COERCE_DONTCARE;
	else if (IsA(node, RelabelType))
		((RelabelType *) node)->relabelformat = COERCE_DONTCARE;
	else if (IsA(node, ConvertRowtypeExpr))
		((ConvertRowtypeExpr *) node)->convertformat = COERCE_DONTCARE;
	else if (IsA(node, RowExpr))
		((RowExpr *) node)->row_format = COERCE_DONTCARE;
	else if (IsA(node, CoerceToDomain))
		((CoerceToDomain *) node)->coercionformat = COERCE_DONTCARE;
	return expression_tree_walker(node, set_coercionform_dontcare_walker,
								  context);
}

/*
 * Helper for eval_const_expressions: check that datatype of an attribute
 * is still what it was when the expression was parsed.  This is needed to
 * guard against improper simplification after ALTER COLUMN TYPE.  (XXX we
 * may well need to make similar checks elsewhere?)
 */
static bool
rowtype_field_matches(Oid rowtypeid, int fieldnum,
					  Oid expectedtype, int32 expectedtypmod)
{
	TupleDesc	tupdesc;
	Form_pg_attribute attr;

	/* No issue for RECORD, since there is no way to ALTER such a type */
	if (rowtypeid == RECORDOID)
		return true;
	tupdesc = lookup_rowtype_tupdesc(rowtypeid, -1);
	if (fieldnum <= 0 || fieldnum > tupdesc->natts)
	{
		ReleaseTupleDesc(tupdesc);
		return false;
	}
	attr = tupdesc->attrs[fieldnum - 1];
	if (attr->attisdropped ||
		attr->atttypid != expectedtype ||
		attr->atttypmod != expectedtypmod)
	{
		ReleaseTupleDesc(tupdesc);
		return false;
	}
	ReleaseTupleDesc(tupdesc);
	return true;
}


/**
 * fold_constants
 *
 * Recurses into query tree and folds all constant expressions.
 */
Query *fold_constants(Query *q, ParamListInfo boundParams, Size max_size)
{
	eval_const_expressions_context context;

	context.boundParams = boundParams;
	context.active_fns = NIL;	/* nothing being recursively simplified */
	context.case_val = NULL;	/* no CASE being examined */
	context.transform_stable_funcs = true;	/* safe transformations only */
	context.recurse_queries = true; /* recurse into query structures */
	context.recurse_sublink_testexpr = false; /* do not recurse into sublink test expressions */
	context.transform_saop = false; /* do not transform scalar array ops */
	context.transform_functions_returning_composite_values = true;
	if (optimizer)
	{
		/* when optimizer is on then do not fold functions that return a constant of composite values */
		context.transform_functions_returning_composite_values = false;
	}
	context.max_size = max_size;
	
	return (Query *) query_or_expression_tree_mutator
						(
						(Node *) q,
						eval_const_expressions_mutator,
						&context,
						0
						);
}

/**
 * fold_arrayexpr_constants
 *
 * Fold array expression for optimizer
 */
Node *fold_arrayexpr_constants(ArrayExpr *arrayexpr)
{
	if (optimizer)
	{
		eval_const_expressions_context context;

		context.boundParams = NULL;
		context.active_fns = NIL;	/* nothing being recursively simplified */
		context.case_val = NULL;	/* no CASE being examined */
		context.transform_stable_funcs = true;	/* safe transformations only */
		context.recurse_queries = false; /* do not recurse into query structures */
		context.recurse_sublink_testexpr = false; /* do not recurse into sublink test expressions */
		context.transform_saop = true; /* transform scalar array ops */
		context.transform_functions_returning_composite_values = true;

		/* when optimizer is on then do not fold functions that return a constant of composite values */
		context.transform_functions_returning_composite_values = false;

		context.max_size = GPOPT_MAX_FOLDED_CONSTANT_SIZE;

		return eval_const_expressions_mutator((Node *) arrayexpr, &context);
	}

	return (Node*) arrayexpr;
}

/*--------------------
 * eval_const_expressions
 *
 * Reduce any recognizably constant subexpressions of the given
 * expression tree, for example "2 + 2" => "4".  More interestingly,
 * we can reduce certain boolean expressions even when they contain
 * non-constant subexpressions: "x OR true" => "true" no matter what
 * the subexpression x is.	(XXX We assume that no such subexpression
 * will have important side-effects, which is not necessarily a good
 * assumption in the presence of user-defined functions; do we need a
 * pg_proc flag that prevents discarding the execution of a function?)
 *
 * We do understand that certain functions may deliver non-constant
 * results even with constant inputs, "nextval()" being the classic
 * example.  Functions that are not marked "immutable" in pg_proc
 * will not be pre-evaluated here, although we will reduce their
 * arguments as far as possible.
 *
 * We assume that the tree has already been type-checked and contains
 * only operators and functions that are reasonable to try to execute.
 *
 * NOTE: "root" can be passed as NULL if the caller never wants to do any
 * Param substitutions.
 *
 * NOTE: the planner assumes that this will always flatten nested AND and
 * OR clauses into N-argument form.  See comments in prepqual.c.
 *
 * NOTE: another critical effect is that any function calls that require
 * default arguments will be expanded.
 *--------------------
 */
Node *
eval_const_expressions(PlannerInfo *root, Node *node)
{
	eval_const_expressions_context context;

	if (root)
	{
		context.boundParams = root->glob->boundParams;	/* bound Params */
	}
	else
	{
		context.boundParams = NULL;
	}
	context.active_fns = NIL;	/* nothing being recursively simplified */
	context.case_val = NULL;	/* no CASE being examined */
	context.transform_stable_funcs = true;	/* safe transformations only */
	context.recurse_queries = false; /* do not recurse into query structures */
	context.recurse_sublink_testexpr = true;
	context.transform_saop = true; 	/* transform scalar array ops */
	context.max_size = 0;
	context.transform_functions_returning_composite_values = true;

	return eval_const_expressions_mutator(node, &context);
}

/*--------------------
 * estimate_expression_value
 *
 * This function attempts to estimate the value of an expression for
 * planning purposes.  It is in essence a more aggressive version of
 * eval_const_expressions(): we will perform constant reductions that are
 * not necessarily 100% safe, but are reasonable for estimation purposes.
 *
 * Currently the extra steps that are taken in this mode are:
 * 1. Substitute values for Params, where a bound Param value has been made
 *	  available by the caller of planner(), even if the Param isn't marked
 *	  constant.  This effectively means that we plan using the first supplied
 *	  value of the Param.
 * 2. Fold stable, as well as immutable, functions to constants.
 *--------------------
 */
Node *
estimate_expression_value(PlannerInfo *root, Node *node)
{
	eval_const_expressions_context context;

	context.boundParams = root->glob->boundParams;		/* bound Params */
	context.active_fns = NIL;	/* nothing being recursively simplified */
	context.case_val = NULL;	/* no CASE being examined */
	context.transform_stable_funcs = true;	/* unsafe transformations OK */
	context.recurse_queries = false; /* do not recurse into query structures */
	context.recurse_sublink_testexpr = true;
	context.transform_saop = true; 	/* transform scalar array ops */
	context.max_size = 0;
	context.transform_functions_returning_composite_values = true;

	return eval_const_expressions_mutator(node, &context);
}

static Node *
eval_const_expressions_mutator(Node *node,
							   eval_const_expressions_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		/* Look to see if we've been given a value for this Param */
		if (param->paramkind == PARAM_EXTERN &&
			context->boundParams != NULL &&
			param->paramid > 0 &&
			param->paramid <= context->boundParams->numParams)
		{
			ParamExternData *prm = &context->boundParams->params[param->paramid - 1];

			if (OidIsValid(prm->ptype))
			{
				/* OK to substitute parameter value? */
				if (context->transform_stable_funcs || (prm->pflags & PARAM_FLAG_CONST))
				{
					/*
					 * Return a Const representing the param value.  Must copy
					 * pass-by-ref datatypes, since the Param might be in a
					 * memory context shorter-lived than our output plan
					 * should be.
					 */
					int16		typLen;
					bool		typByVal;
					Datum		pval;

					Assert(prm->ptype == param->paramtype);
					get_typlenbyval(param->paramtype, &typLen, &typByVal);
					if (prm->isnull || typByVal)
						pval = prm->value;
					else
						pval = datumCopy(prm->value, typByVal, typLen);
					return (Node *) makeConst(param->paramtype,
											  -1,
											  (int) typLen,
											  pval,
											  prm->isnull,
											  typByVal);
				}
			}
		}
		/* Not replaceable, so just copy the Param (no need to recurse) */
		return (Node *) copyObject(param);
	}
	if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;

		if (!context->transform_functions_returning_composite_values && type_is_rowtype(expr->funcresulttype))
		{
			return copyObject(expr);
		}

		List	   *args = NIL;
		Expr	   *simple = NULL;
		FuncExpr   *newexpr = NULL;

		/*
		 * Reduce constants in the FuncExpr's arguments.  We know args is
		 * either NIL or a List node, so we can call expression_tree_mutator
		 * directly rather than recursing to self.
		 */
		args = (List *) expression_tree_mutator((Node *) expr->args,
											  eval_const_expressions_mutator,
												(void *) context);

		/*
		 * Code for op/func reduction is pretty bulky, so split it out as a
		 * separate function.
		 */
		simple = simplify_function(expr->funcid, expr->funcresulttype, args,
								   true, context);
		if (simple)				/* successfully simplified it */
			return (Node *) simple;

		/*
		 * The expression cannot be simplified any further, so build and
		 * return a replacement FuncExpr node using the possibly-simplified
		 * arguments.
		 */
		newexpr = makeNode(FuncExpr);
		newexpr->funcid = expr->funcid;
		newexpr->funcresulttype = expr->funcresulttype;
		newexpr->funcretset = expr->funcretset;
		newexpr->funcformat = expr->funcformat;
		newexpr->args = args;
		return (Node *) newexpr;
	}
	if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;
		List	   *args;
		Expr	   *simple;
		OpExpr	   *newexpr;

		/*
		 * Reduce constants in the OpExpr's arguments.  We know args is either
		 * NIL or a List node, so we can call expression_tree_mutator directly
		 * rather than recursing to self.
		 */
		args = (List *) expression_tree_mutator((Node *) expr->args,
											  eval_const_expressions_mutator,
												(void *) context);

		/*
		 * Need to get OID of underlying function.	Okay to scribble on input
		 * to this extent.
		 */
		set_opfuncid(expr);
		
		/*
		 * CDB: don't optimize divide, because we might hit a divide by zero in
		 * an expression that won't necessarily get executed later.  2006-01-09
		 */
		if ((expr->opfuncid >=153 && expr->opfuncid <=157) ||
			(expr->opfuncid >=172 && expr->opfuncid <=176))
		{
			simple = NULL;
		}
		else

		/*
		 * Code for op/func reduction is pretty bulky, so split it out as a
		 * separate function.
		 */
		simple = simplify_function(expr->opfuncid, expr->opresulttype, args,
								   true, context);
		if (simple)				/* successfully simplified it */
			return (Node *) simple;

		/*
		 * If the operator is boolean equality, we know how to simplify cases
		 * involving one constant and one non-constant argument.
		 */
		if (expr->opno == BooleanEqualOperator)
		{
			simple = simplify_boolean_equality(args);
			if (simple)			/* successfully simplified it */
				return (Node *) simple;
		}

		/*
		 * The expression cannot be simplified any further, so build and
		 * return a replacement OpExpr node using the possibly-simplified
		 * arguments.
		 */
		newexpr = makeNode(OpExpr);
		newexpr->opno = expr->opno;
		newexpr->opfuncid = expr->opfuncid;
		newexpr->opresulttype = expr->opresulttype;
		newexpr->opretset = expr->opretset;
		newexpr->args = args;
		return (Node *) newexpr;
	}
	if (IsA(node, DistinctExpr))
	{
		DistinctExpr *expr = (DistinctExpr *) node;
		List	   *args;
		ListCell   *arg;
		bool		has_null_input = false;
		bool		all_null_input = true;
		bool		has_nonconst_input = false;
		Expr	   *simple;
		DistinctExpr *newexpr;

		/*
		 * Reduce constants in the DistinctExpr's arguments.  We know args is
		 * either NIL or a List node, so we can call expression_tree_mutator
		 * directly rather than recursing to self.
		 */
		args = (List *) expression_tree_mutator((Node *) expr->args,
											  eval_const_expressions_mutator,
												(void *) context);

		/*
		 * We must do our own check for NULLs because DistinctExpr has
		 * different results for NULL input than the underlying operator does.
		 */
		foreach(arg, args)
		{
			if (IsA(lfirst(arg), Const))
			{
				has_null_input |= ((Const *) lfirst(arg))->constisnull;
				all_null_input &= ((Const *) lfirst(arg))->constisnull;
			}
			else
				has_nonconst_input = true;
		}

		/* all constants? then can optimize this out */
		if (!has_nonconst_input)
		{
			/* all nulls? then not distinct */
			if (all_null_input)
				return makeBoolConst(false, false);

			/* one null? then distinct */
			if (has_null_input)
				return makeBoolConst(true, false);

			/* otherwise try to evaluate the '=' operator */
			/* (NOT okay to try to inline it, though!) */

			/*
			 * Need to get OID of underlying function.	Okay to scribble on
			 * input to this extent.
			 */
			set_opfuncid((OpExpr *) expr);		/* rely on struct equivalence */

			/*
			 * Code for op/func reduction is pretty bulky, so split it out as
			 * a separate function.
			 */
			simple = simplify_function(expr->opfuncid, expr->opresulttype,
									   args, false, context);
			if (simple)			/* successfully simplified it */
			{
				/*
				 * Since the underlying operator is "=", must negate its
				 * result
				 */
				Const	   *csimple = (Const *) simple;

				Assert(IsA(csimple, Const));
				csimple->constvalue =
					BoolGetDatum(!DatumGetBool(csimple->constvalue));
				return (Node *) csimple;
			}
		}

		/*
		 * The expression cannot be simplified any further, so build and
		 * return a replacement DistinctExpr node using the
		 * possibly-simplified arguments.
		 */
		newexpr = makeNode(DistinctExpr);
		newexpr->opno = expr->opno;
		newexpr->opfuncid = expr->opfuncid;
		newexpr->opresulttype = expr->opresulttype;
		newexpr->opretset = expr->opretset;
		newexpr->args = args;
		return (Node *) newexpr;
	}
	if (IsA(node, BoolExpr))
	{
		BoolExpr   *expr = (BoolExpr *) node;

		switch (expr->boolop)
		{
			case OR_EXPR:
				{
					List	   *newargs;
					bool		haveNull = false;
					bool		forceTrue = false;

					newargs = simplify_or_arguments(expr->args, context,
													&haveNull, &forceTrue);
					if (forceTrue)
						return makeBoolConst(true, false);
					if (haveNull)
						newargs = lappend(newargs, makeBoolConst(false, true));
					/* If all the inputs are FALSE, result is FALSE */
					if (newargs == NIL)
						return makeBoolConst(false, false);
					/* If only one nonconst-or-NULL input, it's the result */
					if (list_length(newargs) == 1)
						return (Node *) linitial(newargs);
					/* Else we still need an OR node */
					return (Node *) make_orclause(newargs);
				}
			case AND_EXPR:
				{
					List	   *newargs;
					bool		haveNull = false;
					bool		forceFalse = false;

					newargs = simplify_and_arguments(expr->args, context,
													 &haveNull, &forceFalse);
					if (forceFalse)
						return makeBoolConst(false, false);
					if (haveNull)
						newargs = lappend(newargs, makeBoolConst(false, true));
					/* If all the inputs are TRUE, result is TRUE */
					if (newargs == NIL)
						return makeBoolConst(true, false);
					/* If only one nonconst-or-NULL input, it's the result */
					if (list_length(newargs) == 1)
						return (Node *) linitial(newargs);
					/* Else we still need an AND node */
					return (Node *) make_andclause(newargs);
				}
			case NOT_EXPR:
				{
					Node	   *arg;

					Assert(list_length(expr->args) == 1);
					arg = eval_const_expressions_mutator(linitial(expr->args),
														 context);
					if (IsA(arg, Const))
					{
						Const	   *const_input = (Const *) arg;

						/* NOT NULL => NULL */
						if (const_input->constisnull)
							return makeBoolConst(false, true);
						/* otherwise pretty easy */
						return makeBoolConst(!DatumGetBool(const_input->constvalue),
											 false);
					}
					else if (not_clause(arg))
					{
						/* Cancel NOT/NOT */
						return (Node *) get_notclausearg((Expr *) arg);
					}
					/* Else we still need a NOT node */
					return (Node *) make_notclause((Expr *) arg);
				}
			default:
				elog(ERROR, "unrecognized boolop: %d",
					 (int) expr->boolop);
				break;
		}
	}
	if (IsA(node, SubPlan))
	{
		/*
		 * Return a SubPlan unchanged --- too late to do anything with it.
		 *
		 * XXX should we ereport() here instead?  Probably this routine should
		 * never be invoked after SubPlan creation.
		 */
		return node;
	}
	if (IsA(node, RelabelType))
	{
		/*
		 * If we can simplify the input to a constant, then we don't need the
		 * RelabelType node anymore: just change the type field of the Const
		 * node.  Otherwise, must copy the RelabelType node.
		 */
		RelabelType *relabel = (RelabelType *) node;
		Node	   *arg;

		arg = eval_const_expressions_mutator((Node *) relabel->arg,
											 context);

		/*
		 * If we find stacked RelabelTypes (eg, from foo :: int :: oid) we can
		 * discard all but the top one.
		 */
		while (arg && IsA(arg, RelabelType))
			arg = (Node *) ((RelabelType *) arg)->arg;

		if (arg && IsA(arg, Const))
		{
			Const	   *con = (Const *) arg;

			con->consttype = relabel->resulttype;

			/*
			 * relabel's resulttypmod is discarded, which is OK for now; if
			 * the type actually needs a runtime length coercion then there
			 * should be a function call to do it just above this node.
			 */
			return (Node *) con;
		}
		else
		{
			RelabelType *newrelabel = makeNode(RelabelType);

			newrelabel->arg = (Expr *) arg;
			newrelabel->resulttype = relabel->resulttype;
			newrelabel->resulttypmod = relabel->resulttypmod;
			newrelabel->relabelformat = relabel->relabelformat;
			return (Node *) newrelabel;
		}
	}
	if (IsA(node, CaseExpr))
	{
		/*----------
		 * CASE expressions can be simplified if there are constant
		 * condition clauses:
		 *		FALSE (or NULL): drop the alternative
		 *		TRUE: drop all remaining alternatives
		 * If the first non-FALSE alternative is a constant TRUE, we can
		 * simplify the entire CASE to that alternative's expression.
		 * If there are no non-FALSE alternatives, we simplify the entire
		 * CASE to the default result (ELSE result).
		 *
		 * If we have a simple-form CASE with constant test expression,
		 * we substitute the constant value for contained CaseTestExpr
		 * placeholder nodes, so that we have the opportunity to reduce
		 * constant test conditions.  For example this allows
		 *		CASE 0 WHEN 0 THEN 1 ELSE 1/0 END
		 * to reduce to 1 rather than drawing a divide-by-0 error.
		 *----------
		 */
		CaseExpr   *caseexpr = (CaseExpr *) node;
		CaseExpr   *newcase;
		Node	   *save_case_val;
		Node	   *newarg;
		List	   *newargs;
		bool		const_true_cond;
		Node	   *defresult = NULL;
		ListCell   *arg;

		/* Simplify the test expression, if any */
		newarg = eval_const_expressions_mutator((Node *) caseexpr->arg,
												context);

		/* Set up for contained CaseTestExpr nodes */
		save_case_val = context->case_val;
		if (newarg && IsA(newarg, Const))
			context->case_val = newarg;
		else
			context->case_val = NULL;

		/* Simplify the WHEN clauses */
		newargs = NIL;
		const_true_cond = false;
		foreach(arg, caseexpr->args)
		{
			CaseWhen   *oldcasewhen = (CaseWhen *) lfirst(arg);
			Node	   *casecond;
			Node	   *caseresult;

			Assert(IsA(oldcasewhen, CaseWhen));

			/* Simplify this alternative's test condition */
			casecond =
				eval_const_expressions_mutator((Node *) oldcasewhen->expr,
											   context);

			/*
			 * If the test condition is constant FALSE (or NULL), then drop
			 * this WHEN clause completely, without processing the result.
			 */
			if (casecond && IsA(casecond, Const))
			{
				Const	   *const_input = (Const *) casecond;

				if (const_input->constisnull ||
					!DatumGetBool(const_input->constvalue))
					continue;	/* drop alternative with FALSE condition */
				/* Else it's constant TRUE */
				const_true_cond = true;
			}

			/* Simplify this alternative's result value */
			caseresult =
				eval_const_expressions_mutator((Node *) oldcasewhen->result,
											   context);

			/* If non-constant test condition, emit a new WHEN node */
			if (!const_true_cond)
			{
				CaseWhen   *newcasewhen = makeNode(CaseWhen);

				newcasewhen->expr = (Expr *) casecond;
				newcasewhen->result = (Expr *) caseresult;
				newargs = lappend(newargs, newcasewhen);
				continue;
			}

			/*
			 * Found a TRUE condition, so none of the remaining alternatives
			 * can be reached.	We treat the result as the default result.
			 */
			defresult = caseresult;
			break;
		}

		/* Simplify the default result, unless we replaced it above */
		if (!const_true_cond)
			defresult =
				eval_const_expressions_mutator((Node *) caseexpr->defresult,
											   context);

		context->case_val = save_case_val;

		/* If no non-FALSE alternatives, CASE reduces to the default result */
		if (newargs == NIL)
			return defresult;
		/* Otherwise we need a new CASE node */
		newcase = makeNode(CaseExpr);
		newcase->casetype = caseexpr->casetype;
		newcase->arg = (Expr *) newarg;
		newcase->args = newargs;
		newcase->defresult = (Expr *) defresult;
		return (Node *) newcase;
	}
	if (IsA(node, CaseTestExpr))
	{
		/*
		 * If we know a constant test value for the current CASE construct,
		 * substitute it for the placeholder.  Else just return the
		 * placeholder as-is.
		 */
		if (context->case_val)
			return copyObject(context->case_val);
		else
			return copyObject(node);
	}

	if (IsA(node, ScalarArrayOpExpr ))
	{
        ScalarArrayOpExpr *saop;
        Node *scalar;
        Node *values;

		saop = (ScalarArrayOpExpr *) expression_tree_mutator(node, eval_const_expressions_mutator,
															 (void *) context);

		Assert( IsA(saop, ScalarArrayOpExpr));
		Assert( list_length(saop->args) == 2);

		scalar = (Node *) linitial(saop->args);
		values = (Node *) lsecond(saop->args);

		if ( IsA(scalar, Const) && IsA(values, Const))
		{
			Node *result = (Node *) evaluate_expr( (Expr *) saop, BOOLOID);
			Assert(IsA(result, Const));
			return result;
		}

		/* note a couple things:
		 *
		 * 1) If values is not a constant (it could be an ArrayExpr) then that means
		 *    the ArrayExpr could not be reduced to a constant ...
		 * 2) We could further improve this by, for OR scalar array ops, extract only the Const values
		 *    from the array and make a constant out of those to evaluate.  If it evaluates to true then
		 *    we can return true, otherwise we can return a new ScalarArrayOpExpr with values being
		 *    only the non-Const values.  This seems overkill: I think      a in (1,2,3,x,y,z)    is rare
		 */

        return (Node *) saop; /* this has been walked and is a new one */
	}
	if (IsA(node, ArrayExpr) && context->transform_saop)
	{
		ArrayExpr  *arrayexpr = (ArrayExpr *) node;
		ArrayExpr  *newarray;
		bool		all_const = true;
		List	   *newelems;
		ListCell   *element;

		newelems = NIL;
		foreach(element, arrayexpr->elements)
		{
			Node	   *e;

			e = eval_const_expressions_mutator((Node *) lfirst(element),
											   context);
			if (!IsA(e, Const))
				all_const = false;
			newelems = lappend(newelems, e);
		}

		newarray = makeNode(ArrayExpr);
		newarray->array_typeid = arrayexpr->array_typeid;
		newarray->element_typeid = arrayexpr->element_typeid;
		newarray->elements = newelems;
		newarray->multidims = arrayexpr->multidims;

		if (all_const)
			return (Node *) evaluate_expr((Expr *) newarray,
										  newarray->array_typeid);

		return (Node *) newarray;
	}
	if (IsA(node, CoalesceExpr))
	{
		CoalesceExpr *coalesceexpr = (CoalesceExpr *) node;
		CoalesceExpr *newcoalesce;
		List	   *newargs;
		ListCell   *arg;

		newargs = NIL;
		foreach(arg, coalesceexpr->args)
		{
			Node	   *e;

			e = eval_const_expressions_mutator((Node *) lfirst(arg),
											   context);

			/*
			 * We can remove null constants from the list. For a non-null
			 * constant, if it has not been preceded by any other
			 * non-null-constant expressions then that is the result.
			 */
			if (IsA(e, Const))
			{
				if (((Const *) e)->constisnull)
					continue;	/* drop null constant */
				if (newargs == NIL)
					return e;	/* first expr */
			}
			newargs = lappend(newargs, e);
		}

		/* If all the arguments were constant null, the result is just null */
		if (newargs == NIL)
			return (Node *) makeNullConst(coalesceexpr->coalescetype, -1);

		newcoalesce = makeNode(CoalesceExpr);
		newcoalesce->coalescetype = coalesceexpr->coalescetype;
		newcoalesce->args = newargs;
		return (Node *) newcoalesce;
	}
	if (IsA(node, FieldSelect))
	{
		/*
		 * We can optimize field selection from a whole-row Var into a simple
		 * Var.  (This case won't be generated directly by the parser, because
		 * ParseComplexProjection short-circuits it. But it can arise while
		 * simplifying functions.)	Also, we can optimize field selection from
		 * a RowExpr construct.
		 *
		 * We must however check that the declared type of the field is still
		 * the same as when the FieldSelect was created --- this can change if
		 * someone did ALTER COLUMN TYPE on the rowtype.
		 */
		FieldSelect *fselect = (FieldSelect *) node;
		FieldSelect *newfselect;
		Node	   *arg;

		arg = eval_const_expressions_mutator((Node *) fselect->arg,
											 context);
		if (arg && IsA(arg, Var) &&
			((Var *) arg)->varattno == InvalidAttrNumber)
		{
			if (rowtype_field_matches(((Var *) arg)->vartype,
									  fselect->fieldnum,
									  fselect->resulttype,
									  fselect->resulttypmod))
				return (Node *) makeVar(((Var *) arg)->varno,
										fselect->fieldnum,
										fselect->resulttype,
										fselect->resulttypmod,
										((Var *) arg)->varlevelsup);
		}
		if (arg && IsA(arg, RowExpr))
		{
			RowExpr    *rowexpr = (RowExpr *) arg;

			if (fselect->fieldnum > 0 &&
				fselect->fieldnum <= list_length(rowexpr->args))
			{
				Node	   *fld = (Node *) list_nth(rowexpr->args,
													fselect->fieldnum - 1);

				if (rowtype_field_matches(rowexpr->row_typeid,
										  fselect->fieldnum,
										  fselect->resulttype,
										  fselect->resulttypmod) &&
					fselect->resulttype == exprType(fld) &&
					fselect->resulttypmod == exprTypmod(fld))
					return fld;
			}
		}
		newfselect = makeNode(FieldSelect);
		newfselect->arg = (Expr *) arg;
		newfselect->fieldnum = fselect->fieldnum;
		newfselect->resulttype = fselect->resulttype;
		newfselect->resulttypmod = fselect->resulttypmod;
		return (Node *) newfselect;
	}
	if (IsA(node, NullTest))
	{
		NullTest   *ntest = (NullTest *) node;
		NullTest   *newntest;
		Node	   *arg;

		arg = eval_const_expressions_mutator((Node *) ntest->arg,
											 context);
		if (arg && IsA(arg, RowExpr))
		{
			RowExpr    *rarg = (RowExpr *) arg;
			List	   *newargs = NIL;
			ListCell   *l;

			/*
			 * We break ROW(...) IS [NOT] NULL into separate tests on its
			 * component fields.  This form is usually more efficient to
			 * evaluate, as well as being more amenable to optimization.
			 */
			foreach(l, rarg->args)
			{
				Node	   *relem = (Node *) lfirst(l);

				/*
				 * A constant field refutes the whole NullTest if it's of the
				 * wrong nullness; else we can discard it.
				 */
				if (relem && IsA(relem, Const))
				{
					Const	   *carg = (Const *) relem;

					if (carg->constisnull ?
						(ntest->nulltesttype == IS_NOT_NULL) :
						(ntest->nulltesttype == IS_NULL))
						return makeBoolConst(false, false);
					continue;
				}
				newntest = makeNode(NullTest);
				newntest->arg = (Expr *) relem;
				newntest->nulltesttype = ntest->nulltesttype;
				newargs = lappend(newargs, newntest);
			}
			/* If all the inputs were constants, result is TRUE */
			if (newargs == NIL)
				return makeBoolConst(true, false);
			/* If only one nonconst input, it's the result */
			if (list_length(newargs) == 1)
				return (Node *) linitial(newargs);
			/* Else we need an AND node */
			return (Node *) make_andclause(newargs);
		}
		if (arg && IsA(arg, Const))
		{
			Const	   *carg = (Const *) arg;
			bool		result;

			switch (ntest->nulltesttype)
			{
				case IS_NULL:
					result = carg->constisnull;
					break;
				case IS_NOT_NULL:
					result = !carg->constisnull;
					break;
				default:
					elog(ERROR, "unrecognized nulltesttype: %d",
						 (int) ntest->nulltesttype);
					result = false;		/* keep compiler quiet */
					break;
			}

			return makeBoolConst(result, false);
		}

		newntest = makeNode(NullTest);
		newntest->arg = (Expr *) arg;
		newntest->nulltesttype = ntest->nulltesttype;
		return (Node *) newntest;
	}
	if (IsA(node, BooleanTest))
	{
		BooleanTest *btest = (BooleanTest *) node;
		BooleanTest *newbtest;
		Node	   *arg;

		arg = eval_const_expressions_mutator((Node *) btest->arg,
											 context);
		if (arg && IsA(arg, Const))
		{
			Const	   *carg = (Const *) arg;
			bool		result;

			switch (btest->booltesttype)
			{
				case IS_TRUE:
					result = (!carg->constisnull &&
							  DatumGetBool(carg->constvalue));
					break;
				case IS_NOT_TRUE:
					result = (carg->constisnull ||
							  !DatumGetBool(carg->constvalue));
					break;
				case IS_FALSE:
					result = (!carg->constisnull &&
							  !DatumGetBool(carg->constvalue));
					break;
				case IS_NOT_FALSE:
					result = (carg->constisnull ||
							  DatumGetBool(carg->constvalue));
					break;
				case IS_UNKNOWN:
					result = carg->constisnull;
					break;
				case IS_NOT_UNKNOWN:
					result = !carg->constisnull;
					break;
				default:
					elog(ERROR, "unrecognized booltesttype: %d",
						 (int) btest->booltesttype);
					result = false;		/* keep compiler quiet */
					break;
			}

			return makeBoolConst(result, false);
		}

		newbtest = makeNode(BooleanTest);
		newbtest->arg = (Expr *) arg;
		newbtest->booltesttype = btest->booltesttype;
		return (Node *) newbtest;
	}
	
	/* prevent recursion into sublinks */
	if (IsA(node, SubLink) && !context->recurse_sublink_testexpr)
	{
		SubLink    *sublink = (SubLink *) node;
		SubLink    *newnode = copyObject(sublink);

		/*
		 * Also invoke the mutator on the sublink's Query node, so it
		 * can recurse into the sub-query if it wants to.
		 */
		newnode->subselect = (Node *) query_tree_mutator((Query *) sublink->subselect, eval_const_expressions_mutator, (void*) context, 0);
		return (Node *) newnode;
	}

	/* recurse into query structure if requested */
	if (IsA(node, Query) && context->recurse_queries)
	{
		return (Node *)
					query_tree_mutator
					(
					(Query *) node,
					eval_const_expressions_mutator,
					(void *) context,
					0);
	}

	/*
	 * For any node type not handled above, we recurse using
	 * expression_tree_mutator, which will copy the node unchanged but try to
	 * simplify its arguments (if any) using this routine. For example: we
	 * cannot eliminate an ArrayRef node, but we might be able to simplify
	 * constant expressions in its subscripts.
	 */
	return expression_tree_mutator(node, eval_const_expressions_mutator,
								   (void *) context);
}

/*
 * Subroutine for eval_const_expressions: process arguments of an OR clause
 *
 * This includes flattening of nested ORs as well as recursion to
 * eval_const_expressions to simplify the OR arguments.
 *
 * After simplification, OR arguments are handled as follows:
 *		non constant: keep
 *		FALSE: drop (does not affect result)
 *		TRUE: force result to TRUE
 *		NULL: keep only one
 * We must keep one NULL input because ExecEvalOr returns NULL when no input
 * is TRUE and at least one is NULL.  We don't actually include the NULL
 * here, that's supposed to be done by the caller.
 *
 * The output arguments *haveNull and *forceTrue must be initialized FALSE
 * by the caller.  They will be set TRUE if a null constant or true constant,
 * respectively, is detected anywhere in the argument list.
 */
static List *
simplify_or_arguments(List *args,
					  eval_const_expressions_context *context,
					  bool *haveNull, bool *forceTrue)
{
	List	   *newargs = NIL;
	List	   *unprocessed_args;

	/*
	 * Since the parser considers OR to be a binary operator, long OR lists
	 * become deeply nested expressions.  We must flatten these into long
	 * argument lists of a single OR operator.	To avoid blowing out the stack
	 * with recursion of eval_const_expressions, we resort to some tenseness
	 * here: we keep a list of not-yet-processed inputs, and handle flattening
	 * of nested ORs by prepending to the to-do list instead of recursing.
	 */
	unprocessed_args = list_copy(args);
	while (unprocessed_args)
	{
		Node	   *arg = (Node *) linitial(unprocessed_args);

		unprocessed_args = list_delete_first(unprocessed_args);

		/* flatten nested ORs as per above comment */
		if (or_clause(arg))
		{
			List	   *subargs = list_copy(((BoolExpr *) arg)->args);

			/* overly tense code to avoid leaking unused list header */
			if (!unprocessed_args)
				unprocessed_args = subargs;
			else
			{
				List	   *oldhdr = unprocessed_args;

				unprocessed_args = list_concat(subargs, unprocessed_args);
				pfree(oldhdr);
			}
			continue;
		}

		/* If it's not an OR, simplify it */
		arg = eval_const_expressions_mutator(arg, context);

		/*
		 * It is unlikely but not impossible for simplification of a non-OR
		 * clause to produce an OR.  Recheck, but don't be too tense about it
		 * since it's not a mainstream case. In particular we don't worry
		 * about const-simplifying the input twice.
		 */
		if (or_clause(arg))
		{
			List	   *subargs = list_copy(((BoolExpr *) arg)->args);

			unprocessed_args = list_concat(subargs, unprocessed_args);
			continue;
		}

		/*
		 * OK, we have a const-simplified non-OR argument.	Process it per
		 * comments above.
		 */
		if (IsA(arg, Const))
		{
			Const	   *const_input = (Const *) arg;

			if (const_input->constisnull)
				*haveNull = true;
			else if (DatumGetBool(const_input->constvalue))
			{
				*forceTrue = true;

				/*
				 * Once we detect a TRUE result we can just exit the loop
				 * immediately.  However, if we ever add a notion of
				 * non-removable functions, we'd need to keep scanning.
				 */
				return NIL;
			}
			/* otherwise, we can drop the constant-false input */
			continue;
		}

		/* else emit the simplified arg into the result list */
		newargs = lappend(newargs, arg);
	}

	return newargs;
}

/*
 * Subroutine for eval_const_expressions: process arguments of an AND clause
 *
 * This includes flattening of nested ANDs as well as recursion to
 * eval_const_expressions to simplify the AND arguments.
 *
 * After simplification, AND arguments are handled as follows:
 *		non constant: keep
 *		TRUE: drop (does not affect result)
 *		FALSE: force result to FALSE
 *		NULL: keep only one
 * We must keep one NULL input because ExecEvalAnd returns NULL when no input
 * is FALSE and at least one is NULL.  We don't actually include the NULL
 * here, that's supposed to be done by the caller.
 *
 * The output arguments *haveNull and *forceFalse must be initialized FALSE
 * by the caller.  They will be set TRUE if a null constant or false constant,
 * respectively, is detected anywhere in the argument list.
 */
static List *
simplify_and_arguments(List *args,
					   eval_const_expressions_context *context,
					   bool *haveNull, bool *forceFalse)
{
	List	   *newargs = NIL;
	List	   *unprocessed_args;

	/* See comments in simplify_or_arguments */
	unprocessed_args = list_copy(args);
	while (unprocessed_args)
	{
		Node	   *arg = (Node *) linitial(unprocessed_args);

		unprocessed_args = list_delete_first(unprocessed_args);

		/* flatten nested ANDs as per above comment */
		if (and_clause(arg))
		{
			List	   *subargs = list_copy(((BoolExpr *) arg)->args);

			/* overly tense code to avoid leaking unused list header */
			if (!unprocessed_args)
				unprocessed_args = subargs;
			else
			{
				List	   *oldhdr = unprocessed_args;

				unprocessed_args = list_concat(subargs, unprocessed_args);
				pfree(oldhdr);
			}
			continue;
		}

		/* If it's not an AND, simplify it */
		arg = eval_const_expressions_mutator(arg, context);

		/*
		 * It is unlikely but not impossible for simplification of a non-AND
		 * clause to produce an AND.  Recheck, but don't be too tense about it
		 * since it's not a mainstream case. In particular we don't worry
		 * about const-simplifying the input twice.
		 */
		if (and_clause(arg))
		{
			List	   *subargs = list_copy(((BoolExpr *) arg)->args);

			unprocessed_args = list_concat(subargs, unprocessed_args);
			continue;
		}

		/*
		 * OK, we have a const-simplified non-AND argument.  Process it per
		 * comments above.
		 */
		if (IsA(arg, Const))
		{
			Const	   *const_input = (Const *) arg;

			if (const_input->constisnull)
				*haveNull = true;
			else if (!DatumGetBool(const_input->constvalue))
			{
				*forceFalse = true;

				/*
				 * Once we detect a FALSE result we can just exit the loop
				 * immediately.  However, if we ever add a notion of
				 * non-removable functions, we'd need to keep scanning.
				 */
				return NIL;
			}
			/* otherwise, we can drop the constant-true input */
			continue;
		}

		/* else emit the simplified arg into the result list */
		newargs = lappend(newargs, arg);
	}

	return newargs;
}

/*
 * Subroutine for eval_const_expressions: try to simplify boolean equality
 *
 * Input is the list of simplified arguments to the operator.
 * Returns a simplified expression if successful, or NULL if cannot
 * simplify the expression.
 *
 * The idea here is to reduce "x = true" to "x" and "x = false" to "NOT x".
 * This is only marginally useful in itself, but doing it in constant folding
 * ensures that we will recognize the two forms as being equivalent in, for
 * example, partial index matching.
 *
 * We come here only if simplify_function has failed; therefore we cannot
 * see two constant inputs, nor a constant-NULL input.
 */
static Expr *
simplify_boolean_equality(List *args)
{
	Expr	   *leftop;
	Expr	   *rightop;

	Assert(list_length(args) == 2);
	leftop = linitial(args);
	rightop = lsecond(args);
	if (leftop && IsA(leftop, Const))
	{
		Assert(!((Const *) leftop)->constisnull);
		if (DatumGetBool(((Const *) leftop)->constvalue))
			return rightop;		/* true = foo */
		else
			return make_notclause(rightop);		/* false = foo */
	}
	if (rightop && IsA(rightop, Const))
	{
		Assert(!((Const *) rightop)->constisnull);
		if (DatumGetBool(((Const *) rightop)->constvalue))
			return leftop;		/* foo = true */
		else
			return make_notclause(leftop);		/* foo = false */
	}
	return NULL;
}

/*
 * Subroutine for eval_const_expressions: try to simplify a function call
 * (which might originally have been an operator; we don't care)
 *
 * Inputs are the function OID, actual result type OID (which is needed for
 * polymorphic functions), and the pre-simplified argument list;
 * also the context data for eval_const_expressions.
 *
 * Returns a simplified expression if successful, or NULL if cannot
 * simplify the function call.
 */
static Expr *
simplify_function(Oid funcid, Oid result_type, List *args,
				  bool allow_inline,
				  eval_const_expressions_context *context)
{
	HeapTuple	func_tuple;
	Expr	   *newexpr;
	cqContext  *pcqCtx;

	/*
	 * We have two strategies for simplification: either execute the function
	 * to deliver a constant result, or expand in-line the body of the
	 * function definition (which only works for simple SQL-language
	 * functions, but that is a common case).  In either case we need access
	 * to the function's pg_proc tuple, so fetch it just once to use in both
	 * attempts.
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_proc "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(funcid)));

	func_tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(func_tuple))
		elog(ERROR, "cache lookup failed for function %u", funcid);

	newexpr = evaluate_function(funcid, result_type, args,
								func_tuple, context);

	if (large_const(newexpr, context->max_size))
	{
		// folded expression prohibitively large
			newexpr = NULL;
	}

	if (!newexpr && allow_inline)
		newexpr = inline_function(funcid, result_type, args,
								  pcqCtx,
								  func_tuple, context);

	caql_endscan(pcqCtx);

	return newexpr;
}

/*
 * large_const: check if given expression is a Const expression larger than
 * the given size
 *
 */
static bool
large_const(Expr *expr, Size max_size)
{
	if (NULL == expr || 0 == max_size)
	{
		return false;
	}
	
	if (!IsA(expr, Const))
	{
		return false;
	}
	
	Const *const_expr = (Const *) expr;
	
	if (const_expr->constisnull)
	{
		return false;
	}
	
	Size size = datumGetSize(const_expr->constvalue, const_expr->constbyval, const_expr->constlen);
	return size > max_size;
}

/*
 * evaluate_function: try to pre-evaluate a function call
 *
 * We can do this if the function is strict and has any constant-null inputs
 * (just return a null constant), or if the function is immutable and has all
 * constant inputs (call it and return the result as a Const node).  In
 * estimation mode we are willing to pre-evaluate stable functions too.
 *
 * Returns a simplified expression if successful, or NULL if cannot
 * simplify the function.
 */
static Expr *
evaluate_function(Oid funcid, Oid result_type, List *args,
				  HeapTuple func_tuple,
				  eval_const_expressions_context *context)
{
	Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
	bool		has_nonconst_input = false;
	bool		has_null_input = false;
	ListCell   *arg;
	FuncExpr   *newexpr;

	/*
	 * Can't simplify if it returns a set.
	 */
	if (funcform->proretset)
		return NULL;

	/*
	 * Can't simplify if it returns RECORD.  The immediate problem is that it
	 * will be needing an expected tupdesc which we can't supply here.
	 *
	 * In the case where it has OUT parameters, it could get by without an
	 * expected tupdesc, but we still have issues: get_expr_result_type()
	 * doesn't know how to extract type info from a RECORD constant, and in
	 * the case of a NULL function result there doesn't seem to be any clean
	 * way to fix that.  In view of the likelihood of there being still other
	 * gotchas, seems best to leave the function call unreduced.
	 */
	if (funcform->prorettype == RECORDOID)
		return NULL;

	/*
	 * Check for constant inputs and especially constant-NULL inputs.
	 */
	foreach(arg, args)
	{
		if (IsA(lfirst(arg), Const))
			has_null_input |= ((Const *) lfirst(arg))->constisnull;
		else
			has_nonconst_input = true;
	}

	/*
	 * If the function is strict and has a constant-NULL input, it will never
	 * be called at all, so we can replace the call by a NULL constant, even
	 * if there are other inputs that aren't constant, and even if the
	 * function is not otherwise immutable.
	 */
	if (funcform->proisstrict && has_null_input)
		return (Expr *) makeNullConst(result_type, -1);

	/*
	 * Otherwise, can simplify only if all inputs are constants. (For a
	 * non-strict function, constant NULL inputs are treated the same as
	 * constant non-NULL inputs.)
	 */
	if (has_nonconst_input)
		return NULL;

	/*
	 * Ordinarily we are only allowed to simplify immutable functions. But for
	 * purposes of estimation, we consider it okay to simplify functions that
	 * are merely stable; the risk that the result might change from planning
	 * time to execution time is worth taking in preference to not being able
	 * to estimate the value at all.
	 */
	if (funcform->provolatile == PROVOLATILE_IMMUTABLE)
		 /* okay */ ;
	else if (context->transform_stable_funcs && funcform->provolatile == PROVOLATILE_STABLE)
		 /* okay */ ;
	else
		return NULL;

	/*
	 * OK, looks like we can simplify this operator/function.
	 *
	 * Build a new FuncExpr node containing the already-simplified arguments.
	 */
	newexpr = makeNode(FuncExpr);
	newexpr->funcid = funcid;
	newexpr->funcresulttype = result_type;
	newexpr->funcretset = false;
	newexpr->funcformat = COERCE_DONTCARE;		/* doesn't matter */
	newexpr->args = args;

	return evaluate_expr((Expr *) newexpr, result_type);
}

/*
 * inline_function: try to expand a function call inline
 *
 * If the function is a sufficiently simple SQL-language function
 * (just "SELECT expression"), then we can inline it and avoid the rather
 * high per-call overhead of SQL functions.  Furthermore, this can expose
 * opportunities for constant-folding within the function expression.
 *
 * We have to beware of some special cases however.  A directly or
 * indirectly recursive function would cause us to recurse forever,
 * so we keep track of which functions we are already expanding and
 * do not re-expand them.  Also, if a parameter is used more than once
 * in the SQL-function body, we require it not to contain any volatile
 * functions (volatiles might deliver inconsistent answers) nor to be
 * unreasonably expensive to evaluate.	The expensiveness check not only
 * prevents us from doing multiple evaluations of an expensive parameter
 * at runtime, but is a safety value to limit growth of an expression due
 * to repeated inlining.
 *
 * We must also beware of changing the volatility or strictness status of
 * functions by inlining them.
 *
 * Returns a simplified expression if successful, or NULL if cannot
 * simplify the function.
 */
static Expr *
inline_function(Oid funcid, Oid result_type, List *args,
				cqContext *pcqCtx,
				HeapTuple func_tuple,
				eval_const_expressions_context *context)
{
	Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
	Oid		   *argtypes;
	char	   *src;
	Datum		tmp;
	bool		isNull;
	MemoryContext oldcxt;
	MemoryContext mycxt;
	ErrorContextCallback sqlerrcontext;
	List	   *raw_parsetree_list;
	List	   *querytree_list;
	Query	   *querytree;
	Node	   *newexpr;
	int		   *usecounts;
	ListCell   *arg;
	int			i;

	/*
	 * Forget it if the function is not SQL-language or has other showstopper
	 * properties.	(The nargs check is just paranoia.)
	 */
	if (funcform->prolang != SQLlanguageId ||
		funcform->prosecdef ||
		funcform->proretset ||
		funcform->pronargs != list_length(args))
		return NULL;

	/* Check for recursive function, and give up trying to expand if so */
	if (list_member_oid(context->active_fns, funcid))
		return NULL;

	/* Check permission to call function (fail later, if not) */
	if (pg_proc_aclcheck(funcid, GetUserId(), ACL_EXECUTE) != ACLCHECK_OK)
		return NULL;

	/*
	 * Setup error traceback support for ereport().  This is so that we can
	 * finger the function that bad information came from.
	 */
	sqlerrcontext.callback = sql_inline_error_callback;
	/* XXX XXX: could pass the pcqCtx or prosrc directly here to avoid
	 * SysCacheGetAttr 
	 */
	sqlerrcontext.arg = func_tuple;
	sqlerrcontext.previous = error_context_stack;
	error_context_stack = &sqlerrcontext;

	/*
	 * Make a temporary memory context, so that we don't leak all the stuff
	 * that parsing might create.
	 */
	mycxt = AllocSetContextCreate(CurrentMemoryContext,
								  "inline_function",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(mycxt);

	/* Check for polymorphic arguments, and substitute actual arg types */
	argtypes = (Oid *) palloc(funcform->pronargs * sizeof(Oid));
	memcpy(argtypes, funcform->proargtypes.values,
		   funcform->pronargs * sizeof(Oid));
	for (i = 0; i < funcform->pronargs; i++)
	{
		if (argtypes[i] == ANYARRAYOID ||
			argtypes[i] == ANYELEMENTOID)
		{
			argtypes[i] = exprType((Node *) list_nth(args, i));
		}
	}

	/* Fetch and parse the function body */
	tmp = caql_getattr(pcqCtx,
					   Anum_pg_proc_prosrc,
					   &isNull);
	if (isNull)
		elog(ERROR, "null prosrc for function %u", funcid);
	src = DatumGetCString(DirectFunctionCall1(textout, tmp));

	/*
	 * We just do parsing and parse analysis, not rewriting, because rewriting
	 * will not affect table-free-SELECT-only queries, which is all that we
	 * care about.	Also, we can punt as soon as we detect more than one
	 * command in the function body.
	 */
	raw_parsetree_list = pg_parse_query(src);
	if (list_length(raw_parsetree_list) != 1)
		goto fail;

	querytree_list = parse_analyze(linitial(raw_parsetree_list), src,
								   argtypes, funcform->pronargs);

	if (list_length(querytree_list) != 1)
		goto fail;

	querytree = (Query *) linitial(querytree_list);

	/*
	 * The single command must be a simple "SELECT expression".
	 */
	if (!IsA(querytree, Query) ||
		querytree->commandType != CMD_SELECT ||
		querytree->intoClause ||
		querytree->hasAggs ||
		querytree->hasSubLinks ||
		querytree->rtable ||
		querytree->jointree->fromlist ||
		querytree->jointree->quals ||
		querytree->groupClause ||
		querytree->havingQual ||
		querytree->distinctClause ||
		querytree->sortClause ||
		querytree->limitOffset ||
		querytree->limitCount ||
		querytree->setOperations ||
		list_length(querytree->targetList) != 1)
		goto fail;

	newexpr = (Node *) ((TargetEntry *) linitial(querytree->targetList))->expr;

	/*
	 * Make sure the function (still) returns what it's declared to.  This will
	 * raise an error if wrong, but that's okay since the function would fail
	 * at runtime anyway.  Note we do not try this until we have verified that
	 * no rewriting was needed; that's probably not important, but let's be
	 * careful.
	 */
	if (check_sql_fn_retval(funcid, result_type, querytree_list, NULL))
		goto fail;				/* reject whole-tuple-result cases */

	/*
	 * Additional validity checks on the expression.  It mustn't return a set,
	 * and it mustn't be more volatile than the surrounding function (this is
	 * to avoid breaking hacks that involve pretending a function is immutable
	 * when it really ain't).  If the surrounding function is declared strict,
	 * then the expression must contain only strict constructs and must use
	 * all of the function parameters (this is overkill, but an exact analysis
	 * is hard).
	 */
	if (expression_returns_set(newexpr))
		goto fail;

	if (funcform->provolatile == PROVOLATILE_IMMUTABLE &&
		contain_mutable_functions(newexpr))
		goto fail;
	else if (funcform->provolatile == PROVOLATILE_STABLE &&
			 contain_volatile_functions(newexpr))
		goto fail;

	if (funcform->proisstrict &&
		contain_nonstrict_functions(newexpr))
		goto fail;

	/*
	 * We may be able to do it; there are still checks on parameter usage to
	 * make, but those are most easily done in combination with the actual
	 * substitution of the inputs.	So start building expression with inputs
	 * substituted.
	 */
	usecounts = (int *) palloc0(funcform->pronargs * sizeof(int));
	newexpr = substitute_actual_parameters(newexpr, funcform->pronargs,
										   args, usecounts);

	/* Now check for parameter usage */
	i = 0;
	foreach(arg, args)
	{
		Node	   *param = lfirst(arg);

		if (usecounts[i] == 0)
		{
			/* Param not used at all: uncool if func is strict */
			if (funcform->proisstrict)
				goto fail;
		}
		else if (usecounts[i] != 1)
		{
			/* Param used multiple times: uncool if expensive or volatile */
			QualCost	eval_cost;

			/*
			 * We define "expensive" as "contains any subplan or more than 10
			 * operators".  Note that the subplan search has to be done
			 * explicitly, since cost_qual_eval() will barf on unplanned
			 * subselects.
			 */
			if (contain_subplans(param))
				goto fail;
			cost_qual_eval(&eval_cost, list_make1(param), NULL);
			if (eval_cost.startup + eval_cost.per_tuple >
				10 * cpu_operator_cost)
				goto fail;

			/*
			 * Check volatility last since this is more expensive than the
			 * above tests
			 */
			if (contain_volatile_functions(param))
				goto fail;
		}
		i++;
	}

	/*
	 * Whew --- we can make the substitution.  Copy the modified expression
	 * out of the temporary memory context, and clean up.
	 */
	MemoryContextSwitchTo(oldcxt);

	newexpr = copyObject(newexpr);

	MemoryContextDelete(mycxt);

	/*
	 * Since check_sql_fn_retval allows binary-compatibility cases, the
	 * expression we now have might return some type that's only binary
	 * compatible with the original expression result type.  To avoid
	 * confusing matters, insert a RelabelType in such cases.
	 */
	if (exprType(newexpr) != result_type)
	{
		Assert(IsBinaryCoercible(exprType(newexpr), result_type));
		newexpr = (Node *) makeRelabelType((Expr *) newexpr,
										   result_type,
										   -1,
										   COERCE_IMPLICIT_CAST);
	}

	/*
	 * Recursively try to simplify the modified expression.  Here we must add
	 * the current function to the context list of active functions.
	 */
	context->active_fns = lcons_oid(funcid, context->active_fns);
	newexpr = eval_const_expressions_mutator(newexpr, context);
	context->active_fns = list_delete_first(context->active_fns);

	error_context_stack = sqlerrcontext.previous;

	return (Expr *) newexpr;

	/* Here if func is not inlinable: release temp memory and return NULL */
fail:
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(mycxt);
	error_context_stack = sqlerrcontext.previous;

	return NULL;
}

/*
 * Replace Param nodes by appropriate actual parameters
 */
static Node *
substitute_actual_parameters(Node *expr, int nargs, List *args,
							 int *usecounts)
{
	substitute_actual_parameters_context context;

	context.nargs = nargs;
	context.args = args;
	context.usecounts = usecounts;

	return substitute_actual_parameters_mutator(expr, &context);
}

static Node *
substitute_actual_parameters_mutator(Node *node,
							   substitute_actual_parameters_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind != PARAM_EXTERN)
			elog(ERROR, "unexpected paramkind: %d", (int) param->paramkind);
		if (param->paramid <= 0 || param->paramid > context->nargs)
			elog(ERROR, "invalid paramid: %d", param->paramid);

		/* Count usage of parameter */
		context->usecounts[param->paramid - 1]++;

		/* Select the appropriate actual arg and replace the Param with it */
		/* We don't need to copy at this time (it'll get done later) */
		return list_nth(context->args, param->paramid - 1);
	}
	return expression_tree_mutator(node, substitute_actual_parameters_mutator,
								   (void *) context);
}

/*
 * error context callback to let us supply a call-stack traceback
 */
static void
sql_inline_error_callback(void *arg)
{
	HeapTuple	func_tuple = (HeapTuple) arg;
	Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
	int			syntaxerrposition;

	/* If it's a syntax error, convert to internal syntax error report */
	syntaxerrposition = geterrposition();
	if (syntaxerrposition > 0)
	{
		bool		isnull;
		Datum		tmp;
		char	   *prosrc;

		tmp = SysCacheGetAttr(PROCOID, func_tuple, Anum_pg_proc_prosrc,
							  &isnull);
		if (isnull)
			elog(ERROR, "null prosrc");
		prosrc = DatumGetCString(DirectFunctionCall1(textout, tmp));
		errposition(0);
		internalerrposition(syntaxerrposition);
		internalerrquery(prosrc);
	}

	errcontext("SQL function \"%s\" during inlining",
			   NameStr(funcform->proname));
}

/*
 * evaluate_expr: pre-evaluate a constant expression
 *
 * We use the executor's routine ExecEvalExpr() to avoid duplication of
 * code and ensure we get the same result as the executor would get.
 */
Expr *
evaluate_expr(Expr *expr, Oid result_type)
{
	EState	   *estate;
	ExprState  *exprstate;
	MemoryContext oldcontext;
	Datum		const_val;
	bool		const_is_null;
	int16		resultTypLen;
	bool		resultTypByVal;

	/*
	 * To use the executor, we need an EState.
	 */
	estate = CreateExecutorState();

	/* We can use the estate's working context to avoid memory leaks. */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	/*
	 * Prepare expr for execution.
	 */
	exprstate = ExecPrepareExpr(expr, estate);

	/*
	 * And evaluate it.
	 *
	 * It is OK to use a default econtext because none of the ExecEvalExpr()
	 * code used in this situation will use econtext.  That might seem
	 * fortuitous, but it's not so unreasonable --- a constant expression does
	 * not depend on context, by definition, n'est ce pas?
	 */
	const_val = ExecEvalExprSwitchContext(exprstate,
										  GetPerTupleExprContext(estate),
										  &const_is_null, NULL);

	/* Get info needed about result datatype */
	get_typlenbyval(result_type, &resultTypLen, &resultTypByVal);

	/* Get back to outer memory context */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Must copy result out of sub-context used by expression eval.
	 *
	 * Also, if it's varlena, forcibly detoast it.  This protects us against
	 * storing TOAST pointers into plans that might outlive the referenced
	 * data.
	 */
	if (!const_is_null)
	{
		if (resultTypLen == -1)
			const_val = PointerGetDatum(PG_DETOAST_DATUM_COPY(const_val));
		else
			const_val = datumCopy(const_val, resultTypByVal, resultTypLen);
	}

	/* Release all the junk we just created */
	FreeExecutorState(estate);

	/*
	 * Make the constant result node.
	 */
	return (Expr *) makeConst(result_type, -1, resultTypLen,
							  const_val, const_is_null,
							  resultTypByVal);
}


/*--------------------
 * expression_tree_mutator() is designed to support routines that make a
 * modified copy of an expression tree, with some nodes being added,
 * removed, or replaced by new subtrees.  The original tree is (normally)
 * not changed.  Each recursion level is responsible for returning a copy of
 * (or appropriately modified substitute for) the subtree it is handed.
 * A mutator routine should look like this:
 *
 * Node * my_mutator (Node *node, my_struct *context)
 * {
 *		if (node == NULL)
 *			return NULL;
 *		// check for nodes that special work is required for, eg:
 *		if (IsA(node, Var))
 *		{
 *			... create and return modified copy of Var node
 *		}
 *		else if (IsA(node, ...))
 *		{
 *			... do special transformations of other node types
 *		}
 *		// for any node type not specially processed, do:
 *		return expression_tree_mutator(node, my_mutator, (void *) context);
 * }
 *
 * The "context" argument points to a struct that holds whatever context
 * information the mutator routine needs --- it can be used to return extra
 * data gathered by the mutator, too.  This argument is not touched by
 * expression_tree_mutator, but it is passed down to recursive sub-invocations
 * of my_mutator.  The tree walk is started from a setup routine that
 * fills in the appropriate context struct, calls my_mutator with the
 * top-level node of the tree, and does any required post-processing.
 *
 * Each level of recursion must return an appropriately modified Node.
 * If expression_tree_mutator() is called, it will make an exact copy
 * of the given Node, but invoke my_mutator() to copy the sub-node(s)
 * of that Node.  In this way, my_mutator() has full control over the
 * copying process but need not directly deal with expression trees
 * that it has no interest in.
 *
 * Just as for expression_tree_walker, the node types handled by
 * expression_tree_mutator include all those normally found in target lists
 * and qualifier clauses during the planning stage.
 *
 * expression_tree_mutator will handle SubLink nodes by recursing normally
 * into the "testexpr" subtree (which is an expression belonging to the outer
 * plan).  It will also call the mutator on the sub-Query node; however, when
 * expression_tree_mutator itself is called on a Query node, it does nothing
 * and returns the unmodified Query node.  The net effect is that unless the
 * mutator does something special at a Query node, sub-selects will not be
 * visited or modified; the original sub-select will be linked to by the new
 * SubLink node.  Mutators that want to descend into sub-selects will usually
 * do so by recognizing Query nodes and calling query_tree_mutator (below).
 *
 * expression_tree_mutator will handle a SubPlan node by recursing into the
 * "testexpr" and the "args" list (which belong to the outer plan), but it
 * will simply copy the link to the inner plan, since that's typically what
 * expression tree mutators want.  A mutator that wants to modify the subplan
 * can force appropriate behavior by recognizing SubPlan expression nodes
 * and doing the right thing.
 *--------------------
 */

Node *
expression_tree_mutator(Node *node,
						Node *(*mutator) (),
						void *context)
{
	/*
	 * The mutator has already decided not to modify the current node, but we
	 * must call the mutator for any sub-nodes.
	 */

#define FLATCOPY(newnode, node, nodetype)  \
	( (newnode) = makeNode(nodetype), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

#define CHECKFLATCOPY(newnode, node, nodetype)	\
	( AssertMacro(IsA((node), nodetype)), \
	  (newnode) = makeNode(nodetype), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

#define MUTATE(newfield, oldfield, fieldtype)  \
		( (newfield) = (fieldtype) mutator((Node *) (oldfield), context) )

	if (node == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Var:
		case T_Const:
		case T_Param:
		case T_CoerceToDomainValue:
		case T_CurrentOfExpr:
		case T_CaseTestExpr:
		case T_DynamicIndexScan:
		case T_SetToDefault:
		case T_RangeTblRef:
		case T_OuterJoinInfo:
		case T_String:
		case T_Null:
		case T_DML:
		case T_RowTrigger:
		case T_PartOidExpr:
		case T_PartDefaultExpr:
		case T_PartBoundExpr:
		case T_PartBoundInclusionExpr:
		case T_PartBoundOpenExpr:
			/* primitive node types with no expression subnodes */
			return (Node *) copyObject(node);
		case T_Aggref:
			{
				Aggref	   *aggref = (Aggref *) node;
				Aggref	   *newnode;

				FLATCOPY(newnode, aggref, Aggref);
				MUTATE(newnode->args, aggref->args, List *);
				MUTATE(newnode->aggorder, aggref->aggorder, AggOrder*);
				return (Node *) newnode;
			}
			break;
		case T_AggOrder:
			{
				AggOrder	*aggorder = (AggOrder *)node;
				AggOrder	*newnode;
				
				FLATCOPY(newnode, aggorder, AggOrder);
				MUTATE(newnode->sortTargets, aggorder->sortTargets, List *);
				MUTATE(newnode->sortClause, aggorder->sortClause, List *);
				return (Node *) newnode;
			}
			break;
		case T_GroupingFunc:
			{
				GroupingFunc *newnode;

				newnode = copyObject(node);
				return (Node *)newnode;
			}
			break;
		case T_Grouping:
			{
				Grouping *grping = (Grouping *) node;
				Grouping *newnode;

				FLATCOPY(newnode, grping, Grouping);
				return (Node *) newnode;
			}
			break;
		case T_GroupId:
			{
				GroupId *grpid = (GroupId *) node;
				GroupId *newnode;

				FLATCOPY(newnode, grpid, GroupId);
				return (Node *) newnode;
			}
			break;
		case T_TableFunctionScan:
			{
				TableFunctionScan *tablefunc = (TableFunctionScan *) node;
				TableFunctionScan *newnode;

				FLATCOPY(newnode, tablefunc, TableFunctionScan);
				return (Node *) newnode;
			}
			break;
		case T_WindowRef:
			{
				WindowRef   *windowref = (WindowRef *) node;
				WindowRef   *newnode;

				FLATCOPY(newnode, windowref, WindowRef);
				MUTATE(newnode->args, windowref->args, List *);
				return (Node *) newnode;
			}
			break;
		case T_WindowFrame:
			{
				WindowFrame	*frame = (WindowFrame *) node;
				WindowFrame *newnode;
				
				FLATCOPY(newnode, frame, WindowFrame);
				MUTATE(newnode->trail, frame->trail, WindowFrameEdge *);
				MUTATE(newnode->lead, frame->lead, WindowFrameEdge *);
				return (Node *) newnode;
			}
			break;
		case T_WindowFrameEdge:
			{
				WindowFrameEdge	*edge = (WindowFrameEdge *) node;
				WindowFrameEdge *newnode;
				
				FLATCOPY(newnode, edge, WindowFrameEdge);
				MUTATE(newnode->val, edge->val, Node *);
				return (Node *) newnode;
			}
			break;
		case T_WindowSpec:
			{
				WindowSpec *winspec = (WindowSpec *) node;
				WindowSpec *newnode;

				FLATCOPY(newnode, winspec, WindowSpec);

				MUTATE(newnode->partition, winspec->partition, List *);
				MUTATE(newnode->order, winspec->order, List *);
				MUTATE(newnode->frame, winspec->frame, WindowFrame *);

				return (Node *) newnode;

			}
		case T_PercentileExpr:
			{
				PercentileExpr *perc = (PercentileExpr *) node;
				PercentileExpr *newnode;

				FLATCOPY(newnode, perc, PercentileExpr);

				MUTATE(newnode->args, perc->args, List *);
				MUTATE(newnode->sortClause, perc->sortClause, List *);
				MUTATE(newnode->sortTargets, perc->sortTargets, List *);
				MUTATE(newnode->pcExpr, perc->pcExpr, Expr *);
				MUTATE(newnode->tcExpr, perc->tcExpr, Expr *);

				return (Node *) newnode;
			}
		case T_SortClause:
			{
				SortClause *sortcl = (SortClause *) node;
				SortClause *newnode;

				FLATCOPY(newnode, sortcl, SortClause);

				return (Node *) newnode;
			}
		case T_GroupClause: /* same as SortClause for now */
			{
				GroupClause *groupcl = (GroupClause *) node;
				GroupClause *newnode;
				
				FLATCOPY(newnode, groupcl, GroupClause);
				
				return (Node *) newnode;
			}
		case T_GroupingClause:
			{
				GroupingClause *grpingcl = (GroupingClause *) node;
				GroupingClause *newnode;
				
				FLATCOPY(newnode, grpingcl, GroupingClause);
				MUTATE(newnode->groupsets, grpingcl->groupsets, List *);
				return (Node *)newnode;
			}
			break;
		case T_ArrayRef:
			{
				ArrayRef   *arrayref = (ArrayRef *) node;
				ArrayRef   *newnode;

				FLATCOPY(newnode, arrayref, ArrayRef);
				MUTATE(newnode->refupperindexpr, arrayref->refupperindexpr,
					   List *);
				MUTATE(newnode->reflowerindexpr, arrayref->reflowerindexpr,
					   List *);
				MUTATE(newnode->refexpr, arrayref->refexpr,
					   Expr *);
				MUTATE(newnode->refassgnexpr, arrayref->refassgnexpr,
					   Expr *);
				return (Node *) newnode;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *expr = (FuncExpr *) node;
				FuncExpr   *newnode;

				FLATCOPY(newnode, expr, FuncExpr);
				MUTATE(newnode->args, expr->args, List *);
				return (Node *) newnode;
			}
			break;
		case T_TableValueExpr:
			{
				TableValueExpr   *expr = (TableValueExpr *) node;
				TableValueExpr   *newnode;

				FLATCOPY(newnode, expr, TableValueExpr);

				/* The subquery already pulled up into the T_TableFunctionScan node */
				newnode->subquery = (Node *) NULL;
				return (Node *) newnode;
			}
			break;
		case T_OpExpr:
			{
				OpExpr	   *expr = (OpExpr *) node;
				OpExpr	   *newnode;

				FLATCOPY(newnode, expr, OpExpr);
				MUTATE(newnode->args, expr->args, List *);
				return (Node *) newnode;
			}
			break;
		case T_DistinctExpr:
			{
				DistinctExpr *expr = (DistinctExpr *) node;
				DistinctExpr *newnode;

				FLATCOPY(newnode, expr, DistinctExpr);
				MUTATE(newnode->args, expr->args, List *);
				return (Node *) newnode;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;
				ScalarArrayOpExpr *newnode;

				FLATCOPY(newnode, expr, ScalarArrayOpExpr);
				MUTATE(newnode->args, expr->args, List *);
				return (Node *) newnode;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *expr = (BoolExpr *) node;
				BoolExpr   *newnode;

				FLATCOPY(newnode, expr, BoolExpr);
				MUTATE(newnode->args, expr->args, List *);
				return (Node *) newnode;
			}
			break;
		case T_SubLink:
			{
				SubLink    *sublink = (SubLink *) node;
				SubLink    *newnode;

				FLATCOPY(newnode, sublink, SubLink);
				
				MUTATE(newnode->testexpr, sublink->testexpr, Node *);

				/*
				 * Also invoke the mutator on the sublink's Query node, so it
				 * can recurse into the sub-query if it wants to.
				 */
				MUTATE(newnode->subselect, sublink->subselect, Node *);
				return (Node *) newnode;
			}
			break;
		case T_SubPlan:
			{
				SubPlan    *subplan = (SubPlan *) node;
				SubPlan    *newnode;

				FLATCOPY(newnode, subplan, SubPlan);
				/* transform testexpr */
				MUTATE(newnode->testexpr, subplan->testexpr, Node *);
				/* transform args list (params to be passed to subplan) */
				MUTATE(newnode->args, subplan->args, List *);
				/* but not the sub-Plan itself, which is referenced as-is */
				return (Node *) newnode;
			}
			break;
		case T_FieldSelect:
			{
				FieldSelect *fselect = (FieldSelect *) node;
				FieldSelect *newnode;

				FLATCOPY(newnode, fselect, FieldSelect);
				MUTATE(newnode->arg, fselect->arg, Expr *);
				return (Node *) newnode;
			}
			break;
		case T_FieldStore:
			{
				FieldStore *fstore = (FieldStore *) node;
				FieldStore *newnode;

				FLATCOPY(newnode, fstore, FieldStore);
				MUTATE(newnode->arg, fstore->arg, Expr *);
				MUTATE(newnode->newvals, fstore->newvals, List *);
				newnode->fieldnums = list_copy(fstore->fieldnums);
				return (Node *) newnode;
			}
			break;
		case T_RelabelType:
			{
				RelabelType *relabel = (RelabelType *) node;
				RelabelType *newnode;

				FLATCOPY(newnode, relabel, RelabelType);
				MUTATE(newnode->arg, relabel->arg, Expr *);
				return (Node *) newnode;
			}
			break;
		case T_ConvertRowtypeExpr:
			{
				ConvertRowtypeExpr *convexpr = (ConvertRowtypeExpr *) node;
				ConvertRowtypeExpr *newnode;

				FLATCOPY(newnode, convexpr, ConvertRowtypeExpr);
				MUTATE(newnode->arg, convexpr->arg, Expr *);
				return (Node *) newnode;
			}
			break;
		case T_CaseExpr:
			{
				CaseExpr   *caseexpr = (CaseExpr *) node;
				CaseExpr   *newnode;

				FLATCOPY(newnode, caseexpr, CaseExpr);
				MUTATE(newnode->arg, caseexpr->arg, Expr *);
				MUTATE(newnode->args, caseexpr->args, List *);
				MUTATE(newnode->defresult, caseexpr->defresult, Expr *);
				return (Node *) newnode;
			}
			break;
		case T_CaseWhen:
			{
				CaseWhen   *casewhen = (CaseWhen *) node;
				CaseWhen   *newnode;

				FLATCOPY(newnode, casewhen, CaseWhen);
				MUTATE(newnode->expr, casewhen->expr, Expr *);
				MUTATE(newnode->result, casewhen->result, Expr *);
				return (Node *) newnode;
			}
			break;
		case T_ArrayExpr:
			{
				ArrayExpr  *arrayexpr = (ArrayExpr *) node;
				ArrayExpr  *newnode;

				FLATCOPY(newnode, arrayexpr, ArrayExpr);
				MUTATE(newnode->elements, arrayexpr->elements, List *);
				return (Node *) newnode;
			}
			break;
		case T_RowExpr:
			{
				RowExpr    *rowexpr = (RowExpr *) node;
				RowExpr    *newnode;

				FLATCOPY(newnode, rowexpr, RowExpr);
				MUTATE(newnode->args, rowexpr->args, List *);
				return (Node *) newnode;
			}
			break;
		case T_RowCompareExpr:
			{
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;
				RowCompareExpr *newnode;

				FLATCOPY(newnode, rcexpr, RowCompareExpr);
				MUTATE(newnode->largs, rcexpr->largs, List *);
				MUTATE(newnode->rargs, rcexpr->rargs, List *);
				return (Node *) newnode;
			}
			break;
		case T_CoalesceExpr:
			{
				CoalesceExpr *coalesceexpr = (CoalesceExpr *) node;
				CoalesceExpr *newnode;

				FLATCOPY(newnode, coalesceexpr, CoalesceExpr);
				MUTATE(newnode->args, coalesceexpr->args, List *);
				return (Node *) newnode;
			}
			break;
		case T_MinMaxExpr:
			{
				MinMaxExpr *minmaxexpr = (MinMaxExpr *) node;
				MinMaxExpr *newnode;

				FLATCOPY(newnode, minmaxexpr, MinMaxExpr);
				MUTATE(newnode->args, minmaxexpr->args, List *);
				return (Node *) newnode;
			}
			break;
		case T_NullIfExpr:
			{
				NullIfExpr *expr = (NullIfExpr *) node;
				NullIfExpr *newnode;

				FLATCOPY(newnode, expr, NullIfExpr);
				MUTATE(newnode->args, expr->args, List *);
				return (Node *) newnode;
			}
			break;
		case T_NullTest:
			{
				NullTest   *ntest = (NullTest *) node;
				NullTest   *newnode;

				FLATCOPY(newnode, ntest, NullTest);
				MUTATE(newnode->arg, ntest->arg, Expr *);
				return (Node *) newnode;
			}
			break;
		case T_BooleanTest:
			{
				BooleanTest *btest = (BooleanTest *) node;
				BooleanTest *newnode;

				FLATCOPY(newnode, btest, BooleanTest);
				MUTATE(newnode->arg, btest->arg, Expr *);
				return (Node *) newnode;
			}
			break;
		case T_CoerceToDomain:
			{
				CoerceToDomain *ctest = (CoerceToDomain *) node;
				CoerceToDomain *newnode;

				FLATCOPY(newnode, ctest, CoerceToDomain);
				MUTATE(newnode->arg, ctest->arg, Expr *);
				return (Node *) newnode;
			}
			break;
		case T_TargetEntry:
			{
				TargetEntry *targetentry = (TargetEntry *) node;
				TargetEntry *newnode;

				FLATCOPY(newnode, targetentry, TargetEntry);
				MUTATE(newnode->expr, targetentry->expr, Expr *);
				return (Node *) newnode;
			}
			break;
		case T_Query:
			/* Do nothing with a sub-Query, per discussion above */
			return node;
		case T_CommonTableExpr:
			{
				CommonTableExpr *cte = (CommonTableExpr *) node;
				CommonTableExpr *newnode;

				FLATCOPY(newnode, cte, CommonTableExpr);

				/*
				 * Also invoke the mutator on the CTE's Query node, so it can
				 * recurse into the sub-query if it wants to.
				 */
				MUTATE(newnode->ctequery, cte->ctequery, Node *);
				return (Node *) newnode;
			}
			break;
		case T_List:
			{
				/*
				 * We assume the mutator isn't interested in the list nodes
				 * per se, so just invoke it on each list element. NOTE: this
				 * would fail badly on a list with integer elements!
				 */
				List	   *resultlist;
				ListCell   *temp;

				resultlist = NIL;
				foreach(temp, (List *) node)
				{
					resultlist = lappend(resultlist,
										 mutator((Node *) lfirst(temp),
												 context));
				}
				return (Node *) resultlist;
			}
			break;
		case T_FromExpr:
			{
				FromExpr   *from = (FromExpr *) node;
				FromExpr   *newnode;

				FLATCOPY(newnode, from, FromExpr);
				MUTATE(newnode->fromlist, from->fromlist, List *);
				MUTATE(newnode->quals, from->quals, Node *);
				return (Node *) newnode;
			}
			break;
		case T_JoinExpr:
			{
				JoinExpr   *join = (JoinExpr *) node;
				JoinExpr   *newnode;

				FLATCOPY(newnode, join, JoinExpr);
				MUTATE(newnode->larg, join->larg, Node *);
				MUTATE(newnode->rarg, join->rarg, Node *);
				MUTATE(newnode->subqfromlist, join->subqfromlist, List *);  /*CDB*/
				MUTATE(newnode->quals, join->quals, Node *);
				/* We do not mutate alias or using by default */
				return (Node *) newnode;
			}
			break;
		case T_SetOperationStmt:
			{
				SetOperationStmt *setop = (SetOperationStmt *) node;
				SetOperationStmt *newnode;

				FLATCOPY(newnode, setop, SetOperationStmt);
				MUTATE(newnode->larg, setop->larg, Node *);
				MUTATE(newnode->rarg, setop->rarg, Node *);
				return (Node *) newnode;
			}
			break;
		case T_InClauseInfo:
			{
				InClauseInfo *ininfo = (InClauseInfo *) node;
				InClauseInfo *newnode;

				FLATCOPY(newnode, ininfo, InClauseInfo);
				MUTATE(newnode->sub_targetlist, ininfo->sub_targetlist, List *);
				return (Node *) newnode;
			}
			break;
		case T_AppendRelInfo:
			{
				AppendRelInfo *appinfo = (AppendRelInfo *) node;
				AppendRelInfo *newnode;

				FLATCOPY(newnode, appinfo, AppendRelInfo);
				MUTATE(newnode->translated_vars, appinfo->translated_vars, List *);
				return (Node *) newnode;
			}
			break;
		default:
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                            		errmsg_internal("unrecognized node type: %d",
				                            nodeTag(node)) ));
			break;
	}
	/* can't get here, but keep compiler happy */
	return NULL;
}


/*
 * query_tree_mutator --- initiate modification of a Query's expressions
 *
 * This routine exists just to reduce the number of places that need to know
 * where all the expression subtrees of a Query are.  Note it can be used
 * for starting a walk at top level of a Query regardless of whether the
 * mutator intends to descend into subqueries.	It is also useful for
 * descending into subqueries within a mutator.
 *
 * Some callers want to suppress mutating of certain items in the Query,
 * typically because they need to process them specially, or don't actually
 * want to recurse into subqueries.  This is supported by the flags argument,
 * which is the bitwise OR of flag values to suppress mutating of
 * indicated items.  (More flag bits may be added as needed.)
 *
 * Normally the Query node itself is copied, but some callers want it to be
 * modified in-place; they must pass QTW_DONT_COPY_QUERY in flags.	All
 * modified substructure is safely copied in any case.
 */
Query *
query_tree_mutator(Query *query,
				   Node *(*mutator) (),
				   void *context,
				   int flags)
{
	Assert(query != NULL && IsA(query, Query));

	if (!(flags & QTW_DONT_COPY_QUERY))
	{
		Query	   *newquery;

		FLATCOPY(newquery, query, Query);
		query = newquery;
	}

	MUTATE(query->targetList, query->targetList, List *);
	MUTATE(query->returningList, query->returningList, List *);
	MUTATE(query->jointree, query->jointree, FromExpr *);
	MUTATE(query->setOperations, query->setOperations, Node *);
	MUTATE(query->groupClause, query->groupClause, List *);
	MUTATE(query->scatterClause, query->scatterClause, List *);
	MUTATE(query->havingQual, query->havingQual, Node *);
	MUTATE(query->windowClause, query->windowClause, List *);
	MUTATE(query->limitOffset, query->limitOffset, Node *);
	MUTATE(query->limitCount, query->limitCount, Node *);
	if (!(flags & QTW_IGNORE_CTE_SUBQUERIES))
		MUTATE(query->cteList, query->cteList, List *);
	else	/* else copy CTE list as-is */
		query->cteList = copyObject(query->cteList);
	query->rtable = range_table_mutator(query->rtable,
										mutator, context, flags);
	return query;
}

/*
 * range_table_mutator is just the part of query_tree_mutator that processes
 * a query's rangetable.  This is split out since it can be useful on
 * its own.
 */
List *
range_table_mutator(List *rtable,
					Node *(*mutator) (),
					void *context,
					int flags)
{
	List	   *newrt = NIL;
	ListCell   *rt;

	foreach(rt, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);
		RangeTblEntry *newrte;

		FLATCOPY(newrte, rte, RangeTblEntry);
		switch (rte->rtekind)
		{
			case RTE_RELATION:
			case RTE_SPECIAL:
            case RTE_VOID:
			case RTE_CTE:
				/* we don't bother to copy eref, aliases, etc; OK? */
				break;
			case RTE_SUBQUERY:
				if (!(flags & QTW_IGNORE_RT_SUBQUERIES))
				{
					CHECKFLATCOPY(newrte->subquery, rte->subquery, Query);
					MUTATE(newrte->subquery, newrte->subquery, Query *);
				}
				else
				{
					/* else, copy RT subqueries as-is */
					newrte->subquery = copyObject(rte->subquery);
				}
				break;
			case RTE_JOIN:
				if (!(flags & QTW_IGNORE_JOINALIASES))
					MUTATE(newrte->joinaliasvars, rte->joinaliasvars, List *);
				else
				{
					/* else, copy join aliases as-is */
					newrte->joinaliasvars = copyObject(rte->joinaliasvars);
				}
				break;
			case RTE_FUNCTION:
				MUTATE(newrte->funcexpr, rte->funcexpr, Node *);
				break;
			case RTE_TABLEFUNCTION:
				MUTATE(newrte->funcexpr, rte->funcexpr, Node *);
				MUTATE(newrte->subquery, rte->subquery, Query *);
				break;
			case RTE_VALUES:
				MUTATE(newrte->values_lists, rte->values_lists, List *);
				break;
		}
		newrt = lappend(newrt, newrte);
	}
	return newrt;
}


/*
 * flatten_join_alias_var_optimizer
 *	  Replace Vars that reference JOIN outputs with references to the original
 *	  relation variables instead.
 */
Query *
flatten_join_alias_var_optimizer(Query *query, int queryLevel)
{
	Query *queryNew = (Query *) copyObject(query);

	/* Create a PlannerInfo data structure for this subquery */
	PlannerInfo *root = makeNode(PlannerInfo);
	root->parse = queryNew;
	root->query_level = queryLevel;

	root->glob = makeNode(PlannerGlobal);
	root->glob->boundParams = NULL;
	root->glob->paramlist = NIL;
	root->glob->subplans = NIL;
	root->glob->subrtables = NIL;
	root->glob->rewindPlanIDs = NULL;
	root->glob->finalrtable = NIL;
	root->glob->relationOids = NIL;
	root->glob->invalItems = NIL;
	root->glob->transientPlan = false;

	root->config = DefaultPlannerConfig();

	root->parent_root = NULL;
	root->planner_cxt = CurrentMemoryContext;
	root->init_plans = NIL;

	root->list_cteplaninfo = NIL;
	root->in_info_list = NIL;
	root->append_rel_list = NIL;

	root->hasJoinRTEs = false;
	root->hasOuterJoins = false;

	ListCell *plc = NULL;
	foreach(plc, queryNew->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(plc);

		if (rte->rtekind == RTE_JOIN)
		{
			root->hasJoinRTEs = true;
			if (IS_OUTER_JOIN(rte->jointype))
			{
				root->hasOuterJoins = true;
				break;
			}
		}
	}

	/*
	 * Flatten join alias for expression in
	 * 1. targetlist
	 * 2. returningList
	 * 3. having qual
	 * 4. scatterClause
	 * 5. limit offset
	 * 6. limit count
	 * 
	 * We flatten the above expressions since these entries may be moved during the query 
	 * normalization step before algebrization. In contrast, the planner flattens alias 
	 * inside quals to allow predicates involving such vars to be pushed down. 
	 * 
	 * Here we ignore the flattening of quals due to the following reasons:
	 * 1. we assume that the function will be called before Query->DXL translation:
	 * 2. the quals never gets moved from old query to the new top-level query in the 
	 * query normalization phase before algebrization. In other words, the quals hang of 
	 * the same query structure that is now the new derived table.
	 * 3. the algebrizer can resolve the abiquity of join aliases in quals since we maintain 
	 * all combinations of <query level, varno, varattno> to DXL-ColId during Query->DXL translation.
	 * 
	 */

	List *targetList = queryNew->targetList;
	if (NIL != targetList)
	{
		queryNew->targetList = (List *) flatten_join_alias_vars(root, (Node *) targetList);
		pfree(targetList);
	}

	List * returningList = queryNew->returningList;
	if (NIL != returningList)
	{
		queryNew->returningList = (List *) flatten_join_alias_vars(root, (Node *) returningList);
		pfree(returningList);
	}

	Node *havingQual = queryNew->havingQual;
	if (NULL != havingQual)
	{
		queryNew->havingQual = flatten_join_alias_vars(root, havingQual);
		pfree(havingQual);
	}

	List *scatterClause = queryNew->scatterClause;
	if (NIL != scatterClause)
	{
		queryNew->scatterClause = (List *) flatten_join_alias_vars(root, (Node *) scatterClause);
		pfree(scatterClause);
	}

	Node *limitOffset = queryNew->limitOffset;
	if (NULL != limitOffset)
	{
		queryNew->limitOffset = flatten_join_alias_vars(root, limitOffset);
		pfree(limitOffset);
	}

	List *windowClause = queryNew->windowClause;
	if (NIL != queryNew->windowClause)
	{
		ListCell *l = NULL;
		foreach (l, windowClause)
		{
			WindowSpec *w =(WindowSpec*)lfirst(l);

			if ( w != NULL && w->frame != NULL )
			{
				WindowFrame *f = w->frame;

				if (f->trail != NULL )
				{
					f->trail->val = flatten_join_alias_vars(root, f->trail->val);
				}
				if ( f->lead != NULL )
				{
					f->lead->val = flatten_join_alias_vars(root, f->lead->val);
				}
			}
		}
	}

	Node *limitCount = queryNew->limitCount;
	if (NULL != limitCount)
	{
		queryNew->limitCount = flatten_join_alias_vars(root, limitCount);
		pfree(limitCount);
	}

    return queryNew;
}

/*
 * query_or_expression_tree_mutator --- hybrid form
 *
 * This routine will invoke query_tree_mutator if called on a Query node,
 * else will invoke the mutator directly.  This is a useful way of starting
 * the recursion when the mutator's normal change of state is not appropriate
 * for the outermost Query node.
 */
Node *
query_or_expression_tree_mutator(Node *node,
								 Node *(*mutator) (),
								 void *context,
								 int flags)
{
	if (node && IsA(node, Query))
		return (Node *) query_tree_mutator((Query *) node,
										   mutator,
										   context,
										   flags);
	else
		return mutator(node, context);
}

/* 
 * Does grp contain GroupingClause or not? Useful for indentifying use of
 * ROLLUP, CUBE and grouping sets.
 */
bool
contain_extended_grouping(List *grp)
{
	return contain_grouping_clause_walker((Node *)grp, NULL);
}

static bool
contain_grouping_clause_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	else if (IsA(node, GroupingClause))
		return true;			/* abort the tree traversal and return true */
	
	Assert(!IsA(node, SubLink));
	return expression_tree_walker(node, contain_grouping_clause_walker, 
								  context);
}

/*
 * is_grouping_extension -
 *     Return true if a given grpsets contain multiple grouping sets.
 *
 * This function also returns false when a query has a single unique
 * groupig set appearing multiple times.
 */
bool
is_grouping_extension(CanonicalGroupingSets *grpsets)
{
	if (grpsets == NULL ||
		grpsets->ngrpsets == 0)
		return false;

	if (grpsets->ngrpsets == 1)
		return false;

	return true;
}

/**
 * Returns true if the equality operator with the given opno
 *   values is a true equality operator (unlike some of the
 *   box/rectangle/etc. types which implement 'goofy' operators).
 *
 * This function currently only knows about built-in types, and is
 *   conservative with regard to which it returns true for (only
 *     ones which have been verified to work the right way).
 */
bool is_builtin_true_equality_between_same_type(int opno)
{
	switch (opno)
	{
	case BitEqualOperator:
	case BooleanEqualOperator:
	case BPCharEqualOperator:
	case CashEqualOperator:
	case CharEqualOperator:
	case CidEqualOperator:
	case DateEqualOperator:
	case Float4EqualOperator:
	case Float8EqualOperator:
	case Int2EqualOperator:
	case Int4EqualOperator:
	case Int8EqualOperator:
	case IntervalEqualOperator:
	case NameEqualOperator:
	case NumericEqualOperator:
	case OidEqualOperator:
	case RelTimeEqualOperator:
	case TextEqualOperator:
	case TIDEqualOperator:
	case TimeEqualOperator:
	case TimestampEqualOperator:
	case TimestampTZEqualOperator:
	case TimeTZEqualOperator:
	case XidEqualOperator:
		return true;

	default:
		return false;
	}
}

/**
 * Returns true if the equality operator with the given opno
 *   values is an equality operator, with same type on both sides
 *  (unlike int24 equality) AND the type being compare is greenplum hashable
 *
 *
 * Note that this function is conservative with regard to when it returns true:
 *   it is okay to have some greenplum hashtable types that don't have entries here
 *   (this function may return false even if this is a comparison between
 *        a greenplum hashable type and itself
 *
 * Note also that i think it might be possible for this to return true even
 *   if the operands of the operator are not themselves greenplum hashable,
 *   because of type conversion or something.  I'm not 100% sure on that.
 */
bool
is_builtin_greenplum_hashable_equality_between_same_type(int opno)
{
    switch(opno)
    {
        case BitEqualOperator:
        case BooleanEqualOperator:
        case BPCharEqualOperator:
        case CashEqualOperator:
        case CharEqualOperator:
        case DateEqualOperator:
        case Float4EqualOperator:
        case Float8EqualOperator:
        case Int2EqualOperator:
        case Int4EqualOperator:
        case Int8EqualOperator:
        case IntervalEqualOperator:
        case NameEqualOperator:
        case NumericEqualOperator:
        case OidEqualOperator:
        case RelTimeEqualOperator:
        case TextEqualOperator:
        case TIDEqualOperator:
        case TimeEqualOperator:
        case TimestampEqualOperator:
        case TimestampTZEqualOperator:
        case TimeTZEqualOperator:

        /* these new ones were added to list for MPP-7858 */
        case AbsTimeEqualOperator:
        case ByteaEqualOperator:
        case InetEqualOperator: /* for inet and cidr */
        case MacAddrEqualOperator:
        case TIntervalEqualOperator:
        case VarbitEqualOperator:
            return true;

/*
        these types are greenplum hashable but haven't checked the semantics of these types function
        case ACLITEMOID:
        case ANYARRAYOID: 
        case INT2VECTOROID: 
        case OIDVECTOROID: 
        case REGPROCOID: 
        case REGPROCEDUREOID: 
        case REGOPEROID: 
        case REGOPERATOROID: 
        case REGCLASSOID: 
        case REGTYPEOID: 
  */
        default:

        return false;
    }
}

/**
 * Structs and Methods to support searching of matching subexpressions.
 */

typedef struct subexpression_matching_context
{
	Expr *needle;	/* This is the expression being searched */
} subexpression_matching_context;

/**
 * expression_matching_walker checks if the expression 'needle' in context is a sub-expression of hayStack.
 */
static bool subexpression_matching_walker(Node *hayStack, void *context)
{
	Assert(context);
	subexpression_matching_context *ctx = (subexpression_matching_context *) context;
	Assert(ctx->needle);

	if (!hayStack)
	{
		return false;
	}

	if (equal(ctx->needle, hayStack))
	{
		return true;
	}

	return expression_tree_walker(hayStack, subexpression_matching_walker, (void *) context);
}

/**
 * Method checks if expr1 is a subexpression of expr2.
 * For example, expr1 = (x + 2) and expr = (x + 2 ) * 100 + 20 would return true.
 */
bool subexpression_match(Expr *expr1, Expr *expr2)
{
	subexpression_matching_context ctx;
	ctx.needle = expr1;
	return subexpression_matching_walker((Node *) expr2, (void *) &ctx);
}
