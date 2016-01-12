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
 * tidpath.c
 *	  Routines to determine which TID conditions are usable for scanning
 *	  a given relation, and create TidPaths accordingly.
 *
 * What we are looking for here is WHERE conditions of the form
 * "CTID = pseudoconstant", which can be implemented by just fetching
 * the tuple directly via heap_fetch().  We can also handle OR'd conditions
 * such as (CTID = const1) OR (CTID = const2), as well as ScalarArrayOpExpr
 * conditions of the form CTID = ANY(pseudoconstant_array).  In particular
 * this allows
 *		WHERE ctid IN (tid1, tid2, ...)
 *
 * There is currently no special support for joins involving CTID; in
 * particular nothing corresponding to best_inner_indexscan().	Since it's
 * not very useful to store TIDs of one table in another table, there
 * doesn't seem to be enough use-case to justify adding a lot of code
 * for that.
 *
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/optimizer/path/tidpath.c,v 1.28 2006/10/04 00:29:54 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "parser/parse_expr.h"


static bool IsTidEqualClause(OpExpr *node, int varno);
static bool IsTidEqualAnyClause(ScalarArrayOpExpr *node, int varno);
static List *TidQualFromExpr(Node *expr, int varno);
static List *TidQualFromRestrictinfo(List *restrictinfo, int varno);


/*
 * Check to see if an opclause is of the form
 *		CTID = pseudoconstant
 * or
 *		pseudoconstant = CTID
 *
 * We check that the CTID Var belongs to relation "varno".	That is probably
 * redundant considering this is only applied to restriction clauses, but
 * let's be safe.
 */
static bool
IsTidEqualClause(OpExpr *node, int varno)
{
	Node	   *arg1,
			   *arg2,
			   *other;
	Var		   *var;

	/* Operator must be tideq */
	if (node->opno != TIDEqualOperator)
		return false;
	if (list_length(node->args) != 2)
		return false;
	arg1 = linitial(node->args);
	arg2 = lsecond(node->args);

	/* Look for CTID as either argument */
	other = NULL;
	if (arg1 && IsA(arg1, Var))
	{
		var = (Var *) arg1;
		if (var->varattno == SelfItemPointerAttributeNumber &&
			var->vartype == TIDOID &&
			var->varno == varno &&
			var->varlevelsup == 0)
			other = arg2;
	}
	if (!other && arg2 && IsA(arg2, Var))
	{
		var = (Var *) arg2;
		if (var->varattno == SelfItemPointerAttributeNumber &&
			var->vartype == TIDOID &&
			var->varno == varno &&
			var->varlevelsup == 0)
			other = arg1;
	}
	if (!other)
		return false;
	if (exprType(other) != TIDOID)
		return false;			/* probably can't happen */

	/* The other argument must be a pseudoconstant */
	if (!is_pseudo_constant_clause(other))
		return false;

	return true;				/* success */
}

/*
 * Check to see if a clause is of the form
 *		CTID = ANY (pseudoconstant_array)
 */
static bool
IsTidEqualAnyClause(ScalarArrayOpExpr *node, int varno)
{
	Node	   *arg1,
			   *arg2;

	/* Operator must be tideq */
	if (node->opno != TIDEqualOperator)
		return false;
	if (!node->useOr)
		return false;
	Assert(list_length(node->args) == 2);
	arg1 = linitial(node->args);
	arg2 = lsecond(node->args);

	/* CTID must be first argument */
	if (arg1 && IsA(arg1, Var))
	{
		Var		   *var = (Var *) arg1;

		if (var->varattno == SelfItemPointerAttributeNumber &&
			var->vartype == TIDOID &&
			var->varno == varno &&
			var->varlevelsup == 0)
		{
			/* The other argument must be a pseudoconstant */
			if (is_pseudo_constant_clause(arg2))
				return true;	/* success */
		}
	}

	return false;
}

/*
 *	Extract a set of CTID conditions from the given qual expression
 *
 *	Returns a List of CTID qual expressions (with implicit OR semantics
 *	across the list), or NIL if there are no usable conditions.
 *
 *	If the expression is an AND clause, we can use a CTID condition
 *	from any sub-clause.  If it is an OR clause, we must be able to
 *	extract a CTID condition from every sub-clause, or we can't use it.
 *
 *	In theory, in the AND case we could get CTID conditions from different
 *	sub-clauses, in which case we could try to pick the most efficient one.
 *	In practice, such usage seems very unlikely, so we don't bother; we
 *	just exit as soon as we find the first candidate.
 */
static List *
TidQualFromExpr(Node *expr, int varno)
{
	List	   *rlst = NIL;
	ListCell   *l;

	if (is_opclause(expr))
	{
		/* base case: check for tideq opclause */
		if (IsTidEqualClause((OpExpr *) expr, varno))
			rlst = list_make1(expr);
	}
	else if (expr && IsA(expr, ScalarArrayOpExpr))
	{
		/* another base case: check for tid = ANY clause */
		if (IsTidEqualAnyClause((ScalarArrayOpExpr *) expr, varno))
			rlst = list_make1(expr);
	}
	else if (expr && IsA(expr, CurrentOfExpr))
	{
		/* another base case: check for CURRENT OF on this rel */
		Insist(((CurrentOfExpr *) expr)->cvarno == varno);
		rlst = list_make1(expr);
	}
	else if (and_clause(expr))
	{
		foreach(l, ((BoolExpr *) expr)->args)
		{
			rlst = TidQualFromExpr((Node *) lfirst(l), varno);
			if (rlst)
				break;
		}
	}
	else if (or_clause(expr))
	{
		foreach(l, ((BoolExpr *) expr)->args)
		{
			List	   *frtn = TidQualFromExpr((Node *) lfirst(l), varno);

			if (frtn)
				rlst = list_concat(rlst, frtn);
			else
			{
				if (rlst)
					list_free(rlst);
				rlst = NIL;
				break;
			}
		}
	}
	return rlst;
}

/*
 *	Extract a set of CTID conditions from the given restrictinfo list
 *
 *	This is essentially identical to the AND case of TidQualFromExpr,
 *	except for the format of the input.
 */
static List *
TidQualFromRestrictinfo(List *restrictinfo, int varno)
{
	List	   *rlst = NIL;
	ListCell   *l;

	foreach(l, restrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		if (!IsA(rinfo, RestrictInfo))
			continue;			/* probably should never happen */
		rlst = TidQualFromExpr((Node *) rinfo->clause, varno);
		if (rlst)
			break;
	}
	return rlst;
}

/*
 * create_tidscan_paths
 *	  Create paths corresponding to direct TID scans of the given rel.
 *
 *	  Candidate paths are added to the rel's pathlist (using add_path).
 *
 * CDB: Instead of handing the paths to add_path(), we append them to a List
 * (*ppathlist) belonging to the caller.
 *
 * CDB TODO: Set rel->onerow if at most one tid is to be fetched.
 */
void
create_tidscan_paths(PlannerInfo *root, RelOptInfo *rel, List** ppathlist)
{
	List	   *tidquals;

	tidquals = TidQualFromRestrictinfo(rel->baserestrictinfo, rel->relid);

	if (tidquals)
		*ppathlist = lappend(*ppathlist, create_tidscan_path(root, rel, tidquals));
}
