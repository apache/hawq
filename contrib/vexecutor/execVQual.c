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
#include "utils/builtins.h"
#include "execVQual.h"
#include "cdb/cdbhash.h"
static bool
ExecVTargetList(List *targetlist,
			   ExprContext *econtext,
			   TupleTableSlot *slot,
			   ExprDoneCond *itemIsDone,
			   ExprDoneCond *isDone);
/*
 * ExecVariableList
 *		Evaluates a simple-Variable-list projection.
 *
 * Results are stored into the passed values and isnull arrays.
 */
static void
ExecVecVariableList(ProjectionInfo *projInfo,
                 Datum values)
{
    ExprContext *econtext = projInfo->pi_exprContext;
    int		   *varSlotOffsets = projInfo->pi_varSlotOffsets;
    int		   *varNumbers = projInfo->pi_varNumbers;
    TupleBatch  tb = (TupleBatch) DatumGetPointer(values);
    int			i;
    tb->ncols = list_length(projInfo->pi_targetlist);

    /*
     * Assign to result by direct extraction of fields from source slots ... a
     * mite ugly, but fast ...
     */
    for (i = list_length(projInfo->pi_targetlist) - 1; i >= 0; i--)
    {
        char	   *slotptr = ((char *) econtext) + varSlotOffsets[i];
        TupleTableSlot *varSlot = *((TupleTableSlot **) slotptr);
        int			varNumber = varNumbers[i] - 1;
        tb->datagroup[i] = ((TupleBatch)varSlot->PRIVATE_tb)->datagroup[varNumber];
    }
}

TupleTableSlot *
ExecVProject(ProjectionInfo *projInfo, ExprDoneCond *isDone)
{
    TupleTableSlot *slot;
    Assert(projInfo != NULL);

    /*
     * get the projection info we want
     */
    slot = projInfo->pi_slot;

    /*
     * Clear any former contents of the result slot.  This makes it safe for
     * us to use the slot's Datum/isnull arrays as workspace. (Also, we can
     * return the slot as-is if we decide no rows can be projected.)
     */
    ExecClearTuple(slot);

    /*
     * form a new result tuple (if possible); if successful, mark the result
     * slot as containing a valid virtual tuple
     */
    if (projInfo->pi_isVarList)
    {
        /* simple Var list: this always succeeds with one result row */
        if (isDone)
            *isDone = ExprSingleResult;

        ExecVecVariableList(projInfo,slot->PRIVATE_tb);
        ExecStoreVirtualTuple(slot);
    }
    else
    {
       if (ExecVTargetList(projInfo->pi_targetlist,
                           projInfo->pi_exprContext,
                           slot,
                           (ExprDoneCond *) projInfo->pi_itemIsDone,
                           isDone))
            ExecStoreVirtualTuple(slot);
    }

    return slot;
}

/*
 * VirtualNodeProc
 *      return value indicate whether has a tuple data fill in slot->PRIVATE_tts_values slot
 *      This function will be invoked in V->N process.
 */
bool
VirtualNodeProc(TupleTableSlot *slot){
    if(TupIsNull(slot) )
        return false;

    TupleBatch tb = (TupleBatch)DatumGetPointer(slot->PRIVATE_tb);
    ExecClearTuple(slot);

    while (tb->skip[tb->iter] && tb->iter < tb->nrows)
        tb->iter++;

    if(tb->iter == tb->nrows)
        return false;

    for(int i = 0;i < tb->ncols;i ++)
    {
        vtype *vt = tb->datagroup[i];
        slot->PRIVATE_tts_values[i] = vt->values[tb->iter];
        slot->PRIVATE_tts_isnull[i] = vt->isnull[tb->iter];
    }
    tb->iter ++;
    ExecStoreVirtualTuple(slot);
    return true;
}

/*
 * get values from vectorized tuple slot
 * copy from src/backend/executor/execQual.c
 */
static Datum
VExecEvalScalarVar(ExprState *exprstate, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone)
{
	Var		   *variable = (Var *) exprstate->expr;
	TupleTableSlot *slot;
	AttrNumber	attnum;
	TupleBatch tb;

	if (isDone)
		*isDone = ExprSingleResult;

	Assert(econtext->ecxt_scantuple != NULL || econtext->ecxt_innertuple != NULL || econtext->ecxt_outertuple != NULL);
	/*
	 * Get the input slot and attribute number we want
	 *
	 * The asserts check that references to system attributes only appear at
	 * the level of a relation scan; at higher levels, system attributes must
	 * be treated as ordinary variables (since we no longer have access to the
	 * original tuple).
	 */
	attnum = variable->varattno;

	switch (variable->varno)
	{
		case INNER:				/* get the tuple from the inner node */
			slot = econtext->ecxt_innertuple;
			Assert(attnum > 0);
			break;

		case OUTER:				/* get the tuple from the outer node */
			slot = econtext->ecxt_outertuple;
			Assert(attnum > 0);
			break;

		default:				/* get the tuple from the relation being
								 * scanned */
			slot = econtext->ecxt_scantuple;
			break;
	}

	/* isNull is a single value, it can not be used when data is vectorized */
	*isNull = false;

	/* Fetch the value from the slot */
	Assert(NULL != slot);
	tb = (TupleBatch )slot->PRIVATE_tb;

	Assert(NULL != tb);

	return PointerGetDatum(tb->datagroup[attnum - 1]);
}


/*
 * get values from vectorized tuple slot
 * copy from src/backend/executor/execQual.c
 */
static Datum
VExecEvalVar(ExprState *exprstate, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone)
{
	Var		   *variable = (Var *) exprstate->expr;
	TupleTableSlot *slot;
	AttrNumber	attnum;

	if (isDone)
		*isDone = ExprSingleResult;

	Assert(econtext->ecxt_scantuple != NULL || econtext->ecxt_innertuple != NULL || econtext->ecxt_outertuple != NULL);
	/*
	 * Get the input slot and attribute number we want
	 *
	 * The asserts check that references to system attributes only appear at
	 * the level of a relation scan; at higher levels, system attributes must
	 * be treated as ordinary variables (since we no longer have access to the
	 * original tuple).
	 */
	attnum = variable->varattno;

	switch (variable->varno)
	{
		case INNER:				/* get the tuple from the inner node */
			slot = econtext->ecxt_innertuple;
			Assert(attnum > 0);
			break;

		case OUTER:				/* get the tuple from the outer node */
			slot = econtext->ecxt_outertuple;
			Assert(attnum > 0);
			break;

		default:				/* get the tuple from the relation being
								 * scanned */
			slot = econtext->ecxt_scantuple;
			break;
	}

	Assert(NULL != slot);

	if (attnum != InvalidAttrNumber)
	{
		TupleBatch tb;
		/*
		 * Scalar variable case.
		 *
		 * If it's a user attribute, check validity (bogus system attnums will
		 * be caught inside slot_getattr).  What we have to check for here
		 * is the possibility of an attribute having been changed in type
		 * since the plan tree was created.  Ideally the plan would get
		 * invalidated and not re-used, but until that day arrives, we need
		 * defenses.  Fortunately it's sufficient to check once on the first
		 * time through.
		 *
		 * Note: we allow a reference to a dropped attribute.  slot_getattr
		 * will force a NULL result in such cases.
		 *
		 * Note: ideally we'd check typmod as well as typid, but that seems
		 * impractical at the moment: in many cases the tupdesc will have
		 * been generated by ExecTypeFromTL(), and that can't guarantee to
		 * generate an accurate typmod in all cases, because some expression
		 * node types don't carry typmod.
		 */
		if (attnum > 0)
		{
			TupleDesc	slot_tupdesc = slot->tts_tupleDescriptor;
			Form_pg_attribute attr;

			if (attnum > slot_tupdesc->natts)	/* should never happen */
				elog(ERROR, "attribute number %d exceeds number of columns %d",
					 attnum, slot_tupdesc->natts);

			attr = slot_tupdesc->attrs[attnum - 1];

			/* can't check type if dropped, since atttypid is probably 0 */
			if (!attr->attisdropped)
			{
				if (variable->vartype != attr->atttypid &&
					GetNtype(variable->vartype) != attr->atttypid)
					ereport(ERROR,
							(errmsg("attribute %d has wrong type", attnum),
							 errdetail("Table has type %s, but query expects %s.",
									   format_type_be(attr->atttypid),
									   format_type_be(variable->vartype))));
			}
		}

		/* Skip the checking on future executions of node */
		exprstate->evalfunc = VExecEvalScalarVar;

		/* isNull is a single value, it can not be used when data is vectorized */
		*isNull = false;

		/* Fetch the value from the slot */
		tb = (TupleBatch )slot->PRIVATE_tb;

		Assert(NULL != tb);
		return PointerGetDatum(tb->datagroup[attnum - 1]);
	}
	else
	{
		/* NOT support so far */
		Assert(false);
	}

	return PointerGetDatum(NULL);
}


/* ----------------------------------------------------------------
 *		VExecEvalNot
 *		VExecEvalOr
 *		VExecEvalAnd
 *
 * copy from src/backend/executor/execQual.c
 *
 *		Evaluate boolean expressions, with appropriate short-circuiting.
 *
 *		The query planner reformulates clause expressions in the
 *		qualification to conjunctive normal form.  If we ever get
 *		an AND to evaluate, we can be sure that it's not a top-level
 *		clause in the qualification, but appears lower (as a function
 *		argument, for example), or in the target list.	Not that you
 *		need to know this, mind you...
 * ----------------------------------------------------------------
 */
static Datum
VExecEvalNot(BoolExprState *notclause, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone)
{
	ExprState  *clause = linitial(notclause->args);
	Datum		expr_value;
	vbool		*ret;
	int			i;

	if (isDone)
		*isDone = ExprSingleResult;

	expr_value = ExecEvalExpr(clause, econtext, isNull, NULL);

	ret = (vbool*)DatumGetPointer(expr_value);
	for(i = 0; i < ret->dim; i++)
	{
		if(!ret->isnull[i])
			ret->values[i] = !ret->values[i];
	}

	/*
	 * evaluation of 'not' is simple.. expr is false, then return 'true' and
	 * vice versa.
	 */
	return PointerGetDatum(ret);
}

/* ----------------------------------------------------------------
 *		ExecEvalOr
 * ----------------------------------------------------------------
 */
static Datum
VExecEvalOr(BoolExprState *orExpr, ExprContext *econtext,
		   bool *isNull, ExprDoneCond *isDone)
{
	List	   *clauses = orExpr->args;
	ListCell   *clause;
	vbool	*res = NULL;
	vbool	*next = 	NULL;
	bool		skip = true;
	int 		i = 0;

	if (isDone)
		*isDone = ExprSingleResult;

	/*
	 * If any of the clauses is TRUE, the OR result is TRUE regardless of the
	 * states of the rest of the clauses, so we can stop evaluating and return
	 * TRUE immediately.  If none are TRUE and one or more is NULL, we return
	 * NULL; otherwise we return FALSE.  This makes sense when you interpret
	 * NULL as "don't know": if we have a TRUE then the OR is TRUE even if we
	 * aren't sure about some of the other inputs. If all the known inputs are
	 * FALSE, but we have one or more "don't knows", then we have to report
	 * that we "don't know" what the OR's result should be --- perhaps one of
	 * the "don't knows" would have been TRUE if we'd known its value.  Only
	 * when all the inputs are known to be FALSE can we state confidently that
	 * the OR's result is FALSE.
	 */
	foreach(clause, clauses)
	{
		ExprState  *clausestate = (ExprState *) lfirst(clause);
		Datum		clause_value;

		/*
		 * to check if all the values is true, then skip to evaluate some
		 * expressions
		 */
		skip = true;

		clause_value = ExecEvalExpr(clausestate, econtext, isNull, NULL);

		if(NULL == res)
		{
			res = DatumGetPointer(clause_value);
			Assert(NULL != res->isnull);
			for(i = 0; i < res->dim; i++)
			{
				if(res->isnull[i] ||
					!res->values[i])
				{
					skip = false;
					break;
				}
			}
		}
		else
		{
			next = DatumGetPointer(clause_value);
			Assert(NULL != res->isnull && NULL != next->isnull);
			for(i = 0; i < res->dim; i++)
			{
				res->isnull[i] =
						(res->isnull[i] || next->isnull[i]);
				res->values[i] = (res->values[i] || next->values[i]);
				if(skip && (res->isnull[i] || !res->values[i]))
					skip = false;
			}
		}

		if(skip)
		{
			*isNull = false;
			return PointerGetDatum(res);
		}
	}

	*isNull = false;
	return PointerGetDatum(res);
}

static Datum
VExecEvalAndInternal(List* clauses, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone)
{
	ListCell   *clause;
	vbool	*res = NULL;
	vbool	*next = 	NULL;
	bool		skip = true;
	int 		i = 0;

	if (isDone)
		*isDone = ExprSingleResult;

	/*
	 * If any of the clauses is FALSE, the AND result is FALSE regardless of
	 * the states of the rest of the clauses, so we can stop evaluating and
	 * return FALSE immediately.  If none are FALSE and one or more is NULL,
	 * we return NULL; otherwise we return TRUE.  This makes sense when you
	 * interpret NULL as "don't know", using the same sort of reasoning as for
	 * OR, above.
	 */

	foreach(clause, clauses)
	{
		ExprState  *clausestate = (ExprState *) lfirst(clause);
		Datum		clause_value;

		/*
		 * to check if all the values is false, then skip to evaluate some
		 * expressions
		 */
		skip = true;

		clause_value = ExecEvalExpr(clausestate, econtext, isNull, NULL);

		if(NULL == res)
		{
			res = DatumGetPointer(clause_value);
			Assert(NULL != res->isnull);
			for(i = 0; i < res->dim; i++)
			{
				if(res->isnull[i] || res->values[i])
				{
					skip = false;
					break;
				}
			}
		}
		else
		{
			next = DatumGetPointer(clause_value);
			Assert(NULL != res->isnull && NULL != next->isnull);
			for(i = 0; i < res->dim; i++)
			{
				res->isnull[i] =
						(res->isnull[i] || next->isnull[i]);
				res->values[i] = (res->values[i] && next->values[i]);
				if(skip && (res->isnull[i] || res->values[i]))
					skip = false;
			}
		}

		if(skip)
		{
			*isNull = false;
			return PointerGetDatum(res);
		}
	}

	*isNull = false;
	return PointerGetDatum(res);
}

/* ----------------------------------------------------------------
 *		ExecEvalAnd
 * ----------------------------------------------------------------
 */
static Datum
VExecEvalAnd(BoolExprState *andExpr, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone)
{
	return VExecEvalAndInternal(andExpr->args, econtext, isNull, isDone);
}

/*
 * Init the vectorized expressions
 */
ExprState *
VExecInitExpr(Expr *node, PlanState *parent)
{
	ExprState *state = NULL;

	/*
	 * Because Var is the leaf node of the expression tree, it have to be
	 * refactored first, otherwise the all call stack should be refactored.
	 */
	switch (nodeTag(node))
	{
		case T_Var:
			state = (ExprState *) makeNode(ExprState);
			state->evalfunc = VExecEvalVar;
			break;
		case T_BoolExpr:
			{
				BoolExpr   *boolexpr = (BoolExpr *) node;
				BoolExprState *bstate = makeNode(BoolExprState);

				switch (boolexpr->boolop)
				{
					case AND_EXPR:
						bstate->xprstate.evalfunc = (ExprStateEvalFunc) VExecEvalAnd;
						break;
					case OR_EXPR:
						bstate->xprstate.evalfunc = (ExprStateEvalFunc) VExecEvalOr;
						break;
					case NOT_EXPR:
						bstate->xprstate.evalfunc = (ExprStateEvalFunc) VExecEvalNot;
						break;
					default:
						elog(ERROR, "unrecognized boolop: %d",
							 (int) boolexpr->boolop);
						break;
				}
				bstate->args = (List *)
					ExecInitExpr((Expr *) boolexpr->args, parent);
				state = (ExprState *) bstate;
			}
			break;

		/*TODO: More and more expressions should be vectorized */
		default:
			break;
	}

	/* Common code for all state-node types */
	if(NULL != state)
		state->expr = node;

	return state;
}

/* ----------------------------------------------------------------
 *	copy from src/backend/executor/execQual.c
 *
 * NOTE:resultForNull do not used now, we can process it when call the
 * ExecVQual.
 * ----------------------------------------------------------------
 */
vbool*
ExecVQual(List *qual, ExprContext *econtext, bool resultForNull)
{
	vbool *result;
	MemoryContext oldContext;
	bool	 isNull;

	/*
	 * debugging stuff
	 */
	EV_printf("ExecQual: qual is ");
	EV_nodeDisplay(qual);
	EV_printf("\n");

	/*
	 * Run in short-lived per-tuple context while computing expressions.
	 */
	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	result = (vbool*)DatumGetPointer(VExecEvalAndInternal(qual, econtext, &isNull, NULL));

	MemoryContextSwitchTo(oldContext);

	return result;
}

/*
 * copy from src/backend/executor/ExecQual.c
 * ExecTargetList
 *		Evaluates a targetlist with respect to the given
 *		expression context.  Returns TRUE if we were able to create
 *		a result, FALSE if we have exhausted a set-valued expression.
 *
 * Results are stored into the passed values and isnull arrays.
 * The caller must provide an itemIsDone array that persists across calls.
 *
 * As with ExecEvalExpr, the caller should pass isDone = NULL if not
 * prepared to deal with sets of result tuples.  Otherwise, a return
 * of *isDone = ExprMultipleResult signifies a set element, and a return
 * of *isDone = ExprEndResult signifies end of the set of tuple.
 */
static bool
ExecVTargetList(List *targetlist,
			   ExprContext *econtext,
			   TupleTableSlot *slot,
			   ExprDoneCond *itemIsDone,
			   ExprDoneCond *isDone)
{
	MemoryContext oldContext;
	ListCell   *tl;
	TupleBatch tb;
	bool isnull;

	Assert(NULL != slot);

	tb = (TupleBatch)DatumGetPointer(slot->PRIVATE_tb);

	Assert(NULL != tb);

	tb->ncols = list_length(targetlist);

	/*
	 * Run in short-lived per-tuple context while computing expressions.
	 */
	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/*
	 * evaluate all the expressions in the target list
	 */
	if (isDone)
		*isDone = ExprSingleResult;		/* until proven otherwise */

	foreach(tl, targetlist)
	{
		GenericExprState *gstate = (GenericExprState *) lfirst(tl);
		TargetEntry *tle = (TargetEntry *) gstate->xprstate.expr;
		AttrNumber	resind = tle->resno - 1;

		tb->datagroup[resind] = (vtype*)ExecEvalExpr(gstate->arg,
									  econtext,
									  &isnull,
									  &itemIsDone[resind]);

		if (itemIsDone[resind] != ExprSingleResult)
		{
			/*TODO: DO NOT SUPPORT SO FAR*/
			elog(ERROR, "Only support single result so far.");
		}
	}

	/* Report success */
	MemoryContextSwitchTo(oldContext);

	return true;
}
