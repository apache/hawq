/*-------------------------------------------------------------------------
 *
 * execQual.c
 *	  Routines to evaluate qualification and targetlist expressions
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/execQual.c,v 1.199.2.3 2007/08/31 18:33:47 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecEvalExpr	- (now a macro) evaluate an expression, return a datum
 *		ExecEvalExprSwitchContext - same, but switch into eval memory context
 *		ExecQual		- return true/false if qualification is satisfied
 *		ExecProject		- form a new tuple by projecting the given tuple
 *
 *	 NOTES
 *		The more heavily used ExecEvalExpr routines, such as ExecEvalVar(),
 *		are hotspots. Making these faster will speed up the entire system.
 *
 *		ExecProject() is used to make tuple projections.  Rather then
 *		trying to speed it up, the execution plan should be pre-processed
 *		to facilitate attribute sharing between nodes wherever possible,
 *		instead of doing needless copying.	-cim 5/31/91
 *
 *		During expression evaluation, we check_stack_depth only in
 *		ExecMakeFunctionResult (and substitute routines) rather than at every
 *		single node.  This is a compromise that trades off precision of the
 *		stack limit setting to gain speed.
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/tuptoaster.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "cdb/partitionselection.h"
#include "commands/typecmds.h"
#include "executor/execdebug.h"
#include "executor/nodeAgg.h"
#include "executor/nodeSubplan.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "parser/parse_expr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"


/* static function decls */
static Datum ExecEvalArrayRef(ArrayRefExprState *astate,
				 ExprContext *econtext,
				 bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalAggref(AggrefExprState *aggref,
			   ExprContext *econtext,
			   bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalGroupingFunc(GroupingFuncExprState *gstate,
								  ExprContext *econtext,
								  bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalGrouping(ExprState *gstate,
							  ExprContext *econtext,
							  bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalGroupId(ExprState *gstate,
							 ExprContext *econtext,
							 bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalWindowRef(WindowRefExprState *winref,
			   ExprContext *econtext,
			   bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalVar(ExprState *exprstate, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalScalarVar(ExprState *exprstate, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalWholeRowVar(ExprState *exprstate, ExprContext *econtext,
					bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalWholeRowSlow(ExprState *exprstate, ExprContext *econtext,
					bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalConst(ExprState *exprstate, ExprContext *econtext,
			  bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalParam(ExprState *exprstate, ExprContext *econtext,
			  bool *isNull, ExprDoneCond *isDone);
static void ShutdownFuncExpr(Datum arg);
static TupleDesc get_cached_rowtype(Oid type_id, int32 typmod,
				   TupleDesc *cache_field, ExprContext *econtext);
static void ShutdownTupleDescRef(Datum arg);
static void ExecPrepareTuplestoreResult(FuncExprState *fcache,
							ExprContext *econtext,
							Tuplestorestate *resultStore,
							TupleDesc resultDesc);
static void tupledesc_match(TupleDesc dst_tupdesc, TupleDesc src_tupdesc);
static Datum ExecMakeFunctionResult(FuncExprState *fcache,
					   ExprContext *econtext,
					   bool *isNull,
					   ExprDoneCond *isDone);
static Datum ExecMakeFunctionResultNoSets(FuncExprState *fcache,
							 ExprContext *econtext,
							 bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalFunc(FuncExprState *fcache, ExprContext *econtext,
			 bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalOper(FuncExprState *fcache, ExprContext *econtext,
			 bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalDistinct(FuncExprState *fcache, ExprContext *econtext,
				 bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalScalarArrayOp(ScalarArrayOpExprState *sstate,
					  ExprContext *econtext,
					  bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalNot(BoolExprState *notclause, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalOr(BoolExprState *orExpr, ExprContext *econtext,
		   bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalAnd(BoolExprState *andExpr, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalConvertRowtype(ConvertRowtypeExprState *cstate,
					   ExprContext *econtext,
					   bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalCase(CaseExprState *caseExpr, ExprContext *econtext,
			 bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalCaseTestExpr(ExprState *exprstate,
					 ExprContext *econtext,
					 bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalArray(ArrayExprState *astate,
			  ExprContext *econtext,
			  bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalRow(RowExprState *rstate,
			ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalRowCompare(RowCompareExprState *rstate,
				   ExprContext *econtext,
				   bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalTableValue(ExprState   *estate,
								ExprContext *econtext,
								bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalCoalesce(CoalesceExprState *coalesceExpr,
				 ExprContext *econtext,
				 bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalMinMax(MinMaxExprState *minmaxExpr,
			   ExprContext *econtext,
			   bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalNullIf(FuncExprState *nullIfExpr,
			   ExprContext *econtext,
			   bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalNullTest(NullTestState *nstate,
				 ExprContext *econtext,
				 bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalBooleanTest(GenericExprState *bstate,
					ExprContext *econtext,
					bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalCoerceToDomain(CoerceToDomainState *cstate,
					   ExprContext *econtext,
					   bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalCoerceToDomainValue(ExprState *exprstate,
							ExprContext *econtext,
							bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalPercentileExpr(PercentileExprState *exprstate,
					ExprContext *econtext,
					bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalFieldSelect(FieldSelectState *fstate,
					ExprContext *econtext,
					bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalFieldStore(FieldStoreState *fstate,
				   ExprContext *econtext,
				   bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalRelabelType(GenericExprState *exprstate,
					ExprContext *econtext,
					bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalPartOidExpr(PartOidExprState *exprstate,
						ExprContext *econtext,
						bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalPartDefaultExpr(PartDefaultExprState *exprstate,
						ExprContext *econtext,
						bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalPartBoundExpr(PartBoundExprState *exprstate,
						ExprContext *econtext,
						bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalPartBoundInclusionExpr(PartBoundInclusionExprState *exprstate,
								ExprContext *econtext,
								bool *isNull, ExprDoneCond *isDone);
static Datum ExecEvalPartBoundOpenExpr(PartBoundOpenExprState *exprstate,
								ExprContext *econtext,
								bool *isNull, ExprDoneCond *isDone);
static bool ExecIsExprUnsafeToConst_walker(Node *node, void *context);
static bool ExecIsExprUnsafeToConst(Node *node);


/* ----------------------------------------------------------------
 *		ExecEvalExpr routines
 *
 *		Recursively evaluate a targetlist or qualification expression.
 *
 * Each of the following routines having the signature
 *		Datum ExecEvalFoo(ExprState *expression,
 *						  ExprContext *econtext,
 *						  bool *isNull,
 *						  ExprDoneCond *isDone);
 * is responsible for evaluating one type or subtype of ExprState node.
 * They are normally called via the ExecEvalExpr macro, which makes use of
 * the function pointer set up when the ExprState node was built by
 * ExecInitExpr.  (In some cases, we change this pointer later to avoid
 * re-executing one-time overhead.)
 *
 * Note: for notational simplicity we declare these functions as taking the
 * specific type of ExprState that they work on.  This requires casting when
 * assigning the function pointer in ExecInitExpr.	Be careful that the
 * function signature is declared correctly, because the cast suppresses
 * automatic checking!
 *
 *
 * All these functions share this calling convention:
 *
 * Inputs:
 *		expression: the expression state tree to evaluate
 *		econtext: evaluation context information
 *
 * Outputs:
 *		return value: Datum value of result
 *		*isNull: set to TRUE if result is NULL (actual return value is
 *				 meaningless if so); set to FALSE if non-null result
 *		*isDone: set to indicator of set-result status
 *
 * A caller that can only accept a singleton (non-set) result should pass
 * NULL for isDone; if the expression computes a set result then an error
 * will be reported via ereport.  If the caller does pass an isDone pointer
 * then *isDone is set to one of these three states:
 *		ExprSingleResult		singleton result (not a set)
 *		ExprMultipleResult		return value is one element of a set
 *		ExprEndResult			there are no more elements in the set
 * When ExprMultipleResult is returned, the caller should invoke
 * ExecEvalExpr() repeatedly until ExprEndResult is returned.  ExprEndResult
 * is returned after the last real set element.  For convenience isNull will
 * always be set TRUE when ExprEndResult is returned, but this should not be
 * taken as indicating a NULL element of the set.  Note that these return
 * conventions allow us to distinguish among a singleton NULL, a NULL element
 * of a set, and an empty set.
 *
 * The caller should already have switched into the temporary memory
 * context econtext->ecxt_per_tuple_memory.  The convenience entry point
 * ExecEvalExprSwitchContext() is provided for callers who don't prefer to
 * do the switch in an outer loop.	We do not do the switch in these routines
 * because it'd be a waste of cycles during nested expression evaluation.
 * ----------------------------------------------------------------
 */


/*----------
 *	  ExecEvalArrayRef
 *
 *	   This function takes an ArrayRef and returns the extracted Datum
 *	   if it's a simple reference, or the modified array value if it's
 *	   an array assignment (i.e., array element or slice insertion).
 *
 * NOTE: if we get a NULL result from a subscript expression, we return NULL
 * when it's an array reference, or raise an error when it's an assignment.
 *
 * NOTE: we deliberately refrain from applying DatumGetArrayTypeP() here,
 * even though that might seem natural, because this code needs to support
 * both varlena arrays and fixed-length array types.  DatumGetArrayTypeP()
 * only works for the varlena kind.  The routines we call in arrayfuncs.c
 * have to know the difference (that's what they need refattrlength for).
 *----------
 */
static Datum
ExecEvalArrayRef(ArrayRefExprState *astate,
				 ExprContext *econtext,
				 bool *isNull,
				 ExprDoneCond *isDone)
{
	ArrayRef   *arrayRef = (ArrayRef *) astate->xprstate.expr;
	ArrayType  *array_source;
	ArrayType  *resultArray;
	bool		isAssignment = (arrayRef->refassgnexpr != NULL);
	bool		eisnull;
	ListCell   *l;
	int			i = 0,
				j = 0;
	IntArray	upper,
				lower;
	int		   *lIndex;

	array_source = (ArrayType *)
		DatumGetPointer(ExecEvalExpr(astate->refexpr,
									 econtext,
									 isNull,
									 isDone));

	/*
	 * If refexpr yields NULL, and it's a fetch, then result is NULL. In the
	 * assignment case, we'll cons up something below.
	 */
	if (*isNull)
	{
		if (isDone && *isDone == ExprEndResult)
			return 0;	/* end of set result */
		if (!isAssignment)
			return 0;
	}

	foreach(l, astate->refupperindexpr)
	{
		ExprState  *eltstate = (ExprState *) lfirst(l);

		if (i >= MAXDIM)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
							i, MAXDIM)));

		upper.indx[i++] = DatumGetInt32(ExecEvalExpr(eltstate,
													 econtext,
													 &eisnull,
													 NULL));
		/* If any index expr yields NULL, result is NULL or error */
		if (eisnull)
		{
			if (isAssignment)
				ereport(ERROR,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				  errmsg("array subscript in assignment must not be null")));
			*isNull = true;
			return 0;
		}
	}

	if (astate->reflowerindexpr != NIL)
	{
		foreach(l, astate->reflowerindexpr)
		{
			ExprState  *eltstate = (ExprState *) lfirst(l);

			if (j >= MAXDIM)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
								i, MAXDIM)));

			lower.indx[j++] = DatumGetInt32(ExecEvalExpr(eltstate,
														 econtext,
														 &eisnull,
														 NULL));
			/* If any index expr yields NULL, result is NULL or error */
			if (eisnull)
			{
				if (isAssignment)
					ereport(ERROR,
							(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							 errmsg("array subscript in assignment must not be null")));
				*isNull = true;
				return 0;
			}
		}
		/* this can't happen unless parser messed up */
		if (i != j)
			elog(ERROR, "upper and lower index lists are not same length");
		lIndex = lower.indx;
	}
	else
		lIndex = NULL;

	if (isAssignment)
	{
		Datum		sourceData;

		/*
		 * Evaluate the value to be assigned into the array.
		 *
		 * XXX At some point we'll need to look into making the old value of
		 * the array element available via CaseTestExpr, as is done by
		 * ExecEvalFieldStore.	This is not needed now but will be needed to
		 * support arrays of composite types; in an assignment to a field of
		 * an array member, the parser would generate a FieldStore that
		 * expects to fetch its input tuple via CaseTestExpr.
		 */
		sourceData = ExecEvalExpr(astate->refassgnexpr,
								  econtext,
								  &eisnull,
								  NULL);

		/*
		 * For an assignment to a fixed-length array type, both the original
		 * array and the value to be assigned into it must be non-NULL, else
		 * we punt and return the original array.
		 */
		if (astate->refattrlength > 0)	/* fixed-length array? */
			if (eisnull || *isNull)
				return PointerGetDatum(array_source);

		/*
		 * For assignment to varlena arrays, we handle a NULL original array
		 * by substituting an empty (zero-dimensional) array; insertion of the
		 * new element will result in a singleton array value.	It does not
		 * matter whether the new element is NULL.
		 */
		if (*isNull)
		{
			array_source = construct_empty_array(arrayRef->refelemtype);
			*isNull = false;
		}

		if (lIndex == NULL)
			resultArray = array_set(array_source, i,
									upper.indx,
									sourceData,
									eisnull,
									astate->refattrlength,
									astate->refelemlength,
									astate->refelembyval,
									astate->refelemalign);
		else
			resultArray = array_set_slice(array_source, i,
										  upper.indx, lower.indx,
								   (ArrayType *) DatumGetPointer(sourceData),
										  eisnull,
										  astate->refattrlength,
										  astate->refelemlength,
										  astate->refelembyval,
										  astate->refelemalign);
		return PointerGetDatum(resultArray);
	}

	if (lIndex == NULL)
		return array_ref(array_source, i, upper.indx,
						 astate->refattrlength,
						 astate->refelemlength,
						 astate->refelembyval,
						 astate->refelemalign,
						 isNull);
	else
	{
		resultArray = array_get_slice(array_source, i,
									  upper.indx, lower.indx,
									  astate->refattrlength,
									  astate->refelemlength,
									  astate->refelembyval,
									  astate->refelemalign);
		return PointerGetDatum(resultArray);
	}
}


/* ----------------------------------------------------------------
 *		ExecEvalAggref
 *
 *		Returns a Datum whose value is the value of the precomputed
 *		aggregate found in the given expression context.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalAggref(AggrefExprState *aggref, ExprContext *econtext,
			   bool *isNull, ExprDoneCond *isDone)
{
	if (isDone)
		*isDone = ExprSingleResult;

	if (econtext->ecxt_aggvalues == NULL)		/* safety check */
		elog(ERROR, "no aggregates in this expression context");

	*isNull = econtext->ecxt_aggnulls[aggref->aggno];
	return econtext->ecxt_aggvalues[aggref->aggno];
}

/*----------------------------------------------------------------
 *		ExecEvalGroupingFunc
 *
 *		Returns a Datum whose value is the value of a GROUPING_ID
 *		with respect to the given context.
 */
static Datum 
ExecEvalGroupingFunc(GroupingFuncExprState *gstate,
					 ExprContext *econtext,
					 bool *isNull, ExprDoneCond *isDone)
{
	uint64 grpid = 0;
	ListCell *tmp;
	int num_args = list_length(gstate->args);
	int argno = 0;

	if (isDone)
		*isDone = ExprSingleResult;

	foreach(tmp, gstate->args)
	{
		int arg = (int)intVal(lfirst(tmp));
		int pos_in_grpcols = gstate->ngrpcols - arg - 1;
		int pos_in_grpingfunc = num_args - argno - 1;

		Assert(pos_in_grpcols >= 0 && pos_in_grpingfunc >= 0);

		if (econtext->grouping & ( ((uint64)1) << pos_in_grpcols))
			grpid |= ( ((uint64)1) << pos_in_grpingfunc);

		argno++;
	}

	*isNull = false;
	return Int64GetDatum(grpid);
}

/*----------------------------------------------------------------
 *		ExecEvalGrouping
 *
 *		Returns a Datum whose value is the value of a GROUPING
 *		with respect to the given context.
 */
static Datum
ExecEvalGrouping(ExprState *gstate, ExprContext *econtext,
				 bool *isNull, ExprDoneCond *isDone)
{
	if (isDone)
		*isDone = ExprSingleResult;

	*isNull = false;
	return Int64GetDatum(econtext->grouping);
}

/*----------------------------------------------------------------
 *		ExecEvalGroupId
 *
 *		Returns a Datum whose value is the value of a GROUP_ID
 *		with respect to the given context.
 */
static Datum
ExecEvalGroupId(ExprState *gstate, ExprContext *econtext,
				bool *isNull, ExprDoneCond *isDone)
{
	if (isDone)
		*isDone = ExprSingleResult;

	*isNull = false;
	return UInt32GetDatum(econtext->group_id);
}

/* ----------------------------------------------------------------
 *		ExecEvalWindowRef
 *
 *		Returns a Datum whose value is the value of the window
 *		function with respect to the given context.
 *
 * XXX	Note that this routine is essentially the same as
 *      ExecEvalAggref since we use the same buffers. However,
 *      since the state structures for WindowRef and Aggref 
 *      are different, we separate the execution routines, too.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalWindowRef(WindowRefExprState *winref, ExprContext *econtext,
				  bool *isNull, ExprDoneCond *isDone)
{
	if (isDone)
		*isDone = ExprSingleResult;
	
	if (econtext->ecxt_aggvalues == NULL)		/* safety check */
		elog(ERROR, "no window functions in this expression context");

	*isNull = econtext->ecxt_aggnulls[winref->funcno];
	return econtext->ecxt_aggvalues[winref->funcno];

}

/* ----------------------------------------------------------------
 *		ExecEvalVar
 *
 *		Returns a Datum whose value is the value of a range
 *		variable with respect to given expression context.
 *
 * Note: ExecEvalVar is executed only the first time through in a given plan;
 * it changes the ExprState's function pointer to pass control directly to
 * ExecEvalScalarVar, ExecEvalWholeRowVar, or ExecEvalWholeRowSlow after
 * making one-time checks.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalVar(ExprState *exprstate, ExprContext *econtext,
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

	if (attnum != InvalidAttrNumber)
	{
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
				if (variable->vartype != attr->atttypid)
					ereport(ERROR,
							(errmsg("attribute %d has wrong type", attnum),
							 errdetail("Table has type %s, but query expects %s.",
									   format_type_be(attr->atttypid),
									   format_type_be(variable->vartype))));
			}
		}

		/* Skip the checking on future executions of node */
		exprstate->evalfunc = ExecEvalScalarVar;

		/* Fetch the value from the slot */
		return slot_getattr(slot, attnum, isNull);
	}
	else
	{
		/*
		 * Whole-row variable.
		 *
		 * If it's a RECORD Var, we'll use the slot's type ID info.  It's
		 * likely that the slot's type is also RECORD; if so, make sure it's
		 * been "blessed", so that the Datum can be interpreted later.
		 *
		 * If the Var identifies a named composite type, we must check that
		 * the actual tuple type is compatible with it.
		 */
		TupleDesc	slot_tupdesc = slot->tts_tupleDescriptor;
		bool		needslow = false;

		if (variable->vartype == RECORDOID)
		{
			if (slot_tupdesc->tdtypeid == RECORDOID &&
				slot_tupdesc->tdtypmod < 0)
				assign_record_type_typmod(slot_tupdesc);
		}
		else
		{
			TupleDesc	var_tupdesc;
			int			i;

			/*
			 * We really only care about number of attributes and data type.
			 * Also, we can ignore type mismatch on columns that are dropped
			 * in the destination type, so long as the physical storage
			 * matches.  This is helpful in some cases involving out-of-date
			 * cached plans.  Also, we have to allow the case that the slot
			 * has more columns than the Var's type, because we might be
			 * looking at the output of a subplan that includes resjunk
			 * columns.  (XXX it would be nice to verify that the extra
			 * columns are all marked resjunk, but we haven't got access to
			 * the subplan targetlist here...)  Resjunk columns should always
			 * be at the end of a targetlist, so it's sufficient to ignore
			 * them here; but we need to use ExecEvalWholeRowSlow to get
			 * rid of them in the eventual output tuples.
			 */
			var_tupdesc = lookup_rowtype_tupdesc(variable->vartype, -1);

			if (var_tupdesc->natts > slot_tupdesc->natts)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Table row contains %d attributes, but query expects %d.",
								   slot_tupdesc->natts, var_tupdesc->natts)));
			else if (var_tupdesc->natts < slot_tupdesc->natts)
				needslow = true;

			for (i = 0; i < var_tupdesc->natts; i++)
			{
				Form_pg_attribute vattr = var_tupdesc->attrs[i];
				Form_pg_attribute sattr = slot_tupdesc->attrs[i];

				if (vattr->atttypid == sattr->atttypid)
					continue;			/* no worries */
				if (!vattr->attisdropped)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("table row type and query-specified row type do not match"),
							 errdetail("Table has type %s at ordinal position %d, but query expects %s.",
									   format_type_be(sattr->atttypid),
									   i + 1,
									   format_type_be(vattr->atttypid))));

				if (vattr->attlen != sattr->attlen ||
					vattr->attalign != sattr->attalign)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("table row type and query-specified row type do not match"),
							 errdetail("Physical storage mismatch on dropped attribute at ordinal position %d.",
									   i + 1)));
			}

			ReleaseTupleDesc(var_tupdesc);
		}

		/* Skip the checking on future executions of node */
		if (needslow)
			exprstate->evalfunc = ExecEvalWholeRowSlow;
		else
			exprstate->evalfunc = ExecEvalWholeRowVar;

		/* Fetch the value */
		return ExecEvalWholeRowVar(exprstate, econtext, isNull, isDone);
	}
}

/* ----------------------------------------------------------------
 *		ExecEvalScalarVar
 *
 *		Returns a Datum for a scalar variable.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalScalarVar(ExprState *exprstate, ExprContext *econtext,
				  bool *isNull, ExprDoneCond *isDone)
{
	Var		   *variable = (Var *) exprstate->expr;
	TupleTableSlot *slot;
	AttrNumber	attnum;

	if (isDone)
		*isDone = ExprSingleResult;

	/* Get the input slot and attribute number we want */
	switch (variable->varno)
	{
		case INNER:				/* get the tuple from the inner node */
			slot = econtext->ecxt_innertuple;
			break;

		case OUTER:				/* get the tuple from the outer node */
			slot = econtext->ecxt_outertuple;
			break;

		default:				/* get the tuple from the relation being
								 * scanned */
			slot = econtext->ecxt_scantuple;
			break;
	}

	attnum = variable->varattno;

	/* Fetch the value from the slot */
	return slot_getattr(slot, attnum, isNull);
}

/* ----------------------------------------------------------------
 *		ExecEvalWholeRowVar
 *
 *		Returns a Datum for a whole-row variable.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalWholeRowVar(ExprState *exprstate, ExprContext *econtext,
					bool *isNull, ExprDoneCond *isDone)
{
	Var		   *variable = (Var *) exprstate->expr;
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	HeapTuple	tuple;
	TupleDesc	tupleDesc;
	HeapTupleHeader dtuple;

	if (isDone)
		*isDone = ExprSingleResult;
	*isNull = false;

	tuple = ExecFetchSlotHeapTuple(slot);
	tupleDesc = slot->tts_tupleDescriptor;

	/*
	 * We have to make a copy of the tuple so we can safely insert the Datum
	 * overhead fields, which are not set in on-disk tuples.
	 */
	dtuple = (HeapTupleHeader) palloc(tuple->t_len);
	memcpy((char *) dtuple, (char *) tuple->t_data, tuple->t_len);

	HeapTupleHeaderSetDatumLength(dtuple, tuple->t_len);

	/*
	 * If the Var identifies a named composite type, label the tuple with that
	 * type; otherwise use what is in the tupleDesc.
	 */
	if (variable->vartype != RECORDOID)
	{
		HeapTupleHeaderSetTypeId(dtuple, variable->vartype);
		HeapTupleHeaderSetTypMod(dtuple, variable->vartypmod);
	}
	else
	{
		HeapTupleHeaderSetTypeId(dtuple, tupleDesc->tdtypeid);
		HeapTupleHeaderSetTypMod(dtuple, tupleDesc->tdtypmod);
	}

	return PointerGetDatum(dtuple);
}

/* ----------------------------------------------------------------
 *		ExecEvalWholeRowSlow
 *
 *		Returns a Datum for a whole-row variable, in the "slow" case where
 *		we can't just copy the subplan's output.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalWholeRowSlow(ExprState *exprstate, ExprContext *econtext,
					 bool *isNull, ExprDoneCond *isDone)
{
	Var		   *variable = (Var *) exprstate->expr;
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	HeapTuple	tuple;
	TupleDesc	var_tupdesc;
	HeapTupleHeader dtuple;

	if (isDone)
		*isDone = ExprSingleResult;
	*isNull = false;

	/*
	 * Currently, the only case handled here is stripping of trailing
	 * resjunk fields, which we do in a slightly chintzy way by just
	 * adjusting the tuple's natts header field.  Possibly there will someday
	 * be a need for more-extensive rearrangements, in which case it'd
	 * be worth disassembling and reassembling the tuple (perhaps use a
	 * JunkFilter for that?)
	 */
	Assert(variable->vartype != RECORDOID);
	var_tupdesc = lookup_rowtype_tupdesc(variable->vartype, -1);

	tuple = ExecFetchSlotHeapTuple(slot);

	/*
	 * We have to make a copy of the tuple so we can safely insert the Datum
	 * overhead fields, which are not set in on-disk tuples; not to mention
	 * fooling with its natts field.
	 */
	dtuple = (HeapTupleHeader) palloc(tuple->t_len);
	memcpy((char *) dtuple, (char *) tuple->t_data, tuple->t_len);

	HeapTupleHeaderSetDatumLength(dtuple, tuple->t_len);
	HeapTupleHeaderSetTypeId(dtuple, variable->vartype);
	HeapTupleHeaderSetTypMod(dtuple, variable->vartypmod);

	Assert(HeapTupleHeaderGetNatts(dtuple) >= var_tupdesc->natts);
	HeapTupleHeaderSetNatts(dtuple, var_tupdesc->natts);
	
	ReleaseTupleDesc(var_tupdesc);

	return PointerGetDatum(dtuple);
}

/* ----------------------------------------------------------------
 *		ExecEvalConst
 *
 *		Returns the value of a constant.
 *
 *		Note that for pass-by-ref datatypes, we return a pointer to the
 *		actual constant node.  This is one of the reasons why functions
 *		must treat their input arguments as read-only.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalConst(ExprState *exprstate, ExprContext *econtext,
			  bool *isNull, ExprDoneCond *isDone)
{
	Const	   *con = (Const *) exprstate->expr;

	if (isDone)
		*isDone = ExprSingleResult;

	*isNull = con->constisnull;
	return con->constvalue;
}

/* ----------------------------------------------------------------
 *		ExecEvalParam
 *
 *		Returns the value of a parameter.  A param node contains
 *		something like ($.name) and the expression context contains
 *		the current parameter bindings (name = "sam") (age = 34)...
 *		so our job is to find and return the appropriate datum ("sam").
 * ----------------------------------------------------------------
 */

/*
 * Greenplum Database Changes:
 * In executor mode, a PARAM_EXEC parameter can not be evaluated by executing
 * the subplan.  The subplan was executed on the dispatcher prior to
 * launching the main query.  The value of the result is passed to the qExec
 * in the ParamInfo, with a kind of PARAM_EXEC_REMOTE.
 * So, this function was changed to just do a lookup in that case.
 */
static Datum
ExecEvalParam(ExprState *exprstate, ExprContext *econtext,
			  bool *isNull, ExprDoneCond *isDone)
{
	Param	   *expression = (Param *) exprstate->expr;
	int			thisParamId = expression->paramid;

	if (isDone)
		*isDone = ExprSingleResult;

	if (expression->paramkind == PARAM_EXEC)
	{
		/*
		 * PARAM_EXEC params (internal executor parameters) are stored in the
		 * ecxt_param_exec_vals array, and can be accessed by array index.
		 */
		ParamExecData *prmExec = &(econtext->ecxt_param_exec_vals[thisParamId]);
		
		/* 
		 * Maybe this parameter has already been evaluated. If so, execPlan
		 * would be NULL.
		 */
		if (prmExec->execPlan != NULL)
		{
			/* Parameter not evaluated yet, so go do it */
			ExecSetParamPlan(prmExec->execPlan, econtext, NULL);
			/* ExecSetParamPlan should have processed this param... */
			Assert(prmExec->execPlan == NULL);
		}
		
		*isNull = prmExec->isnull;
		return prmExec->value;
	}
	else
	{
		/*
		 * PARAM_EXTERN parameters must be sought in ecxt_param_list_info.
		 */
		ParamListInfo paramInfo = econtext->ecxt_param_list_info;

		Assert(expression->paramkind == PARAM_EXTERN);
		if (paramInfo &&
			thisParamId > 0 && thisParamId <= paramInfo->numParams)
		{
			ParamExternData *prm = &paramInfo->params[thisParamId - 1];

			if (OidIsValid(prm->ptype))
			{
				Assert(prm->ptype == expression->paramtype);
				*isNull = prm->isnull;
				return prm->value;
			}
		}
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("no value found for parameter %d", thisParamId)));
		return (Datum) 0;		/* keep compiler quiet */
	}
}


/* ----------------------------------------------------------------
 *		ExecEvalOper / ExecEvalFunc support routines
 * ----------------------------------------------------------------
 */

/*
 *		GetAttributeByName
 *		GetAttributeByNum
 *
 *		These functions return the value of the requested attribute
 *		out of the given tuple Datum.
 *		C functions which take a tuple as an argument are expected
 *		to use these.  Ex: overpaid(EMP) might call GetAttributeByNum().
 *		Note: these are actually rather slow because they do a typcache
 *		lookup on each call.
 */
Datum
GetAttributeByNum(HeapTupleHeader tuple,
				  AttrNumber attrno,
				  bool *isNull)
{
	Datum		result;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupDesc;
	HeapTupleData tmptup;

	if (!AttributeNumberIsValid(attrno))
		elog(ERROR, "invalid attribute number %d", attrno);

	if (isNull == NULL)
		elog(ERROR, "a NULL isNull pointer was passed");

	if (tuple == NULL)
	{
		/* Kinda bogus but compatible with old behavior... */
		*isNull = true;
		return (Datum) 0;
	}

	tupType = HeapTupleHeaderGetTypeId(tuple);
	tupTypmod = HeapTupleHeaderGetTypMod(tuple);
	tupDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	/*
	 * heap_getattr needs a HeapTuple not a bare HeapTupleHeader.  We set all
	 * the fields in the struct just in case user tries to inspect system
	 * columns.
	 */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
	ItemPointerSetInvalid(&(tmptup.t_self));
	tmptup.t_data = tuple;

	result = heap_getattr(&tmptup,
						  attrno,
						  tupDesc,
						  isNull);

	ReleaseTupleDesc(tupDesc);

	return result;
}

Datum
GetAttributeByName(HeapTupleHeader tuple, const char *attname, bool *isNull)
{
	AttrNumber	attrno;
	Datum		result;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupDesc;
	HeapTupleData tmptup;
	int			i;

	if (attname == NULL)
		elog(ERROR, "invalid attribute name");

	if (isNull == NULL)
		elog(ERROR, "a NULL isNull pointer was passed");

	if (tuple == NULL)
	{
		/* Kinda bogus but compatible with old behavior... */
		*isNull = true;
		return (Datum) 0;
	}

	tupType = HeapTupleHeaderGetTypeId(tuple);
	tupTypmod = HeapTupleHeaderGetTypMod(tuple);
	tupDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	attrno = InvalidAttrNumber;
	for (i = 0; i < tupDesc->natts; i++)
	{
		if (namestrcmp(&(tupDesc->attrs[i]->attname), attname) == 0)
		{
			attrno = tupDesc->attrs[i]->attnum;
			break;
		}
	}

	if (attrno == InvalidAttrNumber)
		elog(ERROR, "attribute \"%s\" does not exist", attname);

	/*
	 * heap_getattr needs a HeapTuple not a bare HeapTupleHeader.  We set all
	 * the fields in the struct just in case user tries to inspect system
	 * columns.
	 */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
	ItemPointerSetInvalid(&(tmptup.t_self));
	tmptup.t_data = tuple;

	result = heap_getattr(&tmptup,
						  attrno,
						  tupDesc,
						  isNull);

	ReleaseTupleDesc(tupDesc);

	return result;
}

/*
 * init_fcache - initialize a FuncExprState node during first use
 */
void
init_fcache(Oid foid, FuncExprState *fcache,
			MemoryContext fcacheCxt, bool needDescForSets)
{
	AclResult	aclresult;

	/* Check permission to call function */
	aclresult = pg_proc_aclcheck(foid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(foid));

	/*
	 * Safety check on nargs.  Under normal circumstances this should never
	 * fail, as parser should check sooner.  But possibly it might fail if
	 * server has been compiled with FUNC_MAX_ARGS smaller than some functions
	 * declared in pg_proc?
	 */
	if (list_length(fcache->args) > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg("cannot pass more than %d arguments to a function",
						FUNC_MAX_ARGS)));

	/* Set up the primary fmgr lookup information */
	fmgr_info_cxt(foid, &(fcache->func), fcacheCxt);
	fcache->func.fn_expr = (Node *) fcache->xprstate.expr;

	/* If function returns set, prepare expected tuple descriptor */
	if (fcache->func.fn_retset && needDescForSets)
	{
		TypeFuncClass functypclass;
		Oid			funcrettype;
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		functypclass = get_expr_result_type(fcache->func.fn_expr,
											&funcrettype,
											&tupdesc);

		/* Must save tupdesc in fcache's context */
		oldcontext = MemoryContextSwitchTo(fcacheCxt);

		if (functypclass == TYPEFUNC_COMPOSITE)
		{
			/* Composite data type, e.g. a table's row type */
			Assert(tupdesc);
			/* Must copy it out of typcache for safety */
			fcache->funcResultDesc = CreateTupleDescCopy(tupdesc);
			fcache->funcReturnsTuple = true;
		}
		else if (functypclass == TYPEFUNC_SCALAR)
		{
			/* Base data type, i.e. scalar */
			tupdesc = CreateTemplateTupleDesc(1, false);
			TupleDescInitEntry(tupdesc,
							   (AttrNumber) 1,
							   NULL,
							   funcrettype,
							   -1,
							   0);
			fcache->funcResultDesc = tupdesc;
			fcache->funcReturnsTuple = false;
		}
		else
		{
			/* Else, we will complain if function wants materialize mode */
			fcache->funcResultDesc = NULL;
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		fcache->funcResultDesc = NULL;
	}

	/* Initialize additional state */
	fcache->funcResultStore = NULL;
	fcache->funcResultSlot = NULL;
	fcache->setArgsValid = false;
	fcache->shutdown_reg = false;
}

/*
 * callback function in case a FuncExpr returning a set needs to be shut down
 * before it has been run to completion
 */
static void
ShutdownFuncExpr(Datum arg)
{
	FuncExprState *fcache = (FuncExprState *) DatumGetPointer(arg);

	/* If we have a slot, make sure it's let go of any tuplestore pointer */
	if (fcache->funcResultSlot)
		ExecClearTuple(fcache->funcResultSlot);

	/* Release any open tuplestore */
	if (fcache->funcResultStore)
		tuplestore_end(fcache->funcResultStore);
	fcache->funcResultStore = NULL;

	/* Clear any active set-argument state */
	fcache->setArgsValid = false;

	/* execUtils will deregister the callback... */
	fcache->shutdown_reg = false;
}

/*
 * get_cached_rowtype: utility function to lookup a rowtype tupdesc
 *
 * type_id, typmod: identity of the rowtype
 * cache_field: where to cache the TupleDesc pointer in expression state node
 *		(field must be initialized to NULL)
 * econtext: expression context we are executing in
 *
 * NOTE: because the shutdown callback will be called during plan rescan,
 * must be prepared to re-do this during any node execution; cannot call
 * just once during expression initialization
 */
static TupleDesc
get_cached_rowtype(Oid type_id, int32 typmod,
				   TupleDesc *cache_field, ExprContext *econtext)
{
	TupleDesc	tupDesc = *cache_field;

	/* Do lookup if no cached value or if requested type changed */
	if (tupDesc == NULL ||
		type_id != tupDesc->tdtypeid ||
		typmod != tupDesc->tdtypmod)
	{
		tupDesc = lookup_rowtype_tupdesc(type_id, typmod);

		if (*cache_field)
		{
			/* Release old tupdesc; but callback is already registered */
			ReleaseTupleDesc(*cache_field);
		}
		else
		{
			/* Need to register shutdown callback to release tupdesc */
			RegisterExprContextCallback(econtext,
										ShutdownTupleDescRef,
										PointerGetDatum(cache_field));
		}
		*cache_field = tupDesc;
	}
	return tupDesc;
}

/*
 * Callback function to release a tupdesc refcount at expression tree shutdown
 */
static void
ShutdownTupleDescRef(Datum arg)
{
	TupleDesc  *cache_field = (TupleDesc *) DatumGetPointer(arg);

	if (*cache_field)
		ReleaseTupleDesc(*cache_field);
	*cache_field = NULL;
}

/*
 * Evaluate arguments for a function.
 */
ExprDoneCond
ExecEvalFuncArgs(FunctionCallInfo fcinfo,
				 List *argList,
				 ExprContext *econtext)
{
	ExprDoneCond argIsDone;
	int			i;
	ListCell   *arg;

	argIsDone = ExprSingleResult;		/* default assumption */

	i = 0;
	foreach(arg, argList)
	{
		ExprState  *argstate = (ExprState *) lfirst(arg);
		ExprDoneCond thisArgIsDone;

		fcinfo->arg[i] = ExecEvalExpr(argstate,
									  econtext,
									  &fcinfo->argnull[i],
									  &thisArgIsDone);

		if (thisArgIsDone != ExprSingleResult)
		{
			/*
			 * We allow only one argument to have a set value; we'd need much
			 * more complexity to keep track of multiple set arguments (cf.
			 * ExecTargetList) and it doesn't seem worth it.
			 */
			if (argIsDone != ExprSingleResult)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("functions and operators can take at most one set argument")));
			argIsDone = thisArgIsDone;
		}
		i++;
	}

	fcinfo->nargs = i;

	return argIsDone;
}

/*
 *		ExecPrepareTuplestoreResult
 *
 * Subroutine for ExecMakeFunctionResult: prepare to extract rows from a
 * tuplestore function result.  We must set up a funcResultSlot (unless
 * already done in a previous call cycle) and verify that the function
 * returned the expected tuple descriptor.
 */
static void
ExecPrepareTuplestoreResult(FuncExprState *fcache,
							ExprContext *econtext,
							Tuplestorestate *resultStore,
							TupleDesc resultDesc)
{
	fcache->funcResultStore = resultStore;

	if (fcache->funcResultSlot == NULL)
	{
		/* Create a slot so we can read data out of the tuplestore */
		MemoryContext oldcontext;

		/* We must have been able to determine the result rowtype */
		if (fcache->funcResultDesc == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning setof record called in "
							"context that cannot accept type record")));

		oldcontext = MemoryContextSwitchTo(fcache->func.fn_mcxt);
		fcache->funcResultSlot =
			MakeSingleTupleTableSlot(fcache->funcResultDesc);
		MemoryContextSwitchTo(oldcontext);
	}

	/*
	 * If function provided a tupdesc, cross-check it.	We only really
	 * need to do this for functions returning RECORD, but might as well
	 * do it always.
	 */
	if (resultDesc)
	{
		if (fcache->funcResultDesc)
			tupledesc_match(fcache->funcResultDesc, resultDesc);

		/*
		 * If it is a dynamically-allocated TupleDesc, free it: it is
		 * typically allocated in a per-query context, so we must avoid
		 * leaking it across multiple usages.
		 */
		if (resultDesc->tdrefcount == -1)
			FreeTupleDesc(resultDesc);
	}

	/* Register cleanup callback if we didn't already */
	if (!fcache->shutdown_reg)
	{
		RegisterExprContextCallback(econtext,
									ShutdownFuncExpr,
									PointerGetDatum(fcache));
		fcache->shutdown_reg = true;
	}
}

/*
 * Check that function result tuple type (src_tupdesc) matches or can
 * be considered to match what the query expects (dst_tupdesc). If
 * they don't match, ereport.
 *
 * We really only care about number of attributes and data type.
 * Also, we can ignore type mismatch on columns that are dropped in the
 * destination type, so long as the physical storage matches.  This is
 * helpful in some cases involving out-of-date cached plans.
 */
static void
tupledesc_match(TupleDesc dst_tupdesc, TupleDesc src_tupdesc)
{
	int			i;

	if (dst_tupdesc->natts != src_tupdesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("function return row and query-specified return row do not match"),
				 errdetail("Returned row contains %d attributes, but query expects %d.",
						   src_tupdesc->natts, dst_tupdesc->natts)));

	for (i = 0; i < dst_tupdesc->natts; i++)
	{
		Form_pg_attribute dattr = dst_tupdesc->attrs[i];
		Form_pg_attribute sattr = src_tupdesc->attrs[i];

		if (dattr->atttypid == sattr->atttypid)
			continue;			/* no worries */
		if (!dattr->attisdropped)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("function return row and query-specified return row do not match"),
					 errdetail("Returned type %s at ordinal position %d, but query expects %s.",
							   format_type_be(sattr->atttypid),
							   i + 1,
							   format_type_be(dattr->atttypid))));

		if (dattr->attlen != sattr->attlen ||
			dattr->attalign != sattr->attalign)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("function return row and query-specified return row do not match"),
					 errdetail("Physical storage mismatch on dropped attribute at ordinal position %d.",
							   i + 1)));
	}
}

/*
 *		ExecMakeFunctionResult
 *
 * Evaluate the arguments to a function and then the function itself.
 * init_fcache is presumed already run on the FuncExprState.
 *
 * This function handles the most general case, wherein the function or
 * one of its arguments might (or might not) return a set.  If we find
 * no sets involved, we will change the FuncExprState's function pointer
 * to use a simpler method on subsequent calls.
 */
static Datum
ExecMakeFunctionResult(FuncExprState *fcache,
					   ExprContext *econtext,
					   bool *isNull,
					   ExprDoneCond *isDone)
{
	Datum		result;
	FunctionCallInfoData fcinfo;
	ReturnSetInfo rsinfo;		/* for functions returning sets */
	ExprDoneCond argDone;
	bool		hasSetArg;
	int			i;

restart:

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	/*
	 * If a previous call of the function returned a set result in the form
	 * of a tuplestore, continue reading rows from the tuplestore until it's
	 * empty.
	 */
	if (fcache->funcResultStore)
	{
		Assert(isDone);				/* it was provided before ... */
		if (tuplestore_gettupleslot(fcache->funcResultStore, true,
									fcache->funcResultSlot))
		{
			*isDone = ExprMultipleResult;
			if (fcache->funcReturnsTuple)
			{
				/* We must return the whole tuple as a Datum. */
				*isNull = false;
				return ExecFetchSlotTupleDatum(fcache->funcResultSlot);
			}
			else
			{
				/* Extract the first column and return it as a scalar. */
				return slot_getattr(fcache->funcResultSlot, 1, isNull);
			}
		}
		/* Exhausted the tuplestore, so clean up */
		tuplestore_end(fcache->funcResultStore);
		fcache->funcResultStore = NULL;
		/* We are done unless there was a set-valued argument */
		if (!fcache->setHasSetArg)
		{
			*isDone = ExprEndResult;
			*isNull = true;
			return (Datum) 0;
		}
		/* If there was, continue evaluating the argument values */
		Assert(!fcache->setArgsValid);
	}

	/*
	 * arguments is a list of expressions to evaluate before passing to the
	 * function manager.  We skip the evaluation if it was already done in the
	 * previous call (ie, we are continuing the evaluation of a set-valued
	 * function).  Otherwise, collect the current argument values into fcinfo.
	 */
	List	   *arguments = fcache->args;
	if (!fcache->setArgsValid)
	{
		/* Need to prep callinfo structure */
		InitFunctionCallInfoData(fcinfo, &(fcache->func), 0, NULL, NULL);
		argDone = ExecEvalFuncArgs(&fcinfo, arguments, econtext);
		if (argDone == ExprEndResult)
		{
			/* input is an empty set, so return an empty set. */
			*isNull = true;
			if (isDone)
				*isDone = ExprEndResult;
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("set-valued function called in context that cannot accept a set")));
			return (Datum) 0;
		}
		hasSetArg = (argDone != ExprSingleResult);
	}
	else
	{
		/* Copy callinfo from previous evaluation */
		memcpy(&fcinfo, &fcache->setArgs, sizeof(fcinfo));
		hasSetArg = fcache->setHasSetArg;
		/* Reset flag (we may set it again below) */
		fcache->setArgsValid = false;
	}

	/*
	 * Prepare a resultinfo node for communication.  If the function
	 * doesn't itself return set, we don't pass the resultinfo to the
	 * function, but we need to fill it in anyway for internal use.
	 */
	if (fcache->func.fn_retset)
	{
		fcinfo.resultinfo = (Node *) &rsinfo;
	}
	rsinfo.type = T_ReturnSetInfo;
	rsinfo.econtext = econtext;
	rsinfo.expectedDesc = fcache->funcResultDesc;
	rsinfo.allowedModes = (int) (SFRM_ValuePerCall | SFRM_Materialize);
	/* note we do not set SFRM_Materialize_Random or _Preferred */
	rsinfo.returnMode = SFRM_ValuePerCall;
	/* isDone is filled below */
	rsinfo.setResult = NULL;
	rsinfo.setDesc = NULL;

	/*
	 * Now call the function, passing the evaluated parameter values.
	 */
	if (fcache->func.fn_retset || hasSetArg)
	{
		/*
		 * We need to return a set result.	Complain if caller not ready to
		 * accept one.
		 */
		if (isDone == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("set-valued function called in context that cannot accept a set")));

		/*
		 * This loop handles the situation where we have both a set argument
		 * and a set-valued function.  Once we have exhausted the function's
		 * value(s) for a particular argument value, we have to get the next
		 * argument value and start the function over again. We might have to
		 * do it more than once, if the function produces an empty result set
		 * for a particular input value.
		 */
		for (;;)
		{
			/*
			 * If function is strict, and there are any NULL arguments, skip
			 * calling the function (at least for this set of args).
			 */
			bool		callit = true;

			if (fcache->func.fn_strict)
			{
				for (i = 0; i < fcinfo.nargs; i++)
				{
					if (fcinfo.argnull[i])
					{
						callit = false;
						break;
					}
				}
			}

			if (callit)
			{
				fcinfo.isnull = false;
				rsinfo.isDone = ExprSingleResult;
				result = FunctionCallInvoke(&fcinfo);
				*isNull = fcinfo.isnull;
				*isDone = rsinfo.isDone;
			}
			else
			{
				result = (Datum) 0;
				*isNull = true;
				*isDone = ExprEndResult;
			}

			/* Which protocol does function want to use? */
			if (rsinfo.returnMode == SFRM_ValuePerCall)
			{
				if (*isDone != ExprEndResult)
				{
					/*
					 * Got a result from current argument. If function itself
					 * returns set, save the current argument values to re-use
					 * on the next call.
					 */
					if (fcache->func.fn_retset &&
						*isDone == ExprMultipleResult)
					{
						memcpy(&fcache->setArgs, &fcinfo, sizeof(fcinfo));
						fcache->setHasSetArg = hasSetArg;
						fcache->setArgsValid = true;
						/* Register cleanup callback if we didn't already */
						if (!fcache->shutdown_reg)
						{
							RegisterExprContextCallback(econtext,
														ShutdownFuncExpr,
														PointerGetDatum(fcache));
							fcache->shutdown_reg = true;
						}
					}

					/*
					 * Make sure we say we are returning a set, even if the
					 * function itself doesn't return sets.
					 */
					if (hasSetArg)
						*isDone = ExprMultipleResult;
					break;
				}
			}
			else if (rsinfo.returnMode == SFRM_Materialize)
			{
				/* check we're on the same page as the function author */
				if (rsinfo.isDone != ExprSingleResult)
					ereport(ERROR,
							(errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
							 errmsg("table-function protocol for materialize mode was not followed")));
				if (rsinfo.setResult != NULL)
				{
					/* prepare to return values from the tuplestore */
					ExecPrepareTuplestoreResult(fcache, econtext,
												rsinfo.setResult,
												rsinfo.setDesc);
					/* remember whether we had set arguments */
					fcache->setHasSetArg = hasSetArg;
					/* loop back to top to start returning from tuplestore */
					goto restart;
				}
				/* if setResult was left null, treat it as empty set */
				*isDone = ExprEndResult;
				*isNull = true;
				result = (Datum) 0;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
						 errmsg("unrecognized table-function returnMode: %d",
								(int) rsinfo.returnMode)));

			/* Else, done with this argument */
			if (!hasSetArg)
				break;			/* input not a set, so done */

			/* Re-eval args to get the next element of the input set */
			argDone = ExecEvalFuncArgs(&fcinfo, arguments, econtext);

			if (argDone != ExprMultipleResult)
			{
				/* End of argument set, so we're done. */
				*isNull = true;
				*isDone = ExprEndResult;
				result = (Datum) 0;
				break;
			}

			/*
			 * If we reach here, loop around to run the function on the new
			 * argument.
			 */
		}
	}
	else
	{
		/*
		 * Non-set case: much easier.
		 *
		 * We change the ExprState function pointer to use the simpler
		 * ExecMakeFunctionResultNoSets on subsequent calls.  This amounts to
		 * assuming that no argument can return a set if it didn't do so the
		 * first time.
		 */
		fcache->xprstate.evalfunc = (ExprStateEvalFunc) ExecMakeFunctionResultNoSets;

		if (isDone)
			*isDone = ExprSingleResult;

		/*
		 * If function is strict, and there are any NULL arguments, skip
		 * calling the function and return NULL.
		 */
		if (fcache->func.fn_strict)
		{
			for (i = 0; i < fcinfo.nargs; i++)
			{
				if (fcinfo.argnull[i])
				{
					*isNull = true;
					return (Datum) 0;
				}
			}
		}
		fcinfo.isnull = false;
		result = FunctionCallInvoke(&fcinfo);
		*isNull = fcinfo.isnull;
	}

	return result;
}

/*
 *		ExecMakeFunctionResultNoSets
 *
 * Simplified version of ExecMakeFunctionResult that can only handle
 * non-set cases.  Hand-tuned for speed.
 */
static Datum
ExecMakeFunctionResultNoSets(FuncExprState *fcache,
							 ExprContext *econtext,
							 bool *isNull,
							 ExprDoneCond *isDone)
{
	ListCell   *arg;
	Datum		result;
	FunctionCallInfoData fcinfo;
	int			i;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	if (isDone)
		*isDone = ExprSingleResult;

	/* inlined, simplified version of ExecEvalFuncArgs */
	i = 0;
	foreach(arg, fcache->args)
	{
		ExprState  *argstate = (ExprState *) lfirst(arg);

		fcinfo.arg[i] = ExecEvalExpr(argstate,
									 econtext,
									 &fcinfo.argnull[i],
									 NULL);
		i++;
	}

	InitFunctionCallInfoData(fcinfo, &(fcache->func), i, NULL, NULL);

	/*
	 * If function is strict, and there are any NULL arguments, skip calling
	 * the function and return NULL.
	 */
	if (fcache->func.fn_strict)
	{
		while (--i >= 0)
		{
			if (fcinfo.argnull[i])
			{
				*isNull = true;
				return (Datum) 0;
			}
		}
	}
	/* fcinfo.isnull = false; */	/* handled by InitFunctionCallInfoData */
	result = FunctionCallInvoke(&fcinfo);
	*isNull = fcinfo.isnull;

	return result;
}


/*
 *		ExecMakeTableFunctionResult
 *
 * Evaluate a table function, producing a materialized result in a Tuplestore
 * object.
 */
Tuplestorestate *
ExecMakeTableFunctionResult(ExprState *funcexpr,
							ExprContext *econtext,
							TupleDesc expectedDesc,
							uint64 operatorMemKB) 
{
	Tuplestorestate *tupstore = NULL;
	TupleDesc	tupdesc = NULL;
	MemTupleBinding *mt_bind = NULL;
	Oid			funcrettype;
	bool		returnsTuple;
	bool		returnsSet = false;
	FunctionCallInfoData fcinfo;
	ReturnSetInfo rsinfo;
	HeapTupleData tmptup;
	MemoryContext callerContext;
	MemoryContext oldcontext;
	bool		direct_function_call;
	bool		first_time = true;

	callerContext = CurrentMemoryContext;

	funcrettype = exprType((Node *) funcexpr->expr);

	returnsTuple = type_is_rowtype(funcrettype);

	/*
	 * Prepare a resultinfo node for communication.  We always do this even if
	 * not expecting a set result, so that we can pass expectedDesc.  In the
	 * generic-expression case, the expression doesn't actually get to see the
	 * resultinfo, but set it up anyway because we use some of the fields as
	 * our own state variables.
	 */
	InitFunctionCallInfoData(fcinfo, NULL, 0, NULL, (Node *) &rsinfo);
	rsinfo.type = T_ReturnSetInfo;
	rsinfo.econtext = econtext;
	rsinfo.expectedDesc = expectedDesc;
	rsinfo.allowedModes = (int) (SFRM_ValuePerCall | SFRM_Materialize);
	rsinfo.returnMode = SFRM_ValuePerCall;
	/* isDone is filled below */
	rsinfo.setResult = NULL;
	rsinfo.setDesc = NULL;

	/*
	 * Normally the passed expression tree will be a FuncExprState, since the
	 * grammar only allows a function call at the top level of a table
	 * function reference.	However, if the function doesn't return set then
	 * the planner might have replaced the function call via constant-folding
	 * or inlining.  So if we see any other kind of expression node, execute
	 * it via the general ExecEvalExpr() code; the only difference is that we
	 * don't get a chance to pass a special ReturnSetInfo to any functions
	 * buried in the expression.
	 */
	if (funcexpr && IsA(funcexpr, FuncExprState) &&
		IsA(funcexpr->expr, FuncExpr))
	{
		FuncExprState *fcache = (FuncExprState *) funcexpr;
		ExprDoneCond argDone;

		/*
		 * This path is similar to ExecMakeFunctionResult.
		 */
		direct_function_call = true;

		/*
		 * Initialize function cache if first time through
		 */
		if (fcache->func.fn_oid == InvalidOid)
		{
			FuncExpr   *func = (FuncExpr *) fcache->xprstate.expr;

			init_fcache(func->funcid, fcache,
						econtext->ecxt_per_query_memory, false);
		}
		returnsSet = fcache->func.fn_retset;

		/*
		 * Evaluate the function's argument list.
		 *
		 * Note: ideally, we'd do this in the per-tuple context, but then the
		 * argument values would disappear when we reset the context in the
		 * inner loop.	So do it in caller context.  Perhaps we should make a
		 * separate context just to hold the evaluated arguments?
		 */
		fcinfo.flinfo = &(fcache->func);
		argDone = ExecEvalFuncArgs(&fcinfo, fcache->args, econtext);
		/* We don't allow sets in the arguments of the table function */
		if (argDone != ExprSingleResult)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("set-valued function called in context that cannot accept a set")));

		/*
		 * If function is strict, and there are any NULL arguments, skip
		 * calling the function and act like it returned NULL (or an empty
		 * set, in the returns-set case).
		 */
		if (fcache->func.fn_strict)
		{
			int			i;

			for (i = 0; i < fcinfo.nargs; i++)
			{
				if (fcinfo.argnull[i])
					goto no_function_result;
			}
		}
	}
	else
	{
		/* Treat funcexpr as a generic expression */
		direct_function_call = false;
	}

	/*
	 * Switch to short-lived context for calling the function or expression.
	 */
	MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/*
	 * Loop to handle the ValuePerCall protocol (which is also the same
	 * behavior needed in the generic ExecEvalExpr path).
	 */
	for (;;)
	{
		Datum		result;
		MemTuple	tuple;

		CHECK_FOR_INTERRUPTS();

		/*
		 * reset per-tuple memory context before each call of the function or
		 * expression. This cleans up any local memory the function may leak
		 * when called.
		 */
		ResetExprContext(econtext);

		/* Call the function or expression one time */
		if (direct_function_call)
		{
			fcinfo.isnull = false;
			rsinfo.isDone = ExprSingleResult;
			result = FunctionCallInvoke(&fcinfo);
		}
		else
		{
			result = ExecEvalExpr(funcexpr, econtext,
								  &fcinfo.isnull, &rsinfo.isDone);
		}

		/* Which protocol does function want to use? */
		if (rsinfo.returnMode == SFRM_ValuePerCall)
		{
			/*
			 * Check for end of result set.
			 */
			if (rsinfo.isDone == ExprEndResult)
				break;

			/*
			 * Can't do anything very useful with NULL rowtype values. For a
			 * function returning set, we consider this a protocol violation
			 * (but another alternative would be to just ignore the result and
			 * "continue" to get another row).	For a function not returning
			 * set, we fall out of the loop; we'll cons up an all-nulls result
			 * row below.
			 */
			if (returnsTuple && fcinfo.isnull)
			{
				if (!returnsSet)
					break;
				ereport(ERROR,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("function returning set of rows cannot return null value")));
			}

			/*
			 * If first time through, build tupdesc and tuplestore for result
			 */
			if (first_time)
			{
				oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
				if (returnsTuple)
				{
					/*
					 * Use the type info embedded in the rowtype Datum to look
					 * up the needed tupdesc.  Make a copy for the query.
					 */
					HeapTupleHeader td;

					td = DatumGetHeapTupleHeader(result);
					tupdesc = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(td),
											   HeapTupleHeaderGetTypMod(td));
				}
				else
				{
					/*
					 * Scalar type, so make a single-column descriptor
					 */
					tupdesc = CreateTemplateTupleDesc(1, false);
					TupleDescInitEntry(tupdesc,
									   (AttrNumber) 1,
									   "column",
									   funcrettype,
									   -1,
									   0);
				}

				mt_bind = create_memtuple_binding(tupdesc);

				tupstore = tuplestore_begin_heap(true, false, operatorMemKB); 
				MemoryContextSwitchTo(oldcontext);
				rsinfo.setResult = tupstore;
				rsinfo.setDesc = tupdesc;
			}

			/*
			 * Store current resultset item.
			 */
			if (returnsTuple)
			{
			    const int staticBufferLimit = 200;
				HeapTupleHeader td;
				Datum staticPd[staticBufferLimit];
				bool staticNull[staticBufferLimit];
				Datum *pd;
				bool *pn;

                /**
                 * use memory on stack if possible, to save palloc calls
                 */
				if ( tupdesc->natts > staticBufferLimit)
				{
                    pd = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
                    pn = (bool *) palloc(tupdesc->natts * sizeof(bool));
                }
                else
                {
                    pd = staticPd;
                    pn = staticNull;
                }


				td = DatumGetHeapTupleHeader(result);

				/*
				 * tuplestore_puttuple needs a HeapTuple not a bare
				 * HeapTupleHeader, but it doesn't need all the fields.
				 */
				tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
				tmptup.t_data = td;

				heap_deform_tuple(&tmptup, tupdesc, pd, pn);
				tuple = memtuple_form_to(mt_bind, pd, pn, NULL, NULL, false);
			}
			else
			{
				tuple = memtuple_form_to(mt_bind, &result, &fcinfo.isnull, NULL, NULL, false);
			}

			oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
			tuplestore_puttuple(tupstore, (HeapTuple) tuple);
			MemoryContextSwitchTo(oldcontext);

			/*
			 * Are we done?
			 */
			if (rsinfo.isDone != ExprMultipleResult)
				break;
		}
		else if (rsinfo.returnMode == SFRM_Materialize)
		{
			/* check we're on the same page as the function author */
			if (!first_time || rsinfo.isDone != ExprSingleResult)
				ereport(ERROR,
						(errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
						 errmsg("table-function protocol for materialize mode was not followed")));
			/* Done evaluating the set result */
			break;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
					 errmsg("unrecognized table-function returnMode: %d",
							(int) rsinfo.returnMode)));

		first_time = false;
	}

no_function_result:

	/*
	 * If we got nothing from the function (ie, an empty-set or NULL result),
	 * we have to create the tuplestore to return, and if it's a
	 * non-set-returning function then insert a single all-nulls row.
	 */
	if (rsinfo.setResult == NULL)
	{
		MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
		tupstore = tuplestore_begin_heap(true, false, operatorMemKB); 
		rsinfo.setResult = tupstore;  
		if (!returnsSet)
		{
			int			natts = expectedDesc->natts;
			Datum	   *nulldatums;
			bool	   *nullflags;
			HeapTuple	tuple;

			MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
			nulldatums = (Datum *) palloc0(natts * sizeof(Datum));
			nullflags = (bool *) palloc(natts * sizeof(bool));
			MemSetAligned(nullflags, true, natts * sizeof(bool));
			tuple = heap_form_tuple(expectedDesc, nulldatums, nullflags);
			MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
			tuplestore_puttuple(tupstore, tuple);
		}
	}

	/*
	 * If function provided a tupdesc, cross-check it.	We only really
	 * need to do this for functions returning RECORD, but might as well
	 * do it always.
	 */
	if (rsinfo.setDesc)
	{
		tupledesc_match(expectedDesc, rsinfo.setDesc);

		/*
		 * If it is a dynamically-allocated TupleDesc, free it: it is
		 * typically allocated in a per-query context, so we must avoid
		 * leaking it across multiple usages.
		 */
		if (rsinfo.setDesc->tdrefcount == -1)
		{
			FreeTupleDesc(rsinfo.setDesc);
			rsinfo.setDesc = NULL;
		}
	}

	MemoryContextSwitchTo(callerContext);

	/* All done, pass back the tuplestore */
	return rsinfo.setResult;
}


/* ----------------------------------------------------------------
 *		ExecEvalFunc
 *		ExecEvalOper
 *
 *		Evaluate the functional result of a list of arguments by calling the
 *		function manager.
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecEvalFunc
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalFunc(FuncExprState *fcache,
			 ExprContext *econtext,
			 bool *isNull,
			 ExprDoneCond *isDone)
{
	/* This is called only the first time through */
	FuncExpr   *func = (FuncExpr *) fcache->xprstate.expr;

	/* Initialize function lookup info */
	init_fcache(func->funcid, fcache, econtext->ecxt_per_query_memory, true);

	/* Go directly to ExecMakeFunctionResult on subsequent uses */
	fcache->xprstate.evalfunc = (ExprStateEvalFunc) ExecMakeFunctionResult;

	return ExecMakeFunctionResult(fcache, econtext, isNull, isDone);
}

/* ----------------------------------------------------------------
 *		ExecEvalOper
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalOper(FuncExprState *fcache,
			 ExprContext *econtext,
			 bool *isNull,
			 ExprDoneCond *isDone)
{
	/* This is called only the first time through */
	OpExpr	   *op = (OpExpr *) fcache->xprstate.expr;

	/* Initialize function lookup info */
	init_fcache(op->opfuncid, fcache, econtext->ecxt_per_query_memory, true);

	/* Go directly to ExecMakeFunctionResult on subsequent uses */
	fcache->xprstate.evalfunc = (ExprStateEvalFunc) ExecMakeFunctionResult;

	return ExecMakeFunctionResult(fcache, econtext, isNull, isDone);
}

/* ----------------------------------------------------------------
 *		ExecEvalDistinct
 *
 * IS DISTINCT FROM must evaluate arguments to determine whether
 * they are NULL; if either is NULL then the result is already
 * known. If neither is NULL, then proceed to evaluate the
 * function. Note that this is *always* derived from the equals
 * operator, but since we need special processing of the arguments
 * we can not simply reuse ExecEvalOper() or ExecEvalFunc().
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalDistinct(FuncExprState *fcache,
				 ExprContext *econtext,
				 bool *isNull,
				 ExprDoneCond *isDone)
{
	Datum		result;
	FunctionCallInfoData fcinfo;
	ExprDoneCond argDone;
	List	   *argList;

	/* Set default values for result flags: non-null, not a set result */
	*isNull = false;
	if (isDone)
		*isDone = ExprSingleResult;

	/*
	 * Initialize function cache if first time through
	 */
	if (fcache->func.fn_oid == InvalidOid)
	{
		DistinctExpr *op = (DistinctExpr *) fcache->xprstate.expr;

		init_fcache(op->opfuncid, fcache,
					econtext->ecxt_per_query_memory, true);
		Assert(!fcache->func.fn_retset);
	}

	/*
	 * extract info from fcache
	 */
	argList = fcache->args;

	/* Need to prep callinfo structure */
	InitFunctionCallInfoData(fcinfo, &(fcache->func), 0, NULL, NULL);
	argDone = ExecEvalFuncArgs(&fcinfo, argList, econtext);
	if (argDone != ExprSingleResult)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("IS DISTINCT FROM does not support set arguments")));
	Assert(fcinfo.nargs == 2);

	if (fcinfo.argnull[0] && fcinfo.argnull[1])
	{
		/* Both NULL? Then is not distinct... */
		result = BoolGetDatum(FALSE);
	}
	else if (fcinfo.argnull[0] || fcinfo.argnull[1])
	{
		/* Only one is NULL? Then is distinct... */
		result = BoolGetDatum(TRUE);
	}
	else
	{
		fcinfo.isnull = false;
		result = FunctionCallInvoke(&fcinfo);
		*isNull = fcinfo.isnull;
		/* Must invert result of "=" */
		result = BoolGetDatum(!DatumGetBool(result));
	}

	return result;
}

static inline void ExecEvalFPStrict2Arg(FuncExprState *expr, ExprContext *econtext, bool *isNull, ExprDoneCond *isDone)
{ 
	ExprDoneCond argDone[2];

	Assert(expr->fp_arg[0] && expr->fp_arg[1]);
	if(isDone)
		*isDone = ExprSingleResult;
	
	expr->fp_datum[0] = ExecEvalExpr(expr->fp_arg[0], econtext, &expr->fp_null[0], &argDone[0]);
	expr->fp_datum[1] = ExecEvalExpr(expr->fp_arg[1], econtext, &expr->fp_null[1], &argDone[1]);

	*isNull = expr->fp_null[0] || expr->fp_null[1];
}

static Datum ExecEvalFPStrict2_Int2Eq(FuncExprState *fstate, ExprContext *ctxt, bool *isNull, ExprDoneCond *isDone)
{
	ExecEvalFPStrict2Arg(fstate, ctxt, isNull, isDone);
	return BoolGetDatum(
			DatumGetInt16(fstate->fp_datum[0]) == DatumGetInt16(fstate->fp_datum[1])
			);
}

static Datum ExecEvalFPStrict2_Int4Eq(FuncExprState *fstate, ExprContext *ctxt, bool *isNull, ExprDoneCond *isDone)
{

	ExecEvalFPStrict2Arg(fstate, ctxt, isNull, isDone);
	return BoolGetDatum(
			DatumGetInt32(fstate->fp_datum[0]) == DatumGetInt32(fstate->fp_datum[1])
			);
}

static Datum ExecEvalFPStrict2_Int8Eq(FuncExprState *fstate, ExprContext *ctxt, bool *isNull, ExprDoneCond *isDone)
{
	ExecEvalFPStrict2Arg(fstate, ctxt, isNull, isDone);
	return BoolGetDatum(fstate->fp_datum[0] == fstate->fp_datum[1]);
}

/* Some Oids that we want to fast path.  See pg_proc.h */
#define INT2EQ_OID 63
#define INT4EQ_OID 65
#define TEXTEQ_OID 67
#define INT8EQ_OID 467
#define BPCHAREQ_OID 1048
#define DATE_EQ_OID 1086

/* Optimize x op y if op has no side effect.  Almost all our functions are
 * strict, 2 args.  
 * 
 * NOTE: You need to implement the ExecEvalFPStrict2_FUNC FAITHFULLY.  
 * For example, before you fast path int4add, make sure your implementation 
 * is the same as the old int4add, that is, you need to handle under/over flow etc.
 */
static void FastPathStrict2Func(Oid funcoid, FuncExprState *fstate)
{
	static Oid strict2oid[] = {
		INT2EQ_OID,
		INT4EQ_OID,
		DATE_EQ_OID,
		INT8EQ_OID,
	};
	static ExprStateEvalFunc strict2func[] = {
		(ExprStateEvalFunc) ExecEvalFPStrict2_Int2Eq,
		(ExprStateEvalFunc) ExecEvalFPStrict2_Int4Eq,
		(ExprStateEvalFunc) ExecEvalFPStrict2_Int4Eq, /* date_eq is int4 eq */
		(ExprStateEvalFunc) ExecEvalFPStrict2_Int8Eq,
	};

	int i;

	COMPILE_ASSERT(ARRAY_SIZE(strict2oid) == ARRAY_SIZE(strict2func));

	for(i=0; i<ARRAY_SIZE(strict2oid); ++i)
	{
		if (strict2oid[i] == funcoid)
		{
			fstate->xprstate.evalfunc = strict2func[i];
			fstate->fp_arg[0] = linitial(fstate->args);
			fstate->fp_arg[1] = lsecond(fstate->args);
			return;
		}
	}
}

static Datum
ExecEvalFPScalarArrayInt(ScalarArrayOpExprState *sstate,
					  ExprContext *econtext,
					  bool *isNull, ExprDoneCond *isDone)
{
	ScalarArrayOpExpr *opexpr = (ScalarArrayOpExpr *) sstate->fxprstate.xprstate.expr;
	Oid fnoid = opexpr->opfuncid;
	ExprState *arg = linitial(sstate->fxprstate.args);

	ExprDoneCond argDone;
	Datum d;
	bool isnull;

	int i;

	d = ExecEvalExpr(arg, econtext, &isnull, &argDone);
	Assert(argDone == ExprSingleResult);

	if (isDone)
		*isDone = ExprSingleResult;
		
	if (isnull)
	{
		*isNull = true;
		return 0;
	}

	*isNull = false;
	if (fnoid == INT4EQ_OID || fnoid == DATE_EQ_OID)
		d = Int32GetDatum(DatumGetInt32(d));
	else if (fnoid != INT8EQ_OID)
	{
		Assert(fnoid == INT2EQ_OID);
		d = Int16GetDatum(DatumGetInt16(d));
	}

	for(i=0; i<sstate->fp_n; ++i)
	{
		if(d == sstate->fp_datum[i])
			return BoolGetDatum(true);
	}

	return BoolGetDatum(false);
}

static Datum
ExecEvalFPScalarArrayStr(ScalarArrayOpExprState *sstate,
					  ExprContext *econtext,
					  bool *isNull, ExprDoneCond *isDone)
{
	ScalarArrayOpExpr *opexpr = (ScalarArrayOpExpr *) sstate->fxprstate.xprstate.expr;
	Oid fnoid = opexpr->opfuncid;
	ExprState *arg = linitial(sstate->fxprstate.args);

	ExprDoneCond argDone;
	Datum d;
	bool isnull;
	Datum ret;

	char *p; void *tofree; int len;

	int i;

	d = ExecEvalExpr(arg, econtext, &isnull, &argDone);
	Assert(argDone == ExprSingleResult);

	if (isDone)
		*isDone = ExprSingleResult;
		
	if (isnull)
	{
		*isNull = true;
		return 0;
	}

	*isNull = false;

	ret = BoolGetDatum(false);

	varattrib_untoast_ptr_len(d, &p, &len, &tofree);

	/* bpchareq, rid of trailing white space.  see bpeq and bcTruelen */
	if(fnoid == BPCHAREQ_OID)
	{
		while(len > 0 && p[len-1] == ' ')
			--len;
	}

	for(i=0; i<sstate->fp_n; ++i)
	{
		if(sstate->fp_len[i] != len)
			continue;
		if(memcmp(p, DatumGetPointer(sstate->fp_datum[i]), sstate->fp_len[i]) == 0)
		{
			ret = BoolGetDatum(true);
			break;
		}
	}

	if(tofree)
		pfree(tofree);

	return ret;
}

/* Optimize x in ('A', 'B', 'C') */
static void FastPathScalarArrayOp(ScalarArrayOpExpr *opexpr, ScalarArrayOpExprState *sstate)
{
	ExprState *argstate;
	Const *argconst;
	ArrayType *arr;

	char *s;
	bits8 *bitmap;
	int bitmask;

	int16		typlen;
	bool		typbyval;
	char		typalign;

	Oid fnoid = InvalidOid;

	static int4 optimize_func_oid[] = {
		INT2EQ_OID,
		INT4EQ_OID,
		TEXTEQ_OID,
		INT8EQ_OID,
		BPCHAREQ_OID,
		DATE_EQ_OID,
	};

	int i;
		
	/* IN will be evaluated as OR */
	if (!opexpr->useOr)
		return;

	/* Check if we optimize these types */
	for (i=0; i<ARRAY_SIZE(optimize_func_oid); ++i)
	{
		if (optimize_func_oid[i] == opexpr->opfuncid)
		{
			fnoid = opexpr->opfuncid;
			break;
		}
	}

	if(fnoid == InvalidOid)
		return;

	/* Better to have just two args */
	Assert(list_length(sstate->fxprstate.args) == 2);
	
	/* only if the second args are const */
	argstate = (ExprState *) lsecond(sstate->fxprstate.args);
	if (argstate->evalfunc != ExecEvalConst)
		return;

	argconst = (Const *) argstate->expr;

	/* We do not handle null */
	if (argconst->constisnull)
		return;

	arr = DatumGetArrayTypeP(argconst->constvalue);
	sstate->fp_n = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

	/* We do not handle this case */
	if (sstate->fp_n <= 0)
		return;

	sstate->fp_len = (int *) palloc(sizeof(int) * sstate->fp_n);
	sstate->fp_datum = (Datum *) palloc(sizeof(Datum) * sstate->fp_n);

	/* Type stuff */
	if (sstate->element_type != ARR_ELEMTYPE(arr))
	{
		get_typlenbyvalalign(ARR_ELEMTYPE(arr),
				&sstate->typlen,
				&sstate->typbyval,
				&sstate->typalign);
		sstate->element_type = ARR_ELEMTYPE(arr);
	}
	typlen = sstate->typlen;
	typbyval = sstate->typbyval;
	typalign = sstate->typalign;

	/* Loop over the array elements */
	s = (char *) ARR_DATA_PTR(arr);
	bitmap = ARR_NULLBITMAP(arr);
	bitmask = 1;

	for (i = 0; i < sstate->fp_n; i++)
	{
		Datum elt;
		
		/* Do not deal with null yet */
		if (bitmap && (*bitmap & bitmask) == 0)
			return;
		
		elt = fetch_att(s, typbyval, typlen);
		s = att_addlength(s, typlen, PointerGetDatum(s));
		s = (char *) att_align(s, typalign);

		/* int type */
		if (fnoid == INT2EQ_OID)
			sstate->fp_datum[i] = Int16GetDatum(DatumGetInt16(elt));
		else if (fnoid == INT4EQ_OID || fnoid == DATE_EQ_OID)
			sstate->fp_datum[i] = Int32GetDatum(DatumGetInt32(elt));
		else if (fnoid == INT8EQ_OID)
			sstate->fp_datum[i] = elt;
		else if (fnoid == TEXTEQ_OID || fnoid == BPCHAREQ_OID)
		{
			char *p; void *tofree; int len;
			char *pdest;

			varattrib_untoast_ptr_len(elt, &p, &len, &tofree);

			/* bpchareq, rid of trailing white space.  see bpeq and bcTruelen */
			if(fnoid == BPCHAREQ_OID)
			{
				while(len > 0 && p[len-1] == ' ')
					--len;
			}

			sstate->fp_len[i] = len;
			pdest = palloc(len);
			sstate->fp_datum[i] = PointerGetDatum(pdest);

			memcpy(pdest, p, len);

			if(tofree)
				pfree(tofree);
		}
		else
			Assert(!"Wrong optimize_funcoid");

		/* advance bitmap pointer if any */
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100 /* 1<<8 */)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}

	/* Now we are sure we can fast path this */
	if (fnoid == INT2EQ_OID || fnoid == INT4EQ_OID || fnoid == INT8EQ_OID || fnoid == DATE_EQ_OID)
		sstate->fxprstate.xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalFPScalarArrayInt;
	else if (fnoid == TEXTEQ_OID || fnoid == BPCHAREQ_OID) 
		sstate->fxprstate.xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalFPScalarArrayStr; 
	else
		Assert(!"Wrong optimize_funcoid");
}
	
/*
 * ExecEvalScalarArrayOp
 *
 * Evaluate "scalar op ANY/ALL (array)".  The operator always yields boolean,
 * and we combine the results across all array elements using OR and AND
 * (for ANY and ALL respectively).	Of course we short-circuit as soon as
 * the result is known.
 */
	static Datum
ExecEvalScalarArrayOp(ScalarArrayOpExprState *sstate,
		ExprContext *econtext,
		bool *isNull, ExprDoneCond *isDone)
{
	ScalarArrayOpExpr *opexpr = (ScalarArrayOpExpr *) sstate->fxprstate.xprstate.expr;
	bool		useOr = opexpr->useOr;
	ArrayType  *arr;
	int			nitems;
	Datum		result;
	bool		resultnull;
	FunctionCallInfoData fcinfo;
	ExprDoneCond argDone;
	int			i;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char	   *s;
	bits8	   *bitmap;
	int			bitmask;

	/* Set default values for result flags: non-null, not a set result */
	*isNull = false;
	if (isDone)
		*isDone = ExprSingleResult;

	/*
	 * Initialize function cache if first time through
	 */
	if (sstate->fxprstate.func.fn_oid == InvalidOid)
	{
		init_fcache(opexpr->opfuncid, &sstate->fxprstate,
			    econtext->ecxt_per_query_memory, true);
		Assert(!sstate->fxprstate.func.fn_retset);
	}

	/* Need to prep callinfo structure */
	InitFunctionCallInfoData(fcinfo, &(sstate->fxprstate.func), 0, NULL, NULL);
	argDone = ExecEvalFuncArgs(&fcinfo, sstate->fxprstate.args, econtext);
	if (argDone != ExprSingleResult)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("op ANY/ALL (array) does not support set arguments")));
	Assert(fcinfo.nargs == 2);

	/*
	 * If the array is NULL then we return NULL --- it's not very meaningful
	 * to do anything else, even if the operator isn't strict.
	 */
	if (fcinfo.argnull[1])
	{
		*isNull = true;
		return (Datum) 0;
	}
	/* Else okay to fetch and detoast the array */
	arr = DatumGetArrayTypeP(fcinfo.arg[1]);

	/*
	 * If the array is empty, we return either FALSE or TRUE per the useOr
	 * flag.  This is correct even if the scalar is NULL; since we would
	 * evaluate the operator zero times, it matters not whether it would want
	 * to return NULL.
	 */
	nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
	if (nitems <= 0)
		return BoolGetDatum(!useOr);

	/*
	 * If the scalar is NULL, and the function is strict, return NULL; no
	 * point in iterating the loop.
	 */
	if (fcinfo.argnull[0] && sstate->fxprstate.func.fn_strict)
	{
		*isNull = true;
		return (Datum) 0;
	}

	/*
	 * We arrange to look up info about the element type only once per series
	 * of calls, assuming the element type doesn't change underneath us.
	 */
	if (sstate->element_type != ARR_ELEMTYPE(arr))
	{
		get_typlenbyvalalign(ARR_ELEMTYPE(arr),
				&sstate->typlen,
				&sstate->typbyval,
				&sstate->typalign);
		sstate->element_type = ARR_ELEMTYPE(arr);
	}
	typlen = sstate->typlen;
	typbyval = sstate->typbyval;
	typalign = sstate->typalign;

	result = BoolGetDatum(!useOr);
	resultnull = false;

	/* Loop over the array elements */
	s = (char *) ARR_DATA_PTR(arr);
	bitmap = ARR_NULLBITMAP(arr);
	bitmask = 1;

	for (i = 0; i < nitems; i++)
	{
		Datum		elt;
		Datum		thisresult;

		/* Get array element, checking for NULL */
		if (bitmap && (*bitmap & bitmask) == 0)
		{
			fcinfo.arg[1] = (Datum) 0;
			fcinfo.argnull[1] = true;
		}
		else
		{
			elt = fetch_att(s, typbyval, typlen);
			s = att_addlength(s, typlen, PointerGetDatum(s));
			s = (char *) att_align(s, typalign);
			fcinfo.arg[1] = elt;
			fcinfo.argnull[1] = false;
		}

		/* Call comparison function */
		if (fcinfo.argnull[1] && sstate->fxprstate.func.fn_strict)
		{
			fcinfo.isnull = true;
			thisresult = (Datum) 0;
		}
		else
		{
			fcinfo.isnull = false;
			thisresult = FunctionCallInvoke(&fcinfo);
		}

		/* Combine results per OR or AND semantics */
		if (fcinfo.isnull)
			resultnull = true;
		else if (useOr)
		{
			if (DatumGetBool(thisresult))
			{
				result = BoolGetDatum(true);
				resultnull = false;
				break;			/* needn't look at any more elements */
			}
		}
		else
		{
			if (!DatumGetBool(thisresult))
			{
				result = BoolGetDatum(false);
				resultnull = false;
				break;			/* needn't look at any more elements */
			}
		}

		/* advance bitmap pointer if any */
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100 /* 1<<8 */)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}

	*isNull = resultnull;
	return result;
}

/* ----------------------------------------------------------------
 *		ExecEvalNot
 *		ExecEvalOr
 *		ExecEvalAnd
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
ExecEvalNot(BoolExprState *notclause, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone)
{
	ExprState  *clause = linitial(notclause->args);
	Datum		expr_value;

	if (isDone)
		*isDone = ExprSingleResult;

	expr_value = ExecEvalExpr(clause, econtext, isNull, NULL);

	/*
	 * if the expression evaluates to null, then we just cascade the null back
	 * to whoever called us.
	 */
	if (*isNull)
		return expr_value;

	/*
	 * evaluation of 'not' is simple.. expr is false, then return 'true' and
	 * vice versa.
	 */
	return BoolGetDatum(!DatumGetBool(expr_value));
}

/* ----------------------------------------------------------------
 *		ExecEvalOr
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalOr(BoolExprState *orExpr, ExprContext *econtext,
		   bool *isNull, ExprDoneCond *isDone)
{
	List	   *clauses = orExpr->args;
	ListCell   *clause;
	bool		AnyNull;

	if (isDone)
		*isDone = ExprSingleResult;

	AnyNull = false;

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

		clause_value = ExecEvalExpr(clausestate, econtext, isNull, NULL);

		/*
		 * if we have a non-null true result, then return it.
		 */
		if (*isNull)
			AnyNull = true;		/* remember we got a null */
		else if (DatumGetBool(clause_value))
			return clause_value;
	}

	/* AnyNull is true if at least one clause evaluated to NULL */
	*isNull = AnyNull;
	return BoolGetDatum(false);
}

/* ----------------------------------------------------------------
 *		ExecEvalAnd
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalAnd(BoolExprState *andExpr, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone)
{
	List	   *clauses = andExpr->args;
	ListCell   *clause;
	bool		AnyNull;

	if (isDone)
		*isDone = ExprSingleResult;

	AnyNull = false;

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

		clause_value = ExecEvalExpr(clausestate, econtext, isNull, NULL);

		/*
		 * if we have a non-null false result, then return it.
		 */
		if (*isNull)
			AnyNull = true;		/* remember we got a null */
		else if (!DatumGetBool(clause_value))
			return clause_value;
	}

	/* AnyNull is true if at least one clause evaluated to NULL */
	*isNull = AnyNull;
	return BoolGetDatum(!AnyNull);
}

/* ----------------------------------------------------------------
 *		ExecEvalConvertRowtype
 *
 *		Evaluate a rowtype coercion operation.	This may require
 *		rearranging field positions.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalConvertRowtype(ConvertRowtypeExprState *cstate,
					   ExprContext *econtext,
					   bool *isNull, ExprDoneCond *isDone)
{
	ConvertRowtypeExpr *convert = (ConvertRowtypeExpr *) cstate->xprstate.expr;
	HeapTuple	result;
	Datum		tupDatum;
	HeapTupleHeader tuple;
	HeapTupleData tmptup;
	AttrNumber *attrMap;
	Datum	   *invalues;
	bool	   *inisnull;
	Datum	   *outvalues;
	bool	   *outisnull;
	int			i;
	int			outnatts;

	tupDatum = ExecEvalExpr(cstate->arg, econtext, isNull, isDone);

	/* this test covers the isDone exception too: */
	if (*isNull)
		return tupDatum;

	tuple = DatumGetHeapTupleHeader(tupDatum);

	/* Lookup tupdescs if first time through or after rescan */
	if (cstate->indesc == NULL)
		get_cached_rowtype(exprType((Node *) convert->arg), -1,
						   &cstate->indesc, econtext);
	if (cstate->outdesc == NULL)
		get_cached_rowtype(convert->resulttype, -1,
						   &cstate->outdesc, econtext);

	Assert(HeapTupleHeaderGetTypeId(tuple) == cstate->indesc->tdtypeid);
	Assert(HeapTupleHeaderGetTypMod(tuple) == cstate->indesc->tdtypmod);

	/* if first time through, initialize */
	if (cstate->attrMap == NULL)
	{
		MemoryContext old_cxt;
		int			n;

		/* allocate state in long-lived memory context */
		old_cxt = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

		/* prepare map from old to new attribute numbers */
		n = cstate->outdesc->natts;
		cstate->attrMap = (AttrNumber *) palloc0(n * sizeof(AttrNumber));
		for (i = 0; i < n; i++)
		{
			Form_pg_attribute att = cstate->outdesc->attrs[i];
			char	   *attname;
			Oid			atttypid;
			int32		atttypmod;
			int			j;

			if (att->attisdropped)
				continue;		/* attrMap[i] is already 0 */
			attname = NameStr(att->attname);
			atttypid = att->atttypid;
			atttypmod = att->atttypmod;
			for (j = 0; j < cstate->indesc->natts; j++)
			{
				att = cstate->indesc->attrs[j];
				if (att->attisdropped)
					continue;
				if (strcmp(attname, NameStr(att->attname)) == 0)
				{
					/* Found it, check type */
					if (atttypid != att->atttypid || atttypmod != att->atttypmod)
						elog(ERROR, "attribute \"%s\" of type %s does not match corresponding attribute of type %s",
							 attname,
							 format_type_be(cstate->indesc->tdtypeid),
							 format_type_be(cstate->outdesc->tdtypeid));
					cstate->attrMap[i] = (AttrNumber) (j + 1);
					break;
				}
			}
			if (cstate->attrMap[i] == 0)
				elog(ERROR, "attribute \"%s\" of type %s does not exist",
					 attname,
					 format_type_be(cstate->indesc->tdtypeid));
		}
		/* preallocate workspace for Datum arrays */
		n = cstate->indesc->natts + 1;	/* +1 for NULL */
		cstate->invalues = (Datum *) palloc(n * sizeof(Datum));
		cstate->inisnull = (bool *) palloc(n * sizeof(bool));
		n = cstate->outdesc->natts;
		cstate->outvalues = (Datum *) palloc(n * sizeof(Datum));
		cstate->outisnull = (bool *) palloc(n * sizeof(bool));

		MemoryContextSwitchTo(old_cxt);
	}

	attrMap = cstate->attrMap;
	invalues = cstate->invalues;
	inisnull = cstate->inisnull;
	outvalues = cstate->outvalues;
	outisnull = cstate->outisnull;
	outnatts = cstate->outdesc->natts;

	/*
	 * heap_deform_tuple needs a HeapTuple not a bare HeapTupleHeader.
	 */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
	tmptup.t_data = tuple;

	/*
	 * Extract all the values of the old tuple, offsetting the arrays so that
	 * invalues[0] is NULL and invalues[1] is the first source attribute; this
	 * exactly matches the numbering convention in attrMap.
	 */
	heap_deform_tuple(&tmptup, cstate->indesc, invalues + 1, inisnull + 1);
	invalues[0] = (Datum) 0;
	inisnull[0] = true;

	/*
	 * Transpose into proper fields of the new tuple.
	 */
	for (i = 0; i < outnatts; i++)
	{
		int			j = attrMap[i];

		outvalues[i] = invalues[j];
		outisnull[i] = inisnull[j];
	}

	/*
	 * Now form the new tuple.
	 */
	result = heap_form_tuple(cstate->outdesc, outvalues, outisnull);

	return HeapTupleGetDatum(result);
}

/* ----------------------------------------------------------------
 *		ExecEvalCase
 *
 *		Evaluate a CASE clause. Will have boolean expressions
 *		inside the WHEN clauses, and will have expressions
 *		for results.
 *		- thomas 1998-11-09
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalCase(CaseExprState *caseExpr, ExprContext *econtext,
			 bool *isNull, ExprDoneCond *isDone)
{
	List	   *clauses = caseExpr->args;
	ListCell   *clause;
	Datum		save_datum;
	bool		save_isNull;

	if (isDone)
		*isDone = ExprSingleResult;

	/*
	 * If there's a test expression, we have to evaluate it and save the value
	 * where the CaseTestExpr placeholders can find it. We must save and
	 * restore prior setting of econtext's caseValue fields, in case this node
	 * is itself within a larger CASE.
	 */
	save_datum = econtext->caseValue_datum;
	save_isNull = econtext->caseValue_isNull;

	if (caseExpr->arg)
	{
		/*
		 * caseValue_datum and caseValue_isNull from econtext store the results of case
		 * expression. caseValue_isNull will be true if caseValue_datum store null value.
		 * Both caseValue_datum and caseValue_isNull should change at same time and they shouldn't
		 * go out of sync.
		 * Hence pass temporary variable(caseValue_isNull) and once evaluation is done,
		 * update the caseValue_isNull from econtext.
		 */
		bool caseValue_isNull = false;
		econtext->caseValue_datum = ExecEvalExpr(caseExpr->arg,
												 econtext,
												 &caseValue_isNull,
												 NULL);
		econtext->caseValue_isNull = caseValue_isNull;
	}

	/*
	 * we evaluate each of the WHEN clauses in turn, as soon as one is true we
	 * return the corresponding result. If none are true then we return the
	 * value of the default clause, or NULL if there is none.
	 */
	foreach(clause, clauses)
	{
		CaseWhenState *wclause = lfirst(clause);
		Datum		clause_value;

		clause_value = ExecEvalExpr(wclause->expr,
									econtext,
									isNull,
									NULL);

		/*
		 * if we have a true test, then we return the result, since the case
		 * statement is satisfied.	A NULL result from the test is not
		 * considered true.
		 */
		if (DatumGetBool(clause_value) && !*isNull)
		{
			econtext->caseValue_datum = save_datum;
			econtext->caseValue_isNull = save_isNull;
			return ExecEvalExpr(wclause->result,
								econtext,
								isNull,
								isDone);
		}
	}

	econtext->caseValue_datum = save_datum;
	econtext->caseValue_isNull = save_isNull;

	if (caseExpr->defresult)
	{
		return ExecEvalExpr(caseExpr->defresult,
							econtext,
							isNull,
							isDone);
	}

	*isNull = true;
	return (Datum) 0;
}

/*
 * ExecEvalCaseTestExpr
 *
 * Return the value stored by CASE.
 */
static Datum
ExecEvalCaseTestExpr(ExprState *exprstate,
					 ExprContext *econtext,
					 bool *isNull, ExprDoneCond *isDone)
{
	if (isDone)
		*isDone = ExprSingleResult;
	*isNull = econtext->caseValue_isNull;
	return econtext->caseValue_datum;
}

/* ----------------------------------------------------------------
 *		ExecEvalArray - ARRAY[] expressions
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalArray(ArrayExprState *astate, ExprContext *econtext,
			  bool *isNull, ExprDoneCond *isDone)
{
	ArrayExpr  *arrayExpr = (ArrayExpr *) astate->xprstate.expr;
	ArrayType  *result;
	ListCell   *element;
	Oid			element_type = arrayExpr->element_typeid;
	int			ndims = 0;
	int			dims[MAXDIM];
	int			lbs[MAXDIM];

	/* Set default values for result flags: non-null, not a set result */
	*isNull = false;
	if (isDone)
		*isDone = ExprSingleResult;

	if (!arrayExpr->multidims)
	{
		/* Elements are presumably of scalar type */
		int			nelems;
		Datum	   *dvalues;
		bool	   *dnulls;
		int			i = 0;

		ndims = 1;
		nelems = list_length(astate->elements);

		/* Shouldn't happen here, but if length is 0, return empty array */
		if (nelems == 0)
			return PointerGetDatum(construct_empty_array(element_type));

		dvalues = (Datum *) palloc(nelems * sizeof(Datum));
		dnulls = (bool *) palloc(nelems * sizeof(bool));

		/* loop through and build array of datums */
		foreach(element, astate->elements)
		{
			ExprState  *e = (ExprState *) lfirst(element);

			dvalues[i] = ExecEvalExpr(e, econtext, &dnulls[i], NULL);
			i++;
		}

		/* setup for 1-D array of the given length */
		dims[0] = nelems;
		lbs[0] = 1;

		result = construct_md_array(dvalues, dnulls, ndims, dims, lbs,
									element_type,
									astate->elemlength,
									astate->elembyval,
									astate->elemalign);
	}
	else
	{
		/* Must be nested array expressions */
		int			nbytes = 0;
		int			nitems = 0;
		int			outer_nelems = 0;
		int			elem_ndims = 0;
		int		   *elem_dims = NULL;
		int		   *elem_lbs = NULL;
		bool		firstone = true;
		bool		havenulls = false;
		bool		haveempty = false;
		char	  **subdata;
		bits8	  **subbitmaps;
		int		   *subbytes;
		int		   *subnitems;
		int			i;
		int32		dataoffset;
		char	   *dat;
		int			iitem;

		i = list_length(astate->elements);
		subdata = (char **) palloc(i * sizeof(char *));
		subbitmaps = (bits8 **) palloc(i * sizeof(bits8 *));
		subbytes = (int *) palloc(i * sizeof(int));
		subnitems = (int *) palloc(i * sizeof(int));

		/* loop through and get data area from each element */
		foreach(element, astate->elements)
		{
			ExprState  *e = (ExprState *) lfirst(element);
			bool		eisnull;
			Datum		arraydatum;
			ArrayType  *array;
			int			this_ndims;

			arraydatum = ExecEvalExpr(e, econtext, &eisnull, NULL);
			/* temporarily ignore null subarrays */
			if (eisnull)
			{
				haveempty = true;
				continue;
			}

			array = DatumGetArrayTypeP(arraydatum);

			/* run-time double-check on element type */
			if (element_type != ARR_ELEMTYPE(array))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("cannot merge incompatible arrays"),
						 errdetail("Array with element type %s cannot be "
						 "included in ARRAY construct with element type %s.",
								   format_type_be(ARR_ELEMTYPE(array)),
								   format_type_be(element_type))));

			this_ndims = ARR_NDIM(array);
			/* temporarily ignore zero-dimensional subarrays */
			if (this_ndims <= 0)
			{
				haveempty = true;
				continue;
			}

			if (firstone)
			{
				/* Get sub-array details from first member */
				elem_ndims = this_ndims;
				ndims = elem_ndims + 1;
				if (ndims <= 0 || ndims > MAXDIM)
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						  errmsg("number of array dimensions (%d) exceeds " \
								 "the maximum allowed (%d)", ndims, MAXDIM)));

				elem_dims = (int *) palloc(elem_ndims * sizeof(int));
				memcpy(elem_dims, ARR_DIMS(array), elem_ndims * sizeof(int));
				elem_lbs = (int *) palloc(elem_ndims * sizeof(int));
				memcpy(elem_lbs, ARR_LBOUND(array), elem_ndims * sizeof(int));

				firstone = false;
			}
			else
			{
				/* Check other sub-arrays are compatible */
				if (elem_ndims != this_ndims ||
					memcmp(elem_dims, ARR_DIMS(array),
						   elem_ndims * sizeof(int)) != 0 ||
					memcmp(elem_lbs, ARR_LBOUND(array),
						   elem_ndims * sizeof(int)) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
							 errmsg("multidimensional arrays must have array "
									"expressions with matching dimensions")));
			}

			subdata[outer_nelems] = ARR_DATA_PTR(array);
			subbitmaps[outer_nelems] = ARR_NULLBITMAP(array);
			subbytes[outer_nelems] = ARR_SIZE(array) - ARR_DATA_OFFSET(array);
			nbytes += subbytes[outer_nelems];
			subnitems[outer_nelems] = ArrayGetNItems(this_ndims,
													 ARR_DIMS(array));
			nitems += subnitems[outer_nelems];
			havenulls |= ARR_HASNULL(array);
			outer_nelems++;
		}

		/*
		 * If all items were null or empty arrays, return an empty array;
		 * otherwise, if some were and some weren't, raise error.  (Note:
		 * we must special-case this somehow to avoid trying to generate
		 * a 1-D array formed from empty arrays.  It's not ideal...)
		 */
		if (haveempty)
		{
			if (ndims == 0)		/* didn't find any nonempty array */
				return PointerGetDatum(construct_empty_array(element_type));
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("multidimensional arrays must have array "
							"expressions with matching dimensions")));
		}

		/* setup for multi-D array */
		dims[0] = outer_nelems;
		lbs[0] = 1;
		for (i = 1; i < ndims; i++)
		{
			dims[i] = elem_dims[i - 1];
			lbs[i] = elem_lbs[i - 1];
		}

		if (havenulls)
		{
			dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems);
			nbytes += dataoffset;
		}
		else
		{
			dataoffset = 0;		/* marker for no null bitmap */
			nbytes += ARR_OVERHEAD_NONULLS(ndims);
		}

		result = (ArrayType *) palloc(nbytes);
		SET_VARSIZE(result, nbytes);
		result->ndim = ndims;
		result->dataoffset = dataoffset;
		result->elemtype = element_type;
		memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
		memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));

		dat = ARR_DATA_PTR(result);
		iitem = 0;
		for (i = 0; i < outer_nelems; i++)
		{
			memcpy(dat, subdata[i], subbytes[i]);
			dat += subbytes[i];
			if (havenulls)
				array_bitmap_copy(ARR_NULLBITMAP(result), iitem,
								  subbitmaps[i], 0,
								  subnitems[i]);
			iitem += subnitems[i];
		}
	}

	return PointerGetDatum(result);
}

/* ----------------------------------------------------------------
 *		ExecEvalRow - ROW() expressions
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalRow(RowExprState *rstate,
			ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone)
{
	HeapTuple	tuple;
	Datum	   *values;
	bool	   *isnull;
	int			natts;
	ListCell   *arg;
	int			i;

	/* Set default values for result flags: non-null, not a set result */
	*isNull = false;
	if (isDone)
		*isDone = ExprSingleResult;

	/* Allocate workspace */
	natts = rstate->tupdesc->natts;
	values = (Datum *) palloc0(natts * sizeof(Datum));
	isnull = (bool *) palloc(natts * sizeof(bool));

	/* preset to nulls in case rowtype has some later-added columns */
	MemSetAligned(isnull, true, natts * sizeof(bool));

	/* Evaluate field values */
	i = 0;
	foreach(arg, rstate->args)
	{
		ExprState  *e = (ExprState *) lfirst(arg);

		values[i] = ExecEvalExpr(e, econtext, &isnull[i], NULL);
		i++;
	}

	tuple = heap_form_tuple(rstate->tupdesc, values, isnull);

	pfree(values);
	pfree(isnull);

	return HeapTupleGetDatum(tuple);
}

/* ----------------------------------------------------------------
 *		ExecEvalRowCompare - ROW() comparison-op ROW()
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalRowCompare(RowCompareExprState *rstate,
				   ExprContext *econtext,
				   bool *isNull, ExprDoneCond *isDone)
{
	bool		result;
	RowCompareType rctype = ((RowCompareExpr *) rstate->xprstate.expr)->rctype;
	int32		cmpresult = 0;
	ListCell   *l;
	ListCell   *r;
	int			i;

	if (isDone)
		*isDone = ExprSingleResult;
	*isNull = true;				/* until we get a result */

	i = 0;
	forboth(l, rstate->largs, r, rstate->rargs)
	{
		ExprState  *le = (ExprState *) lfirst(l);
		ExprState  *re = (ExprState *) lfirst(r);
		FunctionCallInfoData locfcinfo;

		InitFunctionCallInfoData(locfcinfo, &(rstate->funcs[i]), 2,
								 NULL, NULL);
		locfcinfo.arg[0] = ExecEvalExpr(le, econtext,
										&locfcinfo.argnull[0], NULL);
		locfcinfo.arg[1] = ExecEvalExpr(re, econtext,
										&locfcinfo.argnull[1], NULL);
		if (rstate->funcs[i].fn_strict &&
			(locfcinfo.argnull[0] || locfcinfo.argnull[1]))
			return (Datum) 0;	/* force NULL result */
		locfcinfo.isnull = false;
		cmpresult = DatumGetInt32(FunctionCallInvoke(&locfcinfo));
		if (locfcinfo.isnull)
			return (Datum) 0;	/* force NULL result */
		if (cmpresult != 0)
			break;				/* no need to compare remaining columns */
		i++;
	}

	switch (rctype)
	{
			/* EQ and NE cases aren't allowed here */
		case ROWCOMPARE_LT:
			result = (cmpresult < 0);
			break;
		case ROWCOMPARE_LE:
			result = (cmpresult <= 0);
			break;
		case ROWCOMPARE_GE:
			result = (cmpresult >= 0);
			break;
		case ROWCOMPARE_GT:
			result = (cmpresult > 0);
			break;
		default:
			elog(ERROR, "unrecognized RowCompareType: %d", (int) rctype);
			result = 0;			/* keep compiler quiet */
			break;
	}

	*isNull = false;
	return BoolGetDatum(result);
}

/* ----------------------------------------------------------------
 *		ExecEvalTableValue
 * ----------------------------------------------------------------
 */
static Datum 
ExecEvalTableValue(ExprState	*estate,
				   ExprContext	*econtext,
				   bool			*isNull, 
				   ExprDoneCond	*isDone)
{
	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	if (isDone)
		*isDone = ExprSingleResult;

	/*
	 * When evaluated as an expression a TableValueExpr will return NULL,
	 * this NULL is replaced by an AnyTable datum within nodeTableFunction.c
	 *
	 * Is there a better way to do this?
	 */
	*isNull = true;
	return (Datum) 0;
}

/* ----------------------------------------------------------------
 *		ExecEvalCoalesce
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalCoalesce(CoalesceExprState *coalesceExpr, ExprContext *econtext,
				 bool *isNull, ExprDoneCond *isDone)
{
	ListCell   *arg;

	if (isDone)
		*isDone = ExprSingleResult;

	/* Simply loop through until something NOT NULL is found */
	foreach(arg, coalesceExpr->args)
	{
		ExprState  *e = (ExprState *) lfirst(arg);
		Datum		value;

		value = ExecEvalExpr(e, econtext, isNull, NULL);
		if (!*isNull)
			return value;
	}

	/* Else return NULL */
	*isNull = true;
	return (Datum) 0;
}

/* ----------------------------------------------------------------
 *		ExecEvalMinMax
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalMinMax(MinMaxExprState *minmaxExpr, ExprContext *econtext,
			   bool *isNull, ExprDoneCond *isDone)
{
	Datum		result = (Datum) 0;
	MinMaxOp	op = ((MinMaxExpr *) minmaxExpr->xprstate.expr)->op;
	FunctionCallInfoData locfcinfo;
	ListCell   *arg;

	if (isDone)
		*isDone = ExprSingleResult;
	*isNull = true;				/* until we get a result */

	InitFunctionCallInfoData(locfcinfo, &minmaxExpr->cfunc, 2, NULL, NULL);
	locfcinfo.argnull[0] = false;
	locfcinfo.argnull[1] = false;

	foreach(arg, minmaxExpr->args)
	{
		ExprState  *e = (ExprState *) lfirst(arg);
		Datum		value;
		bool		valueIsNull;
		int32		cmpresult;

		value = ExecEvalExpr(e, econtext, &valueIsNull, NULL);
		if (valueIsNull)
			continue;			/* ignore NULL inputs */

		if (*isNull)
		{
			/* first nonnull input, adopt value */
			result = value;
			*isNull = false;
		}
		else
		{
			/* apply comparison function */
			locfcinfo.arg[0] = result;
			locfcinfo.arg[1] = value;
			locfcinfo.isnull = false;
			cmpresult = DatumGetInt32(FunctionCallInvoke(&locfcinfo));
			if (locfcinfo.isnull)		/* probably should not happen */
				continue;
			if (cmpresult > 0 && op == IS_LEAST)
				result = value;
			else if (cmpresult < 0 && op == IS_GREATEST)
				result = value;
		}
	}

	return result;
}

/* ----------------------------------------------------------------
 *		ExecEvalNullIf
 *
 * Note that this is *always* derived from the equals operator,
 * but since we need special processing of the arguments
 * we can not simply reuse ExecEvalOper() or ExecEvalFunc().
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalNullIf(FuncExprState *nullIfExpr,
			   ExprContext *econtext,
			   bool *isNull, ExprDoneCond *isDone)
{
	Datum		result;
	FunctionCallInfoData fcinfo;
	ExprDoneCond argDone;
	List	   *argList;

	if (isDone)
		*isDone = ExprSingleResult;

	/*
	 * Initialize function cache if first time through
	 */
	if (nullIfExpr->func.fn_oid == InvalidOid)
	{
		NullIfExpr *op = (NullIfExpr *) nullIfExpr->xprstate.expr;

		init_fcache(op->opfuncid, nullIfExpr,
					econtext->ecxt_per_query_memory, true);
		Assert(!nullIfExpr->func.fn_retset);
	}

	/*
	 * extract info from nullIfExpr
	 */
	argList = nullIfExpr->args;

	/* Need to prep callinfo structure */
	InitFunctionCallInfoData(fcinfo, &(nullIfExpr->func), 0, NULL, NULL);
	argDone = ExecEvalFuncArgs(&fcinfo, argList, econtext);
	if (argDone != ExprSingleResult)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("NULLIF does not support set arguments")));
	Assert(fcinfo.nargs == 2);

	/* if either argument is NULL they can't be equal */
	if (!fcinfo.argnull[0] && !fcinfo.argnull[1])
	{
		fcinfo.isnull = false;
		result = FunctionCallInvoke(&fcinfo);
		/* if the arguments are equal return null */
		if (!fcinfo.isnull && DatumGetBool(result))
		{
			*isNull = true;
			return (Datum) 0;
		}
	}

	/* else return first argument */
	*isNull = fcinfo.argnull[0];
	return fcinfo.arg[0];
}

/* ----------------------------------------------------------------
 *		ExecEvalNullTest
 *
 *		Evaluate a NullTest node.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalNullTest(NullTestState *nstate,
				 ExprContext *econtext,
				 bool *isNull,
				 ExprDoneCond *isDone)
{
	NullTest   *ntest = (NullTest *) nstate->xprstate.expr;
	Datum		result;

	result = ExecEvalExpr(nstate->arg, econtext, isNull, isDone);

	if (isDone && *isDone == ExprEndResult)
		return result;			/* nothing to check */

	if (nstate->argisrow && !(*isNull))
	{
		HeapTupleHeader tuple;
		Oid			tupType;
		int32		tupTypmod;
		TupleDesc	tupDesc;
		HeapTupleData tmptup;
		int			att;

		tuple = DatumGetHeapTupleHeader(result);

		tupType = HeapTupleHeaderGetTypeId(tuple);
		tupTypmod = HeapTupleHeaderGetTypMod(tuple);

		/* Lookup tupdesc if first time through or if type changes */
		tupDesc = get_cached_rowtype(tupType, tupTypmod,
									 &nstate->argdesc, econtext);

		/*
		 * heap_attisnull needs a HeapTuple not a bare HeapTupleHeader.
		 */
		tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
		tmptup.t_data = tuple;

		for (att = 1; att <= tupDesc->natts; att++)
		{
			/* ignore dropped columns */
			if (tupDesc->attrs[att - 1]->attisdropped)
				continue;
			if (heap_attisnull(&tmptup, att))
			{
				/* null field disproves IS NOT NULL */
				if (ntest->nulltesttype == IS_NOT_NULL)
					return BoolGetDatum(false);
			}
			else
			{
				/* non-null field disproves IS NULL */
				if (ntest->nulltesttype == IS_NULL)
					return BoolGetDatum(false);
			}
		}

		return BoolGetDatum(true);
	}
	else
	{
		/* Simple scalar-argument case, or a null rowtype datum */
		switch (ntest->nulltesttype)
		{
			case IS_NULL:
				if (*isNull)
				{
					*isNull = false;
					return BoolGetDatum(true);
				}
				else
					return BoolGetDatum(false);
			case IS_NOT_NULL:
				if (*isNull)
				{
					*isNull = false;
					return BoolGetDatum(false);
				}
				else
					return BoolGetDatum(true);
			default:
				elog(ERROR, "unrecognized nulltesttype: %d",
					 (int) ntest->nulltesttype);
				return (Datum) 0;		/* keep compiler quiet */
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecEvalBooleanTest
 *
 *		Evaluate a BooleanTest node.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalBooleanTest(GenericExprState *bstate,
					ExprContext *econtext,
					bool *isNull,
					ExprDoneCond *isDone)
{
	BooleanTest *btest = (BooleanTest *) bstate->xprstate.expr;
	Datum		result;

	result = ExecEvalExpr(bstate->arg, econtext, isNull, isDone);

	if (isDone && *isDone == ExprEndResult)
		return result;			/* nothing to check */

	switch (btest->booltesttype)
	{
		case IS_TRUE:
			if (*isNull)
			{
				*isNull = false;
				return BoolGetDatum(false);
			}
			else if (DatumGetBool(result))
				return BoolGetDatum(true);
			else
				return BoolGetDatum(false);
		case IS_NOT_TRUE:
			if (*isNull)
			{
				*isNull = false;
				return BoolGetDatum(true);
			}
			else if (DatumGetBool(result))
				return BoolGetDatum(false);
			else
				return BoolGetDatum(true);
		case IS_FALSE:
			if (*isNull)
			{
				*isNull = false;
				return BoolGetDatum(false);
			}
			else if (DatumGetBool(result))
				return BoolGetDatum(false);
			else
				return BoolGetDatum(true);
		case IS_NOT_FALSE:
			if (*isNull)
			{
				*isNull = false;
				return BoolGetDatum(true);
			}
			else if (DatumGetBool(result))
				return BoolGetDatum(true);
			else
				return BoolGetDatum(false);
		case IS_UNKNOWN:
			if (*isNull)
			{
				*isNull = false;
				return BoolGetDatum(true);
			}
			else
				return BoolGetDatum(false);
		case IS_NOT_UNKNOWN:
			if (*isNull)
			{
				*isNull = false;
				return BoolGetDatum(false);
			}
			else
				return BoolGetDatum(true);
		default:
			elog(ERROR, "unrecognized booltesttype: %d",
				 (int) btest->booltesttype);
			return (Datum) 0;	/* keep compiler quiet */
	}
}

/*
 * ExecEvalCoerceToDomain
 *
 * Test the provided data against the domain constraint(s).  If the data
 * passes the constraint specifications, pass it through (return the
 * datum) otherwise throw an error.
 */
static Datum
ExecEvalCoerceToDomain(CoerceToDomainState *cstate, ExprContext *econtext,
					   bool *isNull, ExprDoneCond *isDone)
{
	CoerceToDomain *ctest = (CoerceToDomain *) cstate->xprstate.expr;
	Datum		result;
	ListCell   *l;

	result = ExecEvalExpr(cstate->arg, econtext, isNull, isDone);

	if (isDone && *isDone == ExprEndResult)
		return result;			/* nothing to check */

	foreach(l, cstate->constraints)
	{
		DomainConstraintState *con = (DomainConstraintState *) lfirst(l);

		switch (con->constrainttype)
		{
			case DOM_CONSTRAINT_NOTNULL:
				if (*isNull)
					ereport(ERROR,
							(errcode(ERRCODE_NOT_NULL_VIOLATION),
							 errmsg("domain %s does not allow null values",
									format_type_be(ctest->resulttype))));
				break;
			case DOM_CONSTRAINT_CHECK:
				{
					Datum		conResult;
					bool		conIsNull;
					Datum		save_datum;
					bool		save_isNull;

					/*
					 * Set up value to be returned by CoerceToDomainValue
					 * nodes. We must save and restore prior setting of
					 * econtext's domainValue fields, in case this node is
					 * itself within a check expression for another domain.
					 */
					save_datum = econtext->domainValue_datum;
					save_isNull = econtext->domainValue_isNull;

					econtext->domainValue_datum = result;
					econtext->domainValue_isNull = *isNull;

					conResult = ExecEvalExpr(con->check_expr,
											 econtext, &conIsNull, NULL);

					if (!conIsNull &&
						!DatumGetBool(conResult))
						ereport(ERROR,
								(errcode(ERRCODE_CHECK_VIOLATION),
								 errmsg("value for domain %s violates check constraint \"%s\"",
										format_type_be(ctest->resulttype),
										con->name)));
					econtext->domainValue_datum = save_datum;
					econtext->domainValue_isNull = save_isNull;

					break;
				}
			default:
				elog(ERROR, "unrecognized constraint type: %d",
					 (int) con->constrainttype);
				break;
		}
	}

	/* If all has gone well (constraints did not fail) return the datum */
	return result;
}

/*
 * ExecEvalCoerceToDomainValue
 *
 * Return the value stored by CoerceToDomain.
 */
static Datum
ExecEvalCoerceToDomainValue(ExprState *exprstate,
							ExprContext *econtext,
							bool *isNull, ExprDoneCond *isDone)
{
	if (isDone)
		*isDone = ExprSingleResult;
	*isNull = econtext->domainValue_isNull;
	return econtext->domainValue_datum;
}

/*
 * ExecEvalPercentileExpr
 *
 * Returns a Datum whose value is the value of the precomputed
 * the value at the percentile found in the given expression context.
 * Actually,  this is almost same as ExecEvalAggref.  The main reason
 * to add this is because we don't change the catalog at the moment.
 * This will be cleaned when we can change the catalog.
 */
static Datum
ExecEvalPercentileExpr(PercentileExprState *exprstate,
					   ExprContext *econtext,
					   bool *isNull, ExprDoneCond *isDone)
{
	if (isDone)
		*isDone = ExprSingleResult;

	if (econtext->ecxt_aggvalues == NULL)		/* safety check */
		elog(ERROR, "no aggregates in this expression context");

	*isNull = econtext->ecxt_aggnulls[exprstate->aggno];
	return econtext->ecxt_aggvalues[exprstate->aggno];
}

/* ----------------------------------------------------------------
 *		ExecEvalFieldSelect
 *
 *		Evaluate a FieldSelect node.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalFieldSelect(FieldSelectState *fstate,
					ExprContext *econtext,
					bool *isNull,
					ExprDoneCond *isDone)
{
	FieldSelect *fselect = (FieldSelect *) fstate->xprstate.expr;
	AttrNumber	fieldnum = fselect->fieldnum;
	Datum		result;
	Datum		tupDatum;
	HeapTupleHeader tuple;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupDesc;
	Form_pg_attribute attr;
	HeapTupleData tmptup;

	tupDatum = ExecEvalExpr(fstate->arg, econtext, isNull, isDone);

	/* this test covers the isDone exception too: */
	if (*isNull)
		return tupDatum;

	tuple = DatumGetHeapTupleHeader(tupDatum);

	tupType = HeapTupleHeaderGetTypeId(tuple);
	tupTypmod = HeapTupleHeaderGetTypMod(tuple);

	/* Lookup tupdesc if first time through or if type changes */
	tupDesc = get_cached_rowtype(tupType, tupTypmod,
								 &fstate->argdesc, econtext);

	/* Check for dropped column, and force a NULL result if so */
	if (fieldnum <= 0 ||
		fieldnum > tupDesc->natts)	/* should never happen */
				elog(ERROR, "attribute number %d exceeds number of columns %d",
					 fieldnum, tupDesc->natts);
	attr = tupDesc->attrs[fieldnum - 1];
	if (attr->attisdropped)
	{
		*isNull = true;
		return (Datum) 0;
	}

	/* Check for type mismatch --- possible after ALTER COLUMN TYPE? */
	/* As in ExecEvalVar, we should but can't check typmod */
	if (fselect->resulttype != attr->atttypid)
		ereport(ERROR,
				(errmsg("attribute %d has wrong type", fieldnum),
				 errdetail("Table has type %s, but query expects %s.",
						   format_type_be(attr->atttypid),
						   format_type_be(fselect->resulttype))));

	/*
	 * heap_getattr needs a HeapTuple not a bare HeapTupleHeader.  We set all
	 * the fields in the struct just in case user tries to inspect system
	 * columns.
	 */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
	ItemPointerSetInvalid(&(tmptup.t_self));
	tmptup.t_data = tuple;

	result = heap_getattr(&tmptup,
						  fieldnum,
						  tupDesc,
						  isNull);
	return result;
}

/* ----------------------------------------------------------------
 *		ExecEvalFieldStore
 *
 *		Evaluate a FieldStore node.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalFieldStore(FieldStoreState *fstate,
				   ExprContext *econtext,
				   bool *isNull,
				   ExprDoneCond *isDone)
{
	FieldStore *fstore = (FieldStore *) fstate->xprstate.expr;
	HeapTuple	tuple;
	Datum		tupDatum;
	TupleDesc	tupDesc;
	Datum	   *values;
	bool	   *isnull;
	Datum		save_datum;
	bool		save_isNull;
	ListCell   *l1,
			   *l2;

	tupDatum = ExecEvalExpr(fstate->arg, econtext, isNull, isDone);

	if (isDone && *isDone == ExprEndResult)
		return tupDatum;

	/* Lookup tupdesc if first time through or after rescan */
	tupDesc = get_cached_rowtype(fstore->resulttype, -1,
								 &fstate->argdesc, econtext);

	/* Allocate workspace */
	values = (Datum *) palloc(tupDesc->natts * sizeof(Datum));
	isnull = (bool *) palloc(tupDesc->natts * sizeof(bool));

	if (!*isNull)
	{
		/*
		 * heap_deform_tuple needs a HeapTuple not a bare HeapTupleHeader. We
		 * set all the fields in the struct just in case.
		 */
		HeapTupleHeader tuphdr;
		HeapTupleData tmptup;

		tuphdr = DatumGetHeapTupleHeader(tupDatum);
		tmptup.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
		ItemPointerSetInvalid(&(tmptup.t_self));
		tmptup.t_data = tuphdr;

		heap_deform_tuple(&tmptup, tupDesc, values, isnull);
	}
	else
	{
		/* Convert null input tuple into an all-nulls row */
		MemSetAligned(isnull, true, tupDesc->natts * sizeof(bool));
	}

	/* Result is never null */
	*isNull = false;

	save_datum = econtext->caseValue_datum;
	save_isNull = econtext->caseValue_isNull;

	forboth(l1, fstate->newvals, l2, fstore->fieldnums)
	{
		ExprState  *newval = (ExprState *) lfirst(l1);
		AttrNumber	fieldnum = lfirst_int(l2);

		Assert(fieldnum > 0 && fieldnum <= tupDesc->natts);

		/*
		 * Use the CaseTestExpr mechanism to pass down the old value of the
		 * field being replaced; this is useful in case we have a nested field
		 * update situation.  It's safe to reuse the CASE mechanism because
		 * there cannot be a CASE between here and where the value would be
		 * needed.
		 */
		econtext->caseValue_datum = values[fieldnum - 1];
		econtext->caseValue_isNull = isnull[fieldnum - 1];

		values[fieldnum - 1] = ExecEvalExpr(newval,
											econtext,
											&isnull[fieldnum - 1],
											NULL);
	}

	econtext->caseValue_datum = save_datum;
	econtext->caseValue_isNull = save_isNull;

	tuple = heap_form_tuple(tupDesc, values, isnull);

	pfree(values);
	pfree(isnull);

	return HeapTupleGetDatum(tuple);
}

/* ----------------------------------------------------------------
 *		ExecEvalRelabelType
 *
 *		Evaluate a RelabelType node.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalRelabelType(GenericExprState *exprstate,
					ExprContext *econtext,
					bool *isNull, ExprDoneCond *isDone)
{
	return ExecEvalExpr(exprstate->arg, econtext, isNull, isDone);
}

/* ----------------------------------------------------------------
 *		ExecEvalPartOidExpr
 *
 *		Evaluate a PartOidExpr
 * ----------------------------------------------------------------
 */
static Datum ExecEvalPartOidExpr(PartOidExprState *exprstate,
						ExprContext *econtext,
						bool *isNull, ExprDoneCond *isDone)
{

	Assert(NULL != exprstate);
	Assert(NULL != isNull);

	if (isDone)
	{
		*isDone = ExprSingleResult;
	}

	PartitionConstraints *constraint = *exprstate->acceptedLeafPart;
	if (NULL != constraint)
	{
		*isNull = false;
		return UInt32GetDatum(constraint->pRule->parchildrelid);
	}

	*isNull = true;
	return PointerGetDatum(NULL);
}

/* ----------------------------------------------------------------
 *		ExecEvalPartDefaultExpr
 *
 *		Evaluate a PartDefaultExpr
 * ----------------------------------------------------------------
 */
static Datum ExecEvalPartDefaultExpr(PartDefaultExprState *exprstate,
						ExprContext *econtext,
						bool *isNull, ExprDoneCond *isDone)
{
	Assert(NULL != exprstate);
	Assert(NULL != isNull);

	PartDefaultExpr *expr = (PartDefaultExpr *) exprstate->xprstate.expr;
	PartitionConstraints *constraint = (PartitionConstraints *) exprstate->levelPartConstraints[expr->level];
	Assert (NULL != constraint);

	if (isDone)
	{
		*isDone = ExprSingleResult;
	}
	*isNull = false;

	return BoolGetDatum(constraint->defaultPart);
}

/* ----------------------------------------------------------------
 *		ExecEvalPartBoundExpr
 *
 *		Evaluate a PartBoundExpr
 * ----------------------------------------------------------------
 */
static Datum ExecEvalPartBoundExpr(PartBoundExprState *exprstate,
						ExprContext *econtext,
						bool *isNull, ExprDoneCond *isDone)
{
	Assert(NULL != exprstate);
	Assert(NULL != isNull);

	PartBoundExpr *expr = (PartBoundExpr *) exprstate->xprstate.expr;
	PartitionConstraints *constraint = (PartitionConstraints *) exprstate->levelPartConstraints[expr->level];
	Assert (NULL != constraint);
	Const *con = constraint->upperBound;
	if (expr->isLowerBound)
	{
		con = constraint->lowerBound;
	}

	if (isDone)
	{
		*isDone = ExprSingleResult;
	}

	if (NULL != con)
	{
		*isNull = con->constisnull;
		return con->constvalue;
	}

	/* return NULL if no upper bound or lower bound exists */
	*isNull = true;
	return PointerGetDatum(NULL);
}

/* ----------------------------------------------------------------
 *		ExecEvalPartBoundInclusionExpr
 *
 *		Evaluate a PartBoundInclusionExpr
 * ----------------------------------------------------------------
 */
static Datum ExecEvalPartBoundInclusionExpr(PartBoundInclusionExprState *exprstate,
							ExprContext *econtext,
							bool *isNull, ExprDoneCond *isDone)
{
	Assert(NULL != exprstate);
	Assert(NULL != isNull);

	PartBoundInclusionExpr *expr = (PartBoundInclusionExpr *) exprstate->xprstate.expr;
	PartitionConstraints *constraint = (PartitionConstraints *) exprstate->levelPartConstraints[expr->level];
	Assert (NULL != constraint);
	if (isDone)
	{
		*isDone = ExprSingleResult;
	}
	*isNull = false;
	if (expr->isLowerBound)
	{
		return BoolGetDatum(constraint->lbInclusive);
	}
	return BoolGetDatum(constraint->upInclusive);
}

/* ----------------------------------------------------------------
 *		ExecEvalPartBoundOpenExpr
 *
 *		Evaluate a PartBoundOpenExpr
 * ----------------------------------------------------------------
 */
static Datum ExecEvalPartBoundOpenExpr(PartBoundOpenExprState *exprstate,
							ExprContext *econtext,
							bool *isNull, ExprDoneCond *isDone)
{
	Assert(NULL != exprstate);
	Assert(NULL != isNull);

	PartBoundOpenExpr *expr = (PartBoundOpenExpr *) exprstate->xprstate.expr;
	PartitionConstraints *constraint = (PartitionConstraints *) exprstate->levelPartConstraints[expr->level];
	Assert (NULL != constraint);
	if (isDone)
	{
		*isDone = ExprSingleResult;
	}
	*isNull = false;
	if (expr->isLowerBound)
	{
		return BoolGetDatum(constraint->lbOpen);
	}
	return BoolGetDatum(constraint->upOpen);
}

/* ----------------------------------------------------------------
 *    ExecEvalCurrentOfExpr
 *
 *    Evaluate CURRENT OF
 *
 *    Constant folding must have bound observed values of 
 * 	gp_segment_id, ctid, and tableoid into the CurrentOfExpr for
 *	this function's consumption.
 * ----------------------------------------------------------------
 */
static Datum
ExecEvalCurrentOfExpr(ExprState *exprstate, ExprContext *econtext,
						bool *isNull, ExprDoneCond *isDone)
{
	CurrentOfExpr 	*cexpr = (CurrentOfExpr *) exprstate->expr;
	bool 			result = false;
	TupleTableSlot	*slot;

	if (isDone)
		*isDone = ExprSingleResult;
	*isNull = false;

	Assert(cexpr->cvarno != INNER);
	Assert(cexpr->cvarno != OUTER);

	slot = econtext->ecxt_scantuple;
	Assert(!TupIsNull(slot));

	/* 
	 * The currently scanned tuple must use heap storage for it to possibly
	 * satisfy the CURRENT OF qualification. Despite our grand attempts during
	 * parsing and constant folding to demand heap storage, the scanning of an 
	 * AO part is still possible, when the current row uses heap storage, but the 
	 * CURRENT OF invocation uses an unpruned scan of the partition table, yielding 
	 * tuples from the AO parts before the desired heap tuple.
	 */
	if (TupHasHeapTuple(slot))
	{
		if (cexpr->gp_segment_id == GetQEIndex() &&
			ItemPointerEquals(&cexpr->ctid, slot_get_ctid(slot)))
		{
			/*
			 * If tableoid is InvalidOid, this implies that constant folding had
			 * had determined tableoid was not necessary in uniquely identifying a tuple.
			 * Otherwise, the given tuple's tableoid must match the CURRENT OF tableoid.
			 */
			if (!OidIsValid(cexpr->tableoid) ||
				cexpr->tableoid == slot->tts_tableOid)
			{
				result = true;
			}
		}
	}

	return BoolGetDatum(result);
}


/*
 * ExecEvalExprSwitchContext
 *
 * Same as ExecEvalExpr, but get into the right allocation context explicitly.
 */
Datum
ExecEvalExprSwitchContext(ExprState *expression,
						  ExprContext *econtext,
						  bool *isNull,
						  ExprDoneCond *isDone)
{
	Datum		retDatum;
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	retDatum = ExecEvalExpr(expression, econtext, isNull, isDone);
	MemoryContextSwitchTo(oldContext);
	return retDatum;
}


/*
 * ExecInitExpr: prepare an expression tree for execution
 *
 * This function builds and returns an ExprState tree paralleling the given
 * Expr node tree.	The ExprState tree can then be handed to ExecEvalExpr
 * for execution.  Because the Expr tree itself is read-only as far as
 * ExecInitExpr and ExecEvalExpr are concerned, several different executions
 * of the same plan tree can occur concurrently.
 *
 * This must be called in a memory context that will last as long as repeated
 * executions of the expression are needed.  Typically the context will be
 * the same as the per-query context of the associated ExprContext.
 *
 * Any Aggref and SubPlan nodes found in the tree are added to the lists
 * of such nodes held by the parent PlanState.	Otherwise, we do very little
 * initialization here other than building the state-node tree.  Any nontrivial
 * work associated with initializing runtime info for a node should happen
 * during the first actual evaluation of that node.  (This policy lets us
 * avoid work if the node is never actually evaluated.)
 *
 * Note: there is no ExecEndExpr function; we assume that any resource
 * cleanup needed will be handled by just releasing the memory context
 * in which the state tree is built.  Functions that require additional
 * cleanup work can register a shutdown callback in the ExprContext.
 *
 *	'node' is the root of the expression tree to examine
 *	'parent' is the PlanState node that owns the expression.
 *
 * 'parent' may be NULL if we are preparing an expression that is not
 * associated with a plan tree.  (If so, it can't have aggs or subplans.)
 * This case should usually come through ExecPrepareExpr, not directly here.
 */
ExprState *
ExecInitExpr(Expr *node, PlanState *parent)
{
	ExprState  *state;

	if (node == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Var:
			state = (ExprState *) makeNode(ExprState);
			state->evalfunc = ExecEvalVar;
			break;
		case T_Const:
			state = (ExprState *) makeNode(ExprState);
			state->evalfunc = ExecEvalConst;
			break;
		case T_Param:
			state = (ExprState *) makeNode(ExprState);
			state->evalfunc = ExecEvalParam;
			break;
		case T_CoerceToDomainValue:
			state = (ExprState *) makeNode(ExprState);
			state->evalfunc = ExecEvalCoerceToDomainValue;
			break;
		case T_CaseTestExpr:
			state = (ExprState *) makeNode(ExprState);
			state->evalfunc = ExecEvalCaseTestExpr;
			break;
		case T_Aggref:
			{
				Aggref	   *aggref = (Aggref *) node;
				AggrefExprState *astate = makeNode(AggrefExprState);

				astate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalAggref;
				if (parent && IsA(parent, AggState))
				{
					AggState   *aggstate = (AggState *) parent;
					int			naggs;

					aggstate->aggs = lcons(astate, aggstate->aggs);
					naggs = ++aggstate->numaggs;

					/*
					 * Combine the argument and sortkey expressions into a single list
					 * along with the corresponding sortkey clauses, if any.
					 * The code here is a bit different from postgres, because
					 * GPDB does different things in parser for the ordered aggregate;
					 * We don't construct target list in parser but do it here.
					 * These lists are referenced in ExecInitAgg()
					 */
					astate->inputTargets =
							combineAggrefArgs(aggref, &astate->inputSortClauses);
					astate->args = (List *) ExecInitExpr((Expr *) astate->inputTargets,
														 parent);

					/*
					 * Complain if the aggregate's arguments contain any
					 * aggregates; nested agg functions are semantically
					 * nonsensical.  (This should have been caught earlier,
					 * but we defend against it here anyway.)
					 */
					if (naggs != aggstate->numaggs)
						ereport(ERROR,
								(errcode(ERRCODE_GROUPING_ERROR),
								 errmsg("aggregate function calls may not be nested")));
				}
				else
				{
					/* planner messed up */
					elog(ERROR, "aggref found in non-Agg plan node");
				}
				state = (ExprState *) astate;
			}
			break;
		case T_GroupingFunc:
			{
				GroupingFunc *gf = (GroupingFunc *)node;
				GroupingFuncExprState *gstate = makeNode(GroupingFuncExprState);

				gstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalGroupingFunc;
				gstate->args = gf->args;
				gstate->ngrpcols = gf->ngrpcols;
				state = (ExprState *) gstate;
			}
			break;
		case T_Grouping:
			{
				ExprState *gstate = makeNode(ExprState);
				gstate->evalfunc = (ExprStateEvalFunc) ExecEvalGrouping;
				state = (ExprState *) gstate;
			}
			break;
		case T_GroupId:
			{
				ExprState *gstate = makeNode(ExprState);
				gstate->evalfunc = (ExprStateEvalFunc) ExecEvalGroupId;
				state = (ExprState *) gstate;
			}
			break;
		case T_WindowRef:
			{
				WindowRef *windowref = (WindowRef *)node;
				WindowRefExprState *wrstate = makeNode(WindowRefExprState);
				int numrefs;
				WindowState   *winstate = (WindowState *) parent;
				
				wrstate->xprstate.evalfunc = 
					(ExprStateEvalFunc) ExecEvalWindowRef;
				
				Insist(parent && IsA(parent, WindowState));

				winstate->wrxstates = lcons(wrstate, winstate->wrxstates);
				numrefs = list_length(winstate->wrxstates);

				wrstate->args = (List *) ExecInitExpr((Expr *) windowref->args,
													  parent);
				/* 
				 * Nested window functions are invalid and should not have 
				 * reached this point in processing.
				 */
				if (numrefs != list_length(winstate->wrxstates))
					elog(ERRCODE_INTERNAL_ERROR, "nested windowref calls "
						 "found in Window plan node");
				state = (ExprState *) wrstate;
			}
			break;
		case T_ArrayRef:
			{
				ArrayRef   *aref = (ArrayRef *) node;
				ArrayRefExprState *astate = makeNode(ArrayRefExprState);

				astate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalArrayRef;
				astate->refupperindexpr = (List *)
					ExecInitExpr((Expr *) aref->refupperindexpr, parent);
				astate->reflowerindexpr = (List *)
					ExecInitExpr((Expr *) aref->reflowerindexpr, parent);
				astate->refexpr = ExecInitExpr(aref->refexpr, parent);
				astate->refassgnexpr = ExecInitExpr(aref->refassgnexpr,
													parent);
				/* do one-time catalog lookups for type info */
				astate->refattrlength = get_typlen(aref->refarraytype);
				get_typlenbyvalalign(aref->refelemtype,
									 &astate->refelemlength,
									 &astate->refelembyval,
									 &astate->refelemalign);
				state = (ExprState *) astate;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *funcexpr = (FuncExpr *) node;
				FuncExprState *fstate = makeNode(FuncExprState);

				fstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalFunc;
				fstate->args = (List *)
					ExecInitExpr((Expr *) funcexpr->args, parent);
				fstate->func.fn_oid = InvalidOid;		/* not initialized */
				FastPathStrict2Func(funcexpr->funcid, fstate);
				state = (ExprState *) fstate;
			}
			break;
		case T_OpExpr:
			{
				OpExpr	   *opexpr = (OpExpr *) node;
				FuncExprState *fstate = makeNode(FuncExprState);

				fstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalOper;
				fstate->args = (List *)
					ExecInitExpr((Expr *) opexpr->args, parent);
				fstate->func.fn_oid = InvalidOid;		/* not initialized */
				FastPathStrict2Func(opexpr->opfuncid, fstate);
				state = (ExprState *) fstate;
			}
			break;
		case T_DistinctExpr:
			{
				DistinctExpr *distinctexpr = (DistinctExpr *) node;
				FuncExprState *fstate = makeNode(FuncExprState);

				fstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalDistinct;
				fstate->args = (List *)
					ExecInitExpr((Expr *) distinctexpr->args, parent);
				fstate->func.fn_oid = InvalidOid;		/* not initialized */
				state = (ExprState *) fstate;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *opexpr = (ScalarArrayOpExpr *) node;
				ScalarArrayOpExprState *sstate = makeNode(ScalarArrayOpExprState);

				sstate->fxprstate.xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalScalarArrayOp;
				sstate->fxprstate.args = (List *)
					ExecInitExpr((Expr *) opexpr->args, parent);
				sstate->fxprstate.func.fn_oid = InvalidOid;		/* not initialized */
				sstate->element_type = InvalidOid;		/* ditto */

				FastPathScalarArrayOp(opexpr, sstate);
				state = (ExprState *) sstate;

			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *boolexpr = (BoolExpr *) node;
				BoolExprState *bstate = makeNode(BoolExprState);

				switch (boolexpr->boolop)
				{
					case AND_EXPR:
						bstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalAnd;
						break;
					case OR_EXPR:
						bstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalOr;
						break;
					case NOT_EXPR:
						bstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalNot;
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
		case T_SubPlan:
			{
				/* Keep this in sync with ExecInitExprInitPlan, below */
				SubPlan    *subplan = (SubPlan *) node;
				SubPlanState *sstate = makeNode(SubPlanState);

				sstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecSubPlan;

				if (!parent)
					elog(ERROR, "SubPlan found with no parent plan");

				/*
				 * Here we just add the SubPlanState nodes to parent->subPlan.
				 * The subplans will be initialized later.
				 */
				parent->subPlan = lcons(sstate, parent->subPlan);
				sstate->sub_estate = NULL;
				sstate->planstate = NULL;

				sstate->testexpr =
					ExecInitExpr((Expr *) subplan->testexpr, parent);
				sstate->args = (List *)
					ExecInitExpr((Expr *) subplan->args, parent);

				state = (ExprState *) sstate;
			}
			break;
		case T_FieldSelect:
			{
				FieldSelect *fselect = (FieldSelect *) node;
				FieldSelectState *fstate = makeNode(FieldSelectState);

				fstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalFieldSelect;
				fstate->arg = ExecInitExpr(fselect->arg, parent);
				fstate->argdesc = NULL;
				state = (ExprState *) fstate;
			}
			break;
		case T_FieldStore:
			{
				FieldStore *fstore = (FieldStore *) node;
				FieldStoreState *fstate = makeNode(FieldStoreState);

				fstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalFieldStore;
				fstate->arg = ExecInitExpr(fstore->arg, parent);
				fstate->newvals = (List *) ExecInitExpr((Expr *) fstore->newvals, parent);
				fstate->argdesc = NULL;
				state = (ExprState *) fstate;
			}
			break;
		case T_RelabelType:
			{
				RelabelType *relabel = (RelabelType *) node;
				GenericExprState *gstate = makeNode(GenericExprState);

				gstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalRelabelType;
				gstate->arg = ExecInitExpr(relabel->arg, parent);
				state = (ExprState *) gstate;
			}
			break;
		case T_ConvertRowtypeExpr:
			{
				ConvertRowtypeExpr *convert = (ConvertRowtypeExpr *) node;
				ConvertRowtypeExprState *cstate = makeNode(ConvertRowtypeExprState);

				cstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalConvertRowtype;
				cstate->arg = ExecInitExpr(convert->arg, parent);
				state = (ExprState *) cstate;
			}
			break;
		case T_CaseExpr:
			{
				CaseExpr   *caseexpr = (CaseExpr *) node;
				CaseExprState *cstate = makeNode(CaseExprState);
				List	   *outlist = NIL;
				ListCell   *l;

				cstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalCase;
				cstate->arg = ExecInitExpr(caseexpr->arg, parent);
				foreach(l, caseexpr->args)
				{
					CaseWhen   *when = (CaseWhen *) lfirst(l);
					CaseWhenState *wstate = makeNode(CaseWhenState);

					Assert(IsA(when, CaseWhen));
					wstate->xprstate.evalfunc = NULL;	/* not used */
					wstate->xprstate.expr = (Expr *) when;
					wstate->expr = ExecInitExpr(when->expr, parent);
					wstate->result = ExecInitExpr(when->result, parent);
					outlist = lappend(outlist, wstate);
				}
				cstate->args = outlist;
				cstate->defresult = ExecInitExpr(caseexpr->defresult, parent);
				state = (ExprState *) cstate;
			}
			break;
		case T_ArrayExpr:
			{
				ArrayExpr  *arrayexpr = (ArrayExpr *) node;
				ArrayExprState *astate = makeNode(ArrayExprState);
				List	   *outlist = NIL;
				ListCell   *l;

				astate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalArray;
				foreach(l, arrayexpr->elements)
				{
					Expr	   *e = (Expr *) lfirst(l);
					ExprState  *estate;

					estate = ExecInitExpr(e, parent);
					outlist = lappend(outlist, estate);
				}
				astate->elements = outlist;
				/* do one-time catalog lookup for type info */
				get_typlenbyvalalign(arrayexpr->element_typeid,
									 &astate->elemlength,
									 &astate->elembyval,
									 &astate->elemalign);
				state = (ExprState *) astate;
			}
			break;
		case T_RowExpr:
			{
				RowExpr    *rowexpr = (RowExpr *) node;
				RowExprState *rstate = makeNode(RowExprState);
				Form_pg_attribute *attrs;
				List	   *outlist = NIL;
				ListCell   *l;
				int			i;

				rstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalRow;
				/* Build tupdesc to describe result tuples */
				if (rowexpr->row_typeid == RECORDOID)
				{
					/* generic record, use runtime type assignment */
					rstate->tupdesc = ExecTypeFromExprList(rowexpr->args);
					BlessTupleDesc(rstate->tupdesc);
					/* we won't need to redo this at runtime */
				}
				else
				{
					/* it's been cast to a named type, use that */
					rstate->tupdesc = lookup_rowtype_tupdesc_copy(rowexpr->row_typeid, -1);
				}
				/* Set up evaluation, skipping any deleted columns */
				Assert(list_length(rowexpr->args) <= rstate->tupdesc->natts);
				attrs = rstate->tupdesc->attrs;
				i = 0;
				foreach(l, rowexpr->args)
				{
					Expr	   *e = (Expr *) lfirst(l);
					ExprState  *estate;

					if (!attrs[i]->attisdropped)
					{
						/*
						 * Guard against ALTER COLUMN TYPE on rowtype since
						 * the RowExpr was created.  XXX should we check
						 * typmod too?	Not sure we can be sure it'll be the
						 * same.
						 */
						if (exprType((Node *) e) != attrs[i]->atttypid)
							ereport(ERROR,
									(errcode(ERRCODE_DATATYPE_MISMATCH),
									 errmsg("ROW() column has type %s instead of type %s",
										format_type_be(exprType((Node *) e)),
									   format_type_be(attrs[i]->atttypid))));
					}
					else
					{
						/*
						 * Ignore original expression and insert a NULL. We
						 * don't really care what type of NULL it is, so
						 * always make an int4 NULL.
						 */
						e = (Expr *) makeNullConst(INT4OID, -1);
					}
					estate = ExecInitExpr(e, parent);
					outlist = lappend(outlist, estate);
					i++;
				}
				rstate->args = outlist;
				state = (ExprState *) rstate;
			}
			break;
		case T_RowCompareExpr:
			{
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;
				RowCompareExprState *rstate = makeNode(RowCompareExprState);
				int			nopers = list_length(rcexpr->opnos);
				List	   *outlist;
				ListCell   *l;
				ListCell   *l2;
				int			i;

				rstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalRowCompare;
				Assert(list_length(rcexpr->largs) == nopers);
				outlist = NIL;
				foreach(l, rcexpr->largs)
				{
					Expr	   *e = (Expr *) lfirst(l);
					ExprState  *estate;

					estate = ExecInitExpr(e, parent);
					outlist = lappend(outlist, estate);
				}
				rstate->largs = outlist;
				Assert(list_length(rcexpr->rargs) == nopers);
				outlist = NIL;
				foreach(l, rcexpr->rargs)
				{
					Expr	   *e = (Expr *) lfirst(l);
					ExprState  *estate;

					estate = ExecInitExpr(e, parent);
					outlist = lappend(outlist, estate);
				}
				rstate->rargs = outlist;
				Assert(list_length(rcexpr->opclasses) == nopers);
				rstate->funcs = (FmgrInfo *) palloc(nopers * sizeof(FmgrInfo));
				i = 0;
				forboth(l, rcexpr->opnos, l2, rcexpr->opclasses)
				{
					Oid			opno = lfirst_oid(l);
					Oid			opclass = lfirst_oid(l2);
					int			strategy;
					Oid			subtype;
					bool		recheck;
					Oid			proc;

					get_op_opclass_properties(opno, opclass,
											  &strategy, &subtype, &recheck);
					proc = get_opclass_proc(opclass, subtype, BTORDER_PROC);

					/*
					 * If we enforced permissions checks on index support
					 * functions, we'd need to make a check here.  But the
					 * index support machinery doesn't do that, and neither
					 * does this code.
					 */
					fmgr_info(proc, &(rstate->funcs[i]));
					i++;
				}
				state = (ExprState *) rstate;
			}
			break;
		case T_TableValueExpr:
			state = (ExprState *) makeNode(ExprState);
			state->evalfunc = ExecEvalTableValue;
			break;
		case T_CoalesceExpr:
			{
				CoalesceExpr *coalesceexpr = (CoalesceExpr *) node;
				CoalesceExprState *cstate = makeNode(CoalesceExprState);
				List	   *outlist = NIL;
				ListCell   *l;

				cstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalCoalesce;
				foreach(l, coalesceexpr->args)
				{
					Expr	   *e = (Expr *) lfirst(l);
					ExprState  *estate;

					estate = ExecInitExpr(e, parent);
					outlist = lappend(outlist, estate);
				}
				cstate->args = outlist;
				state = (ExprState *) cstate;
			}
			break;
		case T_MinMaxExpr:
			{
				MinMaxExpr *minmaxexpr = (MinMaxExpr *) node;
				MinMaxExprState *mstate = makeNode(MinMaxExprState);
				List	   *outlist = NIL;
				ListCell   *l;
				TypeCacheEntry *typentry;

				mstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalMinMax;
				foreach(l, minmaxexpr->args)
				{
					Expr	   *e = (Expr *) lfirst(l);
					ExprState  *estate;

					estate = ExecInitExpr(e, parent);
					outlist = lappend(outlist, estate);
				}
				mstate->args = outlist;
				/* Look up the btree comparison function for the datatype */
				typentry = lookup_type_cache(minmaxexpr->minmaxtype,
											 TYPECACHE_CMP_PROC);
				if (!OidIsValid(typentry->cmp_proc))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_FUNCTION),
							 errmsg("could not identify a comparison function for type %s",
									format_type_be(minmaxexpr->minmaxtype))));

				/*
				 * If we enforced permissions checks on index support
				 * functions, we'd need to make a check here.  But the index
				 * support machinery doesn't do that, and neither does this
				 * code.
				 */
				fmgr_info(typentry->cmp_proc, &(mstate->cfunc));
				state = (ExprState *) mstate;
			}
			break;
		case T_NullIfExpr:
			{
				NullIfExpr *nullifexpr = (NullIfExpr *) node;
				FuncExprState *fstate = makeNode(FuncExprState);

				fstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalNullIf;
				fstate->args = (List *)
					ExecInitExpr((Expr *) nullifexpr->args, parent);
				fstate->func.fn_oid = InvalidOid;		/* not initialized */
				state = (ExprState *) fstate;
			}
			break;
		case T_NullTest:
			{
				NullTest   *ntest = (NullTest *) node;
				NullTestState *nstate = makeNode(NullTestState);

				nstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalNullTest;
				nstate->arg = ExecInitExpr(ntest->arg, parent);
				nstate->argisrow = type_is_rowtype(exprType((Node *) ntest->arg));
				nstate->argdesc = NULL;
				state = (ExprState *) nstate;
			}
			break;
		case T_BooleanTest:
			{
				BooleanTest *btest = (BooleanTest *) node;
				GenericExprState *gstate = makeNode(GenericExprState);

				gstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalBooleanTest;
				gstate->arg = ExecInitExpr(btest->arg, parent);
				state = (ExprState *) gstate;
			}
			break;
		case T_CoerceToDomain:
			{
				CoerceToDomain *ctest = (CoerceToDomain *) node;
				CoerceToDomainState *cstate = makeNode(CoerceToDomainState);

				cstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalCoerceToDomain;
				cstate->arg = ExecInitExpr(ctest->arg, parent);
				cstate->constraints = GetDomainConstraints(ctest->resulttype);
				state = (ExprState *) cstate;
			}
			break;
		case T_CurrentOfExpr:
			state = (ExprState *) makeNode(ExprState);
			state->evalfunc = ExecEvalCurrentOfExpr;
			break;
		case T_PercentileExpr:
			{
				PercentileExpr	   *p = (PercentileExpr *) node;
				PercentileExprState *pstate = makeNode(PercentileExprState);
				AggState		   *aggstate = (AggState *) parent;
				int					naggs;

				if (!IsA(aggstate, AggState))
					elog(ERROR, "PercentileExpr found in non-Agg plan node: %d",
							(int) nodeTag(parent));

				aggstate->percs = lcons(pstate, aggstate->percs);
				naggs = ++aggstate->numaggs;

				pstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalPercentileExpr;


				/* This is to build TupleDesc. */
				pstate->tlist = combinePercentileArgs(p);

				/* This is to build ProjectionInfo. */
				pstate->args = (List *) ExecInitExpr((Expr *) pstate->tlist, parent);

				/*
				 * Complain if the aggregate's arguments contain any
				 * aggregates; nested agg functions are semantically
				 * nonsensical.  (This should have been caught earlier,
				 * but we defend against it here anyway.)
				 */
				if (naggs != aggstate->numaggs)
					ereport(ERROR,
							(errcode(ERRCODE_GROUPING_ERROR),
							 errmsg("aggregate function calls may not be nested")));

				state = (ExprState *) pstate;
			}
			break;
		case T_TargetEntry:
			{
				TargetEntry *tle = (TargetEntry *) node;
				GenericExprState *gstate = makeNode(GenericExprState);

				gstate->xprstate.evalfunc = NULL;		/* not used */
				gstate->arg = ExecInitExpr(tle->expr, parent);
				state = (ExprState *) gstate;
			}
			break;
		case T_List:
			{
				List	   *outlist = NIL;
				ListCell   *l;

				foreach(l, (List *) node)
				{
					outlist = lappend(outlist,
									  ExecInitExpr((Expr *) lfirst(l),
												   parent));
				}
				/* Don't fall through to the "common" code below */
				return (ExprState *) outlist;
			}
		case T_PartOidExpr:
			{
				Insist(parent && IsA(parent, PartitionSelectorState));
				PartitionSelectorState *psstate = (PartitionSelectorState *) parent;
				PartOidExprState *exprstate = makeNode(PartOidExprState);
#if USE_ASSERT_CHECKING
				PartOidExpr *expr = (PartOidExpr *) node;
				Assert (expr->level == ((PartitionSelector *) psstate->ps.plan)->nLevels - 1 &&
						"PartOidExpr can only refer to leaf level.");
#endif
				exprstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalPartOidExpr;
				/*
				 * exprstate->acceptedLeafPart is a double pointer, pointing
				 * to the field in the PartitionSelector state that will
				 * be holding the actual PartitionConstraints value (GPSQL-2956)
				 * computed for each tuple.
				 */
				exprstate->acceptedLeafPart = psstate->acceptedLeafPart;

				state = (ExprState *) exprstate;
			}
			break;
		case T_PartDefaultExpr:
			{
				Insist(parent && IsA(parent, PartitionSelectorState));
				PartitionSelectorState *psstate = (PartitionSelectorState *) parent;
				PartDefaultExprState *exprstate = makeNode(PartDefaultExprState);
				exprstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalPartDefaultExpr;
				exprstate->levelPartConstraints = psstate->levelPartConstraints;

				state = (ExprState *) exprstate;
			}
			break;
		case T_PartBoundExpr:
			{
				Insist(parent && IsA(parent, PartitionSelectorState));
				PartitionSelectorState *psstate = (PartitionSelectorState *) parent;
				PartBoundExprState *exprstate = makeNode(PartBoundExprState);
				exprstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalPartBoundExpr;
				exprstate->levelPartConstraints = psstate->levelPartConstraints;

				state = (ExprState *) exprstate;
			}
			break;
		case T_PartBoundInclusionExpr:
			{
				Insist(parent && IsA(parent, PartitionSelectorState));
				PartitionSelectorState *psstate = (PartitionSelectorState *) parent;
				PartBoundInclusionExprState *exprstate = makeNode(PartBoundInclusionExprState);
				exprstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalPartBoundInclusionExpr;
				exprstate->levelPartConstraints = psstate->levelPartConstraints;

				state = (ExprState *) exprstate;
			}
			break;
		case T_PartBoundOpenExpr:
			{
				Insist(parent && IsA(parent, PartitionSelectorState));
				PartitionSelectorState *psstate = (PartitionSelectorState *) parent;
				PartBoundOpenExprState *exprstate = makeNode(PartBoundOpenExprState);
				exprstate->xprstate.evalfunc = (ExprStateEvalFunc) ExecEvalPartBoundOpenExpr;
				exprstate->levelPartConstraints = psstate->levelPartConstraints;

				state = (ExprState *) exprstate;
			}
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			state = NULL;		/* keep compiler quiet */
			break;
	}

	/* Common code for all state-node types */
	state->expr = node;

	return state;
}

/*
 * ExecInitExprInitPlan --- initialize a subplan expr that's being handled
 * as an InitPlan.	This is identical to ExecInitExpr's handling of a regular
 * subplan expr, except we do NOT want to add the node to the parent's
 * subplan list.
 */
SubPlanState *
ExecInitExprInitPlan(SubPlan *node, PlanState *parent)
{
	SubPlanState *sstate = makeNode(SubPlanState);

	/* The subplan's state will be initialized later */
	sstate->sub_estate = NULL;
	sstate->planstate = NULL;

	if (parent != NULL)
	{
		sstate->testexpr = ExecInitExpr((Expr *) node->testexpr, parent);
		sstate->args = (List *) ExecInitExpr((Expr *) node->args, parent);
	}
	else
	{
		sstate->testexpr = NULL;
		sstate->args = NULL;
	}

	sstate->xprstate.expr = (Expr *) node;

	return sstate;
}

/*
 * ExecPrepareExpr --- initialize for expression execution outside a normal
 * Plan tree context.
 *
 * This differs from ExecInitExpr in that we don't assume the caller is
 * already running in the EState's per-query context.  Also, we apply
 * fix_opfuncids() to the passed expression tree to be sure it is ready
 * to run.	(In ordinary Plan trees the planner will have fixed opfuncids,
 * but callers outside the executor will not have done this.)
 */
ExprState *
ExecPrepareExpr(Expr *node, EState *estate)
{
	ExprState  *result;
	MemoryContext oldcontext;

	fix_opfuncids((Node *) node);

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	result = ExecInitExpr(node, NULL);

	MemoryContextSwitchTo(oldcontext);

	return result;
}


/* ----------------------------------------------------------------
 *					 ExecQual / ExecTargetList / ExecProject
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecQual
 *
 *		Evaluates a conjunctive boolean expression (qual list) and
 *		returns true iff none of the subexpressions are false.
 *		(We also return true if the list is empty.)
 *
 *	If some of the subexpressions yield NULL but none yield FALSE,
 *	then the result of the conjunction is NULL (ie, unknown)
 *	according to three-valued boolean logic.  In this case,
 *	we return the value specified by the "resultForNull" parameter.
 *
 *	Callers evaluating WHERE clauses should pass resultForNull=FALSE,
 *	since SQL specifies that tuples with null WHERE results do not
 *	get selected.  On the other hand, callers evaluating constraint
 *	conditions should pass resultForNull=TRUE, since SQL also specifies
 *	that NULL constraint conditions are not failures.
 *
 *	NOTE: it would not be correct to use this routine to evaluate an
 *	AND subclause of a boolean expression; for that purpose, a NULL
 *	result must be returned as NULL so that it can be properly treated
 *	in the next higher operator (cf. ExecEvalAnd and ExecEvalOr).
 *	This routine is only used in contexts where a complete expression
 *	is being evaluated and we know that NULL can be treated the same
 *	as one boolean result or the other.
 *
 * ----------------------------------------------------------------
 */
bool
ExecQual(List *qual, ExprContext *econtext, bool resultForNull)
{
	bool		result;
	MemoryContext oldContext;
	ListCell   *l;

	/*
	 * debugging stuff
	 */
	EV_printf("ExecQual: qual is ");
	EV_nodeDisplay(qual);
	EV_printf("\n");

	IncrProcessed();

	/*
	 * Run in short-lived per-tuple context while computing expressions.
	 */
	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/*
	 * Evaluate the qual conditions one at a time.	If we find a FALSE result,
	 * we can stop evaluating and return FALSE --- the AND result must be
	 * FALSE.  Also, if we find a NULL result when resultForNull is FALSE, we
	 * can stop and return FALSE --- the AND result must be FALSE or NULL in
	 * that case, and the caller doesn't care which.
	 *
	 * If we get to the end of the list, we can return TRUE.  This will happen
	 * when the AND result is indeed TRUE, or when the AND result is NULL (one
	 * or more NULL subresult, with all the rest TRUE) and the caller has
	 * specified resultForNull = TRUE.
	 */
	result = true;

	foreach(l, qual)
	{
		ExprState  *clause = (ExprState *) lfirst(l);
		Datum		expr_value;
		bool		isNull;

		expr_value = ExecEvalExpr(clause, econtext, &isNull, NULL);

		if (isNull)
		{
			if (resultForNull == false)
			{
				result = false; /* treat NULL as FALSE */
				break;
			}
		}
		else
		{
			if (!DatumGetBool(expr_value))
			{
				result = false; /* definitely FALSE */
				break;
			}
		}
	}

	MemoryContextSwitchTo(oldContext);

	return result;
}

/*
 * Number of items in a tlist (including any resjunk items!)
 */
int
ExecTargetListLength(List *targetlist)
{
	/* This used to be more complex, but fjoins are dead */
	return list_length(targetlist);
}

/*
 * Number of items in a tlist, not including any resjunk items
 */
int
ExecCleanTargetListLength(List *targetlist)
{
	int			len = 0;
	ListCell   *tl;

	foreach(tl, targetlist)
	{
		TargetEntry *curTle = (TargetEntry *) lfirst(tl);

		Assert(IsA(curTle, TargetEntry));
		if (!curTle->resjunk)
			len++;
	}
	return len;
}

/*
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
ExecTargetList(List *targetlist,
			   ExprContext *econtext,
			   Datum *values,
			   bool *isnull,
			   ExprDoneCond *itemIsDone,
			   ExprDoneCond *isDone)
{
	MemoryContext oldContext;
	ListCell   *tl;
	bool		haveDoneSets;

	/*
	 * Run in short-lived per-tuple context while computing expressions.
	 */
	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/*
	 * evaluate all the expressions in the target list
	 */
	if (isDone)
		*isDone = ExprSingleResult;		/* until proven otherwise */

	haveDoneSets = false;		/* any exhausted set exprs in tlist? */

	foreach(tl, targetlist)
	{
		GenericExprState *gstate = (GenericExprState *) lfirst(tl);
		TargetEntry *tle = (TargetEntry *) gstate->xprstate.expr;
		AttrNumber	resind = tle->resno - 1;

		values[resind] = ExecEvalExpr(gstate->arg,
									  econtext,
									  &isnull[resind],
									  &itemIsDone[resind]);

		if (itemIsDone[resind] != ExprSingleResult)
		{
			/* We have a set-valued expression in the tlist */
			if (isDone == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("set-valued function called in context that cannot accept a set")));
			if (itemIsDone[resind] == ExprMultipleResult)
			{
				/* we have undone sets in the tlist, set flag */
				*isDone = ExprMultipleResult;
			}
			else
			{
				/* we have done sets in the tlist, set flag for that */
				haveDoneSets = true;
			}
		}
	}

	if (haveDoneSets)
	{
		/*
		 * note: can't get here unless we verified isDone != NULL
		 */
		if (*isDone == ExprSingleResult)
		{
			/*
			 * all sets are done, so report that tlist expansion is complete.
			 */
			*isDone = ExprEndResult;
			MemoryContextSwitchTo(oldContext);
			return false;
		}
		else
		{
			/*
			 * We have some done and some undone sets.	Restart the done ones
			 * so that we can deliver a tuple (if possible).
			 */
			foreach(tl, targetlist)
			{
				GenericExprState *gstate = (GenericExprState *) lfirst(tl);
				TargetEntry *tle = (TargetEntry *) gstate->xprstate.expr;
				AttrNumber	resind = tle->resno - 1;

				if (itemIsDone[resind] == ExprEndResult)
				{
					values[resind] = ExecEvalExpr(gstate->arg,
												  econtext,
												  &isnull[resind],
												  &itemIsDone[resind]);

					if (itemIsDone[resind] == ExprEndResult)
					{
						/*
						 * Oh dear, this item is returning an empty set. Guess
						 * we can't make a tuple after all.
						 */
						*isDone = ExprEndResult;
						break;
					}
				}
			}

			/*
			 * If we cannot make a tuple because some sets are empty, we still
			 * have to cycle the nonempty sets to completion, else resources
			 * will not be released from subplans etc.
			 *
			 * XXX is that still necessary?
			 */
			if (*isDone == ExprEndResult)
			{
				foreach(tl, targetlist)
				{
					GenericExprState *gstate = (GenericExprState *) lfirst(tl);
					TargetEntry *tle = (TargetEntry *) gstate->xprstate.expr;
					AttrNumber	resind = tle->resno - 1;

					while (itemIsDone[resind] == ExprMultipleResult)
					{
						values[resind] = ExecEvalExpr(gstate->arg,
													  econtext,
													  &isnull[resind],
													  &itemIsDone[resind]);
					}
				}

				MemoryContextSwitchTo(oldContext);
				return false;
			}
		}
	}

	/* Report success */
	MemoryContextSwitchTo(oldContext);

	return true;
}

/*
 * ExecVariableList
 *		Evaluates a simple-Variable-list projection.
 *
 * Results are stored into the passed values and isnull arrays.
 */
static void
ExecVariableList(ProjectionInfo *projInfo,
				 Datum *values,
				 bool *isnull)
{
	ExprContext *econtext = projInfo->pi_exprContext;
	int		   *varSlotOffsets = projInfo->pi_varSlotOffsets;
	int		   *varNumbers = projInfo->pi_varNumbers;
	int			i;

	/*
	 * Assign to result by direct extraction of fields from source slots ... a
	 * mite ugly, but fast ...
	 */
	for (i = list_length(projInfo->pi_targetlist) - 1; i >= 0; i--)
	{
		char	   *slotptr = ((char *) econtext) + varSlotOffsets[i];
		TupleTableSlot *varSlot = *((TupleTableSlot **) slotptr);
		int			varNumber = varNumbers[i] - 1;

		values[i] = slot_getattr(varSlot, varNumber+1, &(isnull[i])); 
	}
}

/*
 * ExecProject
 *
 *		projects a tuple based on projection info and stores
 *		it in the previously specified tuple table slot.
 *
 *		Note: the result is always a virtual tuple; therefore it
 *		may reference the contents of the exprContext's scan tuples
 *		and/or temporary results constructed in the exprContext.
 *		If the caller wishes the result to be valid longer than that
 *		data will be valid, he must call ExecMaterializeSlot on the
 *		result slot.
 */
TupleTableSlot *
ExecProject(ProjectionInfo *projInfo, ExprDoneCond *isDone)
{
	TupleTableSlot *slot;

	/*
	 * sanity checks
	 */
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

		ExecVariableList(projInfo, slot_get_values(slot), slot_get_isnull(slot));
		ExecStoreVirtualTuple(slot);
	}
	else
	{
		if (ExecTargetList(projInfo->pi_targetlist,
						   projInfo->pi_exprContext,
						   slot_get_values(slot),
						   slot_get_isnull(slot),
						   (ExprDoneCond *) projInfo->pi_itemIsDone,
						   isDone))
			ExecStoreVirtualTuple(slot);
	}

	return slot;
}

/*
 * ExecIsExprUnsafeToConst_walker
 *
 * Almost all of the expressions are not allowed without the executor.
 * Returns true as soon as possible we find such unsafe nodes.
 */
static bool
ExecIsExprUnsafeToConst_walker(Node *node, void *context)
{
	switch(nodeTag(node))
	{
		/*
		 * Param can be a Const in some situation, but the demanded use case
		 * so far doesn't want it.
		 */
		case T_Const:
		case T_CaseTestExpr:
		case T_FuncExpr:
		case T_OpExpr:
		case T_DistinctExpr:
		case T_ScalarArrayOpExpr:
		case T_BoolExpr:
		case T_CaseExpr:
		case T_CoalesceExpr:
		case T_MinMaxExpr:
		case T_NullIfExpr:
		case T_NullTest:
		case T_BooleanTest:
		case T_List:
		case T_TypeCast:
			return false;

		default:
			return true;
	}
}

/*
 * ExecIsExprUnsafeToConst
 *
 * Returns true if the expression cannot be evaluated to a const value.
 */
static bool
ExecIsExprUnsafeToConst(Node *node)
{
	Assert(node != NULL);
	return ExecIsExprUnsafeToConst_walker(node, NULL);
}

/*
 * ExecEvalFunctionArgToConst
 *
 * Evaluates an argument of function expression and returns the result.
 * This is assumed to be used in the parser stage, where
 * dynamic evaluation such like Var is not available, though we put it
 * here so that we can extend it to be useful in other places later.
 */
Datum
ExecEvalFunctionArgToConst(FuncExpr *fexpr, int argno, bool *isnull)
{
	Expr		   *aexpr;
	Oid				argtype;
	Const		   *result;

	/* argument number sanity check */
	if (argno < 0 || list_length(fexpr->args) <= argno)
		elog(ERROR, "invalid argument number found during evaluating function argument");

	aexpr = (Expr *) list_nth(fexpr->args, argno);
	/*
	 * Check if the expression can be evaluated in the Const fasion.
	 */
	if (ExecIsExprUnsafeToConst((Node *) aexpr))
		elog(ERROR, "unable to resolve function argument");

	argtype = exprType((Node *) aexpr);
	if (!OidIsValid(argtype))
		elog(ERROR, "unable to resolve function argument type");

	result = (Const *) evaluate_expr(aexpr, argtype);
	/* evaluate_expr always returns Const */
	Assert(IsA(result, Const));

	if (isnull)
		*isnull = result->constisnull;

	return result->constvalue;
}

typedef struct neededColumnContext
{
	bool *mask;
	int n;
} neededColumnContext;

static bool
neededColumnContextWalker(Node *node, neededColumnContext *c)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *)node;

		if (var->varattno > 0) 
		{
			Assert(var->varattno <= c->n);
			c->mask[var->varattno - 1] = true;
		}
		/*
		 * If all attributes are included,
		 * set all entries in mask to true.
		 */
		else if (var->varattno == 0)
		{
			int i;

			for (i=0; i < c->n; i++)
				c->mask[i] = true;
		}

		return false;
	}
	return expression_tree_walker(node, neededColumnContextWalker, (void * )c);
}

/*
 * n specifies the number of allowed entries in mask: we use
 * it for bounds-checking in the walker above.
 */
void GetNeededColumnsForScan(Node *expr, bool *mask, int n)
{
	neededColumnContext c;

	c.mask = mask;
	c.n = n;

	neededColumnContextWalker(expr, &c);
}

/* ----------------------------------------------------------------
 *	isJoinExprNull
 *
 *	Checks if the join expression evaluates to NULL for a given
 *	input tuple.
 *
 *	The input tuple has to be present in the correct TupleTableSlot
 *	in the ExprContext. For example, if all the expressions
 *	in joinExpr refer to the inner side of the join,
 *	econtext->ecxt_innertuple must be valid.
 * ----------------------------------------------------------------
 */
bool
isJoinExprNull(List *joinExpr, ExprContext *econtext)
{

	Assert(NULL != joinExpr);
	bool joinkeys_null = true;

	ListCell   *lc;
	foreach(lc, joinExpr)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(lc);
		bool		isNull = false;

		/*
		 * Evaluate the current join attribute value of the tuple
		 */
		ExecEvalExpr(keyexpr, econtext, &isNull, NULL);

		if (!isNull)
		{
			/* Found at least one non-null join expression, we're done */
			joinkeys_null = false;
			break;
		}
	}

	return joinkeys_null;
}
