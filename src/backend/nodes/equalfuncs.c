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
 * equalfuncs.c
 *	  Equality functions to compare node trees.
 *
 * NOTE: we currently support comparing all node types found in parse
 * trees.  We do not support comparing executor state trees; there
 * is no need for that, and no point in maintaining all the code that
 * would be needed.  We also do not support comparing Path trees, mainly
 * because the circular linkages between RelOptInfo and Path nodes can't
 * be handled easily in a simple depth-first traversal.
 *
 * Currently, in fact, equal() doesn't know how to compare Plan trees
 * either.	This might need to be fixed someday.
 *
 * NOTE: it is intentional that parse location fields (in nodes that have
 * one) are not compared.  This is because we want, for example, a variable
 * "x" to be considered equal() to another reference to "x" in the query.
 *
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/nodes/equalfuncs.c,v 1.287.2.2 2007/08/31 01:44:14 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/relation.h"
#include "utils/datum.h"
#include "catalog/gp_policy.h"


/*
 * Macros to simplify comparison of different kinds of fields.	Use these
 * wherever possible to reduce the chance for silly typos.	Note that these
 * hard-wire the convention that the local variables in an Equal routine are
 * named 'a' and 'b'.
 */

/* Compare a simple scalar field (int, float, bool, enum, etc) */
#define COMPARE_SCALAR_FIELD(fldname) \
	do { \
		if (a->fldname != b->fldname) \
			return false; \
	} while (0)

/* Compare a field that is a pointer to some kind of Node or Node tree */
#define COMPARE_NODE_FIELD(fldname) \
	do { \
		if (!equal(a->fldname, b->fldname)) \
			return false; \
	} while (0)

/* Compare a field that is a pointer to a Bitmapset */
#define COMPARE_BITMAPSET_FIELD(fldname) \
	do { \
		if (!bms_equal(a->fldname, b->fldname)) \
			return false; \
	} while (0)

/* Compare a field that is a pointer to a C string, or perhaps NULL */
#define COMPARE_STRING_FIELD(fldname) \
	do { \
		if (!equalstr(a->fldname, b->fldname)) \
			return false; \
	} while (0)

/* Macro for comparing string fields that might be NULL */
#define equalstr(a, b)	\
	(((a) != NULL && (b) != NULL) ? (strcmp(a, b) == 0) : (a) == (b))

/* Compare a field that is a pointer to a simple palloc'd object of size sz */
#define COMPARE_POINTER_FIELD(fldname, sz) \
	do { \
		if (memcmp(a->fldname, b->fldname, (sz)) != 0) \
			return false; \
	} while (0)

/*
 * Compare a field that is a varlena datum to the other.
 * Note the result will be false if one is toasted and the other is untoasted.
 * It depends on the context if we can say those are equal or not.
 */
#define COMPARE_VARLENA_FIELD(fldname, len) \
	do{ \
		if (a->fldname != b->fldname) \
		{ \
			if (a->fldname == NULL || b->fldname == NULL) \
				return false; \
			if (!datumIsEqual(PointerGetDatum(a->fldname), \
							  PointerGetDatum(b->fldname), false, len)) \
				return false; \
		} \
	} while (0)

/* Compare a parse location field (this is a no-op, per note above) */
#define COMPARE_LOCATION_FIELD(fldname) \
	((void) 0)


/*
 *	Stuff from primnodes.h
 */

static bool
_equalAlias(Alias *a, Alias *b)
{
	COMPARE_STRING_FIELD(aliasname);
	COMPARE_NODE_FIELD(colnames);

	return true;
}

static bool
_equalRangeVar(RangeVar *a, RangeVar *b)
{
	COMPARE_STRING_FIELD(catalogname);
	COMPARE_STRING_FIELD(schemaname);
	COMPARE_STRING_FIELD(relname);
	COMPARE_SCALAR_FIELD(inhOpt);
	COMPARE_SCALAR_FIELD(istemp);
	COMPARE_NODE_FIELD(alias);
	/* do not compare 'location' field */

	return true;
}

/* 
 * Records information about the target of a CTAS (SELECT ... INTO). 
 * This node type first appears in 3.4 as part of the preparation for 
 * PlannedStmt support.
 *
 * NB Prior to introducting the IntoClause, only relOid, comptypeOid were
 *    tested for equality
 */
static bool
_equalIntoClause(IntoClause *a, IntoClause *b)
{
	COMPARE_NODE_FIELD(rel);
	COMPARE_NODE_FIELD(colNames);
	COMPARE_NODE_FIELD(options);
	COMPARE_SCALAR_FIELD(onCommit);
	COMPARE_STRING_FIELD(tableSpaceName);
	COMPARE_SCALAR_FIELD(oidInfo.relOid);
	COMPARE_SCALAR_FIELD(oidInfo.comptypeOid);
	
	return true;
}

/*
 * We don't need an _equalExpr because Expr is an abstract supertype which
 * should never actually get instantiated.	Also, since it has no common
 * fields except NodeTag, there's no need for a helper routine to factor
 * out comparing the common fields...
 */

static bool
_equalVar(Var *a, Var *b)
{
	COMPARE_SCALAR_FIELD(varno);
	COMPARE_SCALAR_FIELD(varattno);
	COMPARE_SCALAR_FIELD(vartype);
	COMPARE_SCALAR_FIELD(vartypmod);
	COMPARE_SCALAR_FIELD(varlevelsup);
	COMPARE_SCALAR_FIELD(varnoold);
	COMPARE_SCALAR_FIELD(varoattno);

	return true;
}

static bool
_equalConst(Const *a, Const *b)
{
	COMPARE_SCALAR_FIELD(consttype);
	COMPARE_SCALAR_FIELD(constlen);
	COMPARE_SCALAR_FIELD(constisnull);
	COMPARE_SCALAR_FIELD(constbyval);

	/*
	 * We treat all NULL constants of the same type as equal. Someday this
	 * might need to change?  But datumIsEqual doesn't work on nulls, so...
	 */
	if (a->constisnull)
		return true;
	return datumIsEqual(a->constvalue, b->constvalue,
						a->constbyval, a->constlen);
}

static bool
_equalParam(Param *a, Param *b)
{
	COMPARE_SCALAR_FIELD(paramkind);
	COMPARE_SCALAR_FIELD(paramid);
	COMPARE_SCALAR_FIELD(paramtype);

	return true;
}

static bool
_equalAggref(Aggref *a, Aggref *b)
{
	COMPARE_SCALAR_FIELD(aggfnoid);
	COMPARE_SCALAR_FIELD(aggtype);
	COMPARE_NODE_FIELD(args);
	COMPARE_SCALAR_FIELD(agglevelsup);
	COMPARE_SCALAR_FIELD(aggstar);
	COMPARE_SCALAR_FIELD(aggdistinct);
	COMPARE_SCALAR_FIELD(aggstage);
    COMPARE_NODE_FIELD(aggorder);

	return true;
}

static bool
_equalAggOrder(AggOrder *a, AggOrder *b)
{
    COMPARE_SCALAR_FIELD(sortImplicit);
	COMPARE_NODE_FIELD(sortTargets);
    COMPARE_NODE_FIELD(sortClause);

	return true;
}

static bool
_equalWindowRef(WindowRef *a, WindowRef *b)
{
	COMPARE_SCALAR_FIELD(winfnoid);
	COMPARE_SCALAR_FIELD(restype);
	COMPARE_NODE_FIELD(args);
	COMPARE_SCALAR_FIELD(winlevelsup);
	COMPARE_SCALAR_FIELD(windistinct);
	COMPARE_SCALAR_FIELD(winspec);
	COMPARE_SCALAR_FIELD(winindex);
	COMPARE_SCALAR_FIELD(winstage);
	COMPARE_SCALAR_FIELD(winlevel);

	return true;
}

static bool
_equalArrayRef(ArrayRef *a, ArrayRef *b)
{
	COMPARE_SCALAR_FIELD(refrestype);
	COMPARE_SCALAR_FIELD(refarraytype);
	COMPARE_SCALAR_FIELD(refelemtype);
	COMPARE_NODE_FIELD(refupperindexpr);
	COMPARE_NODE_FIELD(reflowerindexpr);
	COMPARE_NODE_FIELD(refexpr);
	COMPARE_NODE_FIELD(refassgnexpr);

	return true;
}

static bool
_equalFuncExpr(FuncExpr *a, FuncExpr *b)
{
	COMPARE_SCALAR_FIELD(funcid);
	COMPARE_SCALAR_FIELD(funcresulttype);
	COMPARE_SCALAR_FIELD(funcretset);

	/*
	 * Special-case COERCE_DONTCARE, so that planner can build coercion nodes
	 * that are equal() to both explicit and implicit coercions.
	 */
	if (a->funcformat != b->funcformat &&
		a->funcformat != COERCE_DONTCARE &&
		b->funcformat != COERCE_DONTCARE)
		return false;

	COMPARE_NODE_FIELD(args);

	return true;
}

static bool
_equalOpExpr(OpExpr *a, OpExpr *b)
{
	COMPARE_SCALAR_FIELD(opno);

	/*
	 * Special-case opfuncid: it is allowable for it to differ if one node
	 * contains zero and the other doesn't.  This just means that the one node
	 * isn't as far along in the parse/plan pipeline and hasn't had the
	 * opfuncid cache filled yet.
	 */
	if (a->opfuncid != b->opfuncid &&
		a->opfuncid != 0 &&
		b->opfuncid != 0)
		return false;

	COMPARE_SCALAR_FIELD(opresulttype);
	COMPARE_SCALAR_FIELD(opretset);
	COMPARE_NODE_FIELD(args);

	return true;
}

static bool
_equalDistinctExpr(DistinctExpr *a, DistinctExpr *b)
{
	COMPARE_SCALAR_FIELD(opno);

	/*
	 * Special-case opfuncid: it is allowable for it to differ if one node
	 * contains zero and the other doesn't.  This just means that the one node
	 * isn't as far along in the parse/plan pipeline and hasn't had the
	 * opfuncid cache filled yet.
	 */
	if (a->opfuncid != b->opfuncid &&
		a->opfuncid != 0 &&
		b->opfuncid != 0)
		return false;

	COMPARE_SCALAR_FIELD(opresulttype);
	COMPARE_SCALAR_FIELD(opretset);
	COMPARE_NODE_FIELD(args);

	return true;
}

static bool
_equalScalarArrayOpExpr(ScalarArrayOpExpr *a, ScalarArrayOpExpr *b)
{
	COMPARE_SCALAR_FIELD(opno);

	/*
	 * Special-case opfuncid: it is allowable for it to differ if one node
	 * contains zero and the other doesn't.  This just means that the one node
	 * isn't as far along in the parse/plan pipeline and hasn't had the
	 * opfuncid cache filled yet.
	 */
	if (a->opfuncid != b->opfuncid &&
		a->opfuncid != 0 &&
		b->opfuncid != 0)
		return false;

	COMPARE_SCALAR_FIELD(useOr);
	COMPARE_NODE_FIELD(args);

	return true;
}

static bool
_equalBoolExpr(BoolExpr *a, BoolExpr *b)
{
	COMPARE_SCALAR_FIELD(boolop);
	COMPARE_NODE_FIELD(args);

	return true;
}

static bool
_equalSubLink(SubLink *a, SubLink *b)
{
	COMPARE_SCALAR_FIELD(subLinkType);
	COMPARE_NODE_FIELD(testexpr);
	COMPARE_NODE_FIELD(operName);
	COMPARE_NODE_FIELD(subselect);
	/* do not compare 'location' field */

	return true;
}

static bool
_equalSubPlan(SubPlan *a, SubPlan *b)
{
	COMPARE_SCALAR_FIELD(subLinkType);
	/* CDB: Ignore value of qDispSliceId. */
	COMPARE_NODE_FIELD(testexpr);
	COMPARE_NODE_FIELD(paramIds);
	COMPARE_SCALAR_FIELD(plan_id);
	COMPARE_SCALAR_FIELD(firstColType);
	COMPARE_SCALAR_FIELD(firstColTypmod);
	COMPARE_SCALAR_FIELD(useHashTable);
	COMPARE_SCALAR_FIELD(unknownEqFalse);
	/* CDB: Ignore value of is_initplan */
	COMPARE_SCALAR_FIELD(is_multirow); /*CDB*/
	COMPARE_NODE_FIELD(setParam);
	COMPARE_NODE_FIELD(parParam);
	COMPARE_NODE_FIELD(args);

	return true;
}

static bool
_equalFieldSelect(FieldSelect *a, FieldSelect *b)
{
	COMPARE_NODE_FIELD(arg);
	COMPARE_SCALAR_FIELD(fieldnum);
	COMPARE_SCALAR_FIELD(resulttype);
	COMPARE_SCALAR_FIELD(resulttypmod);

	return true;
}

static bool
_equalFieldStore(FieldStore *a, FieldStore *b)
{
	COMPARE_NODE_FIELD(arg);
	COMPARE_NODE_FIELD(newvals);
	COMPARE_NODE_FIELD(fieldnums);
	COMPARE_SCALAR_FIELD(resulttype);

	return true;
}

static bool
_equalRelabelType(RelabelType *a, RelabelType *b)
{
	COMPARE_NODE_FIELD(arg);
	COMPARE_SCALAR_FIELD(resulttype);
	COMPARE_SCALAR_FIELD(resulttypmod);

	/*
	 * Special-case COERCE_DONTCARE, so that planner can build coercion nodes
	 * that are equal() to both explicit and implicit coercions.
	 */
	if (a->relabelformat != b->relabelformat &&
		a->relabelformat != COERCE_DONTCARE &&
		b->relabelformat != COERCE_DONTCARE)
		return false;

	return true;
}

static bool
_equalConvertRowtypeExpr(ConvertRowtypeExpr *a, ConvertRowtypeExpr *b)
{
	COMPARE_NODE_FIELD(arg);
	COMPARE_SCALAR_FIELD(resulttype);

	/*
	 * Special-case COERCE_DONTCARE, so that planner can build coercion nodes
	 * that are equal() to both explicit and implicit coercions.
	 */
	if (a->convertformat != b->convertformat &&
		a->convertformat != COERCE_DONTCARE &&
		b->convertformat != COERCE_DONTCARE)
		return false;

	return true;
}

static bool
_equalCaseExpr(CaseExpr *a, CaseExpr *b)
{
	COMPARE_SCALAR_FIELD(casetype);
	COMPARE_NODE_FIELD(arg);
	COMPARE_NODE_FIELD(args);
	COMPARE_NODE_FIELD(defresult);

	return true;
}

static bool
_equalCaseWhen(CaseWhen *a, CaseWhen *b)
{
	COMPARE_NODE_FIELD(expr);
	COMPARE_NODE_FIELD(result);

	return true;
}

static bool
_equalCaseTestExpr(CaseTestExpr *a, CaseTestExpr *b)
{
	COMPARE_SCALAR_FIELD(typeId);
	COMPARE_SCALAR_FIELD(typeMod);

	return true;
}

static bool
_equalArrayExpr(ArrayExpr *a, ArrayExpr *b)
{
	COMPARE_SCALAR_FIELD(array_typeid);
	COMPARE_SCALAR_FIELD(element_typeid);
	COMPARE_NODE_FIELD(elements);
	COMPARE_SCALAR_FIELD(multidims);

	return true;
}

static bool
_equalRowExpr(RowExpr *a, RowExpr *b)
{
	COMPARE_NODE_FIELD(args);
	COMPARE_SCALAR_FIELD(row_typeid);

	/*
	 * Special-case COERCE_DONTCARE, so that planner can build coercion nodes
	 * that are equal() to both explicit and implicit coercions.
	 */
	if (a->row_format != b->row_format &&
		a->row_format != COERCE_DONTCARE &&
		b->row_format != COERCE_DONTCARE)
		return false;

	return true;
}

static bool
_equalRowCompareExpr(RowCompareExpr *a, RowCompareExpr *b)
{
	COMPARE_SCALAR_FIELD(rctype);
	COMPARE_NODE_FIELD(opnos);
	COMPARE_NODE_FIELD(opclasses);
	COMPARE_NODE_FIELD(largs);
	COMPARE_NODE_FIELD(rargs);

	return true;
}

static bool
_equalCoalesceExpr(CoalesceExpr *a, CoalesceExpr *b)
{
	COMPARE_SCALAR_FIELD(coalescetype);
	COMPARE_NODE_FIELD(args);

	return true;
}

static bool
_equalMinMaxExpr(MinMaxExpr *a, MinMaxExpr *b)
{
	COMPARE_SCALAR_FIELD(minmaxtype);
	COMPARE_SCALAR_FIELD(op);
	COMPARE_NODE_FIELD(args);

	return true;
}

static bool
_equalNullIfExpr(NullIfExpr *a, NullIfExpr *b)
{
	COMPARE_SCALAR_FIELD(opno);

	/*
	 * Special-case opfuncid: it is allowable for it to differ if one node
	 * contains zero and the other doesn't.  This just means that the one node
	 * isn't as far along in the parse/plan pipeline and hasn't had the
	 * opfuncid cache filled yet.
	 */
	if (a->opfuncid != b->opfuncid &&
		a->opfuncid != 0 &&
		b->opfuncid != 0)
		return false;

	COMPARE_SCALAR_FIELD(opresulttype);
	COMPARE_SCALAR_FIELD(opretset);
	COMPARE_NODE_FIELD(args);

	return true;
}

static bool
_equalNullTest(NullTest *a, NullTest *b)
{
	COMPARE_NODE_FIELD(arg);
	COMPARE_SCALAR_FIELD(nulltesttype);

	return true;
}

static bool
_equalBooleanTest(BooleanTest *a, BooleanTest *b)
{
	COMPARE_NODE_FIELD(arg);
	COMPARE_SCALAR_FIELD(booltesttype);

	return true;
}

static bool
_equalCoerceToDomain(CoerceToDomain *a, CoerceToDomain *b)
{
	COMPARE_NODE_FIELD(arg);
	COMPARE_SCALAR_FIELD(resulttype);
	COMPARE_SCALAR_FIELD(resulttypmod);

	/*
	 * Special-case COERCE_DONTCARE, so that planner can build coercion nodes
	 * that are equal() to both explicit and implicit coercions.
	 */
	if (a->coercionformat != b->coercionformat &&
		a->coercionformat != COERCE_DONTCARE &&
		b->coercionformat != COERCE_DONTCARE)
		return false;

	return true;
}

static bool
_equalCoerceToDomainValue(CoerceToDomainValue *a, CoerceToDomainValue *b)
{
	COMPARE_SCALAR_FIELD(typeId);
	COMPARE_SCALAR_FIELD(typeMod);

	return true;
}

static bool
_equalSetToDefault(SetToDefault *a, SetToDefault *b)
{
	COMPARE_SCALAR_FIELD(typeId);
	COMPARE_SCALAR_FIELD(typeMod);

	return true;
}

static bool
_equalCurrentOfExpr(CurrentOfExpr *a, CurrentOfExpr *b)
{
	COMPARE_STRING_FIELD(cursor_name);
	COMPARE_SCALAR_FIELD(cvarno);
	COMPARE_SCALAR_FIELD(target_relid);

	/* some attributes omitted as they're bound only just before executor dispatch */

	return true;
}

static bool
_equalTargetEntry(TargetEntry *a, TargetEntry *b)
{
	COMPARE_NODE_FIELD(expr);
	COMPARE_SCALAR_FIELD(resno);
	COMPARE_STRING_FIELD(resname);
	COMPARE_SCALAR_FIELD(ressortgroupref);
	COMPARE_SCALAR_FIELD(resorigtbl);
	COMPARE_SCALAR_FIELD(resorigcol);
	COMPARE_SCALAR_FIELD(resjunk);

	return true;
}

static bool
_equalRangeTblRef(RangeTblRef *a, RangeTblRef *b)
{
	COMPARE_SCALAR_FIELD(rtindex);

	return true;
}

static bool
_equalJoinExpr(JoinExpr *a, JoinExpr *b)
{
	COMPARE_SCALAR_FIELD(jointype);
	COMPARE_SCALAR_FIELD(isNatural);
	COMPARE_NODE_FIELD(larg);
	COMPARE_NODE_FIELD(rarg);
	COMPARE_NODE_FIELD(usingClause);
	COMPARE_NODE_FIELD(quals);
	COMPARE_NODE_FIELD(alias);
	COMPARE_SCALAR_FIELD(rtindex);

	return true;
}

static bool
_equalFromExpr(FromExpr *a, FromExpr *b)
{
	COMPARE_NODE_FIELD(fromlist);
	COMPARE_NODE_FIELD(quals);

	return true;
}

static bool
_equalFlow(Flow *a, Flow *b)
{
	COMPARE_SCALAR_FIELD(flotype);
	COMPARE_SCALAR_FIELD(req_move);
	COMPARE_SCALAR_FIELD(locustype);
	COMPARE_SCALAR_FIELD(segindex);
	COMPARE_SCALAR_FIELD(numSortCols);
	COMPARE_POINTER_FIELD(sortColIdx, a->numSortCols*sizeof(AttrNumber));
	COMPARE_POINTER_FIELD(sortOperators, a->numSortCols*sizeof(Oid));
	COMPARE_NODE_FIELD(hashExpr);

	return true;
}


/*
 * Stuff from relation.h
 */

static bool
_equalPathKeyItem(PathKeyItem *a, PathKeyItem *b)
{
	COMPARE_NODE_FIELD(key);
	COMPARE_SCALAR_FIELD(sortop);

	return true;
}

static bool
_equalRestrictInfo(RestrictInfo *a, RestrictInfo *b)
{
	COMPARE_NODE_FIELD(clause);
	COMPARE_SCALAR_FIELD(is_pushed_down);
	COMPARE_SCALAR_FIELD(outerjoin_delayed);
	COMPARE_BITMAPSET_FIELD(required_relids);

	/*
	 * We ignore all the remaining fields, since they may not be set yet, and
	 * should be derivable from the clause anyway.
	 */

	return true;
}

static bool
_equalOuterJoinInfo(OuterJoinInfo *a, OuterJoinInfo *b)
{
	COMPARE_BITMAPSET_FIELD(min_lefthand);
	COMPARE_BITMAPSET_FIELD(min_righthand);
	COMPARE_BITMAPSET_FIELD(syn_lefthand);
	COMPARE_BITMAPSET_FIELD(syn_righthand);
	COMPARE_SCALAR_FIELD(join_type);
	COMPARE_SCALAR_FIELD(lhs_strict);
	COMPARE_SCALAR_FIELD(delay_upper_joins);
    /* do not compare derived equi_key_list COMPARE_NODE_FIELD(left_equi_key_list); */
    /* do not compare derived equi_key_list COMPARE_NODE_FIELD(right_equi_key_list); */

	return true;
}

static bool
_equalInClauseInfo(InClauseInfo *a, InClauseInfo *b)
{
	COMPARE_BITMAPSET_FIELD(righthand);
	COMPARE_NODE_FIELD(sub_targetlist);

    return true;
}

static bool
_equalAppendRelInfo(AppendRelInfo *a, AppendRelInfo *b)
{
	COMPARE_SCALAR_FIELD(parent_relid);
	COMPARE_SCALAR_FIELD(child_relid);
	COMPARE_SCALAR_FIELD(parent_reltype);
	COMPARE_SCALAR_FIELD(child_reltype);
	COMPARE_NODE_FIELD(col_mappings);
	COMPARE_NODE_FIELD(translated_vars);
	COMPARE_SCALAR_FIELD(parent_reloid);

	return true;
}



/*
 * Stuff from parsenodes.h
 */

static bool
_equalQuery(Query *a, Query *b)
{
	COMPARE_SCALAR_FIELD(commandType);
	COMPARE_SCALAR_FIELD(querySource);
	COMPARE_SCALAR_FIELD(canSetTag);
	COMPARE_NODE_FIELD(utilityStmt);
	COMPARE_SCALAR_FIELD(resultRelation);
	COMPARE_NODE_FIELD(intoClause);
	COMPARE_SCALAR_FIELD(hasAggs);
	COMPARE_SCALAR_FIELD(hasWindFuncs);
	COMPARE_SCALAR_FIELD(hasSubLinks);
	COMPARE_NODE_FIELD(rtable);
	COMPARE_NODE_FIELD(jointree);
	COMPARE_NODE_FIELD(targetList);
	COMPARE_NODE_FIELD(returningList);
	COMPARE_NODE_FIELD(groupClause);
	COMPARE_NODE_FIELD(havingQual);
	COMPARE_NODE_FIELD(windowClause);
	COMPARE_NODE_FIELD(distinctClause);
	COMPARE_NODE_FIELD(sortClause);
	COMPARE_NODE_FIELD(scatterClause);
	COMPARE_NODE_FIELD(cteList);
	COMPARE_SCALAR_FIELD(hasRecursive);
	COMPARE_SCALAR_FIELD(hasModifyingCTE);
	COMPARE_NODE_FIELD(limitOffset);
	COMPARE_NODE_FIELD(limitCount);
	COMPARE_NODE_FIELD(rowMarks);
	COMPARE_NODE_FIELD(setOperations);
	
	/* Prior to 3.4 these fields were compared.
	COMPARE_NODE_FIELD(resultRelations);
	COMPARE_NODE_FIELD(returningLists);
	 * now they are unused.
	 */
	
	/* Prior to 3.4 this test was
	 *     COMPARE_SCALAR_FIELD(intoPolicy); 
	 * Maybe GpPolicy should be a Node?
	 */
	if (! GpPolicyEqual(a->intoPolicy, b->intoPolicy) )
		return false;

	return true;
}

static bool
_equalInsertStmt(InsertStmt *a, InsertStmt *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(cols);
	COMPARE_NODE_FIELD(selectStmt);
	COMPARE_NODE_FIELD(returningList);

	return true;
}

static bool
_equalDeleteStmt(DeleteStmt *a, DeleteStmt *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(usingClause);
	COMPARE_NODE_FIELD(whereClause);
	COMPARE_NODE_FIELD(returningList);

	return true;
}

static bool
_equalUpdateStmt(UpdateStmt *a, UpdateStmt *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(targetList);
	COMPARE_NODE_FIELD(whereClause);
	COMPARE_NODE_FIELD(fromClause);
	COMPARE_NODE_FIELD(returningList);

	return true;
}

static bool
_equalSelectStmt(SelectStmt *a, SelectStmt *b)
{
	COMPARE_NODE_FIELD(distinctClause);
	COMPARE_NODE_FIELD(intoClause);
	COMPARE_NODE_FIELD(targetList);
	COMPARE_NODE_FIELD(fromClause);
	COMPARE_NODE_FIELD(whereClause);
	COMPARE_NODE_FIELD(groupClause);
	COMPARE_NODE_FIELD(havingClause);
	COMPARE_NODE_FIELD(windowClause);
	COMPARE_NODE_FIELD(valuesLists);
	COMPARE_NODE_FIELD(sortClause);
	COMPARE_NODE_FIELD(scatterClause);
	COMPARE_NODE_FIELD(withClause);
	COMPARE_NODE_FIELD(limitOffset);
	COMPARE_NODE_FIELD(limitCount);
	COMPARE_NODE_FIELD(lockingClause);
	COMPARE_SCALAR_FIELD(op);
	COMPARE_SCALAR_FIELD(all);
	COMPARE_NODE_FIELD(larg);
	COMPARE_NODE_FIELD(rarg);
	COMPARE_NODE_FIELD(distributedBy);

	return true;
}

static bool
_equalSetOperationStmt(SetOperationStmt *a, SetOperationStmt *b)
{
	COMPARE_SCALAR_FIELD(op);
	COMPARE_SCALAR_FIELD(all);
	COMPARE_NODE_FIELD(larg);
	COMPARE_NODE_FIELD(rarg);
	COMPARE_NODE_FIELD(colTypes);
	COMPARE_NODE_FIELD(colTypmods);

	return true;
}

static bool
_equalAlterTableStmt(AlterTableStmt *a, AlterTableStmt *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(cmds);
	COMPARE_SCALAR_FIELD(relkind);
	
	/* No need to compare AT workspace fields.  */

	return true;
}

static bool
_equalAlterTableCmd(AlterTableCmd *a, AlterTableCmd *b)
{
	COMPARE_SCALAR_FIELD(subtype);
	COMPARE_STRING_FIELD(name);
	COMPARE_NODE_FIELD(def);
	COMPARE_NODE_FIELD(transform);
	COMPARE_SCALAR_FIELD(behavior);
	COMPARE_SCALAR_FIELD(part_expanded);
	
	/* No need to compare AT workspace field, partoids.  */

	return true;
}

static bool
_equalInheritPartitionCmd(InheritPartitionCmd *a, InheritPartitionCmd *b)
{
	COMPARE_NODE_FIELD(parent);

	return true;
}

static bool
_equalAlterPartitionCmd(AlterPartitionCmd *a, AlterPartitionCmd *b)
{
	COMPARE_NODE_FIELD(partid);
	COMPARE_NODE_FIELD(arg1);
	COMPARE_NODE_FIELD(arg2);
	COMPARE_NODE_FIELD(scantable_splits);
	COMPARE_NODE_FIELD(newpart_aosegnos);
	return true;
}

static bool
_equalAlterPartitionId(AlterPartitionId *a, AlterPartitionId *b)
{
	COMPARE_SCALAR_FIELD(idtype);
	COMPARE_NODE_FIELD(partiddef);

	return true;
}

static bool
_equalAlterDomainStmt(AlterDomainStmt *a, AlterDomainStmt *b)
{
	COMPARE_SCALAR_FIELD(subtype);
	COMPARE_NODE_FIELD(typname);
	COMPARE_STRING_FIELD(name);
	COMPARE_NODE_FIELD(def);
	COMPARE_SCALAR_FIELD(behavior);

	return true;
}

static bool
_equalGrantStmt(GrantStmt *a, GrantStmt *b)
{
	COMPARE_SCALAR_FIELD(is_grant);
	COMPARE_SCALAR_FIELD(objtype);
	COMPARE_NODE_FIELD(objects);
	COMPARE_NODE_FIELD(privileges);
	COMPARE_NODE_FIELD(grantees);
	COMPARE_SCALAR_FIELD(grant_option);
	COMPARE_SCALAR_FIELD(behavior);
	COMPARE_NODE_FIELD(cooked_privs);

	return true;
}

static bool
_equalPrivGrantee(PrivGrantee *a, PrivGrantee *b)
{
	COMPARE_STRING_FIELD(rolname);

	return true;
}

static bool
_equalFuncWithArgs(FuncWithArgs *a, FuncWithArgs *b)
{
	COMPARE_NODE_FIELD(funcname);
	COMPARE_NODE_FIELD(funcargs);

	return true;
}

static bool
_equalGrantRoleStmt(GrantRoleStmt *a, GrantRoleStmt *b)
{
	COMPARE_NODE_FIELD(granted_roles);
	COMPARE_NODE_FIELD(grantee_roles);
	COMPARE_SCALAR_FIELD(is_grant);
	COMPARE_SCALAR_FIELD(admin_opt);
	COMPARE_STRING_FIELD(grantor);
	COMPARE_SCALAR_FIELD(behavior);

	return true;
}

static bool
_equalDeclareCursorStmt(DeclareCursorStmt *a, DeclareCursorStmt *b)
{
	COMPARE_STRING_FIELD(portalname);
	COMPARE_SCALAR_FIELD(options);
	COMPARE_NODE_FIELD(query);
	COMPARE_SCALAR_FIELD(is_simply_updatable);

	return true;
}

static bool
_equalClosePortalStmt(ClosePortalStmt *a, ClosePortalStmt *b)
{
	COMPARE_STRING_FIELD(portalname);

	return true;
}

static bool
_equalClusterStmt(ClusterStmt *a, ClusterStmt *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_STRING_FIELD(indexname);
	COMPARE_SCALAR_FIELD(oidInfo.relOid);
	COMPARE_SCALAR_FIELD(oidInfo.comptypeOid);
	COMPARE_SCALAR_FIELD(oidInfo.toastOid);
	COMPARE_SCALAR_FIELD(oidInfo.toastIndexOid);
	COMPARE_SCALAR_FIELD(oidInfo.aosegOid);
	COMPARE_SCALAR_FIELD(oidInfo.aosegIndexOid);
	COMPARE_SCALAR_FIELD(oidInfo.aoblkdirOid);
	COMPARE_SCALAR_FIELD(oidInfo.aoblkdirIndexOid);
	COMPARE_NODE_FIELD(new_ind_oids);

	return true;
}

static bool
_equalSingleRowErrorDesc(SingleRowErrorDesc *a, SingleRowErrorDesc *b)
{
	COMPARE_NODE_FIELD(errtable);
	COMPARE_SCALAR_FIELD(rejectlimit);
	COMPARE_SCALAR_FIELD(is_keep);
	COMPARE_SCALAR_FIELD(is_limit_in_rows);
	COMPARE_SCALAR_FIELD(reusing_existing_errtable);

	return true;
}

static bool
_equalCopyStmt(CopyStmt *a, CopyStmt *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(query);
	COMPARE_NODE_FIELD(attlist);
	COMPARE_SCALAR_FIELD(is_from);
	COMPARE_STRING_FIELD(filename);
	COMPARE_NODE_FIELD(options);
	COMPARE_NODE_FIELD(sreh);
	COMPARE_NODE_FIELD(err_aosegnos);
	COMPARE_NODE_FIELD(scantable_splits);

	return true;
}

static bool
_equalCreateStmt(CreateStmt *a, CreateStmt *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(tableElts);
	COMPARE_NODE_FIELD(inhRelations);
	COMPARE_NODE_FIELD(constraints);
	COMPARE_NODE_FIELD(options);
	COMPARE_SCALAR_FIELD(oncommit);
	COMPARE_STRING_FIELD(tablespacename);
	COMPARE_NODE_FIELD(distributedBy);
	COMPARE_SCALAR_FIELD(oidInfo.relOid);
	COMPARE_SCALAR_FIELD(oidInfo.comptypeOid);
	COMPARE_SCALAR_FIELD(oidInfo.toastOid);
	COMPARE_SCALAR_FIELD(oidInfo.toastIndexOid);
	COMPARE_SCALAR_FIELD(relKind);
	COMPARE_SCALAR_FIELD(relStorage);
	/* policy omitted */
	/* postCreate omitted */
	/* deferredStmts omitted */
	COMPARE_SCALAR_FIELD(is_part_child);
	COMPARE_SCALAR_FIELD(is_add_part);
	COMPARE_SCALAR_FIELD(is_split_part);
	COMPARE_SCALAR_FIELD(ownerid);
	COMPARE_SCALAR_FIELD(buildAoBlkdir);
	COMPARE_NODE_FIELD(attr_encodings);

	return true;
}

static bool
_equalColumnReferenceStorageDirective(ColumnReferenceStorageDirective *a,
									   ColumnReferenceStorageDirective *b)
{
	COMPARE_NODE_FIELD(column);
	COMPARE_SCALAR_FIELD(deflt);
	COMPARE_NODE_FIELD(encoding);

	return true;
}

static bool
_equalPartitionRangeItem(PartitionRangeItem *a, PartitionRangeItem *b)
{
	COMPARE_NODE_FIELD(partRangeVal);
	COMPARE_SCALAR_FIELD(partedge);
	COMPARE_SCALAR_FIELD(everycount);

	return true;
}

static bool
_equalExtTableTypeDesc(ExtTableTypeDesc *a, ExtTableTypeDesc *b)
{
	COMPARE_SCALAR_FIELD(exttabletype);
	COMPARE_NODE_FIELD(location_list);
	COMPARE_NODE_FIELD(on_clause);
	COMPARE_STRING_FIELD(command_string);

	return true;
}

static bool
_equalCreateExternalStmt(CreateExternalStmt *a, CreateExternalStmt *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(tableElts);
	COMPARE_NODE_FIELD(exttypedesc);
	COMPARE_STRING_FIELD(format);
	COMPARE_NODE_FIELD(formatOpts);
	COMPARE_SCALAR_FIELD(isweb);
	COMPARE_SCALAR_FIELD(iswritable);
	COMPARE_NODE_FIELD(sreh);
	COMPARE_NODE_FIELD(encoding);
	COMPARE_NODE_FIELD(distributedBy);

	return true;
}

static bool
_equalCreateForeignStmt(CreateForeignStmt *a, CreateForeignStmt *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(tableElts);
	COMPARE_STRING_FIELD(srvname);
	COMPARE_NODE_FIELD(options);

	return true;
}

static bool
_equalInhRelation(InhRelation *a, InhRelation *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(options);

	return true;
}

static bool
_equalDefineStmt(DefineStmt *a, DefineStmt *b)
{
	COMPARE_SCALAR_FIELD(kind);
	COMPARE_SCALAR_FIELD(oldstyle);
	COMPARE_NODE_FIELD(defnames);
	COMPARE_NODE_FIELD(args);
	COMPARE_NODE_FIELD(definition);
	COMPARE_SCALAR_FIELD(ordered);  /* CDB */
	COMPARE_SCALAR_FIELD(trusted);  /* CDB */

	return true;
}

static bool
_equalDropStmt(DropStmt *a, DropStmt *b)
{
	COMPARE_NODE_FIELD(objects);
	COMPARE_SCALAR_FIELD(removeType);
	COMPARE_SCALAR_FIELD(behavior);
	COMPARE_SCALAR_FIELD(missing_ok);

	return true;
}

static bool
_equalTruncateStmt(TruncateStmt *a, TruncateStmt *b)
{
	COMPARE_NODE_FIELD(relations);
	COMPARE_SCALAR_FIELD(behavior);
	COMPARE_NODE_FIELD(new_heap_oids);
	COMPARE_NODE_FIELD(new_toast_oids);
	COMPARE_NODE_FIELD(new_aoseg_oids);
	COMPARE_NODE_FIELD(new_aoblkdir_oids);
	COMPARE_NODE_FIELD(new_ind_oids);

	return true;
}

static bool
_equalCommentStmt(CommentStmt *a, CommentStmt *b)
{
	COMPARE_SCALAR_FIELD(objtype);
	COMPARE_NODE_FIELD(objname);
	COMPARE_NODE_FIELD(objargs);
	COMPARE_STRING_FIELD(comment);

	return true;
}

static bool
_equalFetchStmt(FetchStmt *a, FetchStmt *b)
{
	COMPARE_SCALAR_FIELD(direction);
	COMPARE_SCALAR_FIELD(howMany);
	COMPARE_STRING_FIELD(portalname);
	COMPARE_SCALAR_FIELD(ismove);

	return true;
}

static bool
_equalIndexStmt(IndexStmt *a, IndexStmt *b)
{
	COMPARE_STRING_FIELD(idxname);
	COMPARE_NODE_FIELD(relation);
	COMPARE_STRING_FIELD(accessMethod);
	COMPARE_STRING_FIELD(tableSpace);
	COMPARE_NODE_FIELD(indexParams);
	COMPARE_NODE_FIELD(options);
	COMPARE_NODE_FIELD(whereClause);
	COMPARE_NODE_FIELD(rangetable);
	COMPARE_SCALAR_FIELD(is_part_child);
	COMPARE_SCALAR_FIELD(unique);
	COMPARE_SCALAR_FIELD(primary);
	COMPARE_SCALAR_FIELD(isconstraint);
	COMPARE_STRING_FIELD(altconname);
	COMPARE_SCALAR_FIELD(constrOid);
	COMPARE_SCALAR_FIELD(concurrent);
	COMPARE_NODE_FIELD(idxOids);
	COMPARE_SCALAR_FIELD(do_part);

	return true;
}

static bool
_equalCreateFunctionStmt(CreateFunctionStmt *a, CreateFunctionStmt *b)
{
	COMPARE_SCALAR_FIELD(replace);
	COMPARE_NODE_FIELD(funcname);
	COMPARE_NODE_FIELD(parameters);
	COMPARE_NODE_FIELD(returnType);
	COMPARE_NODE_FIELD(options);
	COMPARE_NODE_FIELD(withClause);

	return true;
}

static bool
_equalFunctionParameter(FunctionParameter *a, FunctionParameter *b)
{
	COMPARE_STRING_FIELD(name);
	COMPARE_NODE_FIELD(argType);
	COMPARE_SCALAR_FIELD(mode);

	return true;
}

static bool
_equalAlterFunctionStmt(AlterFunctionStmt *a, AlterFunctionStmt *b)
{
	COMPARE_NODE_FIELD(func);
	COMPARE_NODE_FIELD(actions);

	return true;
}

static bool
_equalRemoveFuncStmt(RemoveFuncStmt *a, RemoveFuncStmt *b)
{
	COMPARE_SCALAR_FIELD(kind);
	COMPARE_NODE_FIELD(name);
	COMPARE_NODE_FIELD(args);
	COMPARE_SCALAR_FIELD(behavior);
	COMPARE_SCALAR_FIELD(missing_ok);

	return true;
}

static bool
_equalRemoveOpClassStmt(RemoveOpClassStmt *a, RemoveOpClassStmt *b)
{
	COMPARE_NODE_FIELD(opclassname);
	COMPARE_STRING_FIELD(amname);
	COMPARE_SCALAR_FIELD(behavior);
	COMPARE_SCALAR_FIELD(missing_ok);

	return true;
}

static bool
_equalRenameStmt(RenameStmt *a, RenameStmt *b)
{
	COMPARE_SCALAR_FIELD(renameType);
	COMPARE_NODE_FIELD(relation);
	COMPARE_SCALAR_FIELD(objid);
	COMPARE_NODE_FIELD(object);
	COMPARE_NODE_FIELD(objarg);
	COMPARE_STRING_FIELD(subname);
	COMPARE_STRING_FIELD(newname);

	return true;
}

static bool
_equalAlterObjectSchemaStmt(AlterObjectSchemaStmt *a, AlterObjectSchemaStmt *b)
{
	COMPARE_SCALAR_FIELD(objectType);
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(object);
	COMPARE_NODE_FIELD(objarg);
	COMPARE_STRING_FIELD(addname);
	COMPARE_STRING_FIELD(newschema);

	return true;
}

static bool
_equalAlterOwnerStmt(AlterOwnerStmt *a, AlterOwnerStmt *b)
{
	COMPARE_SCALAR_FIELD(objectType);
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(object);
	COMPARE_NODE_FIELD(objarg);
	COMPARE_STRING_FIELD(addname);
	COMPARE_STRING_FIELD(newowner);

	return true;
}

static bool
_equalRuleStmt(RuleStmt *a, RuleStmt *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_STRING_FIELD(rulename);
	COMPARE_NODE_FIELD(whereClause);
	COMPARE_SCALAR_FIELD(event);
	COMPARE_SCALAR_FIELD(instead);
	COMPARE_NODE_FIELD(actions);
	COMPARE_SCALAR_FIELD(replace);
	COMPARE_SCALAR_FIELD(ruleOid);

	return true;
}

static bool
_equalNotifyStmt(NotifyStmt *a, NotifyStmt *b)
{
	COMPARE_NODE_FIELD(relation);

	return true;
}

static bool
_equalListenStmt(ListenStmt *a, ListenStmt *b)
{
	COMPARE_NODE_FIELD(relation);

	return true;
}

static bool
_equalUnlistenStmt(UnlistenStmt *a, UnlistenStmt *b)
{
	COMPARE_NODE_FIELD(relation);

	return true;
}

static bool
_equalTransactionStmt(TransactionStmt *a, TransactionStmt *b)
{
	COMPARE_SCALAR_FIELD(kind);
	COMPARE_NODE_FIELD(options);
	COMPARE_STRING_FIELD(gid);

	return true;
}

static bool
_equalCompositeTypeStmt(CompositeTypeStmt *a, CompositeTypeStmt *b)
{
	COMPARE_NODE_FIELD(typevar);
	COMPARE_NODE_FIELD(coldeflist);

	return true;
}

static bool
_equalViewStmt(ViewStmt *a, ViewStmt *b)
{
	COMPARE_NODE_FIELD(view);
	COMPARE_NODE_FIELD(aliases);
	COMPARE_NODE_FIELD(query);
	COMPARE_SCALAR_FIELD(replace);
	COMPARE_SCALAR_FIELD(relOid);
	COMPARE_SCALAR_FIELD(comptypeOid);
	COMPARE_SCALAR_FIELD(rewriteOid);

	return true;
}

static bool
_equalLoadStmt(LoadStmt *a, LoadStmt *b)
{
	COMPARE_STRING_FIELD(filename);

	return true;
}

static bool
_equalCreateDomainStmt(CreateDomainStmt *a, CreateDomainStmt *b)
{
	COMPARE_NODE_FIELD(domainname);
	COMPARE_NODE_FIELD(typname);
	COMPARE_NODE_FIELD(constraints);

	return true;
}

static bool
_equalCreateOpClassStmt(CreateOpClassStmt *a, CreateOpClassStmt *b)
{
	COMPARE_NODE_FIELD(opclassname);
	COMPARE_STRING_FIELD(amname);
	COMPARE_NODE_FIELD(datatype);
	COMPARE_NODE_FIELD(items);
	COMPARE_SCALAR_FIELD(isDefault);

	return true;
}

static bool
_equalCreateOpClassItem(CreateOpClassItem *a, CreateOpClassItem *b)
{
	COMPARE_SCALAR_FIELD(itemtype);
	COMPARE_NODE_FIELD(name);
	COMPARE_NODE_FIELD(args);
	COMPARE_SCALAR_FIELD(number);
	COMPARE_SCALAR_FIELD(recheck);
	COMPARE_NODE_FIELD(storedtype);

	return true;
}

static bool
_equalCreatedbStmt(CreatedbStmt *a, CreatedbStmt *b)
{
	COMPARE_STRING_FIELD(dbname);
	COMPARE_NODE_FIELD(options);
	COMPARE_SCALAR_FIELD(dbOid);
	return true;
}

static bool
_equalAlterDatabaseStmt(AlterDatabaseStmt *a, AlterDatabaseStmt *b)
{
	COMPARE_STRING_FIELD(dbname);
	COMPARE_NODE_FIELD(options);

	return true;
}


static bool
_equalAlterDatabaseSetStmt(AlterDatabaseSetStmt *a, AlterDatabaseSetStmt *b)
{
	COMPARE_STRING_FIELD(dbname);
	COMPARE_STRING_FIELD(variable);
	COMPARE_NODE_FIELD(value);

	return true;
}

static bool
_equalDropdbStmt(DropdbStmt *a, DropdbStmt *b)
{
	COMPARE_STRING_FIELD(dbname);
	COMPARE_SCALAR_FIELD(missing_ok);

	return true;
}

static bool
_equalVacuumStmt(VacuumStmt *a, VacuumStmt *b)
{
	COMPARE_SCALAR_FIELD(vacuum);
	COMPARE_SCALAR_FIELD(full);
	COMPARE_SCALAR_FIELD(analyze);
	COMPARE_SCALAR_FIELD(verbose);
	COMPARE_SCALAR_FIELD(rootonly);
	COMPARE_SCALAR_FIELD(freeze_min_age);
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(va_cols);
	COMPARE_NODE_FIELD(expanded_relids);
	COMPARE_NODE_FIELD(extra_oids);

	return true;
}

static bool
_equalExplainStmt(ExplainStmt *a, ExplainStmt *b)
{
	COMPARE_NODE_FIELD(query);
	COMPARE_SCALAR_FIELD(verbose);
	COMPARE_SCALAR_FIELD(analyze);

	return true;
}

static bool
_equalCreateSeqStmt(CreateSeqStmt *a, CreateSeqStmt *b)
{
	COMPARE_NODE_FIELD(sequence);
	COMPARE_NODE_FIELD(options);
	COMPARE_SCALAR_FIELD(relOid);

	return true;
}

static bool
_equalAlterSeqStmt(AlterSeqStmt *a, AlterSeqStmt *b)
{
	COMPARE_NODE_FIELD(sequence);
	COMPARE_NODE_FIELD(options);

	return true;
}

static bool
_equalVariableSetStmt(VariableSetStmt *a, VariableSetStmt *b)
{
	COMPARE_STRING_FIELD(name);
	COMPARE_NODE_FIELD(args);
	COMPARE_SCALAR_FIELD(is_local);

	return true;
}

static bool
_equalVariableShowStmt(VariableShowStmt *a, VariableShowStmt *b)
{
	COMPARE_STRING_FIELD(name);

	return true;
}

static bool
_equalVariableResetStmt(VariableResetStmt *a, VariableResetStmt *b)
{
	COMPARE_STRING_FIELD(name);

	return true;
}

static bool
_equalCreateFileSpaceStmt(CreateFileSpaceStmt *a, CreateFileSpaceStmt *b)
{
	COMPARE_STRING_FIELD(filespacename);
	COMPARE_STRING_FIELD(owner);
	COMPARE_STRING_FIELD(fsysname);
	COMPARE_STRING_FIELD(location);
	COMPARE_NODE_FIELD(options);

	return true;
}

static bool
_equalCreateTableSpaceStmt(CreateTableSpaceStmt *a, CreateTableSpaceStmt *b)
{
	COMPARE_STRING_FIELD(tablespacename);
	COMPARE_STRING_FIELD(owner);
	COMPARE_STRING_FIELD(filespacename);

	return true;
}

static bool
_equalCreateFdwStmt(CreateFdwStmt *a, CreateFdwStmt *b)
{
	COMPARE_STRING_FIELD(fdwname);
	COMPARE_NODE_FIELD(validator);
	COMPARE_NODE_FIELD(options);

	return true;
}

static bool
_equalAlterFdwStmt(AlterFdwStmt *a, AlterFdwStmt *b)
{
	COMPARE_STRING_FIELD(fdwname);
	COMPARE_NODE_FIELD(validator);
	COMPARE_SCALAR_FIELD(change_validator);
	COMPARE_NODE_FIELD(options);

	return true;
}

static bool
_equalDropFdwStmt(DropFdwStmt *a, DropFdwStmt *b)
{
	COMPARE_STRING_FIELD(fdwname);
	COMPARE_SCALAR_FIELD(missing_ok);
	COMPARE_SCALAR_FIELD(behavior);

	return true;
}

static bool
_equalCreateForeignServerStmt(CreateForeignServerStmt *a, CreateForeignServerStmt *b)
{
	COMPARE_STRING_FIELD(servername);
	COMPARE_STRING_FIELD(servertype);
	COMPARE_STRING_FIELD(version);
	COMPARE_STRING_FIELD(fdwname);
	COMPARE_NODE_FIELD(options);

	return true;
}

static bool
_equalAlterForeignServerStmt(AlterForeignServerStmt *a, AlterForeignServerStmt *b)
{
	COMPARE_STRING_FIELD(servername);
	COMPARE_STRING_FIELD(version);
	COMPARE_NODE_FIELD(options);
	COMPARE_SCALAR_FIELD(has_version);

	return true;
}

static bool
_equalDropForeignServerStmt(DropForeignServerStmt *a, DropForeignServerStmt *b)
{
	COMPARE_STRING_FIELD(servername);
	COMPARE_SCALAR_FIELD(missing_ok);
	COMPARE_SCALAR_FIELD(behavior);

	return true;
}

static bool
_equalCreateUserMappingStmt(CreateUserMappingStmt *a, CreateUserMappingStmt *b)
{
	COMPARE_STRING_FIELD(username);
	COMPARE_STRING_FIELD(servername);
	COMPARE_NODE_FIELD(options);

	return true;
}

static bool
_equalAlterUserMappingStmt(AlterUserMappingStmt *a, AlterUserMappingStmt *b)
{
	COMPARE_STRING_FIELD(username);
	COMPARE_STRING_FIELD(servername);
	COMPARE_NODE_FIELD(options);

	return true;
}

static bool
_equalDropUserMappingStmt(DropUserMappingStmt *a, DropUserMappingStmt *b)
{
	COMPARE_STRING_FIELD(username);
	COMPARE_STRING_FIELD(servername);
	COMPARE_SCALAR_FIELD(missing_ok);

	return true;
}

static bool
_equalCreateTrigStmt(CreateTrigStmt *a, CreateTrigStmt *b)
{
	COMPARE_STRING_FIELD(trigname);
	COMPARE_NODE_FIELD(relation);
	COMPARE_NODE_FIELD(funcname);
	COMPARE_NODE_FIELD(args);
	COMPARE_SCALAR_FIELD(before);
	COMPARE_SCALAR_FIELD(row);
	if (strcmp(a->actions, b->actions) != 0)	/* in-line string field */
		return false;
	COMPARE_SCALAR_FIELD(isconstraint);
	COMPARE_SCALAR_FIELD(deferrable);
	COMPARE_SCALAR_FIELD(initdeferred);
	COMPARE_NODE_FIELD(constrrel);

	return true;
}

static bool
_equalDropPropertyStmt(DropPropertyStmt *a, DropPropertyStmt *b)
{
	COMPARE_NODE_FIELD(relation);
	COMPARE_STRING_FIELD(property);
	COMPARE_SCALAR_FIELD(removeType);
	COMPARE_SCALAR_FIELD(behavior);
	COMPARE_SCALAR_FIELD(missing_ok);

	return true;
}

static bool
_equalCreatePLangStmt(CreatePLangStmt *a, CreatePLangStmt *b)
{
	COMPARE_STRING_FIELD(plname);
	COMPARE_NODE_FIELD(plhandler);
	COMPARE_NODE_FIELD(plvalidator);
	COMPARE_SCALAR_FIELD(pltrusted);
	COMPARE_SCALAR_FIELD(plangOid);
	COMPARE_SCALAR_FIELD(plhandlerOid);
	COMPARE_SCALAR_FIELD(plvalidatorOid);

	return true;
}

static bool
_equalDropPLangStmt(DropPLangStmt *a, DropPLangStmt *b)
{
	COMPARE_STRING_FIELD(plname);
	COMPARE_SCALAR_FIELD(behavior);
	COMPARE_SCALAR_FIELD(missing_ok);

	return true;
}

static bool
_equalCreateRoleStmt(CreateRoleStmt *a, CreateRoleStmt *b)
{
	COMPARE_SCALAR_FIELD(stmt_type);
	COMPARE_STRING_FIELD(role);
	COMPARE_NODE_FIELD(options);
	COMPARE_SCALAR_FIELD(roleOid);

	return true;
}

static bool
_equalDenyLoginInterval(DenyLoginInterval *a, DenyLoginInterval *b)
{
	COMPARE_NODE_FIELD(start);
	COMPARE_NODE_FIELD(end);

	return true;
}

static bool
_equalDenyLoginPoint(DenyLoginPoint *a, DenyLoginPoint *b)
{
	COMPARE_NODE_FIELD(day);
	COMPARE_NODE_FIELD(time);
	
	return true;
}

static bool
_equalAlterRoleStmt(AlterRoleStmt *a, AlterRoleStmt *b)
{
	COMPARE_STRING_FIELD(role);
	COMPARE_NODE_FIELD(options);
	COMPARE_SCALAR_FIELD(action);

	return true;
}

static bool
_equalAlterRoleSetStmt(AlterRoleSetStmt *a, AlterRoleSetStmt *b)
{
	COMPARE_STRING_FIELD(role);
	COMPARE_STRING_FIELD(variable);
	COMPARE_NODE_FIELD(value);

	return true;
}

static bool
_equalDropRoleStmt(DropRoleStmt *a, DropRoleStmt *b)
{
	COMPARE_NODE_FIELD(roles);
	COMPARE_SCALAR_FIELD(missing_ok);

	return true;
}

static bool
_equalLockStmt(LockStmt *a, LockStmt *b)
{
	COMPARE_NODE_FIELD(relations);
	COMPARE_SCALAR_FIELD(mode);
	COMPARE_SCALAR_FIELD(nowait);

	return true;
}

static bool
_equalConstraintsSetStmt(ConstraintsSetStmt *a, ConstraintsSetStmt *b)
{
	COMPARE_NODE_FIELD(constraints);
	COMPARE_SCALAR_FIELD(deferred);

	return true;
}

static bool
_equalReindexStmt(ReindexStmt *a, ReindexStmt *b)
{
	COMPARE_SCALAR_FIELD(kind);
	COMPARE_NODE_FIELD(relation);
	COMPARE_STRING_FIELD(name);
	COMPARE_SCALAR_FIELD(do_system);
	COMPARE_SCALAR_FIELD(do_user);
	COMPARE_NODE_FIELD(new_ind_oids);

	return true;
}

static bool
_equalCreateSchemaStmt(CreateSchemaStmt *a, CreateSchemaStmt *b)
{
	COMPARE_STRING_FIELD(schemaname);
	COMPARE_STRING_FIELD(authid);
	COMPARE_NODE_FIELD(schemaElts);
	COMPARE_SCALAR_FIELD(istemp);
	COMPARE_SCALAR_FIELD(schemaOid);

	return true;
}

static bool
_equalCreateConversionStmt(CreateConversionStmt *a, CreateConversionStmt *b)
{
	COMPARE_NODE_FIELD(conversion_name);
	COMPARE_STRING_FIELD(for_encoding_name);
	COMPARE_STRING_FIELD(to_encoding_name);
	COMPARE_NODE_FIELD(func_name);
	COMPARE_SCALAR_FIELD(def);

	return true;
}

static bool
_equalCreateCastStmt(CreateCastStmt *a, CreateCastStmt *b)
{
	COMPARE_NODE_FIELD(sourcetype);
	COMPARE_NODE_FIELD(targettype);
	COMPARE_NODE_FIELD(func);
	COMPARE_SCALAR_FIELD(context);

	return true;
}

static bool
_equalDropCastStmt(DropCastStmt *a, DropCastStmt *b)
{
	COMPARE_NODE_FIELD(sourcetype);
	COMPARE_NODE_FIELD(targettype);
	COMPARE_SCALAR_FIELD(behavior);
	COMPARE_SCALAR_FIELD(missing_ok);

	return true;
}

static bool
_equalPrepareStmt(PrepareStmt *a, PrepareStmt *b)
{
	COMPARE_STRING_FIELD(name);
	COMPARE_NODE_FIELD(argtypes);
	COMPARE_NODE_FIELD(argtype_oids);
	COMPARE_NODE_FIELD(query);

	return true;
}

static bool
_equalExecuteStmt(ExecuteStmt *a, ExecuteStmt *b)
{
	COMPARE_STRING_FIELD(name);
	COMPARE_NODE_FIELD(into);
	COMPARE_NODE_FIELD(params);

	return true;
}

static bool
_equalDeallocateStmt(DeallocateStmt *a, DeallocateStmt *b)
{
	COMPARE_STRING_FIELD(name);

	return true;
}

static bool
_equalDropOwnedStmt(DropOwnedStmt *a, DropOwnedStmt *b)
{
	COMPARE_NODE_FIELD(roles);
	COMPARE_SCALAR_FIELD(behavior);

	return true;
}


static bool
_equalCreateQueueStmt(CreateQueueStmt *a, CreateQueueStmt *b)
{
	COMPARE_STRING_FIELD(queue);
	COMPARE_NODE_FIELD(options);
	return true;
}

static bool
_equalAlterQueueStmt(AlterQueueStmt *a, AlterQueueStmt *b)
{
	COMPARE_STRING_FIELD(queue);
	COMPARE_NODE_FIELD(options);
	return true;
}

static bool
_equalDropQueueStmt(DropQueueStmt *a, DropQueueStmt *b)
{
	COMPARE_STRING_FIELD(queue);
	return true;
}

/*
 * stuff from parsenodes.h
 */

static bool
_equalReassignOwnedStmt(ReassignOwnedStmt *a, ReassignOwnedStmt *b)
{
	COMPARE_NODE_FIELD(roles);
	COMPARE_NODE_FIELD(newrole);

	return true;
}

static bool
_equalAExpr(A_Expr *a, A_Expr *b)
{
	COMPARE_SCALAR_FIELD(kind);
	COMPARE_NODE_FIELD(name);
	COMPARE_NODE_FIELD(lexpr);
	COMPARE_NODE_FIELD(rexpr);
	COMPARE_SCALAR_FIELD(location);

	return true;
}

static bool
_equalColumnRef(ColumnRef *a, ColumnRef *b)
{
	COMPARE_NODE_FIELD(fields);
	COMPARE_SCALAR_FIELD(location);

	return true;
}

static bool
_equalParamRef(ParamRef *a, ParamRef *b)
{
	COMPARE_SCALAR_FIELD(number);
	/* do not compare 'location' field */

	return true;
}

static bool
_equalAConst(A_Const *a, A_Const *b)
{
	if (!equal(&a->val, &b->val))		/* hack for in-line Value field */
		return false;
	COMPARE_NODE_FIELD(typname);
	/* do not compare 'location' field */;

	return true;
}

static bool
_equalFuncCall(FuncCall *a, FuncCall *b)
{
	COMPARE_NODE_FIELD(funcname);
	COMPARE_NODE_FIELD(args);
    COMPARE_NODE_FIELD(agg_order);
	COMPARE_SCALAR_FIELD(agg_star);
	COMPARE_SCALAR_FIELD(agg_distinct);
	COMPARE_NODE_FIELD(over);
	COMPARE_SCALAR_FIELD(location);
	COMPARE_NODE_FIELD(agg_filter);

	return true;
}

static bool
_equalAIndices(A_Indices *a, A_Indices *b)
{
	COMPARE_NODE_FIELD(lidx);
	COMPARE_NODE_FIELD(uidx);

	return true;
}

static bool
_equalA_Indirection(A_Indirection *a, A_Indirection *b)
{
	COMPARE_NODE_FIELD(arg);
	COMPARE_NODE_FIELD(indirection);

	return true;
}

static bool
_equalResTarget(ResTarget *a, ResTarget *b)
{
	COMPARE_STRING_FIELD(name);
	COMPARE_NODE_FIELD(indirection);
	COMPARE_NODE_FIELD(val);
	COMPARE_SCALAR_FIELD(location);

	return true;
}

static bool
_equalTypeName(TypeName *a, TypeName *b)
{
	COMPARE_NODE_FIELD(names);
	COMPARE_SCALAR_FIELD(typid);
	COMPARE_SCALAR_FIELD(timezone);
	COMPARE_SCALAR_FIELD(setof);
	COMPARE_SCALAR_FIELD(pct_type);
	COMPARE_SCALAR_FIELD(typmod);
	COMPARE_NODE_FIELD(arrayBounds);
	COMPARE_SCALAR_FIELD(location);

	return true;
}

static bool
_equalTypeCast(TypeCast *a, TypeCast *b)
{
	COMPARE_NODE_FIELD(arg);
	COMPARE_NODE_FIELD(typname);

	return true;
}

static bool
_equalSortBy(SortBy *a, SortBy *b)
{
	COMPARE_SCALAR_FIELD(sortby_kind);
	COMPARE_NODE_FIELD(useOp);
	COMPARE_NODE_FIELD(node);

	return true;
}

static bool
_equalRangeSubselect(RangeSubselect *a, RangeSubselect *b)
{
	COMPARE_NODE_FIELD(subquery);
	COMPARE_NODE_FIELD(alias);

	return true;
}

static bool
_equalRangeFunction(RangeFunction *a, RangeFunction *b)
{
	COMPARE_NODE_FIELD(funccallnode);
	COMPARE_NODE_FIELD(alias);
	COMPARE_NODE_FIELD(coldeflist);

	return true;
}

static bool
_equalIndexElem(IndexElem *a, IndexElem *b)
{
	COMPARE_STRING_FIELD(name);
	COMPARE_NODE_FIELD(expr);
	COMPARE_NODE_FIELD(opclass);

	return true;
}

static bool
_equalColumnDef(ColumnDef *a, ColumnDef *b)
{
	COMPARE_STRING_FIELD(colname);
	COMPARE_NODE_FIELD(typname);
	COMPARE_SCALAR_FIELD(inhcount);
	COMPARE_SCALAR_FIELD(is_local);
	COMPARE_SCALAR_FIELD(is_not_null);
	COMPARE_NODE_FIELD(raw_default);
	COMPARE_SCALAR_FIELD(default_is_null);
	COMPARE_STRING_FIELD(cooked_default);
	COMPARE_NODE_FIELD(constraints);

	return true;
}

static bool
_equalConstraint(Constraint *a, Constraint *b)
{
	COMPARE_SCALAR_FIELD(contype);
	COMPARE_SCALAR_FIELD(conoid);
	COMPARE_STRING_FIELD(name);
	COMPARE_NODE_FIELD(raw_expr);
	COMPARE_STRING_FIELD(cooked_expr);
	COMPARE_NODE_FIELD(keys);
	COMPARE_NODE_FIELD(options);
	COMPARE_STRING_FIELD(indexspace);

	return true;
}

static bool
_equalDefElem(DefElem *a, DefElem *b)
{
	COMPARE_STRING_FIELD(defname);
	COMPARE_NODE_FIELD(arg);

	return true;
}

static bool
_equalLockingClause(LockingClause *a, LockingClause *b)
{
	COMPARE_NODE_FIELD(lockedRels);
	COMPARE_SCALAR_FIELD(forUpdate);
	COMPARE_SCALAR_FIELD(noWait);

	return true;
}

static bool
_equalRangeTblEntry(RangeTblEntry *a, RangeTblEntry *b)
{
	COMPARE_SCALAR_FIELD(rtekind);
	COMPARE_SCALAR_FIELD(relid);
	COMPARE_NODE_FIELD(subquery);
	COMPARE_NODE_FIELD(funcexpr);
	COMPARE_NODE_FIELD(funccoltypes);
	COMPARE_NODE_FIELD(funccoltypmods);
	COMPARE_VARLENA_FIELD(funcuserdata, -1);
	COMPARE_NODE_FIELD(values_lists);
	COMPARE_SCALAR_FIELD(jointype);
	COMPARE_NODE_FIELD(joinaliasvars);
	COMPARE_NODE_FIELD(alias);
	COMPARE_NODE_FIELD(eref);
	COMPARE_SCALAR_FIELD(inh);
	COMPARE_SCALAR_FIELD(inFromCl);
	COMPARE_SCALAR_FIELD(requiredPerms);
	COMPARE_SCALAR_FIELD(checkAsUser);
	COMPARE_STRING_FIELD(ctename);
	COMPARE_SCALAR_FIELD(ctelevelsup);
	COMPARE_SCALAR_FIELD(self_reference);
	COMPARE_NODE_FIELD(ctecoltypes);
	COMPARE_NODE_FIELD(ctecoltypmods);

	return true;
}

static bool
_equalSortClause(SortClause *a, SortClause *b)
{
	COMPARE_SCALAR_FIELD(tleSortGroupRef);
	COMPARE_SCALAR_FIELD(sortop);

	return true;
}

static bool
_equalGroupingClause(GroupingClause *a, GroupingClause *b)
{
	COMPARE_SCALAR_FIELD(groupType);
	COMPARE_NODE_FIELD(groupsets);

	return true;
}

static bool
_equalGroupingFunc(GroupingFunc *a, GroupingFunc *b)
{
	COMPARE_NODE_FIELD(args);
	COMPARE_SCALAR_FIELD(ngrpcols);

	return true;
}

static bool
_equalGrouping(Grouping *a __attribute__((unused)), Grouping *b __attribute__((unused)))

{
	return true;
}

static bool
_equalGroupId(GroupId *a __attribute__((unused)), GroupId *b __attribute__((unused)))
{
	return true;
}

static bool
_equalWindowSpecParse(WindowSpecParse *a, WindowSpecParse *b)
{
	COMPARE_STRING_FIELD(name);
	COMPARE_NODE_FIELD(elems);

	return true;
}

static bool
_equalWindowSpec(WindowSpec *a, WindowSpec *b)
{
	COMPARE_STRING_FIELD(name);
	COMPARE_STRING_FIELD(parent);
	COMPARE_NODE_FIELD(partition);
	COMPARE_NODE_FIELD(order);
	COMPARE_NODE_FIELD(frame);
	/* do not compare 'location' field */

	return true;
}

static bool
_equalWindowFrame(WindowFrame *a, WindowFrame *b)
{
	COMPARE_SCALAR_FIELD(is_rows);
	COMPARE_SCALAR_FIELD(is_between);
	COMPARE_NODE_FIELD(trail);
	COMPARE_NODE_FIELD(lead);
	COMPARE_SCALAR_FIELD(exclude);

	return true;
}

static bool
_equalWindowFrameEdge(WindowFrameEdge *a, WindowFrameEdge *b)
{
	COMPARE_SCALAR_FIELD(kind);
	COMPARE_NODE_FIELD(val);

	return true;
}

static bool
_equalPercentileExpr(PercentileExpr *a, PercentileExpr *b)
{
	COMPARE_SCALAR_FIELD(perctype);
	COMPARE_NODE_FIELD(args);
	COMPARE_SCALAR_FIELD(perckind);
	COMPARE_NODE_FIELD(sortClause);
	COMPARE_NODE_FIELD(sortTargets);
	COMPARE_NODE_FIELD(pcExpr);
	COMPARE_NODE_FIELD(tcExpr);
	/* do not compare 'location' field */

	return true;
}

static bool
_equalRowMarkClause(RowMarkClause *a, RowMarkClause *b)
{
	COMPARE_SCALAR_FIELD(rti);
	COMPARE_SCALAR_FIELD(forUpdate);
	COMPARE_SCALAR_FIELD(noWait);

	return true;
}

static bool
_equalWithClause(WithClause *a, WithClause *b)
{
	COMPARE_NODE_FIELD(ctes);
	COMPARE_SCALAR_FIELD(recursive);
	COMPARE_SCALAR_FIELD(location);
	
	return true;
}

static bool
_equalCommonTableExpr(CommonTableExpr *a, CommonTableExpr *b)
{
	COMPARE_STRING_FIELD(ctename);
	COMPARE_NODE_FIELD(aliascolnames);
	COMPARE_NODE_FIELD(ctequery);
	COMPARE_SCALAR_FIELD(location);
	COMPARE_SCALAR_FIELD(cterecursive);
	COMPARE_SCALAR_FIELD(cterefcount);
	COMPARE_NODE_FIELD(ctecolnames);
	COMPARE_NODE_FIELD(ctecoltypes);
	COMPARE_NODE_FIELD(ctecoltypmods);

	return true;
}

static bool
_equalFkConstraint(FkConstraint *a, FkConstraint *b)
{
	COMPARE_STRING_FIELD(constr_name);
	COMPARE_SCALAR_FIELD(constrOid);
	COMPARE_NODE_FIELD(pktable);
	COMPARE_NODE_FIELD(fk_attrs);
	COMPARE_NODE_FIELD(pk_attrs);
	COMPARE_SCALAR_FIELD(fk_matchtype);
	COMPARE_SCALAR_FIELD(fk_upd_action);
	COMPARE_SCALAR_FIELD(fk_del_action);
	COMPARE_SCALAR_FIELD(deferrable);
	COMPARE_SCALAR_FIELD(initdeferred);
	COMPARE_SCALAR_FIELD(skip_validation);
	COMPARE_SCALAR_FIELD(old_pktable_oid);
	COMPARE_SCALAR_FIELD(trig1Oid);
	COMPARE_SCALAR_FIELD(trig2Oid);
	COMPARE_SCALAR_FIELD(trig3Oid);
	COMPARE_SCALAR_FIELD(trig4Oid);

	return true;
}


static bool
_equalTableValueExpr(TableValueExpr *a, TableValueExpr *b)
{
	COMPARE_NODE_FIELD(subquery);

	return true;
}

static bool
_equalAlterTypeStmt(AlterTypeStmt *a, AlterTypeStmt *b)
{
	COMPARE_NODE_FIELD(typname);
	COMPARE_NODE_FIELD(encoding);

	return true;
}

/*
 * Stuff from pg_list.h
 */

static bool
_equalList(List *a, List *b)
{
	ListCell   *item_a;
	ListCell   *item_b;

	/*
	 * Try to reject by simple scalar checks before grovelling through all the
	 * list elements...
	 */
	COMPARE_SCALAR_FIELD(type);
	COMPARE_SCALAR_FIELD(length);

	/*
	 * We place the switch outside the loop for the sake of efficiency; this
	 * may not be worth doing...
	 */
	switch (a->type)
	{
		case T_List:
			forboth(item_a, a, item_b, b)
			{
				if (!equal(lfirst(item_a), lfirst(item_b)))
					return false;
			}
			break;
		case T_IntList:
			forboth(item_a, a, item_b, b)
			{
				if (lfirst_int(item_a) != lfirst_int(item_b))
					return false;
			}
			break;
		case T_OidList:
			forboth(item_a, a, item_b, b)
			{
				if (lfirst_oid(item_a) != lfirst_oid(item_b))
					return false;
			}
			break;
		default:
			elog(ERROR, "unrecognized list node type: %d",
				 (int) a->type);
			return false;		/* keep compiler quiet */
	}

	/*
	 * If we got here, we should have run out of elements of both lists
	 */
	Assert(item_a == NULL);
	Assert(item_b == NULL);

	return true;
}

/*
 * Stuff from value.h
 */

static bool
_equalValue(Value *a, Value *b)
{
	COMPARE_SCALAR_FIELD(type);

	switch (a->type)
	{
		case T_Integer:
			COMPARE_SCALAR_FIELD(val.ival);
			break;
		case T_Float:
		case T_String:
		case T_BitString:
			COMPARE_STRING_FIELD(val.str);
			break;
		case T_Null:
			/* nothing to do */
			break;
		default:
			elog(ERROR, "unrecognized node type: %d", (int) a->type);
			break;
	}

	return true;
}

/*
 * equal
 *	  returns whether two nodes are equal
 */
bool
equal(void *a, void *b)
{
	bool		retval;

	if (a == b)
		return true;

	/*
	 * note that a!=b, so only one of them can be NULL
	 */
	if (a == NULL || b == NULL)
		return false;

	/*
	 * are they the same type of nodes?
	 */
	if (nodeTag(a) != nodeTag(b))
		return false;

	switch (nodeTag(a))
	{
			/*
			 * PRIMITIVE NODES
			 */
		case T_Alias:
			retval = _equalAlias(a, b);
			break;
		case T_RangeVar:
			retval = _equalRangeVar(a, b);
			break;
		case T_IntoClause:
			retval = _equalIntoClause(a, b);
			break;
		case T_Var:
			retval = _equalVar(a, b);
			break;
		case T_Const:
			retval = _equalConst(a, b);
			break;
		case T_Param:
			retval = _equalParam(a, b);
			break;
		case T_Aggref:
			retval = _equalAggref(a, b);
			break;
		case T_AggOrder:
			retval = _equalAggOrder(a, b);
			break;
		case T_WindowRef:
			retval = _equalWindowRef(a, b);
			break;
		case T_ArrayRef:
			retval = _equalArrayRef(a, b);
			break;
		case T_FuncExpr:
			retval = _equalFuncExpr(a, b);
			break;
		case T_OpExpr:
			retval = _equalOpExpr(a, b);
			break;
		case T_DistinctExpr:
			retval = _equalDistinctExpr(a, b);
			break;
		case T_ScalarArrayOpExpr:
			retval = _equalScalarArrayOpExpr(a, b);
			break;
		case T_BoolExpr:
			retval = _equalBoolExpr(a, b);
			break;
		case T_SubLink:
			retval = _equalSubLink(a, b);
			break;
		case T_SubPlan:
			retval = _equalSubPlan(a, b);
			break;
		case T_FieldSelect:
			retval = _equalFieldSelect(a, b);
			break;
		case T_FieldStore:
			retval = _equalFieldStore(a, b);
			break;
		case T_RelabelType:
			retval = _equalRelabelType(a, b);
			break;
		case T_ConvertRowtypeExpr:
			retval = _equalConvertRowtypeExpr(a, b);
			break;
		case T_CaseExpr:
			retval = _equalCaseExpr(a, b);
			break;
		case T_CaseWhen:
			retval = _equalCaseWhen(a, b);
			break;
		case T_CaseTestExpr:
			retval = _equalCaseTestExpr(a, b);
			break;
		case T_ArrayExpr:
			retval = _equalArrayExpr(a, b);
			break;
		case T_RowExpr:
			retval = _equalRowExpr(a, b);
			break;
		case T_RowCompareExpr:
			retval = _equalRowCompareExpr(a, b);
			break;
		case T_CoalesceExpr:
			retval = _equalCoalesceExpr(a, b);
			break;
		case T_MinMaxExpr:
			retval = _equalMinMaxExpr(a, b);
			break;
		case T_NullIfExpr:
			retval = _equalNullIfExpr(a, b);
			break;
		case T_NullTest:
			retval = _equalNullTest(a, b);
			break;
		case T_BooleanTest:
			retval = _equalBooleanTest(a, b);
			break;
		case T_CoerceToDomain:
			retval = _equalCoerceToDomain(a, b);
			break;
		case T_CoerceToDomainValue:
			retval = _equalCoerceToDomainValue(a, b);
			break;
		case T_SetToDefault:
			retval = _equalSetToDefault(a, b);
			break;
		case T_CurrentOfExpr:
			retval = _equalCurrentOfExpr(a, b);
			break;
		case T_TargetEntry:
			retval = _equalTargetEntry(a, b);
			break;
		case T_RangeTblRef:
			retval = _equalRangeTblRef(a, b);
			break;
		case T_FromExpr:
			retval = _equalFromExpr(a, b);
			break;
		case T_Flow:
			retval = _equalFlow(a, b);
			break;
		case T_JoinExpr:
			retval = _equalJoinExpr(a, b);
			break;

			/*
			 * RELATION NODES
			 */
		case T_PathKeyItem:
			retval = _equalPathKeyItem(a, b);
			break;
		case T_RestrictInfo:
			retval = _equalRestrictInfo(a, b);
			break;
		case T_OuterJoinInfo:
			retval = _equalOuterJoinInfo(a, b);
			break;
		case T_InClauseInfo:
			retval = _equalInClauseInfo(a, b);
			break;
		case T_AppendRelInfo:
			retval = _equalAppendRelInfo(a, b);
			break;
		case T_List:
		case T_IntList:
		case T_OidList:
			retval = _equalList(a, b);
			break;

		case T_Integer:
		case T_Float:
		case T_String:
		case T_BitString:
		case T_Null:
			retval = _equalValue(a, b);
			break;

			/*
			 * PARSE NODES
			 */
		case T_Query:
			retval = _equalQuery(a, b);
			break;
		case T_InsertStmt:
			retval = _equalInsertStmt(a, b);
			break;
		case T_DeleteStmt:
			retval = _equalDeleteStmt(a, b);
			break;
		case T_UpdateStmt:
			retval = _equalUpdateStmt(a, b);
			break;
		case T_SelectStmt:
			retval = _equalSelectStmt(a, b);
			break;
		case T_SetOperationStmt:
			retval = _equalSetOperationStmt(a, b);
			break;
		case T_AlterTableStmt:
			retval = _equalAlterTableStmt(a, b);
			break;
		case T_AlterTableCmd:
			retval = _equalAlterTableCmd(a, b);
			break;
		case T_InheritPartitionCmd:
			retval = _equalInheritPartitionCmd(a, b);
			break;
		case T_AlterPartitionCmd:
			retval = _equalAlterPartitionCmd(a, b);
			break;
		case T_AlterPartitionId:
			retval = _equalAlterPartitionId(a, b);
			break;
		case T_AlterDomainStmt:
			retval = _equalAlterDomainStmt(a, b);
			break;
		case T_GrantStmt:
			retval = _equalGrantStmt(a, b);
			break;
		case T_GrantRoleStmt:
			retval = _equalGrantRoleStmt(a, b);
			break;
		case T_DeclareCursorStmt:
			retval = _equalDeclareCursorStmt(a, b);
			break;
		case T_ClosePortalStmt:
			retval = _equalClosePortalStmt(a, b);
			break;
		case T_ClusterStmt:
			retval = _equalClusterStmt(a, b);
			break;
		case T_SingleRowErrorDesc:
			retval = _equalSingleRowErrorDesc(a, b);
			break;
		case T_CopyStmt:
			retval = _equalCopyStmt(a, b);
			break;
		case T_CreateStmt:
			retval = _equalCreateStmt(a, b);
			break;
		case T_ColumnReferenceStorageDirective:
			retval = _equalColumnReferenceStorageDirective(a, b);
			break;
		case T_PartitionRangeItem:
			retval = _equalPartitionRangeItem(a, b);
			break;
		case T_ExtTableTypeDesc:
			retval = _equalExtTableTypeDesc(a, b);
			break;
		case T_CreateExternalStmt:
			retval = _equalCreateExternalStmt(a, b);
			break;
		case T_CreateForeignStmt:
			retval = _equalCreateForeignStmt(a, b);
			break;			
		case T_InhRelation:
			retval = _equalInhRelation(a, b);
			break;
		case T_DefineStmt:
			retval = _equalDefineStmt(a, b);
			break;
		case T_DropStmt:
			retval = _equalDropStmt(a, b);
			break;
		case T_TruncateStmt:
			retval = _equalTruncateStmt(a, b);
			break;
		case T_CommentStmt:
			retval = _equalCommentStmt(a, b);
			break;
		case T_FetchStmt:
			retval = _equalFetchStmt(a, b);
			break;
		case T_IndexStmt:
			retval = _equalIndexStmt(a, b);
			break;
		case T_CreateFunctionStmt:
			retval = _equalCreateFunctionStmt(a, b);
			break;
		case T_FunctionParameter:
			retval = _equalFunctionParameter(a, b);
			break;
		case T_AlterFunctionStmt:
			retval = _equalAlterFunctionStmt(a, b);
			break;
		case T_RemoveFuncStmt:
			retval = _equalRemoveFuncStmt(a, b);
			break;
		case T_RemoveOpClassStmt:
			retval = _equalRemoveOpClassStmt(a, b);
			break;
		case T_RenameStmt:
			retval = _equalRenameStmt(a, b);
			break;
		case T_AlterObjectSchemaStmt:
			retval = _equalAlterObjectSchemaStmt(a, b);
			break;
		case T_AlterOwnerStmt:
			retval = _equalAlterOwnerStmt(a, b);
			break;
		case T_RuleStmt:
			retval = _equalRuleStmt(a, b);
			break;
		case T_NotifyStmt:
			retval = _equalNotifyStmt(a, b);
			break;
		case T_ListenStmt:
			retval = _equalListenStmt(a, b);
			break;
		case T_UnlistenStmt:
			retval = _equalUnlistenStmt(a, b);
			break;
		case T_TransactionStmt:
			retval = _equalTransactionStmt(a, b);
			break;
		case T_CompositeTypeStmt:
			retval = _equalCompositeTypeStmt(a, b);
			break;
		case T_ViewStmt:
			retval = _equalViewStmt(a, b);
			break;
		case T_LoadStmt:
			retval = _equalLoadStmt(a, b);
			break;
		case T_CreateDomainStmt:
			retval = _equalCreateDomainStmt(a, b);
			break;
		case T_CreateOpClassStmt:
			retval = _equalCreateOpClassStmt(a, b);
			break;
		case T_CreateOpClassItem:
			retval = _equalCreateOpClassItem(a, b);
			break;
		case T_CreatedbStmt:
			retval = _equalCreatedbStmt(a, b);
			break;
		case T_AlterDatabaseStmt:
			retval = _equalAlterDatabaseStmt(a, b);
			break;
		case T_AlterDatabaseSetStmt:
			retval = _equalAlterDatabaseSetStmt(a, b);
			break;
		case T_DropdbStmt:
			retval = _equalDropdbStmt(a, b);
			break;
		case T_VacuumStmt:
			retval = _equalVacuumStmt(a, b);
			break;
		case T_ExplainStmt:
			retval = _equalExplainStmt(a, b);
			break;
		case T_CreateSeqStmt:
			retval = _equalCreateSeqStmt(a, b);
			break;
		case T_AlterSeqStmt:
			retval = _equalAlterSeqStmt(a, b);
			break;
		case T_VariableSetStmt:
			retval = _equalVariableSetStmt(a, b);
			break;
		case T_VariableShowStmt:
			retval = _equalVariableShowStmt(a, b);
			break;
		case T_VariableResetStmt:
			retval = _equalVariableResetStmt(a, b);
			break;
		case T_CreateFileSpaceStmt:
			retval = _equalCreateFileSpaceStmt(a, b);
			break;
		case T_CreateTableSpaceStmt:
			retval = _equalCreateTableSpaceStmt(a, b);
			break;
		case T_CreateFdwStmt:
			retval = _equalCreateFdwStmt(a, b);
			break;
		case T_AlterFdwStmt:
			retval = _equalAlterFdwStmt(a, b);
			break;
		case T_DropFdwStmt:
			retval = _equalDropFdwStmt(a, b);
			break;
		case T_CreateForeignServerStmt:
			retval = _equalCreateForeignServerStmt(a, b);
			break;
		case T_AlterForeignServerStmt:
			retval = _equalAlterForeignServerStmt(a, b);
			break;
		case T_DropForeignServerStmt:
			retval = _equalDropForeignServerStmt(a, b);
			break;
		case T_CreateUserMappingStmt:
			retval = _equalCreateUserMappingStmt(a, b);
			break;
		case T_AlterUserMappingStmt:
			retval = _equalAlterUserMappingStmt(a, b);
			break;
		case T_DropUserMappingStmt:
			retval = _equalDropUserMappingStmt(a, b);
			break;
		case T_CreateTrigStmt:
			retval = _equalCreateTrigStmt(a, b);
			break;
		case T_DropPropertyStmt:
			retval = _equalDropPropertyStmt(a, b);
			break;
		case T_CreatePLangStmt:
			retval = _equalCreatePLangStmt(a, b);
			break;
		case T_DropPLangStmt:
			retval = _equalDropPLangStmt(a, b);
			break;
		case T_CreateRoleStmt:
			retval = _equalCreateRoleStmt(a, b);
			break;
		case T_AlterRoleStmt:
			retval = _equalAlterRoleStmt(a, b);
			break;
		case T_AlterRoleSetStmt:
			retval = _equalAlterRoleSetStmt(a, b);
			break;
		case T_DropRoleStmt:
			retval = _equalDropRoleStmt(a, b);
			break;
		case T_LockStmt:
			retval = _equalLockStmt(a, b);
			break;
		case T_ConstraintsSetStmt:
			retval = _equalConstraintsSetStmt(a, b);
			break;
		case T_ReindexStmt:
			retval = _equalReindexStmt(a, b);
			break;
		case T_CheckPointStmt:
			retval = true;
			break;
		case T_CreateSchemaStmt:
			retval = _equalCreateSchemaStmt(a, b);
			break;
		case T_CreateConversionStmt:
			retval = _equalCreateConversionStmt(a, b);
			break;
		case T_CreateCastStmt:
			retval = _equalCreateCastStmt(a, b);
			break;
		case T_DropCastStmt:
			retval = _equalDropCastStmt(a, b);
			break;
		case T_PrepareStmt:
			retval = _equalPrepareStmt(a, b);
			break;
		case T_ExecuteStmt:
			retval = _equalExecuteStmt(a, b);
			break;
		case T_DeallocateStmt:
			retval = _equalDeallocateStmt(a, b);
			break;
		case T_DropOwnedStmt:
			retval = _equalDropOwnedStmt(a, b);
			break;

		case T_ReassignOwnedStmt:
			retval = _equalReassignOwnedStmt(a, b);
			break;

		case T_CreateQueueStmt:
			retval = _equalCreateQueueStmt(a, b);
			break;
		case T_AlterQueueStmt:
			retval = _equalAlterQueueStmt(a, b);
			break;
		case T_DropQueueStmt:
			retval = _equalDropQueueStmt(a, b);
			break;
		case T_A_Expr:
			retval = _equalAExpr(a, b);
			break;
		case T_ColumnRef:
			retval = _equalColumnRef(a, b);
			break;
		case T_ParamRef:
			retval = _equalParamRef(a, b);
			break;
		case T_A_Const:
			retval = _equalAConst(a, b);
			break;
		case T_FuncCall:
			retval = _equalFuncCall(a, b);
			break;
		case T_A_Indices:
			retval = _equalAIndices(a, b);
			break;
		case T_A_Indirection:
			retval = _equalA_Indirection(a, b);
			break;
		case T_ResTarget:
			retval = _equalResTarget(a, b);
			break;
		case T_TypeCast:
			retval = _equalTypeCast(a, b);
			break;
		case T_SortBy:
			retval = _equalSortBy(a, b);
			break;
		case T_RangeSubselect:
			retval = _equalRangeSubselect(a, b);
			break;
		case T_RangeFunction:
			retval = _equalRangeFunction(a, b);
			break;
		case T_TypeName:
			retval = _equalTypeName(a, b);
			break;
		case T_IndexElem:
			retval = _equalIndexElem(a, b);
			break;
		case T_ColumnDef:
			retval = _equalColumnDef(a, b);
			break;
		case T_Constraint:
			retval = _equalConstraint(a, b);
			break;
		case T_DefElem:
			retval = _equalDefElem(a, b);
			break;
		case T_LockingClause:
			retval = _equalLockingClause(a, b);
			break;
		case T_RangeTblEntry:
			retval = _equalRangeTblEntry(a, b);
			break;
		case T_SortClause:
			retval = _equalSortClause(a, b);
			break;
		case T_GroupClause:
			/* GroupClause is equivalent to SortClause */
			retval = _equalSortClause(a, b);
			break;
		case T_GroupingClause:
			retval = _equalGroupingClause(a, b);
			break;
		case T_GroupingFunc:
			retval = _equalGroupingFunc(a, b);
			break;
		case T_Grouping:
			retval = _equalGrouping(a, b);
			break;
		case T_GroupId:
			retval = _equalGroupId(a, b);
			break;
		case T_WindowSpecParse:
			retval = _equalWindowSpecParse(a, b);
			break;
		case T_WindowSpec:
			retval = _equalWindowSpec(a, b);
			break;
		case T_WindowFrame:
			retval = _equalWindowFrame(a, b);
			break;
		case T_WindowFrameEdge:
			retval = _equalWindowFrameEdge(a, b);
			break;
		case T_PercentileExpr:
			retval = _equalPercentileExpr(a, b);
			break;
		case T_RowMarkClause:
			retval = _equalRowMarkClause(a, b);
			break;
		case T_WithClause:
			retval = _equalWithClause(a,b);
			break;
		case T_CommonTableExpr:
			retval = _equalCommonTableExpr(a,b);
			break;
		case T_FkConstraint:
			retval = _equalFkConstraint(a, b);
			break;
		case T_PrivGrantee:
			retval = _equalPrivGrantee(a, b);
			break;
		case T_FuncWithArgs:
			retval = _equalFuncWithArgs(a, b);
			break;
		case T_TableValueExpr:
			retval = _equalTableValueExpr(a, b);
			break;
		case T_DenyLoginInterval:
			retval = _equalDenyLoginInterval(a, b);
			break;
		case T_DenyLoginPoint:
			retval = _equalDenyLoginPoint(a, b);
			break;
		case T_AlterTypeStmt:
			retval = _equalAlterTypeStmt(a, b);
			break;

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(a));
			retval = false;		/* keep compiler quiet */
			break;
	}

	return retval;
}
