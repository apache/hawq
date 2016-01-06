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
 * primnodes.h
 *	  Definitions for "primitive" node types, those that are used in more
 *	  than one of the parse/plan/execute stages of the query pipeline.
 *	  Currently, these are mostly nodes for executable expressions
 *	  and join trees.
 *
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/nodes/primnodes.h,v 1.117 2006/10/04 00:30:09 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PRIMNODES_H
#define PRIMNODES_H

#include "access/attnum.h"
#include "nodes/pg_list.h"
#include "nodes/params.h"  /* For ParamListInfoData */
#include "cdb/cdbpathlocus.h" /* For CdbLocusType */


/* ----------------------------------------------------------------
 *						node definitions
 * ----------------------------------------------------------------
 */

/*
 * Alias -
 *	  specifies an alias for a range variable; the alias might also
 *	  specify renaming of columns within the table.
 *
 * Note: colnames is a list of Value nodes (always strings).  In Alias structs
 * associated with RTEs, there may be entries corresponding to dropped
 * columns; these are normally empty strings ("").	See parsenodes.h for info.
 */
typedef struct Alias
{
	NodeTag		type;
	char	   *aliasname;		/* aliased rel name (never qualified) */
	List	   *colnames;		/* optional list of column aliases */
} Alias;

typedef enum InhOption
{
	INH_NO,						/* Do NOT scan child tables */
	INH_YES,					/* DO scan child tables */
	INH_DEFAULT					/* Use current SQL_inheritance option */
} InhOption;

/* What to do at commit time for temporary relations */
typedef enum OnCommitAction
{
	ONCOMMIT_NOOP,				/* No ON COMMIT clause (do nothing) */
	ONCOMMIT_PRESERVE_ROWS,		/* ON COMMIT PRESERVE ROWS (do nothing) */
	ONCOMMIT_DELETE_ROWS,		/* ON COMMIT DELETE ROWS */
	ONCOMMIT_DROP				/* ON COMMIT DROP */
} OnCommitAction;

/*
 * RangeVar - range variable, used in FROM clauses
 *
 * Also used to represent table names in utility statements; there, the alias
 * field is not used, and inhOpt shows whether to apply the operation
 * recursively to child tables.  In some contexts it is also useful to carry
 * a TEMP table indication here.
 */
typedef struct RangeVar
{
	NodeTag		type;
	char	   *catalogname;	/* the catalog (database) name, or NULL */
	char	   *schemaname;		/* the schema name, or NULL */
	char	   *relname;		/* the relation/sequence name */
	InhOption	inhOpt;			/* expand rel by inheritance? recursively act
								 * on children? */
	bool		istemp;			/* is this a temp relation/sequence? */
	Alias	   *alias;			/* table alias & optional column aliases */
	int			location;		/* token location, or -1 if unknown */
} RangeVar;


typedef struct TableOidInfo
{
	Oid         relOid;			/* If the heap is (re-)created, create  with this relOid */
	Oid         comptypeOid;
	Oid		    toastOid;		/* if toast table needed, use this for the relOid of the toast */
	Oid		    toastIndexOid;	/* if toast table needed, use this for the relOid of the index */
	Oid			toastComptypeOid;
	Oid		    aosegOid;		/* if ao segment table needed, use this for the relOid of the aoseg table */
	Oid		    aosegIndexOid;	/* if ao segment table needed, use this for the relOid of the aoseg index */
	Oid			aosegComptypeOid;
	Oid		    aoblkdirOid;		/* if ao blkdir table needed, use this for the relOid of the aoblkdir table */
	Oid		    aoblkdirIndexOid;	/* if ao blkdir table needed, use this for the relOid of the aoblkdir index */
	Oid			aoblkdirComptypeOid;
} TableOidInfo;

/*
 * IntoClause - target information for SELECT INTO and CREATE TABLE AS
 */
typedef struct IntoClause
{
	NodeTag		type;

	RangeVar   *rel;			/* target relation name */
	List	   *colNames;		/* column names to assign, or NIL */
	List	   *options;		/* options from WITH clause */
	OnCommitAction onCommit;	/* what do we do at COMMIT? */
	char	   *tableSpaceName; /* table space to use, or NULL */
	
	/* MPP */
	TableOidInfo oidInfo;
} IntoClause;


/* ----------------------------------------------------------------
 *					node types for executable expressions
 * ----------------------------------------------------------------
 */

/*
 * Expr - generic superclass for executable-expression nodes
 *
 * All node types that are used in executable expression trees should derive
 * from Expr (that is, have Expr as their first field).  Since Expr only
 * contains NodeTag, this is a formality, but it is an easy form of
 * documentation.  See also the ExprState node types in execnodes.h.
 */
typedef struct Expr
{
	NodeTag		type;
} Expr;

/*
 * Var - expression node representing a variable (ie, a table column)
 *
 * Note: during parsing/planning, varnoold/varoattno are always just copies
 * of varno/varattno.  At the tail end of planning, Var nodes appearing in
 * upper-level plan nodes are reassigned to point to the outputs of their
 * subplans; for example, in a join node varno becomes INNER or OUTER and
 * varattno becomes the index of the proper element of that subplan's target
 * list.  But varnoold/varoattno continue to hold the original values.
 * The code doesn't really need varnoold/varoattno, but they are very useful
 * for debugging and interpreting completed plans, so we keep them around.
 */
#define    INNER		65000
#define    OUTER		65001

#define    PRS2_OLD_VARNO			1
#define    PRS2_NEW_VARNO			2

typedef struct Var
{
	Expr		xpr;
	Index		varno;			/* index of this var's relation in the range
								 * table (could also be INNER or OUTER) */
	AttrNumber	varattno;		/* attribute number of this var, or zero for
								 * all */
	Oid			vartype;		/* pg_type OID for the type of this var */
	int32		vartypmod;		/* pg_attribute typmod value */
	Index		varlevelsup;	/* for subquery variables referencing outer
								 * relations; 0 in a normal var, >0 means N
								 * levels up */
	Index		varnoold;		/* original value of varno, for debugging */
	AttrNumber	varoattno;		/* original value of varattno */
	int			location;		/* token location, or -1 if unknown */
} Var;

/*
 * Const
 */
typedef struct Const
{
	Expr		xpr;
	Oid			consttype;		/* pg_type OID of the constant's datatype */
	int32		consttypmod;	/* typmod value, if any */
	int			constlen;		/* typlen of the constant's datatype */
	Datum		constvalue;		/* the constant's value */
	bool		constisnull;	/* whether the constant is null (if true,
								 * constvalue is undefined) */
	bool		constbyval;		/* whether this datatype is passed by value.
								 * If true, then all the information is stored
								 * in the Datum. If false, then the Datum
								 * contains a pointer to the information. */
	int			location;		/* token location, or -1 if unknown */
} Const;

/* ----------------
 * Param
 *		paramkind - specifies the kind of parameter. The possible values
 *		for this field are:
 *
 *		PARAM_EXTERN:  The parameter value is supplied from outside the plan.
 *				Such parameters are numbered from 1 to n.
 *
 *		PARAM_EXEC:  The parameter is an internal executor parameter, used
 *				for passing values into and out of sub-queries.
 *				For historical reasons, such parameters are numbered from 0.
 *				These numbers are independent of PARAM_EXTERN numbers.
 *
 *		PARAM_SUBLINK:	The parameter represents an output column of a SubLink
 *				node's sub-select.  The column number is contained in the
 *				`paramid' field.  (This type of Param is converted to
 *				PARAM_EXEC during planning.)
 *
 * Note: currently, paramtypmod is valid for PARAM_SUBLINK Params, and for
 * PARAM_EXEC Params generated from them; it is always -1 for PARAM_EXTERN
 * params, since the APIs that supply values for such parameters don't carry
 * any typmod info.
 * ----------------
 */
typedef enum ParamKind
{
	PARAM_EXTERN,
	PARAM_EXEC,
	PARAM_EXEC_REMOTE, /* MPP ???? */
	PARAM_SUBLINK
} ParamKind;

typedef struct Param
{
	Expr		xpr;
	ParamKind	paramkind;		/* kind of parameter. See above */
	int			paramid;		/* numeric ID for parameter */
	Oid			paramtype;		/* pg_type OID of parameter's datatype */
	int32		paramtypmod;	/* typmod value, if known */
	int			location;		/* token location, or -1 if unknown */
} Param;

/* AggStage enumeration indicates how the executor should handle an
 * Aggref node.
 */
typedef enum AggStage
{
	AGGSTAGE_NORMAL = 0,
	AGGSTAGE_PARTIAL, /* First (lower, earlier) stage of 2-stage aggregation. */
	AGGSTAGE_INTERMEDIATE, /* The intermediate stage between AGGSTAGE_PARTIAL and
							* AGGSTAGE_FINAL that handles the higher aggregation
							* level in a (partial) ROLLUP grouping extension
							* query.
							*/
	AGGSTAGE_FINAL /* Second (upper, later) stage of 2-stage aggregation. */
} AggStage;



/*
 * AggOrder describes ordering information for ordered aggregates
 */
typedef struct AggOrder
{
    Expr        xpr;
    bool        sortImplicit;   /* Implict or explicit ordering? */
    List       *sortTargets;    /* Targetlist for order by clause */
    List       *sortClause;     /* Sort clause for the aggregate */
} AggOrder;


/*
 * Aggref
 */
typedef struct Aggref
{
	Expr		xpr;
	Oid			aggfnoid;		/* pg_proc Oid of the aggregate */
	Oid			aggtype;		/* type Oid of result of the aggregate */
	List	   *args;			/* arguments to the aggregate */
	Index		agglevelsup;	/* > 0 if agg belongs to outer query */
	bool		aggstar;		/* TRUE if argument list was really '*' */
	bool		aggdistinct;	/* TRUE if it's agg(DISTINCT ...) */
	AggStage	aggstage;		/* MPP: 2-stage? If so, which stage */
    AggOrder   *aggorder;       /* Ordered aggregate definition */
	int			location;		/* token location, or -1 if unknown */
} Aggref;


/*
 * Grouping: describe the hidden GROUPING column for grouping extensions.
 *
 * Defined for making it easily to distinguish this column with others.
 *
 * Used with GroupingFunc to distinguish 'null' values that are created
 * through grouping with those that are in the raw data. See also GroupingFunc
 * for more details.
 */
typedef struct Grouping
{
	Expr        xpr;
} Grouping;

/*
 * GroupId -
 *    representation of the hidden GROUP_ID column for grouping extensions.
 *
 * Defined to make it easy to distinguish this column from others.
 *
 * This is used to determine whether output tuples are coming from
 * duplicate grouping sets. For example, a table 
 *
 *    test (a integer, b integer)
 *
 * has two rows:
 *
 *       (1,2), (1,2).
 *
 * Consider a rollup clause "rollup(a),a", which contains a grouping
 * set (a) twice. Therefore, the query
 *
 *    select a,sum(b),group_id() from test group by rollup(a),a;
 *
 * returns two rows:
 *
 *    1,4,0
 *    1,4,1
 *
 * The GROUP_ID value 0 indicates this tuple is from the grouping set (a).
 * The value 1 indicates this tuple is from the first duplicate grouping set of
 * (a).
 *
 * This query can be also re-written to the following:
 *
 *    select a,avg(b),0 from test group by a
 *      union all
 *    select a,avg(b),1 from test group by a;
 */
typedef struct GroupId
{
	Expr      xpr;
} GroupId;

/* WinStage enumeration indicates what stage of the evaluation of a
 * window function is expressed by a WindowRef.
 */
typedef enum WinStage
{
	WINSTAGE_IMMEDIATE = 0, /* Evaluate window function. */
	WINSTAGE_PRELIMINARY, /* Evaluate preliminary function. */
	WINSTAGE_ROWKEY /* WINSTAGE_IMMEDIATE for row key generation. */
} WinStage;
 
/*
 * WindowRef: describes a window function call
 *
 * In a query tree, a WindowRef corresponds to a SQL window function
 * call.  In a plan tree, a WindowRef is an expression the corresponds
 * to some or all of the calculation of the window function result.
 * 
 */
typedef struct WindowRef
{
	Expr		xpr;
	Oid			winfnoid;		/* pg_proc Oid of the window function */
	Oid			restype;		/* type Oid of result of the window function */
	List	   *args;			/* arguments */	
	Index		winlevelsup;	/* > 0 if win belongs to outer query  */
	bool		windistinct;	/* TRUE if it's agg(DISTINCT ...) */
	Index		winspec;		/* index into Query window clause */
	
	/* Following fields are significant only in a Plan tree. */
	Index		winindex;		/* RefInfo index during planning. */
	WinStage	winstage;		/* Stage of execution. */
	Index		winlevel;		/* Position of corresponding WindowKey in
								 * the Window node. */
	int			location;		/* token location, or -1 if unknown */
} WindowRef;


/* ----------------
 *	ArrayRef: describes an array subscripting operation
 *
 * An ArrayRef can describe fetching a single element from an array,
 * fetching a subarray (array slice), storing a single element into
 * an array, or storing a slice.  The "store" cases work with an
 * initial array value and a source value that is inserted into the
 * appropriate part of the array; the result of the operation is an
 * entire new modified array value.
 *
 * If reflowerindexpr = NIL, then we are fetching or storing a single array
 * element at the subscripts given by refupperindexpr.	Otherwise we are
 * fetching or storing an array slice, that is a rectangular subarray
 * with lower and upper bounds given by the index expressions.
 * reflowerindexpr must be the same length as refupperindexpr when it
 * is not NIL.
 *
 * Note: the result datatype is the element type when fetching a single
 * element; but it is the array type when doing subarray fetch or either
 * type of store.
 * ----------------
 */
typedef struct ArrayRef
{
	Expr		xpr;
	Oid			refrestype;		/* type of the result of the ArrayRef
								 * operation */
	Oid			refarraytype;	/* type of the array proper */
	Oid			refelemtype;	/* type of the array elements */
	int32		reftypmod;		/* typmod of the array (and elements too) */
	List	   *refupperindexpr;/* expressions that evaluate to upper array
								 * indexes */
	List	   *reflowerindexpr;/* expressions that evaluate to lower array
								 * indexes */
	Expr	   *refexpr;		/* the expression that evaluates to an array
								 * value */
	Expr	   *refassgnexpr;	/* expression for the source value, or NULL if
								 * fetch */
} ArrayRef;

/*
 * CoercionContext - distinguishes the allowed set of type casts
 *
 * NB: ordering of the alternatives is significant; later (larger) values
 * allow more casts than earlier ones.
 */
typedef enum CoercionContext
{
	COERCION_IMPLICIT,			/* coercion in context of expression */
	COERCION_ASSIGNMENT,		/* coercion in context of assignment */
	COERCION_EXPLICIT			/* explicit cast operation */
} CoercionContext;

/*
 * CoercionForm - information showing how to display a function-call node
 */
typedef enum CoercionForm
{
	COERCE_EXPLICIT_CALL,		/* display as a function call */
	COERCE_EXPLICIT_CAST,		/* display as an explicit cast */
	COERCE_IMPLICIT_CAST,		/* implicit cast, so hide it */
	COERCE_DONTCARE				/* special case for planner */
} CoercionForm;

/*
 * FuncExpr - expression node for a function call
 */
typedef struct FuncExpr
{
	Expr		xpr;
	Oid			funcid;			/* PG_PROC OID of the function */
	Oid			funcresulttype; /* PG_TYPE OID of result value */
	bool		funcretset;		/* true if function returns set */
	CoercionForm funcformat;	/* how to display this function call */
	List	   *args;			/* arguments to the function */
	int			location;		/* token location, or -1 if unknown */
	bool        is_tablefunc;   /* Is a TableFunction reference */
} FuncExpr;


/*
 * OpExpr - expression node for an operator invocation
 *
 * Semantically, this is essentially the same as a function call.
 *
 * Note that opfuncid is not necessarily filled in immediately on creation
 * of the node.  The planner makes sure it is valid before passing the node
 * tree to the executor, but during parsing/planning opfuncid can be 0.
 */
typedef struct OpExpr
{
	Expr		xpr;
	Oid			opno;			/* PG_OPERATOR OID of the operator */
	Oid			opfuncid;		/* PG_PROC OID of underlying function */
	Oid			opresulttype;	/* PG_TYPE OID of result value */
	bool		opretset;		/* true if operator returns set */
	List	   *args;			/* arguments to the operator (1 or 2) */
	int			location;		/* token location, or -1 if unknown */
} OpExpr;

/*
 * DistinctExpr - expression node for "x IS DISTINCT FROM y"
 *
 * Except for the nodetag, this is represented identically to an OpExpr
 * referencing the "=" operator for x and y.
 * We use "=", not the more obvious "<>", because more datatypes have "="
 * than "<>".  This means the executor must invert the operator result.
 * Note that the operator function won't be called at all if either input
 * is NULL, since then the result can be determined directly.
 */
typedef OpExpr DistinctExpr;

/*
 * ScalarArrayOpExpr - expression node for "scalar op ANY/ALL (array)"
 *
 * The operator must yield boolean.  It is applied to the left operand
 * and each element of the righthand array, and the results are combined
 * with OR or AND (for ANY or ALL respectively).  The node representation
 * is almost the same as for the underlying operator, but we need a useOr
 * flag to remember whether it's ANY or ALL, and we don't have to store
 * the result type because it must be boolean.
 */
typedef struct ScalarArrayOpExpr
{
	Expr		xpr;
	Oid			opno;			/* PG_OPERATOR OID of the operator */
	Oid			opfuncid;		/* PG_PROC OID of underlying function */
	bool		useOr;			/* true for ANY, false for ALL */
	List	   *args;			/* the scalar and array operands */
	int			location;		/* token location, or -1 if unknown */
} ScalarArrayOpExpr;

/*
 * BoolExpr - expression node for the basic Boolean operators AND, OR, NOT
 *
 * Notice the arguments are given as a List.  For NOT, of course the list
 * must always have exactly one element.  For AND and OR, the executor can
 * handle any number of arguments.	The parser generally treats AND and OR
 * as binary and so it typically only produces two-element lists, but the
 * optimizer will flatten trees of AND and OR nodes to produce longer lists
 * when possible.  There are also a few special cases where more arguments
 * can appear before optimization.
 */
typedef enum BoolExprType
{
	AND_EXPR, OR_EXPR, NOT_EXPR
} BoolExprType;

typedef struct BoolExpr
{
	Expr		xpr;
	BoolExprType boolop;
	List	   *args;			/* arguments to this expression */
	int			location;		/* token location, or -1 if unknown */
} BoolExpr;

/*
 * TableValueExpr - a "TABLE( <subquery> )" expression indicating a subquery
 * expression that is passed as a value to a function.  
 *
 * This is <table value constructor by query> within the SQL Standard
 */
typedef struct TableValueExpr  
{
	NodeTag     type;
	Node       *subquery;
	int         location;
} TableValueExpr;


/*
 * SubLink
 *
 * A SubLink represents a subselect appearing in an expression, and in some
 * cases also the combining operator(s) just above it.	The subLinkType
 * indicates the form of the expression represented:
 *	EXISTS_SUBLINK		EXISTS(SELECT ...)
 *	ALL_SUBLINK			(lefthand) op ALL (SELECT ...)
 *	ANY_SUBLINK			(lefthand) op ANY (SELECT ...)
 *	ROWCOMPARE_SUBLINK	(lefthand) op (SELECT ...)
 *	EXPR_SUBLINK		(SELECT with single targetlist item ...)
 *	ARRAY_SUBLINK		ARRAY(SELECT with single targetlist item ...)
 *	CTE_SUBLINK			WITH query (never actually part of an expression)
 * For ALL, ANY, and ROWCOMPARE, the lefthand is a list of expressions of the
 * same length as the subselect's targetlist.  ROWCOMPARE will *always* have
 * a list with more than one entry; if the subselect has just one target
 * then the parser will create an EXPR_SUBLINK instead (and any operator
 * above the subselect will be represented separately).  Note that both
 * ROWCOMPARE and EXPR require the subselect to deliver only one row.
 * ALL, ANY, and ROWCOMPARE require the combining operators to deliver boolean
 * results.  ALL and ANY combine the per-row results using AND and OR
 * semantics respectively.
 * ARRAY requires just one target column, and creates an array of the target
 * column's type using any number of rows resulting from the subselect.
 *
 * SubLink is classed as an Expr node, but it is not actually executable;
 * it must be replaced in the expression tree by a SubPlan node during
 * planning.
 *
 * NOTE: in the raw output of gram.y, testexpr contains just the raw form
 * of the lefthand expression (if any), and operName is the String name of
 * the combining operator.	Also, subselect is a raw parsetree.  During parse
 * analysis, the parser transforms testexpr into a complete boolean expression
 * that compares the lefthand value(s) to PARAM_SUBLINK nodes representing the
 * output columns of the subselect.  And subselect is transformed to a Query.
 * This is the representation seen in saved rules and in the rewriter.
 *
 * In EXISTS, EXPR, and ARRAY SubLinks, testexpr and operName are unused and
 * are always null.
 */
typedef enum SubLinkType
{
	EXISTS_SUBLINK,
	ALL_SUBLINK,
	ANY_SUBLINK,
	ROWCOMPARE_SUBLINK,
	EXPR_SUBLINK,
	ARRAY_SUBLINK,
	NOT_EXISTS_SUBLINK
} SubLinkType;


typedef struct SubLink
{
	Expr		xpr;
	SubLinkType subLinkType;	/* see above */
	Node	   *testexpr;		/* outer-query test for ALL/ANY/ROWCOMPARE */
	List	   *operName;		/* originally specified operator name */
	Node	   *subselect;		/* subselect as Query* or parsetree */
	int			location;		/* token location, or -1 if unknown */
} SubLink;

/*
 * SubPlan - executable expression node for a subplan (sub-SELECT)
 *
 * The planner replaces SubLink nodes in expression trees with SubPlan
 * nodes after it has finished planning the subquery.  SubPlan references
 * a sub-plantree stored in the subplans list of the toplevel PlannedStmt.
 * (We avoid a direct link to make it easier to copy expression trees
 * without causing multiple processing of the subplan.)
 *
 * In an ordinary subplan, testexpr points to an executable expression
 * (OpExpr, an AND/OR tree of OpExprs, or RowCompareExpr) for the combining
 * operator(s); the left-hand arguments are the original lefthand expressions,
 * and the right-hand arguments are PARAM_EXEC Param nodes representing the
 * outputs of the sub-select.  (NOTE: runtime coercion functions may be
 * inserted as well.)  This is just the same expression tree as testexpr in
 * the original SubLink node, but the PARAM_SUBLINK nodes are replaced by
 * suitably numbered PARAM_EXEC nodes.
 *
 * If the sub-select becomes an initplan rather than a subplan, the executable
 * expression is part of the outer plan's expression tree (and the SubPlan
 * node itself is not).  In this case testexpr is NULL to avoid duplication.
 *
 * The planner also derives lists of the values that need to be passed into
 * and out of the subplan.	Input values are represented as a list "args" of
 * expressions to be evaluated in the outer-query context (currently these
 * args are always just Vars, but in principle they could be any expression).
 * The values are assigned to the global PARAM_EXEC params indexed by parParam
 * (the parParam and args lists must have the same ordering).  setParam is a
 * list of the PARAM_EXEC params that are computed by the sub-select, if it
 * is an initplan; they are listed in order by sub-select output column
 * position.  (parParam and setParam are integer Lists, not Bitmapsets,
 * because their ordering is significant.)
 */
typedef struct SubPlan
{
	Expr		xpr;
	/* Fields copied from original SubLink: */
	SubLinkType subLinkType;	/* see above */
	/* The combining operators, transformed to an executable expression: */
	Node	   *testexpr;		/* OpExpr or RowCompareExpr expression tree */ 
 	List	   *paramIds;		/* IDs of Params embedded in the above */
 	
    int         qDispSliceId;   /* CDB: slice# of initplan's root slice, or 0 */
 	
	/* The subselect, transformed to a Plan: */

	/* Identification of the Plan tree to use: */
	int			plan_id;		/* Index (from 1) in PlannedStmt.subplans */
	/* Identification of the SubPlan for EXPLAIN and debugging purposes: */
	char	   *plan_name;		/* A name assigned during planning */

	/* Extra data useful for determining subplan's output type: */
	Oid			firstColType;	/* Type of first column of subplan result */
	int32		firstColTypmod;	/* Typmod of first column of subplan result */
	/* Information about execution strategy: */
	bool		useHashTable;	/* TRUE to store subselect output in a hash
								 * table (implies we are doing "IN") */
	bool		unknownEqFalse; /* TRUE if it's okay to return FALSE when the
								 * spec result is UNKNOWN; this allows much
								 * simpler handling of null values */
	bool		is_initplan;	/* CDB: Is the subplan implemented as an
								 * initplan? */
	bool		is_multirow;	/* CDB: May the subplan return more than
								 * one row? */
	bool		is_parallelized; /* Has subplan been processed to be executed in parallel setting */
	/* Information for passing params into and out of the subselect: */
	/* setParam and parParam are lists of integers (param IDs) */
	List	   *setParam;		/* initplan subqueries have to set these
								 * Params for parent plan */
	List	   *parParam;		/* indices of input Params from parent plan */
	List	   *args;			/* exprs to pass as parParam values */
} SubPlan;

/* ----------------
 * FieldSelect
 *
 * FieldSelect represents the operation of extracting one field from a tuple
 * value.  At runtime, the input expression is expected to yield a rowtype
 * Datum.  The specified field number is extracted and returned as a Datum.
 * ----------------
 */

typedef struct FieldSelect
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	AttrNumber	fieldnum;		/* attribute number of field to extract */
	Oid			resulttype;		/* type of the field (result type of this
								 * node) */
	int32		resulttypmod;	/* output typmod (usually -1) */
} FieldSelect;

/* ----------------
 * FieldStore
 *
 * FieldStore represents the operation of modifying one field in a tuple
 * value, yielding a new tuple value (the input is not touched!).  Like
 * the assign case of ArrayRef, this is used to implement UPDATE of a
 * portion of a column.
 *
 * A single FieldStore can actually represent updates of several different
 * fields.	The parser only generates FieldStores with single-element lists,
 * but the planner will collapse multiple updates of the same base column
 * into one FieldStore.
 * ----------------
 */

typedef struct FieldStore
{
	Expr		xpr;
	Expr	   *arg;			/* input tuple value */
	List	   *newvals;		/* new value(s) for field(s) */
	List	   *fieldnums;		/* integer list of field attnums */
	Oid			resulttype;		/* type of result (same as type of arg) */
	/* Like RowExpr, we deliberately omit a typmod here */
} FieldStore;

/* ----------------
 * RelabelType
 *
 * RelabelType represents a "dummy" type coercion between two binary-
 * compatible datatypes, such as reinterpreting the result of an OID
 * expression as an int4.  It is a no-op at runtime; we only need it
 * to provide a place to store the correct type to be attributed to
 * the expression result during type resolution.  (We can't get away
 * with just overwriting the type field of the input expression node,
 * so we need a separate node to show the coercion's result type.)
 * ----------------
 */

typedef struct RelabelType
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	Oid			resulttype;		/* output type of coercion expression */
	int32		resulttypmod;	/* output typmod (usually -1) */
	CoercionForm relabelformat; /* how to display this node */
	int			location;		/* token location, or -1 if unknown */
} RelabelType;

/* ----------------
 * ConvertRowtypeExpr
 *
 * ConvertRowtypeExpr represents a type coercion from one composite type
 * to another, where the source type is guaranteed to contain all the columns
 * needed for the destination type plus possibly others; the columns need not
 * be in the same positions, but are matched up by name.  This is primarily
 * used to convert a whole-row value of an inheritance child table into a
 * valid whole-row value of its parent table's rowtype.
 * ----------------
 */

typedef struct ConvertRowtypeExpr
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	Oid			resulttype;		/* output type (always a composite type) */
	/* result typmod is not stored, but must be -1; see RowExpr comments */
	CoercionForm convertformat; /* how to display this node */
	int			location;		/* token location, or -1 if unknown */
} ConvertRowtypeExpr;

/*----------
 * CaseExpr - a CASE expression
 *
 * We support two distinct forms of CASE expression:
 *		CASE WHEN boolexpr THEN expr [ WHEN boolexpr THEN expr ... ]
 *		CASE testexpr WHEN compexpr THEN expr [ WHEN compexpr THEN expr ... ]
 * These are distinguishable by the "arg" field being NULL in the first case
 * and the testexpr in the second case.
 *
 * In the raw grammar output for the second form, the condition expressions
 * of the WHEN clauses are just the comparison values.	Parse analysis
 * converts these to valid boolean expressions of the form
 *		CaseTestExpr '=' compexpr
 * where the CaseTestExpr node is a placeholder that emits the correct
 * value at runtime.  This structure is used so that the testexpr need be
 * evaluated only once.  Note that after parse analysis, the condition
 * expressions always yield boolean.
 *
 * Note: we can test whether a CaseExpr has been through parse analysis
 * yet by checking whether casetype is InvalidOid or not.
 *----------
 */
typedef struct CaseExpr
{
	Expr		xpr;
	Oid			casetype;		/* type of expression result */
	Expr	   *arg;			/* implicit equality comparison argument */
	List	   *args;			/* the arguments (list of WHEN clauses) */
	Expr	   *defresult;		/* the default result (ELSE clause) */
	int			location;		/* token location, or -1 if unknown */
} CaseExpr;

/*
 * CaseWhen - one arm of a CASE expression
 */
typedef struct CaseWhen
{
	Expr		xpr;
	Expr	   *expr;			/* condition expression */
	Expr	   *result;			/* substitution result */
	int			location;		/* token location, or -1 if unknown */
} CaseWhen;

/*
 * Placeholder node for the test value to be processed by a CASE expression.
 * This is effectively like a Param, but can be implemented more simply
 * since we need only one replacement value at a time.
 *
 * We also use this in nested UPDATE expressions.
 * See transformAssignmentIndirection().
 */
typedef struct CaseTestExpr
{
	Expr		xpr;
	Oid			typeId;			/* type for substituted value */
	int32		typeMod;		/* typemod for substituted value */
} CaseTestExpr;

/*
 * ArrayExpr - an ARRAY[] expression
 *
 * Note: if multidims is false, the constituent expressions all yield the
 * scalar type identified by element_typeid.  If multidims is true, the
 * constituent expressions all yield arrays of element_typeid (ie, the same
 * type as array_typeid); at runtime we must check for compatible subscripts.
 */
typedef struct ArrayExpr
{
	Expr		xpr;
	Oid			array_typeid;	/* type of expression result */
	Oid			element_typeid; /* common type of array elements */
	List	   *elements;		/* the array elements or sub-arrays */
	bool		multidims;		/* true if elements are sub-arrays */
	int			location;		/* token location, or -1 if unknown */
} ArrayExpr;

/*
 * RowExpr - a ROW() expression
 *
 * Note: the list of fields must have a one-for-one correspondence with
 * physical fields of the associated rowtype, although it is okay for it
 * to be shorter than the rowtype.	That is, the N'th list element must
 * match up with the N'th physical field.  When the N'th physical field
 * is a dropped column (attisdropped) then the N'th list element can just
 * be a NULL constant.	(This case can only occur for named composite types,
 * not RECORD types, since those are built from the RowExpr itself rather
 * than vice versa.)  It is important not to assume that length(args) is
 * the same as the number of columns logically present in the rowtype.
 *
 * colnames is NIL in a RowExpr built from an ordinary ROW() expression.
 * It is provided in cases where we expand a whole-row Var into a RowExpr,
 * to retain the column alias names of the RTE that the Var referenced
 * (which would otherwise be very difficult to extract from the parsetree).
 * Like the args list, it is one-for-one with physical fields of the rowtype.
 */
typedef struct RowExpr
{
	Expr		xpr;
	List	   *args;			/* the fields */
	Oid			row_typeid;		/* RECORDOID or a composite type's ID */

	/*
	 * Note: we deliberately do NOT store a typmod.  Although a typmod will be
	 * associated with specific RECORD types at runtime, it will differ for
	 * different backends, and so cannot safely be stored in stored
	 * parsetrees.	We must assume typmod -1 for a RowExpr node.
	 */
	CoercionForm row_format;	/* how to display this node */
	List	   *colnames;		/* list of String, or NIL */
	int			location;		/* token location, or -1 if unknown */
} RowExpr;

/*
 * RowCompareExpr - row-wise comparison, such as (a, b) <= (1, 2)
 *
 * We support row comparison for any operator that can be determined to
 * act like =, <>, <, <=, >, or >= (we determine this by looking for the
 * operator in btree opclasses).  Note that the same operator name might
 * map to a different operator for each pair of row elements, since the
 * element datatypes can vary.
 *
 * A RowCompareExpr node is only generated for the < <= > >= cases;
 * the = and <> cases are translated to simple AND or OR combinations
 * of the pairwise comparisons.  However, we include = and <> in the
 * RowCompareType enum for the convenience of parser logic.
 */
typedef enum RowCompareType
{
	/* Values of this enum are chosen to match btree strategy numbers */
	ROWCOMPARE_LT = 1,			/* BTLessStrategyNumber */
	ROWCOMPARE_LE = 2,			/* BTLessEqualStrategyNumber */
	ROWCOMPARE_EQ = 3,			/* BTEqualStrategyNumber */
	ROWCOMPARE_GE = 4,			/* BTGreaterEqualStrategyNumber */
	ROWCOMPARE_GT = 5,			/* BTGreaterStrategyNumber */
	ROWCOMPARE_NE = 6			/* no such btree strategy */
} RowCompareType;

typedef struct RowCompareExpr
{
	Expr		xpr;
	RowCompareType rctype;		/* LT LE GE or GT, never EQ or NE */
	List	   *opnos;			/* OID list of pairwise comparison ops */
	List	   *opclasses;		/* OID list of containing operator classes */
	List	   *largs;			/* the left-hand input arguments */
	List	   *rargs;			/* the right-hand input arguments */
} RowCompareExpr;

/*
 * CoalesceExpr - a COALESCE expression
 */
typedef struct CoalesceExpr
{
	Expr		xpr;
	Oid			coalescetype;	/* type of expression result */
	List	   *args;			/* the arguments */
	int			location;		/* token location, or -1 if unknown */
} CoalesceExpr;

/*
 * MinMaxExpr - a GREATEST or LEAST function
 */
typedef enum MinMaxOp
{
	IS_GREATEST,
	IS_LEAST
} MinMaxOp;

typedef struct MinMaxExpr
{
	Expr		xpr;
	Oid			minmaxtype;		/* common type of arguments and result */
	MinMaxOp	op;				/* function to execute */
	List	   *args;			/* the arguments */
	int			location;		/* token location, or -1 if unknown */
} MinMaxExpr;

/*
 * NullIfExpr - a NULLIF expression
 *
 * Like DistinctExpr, this is represented the same as an OpExpr referencing
 * the "=" operator for x and y.
 */
typedef OpExpr NullIfExpr;

/* ----------------
 * NullTest
 *
 * NullTest represents the operation of testing a value for NULLness.
 * The appropriate test is performed and returned as a boolean Datum.
 *
 * NOTE: the semantics of this for rowtype inputs are noticeably different
 * from the scalar case.  It would probably be a good idea to include an
 * "argisrow" flag in the struct to reflect that, but for the moment,
 * we do not do so to avoid forcing an initdb during 8.2beta.
 * ----------------
 */

typedef enum NullTestType
{
	IS_NULL, IS_NOT_NULL
} NullTestType;

typedef struct NullTest
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	NullTestType nulltesttype;	/* IS NULL, IS NOT NULL */
} NullTest;

/*
 * BooleanTest
 *
 * BooleanTest represents the operation of determining whether a boolean
 * is TRUE, FALSE, or UNKNOWN (ie, NULL).  All six meaningful combinations
 * are supported.  Note that a NULL input does *not* cause a NULL result.
 * The appropriate test is performed and returned as a boolean Datum.
 */

typedef enum BoolTestType
{
	IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE, IS_UNKNOWN, IS_NOT_UNKNOWN
} BoolTestType;

typedef struct BooleanTest
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	BoolTestType booltesttype;	/* test type */
} BooleanTest;

/*
 * CoerceToDomain
 *
 * CoerceToDomain represents the operation of coercing a value to a domain
 * type.  At runtime (and not before) the precise set of constraints to be
 * checked will be determined.	If the value passes, it is returned as the
 * result; if not, an error is raised.	Note that this is equivalent to
 * RelabelType in the scenario where no constraints are applied.
 */
typedef struct CoerceToDomain
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	Oid			resulttype;		/* domain type ID (result type) */
	int32		resulttypmod;	/* output typmod (currently always -1) */
	CoercionForm coercionformat;	/* how to display this node */
	int			location;		/* token location, or -1 if unknown */
} CoerceToDomain;

/*
 * Placeholder node for the value to be processed by a domain's check
 * constraint.	This is effectively like a Param, but can be implemented more
 * simply since we need only one replacement value at a time.
 *
 * Note: the typeId/typeMod will be set from the domain's base type, not
 * the domain itself.  This is because we shouldn't consider the value to
 * be a member of the domain if we haven't yet checked its constraints.
 */
typedef struct CoerceToDomainValue
{
	Expr		xpr;
	Oid			typeId;			/* type for substituted value */
	int32		typeMod;		/* typemod for substituted value */
	int			location;		/* token location, or -1 if unknown */
} CoerceToDomainValue;

/*
 * Placeholder node for a DEFAULT marker in an INSERT or UPDATE command.
 *
 * This is not an executable expression: it must be replaced by the actual
 * column default expression during rewriting.	But it is convenient to
 * treat it as an expression node during parsing and rewriting.
 */
typedef struct SetToDefault
{
	Expr		xpr;
	Oid			typeId;			/* type for substituted value */
	int32		typeMod;		/* typemod for substituted value */
	int			location;		/* token location, or -1 if unknown */
} SetToDefault;

/*
 * Node representing [WHERE] CURRENT OF cursor_name
 *
 * CURRENT OF is a bit like a Var, in that it carries the rangetable index
 * of the target relation being constrained; this aids placing the expression
 * correctly during planning.  We can assume however that its "levelsup" is
 * always zero, due to the syntactic constraints on where it can appear.
 *
 * CURRENT OF is a bit like a stable function, in that it must be evaluated
 * once during constant folding to give the QEs a consistent view of the query.
 * To accomplish this, during constant folding, we evaluate the CURRENT OF
 * expression into constant values of gp_segment_id, ctid, and tableoid; then,
 * we bind these constant values into the CurrentOfExpr here for consumption
 * by the QEs.
 */
typedef struct CurrentOfExpr
{
	Expr    		xpr;
	char     		*cursor_name;  	/* name of referenced cursor */
	/* for planning */
	Index    		cvarno;      	/* RT index of target relation */
	/* for validation */
	Oid				target_relid;	/* OID of original target relation, 
									 * before any inheritance expansion */
	/* for constant folding */
	int		 		gp_segment_id;
	ItemPointerData	ctid;
	Oid				tableoid;
} CurrentOfExpr;

/*--------------------
 * TargetEntry -
 *	   a target entry (used in query target lists)
 *
 * Strictly speaking, a TargetEntry isn't an expression node (since it can't
 * be evaluated by ExecEvalExpr).  But we treat it as one anyway, since in
 * very many places it's convenient to process a whole query targetlist as a
 * single expression tree.
 *
 * In a SELECT's targetlist, resno should always be equal to the item's
 * ordinal position (counting from 1).	However, in an INSERT or UPDATE
 * targetlist, resno represents the attribute number of the destination
 * column for the item; so there may be missing or out-of-order resnos.
 * It is even legal to have duplicated resnos; consider
 *		UPDATE table SET arraycol[1] = ..., arraycol[2] = ..., ...
 * The two meanings come together in the executor, because the planner
 * transforms INSERT/UPDATE tlists into a normalized form with exactly
 * one entry for each column of the destination table.	Before that's
 * happened, however, it is risky to assume that resno == position.
 * Generally get_tle_by_resno() should be used rather than list_nth()
 * to fetch tlist entries by resno, and only in SELECT should you assume
 * that resno is a unique identifier.
 *
 * resname is required to represent the correct column name in non-resjunk
 * entries of top-level SELECT targetlists, since it will be used as the
 * column title sent to the frontend.  In most other contexts it is only
 * a debugging aid, and may be wrong or even NULL.	(In particular, it may
 * be wrong in a tlist from a stored rule, if the referenced column has been
 * renamed by ALTER TABLE since the rule was made.	Also, the planner tends
 * to store NULL rather than look up a valid name for tlist entries in
 * non-toplevel plan nodes.)  In resjunk entries, resname should be either
 * a specific system-generated name (such as "ctid") or NULL; anything else
 * risks confusing ExecGetJunkAttribute!
 *
 * ressortgroupref is used in the representation of ORDER BY, GROUP BY, and
 * DISTINCT items.	Targetlist entries with ressortgroupref=0 are not
 * sort/group items.  If ressortgroupref>0, then this item is an ORDER BY,
 * GROUP BY, and/or DISTINCT target value.	No two entries in a targetlist
 * may have the same nonzero ressortgroupref --- but there is no particular
 * meaning to the nonzero values, except as tags.  (For example, one must
 * not assume that lower ressortgroupref means a more significant sort key.)
 * The order of the associated SortGroupClause lists determine the semantics.
 *
 * resorigtbl/resorigcol identify the source of the column, if it is a
 * simple reference to a column of a base table (or view).	If it is not
 * a simple reference, these fields are zeroes.
 *
 * If resjunk is true then the column is a working column (such as a sort key)
 * that should be removed from the final output of the query.  Resjunk columns
 * must have resnos that cannot duplicate any regular column's resno.  Also
 * note that there are places that assume resjunk columns come after non-junk
 * columns.
 *--------------------
 */
typedef struct TargetEntry
{
	Expr		xpr;
	Expr	   *expr;			/* expression to evaluate */
	AttrNumber	resno;			/* attribute number (see notes above) */
	char	   *resname;		/* name of the column (could be NULL) */
	Index		ressortgroupref;/* nonzero if referenced by a sort/group
								 * clause */
	Oid			resorigtbl;		/* OID of column's source table */
	AttrNumber	resorigcol;		/* column's number in source table */
	bool		resjunk;		/* set to true to eliminate the attribute from
								 * final target list */
} TargetEntry;


/* ----------------------------------------------------------------
 *					node types for join trees
 *
 * The leaves of a join tree structure are RangeTblRef nodes.  Above
 * these, JoinExpr nodes can appear to denote a specific kind of join
 * or qualified join.  Also, FromExpr nodes can appear to denote an
 * ordinary cross-product join ("FROM foo, bar, baz WHERE ...").
 * FromExpr is like a JoinExpr of jointype JOIN_INNER, except that it
 * may have any number of child nodes, not just two.
 *
 * NOTE: the top level of a Query's jointree is always a FromExpr.
 * Even if the jointree contains no rels, there will be a FromExpr.
 *
 * NOTE: the qualification expressions present in JoinExpr nodes are
 * *in addition to* the query's main WHERE clause, which appears as the
 * qual of the top-level FromExpr.	The reason for associating quals with
 * specific nodes in the jointree is that the position of a qual is critical
 * when outer joins are present.  (If we enforce a qual too soon or too late,
 * that may cause the outer join to produce the wrong set of NULL-extended
 * rows.)  If all joins are inner joins then all the qual positions are
 * semantically interchangeable.
 *
 * NOTE: in the raw output of gram.y, a join tree contains RangeVar,
 * RangeSubselect, and RangeFunction nodes, which are all replaced by
 * RangeTblRef nodes during the parse analysis phase.  Also, the top-level
 * FromExpr is added during parse analysis; the grammar regards FROM and
 * WHERE as separate.
 * ----------------------------------------------------------------
 */

/*
 * RangeTblRef - reference to an entry in the query's rangetable
 *
 * We could use direct pointers to the RT entries and skip having these
 * nodes, but multiple pointers to the same node in a querytree cause
 * lots of headaches, so it seems better to store an index into the RT.
 */
typedef struct RangeTblRef
{
	NodeTag		type;
	int			rtindex;
} RangeTblRef;

/*----------
 * JoinExpr - for SQL JOIN expressions
 *
 * isNatural, using, and quals are interdependent.	The user can write only
 * one of NATURAL, USING(), or ON() (this is enforced by the grammar).
 * If he writes NATURAL then parse analysis generates the equivalent USING()
 * list, and from that fills in "quals" with the right equality comparisons.
 * If he writes USING() then "quals" is filled with equality comparisons.
 * If he writes ON() then only "quals" is set.	Note that NATURAL/USING
 * are not equivalent to ON() since they also affect the output column list.
 *
 * alias is an Alias node representing the AS alias-clause attached to the
 * join expression, or NULL if no clause.  NB: presence or absence of the
 * alias has a critical impact on semantics, because a join with an alias
 * restricts visibility of the tables/columns inside it.
 *
 * During parse analysis, an RTE is created for the Join, and its index
 * is filled into rtindex.	This RTE is present mainly so that Vars can
 * be created that refer to the outputs of the join.  The planner sometimes
 * generates JoinExprs internally; these can have rtindex = 0 if there are
 * no join alias variables referencing such joins.
 *
 * CDB: When the planner flattens sublinks in the JOIN...ON clause, it may
 * attach a list of RangeTblRef nodes ('subqfromlist') which are to be
 * included in the cross product along with 'larg' and 'rarg'.
 *----------
 */
typedef struct JoinExpr
{
	NodeTag		type;
	JoinType	jointype;		/* type of join */
	bool		isNatural;		/* Natural join? Will need to shape table */
	Node	   *larg;			/* left subtree */
	Node	   *rarg;			/* right subtree */
	List	   *usingClause;	/* USING clause, if any (list of String) */
	Node	   *quals;			/* qualifiers on join, if any */
	Alias	   *alias;			/* user-written alias clause, if any */
	int			rtindex;		/* RT index assigned for join */
    List       *subqfromlist;   /* CDB: List of join subtrees resulting from
                                 *  flattening of sublinks */
} JoinExpr;

/*----------
 * FromExpr - represents a FROM ... WHERE ... construct
 *
 * This is both more flexible than a JoinExpr (it can have any number of
 * children, including zero) and less so --- we don't need to deal with
 * aliases and so on.  The output column set is implicitly just the union
 * of the outputs of the children.
 *----------
 */
typedef struct FromExpr
{
	NodeTag		type;
	List	   *fromlist;		/* List of join subtrees */
	Node	   *quals;			/* qualifiers on join, if any */
} FromExpr;


typedef enum Movement
{
	MOVEMENT_NONE,			/* No motion required. */
	MOVEMENT_FOCUS,			/* Fixed motion to a single segment. */
	MOVEMENT_BROADCAST,		/* Broadcast motion. */
	MOVEMENT_REPARTITION,	/* Hash motion */
	MOVEMENT_LIM_RESTRUCT,	/* Restructure a Limit node into three stages */
	MOVEMENT_EXPLICIT		/* Move tuples to the segments specified in the segid column */
} Movement;

/*----------
 * Flow - describes a tuple flow in a parallelized plan
 *
 * This node type is a MPP extension.
 *
 * Plan nodes contain a reference to a Flow that characterizes the output
 * tuple flow of the node.  In addition, the node contains fields used for
 * parallelizing specification.
 *----------
 */
typedef struct Flow
{
	NodeTag		type;			/* T_Flow */
	FlowType	flotype;		/* Type of flow produced by the plan. */
	
	/* What motion (including none) should be applied to this Plan's output. */
	Movement	req_move;
	
	/* Locus type (optimizer flow characterization).
	 */
	CdbLocusType	locustype;
	
	/* If flotype is FLOW_SINGLETON, then this is the segment (-1 for entry)
	 * on which tuples occur.  If req_move is MOVEMENT_FOCUS, then this is
	 * the desired segment for the resulting singleton flow.
	 */
	int			segindex;		/* Segment index of singleton flow. */
	
	/* Sort specifications. */
	int			numSortCols;		/* number of sort key columns */
	AttrNumber	*sortColIdx;		/* their indexes in target list */
	Oid			*sortOperators;		/* OID of operators to sort them by */
	
	/* If req_move is MOVEMENT_REPARTITION, these express the desired 
     * partitioning for a hash motion.  Else if flotype is FLOW_PARTITIONED,
     * this is the partitioning key.  Otherwise NIL. 
	 * otherwise, they are NIL. */
	List       *hashExpr;			/* list of hash expressions */

	/* If req_move is MOVEMENT_EXPLICIT, this contains the index of the segid column
	 * to use in the motion	 */
	AttrNumber segidColIdx;
	
    /* The original Flow ptr is saved here upon setting req_move. */
    struct Flow    *flow_before_req_move;
	
} Flow;

typedef enum GroupingType
{
	GROUPINGTYPE_ROLLUP,         /* ROLLUP grouping extension */
	GROUPINGTYPE_CUBE,           /* CUBE grouping extension */
	GROUPINGTYPE_GROUPING_SETS   /* GROUPING SETS grouping extension */
} GroupingType;

typedef enum WindowExclusion
{
	WINDOW_EXCLUSION_NULL = 0,
	WINDOW_EXCLUSION_CUR_ROW, /* exclude current row */
	WINDOW_EXCLUSION_GROUP, /* exclude rows matching us */
	WINDOW_EXCLUSION_TIES, /* exclude rows matching us, and current row */
	WINDOW_EXCLUSION_NO_OTHERS /* don't exclude -- distinct from EMPTY so
								* that we may dump */
} WindowExclusion;

typedef enum WindowBoundingKind
{
	WINDOW_UNBOUND_PRECEDING,
	WINDOW_BOUND_PRECEDING,
	WINDOW_CURRENT_ROW,
	WINDOW_BOUND_FOLLOWING,
	WINDOW_UNBOUND_FOLLOWING,
	WINDOW_DELAYED_BOUND_PRECEDING,
    WINDOW_DELAYED_BOUND_FOLLOWING
} WindowBoundingKind;

typedef struct WindowFrameEdge
{
	NodeTag type;
	WindowBoundingKind kind;
	/* XXX: need to restrict to certain datatypes in order by */
	Node *val; /* an actual value, if provided */
} WindowFrameEdge;

typedef struct WindowFrame
{
	NodeTag type;
	bool is_rows;	/* true if ROWS was specificied, false if RANGE */
	bool is_between; /* user specified BETWEEN */
	
	/*
	 * XXX: determine if trail and lead must be mentioned in that order
	 */
	WindowFrameEdge *trail; /* trailing edge of the frame */
	WindowFrameEdge *lead; /* leading edge of the frame */
	WindowExclusion exclude; /* exclusion clause */
	bool system_generated; /* frame was generated by the parser */
} WindowFrame;


/* ---------------
 * WindowKey is an auxiliary node of the Window node (a Plan node).  It 
 * represents one level of the potentially multi-level ordering key of 
 * the Window node.  The ORDER BY key of the Nth WindowKey of a Window
 * is the concatenation of the sort keys from WindowKeys 0 thru N.
 *
 * Note that, since a window key represents partial sort key, it may be 
 * empty.  For example (ORDER BY a,b ROWS x) and (ORDER BY a,b ROWS y) 
 * would be represented by partial key (a,b) with framing ROWS x followed
 * by partial key () with framing ROWS y.
 * ---------------
 */
typedef struct WindowKey
{
	NodeTag			type;
	int				numSortCols; /* may be zero, see note */
	AttrNumber	   *sortColIdx;
	Oid			   *sortOperators;
	WindowFrame	   *frame;		/* NULL or framing for WindowKey */
} WindowKey;

/*
 * PercKind
 * Represent function type of PercentileExpr
 */
typedef enum PercKind
{
	PERC_MEDIAN,
	PERC_CONT,
	PERC_DISC
} PercKind;

/*
 * PercentileExpr
 *
 * This represents expressions for percentile_cont, percentile_disc and median.
 * They could be expressed as normal Aggref, but at present we are not able
 * to change the catalog, so we introduce this dedicated node.  As such, the node
 * is treated as Aggref in any cases.  Since we don't support Var in its
 * argument, we don't need var-level field here.
 */
typedef struct PercentileExpr
{
	NodeTag			type;
	Oid				perctype;		/* result type */
	List		   *args;			/* list of argument expression */
	PercKind		perckind;		/* type of percentile function */
	List		   *sortClause;		/* ORDER BY clause */
	List		   *sortTargets;	/* target list for ORDER BY clause */
	Expr		   *pcExpr;			/* peer count expression */
	Expr		   *tcExpr;			/* total count expression */
	int				location;		/* token location, or -1 if unknown */
} PercentileExpr;


/*
 * DMLActionExpr
 *
 * Represents the expression which introduces the action in a SplitUpdate statement
 */
typedef struct DMLActionExpr
{
	Expr        xpr;
} DMLActionExpr;

/*
 * PartOidExpr
 * Represents the expression which holds a part oid in a PartitionSelector operator
 */
typedef struct PartOidExpr
{
	Expr		xpr;
	int 		level;			/* partitioning level */
} PartOidExpr;

/*
 * PartDefaultExpr
 * Represents the expression which determines whether this a part is a default part
 */
typedef struct PartDefaultExpr
{
	Expr		xpr;
	int 		level;			/* partitioning level */
} PartDefaultExpr;

/*
 * PartBoundExpr
 * Represents the expression which holds a part boundary in a PartitionSelector operator
 */
typedef struct PartBoundExpr
{
	Expr		xpr;
	int 		level;			/* partitioning level */
	Oid 		boundType;		/* the return type of this boundary - same as part key */
	bool 		isLowerBound;	/* lower (min) or upper (max) bound */
} PartBoundExpr;

/*
 * PartBoundInclusionExpr
 * Represents the expression which determines whether a part boundary is inclusive or not
 * in a PartitionSelector operator
 */
typedef struct PartBoundInclusionExpr
{
	Expr		xpr;
	int			level;			/* partitioning level */
	bool		isLowerBound;	/* lower (min) or upper (max) bound */
} PartBoundInclusionExpr;

/*
 * PartBoundOpenExpr
 * Represents the expression which determines whether a part boundary is open (unbounded) or not
 * in a PartitionSelector operator
 */
typedef struct PartBoundOpenExpr
{
	Expr		xpr;
	int			level;			/* partitioning level */
	bool		isLowerBound;	/* lower (min) or upper (max) bound */
} PartBoundOpenExpr;

#endif   /* PRIMNODES_H */
