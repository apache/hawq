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
 * parsenodes.h
 *	  definitions for parse tree nodes
 *
 * Many of the node types used in parsetrees include a "location" field.
 * This is a byte (not character) offset in the original source text, to be
 * used for positioning an error cursor when there is an error related to
 * the node.  Access to the original source text is needed to make use of
 * the location.
 *
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/nodes/parsenodes.h,v 1.334 2006/11/05 22:42:10 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSENODES_H
#define PARSENODES_H

#include "nodes/bitmapset.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "cdb/cdbquerycontextdispatching.h"

typedef struct PartitionNode PartitionNode; /* see relation.h */

/* Possible sources of a Query */
typedef enum QuerySource
{
	QSRC_ORIGINAL,				/* original parsetree (explicit query) */
	QSRC_PARSER,				/* added by parse analysis */
	QSRC_INSTEAD_RULE,			/* added by unconditional INSTEAD rule */
	QSRC_QUAL_INSTEAD_RULE,		/* added by conditional INSTEAD rule */
	QSRC_NON_INSTEAD_RULE,		/* added by non-INSTEAD rule */
	QSRC_PLANNER				/* added in planner by splicing parse tree */
} QuerySource;

/* Sort ordering options for ORDER BY and CREATE INDEX */
typedef enum SortByDir
{
	SORTBY_DEFAULT,
	SORTBY_ASC,
	SORTBY_DESC,
	SORTBY_USING				/* not allowed in CREATE INDEX ... */
} SortByDir;

typedef enum SortByNulls
{
	SORTBY_NULLS_DEFAULT,
	SORTBY_NULLS_FIRST,
	SORTBY_NULLS_LAST
} SortByNulls;


/*
 * Grantable rights are encoded so that we can OR them together in a bitmask.
 * The present representation of AclItem limits us to 16 distinct rights,
 * even though AclMode is defined as uint32.  See utils/acl.h.
 *
 * Caution: changing these codes breaks stored ACLs, hence forces initdb.
 */
typedef uint32 AclMode;			/* a bitmask of privilege bits */

#define ACL_INSERT		(1<<0)	/* for relations */
#define ACL_SELECT		(1<<1)
#define ACL_UPDATE		(1<<2)
#define ACL_DELETE		(1<<3)
#define ACL_TRUNCATE	(1<<4)
#define ACL_REFERENCES	(1<<5)
#define ACL_TRIGGER		(1<<6)
#define ACL_EXECUTE		(1<<7)	/* for functions */
#define ACL_USAGE		(1<<8)	/* for languages, namespaces, FDWs, servers
								 * and external protocols */
#define ACL_CREATE		(1<<9)	/* for namespaces and databases */
#define ACL_CREATE_TEMP (1<<10) /* for databases */
#define ACL_CONNECT		(1<<11) /* for databases */
#define N_ACL_RIGHTS	12		/* 1 plus the last 1<<x */
#define ACL_NO_RIGHTS	0
/* Currently, SELECT ... FOR UPDATE/FOR SHARE requires UPDATE privileges */
#define ACL_SELECT_FOR_UPDATE	ACL_UPDATE


/*****************************************************************************
 *	Query Tree
 *****************************************************************************/

/*
 * Query -
 *	  Parse analysis turns all statements into a Query tree (via transformStmt)
 *	  for further processing by the rewriter and planner.
 *
 *	  Utility statements (i.e. non-optimizable statements) have the
 *	  utilityStmt field set, and the Query itself is mostly dummy.
 *	  DECLARE CURSOR is a special case: it is represented like a SELECT,
 *	  but the original DeclareCursorStmt is stored in utilityStmt.
 *
 *	  Planning converts a Query tree into a Plan tree headed by a PlannedStmt
 *	  node.  Eventually, the Query tree will not used by the executor.
 *    Currently, however, the Query structure is carried by the PlannedStmt.
 *
 *    TODO Update the preceding comment, when PlannedStmt support is complete.
 */
typedef struct Query
{
		NodeTag		type;
		
		CmdType		commandType;	/* select|insert|update|delete|utility */
		
		QuerySource querySource;	/* where did I come from? */
		
		bool		canSetTag;		/* do I set the command result tag? */
		
		Node	   *utilityStmt;	/* non-null if this is DECLARE CURSOR or a
								 * non-optimizable statement */
		
		int			resultRelation; /* rtable index of target relation for
									 * INSERT/UPDATE/DELETE; 0 for SELECT */
		
		IntoClause *intoClause;		/* target for SELECT INTO / CREATE TABLE AS */
		
		bool		hasAggs;		/* has aggregates in tlist or havingQual */
		bool		hasWindFuncs;	/* has window function(s) in target list */
		bool		hasSubLinks;	/* has subquery SubLink */
		
		List	   *rtable;			/* list of range table entries */
		FromExpr   *jointree;		/* table join tree (FROM and WHERE clauses) */
		
		List	   *targetList;		/* target list (of TargetEntry) */
		
		List	   *returningList;	/* return-values list (of TargetEntry) */
		
		/*
		 * A list of GroupClauses or GroupingClauses.  The order of
		 * GroupClauses or GroupingClauses are based on input
		 * queries. However, in each grouping set, all GroupClauses will
		 * appear in front of GroupingClauses. See the following GROUP BY
		 * clause:
		 *
		 *   GROUP BY ROLLUP(b,c),a, CUBE(e,d)
		 *
		 * the result list can be roughly represented as follows.
		 *
		 *    GroupClause(a) -->
		 *    GroupingClause( ROLLUP, groupsets (GroupClause(b) --> GroupClause(c) ) ) -->
		 *    GroupingClause( CUBE, groupsets (GroupClause(e) --> GroupClause(d) ) )
		 */
		List	   *groupClause;
		
		Node	   *havingQual;		/* qualifications applied to groups */
		
		List	   *windowClause;	/* defined window specifications */
		
		List	   *distinctClause; /* a list of SortGroupClause's */
		
		List	   *sortClause;		/* a list of SortGroupClause's */

		List	   *scatterClause;  /* a list of tle's */

		List	   *cteList; /* a list of CommonTableExprs in WITH clause */
		bool 	   hasRecursive; /* Whether this query has a recursive WITH clause */
		bool 	   hasModifyingCTE; /* has INSERT/UPDATE/DELETE in WITH clause */

		Node	   *limitOffset;	/* # of result tuples to skip (int8 expr) */
		Node	   *limitCount;		/* # of result tuples to return (int8 expr) */
		
		List	   *rowMarks;		/* a list of RowMarkClause's */
		
		Node	   *setOperations;	/* set-operation tree if this is top level of
									 * a UNION/INTERSECT/EXCEPT query */

	/* TODO Eventually we should remove these 4 members because they're not used here.
	 *      Instead they're in PlannedStmt.  However, we need to read old formats, e.g.
	 *      for catalog upgrade. So for now, it's easier to leave them here.
	 */		
		List	   *resultRelations;		/* Unused. Now in PlannedStmt. */
		PartitionNode *result_partitions;	/* Unused. Now in PlannedStmt. */
		List	   *result_aosegnos;		/* Unused. Now in PlannedStmt. */
		List	   *returningLists; 		/* Unused. Now in PlannedStmt. */
		
		/* MPP: Used only on QD. Don't serialize.
		 *      Holds the result distribution policy for SELECT ... INTO and
		 *      set operations.
		 */
		struct GpPolicy  *intoPolicy;

		QueryContextInfo * contextdisp; /* query context for dispatching */

} Query;

/****************************************************************************
 *	Supporting data structures for Parse Trees
 *
 *	Most of these node types appear in raw parsetrees output by the grammar,
 *	and get transformed to something else by the analyzer.	A few of them
 *	are used as-is in transformed querytrees.
 *
 *	Many of the node types used in raw parsetrees include a "location" field.
 *	This is a byte (not character) offset in the original source text, to be
 *	used for positioning an error cursor when there is an analysis-time
 *	error related to the node.
 ****************************************************************************/

/*
 * TypeName - specifies a type in definitions
 *
 * For TypeName structures generated internally, it is often easier to
 * specify the type by OID than by name.  If "names" is NIL then the
 * actual type OID is given by typeid, otherwise typeid is unused.
 * Similarly, if "typmods" is NIL then the actual typmod is expected to
 * be prespecified in typemod, otherwise typemod is unused.
 *
 * If pct_type is TRUE, then names is actually a field name and we look up
 * the type of that field.	Otherwise (the normal case), names is a type
 * name possibly qualified with schema and database name.
 */
typedef struct TypeName
{
	NodeTag		type;
	List	   *names;			/* qualified name (list of Value strings) */
	Oid			typid;		    /* type identified by OID */
	bool		timezone;		/* timezone specified? */
	bool		setof;			/* is a set? */
	bool		pct_type;		/* %TYPE specified? */
	List	   *typmods;		/* type modifier expression(s) */
	int32		typmod;			/* prespecified type modifier */
	List	   *arrayBounds;	/* array bounds */
	int			location;		/* token location, or -1 if unknown */
} TypeName;

/*
 * ColumnRef - specifies a reference to a column, or possibly a whole tuple
 *
 * The "fields" list must be nonempty.	It can contain string Value nodes
 * (representing names) and A_Star nodes (representing occurrence of a '*').
 * Currently, A_Star must appear only as the last list element --- the grammar
 * is responsible for enforcing this!
 *
 * Note: any array subscripting or selection of fields from composite columns
 * is represented by an A_Indirection node above the ColumnRef.  However,
 * for simplicity in the normal case, initial field selection from a table
 * name is represented within ColumnRef and not by adding A_Indirection.
 */
typedef struct ColumnRef
{
	NodeTag		type;
	List	   *fields;			/* field names (Value strings) or A_Star */
	int			location;		/* token location, or -1 if unknown */
} ColumnRef;

/*
 * ParamRef - specifies a $n parameter reference
 */
typedef struct ParamRef
{
	NodeTag		type;
	int			number;			/* the number of the parameter */
	int			location;		/* token location, or -1 if unknown */
} ParamRef;

/*
 * A_Expr - infix, prefix, and postfix expressions
 */
typedef enum A_Expr_Kind
{
	AEXPR_OP,					/* normal operator */
	AEXPR_AND,					/* booleans - name field is unused */
	AEXPR_OR,
	AEXPR_NOT,
	AEXPR_OP_ANY,				/* scalar op ANY (array) */
	AEXPR_OP_ALL,				/* scalar op ALL (array) */
	AEXPR_DISTINCT,				/* IS DISTINCT FROM - name must be "=" */
	AEXPR_NULLIF,				/* NULLIF - name must be "=" */
	AEXPR_OF,					/* IS [NOT] OF - name must be "=" or "<>" */
	AEXPR_IN					/* [NOT] IN - name must be "=" or "<>" */
} A_Expr_Kind;

typedef struct A_Expr
{
	NodeTag		type;
	A_Expr_Kind kind;			/* see above */
	List	   *name;			/* possibly-qualified name of operator */
	Node	   *lexpr;			/* left argument, or NULL if none */
	Node	   *rexpr;			/* right argument, or NULL if none */
	int			location;		/* token location, or -1 if unknown */
} A_Expr;

/*
 * A_Const - a literal constant
 */
typedef struct A_Const
{
	NodeTag		type;
	Value		val;			/* value (includes type info, see value.h) */
	TypeName   *typname;		/* typecast, or NULL if none */
	int			location;		/* token location, or -1 if unknown */
} A_Const;

/*
 * TypeCast - a CAST expression
 *
 * NOTE: for mostly historical reasons, A_Const parsenodes contain
 * room for a TypeName; we only generate a separate TypeCast node if the
 * argument to be casted is not a constant.  In theory either representation
 * would work, but the combined representation saves a bit of code in many
 * productions in gram.y.
 */
typedef struct TypeCast
{
	NodeTag		type;
	Node	   *arg;			/* the expression being casted */
	TypeName   *typname;		/* the target type */
	int			location;		/* token location, or -1 if unknown */
} TypeCast;

/*
 * FuncCall - a function or aggregate invocation
 *
 * agg_star indicates we saw a 'foo(*)' construct, while agg_distinct
 * indicates we saw 'foo(DISTINCT ...)'.  In either case, the construct
 * *must* be an aggregate call.  Otherwise, it might be either an
 * aggregate or some other kind of function.  However, if OVER is present
 * it had better be an aggregate or window function.
 */
typedef struct FuncCall
{
	NodeTag		type;
	List	   *funcname;		/* qualified name of function */
	List	   *args;			/* the arguments (list of exprs) */
    List       *agg_order;      /* ORDER BY (list of SortBy) */
	bool		agg_star;		/* argument was really '*' */
	bool		agg_distinct;	/* arguments were labeled DISTINCT */
	int			location;		/* token location, or -1 if unknown */
	Node	   *over;			/* over clause */
    Node       *agg_filter;     /* aggregation filter clause */
} FuncCall;

/*
 * A_Indices - array reference or bounds ([lidx:uidx] or [uidx])
 */
typedef struct A_Indices
{
	NodeTag		type;
	Node	   *lidx;			/* NULL if it's a single subscript */
	Node	   *uidx;
} A_Indices;

/*
 * A_Indirection - select a field and/or array element from an expression
 *
 * The indirection list can contain both A_Indices nodes (representing
 * subscripting) and string Value nodes (representing field selection
 * --- the string value is the name of the field to select).  For example,
 * a complex selection operation like
 *				(foo).field1[42][7].field2
 * would be represented with a single A_Indirection node having a 4-element
 * indirection list.
 *
 * Note: as of Postgres 8.0, we don't support arrays of composite values,
 * so cases in which a field select follows a subscript aren't actually
 * semantically legal.	However the parser is prepared to handle such.
 */
typedef struct A_Indirection
{
	NodeTag		type;
	Node	   *arg;			/* the thing being selected from */
	List	   *indirection;	/* subscripts and/or field names and/or * */
} A_Indirection;

/*
 * A_ArrayExpr - an ARRAY[] construct
 */
typedef struct A_ArrayExpr
{
	NodeTag		type;
	List	   *elements;		/* array element expressions */
	int			location;		/* token location, or -1 if unknown */
} A_ArrayExpr;

/*
 * ResTarget -
 *	  result target (used in target list of pre-transformed parse trees)
 *
 * In a SELECT target list, 'name' is the column label from an
 * 'AS ColumnLabel' clause, or NULL if there was none, and 'val' is the
 * value expression itself.  The 'indirection' field is not used.
 *
 * INSERT uses ResTarget in its target-column-names list.  Here, 'name' is
 * the name of the destination column, 'indirection' stores any subscripts
 * attached to the destination, and 'val' is not used.
 *
 * In an UPDATE target list, 'name' is the name of the destination column,
 * 'indirection' stores any subscripts attached to the destination, and
 * 'val' is the expression to assign.
 *
 * See A_Indirection for more info about what can appear in 'indirection'.
 */
typedef struct ResTarget
{
	NodeTag		type;
	char	   *name;			/* column name or NULL */
	List	   *indirection;	/* subscripts, field names, and '*', or NIL */
	Node	   *val;			/* the value expression to compute or assign */
	int			location;		/* token location, or -1 if unknown */
} ResTarget;

/*
 * SortBy - for ORDER BY clause
 */
typedef struct SortBy
{
	NodeTag		type;
	int			sortby_kind;	/* see codes above */
	List	   *useOp;			/* name of op to use, if SORTBY_USING */
	Node	   *node;			/* expression to sort on */
	int			location;		/* operator location, or -1 if none/unknown */
} SortBy;

/*
 * RangeSubselect - subquery appearing in a FROM clause
 */
typedef struct RangeSubselect
{
	NodeTag		type;
	Node	   *subquery;		/* the untransformed sub-select clause */
	Alias	   *alias;			/* table alias & optional column aliases */
} RangeSubselect;

/*
 * RangeFunction - function call appearing in a FROM clause
 */
typedef struct RangeFunction
{
	NodeTag		type;
	Node	   *funccallnode;	/* untransformed function call tree */
	Alias	   *alias;			/* table alias & optional column aliases */
	List	   *coldeflist;		/* list of ColumnDef nodes to describe result
								 * of function returning RECORD */
} RangeFunction;

/*
 * ColumnDef - column definition (used in various creates)
 *
 * If the column has a default value, we may have the value expression
 * in either "raw" form (an untransformed parse tree) or "cooked" form
 * (the nodeToString representation of an executable expression tree),
 * depending on how this ColumnDef node was created (by parsing, or by
 * inheritance from an existing relation).	We should never have both
 * in the same node!
 *
 * The constraints list may contain a CONSTR_DEFAULT item in a raw
 * parsetree produced by gram.y, but transformCreateStmt will remove
 * the item and set raw_default instead.  CONSTR_DEFAULT items
 * should not appear in any subsequent processing.
 */
typedef struct ColumnDef
{
	NodeTag		type;
	char	   *colname;		/* name of column */
	TypeName   *typname;		/* type of column */
	int			inhcount;		/* number of times column is inherited */
	bool		is_local;		/* column has local (non-inherited) def'n */
	bool		is_not_null;	/* NOT NULL constraint specified? */
	Node	   *raw_default;	/* default value (untransformed parse tree) */
	char	   *cooked_default; /* nodeToString representation */
	List	   *constraints;	/* other constraints on column */
	List	   *encoding;		/* ENCODING clause */
	bool	   default_is_null;	/* DEFAULT NULL case */
} ColumnDef;

/*
 * inhRelation - Relations a CREATE TABLE is to inherit attributes of
 */
typedef struct InhRelation
{
	NodeTag		type;
	RangeVar   *relation;
	List	   *options;		/* integer List of CreateStmtLikeOption */
} InhRelation;

typedef enum CreateStmtLikeOption
{
	CREATE_TABLE_LIKE_INCLUDING_DEFAULTS,
	CREATE_TABLE_LIKE_EXCLUDING_DEFAULTS,
	CREATE_TABLE_LIKE_INCLUDING_CONSTRAINTS,
	CREATE_TABLE_LIKE_EXCLUDING_CONSTRAINTS,
	CREATE_TABLE_LIKE_INCLUDING_INDEXES,
	CREATE_TABLE_LIKE_EXCLUDING_INDEXES
} CreateStmtLikeOption;

/*
 * IndexElem - index parameters (used in CREATE INDEX)
 *
 * For a plain index attribute, 'name' is the name of the table column to
 * index, and 'expr' is NULL.  For an index expression, 'name' is NULL and
 * 'expr' is the expression tree.
 */
typedef struct IndexElem
{
	NodeTag		type;
	char	   *name;			/* name of attribute to index, or NULL */
	Node	   *expr;			/* expression to index, or NULL */
	List	   *opclass;		/* name of desired opclass; NIL = default */
} IndexElem;

/*
 * column reference encoding clause for storage
 */
typedef struct ColumnReferenceStorageDirective
{
	NodeTag		type;
	Value	   *column;
	bool		deflt;
	List	   *encoding;
} ColumnReferenceStorageDirective;

/*
 * DefElem - a generic "name = value" option definition
 *
 * In some contexts the name can be qualified.	Also, certain SQL commands
 * allow a SET/ADD/DROP action to be attached to option settings, so it's
 * convenient to carry a field for that too.  (Note: currently, it is our
 * practice that the grammar allows namespace and action only in statements
 * where they are relevant; C code can just ignore those fields in other
 * statements.)
 */
typedef enum DefElemAction
{
	DEFELEM_UNSPEC,				/* no action given */
	DEFELEM_SET,
	DEFELEM_ADD,
	DEFELEM_DROP
} DefElemAction;

typedef struct DefElem
{
	NodeTag		type;
	char	   *defname;
	Node	   *arg;			/* a (Value *) or a (TypeName *) */
	DefElemAction defaction;	/* unspecified action, or SET/ADD/DROP */
} DefElem;


/*
 * LockingClause - raw representation of FOR UPDATE/SHARE options
 *
 * Note: lockedRels == NIL means "all relations in query".	Otherwise it
 * is a list of String nodes giving relation eref names.
 */
typedef struct LockingClause
{
	NodeTag		type;
	List	   *lockedRels;		/* FOR UPDATE or FOR SHARE relations */
	bool		forUpdate;		/* true = FOR UPDATE, false = FOR SHARE */
	bool		noWait;			/* NOWAIT option */
} LockingClause;


/****************************************************************************
 *	Nodes for a Query tree
 ****************************************************************************/

/*--------------------
 * RangeTblEntry -
 *	  A range table is a List of RangeTblEntry nodes.
 *
 *	  A range table entry may represent a plain relation, a sub-select in
 *	  FROM, a common table expression, or the result of a JOIN clause. 
 *    (Only explicit JOIN syntax produces an RTE, not the implicit join
 *    resulting from multiple FROM items.  This is because we only need
 *    the RTE to deal with SQL features like outer joins and join-output-column
 *    aliasing.)  Other special RTE types also exist, as indicated by RTEKind.
 *
 *	  alias is an Alias node representing the AS alias-clause attached to the
 *	  FROM expression, or NULL if no clause.
 *
 *	  eref is the table reference name and column reference names (either
 *	  real or aliases).  Note that system columns (OID etc) are not included
 *	  in the column list.
 *	  eref->aliasname is required to be present, and should generally be used
 *	  to identify the RTE for error messages etc.
 *
 *	  In RELATION RTEs, the colnames in both alias and eref are indexed by
 *	  physical attribute number; this means there must be colname entries for
 *	  dropped columns.	When building an RTE we insert empty strings ("") for
 *	  dropped columns.	Note however that a stored rule may have nonempty
 *	  colnames for columns dropped since the rule was created (and for that
 *	  matter the colnames might be out of date due to column renamings).
 *	  The same comments apply to FUNCTION RTEs when the function's return type
 *	  is a named composite type.
 *
 *	  In JOIN RTEs, the colnames in both alias and eref are one-to-one with
 *	  joinaliasvars entries.  A JOIN RTE will omit columns of its inputs when
 *	  those columns are known to be dropped at parse time.	Again, however,
 *	  a stored rule might contain entries for columns dropped since the rule
 *	  was created.	(This is only possible for columns not actually referenced
 *	  in the rule.)  When loading a stored rule, we replace the joinaliasvars
 *	  items for any such columns with NULL Consts.	(We can't simply delete
 *	  them from the joinaliasvars list, because that would affect the attnums
 *	  of Vars referencing the rest of the list.)
 *
 *	  inh is TRUE for relation references that should be expanded to include
 *	  inheritance children, if the rel has any.  This *must* be FALSE for
 *	  RTEs other than RTE_RELATION entries.
 *
 *	  inFromCl marks those range variables that are listed in the FROM clause.
 *	  It's false for RTEs that are added to a query behind the scenes, such
 *	  as the NEW and OLD variables for a rule, or the subqueries of a UNION.
 *	  This flag is not used anymore during parsing, since the parser now uses
 *	  a separate "namespace" data structure to control visibility, but it is
 *	  needed by ruleutils.c to determine whether RTEs should be shown in
 *	  decompiled queries.
 *
 *	  requiredPerms and checkAsUser specify run-time access permissions
 *	  checks to be performed at query startup.	The user must have *all*
 *	  of the permissions that are OR'd together in requiredPerms (zero
 *	  indicates no permissions checking).  If checkAsUser is not zero,
 *	  then do the permissions checks using the access rights of that user,
 *	  not the current effective user ID.  (This allows rules to act as
 *	  setuid gateways.)
 *--------------------
 */
typedef enum RTEKind
{
	RTE_RELATION,				/* ordinary relation reference */
	RTE_SUBQUERY,				/* subquery in FROM */
	RTE_JOIN,					/* join */
	RTE_SPECIAL,				/* special rule relation (NEW or OLD) */
	RTE_FUNCTION,				/* function in FROM */
	RTE_VALUES,					/* VALUES (<exprlist>), (<exprlist>), ... */
    RTE_VOID,                   /* CDB: deleted RTE */
	RTE_CTE,                    /* CommonTableExpr in FROM */
	RTE_TABLEFUNCTION,          /* CDB: Functions over multiset input */
} RTEKind;

typedef struct RangeTblEntry
{
	NodeTag		type;

	RTEKind		rtekind;		/* see above */

	/*
	 * XXX the fields applicable to only some rte kinds should be merged into
	 * a union.  I didn't do this yet because the diffs would impact a lot of
	 * code that is being actively worked on.  FIXME someday.
	 */

	/*
	 * Fields valid for a plain relation RTE (else zero):
	 */
	Oid			relid;			/* OID of the relation */

	/*
	 * Fields valid for a subquery RTE (else NULL):
	 */
	Query	   *subquery;		/* the sub-query */

	/* These are for pre-planned sub-queries only.  They are internal to
	 * window planning.
	 */
	struct Plan *subquery_plan;
	List		*subquery_rtable;
	List		*subquery_pathkeys;

	/*
	 * Fields valid for a function RTE (else NULL):
	 *
	 * If the function returns RECORD, funccoltypes lists the column types
	 * declared in the RTE's column type specification, and funccoltypmods
	 * lists their declared typmods.  Otherwise, both fields are NIL.
	 */
	Node	   *funcexpr;		/* expression tree for func call */
	List	   *funccoltypes;	/* OID list of column type OIDs */
	List	   *funccoltypmods; /* integer list of column typmods */
	bytea	   *funcuserdata;	/* describe function user data. assume bytea */

	/*
	 * Fields valid for a values RTE (else NIL):
	 */
	List	   *values_lists;	/* list of expression lists */

	/*
	 * Fields valid for a join RTE (else NULL/zero):
	 *
	 * joinaliasvars is a list of Vars or COALESCE expressions corresponding
	 * to the columns of the join result.  An alias Var referencing column K
	 * of the join result can be replaced by the K'th element of joinaliasvars
	 * --- but to simplify the task of reverse-listing aliases correctly, we
	 * do not do that until planning time.	In a Query loaded from a stored
	 * rule, it is also possible for joinaliasvars items to be NULL Consts,
	 * denoting columns dropped since the rule was made.
	 */
	JoinType	jointype;		/* type of join */
	List	   *joinaliasvars;	/* list of alias-var expansions */

	/* GPDB: Valid for base-relations, true if GP_DIST_RANDOM
	 * pseudo-function was specified as modifier in FROM-clause
	 */
	bool		forceDistRandom;

	/*
	 * Fields valid for a CTE RTE (else NULL/zero):
	 */
	char	   *ctename;		/* name of the WITH list item */
	Index		ctelevelsup;	/* number of query levels up */
	bool		self_reference; /* is this a recursive self-reference? */
	List	   *ctecoltypes;	/* OID list of column type OIDs */
	List	   *ctecoltypmods;	/* integer list of column typmods */

	/*
	 * Fields valid in all RTEs:
	 */
	Alias	   *alias;			/* user-written alias clause, if any */
	Alias	   *eref;			/* expanded reference names */
	bool		inh;			/* inheritance requested? */
	bool		inFromCl;		/* present in FROM clause? */
	AclMode		requiredPerms;	/* bitmask of required access permissions */
	Oid			checkAsUser;	/* if valid, check access as this role */

    List       *pseudocols;     /* CDB: List of CdbRelColumnInfo nodes defining
                                 *  pseudo columns for targetlist of scan node.
                                 *  Referenced by Var nodes with varattno =
                                 *  FirstLowInvalidHeapAttributeNumber minus
                                 *  the 0-based position in the list.  Used
                                 *  only in planner & EXPLAIN, not in executor.
                                 */
} RangeTblEntry;

/*
 * SortClause -
 *	   representation of ORDER BY clauses
 *
 * tleSortGroupRef must match ressortgroupref of exactly one entry of the
 * associated targetlist; that is the expression to be sorted (or grouped) by.
 * sortop is the OID of the ordering operator.
 *
 * SortClauses are also used to identify targets that we will do a "Unique"
 * filter step on (for SELECT DISTINCT and SELECT DISTINCT ON).  The
 * distinctClause list is simply a copy of the relevant members of the
 * sortClause list.  Note that distinctClause can be a subset of sortClause,
 * but cannot have members not present in sortClause; and the members that
 * do appear must be in the same order as in sortClause.
 */
typedef struct SortClause
{
	NodeTag		type;
	Index		tleSortGroupRef;	/* reference into targetlist */
	Oid			sortop;			/* the sort operator to use */
} SortClause;

/*
 * GroupClause -
 *	   representation of GROUP BY clauses
 *
 * GroupClause is exactly like SortClause except for the nodetag value
 * (it's probably not even really necessary to have two different
 * nodetags...).  We have routines that operate interchangeably on both.
 */
typedef SortClause GroupClause;

/*
 * GroupingClause -
 *     representation of grouping extension clauses,
 *     such as ROLLUP, CUBE, and GROUPING SETS.
 */
typedef struct GroupingClause
{
	NodeTag      type;
	GroupingType groupType;
	List         *groupsets;
} GroupingClause;

/*
 * GroupingFunc -
 *     representation of a grouping function for grouping extension
 *     clauses.
 *
 * This is used to determine whether a null value for a column in
 * the output tuple is the result of grouping. For example, a table
 *
 *    test (a integer, b integer)
 *
 * has two rows:
 *
 *    (1,1), (null,1)
 *
 * The query "select a,sum(b),grouping(a) from test group by rollup(a)"
 * returns
 *
 *    1   ,    1,    0
 *    null,    1,    0
 *    null,    2,    1
 *
 * The output row "null,1,0" indicates that the 'null' value for 'a' is not
 * from grouping, while the row "null,2,1" indicates that the 'null' value for
 * 'a' is from grouping.
 */
typedef struct GroupingFunc
{
	NodeTag   type;
	List     *args;  /* arguments provided in the query. */
	int       ngrpcols; /* the number of grouping attributes */
} GroupingFunc;

/*
 * RowMarkClause -
 *	   representation of FOR UPDATE/SHARE clauses
 *
 * We create a separate RowMarkClause node for each target relation
 */
typedef struct RowMarkClause
{
	NodeTag		type;
	Index		rti;			/* range table index of target relation */
	bool		forUpdate;		/* true = FOR UPDATE, false = FOR SHARE */
	bool		noWait;			/* NOWAIT option */
} RowMarkClause;

/*
 * WithClause -
 *	   representation of WITH clause
 *
 * Note: WithClause does not propagate into the Query representation;
 * but CommonTableExpr does.
 */
typedef struct WithClause
{
	NodeTag		type;
	List	   *ctes;			/* list of CommonTableExprs */
	bool		recursive;		/* true = WITH RECURSIVE */
	int			location;		/* token location, or -1 if unknown */
} WithClause;

/*
 * CommonTableExpr -
 *	   representation of WITH list element
 *
 * We don't currently support the SEARCH or CYCLE clause.
 */
typedef struct CommonTableExpr
{
	NodeTag		type;
	char	   *ctename;		/* query name (never qualified) */
	List	   *aliascolnames;	/* optional list of column names */
	Node	   *ctequery;		/* subquery (SelectStmt or Query) */
	int			location;		/* token location, or -1 if unknown */
	/* These fields are set during parse analysis: */
	bool		cterecursive;	/* is this CTE actually recursive? */
	int			cterefcount;	/* number of RTEs referencing this CTE
								 * (excluding internal self-references) */
	List	   *ctecolnames;	/* list of output column names */
	List	   *ctecoltypes;	/* OID list of output column type OIDs */
	List	   *ctecoltypmods;	/* integer list of output column typmods */
} CommonTableExpr;

#define GetCTETargetList(cte) \
	(AssertMacro((cte)->ctequery != NULL && IsA((cte)->ctequery, Query)), \
	 ((Query *) (cte)->ctequery)->commandType == CMD_SELECT ? \
	 ((Query *) (cte)->ctequery)->targetList : \
	 ((Query *) (cte)->ctequery)->returningList)

/*****************************************************************************
 *		Optimizable Statements
 *****************************************************************************/

/* ----------------------
 *		Insert Statement
 *
 * The source expression is represented by SelectStmt for both the
 * SELECT and VALUES cases.  If selectStmt is NULL, then the query
 * is INSERT ... DEFAULT VALUES.
 * ----------------------
 */
typedef struct InsertStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* relation to insert into */
	List	   *cols;			/* optional: names of the target columns */
	Node	   *selectStmt;		/* the source SELECT/VALUES, or NULL */
	List	   *returningList;	/* list of expressions to return */
} InsertStmt;

/* ----------------------
 *		Delete Statement
 * ----------------------
 */
typedef struct DeleteStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* relation to delete from */
	List	   *usingClause;	/* optional using clause for more tables */
	Node	   *whereClause;	/* qualifications */
	List	   *returningList;	/* list of expressions to return */
} DeleteStmt;

/* ----------------------
 *		Update Statement
 * ----------------------
 */
typedef struct UpdateStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* relation to update */
	List	   *targetList;		/* the target list (of ResTarget) */
	Node	   *whereClause;	/* qualifications */
	List	   *fromClause;		/* optional from clause for more tables */
	List	   *returningList;	/* list of expressions to return */
} UpdateStmt;

/* ----------------------
 *		Select Statement
 *
 * A "simple" SELECT is represented in the output of gram.y by a single
 * SelectStmt node; so is a VALUES construct.  A query containing set
 * operators (UNION, INTERSECT, EXCEPT) is represented by a tree of SelectStmt
 * nodes, in which the leaf nodes are component SELECTs and the internal nodes
 * represent UNION, INTERSECT, or EXCEPT operators.  Using the same node
 * type for both leaf and internal nodes allows gram.y to stick ORDER BY,
 * LIMIT, etc, clause values into a SELECT statement without worrying
 * whether it is a simple or compound SELECT.
 * ----------------------
 */
typedef enum SetOperation
{
	SETOP_NONE = 0,
	SETOP_UNION,
	SETOP_INTERSECT,
	SETOP_EXCEPT
} SetOperation;

typedef struct SelectStmt
{
	NodeTag		type;

	/*
	 * These fields are used only in "leaf" SelectStmts.
	 */
	List	   *distinctClause; /* NULL, list of DISTINCT ON exprs, or
								 * lcons(NIL,NIL) for all (SELECT DISTINCT) */
	IntoClause *intoClause;		/* target for SELECT INTO / CREATE TABLE AS */
	List	   *targetList;		/* the target list (of ResTarget) */
	List	   *fromClause;		/* the FROM clause */
	Node	   *whereClause;	/* WHERE qualification */
	List	   *groupClause;	/* GROUP BY clauses */
	Node	   *havingClause;	/* HAVING conditional-expression */
	List	   *windowClause;	/* window specification clauses */
	List       *scatterClause;	/* GPDB: TableValueExpr data distribution */
	WithClause *withClause; 	/* with clause */

	/*
	 * In a "leaf" node representing a VALUES list, the above fields are all
	 * null, and instead this field is set.  Note that the elements of the
	 * sublists are just expressions, without ResTarget decoration. Also note
	 * that a list element can be DEFAULT (represented as a SetToDefault
	 * node), regardless of the context of the VALUES list. It's up to parse
	 * analysis to reject that where not valid.
	 */
	List	   *valuesLists;	/* untransformed list of expression lists */

	/*
	 * These fields are used in both "leaf" SelectStmts and upper-level
	 * SelectStmts.
	 */
	List	   *sortClause;		/* sort clause (a list of SortBy's) */
	Node	   *limitOffset;	/* # of result tuples to skip */
	Node	   *limitCount;		/* # of result tuples to return */
	List	   *lockingClause;	/* FOR UPDATE (list of LockingClause's) */

	/*
	 * These fields are used only in upper-level SelectStmts.
	 */
	SetOperation op;			/* type of set op */
	bool		all;			/* ALL specified? */
	struct SelectStmt *larg;	/* left child */
	struct SelectStmt *rarg;	/* right child */
	/* Eventually add fields for CORRESPONDING spec here */

	/* This field used by: SELECT INTO, CTAS */
	List       *distributedBy;  /* GPDB: columns to distribute the data on. */

} SelectStmt;

typedef struct WindowSpec
{
	NodeTag type;
	char *name;	/* name of window specification */
	char *parent; /* parent window, e.g. OVER(PB, myspec); */
	List *partition; /* PARTITION BY clause */
	List *order; /* ORDER BY clause */
	WindowFrame *frame;
	int			location;		/* token location, or -1 if unknown */
} WindowSpec;

/*
 * Due to the complexity of the grammar, we need a basic structure inside
 * the grammar parser so that we can get data into the analyzer efficiently
 */
typedef struct WindowSpecParse
{
	NodeTag type;
	char *name;
	List *elems;
} WindowSpecParse;


/* ----------------------
 *		Set Operation node for post-analysis query trees
 *
 * After parse analysis, a SELECT with set operations is represented by a
 * top-level Query node containing the leaf SELECTs as subqueries in its
 * range table.  Its setOperations field shows the tree of set operations,
 * with leaf SelectStmt nodes replaced by RangeTblRef nodes, and internal
 * nodes replaced by SetOperationStmt nodes.
 * ----------------------
 */

typedef struct SetOperationStmt
{
	NodeTag		type;
	SetOperation op;			/* type of set op */
	bool		all;			/* ALL specified? */
	Node	   *larg;			/* left child */
	Node	   *rarg;			/* right child */
	/* Eventually add fields for CORRESPONDING spec here */

	/* Fields derived during parse analysis: */
	List	   *colTypes;		/* OID list of output column type OIDs */
	List	   *colTypmods;		/* integer list of output column typmods */
} SetOperationStmt;


/*****************************************************************************
 *		Other Statements (no optimizations required)
 *
 *		Some of them require a little bit of transformation (which is also
 *		done by transformStmt). The whole structure is then passed on to
 *		ProcessUtility (by-passing the optimization step) as the utilityStmt
 *		field in Query.
 *****************************************************************************/

/*
 * When a command can act on several kinds of objects with only one
 * parse structure required, use these constants to designate the
 * object type.
 */

typedef enum ObjectType
{
	OBJECT_AGGREGATE,
	OBJECT_CAST,
	OBJECT_COLUMN,
	OBJECT_CONSTRAINT,
	OBJECT_CONVERSION,
	OBJECT_DATABASE,
	OBJECT_DOMAIN,
	OBJECT_FDW,
	OBJECT_FOREIGN_SERVER,
	OBJECT_FOREIGNTABLE,
	OBJECT_FUNCTION,
	OBJECT_INDEX,
	OBJECT_LANGUAGE,
	OBJECT_LARGEOBJECT,
	OBJECT_OPCLASS,
	OBJECT_OPERATOR,
	OBJECT_ROLE,
	OBJECT_RULE,
	OBJECT_SCHEMA,
	OBJECT_SEQUENCE,
	OBJECT_TABLE,
	OBJECT_EXTTABLE,
	OBJECT_EXTPROTOCOL,
	OBJECT_FILESPACE,
	OBJECT_FILESYSTEM,
	OBJECT_TABLESPACE,
	OBJECT_TRIGGER,
	OBJECT_TYPE,
	OBJECT_VIEW,
	OBJECT_RESQUEUE
} ObjectType;

/* ----------------------
 *		Create Schema Statement
 *
 * NOTE: the schemaElts list contains raw parsetrees for component statements
 * of the schema, such as CREATE TABLE, GRANT, etc.  These are analyzed and
 * executed after the schema itself is created.
 * ----------------------
 */
typedef struct CreateSchemaStmt
{
	NodeTag		type;
	char	   *schemaname;		/* the name of the schema to create */
	char	   *authid;			/* the owner of the created schema */
	List	   *schemaElts;		/* schema components (list of parsenodes) */
	bool        istemp;         /* true for temp schemas (internal only) */
	Oid			schemaOid;
} CreateSchemaStmt;

typedef enum DropBehavior
{
	DROP_RESTRICT,				/* drop fails if any dependent objects */
	DROP_CASCADE				/* remove dependent objects too */
} DropBehavior;

/* ----------------------
 *	Alter Table
 * ----------------------
 */
typedef struct AlterTableStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* table to work on */
	List	   *cmds;			/* list of subcommands */
	ObjectType	relkind;		/* type of object */

	/* Workspace for use during AT processing/dispatch.  It might be better
	 * to package there in a "context" node than to lay them out here. 
	 */
	List		 *oidmap;		/* For reindex_relation */
	int			oidInfoCount;	/* Vector of TableOidInfo pointers: */
	TableOidInfo *oidInfo;		/* MPP Allow for hierarchy of tables to alter */
} AlterTableStmt;

typedef enum AlterTableType
{
	AT_AddColumn,				/* add column */
	AT_AddColumnRecurse,		/* internal to commands/tablecmds.c */
	AT_ColumnDefault,			/* alter column default */
	AT_DropNotNull,				/* alter column drop not null */
	AT_SetNotNull,				/* alter column set not null */
	AT_SetStatistics,			/* alter column statistics */
	AT_SetStorage,				/* alter column storage */
	AT_DropColumn,				/* drop column */
	AT_DropColumnRecurse,		/* internal to commands/tablecmds.c */
	AT_AddIndex,				/* add index */
	AT_ReAddIndex,				/* internal to commands/tablecmds.c */
	AT_AddConstraint,			/* add constraint */
	AT_AddConstraintRecurse,	/* internal to commands/tablecmds.c */
	AT_ProcessedConstraint,		/* pre-processed add constraint (local in
								 * parser/analyze.c) */
	AT_DropConstraint,			/* drop constraint */
	AT_DropConstraintQuietly,	/* drop constraint, no error/warning (local in
								 * commands/tablecmds.c) */
	AT_AlterColumnType,			/* alter column type */
	AT_ChangeOwner,				/* change owner */
	AT_ClusterOn,				/* CLUSTER ON */
	AT_DropCluster,				/* SET WITHOUT CLUSTER */
	AT_DropOids,				/* SET WITHOUT OIDS */
	AT_SetTableSpace,			/* SET TABLESPACE */
	AT_SetRelOptions,			/* SET (...) -- AM specific parameters */
	AT_ResetRelOptions,			/* RESET (...) -- AM specific parameters */
	AT_EnableTrig,				/* ENABLE TRIGGER name */
	AT_DisableTrig,				/* DISABLE TRIGGER name */
	AT_EnableTrigAll,			/* ENABLE TRIGGER ALL */
	AT_DisableTrigAll,			/* DISABLE TRIGGER ALL */
	AT_EnableTrigUser,			/* ENABLE TRIGGER USER */
	AT_DisableTrigUser,			/* DISABLE TRIGGER USER */
	AT_AddInherit,				/* INHERIT parent */
	AT_DropInherit,				/* NO INHERIT parent */
	AT_SetDistributedBy,		/* SET DISTRIBUTED BY */
	/* CDB: Partitioned Tables */
	AT_PartAdd,					/* Add */
	AT_PartAlter,				/* Alter */
	AT_PartCoalesce,			/* Coalesce */
	AT_PartDrop,				/* Drop */
	AT_PartExchange,			/* Exchange */
	AT_PartMerge,				/* Merge */
	AT_PartModify,				/* Modify */
	AT_PartRename,				/* Rename */
	AT_PartSetTemplate,			/* Set Subpartition Template */
	AT_PartSplit,				/* Split */
	AT_PartTruncate,			/* Truncate */
	AT_PartAddInternal			/* CREATE TABLE time partition addition */
} AlterTableType;

typedef struct AlterTableCmd	/* one subcommand of an ALTER TABLE */
{
	NodeTag		type;
	AlterTableType subtype;		/* Type of table alteration to apply */
	char	   *name;			/* column, constraint, or trigger to act on,
								 * or new owner or tablespace */
	Node	   *def;			/* definition of new column, column type,
								 * index, constraint, or parent table */
	Node	   *transform;		/* transformation expr for ALTER TYPE */
	DropBehavior behavior;		/* RESTRICT or CASCADE for DROP cases */
	bool		part_expanded;	/* expands from another command, for partitioning */
	List	   *partoids;		/* If applicable, OIDs of partition part tables */
} AlterTableCmd;

typedef enum AlterPartitionIdType
{
	AT_AP_IDNone,				/* no ID */
	AT_AP_IDName,				/* IDentify by Name */
	AT_AP_IDValue,				/* IDentifier FOR Value */
	AT_AP_IDRank,				/* IDentifier FOR Rank */
	AT_AP_ID_oid,				/* IDentifier by oid (for internal use only) */
	AT_AP_IDList,				/* List of IDentifier(for internal use only) */
	AT_AP_IDRule,				/* partition rule (for internal use only) */
	AT_AP_IDDefault				/* IDentify DEFAULT partition */
} AlterPartitionIdType;

typedef struct AlterPartitionId /* Identify a partition by name, val, pos */
{
	NodeTag		type;
	AlterPartitionIdType idtype;/* Type of table alteration to apply */
	Node	   *partiddef;		/* partition id definition */
	int					location;	/* token location, or -1 if unknown */
} AlterPartitionId;

typedef struct AlterPartitionCmd /* one subcmd of an ALTER TABLE...PARTITION */
{
	NodeTag		type;
	Node	   *partid;			/* partition id */
	Node	   *arg1;			/* argument 1 */
	Node	   *arg2;			/* argument 2 */
	int					location;	/* token location, or -1 if unknown */
	List *scantable_splits;	/* split assignment for table scan */
	List *newpart_aosegnos;	/* AO 'seg' number for the new partitions to be insert into */
} AlterPartitionCmd;

typedef struct InheritPartitionCmd
{
	NodeTag		type;
	RangeVar   *parent;
} InheritPartitionCmd;

/* ----------------------
 *	Alter Domain
 *
 * The fields are used in different ways by the different variants of
 * this command.
 * ----------------------
 */
typedef struct AlterDomainStmt
{
	NodeTag		type;
	char		subtype;		/*------------
								 *	T = alter column default
								 *	N = alter column drop not null
								 *	O = alter column set not null
								 *	C = add constraint
								 *	X = drop constraint
								 *------------
								 */
	List	   *typname;		/* domain to work on */
	char	   *name;			/* column or constraint name to act on */
	Node	   *def;			/* definition of default or constraint */
	DropBehavior behavior;		/* RESTRICT or CASCADE for DROP cases */
} AlterDomainStmt;


/* ----------------------
 *		Grant|Revoke Statement
 * ----------------------
 */
typedef enum GrantObjectType
{
	ACL_OBJECT_RELATION,		/* table, view */
	ACL_OBJECT_SEQUENCE,		/* sequence */
	ACL_OBJECT_DATABASE,		/* database */
	ACL_OBJECT_EXTPROTOCOL,		/* external table protocol */
	ACL_OBJECT_FILESYSTEM,		/* file system */
	ACL_OBJECT_FDW,				/* foreign-data wrapper */
	ACL_OBJECT_FOREIGN_SERVER,	/* foreign server */
	ACL_OBJECT_FUNCTION,		/* function */
	ACL_OBJECT_LANGUAGE,		/* procedural language */
	ACL_OBJECT_NAMESPACE,		/* namespace */
	ACL_OBJECT_TABLESPACE		/* tablespace */
} GrantObjectType;

typedef struct GrantStmt
{
	NodeTag		type;
	bool		is_grant;		/* true = GRANT, false = REVOKE */
	GrantObjectType objtype;	/* kind of object being operated on */
	List	   *objects;		/* list of RangeVar nodes, FuncWithArgs nodes,
								 * or plain names (as Value strings) */
	List	   *privileges;		/* list of privilege names (as Strings) */
	/* privileges == NIL denotes "all privileges" */
	List	   *grantees;		/* list of PrivGrantee nodes */
	bool		grant_option;	/* grant or revoke grant option */
	DropBehavior behavior;		/* drop behavior (for REVOKE) */
	List	   *cooked_privs;	/* precooked acls (from ADD PARTITION) */
} GrantStmt;

typedef struct PrivGrantee
{
	NodeTag		type;
	char	   *rolname;		/* if NULL then PUBLIC */
} PrivGrantee;

/*
 * Note: FuncWithArgs carries only the types of the input parameters of the
 * function.  So it is sufficient to identify an existing function, but it
 * is not enough info to define a function nor to call it.
 */
typedef struct FuncWithArgs
{
	NodeTag		type;
	List	   *funcname;		/* qualified name of function */
	List	   *funcargs;		/* list of Typename nodes */
} FuncWithArgs;

/* This is only used internally in gram.y. */
typedef struct PrivTarget
{
	NodeTag		type;
	GrantObjectType objtype;
	List	   *objs;
} PrivTarget;

/* ----------------------
 *		Grant/Revoke Role Statement
 *
 * Note: because of the parsing ambiguity with the GRANT <privileges>
 * statement, granted_roles is a list of AccessPriv; the execution code
 * should complain if any column lists appear.	grantee_roles is a list
 * of role names, as Value strings.
 * ----------------------
 */
typedef struct GrantRoleStmt
{
	NodeTag		type;
	List	   *granted_roles;	/* list of roles to be granted/revoked */
	List	   *grantee_roles;	/* list of member roles to add/delete */
	bool		is_grant;		/* true = GRANT, false = REVOKE */
	bool		admin_opt;		/* with admin option */
	char	   *grantor;		/* set grantor to other than current role */
	DropBehavior behavior;		/* drop behavior (for REVOKE) */
} GrantRoleStmt;

/*
 * Node that represents the single row error handling (SREH) clause.
 * used in COPY and External Tables.
 */
typedef struct SingleRowErrorDesc
{
	NodeTag		type;
	RangeVar	*errtable;			/* error table for data format errors */
	int			rejectlimit;		/* per segment error reject limit */
	bool		is_keep;			/* true if KEEP indicated (COPY only) */
	bool		is_limit_in_rows;	/* true for ROWS false for PERCENT */
	bool		reusing_existing_errtable;  /* var used later in trasform... */
} SingleRowErrorDesc;

/* ----------------------
 *		Copy Statement
 *
 * We support "COPY relation FROM file", "COPY relation TO file", and
 * "COPY (query) TO file".	In any given CopyStmt, exactly one of "relation"
 * and "query" must be non-NULL.  Note: "query" is a SelectStmt before
 * parse analysis, and a Query afterwards.
 * ----------------------
 */
typedef struct CopyStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* the relation to copy */
	Query	   *query;			/* the query to copy */
	List	   *attlist;		/* List of column names (as Strings), or NIL
								 * for all columns */
	bool		is_from;		/* TO or FROM */
	char	   *filename;		/* filename, or NULL for STDIN/STDOUT */
	List	   *options;		/* List of DefElem nodes */
	Node	   *sreh;			/* Single row error handling info */
	/* Convenient location for dispatch of misc meta data */
	PartitionNode *partitions;
	List	   *ao_segnos;		/* AO segno map */
	List *ao_segfileinfos;	/* AO segment file information for COPY FROM */
	List *err_aosegnos;	/* AO segno for error table */
	List *err_aosegfileinfos; /* AO segment file information for error table */
	List *scantable_splits;
} CopyStmt;

/* ----------------------
 *		Create Table Statement
 *
 * NOTE: in the raw gram.y output, ColumnDef, Constraint, and FkConstraint
 * nodes are intermixed in tableElts, and constraints is NIL.  After parse
 * analysis, tableElts contains just ColumnDefs, and constraints contains
 * just Constraint nodes (in fact, only CONSTR_CHECK nodes, in the present
 * implementation).
 * ----------------------
 */

typedef struct CreateStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* relation to create */
	List	   *tableElts;		/* column definitions (list of ColumnDef) */
	List	   *inhRelations;	/* relations to inherit from (list of
								 * inhRelation) */
	List	   *constraints;	/* constraints (list of Constraint nodes) */
	List	   *options;		/* options from WITH clause */
	OnCommitAction oncommit;	/* what do we do at COMMIT? */
	char	   *tablespacename; /* table space to use, or NULL */
	List       *distributedBy;   /* what columns we distribute the data by */
	Node       *partitionBy;     /* what columns we partition the data by */
	TableOidInfo oidInfo;
	char	    relKind;         /* CDB: force relkind to this */
	char		relStorage;
	struct GpPolicy  *policy;
	Node       *postCreate;      /* CDB: parse and process after the CREATE */
	List	   *deferredStmts;	/* CDB: Statements, e.g., partial indexes, that can't be 
								 * analyzed until after CREATE (until the target table
								 * is created and visible). */
	bool		is_part_child;	/* CDB: child table in a partition? Marked during analysis for 
								 * interior or leaf parts of the new table.  Not marked for a
								 * a partition root or ordinary table.
								 */
	bool		is_add_part;	/* CDB: is create adding a part to a partition? */
	bool		is_split_part;	/* CDB: is create spliting a part? */
	Oid			ownerid;		/* OID of the role to own this. if InvalidOid, GetUserId() */
	bool		buildAoBlkdir; /* whether to build the block directory for an AO table */
	List	   *attr_encodings; /* attribute storage directives */
} CreateStmt;

typedef enum SharedStorageOp
{
	Op_CreateSegFile, Op_DropSegFile, Op_CreateDir, Op_OverWriteSegFile
} SharedStorageOp;

typedef struct SharedStorageOpStmt
{
	NodeTag 			type;
	SharedStorageOp 	op;

	RelFileNode 		*relFileNode;
	int 				*segmentFileNum;
	char 				**relationName;
//	int					*contentid;
	int					numTasks;
} SharedStorageOpStmt;

/* ----------------------
 *	Definitions for external tables
 * ----------------------
 */
typedef enum ExtTableType
{
	EXTTBL_TYPE_LOCATION,		/* table defined with LOCATION clause */
	EXTTBL_TYPE_EXECUTE			/* table defined with EXECUTE clause */
} ExtTableType;

typedef struct ExtTableTypeDesc
{
	NodeTag			type;
	ExtTableType	exttabletype;
	List			*location_list;
	List			*on_clause;
	char			*command_string;
} ExtTableTypeDesc;

typedef struct CreateExternalStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* external relation to create */
	List	   *tableElts;		/* column definitions (list of ColumnDef) */
	Node	   *exttypedesc;    /* LOCATION or EXECUTE information */
	char	   *format;			/* data format name */
	List	   *formatOpts;		/* List of DefElem nodes for data format */
	bool		isweb;
	bool		iswritable;
	Node	   *sreh;			/* Single row error handling info */
	List	   *encoding;		/* List (size 1 max) of DefElem nodes for
								   data encoding */
	List       *distributedBy;   /* what columns we distribute the data by */
	struct GpPolicy  *policy;	/* used for writable tables */
	
} CreateExternalStmt;

/* ----------------------
 *	Definitions for foreign tables
 * ----------------------
 */
typedef struct CreateForeignStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* foreign relation to create */
	List	   *tableElts;		/* column definitions (list of ColumnDef) */
	char	   *srvname;		/* server name */
	List	   *options;		/* generic options to foreign table */

} CreateForeignStmt;

/* ----------
 * Definitions for plain (non-FOREIGN KEY) constraints in CreateStmt
 *
 * XXX probably these ought to be unified with FkConstraints at some point?
 * To this end we include CONSTR_FOREIGN in the ConstrType enum, even though
 * the parser does not generate it.
 *
 * For constraints that use expressions (CONSTR_DEFAULT, CONSTR_CHECK)
 * we may have the expression in either "raw" form (an untransformed
 * parse tree) or "cooked" form (the nodeToString representation of
 * an executable expression tree), depending on how this Constraint
 * node was created (by parsing, or by inheritance from an existing
 * relation).  We should never have both in the same node!
 *
 * Constraint attributes (DEFERRABLE etc) are initially represented as
 * separate Constraint nodes for simplicity of parsing.  analyze.c makes
 * a pass through the constraints list to attach the info to the appropriate
 * FkConstraint node (and, perhaps, someday to other kinds of constraints).
 * ----------
 */

typedef enum ConstrType			/* types of constraints */
{
	CONSTR_NULL,				/* not SQL92, but a lot of people expect it */
	CONSTR_NOTNULL,
	CONSTR_DEFAULT,
	CONSTR_CHECK,
	CONSTR_FOREIGN,
	CONSTR_PRIMARY,
	CONSTR_UNIQUE,
	CONSTR_ATTR_DEFERRABLE,		/* attributes for previous constraint node */
	CONSTR_ATTR_NOT_DEFERRABLE,
	CONSTR_ATTR_DEFERRED,
	CONSTR_ATTR_IMMEDIATE
} ConstrType;

typedef struct Constraint
{
	NodeTag		type;
	ConstrType	contype;
	Oid			conoid;			/* constraint oid */
	char	   *name;			/* name, or NULL if unnamed */
	Node	   *raw_expr;		/* expr, as untransformed parse tree */
	char	   *cooked_expr;	/* expr, as nodeToString representation */
	List	   *keys;			/* String nodes naming referenced column(s) */
	List	   *options;		/* options from WITH clause */
	char	   *indexspace;		/* index tablespace for PKEY/UNIQUE
								 * constraints; NULL for default */
} Constraint;

/*
 * AlterRewriteTableInfo
 * This is a seriazable representation of AlteredTableInfo.  We use this
 * to dispatch the execution work in phase 3 of ALTER TABLE, so any information
 * that is used only in phase 1 or 2 is not here.
 */
typedef struct AlterRewriteTableInfo
{
	NodeTag		type;
	Oid			relid;			/* Relation to work on */
	char		relkind;		/* Its relkind */
	TupleDesc	oldDesc;		/* Pre-modification tuple descriptor */
	List	   *constraints;	/* List of NewConstraint */
	List	   *newvals;		/* List of NewColumnValue */
	bool		new_notnull;	/* T if we added new NOT NULL constraints */
	bool		new_dropoids;	/* T if we dropped the OID column */
	Oid			newTableSpace;	/* new tablespace; 0 means no change */
	Oid			exchange_relid;	/* for EXCHANGE, the exchanged in rel */
	Oid			newheap_oid;
	List *scantable_splits;
	List *ao_segnos;
} AlterRewriteTableInfo;

/*
 * Serializable version of NewConstraint.
 */
typedef struct AlterRewriteNewConstraint
{
	NodeTag		type;
	char	   *name;			/* Constraint name, or NULL if none */
	ConstrType	contype;		/* CHECK or FOREIGN */
	Oid			refrelid;		/* PK rel, if FOREIGN */
	Node	   *qual;			/* Check expr or FkConstraint struct */
} AlterRewriteNewConstraint;

/*
 * Serializable version of NewColumnValue.
 */
typedef struct AlterRewriteNewColumnValue
{
	NodeTag		type;
	AttrNumber	attnum;			/* which column */
	Expr	   *expr;			/* expression to compute */
} AlterRewriteNewColumnValue;

/* ----------
 * Definitions for FOREIGN KEY constraints in CreateStmt
 *
 * Note: FKCONSTR_ACTION_xxx values are stored into pg_constraint.confupdtype
 * and pg_constraint.confdeltype columns; FKCONSTR_MATCH_xxx values are
 * stored into pg_constraint.confmatchtype.  Changing the code values may
 * require an initdb!
 *
 * If skip_validation is true then we skip checking that the existing rows
 * in the table satisfy the constraint, and just install the catalog entries
 * for the constraint.	This is currently used only during CREATE TABLE
 * (when we know the table must be empty).
 * ----------
 */
#define FKCONSTR_ACTION_NOACTION	'a'
#define FKCONSTR_ACTION_RESTRICT	'r'
#define FKCONSTR_ACTION_CASCADE		'c'
#define FKCONSTR_ACTION_SETNULL		'n'
#define FKCONSTR_ACTION_SETDEFAULT	'd'

#define FKCONSTR_MATCH_FULL			'f'
#define FKCONSTR_MATCH_PARTIAL		'p'
#define FKCONSTR_MATCH_UNSPECIFIED	'u'

typedef struct FkConstraint
{
	NodeTag		type;
	Oid			constrOid;		/* Constraint Oid */
	char	   *constr_name;	/* Constraint name, or NULL if unnamed */
	RangeVar   *pktable;		/* Primary key table */
	List	   *fk_attrs;		/* Attributes of foreign key */
	List	   *pk_attrs;		/* Corresponding attrs in PK table */
	char		fk_matchtype;	/* FULL, PARTIAL, UNSPECIFIED */
	char		fk_upd_action;	/* ON UPDATE action */
	char		fk_del_action;	/* ON DELETE action */
	bool		deferrable;		/* DEFERRABLE */
	bool		initdeferred;	/* INITIALLY DEFERRED */
	bool		skip_validation;	/* skip validation of existing rows? */
	Oid			old_pktable_oid; /* pg_constraint.confrelid of my former self */
	Oid			trig1Oid;
	Oid			trig2Oid;
	Oid			trig3Oid;
	Oid			trig4Oid;
} FkConstraint;

/* ----------
 * Definitions for Table Partition clauses in CreateStmt
 *
 * ----------
 */
typedef enum PartitionByType			/* types of Partitions */
{
	PARTTYP_HASH,
	PARTTYP_RANGE,
	PARTTYP_LIST,
	PARTTYP_REFERENCE /* for future use... */
} PartitionByType;

typedef enum PartitionByVerbosity		/* control Partition messaging */
{
	PART_VERBO_NORMAL, 		/* normal (all messages) */
	PART_VERBO_NODISTRO, 	/* NO DISTRIBution policy messages */
	PART_VERBO_NOPARTNAME   /* NO distro or partition name messages
							   (for SET SUBPARTITION TEMPLATE) */
} PartitionByVerbosity;

typedef struct PartitionBy			/* the Partition By clause */
{
	NodeTag				type;
	PartitionByType		partType;
	List			   *keys;		/* key columns (Partition By ...) */
	List			   *keyopclass;	/* opclass for each key */
	Node			   *partNum;	/* partitionS (constant number)*/
	Node			   *subPart;	/* optional subpartn (PartitionBy ptr) */
	Node			   *partSpec;	/* specification or template */
	Node			   *partDefault;/* DEFAULT partition (if exists) */
	int				    partDepth;	/* depth (starting at zero) */
	RangeVar		   *parentRel;	/* parent relation */
	bool				bKeepMe;    /* keep the top-level pby [nefarious] */
	int				    partQuiet;	/* PartitionByVerbosity */
	int					location;	/* token location, or -1 if unknown */
} PartitionBy;

/*
 * An element in a partition configuration. This represents a single clause --
 * or perhaps an expansion of a single clause.
 */
typedef struct PartitionElem
{
	NodeTag				type;
	Node			   *partName;	/* partition name (optional) */
	Node			   *boundSpec;	/* boundary specification */
	Node			   *subSpec;	/* subpartition spec */
	bool                isDefault;	/* TRUE if default partition declaration */
	Node			   *storeAttr;	/* storage clause attributes */
	char			   *AddPartDesc;/* set by tablecmds.c:atpxAddPart */
	int					partno;		/* number of the partition element */
	long				rrand;		/* if not zero, "random" id for relname */
	List			   *colencs;	/* column encoding clauses */
	int					location;	/* token location, or -1 if unknown */
} PartitionElem;

typedef enum PartitionEdgeBounding
{
	PART_EDGE_UNSPECIFIED,
	PART_EDGE_INCLUSIVE,
	PART_EDGE_EXCLUSIVE
} PartitionEdgeBounding;

/* a "Range Item" is the "values" portition of a LIST partition, or
 * the "values" of a START, END, or EVERY spec in a RANGE partition */
typedef struct PartitionRangeItem
{
	NodeTag				type;
	List			   *partRangeVal;	/*  value */
	PartitionEdgeBounding partedge;		/* inclusive/exclusive ? */
	int					everycount;		/* if EVERY, how many in the set */
	int					location;		/* token location, or -1 if unknown */
} PartitionRangeItem;

/* partition boundary specification */
typedef struct PartitionBoundSpec
{
	NodeTag				type;
	Node			   *partStart;		/* start of range */
	Node			   *partEnd;		/* end */
	Node 			   *partEvery;		/* every specification */
	List 			   *everyGenList;	/* generated EVERY partitions */
	/* MPP-6297: check for WITH (tablename=name) clause */
	char			   *pWithTnameStr;	/* and disable EVERY if tname set */
	int					location;		/* token location, or -1 if unknown */
} PartitionBoundSpec;

/* VALUES clause specification */
typedef struct PartitionValuesSpec
{
	NodeTag				type;
	List			   *partValues;		/* VALUES clause for LIST partition */
	int					location;
} PartitionValuesSpec;

typedef struct PartitionSpec			/* a Partition Specification */
{
	NodeTag				type;
	List			   *partElem;		/* partition element list */
	List			   *enc_clauses;	/* ENCODING () clauses */
	Node			   *subSpec;		/* subpartition spec */
	bool				istemplate;
	int					location;		/* token location, or -1 if unknown */
} PartitionSpec;

/* CREATE FILESPACE ... */
typedef struct CreateFileSpaceStmt
{
	NodeTag		type;
	char	   *filespacename;
	char	   *owner;
	char	   *fsysname;   /* filesystem's name*/
	char       *location;  /* filespace location */
	List       *options;	/* List of options */
} CreateFileSpaceStmt;


/* ----------------------
 *		Create/Drop TableSpace Statements
 * ----------------------
 */

typedef struct CreateTableSpaceStmt
{
	NodeTag		type;
	char	   *tablespacename;
	char	   *owner;
	char       *filespacename;
	Oid         tsoid;
} CreateTableSpaceStmt;

/* ----------------------
 *		Create/Drop FOREIGN DATA WRAPPER Statements
 * ----------------------
 */

typedef struct CreateFdwStmt
{
	NodeTag		type;
	char	   *fdwname;		/* foreign-data wrapper name */
	List	   *validator;		/* optional validator function (qual. name) */
	List	   *options;		/* generic options to FDW */
} CreateFdwStmt;

typedef struct AlterFdwStmt
{
	NodeTag		type;
	char	   *fdwname;		/* foreign-data wrapper name */
	List	   *validator;		/* optional validator function (qual. name) */
	bool		change_validator;
	List	   *options;		/* generic options to FDW */
} AlterFdwStmt;

typedef struct DropFdwStmt
{
	NodeTag		type;
	char	   *fdwname;		/* foreign-data wrapper name */
	bool		missing_ok;		/* don't complain if missing */
	DropBehavior behavior;		/* drop behavior - cascade/restrict */
} DropFdwStmt;

/* ----------------------
 *		Create/Drop FOREIGN SERVER Statements
 * ----------------------
 */

typedef struct CreateForeignServerStmt
{
	NodeTag		type;
	char	   *servername;		/* server name */
	char	   *servertype;		/* optional server type */
	char	   *version;		/* optional server version */
	char	   *fdwname;		/* FDW name */
	List	   *options;		/* generic options to server */
} CreateForeignServerStmt;

typedef struct AlterForeignServerStmt
{
	NodeTag		type;
	char	   *servername;		/* server name */
	char	   *version;		/* optional server version */
	List	   *options;		/* generic options to server */
	bool		has_version;	/* version specified */
} AlterForeignServerStmt;

typedef struct DropForeignServerStmt
{
	NodeTag		type;
	char	   *servername;		/* server name */
	bool		missing_ok;		/* ignore missing servers */
	DropBehavior behavior;		/* drop behavior - cascade/restrict */
} DropForeignServerStmt;

/* ----------------------
 *		Create/Drop USER MAPPING Statements
 * ----------------------
 */

typedef struct CreateUserMappingStmt
{
	NodeTag		type;
	char	   *username;		/* username or PUBLIC/CURRENT_USER */
	char	   *servername;		/* server name */
	List	   *options;		/* generic options to server */
} CreateUserMappingStmt;

typedef struct AlterUserMappingStmt
{
	NodeTag		type;
	char	   *username;		/* username or PUBLIC/CURRENT_USER */
	char	   *servername;		/* server name */
	List	   *options;		/* generic options to server */
} AlterUserMappingStmt;

typedef struct DropUserMappingStmt
{
	NodeTag		type;
	char	   *username;		/* username or PUBLIC/CURRENT_USER */
	char	   *servername;		/* server name */
	bool		missing_ok;		/* ignore missing mappings */
} DropUserMappingStmt;

/* ----------------------
 *		Create/Drop TRIGGER Statements
 * ----------------------
 */

typedef struct CreateTrigStmt
{
	NodeTag		type;
	char	   *trigname;		/* TRIGGER's name */
	RangeVar   *relation;		/* relation trigger is on */
	List	   *funcname;		/* qual. name of function to call */
	List	   *args;			/* list of (T_String) Values or NIL */
	bool		before;			/* BEFORE/AFTER */
	bool		row;			/* ROW/STATEMENT */
	char		actions[4];		/* 1 to 3 of 'i', 'u', 'd', + trailing \0 */

	/* The following are used for referential */
	/* integrity constraint triggers */
	bool		isconstraint;	/* This is an RI trigger */
	bool		deferrable;		/* [NOT] DEFERRABLE */
	bool		initdeferred;	/* INITIALLY {DEFERRED|IMMEDIATE} */
	RangeVar   *constrrel;		/* opposite relation */
	Oid			trigOid;
} CreateTrigStmt;

/* ----------------------
 *		Create/Drop PROCEDURAL LANGUAGE Statement
 * ----------------------
 */
typedef struct CreatePLangStmt
{
	NodeTag		type;
	char	   *plname;			/* PL name */
	List	   *plhandler;		/* PL call handler function (qual. name) */
	List	   *plvalidator;	/* optional validator function (qual. name) */
	bool		pltrusted;		/* PL is trusted */
	Oid	   		plangOid;		/* oid for PL */
	Oid			plhandlerOid;	/* oid for PL call handler function */
	Oid			plvalidatorOid;	/* oid for validator function */
} CreatePLangStmt;

typedef struct DropPLangStmt
{
	NodeTag		type;
	char	   *plname;			/* PL name */
	DropBehavior behavior;		/* RESTRICT or CASCADE behavior */
	bool		missing_ok;		/* skip error if missing? */
} DropPLangStmt;

/* ----------------------
 *	Create/Alter/Drop Resource Queue Statements
 * ----------------------
 */
typedef struct CreateQueueStmt
{
	NodeTag		type;
	char	   *queue;			/* resource queue name */
	List	   *options;		/* List of DefElem nodes */
	Oid	   		queueOid;		/* oid for queue  */
	List	   *optids;			/* List of oids for nodes */
} CreateQueueStmt;

typedef struct AlterQueueStmt
{
	NodeTag		type;
	char	   *queue;			/* resource queue  name */
	List	   *options;		/* List of DefElem nodes */
	List	   *optids;			/* List of oids for nodes */
} AlterQueueStmt;

typedef struct DropQueueStmt
{
	NodeTag		type;
	char	   *queue;			/* resource queue to remove */
} DropQueueStmt;

/* ----------------------
 *	Create/Alter/Drop Role Statements
 *
 * Note: these node types are also used for the backwards-compatible
 * Create/Alter/Drop User/Group statements.  In the ALTER and DROP cases
 * there's really no need to distinguish what the original spelling was,
 * but for CREATE we mark the type because the defaults vary.
 * ----------------------
 */
typedef enum RoleStmtType
{
	ROLESTMT_ROLE,
	ROLESTMT_USER,
	ROLESTMT_GROUP
} RoleStmtType;

typedef struct CreateRoleStmt
{
	NodeTag		type;
	RoleStmtType stmt_type;		/* ROLE/USER/GROUP */
	char	   *role;			/* role name */
	List	   *options;		/* List of DefElem nodes */
	Oid			roleOid;
} CreateRoleStmt;

typedef struct AlterRoleStmt
{
	NodeTag		type;
	char	   *role;			/* role name */
	List	   *options;		/* List of DefElem nodes */
	int			action;			/* +1 = add members, -1 = drop members */
} AlterRoleStmt;

typedef struct AlterRoleSetStmt
{
	NodeTag		type;
	char	   *role;			/* role name */
	char	   *variable;		/* GUC variable name */
	List	   *value;			/* value for variable, or NIL for Reset */
} AlterRoleSetStmt;

typedef struct DropRoleStmt
{
	NodeTag		type;
	List	   *roles;			/* List of roles to remove */
	bool		missing_ok;		/* skip error if a role is missing? */
} DropRoleStmt;

typedef struct DenyLoginPoint
{
	NodeTag			type;
	Value 		   *day;
	Value		   *time;
} DenyLoginPoint;

typedef struct DenyLoginInterval
{
	NodeTag			type;
	DenyLoginPoint *start;
	DenyLoginPoint *end;
} DenyLoginInterval;


/* ----------------------
 *		{Create|Alter} SEQUENCE Statement
 * ----------------------
 */

typedef struct CreateSeqStmt
{
	NodeTag		type;
	RangeVar   *sequence;		/* the sequence to create */
	List	   *options;
	Oid        	relOid;			/* create table with this relOid */
	Oid			comptypeOid;
} CreateSeqStmt;

typedef struct AlterSeqStmt
{
	NodeTag		type;
	RangeVar   *sequence;		/* the sequence to alter */
	List	   *options;
} AlterSeqStmt;

/* ----------------------
 *		Create {Aggregate|Operator|Type|Protocol} Statement
 * ----------------------
 */
typedef struct DefineStmt
{
	NodeTag		type;
	ObjectType	kind;			/* aggregate, operator, type, protocol */
	bool		oldstyle;		/* hack to signal old CREATE AGG syntax */
	List	   *defnames;		/* qualified name (list of Value strings) */
	List	   *args;			/* a list of TypeName (if needed) */
	List	   *definition;		/* a list of DefElem */
	Oid			newOid;			/* for MPP only, the new Oid of the object */
	Oid			shadowOid;
	bool        ordered;        /* signals ordered aggregates */
	bool		trusted;		/* used only for PROTOCOL as this point */
} DefineStmt;

/* ----------------------
 *		Create Domain Statement
 * ----------------------
 */
typedef struct CreateDomainStmt
{
	NodeTag		type;
	List	   *domainname;		/* qualified name (list of Value strings) */
	TypeName   *typname;		/* the base type */
	List	   *constraints;	/* constraints (list of Constraint nodes) */
	Oid		    domainOid;
} CreateDomainStmt;

/* ----------------------
 *		Create Operator Class Statement
 * ----------------------
 */
typedef struct CreateOpClassStmt
{
	NodeTag		type;
	List	   *opclassname;	/* qualified name (list of Value strings) */
	char	   *amname;			/* name of index AM opclass is for */
	TypeName   *datatype;		/* datatype of indexed column */
	List	   *items;			/* List of CreateOpClassItem nodes */
	bool		isDefault;		/* Should be marked as default for type? */
	Oid			opclassOid;
} CreateOpClassStmt;

#define OPCLASS_ITEM_OPERATOR		1
#define OPCLASS_ITEM_FUNCTION		2
#define OPCLASS_ITEM_STORAGETYPE	3

typedef struct CreateOpClassItem
{
	NodeTag		type;
	int			itemtype;		/* see codes above */
	/* fields used for an operator or function item: */
	List	   *name;			/* operator or function name */
	List	   *args;			/* argument types */
	int			number;			/* strategy num or support proc num */
	bool		recheck;		/* only used for operators */
	/* fields used for a storagetype item: */
	TypeName   *storedtype;		/* datatype stored in index */
} CreateOpClassItem;

/* ----------------------
 *		DROP Statement, applies to:
 *        Table, External Table, Sequence, View, Index, Type, Domain, 
 *        Conversion, Schema, Filespace
 * ----------------------
 */

typedef struct DropStmt
{
	NodeTag		type;
	List	   *objects;		/* list of sublists of names (as Values) */
	ObjectType	removeType;		/* object type */
	DropBehavior behavior;		/* RESTRICT or CASCADE behavior */
	bool		missing_ok;		/* skip error if object is missing? */
	bool		bAllowPartn;	/* allow action on a partition */
} DropStmt;

/* ----------------------
 *		Drop Rule|Trigger Statement
 *
 * In general this may be used for dropping any property of a relation;
 * for example, someday soon we may have DROP ATTRIBUTE.
 * ----------------------
 */

typedef struct DropPropertyStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* owning relation */
	char	   *property;		/* name of rule, trigger, etc */
	ObjectType	removeType;		/* OBJECT_RULE or OBJECT_TRIGGER */
	DropBehavior behavior;		/* RESTRICT or CASCADE behavior */
	bool		missing_ok;		/* skip error if missing? */
} DropPropertyStmt;

/* ----------------------
 *				Truncate Table Statement
 * ----------------------
 */
typedef struct TruncateStmt
{
	NodeTag		type;
	List	   *relations;		/* relations (RangeVars) to be truncated */
	DropBehavior behavior;		/* RESTRICT or CASCADE behavior */
	List	   *relids;
	List	   *new_heap_oids;
	List	   *new_toast_oids;
	List	   *new_aoseg_oids;
	List	   *new_aoblkdir_oids;
	List	   *new_ind_oids;
} TruncateStmt;

/* ----------------------
 *				Comment On Statement
 * ----------------------
 */
typedef struct CommentStmt
{
	NodeTag		type;
	ObjectType	objtype;		/* Object's type */
	List	   *objname;		/* Qualified name of the object */
	List	   *objargs;		/* Arguments if needed (eg, for functions) */
	char	   *comment;		/* Comment to insert, or NULL to remove */
} CommentStmt;

/* ----------------------
 *		Declare Cursor Statement
 *
 * Note: the "query" field of DeclareCursorStmt is only used in the raw grammar
 * output.	After parse analysis it's set to null, and the Query points to the
 * DeclareCursorStmt, not vice versa.
 * ----------------------
 */
#define CURSOR_OPT_BINARY		0x0001	/* BINARY */
#define CURSOR_OPT_SCROLL		0x0002	/* SCROLL explicitly given */
#define CURSOR_OPT_NO_SCROLL	0x0004	/* NO SCROLL explicitly given */
#define CURSOR_OPT_INSENSITIVE	0x0008	/* INSENSITIVE */
#define CURSOR_OPT_HOLD			0x0010	/* WITH HOLD */
#define CURSOR_OPT_FAST_PLAN	0x0020	/* prefer fast-start plan */

typedef struct DeclareCursorStmt
{
	NodeTag		type;
	char	   *portalname;		/* name of the portal (cursor) */
	int			options;		/* bitmask of options (see above) */
	Node	   *query;			/* the raw SELECT query */
	bool		is_simply_updatable;
} DeclareCursorStmt;

/* ----------------------
 *		Close Portal Statement
 * ----------------------
 */
typedef struct ClosePortalStmt
{
	NodeTag		type;
	char	   *portalname;		/* name of the portal (cursor) */
	/* NULL means CLOSE ALL */
} ClosePortalStmt;

/* ----------------------
 *		Fetch Statement (also Move)
 * ----------------------
 */
typedef enum FetchDirection
{
	/* for these, howMany is how many rows to fetch; FETCH_ALL means ALL */
	FETCH_FORWARD,
	FETCH_BACKWARD,
	/* for these, howMany indicates a position; only one row is fetched */
	FETCH_ABSOLUTE,
	FETCH_RELATIVE
} FetchDirection;

#define FETCH_ALL	INT64CONST(0x7FFFFFFFFFFFFFFF)

typedef struct FetchStmt
{
	NodeTag		type;
	FetchDirection direction;	/* see above */
	int64		howMany;		/* number of rows, or position argument */
	char	   *portalname;		/* name of portal (cursor) */
	bool		ismove;			/* TRUE if MOVE */
} FetchStmt;

/* ----------------------
 *		Create Index Statement
 * ----------------------
 */
typedef struct IndexStmt
{
	NodeTag		type;
	char	   *idxname;		/* name of new index, or NULL for default */
	RangeVar   *relation;		/* relation to build index on */
	char	   *accessMethod;	/* name of access method (eg. btree) */
	char	   *tableSpace;		/* tablespace, or NULL to use parent's */
	List	   *indexParams;	/* a list of IndexElem */
	List	   *options;		/* options from WITH clause */
	Node	   *whereClause;	/* qualification (partial-index predicate) */
	List	   *rangetable;		/* range table for qual and/or expressions,
								 * filled in by transformStmt() */
	bool		is_part_child;	/* in service of a part of a partition? */
	bool		unique;			/* is index unique? */
	bool		primary;		/* is index on primary key? */
	bool		isconstraint;	/* is it from a CONSTRAINT clause? */
	char	   *altconname;		/* constraint name, if desired name differs
								 * from idxname and isconstraint, else NULL. */
	Oid			constrOid;		/* constraint oid */
	bool		concurrent;		/* should this be a concurrent index build? */
	List		*idxOids;		/* For MPP. We use List here because the
								 * bitmap index needs 3 additional oids. */
	bool		do_part;		/* build indexes for child partitions */
} IndexStmt;

/* ----------------------
 *		Create Function Statement
 * ----------------------
 */
typedef struct CreateFunctionStmt
{
	NodeTag		type;
	bool		replace;		/* T => replace if already exists */
	List	   *funcname;		/* qualified name of function to create */
	List	   *parameters;		/* a list of FunctionParameter */
	TypeName   *returnType;		/* the return type */
	List	   *options;		/* a list of DefElem */
	List	   *withClause;		/* a list of DefElem */
	Oid			funcOid;
	Oid			shelltypeOid;
} CreateFunctionStmt;

typedef enum FunctionParameterMode
{
	/* the assigned enum values appear in pg_proc, don't change 'em! */
	FUNC_PARAM_IN = 'i',		/* input only */
	FUNC_PARAM_OUT = 'o',		/* output only */
	FUNC_PARAM_INOUT = 'b',		/* both */
	FUNC_PARAM_VARIADIC = 'v',	/* variadic (always input) */
	FUNC_PARAM_TABLE = 't'		/* table (always output) */
} FunctionParameterMode;

typedef struct FunctionParameter
{
	NodeTag		type;
	char	   *name;			/* parameter name, or NULL if not given */
	TypeName   *argType;		/* TypeName for parameter type */
	FunctionParameterMode mode; /* IN/OUT/INOUT */
} FunctionParameter;

typedef struct AlterFunctionStmt
{
	NodeTag		type;
	FuncWithArgs *func;			/* name and args of function */
	List	   *actions;		/* list of DefElem */
} AlterFunctionStmt;

/* ----------------------
 *		Drop {Function|Aggregate|Operator} Statement
 * ----------------------
 */
typedef struct RemoveFuncStmt
{
	NodeTag		type;
	ObjectType	kind;			/* function, aggregate, operator */
	List	   *name;			/* qualified name of object to drop */
	List	   *args;			/* types of the arguments */
	DropBehavior behavior;		/* RESTRICT or CASCADE behavior */
	bool		missing_ok;		/* skip error if missing? */
} RemoveFuncStmt;

/* ----------------------
 *		Drop Operator Class Statement
 * ----------------------
 */
typedef struct RemoveOpClassStmt
{
	NodeTag		type;
	List	   *opclassname;	/* qualified name (list of Value strings) */
	char	   *amname;			/* name of index AM opclass is for */
	DropBehavior behavior;		/* RESTRICT or CASCADE behavior */
	bool		missing_ok;		/* skip error if missing? */
} RemoveOpClassStmt;

/* ----------------------
 *		Alter Object Rename Statement
 * ----------------------
 */
typedef struct RenameStmt
{
	NodeTag		type;
	ObjectType	renameType;		/* OBJECT_TABLE, OBJECT_COLUMN, etc */
	RangeVar   *relation;		/* in case it's a table */
	Oid			objid;			/* in case it's a table */
	List	   *object;			/* in case it's some other object */
	List	   *objarg;			/* argument types, if applicable */
	char	   *subname;		/* name of contained object (column, rule,
								 * trigger, etc) */
	char	   *newname;		/* the new name */
	bool		bAllowPartn;	/* allow action on a partition */
} RenameStmt;

/* ----------------------
 *		ALTER object SET SCHEMA Statement
 * ----------------------
 */
typedef struct AlterObjectSchemaStmt
{
	NodeTag		type;
	ObjectType objectType;		/* OBJECT_TABLE, OBJECT_TYPE, etc */
	RangeVar   *relation;		/* in case it's a table */
	List	   *object;			/* in case it's some other object */
	List	   *objarg;			/* argument types, if applicable */
	char	   *addname;		/* additional name if needed */
	char	   *newschema;		/* the new schema */
} AlterObjectSchemaStmt;

/* ----------------------
 *		Alter Object Owner Statement
 * ----------------------
 */
typedef struct AlterOwnerStmt
{
	NodeTag		type;
	ObjectType objectType;		/* OBJECT_TABLE, OBJECT_TYPE, etc */
	RangeVar   *relation;		/* in case it's a table */
	List	   *object;			/* in case it's some other object */
	List	   *objarg;			/* argument types, if applicable */
	char	   *addname;		/* additional name if needed */
	char	   *newowner;		/* the new owner */
} AlterOwnerStmt;

/* ----------------------
 * ALTER TYPE ... SET DEFAULT ENCODING ()
 * ----------------------
 */
typedef struct AlterTypeStmt
{
	NodeTag		type;
	TypeName   *typname;
	List	   *encoding;
} AlterTypeStmt;

/* ----------------------
 *		Create Rule Statement
 * ----------------------
 */
typedef struct RuleStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* relation the rule is for */
	char	   *rulename;		/* name of the rule */
	Node	   *whereClause;	/* qualifications */
	CmdType		event;			/* SELECT, INSERT, etc */
	bool		instead;		/* is a 'do instead'? */
	List	   *actions;		/* the action statements */
	bool		replace;		/* OR REPLACE */
	Oid			ruleOid;		/* rule Oid */
} RuleStmt;

/* ----------------------
 *		Notify Statement
 * ----------------------
 */
typedef struct NotifyStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* qualified name to notify */
} NotifyStmt;

/* ----------------------
 *		Listen Statement
 * ----------------------
 */
typedef struct ListenStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* qualified name to listen on */
} ListenStmt;

/* ----------------------
 *		Unlisten Statement
 * ----------------------
 */
typedef struct UnlistenStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* qualified name to unlisten on, or '*' */
} UnlistenStmt;

/* ----------------------
 *		{Begin|Commit|Rollback} Transaction Statement
 * ----------------------
 */
typedef enum TransactionStmtKind
{
	TRANS_STMT_BEGIN,
	TRANS_STMT_START,			/* semantically identical to BEGIN */
	TRANS_STMT_COMMIT,
	TRANS_STMT_ROLLBACK,
	TRANS_STMT_SAVEPOINT,
	TRANS_STMT_RELEASE,
	TRANS_STMT_ROLLBACK_TO,
	TRANS_STMT_PREPARE,
	TRANS_STMT_COMMIT_PREPARED,
	TRANS_STMT_ROLLBACK_PREPARED
} TransactionStmtKind;

typedef struct TransactionStmt
{
	NodeTag		type;
	TransactionStmtKind kind;	/* see above */
	List	   *options;		/* for BEGIN/START and savepoint commands */
	char	   *gid;			/* for two-phase-commit related commands */
} TransactionStmt;

/* ----------------------
 *		Create Type Statement, composite types
 * ----------------------
 */
typedef struct CompositeTypeStmt
{
	NodeTag		type;
	RangeVar   *typevar;		/* the composite type to be created */
	List	   *coldeflist;		/* list of ColumnDef nodes */
	Oid			relOid;
	Oid			comptypeOid;
} CompositeTypeStmt;


/* ----------------------
 *		Create View Statement
 * ----------------------
 */
typedef struct ViewStmt
{
	NodeTag		type;
	RangeVar   *view;			/* the view to be created */
	List	   *aliases;		/* target column names */
	Query	   *query;			/* the SQL statement */
	bool		replace;		/* replace an existing view? */
	Oid         relOid;			/* create view with this relOid */
	Oid			comptypeOid;
	Oid			rewriteOid;		/* Oid for rewrite rule */
} ViewStmt;

/* ----------------------
 *		Load Statement
 * ----------------------
 */
typedef struct LoadStmt
{
	NodeTag		type;
	char	   *filename;		/* file to load */
} LoadStmt;

/* ----------------------
 *		Createdb Statement
 * ----------------------
 */
typedef struct CreatedbStmt
{
	NodeTag		type;
	char	   *dbname;			/* name of database to create */
	List	   *options;		/* List of DefElem nodes */
	Oid			dbOid;
} CreatedbStmt;

/* ----------------------
 *	Alter Database
 * ----------------------
 */
typedef struct AlterDatabaseStmt
{
	NodeTag		type;
	char	   *dbname;			/* name of database to alter */
	List	   *options;		/* List of DefElem nodes */
} AlterDatabaseStmt;

typedef struct AlterDatabaseSetStmt
{
	NodeTag		type;
	char	   *dbname;
	char	   *variable;
	List	   *value;
} AlterDatabaseSetStmt;

/* ----------------------
 *		Dropdb Statement
 * ----------------------
 */
typedef struct DropdbStmt
{
	NodeTag		type;
	char	   *dbname;			/* database to drop */
	bool		missing_ok;		/* skip error if db is missing? */
} DropdbStmt;

/* ----------------------
 *		Cluster Statement (support pbrown's cluster index implementation)
 * ----------------------
 */
typedef struct ClusterStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* relation being indexed, or NULL if all */
	char	   *indexname;		/* original index defined */
	TableOidInfo oidInfo;
	List 	   *new_ind_oids;	/* new OIDs for indexes */
} ClusterStmt;

/* ----------------------
 *		Vacuum and Analyze Statements
 *
 * Even though these are nominally two statements, it's convenient to use
 * just one node type for both.
 * ----------------------
 */
typedef struct VacuumStmt
{
	NodeTag		type;
	bool		vacuum;			/* do VACUUM step */
	bool		full;			/* do FULL (non-concurrent) vacuum */
	bool		analyze;		/* do ANALYZE step */
	bool		verbose;		/* print progress info */
	bool		rootonly;		/* only ANALYZE root partition tables */
	int			freeze_min_age;	/* min freeze age, or -1 to use default */
	RangeVar   *relation;		/* single table to process, or NULL */
	List	   *va_cols;		/* list of column names, or NIL for all */

	/* Object IDs for relations which expand from partition definitions */
	List	   *expanded_relids;

	/*
	 * OIDs for relfilenode, the internal heap and index relfilenodes for a bitmap index.
	 * We need these since vacuuming a bitmap index is done through reindex.
	 */
	List *extra_oids;
} VacuumStmt;

/* ----------------------
 *		Explain Statement
 * ----------------------
 */
typedef struct ExplainStmt
{
	NodeTag		type;
	Query	   *query;			/* the query */
	bool		verbose;		/* print plan info */
	bool		analyze;		/* get statistics by executing plan */
	bool		dxl;			/* display plan in dxl format */
} ExplainStmt;

/* ----------------------
 * Checkpoint Statement
 * ----------------------
 */
typedef struct CheckPointStmt
{
	NodeTag		type;
} CheckPointStmt;

/* ----------------------
 * Set Statement
 * ----------------------
 */

typedef struct VariableSetStmt
{
	NodeTag		type;
	char	   *name;
	List	   *args;
	bool		is_local;		/* SET LOCAL */
} VariableSetStmt;

/* ----------------------
 * Show Statement
 * ----------------------
 */

typedef struct VariableShowStmt
{
	NodeTag		type;
	char	   *name;
} VariableShowStmt;

/* ----------------------
 * Reset Statement
 * ----------------------
 */

typedef struct VariableResetStmt
{
	NodeTag		type;
	char	   *name;
} VariableResetStmt;

/* ----------------------
 *		LOCK Statement
 * ----------------------
 */
typedef struct LockStmt
{
	NodeTag		type;
	List	   *relations;		/* relations to lock */
	int			mode;			/* lock mode */
	bool		nowait;			/* no wait mode */
} LockStmt;

/* ----------------------
 *		SET CONSTRAINTS Statement
 * ----------------------
 */
typedef struct ConstraintsSetStmt
{
	NodeTag		type;
	List	   *constraints;	/* List of names as RangeVars */
	bool		deferred;
} ConstraintsSetStmt;

/* ----------------------
 *		REINDEX Statement
 * ----------------------
 */
typedef struct ReindexStmt
{
	NodeTag		type;
	ObjectType	kind;			/* OBJECT_INDEX, OBJECT_TABLE, OBJECT_DATABASE */
	RangeVar   *relation;		/* Table or index to reindex */
	const char *name;			/* name of database to reindex */
	bool		do_system;		/* include system tables in database case */
	bool		do_user;		/* include user tables in database case */
	List	   *new_ind_oids;	/* index oids */
} ReindexStmt;

/* ----------------------
 *		CREATE CONVERSION Statement
 * ----------------------
 */
typedef struct CreateConversionStmt
{
	NodeTag		type;
	List	   *conversion_name;	/* Name of the conversion */
	char	   *for_encoding_name;		/* source encoding name */
	char	   *to_encoding_name;		/* destination encoding name */
	List	   *func_name;		/* qualified conversion function name */
	bool		def;			/* is this a default conversion? */
	Oid			convOid;
} CreateConversionStmt;

/* ----------------------
 *	CREATE CAST Statement
 * ----------------------
 */
typedef struct CreateCastStmt
{
	NodeTag		type;
	TypeName   *sourcetype;
	TypeName   *targettype;
	FuncWithArgs *func;
	CoercionContext context;
	Oid			castOid;
} CreateCastStmt;

/* ----------------------
 *	DROP CAST Statement
 * ----------------------
 */
typedef struct DropCastStmt
{
	NodeTag		type;
	TypeName   *sourcetype;
	TypeName   *targettype;
	DropBehavior behavior;
	bool		missing_ok;		/* skip error if missing? */
} DropCastStmt;


/* ----------------------
 *		PREPARE Statement
 * ----------------------
 */
typedef struct PrepareStmt
{
	NodeTag		type;
	char	   *name;			/* Name of plan, arbitrary */
	List	   *argtypes;		/* Types of parameters (List of TypeName) */
	List	   *argtype_oids;	/* Types of parameters (OIDs) */
	Query	   *query;			/* The query itself */
} PrepareStmt;


/* ----------------------
 *		EXECUTE Statement
 * ----------------------
 */

typedef struct ExecuteStmt
{
	NodeTag		type;
	char	   *name;			/* The name of the plan to execute */
	IntoClause *into;			/* CTAS target or NULL */
//	RangeVar   *into;			/* Optional table to store results in */
//	List	   *intoOptions;	/* Options from WITH clause */
//	OnCommitAction into_on_commit;		/* What do we do at COMMIT? */
//	char	   *into_tbl_space; /* Tablespace to use, or NULL */
	List	   *params;			/* Values to assign to parameters */
} ExecuteStmt;


/* ----------------------
 *		DEALLOCATE Statement
 * ----------------------
 */
typedef struct DeallocateStmt
{
	NodeTag		type;
	char	   *name;			/* The name of the plan to remove */
	/* NULL means DEALLOCATE ALL */
} DeallocateStmt;

/*
 *		DROP OWNED statement
 */
typedef struct DropOwnedStmt
{
	NodeTag		type;
	List	   *roles;
	DropBehavior behavior;
} DropOwnedStmt;

/*
 *		REASSIGN OWNED statement
 */
typedef struct ReassignOwnedStmt
{
	NodeTag		type;
	List	   *roles;
	char	   *newrole;
} ReassignOwnedStmt;

#endif   /* PARSENODES_H */
