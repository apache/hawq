/*-------------------------------------------------------------------------
 *
 * parse_expr.c
 *	  handle expressions in parser
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/parser/parse_expr.c,v 1.198 2006/10/04 00:29:55 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/gramparse.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

bool		Transform_null_equals = false;

static Node *transformParamRef(ParseState *pstate, ParamRef *pref);
static Node *transformAExprOp(ParseState *pstate, A_Expr *a);
static Node *transformAExprAnd(ParseState *pstate, A_Expr *a);
static Node *transformAExprOr(ParseState *pstate, A_Expr *a);
static Node *transformAExprNot(ParseState *pstate, A_Expr *a);
static Node *transformAExprOpAny(ParseState *pstate, A_Expr *a);
static Node *transformAExprOpAll(ParseState *pstate, A_Expr *a);
static Node *transformAExprDistinct(ParseState *pstate, A_Expr *a);
static Node *transformAExprNullIf(ParseState *pstate, A_Expr *a);
static Node *transformAExprOf(ParseState *pstate, A_Expr *a);
static Node *transformAExprIn(ParseState *pstate, A_Expr *a);
static Node *transformFuncCall(ParseState *pstate, FuncCall *fn);
static Node *transformSubLink(ParseState *pstate, SubLink *sublink);
static Node *transformArrayExpr(ParseState *pstate, ArrayExpr *a);
static Node *transformRowExpr(ParseState *pstate, RowExpr *r);
static Node *transformTableValueExpr(ParseState *pstate, TableValueExpr *t);
static Node *transformCoalesceExpr(ParseState *pstate, CoalesceExpr *c);
static Node *transformMinMaxExpr(ParseState *pstate, MinMaxExpr *m);
static Node *transformBooleanTest(ParseState *pstate, BooleanTest *b);
static Node *transformColumnRef(ParseState *pstate, ColumnRef *cref);
static Node *transformWholeRowRef(ParseState *pstate, char *catalogname, char *schemaname,
					 char *relname, int location);
static Node *transformBooleanTest(ParseState *pstate, BooleanTest *b);
static Node *transformIndirection(ParseState *pstate, Node *basenode,
					 List *indirection);
static Node *transformGroupingFunc(ParseState *pstate, GroupingFunc *gf);
static Node *transformPercentileExpr(ParseState *pstate, PercentileExpr *p);
static Node *typecast_expression(ParseState *pstate, Node *expr,
					TypeName *typname);
static Node *make_row_comparison_op(ParseState *pstate, List *opname,
					   List *largs, List *rargs, int location);
static Node *make_row_distinct_op(ParseState *pstate, List *opname,
					 RowExpr *lrow, RowExpr *rrow, int location);
static Expr *make_distinct_op(ParseState *pstate, List *opname,
				 Node *ltree, Node *rtree, int location);
static bool isWhenIsNotDistinctFromExpr(Node *warg);
static char *percentileFuncString(PercentileExpr *p, Oid *argtypes, int arglen,
					 Oid *sorttypes, int sortlen);

/*
 * transformExpr -
 *	  Analyze and transform expressions. Type checking and type casting is
 *	  done here. The optimizer and the executor cannot handle the original
 *	  (raw) expressions collected by the parse tree. Hence the transformation
 *	  here.
 *
 * NOTE: there are various cases in which this routine will get applied to
 * an already-transformed expression.  Some examples:
 *	1. At least one construct (BETWEEN/AND) puts the same nodes
 *	into two branches of the parse tree; hence, some nodes
 *	are transformed twice.
 *	2. Another way it can happen is that coercion of an operator or
 *	function argument to the required type (via coerce_type())
 *	can apply transformExpr to an already-transformed subexpression.
 *	An example here is "SELECT count(*) + 1.0 FROM table".
 * While it might be possible to eliminate these cases, the path of
 * least resistance so far has been to ensure that transformExpr() does
 * no damage if applied to an already-transformed tree.  This is pretty
 * easy for cases where the transformation replaces one node type with
 * another, such as A_Const => Const; we just do nothing when handed
 * a Const.  More care is needed for node types that are used as both
 * input and output of transformExpr; see SubLink for example.
 *
 * CDB: On return, pstate->breadcrumb.node points to the original 'expr' node.
 * This is intended to provide a default cursor location in case an error is
 * reported during further processing of the result, such as conversion to a
 * target type.
 */
Node *
transformExpr(ParseState *pstate, Node *expr)
{
	Node	               *result;
	ParseStateBreadCrumb    savebreadcrumb;

	if (expr == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	/* CDB: Drop a breadcrumb, then push location stack. Must pop before return! */
	Assert(pstate);
	pstate->p_breadcrumb.node = (Node *)expr;   
	savebreadcrumb = pstate->p_breadcrumb;
	pstate->p_breadcrumb.pop = &savebreadcrumb;

	result = NULL;
	switch (nodeTag(expr))
	{
		case T_ColumnRef:
			result = transformColumnRef(pstate, (ColumnRef *) expr);
			break;

		case T_ParamRef:
			result = transformParamRef(pstate, (ParamRef *) expr);
			break;

		case T_A_Const:
			{
				A_Const    *con = (A_Const *) expr;
				Value	   *val = &con->val;

				result = (Node *) make_const(pstate, val, -1);
				if (con->typname != NULL)
					result = typecast_expression(pstate, result,
												 con->typname);
				break;
			}

		case T_A_Indirection:
			{
				A_Indirection *ind = (A_Indirection *) expr;

				result = transformExpr(pstate, ind->arg);
				result = transformIndirection(pstate, result,
											  ind->indirection);
				break;
			}

		case T_TypeCast:
			{
				TypeCast   *tc = (TypeCast *) expr;
				Node	   *arg = transformExpr(pstate, tc->arg);

				result = typecast_expression(pstate, arg, tc->typname);
				break;
			}

		case T_A_Expr:
			{
				A_Expr	   *a = (A_Expr *) expr;

				switch (a->kind)
				{
					case AEXPR_OP:
						result = transformAExprOp(pstate, a);
						break;
					case AEXPR_AND:
						result = transformAExprAnd(pstate, a);
						break;
					case AEXPR_OR:
						result = transformAExprOr(pstate, a);
						break;
					case AEXPR_NOT:
						result = transformAExprNot(pstate, a);
						break;
					case AEXPR_OP_ANY:
						result = transformAExprOpAny(pstate, a);
						break;
					case AEXPR_OP_ALL:
						result = transformAExprOpAll(pstate, a);
						break;
					case AEXPR_DISTINCT:
						result = transformAExprDistinct(pstate, a);
						break;
					case AEXPR_NULLIF:
						result = transformAExprNullIf(pstate, a);
						break;
					case AEXPR_OF:
						result = transformAExprOf(pstate, a);
						break;
					case AEXPR_IN:
						result = transformAExprIn(pstate, a);
						break;
					default:
						elog(ERROR, "unrecognized A_Expr kind: %d", a->kind);
				}
				break;
			}

		case T_FuncCall:
			result = transformFuncCall(pstate, (FuncCall *) expr);
			break;


		case T_SubLink:
			result = transformSubLink(pstate, (SubLink *) expr);
			break;

		case T_CaseExpr:
			result = transformCaseExpr(pstate, (CaseExpr *) expr);
			break;

		case T_ArrayExpr:
			result = transformArrayExpr(pstate, (ArrayExpr *) expr);
			break;

		case T_RowExpr:
			result = transformRowExpr(pstate, (RowExpr *) expr);
			break;

		case T_TableValueExpr:
			result = transformTableValueExpr(pstate, (TableValueExpr *) expr);
			break;

		case T_CoalesceExpr:
			result = transformCoalesceExpr(pstate, (CoalesceExpr *) expr);
			break;

		case T_MinMaxExpr:
			result = transformMinMaxExpr(pstate, (MinMaxExpr *) expr);
			break;

		case T_NullTest:
			{
				NullTest   *n = (NullTest *) expr;

				n->arg = (Expr *) transformExpr(pstate, (Node *) n->arg);
				/* the argument can be any type, so don't coerce it */
				result = expr;
				break;
			}

		case T_BooleanTest:
			result = transformBooleanTest(pstate, (BooleanTest *) expr);
			break;

		case T_CurrentOfExpr:
			{
				/*
				 * The target RTE must be simply updatable. If not, we error out
				 * early here to avoid having to deal with error cases later:
				 * rewriting/planning against views, for example.
				 */
				Assert(pstate->p_target_rangetblentry != NULL);
				if (!isSimplyUpdatableRelation(pstate->p_target_rangetblentry->relid))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("\"%s\" is not simply updatable",
									pstate->p_target_relation->rd_rel->relname.data)));

				CurrentOfExpr *c = (CurrentOfExpr *) expr;
				int sublevels_up;
				c->cvarno = RTERangeTablePosn(pstate,
											  pstate->p_target_rangetblentry,
											  &sublevels_up);
				c->target_relid = pstate->p_target_rangetblentry->relid;
				Assert(sublevels_up == 0);
				result = expr;
				break;
			}

		case T_GroupingFunc:
			{
				GroupingFunc *gf = (GroupingFunc *)expr;
				result = transformGroupingFunc(pstate, gf);
				break;
			}

		case T_PartitionBoundSpec:
			{
				PartitionBoundSpec *in = (PartitionBoundSpec *)expr;
				PartitionRangeItem *ri;
				List *out = NIL;
				ListCell *lc;

				if (in->partStart)
				{
					ri = (PartitionRangeItem *)in->partStart;

					/* ALTER TABLE ... ADD PARTITION might feed
					 * "pre-cooked" expressions into the boundspec for
					 * range items (which are Lists) 
					 */
					{
						Assert(IsA(in->partStart, PartitionRangeItem));

						foreach(lc, ri->partRangeVal)
						{
							Node *n = lfirst(lc);
							out = lappend(out, transformExpr(pstate, n));
						}
						ri->partRangeVal = out;
						out = NIL;
					}
				}
				if (in->partEnd)
				{
					ri = (PartitionRangeItem *)in->partEnd;

					/* ALTER TABLE ... ADD PARTITION might feed
					 * "pre-cooked" expressions into the boundspec for
					 * range items (which are Lists) 
					 */
					{
						Assert(IsA(in->partEnd, PartitionRangeItem));
						foreach(lc, ri->partRangeVal)
						{
							Node *n = lfirst(lc);
							out = lappend(out, transformExpr(pstate, n));
						}
						ri->partRangeVal = out;
						out = NIL;
					}
				}
				if (in->partEvery)
				{
					ri = (PartitionRangeItem *)in->partEvery;
					Assert(IsA(in->partEvery, PartitionRangeItem));
					foreach(lc, ri->partRangeVal)
					{
						Node *n = lfirst(lc);
						out = lappend(out, transformExpr(pstate, n));
					}
					ri->partRangeVal = out;
				}

				result = (Node *)in;
			}
			break;

		case T_PercentileExpr:
			result = transformPercentileExpr(pstate, (PercentileExpr *) expr);
			break;

			/*********************************************
			 * Quietly accept node types that may be presented when we are
			 * called on an already-transformed tree.
			 *
			 * Do any other node types need to be accepted?  For now we are
			 * taking a conservative approach, and only accepting node
			 * types that are demonstrably necessary to accept.
			 *********************************************/
		case T_Var:
		case T_Const:
		case T_Param:
		case T_Aggref:
		case T_ArrayRef:
		case T_FuncExpr:
		case T_OpExpr:
		case T_DistinctExpr:
		case T_ScalarArrayOpExpr:
		case T_NullIfExpr:
		case T_BoolExpr:
		case T_FieldSelect:
		case T_FieldStore:
		case T_RelabelType:
		case T_ConvertRowtypeExpr:
		case T_CaseTestExpr:
		case T_CoerceToDomain:
		case T_CoerceToDomainValue:
		case T_SetToDefault:
		case T_GroupId:
		case T_Integer:
			{
				result = (Node *) expr;
				break;
			}

		default:
			/* should not reach here */
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
			break;
	}

	/* CDB: Pop error location stack, leaving breadcrumb on our input expr. */
	Assert(pstate->p_breadcrumb.pop == &savebreadcrumb);
	pstate->p_breadcrumb = savebreadcrumb;

	return result;
}


/*
 * parse_expr_location
 *
 * Looks for a node with a 'location' field in the given parse tree expression.
 * Returns the location (the byte offset of a token within the source string),
 * or -1 if unknown.
 *
 * NB: For best results, expr should be an untransformed expression, because
 * after transformExpr() most nodes have no 'location' field.
 */
int
parse_expr_location(Expr *expr)
{
    int         loc;
    ListCell   *cell;

    if (expr == NULL)
         return -1;

    switch (nodeTag(expr))
    {
        case T_ColumnRef:
            loc = ((ColumnRef *)expr)->location;
            break;

        case T_ParamRef:
            loc = ((ParamRef *)expr)->location;
            break;

        case T_A_Const:
            loc = ((A_Const *)expr)->location;
            break;

        case T_A_Indirection:
        {
            A_Indirection  *ind = (A_Indirection *)expr;

            loc = parse_expr_location((Expr *)ind->arg);
            if (loc < 0)
                loc = parse_expr_location((Expr *)ind->indirection);
            break;
        }

        case T_TypeCast:
        {
            TypeCast   *typecast = (TypeCast *)expr;

            loc = parse_expr_location((Expr *)typecast->arg);
            if (loc < 0)
                loc = parse_expr_location((Expr *)typecast->typname);
            break;
        }

        case T_A_Expr:
            loc = ((A_Expr *)expr)->location;
            break;

        case T_FuncCall:
            loc = ((FuncCall *)expr)->location;
            break;

        case T_SubLink:
            loc = ((SubLink *)expr)->location;
            break;

        case T_CaseExpr:
        {
            CaseExpr   *caseexpr = (CaseExpr *)expr;
            CaseWhen   *casewhen;

            loc = parse_expr_location(caseexpr->arg);
            foreach(cell, caseexpr->args)
            {
                if (loc >= 0)
                    break;
                casewhen = (CaseWhen *)lfirst(cell);
                loc = parse_expr_location(casewhen->expr);
                if (loc < 0)
                    loc = parse_expr_location(casewhen->result);
            }
            if (loc < 0)
                loc = parse_expr_location(caseexpr->defresult);
            break;
        }

        case T_ArrayExpr:
            loc = parse_expr_location((Expr *)((ArrayExpr *)expr)->elements);
            break;

        case T_RowExpr:
            loc = parse_expr_location((Expr *)((RowExpr *)expr)->args);
            break;

        case T_CoalesceExpr:
            loc = parse_expr_location((Expr *)((CoalesceExpr *)expr)->args);
            break;

        case T_MinMaxExpr:
            loc = parse_expr_location((Expr *)((MinMaxExpr *)expr)->args);
            break;

        case T_NullTest:
            loc = parse_expr_location(((NullTest *)expr)->arg);
            break;

        case T_BooleanTest:
            loc = parse_expr_location(((BooleanTest *)expr)->arg);
            break;

        case T_GroupingFunc:
            loc = parse_expr_location((Expr *)((GroupingFunc *)expr)->args);
            break;

        /* Above nodes taken from transformExpr().  Additional nodes below. */

        case T_JoinExpr:
            loc = parse_expr_location((Expr *)((JoinExpr *)expr)->rarg);
            break;

        case T_List:
            loc = -1;
            foreach(cell, (List *)expr)
            {
                loc = parse_expr_location((Expr *)lfirst(cell));
                if (loc >= 0)
                    break;
            }
            break;

        case T_RangeFunction:
            loc = parse_expr_location((Expr *)((RangeFunction *)expr)->funccallnode);
            break;

        case T_RangeSubselect:
            loc = parse_expr_location((Expr *)((RangeSubselect *)expr)->subquery);
            break;

        case T_RangeVar:
            loc = ((RangeVar *)expr)->location;
            break;

        case T_ResTarget:
            loc = ((ResTarget *)expr)->location;
            break;

        case T_SelectStmt:
        {
            SelectStmt *ss = (SelectStmt *)expr;

            if (ss->larg)
                loc = parse_expr_location((Expr *)ss->larg);        /* set op */
            else if (ss->valuesLists)
                loc = parse_expr_location((Expr *)ss->valuesLists); /* VALUES */
            else
                loc = parse_expr_location((Expr *)ss->targetList);  /* SELECT */
            break;
        }

        case T_TypeName:
            loc = ((TypeName *)expr)->location;
            break;

        case T_WindowSpec:
            loc = ((WindowSpec *)expr)->location;
            break;

		case T_PercentileExpr:
			loc = ((PercentileExpr *) expr)->location;
			break;

        default:
            loc = -1;
    }
    return loc;
}                               /* parse_expr_location */


static Node *
transformIndirection(ParseState *pstate, Node *basenode, List *indirection)
{
	Node	   *result = basenode;
	List	   *subscripts = NIL;
	ListCell   *i;
	ParseStateBreadCrumb    savebreadcrumb;

	/* CDB: Push error location stack.  Must pop before return! */
	Assert(pstate);
	savebreadcrumb = pstate->p_breadcrumb;
	pstate->p_breadcrumb.pop = &savebreadcrumb;

	/*
	 * We have to split any field-selection operations apart from
	 * subscripting.  Adjacent A_Indices nodes have to be treated as a single
	 * multidimensional subscript operation.
	 */
	foreach(i, indirection)
	{
		Node	   *n = lfirst(i);

		if (IsA(n, A_Indices))
			subscripts = lappend(subscripts, n);
		else
		{
			Assert(IsA(n, String));

			/* process subscripts before this field selection */
			if (subscripts)
				result = (Node *) transformArraySubscripts(pstate,
														   result,
														   exprType(result),
														   InvalidOid,
														   -1,
														   subscripts,
														   NULL);
			subscripts = NIL;

			result = ParseFuncOrColumn(pstate,
									   list_make1(n),
									   list_make1(result),
                                       NIL, false, false, true,
                                       NULL, -1, NULL);
		}
	}
	/* process trailing subscripts, if any */
	if (subscripts)
		result = (Node *) transformArraySubscripts(pstate,
												   result,
												   exprType(result),
												   InvalidOid,
												   -1,
												   subscripts,
												   NULL);

	/* CDB: Pop error location stack. */
	Assert(pstate->p_breadcrumb.pop == &savebreadcrumb);
	pstate->p_breadcrumb = savebreadcrumb;

	return result;
}

static Node *
transformColumnRef(ParseState *pstate, ColumnRef *cref)
{
	int			numnames = list_length(cref->fields);
	Node	   *node;
	int			levels_up;
	ParseStateBreadCrumb    savebreadcrumb;

	/* CDB: Push error location stack.  Must pop before return! */
	Assert(pstate);
	savebreadcrumb = pstate->p_breadcrumb;
	pstate->p_breadcrumb.pop = &savebreadcrumb;
	pstate->p_breadcrumb.node = (Node *)cref;

	/*----------
	 * The allowed syntaxes are:
	 *
	 * A		First try to resolve as unqualified column name;
	 *			if no luck, try to resolve as unqualified table name (A.*).
	 * A.B		A is an unqualified table name; B is either a
	 *			column or function name (trying column name first).
	 * A.B.C	schema A, table B, col or func name C.
	 * A.B.C.D	catalog A, schema B, table C, col or func D.
	 * A.*		A is an unqualified table name; means whole-row value.
	 * A.B.*	whole-row value of table B in schema A.
	 * A.B.C.*	whole-row value of table C in schema B in catalog A.
	 *
	 * We do not need to cope with bare "*"; that will only be accepted by
	 * the grammar at the top level of a SELECT list, and transformTargetList
	 * will take care of it before it ever gets here.  Also, "A.*" etc will
	 * be expanded by transformTargetList if they appear at SELECT top level,
	 * so here we are only going to see them as function or operator inputs.
	 *
	 * Currently, if a catalog name is given then it must equal the current
	 * database name; we check it here and then discard it.
	 *----------
	 */
	switch (numnames)
	{
		case 1:
			{
				char	   *name = strVal(linitial(cref->fields));

				/* Try to identify as an unqualified column */
				node = colNameToVar(pstate, name, false, cref->location);

				if (node == NULL)
				{
					/*
					 * Not known as a column of any range-table entry.
					 *
					 * Consider the possibility that it's VALUE in a domain
					 * check expression.  (We handle VALUE as a name, not a
					 * keyword, to avoid breaking a lot of applications that
					 * have used VALUE as a column name in the past.)
					 */
					if (pstate->p_value_substitute != NULL &&
						strcmp(name, "value") == 0)
					{
						node = (Node *) copyObject(pstate->p_value_substitute);
						break;
					}

					/*
					 * Try to find the name as a relation.	Note that only
					 * relations already entered into the rangetable will be
					 * recognized.
					 *
					 * This is a hack for backwards compatibility with
					 * PostQUEL-inspired syntax.  The preferred form now is
					 * "rel.*".
					 */
					if (refnameRangeTblEntry(pstate, NULL /*catalogname*/, NULL /*schemaname*/, name, cref->location,
											 &levels_up) != NULL)
						node = transformWholeRowRef(pstate, NULL /*catalogname*/, NULL, name,
													cref->location);
					else
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_COLUMN),
								 errmsg("column \"%s\" does not exist",
										name),
								 errOmitLocation(true),
								 parser_errposition(pstate, cref->location)));
				}
				break;
			}
		case 2:
			{
				char	   *name1 = strVal(linitial(cref->fields));
				char	   *name2 = strVal(lsecond(cref->fields));

				/* Whole-row reference? */
				if (strcmp(name2, "*") == 0)
				{
					node = transformWholeRowRef(pstate, NULL /*catalogname*/, NULL /*schemaname*/, name1,
												cref->location);
					break;
				}

				/* Try to identify as a once-qualified column */
				node = qualifiedNameToVar(pstate, NULL /*catalogname*/, NULL /*schemaname*/, name1, name2, true,
										  cref->location);
				if (node == NULL)
				{
					/*
					 * Not known as a column of any range-table entry, so try
					 * it as a function call.  Here, we will create an
					 * implicit RTE for tables not already entered.
					 */
					node = transformWholeRowRef(pstate, NULL /*catalogname*/, NULL, name1,
												cref->location);
					node = ParseFuncOrColumn(pstate,
											 list_make1(makeString(name2)),
											 list_make1(node),
											 NIL, false, false, true, NULL,
											 cref->location, NULL);
				}
				break;
			}
		case 3:
			{
				char	   *name1 = strVal(linitial(cref->fields));
				char	   *name2 = strVal(lsecond(cref->fields));
				char	   *name3 = strVal(lthird(cref->fields));

				/* Whole-row reference? */
				if (strcmp(name3, "*") == 0)
				{
					node = transformWholeRowRef(pstate, NULL /*catalogname*/, name1, name2,
												cref->location);
					break;
				}

				/* Try to identify as a twice-qualified column */
				node = qualifiedNameToVar(pstate, NULL /*catalogname*/, name1, name2, name3, true,
										  cref->location);
				if (node == NULL)
				{
					/* Try it as a function call */
					node = transformWholeRowRef(pstate, NULL /*catalogname*/, name1, name2,
												cref->location);
					node = ParseFuncOrColumn(pstate,
											 list_make1(makeString(name3)),
											 list_make1(node),
											 NIL, false, false, true, NULL,
											 cref->location, NULL);
				}
				break;
			}
		case 4:
			{
				char	   *name1 = strVal(linitial(cref->fields));
				char	   *name2 = strVal(lsecond(cref->fields));
				char	   *name3 = strVal(lthird(cref->fields));
				char	   *name4 = strVal(lfourth(cref->fields));

				/* Whole-row reference? */
				if (strcmp(name4, "*") == 0)
				{
					node = transformWholeRowRef(pstate, name1, name2, name3,
												cref->location);
					break;
				}

				/* Try to identify as a twice-qualified column */
				node = qualifiedNameToVar(pstate, name1, name2, name3, name4, true,
										  cref->location);
				if (node == NULL)
				{
					/* Try it as a function call */
					node = transformWholeRowRef(pstate, name1, name2, name3,
												cref->location);
					node = ParseFuncOrColumn(pstate,
											 list_make1(makeString(name4)),
											 list_make1(node),
											 NIL, false, false, true, NULL,
											 cref->location, NULL);
				}
				break;
			}
		default:
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("improper qualified name (too many dotted names): %s",
					   NameListToString(cref->fields)),
					 parser_errposition(pstate, cref->location)));
			node = NULL;		/* keep compiler quiet */
			break;
	}

	/* CDB: Pop error location stack. */
	Assert(pstate->p_breadcrumb.pop == &savebreadcrumb);
	pstate->p_breadcrumb = savebreadcrumb;

	return node;
}

static Node *
transformParamRef(ParseState *pstate, ParamRef *pref)
{
	int			paramno = pref->number;
	ParseState *toppstate;
	Param	   *param;

	/*
	 * Find topmost ParseState, which is where paramtype info lives.
	 */
	toppstate = pstate;
	while (toppstate->parentParseState != NULL)
		toppstate = toppstate->parentParseState;

	/* Check parameter number is in range */
	if (paramno <= 0)			/* probably can't happen? */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_PARAMETER),
				 errmsg("there is no parameter $%d", paramno)));
	if (paramno > toppstate->p_numparams)
	{
		if (!toppstate->p_variableparams)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_PARAMETER),
					 errmsg("there is no parameter $%d",
							paramno),
									   errOmitLocation(true)));
		/* Okay to enlarge param array */
		if (toppstate->p_paramtypes)
			toppstate->p_paramtypes =
				(Oid *) repalloc(toppstate->p_paramtypes,
								 paramno * sizeof(Oid));
		else
			toppstate->p_paramtypes =
				(Oid *) palloc(paramno * sizeof(Oid));
		/* Zero out the previously-unreferenced slots */
		MemSet(toppstate->p_paramtypes + toppstate->p_numparams,
			   0,
			   (paramno - toppstate->p_numparams) * sizeof(Oid));
		toppstate->p_numparams = paramno;
	}
	if (toppstate->p_variableparams)
	{
		/* If not seen before, initialize to UNKNOWN type */
		if (toppstate->p_paramtypes[paramno - 1] == InvalidOid)
			toppstate->p_paramtypes[paramno - 1] = UNKNOWNOID;
	}

	param = makeNode(Param);
	param->paramkind = PARAM_EXTERN;
	param->paramid = paramno;
	param->paramtype = toppstate->p_paramtypes[paramno - 1];

	return (Node *) param;
}

static Node *
transformAExprOp(ParseState *pstate, A_Expr *a)
{
	Node	   *lexpr = a->lexpr;
	Node	   *rexpr = a->rexpr;
	Node	   *result;

	/*
	 * Special-case "foo = NULL" and "NULL = foo" for compatibility with
	 * standards-broken products (like Microsoft's).  Turn these into IS NULL
	 * exprs.
	 */
	if (Transform_null_equals &&
		list_length(a->name) == 1 &&
		strcmp(strVal(linitial(a->name)), "=") == 0 &&
		(exprIsNullConstant(lexpr) || exprIsNullConstant(rexpr)))
	{
		NullTest   *n = makeNode(NullTest);

		n->nulltesttype = IS_NULL;

		if (exprIsNullConstant(lexpr))
			n->arg = (Expr *) rexpr;
		else
			n->arg = (Expr *) lexpr;

		result = transformExpr(pstate, (Node *) n);
	}
	else if (lexpr && IsA(lexpr, RowExpr) &&
			 rexpr && IsA(rexpr, SubLink) &&
			 ((SubLink *) rexpr)->subLinkType == EXPR_SUBLINK)
	{
		/*
		 * Convert "row op subselect" into a ROWCOMPARE sublink. Formerly the
		 * grammar did this, but now that a row construct is allowed anywhere
		 * in expressions, it's easier to do it here.
		 */
		SubLink    *s = (SubLink *) rexpr;

		s->subLinkType = ROWCOMPARE_SUBLINK;
		s->testexpr = lexpr;
		s->operName = a->name;
		result = transformExpr(pstate, (Node *) s);
	}
	else if (lexpr && IsA(lexpr, RowExpr) &&
			 rexpr && IsA(rexpr, RowExpr))
	{
		/* "row op row" */
		lexpr = transformExpr(pstate, lexpr);
		rexpr = transformExpr(pstate, rexpr);
		Assert(IsA(lexpr, RowExpr));
		Assert(IsA(rexpr, RowExpr));

		/* CDB: Drop a breadcrumb in case of error. */
		pstate->p_breadcrumb.node = (Node *)a;

		result = make_row_comparison_op(pstate,
										a->name,
										((RowExpr *) lexpr)->args,
										((RowExpr *) rexpr)->args,
										a->location);
	}
	else
	{
		/* Ordinary scalar operator */
		lexpr = transformExpr(pstate, lexpr);
		rexpr = transformExpr(pstate, rexpr);

		/* CDB: Drop a breadcrumb in case of error. */
		pstate->p_breadcrumb.node = (Node *)a;

		result = (Node *) make_op(pstate,
								  a->name,
								  lexpr,
								  rexpr,
								  a->location);
	}

	return result;
}

static Node *
transformAExprAnd(ParseState *pstate, A_Expr *a)
{
	Node	   *lexpr = transformExpr(pstate, a->lexpr);
	Node	   *rexpr = transformExpr(pstate, a->rexpr);

	lexpr = coerce_to_boolean(pstate, lexpr, "AND");
	rexpr = coerce_to_boolean(pstate, rexpr, "AND");

	return (Node *) makeBoolExpr(AND_EXPR,
								 list_make2(lexpr, rexpr), a->location);
}

static Node *
transformAExprOr(ParseState *pstate, A_Expr *a)
{
	Node	   *lexpr = transformExpr(pstate, a->lexpr);
	Node	   *rexpr = transformExpr(pstate, a->rexpr);

	lexpr = coerce_to_boolean(pstate, lexpr, "OR");
	rexpr = coerce_to_boolean(pstate, rexpr, "OR");

	return (Node *) makeBoolExpr(OR_EXPR,
								 list_make2(lexpr, rexpr), a->location);
}

static Node *
transformAExprNot(ParseState *pstate, A_Expr *a)
{
	Node	   *rexpr = transformExpr(pstate, a->rexpr);

	rexpr = coerce_to_boolean(pstate, rexpr, "NOT");

	return (Node *) makeBoolExpr(NOT_EXPR,
								 list_make1(rexpr), a->location);
}

static Node *
transformAExprOpAny(ParseState *pstate, A_Expr *a)
{
	Node	   *lexpr = transformExpr(pstate, a->lexpr);
	Node	   *rexpr = transformExpr(pstate, a->rexpr);

	return (Node *) make_scalar_array_op(pstate,
										 a->name,
										 true,
										 lexpr,
										 rexpr,
										 a->location);
}

static Node *
transformAExprOpAll(ParseState *pstate, A_Expr *a)
{
	Node	   *lexpr = transformExpr(pstate, a->lexpr);
	Node	   *rexpr = transformExpr(pstate, a->rexpr);

	return (Node *) make_scalar_array_op(pstate,
										 a->name,
										 false,
										 lexpr,
										 rexpr,
										 a->location);
}

static Node *
transformAExprDistinct(ParseState *pstate, A_Expr *a)
{
	Node	   *lexpr = transformExpr(pstate, a->lexpr);
	Node	   *rexpr = transformExpr(pstate, a->rexpr);

	if (lexpr && IsA(lexpr, RowExpr) &&
		rexpr && IsA(rexpr, RowExpr))
	{
		/* "row op row" */
		return make_row_distinct_op(pstate, a->name,
									(RowExpr *) lexpr,
									(RowExpr *) rexpr,
									a->location);
	}
	else
	{
		/* Ordinary scalar operator */
		return (Node *) make_distinct_op(pstate,
										 a->name,
										 lexpr,
										 rexpr,
										 a->location);
	}
}

static Node *
transformAExprNullIf(ParseState *pstate, A_Expr *a)
{
	Node	   *lexpr = transformExpr(pstate, a->lexpr);
	Node	   *rexpr = transformExpr(pstate, a->rexpr);
	Node	   *result;

	result = (Node *) make_op(pstate,
							  a->name,
							  lexpr,
							  rexpr,
							  a->location);
	if (((OpExpr *) result)->opresulttype != BOOLOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("NULLIF requires = operator to yield boolean"),
				 parser_errposition(pstate, a->location)));

	/*
	 * We rely on NullIfExpr and OpExpr being the same struct
	 */
	NodeSetTag(result, T_NullIfExpr);

	return result;
}

static Node *
transformAExprOf(ParseState *pstate, A_Expr *a)
{
	/*
	 * Checking an expression for match to a list of type names. Will result
	 * in a boolean constant node.
	 */
	Node	   *lexpr = transformExpr(pstate, a->lexpr);
	ListCell   *telem;
	Oid			ltype,
				rtype;
	bool		matched = false;

	ltype = exprType(lexpr);
	foreach(telem, (List *) a->rexpr)
	{
		rtype = typenameTypeId(pstate, lfirst(telem));
		matched = (rtype == ltype);
		if (matched)
			break;
	}

	/*
	 * We have two forms: equals or not equals. Flip the sense of the result
	 * for not equals.
	 */
	if (strcmp(strVal(linitial(a->name)), "<>") == 0)
		matched = (!matched);

	return makeBoolConst(matched, false);
}

static Node *
transformAExprIn(ParseState *pstate, A_Expr *a)
{
	Node	   *lexpr;
	List	   *rexprs;
	List	   *typeids;
	bool		useOr;
	bool		haveRowExpr;
	Node	   *result;
	ListCell   *l;

	/*
	 * If the operator is <>, combine with AND not OR.
	 */
	if (strcmp(strVal(linitial(a->name)), "<>") == 0)
		useOr = false;
	else
		useOr = true;

	/*
	 * We try to generate a ScalarArrayOpExpr from IN/NOT IN, but this is only
	 * possible if the inputs are all scalars (no RowExprs) and there is a
	 * suitable array type available.  If not, we fall back to a boolean
	 * condition tree with multiple copies of the lefthand expression.
	 *
	 * First step: transform all the inputs, and detect whether any are
	 * RowExprs.
	 */
	lexpr = transformExpr(pstate, a->lexpr);
	haveRowExpr = (lexpr && IsA(lexpr, RowExpr));
	typeids = list_make1_oid(exprType(lexpr));
	rexprs = NIL;
	foreach(l, (List *) a->rexpr)
	{
		Node	   *rexpr = transformExpr(pstate, lfirst(l));

		haveRowExpr |= (rexpr && IsA(rexpr, RowExpr));
		rexprs = lappend(rexprs, rexpr);
		typeids = lappend_oid(typeids, exprType(rexpr));
	}

	/* CDB: Drop a breadcrumb in case of error. */
	pstate->p_breadcrumb.node = (Node *)a;

	/*
	 * If not forced by presence of RowExpr, try to resolve a common scalar
	 * type for all the expressions, and see if it has an array type. (But if
	 * there's only one righthand expression, we may as well just fall through
	 * and generate a simple = comparison.)
	 */
	if (!haveRowExpr && list_length(rexprs) != 1)
	{
		Oid			scalar_type;
		Oid			array_type;

		/*
		 * Select a common type for the array elements.  Note that since the
		 * LHS' type is first in the list, it will be preferred when there is
		 * doubt (eg, when all the RHS items are unknown literals).
		 */
		scalar_type = select_common_type(typeids, "IN");

		/* Do we have an array type to use? */
		array_type = get_array_type(scalar_type);
		if (array_type != InvalidOid)
		{
			/*
			 * OK: coerce all the right-hand inputs to the common type and
			 * build an ArrayExpr for them.
			 */
			List	   *aexprs;
			ArrayExpr  *newa;

			aexprs = NIL;
			foreach(l, rexprs)
			{
				Node	   *rexpr = (Node *) lfirst(l);

				rexpr = coerce_to_common_type(pstate, rexpr,
											  scalar_type,
											  "IN");
				aexprs = lappend(aexprs, rexpr);
			}
			newa = makeNode(ArrayExpr);
			newa->array_typeid = array_type;
			newa->element_typeid = scalar_type;
			newa->elements = aexprs;
			newa->multidims = false;

			return (Node *) make_scalar_array_op(pstate,
												 a->name,
												 useOr,
												 lexpr,
												 (Node *) newa,
												 a->location);
		}
	}

	/*
	 * Must do it the hard way, ie, with a boolean expression tree.
	 */
	result = NULL;
	foreach(l, rexprs)
	{
		Node	   *rexpr = (Node *) lfirst(l);
		Node	   *cmp;

		if (haveRowExpr)
		{
			if (!IsA(lexpr, RowExpr) ||
				!IsA(rexpr, RowExpr))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				   errmsg("arguments of row IN must all be row expressions"),
						 parser_errposition(pstate, a->location)));
			cmp = make_row_comparison_op(pstate,
										 a->name,
							  (List *) copyObject(((RowExpr *) lexpr)->args),
										 ((RowExpr *) rexpr)->args,
										 a->location);
		}
		else
			cmp = (Node *) make_op(pstate,
								   a->name,
								   copyObject(lexpr),
								   rexpr,
								   a->location);

		cmp = coerce_to_boolean(pstate, cmp, "IN");
		if (result == NULL)
			result = cmp;
		else
			result = (Node *) makeBoolExpr(useOr ? OR_EXPR : AND_EXPR,
										   list_make2(result, cmp), a->location);
	}

	return result;
}

static Node *
transformFuncCall(ParseState *pstate, FuncCall *fn)
{
	List	   *targs;
	ListCell   *args;

	/*
	 * Transform the list of arguments.  We use a shallow list copy and then
	 * transform-in-place to avoid O(N^2) behavior from repeated lappend's.
	 *
	 * XXX: repeated lappend() would no longer result in O(n^2) behavior;
	 * worth reconsidering this design?
	 */
	targs = list_copy(fn->args);
	foreach(args, targs)
	{
		lfirst(args) = transformExpr(pstate,
									 (Node *) lfirst(args));
	}

	/* CDB: Drop a breadcrumb in case of error. */
	pstate->p_breadcrumb.node = (Node *)fn;
    
	return ParseFuncOrColumn(pstate,
							 fn->funcname,
							 targs,
                             fn->agg_order,
							 fn->agg_star,
							 fn->agg_distinct,
							 false,
							 (WindowSpec *)fn->over,
							 fn->location, 
							 fn->agg_filter);
}

/*
 * Check if this is CASE x WHEN IS NOT DISTINCT FROM y:
 * From the raw grammar output, we produce AEXPR_NOT expression
 * 		which has the rhs = AEXPR_DISTINCT expression which has its lhs = NULL
 */
static bool
isWhenIsNotDistinctFromExpr(Node *warg)
{
	if (IsA(warg, A_Expr))
	{
		A_Expr *top = (A_Expr *) warg;
		if (top->kind == AEXPR_NOT && IsA(top->rexpr, A_Expr))
		{
			A_Expr *expr = (A_Expr *) top->rexpr;
			if (expr->kind == AEXPR_DISTINCT && expr->lexpr == NULL)
				return true;
		}
	}
	return false;
}

Node *
transformCaseExpr(ParseState *pstate, CaseExpr *c)
{
	CaseExpr   *newc;
	Node	   *arg;
	CaseTestExpr *placeholder;
	List	   *newargs;
	List	   *typeids;
	ListCell   *l;
	Node	   *defresult;
	Oid			ptype;

	/* If we already transformed this node, do nothing */
	if (OidIsValid(c->casetype))
		return (Node *) c;

	newc = makeNode(CaseExpr);

	/* transform the test expression, if any */
	arg = transformExpr(pstate, (Node *) c->arg);

	/* generate placeholder for test expression */
	if (arg)
	{
		/*
		 * If test expression is an untyped literal, force it to text. We have
		 * to do something now because we won't be able to do this coercion on
		 * the placeholder.  This is not as flexible as what was done in 7.4
		 * and before, but it's good enough to handle the sort of silly coding
		 * commonly seen.
		 */
		if (exprType(arg) == UNKNOWNOID)
			arg = coerce_to_common_type(pstate, arg, TEXTOID, "CASE");

		placeholder = makeNode(CaseTestExpr);
		placeholder->typeId = exprType(arg);
		placeholder->typeMod = exprTypmod(arg);
	}
	else
		placeholder = NULL;

	newc->arg = (Expr *) arg;

	/* transform the list of arguments */
	newargs = NIL;
	typeids = NIL;
	foreach(l, c->args)
	{
		CaseWhen   *w = (CaseWhen *) lfirst(l);
		CaseWhen   *neww = makeNode(CaseWhen);
		Node	   *warg;

		Assert(IsA(w, CaseWhen));

		warg = (Node *) w->expr;
		if (placeholder)
		{
			/* 
			 * CASE placeholder WHEN IS NOT DISTINCT FROM warg:
			 * 		set: warg->rhs->lhs = placeholder
			 */
			if (isWhenIsNotDistinctFromExpr(warg))
			{
                         /*
 			 * Make a copy before we change warg.
 			 * In transformation we don't want to change source (CaseExpr* Node).
 			 * Always create new node and do the transformation
 			 */
 		              	warg = copyObject(warg);    
				A_Expr *top  = (A_Expr *) warg;
				A_Expr *expr = (A_Expr *) top->rexpr;
				expr->lexpr = (Node *) placeholder;
			}
			else
				warg = (Node *) makeSimpleA_Expr(AEXPR_OP, "=",
													(Node *) placeholder,
													 warg,
													 -1);
		}
		else
		{
			if (isWhenIsNotDistinctFromExpr(warg))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("syntax error at or near \"NOT\""),
						 errhint("Missing <operand> for \"CASE <operand> WHEN IS NOT DISTINCT FROM ...\"")));
		}
		neww->expr = (Expr *) transformExpr(pstate, warg);

		neww->expr = (Expr *) coerce_to_boolean(pstate,
												(Node *) neww->expr,
												"CASE/WHEN");

		warg = (Node *) w->result;
		neww->result = (Expr *) transformExpr(pstate, warg);

		newargs = lappend(newargs, neww);
		typeids = lappend_oid(typeids, exprType((Node *) neww->result));
	}

	newc->args = newargs;

	/* transform the default clause */
	defresult = (Node *) c->defresult;
	if (defresult == NULL)
	{
		A_Const    *n = makeNode(A_Const);

		n->val.type = T_Null;
		defresult = (Node *) n;
	}
	newc->defresult = (Expr *) transformExpr(pstate, defresult);

	/*
	 * Note: default result is considered the most significant type in
	 * determining preferred type. This is how the code worked before, but it
	 * seems a little bogus to me --- tgl
	 */
	typeids = lcons_oid(exprType((Node *) newc->defresult), typeids);

	/* CDB: Drop a breadcrumb in case of error. */
	pstate->p_breadcrumb.node = (Node *)c;

	ptype = select_common_type(typeids, "CASE");
	Assert(OidIsValid(ptype));
	newc->casetype = ptype;

	/* Convert default result clause, if necessary */
	newc->defresult = (Expr *)
		coerce_to_common_type(pstate,
							  (Node *) newc->defresult,
							  ptype,
							  "CASE/ELSE");

	/* Convert when-clause results, if necessary */
	foreach(l, newc->args)
	{
		CaseWhen   *w = (CaseWhen *) lfirst(l);

		w->result = (Expr *)
			coerce_to_common_type(pstate,
								  (Node *) w->result,
								  ptype,
								  "CASE/WHEN");
	}

	return (Node *) newc;
}

static Node *
transformSubLink(ParseState *pstate, SubLink *sublink)
{
	List	   *qtrees;
	Query	   *qtree;
	Node	   *result = (Node *) sublink;

	/* If we already transformed this node, do nothing */
	if (IsA(sublink->subselect, Query))
		return result;

	pstate->p_hasSubLinks = true;
	qtrees = parse_sub_analyze(sublink->subselect, pstate);

	/*
	 * Check that we got something reasonable.	Many of these conditions are
	 * impossible given restrictions of the grammar, but check 'em anyway.
	 */
	Insist(list_length(qtrees) == 1);
	qtree = (Query *) linitial(qtrees);
	if (!IsA(qtree, Query) ||
		qtree->commandType != CMD_SELECT ||
		qtree->utilityStmt != NULL)
		elog(ERROR, "unexpected non-SELECT command in SubLink");
	if (qtree->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("subquery cannot have SELECT INTO"),
				 errOmitLocation(true)));

	sublink->subselect = (Node *) qtree;

	if (sublink->subLinkType == EXISTS_SUBLINK)
	{
		/*
		 * EXISTS needs no test expression or combining operator. These fields
		 * should be null already, but make sure.
		 */
		sublink->testexpr = NULL;
		sublink->operName = NIL;
	}
	else if (sublink->subLinkType == EXPR_SUBLINK ||
			 sublink->subLinkType == ARRAY_SUBLINK)
	{
		ListCell   *tlist_item = list_head(qtree->targetList);

		/*
		 * Make sure the subselect delivers a single column (ignoring resjunk
		 * targets).
		 */
		if (tlist_item == NULL ||
			((TargetEntry *) lfirst(tlist_item))->resjunk)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("subquery must return a column")));
		while ((tlist_item = lnext(tlist_item)) != NULL)
		{
			if (!((TargetEntry *) lfirst(tlist_item))->resjunk)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("subquery must return only one column")));
		}

		/*
		 * EXPR and ARRAY need no test expression or combining operator. These
		 * fields should be null already, but make sure.
		 */
		sublink->testexpr = NULL;
		sublink->operName = NIL;
	}
	else
	{
		/* ALL, ANY, or ROWCOMPARE: generate row-comparing expression */
		Node	   *lefthand;
		List	   *left_list;
		List	   *right_list;
		ListCell   *l;

		/*
		 * Transform lefthand expression, and convert to a list
		 */
		lefthand = transformExpr(pstate, sublink->testexpr);
		if (lefthand && IsA(lefthand, RowExpr))
			left_list = ((RowExpr *) lefthand)->args;
		else
			left_list = list_make1(lefthand);

		/*
		 * Build a list of PARAM_SUBLINK nodes representing the output columns
		 * of the subquery.
		 */
		right_list = NIL;
		foreach(l, qtree->targetList)
		{
			TargetEntry *tent = (TargetEntry *) lfirst(l);
			Param	   *param;

			if (tent->resjunk)
				continue;

			param = makeNode(Param);
			param->paramkind = PARAM_SUBLINK;
			param->paramid = tent->resno;
			param->paramtype = exprType((Node *) tent->expr);

			right_list = lappend(right_list, param);
		}

		/* CDB: Drop a breadcrumb in case of error. */
		pstate->p_breadcrumb.node = (Node *)sublink;

		/*
		 * We could rely on make_row_comparison_op to complain if the list
		 * lengths differ, but we prefer to generate a more specific error
		 * message.
		 */
		if (list_length(left_list) < list_length(right_list))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("subquery has too many columns")));
		if (list_length(left_list) > list_length(right_list))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("subquery has too few columns")));

		/*
		 * Identify the combining operator(s) and generate a suitable
		 * row-comparison expression.
		 */
		sublink->testexpr = make_row_comparison_op(pstate,
												   sublink->operName,
												   left_list,
												   right_list,
												   -1);
	}

	return result;
}

static Node *
transformArrayExpr(ParseState *pstate, ArrayExpr *a)
{
	ArrayExpr  *newa = makeNode(ArrayExpr);
	List	   *newelems = NIL;
	List	   *newcoercedelems = NIL;
	List	   *typeids = NIL;
	ListCell   *element;
	Oid			array_type;
	Oid			element_type;

	/* Transform the element expressions */
	foreach(element, a->elements)
	{
		Node	   *e = (Node *) lfirst(element);
		Node	   *newe;

		newe = transformExpr(pstate, e);
		newelems = lappend(newelems, newe);
		typeids = lappend_oid(typeids, exprType(newe));
	}

	/* CDB: Drop a breadcrumb in case of error. */
	pstate->p_breadcrumb.node = (Node *)a;

	/* Select a common type for the elements */
	element_type = select_common_type(typeids, "ARRAY");

	/* Coerce arguments to common type if necessary */
	foreach(element, newelems)
	{
		Node	   *e = (Node *) lfirst(element);
		Node	   *newe;

		newe = coerce_to_common_type(pstate, e,
									 element_type,
									 "ARRAY");
		newcoercedelems = lappend(newcoercedelems, newe);
	}

	/* Do we have an array type to use? */
	array_type = get_array_type(element_type);
	if (array_type != InvalidOid)
	{
		/* Elements are presumably of scalar type */
		newa->multidims = false;
	}
	else
	{
		/* Must be nested array expressions */
		newa->multidims = true;

		array_type = element_type;
		element_type = get_element_type(array_type);
		if (!OidIsValid(element_type))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("could not find array type for data type %s",
							format_type_be(array_type))));
	}

	newa->array_typeid = array_type;
	newa->element_typeid = element_type;
	newa->elements = newcoercedelems;

	return (Node *) newa;
}

static Node *
transformRowExpr(ParseState *pstate, RowExpr *r)
{
	RowExpr    *newr = makeNode(RowExpr);

	/* Transform the field expressions */
	newr->args = transformExpressionList(pstate, r->args);

	/* Barring later casting, we consider the type RECORD */
	newr->row_typeid = RECORDOID;
	newr->row_format = COERCE_IMPLICIT_CAST;

	return (Node *) newr;
}

static Node *
transformTableValueExpr(ParseState *pstate, TableValueExpr *t)
{
	List		*parsetrees;
	Query		*query;

	/* If we already transformed this node, do nothing */
	if (IsA(t->subquery, Query))
		return (Node*) t;

	/* 
	 * Table Value Expressions are subselects that can occur as parameters to
	 * functions.  One result of this is that this code shares a lot with
	 * transformRangeSubselect due to the nature of subquery resolution.
	 */
	pstate->p_hasTblValueExpr = true;

	/* Analyze and transform the subquery */
	parsetrees = parse_sub_analyze(t->subquery, pstate);

	/* 
	 * Check that we got something reasonable.  Most of these conditions
	 * are probably impossible given restrictions in the grammar.
	 */
	Insist(list_length(parsetrees) == 1);
	query = (Query *) linitial(parsetrees);
	if (query == NULL || !IsA(query, Query))
		elog(ERROR, "unexpected non-SELECT command in TableValueExpr");
	if (query->commandType != CMD_SELECT)
		elog(ERROR, "unexpected non-SELECT command in TableValueExpr");
	if (query->intoClause != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("subquery in TABLE value expression cannot have SELECT INTO"),
				 errOmitLocation(true),
				 parser_errposition(pstate, t->location)));
	t->subquery = (Node*) query;

	/*
	 * Insist that the TABLE value expression does not contain references to the outer
	 * range table, this would be an unsupported correlated TABLE value expression.
	 */
	if (contain_vars_of_level_or_above((Node *) query, 1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("subquery in TABLE value expression may not refer "
						"to relation of another query level"),
				 errOmitLocation(true),
				 parser_errposition(pstate, t->location)));

	return (Node*) t;
}

static Node *
transformCoalesceExpr(ParseState *pstate, CoalesceExpr *c)
{
	CoalesceExpr *newc = makeNode(CoalesceExpr);
	List	   *newargs = NIL;
	List	   *newcoercedargs = NIL;
	List	   *typeids = NIL;
	ListCell   *args;

	foreach(args, c->args)
	{
		Node	   *e = (Node *) lfirst(args);
		Node	   *newe;

		newe = transformExpr(pstate, e);
		newargs = lappend(newargs, newe);
		typeids = lappend_oid(typeids, exprType(newe));
	}

	newc->coalescetype = select_common_type(typeids, "COALESCE");

	/* Convert arguments if necessary */
	foreach(args, newargs)
	{
		Node	   *e = (Node *) lfirst(args);
		Node	   *newe;

		newe = coerce_to_common_type(pstate, e,
									 newc->coalescetype,
									 "COALESCE");
		newcoercedargs = lappend(newcoercedargs, newe);
	}

	newc->args = newcoercedargs;
	return (Node *) newc;
}

static Node *
transformMinMaxExpr(ParseState *pstate, MinMaxExpr *m)
{
	MinMaxExpr *newm = makeNode(MinMaxExpr);
	List	   *newargs = NIL;
	List	   *newcoercedargs = NIL;
	List	   *typeids = NIL;
	ListCell   *args;

	newm->op = m->op;
	foreach(args, m->args)
	{
		Node	   *e = (Node *) lfirst(args);
		Node	   *newe;

		newe = transformExpr(pstate, e);
		newargs = lappend(newargs, newe);
		typeids = lappend_oid(typeids, exprType(newe));
	}

	newm->minmaxtype = select_common_type(typeids, "GREATEST/LEAST");

	/* Convert arguments if necessary */
	foreach(args, newargs)
	{
		Node	   *e = (Node *) lfirst(args);
		Node	   *newe;

		newe = coerce_to_common_type(pstate, e,
									 newm->minmaxtype,
									 "GREATEST/LEAST");
		newcoercedargs = lappend(newcoercedargs, newe);
	}

	newm->args = newcoercedargs;
	return (Node *) newm;
}

static Node *
transformBooleanTest(ParseState *pstate, BooleanTest *b)
{
	const char *clausename;

	switch (b->booltesttype)
	{
		case IS_TRUE:
			clausename = "IS TRUE";
			break;
		case IS_NOT_TRUE:
			clausename = "IS NOT TRUE";
			break;
		case IS_FALSE:
			clausename = "IS FALSE";
			break;
		case IS_NOT_FALSE:
			clausename = "IS NOT FALSE";
			break;
		case IS_UNKNOWN:
			clausename = "IS UNKNOWN";
			break;
		case IS_NOT_UNKNOWN:
			clausename = "IS NOT UNKNOWN";
			break;
		default:
			elog(ERROR, "unrecognized booltesttype: %d",
				 (int) b->booltesttype);
			clausename = NULL;	/* keep compiler quiet */
	}

	b->arg = (Expr *) transformExpr(pstate, (Node *) b->arg);

	b->arg = (Expr *) coerce_to_boolean(pstate,
										(Node *) b->arg,
										clausename);

	return (Node *) b;
}

/*
 * Construct a whole-row reference to represent the notation "relation.*".
 *
 * A whole-row reference is a Var with varno set to the correct range
 * table entry, and varattno == 0 to signal that it references the whole
 * tuple.  (Use of zero here is unclean, since it could easily be confused
 * with error cases, but it's not worth changing now.)  The vartype indicates
 * a rowtype; either a named composite type, or RECORD.
 */
static Node *
transformWholeRowRef(ParseState *pstate, char *catalogname, char *schemaname, char *relname,
					 int location)
{
	Node	   *result;
	RangeTblEntry *rte;
	int			vnum;
	int			sublevels_up;
	Oid			toid;

	/* Look up the referenced RTE, creating it if needed */

	rte = refnameRangeTblEntry(pstate, catalogname, schemaname, relname, location,
							   &sublevels_up);

	if (rte == NULL)
		rte = addImplicitRTE(pstate, makeRangeVar(catalogname, schemaname, relname, location), location);

	vnum = RTERangeTablePosn(pstate, rte, &sublevels_up);

	/* Build the appropriate referencing node */

	switch (rte->rtekind)
	{
		case RTE_RELATION:
			/* relation: the rowtype is a named composite type */
			toid = get_rel_type_id(rte->relid);
			if (!OidIsValid(toid))
				elog(ERROR, "could not find type OID for relation %u",
					 rte->relid);
			result = (Node *) makeVar(vnum,
									  InvalidAttrNumber,
									  toid,
									  -1,
									  sublevels_up);
			break;
		case RTE_TABLEFUNCTION:
		case RTE_FUNCTION:
			toid = exprType(rte->funcexpr);
			if (toid == RECORDOID || get_typtype(toid) == 'c')
			{
				/* func returns composite; same as relation case */
				result = (Node *) makeVar(vnum,
										  InvalidAttrNumber,
										  toid,
										  -1,
										  sublevels_up);
			}
			else
			{
				/*
				 * func returns scalar; instead of making a whole-row Var,
				 * just reference the function's scalar output.  (XXX this
				 * seems a tad inconsistent, especially if "f.*" was
				 * explicitly written ...)
				 */
				result = (Node *) makeVar(vnum,
										  1,
										  toid,
										  -1,
										  sublevels_up);
			}
			break;
		case RTE_VALUES:
			toid = RECORDOID;
			/* returns composite; same as relation case */
			result = (Node *) makeVar(vnum,
									  InvalidAttrNumber,
									  toid,
									  -1,
									  sublevels_up);
			break;
		default:

			/*
			 * RTE is a join or subselect.	We represent this as a whole-row
			 * Var of RECORD type.	(Note that in most cases the Var will be
			 * expanded to a RowExpr during planning, but that is not our
			 * concern here.)
			 */
			result = (Node *) makeVar(vnum,
									  InvalidAttrNumber,
									  RECORDOID,
									  -1,
									  sublevels_up);
			break;
	}

	return result;
}

static Node *
transformGroupingFunc(ParseState *pstate, GroupingFunc *gf)
{
	List *targs = NIL;
	ListCell *lc;
	GroupingFunc *new_gf;

	new_gf = makeNode(GroupingFunc);

	/*
	 * Transform the list of arguments.
	 */
	foreach (lc, gf->args)
		targs = lappend(targs, transformExpr(pstate, (Node *)lfirst(lc)));

	new_gf->args = targs;

	new_gf->ngrpcols = gf->ngrpcols;

	return (Node *)new_gf;
}

/*
 * Returns string which represents percentile signature.
 * sorttypes and sortlen can be omitted if they are unknown.
 */
static char *
percentileFuncString(PercentileExpr *p, Oid *argtypes, int arglen,
					 Oid *sorttypes, int sortlen)
{
	StringInfoData	buf;
	char		   *funcname = NULL;
	int				i;

	if (p->perckind == PERC_MEDIAN)
		funcname = "median";
	else if (p->perckind == PERC_CONT)
		funcname = "percentile_cont";
	else if (p->perckind == PERC_DISC)
		funcname = "percentile_disc";
	else
		elog(ERROR, "unexpected perckind: %d", (int) p->perckind);

	initStringInfo(&buf);
	if (p->perckind == PERC_MEDIAN)
		appendStringInfo(&buf, "%s",
				func_signature_string(list_make1(makeString(funcname)),
				sortlen, sorttypes));
	else
	{
		appendStringInfo(&buf, "%s",
				func_signature_string(list_make1(makeString(funcname)),
				arglen, argtypes));
		if (sortlen > 0)
		{
			appendStringInfo(&buf, " ORDER BY (");
			for (i = 0 ; i < sortlen; i++)
			{
				appendStringInfo(&buf, "%s", format_type_be(sorttypes[i]));
				if (i != sortlen - 1)
					appendStringInfo(&buf, ", ");
			}
			appendStringInfo(&buf, ")");
		}
	}

	return pstrdup(buf.data);
}

/*
 * transformPercentileExpr
 *
 * Its argument expression and ORDER BY clause are transformed.
 * Various syntax checks are also done here.
 */
static Node *
transformPercentileExpr(ParseState *pstate, PercentileExpr *p)
{
	Node	   *arg;
	Oid			argtype;
	AttrNumber	save_next_resno;

	/* args should be one for the moment, but can be extended later. */
	Assert(list_length(p->args) == 1);

	arg = transformExpr(pstate, linitial(p->args));
	argtype = exprType(arg);
	if (!IsBinaryCoercible(argtype, FLOAT8OID))
	{
		arg = coerce_to_target_type(pstate, arg, argtype,
									FLOAT8OID, -1, COERCION_EXPLICIT,
									COERCE_EXPLICIT_CALL, p->location);

		/*
		 * The argument cannot be cast to the desired type.
		 */
		if (!arg)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("function \"%s\" does not exist",
						 percentileFuncString(p, &argtype, 1, NULL, 0)),
					 errhint("No function matches the given name and argument types. "
							 "You may need to add explicit type casts.")));
	}
	argtype = FLOAT8OID;

	/*
	 * The argument check is almost same as LIMIT clause.
	 */
	if (contain_vars_of_level(arg, 0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("argument of percentile function must not contain variables")));
	if (checkExprHasAggs(arg))
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg("argument of percentile function must not contain aggregates")));
	if (checkExprHasWindFuncs(arg))
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg("argument of percentile function must not contain window functions")));
	if (checkExprHasGroupExtFuncs(arg))
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg("argument of percentile function must not contain grouping(), or group_id()")));
	/*
	 * The argument should be stable within a group.  We don't know what is the
	 * right behavior for the volatile argument.  Simply erroring out for now.
	 */
	if (contain_volatile_functions(arg))
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg("argument of percentile function must not contain volatile functions")));
	/*
	 * It might be possible to support SubLink in the argument, but the limitation
	 * here is as LIMIT clause.  Erroring out for now.
	 */
	if (checkExprHasSubLink(arg))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("argument of percentile function must not contain subqueries")));
	/*
	 * Percentile functions support only one sort key.
	 */
	if (list_length(p->sortClause) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("function \"%s\" cannot accept more than one expression in ORDER BY",
					 percentileFuncString(p, &argtype, 1, NULL, 0))));


	p->args = list_make1(arg);

	/*
	 * sortClause should not be transformed twice, and !sortTargets is
	 * not a good test to know if it's done, but it's ok for now.
	 */
	if (!p->sortTargets)
	{
		List		   *tlist = NIL, *sortlist = NIL, *pre_tlist = NIL;
		ListCell	   *l;
		int				i;
		int				min_varlevel;
		int				sortlen;
		Oid			   *sorttypes;
		struct _FuncCandidateList	float8_sig,
									timestamp_sig,
									timestamptz_sig,
									interval_sig,
									*candidates;
		int		ncandidates;

		/*
		 * This part is similar to transformSortClause(), but there is a reason
		 * we are not able to use it;  each expression should be coerced to
		 * the type this function desires, as the argument expressions are treated.
		 * Also, another task is function resolution by input types.
		 * As we consider ORDER BY clause as arguments, the resolution process
		 * is different from normal func_get_detail().  Though, some parts
		 * can be shared without reinventing.
		 *
		 * Save resno and start from 1 for only this part of transformation.
		 */
		save_next_resno = pstate->p_next_resno;
		pstate->p_next_resno = 1;

		sortlen = list_length(p->sortClause);
		sorttypes = (Oid *) palloc0(sizeof(Oid) * sortlen);
		foreach_with_count (l, p->sortClause, i)
		{
			SortBy		   *sortby = lfirst(l);
			Node		   *expr;
			TargetEntry	   *tle;
			Oid				sorttype;

			expr = transformExpr(pstate, sortby->node);
			tle = transformTargetEntry(pstate, sortby->node, expr, NULL, true);

			sorttype = exprType((Node *) tle->expr);
			sorttypes[i] = sorttype;
			pre_tlist = lappend(pre_tlist, tle);
		}

		/*
		 * The logic for function resolution is same as the normal case except
		 * that we look for candidates by ORDER BY clause.  Because PercentileExpr
		 * supports limited number of data types, we write possible types here manually.
		 * The code should look similar to func_get_detail().
		 */
		float8_sig.nargs = 1;
		float8_sig.args[0] = FLOAT8OID;
		timestamp_sig.nargs = 1;
		timestamp_sig.args[0] = TIMESTAMPOID;
		timestamptz_sig.nargs = 1;
		timestamptz_sig.args[0] = TIMESTAMPTZOID;
		interval_sig.nargs = 1;
		interval_sig.args[0] = INTERVALOID;
		float8_sig.next = &timestamp_sig;
		timestamp_sig.next = &timestamptz_sig;
		timestamptz_sig.next = &interval_sig;
		interval_sig.next = NULL;

		ncandidates = func_match_argtypes(sortlen, sorttypes, &float8_sig, &candidates);

		/*
		 * Multiple candidates? then better decide or throw an error...
		 */
		if (ncandidates > 1)
		{
			candidates = func_select_candidate(sortlen, sorttypes, candidates);
			if (!candidates)
				ereport(ERROR,
						(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
						 errmsg("function \"%s\" is not unique",
							percentileFuncString(p, &argtype, 1, sorttypes, sortlen)),
						 errhint("Could not choose a best candidate function. "
								 "You may need to add explicit type casts.")));
		}
		else if (ncandidates == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("function \"%s\" does not exist",
						percentileFuncString(p, &argtype, 1, sorttypes, sortlen)),
					 errhint("No function matches the given name and argument types. "
							 "You may need to add explicit type casts.")));
		}

		p->perctype = candidates->args[0];

		/*
		 * Coerce each of ORDER BY clause to the types which this function desires.
		 */
		foreach_with_count (l, pre_tlist, i)
		{
			TargetEntry	   *tle = lfirst(l);
			Oid				sorttype = sorttypes[i];
			Oid				candtype = candidates->args[i];
			SortBy		   *sortby = list_nth(p->sortClause, i);

			if (!IsBinaryCoercible(sorttype, candtype))
			{
				tle->expr = (Expr *) coerce_to_target_type(pstate,
														   (Node *) tle->expr,
														   sorttype,
														   candtype,
														   -1,
														   COERCION_EXPLICIT,
														   COERCE_EXPLICIT_CALL,
														   p->location);
				/*
				 * Cast should succeed as we checked above.
				 */
				Assert(tle->expr);
			}

			tlist = lappend(tlist, tle);
			/*
			 * Now we can add this target entry to the list.
			 */
			sortlist = addTargetToSortList(pstate, tle,
										   sortlist, tlist,
										   sortby->sortby_kind,
										   sortby->useOp,
										   true);

		}
		p->sortClause = sortlist;
		p->sortTargets = tlist;
		pfree(sorttypes);
		/*
		 * Restore back the saved resno.
		 */
		pstate->p_next_resno = save_next_resno;

		/*
		 * Although its argument is not allowed to refer to any Var,
		 * ORDER BY is allowed.  Since PercentileExpr is semantically Aggref,
		 * when it refers to outer vars, the whole expression should be
		 * treated as an outer var.  However, it is too invasive to do it
		 * for PercentileExpr, and as we plan to integrate it Aggref, we
		 * just disallow such cases for now.  Revisit later.
		 */
		min_varlevel = find_minimum_var_level((Node *) p->sortTargets);
		if (min_varlevel > 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("percentile functions cannot reference columns from outer queries")));

		if (checkExprHasAggs((Node *) p->sortTargets))
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
					 errmsg("argument of percentile function must not contain aggregates")));
		if (checkExprHasWindFuncs((Node *) p->sortTargets))
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
					 errmsg("argument of percentile function must not contain window functions")));
		if (checkExprHasGroupExtFuncs((Node *) p->sortTargets))
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
					 errmsg("argument of percentile function must not contain grouping(), or group_id()")));
	}

	/* Percentiles are actually aggregates. */
	pstate->p_hasAggs = true;

	return (Node *) p;
}

/*
 *	exprType -
 *	  returns the Oid of the type of the expression. (Used for typechecking.)
 */
Oid
exprType(Node *expr)
{
	Oid			type;

	if (!expr)
		return InvalidOid;

	switch (nodeTag(expr))
	{
		case T_Var:
			type = ((Var *) expr)->vartype;
			break;
		case T_Const:
			type = ((Const *) expr)->consttype;
			break;
		case T_Param:
			type = ((Param *) expr)->paramtype;
			break;
		case T_Aggref:
			type = ((Aggref *) expr)->aggtype;
			break;
		case T_WindowRef:
			type = ((WindowRef *) expr)->restype;
			break;
		case T_ArrayRef:
			type = ((ArrayRef *) expr)->refrestype;
			break;
		case T_FuncExpr:
			type = ((FuncExpr *) expr)->funcresulttype;
			break;
		case T_OpExpr:
			type = ((OpExpr *) expr)->opresulttype;
			break;
		case T_DistinctExpr:
			type = ((DistinctExpr *) expr)->opresulttype;
			break;
		case T_ScalarArrayOpExpr:
			type = BOOLOID;
			break;
		case T_BoolExpr:
			type = BOOLOID;
			break;
		case T_SubLink:
			{
				SubLink    *sublink = (SubLink *) expr;

				if (sublink->subLinkType == EXPR_SUBLINK ||
					sublink->subLinkType == ARRAY_SUBLINK)
				{
					/* get the type of the subselect's first target column */
					Query	   *qtree = (Query *) sublink->subselect;
					TargetEntry *tent;

					if (!qtree || !IsA(qtree, Query))
						elog(ERROR, "cannot get type for untransformed sublink");
					tent = (TargetEntry *) linitial(qtree->targetList);
					Assert(IsA(tent, TargetEntry));
					Assert(!tent->resjunk);
					type = exprType((Node *) tent->expr);
					if (sublink->subLinkType == ARRAY_SUBLINK)
					{
						type = get_array_type(type);
						if (!OidIsValid(type))
							ereport(ERROR,
									(errcode(ERRCODE_UNDEFINED_OBJECT),
									 errmsg("could not find array type for data type %s",
							format_type_be(exprType((Node *) tent->expr)))));
					}
				}
				else
				{
					/* for all other sublink types, result is boolean */
					type = BOOLOID;
				}
			}
			break;
		case T_SubPlan:
			{
				/*
				 * Although the parser does not ever deal with already-planned
				 * expression trees, we support SubPlan nodes in this routine
				 * for the convenience of ruleutils.c.
				 */
				SubPlan    *subplan = (SubPlan *) expr;

				if (subplan->subLinkType == EXPR_SUBLINK ||
					subplan->subLinkType == ARRAY_SUBLINK)
				{
					/* get the type of the subselect's first target column */
					Oid itemtype = subplan->firstColType;
					type = itemtype;
					if (subplan->subLinkType == ARRAY_SUBLINK)
					{
						type = get_array_type(itemtype);
						if (!OidIsValid(type))
							ereport(ERROR,
									(errcode(ERRCODE_UNDEFINED_OBJECT),
									 errmsg("could not find array type for data type %s",
							format_type_be(itemtype))));
					}
				}
				else
				{
					/* for all other subplan types, result is boolean */
					type = BOOLOID;
				}
			}
			break;
		case T_FieldSelect:
			type = ((FieldSelect *) expr)->resulttype;
			break;
		case T_FieldStore:
			type = ((FieldStore *) expr)->resulttype;
			break;
		case T_RelabelType:
			type = ((RelabelType *) expr)->resulttype;
			break;
		case T_ConvertRowtypeExpr:
			type = ((ConvertRowtypeExpr *) expr)->resulttype;
			break;
		case T_CaseExpr:
			type = ((CaseExpr *) expr)->casetype;
			break;
		case T_CaseWhen:
			type = exprType((Node *) ((CaseWhen *) expr)->result);
			break;
		case T_CaseTestExpr:
			type = ((CaseTestExpr *) expr)->typeId;
			break;
		case T_ArrayExpr:
			type = ((ArrayExpr *) expr)->array_typeid;
			break;
		case T_RowExpr:
			type = ((RowExpr *) expr)->row_typeid;
			break;
		case T_TableValueExpr:
			type = ANYTABLEOID;  /* MULTISET values are a special pseudotype */
			break;
		case T_RowCompareExpr:
			type = BOOLOID;
			break;
		case T_CoalesceExpr:
			type = ((CoalesceExpr *) expr)->coalescetype;
			break;
		case T_MinMaxExpr:
			type = ((MinMaxExpr *) expr)->minmaxtype;
			break;
		case T_NullIfExpr:
			type = exprType((Node *) linitial(((NullIfExpr *) expr)->args));
			break;
		case T_NullTest:
			type = BOOLOID;
			break;
		case T_BooleanTest:
			type = BOOLOID;
			break;
		case T_CoerceToDomain:
			type = ((CoerceToDomain *) expr)->resulttype;
			break;
		case T_CoerceToDomainValue:
			type = ((CoerceToDomainValue *) expr)->typeId;
			break;
		case T_SetToDefault:
			type = ((SetToDefault *) expr)->typeId;
			break;
		case T_CurrentOfExpr:
			type = BOOLOID;
			break;
		case T_GroupingFunc:
			type = INT8OID;
			break;
		case T_Grouping:
			type = INT8OID;
			break;
		case T_GroupId:
			type = INT4OID;
			break;
		case T_PercentileExpr:
			type = ((PercentileExpr *) expr)->perctype;
			break;
		case T_DMLActionExpr:
			type = INT4OID;
			break;
		case T_PartOidExpr:
			type = OIDOID;
			break;
		case T_PartDefaultExpr:
			type = BOOLOID;
			break;
		case T_PartBoundExpr:
			type = ((PartBoundExpr *) expr)->boundType;
			break;
		case T_PartBoundInclusionExpr:
			type = BOOLOID;
			break;
		case T_PartBoundOpenExpr:
			type = BOOLOID;
			break;
		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
			type = InvalidOid;	/* keep compiler quiet */
			break;
	}
	return type;
}

/*
 *	exprTypmod -
 *	  returns the type-specific attrmod of the expression, if it can be
 *	  determined.  In most cases, it can't and we return -1.
 */
int32
exprTypmod(Node *expr)
{
	if (!expr)
		return -1;

	switch (nodeTag(expr))
	{
		case T_Var:
			return ((Var *) expr)->vartypmod;
		case T_Const:
			{
				/* Be smart about string constants... */
				Const	   *con = (Const *) expr;

				switch (con->consttype)
				{
					case BPCHAROID:
						if (!con->constisnull)
						{
							int32		len = VARSIZE(DatumGetPointer(con->constvalue)) - VARHDRSZ;

							/* if multi-byte, take len and find # characters */
							if (pg_database_encoding_max_length() > 1)
								len = pg_mbstrlen_with_len(VARDATA(DatumGetPointer(con->constvalue)), len);
							return len + VARHDRSZ;
						}
						break;
					default:
						break;
				}
			}
			break;
		case T_FuncExpr:
			{
				int32		coercedTypmod;

				/* Be smart about length-coercion functions... */
				if (exprIsLengthCoercion(expr, &coercedTypmod))
					return coercedTypmod;
			}
			break;
		case T_FieldSelect:
			return ((FieldSelect *) expr)->resulttypmod;
		case T_RelabelType:
			return ((RelabelType *) expr)->resulttypmod;
		case T_CaseExpr:
			{
				/*
				 * If all the alternatives agree on type/typmod, return that
				 * typmod, else use -1
				 */
				CaseExpr   *cexpr = (CaseExpr *) expr;
				Oid			casetype = cexpr->casetype;
				int32		typmod;
				ListCell   *arg;

				if (!cexpr->defresult)
					return -1;
				if (exprType((Node *) cexpr->defresult) != casetype)
					return -1;
				typmod = exprTypmod((Node *) cexpr->defresult);
				if (typmod < 0)
					return -1;	/* no point in trying harder */
				foreach(arg, cexpr->args)
				{
					CaseWhen   *w = (CaseWhen *) lfirst(arg);

					Assert(IsA(w, CaseWhen));
					if (exprType((Node *) w->result) != casetype)
						return -1;
					if (exprTypmod((Node *) w->result) != typmod)
						return -1;
				}
				return typmod;
			}
			break;
		case T_CaseTestExpr:
			return ((CaseTestExpr *) expr)->typeMod;
		case T_CoalesceExpr:
			{
				/*
				 * If all the alternatives agree on type/typmod, return that
				 * typmod, else use -1
				 */
				CoalesceExpr *cexpr = (CoalesceExpr *) expr;
				Oid			coalescetype = cexpr->coalescetype;
				int32		typmod;
				ListCell   *arg;

				if (exprType((Node *) linitial(cexpr->args)) != coalescetype)
					return -1;
				typmod = exprTypmod((Node *) linitial(cexpr->args));
				if (typmod < 0)
					return -1;	/* no point in trying harder */
				for_each_cell(arg, lnext(list_head(cexpr->args)))
				{
					Node	   *e = (Node *) lfirst(arg);

					if (exprType(e) != coalescetype)
						return -1;
					if (exprTypmod(e) != typmod)
						return -1;
				}
				return typmod;
			}
			break;
		case T_MinMaxExpr:
			{
				/*
				 * If all the alternatives agree on type/typmod, return that
				 * typmod, else use -1
				 */
				MinMaxExpr *mexpr = (MinMaxExpr *) expr;
				Oid			minmaxtype = mexpr->minmaxtype;
				int32		typmod;
				ListCell   *arg;

				if (exprType((Node *) linitial(mexpr->args)) != minmaxtype)
					return -1;
				typmod = exprTypmod((Node *) linitial(mexpr->args));
				if (typmod < 0)
					return -1;	/* no point in trying harder */
				for_each_cell(arg, lnext(list_head(mexpr->args)))
				{
					Node	   *e = (Node *) lfirst(arg);

					if (exprType(e) != minmaxtype)
						return -1;
					if (exprTypmod(e) != typmod)
						return -1;
				}
				return typmod;
			}
			break;
		case T_NullIfExpr:
			{
				NullIfExpr *nexpr = (NullIfExpr *) expr;

				return exprTypmod((Node *) linitial(nexpr->args));
			}
			break;
		case T_CoerceToDomain:
			return ((CoerceToDomain *) expr)->resulttypmod;
		case T_CoerceToDomainValue:
			return ((CoerceToDomainValue *) expr)->typeMod;
		case T_SetToDefault:
			return ((SetToDefault *) expr)->typeMod;
		default:
			break;
	}
	return -1;
}

/*
 * exprIsLengthCoercion
 *		Detect whether an expression tree is an application of a datatype's
 *		typmod-coercion function.  Optionally extract the result's typmod.
 *
 * If coercedTypmod is not NULL, the typmod is stored there if the expression
 * is a length-coercion function, else -1 is stored there.
 *
 * Note that a combined type-and-length coercion will be treated as a
 * length coercion by this routine.
 */
bool
exprIsLengthCoercion(Node *expr, int32 *coercedTypmod)
{
	FuncExpr   *func;
	int			nargs;
	Const	   *second_arg;

	if (coercedTypmod != NULL)
		*coercedTypmod = -1;	/* default result on failure */

	/* Is it a function-call at all? */
	if (expr == NULL || !IsA(expr, FuncExpr))
		return false;
	func = (FuncExpr *) expr;

	/*
	 * If it didn't come from a coercion context, reject.
	 */
	if (func->funcformat != COERCE_EXPLICIT_CAST &&
		func->funcformat != COERCE_IMPLICIT_CAST)
		return false;

	/*
	 * If it's not a two-argument or three-argument function with the second
	 * argument being an int4 constant, it can't have been created from a
	 * length coercion (it must be a type coercion, instead).
	 */
	nargs = list_length(func->args);
	if (nargs < 2 || nargs > 3)
		return false;

	second_arg = (Const *) lsecond(func->args);
	if (!IsA(second_arg, Const) ||
		second_arg->consttype != INT4OID ||
		second_arg->constisnull)
		return false;

	/*
	 * OK, it is indeed a length-coercion function.
	 */
	if (coercedTypmod != NULL)
		*coercedTypmod = DatumGetInt32(second_arg->constvalue);

	return true;
}

/*
 * Handle an explicit CAST construct.
 *
 * The given expr has already been transformed, but we need to lookup
 * the type name and then apply any necessary coercion function(s).
 */
static Node *
typecast_expression(ParseState *pstate, Node *expr, TypeName *typname)
{
	Oid			inputType = exprType(expr);
	Oid			targetType;

	targetType = typenameTypeId(pstate, typname);

	if (inputType == InvalidOid)
		return expr;			/* do nothing if NULL input */

	expr = coerce_to_target_type(pstate, expr, inputType,
								 targetType, typname->typmod,
								 COERCION_EXPLICIT,
								 COERCE_EXPLICIT_CAST,
								 -1);
	if (expr == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CANNOT_COERCE),
				 errmsg("cannot cast type %s to %s",
						format_type_be(inputType),
						format_type_be(targetType)),
								errOmitLocation(true),
				 parser_errposition(pstate, typname->location)));

	return expr;
}

/*
 * Transform a "row compare-op row" construct
 *
 * The inputs are lists of already-transformed expressions.
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 *
 * The output may be a single OpExpr, an AND or OR combination of OpExprs,
 * or a RowCompareExpr.  In all cases it is guaranteed to return boolean.
 * The AND, OR, and RowCompareExpr cases further imply things about the
 * behavior of the operators (ie, they behave as =, <>, or < <= > >=).
 */
static Node *
make_row_comparison_op(ParseState *pstate, List *opname,
					   List *largs, List *rargs, int location)
{
	RowCompareExpr *rcexpr;
	RowCompareType rctype;
	List	   *opexprs;
	List	   *opnos;
	List	   *opclasses;
	ListCell   *l,
			   *r;
	List	  **opclass_lists;
	List	  **opstrat_lists;
	Bitmapset  *strats;
	int			nopers;
	int			i;

	nopers = list_length(largs);
	if (nopers != list_length(rargs))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("unequal number of entries in row expressions"),
						 errOmitLocation(true),
				 parser_errposition(pstate, location)));

	/*
	 * We can't compare zero-length rows because there is no principled basis
	 * for figuring out what the operator is.
	 */
	if (nopers == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot compare rows of zero length"),
						 errOmitLocation(true),
				 parser_errposition(pstate, location)));

	/*
	 * Identify all the pairwise operators, using make_op so that behavior is
	 * the same as in the simple scalar case.
	 */
	opexprs = NIL;
	forboth(l, largs, r, rargs)
	{
		Node	   *larg = (Node *) lfirst(l);
		Node	   *rarg = (Node *) lfirst(r);
		OpExpr	   *cmp;

		cmp = (OpExpr *) make_op(pstate, opname, larg, rarg, location);
		Assert(IsA(cmp, OpExpr));

		/*
		 * We don't use coerce_to_boolean here because we insist on the
		 * operator yielding boolean directly, not via coercion.  If it
		 * doesn't yield bool it won't be in any index opclasses...
		 */
		if (cmp->opresulttype != BOOLOID)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
				   errmsg("row comparison operator must yield type boolean, "
						  "not type %s",
						  format_type_be(cmp->opresulttype)),
								  errOmitLocation(true),
					 parser_errposition(pstate, location)));
		if (expression_returns_set((Node *) cmp))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("row comparison operator must not return a set"),
							 errOmitLocation(true),
					 parser_errposition(pstate, location)));
		opexprs = lappend(opexprs, cmp);
	}

	/*
	 * If rows are length 1, just return the single operator.  In this case we
	 * don't insist on identifying btree semantics for the operator (but we
	 * still require it to return boolean).
	 */
	if (nopers == 1)
		return (Node *) linitial(opexprs);

	/*
	 * Now we must determine which row comparison semantics (= <> < <= > >=)
	 * apply to this set of operators.	We look for btree opclasses containing
	 * the operators, and see which interpretations (strategy numbers) exist
	 * for each operator.
	 */
	opclass_lists = (List **) palloc(nopers * sizeof(List *));
	opstrat_lists = (List **) palloc(nopers * sizeof(List *));
	strats = NULL;
	i = 0;
	foreach(l, opexprs)
	{
		Bitmapset  *this_strats;
		ListCell   *j;

		get_op_btree_interpretation(((OpExpr *) lfirst(l))->opno,
									&opclass_lists[i], &opstrat_lists[i]);

		/*
		 * convert strategy number list to a Bitmapset to make the
		 * intersection calculation easy.
		 */
		this_strats = NULL;
		foreach(j, opstrat_lists[i])
		{
			this_strats = bms_add_member(this_strats, lfirst_int(j));
		}
		if (i == 0)
			strats = this_strats;
		else
			strats = bms_int_members(strats, this_strats);
		i++;
	}

	switch (bms_membership(strats))
	{
		case BMS_EMPTY_SET:
			/* No common interpretation, so fail */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("could not determine interpretation of row comparison operator %s",
							strVal(llast(opname))),
					 errhint("Row comparison operators must be associated with btree operator classes."),
							 errOmitLocation(true),
					 parser_errposition(pstate, location)));
			rctype = 0;			/* keep compiler quiet */
			break;
		case BMS_SINGLETON:
			/* Simple case: just one possible interpretation */
			rctype = bms_singleton_member(strats);
			break;
		case BMS_MULTIPLE:
		default:				/* keep compiler quiet */
			{
				/*
				 * Prefer the interpretation with the most default opclasses.
				 */
				int			best_defaults = 0;
				bool		multiple_best = false;
				int			this_rctype;

				rctype = 0;		/* keep compiler quiet */
				while ((this_rctype = bms_first_member(strats)) >= 0)
				{
					int			ndefaults = 0;

					for (i = 0; i < nopers; i++)
					{
						forboth(l, opclass_lists[i], r, opstrat_lists[i])
						{
							Oid			opclass = lfirst_oid(l);
							int			opstrat = lfirst_int(r);

							if (opstrat == this_rctype &&
								opclass_is_default(opclass))
								ndefaults++;
						}
					}
					if (ndefaults > best_defaults)
					{
						best_defaults = ndefaults;
						rctype = this_rctype;
						multiple_best = false;
					}
					else if (ndefaults == best_defaults)
						multiple_best = true;
				}
				if (best_defaults == 0 || multiple_best)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("could not determine interpretation of row comparison operator %s",
									strVal(llast(opname))),
							 errdetail("There are multiple equally-plausible candidates."),
									 errOmitLocation(true),
							 parser_errposition(pstate, location)));
				break;
			}
	}

	/*
	 * For = and <> cases, we just combine the pairwise operators with AND or
	 * OR respectively.
	 *
	 * Note: this is presently the only place where the parser generates
	 * BoolExpr with more than two arguments.  Should be OK since the rest of
	 * the system thinks BoolExpr is N-argument anyway.
	 */
	if (rctype == ROWCOMPARE_EQ)
		return (Node *) makeBoolExpr(AND_EXPR, opexprs, location);
	if (rctype == ROWCOMPARE_NE)
		return (Node *) makeBoolExpr(OR_EXPR, opexprs, location);

	/*
	 * Otherwise we need to determine exactly which opclass to associate with
	 * each operator.
	 */
	opclasses = NIL;
	for (i = 0; i < nopers; i++)
	{
		Oid			best_opclass = 0;
		int			ndefault = 0;
		int			nmatch = 0;

		forboth(l, opclass_lists[i], r, opstrat_lists[i])
		{
			Oid			opclass = lfirst_oid(l);
			int			opstrat = lfirst_int(r);

			if (opstrat == rctype)
			{
				if (ndefault == 0)
					best_opclass = opclass;
				if (opclass_is_default(opclass))
					ndefault++;
				else
					nmatch++;
			}
		}
		if (ndefault == 1 || (ndefault == 0 && nmatch == 1))
			opclasses = lappend_oid(opclasses, best_opclass);
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("could not determine interpretation of row comparison operator %s",
							strVal(llast(opname))),
			   errdetail("There are multiple equally-plausible candidates."),
					   errOmitLocation(true),
					 parser_errposition(pstate, location)));
	}

	/*
	 * Now deconstruct the OpExprs and create a RowCompareExpr.
	 *
	 * Note: can't just reuse the passed largs/rargs lists, because of
	 * possibility that make_op inserted coercion operations.
	 */
	opnos = NIL;
	largs = NIL;
	rargs = NIL;
	foreach(l, opexprs)
	{
		OpExpr	   *cmp = (OpExpr *) lfirst(l);

		opnos = lappend_oid(opnos, cmp->opno);
		largs = lappend(largs, linitial(cmp->args));
		rargs = lappend(rargs, lsecond(cmp->args));
	}

	rcexpr = makeNode(RowCompareExpr);
	rcexpr->rctype = rctype;
	rcexpr->opnos = opnos;
	rcexpr->opclasses = opclasses;
	rcexpr->largs = largs;
	rcexpr->rargs = rargs;

	return (Node *) rcexpr;
}

/*
 * Transform a "row IS DISTINCT FROM row" construct
 *
 * The input RowExprs are already transformed
 */
static Node *
make_row_distinct_op(ParseState *pstate, List *opname,
					 RowExpr *lrow, RowExpr *rrow,
					 int location)
{
	Node	   *result = NULL;
	List	   *largs = lrow->args;
	List	   *rargs = rrow->args;
	ListCell   *l,
			   *r;

	if (list_length(largs) != list_length(rargs))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("unequal number of entries in row expressions"),
						 errOmitLocation(true),
				 parser_errposition(pstate, location)));

	forboth(l, largs, r, rargs)
	{
		Node	   *larg = (Node *) lfirst(l);
		Node	   *rarg = (Node *) lfirst(r);
		Node	   *cmp;

		cmp = (Node *) make_distinct_op(pstate, opname, larg, rarg, location);
		if (result == NULL)
			result = cmp;
		else
			result = (Node *) makeBoolExpr(OR_EXPR,
										   list_make2(result, cmp), location);
	}

	if (result == NULL)
	{
		/* zero-length rows?  Generate constant FALSE */
		result = makeBoolConst(false, false);
	}

	return result;
}

/*
 * make the node for an IS DISTINCT FROM operator
 */
static Expr *
make_distinct_op(ParseState *pstate, List *opname, Node *ltree, Node *rtree,
				 int location)
{
	Expr	   *result;

	result = make_op(pstate, opname, ltree, rtree, location);
	if (((OpExpr *) result)->opresulttype != BOOLOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
			 errmsg("IS DISTINCT FROM requires = operator to yield boolean"),
					 errOmitLocation(true),
				 parser_errposition(pstate, location)));

	/*
	 * We rely on DistinctExpr and OpExpr being same struct
	 */
	NodeSetTag(result, T_DistinctExpr);

	return result;
}
