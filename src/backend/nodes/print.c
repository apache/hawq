/*-------------------------------------------------------------------------
 *
 * print.c
 *	  various print routines (used mostly for debugging)
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/nodes/print.c,v 1.81 2006/08/02 01:59:45 joe Exp $
 *
 * HISTORY
 *	  AUTHOR			DATE			MAJOR EVENT
 *	  Andrew Yu			Oct 26, 1994	file creation
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/printtup.h"
#include "nodes/plannodes.h"            /* Plan, Query */
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

/*
 * print
 *	  print contents of Node to stdout
 */
void
print(void *obj)
{
	char	   *s;
	char	   *f;

	s = nodeToString(obj);
	f = format_node_dump(s);
	pfree(s);
	printf("%s\n", f);
	fflush(stdout);
	
	pfree(f);
}

/*
 * pprint
 *	  pretty-print contents of Node to stdout
 */
void
pprint(void *obj)
{
	char	   *s;
	char	   *f;

	s = nodeToString(obj);
	f = pretty_format_node_dump(s);
	pfree(s);
	printf("%s\n", f);
	fflush(stdout);
	
	pfree(f);
}

/*
 * elog_node_display
 *	  send pretty-printed contents of Node to postmaster log
 */
void
elog_node_display(int lev, const char *title, void *obj, bool pretty)
{
	char	   *s;
	char	   *f;

	s = nodeToString(obj);
	if (pretty)
		f = pretty_format_node_dump(s);
	else
		f = format_node_dump(s);
	pfree(s);
	ereport(lev,
			(errmsg_internal("%s:", title),
			 errdetail("%s", f)));
	pfree(f);
}

/*
 * Format a nodeToString output for display on a terminal.
 *
 * The result is a palloc'd string.
 *
 * This version just tries to break at whitespace.
 */
char *
format_node_dump(const char *dump)
{
#define LINELEN		78
	char		line[LINELEN + 1];
	StringInfoData str;
	int			i;
	int			j;
	int			k;

	initStringInfo(&str);
	i = 0;
	for (;;)
	{
		for (j = 0; j < LINELEN && dump[i] != '\0'; i++, j++)
			line[j] = dump[i];
		if (dump[i] == '\0')
			break;
		if (dump[i] == ' ')
		{
			/* ok to break at adjacent space */
			i++;
		}
		else
		{
			for (k = j - 1; k > 0; k--)
				if (line[k] == ' ')
					break;
			if (k > 0)
			{
				/* back up; will reprint all after space */
				i -= (j - k - 1);
				j = k;
			}
		}
		line[j] = '\0';
		appendStringInfo(&str, "%s\n", line);
	}
	if (j > 0)
	{
		line[j] = '\0';
		appendStringInfo(&str, "%s\n", line);
	}
	return str.data;
#undef LINELEN
}

/*
 * Format a nodeToString output for display on a terminal.
 *
 * The result is a palloc'd string.
 *
 * This version tries to indent intelligently.
 */
char *
pretty_format_node_dump(const char *dump)
{
#define INDENTSTOP	3
#define MAXINDENT	60
#define LINELEN		78
	char		line[LINELEN + 1];
	StringInfoData str;
	int			indentLev;
	int			indentDist;
	int			i;
	int			j;

	initStringInfo(&str);
	indentLev = 0;				/* logical indent level */
	indentDist = 0;				/* physical indent distance */
	i = 0;
	for (;;)
	{
		for (j = 0; j < indentDist; j++)
			line[j] = ' ';
		for (; j < LINELEN && dump[i] != '\0'; i++, j++)
		{
			line[j] = dump[i];
			switch (line[j])
			{
				case '}':
					if (j != indentDist)
					{
						/* print data before the } */
						line[j] = '\0';
						appendStringInfo(&str, "%s\n", line);
					}
					/* print the } at indentDist */
					line[indentDist] = '}';
					line[indentDist + 1] = '\0';
					appendStringInfo(&str, "%s\n", line);
					/* outdent */
					if (indentLev > 0)
					{
						indentLev--;
						indentDist = Min(indentLev * INDENTSTOP, MAXINDENT);
					}
					j = indentDist - 1;
					/* j will equal indentDist on next loop iteration */
					/* suppress whitespace just after } */
					while (dump[i + 1] == ' ')
						i++;
					break;
				case ')':
					/* force line break after ), unless another ) follows */
					if (dump[i + 1] != ')')
					{
						line[j + 1] = '\0';
						appendStringInfo(&str, "%s\n", line);
						j = indentDist - 1;
						while (dump[i + 1] == ' ')
							i++;
					}
					break;
				case '{':
					/* force line break before { */
					if (j != indentDist)
					{
						line[j] = '\0';
						appendStringInfo(&str, "%s\n", line);
					}
					/* indent */
					indentLev++;
					indentDist = Min(indentLev * INDENTSTOP, MAXINDENT);
					for (j = 0; j < indentDist; j++)
						line[j] = ' ';
					line[j] = dump[i];
					break;
				case ':':
					/* force line break before : */
					if (j != indentDist)
					{
						line[j] = '\0';
						appendStringInfo(&str, "%s\n", line);
					}
					j = indentDist;
					line[j] = dump[i];
					break;
			}
		}
		line[j] = '\0';
		if (dump[i] == '\0')
			break;
		appendStringInfo(&str, "%s\n", line);
	}
	if (j > 0)
		appendStringInfo(&str, "%s\n", line);
	return str.data;
#undef INDENTSTOP
#undef MAXINDENT
#undef LINELEN
}

/*
 * print_rt
 *	  print contents of range table
 */
void
print_rt(List *rtable)
{
	ListCell   *l;
	int			i = 1;

	printf("resno\trefname  \trelid\tinFromCl\n");
	printf("-----\t---------\t-----\t--------\n");
	foreach(l, rtable)
	{
		RangeTblEntry *rte = lfirst(l);
		const char    *name = rte->eref ? rte->eref->aliasname
		                                : "<null>";

		switch (rte->rtekind)
		{
			case RTE_RELATION:
				printf("%d\t%s\t%u",
					   i, name, rte->relid);
				break;
			case RTE_SUBQUERY:
				printf("%d\t%s\t[subquery]",
					   i, name);
				break;
			case RTE_CTE:
				printf("%d\t%s\t[cte]",
					   i, name);
				break;
			case RTE_TABLEFUNCTION:
				printf("%d\t%s\t[tablefunction]",
					   i, name);
				break;
			case RTE_FUNCTION:
				printf("%d\t%s\t[rangefunction]",
					   i, name);
				break;
			case RTE_VALUES:
				printf("%d\t%s\t[values list]",
					   i, rte->eref->aliasname);
				break;
			case RTE_JOIN:
				printf("%d\t%s\t[join]",
					   i, name);
				break;
			case RTE_SPECIAL:
				printf("%d\t%s\t[special]",
					   i, name);
				break;
			case RTE_VOID:
				printf("%d\t%s\t[void]",
					   i, name);
				break;
			default:
				printf("%d\t%s\t[unknown rtekind]",
					   i, name);
		}

		printf("\t%s\t%s\n",
			   (rte->inh ? "inh" : ""),
			   (rte->inFromCl ? "inFromCl" : ""));
		i++;
	}
}


/*
 * print_expr
 *	  print an expression
 */
void
print_expr(Node *expr, List *rtable)
{
	if (expr == NULL)
	{
		printf("<>");
		return;
	}

	if (IsA(expr, Var))
	{
		Var		   *var = (Var *) expr;
		const char *relname;
		const char *attname;

		switch (var->varno)
		{
			case INNER:
				relname = "INNER";
				attname = "?";
				break;
			case OUTER:
				relname = "OUTER";
				attname = "?";
				break;
			default:
				{
					RangeTblEntry *rte;

					Assert(var->varno > 0 &&
						   (int) var->varno <= list_length(rtable));
					rte = rt_fetch(var->varno, rtable);
					relname = rte->eref->aliasname;
					attname = get_rte_attribute_name(rte, var->varattno);
				}
				break;
		}
		printf("%s.%s", relname, attname);
	}
	else if (IsA(expr, Const))
	{
		Const	   *c = (Const *) expr;
		Oid			typoutput;
		bool		typIsVarlena;
		char	   *outputstr;

		if (c->constisnull)
		{
			printf("NULL");
			return;
		}

		getTypeOutputInfo(c->consttype,
						  &typoutput, &typIsVarlena);

		outputstr = OidOutputFunctionCall(typoutput, c->constvalue);
		printf("%s", outputstr);
		pfree(outputstr);
	}
	else if (IsA(expr, OpExpr))
	{
		OpExpr	   *e = (OpExpr *) expr;
		char	   *opname;

		opname = get_opname(e->opno);
		if (list_length(e->args) > 1)
		{
			print_expr(get_leftop((Expr *) e), rtable);
			printf(" %s ", ((opname != NULL) ? opname : "(invalid operator)"));
			print_expr(get_rightop((Expr *) e), rtable);
		}
		else
		{
			/* we print prefix and postfix ops the same... */
			printf("%s ", ((opname != NULL) ? opname : "(invalid operator)"));
			print_expr(get_leftop((Expr *) e), rtable);
		}
	}
	else if (IsA(expr, FuncExpr))
	{
		FuncExpr   *e = (FuncExpr *) expr;
		char	   *funcname;
		ListCell   *l;

		funcname = get_func_name(e->funcid);
		printf("%s(", ((funcname != NULL) ? funcname : "(invalid function)"));
		foreach(l, e->args)
		{
			print_expr(lfirst(l), rtable);
			if (lnext(l))
				printf(",");
		}
		printf(")");
	}
	else
		printf("unknown expr");
}

/*
 * print_pathkeys -
 *	  pathkeys list of list of PathKeyItems
 */
void
print_pathkeys(List *pathkeys, List *rtable)
{
	ListCell   *i;

	printf("(");
	foreach(i, pathkeys)
	{
		List	   *pathkey = (List *) lfirst(i);
		ListCell   *k;

		printf("(");
		foreach(k, pathkey)
		{
			PathKeyItem *item = (PathKeyItem *) lfirst(k);

			print_expr(item->key, rtable);
			if (lnext(k))
				printf(", ");
		}
		printf(")");
		if (lnext(i))
			printf(", ");
	}
	printf(")\n");
}

/*
 * print_tl
 *	  print targetlist in a more legible way.
 */
void
print_tl(List *tlist, List *rtable)
{
	ListCell   *tl;

	printf("(\n");
	foreach(tl, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tl);

		printf("\t%d %s\t", tle->resno,
			   tle->resname ? tle->resname : "<null>");
		if (tle->ressortgroupref != 0)
			printf("(%u):\t", tle->ressortgroupref);
		else
			printf("    :\t");
		print_expr((Node *) tle->expr, rtable);
		printf("\n");
	}
	printf(")\n");
}

/*
 * print_slot
 *	  print out the tuple with the given TupleTableSlot
 */
void
print_slot(struct TupleTableSlot *slot)
{
	if (TupIsNull(slot))
		printf("tuple is null.\n");
	else if (!slot->tts_tupleDescriptor)
		printf("no tuple descriptor.\n");
	else
		debugtup(slot, NULL);
	
	fflush(stdout);
}

char * plannode_type(Plan *p)
{
	switch (nodeTag(p))
	{
		case T_Plan:
			return "PLAN";
		case T_Result:
			return "RESULT";
		case T_Append:
			return "APPEND";
		case T_BitmapAnd:
			return "BITMAPAND";
		case T_BitmapOr:
			return "BITMAPOR";
		case T_Scan:
			return "SCAN";
		case T_SeqScan:
			return "SEQSCAN";
		case T_AppendOnlyScan:
			return "APPENDONLYSCAN";
		case T_ParquetScan:
			return "PARQUETSCAN";
		case T_ExternalScan:
			return "EXTERNALSCAN";
		case T_IndexScan:
			return "INDEXSCAN";
		case T_BitmapIndexScan:
			return "BITMAPINDEXSCAN";
		case T_BitmapHeapScan:
			return "BITMAPHEAPSCAN";
		case T_TidScan:
			return "TIDSCAN";
		case T_SubqueryScan:
			return "SUBQUERYSCAN";
		case T_FunctionScan:
			return "FUNCTIONSCAN";
		case T_ValuesScan:
			return "VALUESSCAN";
		case T_BitmapTableScan:
			return "BITMAPTABLESCAN";
		case T_Join:
			return "JOIN";
		case T_NestLoop:
			return "NESTLOOP";
		case T_MergeJoin:
			return "MERGEJOIN";
		case T_HashJoin:
			return "HASHJOIN";
		case T_ShareInputScan:
			return "SHAREINPUTSCAN";
		case T_Material:
			return "MATERIAL";
		case T_Sort:
			return "SORT";
		case T_Agg:
			return "AGG";
		case T_Window:
			return "WINDOW";
		case T_TableFunctionScan:
			return "TABLEFUNCTIONSCAN";
		case T_Unique:
			return "UNIQUE";
		case T_SetOp:
			return "SETOP";
		case T_Limit:
			return "LIMIT";
		case T_Hash:
			return "HASH";
		case T_Motion:
			return "MOTION";
		case T_Repeat:
			return "REPEAT";
		default:
			return "UNKNOWN";
	}
}

/*
 * Recursively prints a simple text description of the plan tree
 */
void
print_plan_recursive(struct Plan *p, struct Query *parsetree, int indentLevel, char *label)
{
	int			i;
	char		extraInfo[NAMEDATALEN + 100];

	if (!p)
		return;
	for (i = 0; i < indentLevel; i++)
		printf(" ");
	printf("%s%s :c=%.2f..%.2f :r=%.0f :w=%d ", label, plannode_type(p),
		   p->startup_cost, p->total_cost,
		   p->plan_rows, p->plan_width);
	if (IsA(p, Scan) ||
		IsA(p, SeqScan) ||
		IsA(p, BitmapHeapScan))
	{
		RangeTblEntry *rte;

		rte = rt_fetch(((Scan *) p)->scanrelid, parsetree->rtable);
		StrNCpy(extraInfo, rte->eref->aliasname, NAMEDATALEN);
	}
	else if (IsA(p, IndexScan))
	{
		RangeTblEntry *rte;

		rte = rt_fetch(((IndexScan *) p)->scan.scanrelid, parsetree->rtable);
		StrNCpy(extraInfo, rte->eref->aliasname, NAMEDATALEN);
	}
	else if (IsA(p, FunctionScan))
	{
		RangeTblEntry *rte;

		rte = rt_fetch(((FunctionScan *) p)->scan.scanrelid, parsetree->rtable);
		StrNCpy(extraInfo, rte->eref->aliasname, NAMEDATALEN);
	}
	else if (IsA(p, ValuesScan))
	{
		RangeTblEntry *rte;

		rte = rt_fetch(((ValuesScan *) p)->scan.scanrelid, parsetree->rtable);
		StrNCpy(extraInfo, rte->eref->aliasname, NAMEDATALEN);
	}
	else
		extraInfo[0] = '\0';
	if (extraInfo[0] != '\0')
		printf(" ( %s )\n", extraInfo);
	else
		printf("\n");
	print_plan_recursive(p->lefttree, parsetree, indentLevel + 3, "l: ");
	print_plan_recursive(p->righttree, parsetree, indentLevel + 3, "r: ");

	if (IsA(p, Append))
	{
		ListCell   *l;
		Append	   *appendplan = (Append *) p;

		foreach(l, appendplan->appendplans)
		{
			Plan	   *subnode = (Plan *) lfirst(l);

			print_plan_recursive(subnode, parsetree, indentLevel + 3, "a: ");
		}
	}

	if (IsA(p, BitmapAnd))
	{
		ListCell   *l;
		BitmapAnd  *bitmapandplan = (BitmapAnd *) p;

		foreach(l, bitmapandplan->bitmapplans)
		{
			Plan	   *subnode = (Plan *) lfirst(l);

			print_plan_recursive(subnode, parsetree, indentLevel + 3, "a: ");
		}
	}

	if (IsA(p, BitmapOr))
	{
		ListCell   *l;
		BitmapOr   *bitmaporplan = (BitmapOr *) p;

		foreach(l, bitmaporplan->bitmapplans)
		{
			Plan	   *subnode = (Plan *) lfirst(l);

			print_plan_recursive(subnode, parsetree, indentLevel + 3, "a: ");
		}
	}
}

/*
 * print_plan
 *
 * prints just the plan node types
 */
void
print_plan(struct Plan *p, struct Query *parsetree)
{
	print_plan_recursive(p, parsetree, 0, "");
}
