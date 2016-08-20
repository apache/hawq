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

/*
 * pxffilters.c
 *
 * Functions for handling push down of supported scan level filters to PXF.
 * 
 */
#include "access/pxffilters.h"
#include "catalog/pg_operator.h"
#include "optimizer/clauses.h"
#include "parser/parse_expr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

static List* pxf_make_filter_list(List* quals);
static void pxf_free_filter(PxfFilterDesc* filter);
static void pxf_free_filter_list(List *filters);
static char* pxf_serialize_filter_list(List *filters);
static bool opexpr_to_pxffilter(OpExpr *expr, PxfFilterDesc *filter);
static bool supported_filter_type(Oid type);
static void const_to_str(Const *constval, StringInfo buf);
static List* append_attr_from_var(Var* var, List* attrs);

/*
 * All supported HAWQ operators, and their respective HFDS operator code.
 * Note that it is OK to use hardcoded OIDs, since these are all pinned
 * down system catalog operators.
 * see pg_operator.h
 */
dbop_pxfop_map pxf_supported_opr[] =
{
	/* int2 */
	{Int2EqualOperator  /* int2eq */, PXFOP_EQ},
	{95  /* int2lt */, PXFOP_LT},
	{520 /* int2gt */, PXFOP_GT},
	{522 /* int2le */, PXFOP_LE},
	{524 /* int2ge */, PXFOP_GE},
	{519 /* int2ne */, PXFOP_NE},

	/* int4 */
	{Int4EqualOperator  /* int4eq */, PXFOP_EQ},
	{97  /* int4lt */, PXFOP_LT},
	{521 /* int4gt */, PXFOP_GT},
	{523 /* int4le */, PXFOP_LE},
	{525 /* int4ge */, PXFOP_GE},
	{518 /* int4lt */, PXFOP_NE},

	/* int8 */
	{Int8EqualOperator /* int8eq */, PXFOP_EQ},
	{412 /* int8lt */, PXFOP_LT},
	{413 /* int8gt */, PXFOP_GT},
	{414 /* int8le */, PXFOP_LE},
	{415 /* int8ge */, PXFOP_GE},
	{411 /* int8lt */, PXFOP_NE},

	/* text */
	{TextEqualOperator  /* texteq  */, PXFOP_EQ},
	{664 /* text_lt */, PXFOP_LT},
	{666 /* text_gt */, PXFOP_GT},
	{665 /* text_le */, PXFOP_LE},
	{667 /* text_ge */, PXFOP_GE},
	{531 /* textlt  */, PXFOP_NE},

	/* int2 to int4 */
	{Int24EqualOperator /* int24eq */, PXFOP_EQ},
	{534  /* int24lt */, PXFOP_LT},
	{536 /* int24gt */, PXFOP_GT},
	{540 /* int24le */, PXFOP_LE},
	{542 /* int24ge */, PXFOP_GE},
	{538 /* int24ne */, PXFOP_NE},

	/* int4 to int2 */
	{Int42EqualOperator /* int42eq */, PXFOP_EQ},
	{535  /* int42lt */, PXFOP_LT},
	{537 /* int42gt */, PXFOP_GT},
	{541 /* int42le */, PXFOP_LE},
	{543 /* int42ge */, PXFOP_GE},
	{539 /* int42ne */, PXFOP_NE},

	/* int8 to int4 */
	{Int84EqualOperator /* int84eq */, PXFOP_EQ},
	{418  /* int84lt */, PXFOP_LT},
	{419 /* int84gt */, PXFOP_GT},
	{420 /* int84le */, PXFOP_LE},
	{430 /* int84ge */, PXFOP_GE},
	{417 /* int84ne */, PXFOP_NE},

	/* int4 to int8 */
	{Int48EqualOperator /* int48eq */, PXFOP_EQ},
	{37  /* int48lt */, PXFOP_LT},
	{76 /* int48gt */, PXFOP_GT},
	{80 /* int48le */, PXFOP_LE},
	{82 /* int48ge */, PXFOP_GE},
	{36 /* int48ne */, PXFOP_NE},

	/* int2 to int8 */
	{Int28EqualOperator /* int28eq */, PXFOP_EQ},
	{1864  /* int28lt */, PXFOP_LT},
	{1865 /* int28gt */, PXFOP_GT},
	{1866 /* int28le */, PXFOP_LE},
	{1867 /* int28ge */, PXFOP_GE},
	{1863 /* int28ne */, PXFOP_NE},

	/* int8 to int2 */
	{Int82EqualOperator /* int82eq */, PXFOP_EQ},
	{1870  /* int82lt */, PXFOP_LT},
	{1871 /* int82gt */, PXFOP_GT},
	{1872 /* int82le */, PXFOP_LE},
	{1873 /* int82ge */, PXFOP_GE},
	{1869 /* int82ne */, PXFOP_NE}

};

Oid pxf_supported_types[] =
{
	INT2OID,
	INT4OID,
	INT8OID,
	FLOAT4OID,
	FLOAT8OID,
	NUMERICOID,
	TEXTOID,
	VARCHAROID,
	BPCHAROID,
	CHAROID,
	BYTEAOID,
	BOOLOID
};

/*
 * pxf_make_filter_list
 *
 * Given a scan node qual list, find the filters that are eligible to be used
 * by PXF, construct a PxfFilterDesc list that describes the filter information,
 * and return it to the caller.
 *
 * Caller is responsible for pfreeing the returned PxfFilterDesc List.
 */
static List *
pxf_make_filter_list(List *quals)
{
	List			*result = NIL;
	ListCell		*lc = NULL;
	
	if (list_length(quals) == 0)
		return NIL;

	/*
	 * Iterate over all implicitly ANDed qualifiers and add the ones
	 * that are supported for push-down into the result filter list.
	 */
	foreach (lc, quals)
	{
		Node *node = (Node *) lfirst(lc);
		NodeTag tag = nodeTag(node);

		switch (tag)
		{
			case T_OpExpr:
			{
				OpExpr			*expr 	= (OpExpr *) node;
				PxfFilterDesc	*filter;

				filter = (PxfFilterDesc *) palloc0(sizeof(PxfFilterDesc));
				elog(DEBUG5, "pxf_make_filter_list: node tag %d (T_OpExpr)", tag);

				if (opexpr_to_pxffilter(expr, filter))
					result = lappend(result, filter);
				else
					pfree(filter);

				break;
			}
			case T_BoolExpr:
			{
				BoolExpr	*expr = (BoolExpr *) node;
				BoolExprType boolType = expr->boolop;
				elog(DEBUG5, "pxf_make_filter_list: node tag %d (T_BoolExpr), bool node type %d %s",
						tag, boolType, boolType==AND_EXPR ? "(AND_EXPR)" : "");

				/* only AND_EXPR is supported */
				if (expr->boolop == AND_EXPR)
				{
					List *inner_result = pxf_make_filter_list(expr->args);
					elog(DEBUG5, "pxf_make_filter_list: inner result size %d", list_length(inner_result));
					result = list_concat(result, inner_result);
				}
				break;
			}
			default:
				/* expression not supported. ignore */
				elog(DEBUG5, "pxf_make_filter_list: unsupported node tag %d", tag);
				break;
		}
	}
	
	return result;
}

static void
pxf_free_filter(PxfFilterDesc* filter)
{
	if (!filter)
		return;

	if (filter->l.conststr)
	{
		if (filter->l.conststr->data)
			pfree(filter->l.conststr->data);
		pfree(filter->l.conststr);
	}
	if (filter->r.conststr)
	{
		if (filter->r.conststr->data)
			pfree(filter->r.conststr->data);
		pfree(filter->r.conststr);
	}

	pfree(filter);
}

/*
 * pxf_free_filter_list
 *
 * free all memory associated with the filters once no longer needed.
 * alternatively we could have allocated them in a shorter lifespan
 * memory context, however explicitly freeing them is easier and makes
 * more sense.
 */
static void
pxf_free_filter_list(List *filters)
{
	ListCell		*lc 	= NULL;
	PxfFilterDesc 	*filter = NULL;

	if (list_length(filters) == 0)
		return;

	foreach (lc, filters)
	{
		filter	= (PxfFilterDesc *) lfirst(lc);
		pxf_free_filter(filter);
	}
}

/*
 * pxf_serialize_filter_list
 *
 * Given a list of implicitly ANDed PxfFilterDesc objects, produce a
 * serialized string representation in order to communicate this list
 * over the wire.
 *
 * The serialized string is in a RPN (Reversed Polish Notation) format
 * as flattened tree. Operands and operators are represented with their
 * respective codes. Each filter is serialized as follows:
 *
 * <attcode><attnum><constcode><constval><opercode><opernum>
 *
 * Example filter list:
 *
 * Column(0) > 1
 * Column(0) < 5
 * Column(2) == "third"
 *
 * Yields the following serialized string:
 *
 * a0c1o2a0c5o1o7a2c"third"o5o7
 *
 */
static char *
pxf_serialize_filter_list(List *filters)
{
	StringInfo	 resbuf;
	StringInfo	 curbuf;
	ListCell	*lc = NULL;

	if (list_length(filters) == 0)
		return NULL;

	resbuf = makeStringInfo();
	initStringInfo(resbuf);
	curbuf = makeStringInfo();
	initStringInfo(curbuf);

	/*
	 * Iterate through the filters in the list and serialize them one after
	 * the other. We use buffer copying because it's clear. Considering the
	 * typical small number of memcpy's this generates overall, there's no
	 * point in optimizing, better keep it clear.
	 */
	foreach (lc, filters)
	{
		PxfFilterDesc		*filter	= (PxfFilterDesc *) lfirst(lc);
		PxfOperand			 l		= filter->l;
		PxfOperand			 r 		= filter->r;
		PxfOperatorCode	 o 		= filter->op;

		/* last result is stored in 'oldbuf'. start 'curbuf' clean */
		resetStringInfo(curbuf);

		/* format the operands */
		if (pxfoperand_is_attr(l) && pxfoperand_is_const(r))
		{
			appendStringInfo(curbuf, "%c%d%c%s",
									 PXF_ATTR_CODE, l.attnum - 1, /* Java attrs are 0-based */
									 PXF_CONST_CODE, (r.conststr)->data);
		}
		else if (pxfoperand_is_const(l) && pxfoperand_is_attr(r))
		{
			appendStringInfo(curbuf, "%c%s%c%d",
									 PXF_CONST_CODE, (l.conststr)->data,
									 PXF_ATTR_CODE, r.attnum - 1); /* Java attrs are 0-based */
		}
		else
		{
			/* pxf_make_filter_list() should have never let this happen */
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal error in pxffilters.c:pxf_serialize_"
							 "filter_list. Found a non const+attr filter")));
		}

		/* format the operator */
		appendStringInfo(curbuf, "%c%d", PXF_OPERATOR_CODE, o);

		/* append this result to the previous result */
		appendBinaryStringInfo(resbuf, curbuf->data, curbuf->len);

		/* if there was a previous result, append a trailing AND operator */
		if(resbuf->len > curbuf->len)
		{
			appendStringInfo(resbuf, "%c%d", PXF_OPERATOR_CODE, PXFOP_AND);
		}
	}

	pfree(curbuf->data);

	return resbuf->data;
}


/*
 * opexpr_to_pxffilter
 *
 * check if an OpExpr qualifies to be pushed-down to PXF.
 * if it is - create it and return a success code.
 */
static bool
opexpr_to_pxffilter(OpExpr *expr, PxfFilterDesc *filter)
{
	int		 i;
	int		 nargs 		= sizeof(pxf_supported_opr) / sizeof(dbop_pxfop_map);
	Node	*leftop 	= NULL;
	Node	*rightop	= NULL;
	Oid		 rightop_type = InvalidOid;
	Oid		 leftop_type = InvalidOid;

	if ((!expr) || (!filter))
		return false;

	leftop = get_leftop((Expr*)expr);
	rightop	= get_rightop((Expr*)expr);
	leftop_type = exprType(leftop);
	rightop_type = exprType(rightop);

	/* only binary oprs supported currently */
	if (!rightop)
	{
		elog(DEBUG5, "opexpr_to_pxffilter: unary op! leftop_type: %d, op: %d",
			 leftop_type, expr->opno);
		return false;
	}

	elog(DEBUG5, "opexpr_to_gphdfilter: leftop (expr type: %d, arg type: %d), "
			"rightop_type (expr type: %d, arg type %d), op: %d",
			leftop_type, nodeTag(leftop),
			rightop_type, nodeTag(rightop),
			expr->opno);

	/*
	 * check if supported type -
	 */
	if (!supported_filter_type(rightop_type) || !supported_filter_type(leftop_type))
		return false;

	/* arguments must be VAR and CONST */
	if (IsA(leftop,  Var) && IsA(rightop, Const))
	{
		filter->l.opcode = PXF_ATTR_CODE;
		filter->l.attnum = ((Var *) leftop)->varattno;
		if (filter->l.attnum <= InvalidAttrNumber)
			return false; /* system attr not supported */

		filter->r.opcode = PXF_CONST_CODE;
		filter->r.attnum = InvalidAttrNumber;
		filter->r.conststr = makeStringInfo();
		initStringInfo(filter->r.conststr);
		const_to_str((Const *)rightop, filter->r.conststr);
	}
	else if (IsA(leftop, Const) && IsA(rightop, Var))
	{
		filter->l.opcode = PXF_CONST_CODE;
		filter->l.attnum = InvalidAttrNumber;
		filter->l.conststr = makeStringInfo();
		initStringInfo(filter->l.conststr);
		const_to_str((Const *)leftop, filter->l.conststr);

		filter->r.opcode = PXF_ATTR_CODE;
		filter->r.attnum = ((Var *) rightop)->varattno;
		if (filter->r.attnum <= InvalidAttrNumber)
			return false; /* system attr not supported */
	}
	else
	{
		return false;
	}

	/* is operator supported? if so, set the corresponding PXFOP */
	for (i = 0; i < nargs; i++)
	{
		/* NOTE: switch to hash table lookup if   */
		/* array grows. for now it's cheap enough */
		if(expr->opno == pxf_supported_opr[i].dbop)
		{
			filter->op = pxf_supported_opr[i].pxfop;
			return true; /* filter qualifies! */
		}
	}

	/* NOTE: if more validation needed, add it before the operators test
	 * or alternatively change it to use a false flag and return true below */

	return false;
}

static List*
append_attr_from_var(Var* var, List* attrs)
{
	AttrNumber varattno = var->varattno;
	/* system attr not supported */
	if (varattno > InvalidAttrNumber)
		return lappend_int(attrs, varattno - 1);

	return attrs;
}

static List*
get_attrs_from_expr(Expr *expr)
{
	Node	*leftop 	= NULL;
	Node	*rightop	= NULL;
	List	*attrs = NIL;

	if ((!expr))
		return attrs;

	if (IsA(expr, OpExpr))
	{
		leftop = get_leftop(expr);
		rightop	= get_rightop(expr);
	} else if (IsA(expr, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) expr;
		leftop = (Node *) linitial(saop->args);
		rightop = (Node *) lsecond(saop->args);
	}

	if (IsA(leftop, Var))
	{
		attrs = append_attr_from_var((Var *) leftop, attrs);
	}
	if (IsA(leftop, Const))
	{
		attrs = append_attr_from_var((Var *) rightop, attrs);
	}

	return attrs;

}

/*
 * supported_filter_type
 *
 * Return true if the type is supported by pxffilters.
 * Supported defines are defined in pxf_supported_types.
 */
static bool
supported_filter_type(Oid type)
{
	int		 nargs 		= sizeof(pxf_supported_types) / sizeof(Oid);
	int 	 i;

	/* is type supported? */
	for (i = 0; i < nargs; i++)
	{
		if (type == pxf_supported_types[i])
			return true;
	}
	return false;
}

/*
 * const_to_str
 *
 * Extract the value stored in a const operand into a string. If the operand
 * type is text based, make sure to escape the value with surrounding quotes.
 */
static void
const_to_str(Const *constval, StringInfo buf)
{
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	if (constval->constisnull)
	{
		/* TODO: test this edge case and its consequences */
		appendStringInfo(buf, "\"NULL\"");
		return;
	}

	getTypeOutputInfo(constval->consttype,
					  &typoutput, &typIsVarlena);

	extval = OidOutputFunctionCall(typoutput, constval->constvalue);

	switch (constval->consttype)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			appendStringInfo(buf, "%s", extval);
			break;

		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case CHAROID:
		case BYTEAOID:
			appendStringInfo(buf, "\\\"%s\\\"", extval);
			break;

		case BOOLOID:
			if (strcmp(extval, "t") == 0)
				appendStringInfo(buf, "\"true\"");
			else
				appendStringInfo(buf, "\"false\"");
			break;

		default:
			/* should never happen. we filter on types earlier */
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal error in pxffilters.c:const_to_str. "
							"Using unsupported data type (%d) (value %s)",
							constval->consttype, extval)));

	}

	pfree(extval);
}


/*
 * serializePxfFilterQuals
 *
 * Wrapper around pxf_make_filter_list -> pxf_serialize_filter_list.
 * Currently the only function exposed to the outside, however
 * we could expose the others in the future if needed.
 *
 * The function accepts the scan qual list and produces a serialized
 * string that represents the push down filters (See called functions
 * headers for more information).
 */
char *serializePxfFilterQuals(List *quals)
{
	char	*result = NULL;

	if (pxf_enable_filter_pushdown)
	{
		List *filters = pxf_make_filter_list(quals);

		result  = pxf_serialize_filter_list(filters);
		pxf_free_filter_list(filters);
	}
	elog(DEBUG2, "serializePxfFilterQuals: filter result: %s", (result == NULL) ? "null" : result);

	return result;
}


/*
 * Returns a list of attributes, extracted from quals.
 * Supports AND, OR, NOT operations.
 * Supports =, <, <=, >, >=, IS NULL, IS NOT NULL, BETWEEN, IN operators.
 * List might contain duplicates.
 * Caller should release memory once result is not needed.
 */
List* extractPxfAttributes(List* quals)
{

	ListCell		*lc = NULL;
	List *attributes = NIL;

	if (list_length(quals) == 0)
		return NIL;

	foreach (lc, quals)
	{
		Node *node = (Node *) lfirst(lc);
		NodeTag tag = nodeTag(node);

		switch (tag)
		{
			case T_OpExpr:
			case T_ScalarArrayOpExpr:
			{
				Expr* expr = (Expr *) node;
				List			*attrs = get_attrs_from_expr(expr);
				attributes = list_concat(attributes, attrs);
				break;
			}
			case T_BoolExpr:
			{
				BoolExpr* expr = (BoolExpr *) node;
				List *inner_result = extractPxfAttributes(expr->args);
				attributes = list_concat(attributes, inner_result);
				break;
			}
			case T_NullTest:
			{
				NullTest* expr = (NullTest *) node;
				attributes = append_attr_from_var((Var *) expr->arg, attributes);
				break;
			}
			default:
				/*
				 * tag is not supported, it's risk of having:
				 * 1) false-positive tuples
				 * 2) unable to join tables
				 * 3) etc
				 */
				elog(ERROR, "extractPxfAttributes: unsupported node tag %d, unable to extract attribute from qualifier", tag);
				break;
		}
	}

	return attributes;
}
