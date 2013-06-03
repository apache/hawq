/*
 * gphdfilters.c
 *
 * Functions for handling push down of supported scan level filters to GPBridge.
 * 
 * Copyright (c) 2012, Greenplum inc
 */

#include "gphdfilters.h"
#include "optimizer/clauses.h"
#include "parser/parse_expr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

static bool opexpr_to_gphdfilter(OpExpr *expr, GPHDFilterDesc *filter);
static void const_to_str(Const *constval, StringInfo buf);

/*
 * All supported GPDB operators, and their respective HFDS operator code.
 * Note that it is OK to use hardcoded OIDs, since these are all pinned
 * down system catalog operators.
 */
gpop_hdop_map gphd_supported_opr[] =
{
	/* int2 */
	{94  /* int2eq */, HDOP_EQ},
	{95  /* int2lt */, HDOP_LT},
	{520 /* int2gt */, HDOP_GT},
	{524 /* int2ge */, HDOP_GE},
	{522 /* int2le */, HDOP_LE},
	{519 /* int2ne */, HDOP_NE},

	/* int4 */
	{96  /* int4eq */, HDOP_EQ},
	{97  /* int4lt */, HDOP_LT},
	{521 /* int4gt */, HDOP_GT},
	{525 /* int4ge */, HDOP_GE},
	{523 /* int4le */, HDOP_LE},
	{518 /* int4lt */, HDOP_NE},

	/* int8 */
	{410 /* int8eq */, HDOP_EQ},
	{412 /* int8lt */, HDOP_LT},
	{413 /* int8gt */, HDOP_GT},
	{415 /* int8ge */, HDOP_GE},
	{414 /* int8le */, HDOP_LE},
	{411 /* int8lt */, HDOP_NE},

	/* text */
	{98  /* texteq  */, HDOP_EQ},
	{664 /* text_lt */, HDOP_LT},
	{666 /* text_gt */, HDOP_GT},
	{667 /* text_ge */, HDOP_GE},
	{665 /* text_le */, HDOP_LE},
	{531 /* textlt  */, HDOP_NE},
};


/*
 * gphd_make_filter_list
 *
 * Given a scan node qual list, find the filters that are eligible to be used in
 * the GPBridge, construct a GPHDFilter that describes the filter information,
 * and return it to the caller.
 *
 * Caller is responsible for pfreeing the returned GPHDFilter List.
 */
static List *
gphd_make_filter_list(List *quals)
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

		switch (nodeTag(node))
		{
			case T_OpExpr:
			{
				OpExpr			*expr 	= (OpExpr *) node;
				GPHDFilterDesc	*filter;

				filter = (GPHDFilterDesc *) palloc0(sizeof(GPHDFilterDesc));

				if (opexpr_to_gphdfilter(expr, filter))
					result = lappend(result, filter);
				else
					pfree(filter);

				break;
			}
			default:
				/* expression not supported. ignore */
				break;
		}
	}
	
	return result;
}

/*
 * gphd_free_filter_list
 *
 * free all memory associated with the filters once no longer needed.
 * alternatively we could have allocated them in a shorter lifespan
 * memory context, however explicitly freeing them is easier and makes
 * more sense.
 */
static void
gphd_free_filter_list(List *filters)
{
	ListCell		*lc 	= NULL;
	GPHDFilterDesc 	*filter = NULL;

	if (list_length(filters) == 0)
		return;

	foreach (lc, filters)
	{
		filter	= (GPHDFilterDesc *) lfirst(lc);
		pfree(filter);
	}
}

/*
 * gphd_serialize_filter_list
 *
 * Given a list of implicitly ANDed GPHDFilterDesc objects, produce a
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
gphd_serialize_filter_list(List *filters)
{
	StringInfo	 oldbuf;
	StringInfo	 curbuf;
	ListCell	*lc = NULL;

	if (list_length(filters) == 0)
		return NULL;

	oldbuf = makeStringInfo();
	initStringInfo(oldbuf);
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
		GPHDFilterDesc		*filter	= (GPHDFilterDesc *) lfirst(lc);
		GPHDOperand			 l		= filter->l;
		GPHDOperand			 r 		= filter->r;
		GPHDOperatorCode	 o 		= filter->op;

		/* store last result in 'oldbuf'. start 'curbuf' clean */
		resetStringInfo(oldbuf);
		appendBinaryStringInfo(oldbuf, curbuf->data, curbuf->len);
		resetStringInfo(curbuf);

		/* format the operands */
		if (gphdoperand_is_attr(l) && gphdoperand_is_const(r))
		{
			appendStringInfo(curbuf, "%c%d%c%s",
									 GPHD_ATTR_CODE, l.attnum - 1, /* Java attrs are 0-based */
									 GPHD_CONST_CODE, (r.conststr)->data);
		}
		else if (gphdoperand_is_const(l) && gphdoperand_is_attr(r))
		{
			appendStringInfo(curbuf, "%c%s%c%d",
									 GPHD_CONST_CODE, (l.conststr)->data,
									 GPHD_ATTR_CODE, r.attnum - 1); /* Java attrs are 0-based */
		}
		else
		{
			/* GphdFilterMakeList() should have never let this happen */
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal error in gphdfilters.c:SerializeGphd"
							"FilterList. Found a non const+attr filter")));
		}

		/* format the operator */
		appendStringInfo(curbuf, "%c%d", GPHD_OPERATOR_CODE, o);

		/* append the previous result, if any, with a trailing AND operator */
		if(oldbuf->len > 0)
		{
			appendBinaryStringInfo(curbuf, oldbuf->data, oldbuf->len);
			appendStringInfo(curbuf, "%c%d", GPHD_OPERATOR_CODE, HDOP_AND);
		}
	}

	pfree(oldbuf->data);

	return curbuf->data;
}


/*
 * opexpr_to_gphdfilter
 *
 * check if an OpExpr qualifies to be pushed-down to PXF.
 * if it is - create it and return a success code.
 */
static bool
opexpr_to_gphdfilter(OpExpr *expr, GPHDFilterDesc *filter)
{
	int		 i;
	int		 nargs 		= sizeof(gphd_supported_opr) / sizeof(gpop_hdop_map);
	Node	*leftop 	= get_leftop((Expr*)expr);
	Node	*rightop	= get_rightop((Expr*)expr);

	/* only binary oprs supported currently */
	if (!rightop)
		return false;

	/* both side data types must be identical (for now) */
	if (exprType(rightop) != exprType(leftop))
		return false;

	/* TODO: is it a supported data type? This may actually not be needed *
	 * as the supported operator list implicitly does the work for us     */

	/* arguments must be VAR and CONST */
	if (IsA(leftop,  Var) && IsA(rightop, Const))
	{
		filter->l.opcode = GPHD_ATTR_CODE;
		filter->l.attnum = ((Var *) leftop)->varattno;
		if (filter->l.attnum <= InvalidAttrNumber)
			return false; /* system attr not supported */

		filter->r.opcode = GPHD_CONST_CODE;
		filter->r.attnum = InvalidAttrNumber;
		filter->r.conststr = makeStringInfo();
		initStringInfo(filter->r.conststr);
		const_to_str((Const *)rightop, filter->r.conststr);
	}
	else if (IsA(leftop, Const) && IsA(rightop, Var))
	{
		filter->l.opcode = GPHD_CONST_CODE;
		filter->l.attnum = InvalidAttrNumber;
		filter->l.conststr = makeStringInfo();
		initStringInfo(filter->l.conststr);
		const_to_str((Const *)leftop, filter->l.conststr);

		filter->r.opcode = GPHD_ATTR_CODE;
		filter->r.attnum = ((Var *) rightop)->varattno;
		if (filter->r.attnum <= InvalidAttrNumber)
			return false; /* system attr not supported */
	}
	else
	{
		return false;
	}

	/* is operator supported? if so, set the corresponding HDOP */
	for (i = 0; i < nargs; i++)
	{
		/* NOTE: switch to hash table lookup if   */
		/* array grows. for now it's cheap enough */
		if(expr->opno == gphd_supported_opr[i].gpop)
		{
			filter->op = gphd_supported_opr[i].hdop;
			return true; /* filter qualifies! */
		}
	}

	/* NOTE: if more validation needed, add it before the operators test
	 * or alternatively change it to use a false flag and return true below */

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
					 errmsg("internal error in gphdfilters.c:const_to_str. "
							"Using unsupported data type")));

	}

	pfree(extval);
}

/*
 * GphdFilterSerializeQuals
 *
 * Wrapper around GphdFilterMakeList -> GphdFilterSerializeList.
 * Currently the only function exposed to the outside, however
 * if we could expose the others in the future if needed.
 *
 * The function accepts the scan qual list and produces a serialized
 * string that represents the push down filters (See called functions
 * headers for more information).
 */
char *serializeGPHDFilterQuals(List *quals)
{
	char	*result = NULL;

	if (pxf_enable_filter_pushdown)
	{
		List *filters = gphd_make_filter_list(quals);

		result  = gphd_serialize_filter_list(filters);
		gphd_free_filter_list(filters);
	}

	return result;
}

