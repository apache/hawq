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

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "../pxffilters.c"

void
test__supported_filter_type(void **state)
{
	Oid oids[] =
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
		BOOLOID,
		CIRCLEOID /* unsupported type */
	};

	int array_size = sizeof(oids) / sizeof(oids[0]);
	bool result = false;
	int i = 0;

	/* supported types */
	for (; i < array_size-1; ++i)
	{
		result = supported_filter_type(oids[i]);
		assert_true(result);
	}
	/* unsupported type */
	result = supported_filter_type(oids[i]);
	assert_false(result);

	/* go over pxf_supported_types array */
	int nargs = sizeof(pxf_supported_types) / sizeof(Oid);
	assert_int_equal(nargs, 12);
	for (i = 0; i < nargs; ++i)
	{
		assert_true(supported_filter_type(pxf_supported_types[i]));
	}

}

/*
 * const_value must be palloc'ed, it will be freed by const_to_str
 */
void
mock__const_to_str(Oid const_type, char* const_value)
{
	expect_value(getTypeOutputInfo, type, const_type);
	expect_any(getTypeOutputInfo, typOutput);
	expect_any(getTypeOutputInfo, typIsVarlena);
	will_return(getTypeOutputInfo, NULL);

	expect_any(OidOutputFunctionCall, functionId);
	expect_any(OidOutputFunctionCall, val);
	will_return(OidOutputFunctionCall, const_value);
}

void
verify__const_to_str(bool is_null, char* const_value, Oid const_type, char* expected)
{
	StringInfo result = makeStringInfo();
	char* value = NULL;
	Const* input = (Const*) palloc0(sizeof(Const));
	input->constisnull = is_null;
	input->consttype = const_type;

	/* need to prepare inner functions */
	if (!is_null)
	{
		value = strdup(const_value); /* will be free'd by const_to_str */

		mock__const_to_str(const_type, value);
	}

	/* no expected value means it's a negative test */
	if (expected)
	{
		run__const_to_str(input, result, expected);
	}
	else
	{
		run__const_to_str__negative(input, result, value);
		pfree(value); /* value was not freed by const_to_str b/c of failure */
	}

	pfree(result->data);
	pfree(result);
	pfree(input);
}

void run__const_to_str(Const* input, StringInfo result, char* expected)
{
	const_to_str(input, result);
	assert_string_equal(result->data, expected);
}

void run__const_to_str__negative(Const* input, StringInfo result, char* value)
{

	StringInfo err_msg = makeStringInfo();
	appendStringInfo(err_msg,
			"internal error in pxffilters.c:const_to_str. "
			"Using unsupported data type (%d) (value %s)", input->consttype, value);

	/* Setting the test -- code omitted -- */
	PG_TRY();
	{
		/* This will throw a ereport(ERROR).*/
		const_to_str(input, result);
	}
	PG_CATCH();
	{
		CurrentMemoryContext = 1;
		ErrorData *edata = CopyErrorData();

		/* Validate the type of expected error */
		assert_true(edata->sqlerrcode == ERRCODE_INTERNAL_ERROR);
		assert_true(edata->elevel == ERROR);
		assert_string_equal(edata->message, err_msg->data);

		pfree(err_msg->data);
		pfree(err_msg);

		return;
	}
	PG_END_TRY();

	assert_true(false);
}


void
test__const_to_str__null(void **state)
{
	verify__const_to_str(true, NULL, 1, "\"NULL\"");
}

void
test__const_to_str__int(void **state)
{
	verify__const_to_str(false, "1234", INT2OID, "1234");
	verify__const_to_str(false, "1234", INT4OID, "1234");
	verify__const_to_str(false, "1234", INT8OID, "1234");
	verify__const_to_str(false, "1.234", FLOAT4OID, "1.234");
	verify__const_to_str(false, "1.234", FLOAT8OID, "1.234");
	verify__const_to_str(false, "1234", NUMERICOID, "1234");
}

void
test__const_to_str__text(void **state)
{
	verify__const_to_str(false, "that", TEXTOID, "\\\"that\\\"");
	verify__const_to_str(false, "joke", VARCHAROID, "\\\"joke\\\"");
	verify__const_to_str(false, "isn't", BPCHAROID, "\\\"isn't\\\"");
	verify__const_to_str(false, "funny", CHAROID, "\\\"funny\\\"");
	verify__const_to_str(false, "anymore", BYTEAOID, "\\\"anymore\\\"");
}

void
test__const_to_str__boolean(void **state)
{
	verify__const_to_str(false, "t", BOOLOID, "\"true\"");
	verify__const_to_str(false, "f", BOOLOID, "\"false\"");
}

void
test__const_to_str__NegativeCircle(void **state)
{
	verify__const_to_str(false, "<3,3,9>", CIRCLEOID, NULL);
}

void
test__opexpr_to_pxffilter__null(void **state)
{
	PxfFilterDesc *filter = (PxfFilterDesc*) palloc0(sizeof(PxfFilterDesc));
	OpExpr *expr = (OpExpr*) palloc0(sizeof(OpExpr));

	assert_false(opexpr_to_pxffilter(NULL, NULL));
	assert_false(opexpr_to_pxffilter(NULL, filter));
	assert_false(opexpr_to_pxffilter(expr, NULL));

	expr->args = NIL;
	assert_false(opexpr_to_pxffilter(expr, filter));

	pfree(filter);
	pfree(expr);
}

void
test__opexpr_to_pxffilter__unary_expr(void **state)
{
	PxfFilterDesc *filter = (PxfFilterDesc*) palloc0(sizeof(PxfFilterDesc));
	OpExpr *expr = (OpExpr*) palloc0(sizeof(OpExpr));
	Var *arg = (Var*) palloc0(sizeof(Var));
	arg->xpr.type = T_Var;

	assert_false(opexpr_to_pxffilter(NULL, NULL));
	assert_false(opexpr_to_pxffilter(NULL, filter));
	assert_false(opexpr_to_pxffilter(expr, NULL));

	expr->args = NIL;
	expr->args = lappend(expr->args, arg);
	assert_false(opexpr_to_pxffilter(expr, filter));

	pfree(arg);
	pfree(filter);
	pfree(expr);
}

void
compare_filters(PxfFilterDesc* result, PxfFilterDesc* expected)
{
	assert_int_equal(result->l.opcode, expected->l.opcode);
	assert_int_equal(result->l.attnum, expected->l.attnum);
	if (expected->l.conststr)
		assert_string_equal(result->l.conststr->data, expected->l.conststr->data);
	else
		assert_true(result->l.conststr == NULL);

	assert_true(result->r.opcode == expected->r.opcode);
	assert_int_equal(result->r.attnum, expected->r.attnum);
	if (expected->r.conststr)
		assert_string_equal(result->r.conststr->data, expected->r.conststr->data);
	else
		assert_true(result->r.conststr == NULL);

	assert_int_equal(result->op, expected->op);
}

PxfFilterDesc* build_filter(char lopcode, int lattnum, char* lconststr,
							 char ropcode, int rattnum, char* rconststr,
							 PxfOperatorCode op)
{
	PxfFilterDesc *filter = (PxfFilterDesc*) palloc0(sizeof(PxfFilterDesc));

	filter->l.opcode = lopcode;
	filter->l.attnum = lattnum;
	if (lconststr)
	{
		filter->l.conststr = makeStringInfo();
		appendStringInfoString(filter->l.conststr, lconststr);
	}

	filter->r.opcode = ropcode;
	filter->r.attnum = rattnum;
	if (rconststr)
	{
		filter->r.conststr = makeStringInfo();
		appendStringInfoString(filter->r.conststr, rconststr);
	}

	filter->op = op;

	return filter;
}

Var* build_var(Oid oid, int attno) {
	Var *arg_var = (Var*) palloc0(sizeof(Var));
	arg_var->xpr.type = T_Var;
	arg_var->vartype = oid;
	arg_var->varattno = attno;
	return arg_var;
}

Const* build_const(Oid oid, char* value)
{
	Const* arg_const = (Const*) palloc0(sizeof(Const));
	arg_const->xpr.type = T_Const;
	arg_const->constisnull = (value == NULL);
	arg_const->consttype = oid;
	if (value != NULL)
	{
		mock__const_to_str(oid, value);
	}

	return arg_const;
}

OpExpr* build_op_expr(void* left, void* right, int op)
{
	OpExpr *expr = (OpExpr*) palloc0(sizeof(OpExpr));
	expr->args = NIL;
	expr->args = lappend(expr->args, left);
	expr->args = lappend(expr->args, right);

	expr->opno = op;
	expr->xpr.type = T_OpExpr;
	return expr;
}

void run__opexpr_to_pxffilter__positive(Oid dbop, PxfOperatorCode expectedPxfOp)
{
	PxfFilterDesc *filter = (PxfFilterDesc*) palloc0(sizeof(PxfFilterDesc));
	Var *arg_var = build_var(INT2OID, 1);
	char* const_value = strdup("1984"); /* will be free'd by const_to_str */
	Const* arg_const = build_const(INT2OID, const_value);

	OpExpr *expr = build_op_expr(arg_var, arg_const, dbop);
	PxfFilterDesc* expected = build_filter(
			PXF_ATTR_CODE, 1, NULL,
			PXF_CONST_CODE, 0, "1984",
			expectedPxfOp);

	/* run test */
	assert_true(opexpr_to_pxffilter(expr, filter));

	compare_filters(filter, expected);

	pxf_free_filter(expected);
	pxf_free_filter(filter);

	list_free_deep(expr->args); /* free all args */
	pfree(expr);
}

void
test__opexpr_to_pxffilter__intGT(void **state)
{
	run__opexpr_to_pxffilter__positive(520 /* int2gt */, PXFOP_GT);
}

void
test__opexpr_to_pxffilter__allSupportedTypes(void **state)
{
	int nargs = sizeof(pxf_supported_opr) / sizeof(dbop_pxfop_map);
	PxfOperatorCode pxfop = 0;
	Oid dbop = InvalidOid;

	for (int i = 0; i < nargs; ++i)
	{
		dbop = pxf_supported_opr[i].dbop;
		pxfop = pxf_supported_opr[i].pxfop;
		run__opexpr_to_pxffilter__positive(dbop, pxfop);
	}
}

/* NOTE: this test is not  a use case - when the query includes
 * 'is null' or 'is not null' the qualifier code is T_NullTest and not T_OpExpr */
void
test__opexpr_to_pxffilter__attributeIsNull(void **state)
{
	PxfFilterDesc *filter = (PxfFilterDesc*) palloc0(sizeof(PxfFilterDesc));
	Var *arg_var = build_var(INT2OID, 1);
	Const* arg_const = build_const(INT2OID, NULL);
	OpExpr *expr = build_op_expr(arg_var, arg_const, 94 /* int2eq */);

	PxfFilterDesc* expected = build_filter(
				PXF_ATTR_CODE, 1, NULL,
				PXF_CONST_CODE, 0, "\"NULL\"",
				PXFOP_EQ);

	/* run test */
	assert_true(opexpr_to_pxffilter(expr, filter));
	compare_filters(filter, expected);

	pxf_free_filter(filter);
	pxf_free_filter(expected);

	list_free_deep(expr->args); /* free all args */
	pfree(expr);
}

/*
 * Test for a query with different types.
 * Types pairing are not checked, it is covered by the
 * supported operations which are type specific.
 */
void
test__opexpr_to_pxffilter__differentTypes(void **state)
{
	PxfFilterDesc *filter = (PxfFilterDesc*) palloc0(sizeof(PxfFilterDesc));
	Var *arg_var = build_var(INT2OID, 3);
	char* const_value = strdup("13"); /* will be free'd by const_to_str */
	Const *arg_const = build_const(INT8OID, const_value);
	OpExpr *expr = build_op_expr(arg_const, arg_var, 1864 /* int28lt */);


	/* run test */
	assert_true(opexpr_to_pxffilter(expr, filter));
	PxfFilterDesc *expected = build_filter(
			PXF_CONST_CODE, 0, "13",
			PXF_ATTR_CODE, 3, NULL,
			PXFOP_LT);
	compare_filters(filter, expected);

	pxf_free_filter(filter);
	list_free_deep(expr->args); /* free all args */
	pfree(expr);
}

void
test__opexpr_to_pxffilter__unsupportedTypeCircle(void **state)
{
	PxfFilterDesc *filter = (PxfFilterDesc*) palloc0(sizeof(PxfFilterDesc));
	Var *arg_var = build_var(CIRCLEOID, 8);
	Const *arg_const = build_const(CIRCLEOID, NULL);
	OpExpr *expr = build_op_expr(arg_const, arg_var, 0 /* whatever */);

	/* run test */
	assert_false(opexpr_to_pxffilter(expr, filter));

	pxf_free_filter(filter);

	list_free_deep(expr->args); /* free all args */
	pfree(expr);
}

void
test__opexpr_to_pxffilter__twoVars(void **state)
{
	PxfFilterDesc *filter = (PxfFilterDesc*) palloc0(sizeof(PxfFilterDesc));
	Var *arg_var_left = build_var(INT4OID, 8);
	Var *arg_var_right = build_var(INT4OID, 9);
	OpExpr *expr = build_op_expr(arg_var_left, arg_var_right, 0 /* whatever */);

	/* run test */
	assert_false(opexpr_to_pxffilter(expr, filter));

	pxf_free_filter(filter);

	list_free_deep(expr->args); /* free all args */
	pfree(expr);
}

void
test__opexpr_to_pxffilter__unsupportedOpNot(void **state)
{
	PxfFilterDesc *filter = (PxfFilterDesc*) palloc0(sizeof(PxfFilterDesc));
	Var *arg_var = build_var(INT2OID, 3);
	char* const_value = strdup("not"); /* will be free'd by const_to_str */
	Const *arg_const = build_const(INT2OID, const_value);
	OpExpr *expr = build_op_expr(arg_const, arg_var, 1877 /* int2not */);

	/* run test */
	assert_false(opexpr_to_pxffilter(expr, filter));

	pxf_free_filter(filter);

	list_free_deep(expr->args); /* free all args */
	pfree(expr);
}

void
test__pxf_serialize_filter_list__oneFilter(void **state)
{
	List* filter_list = NIL;

	PxfFilterDesc* filter = build_filter(
			PXF_ATTR_CODE, 1, NULL,
			PXF_CONST_CODE, 0, "1984",
			PXFOP_GT);
	filter_list = lappend(filter_list, filter);

	char* result = pxf_serialize_filter_list(filter_list);
	assert_string_equal(result, "a0c1984o2");

	pxf_free_filter_list(filter_list);
	filter_list = NIL;
	pfree(result);

	filter = build_filter(
			PXF_ATTR_CODE, 8, NULL,
			PXF_CONST_CODE, 0, "\"George Orwell\"",
			PXFOP_EQ);
	filter_list = lappend(filter_list, filter);

	result = pxf_serialize_filter_list(filter_list);
	assert_string_equal(result, "a7c\"George Orwell\"o5");

	pxf_free_filter_list(filter_list);
	pfree(result);
}

void
test__pxf_serialize_filter_list__manyFilters(void **state)
{
	char* result = NULL;
	List* filter_list = NIL;

	PxfFilterDesc* filter1 = build_filter(
			PXF_ATTR_CODE, 2, NULL,
			PXF_CONST_CODE, 0, "1983",
			PXFOP_GT);
	PxfFilterDesc* filter2 = build_filter(
			PXF_ATTR_CODE, 3, NULL,
			PXF_CONST_CODE, 0, "1985",
			PXFOP_LT);
	PxfFilterDesc* filter3 = build_filter(
			PXF_ATTR_CODE, 4, NULL,
			PXF_CONST_CODE, 0, "\"George Orwell\"",
			PXFOP_EQ);
	PxfFilterDesc* filter4 = build_filter(
			PXF_ATTR_CODE, 5, NULL,
			PXF_CONST_CODE, 0, "\"Winston\"",
			PXFOP_GE);

	filter_list = lappend(filter_list, filter1);
	filter_list = lappend(filter_list, filter2);

	result = pxf_serialize_filter_list(filter_list);
	assert_string_equal(result, "a1c1983o2a2c1985o1o7");
	pfree(result);

	filter_list = lappend(filter_list, filter3);

	result = pxf_serialize_filter_list(filter_list);
	assert_string_equal(result, "a1c1983o2a2c1985o1o7a3c\"George Orwell\"o5o7");
	pfree(result);

	filter_list = lappend(filter_list, filter4);

	result = pxf_serialize_filter_list(filter_list);
	assert_string_equal(result, "a1c1983o2a2c1985o1o7a3c\"George Orwell\"o5o7a4c\"Winston\"o4o7");
	pfree(result);

	pxf_free_filter_list(filter_list);
	filter_list = NIL;
}

int 
main(int argc, char* argv[]) 
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__supported_filter_type),
			unit_test(test__const_to_str__null),
			unit_test(test__const_to_str__int),
			unit_test(test__const_to_str__text),
			unit_test(test__const_to_str__boolean),
			unit_test(test__const_to_str__NegativeCircle),
			unit_test(test__opexpr_to_pxffilter__null),
			unit_test(test__opexpr_to_pxffilter__unary_expr),
			unit_test(test__opexpr_to_pxffilter__intGT),
			unit_test(test__opexpr_to_pxffilter__allSupportedTypes),
			unit_test(test__opexpr_to_pxffilter__attributeIsNull),
			unit_test(test__opexpr_to_pxffilter__differentTypes),
			unit_test(test__opexpr_to_pxffilter__unsupportedTypeCircle),
			unit_test(test__opexpr_to_pxffilter__twoVars),
			unit_test(test__opexpr_to_pxffilter__unsupportedOpNot),
			unit_test(test__pxf_serialize_filter_list__oneFilter),
			unit_test(test__pxf_serialize_filter_list__manyFilters)
	};
	return run_tests(tests);
}
