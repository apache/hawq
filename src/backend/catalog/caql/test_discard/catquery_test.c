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

#include <stdio.h>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "postgres.h"
#include "catquery_mock.c"
#include "postmaster/identity.h"


bool
catalog_check_helper(cqContext *pCtx, HeapTuple tuple)
{
	bool ret = false;
	PG_TRY();
	{
		disable_catalog_check(pCtx, tuple);
	}
	PG_CATCH();
	{
		ret = true;
	}
	PG_END_TRY();
	return ret;
}

/*
 * Tests
 */
void
test__is_builtin_object__oid(void **state)
{
	HeapTuple ht = build_pg_class_tuple();

	HeapTupleSetOid(ht, 1000);
	assert_true(is_builtin_object(NULL, ht));

	HeapTupleSetOid(ht, 17000);
	assert_false(is_builtin_object(NULL, ht));
}

void
test__is_builtin_object__non_oid(void **state)
{
	cqContext ctx;
	ctx.cq_relationId = AttributeRelationId;
	HeapTuple ht = build_pg_attribute_tuple(1255);

	assert_true(is_builtin_object(&ctx, ht));

	ht = build_pg_attribute_tuple(17000);
	assert_false(is_builtin_object(&ctx, ht));
}

void
test__disable_catalog_check__false(void **state)
{
	cqContext ctx;
	HeapTuple ht;

	ctx.cq_relationId = AttributeRelationId;
	ht = build_pg_attribute_tuple(18000);

	Gp_role = GP_ROLE_DISPATCH;
	UnitTestSetSegmentIdToMaster();
	assert_false(catalog_check_helper(&ctx, ht));

	Gp_role = GP_ROLE_EXECUTE;
	UnitTestSetSegmentIdToSegment();
	gp_disable_catalog_access_on_segment = false;
	assert_false(catalog_check_helper(&ctx, ht));
}

void
test__disable_catalog_check__true(void **state)
{
	cqContext ctx;
	HeapTuple ht = build_pg_class_tuple();
	HeapTupleSetOid(ht, 18000);

	Gp_role = GP_ROLE_EXECUTE;
	UnitTestSetSegmentIdToSegment();
	gp_disable_catalog_access_on_segment = true;

	assert_true(catalog_check_helper(&ctx, ht));
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);
	const UnitTest tests[] =
	{
		unit_test(test__is_builtin_object__oid),
		unit_test(test__is_builtin_object__non_oid),
		unit_test(test__disable_catalog_check__false),
		unit_test(test__disable_catalog_check__true),
	};
	
	return run_tests(tests);
}

