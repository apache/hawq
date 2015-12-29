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
#include "../tupdesc.c"

void
test__ReleaseTupleDesc__no_ref_count(void **state)
{
	TupleDesc td = CreateTemplateTupleDesc(2, true);

	td->tdrefcount = -1;

	/* should not do anything */
	ReleaseTupleDesc(td);

	assert_int_equal(-1, td->tdrefcount);

	td->tdrefcount = 0;

	/* should not do anything */
	ReleaseTupleDesc(td);

	assert_int_equal(0, td->tdrefcount);

	pfree(td);
}

void
test__ReleaseTupleDesc__ref_count(void **state)
{
	TupleDesc td = CreateTemplateTupleDesc(2, true);

	td->tdrefcount = 3;

	expect_any(ResourceOwnerForgetTupleDesc, owner);
	expect_value(ResourceOwnerForgetTupleDesc, tupdesc, td);
	will_be_called(ResourceOwnerForgetTupleDesc);

	/* should decrement refcount but not free */
	ReleaseTupleDesc(td);

	assert_int_equal(2, td->tdrefcount);

	pfree(td);
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__ReleaseTupleDesc__no_ref_count),
			unit_test(test__ReleaseTupleDesc__ref_count)
	};
	return run_tests(tests);
}
