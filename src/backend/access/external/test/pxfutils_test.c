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
#include "../pxfutils.c"

void
test_negative_dfs_url_to_address(char* input, char* err_msg);

void
test__dfs_url_to_address(void **state)
{
	char* address;
	dfs_url_to_address("1.2.3.4:1234/4321", &address);
	assert_string_equal(address, "1.2.3.4:1234");
	pfree(address);
}

void
test__dfs_url_to_address__nameservice(void **state)
{
	char* address;
	dfs_url_to_address("phdcluster/4321", &address);
	assert_string_equal(address, "phdcluster");
	pfree(address);
}

void
test__dfs_url_to_address__negative_empty(void **state)
{
	test_negative_dfs_url_to_address("",
			"dfs_url needs to be of the form host:port/path. Configured value is empty");
}

void
test__dfs_url_to_address__negative_null(void **state)
{
	test_negative_dfs_url_to_address(NULL,
			"dfs_url needs to be of the form host:port/path. Configured value is empty");
}

void
test_negative_dfs_url_to_address(char* input, char* err_msg)
{
	char* address;
	/* Setting the test -- code omitted -- */
	PG_TRY();
	{
		/* This will throw a elog(ERROR).*/
		dfs_url_to_address(input, &address);
	}
	PG_CATCH();
	{
		CurrentMemoryContext = 1;
		ErrorData *edata = CopyErrorData();

		assert_true(edata->elevel == ERROR);
		assert_string_equal(edata->message,
				err_msg);
		return;
	}
	PG_END_TRY();
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__dfs_url_to_address),
			unit_test(test__dfs_url_to_address__nameservice),
			unit_test(test__dfs_url_to_address__negative_empty),
			unit_test(test__dfs_url_to_address__negative_null),
	};
	return run_tests(tests);
}
