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
#include "../list.c"


void
list_reverse_ints__sizeX(int listSize);


void
test__list_reverse_ints__size1(void **state)
{
	list_reverse_ints__sizeX(1);
}

void
test__list_reverse_ints__size2(void **state)
{
	list_reverse_ints__sizeX(2);
}

void
test__list_reverse_ints__size3(void **state)
{
	list_reverse_ints__sizeX(3);
}

void
test__list_reverse_ints__size10(void **state)
{
	list_reverse_ints__sizeX(10);
}

void
test__list_reverse_ints__size15(void **state)
{
	list_reverse_ints__sizeX(15);
}

void
test__list_reverse_ints__empty(void **state)
{
	list_reverse_ints__sizeX(0);
}

void
list_reverse_ints__sizeX(int listSize)
{
	List* list = NIL;
	List* reverseList = NIL;

	for (int i = 0; i < listSize; i++)
	{
		list = lappend_int(list, i);
	}

	/* sanity */
	assert_int_equal(listSize, list_length(list));
	for (int i = 0; i < listSize; i++)
	{
		assert_int_equal(i, list_nth_int(list, i));
	}

	reverseList = list_reverse_ints(list);

	assert_int_equal(listSize, list_length(reverseList));
	for (int i = 0; i < listSize; i++)
	{
		assert_int_equal(listSize - 1 - i, list_nth_int(reverseList, i));
	}

	list_free(list);
	list_free(reverseList);
}

int		
main(int argc, char* argv[]) {
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__list_reverse_ints__size1),
			unit_test(test__list_reverse_ints__size2),
			unit_test(test__list_reverse_ints__size3),
			unit_test(test__list_reverse_ints__size10),
			unit_test(test__list_reverse_ints__size15),
			unit_test(test__list_reverse_ints__empty),
	};
	return run_tests(tests);
}
