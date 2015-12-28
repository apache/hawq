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
#include "postgres.h"
#include "../define.c"

/* ==================== FreeStrFromDefGetString ==================== */

/*
 * Tests that need_free flag correctly set in defGetString method.
 */
void
test__DefGetString_NeedFreeFlag(void **state)
{
	bool need_free = false;
	char *value = NULL;

	/* case: T_String expected value: false */
	need_free = true;
	DefElem *e1 = makeDefElem("def_string", (Node *) makeString("none"));
	value = defGetString(e1, &need_free);
	assert_false(need_free);

	/* case: T_Integer expected value: true */
	need_free = false;
	DefElem *e2 = makeDefElem("def_int", (Node *) makeInteger(0));
	value = defGetString(e2, &need_free);
	assert_true(need_free);

	/* case: T_Float expected value: false */
	need_free = true;
	DefElem *e3 = makeDefElem("def_float", (Node *) makeFloat("3.14"));
	value = defGetString(e3, &need_free);
	assert_false(need_free);

	/* case: T_TypeName expected value: true */
	need_free = false;
	TypeName   *tName = makeNode(TypeName);
	tName->names = list_make2(makeString("pg_catalog"), makeString("unknown"));
	tName->typmod = -1;
	tName->location = -1;
	DefElem *e4 = makeDefElem("def_typename", (Node *) tName);
	value = defGetString(e4, &need_free);
	assert_true(need_free);

	/* case: T_List expected value: true */
	need_free = false;
	List *list = NIL;
	list = lappend(list, makeString("str1"));
	list = lappend(list, makeString("str2"));
	DefElem *e5 = makeDefElem("def_list", (Node *) list);
	value = defGetString(e5, &need_free);
	assert_true(need_free);

}

/* ==================== main ==================== */
int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__DefGetString_NeedFreeFlag)
	};
	return run_tests(tests);
}

