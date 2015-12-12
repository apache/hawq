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
#include "../guc.c"

#define assert_null(c) assert_true(c == NULL)
#define assert_not_null(c) assert_true(c != NULL)

#define VARLEN(d) VARSIZE(d)-VARHDRSZ

/* Values for text datum */
#define TEXT_TYPLEN		-1	
#define TEXT_TYPBYVAL	false
#define TEXT_TYPALIGN	'i'

/* Helper function */
ArrayType *create_guc_array(List *guc_list, int elems);
ArrayType *create_md_guc_array(List *guc_list, int elems, int ndims);
Datum *create_guc_datum_array(List *guc_list, int num);

/*
 * Test set_config_option
 */
void
test__set_config_option(void **state) 
{
	build_guc_variables();

	bool ret;
	ret = set_config_option("password_encryption", "off", PGC_POSTMASTER, PGC_S_SESSION, false, false);
	assert_true(ret);
	ret = set_config_option("gp_disable_catalog_access_on_segment", "on", PGC_POSTMASTER, PGC_S_SESSION, false, false);
	assert_true(ret);
}

/*
 * Test find_option
 */
void
test__find_option(void **state) 
{
	build_guc_variables();

	struct config_generic *config;
	config = find_option("unknown_name", LOG);
	assert_null(config);

	config = find_option("password_encryption", LOG);
	assert_not_null(config);
	config = find_option("non_exist_guc", LOG);
	assert_null(config);

	/* supported obsolete guc name */
	config = find_option("work_mem", LOG);
	assert_not_null(config);
}


/*
 * Helper function
 */
Datum *
create_guc_datum_array(List *guc_list, int num)
{
	Datum      *darray;
	ListCell   *item;
	int         i;

	darray = (Datum *) palloc0(num * sizeof(Datum));

	i = 0;
	foreach(item, guc_list)
		darray[i++] = CStringGetTextDatum((char *) lfirst(item));

	return darray;
}

ArrayType *
create_guc_array(List *guc_list, int elems)
{
	ArrayType  *array;
	Datum 	   *darray;

	darray = create_guc_datum_array(guc_list, elems);
	array = construct_array(darray, elems, TEXTOID, TEXT_TYPLEN, TEXT_TYPBYVAL, TEXT_TYPALIGN);

	pfree(darray);
	return array;
}

ArrayType *
create_md_guc_array(List *guc_list, int elems, int ndims)
{
	ArrayType  *array;
	Datum	   *darray;
	int			dims[ndims];
	int			lbs[1];
	
	darray = create_guc_datum_array(guc_list, elems * ndims);

	dims[0] = elems;
	dims[1] = elems;
	lbs[0] = 1;
	array = construct_md_array(darray, NULL, ndims, dims, lbs,
							   TEXTOID, TEXT_TYPLEN, TEXT_TYPBYVAL, TEXT_TYPALIGN);
	pfree(darray);
	return array;
}


int 
main(int argc, char* argv[]) 
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__set_config_option),
			unit_test(test__find_option)
	};
	return run_tests(tests);
}

