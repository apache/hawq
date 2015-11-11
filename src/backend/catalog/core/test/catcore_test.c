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

#include "postgres.h"
#include "catalog/catcore.h"

#include "../catcore.c"


/*
 * CatCoreRelations should be ordered by relname because we use bsearch.
 */
void
test__catcore_relation_ordered(void **state)
{
	int			i;
	const char *prev_relname;

	prev_relname = CatCoreRelations[0].relname;
	for (i = 1; i < CatCoreRelationSize; i++)
	{
		const CatCoreRelation	   *relation;

		relation = &CatCoreRelations[i];
		assert_true(strcmp(prev_relname, relation->relname) < 0);

		prev_relname = relation->relname;
	}
}

/*
 * Index nkeys should be <= MAX_SCAN_NUM
 */
void
test__catcore_index_max_scan_num(void **state)
{
	int			i, j;

	for (i = 0; i < CatCoreRelationSize; i++)
	{
		const CatCoreRelation	   *relation;

		relation = &CatCoreRelations[i];
		for (j = 0; j < relation->nindexes; j++)
		{
			const CatCoreIndex	   *index;

			index = &relation->indexes[j];
			assert_true(index->nkeys <= MAX_SCAN_NUM);
		}
	}
}

/*
 * FKey nkeys should be <= MAX_SCAN_NUM
 */
void
test__catcore_fkey_max_scan_num(void **state)
{
	int			i, j;

	for (i = 0; i < CatCoreRelationSize; i++)
	{
		const CatCoreRelation	   *relation;

		relation = &CatCoreRelations[i];
		for (j = 0; j < relation->nfkeys; j++)
		{
			const CatCoreFKey	   *fkey;

			fkey = &relation->fkeys[j];
			assert_true(fkey->nkeys <= MAX_SCAN_NUM);
		}
	}
}

/*
 * OID column should be found if available.
 */
void
test__catcore_oid_attr(void **state)
{
	const CatCoreRelation  *relation;
	const CatCoreAttr	   *attr;

	/* for relations with oid column, returns special attribute */
	relation = catcore_lookup_rel("pg_class");
	attr = catcore_lookup_attr(relation, "oid");
	assert_true(attr != NULL);
	assert_true(attr->atttyp->typid == OIDOID);
	assert_true(attr->attnum == ObjectIdAttributeNumber);

	/* for relations without oid column, returns NULL */
	relation = catcore_lookup_rel("gp_distribution_policy");
	attr = catcore_lookup_attr(relation, "oid");
	assert_true(attr == NULL);
}

/*
 * Test for catcore_lookup_attnum().
 */
void
test__catcore_lookup_attnum(void **state)
{
	const CatCoreRelation  *relation;
	AttrNumber				attnum;

	relation = catcore_lookup_rel("pg_type");
	attnum = catcore_lookup_attnum(relation, "typname");
	assert_int_equal(attnum, 1);

	attnum = catcore_lookup_attnum(relation, "nonexistent");

	assert_int_equal(attnum, InvalidOid);
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__catcore_relation_ordered),
			unit_test(test__catcore_index_max_scan_num),
			unit_test(test__catcore_fkey_max_scan_num),
			unit_test(test__catcore_oid_attr),
			unit_test(test__catcore_lookup_attnum),
	};
	return run_tests(tests);
}
