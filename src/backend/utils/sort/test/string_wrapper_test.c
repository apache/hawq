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
#include "utils/string_wrapper.h"

#define STRXFRM_INPUT_LENGTH_LIMIT (50)

/* Test conversion of a long string (larger than STRXFRM_INPUT_LENGTH_LIMIT)*/
void
test__gp_strxfrm__LongQuery(void **state)
{

	char *query = "select v from \
						(select Ta.environment as v, row_number() over (order by Ta.environment) as r \
							from only pg_temp_10.pg_analyze_50600_3 as Ta \
							where environment is not null \
							union \
						select max(Tb.environment) as v, 1 as r \
							from only pg_temp_10.pg_analyze_50600_3 as Tb \
							where environment is not null) as foo \
					where r % 894 = 1 \
					group by v \
					order by v";

	int len = strlen(query);
	
	char *tmp_dst = (char *)palloc(len*(char) + 1);

	size_t result = gp_strxfrm(tmp_dst, query, STRXFRM_INPUT_LENGTH_LIMIT + 1);

	assert_int_equal((int)result, len*sizeof(char));
	assert_int_equal(STRXFRM_INPUT_LENGTH_LIMIT, strlen(tmp_dst));
}

int
main(int argc, char* argv[]) {
        cmockery_parse_arguments(argc, argv);

        const UnitTest tests[] = {
                        unit_test(test__gp_strxfrm__LongQuery)
        };
        return run_tests(tests);
}

