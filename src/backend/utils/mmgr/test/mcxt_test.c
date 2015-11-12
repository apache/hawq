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
#include "nodes/nodes.h"
#include "../mcxt.c"
#include "utils/memaccounting.h"

/*
 * Checks if MemoryContextInit() calls MemoryAccounting_Reset()
 */
void 
test__MemoryContextInit__CallsMemoryAccountingReset(void **state)
{
	will_be_called(MemoryAccounting_Reset);
	MemoryContextInit();
}

int 
main(int argc, char* argv[]) 
{
        cmockery_parse_arguments(argc, argv);

        const UnitTest tests[] = {
        		unit_test(test__MemoryContextInit__CallsMemoryAccountingReset)

        };
        return run_tests(tests);
}

