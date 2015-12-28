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

#include "../execAmi.c"

/* ==================== ExecEagerFree ==================== */
/*
 * Tests that ExecEageFree calls the new ExecEagerFreeShareInputScan
 * function when the input is a ShareInputScanState
 */
void
test__ExecEagerFree_ExecEagerFreeShareInputScan(void **state)
{
	ShareInputScanState *sisc = makeNode(ShareInputScanState);

	expect_value(ExecEagerFreeShareInputScan, node, sisc);
	will_be_called(ExecEagerFreeShareInputScan);

	ExecEagerFree(sisc);
}

int
main(int argc, char* argv[])
{
        cmockery_parse_arguments(argc, argv);

        const UnitTest tests[] = {
                        unit_test(test__ExecEagerFree_ExecEagerFreeShareInputScan)
        };
        return run_tests(tests);
}
