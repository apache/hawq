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
#include "storage/buffile.h"
#include "storage/bfz.h"
#include "executor/execWorkfile.h"
#include "utils/memutils.h"

/* Ignore elog */
#include "utils/elog.h"

#undef elog
#define elog

/* Provide specialized mock implementations for memory allocation functions */
#undef palloc0
#define palloc0 execWorkfile_palloc0_mock
void *execWorkfile_palloc0_mock(Size size);

#undef pstrdup
#define pstrdup execWorkfile_pstrdup_mock
char *execWorkfile_pstrdup_mock(const char *string);

#include "../execWorkfile.c"

/*
 * This is a mocked version of palloc0 to be used in ExecWorkFile_Create.
 *   It asserts that it is executed in the TopMemoryContext.
 */
void *
execWorkfile_palloc0_mock(Size size)
{
	assert_int_equal(CurrentMemoryContext, TopMemoryContext);
	return MemoryContextAllocZero(CurrentMemoryContext, size);
}

/*
 * This is a mocked version of pstrdup to be used in ExecWorkFile_Create.
 *   It asserts that it is executed in the TopMemoryContext.
 */
char *execWorkfile_pstrdup_mock(const char *string)
{
	assert_int_equal(CurrentMemoryContext, TopMemoryContext);
	return MemoryContextStrdup(CurrentMemoryContext, string);
}


/* ==================== ExecWorkFile_Create ==================== */
/*
 * Test that the ExecWorkFile struct is allocated in TopMemoryContext
 */
void
test__ExecWorkFile_Create__InTopMemContext(void **state)
{

	char *test_filename = "foo";

	will_return(WorkfileQueryspace_AddWorkfile, true);

	expect_value(BufFileCreateFile, fileName, test_filename);
	expect_value(BufFileCreateFile, delOnClose, true);
	expect_value(BufFileCreateFile, interXact, false);
	will_return(BufFileCreateFile, NULL);

	expect_value(BufFileSetWorkfile, buffile, NULL);
	will_be_called(BufFileSetWorkfile);

	/*
	 * All the memory context stuff is mocked, so the TopMemoryContext is NULL
	 * at this point. Set it to something specific so we can distinguish it from
	 * the CurrentMemoryContext.
	 */
	TopMemoryContext = (MemoryContext) 0xdeadbeef;
	CurrentMemoryContext = (MemoryContext) 0xfeadbead;

	/*
	 * ExecWorkFile_Create will call our mocked palloc0 function execWorkfile__palloc0_mock
	 * and our mocked pstrdup function execWorkfile_pstrdup_mock.
	 * These functions will assert that the allocation of the result happens
	 * in the TopMemoryContext.
	 */
	ExecWorkFile *ewf = ExecWorkFile_Create(test_filename, BUFFILE, true /* delOnClose */, 0 /* compressType */);

}

/* ==================== main ==================== */
int
main(int argc, char* argv[])
{
    cmockery_parse_arguments(argc, argv);

    const UnitTest tests[] = {
                                 unit_test(test__ExecWorkFile_Create__InTopMemContext)
                             };

    return run_tests(tests);
}
