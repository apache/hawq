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
#include "../aomd.c"


void 
test__AOSegmentFilePathNameLen(void **state) 
{
	RelationData reldata;
	char* basepath = "base/21381/123";

	expect_any(relpath, &rnode);
	will_return(relpath, strdup(basepath));

	int r = AOSegmentFilePathNameLen(&reldata);

	assert_in_range(r, strlen(basepath) + 3, strlen(basepath) + 10);
}

void 
test__FormatAOSegmentFileName(void **state) 
{
#ifdef FIX_UNIT_TEST
	char* basepath = "base/21381/123";
	int32 fileSegNo;
	char filepathname[256];

	// seg 0, no columns
	FormatAOSegmentFileName(basepath, 0, -1, &fileSegNo, filepathname);
	assert_string_equal(filepathname, "base/21381/123");
	assert_int_equal(fileSegNo, 0);

	// seg 1, no columns
	FormatAOSegmentFileName(basepath, 1, -1, &fileSegNo, filepathname);
	assert_string_equal(filepathname, "base/21381/123.1");
	assert_int_equal(fileSegNo, 1);

	// seg 0, column 1
	FormatAOSegmentFileName(basepath, 0, 1, &fileSegNo, filepathname);
	assert_string_equal(filepathname, "base/21381/123.128");
	assert_int_equal(fileSegNo, 128);

	// seg 1, column 1
	FormatAOSegmentFileName(basepath, 1, 1, &fileSegNo, filepathname);
	assert_string_equal(filepathname, "base/21381/123.129");
	assert_int_equal(fileSegNo, 129);

	// seg 0, column 2
	FormatAOSegmentFileName(basepath, 0, 2, &fileSegNo, filepathname);
	assert_string_equal(filepathname, "base/21381/123.256");
	assert_int_equal(fileSegNo, 256);
#endif /* FIX_UNIT_TEST */
}


void 
test__MakeAOSegmentFileName(void **state) 
{
#ifdef FIX_UNIT_TEST
	char* basepath = "base/21381/123";
	int32 fileSegNo;
	char filepathname[256];
	RelationData reldata;

	expect_any_count(relpath, &rnode, -1);

	// seg 0, no columns
	will_return(relpath, strdup(basepath));
	MakeAOSegmentFileName(&reldata, 0, -1, &fileSegNo, filepathname);
	assert_string_equal(filepathname, "base/21381/123");
	assert_int_equal(fileSegNo, 0);

	// seg 1, no columns
	will_return(relpath, strdup(basepath));
	MakeAOSegmentFileName(&reldata, 1, -1, &fileSegNo, filepathname);
	assert_string_equal(filepathname, "base/21381/123.1");
	assert_int_equal(fileSegNo, 1);

	// seg 0, column 1
	will_return(relpath, strdup(basepath));
	MakeAOSegmentFileName(&reldata, 0, 1, &fileSegNo, filepathname);
	assert_string_equal(filepathname, "base/21381/123.128");
	assert_int_equal(fileSegNo, 128);

	// seg 1, column 1
	will_return(relpath, strdup(basepath));
	MakeAOSegmentFileName(&reldata, 1, 1, &fileSegNo, filepathname);
	assert_string_equal(filepathname, "base/21381/123.129");
	assert_int_equal(fileSegNo, 129);

	// seg 0, column 2
	will_return(relpath, strdup(basepath));
	MakeAOSegmentFileName(&reldata, 0, 2, &fileSegNo, filepathname);
	assert_string_equal(filepathname, "base/21381/123.256");
	assert_int_equal(fileSegNo, 256);
#endif /* FIX_UNIT_TEST */
}


int 
main(int argc, char* argv[]) 
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__AOSegmentFilePathNameLen),
			unit_test(test__FormatAOSegmentFileName),
			unit_test(test__MakeAOSegmentFileName)
	};
	return run_tests(tests);
}
