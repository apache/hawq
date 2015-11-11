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
#include "hd_work_mgr_mock.h"


/*
 * check element list_index in segmenet_list
 * has the expected hostip.
 */
void check_segment_info(List* segment_list, int list_index,
						const char* expected_hostip)
{

	CdbComponentDatabaseInfo* seg_info =
			(CdbComponentDatabaseInfo*)lfirst(list_nth_cell(segment_list, list_index));
	assert_string_equal(seg_info->hostip, expected_hostip);
}

/*
 * Test clustering of segments to hosts.
 * Environment: 10 segments over 3 hosts, all primary.
 */
void 
test__do_segment_clustering_by_host__10SegmentsOn3Hosts(void **state)
{
	List* groups = NIL;
	ListCell* cell = NULL;
	GpHost* gphost = NULL;
	List* segs = NIL;
	CdbComponentDatabaseInfo* seg_info = NULL;

	char* array_of_segs[10] =
		{"1.2.3.1", "1.2.3.1", "1.2.3.1", "1.2.3.1",
		 "1.2.3.2", "1.2.3.2", "1.2.3.2",
		 "1.2.3.3", "1.2.3.3", "1.2.3.3"
	};
	bool array_of_primaries[10] =
	{
		true, true, true, true,
		true, true, true,
		true, true, true
	};
	int number_of_segments = 10;
	/* sanity */
	assert_true(number_of_segments == (sizeof(array_of_segs) / sizeof(array_of_segs[0])));
	assert_true(number_of_segments == (sizeof(array_of_primaries) / sizeof(array_of_primaries[0])));

	buildCdbComponentDatabases(number_of_segments, array_of_segs, array_of_primaries);

	CdbComponentDatabases *cdb = GpAliveSegmentsInfo.cdbComponentDatabases;

	/* sanity for cdbComponentDatabases building*/
	assert_int_equal(cdb->total_segment_dbs, number_of_segments);
	assert_string_equal(cdb->segment_db_info[4].hostip, array_of_segs[4]);

	/* build QueryResource */
	buildQueryResource(10, array_of_segs);
	will_return(GetActiveQueryResource, resource);

	/* test do_segment_clustering_by_host */
	groups = do_segment_clustering_by_host();

	assert_int_equal(list_length(groups), 3);

	cell = list_nth_cell(groups, 0);
	gphost = (GpHost*)lfirst(cell);
	assert_string_equal(gphost->ip, array_of_segs[0]);
	assert_int_equal(list_length(gphost->segs), 4);
    for (int i = 0; i < 4; ++i)
	{
		check_segment_info(gphost->segs, i, "1.2.3.1");
	}

	cell = list_nth_cell(groups, 1);
	gphost = (GpHost*)lfirst(cell);
	assert_string_equal(gphost->ip, "1.2.3.2");
	assert_int_equal(list_length(gphost->segs), 3);
	for (int i = 0; i < 3; ++i)
	{
		check_segment_info(gphost->segs, i, "1.2.3.2");
	}

	cell = list_nth_cell(groups, 2);
	gphost = (GpHost*)lfirst(cell);
	assert_string_equal(gphost->ip, "1.2.3.3");
	assert_int_equal(list_length(gphost->segs), 3);
	for (int i = 0; i < 3; ++i)
	{
		check_segment_info(gphost->segs, i, "1.2.3.3");
	}

	freeQueryResource();
	restoreCdbComponentDatabases();
}

