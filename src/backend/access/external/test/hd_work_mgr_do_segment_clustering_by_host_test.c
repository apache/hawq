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
 * has the expected hostip and segindex.
 */
void check_segment_info(List* segment_list, int list_index,
						const char* expected_hostip,
						int expected_segindex)
{

	Segment* seg_info =
			(Segment*)(list_nth(segment_list, list_index));
	assert_string_equal(seg_info->hostip, expected_hostip);
	assert_int_equal(seg_info->segindex, expected_segindex);
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

	char* array_of_segs[10] =
		{"1.2.3.1", "1.2.3.1", "1.2.3.1", "1.2.3.1",
		 "1.2.3.2", "1.2.3.2", "1.2.3.2",
		 "1.2.3.3", "1.2.3.3", "1.2.3.3"
	};
	int number_of_segments = 10;
	/* sanity */
	assert_true(number_of_segments == (sizeof(array_of_segs) / sizeof(array_of_segs[0])));

	/* build QueryResource */
	buildQueryResource(10, array_of_segs);
	/* sanity for QueryResource building*/
	assert_int_equal(resource->numSegments, 10);
	assert_string_equal(((Segment*)list_nth(resource->segments, 4))->hostip, array_of_segs[4]);
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
		check_segment_info(gphost->segs, i, "1.2.3.1", i);
	}

	cell = list_nth_cell(groups, 1);
	gphost = (GpHost*)lfirst(cell);
	assert_string_equal(gphost->ip, "1.2.3.2");
	assert_int_equal(list_length(gphost->segs), 3);
	for (int i = 0; i < 3; ++i)
	{
		check_segment_info(gphost->segs, i, "1.2.3.2", i+4);
	}

	cell = list_nth_cell(groups, 2);
	gphost = (GpHost*)lfirst(cell);
	assert_string_equal(gphost->ip, "1.2.3.3");
	assert_int_equal(list_length(gphost->segs), 3);
	for (int i = 0; i < 3; ++i)
	{
		check_segment_info(gphost->segs, i, "1.2.3.3", i+7);
	}

	freeQueryResource();
}

/*
 * Test clustering of segments to hosts.
 * Environment: 10 segments over 3 hosts, some of them mirrors.
 */
void
test__do_segment_clustering_by_host__10SegmentsOn3HostsWithMirrors(void **state)
{
	List* groups = NIL;
	ListCell* cell = NULL;
	GpHost* gphost = NULL;
	List* segs = NIL;
	CdbComponentDatabaseInfo* seg_info = NULL;
	CdbComponentDatabases *cdb = NULL;

	char* array_of_segs[10] =
	{
		"1.2.3.1", "1.2.3.1", "1.2.3.1",
		"1.2.3.2", "1.2.3.2", "1.2.3.2",
		"1.2.3.3", "1.2.3.3", "1.2.3.3",
		"1.2.3.1" /* another segment on the first host */
	};
	bool array_of_primaries[10] =
	{
		true, false, true,
		true, true, true,
		true, true, false,
		true
	};
	int number_of_segments = 10;
	/* sanity */
	assert_true(number_of_segments == (sizeof(array_of_segs) / sizeof(array_of_segs[0])));
	assert_true(number_of_segments == (sizeof(array_of_primaries) / sizeof(array_of_primaries[0])));

	int array_for_host1[3] = {0, 2, 9};
	int array_for_host2[3] = {3, 4, 5};
	int array_for_host3[2] = {6, 7};

	buildCdbComponentDatabases(number_of_segments, array_of_segs, array_of_primaries);

	cdb = GpAliveSegmentsInfo.cdbComponentDatabases;

	/* sanity for cdbComponentDatabases building*/
	assert_int_equal(cdb->total_segment_dbs, number_of_segments);
	assert_string_equal(cdb->segment_db_info[4].hostip, array_of_segs[4]);

	/* test do_segment_clustering_by_host */
	groups = do_segment_clustering_by_host();

	assert_int_equal(list_length(groups), 3);

	cell = list_nth_cell(groups, 0);
	gphost = (GpHost*)lfirst(cell);
	assert_string_equal(gphost->ip, "1.2.3.1");
	assert_int_equal(list_length(gphost->segs), 3);
	for (int i = 0; i < 3; ++i)
	{
		check_segment_info(gphost->segs, i, "1.2.3.1", array_for_host1[i]);
	}

	cell = list_nth_cell(groups, 1);
	gphost = (GpHost*)lfirst(cell);
	assert_string_equal(gphost->ip, "1.2.3.2");
	assert_int_equal(list_length(gphost->segs), 3);
	for (int i = 0; i < 3; ++i)
	{
		check_segment_info(gphost->segs, i, "1.2.3.2", array_for_host2[i]);
	}

	cell = list_nth_cell(groups, 2);
	gphost = (GpHost*)lfirst(cell);
	assert_string_equal(gphost->ip, "1.2.3.3");
	assert_int_equal(list_length(gphost->segs), 2);
	for (int i = 0; i < 2; ++i)
	{
		check_segment_info(gphost->segs, i, "1.2.3.3", array_for_host3[i]);
	}

	restoreCdbComponentDatabases();
}
