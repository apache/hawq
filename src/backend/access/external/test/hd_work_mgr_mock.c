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
 * Helper functions to create and restore GpAliveSegmentsInfo.cdbComponentDatabases element
 * used by hd_work_mgr
 */

/*
 * Builds an array of CdbComponentDatabaseInfo.
 * Each segment is assigned a sequence number and an ip.
 * segs_num - the number of segments
 * segs_hostips - array of the ip of each segment
 * primaries_map - array of which segments are primaries
 */
void buildCdbComponentDatabases(int segs_num,
								char* segs_hostips[],
								bool primaries_map[])
{
	CdbComponentDatabases *test_cdb = palloc0(sizeof(CdbComponentDatabases));
	CdbComponentDatabaseInfo* component = NULL;
	test_cdb->total_segment_dbs = segs_num;
	test_cdb->segment_db_info =
			(CdbComponentDatabaseInfo *) palloc0(sizeof(CdbComponentDatabaseInfo) * test_cdb->total_segment_dbs);

	for (int i = 0; i < test_cdb->total_segment_dbs; ++i)
	{
		component = &test_cdb->segment_db_info[i];
		component->segindex = i;
		component->role = primaries_map[i] ? SEGMENT_ROLE_PRIMARY : SEGMENT_ROLE_MIRROR;
		component->hostip = pstrdup(segs_hostips[i]);
	}

	orig_cdb = GpAliveSegmentsInfo.cdbComponentDatabases;
	orig_seg_count = GpAliveSegmentsInfo.aliveSegmentsCount;
	GpAliveSegmentsInfo.cdbComponentDatabases = test_cdb;
	GpAliveSegmentsInfo.aliveSegmentsCount = segs_num;
}

void restoreCdbComponentDatabases()
{
	/* free test CdbComponentDatabases */
	if (GpAliveSegmentsInfo.cdbComponentDatabases)
		freeCdbComponentDatabases(GpAliveSegmentsInfo.cdbComponentDatabases);

	GpAliveSegmentsInfo.cdbComponentDatabases = orig_cdb;
	GpAliveSegmentsInfo.aliveSegmentsCount = orig_seg_count;
}

/* Builds the QueryResource for a query */
void buildQueryResource(int segs_num,
                        char * segs_hostips[])
{
	resource = (QueryResource *)palloc(sizeof(QueryResource));

	resource->segments = NULL;
	for (int i = 0; i < segs_num; ++i)
	{
		Segment *segment = (Segment *)palloc0(sizeof(Segment));
		segment->hostip = pstrdup(segs_hostips[i]);
		segment->segindex = i;
		segment->alive = true;

		resource->segments = lappend(resource->segments, segment);
	}

	return resource;
}

/* Restores the QueryResource for a query */
void freeQueryResource()
{
	if (resource)
	{
		ListCell *cell;

		if (resource->segments)
		{
			cell = list_head(resource->segments);
			while(cell != NULL)
			{
				ListCell *tmp = cell;
				cell = lnext(cell);

				pfree(((Segment *)lfirst(tmp))->hostip);
				pfree(lfirst(tmp));
				pfree(tmp);
			}
		}

		pfree(resource);
		resource = NULL;
	}
}

