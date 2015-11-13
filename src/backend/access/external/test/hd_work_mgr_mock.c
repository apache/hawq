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
 * Helper functions to create and free QueryResource element
 * used by hd_work_mgr
 */

/* Builds the QueryResource for a query.
 * Each segment is assigned a sequence number and an ip.
 * segs_num - the number of segments
 * segs_hostips - array of the ip of each segment
 */
void buildQueryResource(int segs_num,
                        char * segs_hostips[])
{
	resource = (QueryResource *)palloc0(sizeof(QueryResource));

	resource->numSegments = segs_num;
	resource->segments = NULL;
	for (int i = 0; i < segs_num; ++i)
	{
		Segment *segment = (Segment *)palloc0(sizeof(Segment));
		segment->hostip = pstrdup(segs_hostips[i]);
		segment->segindex = i;
		segment->alive = true;

		resource->segments = lappend(resource->segments, segment);
	}
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

