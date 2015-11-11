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

#ifndef HD_WORK_MGR_MOCK_
#define HD_WORK_MGR_MOCK_

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "../hd_work_mgr.c"

static CdbComponentDatabases *orig_cdb = NULL;
static int orig_seg_count = -1;

static QueryResource * resource = NULL;

struct AliveSegmentsInfo
{
	uint64			fts_statusVersion;
	TransactionId	tid;

	/* Release gangs */
	bool			cleanGangs;

	/* Used for debug & test */
	bool			forceUpdate;
	int				failed_segmentid_start;
	int				failed_segmentid_number;

	/* Used for disptacher. */
	int4			aliveSegmentsCount;
	int4			singleton_segindex;
	Bitmapset		*aliveSegmentsBitmap;
	struct CdbComponentDatabases *cdbComponentDatabases;
};

typedef struct AliveSegmentsInfo AliveSegmentsInfo;

AliveSegmentsInfo GpAliveSegmentsInfo = {0, 0, false, false, 0, 0, UNINITIALIZED_GP_IDENTITY_VALUE, 0, NULL, NULL};

/*
 * Helper functions copied from backend/cdb/cdbutils.c
 */

/*
 * _freeCdbComponentDatabases
 *
 * Releases the storage occupied by the CdbComponentDatabases
 * struct pointed to by the argument.
 */
void
_freeCdbComponentDatabases(CdbComponentDatabases *pDBs);

/*
 * _freeCdbComponentDatabaseInfo:
 * Releases any storage allocated for members variables of a CdbComponentDatabaseInfo struct.
 */
void
_freeCdbComponentDatabaseInfo(CdbComponentDatabaseInfo *cdi);

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
								bool primaries_map[]);


void restoreCdbComponentDatabases();

void buildQueryResource(int segs_num,
                        char * segs_hostips[]);
void freeQueryResource();
                                  
#endif //HD_WORK_MGR_MOCK_
